"""rfi source — feeds whose per-feed spectral kurtosis (SK) is out of bounds.

Read-only. Polls the ``/sk`` GET endpoints served by kotekan's RfiSKMetrics
stage (one per stage instance; each GPU's instance covers its frequency band),
which return ``{"sk": [...], "valid_frac": [...]}`` indexed by element. Clean
Gaussian noise has SK ~= 1; a feed sitting persistently away from 1 is
carrying RFI or a broken signal chain. kotekan computes the single-feed SK for
every feed regardless of the current bad-feed mask (n2k SkKernel), so a
flagged feed keeps being measured and heals when it recovers — no latch.

Element indices are positions in the feed-label list (the CHORD [P][D] element
order, the same indexing bffs sends in ``bad_inputs``), so no channel->input
map is needed. Elements never measured (``sk`` null), beyond the labelled
feeds, or with too few valid cells are left good: this source only flags what
it can measure.

Robustness to X-engine nodes being down: the endpoints come from choco's
node registry, and a node choco's sync loop reports ``down`` or ``idle``
is not polled (its frequency band is simply unmeasured this run); an
endpoint that fails anyway is skipped.  Both are reported ``degraded``
with the node named, never silently.  The ``/sk`` values are exponential
moving averages that freeze when a stage stops receiving frames, so each
node's ``/metrics`` is read for the SK gauges' last-update timestamps and
an instance older than ``max_stale_s`` is ignored (reported ``stale``);
when ``/metrics`` cannot be read the freshness is unknown and the
readings are used.

PROVISIONAL (tune against live data): the default SK bounds and
``min_valid_frac``.

Standalone diagnostic:
``python -m sources.rfi --url http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk``
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import urllib.parse
import urllib.request

from .common import choco_group_nodes, iter_samples, project, report

log = logging.getLogger("bffs.rfi")

# One path per RfiSKMetrics instance in the kotekan config (each GPU's
# instance covers its frequency band); override with ``sk_paths``.
_DEFAULT_SK_PATHS = (
    "rfi_sk_metrics/sk_metrics_0/sk",
    "rfi_sk_metrics/sk_metrics_1/sk",
)
#: A gauge RfiSKMetrics sets on every frame; its Prometheus timestamp is
#: when the stage last saw data.
_FRESHNESS_GAUGE = "kotekan_rfi_sk_per_feed_valid_frac"
#: SK readings whose gauges were last set longer ago than this are frozen
#: EMAs from before the data stopped and are ignored.  The EMA e-folds in
#: ~11 s at the pathfinder frame rate, so a minute is generous.
DEFAULT_MAX_STALE_S = 60.0
#: choco node statuses under which a node's /sk is not worth polling.
_NOT_POLLED = ("down", "idle")


def resolve_urls(src: dict, skipped: list[dict] | None = None) -> list[str]:
    """The ``/sk`` endpoints to poll.

    Explicit ``urls`` (or a single ``url``) win.  Otherwise the node list
    comes from choco's registry: every *started* node of ``group``
    (defaulting to the group bffs broadcasts to, via the injected choco
    context) is polled at each ``sk_paths`` entry — so new nodes are
    picked up without touching the bffs config.  ``sk_paths`` must match
    the RfiSKMetrics instances in the kotekan config.  A node whose live
    status (choco's last probe) is ``down`` or ``idle`` is left out and,
    when ``skipped`` is given, recorded there as ``{"node", "status"}``.
    No pollable node at all gives an empty list: the caller abstains.
    """
    if "urls" in src:
        urls = list(src["urls"])
        if not urls:
            raise ValueError("rfi source: 'urls' is empty")
        return urls
    if "url" in src:
        return [src["url"]]
    choco_url = src.get("choco_url")
    group = src.get("group") or src.get("choco_group")
    if not choco_url or not group:
        raise ValueError(
            "rfi source needs explicit 'urls', or choco url + group "
            "(from the config's choco block) to derive them from")
    paths = [str(p).lstrip("/")
             for p in (src.get("sk_paths") or _DEFAULT_SK_PATHS)]
    nodes = []
    for n in choco_group_nodes(choco_url, group):
        if not n.get("started"):
            continue
        status = n.get("status")
        if status in _NOT_POLLED:
            log.warning("rfi: %s is %s per choco; not polled", n.get("name"), status)
            if skipped is not None:
                skipped.append({"node": str(n.get("name")), "status": str(status)})
            continue
        nodes.append(n)
    return [f"http://{n['host']}:{n.get('port', 12048)}/{p}"
            for n in nodes for p in paths]


def read_sk(url: str) -> dict[int, tuple[float | None, float]]:
    """GET one RfiSKMetrics ``/sk`` endpoint (read-only).

    Returns ``{element: (sk, valid_frac)}``; ``sk`` is None for elements with
    no valid measurement yet.
    """
    with urllib.request.urlopen(url, timeout=10.0) as resp:
        data = json.loads(resp.read())
    return {e: (sk, vf) for e, (sk, vf) in enumerate(zip(data["sk"], data["valid_frac"]))}


def split_sk_url(url: str) -> tuple[str, str]:
    """``(node base URL, stage_name)`` of an ``/sk`` endpoint URL.

    ``http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk`` ->
    ``("http://cx27:12048", "/rfi_sk_metrics/sk_metrics_0")`` — the
    ``stage_name`` label kotekan puts on the stage's gauges.
    """
    parts = urllib.parse.urlsplit(url)
    path = parts.path
    if path.endswith("/sk"):
        path = path[: -len("/sk")]
    return f"{parts.scheme}://{parts.netloc}", "/" + path.strip("/")


def read_sk_freshness(base_url: str) -> dict[str, float]:
    """``{stage_name: epoch seconds the SK gauges were last set}`` from a
    node's ``/metrics`` (read-only).  Stages without a timestamped gauge
    are absent."""
    url = base_url.rstrip("/") + "/metrics"
    with urllib.request.urlopen(url, timeout=10.0) as resp:
        text = resp.read().decode("utf-8", "replace")
    newest: dict[str, float] = {}
    for name, labels, _value, ts in iter_samples(text):
        if name != _FRESHNESS_GAUGE or ts is None:
            continue
        stage = labels.get("stage_name", "")
        newest[stage] = max(newest.get(stage, 0.0), ts / 1000.0)
    return newest


def mask(src: dict, labels, kotekan_file: str):
    """Good-mask over ``labels``: a feed whose SK is out of bounds is bad.

    A feed is bad if any polled endpoint with at least ``min_valid_frac``
    valid cells puts its SK outside ``[sk_lo, sk_hi]``.  Endpoints that
    cannot be read, or whose gauges are older than ``max_stale_s`` (0
    disables the check), contribute nothing — this source only flags what
    it can measure — and the run is reported ``degraded`` naming them.
    Nothing pollable at all (no started node up, every endpoint failed or
    stale) is the same report with no feed judged; the core fails the run
    only if *every* source ends up with nothing to measure.
    """
    skipped_nodes: list[dict] = []
    urls = resolve_urls(src, skipped_nodes)
    sk_lo = float(src.get("sk_lo", 0.7))
    sk_hi = float(src.get("sk_hi", 1.5))
    min_valid = float(src.get("min_valid_frac", 0.25))
    max_stale = float(src.get("max_stale_s", DEFAULT_MAX_STALE_S))
    now = time.time()
    axis = [str(lbl) for lbl in labels]
    input_good: dict[str, bool] = {}
    measured: set[int] = set()
    endpoints: list[dict] = []
    freshness: dict[str, dict[str, float] | None] = {}
    failures = stale = 0
    for url in urls:
        entry: dict = {"url": url, "ok": False}
        try:
            readings = read_sk(url)
        except (OSError, ValueError) as e:
            log.warning("rfi: skipping %s: %s", url, e)
            entry["error"] = str(e)[:200]
            failures += 1
            endpoints.append(entry)
            continue
        entry["ok"] = True
        if max_stale > 0:
            base, stage = split_sk_url(url)
            if base not in freshness:
                try:
                    freshness[base] = read_sk_freshness(base)
                except (OSError, ValueError) as e:
                    log.info("rfi: no freshness for %s (%s); readings used", base, e)
                    freshness[base] = None
            stamps = freshness[base]
            ts = stamps.get(stage) if stamps else None
            if ts is not None:
                age = max(now - ts, 0.0)        # node clocks run slightly ahead
                entry["age_s"] = round(age, 1)
                if age > max_stale:
                    log.warning("rfi: %s last updated %.0f s ago (> %.0f s); "
                                "readings ignored", url, age, max_stale)
                    entry["stale"] = True
                    stale += 1
                    endpoints.append(entry)
                    continue
        n_meas = n_flag = 0
        for element, (sk, valid_frac) in readings.items():
            if element >= len(axis) or sk is None or valid_frac < min_valid:
                continue
            n_meas += 1
            measured.add(element)
            if not (sk_lo <= sk <= sk_hi):
                input_good[axis[element]] = False
                n_flag += 1
        entry.update(n_measured=n_meas, n_flagged=n_flag)
        endpoints.append(entry)

    problems = []
    if skipped_nodes:
        problems.append("not polled (per choco): " + ", ".join(
            f"{n['node']} {n['status']}" for n in skipped_nodes))
    if failures:
        problems.append(f"{failures} of {len(urls)} /sk endpoints unreachable")
    if stale:
        problems.append(f"{stale} of {len(urls)} /sk endpoints stale (> {max_stale:.0f} s)")
    if not urls and not skipped_nodes:
        problems.append("no started nodes in the group: nothing to measure")
    status = "degraded" if problems else "ok"
    reason = "; ".join(problems) if problems else None
    if problems and not measured:
        log.warning("rfi: nothing measured — %s", reason)
    return project(input_good, labels), report(
        status, reason, n_measured=len(measured),
        n_endpoints=len(urls), n_failed=failures, n_stale=stale,
        skipped_nodes=skipped_nodes, endpoints=endpoints,
        sk_bounds=[sk_lo, sk_hi], min_valid_frac=min_valid, max_stale_s=max_stale)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sources.rfi",
                                description="report per-feed SK from kotekan (read-only)")
    p.add_argument("-u", "--url", action="append", required=True,
                   help="RfiSKMetrics /sk endpoint URL (repeatable), e.g. "
                        "http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk")
    p.add_argument("--sk-lo", type=float, default=0.7)
    p.add_argument("--sk-hi", type=float, default=1.5)
    p.add_argument("--min-valid-frac", type=float, default=0.25)
    args = p.parse_args(argv)

    elements: dict[int, list[dict]] = {}
    ages: dict[str, float | None] = {}
    now = time.time()
    for url in args.url:
        base, stage = split_sk_url(url)
        try:
            ts = read_sk_freshness(base).get(stage)
        except (OSError, ValueError):
            ts = None
        ages[url] = round(now - ts, 1) if ts is not None else None
        for element, (sk, valid_frac) in sorted(read_sk(url).items()):
            flagged = (sk is not None and valid_frac >= args.min_valid_frac
                       and not (args.sk_lo <= sk <= args.sk_hi))
            elements.setdefault(element, []).append(
                {"sk": sk, "valid_frac": valid_frac, "flagged": flagged})

    print(json.dumps({
        "n_elements": len(elements),
        "flagged": sorted(e for e, r in elements.items() if any(x["flagged"] for x in r)),
        "gauge_age_s": ages,
        "elements": {str(e): r for e, r in elements.items()},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
