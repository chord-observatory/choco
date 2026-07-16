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

PROVISIONAL (tune against live data): the default SK bounds and
``min_valid_frac``.

Standalone diagnostic:
``python -m sources.rfi --url http://cx27:12048/rfi_sk_metrics/sk_metrics_0/sk``
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request

from .common import choco_group_nodes, project

log = logging.getLogger("bffs.rfi")

# One path per RfiSKMetrics instance in the kotekan config (each GPU's
# instance covers its frequency band); override with ``sk_paths``.
_DEFAULT_SK_PATHS = (
    "rfi_sk_metrics/sk_metrics_0/sk",
    "rfi_sk_metrics/sk_metrics_1/sk",
)


def resolve_urls(src: dict) -> list[str]:
    """The ``/sk`` endpoints to poll.

    Explicit ``urls`` (or a single ``url``) win.  Otherwise the node list
    comes from choco's registry: every *started* node of ``group``
    (defaulting to the group bffs broadcasts to, via the injected choco
    context) is polled at each ``sk_paths`` entry — so new nodes are
    picked up without touching the bffs config.  ``sk_paths`` must match
    the RfiSKMetrics instances in the kotekan config.
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
    nodes = [n for n in choco_group_nodes(choco_url, group)
             if n.get("started")]
    if not nodes:
        # Nothing measurable is a failed run (red badge), the same as
        # every endpoint being unreachable — not a silent all-good.
        raise OSError(
            f"rfi: no started nodes in choco group {group!r} — "
            f"nothing to measure")
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


def mask(src: dict, labels, kotekan_file: str):
    """Good-mask over ``labels``: a feed whose SK is out of bounds is bad.

    A feed is bad if any polled endpoint with at least ``min_valid_frac``
    valid cells puts its SK outside ``[sk_lo, sk_hi]``.  An unreachable
    endpoint is skipped with a warning (this source only flags what it
    can measure — one down node must not stall flagging for the rest),
    but if *every* endpoint failed the run errors: that's a systematic
    problem worth a red badge, not silence.
    """
    urls = resolve_urls(src)
    sk_lo = float(src.get("sk_lo", 0.7))
    sk_hi = float(src.get("sk_hi", 1.5))
    min_valid = float(src.get("min_valid_frac", 0.25))
    input_good: dict[str, bool] = {}
    failures = 0
    for url in urls:
        try:
            readings = read_sk(url)
        except (OSError, ValueError) as e:
            log.warning("rfi: skipping %s: %s", url, e)
            failures += 1
            continue
        for element, (sk, valid_frac) in readings.items():
            if element >= len(labels) or sk is None:
                continue
            if valid_frac >= min_valid and not (sk_lo <= sk <= sk_hi):
                input_good[str(labels[element])] = False
    if urls and failures == len(urls):
        raise OSError(f"rfi: all {len(urls)} /sk endpoints unreachable")
    return project(input_good, labels)


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
    for url in args.url:
        for element, (sk, valid_frac) in sorted(read_sk(url).items()):
            flagged = (sk is not None and valid_frac >= args.min_valid_frac
                       and not (args.sk_lo <= sk <= args.sk_hi))
            elements.setdefault(element, []).append(
                {"sk": sk, "valid_frac": valid_frac, "flagged": flagged})

    print(json.dumps({
        "n_elements": len(elements),
        "flagged": sorted(e for e, r in elements.items() if any(x["flagged"] for x in r)),
        "elements": {str(e): r for e, r in elements.items()},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
