#!/usr/bin/env python3
"""bffs - a minimal feed-flagging script.

Run once per invocation (e.g. by a systemd timer or oneshot service): resolve
the feed labels, ask each configured source which feeds are bad, and POST the
bad-input list to choco.

Labels come from the kotekan config's ``dish_inputs`` table (fetched through
choco's ``/api/config/<group>``) — the same table kotekan indexes its bad-input
mask with — falling back to the N² file's own index map when choco isn't
available (dry runs).  The N² data file feeds the file-based sources
(power-outlier); when it is missing or stale those sources are skipped with a
warning and the rest still flag.

A small JSON file (``state.path`` in the config) records the change history of
the feeds — every transition, by stable feed label, with the flagging source(s)
per feed — and lets the script send to choco only when the bad list actually
changes. Without it, the script is stateless and sends every run.

    python bffs.py --config bffs.example.yaml

A feed is bad if *any* source flags it (the per-source good masks are AND-ed).
Each source is one module under ``sources/`` exposing
``mask(src, labels, kotekan_file)``; ``combine_sources`` dispatches via
``sources.get(kind)``. Built-in kinds: ``manual``, ``power-outlier``, ``power``,
``fpga``, ``rfi`` (see ``sources/``).

The flag values are ``{update_id, start_time, bad_inputs}``; ``start_time`` is
``now + sync_delay`` (a few seconds ahead) so every consumer switches flags at
the same moment. They are sent through choco's group-update API
(``POST /update/<group>`` with an ``updatable_config`` action), which relays
them to every kotekan node in the group at ``POST /<endpoint>``
(``updatable_config/bad_inputs``).
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import ssl
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import yaml

import sources
from kotekan_io import read_labels
from sources.common import choco_group_config

log = logging.getLogger("bffs")


# -- config ---------------------------------------------------------------


@dataclass
class Config:
    kotekan_file: str              # the kotekan N² output (may be a glob; newest match wins)
    max_age: float = 3600.0        # newest file older than this (s) -> fail, don't flag; 0 disables
    sources: list[dict] = field(default_factory=list)
    url: str | None = None         # choco base URL; unset -> payload printed, not sent
    group: str | None = None       # choco node group to broadcast to
    endpoint: str = "updatable_config/bad_inputs"  # kotekan updatable endpoint
    sync_delay: float = 5.0
    state_path: str | None = None  # JSON change-history file; unset -> stateless
    max_history: int = 0           # cap on history entries kept (0 = keep all)


def load_config(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    kotekan_file = raw.get("kotekan_file")
    if not kotekan_file:
        raise ValueError("config needs 'kotekan_file' (the kotekan N² output path)")
    choco = raw.get("choco") or {}
    if choco.get("url") and not choco.get("group"):
        raise ValueError("config needs 'choco.group' (the choco node group) when choco.url is set")
    state = raw.get("state") or {}
    return Config(
        kotekan_file=kotekan_file,
        max_age=float(raw.get("max_age", 3600)),
        sources=list(raw.get("sources") or []),
        url=choco.get("url"), group=choco.get("group"),
        endpoint=str(choco.get("endpoint", "updatable_config/bad_inputs")),
        sync_delay=float(choco.get("sync_delay", 5.0)),
        state_path=state.get("path"), max_history=int(state.get("max_history", 0)),
    )


# -- feed labels ----------------------------------------------------------


def find_dish_inputs(config) -> list | None:
    """The ``dish_inputs`` table from a rendered kotekan config, or None.

    Searched recursively — the table's nesting spot varies between
    config generations.
    """
    if isinstance(config, dict):
        value = config.get("dish_inputs")
        if isinstance(value, list) and value:
            return value
        for child in config.values():
            found = find_dish_inputs(child)
            if found is not None:
                return found
    return None


def dish_input_labels(config: dict, n_elements: int | None = None) -> list[str] | None:
    """Element labels from a kotekan config's ``dish_inputs`` table.

    Mirrors kotekan's ``CHORDTelescope``: an ``n_elements``-slot table,
    all ``Fake``, with each ``dish_inputs`` entry's ``label`` placed at
    its ``dish_idx`` — that table's positions are the indices kotekan's
    bad-input mask consumes.  Without ``n_elements`` the table is just
    big enough for the highest ``dish_idx``; a ``dish_idx`` beyond
    ``n_elements`` raises (the config and the data disagree about the
    element axis, so indexing would be ambiguous).  Returns None when
    the config has no usable ``dish_inputs``.
    """
    table = find_dish_inputs(config)
    if not table or not all(isinstance(e, dict) for e in table):
        return None
    by_idx = {}
    for i, entry in enumerate(table):
        idx = int(entry.get("dish_idx", i))
        by_idx[idx] = str(entry.get("label", f"dish{idx}"))
    n = max(by_idx) + 1 if n_elements is None else int(n_elements)
    if max(by_idx) >= n:
        raise ValueError(
            f"dish_idx {max(by_idx)} in the kotekan config exceeds the "
            f"{n}-element axis — config and data disagree; refusing to "
            f"flag with ambiguous indexing")
    return [by_idx.get(i, "Fake") for i in range(n)]


def uniquify_labels(labels) -> np.ndarray:
    """Suffix repeated labels with their element index (Fake -> Fake[7]).

    Placeholder elements share the label ``Fake``; state diffing and
    per-source projection key by label, so duplicates must be made
    per-element.  Unique labels pass through untouched.
    """
    from collections import Counter
    strs = [str(label) for label in labels]
    counts = Counter(strs)
    return np.array([f"{s}[{i}]" if counts[s] > 1 else s
                     for i, s in enumerate(strs)])


def resolve_kotekan_file(config: Config) -> str | None:
    """The newest usable N² file, or None (no match / too old).

    ``kotekan_file`` may be a glob spanning directories; the newest
    match by mtime wins.  A file older than ``max_age`` is unusable —
    a stopped acquisition's empty tail rows would mark every feed dead —
    but that only sidelines the file-based sources, not the run.
    """
    path = config.kotekan_file
    if path and any(c in path for c in "*?["):
        matches = glob.glob(path)
        path = max(matches, key=os.path.getmtime) if matches else None
    if path and not os.path.exists(path):
        path = None
    if path is None:
        log.warning("no kotekan file matches %r", config.kotekan_file)
        return None
    if config.max_age:
        age = time.time() - os.path.getmtime(path)
        if age > config.max_age:
            log.warning(
                "kotekan data stale: %s was last written %.1f h ago "
                "(max_age %.0f s); file-based sources skipped",
                path, age / 3600, config.max_age)
            return None
    log.info("kotekan file: %s", path)
    return path


def resolve_labels(config: Config, path: str | None) -> np.ndarray:
    """The element labels (and axis) every source masks against.

    The kotekan config's ``dish_inputs`` (fetched through choco) is the
    naming authority — it is the same table kotekan indexes its bad-input
    mask with, and its labels (``A1X``...) are the ones operators know.
    The file's own index map is the fallback (dry runs, choco down).
    When both are available the file fixes the axis length (implicit
    ``Fake`` dishes beyond the listed entries) and must agree with the
    config — a mismatch means the file predates the running config and
    positions would be ambiguous.
    """
    cfg = None
    if config.url and config.group:
        try:
            cfg = choco_group_config(config.url, config.group)
        except (OSError, ValueError) as e:
            log.warning("no kotekan config from choco: %s", e)
    file_labels = read_labels(path) if path else None
    if cfg is not None:
        n = len(file_labels) if file_labels is not None else None
        cfg_labels = dish_input_labels(cfg, n_elements=n)
        if cfg_labels is not None:
            return uniquify_labels(cfg_labels)
        log.warning("kotekan config has no dish_inputs; using file labels")
    if file_labels is not None:
        return uniquify_labels(file_labels)
    raise OSError(
        "no feed labels: no usable kotekan file and no dish_inputs "
        "from choco — nothing to index flags against")


# -- combine sources ------------------------------------------------------


def combine_sources(config: Config) -> tuple[np.ndarray, np.ndarray, dict, list]:
    """AND together each source's good-mask.

    Returns ``(labels, good, flagged_by, degraded)``; ``flagged_by``
    maps each bad feed's label to the source kinds that flagged it (the
    wire payload stays indices-only — attribution is bookkeeping for
    the state file and the web UI), and ``degraded`` lists reasons the
    run was incomplete (skipped sources) — the caller exits 2 so the
    badge shows *degraded*, not ok and not failed.

    A missing or stale kotekan file sidelines only the sources that need
    it (``NEEDS_FILE``, e.g. power-outlier) — the rest still flag, so a
    data outage doesn't take feed flagging down with it.  If *every*
    configured source is sidelined the run fails (red badge): nothing
    measurable is a systematic problem, not an all-good.

    Each source's config dict is passed with the choco context merged in
    as defaults (``choco_url`` / ``choco_group``; explicit keys win), so
    a source can derive per-node endpoints from choco's node registry —
    the rfi source polls every started node of the broadcast group unless
    given explicit ``urls``.
    """
    path = resolve_kotekan_file(config)
    labels = resolve_labels(config, path)
    good = np.ones(len(labels), dtype=bool)
    flagged_by: dict[str, list[str]] = {}
    skipped: list[str] = []
    for src in config.sources:
        kind = src["kind"]
        source = sources.get(kind)
        if source is None:
            raise ValueError(f"unknown source kind {kind!r}")
        if path is None and getattr(source, "NEEDS_FILE", False):
            log.warning("%s: no usable kotekan file; source skipped", kind)
            skipped.append(kind)
            continue
        src = {"choco_url": config.url, "choco_group": config.group, **src}
        mask = np.asarray(source.mask(src, labels, path), dtype=bool)
        for i in np.nonzero(~mask)[0]:
            flagged_by.setdefault(str(labels[i]), []).append(kind)
        good &= mask
    if config.sources and len(skipped) == len(config.sources):
        raise OSError(
            f"all {len(skipped)} sources skipped (no usable kotekan "
            f"file) — nothing to measure")
    degraded = []
    if skipped:
        degraded.append("no usable kotekan file — skipped: "
                        + ", ".join(skipped))
    return labels, good, flagged_by, degraded


# -- state / change history ----------------------------------------------


def run(
    config: Config, *, now: float | None = None, force: bool = False, write: bool = True,
    sender=None,
) -> tuple[dict, bool, list]:
    """Evaluate the sources, send if needed, and update the change-history state.

    Returns ``(payload, send, degraded)``; ``degraded`` lists reasons
    the run was incomplete (see :func:`combine_sources`) for the caller
    to turn into exit code 2.  With ``state.path`` set, the bad-feed set
    (tracked by stable feed *label*) is diffed against the last recorded run: a
    change makes ``send`` true and appends a history entry to the (re)written
    file; an unchanged run sends nothing unless ``force``. Without a state file
    every run sends. ``write=False`` (dry run) computes the diff but writes
    nothing.

    When ``sender`` (a callable taking the payload) is given, it is invoked
    *before* the state is written — a failed send leaves the state file
    untouched, so the next run sees the change again and retries.
    """
    now = time.time() if now is None else now
    labels, good, flagged_by, degraded = combine_sources(config)
    bad_idx = np.nonzero(~good)[0]
    payload = {
        "update_id": f"bffs-{int(now * 1000)}",
        "start_time": now + config.sync_delay,
        "bad_inputs": [int(i) for i in bad_idx],
    }
    if not config.state_path:
        if sender is not None:
            sender(payload)
        return payload, True, degraded

    # Load prior state; a missing or corrupt file is treated as a first run.
    state_file = Path(config.state_path)
    state = None
    if state_file.exists():
        try:
            state = json.loads(state_file.read_text() or "{}")
        except json.JSONDecodeError:
            log.warning("state file %s is corrupt; starting fresh", state_file)

    bad_labels = sorted(str(labels[i]) for i in bad_idx)
    prev = None if state is None else list(state.get("bad_inputs", []))
    if prev is None:  # first run: record the baseline
        became_bad, became_good, changed = bad_labels, [], True
    else:
        became_bad = sorted(set(bad_labels) - set(prev))
        became_good = sorted(set(prev) - set(bad_labels))
        changed = bool(became_bad or became_good)

    send = changed or force
    if send and sender is not None:
        sender(payload)  # deliver first; a raised error leaves the state unwritten

    if changed and write:
        state = state or {}
        state["updated"] = now
        state["update_id"] = payload["update_id"]
        state["bad_inputs"] = bad_labels
        # which source(s) flagged each feed, as of this change — display
        # bookkeeping only, never part of the payload sent to kotekan
        state["flagged_by"] = {label: flagged_by.get(label, [])
                               for label in bad_labels}
        history = state.get("history", [])
        history.append({
            "time": now,
            "update_id": payload["update_id"],
            "became_bad": became_bad,
            "became_good": became_good,
            "bad_inputs": bad_labels,
        })
        if config.max_history and len(history) > config.max_history:
            history = history[-config.max_history:]
        state["history"] = history
        # Write atomically: temp file + rename.
        state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_file.with_name(state_file.name + ".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        tmp.replace(state_file)

    return payload, send, degraded


# -- send & CLI -----------------------------------------------------------


def send_to_choco(config: Config, payload: dict) -> None:
    """POST the flag values to choco's group-update API.

    choco accepts ``{"action": "updatable_config", "endpoint": ..., "values":
    ...}`` at ``POST /update/<group>`` and relays the values to every kotekan
    node in the group. choco serves a self-signed certificate (and bypasses
    auth only for localhost callers), so TLS goes unverified — point the URL
    at choco on localhost.
    """
    data = json.dumps({
        "action": "updatable_config",
        "endpoint": config.endpoint,
        "values": payload,
    }).encode()
    url = config.url.rstrip("/") + f"/update/{config.group}"
    req = urllib.request.Request(
        url, data=data, method="POST", headers={"Content-Type": "application/json"},
    )
    ctx = None
    if url.startswith("https:"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=10.0, context=ctx) as resp:
        resp.read()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="bffs", description="feed-flagging script")
    p.add_argument("-c", "--config", required=True, help="path to YAML config")
    p.add_argument("--kotekan-file", default=None, help="override the kotekan N² output path")
    p.add_argument("-n", "--dry-run", action="store_true", help="compute only; write and send nothing")
    p.add_argument("-f", "--force", action="store_true", help="send even if the bad list is unchanged")
    p.add_argument("-v", "--verbose", action="count", default=0)
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING - 10 * min(args.verbose, 2),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    try:
        config = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        return 1
    if args.kotekan_file:
        config.kotekan_file = args.kotekan_file

    sender = None
    if not args.dry_run and config.url:
        sender = lambda payload: send_to_choco(config, payload)  # noqa: E731

    # Exit codes (shared job convention, read by choco's badge):
    #   0 ok; 2 degraded — the job is fine but a dependency or input
    #   wasn't (no/stale data, choco or nodes unreachable; retries
    #   self-heal); 1 failed — config error or bug, needs a human.
    try:
        payload, send, degraded = run(
            config, force=args.force, write=not args.dry_run, sender=sender)
    except OSError as e:
        # Environmental: no usable kotekan file with nothing else to
        # measure, unreadable HDF5, choco/nodes unreachable (urllib
        # errors are OSError). One useful line; -vv adds the traceback.
        log.error("%s: %s", type(e).__name__, e)
        log.debug("traceback:", exc_info=True)
        return 2
    except (ValueError, yaml.YAMLError) as e:
        # Config or consistency errors (unknown source kind, element
        # count mismatch, a bad override file) — needs a human.
        log.error("%s: %s", type(e).__name__, e)
        log.debug("traceback:", exc_info=True)
        return 1
    if args.dry_run or not config.url:
        print(json.dumps(payload))
        log.info("not sent (%s)", "dry run" if args.dry_run else "no choco url")
    elif send:
        log.info("sent %s (%d bad)", payload["update_id"], len(payload["bad_inputs"]))
    else:
        log.info("unchanged; nothing sent")
    if degraded:
        log.warning("degraded run: %s", "; ".join(degraded))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
