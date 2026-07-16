#!/usr/bin/env python3
"""bffs - a minimal feed-flagging script.

Run once per invocation (e.g. by a systemd timer or oneshot service): read the
kotekan N² output's feed labels (``index_map/input``), ask each configured
source which feeds are bad, and POST the bad-input list to choco.

A small JSON file (``state.path`` in the config) records the change history of
the feeds — every transition, by stable feed label — and lets the script send to
choco only when the bad list actually changes. Without it, the script is
stateless and sends every run.

    python bffs.py --config bffs.example.yaml

A feed is bad if *any* source flags it (the per-source good masks are AND-ed).
Each source is one module under ``sources/`` exposing
``mask(src, labels, kotekan_file)``; ``combine_sources`` dispatches via
``sources.get(kind)``. Built-in kinds: ``manual``, ``power-outlier``, ``power``,
``fpga`` (see ``sources/`` and REVIEW.md).

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

log = logging.getLogger("bffs")


# -- config ---------------------------------------------------------------


@dataclass
class Config:
    kotekan_file: str              # the kotekan N² output (may be a glob; newest match wins)
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
        kotekan_file=kotekan_file, sources=list(raw.get("sources") or []),
        url=choco.get("url"), group=choco.get("group"),
        endpoint=str(choco.get("endpoint", "updatable_config/bad_inputs")),
        sync_delay=float(choco.get("sync_delay", 5.0)),
        state_path=state.get("path"), max_history=int(state.get("max_history", 0)),
    )


# -- combine sources ------------------------------------------------------


def combine_sources(config: Config) -> tuple[np.ndarray, np.ndarray]:
    """AND together each source's good-mask. Returns ``(labels, good)``.

    Feed labels and order come from the kotekan file's index map; each source
    returns a mask in that same order, and a feed is bad if any flags it.
    ``kotekan_file`` may be a glob pattern — the newest match (by mtime) is
    read, so a timer-driven bffs follows kotekan's current output file.
    """
    path = config.kotekan_file
    if any(c in path for c in "*?["):
        matches = glob.glob(path)
        if not matches:
            raise FileNotFoundError(f"no kotekan file matches {path!r}")
        path = max(matches, key=os.path.getmtime)
        log.info("kotekan file: %s", path)
    labels = read_labels(path)
    good = np.ones(len(labels), dtype=bool)
    for src in config.sources:
        source = sources.get(src["kind"])
        if source is None:
            raise ValueError(f"unknown source kind {src['kind']!r}")
        good &= source.mask(src, labels, path)
    return labels, good


# -- state / change history ----------------------------------------------


def run(
    config: Config, *, now: float | None = None, force: bool = False, write: bool = True,
    sender=None,
) -> tuple[dict, bool]:
    """Evaluate the sources, send if needed, and update the change-history state.

    Returns ``(payload, send)``. With ``state.path`` set, the bad-feed set
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
    labels, good = combine_sources(config)
    bad_idx = np.nonzero(~good)[0]
    payload = {
        "update_id": f"bffs-{int(now * 1000)}",
        "start_time": now + config.sync_delay,
        "bad_inputs": [int(i) for i in bad_idx],
    }
    if not config.state_path:
        if sender is not None:
            sender(payload)
        return payload, True

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

    return payload, send


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

    try:
        payload, send = run(config, force=args.force, write=not args.dry_run, sender=sender)
    except (OSError, ValueError) as e:
        # Expected environmental failures — no kotekan N² file (yet), an
        # unreadable HDF5 (h5py raises OSError), choco not up (urllib
        # errors are OSError), a source misconfigured (ValueError) — get
        # one useful line instead of a traceback.  Still exit nonzero:
        # systemd records the failure and the next timer tick retries.
        # Anything else is a bug and keeps its traceback — and since a
        # bug can also surface as ValueError/OSError, -vv shows the
        # full traceback for these too.
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
