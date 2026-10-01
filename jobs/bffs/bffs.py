#!/usr/bin/env python3
"""bffs - a minimal feed-flagging script.

Run once per invocation (e.g. by a systemd timer or oneshot service): resolve
the feed labels, ask each configured source which feeds are bad, and POST the
bad-input list to choco.

Labels come from the kotekan config's ``dish_inputs`` table (fetched through
choco's ``/api/config/<group>``) — the same table kotekan indexes its bad-input
mask with — falling back to the N² file's own index map when choco isn't
available (dry runs).  Both table layouts are handled: the pre-2026-08 tables
name every element (``A1X``), the 2026-08 kotekan layout names each dish once
(``A1``) and the element axis is [P][D] (``element = dish_idx + pol *
num_dishes``), so per-element labels are derived as label + X/Y — reproducing
the old names, which keeps the label-keyed hardware maps working.  The N² data file feeds the file-based sources
(power-outlier); when it is missing or stale those sources are skipped with a
warning and the rest still flag.

A small JSON file (``state.path`` in the config) records the change history of
the feeds — every transition, by stable feed label, with the flagging source(s)
per feed — and lets the script send to choco only when the bad list actually
changes. Without it, the script is stateless and sends every run.  A second
file (``state.run_path``, default ``run.json`` beside the state file) is
rewritten on *every* run with how it went: exit status, the degraded
reasons, the N² file used, and each source's report — what it measured,
what it flagged, why it abstained.  choco's BFFS page reads it.

    python bffs.py --config bffs.example.yaml

A feed is bad if *any* source flags it (the per-source good masks are AND-ed).
Each source is one module under ``sources/`` exposing
``mask(src, labels, kotekan_file)`` — a good-mask, or ``(mask, report)`` with
a ``sources.common.report`` saying how the measurement went; ``combine_sources``
dispatches via ``sources.get(kind)``. Built-in kinds: ``manual``,
``power-outlier``, ``power``, ``fpga``, ``rfi`` (see ``sources/``).  A source
that cannot measure abstains (leaves feeds good) and reports ``degraded``
rather than flagging; the run then exits 2 with the reason on record.

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
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import yaml

import sources
from choco.dishlabels import (PLACEHOLDER_LABEL, expand_dish_labels,
                              find_dish_inputs, find_key,
                              labels_are_per_element)
from choco.jobclient import post_json, write_json_atomic
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
    run_path: str | None = None    # per-run status file (every run); unset -> none


def load_config(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    kotekan_file = raw.get("kotekan_file")
    if not kotekan_file:
        raise ValueError("config needs 'kotekan_file' (the kotekan N² output path)")
    choco = raw.get("choco") or {}
    if choco.get("url") and not choco.get("group"):
        raise ValueError("config needs 'choco.group' (the choco node group) when choco.url is set")
    state = raw.get("state") or {}
    state_path = state.get("path")
    # The run file defaults to a sibling of the state file, so a deployed
    # config from before it existed grows one without an edit.
    run_path = state.get("run_path")
    if not run_path and state_path:
        run_path = str(Path(state_path).with_name("run.json"))
    return Config(
        kotekan_file=kotekan_file,
        max_age=float(raw.get("max_age", 3600)),
        sources=list(raw.get("sources") or []),
        url=choco.get("url"), group=choco.get("group"),
        endpoint=str(choco.get("endpoint", "updatable_config/bad_inputs")),
        sync_delay=float(choco.get("sync_delay", 5.0)),
        state_path=state_path, max_history=int(state.get("max_history", 0)),
        run_path=str(run_path) if run_path else None,
    )


# -- feed labels ----------------------------------------------------------


def _config_int(config, key, default=None):
    """*key* as an int, *default* when absent; raises on an expression.

    kotekan evaluates arithmetic expressions in config values
    (``num_elements: num_polarizations * 64``); choco and bffs do not.
    A key that is present but not a plain integer sizes the flag axis
    and cannot be guessed at.
    """
    value = find_key(config, key)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"kotekan config {key} = {value!r} is not a plain integer "
            f"(a kotekan expression?) — cannot size the element axis")


def element_labels_from_config(config: dict, file_labels=None) -> list[str] | None:
    """Element labels from a kotekan config's per-dish ``dish_inputs`` table.

    Only the 2026-08 per-dish layout is accepted: the table names each
    dish once (``A1``) and the element axis is [P][D] —
    ``num_polarizations`` blocks of ``num_dishes``, ``element = dish_idx
    + pol * num_dishes`` — so per-element labels are derived as label +
    X/Y.  A pre-2026-08 per-element table (labels like ``A1X``, see
    ``dishlabels.labels_are_per_element``) is REFUSED: its element
    ordering was wrong, and indexing kotekan's bad-input mask with it
    would flag the wrong feeds.  Raises ``OSError`` so the run reports
    degraded (exit 2) and heals once the config is migrated, with no
    job-side action needed.

    ``file_labels`` is the N² file's element axis
    (``kotekan_io.read_labels``); the config is the naming authority and
    the file is a cross-check — a file whose axis disagrees with the
    config predates it, and positions would be ambiguous.
    """
    table = find_dish_inputs(config)
    if not table or not all(isinstance(e, dict) for e in table):
        return None
    if labels_are_per_element(str(e.get("label", "")) for e in table):
        raise OSError(
            "the kotekan config still carries a pre-2026-08 per-element "
            "dish_inputs table — its element ordering is untrustworthy; "
            "refusing to flag until the config is migrated to the "
            "per-dish layout")
    by_idx = {}
    for i, entry in enumerate(table):
        idx = int(entry.get("dish_idx", i))
        by_idx[idx] = str(entry.get("label", f"dish{idx}"))
    ndish = _config_int(config, "num_dishes")
    if ndish is None:
        ndish = max(by_idx) + 1
        log.warning("kotekan config has no plain num_dishes; using the "
                    "dish_inputs table's %d", ndish)
    if max(by_idx) >= ndish:
        raise ValueError(
            f"dish_idx {max(by_idx)} in the kotekan config exceeds "
            f"num_dishes ({ndish}) — refusing to flag with ambiguous "
            f"indexing")
    npol = _config_int(config, "num_polarizations", default=2)
    dish_labels = [by_idx.get(i, PLACEHOLDER_LABEL) for i in range(ndish)]
    labels = list(expand_dish_labels(dish_labels, npol))
    if file_labels is not None and [str(l) for l in file_labels] != labels:
        raise ValueError(
            "the N² file's element axis disagrees with the kotekan "
            "config's dish_inputs (a file from before the running "
            "config?) — refusing to flag with ambiguous indexing")
    return labels


def uniquify_labels(labels) -> np.ndarray:
    """Suffix repeated labels with their element index (Missing -> Missing[7]).

    Placeholder elements share the label ``Missing`` (``MissingX``/``MissingY``
    on a per-dish axis); state diffing and per-source projection key by
    label, so duplicates must be made per-element.  Unique labels pass
    through untouched.
    """
    from collections import Counter
    strs = [str(label) for label in labels]
    counts = Counter(strs)
    return np.array([f"{s}[{i}]" if counts[s] > 1 else s
                     for i, s in enumerate(strs)])


def resolve_kotekan_file(config: Config, run: dict | None = None) -> str | None:
    """The newest usable N² file, or None (no match / too old).

    ``kotekan_file`` may be a glob spanning directories; the newest
    match by mtime wins.  A file older than ``max_age`` is unusable —
    the acquisition that wrote it has stopped, and its data says nothing
    about the feeds now — but that only sidelines the file-based
    sources, not the run.  *run*, if given, records the newest match, its
    age and why it was passed over (``kotekan_file``,
    ``kotekan_file_age_s``, ``kotekan_file_reason``).
    """
    run = {} if run is None else run
    path = config.kotekan_file
    if path and any(c in path for c in "*?["):
        matches = glob.glob(path)
        path = max(matches, key=os.path.getmtime) if matches else None
    if path and not os.path.exists(path):
        path = None
    run["kotekan_file"] = path
    run["kotekan_file_age_s"] = None
    run["kotekan_file_reason"] = None
    if path is None:
        log.warning("no kotekan file matches %r", config.kotekan_file)
        run["kotekan_file_reason"] = f"no file matches {config.kotekan_file}"
        return None
    age = time.time() - os.path.getmtime(path)
    run["kotekan_file_age_s"] = round(age, 1)
    if config.max_age and age > config.max_age:
        log.warning(
            "kotekan data stale: %s was last written %.1f h ago "
            "(max_age %.0f s); file-based sources skipped",
            path, age / 3600, config.max_age)
        run["kotekan_file_reason"] = (
            f"last written {age / 3600:.1f} h ago, older than max_age "
            f"{config.max_age:.0f} s")
        return None
    log.info("kotekan file: %s", path)
    return path


def resolve_labels(config: Config, path: str | None) -> np.ndarray:
    """The element labels (and axis) every source masks against.

    The kotekan config's ``dish_inputs`` (fetched through choco) is the
    naming authority — it is the same table kotekan indexes its bad-input
    mask with, and its labels (``A1X``...; derived as label + X/Y when
    the table is the 2026-08 per-dish layout) are the ones operators
    know.  The file's own index map is the fallback (dry runs, choco
    down; ``read_labels`` spells the file's per-element ``B4p1`` labels
    the same way).  When both are available they must agree — a mismatch means
    the file predates the running config and positions would be
    ambiguous (see :func:`element_labels_from_config`).
    """
    cfg = None
    if config.url and config.group:
        try:
            cfg = choco_group_config(config.url, config.group)
        except (OSError, ValueError) as e:
            log.warning("no kotekan config from choco: %s", e)
    file_labels = read_labels(path) if path else None
    if cfg is not None:
        cfg_labels = element_labels_from_config(cfg, file_labels=file_labels)
        if cfg_labels is not None:
            return uniquify_labels(cfg_labels)
        log.warning("kotekan config has no dish_inputs; using file labels")
    if file_labels is not None:
        return uniquify_labels(file_labels)
    raise OSError(
        "no feed labels: no usable kotekan file and no dish_inputs "
        "from choco — nothing to index flags against")


# -- combine sources ------------------------------------------------------


def _source_report(kind: str, rep, mask: np.ndarray) -> dict:
    """Normalise what a source returned beside its mask into a run-file entry.

    A bare mask (``rep`` None) is an ``ok`` report.  Anything else must be
    a ``sources.common.report`` dict; a status other than ``ok`` /
    ``degraded`` is a source bug (``ValueError``, exit 1).
    """
    out = {"kind": kind, "status": "ok", "reason": None, "n_measured": None,
           "n_flagged": int(np.count_nonzero(~mask)), "detail": {}}
    if rep is None:
        return out
    if not isinstance(rep, dict):
        raise ValueError(f"source {kind!r} returned a {type(rep).__name__} "
                         f"instead of a report dict")
    status = str(rep.get("status") or "ok")
    if status not in ("ok", "degraded"):
        raise ValueError(f"source {kind!r} reported status {status!r}")
    out["status"] = status
    out["reason"] = str(rep["reason"]) if rep.get("reason") else None
    n_measured = rep.get("n_measured")
    out["n_measured"] = int(n_measured) if n_measured is not None else None
    detail = rep.get("detail")
    out["detail"] = dict(detail) if isinstance(detail, dict) else {}
    return out


def combine_sources(config: Config, run: dict | None = None,
                    ) -> tuple[np.ndarray, np.ndarray, dict, list]:
    """AND together each source's good-mask.

    Returns ``(labels, good, flagged_by, degraded)``; ``flagged_by``
    maps each bad feed's label to the source kinds that flagged it (the
    wire payload stays indices-only — attribution is bookkeeping for
    the state file and the web UI), and ``degraded`` lists reasons the
    run was incomplete (skipped sources, sources that abstained or lost
    coverage) — the caller exits 2 so the badge shows *degraded*, not
    ok and not failed.

    *run*, if given, is filled in as the run proceeds (so a caller that
    catches an exception still has the partial record): the N² file
    facts from :func:`resolve_kotekan_file`, ``n_elements``, the same
    ``degraded`` list, and ``sources`` — one normalised report per
    configured source, in config order, including the ones skipped.

    A missing or stale kotekan file sidelines only the sources that need
    it (``NEEDS_FILE``, e.g. power-outlier) — the rest still flag, so a
    data outage doesn't take feed flagging down with it.  If *every*
    configured source is sidelined or measured nothing, the run fails
    (``OSError``, exit 2): nothing measurable is a systematic problem,
    not an all-good.

    Each source's config dict is passed with the choco context merged in
    as defaults (``choco_url`` / ``choco_group``; explicit keys win), so
    a source can derive per-node endpoints from choco's node registry —
    the rfi source polls every started node of the broadcast group unless
    given explicit ``urls``.
    """
    run = {} if run is None else run
    degraded: list[str] = []
    reports: list[dict] = []
    run["degraded"] = degraded
    run["sources"] = reports
    path = resolve_kotekan_file(config, run)
    labels = resolve_labels(config, path)
    run["n_elements"] = len(labels)
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
            reports.append({
                "kind": kind, "status": "skipped",
                "reason": "no usable kotekan file: "
                          + str(run.get("kotekan_file_reason") or "none"),
                "n_measured": 0, "n_flagged": 0, "detail": {}})
            continue
        src = {"choco_url": config.url, "choco_group": config.group, **src}
        result = source.mask(src, labels, path)
        mask, rep = result if isinstance(result, tuple) else (result, None)
        mask = np.asarray(mask, dtype=bool)
        if mask.shape != (len(labels),):
            raise ValueError(f"source {kind!r} returned a mask of shape "
                             f"{mask.shape} for {len(labels)} elements")
        report = _source_report(kind, rep, mask)
        reports.append(report)
        if report["status"] == "degraded":
            degraded.append(f"{kind}: {report['reason'] or 'degraded'}")
        for i in np.nonzero(~mask)[0]:
            flagged_by.setdefault(str(labels[i]), []).append(kind)
        good &= mask
    # A source that was skipped, or ran but judged no feed at all, had
    # nothing to measure; when that is every source the run must not
    # pass as an all-good.
    unavailable = [r["kind"] for r in reports
                   if r["status"] == "skipped"
                   or (r["status"] == "degraded" and not r["n_measured"])]
    if config.sources and len(unavailable) == len(config.sources):
        raise OSError(
            f"all {len(unavailable)} sources had nothing to measure "
            f"({', '.join(unavailable)}) — nothing to measure")
    if skipped:
        degraded.insert(0, "no usable kotekan file — skipped: "
                        + ", ".join(skipped))
    return labels, good, flagged_by, degraded


# -- state / change history ----------------------------------------------


def run(
    config: Config, *, now: float | None = None, force: bool = False, write: bool = True,
    sender=None, report: dict | None = None,
) -> tuple[dict, bool, list]:
    """Evaluate the sources, send if needed, and update the change-history state.

    Returns ``(payload, send, degraded)``; ``degraded`` lists reasons
    the run was incomplete (see :func:`combine_sources`) for the caller
    to turn into exit code 2.  *report*, if given, is the run record
    :func:`combine_sources` fills, plus ``n_bad``, ``update_id`` and
    ``sent`` from here (see :func:`main`).  With ``state.path`` set, the bad-feed set
    (tracked by stable feed *label*) is diffed against the last recorded run: a
    change makes ``send`` true and appends a history entry to the (re)written
    file; an unchanged run sends nothing unless ``force``. Without a state file
    every run sends. ``write=False`` (dry run) computes the diff but writes
    nothing.

    The element axis (``labels``, one per element in kotekan's order) is
    recorded alongside — it is what the payload's indices address, and
    what the web page's element grid shows.  A run whose axis differs from
    the recorded one counts as a change even when the bad labels do not:
    the same names on a reordered or resized axis are different indices.

    When ``sender`` (a callable taking the payload) is given, it is invoked
    *before* the state is written — a failed send leaves the state file
    untouched, so the next run sees the change again and retries.
    """
    now = time.time() if now is None else now
    report = {} if report is None else report
    report["sent"] = False
    labels, good, flagged_by, degraded = combine_sources(config, report)
    bad_idx = np.nonzero(~good)[0]
    payload = {
        "update_id": f"bffs-{int(now * 1000)}",
        "start_time": now + config.sync_delay,
        "bad_inputs": [int(i) for i in bad_idx],
    }
    report["n_bad"] = len(payload["bad_inputs"])
    report["update_id"] = payload["update_id"]
    if not config.state_path:
        if sender is not None:
            sender(payload)
            report["sent"] = True
        return payload, True, degraded

    # Load prior state; a missing or corrupt file is treated as a first run.
    state_file = Path(config.state_path)
    state = None
    if state_file.exists():
        try:
            state = json.loads(state_file.read_text() or "{}")
        except json.JSONDecodeError:
            log.warning("state file %s is corrupt; starting fresh", state_file)

    axis = [str(label) for label in labels]
    bad_labels = sorted(axis[i] for i in bad_idx)
    prev = None if state is None else list(state.get("bad_inputs", []))
    prev_axis = None if state is None else state.get("labels")
    if prev is None:  # first run: record the baseline
        became_bad, became_good, changed = bad_labels, [], True
    else:
        became_bad = sorted(set(bad_labels) - set(prev))
        became_good = sorted(set(prev) - set(bad_labels))
        # A state file from before the axis was recorded has no
        # prev_axis and cannot tell; it gains one below.
        axis_changed = prev_axis is not None and list(prev_axis) != axis
        changed = bool(became_bad or became_good) or axis_changed

    send = changed or force
    if send and sender is not None:
        sender(payload)  # deliver first; a raised error leaves the state unwritten
        report["sent"] = True

    if changed and write:
        state = state or {}
        state["updated"] = now
        state["update_id"] = payload["update_id"]
        state["bad_inputs"] = bad_labels
        # the element axis the indices address, one label per element in
        # kotekan's order — what the web page's element grid is drawn from
        state["labels"] = axis
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
        write_json_atomic(state_file, state)
    elif write and state is not None and prev_axis is None:
        # A state file from before the axis was recorded: add it now so
        # the element grid can render, without inventing a transition.
        state["labels"] = axis
        write_json_atomic(state_file, state)

    return payload, send, degraded


# -- send & CLI -----------------------------------------------------------


def send_to_choco(config: Config, payload: dict) -> None:
    """POST the flag values to choco's group-update API.

    choco accepts ``{"action": "updatable_config", "endpoint": ..., "values":
    ...}`` at ``POST /update/<group>`` and relays the values to every kotekan
    node in the group.  Loopback, unverified TLS: ``choco.jobclient``.
    """
    post_json(config.url, f"/update/{config.group}", {
        "action": "updatable_config",
        "endpoint": config.endpoint,
        "values": payload,
    }, timeout=10.0)


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
    now = time.time()
    report: dict = {"time": now, "dry_run": bool(args.dry_run)}
    rc, error = 0, None
    try:
        payload, send, degraded = run(
            config, now=now, force=args.force, write=not args.dry_run,
            sender=sender, report=report)
    except OSError as e:
        # Environmental: no usable kotekan file with nothing else to
        # measure, unreadable HDF5, choco/nodes unreachable (urllib
        # errors are OSError). One useful line; -vv adds the traceback.
        log.error("%s: %s", type(e).__name__, e)
        log.debug("traceback:", exc_info=True)
        rc, error = 2, f"{type(e).__name__}: {e}"
    except (ValueError, yaml.YAMLError) as e:
        # Config or consistency errors (unknown source kind, element
        # count mismatch, a bad override file) — needs a human.
        log.error("%s: %s", type(e).__name__, e)
        log.debug("traceback:", exc_info=True)
        rc, error = 1, f"{type(e).__name__}: {e}"
    else:
        if args.dry_run or not config.url:
            print(json.dumps(payload))
            log.info("not sent (%s)", "dry run" if args.dry_run else "no choco url")
        elif send:
            log.info("sent %s (%d bad)", payload["update_id"], len(payload["bad_inputs"]))
        else:
            log.info("unchanged; nothing sent")
        if degraded:
            log.warning("degraded run: %s", "; ".join(degraded))
            rc = 2
    _write_run_report(config, report, rc, error, write=not args.dry_run)
    return rc


_RUN_STATUS = {0: "ok", 2: "degraded", 1: "failed"}


def _write_run_report(config: Config, report: dict, rc: int,
                      error: str | None, *, write: bool) -> None:
    """Record how this run went (``state.run_path``), whatever the outcome.

    Unlike the state file, which changes only when the bad list does,
    this is rewritten every run so choco's BFFS page can show which
    sources measured, which abstained and why, and what the exit status
    meant.  A dry run writes nothing.  Failing to write it is logged and
    never changes the exit code — it is a window onto the run, not part
    of it.
    """
    report.update(status=_RUN_STATUS.get(rc, "failed"), exit_code=rc, error=error)
    report.setdefault("degraded", [])
    report.setdefault("sources", [])
    if not write or not config.run_path:
        return
    try:
        write_json_atomic(config.run_path, report)
    except OSError as e:
        log.warning("could not write run file %s: %s", config.run_path, e)


if __name__ == "__main__":
    raise SystemExit(main())
