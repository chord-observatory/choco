#!/usr/bin/env python3
"""waterfall — render kotekan N² visibility files as growing waterfall images.

Run once per invocation; a systemd timer sets the cadence and the script
self-gates, exiting immediately when nothing new has landed.  Each run
folds newly-arrived source files into their acquisition's triangle of
PNGs, one image per correlation product, appended in place.

Acquisitions are visited **newest first** and each run is capped
(``max_files_per_run``), so the live acquisition always keeps up while a
backfill of the archive makes steady progress behind it — a backfill is
~24 h of work and must not be allowed to starve the night's data.

kotekan writes into ``.partial/`` and *renames* the finished file into
the acquisition directory, so anything matching ``*.h5`` there is
complete and immutable and the file list only ever grows.  That is what
makes this append-only: nothing is ever recomputed.

    python waterfall.py --config waterfall.example.yaml -n

Exit codes: 0 = success or nothing to do; 2 = degraded (a mount or file
was unavailable — retries self-heal); 1 = config error or bug.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import glob
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
import yaml

import reduce as R
import wfpng
from store import AcquisitionStore

log = logging.getLogger("waterfall")

DEFAULTS = {
    "waterfalls_dir": "/mnt/cs00/data/kotekan_vis_files/waterfalls",
    "state_file": "/var/lib/choco/waterfall/state.json",
    "level": wfpng.DEFAULT_LEVEL,
    # ~5 min of work: enough to stay ahead of the ~3.2 min file cadence
    # with room left over for backfill, while still returning promptly.
    "max_files_per_run": 40,
    "lock_file": None,          # defaults to waterfall.lock beside the state file
    "roots": [],
}


#: kotekan names a completed file ``vis_<abs_file_idx>_<stamp>.h5``.  The
#: index in the name is what lets an already-folded file be skipped
#: without opening it — the archive is ~16,000 files and the timer runs
#: every two minutes, so opening them all would mean a permanent load on
#: the same NFS server kotekan is writing to, worst once there is nothing
#: left to do.
_NAME_INDEX_RE = re.compile(r"^vis_0*(\d+)_")


class Degraded(Exception):
    """A dependency was unavailable.  Retries self-heal; exit 2."""


def index_from_name(basename: str) -> int | None:
    """The source index in a filename, or None if it is not named that way."""
    m = _NAME_INDEX_RE.match(basename)
    return int(m.group(1)) if m else None


@contextlib.contextmanager
def single_run(path):
    """Hold an exclusive lock for the duration of a run, or yield False.

    systemd will not start a second ``choco-waterfall.service`` while one
    is running, but nothing stops a manual run racing the timer — and two
    processes appending to the same image would interleave their deflate
    segments and corrupt it.  The crash-recovery discipline replays a
    *sequential* history; it cannot repair an interleaved one.
    """
    lock = Path(path)
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock, "w")
    except OSError as exc:
        logger_warn = logging.getLogger("waterfall")
        logger_warn.warning("could not open lock file %s: %s", lock, exc)
        yield True                      # a missing lock must not stop the job
        return
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        yield True
    finally:
        fh.close()


def load_config(path) -> dict:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("config must be a mapping")
    cfg = dict(DEFAULTS)
    cfg.update(raw)
    roots = cfg.get("roots") or []
    if not roots:
        raise ValueError("no roots configured; nothing to render")
    norm = []
    for r in roots:
        if isinstance(r, str):
            r = {"path": r, "name": Path(r).name}
        if not r.get("path"):
            raise ValueError(f"root {r!r} has no path")
        norm.append({"path": str(r["path"]),
                     "name": str(r.get("name") or Path(r["path"]).name)})
    cfg["roots"] = norm
    cfg["level"] = int(cfg["level"])
    cfg["max_files_per_run"] = int(cfg["max_files_per_run"])
    return cfg


def list_acquisitions(root: str) -> list[str]:
    """Acquisition directories, newest first."""
    try:
        names = sorted((d for d in os.listdir(root)
                        if d.startswith("acq_")
                        and os.path.isdir(os.path.join(root, d))),
                       reverse=True)
    except OSError as e:
        raise Degraded(f"{root}: {e}") from e
    return names


def source_files(acq_dir: str) -> list[str]:
    """Completed files only — ``.partial`` is kotekan's staging area.

    A plain ``*.h5`` glob already excludes it (it is a subdirectory, and
    a dotted one), which is the same rule ``datafiles.py`` applies to the
    file counts on choco's /files page.
    """
    return sorted(glob.glob(os.path.join(acq_dir, "*.h5")))


def process_acquisition(root: dict, acq: str, cfg: dict, budget: int,
                        report: dict) -> int:
    """Fold up to ``budget`` new files into one acquisition.  Returns how many."""
    acq_dir = os.path.join(root["path"], acq)
    out_dir = Path(cfg["waterfalls_dir"]) / root["name"] / acq
    store = AcquisitionStore(out_dir, acq, root["name"], source_path=root["path"])
    if store.broken:
        report["errors"].append(f"{acq}: unreadable index; skipped")
        return 0

    files = source_files(acq_dir)
    if not files:
        return 0

    done, skipped = store.processed, store.skipped

    dropped: dict = {}

    def drop(basename: str, why: str) -> None:
        """Record a file that can never be folded in.

        Permanent trouble is remembered rather than re-reported: a file
        with no index, or one whose shape cannot join this acquisition,
        would otherwise reappear in the backlog and in the exit code on
        every run, and exit 2 is supposed to mean "retries self-heal".

        Collected rather than written straight through, because on a
        first run the scan that finds these happens before the store
        exists to record them in.
        """
        report["errors"].append(f"{basename}: {why}")
        report["skipped"] += 1
        dropped[basename] = why
        if store.started:
            store.skip(dropped)
            dropped.clear()

    # Decide what is pending from the *filenames*, and open only the files
    # this run will actually render.  Opening every pending file just to
    # size the backlog cost 454 s against a 10,864-file archive, which no
    # 2-minute timer can absorb.
    candidates = []
    for path in files:
        basename = os.path.basename(path)
        if basename in skipped:
            continue
        named = index_from_name(basename)
        if named is not None and named in done:
            continue
        candidates.append((named, path, basename))
    # index order where the name gives one; anything unnamed goes last,
    # since its index is only knowable by opening it
    candidates.sort(key=lambda c: (c[0] is None, c[0] or 0, c[2]))
    report["backlog"] += len(candidates)
    if not candidates or budget <= 0:
        # Out of budget for this run, but the backlog above is still
        # counted -- an exhausted run must report what it left behind.
        return 0

    pending = []
    for named, path, basename in candidates:
        if len(pending) >= budget:
            break
        try:
            axes = R.read_axes(path)
        except OSError as exc:                        # transient: mount, races
            report["errors"].append(f"{basename}: {exc}")
            report["degraded"] = True
            continue
        except (ValueError, KeyError) as exc:
            drop(basename, f"unexpected layout ({exc})")
            continue
        if axes.file_idx < 0:
            drop(basename, "no abs_file_idx")
            continue
        if axes.n_time <= 0 or axes.n_freq <= 0 or axes.n_prod <= 0:
            drop(basename, f"empty frame ({axes.n_freq}x{axes.n_prod}x{axes.n_time})")
            continue
        if axes.file_idx in done:
            # the filename did not say so, but the index did
            report["backlog"] -= 1
            continue
        pending.append((axes.file_idx, path, axes))
    pending.sort(key=lambda t: t[0])
    if not pending:
        return 0

    if not store.started:
        _, first_path, first_axes = pending[0]
        log.info("%s: new acquisition, %d products, %d x %d — setting scale",
                 acq, first_axes.n_prod, first_axes.n_freq, first_axes.n_time)
        try:
            sample = R.sample_magnitudes(first_path)
        except OSError as exc:
            report["errors"].append(f"{first_path}: {exc}")
            report["degraded"] = True
            return 0
        lo, hi = R.scale_from_sample(sample)
        n_scaled = int(np.isfinite(lo).sum())
        if n_scaled == 0:
            drop(os.path.basename(first_path), "no product has usable data")
            return 0
        if n_scaled < first_axes.n_prod:
            log.warning("%s: %d of %d products have no usable data",
                        acq, first_axes.n_prod - n_scaled, first_axes.n_prod)
        store.start(first_axes, lo, hi)
        store.skip(dropped)                           # collected before it existed
        dropped.clear()

    lo, hi = store.scale()
    buf = None
    written = 0
    for idx, path, axes in pending:
        basename = os.path.basename(path)
        why = store.matches(axes)
        if why is not None:
            drop(basename, why)
            continue
        if buf is None or buf.shape != (axes.n_prod, axes.n_time, axes.n_freq):
            buf = np.empty((axes.n_prod, axes.n_time, axes.n_freq), np.uint8)
        try:
            R.quantize_file(path, lo, hi, out=buf)
            store.add_file(axes, buf, level=cfg["level"])
        except OSError as exc:
            # Stop this acquisition but keep what was committed: returning
            # here rather than raising is what keeps the run's budget
            # honest, so a flaky mount cannot let one run render several
            # times the cap and starve the live acquisition.
            report["errors"].append(f"{path}: {exc}")
            report["degraded"] = True
            break
        written += 1
        report["last_acquisition"] = acq
        report["last_file_idx"] = int(idx)
        log.info("%s: folded %s (%d rows total)", acq, basename, store.n_rows)

    if written:
        # The display tracks the whole acquisition even though the pixels
        # were written minutes or days apart -- neither call reads a
        # visibility or rewrites a pixel.
        store.refresh_palettes()
        store.update_thumbnails(level=cfg["level"])
        store.refresh_thumbnail_palettes()
        report["backlog"] -= written
    return written


def repalette(cfg: dict, only_acq: str | None = None) -> dict:
    """Rewrite every rendered image's palette from its stored histogram.

    A maintenance pass, not part of the timer's cycle: the normal run
    only refreshes acquisitions it appends to, so a colormap change would
    otherwise never reach an acquisition that is already finished.
    Constant time per image — no pixel is read or written — and listed
    from the *image tree*, so it works even when a source mount is gone.
    """
    t0 = time.perf_counter()
    report = {"acquisitions_touched": 0, "images": 0, "errors": [],
              "degraded": False}
    for root in cfg["roots"]:
        tree = Path(cfg["waterfalls_dir"]) / root["name"]
        try:
            acqs = sorted((d for d in os.listdir(tree)
                           if (tree / d).is_dir()), reverse=True)
        except OSError as e:
            report["errors"].append(f"{tree}: {e}")
            report["degraded"] = True
            continue
        for acq in acqs:
            if only_acq and acq != only_acq:
                continue
            store = AcquisitionStore(tree / acq, acq, root["name"])
            if not store.started:
                continue
            n = store.refresh_palettes() + store.refresh_thumbnail_palettes()
            if n:
                report["acquisitions_touched"] += 1
                report["images"] += n
                log.info("%s/%s: %d palettes refreshed", root["name"], acq, n)
    report["run_seconds"] = round(time.perf_counter() - t0, 2)
    return report


def run(cfg: dict, only_acq: str | None = None) -> dict:
    t0 = time.perf_counter()
    report = {"files_rendered": 0, "acquisitions_touched": 0, "backlog": 0,
              "skipped": 0, "errors": [], "last_acquisition": None,
              "last_file_idx": None, "degraded": False}
    budget = cfg["max_files_per_run"] or 1 << 30

    for root in cfg["roots"]:
        try:
            acqs = list_acquisitions(root["path"])
        except Degraded as e:
            report["errors"].append(str(e))
            report["degraded"] = True
            continue
        for acq in acqs:
            if only_acq and acq != only_acq:
                continue
            n = process_acquisition(root, acq, cfg, budget, report)
            if n:
                report["acquisitions_touched"] += 1
                report["files_rendered"] += n
                budget -= n

    report["run_seconds"] = round(time.perf_counter() - t0, 2)
    return report


def write_state(path, cfg: dict, report: dict) -> None:
    p = Path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "updated": time.time(),
            "waterfalls_dir": cfg["waterfalls_dir"],
            "roots": [r["name"] for r in cfg["roots"]],
            **{k: v for k, v in report.items() if k != "degraded"},
        }
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=1))
        os.replace(tmp, p)
    except OSError as e:
        log.warning("could not write state file %s: %s", path, e)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="waterfall", description="render visibility waterfall images")
    ap.add_argument("-c", "--config", required=True, help="path to YAML config")
    ap.add_argument("--acq", default=None, help="render only this acquisition")
    ap.add_argument("--max-files", type=int, default=None,
                    help="override max_files_per_run (0 = unlimited)")
    ap.add_argument("--level", type=int, default=None,
                    help="deflate level (default 6; 4 is faster for a backfill)")
    ap.add_argument("-n", "--dry-run", action="store_true",
                    help="report what would be rendered, write nothing")
    ap.add_argument("--repalette", action="store_true",
                    help="refresh every rendered image's palette and exit "
                         "(after a colormap change; touches no pixels)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING - 10 * min(args.verbose, 2),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log.setLevel(min(log.getEffectiveLevel(), logging.INFO))

    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        return 1
    if args.max_files is not None:
        cfg["max_files_per_run"] = args.max_files
    if args.level is not None:
        cfg["level"] = args.level

    if args.dry_run:
        total = 0
        for root in cfg["roots"]:
            try:
                acqs = list_acquisitions(root["path"])
            except Degraded as e:
                log.error("%s", e)
                return 2
            for acq in acqs:
                store = AcquisitionStore(
                    Path(cfg["waterfalls_dir"]) / root["name"] / acq, acq, root["name"])
                n = len(source_files(os.path.join(root["path"], acq)))
                pend = n - len(store.processed)
                if pend > 0:
                    print(f"{root['name']}/{acq}: {pend} of {n} files pending")
                    total += pend
        print(f"{total} files pending")
        return 0

    lock_path = cfg.get("lock_file") or (Path(cfg["state_file"]).parent / "waterfall.lock")
    with single_run(lock_path) as acquired:
        if not acquired:
            log.info("another run holds %s; nothing to do", lock_path)
            return 0
        if args.repalette:
            # a maintenance pass: no state file, that is the timer's record
            report = repalette(cfg, only_acq=args.acq)
            for e in report["errors"][:20]:
                log.warning("%s", e)
            log.info("refreshed %d palettes across %d acquisition(s) in %.1fs",
                     report["images"], report["acquisitions_touched"],
                     report["run_seconds"])
            return 2 if report["degraded"] else 0
        report = run(cfg, only_acq=args.acq)
        write_state(cfg["state_file"], cfg, report)

    for e in report["errors"][:20]:
        log.warning("%s", e)
    log.info("rendered %d file(s) across %d acquisition(s) in %.1fs; "
             "backlog %d, skipped %d",
             report["files_rendered"], report["acquisitions_touched"],
             report["run_seconds"], report["backlog"], report["skipped"])

    # Only an unavailable dependency is "degraded" -- a file that can
    # never be rendered is recorded once and does not colour the badge
    # yellow forever, since retrying it would not self-heal.
    return 2 if report["degraded"] else 0


if __name__ == "__main__":
    sys.exit(main())
