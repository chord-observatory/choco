"""acqnotes job: measure new N² subset acquisitions and write their READMEs.

Each run scans the subset root for acquisition directories, measures the
ones that are new or have grown (newest first, at most
``max_acqs_per_run``), then renders every measured acquisition's
``README.md`` and ``acq_info.yaml`` and the root's ``timeline.yaml``.
A note is rewritten only when its text changed (the generation date in a
README's header does not count), so a run with nothing new writes nothing.

The measurements live in the job's state directory, one JSON per
acquisition and step (``meta/``, ``transit/``, ``pol/``, ``zerovis/``,
``verify/``), with ``index.json`` recording which files each was taken
from.  The acquisition being written is re-measured at most every
``remeasure_hours``; ``verify`` reads only the files it has not seen.

``--remeasure`` / ``--remeasure-from`` queue a re-measure in the state
directory (``remeasure.d/``), which the next run takes under its lock: a
request made while another run holds the lock waits for the next run
instead of being lost.  ``--queue`` only queues, for a playbook that then
starts the unit.
"""
import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import yaml

from choco.jobclient import (EXIT_DEGRADED, EXIT_FAILED, EXIT_OK, failure, job_state_dir,
                             setup_logging, single_run, write_json_atomic,
                             write_run, write_text_atomic)

import extract
import polswap
import render
import transit
import verify
import zerovis

log = logging.getLogger("acqnotes")

JOB = "acqnotes"
DEFAULTS = {
    "root": "/mnt/cs00/data/kotekan_vis_files/subset",
    "max_acqs_per_run": 3,
    "remeasure_hours": 3.0,
}
INDEX_FILE = "index.json"
LOCK_FILE = "run.lock"
REQUEST_DIR = "remeasure.d"
# A timer tick lands up to a minute or so either side of the hour, so the
# re-measure interval is compared with this much slack: otherwise a
# 3 h interval would wait for the fourth hourly run.
TICK_SLACK_S = 300
ACQ_RE = re.compile(r"^acq_\d{8}_\d{6}_\d+$")
#: --remeasure-from takes an acquisition name or a prefix of one (a day, a minute).
ACQ_PREFIX_RE = re.compile(r"^acq_\d{8}(_\d{6}(_\d+)?)?$")
H5_RE = re.compile(r"^vis_\d+_\d{8}T_\d{6}_\d+\.h5$")

# step -> (subdirectory, measure(acq_dir, previous result) -> JSON-able result)
STEPS = {
    "meta": lambda d, prev: extract.analyse(d),
    "transit": lambda d, prev: transit.analyse(d),
    "pol": lambda d, prev: polswap.analyse(d),
    "zerovis": lambda d, prev: zerovis.analyse(d),
    "verify": lambda d, prev: verify.analyse(d, prev),
}


def load_config(path) -> dict:
    raw = {}
    if path and os.path.exists(path):
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("config must be a mapping")
    unknown = sorted(set(raw) - set(DEFAULTS))
    if unknown:
        raise ValueError(f"unknown key(s): {', '.join(unknown)}")
    cfg = dict(DEFAULTS)
    cfg.update(raw)
    cfg["root"] = str(cfg["root"])
    cfg["max_acqs_per_run"] = int(cfg["max_acqs_per_run"])
    cfg["remeasure_hours"] = float(cfg["remeasure_hours"])
    if cfg["max_acqs_per_run"] < 1:
        raise ValueError("max_acqs_per_run must be at least 1")
    return cfg


def scan(root) -> dict:
    """``{acq: {"n_files", "newest"}}`` for every acquisition directory.

    Raises ``OSError`` when the root cannot be listed (mount down)."""
    out = {}
    with os.scandir(root) as it:
        dirs = [e.name for e in it if e.is_dir() and ACQ_RE.match(e.name)]
    for name in dirs:
        files = sorted(f for f in os.listdir(os.path.join(root, name)) if H5_RE.match(f))
        out[name] = {"n_files": len(files), "newest": files[-1] if files else None}
    return out


def due(acqs: dict, index: dict, now: float, remeasure_s: float) -> list[str]:
    """Acquisitions to measure this run, newest first.

    New ones, and ones whose files changed since they were measured: at
    once for a finished acquisition, after *remeasure_s* for the newest
    (still being written).  An empty directory waits for its first file.
    """
    newest = max(acqs) if acqs else None
    out = []
    for name in sorted(acqs, reverse=True):
        src, rec = acqs[name], index.get(name)
        if not src["n_files"]:
            continue
        if rec is None:
            out.append(name)
        elif (rec["n_files"], rec["newest"]) != (src["n_files"], src["newest"]):
            if name != newest or now - rec["measured"] >= remeasure_s - TICK_SLACK_S:
                out.append(name)
    return out


def measure(name: str, root: str, work: Path) -> None:
    """Run every step for one acquisition and store the results."""
    d = os.path.join(root, name)
    for step, fn in STEPS.items():
        path = work / step / f"{name}.json"
        prev = None
        if path.exists():
            prev = json.loads(path.read_text())
        t0 = time.monotonic()
        write_json_atomic(path, fn(d, prev))
        log.info("%s %s: %.0f s", name, step, time.monotonic() - t0)


def _without_date(text: str) -> str:
    return render.GEN_DATE_RE.sub("", text)


def write_if_changed(path: Path, text: str, readme: bool = False) -> bool:
    try:
        old = path.read_text()
    except FileNotFoundError:
        old = None
    if old is not None and (old == text or (readme and _without_date(old) == _without_date(text))):
        return False
    write_text_atomic(path, text)
    return True


def run(cfg: dict, work: Path, *, max_acqs: int, out=None) -> dict:
    """Measure what is due, then write every note that changed into the
    acquisition directories (or the same layout under *out*)."""
    root = cfg["root"]
    acqs = scan(root)
    index_path = work / INDEX_FILE
    requested = take_requests(work, acqs)
    index = json.loads(index_path.read_text()) if index_path.exists() else {}
    # "measured" is when this run started, so the next interval counts
    # from the same tick, not from however long the measurement took
    started = time.time()
    todo = due(acqs, index, started, 3600 * cfg["remeasure_hours"])
    report = {"acquisitions": len(acqs), "measured": [], "backlog": max(0, len(todo) - max_acqs),
              "requested": requested, "written": [], "degraded": [], "errors": []}
    for name in todo[:max_acqs]:
        # One acquisition's failure does not hold up the others: an
        # unreadable mount or file is degraded (retried next run), anything
        # else is a bug in a measurement and turns the badge red.
        try:
            measure(name, root, work)
        except OSError as e:
            report["degraded"].append(f"{name}: {e}")
            log.warning("%s: %s", name, e)
            continue
        except Exception as e:  # noqa: BLE001
            report["errors"].append(f"{name}: {type(e).__name__}: {e}")
            log.error("%s: %s: %s", name, type(e).__name__, e)
            log.debug("traceback:", exc_info=e)
            continue
        index[name] = {**acqs[name], "measured": started}
        write_json_atomic(index_path, index)
        report["measured"].append(name)

    # Render every measured acquisition still on disk: a timeline edit or
    # a new neighbour (configs are inferred across acquisitions) can
    # change any of them.
    names = sorted(n for n in acqs if n in index)
    # The newest acquisition is the one kotekan may still be writing; its
    # README says so until a newer one starts.
    notes = render.render_all(names, root, str(work), newest=max(acqs) if acqs else None)
    out_root = Path(out or root)
    for name, readme, info in notes:
        d = out_root / name
        if write_if_changed(d / "README.md", readme, readme=True):
            report["written"].append(name)
        write_if_changed(d / "acq_info.yaml", info)
    if write_if_changed(out_root / "timeline.yaml", render.timeline_text()):
        report["written"].append("timeline.yaml")
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog=JOB, description=__doc__.splitlines()[0])
    ap.add_argument("-c", "--config", default=None,
                    help="acqnotes.yaml (defaults apply if absent)")
    ap.add_argument("--state-dir", default=None,
                    help="where the measurements, index.json and run.json live (default: "
                         "systemd's $STATE_DIRECTORY, else /var/lib/choco/acqnotes)")
    ap.add_argument("--max-acqs", type=int, default=None,
                    help="override max_acqs_per_run for this run")
    ap.add_argument("--remeasure", nargs="+", metavar="ACQ", default=(),
                    help="re-measure these acquisitions (e.g. after a script change)")
    ap.add_argument("--remeasure-from", metavar="ACQ", default=None,
                    help="re-measure every acquisition from this one on (a name, or a prefix "
                         "such as acq_20261017)")
    ap.add_argument("--queue", action="store_true",
                    help="only queue the re-measure for the next run; do not run")
    ap.add_argument("--out", default=None, metavar="DIR",
                    help="write the notes under DIR (same layout as the root) instead of into "
                         "the acquisition directories, e.g. to review them")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)
    setup_logging(args.verbose, log)
    state_dir = job_state_dir(JOB, args.state_dir)
    if args.queue and not (args.remeasure or args.remeasure_from):
        ap.error("--queue needs --remeasure or --remeasure-from")
    if args.remeasure or args.remeasure_from:
        try:
            path = request_remeasure(state_dir, args.remeasure, args.remeasure_from)
        except ValueError as e:
            ap.error(str(e))
        log.info("queued %s", path.name)
        if args.queue:
            return EXIT_OK

    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        write_run(state_dir, 1, reason=f"bad config: {e}")
        return 1

    with single_run(state_dir / LOCK_FILE) as acquired:
        if not acquired:
            log.info("another run holds the lock; nothing to do")
            return EXIT_OK
        try:
            report = run(cfg, state_dir, max_acqs=args.max_acqs or cfg["max_acqs_per_run"],
                         out=args.out)
        except (OSError, ValueError, KeyError) as e:
            code, reason = failure(e, log)
            write_run(state_dir, code, reason=reason)
            return code

    code = EXIT_FAILED if report["errors"] else EXIT_DEGRADED if report["degraded"] else EXIT_OK
    log.info("%d acquisition(s); measured %s; backlog %d; wrote %s",
             report["acquisitions"], ", ".join(report["measured"]) or "none",
             report["backlog"], ", ".join(report["written"]) or "nothing")
    # run.json's ``error`` is one line (choco's badge tooltip quotes it)
    errors = report.pop("errors")
    if errors:
        report["error"] = "; ".join(errors[:3]) + (f" (+{len(errors) - 3} more)" if len(errors) > 3 else "")
    write_run(state_dir, code, **report)
    return code


def request_remeasure(state_dir: Path, names=(), since: str | None = None) -> Path:
    """Queue a re-measure of *names* and of every acquisition from *since* on
    for the next run.  One file per request, written whole, so the run that
    takes the queue never sees half of one."""
    for name in names:
        if not ACQ_RE.match(name):
            raise ValueError(f"not an acquisition name: {name!r}")
    if since is not None and not ACQ_PREFIX_RE.match(since):
        raise ValueError(f"not an acquisition name or prefix: {since!r}")
    path = state_dir / REQUEST_DIR / f"{time.time_ns()}-{os.getpid()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, {"names": list(names), "since": since})
    return path


def take_requests(state_dir: Path, acqs) -> list[str]:
    """Forget the measurements the queued requests name and remove the
    requests; the names, for the run record.  A request that arrives while
    this runs is left for the next run."""
    files = sorted((state_dir / REQUEST_DIR).glob("*.json"))
    names: set[str] = set()
    for p in files:
        try:
            req = json.loads(p.read_text())
        except (OSError, ValueError) as e:
            log.warning("dropping unreadable request %s: %s", p.name, e)
            continue
        names.update(n for n in req.get("names") or () if ACQ_RE.match(str(n)))
        since = req.get("since")
        if since:
            names.update(n for n in acqs if n >= since)
    if names:
        forget(state_dir, sorted(names))
    for p in files:
        p.unlink(missing_ok=True)
    return sorted(names)


def forget(state_dir: Path, names) -> None:
    index_path = state_dir / INDEX_FILE
    index = json.loads(index_path.read_text()) if index_path.exists() else {}
    for name in names:
        if not ACQ_RE.match(name):
            raise ValueError(f"not an acquisition name: {name!r}")
        index.pop(name, None)
    write_json_atomic(index_path, index)


if __name__ == "__main__":
    sys.exit(main())
