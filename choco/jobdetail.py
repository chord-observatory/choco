"""What each job's page summarises from the files the job keeps.

One function per job, ``f(state, run, svc)``: the job's state and run
files already read (None when missing) and its registry entry (for the
files beside them -- eigencal's archives, the mapmaker's images).  Each
pre-shapes everything its ``_detail_<name>.html`` touches, so a malformed
file fails here, under :func:`choco.web._service_detail`'s guard, and not
mid-render.  Nothing here reads the request or the app.
"""

import logging
import time
from pathlib import Path

from .jobclient import EIGENCAL_ARCHIVE_GLOB
from .jobstatus import read_state_json
from .manualflags import read_manual_flags

logger = logging.getLogger(__name__)


def file_mtime(path: Path | None) -> int | None:
    """A file's mtime as an integer, None when it is not there."""
    if path is None:
        return None
    try:
        return int(path.stat().st_mtime)
    except OSError:
        return None


def fmt_utc(ts) -> str | None:
    """A unix time as 'YYYY-MM-DD HH:MM:SS UTC'; None for no time."""
    if not ts:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))


#: PNGs the mapmaker job writes, by the name the page requests them under:
#: the fold-in's sky and daily maps, the deconvolution's clean map and
#: that map in context on the 408 MHz sky.
MAPMAKER_IMAGES = {"sky": "sky.png", "daily": "daily.png", "clean": "clean.png",
                   "sky-strip": "sky-strip.png", "daily-strip": "daily-strip.png",
                   "clean-strip": "clean-strip.png", "context": "context.png"}


def _mapmaker_clean_facts(root: Path) -> dict | None:
    """What the deconvolution step (``clean.py``, its own timer) last did,
    from its ``clean.json``: the CLEAN's numbers, the brightest components
    and why it is degraded if it is.  None until it has run."""
    facts = read_state_json(root / "clean.json")
    if not isinstance(facts, dict) or not facts:
        return None
    comps = []
    for c in (facts.get("components") or [])[:8]:
        try:
            comps.append({"ra_deg": float(c["ra_deg"]), "dec_deg": float(c["dec_deg"]),
                          "flux": float(c["flux"])})
        except (KeyError, TypeError, ValueError):
            continue
    beam = facts.get("beam_deg") if isinstance(facts.get("beam_deg"), list) else None

    def num(key):
        v = facts.get(key)
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    return {
        "updated": fmt_utc(facts.get("updated")),
        "data_to": fmt_utc(facts.get("map_data_time")),
        "iterations": int(facts["iterations"]) if isinstance(facts.get("iterations"), int) else None,
        "n_components": int(facts["n_components"]) if isinstance(facts.get("n_components"), int) else None,
        "threshold": num("threshold"),
        "sigma_before": num("sigma_before"),
        "sigma_after": num("sigma_after"),
        "run_seconds": num("run_seconds"),
        "beam_deg": [float(beam[0]), float(beam[1])] if beam and len(beam) == 2 else None,
        "components": comps,
        "degraded": [str(d) for d in (facts.get("degraded") or [])][:5],
    }


def mapmaker_detail(state: dict | None, run, svc) -> dict | None:
    """The MAP page's facts from the job's per-run state file: what was
    folded in, the calibration the maps rest on, and the images' mtimes
    (so the page refetches a PNG exactly when a run replaced it)."""
    if state is None:
        return None
    gains = state.get("gains") if isinstance(state.get("gains"), dict) else None
    coverage = state.get("coverage") if isinstance(state.get("coverage"), dict) else {}
    root = svc["state_file"].parent      # the images sit beside the state file
    images = {key: file_mtime(root / fname) for key, fname in MAPMAKER_IMAGES.items()}
    rng = state.get("colour_range_mhz") or []
    return {
        "updated": fmt_utc(state.get("updated")),
        "clean": _mapmaker_clean_facts(root),
        "pointing_dec_deg": state.get("pointing_dec_deg"),
        "pointing_check": (state["pointing_check"]
                           if isinstance(state.get("pointing_check"), dict) else None),
        "colour_bins": int(state["colour_bins"]) if state.get("colour_bins") is not None else None,
        "colour_range_mhz": (float(rng[0]), float(rng[1])) if len(rng) == 2 else None,
        "stretch": state.get("stretch"),
        "last_file_idx": state.get("last_file_idx"),
        "last_sample": fmt_utc(state.get("last_sample_time")),
        "files_processed": state.get("files_processed"),
        "files_imaged": state.get("files_imaged"),
        "files_uncalibrated": state.get("files_uncalibrated"),
        "backlog": state.get("backlog"),
        "run_seconds": state.get("run_seconds"),
        "n_samples_total": state.get("n_samples_total"),
        "calibrator": state.get("calibrator"),
        "cal_samples_pending": state.get("cal_samples_pending"),
        "gains": {
            "calibrator": gains.get("calibrator"),
            "solved_at": fmt_utc(gains.get("solved_at")),
            "transit": fmt_utc(gains.get("transit_time")),
            "n_samples": gains.get("n_samples"),
            "n_dead_products": gains.get("n_dead_products"),
            "n_products": gains.get("n_products"),
        } if gains else None,
        "coverage": {k: v for k, v in coverage.items() if isinstance(v, dict)},
        "degraded": [str(d) for d in (state.get("degraded") or [])][:10],
        "skipped": [str(e) for e in (state.get("skipped") or [])][:10],
        "skipped_total": len(state.get("skipped") or []),
        "errors": [str(e) for e in (state.get("errors") or [])][:10],
        "images": images,
    }



#: Elements per column of the bffs page's element grid: kotekan's
#: element order runs down each column, so the 64-dish [P][D] axis is
#: eight columns of X then eight of Y.
ELEMENTS_PER_COLUMN = 8


def element_grid(labels: list[str], bad: set[str], sources: dict,
                  per_column: int = ELEMENTS_PER_COLUMN,
                  manual: set[str] | None = None) -> list[list[dict | None]]:
    """The element axis as table rows, *per_column* elements to a column.

    Column *c* holds elements ``c*per_column .. c*per_column+per_column-1``
    top to bottom; row *r* is therefore every element whose index is
    ``r`` modulo *per_column*.  Cells are ``{"label", "index", "bad",
    "sources", "manual"}`` — ``manual`` says the label is in the override
    file now, which can run ahead of ``bad`` (the job applies the file on
    its next run); a ragged last column is padded with ``None``.  Empty
    when there is no axis, so the template can fall back to the list.
    """
    manual = manual or set()
    columns = [labels[i:i + per_column]
               for i in range(0, len(labels), per_column)]
    rows = []
    for r in range(min(per_column, len(labels))):
        rows.append([
            {"label": col[r], "index": c * per_column + r,
             "bad": col[r] in bad, "sources": sources.get(col[r], ""),
             "manual": col[r] in manual}
            if r < len(col) else None
            for c, col in enumerate(columns)])
    return rows


def bffs_manual_state(manual_file, run: dict | None) -> dict:
    """What the page knows about manual flagging.

    ``enabled`` is whether the grid may edit the override file
    (``bffs.control``); ``labels`` is what that file lists now; ``error``
    an unreadable file;
    ``job_path`` the file the job's ``manual`` source reported reading
    (from the run file) and ``mismatch`` whether that differs from ours —
    a click would then write where the job never looks, so the grid says
    so and the route refuses.
    """
    out = {"enabled": manual_file is not None, "path": str(manual_file) if manual_file else None,
           "labels": set(), "error": None, "job_path": None, "mismatch": False}
    if manual_file is None:
        return out
    try:
        out["labels"] = read_manual_flags(manual_file) or set()
    except ValueError as e:
        out["error"] = str(e)
    job_path = job_manual_path(run)
    out["job_path"] = job_path
    if job_path is not None and not same_path(job_path, manual_file):
        out["mismatch"] = True
    return out


def job_manual_path(run: dict | None) -> str | None:
    """The override file the job's ``manual`` source read, per the run
    file; ``""`` when the job ran with no manual source; ``None`` when
    there is no run file to tell."""
    if not isinstance(run, dict) or not isinstance(run.get("sources"), list):
        return None
    for src in run["sources"]:
        if isinstance(src, dict) and src.get("kind") == "manual":
            detail = src.get("detail") if isinstance(src.get("detail"), dict) else {}
            return str(detail.get("path") or "")
    return ""


def same_path(a, b) -> bool:
    try:
        return Path(str(a)).resolve() == Path(str(b)).resolve()
    except OSError:
        return str(a) == str(b)


def float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def eigencal_detail(state: dict | None, run: dict | None, svc: dict) -> dict | None:
    """The EIGENCAL page's summary from the job's two files and the
    archives beside them.

    ``run.json`` is written every run and says what the run did and why
    (skipped: transit not complete / too old / daytime / already done;
    degraded: no data or the quality gate; ok: archived or sent) with the
    transit it considered and the next one -- the question "why has it
    not produced anything?" answered without the journal.  ``state.json``
    changes only when a solution is produced.  The archives are the
    ``gain_*.h5`` files in the state directory, newest first.
    """
    archives = []
    try:
        files = sorted(svc["state_file"].parent.glob(EIGENCAL_ARCHIVE_GLOB),
                       key=lambda f: f.stat().st_mtime, reverse=True)
    except OSError:
        files = []
    for f in files[:10]:
        try:
            st = f.stat()
        except OSError:
            continue
        archives.append({"name": f.name, "bytes": st.st_size,
                         "mtime": st.st_mtime, "mtime_fmt": fmt_utc(st.st_mtime)})
    if state is None and run is None and not archives:
        return None
    out: dict = {"has_state": state is not None, "has_run": run is not None,
                 "archives": archives}
    if state is not None:
        out.update({
            "updated": fmt_utc(state.get("updated")),
            "transit_time": fmt_utc(state.get("transit_time")),
            "source": state.get("source"),
            "good_frac": float_or_none(state.get("good_frac")),
            "n_calibrated": state.get("n_calibrated"),
            "n_elements": state.get("n_elements"),
            "sent": state.get("sent"),
            "archive": (Path(str(state["archive"])).name
                        if state.get("archive") else None),
        })
    if run is not None:
        status = str(run.get("status") or "unknown")
        out["run"] = {
            "status": status,
            "time": float_or_none(run.get("time")),
            "time_fmt": fmt_utc(run.get("time")),
            "reason": run.get("reason"),
            "exit_code": run.get("exit_code"),
            "source": run.get("source"),
            "transit_tag": run.get("transit_tag"),
            "transit_fmt": fmt_utc(run.get("transit")),
            "transit_complete_fmt": fmt_utc(run.get("transit_complete")),
            "eligible_until": float_or_none(run.get("eligible_until")),
            "eligible_until_fmt": fmt_utc(run.get("eligible_until")),
            "next_transit": float_or_none(run.get("next_transit")),
            "next_transit_fmt": fmt_utc(run.get("next_transit")),
            "sun_alt_deg": float_or_none(run.get("sun_alt_deg")),
            "archive_only": run.get("archive_only"),
            "dry_run": run.get("dry_run"),
            "good_frac": float_or_none(run.get("good_frac")),
        }
    return out


def bffs_detail(state: dict | None, run: dict | None, svc: dict) -> dict | None:
    """The BFFS page's summary from the job's two files.

    The state file says what the flags *are* (bad list, element axis,
    transitions; rewritten only when the list changes); the run file
    says how the last run *measured* them (per-source status, what was
    skipped or abstained and why; rewritten every run).  Either may be
    missing; ``None`` only when both are.  The registry entry's manual
    override file adds its current contents to the grid.
    """
    run_summary = _bffs_run_summary(run)
    if state is None and run_summary is None:
        return None
    manual = bffs_manual_state(svc["manual_file"], run)
    has_state = state is not None
    state = state or {}
    history = [h for h in (state.get("history") or [])
               if isinstance(h, dict)]
    flagged_by = state.get("flagged_by") or {}
    flag_reasons = state.get("flag_reasons")
    flag_reasons = flag_reasons if isinstance(flag_reasons, dict) else {}

    def attribution(label, kinds) -> str:
        # which source(s) flagged the feed, each with the reason it gave
        # where it gave one: "power: not in PDB table", "manual"
        reasons = flag_reasons.get(label)
        reasons = reasons if isinstance(reasons, dict) else {}
        return ", ".join(f"{kind}: {reasons[kind]}" if reasons.get(kind) else str(kind)
                         for kind in (kinds or []))

    # absent for state files written before attribution existed
    sources = ({str(label): attribution(str(label), kinds)
                for label, kinds in flagged_by.items()}
               if isinstance(flagged_by, dict) else {})
    bad_inputs = [str(label) for label in (state.get("bad_inputs") or [])]
    # the element axis the flags index, recorded by bffs since
    # 2026-09; an older state file has none and the page shows the
    # bad list alone
    labels = [str(label) for label in (state.get("labels") or [])]
    return {
        "has_state": has_state,
        "updated": fmt_utc(state.get("updated")),
        "update_id": state.get("update_id"),
        "bad_inputs": bad_inputs,
        "flagged_by": sources,
        "n_elements": len(labels),
        "element_rows": element_grid(labels, set(bad_inputs), sources,
                                      manual=manual["labels"]),
        "manual": manual,
        # pre-shape everything the template touches, so a malformed
        # entry fails here (-> detail None) and not mid-render
        "history": [{
            "time_fmt": fmt_utc(h.get("time")),
            "became_bad": list(h.get("became_bad") or []),
            "became_good": list(h.get("became_good") or []),
            "n_bad": len(h.get("bad_inputs") or []),
        } for h in reversed(history[-10:])],
        "history_total": len(history),
        "run": run_summary,
    }


def _opt_int(value):
    return None if value is None else int(value)


def fmt_age(seconds) -> str:
    s = float(seconds)
    if s < 90:
        return f"{s:.0f} s"
    if s < 5400:
        return f"{s / 60:.0f} min"
    if s < 172800:
        return f"{s / 3600:.1f} h"
    return f"{s / 86400:.1f} d"


def _bffs_run_summary(run) -> dict | None:
    """The run file pre-shaped for the template, or ``None``.

    Parsed under its own guard so a malformed run file costs only the
    "last run" block, not the state summary beside it.
    """
    if not isinstance(run, dict):
        return None
    try:
        sources = []
        for src in run.get("sources") or []:
            if not isinstance(src, dict):
                continue
            detail = src.get("detail") if isinstance(src.get("detail"), dict) else {}
            kind = str(src.get("kind") or "?")
            sources.append({
                "kind": kind,
                "status": str(src.get("status") or "unknown"),
                "reason": str(src["reason"]) if src.get("reason") else None,
                "n_flagged": _opt_int(src.get("n_flagged")),
                "n_measured": _opt_int(src.get("n_measured")),
                "summary": bffs_source_summary(kind, detail),
            })
        age = run.get("kotekan_file_age_s")
        return {
            "time": float(run["time"]) if run.get("time") is not None else None,
            "time_fmt": fmt_utc(run.get("time")),
            "status": str(run.get("status") or "unknown"),
            "exit_code": _opt_int(run.get("exit_code")),
            "degraded": [str(r) for r in (run.get("degraded") or [])],
            "error": str(run["error"]) if run.get("error") else None,
            "kotekan_file": (str(run["kotekan_file"])
                             if run.get("kotekan_file") else None),
            "kotekan_file_age": fmt_age(age) if age is not None else None,
            "kotekan_file_reason": (str(run["kotekan_file_reason"])
                                    if run.get("kotekan_file_reason") else None),
            "n_elements": _opt_int(run.get("n_elements")),
            "n_bad": _opt_int(run.get("n_bad")),
            "sent": run.get("sent"),
            "sources": sources,
        }
    except (TypeError, ValueError, KeyError, AttributeError) as e:
        logger.warning(f"bffs run file: unusable contents: {e}")
        return None


def bffs_source_summary(kind: str, d: dict) -> str:
    """One line from a source's free-form report detail, by source kind.

    Every key is optional (older run files, future sources); anything
    unrecognised falls back to ``key=value`` pairs.
    """
    parts: list[str] = []
    if kind == "power-outlier":
        if d.get("band_coverage") is not None:
            parts.append(f"band coverage {float(d['band_coverage']):.0%}")
        if d.get("rows") is not None:
            rows = f"{int(d['rows'])} rows"
            if d.get("tail_rows_skipped"):
                rows += f" ({int(d['tail_rows_skipped'])} empty tail rows skipped)"
            parts.append(rows)
        if d.get("median_power") is not None:
            parts.append(f"median power {float(d['median_power']):.4g}")
        if d.get("n_in_file") is not None and d.get("n_not_in_file"):
            n_in, n_out = int(d["n_in_file"]), int(d["n_not_in_file"])
            parts.append(f"{n_in} of {n_in + n_out} elements in the file")
        if d.get("n_excluded"):
            types = d.get("exclude_types")
            named = ", ".join(str(t) for t in types) if isinstance(types, list) else "type"
            parts.append(f"{int(d['n_excluded'])} {named} elements not compared")
        if d.get("file"):
            parts.append(Path(str(d["file"])).name)
    elif kind == "spectral-shape":
        pols = d.get("pols")
        if isinstance(pols, dict):
            for pol, s in pols.items():
                if isinstance(s, dict) and s.get("limit") is not None:
                    parts.append(f"{pol}: {int(s.get('n') or 0)} feeds, "
                                 f"limit ±{float(s['limit']):.3f} dex/octave")
        if d.get("n_held"):
            parts.append(f"{int(d['n_held'])} held from earlier files")
        if d.get("file"):
            parts.append(Path(str(d["file"])).name)
    elif kind == "dish-type":
        counts = d.get("type_counts")
        if isinstance(counts, dict) and counts:
            parts.append(" / ".join(f"{int(n)} {t}" for t, n in counts.items()))
        bad = d.get("bad_types")
        if isinstance(bad, list) and bad:
            parts.append("bad: " + ", ".join(str(t) for t in bad))
        if d.get("n_untyped"):
            parts.append(f"{int(d['n_untyped'])} untyped (left good)")
    elif kind == "rfi":
        n = d.get("n_endpoints")
        if n is not None:
            used = int(n) - int(d.get("n_failed") or 0) - int(d.get("n_stale") or 0)
            parts.append(f"{used} of {int(n)} /sk endpoints used")
        if d.get("n_failed"):
            parts.append(f"{int(d['n_failed'])} unreachable")
        if d.get("n_stale"):
            parts.append(f"{int(d['n_stale'])} stale")
        skipped = [n for n in (d.get("skipped_nodes") or []) if isinstance(n, dict)]
        if skipped:
            parts.append("not polled: " + ", ".join(
                f"{n.get('node')} ({n.get('status')})" for n in skipped))
        bounds = d.get("sk_bounds")
        if isinstance(bounds, list) and len(bounds) == 2:
            parts.append(f"SK bounds {bounds[0]}–{bounds[1]}")
    elif kind == "power":
        if d.get("n_watched") is not None:
            parts.append(f"{int(d['n_watched'])} feeds watched")
        if d.get("n_unpowered") is not None:
            parts.append(f"{int(d['n_unpowered'])} unpowered")
        if d.get("n_unmapped"):
            parts.append(f"{int(d['n_unmapped'])} not in the PDB table"
                         + (" (flagged absent)" if d.get("unmapped_flagged")
                            else " (left good)"))
        if d.get("map_source"):
            parts.append(f"map: {d['map_source']}")
        if d.get("map_check") and d["map_check"] != "ok":
            parts.append(str(d["map_check"]))
    elif kind == "manual":
        if d.get("n_listed") is not None:
            parts.append(f"{int(d['n_listed'])} listed")
        if d.get("exists") is False:
            parts.append("override file absent")
        if d.get("not_on_axis"):
            parts.append("not on the axis: "
                         + ", ".join(str(x) for x in d["not_on_axis"]))
    elif kind == "fpga":
        if d.get("n_watched") is not None:
            parts.append(f"{int(d['n_watched'])} feeds watched")
        if d.get("channels_sampled") is not None:
            parts.append(f"{int(d['channels_sampled'])} channels sampled")
    if not parts:
        parts = [f"{k}={v}" for k, v in d.items()
                 if isinstance(v, (str, int, float, bool))][:6]
    return " · ".join(parts)


def eop_detail(state: dict | None, run, svc) -> dict | None:
    """The span of the EOP table the job last broadcast."""
    if state is None:
        return None
    table = state.get("earth_orientation_parameter_table") or []
    stamps = sorted(e["t_inst_ns"] for e in table
                    if isinstance(e, dict) and "t_inst_ns" in e)
    if not stamps:
        return None
    # t_inst_ns is instrument time (frame0-anchored TAI ns), within
    # leap-seconds of unix time — fine for a span display.
    return {
        "entries": len(table),
        "first": fmt_utc(stamps[0] / 1e9),
        "last": fmt_utc(stamps[-1] / 1e9),
    }


POINTING_ROWS = "ABCDEFGH"
POINTING_COLS = range(1, 9)


def pointing_detail(state: dict | None, run: dict | None, svc: dict) -> dict | None:
    """The POINTING page's summary: the latest table as the dishes stand
    (``state.json``, rewritten when the table changes) and what the last
    run did (``run.json``, every run).

    The grid is rows A-H of eight dishes, each cell the dish's angle and
    whether it is the array value (``same``), differs from it (``other``),
    or is not in the table (``None``).  The array value is the one the job
    applied, or None when no value had enough dishes.
    """
    if state is None and run is None:
        return None
    out: dict = {"has_state": state is not None, "has_run": run is not None}
    if run is not None:
        out["run"] = {
            "time": fmt_utc(run.get("time")),
            "status": run.get("status"),
            "reason": run.get("reason"),
            "changed": run.get("changed"),
            "previous": float_or_none(run.get("previous")),
            "paused": run.get("paused") or [],
        }
    if state is not None:
        table = state.get("table") or {}
        array_value = float_or_none(state.get("array_value"))
        rows = []
        for r in POINTING_ROWS:
            cells = []
            for c in POINTING_COLS:
                name = f"{r}{c}"
                value = float_or_none(table.get(name))
                cells.append({
                    "name": name, "value": value,
                    "kind": (None if value is None
                             else "same" if value == array_value else "other"),
                })
            rows.append({"name": r, "cells": cells})
        out.update({
            "time": fmt_utc(state.get("time")),
            "array_value": array_value,
            "outliers": [str(x) for x in state.get("outliers") or []],
            "n_dishes": sum(1 for v in table.values() if float_or_none(v) is not None),
            "rows": rows,
            "cols": list(POINTING_COLS),
        })
    return out


def acqnotes_detail(state, run, svc) -> dict | None:
    """What the notes job has measured (its index: {acq: measured at})
    and what its last run did (the run record)."""
    if not isinstance(state, dict) and not isinstance(run, dict):
        return None
    run = run if isinstance(run, dict) else {}
    return {
        "n_with_notes": len(state) if isinstance(state, dict) else 0,
        "acquisitions": run.get("acquisitions"),
        "backlog": run.get("backlog"),
        "measured": [str(a) for a in (run.get("measured") or [])],
        "written": [str(a) for a in (run.get("written") or [])],
        "run_time": fmt_utc(run.get("time")),
        "run_status": run.get("status"),
    }


def waterfall_detail(state: dict | None, run, svc) -> dict | None:
    """The renderer's last run from its state file."""
    if state is None:
        return None
    errors = [str(e) for e in (state.get("errors") or [])]
    return {
        "updated": fmt_utc(state.get("updated")),
        "roots": ", ".join(str(r) for r in (state.get("roots") or [])),
        "waterfalls_dir": state.get("waterfalls_dir"),
        "files_rendered": state.get("files_rendered"),
        "acquisitions_touched": state.get("acquisitions_touched"),
        "backlog": state.get("backlog"),
        "run_seconds": state.get("run_seconds"),
        "last_acquisition": state.get("last_acquisition"),
        "last_file_idx": state.get("last_file_idx"),
        "errors": errors[:10],
        "errors_total": len(errors),
    }


#: Each job's summariser; the page's ``_detail_<name>.html`` renders what
#: it returns.  A job without one shows the common facts alone.
JOB_DETAILS = {
    "eop": eop_detail,
    "bffs": bffs_detail,
    "eigencal": eigencal_detail,
    "mapmaker": mapmaker_detail,
    "pointing": pointing_detail,
    "acqnotes": acqnotes_detail,
    "waterfall": waterfall_detail,
}
