"""spectral-shape source — feeds whose autocorrelation spectrum has the wrong shape.

The F-engine's digital gains flatten each feed's bandpass, so in the N²
file every working feed's autocorrelation has nearly the same shape as
the others of its polarization: on 2026-10-25 the spread of the slopes
below was 0.007-0.012 dex per octave.  A feed whose spectrum tilts
(C02X: -0.37 dex/octave in every file, from its gain table) or changes
shape from file to file (B05Y: -0.03 to +0.13 within an acquisition) is
not working, though its band-averaged power can sit inside the
power-outlier bounds.

Per file: the newest ``chunk`` rows of ``blocks`` chunk-aligned 16-channel
blocks spread over ``freq_lo``-``freq_hi``; per feed and block, the median
over the channels of the time-averaged autocorrelation (a median, so a
few RFI channels do not move it).  Each feed's log level is taken
relative to the median of its polarization, block by block, and fitted
with a line in log2(frequency); one pass drops blocks more than ``clip``
robust sigmas off the line (an RFI-filled block).  The slope is in dex
per octave.

A feed is bad when

- its slope is more than ``nsigma`` robust sigmas **and** more than
  ``min_slope`` dex/octave from its polarization's median slope (the
  floor stops a tight population from flagging a harmless tilt), or
- its slope moved by more than ``max_change`` dex/octave since the file
  ``change_lag`` files earlier (an unstable feed, flagged even while its
  shape happens to look normal).

A flag holds until ``clear_after`` consecutive files have passed.  The
slopes and verdicts of the recent files are kept beside run.json
(``spectral_shape.json``), keyed by file index; that history is also the
cache, since a landed file never changes and the timer fires about 13
times per file.

The source abstains (every feed good, ``degraded``) when fewer than
``min_blocks`` blocks hold data, when they span less than an octave, or,
for one polarization, when fewer than ``min_feeds`` feeds can be fitted.
Feeds it cannot fit (dead, unmeasured, RFI antennas, missing elements)
stay good: power-outlier and dish-type judge those.
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from pathlib import Path

import numpy as np

from choco.acqtree import file_index
from choco.dishlabels import DISH_MISSING, DISH_RFI
from choco.jobclient import write_json_atomic
from kotekan_io import Frame, read_autocorr, uniquify_labels

from .common import project, report

log = logging.getLogger("bffs.sources.spectral_shape")

NEEDS_FILE = True

_MAD_TO_SIGMA = 1.4826
HISTORY_NAME = "spectral_shape.json"
DEFAULT_EXCLUDE_TYPES = (DISH_RFI, DISH_MISSING)
#: Parameters and their defaults.  Every one is part of the history's
#: key: a change starts the history afresh.
DEFAULTS = {
    "freq_lo": 320.0,
    "freq_hi": 1450.0,
    "blocks": 48,
    "chunk": 40,           # time rows: a whole 400 s file of 10 s bins
    "pivot_mhz": 600.0,
    "clip": 4.0,
    "nsigma": 8.0,
    "min_slope": 0.05,     # dex per octave
    "max_change": 0.04,    # dex per octave
    "change_lag": 3,       # files
    "clear_after": 6,      # files
    "min_blocks": 12,
    "min_feeds": 8,
    "min_block_coverage": 0.5,
}


def block_levels(frame: Frame, min_block_coverage: float = 0.5
                 ) -> tuple[np.ndarray, np.ndarray]:
    """``(levels, centres)``: per block and feed the log10 of the median
    over its channels of the time-averaged autocorrelation, nan where the
    block or the feed has no data; and each block's centre in MHz.

    Blocks are the runs of adjacent channels in ``frame.freq`` (what
    :func:`kotekan_io.channel_blocks` read).  A block whose delivered
    (time, channel) cells are fewer than ``min_block_coverage`` is nan for
    every feed.
    """
    freq = np.asarray(frame.freq, dtype=np.float64)
    if freq.size == 0:
        return np.zeros((0, frame.nfeed)), np.zeros(0)
    step = np.diff(freq)
    gap = 1.5 * float(np.median(step)) if step.size else 0.0
    cuts = np.flatnonzero(step > gap) + 1 if step.size else np.zeros(0, int)
    w = np.asarray(frame.weight, dtype=np.float64)
    wsum = w.sum(axis=0)                                       # (nfreq, nfeed)
    mean = np.full(wsum.shape, np.nan)
    np.divide((w * frame.auto).sum(axis=0), wsum, out=mean, where=wsum > 0)
    mean[~(mean > 0)] = np.nan
    levels, centres = [], []
    for chans in np.split(np.arange(freq.size), cuts):
        centres.append(float(freq[chans].mean()))
        if frame.valid[:, chans].mean() < min_block_coverage:
            levels.append(np.full(frame.nfeed, np.nan))
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)     # a feed with no data: nan
            levels.append(np.log10(np.nanmedian(mean[chans], axis=0)))
    return np.array(levels), np.array(centres)


def fit_slope(x: np.ndarray, r: np.ndarray, clip: float, min_blocks: int) -> float:
    """The slope of ``r`` against ``x`` (dex per unit ``x``), refitted once
    without the points more than ``clip`` robust sigmas off the first
    line; nan with fewer than ``min_blocks`` finite points."""
    ok = np.isfinite(r)
    if ok.sum() < min_blocks:
        return float("nan")
    b, a = np.polyfit(x[ok], r[ok], 1)
    res = r - (a + b * x)
    sigma = _MAD_TO_SIGMA * float(np.median(np.abs(res[ok] - np.median(res[ok]))))
    if sigma > 0:
        keep = ok & (np.abs(res) <= clip * sigma)
        if keep.sum() >= min_blocks and keep.sum() < ok.sum():
            b, a = np.polyfit(x[keep], r[keep], 1)
    return float(b)


def judge(frame: Frame, labels, eligible: np.ndarray | None, p: dict):
    """One file's slopes and shape verdict.

    Returns ``(slopes, bad, stats, why_not)``: ``slopes`` maps each fitted
    feed's label to its slope, ``bad`` each flagged label to a reason,
    ``stats`` the per-polarization median, sigma and count; ``why_not``
    is the reason the whole file abstains, else None.
    """
    levels, centres = block_levels(frame, p["min_block_coverage"])
    have = np.isfinite(levels).any(axis=1)
    n_blocks = int(have.sum())
    if n_blocks < p["min_blocks"]:
        return {}, {}, {}, (f"{n_blocks} of {len(centres)} blocks hold data, "
                            f"fewer than {p['min_blocks']}")
    span = float(np.log2(centres[have].max() / centres[have].min()))
    if span < 1.0:
        return {}, {}, {}, f"the blocks with data span {span:.2f} octaves, less than one"
    x = np.log2(centres / p["pivot_mhz"])
    labels = [str(lbl) for lbl in labels]
    usable = np.ones(len(labels), bool) if eligible is None else np.asarray(eligible, bool)
    if frame.measured is not None:
        usable &= frame.measured
    # a feed with data in too few blocks (dead, unmeasured) is not fitted
    usable &= np.isfinite(levels).sum(axis=0) >= p["min_blocks"]
    slopes: dict[str, float] = {}
    bad: dict[str, str] = {}
    stats: dict[str, dict] = {}
    short: list[str] = []
    for pol in ("X", "Y"):
        members = [i for i, lbl in enumerate(labels) if usable[i] and lbl.endswith(pol)]
        if len(members) < p["min_feeds"]:
            if any(lbl.endswith(pol) for lbl in labels):
                short.append(f"{pol}: {len(members)} feeds to fit, fewer than {p['min_feeds']}")
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)     # a block with no data: nan
            ref = np.nanmedian(levels[:, members], axis=1)
        b = {labels[i]: fit_slope(x, levels[:, i] - ref, p["clip"], p["min_blocks"])
             for i in members}
        b = {k: v for k, v in b.items() if np.isfinite(v)}
        if len(b) < p["min_feeds"]:
            short.append(f"{pol}: {len(b)} feeds fitted, fewer than {p['min_feeds']}")
            continue
        vals = np.array(list(b.values()))
        med = float(np.median(vals))
        sigma = _MAD_TO_SIGMA * float(np.median(np.abs(vals - med)))
        limit = max(p["nsigma"] * sigma, p["min_slope"])
        stats[pol] = {"n": len(b), "median": round(med, 4), "sigma": round(sigma, 4),
                      "limit": round(limit, 4)}
        for lbl, v in b.items():
            slopes[lbl] = round(v, 4)
            if abs(v - med) > limit:
                z = f", {abs(v - med) / sigma:.0f}σ" if sigma > 0 else ""
                bad[lbl] = f"slope {v:+.3f} dex/octave ({pol} median {med:+.3f}{z})"
    if not stats:
        return {}, {}, {}, "; ".join(short) or "no feed to fit"
    stats["short"] = short
    return slopes, bad, stats, None


def _params(src: dict) -> dict:
    p = {}
    for k, v in DEFAULTS.items():
        p[k] = type(v)(src.get(k, v))
    return p


def _read_history(path: Path | None, key: dict) -> list[dict]:
    if path is None:
        return []
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(doc, dict) or doc.get("key") != key or not isinstance(doc.get("files"), list):
        return []
    return [e for e in doc["files"] if isinstance(e, dict) and isinstance(e.get("idx"), int)]


def _write_history(path: Path | None, key: dict, files: list[dict]) -> None:
    if path is None:
        return
    try:
        write_json_atomic(path, {"key": key, "files": files})
    except OSError as e:
        log.warning("spectral-shape: could not write %s: %s", path, e)


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """Good-mask over ``labels`` from the shape of the newest file's
    autocorrelations, with the instability test and the hold applied
    from the history of recent files (see the module docstring).  The
    file's own axis is judged and projected onto ``labels`` by name."""
    all_good = np.ones(len(labels), dtype=bool)
    p = _params(src)
    types = src.get("dish_types")
    exclude = {str(t) for t in (src.get("exclude_types") or DEFAULT_EXCLUDE_TYPES)}
    key = {"params": p, "exclude_types": sorted(exclude),
           "dish_types": dict(types) if types else None}
    state_dir = src.get("state_dir")
    hist_path = Path(state_dir) / HISTORY_NAME if state_dir else None
    name = os.path.basename(str(kotekan_file))
    idx = file_index(name)
    try:
        st = os.stat(kotekan_file)
        ident = {"file": str(kotekan_file), "mtime_ns": int(st.st_mtime_ns), "size": int(st.st_size)}
    except OSError:
        ident = None

    files = _read_history(hist_path, key) if idx is not None else []
    entry = next((e for e in files if ident is not None and e.get("ident") == ident), None)
    cached = entry is not None
    if entry is None:
        frame = read_autocorr(kotekan_file, chunk=p["chunk"], blocks=p["blocks"],
                              freq_lo=p["freq_lo"], freq_hi=p["freq_hi"])
        if frame is None:
            return all_good, report("degraded", f"no filled time rows in {name}: nothing to judge",
                                    n_measured=0, file=str(kotekan_file))
        file_axis = uniquify_labels(frame.labels)
        eligible = None
        if types:
            eligible = np.array([types.get(lbl) not in exclude for lbl in file_axis], dtype=bool)
        slopes, bad, stats, why_not = judge(frame, file_axis, eligible, p)
        if why_not is not None:
            log.warning("spectral-shape: %s: %s; no feeds judged", name, why_not)
            return all_good, report("degraded", f"{name}: {why_not}", n_measured=0,
                                    file=str(kotekan_file))
        if idx is not None:
            prev = next((e for e in files if e["idx"] == idx - p["change_lag"]), None)
            for lbl, v in slopes.items():
                old = (prev or {}).get("slopes", {}).get(lbl)
                if old is not None and abs(v - old) > p["max_change"] and lbl not in bad:
                    bad[lbl] = (f"slope moved {v - old:+.3f} dex/octave in "
                                f"{p['change_lag']} files ({old:+.3f} to {v:+.3f})")
        entry = {"idx": idx, "ident": ident, "slopes": slopes, "bad": bad, "stats": stats}
        if idx is not None:
            keep = max(p["clear_after"], p["change_lag"] + 1)
            files = [e for e in files if idx - keep < e["idx"] < idx] + [entry]
            _write_history(hist_path, key, files)

    # the hold: bad in any of the last clear_after files
    reasons = dict(entry["bad"])
    held = 0
    for e in sorted(files, key=lambda e: -e["idx"]):
        if idx is None or e is entry or not (idx - p["clear_after"] < e["idx"] < idx):
            continue
        for lbl, why in (e.get("bad") or {}).items():
            if lbl not in reasons and lbl in entry["slopes"]:
                reasons[lbl] = f"held from file {e['idx']}: {why}"
                held += 1
    verdict = {lbl: lbl not in reasons for lbl in entry["slopes"]}
    stats = entry.get("stats") or {}
    rep = report(
        "degraded" if stats.get("short") else "ok",
        "; ".join(stats.get("short") or []) or None,
        n_measured=len(entry["slopes"]),
        file=str(kotekan_file), cached=cached,
        pols={k: v for k, v in stats.items() if k != "short"},
        n_held=held, feed_reasons=reasons,
        flagged_slopes={lbl: entry["slopes"][lbl] for lbl in reasons},
    )
    return project(verdict, labels), rep
