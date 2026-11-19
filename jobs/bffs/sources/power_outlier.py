"""power-outlier source — feeds whose band-averaged power is an outlier.

Reads the kotekan N² autocorrelation; the marquee data-driven, self-healing
flag. Sibling of CHIME's ``autovar``/``ampvar``.

The file is judged on its *own* element axis and the verdict is projected
onto the flag axis by label.  The ``subset/`` files this reads in
production are compact ``DishInputs`` frames over the wired elements only
(48 of the correlator's 128 as of 2026-10), so most of the flag axis is
simply not in the file: those feeds are unmeasured and stay good, and the
report says how many (``n_not_in_file``).  Position is never trusted
across the two axes — the core checks beforehand that every file label is
on the flag axis and skips the file otherwise.

Only main-array dishes are judged against each other.  The RFI-monitor
antennas (kotekan type ``RFIDish``) are real receivers but not feeds of
the array: they see the horizon, not the sky the dishes see, so their
power is no evidence about them *as feeds* and they are left out of the
median and never flagged here (``exclude_types``, read from the dish
types the core passes in).

The source judges feeds only when enough of the band is present.  Every
(time, frequency) cell the receiver never filled — the channels of an
X-engine node that is down, the empty tail of a stopped acquisition — is
excluded from the average, and when fewer than ``min_coverage`` of the
band's cells hold data the source abstains (every feed left good,
``degraded`` reported) instead of reading the gap as 128 dead feeds.

Feeds that another source already knows are not working feeds —
unpowered (``power``), marked by an operator (``manual``), not connected
(``dish-type``) — are left out of the median and the spread as well, and
are not judged here (``exclude_flagged_by``).  The digital gains lift a
dead feed to within about 2 dB of the rest, so a dozen unpowered dishes
sit a few sigma low, widen the spread and, when they are half the array,
take the median with them (2026-11-18: 83 working feeds flagged while
the dishes were being powered up).  With them out, the statistics
describe the working feeds, and stay meaningful as dishes come and go.
Those sources must come before this one in the config.

As a backstop the source also abstains when it would flag more than
``max_flag_frac`` of the feeds it judged: an outlier test has nothing to
say when there is no clear majority to be an outlier from.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import numpy as np

from choco.dishlabels import DISH_RFI
from choco.jobclient import write_json_atomic
from kotekan_io import DEFAULT_BLOCKS, Frame, read_autocorr, uniquify_labels

from .common import project, report

log = logging.getLogger("bffs.sources.power_outlier")

# This source measures the N² file itself; with no usable file the core
# skips it (and the other sources still flag).
NEEDS_FILE = True

_MAD_TO_SIGMA = 1.4826  # turns a median-absolute-deviation into a standard deviation
_KEYS = ("freq_lo", "freq_hi", "nsigma", "abs_lo", "abs_hi", "min_valid_frac")
#: The verdict on the newest file, kept beside run.json: a file is
#: complete when it lands and never changes, and the timer fires 13
#: times between files, so the same read would otherwise repeat 13 times.
CACHE_NAME = "power_outlier.json"
#: Elements of these kotekan dish types are neither compared nor flagged.
DEFAULT_EXCLUDE_TYPES = (DISH_RFI,)
#: Least fraction of the band's (time, freq) cells that must hold data
#: before feeds are judged at all.  One X-engine node is 1/8 of the
#: band, so a quarter still lets the source work with most of the
#: cluster down, while a stopped acquisition's tail (one channel in
#: thousands) or a receiver fed by a lone node abstains.
DEFAULT_MIN_COVERAGE = 0.25
#: Feeds these sources flagged earlier in the run are neither compared
#: nor flagged: they are known not to be working feeds.
DEFAULT_EXCLUDE_FLAGGED_BY = ("manual", "power", "dish-type")
#: Largest fraction of the judged feeds this source will flag; above it
#: there is no majority to judge against and it abstains.
DEFAULT_MAX_FLAG_FRAC = 0.25


def band_mask(frame: Frame, freq_lo: float | None = None,
              freq_hi: float | None = None) -> np.ndarray:
    """(nfreq,) bool: the frame's channels inside ``[freq_lo, freq_hi]``."""
    band = np.ones(frame.freq.shape[0], dtype=bool)
    if freq_lo is not None:
        band &= frame.freq >= freq_lo
    if freq_hi is not None:
        band &= frame.freq <= freq_hi
    return band


def band_coverage(frame: Frame, freq_lo: float | None = None,
                  freq_hi: float | None = None) -> float:
    """Fraction of the frame's (time, band-frequency) cells that hold data.

    ``frame.valid`` is the receiver's per-cell record of frames received,
    shared by every feed, so this is a property of the data stream — how
    much of the band the X-engines delivered — not of any feed.
    """
    band = band_mask(frame, freq_lo, freq_hi)
    n = frame.ntime * int(band.sum())
    return float(np.count_nonzero(frame.valid[:, band]) / n) if n else 0.0


def power_outlier_mask(
    frame: Frame,
    *,
    freq_lo: float | None = None,
    freq_hi: float | None = None,
    nsigma: float = 5.0,
    abs_lo: float | None = None,
    abs_hi: float | None = None,
    min_valid_frac: float = 0.0,
    eligible: np.ndarray | None = None,
    stats: dict | None = None,
) -> np.ndarray:
    """Good-mask (``True`` = good) over the frame's feeds, from each feed's band power.

    Reduce the frame to one power level per feed (a weighted average over time
    and a frequency band), then flag any feed sitting more than ``nsigma`` away
    from the median of the other feeds (using the median absolute deviation as
    the spread, so a few bad feeds don't skew the threshold). Also flag dead
    feeds (no valid/positive data) and any feed outside the absolute bounds —
    except feeds the file never correlates (``frame.measured`` False; unwired
    slots in a subset layout), which stay good: no data by construction is
    not a dead feed.

    ``min_valid_frac`` is per feed and relative to the cells that hold data
    at all (``frame.valid`` within the band): a feed whose own weights are
    zero on most of the delivered cells is dead, but a band only partly
    delivered (X-engine nodes down) costs every feed the same cells and
    counts against none of them.  Callers gate on :func:`band_coverage`
    for that.  ``eligible`` (``(nfeed,)`` bool) restricts the comparison
    to those feeds: the others enter neither the median nor the spread
    and are always good — the RFI antennas, which are not feeds of the
    array.  ``stats``, if given, receives ``n_live``, ``median`` and
    ``spread`` for reporting.
    """
    band = band_mask(frame, freq_lo, freq_hi)

    # one power level per feed: a weighted mean over time and the selected band
    # (feeds with no usable samples keep power 0, since `where` skips them)
    delivered = frame.valid & band[None, :]          # (ntime, nfreq), same for every feed
    usable = delivered[:, :, None]
    w = np.where(usable, frame.weight, 0.0).astype(np.float64)
    wsum = w.sum(axis=(0, 1))
    power = np.zeros(frame.nfeed, dtype=np.float64)
    has_data = wsum > 0
    np.divide((w * frame.auto).sum(axis=(0, 1)), wsum, out=power, where=has_data)

    if min_valid_frac > 0:
        n_delivered = int(np.count_nonzero(delivered))
        kept_frac = np.count_nonzero(w > 0, axis=(0, 1)) / max(n_delivered, 1)
        has_data &= kept_frac >= min_valid_frac

    live = has_data & (power > 0)  # dead feeds (no valid/positive power) are bad
    if eligible is not None:
        live &= np.asarray(eligible, dtype=bool)
    good = live.copy()

    median = spread = None
    live_power = power[live]  # compare each feed only against the other live feeds
    if live_power.size >= 2:
        median = float(np.median(live_power))
        spread = _MAD_TO_SIGMA * float(np.median(np.abs(live_power - median)))
        distance = np.abs(power - median)
        if spread > 0:
            good &= ~(live & (distance / spread > nsigma))
        else:
            # Every working feed reads the same level, so there is no spread to
            # measure against — treat any feed that differs at all as bad.
            tol = 1e-9 * max(abs(median), 1.0)
            good &= ~(live & (distance > tol))

    if abs_lo is not None:
        good &= ~(live & (power < abs_lo))
    if abs_hi is not None:
        good &= ~(live & (power > abs_hi))
    if frame.measured is not None:
        # Feeds the file's product list never correlates (unwired slots
        # in a subset layout) have no data by construction — this source
        # only flags what it can measure, so they stay good.  A feed
        # that *is* in the products but silent is still dead-and-bad.
        good |= ~frame.measured
    if eligible is not None:
        good |= ~np.asarray(eligible, dtype=bool)
    if stats is not None:
        n_live = int(live.sum()) if frame.measured is None else int((live & frame.measured).sum())
        stats.update(n_live=n_live, median=median, spread=spread)
    return good


def _cache_key(src: dict, kotekan_file: str, chunk: int, blocks: int,
               params: dict, min_coverage: float, exclude: set, types,
               flagged: dict, max_flag_frac: float) -> dict | None:
    """What the verdict depends on: the file (path, mtime, size) and
    every parameter that shapes the judgement.  None when the file
    cannot be stat'ed (the reader then reports it missing)."""
    try:
        st = os.stat(kotekan_file)
    except OSError:
        return None
    return {"file": str(kotekan_file), "mtime_ns": int(st.st_mtime_ns), "size": int(st.st_size),
            "chunk": chunk, "blocks": blocks, "params": params, "min_coverage": min_coverage,
            "exclude_types": sorted(exclude), "dish_types": dict(types) if types else None,
            "flagged": dict(sorted(flagged.items())), "max_flag_frac": max_flag_frac}


def _cache_path(src: dict) -> Path | None:
    state_dir = src.get("state_dir")
    return Path(state_dir) / CACHE_NAME if state_dir else None


def _read_cache(path: Path | None, key: dict | None):
    """``(verdict, report)`` cached for *key*, or None."""
    if path is None or key is None:
        return None
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict) or doc.get("key") != key:
        return None
    verdict, rep = doc.get("verdict"), doc.get("report")
    if not isinstance(rep, dict) or (verdict is not None and not isinstance(verdict, dict)):
        return None
    return verdict, rep


def _write_cache(path: Path | None, key: dict | None, verdict, rep: dict) -> None:
    if path is None or key is None:
        return
    try:
        write_json_atomic(path, {"key": key, "verdict": verdict, "report": rep})
    except OSError as e:
        log.warning("power-outlier: could not write %s: %s", path, e)


def _judge(src: dict, kotekan_file: str, chunk: int, blocks: int, params: dict,
           min_coverage: float, exclude: set, types, flagged: dict,
           max_flag_frac: float):
    """Read the file and judge its own axis.

    Returns ``(verdict, report)``: ``verdict`` maps each file label to
    good/bad, or is None when the source abstained (the report says why).
    Neither depends on the flag axis, so both can be cached per file.
    *flagged* maps the labels other sources already flag to the source
    kind; those elements are left out like the excluded dish types.
    """
    name = os.path.basename(str(kotekan_file))
    frame = read_autocorr(kotekan_file, chunk=chunk, blocks=blocks,
                          freq_lo=params.get("freq_lo"), freq_hi=params.get("freq_hi"))
    if frame is None:
        log.warning("power-outlier: no filled time rows in %s; no feeds judged", kotekan_file)
        return None, report(
            "degraded", f"no filled time rows in {name}: nothing to judge",
            n_measured=0, file=str(kotekan_file))
    coverage = band_coverage(frame, params.get("freq_lo"), params.get("freq_hi"))
    detail = {
        "file": str(kotekan_file),
        "rows": frame.ntime,
        "rows_in_file": frame.file_ntime,
        "tail_rows_skipped": frame.tail_skipped,
        "channels_read": int(frame.freq.shape[0]),
        "blocks": blocks,
        "band_coverage": round(coverage, 4),
        "min_coverage": min_coverage,
    }
    if coverage < min_coverage:
        log.warning(
            "power-outlier: %s holds data for %.0f%% of the band's cells in "
            "its newest %d rows (min_coverage %.0f%%); no feeds judged — "
            "X-engine nodes down, or the acquisition has ended",
            name, 100 * coverage, frame.ntime, 100 * min_coverage)
        return None, report(
            "degraded",
            f"band coverage {coverage:.0%} is below {min_coverage:.0%}: too "
            f"little N² data to judge feeds (X-engine nodes down, or the "
            f"acquisition has ended)",
            n_measured=0, **detail)
    # The file's own axis; its elements are judged in place and projected
    # onto the flag axis by label by the caller.
    file_axis = [str(lbl) for lbl in uniquify_labels(frame.labels)]
    eligible = np.ones(len(file_axis), dtype=bool)
    if types:
        eligible = np.array([types.get(lbl) not in exclude for lbl in file_axis],
                            dtype=bool)
    detail["exclude_types"] = sorted(exclude)
    detail["n_excluded"] = int((~eligible).sum())
    detail["types_known"] = bool(types)
    # Feeds another source already flags: counted by the source that
    # flagged them, and out of the comparison like the excluded types.
    excluded_by: dict[str, int] = {}
    for i, lbl in enumerate(file_axis):
        if eligible[i] and lbl in flagged:
            eligible[i] = False
            excluded_by[flagged[lbl]] = excluded_by.get(flagged[lbl], 0) + 1
    detail["excluded_by"] = excluded_by
    detail["n_excluded_flagged"] = sum(excluded_by.values())
    stats: dict = {}
    file_good = power_outlier_mask(frame, stats=stats, eligible=eligible, **params)
    if stats.get("median") is not None:
        detail["median_power"] = float(f"{stats['median']:.4g}")
        detail["spread"] = float(f"{stats['spread']:.4g}")
    detail["n_file_elements"] = int(frame.nfeed)
    detail["max_flag_frac"] = max_flag_frac
    judged = eligible if frame.measured is None else eligible & frame.measured
    n_judged, n_bad = int(judged.sum()), int((judged & ~file_good).sum())
    if n_bad > max_flag_frac * n_judged:
        log.warning(
            "power-outlier: %d of %d feeds in %s would be flagged (limit %.0f%%); "
            "no feeds judged — no majority to be an outlier from",
            n_bad, n_judged, name, 100 * max_flag_frac)
        return None, report(
            "degraded",
            f"{n_bad} of {n_judged} feeds would be flagged, more than "
            f"{max_flag_frac:.0%}: no clear majority to judge against, so "
            f"none is flagged",
            n_measured=0, **detail)
    verdict = {str(lbl): bool(g) for lbl, g in zip(file_axis, file_good)}
    return verdict, report("ok", n_measured=stats.get("n_live"), **detail)


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """Good-mask over ``labels`` from the kotekan file's autocorrelation power.

    The outlier statistics run over the file's own axis (``frame.labels``,
    every element the file carries); each element's verdict is then
    projected onto ``labels`` by name, so a compact ``subset/`` file judges
    the feeds it holds and leaves the rest of the axis good.  Abstains (all
    good, ``degraded``) when the file's newest filled rows cover less than
    ``min_coverage`` of the band — see the module docstring.  The report's
    ``n_measured`` counts the live feeds judged; ``n_in_file`` /
    ``n_not_in_file`` split the flag axis by whether the file carries it.
    Elements whose dish type (``src["dish_types"]``, from the core) is in
    ``exclude_types`` (default ``RFIDish``) are left out of the comparison
    and never flagged, ``n_excluded`` of them; without types every
    element is judged.  So are the feeds that the sources named in
    ``exclude_flagged_by`` (default ``manual``, ``power``,
    ``dish-type``) flagged earlier in this run (``src["flagged_before"]``,
    from the core), ``n_excluded_flagged`` of them; listing one of those
    sources after this one is a config error.  And when more than
    ``max_flag_frac`` (default 0.25) of the judged feeds would be
    flagged, the source abstains.

    The file is read in ``blocks`` chunk-aligned frequency blocks spread
    over the band (:func:`kotekan_io.channel_blocks`; 0 reads every
    channel), and the verdict is cached in the state directory keyed on
    the file's path, mtime and size and on these parameters: a kotekan
    file never changes once it has landed, so a run that finds the same
    file reuses the verdict (``cached`` in the report) instead of the
    read.
    """
    all_good = np.ones(len(labels), dtype=bool)
    chunk = int(src.get("chunk", 16))
    blocks = int(src.get("blocks", DEFAULT_BLOCKS))
    params = {k: src[k] for k in _KEYS if k in src}
    min_coverage = float(src.get("min_coverage", DEFAULT_MIN_COVERAGE))
    types = src.get("dish_types")
    exclude = {str(t) for t in (src.get("exclude_types") or DEFAULT_EXCLUDE_TYPES)}
    max_flag_frac = float(src.get("max_flag_frac", DEFAULT_MAX_FLAG_FRAC))
    kinds = src.get("exclude_flagged_by", DEFAULT_EXCLUDE_FLAGGED_BY) or ()
    late = [k for k in kinds if k in (src.get("kinds_after") or ())]
    if late:
        raise ValueError(
            f"power-outlier leaves out the feeds that {', '.join(late)} "
            f"flag(s), so it must come after them in `sources`")
    # label -> the first source that flagged it, of the kinds asked for
    flagged: dict[str, str] = {}
    for k in kinds:
        for lbl in (src.get("flagged_before") or {}).get(k, ()):
            flagged.setdefault(str(lbl), str(k))

    key = _cache_key(src, kotekan_file, chunk, blocks, params, min_coverage, exclude, types,
                     flagged, max_flag_frac)
    cache = _cache_path(src)
    hit = _read_cache(cache, key)
    if hit is not None:
        verdict, rep = hit
        log.debug("power-outlier: verdict for %s from %s", kotekan_file, cache)
    else:
        verdict, rep = _judge(src, kotekan_file, chunk, blocks, params, min_coverage, exclude,
                              types, flagged, max_flag_frac)
        _write_cache(cache, key, verdict, rep)
    rep = {**rep, "detail": dict(rep.get("detail") or {})}
    rep["detail"]["cached"] = hit is not None
    if verdict is None:
        return all_good, rep
    # Feeds the file does not carry have no entry and project to good
    # (unmeasured).
    in_file = [lbl for lbl in (str(l) for l in labels) if lbl in verdict]
    rep["detail"]["n_in_file"] = len(in_file)
    rep["detail"]["n_not_in_file"] = len(labels) - len(in_file)
    return project(verdict, labels), rep
