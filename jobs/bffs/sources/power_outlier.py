"""power-outlier source — feeds whose band-averaged power is an outlier.

Reads the kotekan N² autocorrelation; the marquee data-driven, self-healing
flag. Sibling of CHIME's ``autovar``/``ampvar``.

The source judges feeds only when enough of the band is present.  Every
(time, frequency) cell the receiver never filled — the channels of an
X-engine node that is down, the empty tail of a stopped acquisition — is
excluded from the average, and when fewer than ``min_coverage`` of the
band's cells hold data the source abstains (every feed left good,
``degraded`` reported) instead of reading the gap as 128 dead feeds.
"""

from __future__ import annotations

import logging
import os

import numpy as np

from kotekan_io import Frame, read_autocorr

from .common import report

log = logging.getLogger("bffs.sources.power_outlier")

# This source measures the N² file itself; with no usable file the core
# skips it (and the other sources still flag).
NEEDS_FILE = True

_MAD_TO_SIGMA = 1.4826  # turns a median-absolute-deviation into a standard deviation
_KEYS = ("freq_lo", "freq_hi", "nsigma", "abs_lo", "abs_hi", "min_valid_frac")
#: Least fraction of the band's (time, freq) cells that must hold data
#: before feeds are judged at all.  One X-engine node is 1/8 of the
#: band, so a quarter still lets the source work with most of the
#: cluster down, while a stopped acquisition's tail (one channel in
#: thousands) or a receiver fed by a lone node abstains.
DEFAULT_MIN_COVERAGE = 0.25


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
    for that.  ``stats``, if given, receives ``n_live``, ``median`` and
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
    if stats is not None:
        n_live = int(live.sum()) if frame.measured is None else int((live & frame.measured).sum())
        stats.update(n_live=n_live, median=median, spread=spread)
    return good


def mask(src: dict, labels: np.ndarray, kotekan_file: str):
    """Good-mask over ``labels`` from the kotekan file's autocorrelation power.

    ``frame.inputs`` is the same ``index_map/input`` as ``labels`` (same file),
    so the mask is already in axis order — no re-mapping needed.  Abstains
    (all good, ``degraded``) when the file's newest filled rows cover less
    than ``min_coverage`` of the band — see the module docstring.
    """
    all_good = np.ones(len(labels), dtype=bool)
    name = os.path.basename(str(kotekan_file))
    frame = read_autocorr(kotekan_file, chunk=int(src.get("chunk", 16)))
    if frame is None:
        log.warning("power-outlier: no filled time rows in %s; no feeds judged", kotekan_file)
        return all_good, report(
            "degraded", f"no filled time rows in {name}: nothing to judge",
            n_measured=0, file=str(kotekan_file))
    params = {k: src[k] for k in _KEYS if k in src}
    min_coverage = float(src.get("min_coverage", DEFAULT_MIN_COVERAGE))
    coverage = band_coverage(frame, params.get("freq_lo"), params.get("freq_hi"))
    detail = {
        "file": str(kotekan_file),
        "rows": frame.ntime,
        "rows_in_file": frame.file_ntime,
        "tail_rows_skipped": frame.tail_skipped,
        "band_coverage": round(coverage, 4),
        "min_coverage": min_coverage,
    }
    if coverage < min_coverage:
        log.warning(
            "power-outlier: %s holds data for %.0f%% of the band's cells in "
            "its newest %d rows (min_coverage %.0f%%); no feeds judged — "
            "X-engine nodes down, or the acquisition has ended",
            name, 100 * coverage, frame.ntime, 100 * min_coverage)
        return all_good, report(
            "degraded",
            f"band coverage {coverage:.0%} is below {min_coverage:.0%}: too "
            f"little N² data to judge feeds (X-engine nodes down, or the "
            f"acquisition has ended)",
            n_measured=0, **detail)
    stats: dict = {}
    good = power_outlier_mask(frame, stats=stats, **params)
    if stats.get("median") is not None:
        detail["median_power"] = float(f"{stats['median']:.4g}")
        detail["spread"] = float(f"{stats['spread']:.4g}")
    return good, report("ok", n_measured=stats.get("n_live"), **detail)
