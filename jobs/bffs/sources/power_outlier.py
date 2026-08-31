"""power-outlier source — feeds whose band-averaged power is an outlier.

Reads the kotekan N² autocorrelation; the marquee data-driven, self-healing
flag. Sibling of CHIME's ``autovar``/``ampvar``.
"""

from __future__ import annotations

import logging

import numpy as np

from kotekan_io import Frame, read_autocorr

log = logging.getLogger("bffs.sources.power_outlier")

# This source measures the N² file itself; with no usable file the core
# skips it (and the other sources still flag).
NEEDS_FILE = True

_MAD_TO_SIGMA = 1.4826  # turns a median-absolute-deviation into a standard deviation
_KEYS = ("freq_lo", "freq_hi", "nsigma", "abs_lo", "abs_hi", "min_valid_frac")


def power_outlier_mask(
    frame: Frame,
    *,
    freq_lo: float | None = None,
    freq_hi: float | None = None,
    nsigma: float = 5.0,
    abs_lo: float | None = None,
    abs_hi: float | None = None,
    min_valid_frac: float = 0.0,
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
    """
    band = np.ones(frame.freq.shape[0], dtype=bool)
    if freq_lo is not None:
        band &= frame.freq >= freq_lo
    if freq_hi is not None:
        band &= frame.freq <= freq_hi

    # one power level per feed: a weighted mean over time and the selected band
    # (feeds with no usable samples keep power 0, since `where` skips them)
    usable = frame.valid[:, :, None] & band[None, :, None]
    w = np.where(usable, frame.weight, 0.0).astype(np.float64)
    wsum = w.sum(axis=(0, 1))
    power = np.zeros(frame.nfeed, dtype=np.float64)
    has_data = wsum > 0
    np.divide((w * frame.auto).sum(axis=(0, 1)), wsum, out=power, where=has_data)

    if min_valid_frac > 0:
        nsamp = frame.ntime * int(band.sum())
        kept_frac = np.count_nonzero(w > 0, axis=(0, 1)) / max(nsamp, 1)
        has_data &= kept_frac >= min_valid_frac

    live = has_data & (power > 0)  # dead feeds (no valid/positive power) are bad
    good = live.copy()

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
    return good


def mask(src: dict, labels: np.ndarray, kotekan_file: str) -> np.ndarray:
    """Good-mask over ``labels`` from the kotekan file's autocorrelation power.

    ``frame.inputs`` is the same ``index_map/input`` as ``labels`` (same file),
    so the mask is already in axis order — no re-mapping needed.
    """
    frame = read_autocorr(kotekan_file, chunk=int(src.get("chunk", 16)))
    if frame is None:
        log.warning("power-outlier: no data in %s; no feeds flagged", kotekan_file)
        return np.ones(len(labels), dtype=bool)
    params = {k: src[k] for k in _KEYS if k in src}
    return power_outlier_mask(frame, **params)
