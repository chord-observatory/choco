"""Read one kotekan N² file into sub-band visibilities for the mapmaker.

The file is swept once in chunk-aligned frequency blocks (``vis`` is
chunked ``(16 freq, 16 prod, 20 time)``; a block is one imaging sub-band,
64 channels by default).  Per block, for each polarization's array-dish
cross products: weights are zeroed by the element flags kotekan wrote
(``flags``, the bad-feed list applied), by the per-sample RFI fraction,
by the configured bad frequency ranges and by non-finite values; the
calibrator transit's gain sums are accumulated at full spectral
resolution; the per-channel gains are divided out; and the channels are
averaged with their weights into one visibility per sub-band.

Gains must be applied before the sub-band average and the redundant
average: uncalibrated cable delays wind the phase by a turn every few
MHz, so averaging first would cancel the signal.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import h5py
import hdf5plugin  # noqa: F401  -- registers the bitshuffle filter
import numpy as np

from choco.dishlabels import file_element_labels

from imaging import ERA_DEG_PER_DAY, CalAccumulator, apply_gains, beam_fwhm_deg, beam_sq

log = logging.getLogger("mapmaker.reduce")

#: kotekan names a completed file ``vis_<abs_file_idx>_<YYYYMMDD>T_<HHMMSS>_<ns>.h5``.
NAME_RE = re.compile(r"^vis_0*(\d+)_(\d{8})T_(\d{6})_(\d+)\.h5$")

#: ``index_map/type`` codes (kotekan ``DishType``).
TYPE_ARRAY_DISH = 0


class Unusable(Exception):
    """A file this job cannot image (layout, geometry); skip it, don't retry."""


def index_from_name(basename: str) -> int | None:
    m = NAME_RE.match(basename)
    return int(m.group(1)) if m else None


def time_from_name(basename: str) -> float | None:
    """Unix time of the file's first sample, from its name."""
    m = NAME_RE.match(basename)
    if not m:
        return None
    dt = datetime.strptime(m.group(2) + m.group(3), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    frac = int(m.group(4)) * 1e-9 if len(m.group(4)) == 9 else 0.0
    return dt.timestamp() + frac


@dataclass
class FileAxes:
    path: str
    file_idx: int
    n_freq: int
    n_prod: int
    n_time: int
    n_elements: int
    labels: list
    types: np.ndarray        # (n_el,)
    pols: np.ndarray         # (n_el,)
    input_a: np.ndarray      # (n_prod,)
    input_b: np.ndarray
    freq_mhz: np.ndarray     # (n_freq,)
    feed_pos: np.ndarray     # (n_el, 3) metres, grid frame
    lat_deg: float
    lon_deg: float
    sep_x: float
    sep_y: float
    era_deg: np.ndarray      # (n_time,) Earth rotation angle per sample
    t_unix: np.ndarray       # (n_time,)


def read_axes(path: str) -> FileAxes:
    """Metadata only.  ``OSError`` (mount, unreadable) propagates; a layout
    this job cannot use raises :class:`Unusable`."""
    import os
    base = os.path.basename(path)
    idx = index_from_name(base)
    if idx is None:
        raise Unusable(f"{base}: not a kotekan vis file name")
    with h5py.File(path, "r") as f:
        im = f["index_map"]
        attrs = f.attrs
        for key in ("feed_positions_m", "itrs_lat_deg", "itrs_lon_deg"):
            if key not in attrs:
                raise Unusable(f"{base}: no {key} attribute")
        if "type" not in im or "pol" not in im or "prod" not in im or "freq" not in im:
            raise Unusable(f"{base}: index_map lacks type/pol/prod/freq")
        if "bin_ERA_deg" not in f:
            raise Unusable(f"{base}: no bin_ERA_deg")
        vis = f["vis"]
        prod = im["prod"][:]
        types = im["type"][:].astype(int)
        pols = im["pol"][:].astype(int)
        n_el = int(attrs.get("num_elements", len(types)))
        raw = []
        if "label" in im:
            raw = [b.decode() if isinstance(b, bytes) else str(b) for b in im["label"][:]]
        try:
            labels = file_element_labels(raw, n_el, pols) if raw else []
        except ValueError as exc:
            log.warning("%s: %s; using element indices", base, exc)
            labels = []
        era = f["bin_ERA_deg"][:].astype(float)
        t0 = time_from_name(base)
        if t0 is None or not np.all(np.isfinite(era)):
            raise Unusable(f"{base}: no usable time axis")
        # sample times from the ERA increments (the name times the first)
        t = t0 + (np.unwrap(np.deg2rad(era)) - np.unwrap(np.deg2rad(era))[0]) / np.deg2rad(ERA_DEG_PER_DAY) * 86400.0
        return FileAxes(
            path=path, file_idx=idx,
            n_freq=vis.shape[0], n_prod=vis.shape[1], n_time=vis.shape[2],
            n_elements=n_el, labels=labels, types=types, pols=pols,
            input_a=prod["input_a"].astype(int), input_b=prod["input_b"].astype(int),
            freq_mhz=im["freq"]["centre"][:].astype(float),
            feed_pos=np.asarray(attrs["feed_positions_m"], dtype=float),
            lat_deg=float(attrs["itrs_lat_deg"]), lon_deg=float(attrs["itrs_lon_deg"]),
            sep_x=float(attrs.get("feed_separation_x_m", 6.3)),
            sep_y=float(attrs.get("feed_separation_y_m", 8.5)),
            era_deg=era, t_unix=t,
        )


@dataclass
class PolSet:
    """One polarization's array-dish cross products."""

    name: str
    pol: int
    elements: np.ndarray     # element indices of the array dishes
    prod_idx: np.ndarray     # indices into the file's product axis
    bl_xy: np.ndarray        # (n_prod, 2): pos[input_b] - pos[input_a]
    a: np.ndarray            # element index of input_a per product
    b: np.ndarray


def pol_sets(ax: FileAxes, pol_names=("X", "Y")) -> list[PolSet]:
    """The cross products of the array dishes, per polarization, with the
    RFI antennas and ``Missing`` elements left out.  Both pols must list
    the same dish pairs in the same order (they do: kotekan's axis is
    [pol][dish]) so the two share one baseline table."""
    out = []
    for p, name in enumerate(pol_names):
        els = np.where((ax.types == TYPE_ARRAY_DISH) & (ax.pols == p))[0]
        s = set(els.tolist())
        sel = np.array([i for i, (a, b) in enumerate(zip(ax.input_a, ax.input_b))
                        if a in s and b in s and a != b], dtype=int)
        if len(sel) == 0:
            continue
        a, b = ax.input_a[sel], ax.input_b[sel]
        out.append(PolSet(name + name, p, els, sel, ax.feed_pos[b, :2] - ax.feed_pos[a, :2], a, b))
    if not out:
        raise Unusable(f"{ax.path}: no array-dish cross products")
    n = {len(ps.prod_idx) for ps in out}
    if len(n) != 1:
        raise Unusable(f"{ax.path}: polarizations have different product counts {sorted(n)}")
    return out


def channel_mask(freq_mhz: np.ndarray, bad_ranges) -> np.ndarray:
    """True for channels inside any configured ``[lo, hi]`` MHz range."""
    bad = np.zeros(len(freq_mhz), bool)
    for lo, hi in bad_ranges or ():
        bad |= (freq_mhz >= float(lo)) & (freq_mhz < float(hi))
    return bad


@dataclass
class CalWindow:
    """Which samples of a file fall in the calibrator's transit window and
    what the calibrator looks like in them."""

    in_window: np.ndarray    # (n_time,) bool
    tau: np.ndarray          # (n_pol, n_prod, n_time) geometric delays (s)
    cosang: np.ndarray       # (n_time,) cosine of the calibrator's angle to the pointing
    dish_diameter_m: float
    beam_fwhm_factor: float

    def beam_sq(self, freq_mhz: np.ndarray, sel) -> np.ndarray:
        """Model power beam at the calibrator, ``(n_chan, n_sel)``."""
        fwhm = beam_fwhm_deg(freq_mhz, self.dish_diameter_m, self.beam_fwhm_factor)
        return beam_sq(self.cosang[sel][None, :], np.asarray(fwhm)[:, None])


@dataclass
class Reduced:
    ax: FileAxes
    pols: list
    sub_freq_mhz: np.ndarray   # (n_sub,)
    V: np.ndarray              # (n_sub, n_pol, n_prod, n_time) complex64
    W: np.ndarray              # (n_sub, n_pol, n_prod, n_time) float32
    calibrated: bool
    frac_flagged: float        # fraction of (channel, product, sample) weights zeroed


def read_reduced(ax: FileAxes, pols: list, subband_channels: int, bad_ranges,
                 max_rfi_fraction: float, gains: np.ndarray | None,
                 cal: CalAccumulator | None = None, window: CalWindow | None = None) -> Reduced:
    """Sweep the file once; see the module docstring."""
    n_pol, n_prod, n_time = len(pols), len(pols[0].prod_idx), ax.n_time
    nsub = int(np.ceil(ax.n_freq / subband_channels))
    V_out = np.zeros((nsub, n_pol, n_prod, n_time), np.complex64)
    W_out = np.zeros((nsub, n_pol, n_prod, n_time), np.float32)
    sub_freq = np.zeros(nsub)
    bad_chan = channel_mask(ax.freq_mhz, bad_ranges)
    n_zero = 0
    n_total = 0
    with h5py.File(ax.path, "r") as f:
        dvis, dw = f["vis"], f["vis_weight"]
        dflags = f["flags"] if "flags" in f else None
        drfi = f["frac_rfi"] if "frac_rfi" in f else None
        for s in range(nsub):
            lo, hi = s * subband_channels, min((s + 1) * subband_channels, ax.n_freq)
            sl = slice(lo, hi)
            sub_freq[s] = ax.freq_mhz[sl].mean()
            vis = dvis[sl]                                   # (nc, n_prod_all, n_time)
            wgt = dw[sl].astype(np.float32)
            flags = dflags[sl] if dflags is not None else None  # (nc, n_el, n_time)
            rfi = drfi[sl] if drfi is not None else None        # (nc, n_time)
            V = np.stack([vis[:, ps.prod_idx, :] for ps in pols], axis=1)   # (nc, n_pol, n_prod, n_time)
            W = np.stack([wgt[:, ps.prod_idx, :] for ps in pols], axis=1)
            if flags is not None:
                fa = np.stack([flags[:, ps.a, :] for ps in pols], axis=1)
                fb = np.stack([flags[:, ps.b, :] for ps in pols], axis=1)
                W = W * (fa > 0) * (fb > 0)
            if rfi is not None and max_rfi_fraction is not None:
                W = W * (rfi[:, None, None, :] <= max_rfi_fraction)
            W = W * (~bad_chan[sl])[:, None, None, None]
            W = np.where(np.isfinite(V) & np.isfinite(W), W, 0).astype(np.float32)
            V = np.where(W > 0, V, 0).astype(np.complex64)
            n_zero += int((W == 0).sum())
            n_total += W.size
            if cal is not None and window is not None and window.in_window.any():
                w = window.in_window
                cal.add_block(sl, V[..., w], W[..., w], ax.freq_mhz[sl],
                              window.tau[..., w], window.beam_sq(ax.freq_mhz[sl], w), ax.t_unix[w])
            if gains is not None:
                V, W = apply_gains(V, W, gains[sl])
            wsum = W.sum(axis=0)
            with np.errstate(invalid="ignore", divide="ignore"):
                V_out[s] = np.where(wsum > 0, (W * V).sum(axis=0) / np.maximum(wsum, 1e-30), 0)
            W_out[s] = wsum
    return Reduced(ax=ax, pols=pols, sub_freq_mhz=sub_freq, V=V_out, W=W_out,
                   calibrated=gains is not None,
                   frac_flagged=(n_zero / n_total) if n_total else 1.0)
