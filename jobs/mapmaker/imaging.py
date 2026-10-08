"""Drift-scan imaging math for the mapmaker job: geometry, beam, kernels,
the scatter-add into the map accumulators, point-source self-calibration
and the sidereal-pass bookkeeping behind the daily map.

Everything here is numpy; the only astronomy is in :func:`enu`, which
turns a CIRS (RA, Dec) into a unit vector in the telescope's East/North/Up
frame at a given local Earth rotation angle.  The conventions were fixed
against a Cyg A transit on 2026-10-05 (see docs/design/mapmaker.md):

* kotekan's ``bin_ERA_deg`` plus the site longitude is the local sidereal
  angle; a source at CIRS right ascension ``ra`` has hour angle
  ``H = era_local - ra``.
* a baseline is ``b = pos[input_b] - pos[input_a]`` (``feed_positions_m``,
  grid x = east, y = north) and a point source in direction ``u`` gives
  the phase ``-k (b_x u_E + b_y u_N)``.  The coherence test picked this
  sign over its alternatives at 0.91 against 0.07.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

C_M_PER_S = 299792458.0
#: Mean solar seconds per sidereal day and the Earth rotation rate.
SIDEREAL_DAY_S = 86164.0905
ERA_DEG_PER_DAY = 360.98564736629
#: Unix time of J2000.0 (2000-01-01T12:00:00 UTC; UT1 differs by <1 s).
J2000_UNIX = 946728000.0
#: IERS Conventions (2010) eq. 5.15: ERA = 2π (0.779… + 1.00273… Tu).
_ERA_T0 = 0.7790572732640
_ERA_RATE = 1.00273781191135448


# --- sidereal grid ---------------------------------------------------------

def era_local_deg(era_deg, lon_deg: float):
    """Local Earth rotation angle in [0, 360): the map's RA axis."""
    return np.mod(np.asarray(era_deg, dtype=float) + lon_deg, 360.0)


def ra_bin(era_local, n_bins: int):
    """The sidereal bin a sample at *era_local* (deg) falls in."""
    return (np.floor(np.asarray(era_local) / 360.0 * n_bins).astype(int)) % n_bins


def pass_index(t_unix, lon_deg: float):
    """Which sidereal day (pass) a sample belongs to.

    Counts full turns of the local sidereal angle since J2000, so the
    boundary between passes is local sidereal angle 0: the point where
    the map's RA axis wraps.  Seconds of timing error only matter within
    one bin of that boundary, and the daily composite is driven by
    exposure, not by bin ranges, so that does no harm.
    """
    turns = (_ERA_T0 + lon_deg / 360.0
             + _ERA_RATE * (np.asarray(t_unix, dtype=float) - J2000_UNIX) / 86400.0)
    return np.floor(turns).astype(int)


# --- directions ------------------------------------------------------------

def enu(ra_deg, dec_deg, era_local_deg, lat_deg: float):
    """East/North/Up unit vector(s) of CIRS (ra, dec) at local ERA.

    Broadcasts over its array arguments.  Returns ``(uE, uN, uU)``.
    """
    H = np.deg2rad(np.mod(np.asarray(era_local_deg, dtype=float) - np.asarray(ra_deg, dtype=float)
                          + 180.0, 360.0) - 180.0)
    d = np.deg2rad(np.asarray(dec_deg, dtype=float))
    p = math.radians(lat_deg)
    uE = -np.cos(d) * np.sin(H)
    uN = np.sin(d) * np.cos(p) - np.cos(d) * np.cos(H) * np.sin(p)
    uU = np.sin(d) * np.sin(p) + np.cos(d) * np.cos(H) * np.cos(p)
    return uE, uN, uU


def pointing_vector(dec_deg: float, lat_deg: float):
    """ENU unit vector of the fixed pointing: the meridian at *dec_deg*."""
    zd = math.radians(dec_deg - lat_deg)
    return np.array([0.0, math.sin(zd), math.cos(zd)])


def beam_fwhm_deg(freq_mhz, dish_diameter_m: float, factor: float = 1.03):
    """Primary power-beam FWHM: *factor* × λ/D (1.03 for an Airy disc).

    Cyg A transits measured 4.9°, 3.2° and 2.0° at 677, 1002 and 1402 MHz
    against 4.4°, 2.9° and 2.1° from this formula; close enough for a
    weighting function.
    """
    lam = C_M_PER_S / (np.asarray(freq_mhz, dtype=float) * 1e6)
    return np.rad2deg(factor * lam / dish_diameter_m)


def beam_sq(cosang, fwhm_deg):
    """Gaussian stand-in for the power beam, as a function of cos(angle)."""
    ang = np.arccos(np.clip(cosang, -1.0, 1.0))
    sigma = np.deg2rad(fwhm_deg) / 2.3548200450309493
    return np.exp(-0.5 * (ang / sigma) ** 2)


def source_delays(baselines_xy, uE, uN):
    """Geometric delay τ (s) per baseline for direction(s) (uE, uN):
    the phase of a point source is ``exp(2πi ν τ)``.

    ``baselines_xy`` is (nb, 2); uE/uN broadcast to the trailing axes, so
    the result is (nb, ...)."""
    bx = np.asarray(baselines_xy)[:, 0]
    by = np.asarray(baselines_xy)[:, 1]
    return -(bx[(slice(None),) + (None,) * np.ndim(uE)] * uE
             + by[(slice(None),) + (None,) * np.ndim(uN)] * uN) / C_M_PER_S


# --- the map grid and kernels ---------------------------------------------

@dataclass
class Grid:
    """The map's pixel grid: sidereal bins in RA, a strip of Dec rows
    centred on the pointing."""

    n_bins: int
    dec_deg: np.ndarray          # (n_dec,)
    pointing_dec_deg: float
    lat_deg: float
    lon_deg: float

    @classmethod
    def build(cls, n_bins: int, pointing_dec_deg: float, halfwidth_deg: float,
              step_deg: float, lat_deg: float, lon_deg: float) -> "Grid":
        n = int(round(2 * halfwidth_deg / step_deg)) + 1
        decs = pointing_dec_deg + (np.arange(n) - n // 2) * step_deg
        return cls(int(n_bins), decs.astype(float), float(pointing_dec_deg),
                   float(lat_deg), float(lon_deg))

    @property
    def bin_deg(self) -> float:
        return 360.0 / self.n_bins

    @property
    def n_dec(self) -> int:
        return len(self.dec_deg)

    def footprint_bins(self, fwhm_deg: float, factor: float) -> int:
        """Half-width M of a kernel, in RA bins: *factor* FWHM on the sky,
        stretched by 1/cos(dec) because a degree of sky is more degrees
        of hour angle away from the equator."""
        ext = factor * fwhm_deg / max(math.cos(math.radians(self.pointing_dec_deg)), 0.05)
        return max(1, int(math.ceil(ext / self.bin_deg)))


@dataclass
class Kernel:
    """A sub-band's imaging kernel: for a visibility sample at bin *n*,
    pixel column ``n + d`` (d in [-M, M]) and Dec row *j* get
    ``Re(Σ_b W_b V_b conj(K[b, j, d]))`` with ``K = B² exp(iφ)``, and every
    row's exposure gets ``Σ_b W_b · E[d]`` with ``E = B⁴`` along the
    pointing row.

    That normalisation is a choice.  Dividing by ``Σ W B²(j, d)`` instead
    makes every Dec grating lobe of the two-row array as tall as the source
    (the beam taper cancels), and dividing by ``Σ W B⁴(j, d)`` amplifies
    them; dividing by the pointing row's own ``B⁴`` leaves the natural
    dirty-map taper, a source on the pointing row reads its flux, a lobe
    reads ``B²`` of its offset, and the result is still insensitive to how
    often a pixel was covered."""

    M: int
    Kc: np.ndarray       # conj(K): (nb, n_dec, 2M+1) complex64, stored conjugated for the scatter
    B2: np.ndarray       # (n_dec, 2M+1) float32
    E: np.ndarray        # (2M+1,) float32: B⁴ along the pointing row, the per-sample exposure


def build_kernel(grid: Grid, baselines_xy: np.ndarray, freq_mhz: float,
                 fwhm_deg: float, footprint_factor: float) -> Kernel:
    M = grid.footprint_bins(fwhm_deg, footprint_factor)
    d = np.arange(-M, M + 1)
    # pixel column p = n + d sees the sample at hour angle H = (n - p)·Δ = -d·Δ
    H_deg = -d * grid.bin_deg
    era_local = H_deg[None, :]                     # with ra = 0 the hour angle is era_local
    uE, uN, uU = enu(0.0, grid.dec_deg[:, None], era_local, grid.lat_deg)   # (n_dec, 2M+1)
    pvec = pointing_vector(grid.pointing_dec_deg, grid.lat_deg)
    cosang = uE * pvec[0] + uN * pvec[1] + uU * pvec[2]
    B2 = beam_sq(cosang, fwhm_deg).astype(np.float32)
    tau = source_delays(baselines_xy, uE, uN)                                # (nb, n_dec, 2M+1)
    Kc = (B2[None] * np.exp(-2j * np.pi * freq_mhz * 1e6 * tau)).astype(np.complex64)
    j0 = int(np.argmin(np.abs(grid.dec_deg - grid.pointing_dec_deg)))
    return Kernel(M=M, Kc=Kc, B2=B2, E=(B2[j0] ** 2).astype(np.float32))


def scatter(num: np.ndarray, exp: np.ndarray, kern: Kernel, n: int,
            V: np.ndarray, W: np.ndarray) -> None:
    """Add one sample (bin *n*, visibilities *V* and weights *W* per
    baseline) into the ``(n_dec, n_bins)`` accumulators *num* / *exp*."""
    WV = (W * V).astype(np.complex64)
    patch = np.einsum("b,bjd->jd", WV, kern.Kc).real
    epatch = np.broadcast_to(float(W.sum()) * kern.E, patch.shape)
    n_bins = num.shape[-1]
    cols = (n + np.arange(-kern.M, kern.M + 1)) % n_bins
    # the footprint is at most a few hundred bins, far below n_bins, so a
    # wrap splits into two contiguous runs at most
    if cols[0] <= cols[-1]:
        num[:, cols[0]:cols[-1] + 1] += patch
        exp[:, cols[0]:cols[-1] + 1] += epatch
    else:
        k = int(np.argmax(cols == 0))
        num[:, cols[0]:] += patch[:, :k]
        exp[:, cols[0]:] += epatch[:, :k]
        num[:, :cols[-1] + 1] += patch[:, k:]
        exp[:, :cols[-1] + 1] += epatch[:, k:]


# --- redundant baselines ---------------------------------------------------

@dataclass
class Baselines:
    """The unique baselines of one polarization's cross products.

    ``group[i]`` is the unique-baseline index of product *i* and
    ``conj[i]`` says the product measures the reversed vector, so its
    visibility is conjugated before averaging."""

    xy: np.ndarray        # (n_unique, 2) metres, canonical orientation
    group: np.ndarray     # (n_prod,) int
    conj: np.ndarray      # (n_prod,) bool

    @property
    def n_unique(self) -> int:
        return len(self.xy)


def group_baselines(bl_xy: np.ndarray, sep_x: float, sep_y: float) -> Baselines:
    """Group products by their baseline vector in grid units.

    A product whose vector points west, or due south, is mapped onto the
    opposite vector with a conjugation so each physical separation is
    counted once."""
    kx = np.rint(bl_xy[:, 0] / sep_x).astype(int)
    ky = np.rint(bl_xy[:, 1] / sep_y).astype(int)
    conj = (kx < 0) | ((kx == 0) & (ky < 0))
    kx = np.where(conj, -kx, kx)
    ky = np.where(conj, -ky, ky)
    keys = sorted(set(zip(kx.tolist(), ky.tolist())))
    index = {k: i for i, k in enumerate(keys)}
    group = np.array([index[(a, b)] for a, b in zip(kx.tolist(), ky.tolist())], dtype=int)
    xy = np.array([[a * sep_x, b * sep_y] for a, b in keys], dtype=float)
    return Baselines(xy=xy, group=group, conj=conj)


def average_redundant(V: np.ndarray, W: np.ndarray, bl: Baselines):
    """Weighted mean over redundant products.  *V*, *W* are
    ``(..., n_prod)``; returns ``(..., n_unique)`` arrays."""
    Vc = np.where(bl.conj, np.conj(V), V)
    shape = V.shape[:-1] + (bl.n_unique,)
    num = np.zeros(shape, dtype=np.complex64)
    den = np.zeros(shape, dtype=np.float32)
    for i in range(bl.n_unique):
        sel = bl.group == i
        den[..., i] = W[..., sel].sum(axis=-1)
        num[..., i] = (W[..., sel] * Vc[..., sel]).sum(axis=-1)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.where(den > 0, num / np.maximum(den, 1e-30), 0)
    return out.astype(np.complex64), den


# --- self-calibration on a transiting point source -------------------------

@dataclass
class CalAccumulator:
    """Running sums for the per-channel, per-product gain of a calibrator
    transit: ``g = Σ w B² V e^{-iφ} / Σ w B⁴`` once the window has passed.

    With that normalisation ``V / g`` is what a unit-flux source at the
    calibrator's position looks like through the model beam, which is
    exactly the imaging template, so the maps come out in units of the
    calibrator's flux density."""

    gsum: np.ndarray        # (n_freq, n_pol, n_prod) complex128
    gwt: np.ndarray         # (n_freq, n_pol, n_prod) float64
    n_samples: int = 0
    calibrator: str = ""
    first_time: float | None = None
    last_time: float | None = None

    @classmethod
    def empty(cls, n_freq: int, n_pol: int, n_prod: int, calibrator: str) -> "CalAccumulator":
        return cls(np.zeros((n_freq, n_pol, n_prod), np.complex128),
                   np.zeros((n_freq, n_pol, n_prod), np.float64), 0, calibrator)

    def add_block(self, sl: slice, V: np.ndarray, W: np.ndarray, freq_mhz: np.ndarray,
                  tau: np.ndarray, b2: np.ndarray, t_unix: np.ndarray) -> None:
        """A block of channels *sl* for several samples: *V*, *W* are
        ``(n_chan, n_pol, n_prod, n_time)``, *freq_mhz* ``(n_chan,)``,
        *tau* the calibrator's geometric delays ``(n_pol, n_prod, n_time)``
        and *b2* its model beam response per channel ``(n_chan, n_time)``
        (the beam scales with wavelength, so a single width across the
        band would leave a frequency-dependent bias in the amplitudes)."""
        model = np.exp(2j * np.pi * (freq_mhz[:, None, None, None] * 1e6) * tau[None])
        b2c = b2[:, None, None, :]
        self.gsum[sl] += (W * b2c * V * np.conj(model)).sum(axis=-1)
        self.gwt[sl] += (W * (b2c * b2c)).sum(axis=-1)
        if sl.start == 0 or sl.start is None:        # count samples once per file, not per block
            self.n_samples += int(len(t_unix))
            lo, hi = float(np.min(t_unix)), float(np.max(t_unix))
            self.first_time = lo if self.first_time is None else min(self.first_time, lo)
            self.last_time = hi if self.last_time is None else max(self.last_time, hi)

    def solve(self, dead_fraction: float = 0.05) -> np.ndarray:
        """Gains ``(n_freq, n_pol, n_prod)`` complex64; a product whose
        amplitude is below *dead_fraction* of its channel's median across
        products is a dead feed and gets gain 0 (= excluded)."""
        with np.errstate(invalid="ignore", divide="ignore"):
            g = np.where(self.gwt > 0, self.gsum / np.maximum(self.gwt, 1e-300), 0)
        amp = np.abs(g)
        med = np.median(np.where(amp > 0, amp, np.nan), axis=-1, keepdims=True)
        med = np.nan_to_num(med, nan=0.0)
        dead = amp < dead_fraction * med
        g = np.where(dead, 0, g)
        return g.astype(np.complex64)


def apply_gains(V: np.ndarray, W: np.ndarray, g: np.ndarray):
    """Divide out per-product gains; a zero gain zeroes the weight.
    Shapes broadcast: *g* is ``(n_freq, n_pol, n_prod)``, *V*/*W* add a
    trailing time axis."""
    gg = g[..., None]
    live = gg != 0
    with np.errstate(invalid="ignore", divide="ignore"):
        Vc = np.where(live, V / np.where(live, gg, 1), 0)
    Wc = np.where(live, W * np.abs(gg) ** 2, 0)
    return Vc.astype(np.complex64), Wc.astype(np.float32)


# --- the daily map: one accumulator pair per sidereal pass -----------------

@dataclass
class PassSlots:
    """Two accumulator sets, one per parity of the sidereal pass index.

    Each bin of the sky is visited once per pass, so "the last day" is
    the current pass where it has exposure and the previous pass
    elsewhere; a pass more than one behind is stale and dropped."""

    num: np.ndarray       # (2, n_pol, n_band, n_dec, n_bins)
    exp: np.ndarray
    idx: np.ndarray = field(default_factory=lambda: np.array([-1, -1]))   # pass index held per slot

    @classmethod
    def empty(cls, n_pol: int, n_band: int, n_dec: int, n_bins: int) -> "PassSlots":
        shape = (2, n_pol, n_band, n_dec, n_bins)
        return cls(np.zeros(shape, np.float32), np.zeros(shape, np.float32))

    def slot_for(self, p: int) -> int:
        """The slot for pass *p*, cleared if it held an older pass."""
        s = p % 2
        if self.idx[s] != p:
            self.num[s] = 0
            self.exp[s] = 0
            self.idx[s] = p
        return s

    def composite(self, current: int):
        """``(num, exp)`` of the last sidereal day as of pass *current*."""
        cur = self.idx == current
        prev = self.idx == current - 1
        num = np.zeros(self.num.shape[1:], np.float32)
        exp = np.zeros(self.exp.shape[1:], np.float32)
        if prev.any():
            s = int(np.argmax(prev))
            num[:] = self.num[s]
            exp[:] = self.exp[s]
        if cur.any():
            s = int(np.argmax(cur))
            fresh = self.exp[s] > 0
            num = np.where(fresh, self.num[s], num)
            exp = np.where(fresh, self.exp[s], exp)
        return num, exp
