"""transit_fit — the point-source transit fit, as pure numpy.

The science core of eigencal, kept free of I/O and astropy so it is
testable on numpy alone.  ``fit_transits`` is a batched (vectorised)
re-implementation of CHIME's ``ch_cal.utils.fit_point_source_transit``:
an iteratively reweighted linear least-squares fit of polynomials to the
log-amplitude and phase of the complex point-source response versus hour
angle, solved for all (frequency, input) series at once with stacked
normal equations instead of a Python loop per series.

Differences from the ch_cal original, all deliberate:

* one solve per iteration for the whole batch (``np.linalg.solve`` /
  ``inv`` on stacked (p, p) systems) instead of per-series ``lstsq``;
* the per-series amplitude peak is located by evaluating the fitted
  polynomial on a fine hour-angle grid and taking the argmax, instead of
  root-finding on the derivative (identical to grid resolution);
* the Student-t quantile for the error bars uses a series expansion
  around the normal quantile (choco's venv has no scipy); at the ndof of
  a real transit (~100+) the difference is negligible.

As in the original, parameter covariances are rescaled by chi^2/ndof
(``absolute_sigma=False``), so only *relative* input errors matter.
"""

from __future__ import annotations

import numpy as np


def invert_no_zero(x):
    """1/x with 0 -> 0 (ch_util.tools convention)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(x != 0, 1.0 / x, 0.0)


# -- Student-t quantile without scipy --------------------------------------


def norm_ppf(p):
    """Inverse normal CDF (Acklam's rational approximation, |err| < 1.2e-9)."""
    p = np.asarray(p, dtype=float)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00]
    plow = 0.02425

    out = np.empty_like(p)
    lo, hi = p < plow, p > 1 - plow
    mid = ~(lo | hi)

    q = np.sqrt(-2 * np.log(np.where(lo, p, 0.5)))
    out[lo] = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])[lo] / \
              ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)[lo]
    q = np.sqrt(-2 * np.log(np.where(hi, 1 - p, 0.5)))
    out[hi] = -((((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])[hi] /
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)[hi])
    q = np.where(mid, p, 0.5) - 0.5
    r = q * q
    out[mid] = ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q)[mid] / \
               (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)[mid]
    return out


def t_ppf(p, dof):
    """Student-t quantile via the Cornish-Fisher expansion in 1/dof.

    Good to <1% for dof >= 10; transit fits have dof ~ 100+.
    """
    z = norm_ppf(p)
    dof = np.maximum(np.asarray(dof, dtype=float), 1.0)
    g1 = (z ** 3 + z) / 4.0
    g2 = (5 * z ** 5 + 16 * z ** 3 + 3 * z) / 96.0
    return z + g1 / dof + g2 / dof ** 2


# -- geometry ---------------------------------------------------------------


def fringestop_phase(ha, lat, dec, u, v, sign=1.0):
    """exp(sign * 2*pi*i * n_hat . b) with baseline b = (u, v) in wavelengths.

    Same geometry as ch_util.tools.fringestop_phase (E and N components of
    the source unit vector at hour angle ``ha``).  VERIFY ``sign`` against
    the CHORD correlator's conjugation convention: with the wrong sign the
    fitted phase winds rapidly with hour angle instead of sitting flat.

    ha : (nt,) radians;  u, v : (nf, nfeed)  ->  (nt, nf, nfeed) complex
    """
    e_comp = -np.cos(dec) * np.sin(ha)
    n_comp = np.cos(lat) * np.sin(dec) - np.sin(lat) * np.cos(dec) * np.cos(ha)
    phase = e_comp[:, None, None] * u[None] + n_comp[:, None, None] * v[None]
    return np.exp(sign * 2.0j * np.pi * phase)


# -- the batched fit --------------------------------------------------------


def fit_transits(ha, resp, resp_err, flag, ha_eval,
                 poly_deg_amp=5, poly_deg_phi=5, niter=3, alpha=0.32,
                 window=None):
    """Fit polynomials to log-amplitude and phase vs hour angle, batched.

    Parameters
    ----------
    ha : (nt,) float
        Hour angle of each sample, radians (offset so the beam peak ~ 0).
    resp, resp_err : (nt, ns) complex / float
        Point-source response and its 1-sigma error for each series.
    flag : (nt, ns) bool
        Samples eligible for the fit.
    ha_eval : sequence of float or None/nan
        Hour angles at which to evaluate the model gain; None/nan means
        "at this series' fitted amplitude peak".
    window : None, scalar or (ns,) float
        Half-width (radians) of the moving fit window around the fitted
        peak; None fits every flagged sample.

    Returns
    -------
    dict with ``gain`` (neval, ns) model response, ``frac_err`` (neval, ns)
    complex fractional errors (real=amplitude, imag=phase), ``valid`` (ns,),
    ``chisq_per_dof`` (2, ns), ``ndof`` (2, ns), ``peak_ha`` (ns,),
    ``coeff`` (ns, pa+pp).
    """
    poly = np.polynomial.polynomial
    nt, ns = resp.shape
    pa, pp = poly_deg_amp + 1, poly_deg_phi + 1
    min_nfit = pa + pp + 1

    amp = np.abs(resp)
    good = flag & (amp > 0) & (resp_err > 0) & np.isfinite(resp)
    w0 = np.where(good, invert_no_zero(resp_err) ** 2, 0.0)
    ya = np.log(np.where(amp > 0, amp, 1.0))

    # Phase: reference to the transit sample, then unwrap by one turn
    # relative to it (same scheme as the ch_cal original).
    itrans = int(np.argmin(np.abs(ha)))
    phi = np.angle(resp)
    yp = phi - phi[itrans][None, :]
    yp -= 2 * np.pi * (yp > np.pi)
    yp += 2 * np.pi * (yp < -np.pi)
    yp += phi[itrans][None, :]

    Aa = poly.polyvander(ha, poly_deg_amp)            # (nt, pa)
    Ap = poly.polyvander(ha, poly_deg_phi)
    ha_grid = np.linspace(ha.min(), ha.max(), 512)
    Ag = poly.polyvander(ha_grid, poly_deg_amp)       # (ngrid, pa)

    if window is not None:
        window = np.broadcast_to(np.asarray(window, dtype=float), (ns,))

    def wsolve(A, w, y):
        # Batched weighted normal equations: one (p, p) system per series.
        C = np.einsum("ti,ts,tj->sij", A, w, A, optimize=True)
        b = np.einsum("ti,ts->si", A, w * y, optimize=True)
        p = A.shape[1]
        ridge = 1e-10 * np.einsum("sii->s", C) / p + 1e-300
        return C + ridge[:, None, None] * np.eye(p), b

    valid = (2 * good.sum(axis=0)) >= min_nfit
    model_amp = amp.copy()
    centre = np.zeros(ns)
    ca = np.zeros((ns, pa))

    for k in range(niter):
        wk = w0 * model_amp ** 2
        if window is not None:
            if k > 0:
                centre = ha_grid[np.argmax(Ag @ ca.T, axis=0)]
            wk = wk * (np.abs(ha[:, None] - centre[None, :]) <= window[None, :])
        valid &= (2 * (wk > 0).sum(axis=0)) >= min_nfit
        C, b = wsolve(Aa, wk, ya)
        ca = np.linalg.solve(C, b[..., None])[..., 0]
        model_amp = np.exp(Aa @ ca.T)

    wf = w0 * model_amp ** 2
    if window is not None:
        centre = ha_grid[np.argmax(Ag @ ca.T, axis=0)]
        wf = wf * (np.abs(ha[:, None] - centre[None, :]) <= window[None, :])
    ndata = (wf > 0).sum(axis=0)
    valid &= (2 * ndata) >= min_nfit

    C, b = wsolve(Aa, wf, ya)
    cova = np.linalg.inv(C)
    ca = np.einsum("sij,sj->si", cova, b)
    Cp, bp = wsolve(Ap, wf, yp)
    covp = np.linalg.inv(Cp)
    cp = np.einsum("sij,sj->si", covp, bp)

    ndofa = np.maximum(ndata - pa, 1)
    ndofp = np.maximum(ndata - pp, 1)
    chisqa = np.sum(wf * (ya - Aa @ ca.T) ** 2, axis=0)
    chisqp = np.sum(wf * (yp - Ap @ cp.T) ** 2, axis=0)

    # Rescale covariance by chi^2/dof (absolute_sigma=False in ch_cal).
    cova = cova * (chisqa * invert_no_zero(ndofa.astype(float)))[:, None, None]
    covp = covp * (chisqp * invert_no_zero(ndofp.astype(float)))[:, None, None]

    prob = 1.0 - alpha / 2.0
    tva = t_ppf(prob, ndofa)
    tvp = t_ppf(prob, ndofp)

    neval = len(ha_eval)
    gain = np.zeros((neval, ns), dtype=np.complex64)
    frac_err = np.zeros((neval, ns), dtype=np.complex64)
    for j, x in enumerate(ha_eval):
        xe = centre if (x is None or np.isnan(x)) else np.full(ns, float(x))
        Va = xe[:, None] ** np.arange(pa)
        Vp = xe[:, None] ** np.arange(pp)
        la = np.einsum("sk,sk->s", Va, ca)
        lp = np.einsum("sk,sk->s", Vp, cp)
        gain[j] = np.exp(la + 1.0j * lp)
        ea = tva * np.sqrt(np.abs(np.einsum("sk,skl,sl->s", Va, cova, Va)))
        ep = tvp * np.sqrt(np.abs(np.einsum("sk,skl,sl->s", Vp, covp, Vp)))
        frac_err[j] = ea + 1.0j * ep

    valid &= np.isfinite(gain).all(axis=0) & np.isfinite(frac_err).all(axis=0)

    return {
        "gain": gain, "frac_err": frac_err, "valid": valid,
        "chisq_per_dof": np.stack([chisqa * invert_no_zero(ndofa.astype(float)),
                                   chisqp * invert_no_zero(ndofp.astype(float))]),
        "ndof": np.stack([ndofa, ndofp]), "peak_ha": centre,
        "coeff": np.concatenate([ca, cp], axis=1),
    }


# -- frequency gap fill -----------------------------------------------------


def interpolate_gaps(gain, weight, max_gap):
    """Linear complex interpolation over short flagged gaps in frequency.

    A channel flagged only *during the transit* (e.g. transient RFI) still
    needs a usable gain for the rest of the day; only the calibration side
    can fill it.  CHIME used a Gaussian-process interpolation
    (ch_util.cal_utils.interpolate_gain); linear interpolation over gaps of
    at most ``max_gap`` channels is the dependency-free stand-in.  Filled
    channels get half the weight of their weaker neighbour.  Modifies and
    returns ``gain``/``weight`` (nfreq, ninput) in place.
    """
    nfreq, ninput = gain.shape
    for ii in range(ninput):
        gi = np.flatnonzero(weight[:, ii] > 0)
        if gi.size < 2:
            continue
        bad = np.flatnonzero(weight[:, ii] <= 0)
        bad = bad[(bad > gi[0]) & (bad < gi[-1])]      # interior gaps only
        if bad.size == 0:
            continue
        right = np.searchsorted(gi, bad)               # bounding good channels
        gap_len = gi[right] - gi[right - 1] - 1
        fill = bad[gap_len <= max_gap]
        if fill.size == 0:
            continue
        gain[fill, ii] = (np.interp(fill, gi, gain[gi, ii].real)
                          + 1.0j * np.interp(fill, gi, gain[gi, ii].imag))
        r = np.searchsorted(gi, fill)
        weight[fill, ii] = 0.5 * np.minimum(weight[gi[r - 1], ii],
                                            weight[gi[r], ii])
    return gain, weight
