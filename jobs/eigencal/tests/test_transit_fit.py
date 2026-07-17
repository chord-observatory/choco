"""Tests for the batched transit fit (pure numpy, no telescope I/O)."""

import numpy as np
import pytest

from transit_fit import (fit_transits, interpolate_gaps, invert_no_zero,
                         norm_ppf, t_ppf)


def test_norm_ppf_known_values():
    assert norm_ppf(0.5) == pytest.approx(0.0, abs=1e-8)
    assert norm_ppf(0.975) == pytest.approx(1.959964, abs=1e-5)
    assert norm_ppf(0.025) == pytest.approx(-1.959964, abs=1e-5)
    assert norm_ppf(0.84) == pytest.approx(0.994458, abs=1e-5)


def test_t_ppf_limits():
    # Converges to the normal quantile for large dof, grows as dof shrinks.
    assert t_ppf(0.84, 1e6) == pytest.approx(norm_ppf(0.84), abs=1e-4)
    q = t_ppf(0.84, np.array([10.0, 30.0, 100.0, 1e6]))
    assert np.all(np.diff(q) < 0) and q[0] > norm_ppf(0.84)


def _synthetic_transits(rng, nt=150, ns=400, sigma_beam=0.025, noise=0.01):
    ha = np.linspace(-0.06, 0.06, nt)
    amp0 = rng.uniform(0.5, 2.0, ns)
    phi0 = rng.uniform(-np.pi, np.pi, ns)
    slope = rng.uniform(-8, 8, ns)
    peak = rng.uniform(-0.004, 0.004, ns)
    model = (amp0[None] * np.exp(-0.5 * ((ha[:, None] - peak[None]) / sigma_beam) ** 2)
             * np.exp(1j * (phi0[None] + slope[None] * ha[:, None])))
    resp = model + noise * (rng.standard_normal((nt, ns))
                            + 1j * rng.standard_normal((nt, ns)))
    err = np.full((nt, ns), noise)
    truth = amp0 * np.exp(-0.5 * (peak / sigma_beam) ** 2) * np.exp(1j * phi0)
    return ha, resp, err, truth, sigma_beam


def test_fit_recovers_gains():
    rng = np.random.default_rng(42)
    ha, resp, err, truth, sigma_beam = _synthetic_transits(rng)
    nt, ns = resp.shape
    flag = rng.random((nt, ns)) >= 0.05          # 5% randomly masked

    fit = fit_transits(ha, resp, err, flag, ha_eval=[0.0],
                       window=np.full(ns, sigma_beam))
    ok = fit["valid"]
    assert ok.mean() > 0.99

    g = fit["gain"][0][ok]
    t = truth[ok]
    amp_err = np.abs(np.abs(g) - np.abs(t)) / np.abs(t)
    ph_err = np.abs(np.angle(g * np.conj(t)))
    assert np.median(amp_err) < 0.005
    assert np.percentile(amp_err, 95) < 0.02
    assert np.median(ph_err) < 0.005
    # chi^2/dof consistent with the injected noise
    assert 0.7 < np.median(fit["chisq_per_dof"][0][ok]) < 1.5
    assert 0.7 < np.median(fit["chisq_per_dof"][1][ok]) < 1.5


def test_fit_invalidates_underdetermined_series():
    rng = np.random.default_rng(1)
    ha, resp, err, _, sigma_beam = _synthetic_transits(rng, ns=50)
    nt, ns = resp.shape
    flag = np.ones((nt, ns), dtype=bool)
    flag[:, :5] = False
    flag[:3, :5] = True                          # 3 samples: not enough for 12 params
    fit = fit_transits(ha, resp, err, flag, ha_eval=[0.0],
                       window=np.full(ns, sigma_beam))
    assert not fit["valid"][:5].any()
    assert fit["valid"][5:].all()


def test_fit_evaluates_at_peak():
    rng = np.random.default_rng(7)
    ha, resp, err, _, sigma_beam = _synthetic_transits(rng, ns=100)
    flag = np.ones(resp.shape, dtype=bool)
    fit = fit_transits(ha, resp, err, flag, ha_eval=[np.nan],
                       window=np.full(resp.shape[1], sigma_beam))
    # Peak location recovered well inside the injected +/-0.004 rad offsets.
    assert np.median(np.abs(fit["peak_ha"][fit["valid"]])) < 0.005


def test_interpolate_gaps():
    nfreq = 20
    gain = np.tile((np.arange(nfreq) + 1.0 + 0.5j), (2, 1)).T.astype(complex)
    weight = np.ones((nfreq, 2), dtype=np.float32)
    weight[8:10, 0] = 0.0                        # short interior gap: filled
    weight[5:15, 1] = 0.0                        # long gap: left alone
    g = gain.copy()
    g[weight == 0] = 0.0
    g, w = interpolate_gaps(g, weight.copy(), max_gap=3)
    assert np.allclose(g[8:10, 0], gain[8:10, 0])          # linear ramp restored
    assert np.all(w[8:10, 0] == 0.5)                        # half neighbour weight
    assert np.all(w[5:15, 1] == 0.0) and np.all(g[5:15, 1] == 0.0)
    # Edge gaps are never extrapolated.
    weight2 = np.ones((nfreq, 1), dtype=np.float32)
    weight2[:3, 0] = 0.0
    g2 = np.ones((nfreq, 1), dtype=complex)
    g2[:3] = 0.0
    g2, w2 = interpolate_gaps(g2, weight2, max_gap=5)
    assert np.all(w2[:3, 0] == 0.0)


def test_invert_no_zero():
    x = np.array([0.0, 2.0, -4.0])
    assert np.allclose(invert_no_zero(x), [0.0, 0.5, -0.25])
