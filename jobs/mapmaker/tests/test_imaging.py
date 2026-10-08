"""Geometry, kernels, scatter, redundancy, self-cal and pass bookkeeping."""

import math

import numpy as np
import pytest

import imaging as I
from conftest import LAT, LON


class TestSidereal:
    def test_era_local_wraps(self):
        assert I.era_local_deg(359.0, 2.0) == pytest.approx(1.0)
        assert I.era_local_deg(59.49, LON) == pytest.approx(299.87, abs=0.01)

    def test_ra_bin_covers_the_circle(self):
        bins = I.ra_bin(np.array([0.0, 359.999, 180.0]), 4096)
        assert bins.tolist() == [0, 4095, 2048]

    def test_pass_index_increments_once_per_sidereal_day(self):
        t = 1_791_000_000.0
        p0 = I.pass_index(t, LON)
        assert I.pass_index(t + I.SIDEREAL_DAY_S, LON) == p0 + 1
        assert I.pass_index(t + 0.5 * I.SIDEREAL_DAY_S, LON) in (p0, p0 + 1)

    def test_pass_boundary_is_local_sidereal_zero(self):
        # scan a day for the second where the pass index steps; the local
        # ERA computed from the same formula must wrap there
        t = 1_791_000_000.0 + np.arange(0, 90000, 60.0)
        p = I.pass_index(t, LON)
        step = int(np.argmax(np.diff(p) != 0))
        turns = (0.7790572732640 + LON / 360.0
                 + 1.00273781191135448 * (t[step + 1] - I.J2000_UNIX) / 86400.0)
        assert (turns % 1.0) * 360 < 0.3     # within one minute of the wrap


class TestDirections:
    def test_transit_is_on_the_meridian(self):
        uE, uN, uU = I.enu(120.0, 40.8, 120.0, LAT)
        assert uE == pytest.approx(0.0, abs=1e-12)
        assert uU == pytest.approx(math.cos(math.radians(LAT - 40.8)))
        assert uN == pytest.approx(math.sin(math.radians(40.8 - LAT)))   # south of zenith

    def test_source_east_of_meridian_has_positive_uE(self):
        uE, _, _ = I.enu(120.0, 40.8, 110.0, LAT)      # hour angle -10 deg: rising, east
        assert uE > 0

    def test_pointing_vector_matches_enu_at_transit(self):
        p = I.pointing_vector(40.8, LAT)
        uE, uN, uU = I.enu(0.0, 40.8, 0.0, LAT)
        assert np.allclose(p, [uE, uN, uU])

    def test_beam_fwhm_is_airy(self):
        assert float(I.beam_fwhm_deg(1000.0, 6.0)) == pytest.approx(2.95, abs=0.01)

    def test_beam_sq_peaks_on_axis(self):
        assert I.beam_sq(1.0, 3.0) == pytest.approx(1.0)
        assert I.beam_sq(math.cos(math.radians(1.5)), 3.0) == pytest.approx(0.5, abs=1e-3)

    def test_source_delay_sign(self):
        # a source due east (uE > 0) arrives first at the eastern dish, so a
        # baseline pointing east has a negative delay in this convention
        tau = I.source_delays(np.array([[6.3, 0.0]]), np.array([0.5]), np.array([0.0]))
        assert tau[0, 0] == pytest.approx(-6.3 * 0.5 / I.C_M_PER_S)


class TestKernel:
    def grid(self):
        return I.Grid.build(1024, 40.8, 6.0, 0.5, LAT, LON)

    def test_grid_is_centred_on_the_pointing(self):
        g = self.grid()
        assert g.n_dec == 25
        assert g.dec_deg[g.n_dec // 2] == pytest.approx(40.8)
        assert g.bin_deg == pytest.approx(360 / 1024)

    def test_footprint_grows_with_the_beam(self):
        g = self.grid()
        assert g.footprint_bins(10.0, 1.2) > g.footprint_bins(2.0, 1.2) >= 1

    def test_kernel_peaks_at_the_pointing_pixel(self):
        g = self.grid()
        k = I.build_kernel(g, np.array([[6.3, 0.0], [0.0, 8.5]]), 600.0, 5.0, 1.2)
        assert k.Kc.shape == (2, g.n_dec, 2 * k.M + 1)
        j, d = np.unravel_index(np.argmax(k.B2), k.B2.shape)
        assert (j, d) == (g.n_dec // 2, k.M)
        assert k.B2[j, d] == pytest.approx(1.0)

    def test_scatter_puts_a_point_source_where_it_is(self):
        """A visibility set made from a source at (dec row j0, bin n0), fed
        through the kernel at the sample's bin, peaks at (j0, n0)."""
        g = self.grid()
        bl = np.array([[6.3, 0.0], [12.6, 0.0], [0.0, 8.5], [6.3, 8.5]])
        f = 600.0
        k = I.build_kernel(g, bl, f, 5.0, 1.2)
        num = np.zeros((g.n_dec, g.n_bins), np.float32)
        exp = np.zeros_like(num)
        j0, n0 = g.n_dec // 2 + 2, 500
        for n in range(n0 - k.M, n0 + k.M + 1):
            # the source's direction as seen from bin n: hour angle (n - n0) bins
            era_local = n * g.bin_deg
            uE, uN, uU = I.enu(n0 * g.bin_deg, g.dec_deg[j0], era_local, LAT)
            pvec = I.pointing_vector(40.8, LAT)
            b2 = I.beam_sq(uE * pvec[0] + uN * pvec[1] + uU * pvec[2], 5.0)
            V = b2 * np.exp(2j * np.pi * f * 1e6 * I.source_delays(bl, uE, uN))
            I.scatter(num, exp, k, n, V.astype(np.complex64), np.ones(len(bl), np.float32))
        m = np.where(exp > 0.02 * exp.max(), num / np.maximum(exp, 1e-30), -np.inf)
        j, n = np.unravel_index(np.argmax(m), m.shape)
        assert n == n0
        assert abs(j - j0) <= 1
        # the Dec grating lobes of a two-row array are tapered by the beam,
        # not flattened: the first lobe reads well below the source
        lobe = int(round(np.rad2deg(0.4997 / 8.5) / 0.5))          # λ/8.5 m at 600 MHz in rows
        assert m[j0 + lobe, n0] < 0.6 * m[j0, n0] and m[j0 - lobe, n0] < 0.6 * m[j0, n0]

    def test_scatter_wraps_around_the_ra_seam(self):
        g = self.grid()
        k = I.build_kernel(g, np.array([[6.3, 0.0]]), 600.0, 5.0, 1.2)
        num = np.zeros((g.n_dec, g.n_bins), np.float32)
        exp = np.zeros_like(num)
        I.scatter(num, exp, k, 2, np.ones(1, np.complex64), np.ones(1, np.float32))
        assert exp[:, 0].any() and exp[:, -1].any()       # both sides of the seam
        assert exp.sum() == pytest.approx(k.E.sum() * g.n_dec, rel=1e-5)


class TestRedundancy:
    def test_grouping_is_by_vector_with_canonical_sign(self):
        bl = np.array([[6.3, 0], [-6.3, 0], [0, 8.5], [0, -8.5], [6.3, 8.5], [-6.3, -8.5], [12.6, 0]])
        b = I.group_baselines(bl, 6.3, 8.5)
        assert b.n_unique == 4
        assert b.group[0] == b.group[1] and b.conj[1] and not b.conj[0]
        assert b.group[2] == b.group[3] and b.conj[3]
        assert b.group[4] == b.group[5] and b.conj[5]
        assert np.all(b.xy[:, 0] >= 0)

    def test_average_conjugates_reversed_products(self):
        bl = np.array([[6.3, 0], [-6.3, 0]])
        b = I.group_baselines(bl, 6.3, 8.5)
        V = np.array([[1 + 1j, 1 - 1j]], np.complex64)
        W = np.array([[1.0, 3.0]], np.float32)
        out, den = I.average_redundant(V, W, b)
        assert out.shape == (1, 1) and den[0, 0] == 4.0
        assert out[0, 0] == pytest.approx(1 + 1j)

    def test_zero_weight_products_do_not_count(self):
        bl = np.array([[6.3, 0], [6.3, 0]])
        b = I.group_baselines(bl, 6.3, 8.5)
        out, den = I.average_redundant(np.array([[5.0, 99.0]], np.complex64),
                                       np.array([[2.0, 0.0]], np.float32), b)
        assert out[0, 0] == pytest.approx(5.0) and den[0, 0] == 2.0


class TestSelfCal:
    def test_recovers_product_gains_and_flags_dead_ones(self):
        rng = np.random.default_rng(1)
        n_freq, n_pol, n_prod, n_time = 8, 1, 6, 30
        freq = np.linspace(400, 500, n_freq)
        tau = rng.uniform(-50e-9, 50e-9, (n_pol, n_prod, n_time))
        b2 = np.linspace(0.3, 1.0, n_time)[None, :] ** np.linspace(1.0, 1.5, n_freq)[:, None]   # (f, t)
        g_true = rng.uniform(0.5, 2, (n_freq, n_pol, n_prod)) * np.exp(1j * rng.uniform(-3, 3, (n_freq, n_pol, n_prod)))
        g_true[:, :, 5] = 0                                                   # a dead product
        model = np.exp(2j * np.pi * (freq[:, None, None, None] * 1e6) * tau[None])
        V = g_true[..., None] * b2[:, None, None, :] * model
        W = np.ones_like(V, dtype=float)
        acc = I.CalAccumulator.empty(n_freq, n_pol, n_prod, "t")
        acc.add_block(slice(0, 4), V[:4], W[:4], freq[:4], tau, b2[:4], np.arange(n_time) * 10.0)
        acc.add_block(slice(4, 8), V[4:], W[4:], freq[4:], tau, b2[4:], np.arange(n_time) * 10.0)
        assert acc.n_samples == n_time
        g = acc.solve(0.05)
        assert np.allclose(g[:, :, :5], g_true[:, :, :5], atol=1e-5)
        assert np.all(g[:, :, 5] == 0)

    def test_apply_gains_divides_and_zeroes_dead(self):
        g = np.array([[[2.0, 0.0]]], np.complex64)
        V = np.array([[[[4.0, 4.0], [7.0, 7.0]]]], np.complex64)        # (f, pol, prod, t)
        W = np.ones_like(V, dtype=np.float32)
        Vc, Wc = I.apply_gains(V, W, g)
        assert np.allclose(Vc[0, 0, 0], 2.0) and np.all(Vc[0, 0, 1] == 0)
        assert np.allclose(Wc[0, 0, 0], 4.0) and np.all(Wc[0, 0, 1] == 0)


class TestPassSlots:
    def test_slot_for_clears_an_older_pass(self):
        s = I.PassSlots.empty(1, 1, 2, 8)
        a = s.slot_for(10)
        s.num[a] = 1
        assert s.slot_for(10) == a and s.num[a].all()
        b = s.slot_for(11)
        assert b != a
        assert s.slot_for(12) == a and not s.num[a].any()                # overwritten slot cleared

    def test_composite_prefers_the_current_pass_where_exposed(self):
        s = I.PassSlots.empty(1, 1, 1, 4)
        p = s.slot_for(5)
        s.num[p, ..., :] = [1, 1, 1, 1]
        s.exp[p, ..., :] = [1, 1, 1, 1]
        c = s.slot_for(6)
        s.num[c, ..., :2] = [5, 5]
        s.exp[c, ..., :2] = [1, 1]
        num, exp = s.composite(6)
        assert num[0, 0, 0].tolist() == [5, 5, 1, 1]
        assert exp[0, 0, 0].tolist() == [1, 1, 1, 1]

    def test_composite_drops_a_pass_two_behind(self):
        s = I.PassSlots.empty(1, 1, 1, 4)
        p = s.slot_for(5)
        s.exp[p] = 1
        s.slot_for(7)                                   # clears slot 7 % 2 == 1 == 5 % 2
        num, exp = s.composite(7)
        assert not exp.any()
