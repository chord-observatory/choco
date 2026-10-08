"""Reading a synthetic N² file: axes, product selection, masks, averaging."""

import numpy as np
import pytest

import imaging as I
import reduce as R
from conftest import SRC_DEC, SRC_RA, file_name, write_vis_file


@pytest.fixture
def vis_file(tmp_path):
    t0 = 1_791_000_000.0
    p = tmp_path / "acq_x" / file_name(42, t0)
    write_vis_file(str(p), 42, t0, 100.0)
    return str(p)


def test_names_carry_index_and_time():
    assert R.index_from_name("vis_0004233671_20261004T_030643_744722230.h5") == 4233671
    assert R.time_from_name("vis_0004233671_20261004T_030643_744722230.h5") == pytest.approx(1791083203.7447, abs=1e-3)
    assert R.index_from_name("notes.txt") is None and R.time_from_name("x.h5") is None


def test_read_axes(vis_file):
    ax = R.read_axes(vis_file)
    assert ax.file_idx == 42 and ax.n_time == 20 and ax.n_freq == 128
    assert ax.labels[:2] == ["A01X", "A02X"] and ax.labels[5] == "A01Y"
    assert ax.lat_deg == pytest.approx(49.3208, abs=1e-3)
    assert np.all(np.diff(ax.t_unix) > 9.9) and np.all(np.diff(ax.era_deg) > 0)
    assert ax.feed_pos.shape == (10, 3)


def test_read_axes_rejects_a_foreign_name(tmp_path):
    p = tmp_path / "data.h5"
    import h5py
    with h5py.File(p, "w") as f:
        f.create_dataset("vis", data=np.zeros((1, 1, 1), np.complex64))
    with pytest.raises(R.Unusable):
        R.read_axes(str(p))


def test_pol_sets_skip_rfi_antennas_and_autos(vis_file):
    ax = R.read_axes(vis_file)
    pols = R.pol_sets(ax)
    assert [ps.name for ps in pols] == ["XX", "YY"]
    for ps in pols:
        assert len(ps.prod_idx) == 6                      # 4 dishes -> 6 cross products
        assert np.all(ax.types[ps.a] == 0) and np.all(ax.types[ps.b] == 0)
        assert np.all(ps.a != ps.b)
    assert np.allclose(pols[0].bl_xy, pols[1].bl_xy)      # same dish pairs in both pols


def test_channel_mask():
    f = np.array([300.0, 360.0, 400.0, 1000.0])
    assert R.channel_mask(f, [[350, 400]]).tolist() == [False, True, False, False]
    assert not R.channel_mask(f, []).any()


def test_reduced_shapes_and_masks(tmp_path):
    t0 = 1_791_000_000.0
    p = tmp_path / "acq_x" / file_name(7, t0)
    write_vis_file(str(p), 7, t0, 100.0, dead_elements=(1,), rfi_channels=(3,))
    ax = R.read_axes(str(p))
    pols = R.pol_sets(ax)
    red = R.read_reduced(ax, pols, 16, [[600, 650]], 0.05, None)
    assert red.V.shape == (8, 2, 6, 20) and red.W.shape == red.V.shape
    assert red.sub_freq_mhz[0] == pytest.approx(ax.freq_mhz[:16].mean())
    # the dead element kills every product it is in, nothing else
    dead = (pols[0].a == 1) | (pols[0].b == 1)
    bad_sub = [s for s in range(8) if 600 <= red.sub_freq_mhz[s] < 650]
    good_sub = [s for s in range(8) if s not in bad_sub]
    assert not red.W[:, 0, dead, :].any()
    assert red.W[good_sub][:, 0, ~dead, :].all()
    # a bad frequency range zeroes its sub-band; an RFI channel costs its
    # share of the first sub-band's weight (15 of 16 channels left)
    assert bad_sub and not red.W[bad_sub].any()
    assert red.W[0, 0, ~dead, 0].max() == pytest.approx(red.W[1, 0, ~dead, 0].max() * 15 / 16, rel=1e-3)
    assert red.frac_flagged > 0
    assert not red.calibrated


def test_sub_band_average_of_a_calibrated_source_is_coherent(tmp_path):
    """With the true gains divided out, averaging 16 channels keeps the
    source amplitude; without them the delays decorrelate it."""
    from conftest import LAT, LON, element_gains, element_table
    t0 = 1_791_000_000.0
    # a file straddling the transit, source at the pointing
    era0 = (SRC_RA - LON - 0.02 * I.ERA_DEG_PER_DAY / 24) % 360
    p = tmp_path / "acq_x" / file_name(9, t0)
    write_vis_file(str(p), 9, t0, era0, noise=1e-6)
    ax = R.read_axes(str(p))
    pols = R.pol_sets(ax)
    labels, types, pols_el, pos = element_table()
    amp, phi0, tau_c = element_gains(len(labels), ax.n_freq)
    g_el = amp[None] * np.exp(1j * (phi0[None] + 2 * np.pi * ax.freq_mhz[:, None] * 1e6 * tau_c[None]))
    g_prod = np.stack([g_el[:, ps.a] * np.conj(g_el[:, ps.b]) for ps in pols], axis=1)   # (f, pol, prod)
    raw = R.read_reduced(ax, pols, 16, [], None, None)
    cal = R.read_reduced(ax, pols, 16, [], None, g_prod.astype(np.complex64))
    assert cal.calibrated
    # the source sits at the pointing: |V| ~ flux * B^2 ~ 1 near transit
    assert np.median(np.abs(cal.V[:, 0, :, 10])) == pytest.approx(1.0, abs=0.1)
    assert np.median(np.abs(raw.V[:, 0, :, 10])) < 0.9 * np.median(np.abs(cal.V[:, 0, :, 10]))
