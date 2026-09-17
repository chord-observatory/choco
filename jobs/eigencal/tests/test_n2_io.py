"""Tests for the N² reader against tiny synthetic files in both layouts."""

import h5py
import numpy as np
import pytest

import n2_io

NFEED, NFREQ, NTIME = 4, 3, 6
# kotekan's per-element label table (chord.2021.10+988): 2 dishes x 2 pol
# = NFEED elements in [P][D] order, dish label + p1/p2, read back as
# d0X, d1X, d0Y, d1Y.
FILE_LABELS = [b"d0p1", b"d1p1", b"d0p2", b"d1p2"]
FILE_POL = [0, 0, 1, 1]
ELEMENT_LABELS = ["d0X", "d1X", "d0Y", "d1Y"]
# Per-element geometry as hdf5N2Write records it: DishType per element
# (dish 1 is a placeholder), grid-frame positions (dish pitch 6.3 m along
# grid x), and the grid frame rotated GRID_ROT_DEG east of north about Up:
# v_grid = R . v_topo, i.e. R's rows are the grid axes in the E/N/U basis.
FILE_TYPE = [0, -1, 0, -1]
FILE_POS = [[0.0, 0.0, 0.0], [6.3, 0.0, 0.0], [0.0, 0.0, 0.0], [6.3, 0.0, 0.0]]
GRID_ROT_DEG = 30.0
_c, _s = np.cos(np.radians(GRID_ROT_DEG)), np.sin(np.radians(GRID_ROT_DEG))
GRID_ORIENTATION = [[_c, _s, 0.0], [-_s, _c, 0.0], [0.0, 0.0, 1.0]]
# Pre-2026-08 per-element labels, used only by the refusal tests.
OLD_LABELS = [b"d0_pA", b"d0_pB", b"d1_pA", b"d1_pB"]


def _prods(n):
    a, b = np.triu_indices(n)
    return a.astype(np.uint16), b.astype(np.uint16)


def _vis_values(a, b):
    """Deterministic vis: encodes (freq, prod, time) so reads can be checked."""
    nprod = a.size
    f, p, t = np.meshgrid(np.arange(NFREQ), np.arange(nprod),
                          np.arange(NTIME), indexing="ij")
    return (f * 100 + p * 10 + t + 1j * p).astype(np.complex64)


@pytest.fixture
def chord_file(tmp_path):
    """CHORD hdf5N2Write flavour: per-element labels, vis[freq, prod, time]."""
    a, b = _prods(NFEED)
    path = tmp_path / "chord.h5"
    with h5py.File(path, "w") as f:
        f.attrs["num_elements"] = NFEED
        f.attrs["feed_positions_m"] = np.array(FILE_POS)
        f.attrs["grid_orientation"] = np.array(GRID_ORIENTATION)
        im = f.create_group("index_map")
        im.create_dataset("label", data=np.array(FILE_LABELS, dtype="S10"))
        im.create_dataset("pol", data=np.array(FILE_POL, dtype=np.int32))
        im.create_dataset("type", data=np.array(FILE_TYPE, dtype=np.int32))
        freq = np.zeros(NFREQ, dtype=[("centre", "<f8"), ("width", "<f8")])
        freq["centre"] = [400.0, 500.0, 600.0]
        freq["width"] = 100.0
        im.create_dataset("freq", data=freq)
        t = np.zeros(NTIME, dtype=[("fpga_count", "<u8"), ("ctime", "<f8")])
        t["ctime"] = 1000.0 + 10.0 * np.arange(NTIME)
        im.create_dataset("time", data=t)
        im.create_dataset("prod", data=np.stack([a, b], axis=-1))
        f.create_dataset("vis", data=_vis_values(a, b))
        fa = np.ones((NFREQ, NTIME), dtype=np.int64)
        fa[1, 2] = 0                              # one dead (freq, time) cell
        f.create_dataset("frames_added", data=fa)
        flags = np.ones(NFEED, dtype=np.float32)
        flags[3] = 0.0                            # kotekan flagged feed 3
        f.create_dataset("flags", data=flags)
    return path


@pytest.fixture
def chime_file(tmp_path):
    """Pre-2026-08 CHIME flavour (index_map/input) — refused by the reader."""
    a, b = _prods(NFEED)
    path = tmp_path / "chime.h5"
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        inp = np.zeros(NFEED, dtype=[("chan_id", "<u2"), ("correlator_input", "S10")])
        inp["chan_id"] = np.arange(NFEED)
        inp["correlator_input"] = OLD_LABELS
        im.create_dataset("input", data=inp)
        im.create_dataset("freq", data=np.array([400.0, 500.0, 600.0]))
        im.create_dataset("time", data=1000.0 + 10.0 * np.arange(NTIME))
        prod = np.zeros(a.size, dtype=[("input_a", "<u2"), ("input_b", "<u2")])
        prod["input_a"], prod["input_b"] = a, b
        im.create_dataset("prod", data=prod)
        f.create_dataset("vis", data=_vis_values(a, b).transpose(2, 0, 1))
    return path


def test_read_meta_chord(chord_file):
    m = n2_io.read_meta(chord_file)
    assert not m.time_first
    assert list(m.labels) == ELEMENT_LABELS
    assert np.allclose(m.freq_mhz, [400.0, 500.0, 600.0])
    assert np.allclose(m.freq_width_mhz, 100.0)
    assert np.allclose(m.time, 1005.0 + 10.0 * np.arange(NTIME))  # centred
    assert m.prod_a.size == NFEED * (NFEED + 1) // 2


@pytest.fixture
def live_chord_file(tmp_path, chord_file):
    """The layout kotekan actually writes (checked 2026-09-13): no
    index_map/time; root-level time_center_t_inst_ns holds int64 unix ns
    at the integration *centre*."""
    with h5py.File(chord_file, "r+") as f:
        del f["index_map"]["time"]
        centres_ns = (1_789_279_147_885_186_870
                      + np.arange(NTIME, dtype=np.int64) * 9_982_443_520)
        f.create_dataset("time_center_t_inst_ns", data=centres_ns)
    return chord_file


def test_read_meta_live_chord_time_axis(live_chord_file):
    """time_center_t_inst_ns is taken as-is (no half-bin shift) and keeps
    its sub-second part to well under a microsecond."""
    m = n2_io.read_meta(live_chord_file)
    expect = 1_789_279_147.885186870 + 9.982443520 * np.arange(NTIME)
    assert m.time.shape == (NTIME,)
    assert np.all(np.abs(m.time - expect) < 1e-6)
    assert abs(m.time[0] - 1_789_279_147.885186870) < 1e-7


def test_read_meta_feed_geometry(chord_file):
    m = n2_io.read_meta(chord_file)
    assert m.pol.tolist() == FILE_POL
    assert m.dish_type.tolist() == FILE_TYPE
    assert (m.dish_type == n2_io.DISH_ARRAY).tolist() == [True, False, True, False]
    assert np.allclose(m.position_m, FILE_POS)
    assert np.allclose(m.grid_orientation, GRID_ORIENTATION)


def test_enu_positions_rotate_grid_frame_to_east_north(chord_file):
    """R's rows are the grid axes in the E/N/U basis, so with the grid
    x-axis at (cos th, sin th, 0) -- th north of east -- a feed 6.3 m
    along it sits at 6.3 (cos th, sin th) East/North."""
    m = n2_io.read_meta(chord_file)
    enu = n2_io.enu_positions_m(m)
    assert enu.shape == (NFEED, 3)
    th = np.radians(GRID_ROT_DEG)
    assert np.allclose(enu[1], [6.3 * np.cos(th), 6.3 * np.sin(th), 0.0])
    assert np.allclose(enu[0], 0.0)


def test_enu_positions_without_orientation_pass_through(chord_file):
    with h5py.File(chord_file, "r+") as f:
        del f.attrs["grid_orientation"]
    m = n2_io.read_meta(chord_file)
    assert m.grid_orientation is None
    assert np.allclose(n2_io.enu_positions_m(m), FILE_POS)


def test_geometry_is_optional(chord_file):
    """A file from before kotekan wrote the geometry still reads; the
    fields are None and the ENU helper says so."""
    with h5py.File(chord_file, "r+") as f:
        del f["index_map"]["type"]
        del f.attrs["feed_positions_m"]
        del f.attrs["grid_orientation"]
    m = n2_io.read_meta(chord_file)
    assert m.pol is not None                 # kept: the label layout needs it
    assert m.dish_type is None and m.position_m is None
    with pytest.raises(ValueError, match="no feed positions"):
        n2_io.enu_positions_m(m)


def test_geometry_length_mismatch_is_refused(chord_file):
    with h5py.File(chord_file, "r+") as f:
        f.attrs["feed_positions_m"] = np.zeros((NFEED + 1, 3))
    with pytest.raises(OSError, match="entries for 4 elements"):
        n2_io.read_meta(chord_file)


def test_file_without_any_time_axis_is_refused(chord_file):
    with h5py.File(chord_file, "r+") as f:
        del f["index_map"]["time"]
    with pytest.raises(OSError, match="no time axis"):
        n2_io.read_meta(chord_file)


def test_chime_style_file_is_refused(chime_file):
    """Pre-2026-08 files carried a wrong element ordering; feeds
    selected by label against them would be the wrong elements."""
    with pytest.raises(OSError, match="per-element label"):
        n2_io.read_meta(chime_file)


def test_pre_2026_08_per_element_labels_are_refused(tmp_path, chord_file):
    with h5py.File(chord_file, "r+") as f:
        del f["index_map"]["label"]
        del f["index_map"]["pol"]
        f["index_map"].create_dataset(
            "label", data=np.array(OLD_LABELS, dtype="S10"))
    with pytest.raises(OSError, match="per-element label"):
        n2_io.read_meta(chord_file)


def test_per_dish_label_table_is_refused(tmp_path):
    """The 2026-08..09 layout (one label per dish, expanded by the reader)
    is not accepted any more: the count does not match the element axis."""
    a, b = _prods(4)
    path = tmp_path / "chord_per_dish.h5"
    with h5py.File(path, "w") as f:
        f.attrs["num_elements"] = 4
        im = f.create_group("index_map")
        im.create_dataset("label", data=np.array([b"A1", b"B1"], dtype="S10"))
        freq = np.zeros(1, dtype=[("centre", "<f8"), ("width", "<f8")])
        freq["centre"], freq["width"] = 400.0, 100.0
        im.create_dataset("freq", data=freq)
        im.create_dataset("time", data=np.array([1000.0]))
        prod = np.zeros(a.size, dtype=[("input_a", "<u2"), ("input_b", "<u2")])
        prod["input_a"], prod["input_b"] = a, b
        im.create_dataset("prod", data=prod)
        f.create_dataset("vis", data=np.zeros((1, a.size, 1), np.complex64))
    with pytest.raises(OSError, match="2 entries for 4 elements"):
        n2_io.read_meta(path)


def test_read_products(chord_file):
    m = n2_io.read_meta(chord_file)
    a, b = _prods(NFEED)
    expect = _vis_values(a, b)                    # (freq, prod, time)

    prod_idx = np.array([0, 3, 7])
    time_sel = np.array([1, 2, 4])
    fsl = slice(1, 3)
    out = n2_io.read_products(m, prod_idx, time_sel, fsl)
    assert out.shape == (3, 2, 3)
    want = expect[fsl][:, prod_idx][:, :, time_sel]      # (nf, np, nt)
    assert np.allclose(out, np.moveaxis(want, -1, 0))


def test_read_valid_and_flags(chord_file):
    m = n2_io.read_meta(chord_file)
    time_sel = np.arange(NTIME)
    valid = n2_io.read_valid(m, time_sel, slice(0, NFREQ))
    assert valid.shape == (NTIME, NFREQ)
    assert not valid[2, 1] and valid.sum() == NTIME * NFREQ - 1

    flags = n2_io.read_input_flags(m, time_sel)
    assert flags.shape == (NTIME, NFEED)
    assert not flags[:, 3].any() and flags[:, :3].all()


def test_valid_and_flags_default_to_good(chord_file):
    # A file without frames_added / flags datasets defaults to all-good.
    with h5py.File(chord_file, "r+") as f:
        del f["frames_added"]
        del f["flags"]
    m = n2_io.read_meta(chord_file)
    time_sel = np.arange(3)
    assert n2_io.read_valid(m, time_sel, slice(0, NFREQ)).all()
    assert n2_io.read_input_flags(m, time_sel).all()


def test_pol_products(chord_file):
    m = n2_io.read_meta(chord_file)
    feeds = np.array([0, 2])                      # the two pol-A inputs
    prod_idx, ai, bi = n2_io.pol_products(m, feeds)
    # products among feeds {0, 2}: (0,0), (0,2), (2,2)
    a, b = _prods(NFEED)
    assert [(a[i], b[i]) for i in prod_idx] == [(0, 0), (0, 2), (2, 2)]
    assert list(ai) == [0, 0, 1] and list(bi) == [0, 1, 1]
    # scatter into a 2x2 Hermitian matrix covers every cell
    V = np.zeros((2, 2), dtype=complex)
    V[ai, bi] = 1.0
    V[bi, ai] = 1.0
    assert V.all()
