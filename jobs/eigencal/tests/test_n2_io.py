"""Tests for the N² reader against tiny synthetic files in both layouts."""

import h5py
import numpy as np
import pytest

import n2_io

NFEED, NFREQ, NTIME = 4, 3, 6
LABELS = [b"d0_pA", b"d0_pB", b"d1_pA", b"d1_pB"]


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
    """CHORD hdf5N2Write flavour: label index map, vis[freq, prod, time]."""
    a, b = _prods(NFEED)
    path = tmp_path / "chord.h5"
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("label", data=np.array(LABELS, dtype="S10"))
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
    """CHIME flavour: compound input index map, vis[time, freq, prod]."""
    a, b = _prods(NFEED)
    path = tmp_path / "chime.h5"
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        inp = np.zeros(NFEED, dtype=[("chan_id", "<u2"), ("correlator_input", "S10")])
        inp["chan_id"] = np.arange(NFEED)
        inp["correlator_input"] = LABELS
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
    assert list(m.labels) == [l.decode() for l in LABELS]
    assert np.allclose(m.freq_mhz, [400.0, 500.0, 600.0])
    assert np.allclose(m.freq_width_mhz, 100.0)
    assert np.allclose(m.time, 1005.0 + 10.0 * np.arange(NTIME))  # centred
    assert m.prod_a.size == NFEED * (NFEED + 1) // 2


def test_read_meta_chime(chime_file):
    m = n2_io.read_meta(chime_file)
    assert m.time_first
    assert list(m.labels) == [l.decode() for l in LABELS]


@pytest.mark.parametrize("fixture", ["chord_file", "chime_file"])
def test_read_products_matches_both_layouts(fixture, request):
    path = request.getfixturevalue(fixture)
    m = n2_io.read_meta(path)
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


def test_valid_and_flags_default_to_good(chime_file):
    m = n2_io.read_meta(chime_file)
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
