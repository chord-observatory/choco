"""Tests for the file reducer, against a synthesized N² file.

The fixture mirrors the real layout (``vis`` as
``(n_freq, n_prod, n_time)`` complex64, an ``index_map`` group, and the
``frames_added`` validity grid) so the tests exercise the same code paths
the live files do, without needing the mount.
"""

import h5py
import numpy as np
import pytest

import reduce as R
import wfpng


def make_file(path, n_freq=64, n_elem=4, n_time=6, seed=0, dead_cells=()):
    """A miniature kotekan N² visibility file."""
    rng = np.random.default_rng(seed)
    pairs = [(a, b) for a in range(n_elem) for b in range(a, n_elem)]
    n_prod = len(pairs)

    # autos bright and real, crosses fainter — enough structure that a
    # per-product scale is meaningfully different per product
    vis = np.empty((n_freq, n_prod, n_time), np.complex64)
    for p, (a, b) in enumerate(pairs):
        amp = 1e4 if a == b else 1e2 * (1 + a + b)
        vis[:, p, :] = (amp * (1 + 0.3 * rng.standard_normal((n_freq, n_time)))
                        ).astype(np.complex64)

    frames = np.ones((n_freq, n_time), np.uint8)
    for fi, ti in dead_cells:
        frames[fi, ti] = 0
        vis[fi, :, ti] = 0

    with h5py.File(path, "w") as f:
        f.create_dataset("vis", data=vis)
        f.create_dataset("frames_added", data=frames)
        f.create_dataset("time_center_ut1_ns",
                         data=np.arange(n_time, dtype=np.int64) * 10_000_000_000)
        g = f.create_group("index_map")
        g.create_dataset("prod", data=np.array(
            pairs, dtype=[("input_a", "<u2"), ("input_b", "<u2")]).ravel())
        g.create_dataset("freq", data=np.array(
            [(300.0 + i, 1.0) for i in range(n_freq)],
            dtype=[("centre", "<f8"), ("width", "<f8")]))
        # Per-dish labels (2026-08 layout): n_elem // 2 dishes x 2 pol.
        g.create_dataset("label", data=np.array(
            [f"A{i}".encode() for i in range(n_elem // 2)]))
        f.attrs["num_elements"] = n_elem
        f.attrs["num_prod"] = n_prod
        f.attrs["abs_file_idx"] = 4202415
    return pairs


@pytest.fixture
def vis_file(tmp_path):
    path = tmp_path / "vis_0004202415_x.h5"
    pairs = make_file(path, dead_cells=[(3, 2), (10, 0)])
    return path, pairs


# --- axes ----------------------------------------------------------------

def test_read_axes(vis_file):
    path, pairs = vis_file
    ax = R.read_axes(path)
    assert (ax.n_freq, ax.n_prod, ax.n_time) == (64, len(pairs), 6)
    assert ax.n_elements == 4 and ax.file_idx == 4202415
    assert list(zip(ax.input_a, ax.input_b)) == pairs
    assert ax.freq_mhz[0] == 300.0 and len(ax.freq_mhz) == 64
    assert ax.labels[:2] == ["A0X", "A1X"]
    assert ax.times_ns.shape == (6,)


def test_read_axes_per_element_labels_store_nothing(tmp_path, caplog):
    """Pre-2026-08 per-element labels carried a wrong element ordering:
    they are dropped, and the viewer falls back to element indices."""
    path = tmp_path / "vis_0004202415_x.h5"
    make_file(path)
    with h5py.File(path, "r+") as f:
        del f["index_map/label"]
        f["index_map"].create_dataset(
            "label", data=np.array([b"A0X", b"A1X", b"A0Y", b"A1Y"]))
    ax = R.read_axes(path)
    assert ax.labels == []
    assert "pre-2026-08" in caplog.text


def test_read_axes_per_dish_labels_expand(tmp_path):
    """2026-08 layout: index_map/label is one label per dish; the stored
    labels must cover the whole [P][D] element axis (X block then Y),
    so the contact sheet can name the second-polarisation elements."""
    path = tmp_path / "vis_0000000001_x.h5"
    make_file(path, n_elem=4)
    with h5py.File(path, "a") as f:
        del f["index_map/label"]
        f["index_map"].create_dataset(
            "label", data=np.array([b"A1", b"B1"]))  # 2 dishes, 4 elements
    ax = R.read_axes(path)
    assert ax.labels == ["A1X", "B1X", "A1Y", "B1Y"]


def test_product_names_are_sortable_and_padded(vis_file):
    path, _ = vis_file
    names = R.read_axes(path).product_names()
    assert names[0] == "e0000xe0000"
    assert "e0001xe0003" in names
    assert names == sorted(names)          # lexical order == numeric order


def test_is_auto_flags_the_diagonal(vis_file):
    path, pairs = vis_file
    ax = R.read_axes(path)
    assert ax.is_auto.sum() == 4
    assert [pairs[i] for i in np.flatnonzero(ax.is_auto)] == [(i, i) for i in range(4)]


# --- scale ---------------------------------------------------------------

def test_sample_is_chunk_strided_but_spans_the_band(vis_file):
    path, pairs = vis_file
    s = R.sample_magnitudes(path, block_stride=2)
    assert s.shape[0] == len(pairs)
    assert s.shape[1] == 32 * 6           # two 16-row blocks x 6 times
    assert np.isfinite(s).all()


def test_scale_pads_outward_from_the_observed_span(vis_file):
    path, _ = vis_file
    s = R.sample_magnitudes(path)
    lo, hi = R.scale_from_sample(s, headroom=0.5)
    assert np.isfinite(lo).all() and np.isfinite(hi).all()
    assert (hi > lo).all()
    for p in range(s.shape[0]):
        a, b = np.percentile(s[p][s[p] > 0], [0.5, 99.5])
        assert lo[p] < a and hi[p] > b     # the observed range sits inside
        # padding is proportional: total span is ~2x the observed span
        assert np.log10(hi[p] / lo[p]) == pytest.approx(2 * np.log10(b / a), rel=1e-6)


def test_scale_gives_autos_and_crosses_different_ranges(vis_file):
    path, _ = vis_file
    ax = R.read_axes(path)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    assert lo[ax.is_auto].min() > lo[~ax.is_auto].max()


def test_scale_is_nan_for_a_product_with_no_signal(tmp_path):
    path = tmp_path / "z.h5"
    make_file(path)
    with h5py.File(path, "r+") as f:
        f["vis"][:, 2, :] = 0
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    assert np.isnan(lo[2]) and np.isnan(hi[2])
    assert np.isfinite(lo[0])


def test_headroom_zero_is_a_tight_range(vis_file):
    path, _ = vis_file
    s = R.sample_magnitudes(path)
    lo, hi = R.scale_from_sample(s, headroom=0.0)
    a, b = np.percentile(s[0][s[0] > 0], [0.5, 99.5])
    assert lo[0] == pytest.approx(a) and hi[0] == pytest.approx(b)


# --- quantization --------------------------------------------------------

def test_quantize_shape_is_scanline_order(vis_file):
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    assert q.shape == (len(pairs), 6, 64)     # (prod, time, freq)
    assert q.dtype == np.uint8


def test_quantize_blanks_cells_with_no_frames(vis_file):
    """frames_added == 0 is exactly where nothing landed."""
    path, _ = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    assert (q[:, 2, 3] == wfpng.MISSING).all()     # dead cell (freq 3, time 2)
    assert (q[:, 0, 10] == wfpng.MISSING).all()
    assert (q[:, 1, 3] != wfpng.MISSING).all()     # neighbours untouched


def test_quantize_is_monotonic_in_magnitude(tmp_path):
    path = tmp_path / "m.h5"
    make_file(path, n_freq=32, n_elem=2, n_time=4)
    with h5py.File(path, "r+") as f:
        ramp = np.logspace(1, 5, 32).astype(np.complex64)
        f["vis"][:, 0, 0] = ramp
    lo = np.full(3, 1.0)
    hi = np.full(3, 1e6)
    q = R.quantize_file(path, lo, hi)
    line = q[0, 0, :]
    assert (np.diff(line.astype(int)) >= 0).all()
    assert line[0] > wfpng.MISSING and line[-1] <= wfpng.N_LEVELS


def test_quantize_blanks_products_without_a_scale(vis_file):
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    lo[1] = hi[1] = np.nan
    q = R.quantize_file(path, lo, hi)
    assert (q[1] == wfpng.MISSING).all()
    assert (q[0] != wfpng.MISSING).any()


def test_quantize_accepts_a_caller_buffer(vis_file):
    """The job reuses one buffer across files rather than reallocating."""
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    buf = np.zeros((len(pairs), 6, 64), np.uint8)
    out = R.quantize_file(path, lo, hi, out=buf)
    assert out is buf and (buf != 0).any()


def test_quantize_rejects_a_mismatched_buffer(vis_file):
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    with pytest.raises(ValueError, match="out is"):
        R.quantize_file(path, lo, hi, out=np.zeros((2, 2, 2), np.uint8))


def test_quantize_rejects_a_mismatched_scale(vis_file):
    path, _ = vis_file
    with pytest.raises(ValueError, match="products"):
        R.quantize_file(path, np.ones(3), np.ones(3) * 10)


def test_quantize_survives_a_file_without_frames_added(tmp_path):
    path = tmp_path / "nf.h5"
    make_file(path)
    with h5py.File(path, "r+") as f:
        del f["frames_added"]
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    assert (q != wfpng.MISSING).any()


# --- thumbnails ----------------------------------------------------------

def test_thumb_rows_shape_and_binning(vis_file):
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    t = R.thumb_rows(q, n_bins=16)
    assert t.shape == (len(pairs), 16)
    assert (t != wfpng.MISSING).any()


def test_thumb_rows_keep_a_dropout(tmp_path):
    """A single dead channel must not be averaged away by its neighbours."""
    path = tmp_path / "d.h5"
    make_file(path, n_freq=64, n_elem=2, n_time=4)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    q[0, :, 5] = 1                      # one very low channel
    t = R.thumb_rows(q, n_bins=8)
    assert t[0, 0] == 1                 # the bin containing it reports the low value


def test_thumb_rows_report_a_fully_missing_bin(tmp_path):
    path = tmp_path / "e.h5"
    make_file(path, n_freq=32, n_elem=2, n_time=4)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    q[0, :, 0:4] = wfpng.MISSING
    t = R.thumb_rows(q, n_bins=8)
    assert t[0, 0] == wfpng.MISSING
    assert t[0, 1] != wfpng.MISSING


def test_thumb_rows_clamp_bins_to_the_band(vis_file):
    path, pairs = vis_file
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    q = R.quantize_file(path, lo, hi)
    assert R.thumb_rows(q, n_bins=10_000).shape == (len(pairs), 64)
