"""Tests for the file reducer, against a synthesized N² file.

The fixture mirrors the real layout (``vis`` as
``(n_freq, n_prod, n_time)`` complex64, an ``index_map`` group, and the
``frames_added`` validity grid) so the tests exercise the same code paths
the live files do, without needing the mount.
"""

import json

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
        # kotekan's per-element table (chord.2021.10+988): n_elem // 2
        # dishes x 2 pol in [P][D] order, dish label + p1/p2, with the
        # polarization index alongside.
        n_dish = n_elem // 2
        g.create_dataset("label", data=np.array(
            [f"A{d}p{p + 1}".encode() for p in range(2) for d in range(n_dish)]))
        g.create_dataset("pol", data=np.array(
            [p for p in range(2) for _ in range(n_dish)], np.int32))
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


def _labels(path, labels, pol=None):
    """Replace the fixture file's label table (and its pol index)."""
    with h5py.File(path, "a") as f:
        del f["index_map/label"]
        f["index_map"].create_dataset(
            "label", data=np.array([l.encode() for l in labels]))
        if "index_map/pol" in f:
            del f["index_map/pol"]
        if pol is not None:
            f["index_map"].create_dataset("pol", data=np.array(pol, np.int32))


def test_read_axes_compact_labels_are_read_as_written(tmp_path):
    """A DishInputs file over 4 of a wider table's elements carries exactly
    those 4 labels, in its own order — nothing is expanded or looked up.
    (Live case: the 48-element subset files, B4p1..RFIA1p1, B4p2..RFIA1p2.)"""
    path = tmp_path / "vis_0000000001_x.h5"
    make_file(path, n_elem=4)
    _labels(path, ["B4p1", "RFIA1p1", "B4p2", "RFIA1p2"], pol=[0, 0, 1, 1])
    with h5py.File(path, "a") as f:
        f.attrs["n2_layout"] = "DishInputs"
        f.attrs["input_list"] = np.array([0, 63, 64, 127], np.int32)
    assert R.read_axes(path).labels == ["B4X", "RFIA1X", "B4Y", "RFIA1Y"]


def test_read_axes_xy_labels_are_read_as_written(tmp_path):
    """kotekan PR #1695 (2026-09-22; subset acquisitions from 2026-10-01):
    the polarization *name* is the suffix and index_map/pol is alongside.
    (Live case: A01X..RFIB4X, A01Y..RFIB4Y.)"""
    path = tmp_path / "vis_0000000001_x.h5"
    make_file(path, n_elem=4)
    _labels(path, ["A01X", "RFIB4X", "A01Y", "RFIB4Y"], pol=[0, 0, 1, 1])
    with h5py.File(path, "a") as f:
        f.attrs["n2_layout"] = "DishInputs"
    assert R.read_axes(path).labels == ["A01X", "RFIB4X", "A01Y", "RFIB4Y"]


def test_read_axes_xy_labels_without_pol_index_store_nothing(tmp_path, caplog):
    """Pre-2026-08 per-element labels (A0X) carried a wrong element
    ordering and are the same text as the PR #1695 spelling; without
    index_map/pol to vouch for them they are refused, not reinterpreted."""
    path = tmp_path / "vis_0000000001_x.h5"
    make_file(path, n_elem=4)
    _labels(path, ["A0X", "A1X", "A0Y", "A1Y"])
    assert R.read_axes(path).labels == []
    assert "no index_map/pol to vouch" in caplog.text


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


# --- the selection ---------------------------------------------------------

def test_select_by_reference_and_pairs(vis_file):
    path, _ = vis_file
    ax = R.read_axes(path)                      # elements A0X, A1X, A0Y, A1Y
    assert ax.labels == ["A0X", "A1X", "A0Y", "A1Y"] and ax.chunk_rows == 16
    sel, warns = R.select(ax, {"autos": True, "reference": ["A0X"], "pairs": [["A0Y", "A1Y"]]})
    assert warns == []
    # the file's own order; the reference keeps same-polarization crosses only
    assert list(zip(sel.input_a, sel.input_b)) == [(0, 0), (0, 1), (1, 1), (2, 2), (2, 3), (3, 3)]
    assert sel.n_freq == 64 and sel.product_names()[1] == "e0000xe0001"
    sel, warns = R.select(ax, {"autos": False, "reference": ["Z9X"], "pairs": [["A0X", "A0X"], ["A0X"]]})
    assert sel.n_prod == 1 and list(zip(sel.input_a, sel.input_b)) == [(0, 0)]
    assert any("Z9X: not an element" in w for w in warns) and any("[label, label]" in w for w in warns)
    sel, warns = R.select(ax, None)
    assert sel.n_prod == 10 and sel.is_all_products(ax) and warns == []


def test_strided_channels_keep_one_chunk_in_n(vis_file):
    path, _ = vis_file
    ax = R.read_axes(path)
    sel, _ = R.select(ax, {"autos": True}, freq_stride=2)
    assert sel.freq_idx.tolist() == list(range(0, 16)) + list(range(32, 48))
    assert np.allclose(sel.freq_mhz, ax.freq_mhz[sel.freq_idx])
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path))
    full = R.quantize_file(path, lo, hi)
    part = R.quantize_file(path, lo[sel.prod_idx], hi[sel.prod_idx], sel=sel, axes=ax)
    assert part.shape == (4, 6, 32)
    # the same pixels the full pass makes, for the selected products and channels
    np.testing.assert_array_equal(part, full[sel.prod_idx][:, :, sel.freq_idx])
    # the scale sample covers only the selected chunks and products
    sample = R.sample_magnitudes(path, sel, block_stride=1)
    assert sample.shape == (4, 32 * 6)
    assert R.strided_channels(ax, 1).tolist() == list(range(64))


def test_selection_from_index_matches_by_inputs(vis_file):
    path, _ = vis_file
    ax = R.read_axes(path)
    index = {"n_freq": 32, "n_prod": 2, "n_prod_file": 10, "n_freq_file": 64,
             "freq_idx": list(range(16)) + list(range(32, 48)),
             "products": [{"name": "e0001xe0001", "a": 1, "b": 1}, {"name": "e0002xe0003", "a": 2, "b": 3}]}
    sel, why = R.selection_from_index(ax, index)
    assert why is None and sel.prod_idx.tolist() == [4, 8] and sel.n_freq == 32
    # an index from before the selection existed: everything, by the file's counts
    sel, why = R.selection_from_index(ax, {"n_freq": 64, "n_prod": 10,
                                           "products": [{"name": "x", "a": a, "b": b} for a in range(4) for b in range(a, 4)]})
    assert why is None and sel.is_all_products(ax) and sel.is_all_channels(ax)
    _, why = R.selection_from_index(ax, {**index, "n_prod_file": 6})
    assert "products" in why
    _, why = R.selection_from_index(ax, {**index, "products": [{"name": "x", "a": 7, "b": 7}]})
    assert "not in this file" in why


# --- the overview ----------------------------------------------------------

def test_overview_spec_columns_and_bands(vis_file):
    path, _ = vis_file
    ax = R.read_axes(path)                            # 64 channels, chunk rows 16
    sel, _ = R.select(ax, None, freq_stride=2)        # channels 0..15 and 32..47
    spec = R.overview_spec(ax, sel, 2)
    assert spec.n_chunk == 2 and spec.chunk_starts.tolist() == [0, 16]
    assert np.allclose(spec.chunk_freq, [ax.freq_mhz[0:16].mean(), ax.freq_mhz[32:48].mean()])
    assert len(spec.edges) == 3 and spec.edges[0] == ax.freq_mhz.min() and spec.edges[-1] == ax.freq_mhz.max()
    assert spec.band_of(spec.edges[0]) == 0 and spec.band_of(spec.edges[-1]) == 1
    assert spec.band_of(1.0) is None
    assert spec.is_triangle and spec.to_index()["prod_order"] == "triangle"
    full = R.overview_spec(ax, R.everything(ax), 8)
    assert full.n_chunk == 4 and full.n_band == 8
    a, b = R.triangle_order(4)
    assert list(zip(a, b)) == [(0, 0), (0, 1), (0, 2), (0, 3), (1, 1), (1, 2), (1, 3), (2, 2), (2, 3), (3, 3)]
    # the recorded edges win over a band count
    again = R.overview_spec(ax, sel, 8, edges=spec.edges)
    assert again.edges == spec.edges and again.n_band == 2


def test_fold_file_overview_cubes(vis_file):
    import h5py
    path, _ = vis_file
    ax = R.read_axes(path)
    sel, _ = R.select(ax, {"autos": True})
    spec = R.overview_spec(ax, sel, 2)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(path, sel))
    out, ov = R.fold_file(path, lo, hi, sel=sel, axes=ax, overview=spec)
    np.testing.assert_array_equal(out, R.quantize_file(path, lo, hi, sel=sel, axes=ax))
    assert ov.coarse.shape == (10, 4) and ov.coherence.shape == (4, 4) and ov.matrix.shape == (2, 4, 4)
    with h5py.File(path) as f:
        mag = np.abs(f["vis"][()])
        live = (mag > 0) & (f["frames_added"][()][:, None, :] > 0)
    # coarse: the mean |V| over a chunk's live cells, per product
    for k in range(4):
        c = slice(16 * k, 16 * k + 16)
        m = np.where(live[c], mag[c], 0).sum(axis=(0, 2)) / live[c].sum(axis=(0, 2))
        np.testing.assert_allclose(ov.coarse[:, k], m, rtol=1e-5)
    # coherence of element 0 in chunk 0: its partners' |V_0b| / sqrt(V_00 V_bb), averaged
    auto = {0: 0, 1: 4, 2: 7, 3: 9}
    vals = []
    for p, b in ((1, 1), (2, 2), (3, 3)):
        with np.errstate(invalid="ignore", divide="ignore"):
            n = mag[0:16, p, :] / np.sqrt(mag[0:16, 0, :] * mag[0:16, auto[b], :])
        vals.append(n[live[0:16, p, :]])
    assert ov.coherence[0, 0] == pytest.approx(np.concatenate(vals).mean(), rel=1e-4)
    assert np.nanmin(ov.coherence) >= 0 and np.nanmax(ov.coherence) <= 1.0 + 1e-6
    # the matrix: band means, normalized by the autos' band means, symmetric, unit diagonal
    band0 = slice(0, 32)                              # chunks 0 and 1 (centres below the band edge)
    def band_mean(p):
        return np.where(live[band0, p], mag[band0, p], 0).sum() / live[band0, p].sum()
    expect = band_mean(1) / np.sqrt(band_mean(0) * band_mean(4))
    assert ov.matrix[0, 0, 1] == pytest.approx(expect, rel=1e-4) and ov.matrix[0, 1, 0] == ov.matrix[0, 0, 1]
    assert np.allclose(np.diagonal(ov.matrix[0]), 1.0)
    assert np.allclose(ov.matrix[1], ov.matrix[1].T, equal_nan=True)


def test_cube_quantizers():
    # coherence: log scale from the floor (1e-3 -> 1) to 1 (-> 255); nothing -> 0
    np.testing.assert_array_equal(R.quantize_unit(np.array([[1e-3, 1e-2, 0.1, 1.0, np.nan, 2.0, 0.0, 1e-5]])),
                                  [[1, 86, 170, 255, 0, 255, 0, 1]])
    lo, hi = np.array([1.0, np.nan]), np.array([100.0, np.nan])
    q = R.quantize_log(np.array([[1.0, 10.0, 100.0, 0.0, np.nan], [5.0] * 5]), lo, hi)
    assert q.tolist() == [[1, 128, 255, 0, 0], [0] * 5]


# --- the mask and delivery record ------------------------------------------

def _with_masks(path, n_freq=64, n_elem=4, n_time=6):
    """Add kotekan's per-feed mask and per-sample fractions to a fixture
    file: element 1 masked on every stream from bin 2, element 3 on
    stream 5 only; stream 7 delivers nothing in bin 0."""
    with h5py.File(path, "r+") as f:
        flags = np.ones((n_freq, n_elem, n_time), np.float32)
        flags[:, 1, 2:] = 0
        flags[5::R.N_STREAMS, 3, :] = 0
        f.create_dataset("flags", data=flags)
        frames = f["frames_added"][()]
        frames[7::R.N_STREAMS, 0] = 0
        f["frames_added"][...] = frames
        rfi = np.full((n_freq, n_time), 0.25, np.float32)
        f.create_dataset("frac_rfi", data=rfi)
        f.create_dataset("frac_pl", data=np.zeros((n_freq, n_time), np.float32))
        f.create_dataset("frac_rfi_first_stage_enabled", data=np.ones((n_freq, n_time), np.float32))
        f.create_dataset("fringestop_enabled", data=np.zeros(n_time, bool))
        f.create_dataset("digital_gains/update_id", data=np.array([b"gain_a"], object),
                         dtype=h5py.string_dtype())
        f.create_dataset("config_json", data=np.array(
            [json.dumps({"kotekan_version": v, "config": {}}) for v in ("b", "a", "b", "")], object),
            dtype=h5py.string_dtype())


def test_read_diag_counts_masks_per_stream(tmp_path):
    path = tmp_path / "v.h5"
    make_file(path)
    _with_masks(path)
    d = R.read_diag(path, R.read_axes(path))
    assert (d["file"], d["n_bin"], d["n_streams"]) == (4202415, 6, R.N_STREAMS)
    assert d["flag_live"] == [15, 16, 16, 16, 16, 16]
    # every delivering stream masks A1p1 (element 1) from bin 2; one masks element 3
    labels = R.read_axes(path).labels
    assert d["masked"] == {labels[1]: [[2, 6, 16]], labels[3]: [[0, 6, 1]]}
    # stream 5 masks element 3 too; stream 7 delivered nothing in bin 0
    assert d["stream_masked"][0] == [0, 0, 1, 1, 1, 1]
    assert d["stream_masked"][5] == [1, 1, 2, 2, 2, 2] and d["stream_masked"][7][0] == -1
    assert d["missing"][7][0] == 10000 and d["missing"][7][1] == 0 and d["missing"][0][0] == 0
    assert d["rfi"][0] == [2500] * 6 and d["pl"][3] == [0] * 6 and d["rfi_on"][1] == [10000] * 6
    assert d["rfi"][7][0] == -1                                   # nothing live to average
    assert d["fringestop"] == 0 and d["gains"] == "gain_a" and d["versions"] == ["a", "b"]
    assert len(d["times"]) == 6
    json.dumps(d)                                                 # one JSON line


def test_read_diag_without_the_datasets_says_less(vis_file):
    path, _ = vis_file
    d = R.read_diag(path, R.read_axes(path))
    assert "masked" not in d and "rfi" not in d and "gains" not in d
    assert len(d["missing"]) == R.N_STREAMS


def test_array_levels_are_the_median_over_elements_with_data():
    lv = np.array([[[10, 0]], [[30, 0]], [[20, 0]], [[0, 0]]], np.uint8)     # (4 el, 1 row, 2 chunks)
    out = R.array_levels(lv)
    assert out.shape == (1, 2) and out[0, 0] == 20 and out[0, 1] == wfpng.MISSING
    # an even count takes the lower middle
    assert R.array_levels(np.array([[[10]], [[40]]], np.uint8))[0, 0] == 10
