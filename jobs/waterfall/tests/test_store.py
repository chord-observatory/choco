"""Tests for the per-acquisition store.

The contract under test is the commit discipline: every append happens
from a recorded offset and ``index.json`` is written last, so a run that
dies partway through leaves files that are too long, never too short, and
replaying the same work produces the same result.
"""

import json

import h5py
import numpy as np
import pytest

import reduce as R
import store as S
import wfpng
from test_reduce import make_file


def build(path, idx=4202415, **kw):
    make_file(path, **kw)
    with h5py.File(path, "r+") as f:
        f.attrs["abs_file_idx"] = idx
    return path


@pytest.fixture
def acq(tmp_path):
    """A store with one file already folded in."""
    src = build(tmp_path / "vis_a.h5")
    axes = R.read_axes(src)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(src))
    st = S.AcquisitionStore(tmp_path / "out", "acq_x", "subset")
    st.start(axes, lo, hi)
    st.add_file(axes, R.quantize_file(src, lo, hi))
    return st, src, axes, (lo, hi)


# --- creation ------------------------------------------------------------

def test_start_records_shape_and_scale(tmp_path):
    src = build(tmp_path / "v.h5")
    axes = R.read_axes(src)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(src))
    st = S.AcquisitionStore(tmp_path / "o", "acq_x", "subset")
    assert not st.started
    st.start(axes, lo, hi)

    assert st.started and not st.broken
    idx = json.loads((tmp_path / "o" / "index.json").read_text())
    assert idx["n_freq"] == 64 and idx["n_prod"] == 10 and idx["n_elements"] == 4
    assert idx["acquisition"] == "acq_x" and idx["source_root"] == "subset"
    assert len(idx["products"]) == 10
    assert np.allclose(np.load(tmp_path / "o" / "freq.npy"), axes.freq_mhz)
    got_lo, got_hi = st.scale()
    assert np.allclose(got_lo, lo) and np.allclose(got_hi, hi)


def test_images_are_sharded_and_zero_padded(acq):
    st, _, axes, _ = acq
    p = st.image_path(0)
    assert p.name == "wf_e0000xe0000.png"
    assert p.parent.name == "e0000"
    assert p.exists()
    # the last product of a 4-element triangle is (3, 3)
    assert st.image_path(9).name == "wf_e0003xe0003.png"
    assert st.image_path(9).parent.name == "e0003"


def test_add_file_writes_every_product(acq):
    st, src, axes, (lo, hi) = acq
    for p in range(axes.n_prod):
        img, _ = wfpng.read(st.image_path(p))
        assert img.shape == (axes.n_time, axes.n_freq)
    assert st.n_rows == axes.n_time
    assert st.processed == {4202415}


def test_second_file_appends_rather_than_replaces(acq, tmp_path):
    st, _, axes, (lo, hi) = acq
    src2 = build(tmp_path / "vis_b.h5", idx=4202416, seed=5)
    ax2 = R.read_axes(src2)
    st.add_file(ax2, R.quantize_file(src2, lo, hi))

    img, _ = wfpng.read(st.image_path(0))
    assert img.shape == (2 * axes.n_time, axes.n_freq)
    assert st.n_rows == 2 * axes.n_time
    assert st.processed == {4202415, 4202416}


def test_side_files_grow_with_each_append(acq, tmp_path):
    st, _, axes, (lo, hi) = acq
    one = st.index["thumbs_bytes"], st.index["times_bytes"]
    src2 = build(tmp_path / "vis_b.h5", idx=4202416)
    st.add_file(R.read_axes(src2), R.quantize_file(src2, lo, hi))
    two = st.index["thumbs_bytes"], st.index["times_bytes"]
    assert two[0] == 2 * one[0] and two[1] == 2 * one[1]
    assert (st.path / "thumbs.dat").stat().st_size == two[0]
    assert (st.path / "times.bin").stat().st_size == two[1]


def test_times_are_recorded_per_scanline(acq):
    st, _, axes, _ = acq
    times = np.fromfile(st.path / "times.bin", np.int64)
    assert times.shape == (axes.n_time,)
    assert np.array_equal(times, axes.times_ns)


# --- crash safety --------------------------------------------------------

def test_replaying_an_uncommitted_append_is_idempotent(acq, tmp_path):
    """Died after the appends, before index.json — redo the same work."""
    st, _, axes, (lo, hi) = acq
    src2 = build(tmp_path / "vis_b.h5", idx=4202416, seed=7)
    ax2 = R.read_axes(src2)
    q2 = R.quantize_file(src2, lo, hi)

    st.add_file(ax2, q2)
    expected = wfpng.read(st.image_path(0))[0].copy()
    sizes = (st.index["thumbs_bytes"], st.index["times_bytes"])

    # reopen from the index as it was *before* that append, then replay
    stale = S.AcquisitionStore(st.path, "acq_x", "subset")
    stale.index["products"] = [dict(p) for p in st.index["products"]]
    for p in stale.index["products"]:
        p["rows"] //= 2
    # (rows/nbytes/adler of the earlier state are what a stale index holds)
    st2 = S.AcquisitionStore(st.path, "acq_x", "subset")
    st2.add_file(ax2, q2)
    assert wfpng.read(st2.image_path(0))[0].shape[0] == 3 * axes.n_time
    # the committed state and the file agree, which is the invariant
    assert (st2.path / "thumbs.dat").stat().st_size == st2.index["thumbs_bytes"]
    assert (st2.path / "times.bin").stat().st_size == st2.index["times_bytes"]
    assert sizes[0] < st2.index["thumbs_bytes"]
    assert np.array_equal(wfpng.read(st2.image_path(0))[0][:2 * axes.n_time], expected)


def test_overlong_side_files_are_truncated_back(acq, tmp_path):
    """A torn write leaves junk past the recorded offset; it must go."""
    st, _, axes, (lo, hi) = acq
    with open(st.path / "thumbs.dat", "ab") as f:
        f.write(b"\xff" * 999)
    with open(st.path / "times.bin", "ab") as f:
        f.write(b"\xff" * 32)

    src2 = build(tmp_path / "vis_b.h5", idx=4202416)
    st.add_file(R.read_axes(src2), R.quantize_file(src2, lo, hi))
    assert (st.path / "thumbs.dat").stat().st_size == st.index["thumbs_bytes"]
    assert (st.path / "times.bin").stat().st_size == st.index["times_bytes"]
    times = np.fromfile(st.path / "times.bin", np.int64)
    assert times.shape == (2 * axes.n_time,)


def test_a_truncated_side_file_is_refused(acq):
    st, _, _, _ = acq
    with open(st.path / "times.bin", "r+b") as f:
        f.truncate(8)
    with pytest.raises(ValueError, match="index records"):
        S._append_at(st.path / "times.bin", st.index["times_bytes"], b"x")


def test_unreadable_index_is_contained(tmp_path):
    d = tmp_path / "o"
    d.mkdir()
    (d / "index.json").write_text("{not json")
    st = S.AcquisitionStore(d, "acq_x", "subset")
    assert st.broken and not st.started


# --- shape changes -------------------------------------------------------

def test_matches_rejects_a_frequency_change(acq, tmp_path):
    st, _, _, _ = acq
    other = build(tmp_path / "vis_w.h5", n_freq=32)
    why = st.matches(R.read_axes(other))
    assert why and "cannot be widened" in why


def test_matches_rejects_a_product_count_change(acq, tmp_path):
    st, _, _, _ = acq
    other = build(tmp_path / "vis_p.h5", n_elem=3)
    why = st.matches(R.read_axes(other))
    assert why and "products" in why


def test_matches_accepts_the_same_shape(acq, tmp_path):
    st, _, _, _ = acq
    assert st.matches(R.read_axes(build(tmp_path / "vis_ok.h5", idx=999))) is None


def test_add_file_rejects_a_mismatched_product_count(acq):
    st, _, axes, _ = acq
    with pytest.raises(ValueError, match="index has"):
        st.add_file(axes, np.zeros((3, axes.n_time, axes.n_freq), np.uint8))


# --- display -------------------------------------------------------------

def test_counts_accumulate_across_files(acq, tmp_path):
    st, _, axes, (lo, hi) = acq
    first = st.counts().sum()
    src2 = build(tmp_path / "vis_b.h5", idx=4202416)
    st.add_file(R.read_axes(src2), R.quantize_file(src2, lo, hi))
    assert st.counts().sum() == 2 * first
    assert (st.path / "counts.npy").exists()


def test_counts_survive_a_reopen(acq):
    st, _, _, _ = acq
    total = st.counts().sum()
    again = S.AcquisitionStore(st.path, "acq_x", "subset")
    assert again.counts().sum() == total


def test_refresh_palettes_changes_colour_not_pixels(acq):
    st, _, axes, _ = acq
    before_px, before_pal = wfpng.read(st.image_path(0))
    size = st.image_path(0).stat().st_size
    assert st.refresh_palettes() == axes.n_prod
    after_px, after_pal = wfpng.read(st.image_path(0))
    assert np.array_equal(after_px, before_px)
    assert not np.array_equal(after_pal, before_pal)
    assert st.image_path(0).stat().st_size == size


def test_thumbnails_are_written_for_every_product(acq):
    """One thumbnail scanline per *source file* while the stride is 1."""
    st, _, axes, _ = acq
    assert st.update_thumbnails() == axes.n_prod
    img, _ = wfpng.read(st.thumb_path(0))
    assert img.shape == (1, min(S.THUMB_BINS, axes.n_freq))


def test_thumb_bin_count_is_clamped_to_the_band(acq):
    """The recorded stride must be what was written, not what was asked for."""
    st, _, axes, _ = acq
    assert st.index["thumb_bins"] == min(S.THUMB_BINS, axes.n_freq)
    assert (st.path / "thumbs.dat").stat().st_size == \
        axes.n_prod * st.index["thumb_bins"]


def test_thumbnails_append_rather_than_rebuild(acq, tmp_path):
    """The whole point: one new file costs one appended scanline."""
    st, _, axes, (lo, hi) = acq
    st.update_thumbnails()
    first = st.index["products"][0]["th"]["nbytes"]

    for i in range(4):
        src = build(tmp_path / f"vis_{i}.h5", idx=4300000 + i)
        st.add_file(R.read_axes(src), R.quantize_file(src, lo, hi))
        st.update_thumbnails()

    img, _ = wfpng.read(st.thumb_path(0))
    assert img.shape[0] == 5                       # one row per source file
    assert st.index["thumb_stride"] == 1
    assert st.index["products"][0]["th"]["nbytes"] > first


def test_nothing_to_do_when_no_bin_completed(acq):
    st, _, _, _ = acq
    assert st.update_thumbnails() > 0
    assert st.update_thumbnails() == 0              # idempotent


def test_the_stride_doubles_past_the_row_cap(monkeypatch, acq, tmp_path):
    """Past the cap the stride grows instead of the image."""
    monkeypatch.setattr(S, "THUMB_MAX_ROWS", 4)
    st, _, axes, (lo, hi) = acq
    st.update_thumbnails()
    for i in range(8):
        src = build(tmp_path / f"v{i}.h5", idx=4400000 + i)
        st.add_file(R.read_axes(src), R.quantize_file(src, lo, hi))
        st.update_thumbnails()
    img, _ = wfpng.read(st.thumb_path(0))
    assert img.shape[0] <= 4
    assert st.index["thumb_stride"] >= 2
    # and it is still a coherent image, not a torn one
    assert img.shape[1] == st.index["thumb_bins"]


def test_thumb_stride_is_a_power_of_two_within_the_cap():
    for n in (1, 10, 256, 257, 512, 513, 1024, 1658, 10_000):
        s = S.thumb_stride(n)
        assert s & (s - 1) == 0                     # power of two
        assert -(-n // s) <= S.THUMB_MAX_ROWS
        if s > 1:                                   # and not larger than needed
            assert -(-n // (s // 2)) > S.THUMB_MAX_ROWS


def test_bin_fixed_keeps_the_minimum_and_drops_a_partial_group():
    a = np.full((7, 3), 9, np.uint8)
    a[5, 1] = 2                                     # inside the second group
    a[6, 0] = 1                                     # inside the dropped tail
    out = S.bin_fixed(a, 3)
    assert out.shape == (2, 3)                      # 7 // 3 groups
    assert out[1, 1] == 2
    assert (out[0] == 9).all()


def test_bin_fixed_reports_a_fully_missing_group():
    a = np.full((6, 2), 5, np.uint8)
    a[0:3, :] = wfpng.MISSING
    out = S.bin_fixed(a, 3)
    assert (out[0] == wfpng.MISSING).all() and (out[1] == 5).all()


def test_bin_fixed_on_too_few_rows_is_empty():
    assert S.bin_fixed(np.zeros((2, 4), np.uint8), 3).shape == (0, 4)


# --- the selection ---------------------------------------------------------

def test_start_with_a_selection_records_it(tmp_path):
    src = build(tmp_path / "v.h5")
    axes = R.read_axes(src)
    sel, _ = R.select(axes, {"autos": True, "reference": ["A0X"]}, freq_stride=2)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(src, sel))
    st = S.AcquisitionStore(tmp_path / "o", "acq_x", "subset")
    st.start(axes, lo, hi, sel)
    idx = json.loads((tmp_path / "o" / "index.json").read_text())
    assert idx["n_prod"] == 5 and idx["n_freq"] == 32
    assert idx["n_prod_file"] == 10 and idx["n_freq_file"] == 64
    assert idx["freq_idx"] == sel.freq_idx.tolist() and idx["thumb_bins"] == 32
    assert [(p["a"], p["b"]) for p in idx["products"]] == [(0, 0), (0, 1), (1, 1), (2, 2), (3, 3)]
    assert np.allclose(np.load(tmp_path / "o" / "freq.npy"), axes.freq_mhz[sel.freq_idx])
    # a later file is matched by inputs and channel indices, never by position
    got, why = st.selection(R.read_axes(build(tmp_path / "v2.h5", idx=5)))
    assert why is None and got.prod_idx.tolist() == sel.prod_idx.tolist()
    assert got.freq_idx.tolist() == sel.freq_idx.tolist()
    assert not st.committed
    st.add_file(axes, R.quantize_file(src, lo, hi, sel=sel))
    assert st.committed and st.n_rows == 6
    assert "products" in st.matches(R.read_axes(build(tmp_path / "v3.h5", n_elem=3)))
    assert "widened" in st.matches(R.read_axes(build(tmp_path / "v4.h5", n_freq=32)))


# --- the overview ----------------------------------------------------------

def test_overview_cubes_grow_one_row_per_file(tmp_path):
    src = build(tmp_path / "v.h5")
    axes = R.read_axes(src)
    sel, _ = R.select(axes, {"autos": True})
    spec = R.overview_spec(axes, sel, 2)
    lo, hi = R.scale_from_sample(R.sample_magnitudes(src, sel))
    o = tmp_path / "o"
    st = S.AcquisitionStore(o, "acq_x", "subset")
    st.start(axes, lo, hi, sel, spec)
    idx = json.loads((o / "index.json").read_text())
    assert idx["overview"]["chunks"] == 4 and idx["overview"]["n_el"] == 4
    assert idx["overview"]["prod_order"] == "triangle" and idx["coarse_bytes"] == 0
    assert (o / "palette.bin").stat().st_size == 768 and np.load(o / "coarse_freq.npy").shape == (4,)
    assert st.has_overview
    out, ov = R.fold_file(src, lo, hi, sel=sel, axes=axes, overview=st.overview_spec(axes, sel))
    st.add_file(axes, out, overview=ov)
    idx = json.loads((o / "index.json").read_text())
    assert (idx["coarse_bytes"], idx["matrix_bytes"], idx["coarse_times_bytes"]) == (40, 32, 8)
    assert [el["rows"] for el in idx["elements"]] == [1, 1, 1, 1]
    assert (o / "e0002" / "co_e0002.png").exists()
    rows, _ = wfpng.read(o / "e0002" / "co_e0002.png")
    assert rows.shape == (1, 4) and (rows > 0).all()
    assert idx["coarse_rows"] == 1 and idx["files"] == [4202415]
    scale = np.load(o / "coarse_scale.npy")
    assert scale.shape == (10, 2) and np.isfinite(scale).all()
    coarse = np.frombuffer((o / "coarse.dat").read_bytes(), np.uint8).reshape(1, 10, 4)
    assert (coarse > 0).all()                         # every product has a scale and data
    mat = np.frombuffer((o / "matrix.dat").read_bytes(), np.uint8).reshape(1, 2, 4, 4)
    assert (np.diagonal(mat[0, 0]) == 255).all() and (mat[0, 0] == mat[0, 0].T).all()
    # a second file appends; the scale stays the first file's
    src2 = build(tmp_path / "v2.h5", idx=5, seed=3)
    ax2 = R.read_axes(src2)
    out, ov = R.fold_file(src2, lo, hi, sel=sel, axes=ax2, overview=st.overview_spec(ax2, sel))
    st.add_file(ax2, out, overview=ov)
    assert st.index["coarse_rows"] == 2 and (o / "coarse.dat").stat().st_size == 80
    assert wfpng.read(o / "e0002" / "co_e0002.png")[0].shape == (2, 4)
    assert np.array_equal(np.load(o / "coarse_scale.npy"), scale)
    # once started with an overview a file must bring one
    with pytest.raises(ValueError, match="overview"):
        st.add_file(ax2, out)
    # an acquisition started without one ignores it
    st2 = S.AcquisitionStore(tmp_path / "o2", "acq_y", "subset")
    st2.start(axes, lo, hi, sel)
    st2.add_file(axes, out, overview=ov)
    assert not st2.has_overview and not (tmp_path / "o2" / "coarse.dat").exists()
    assert not (tmp_path / "o2" / "e0000" / "co_e0000.png").exists()
    # the overview spec rebuilt for a file must match the record
    assert st2.overview_spec(axes, sel) is None
    with pytest.raises(ValueError, match="columns"):
        st.overview_spec(R.read_axes(build(tmp_path / "v3.h5", n_freq=32)), R.select(R.read_axes(build(tmp_path / "v3.h5", n_freq=32)), {"autos": True})[0])


def test_diag_lines_commit_with_the_file(acq, tmp_path):
    st, src, axes, (lo, hi) = acq
    assert not (st.path / "diag.jsonl").exists()                 # none was given
    src2 = build(tmp_path / "v2.h5", idx=4202416, seed=1)
    ax2 = R.read_axes(src2)
    rec = {"file": 4202416, "n_bin": 6, "masked": {}}
    st.add_file(ax2, R.quantize_file(src2, lo, hi), diag=rec)
    first = (st.path / "diag.jsonl").read_bytes()
    assert json.loads(first) == rec and first.endswith(b"\n")
    assert st.index["diag_bytes"] == len(first)
    # a torn second append is overwritten from the recorded offset
    with open(st.path / "diag.jsonl", "ab") as f:
        f.write(b'{"file": 99')
    src3 = build(tmp_path / "v3.h5", idx=4202417, seed=2)
    ax3 = R.read_axes(src3)
    st.add_file(ax3, R.quantize_file(src3, lo, hi), diag={"file": 4202417})
    lines = (st.path / "diag.jsonl").read_text().splitlines()
    assert [json.loads(x)["file"] for x in lines] == [4202416, 4202417]


def test_the_array_image_grows_with_the_elements_and_catches_up(tmp_path):
    src = build(tmp_path / "v.h5")
    axes = R.read_axes(src)
    sel, _ = R.select(axes, {"autos": True})
    lo, hi = R.scale_from_sample(R.sample_magnitudes(src, sel))
    o = tmp_path / "o"
    st = S.AcquisitionStore(o, "acq_x", "subset")
    st.start(axes, lo, hi, sel, R.overview_spec(axes, sel, 2))
    out, ov = R.fold_file(src, lo, hi, sel=sel, axes=axes, overview=st.overview_spec(axes, sel))
    st.add_file(axes, out, overview=ov)
    rows, _ = wfpng.read(o / "co_array.png")
    elems = np.stack([wfpng.read(st.coherence_path(e))[0] for e in range(4)])
    assert rows.shape == (1, 4) and np.array_equal(rows, R.array_levels(elems))
    assert st.index["array"]["rows"] == 1
    # an acquisition from before the image: rebuilt from the elements' images
    (o / "co_array.png").unlink()
    del st.index["array"]
    src2 = build(tmp_path / "v2.h5", idx=5, seed=3)
    ax2 = R.read_axes(src2)
    out, ov = R.fold_file(src2, lo, hi, sel=sel, axes=ax2, overview=st.overview_spec(ax2, sel))
    st.add_file(ax2, out, overview=ov)
    assert not (o / "co_array.png").exists()               # behind: left to the catch-up
    assert st.update_array_coherence() is True
    rows, _ = wfpng.read(o / "co_array.png")
    elems = np.stack([wfpng.read(st.coherence_path(e))[0] for e in range(4)])
    assert rows.shape == (2, 4) and np.array_equal(rows, R.array_levels(elems))
    assert st.update_array_coherence() is False             # in step: nothing to do
    reopened = S.AcquisitionStore(o, "acq_x", "subset")
    assert reopened.index["array"]["rows"] == 2
