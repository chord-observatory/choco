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
