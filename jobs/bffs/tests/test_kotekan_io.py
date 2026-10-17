"""Tests for kotekan_io — run with `pytest`. No network needed."""

import h5py
import numpy as np
import pytest

import kotekan_io
from testhelpers import write_chord_n2, write_normalized


def test_read_labels_spells_p_suffixes_as_xy(tmp_path):
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    assert list(kotekan_io.read_labels(path)) == ["f0X", "f1X"]


def test_read_autocorr_normalized(tmp_path):
    path = tmp_path / "n2.h5"
    auto = np.ones((2, 4, 3), "f4") * 7.0
    auto[..., 1] = 99.0
    write_normalized(path, ["f0", "f1", "f2"], np.linspace(400, 800, 4), auto)
    frame = kotekan_io.read_autocorr(path)
    assert (frame.nfeed, frame.ntime) == (3, 2)
    assert frame.auto[0, 0, 1] == 99.0


def test_read_autocorr_carries_the_file_axis(tmp_path):
    """Both layouts hand the file's own element labels back on the frame:
    the sources project by them, never by position."""
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    assert list(kotekan_io.read_autocorr(path).labels) == ["f0X", "f1X"]
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1", "B1"], [400.0], np.ones((1, 1, 4), "f4"),
                   num_elements=4)
    assert list(kotekan_io.read_autocorr(path).labels) == ["A1X", "B1X", "A1Y", "B1Y"]


def test_uniquify_labels_suffixes_duplicates():
    out = list(kotekan_io.uniquify_labels(["A1X", "Fake", "Fake", "B2Y"]))
    assert out == ["A1X", "Fake[1]", "Fake[2]", "B2Y"]


def test_read_autocorr_takes_recent_chunk(tmp_path):
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1"], [400.0, 500.0], np.ones((10, 2, 2), "f4"))
    frame = kotekan_io.read_autocorr(path, chunk=3)
    assert frame.ntime == 3  # only the most recent rows


def test_read_autocorr_missing_file(tmp_path):
    assert kotekan_io.read_autocorr(tmp_path / "absent.h5") is None


def test_chime_style_input_map_is_refused(tmp_path):
    # index_map/input marks a pre-2026-08 file regardless of label text.
    path = tmp_path / "chime.h5"
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("input", data=np.array(["f0", "f1"], dtype=object),
                          dtype=h5py.string_dtype(encoding="utf-8"))
    with pytest.raises(OSError, match="per-element label"):
        kotekan_io.read_labels(path)


def test_read_labels_xy_spelling(tmp_path):
    # kotekan PR #1695 (2026-09-22; files from 2026-10-01): the
    # polarization name is the suffix, vouched for by index_map/pol.
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A01", "B01"], [400.0], np.ones((1, 1, 4), "f4"),
                   num_elements=4)
    with h5py.File(path, "r+") as f:
        del f["index_map/label"]
        f["index_map"].create_dataset(
            "label", data=np.array(["A01X", "B01X", "A01Y", "B01Y"], dtype=object),
            dtype=h5py.string_dtype(encoding="utf-8"))
    assert list(kotekan_io.read_labels(path)) == ["A01X", "B01X", "A01Y", "B01Y"]
    with h5py.File(path, "r+") as f:
        del f["index_map/pol"]
    with pytest.raises(OSError, match="no index_map/pol to vouch"):
        kotekan_io.read_labels(path)


def test_read_labels_per_element_pd_order(tmp_path):
    # kotekan's table: the X block (p1) then the Y block (p2), one entry
    # per element, read position for position.
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1", "B1"], [400.0], np.ones((1, 1, 4), "f4"),
                   num_elements=4)
    with h5py.File(path, "r") as f:
        assert [x.decode() for x in f["index_map/label"][()]] == ["A1p1", "B1p1", "A1p2", "B1p2"]
    assert list(kotekan_io.read_labels(path)) == ["A1X", "B1X", "A1Y", "B1Y"]


def test_read_autocorr_keeps_second_pol(tmp_path):
    # Both polarization blocks are real elements — the Y-pol autos
    # (elements num_dishes..2*num_dishes-1) must survive.
    path = tmp_path / "chord.h5"
    power = np.ones((1, 2, 4), "f4") * 5.0
    power[..., 3] = 50.0  # B1Y
    write_chord_n2(path, ["A1", "B1"], [400.0, 500.0], power, num_elements=4)
    frame = kotekan_io.read_autocorr(path)
    assert frame.nfeed == 4
    np.testing.assert_allclose(frame.auto[0, 0], [5, 5, 5, 50])


def test_read_autocorr_subset_products_mark_measured(tmp_path):
    # DishInputs layout: the product list covers only the wired elements
    # (here dish A1, both pols = elements 0 and 2); the rest of the axis
    # is real but never correlated, and Frame.measured says so.
    path = tmp_path / "chord.h5"
    power = np.zeros((1, 1, 3), "f4")
    power[..., 0], power[..., 2] = 5.0, 7.0
    write_chord_n2(path, ["A1", "B1"], [400.0], power, num_elements=4,
                   products=[(0, 0), (0, 2), (2, 2)])
    frame = kotekan_io.read_autocorr(path)
    assert frame.nfeed == 4
    np.testing.assert_array_equal(frame.measured, [True, False, True, False])
    np.testing.assert_allclose(frame.auto[0, 0], [5, 0, 7, 0])


def test_read_autocorr_chord_frames_added_validity(tmp_path):
    # A channel the receiver never filled inside the window (an X-engine
    # node down) is invalid for every feed, in that cell only.
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 2), "f4")
    frames_added = np.ones((2, 3), "u1")  # [freq, time]
    frames_added[1, 2] = 0                # channel 1 missing in the newest row
    write_chord_n2(path, ["A1"], [400.0, 500.0], power, num_elements=2,
                   frames_added=frames_added)
    frame = kotekan_io.read_autocorr(path, chunk=2)
    np.testing.assert_array_equal(frame.valid, [[True, True], [True, False]])
    assert frame.tail_skipped == 0 and frame.file_ntime == 3


def test_read_autocorr_chord_skips_the_empty_tail(tmp_path):
    """A stopped acquisition leaves rows no frame ever reached; the
    window ends at the newest filled row instead, so those rows cannot
    read as every feed dead."""
    path = tmp_path / "chord.h5"
    power = np.ones((5, 2, 2), "f4")
    frames_added = np.ones((2, 5), "u1")
    frames_added[:, 3:] = 0               # the last two rows are empty
    write_chord_n2(path, ["A1"], [400.0, 500.0], power, num_elements=2,
                   frames_added=frames_added)
    frame = kotekan_io.read_autocorr(path, chunk=2)
    assert frame.ntime == 2               # rows 1 and 2
    assert frame.valid.all()
    assert frame.tail_skipped == 2 and frame.file_ntime == 5


def test_read_autocorr_chord_no_filled_rows_is_none(tmp_path):
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 2), "f4")
    write_chord_n2(path, ["A1"], [400.0, 500.0], power, num_elements=2,
                   frames_added=np.zeros((2, 3), "u1"))
    assert kotekan_io.read_autocorr(path) is None


def test_channel_blocks_spreads_aligned_blocks_over_the_band():
    freq = np.arange(128, dtype="f4")               # 8 blocks of 16
    assert kotekan_io.channel_blocks(freq, 16, None, None, None).size == 128
    assert kotekan_io.channel_blocks(freq, 16, None, None, 0).size == 128
    # the band 32..95 spans blocks 2..5; two of them -> the first and last
    sel = kotekan_io.channel_blocks(freq, 16, 32.0, 95.0, 2)
    assert sel.tolist() == list(range(32, 48)) + list(range(80, 96))
    # more blocks than the band has -> every block of the band
    assert kotekan_io.channel_blocks(freq, 16, 32.0, 95.0, 10).tolist() == list(range(32, 96))
    # a band edge inside a block keeps only the in-band channels
    assert kotekan_io.channel_blocks(freq, 16, 40.0, 50.0, 8).tolist() == list(range(40, 51))
    assert kotekan_io.channel_blocks(freq, 16, 500.0, 600.0, 8).size == 0


def test_read_autocorr_reads_only_the_chosen_blocks(tmp_path):
    path = tmp_path / "chord.h5"
    nfreq = 64
    power = np.ones((2, nfreq, 2), "f4") * np.arange(nfreq, dtype="f4")[None, :, None]
    freq = np.linspace(400, 400 + nfreq - 1, nfreq)
    write_chord_n2(path, ["A1"], freq, power, num_elements=2)
    # band = channels 16..47, two 16-channel blocks; one block -> the first
    frame = kotekan_io.read_autocorr(path, freq_lo=416.0, freq_hi=447.0, blocks=1)
    assert frame.freq.tolist() == freq[16:32].tolist()
    np.testing.assert_allclose(frame.auto[0, :, 0], np.arange(16, 32))
    assert frame.weight.shape == frame.auto.shape and frame.valid.shape == (2, 16)
    # the whole band when blocks is not set
    frame = kotekan_io.read_autocorr(path, freq_lo=416.0, freq_hi=447.0)
    assert frame.freq.tolist() == freq[16:48].tolist()


def test_read_autocorr_weight_is_the_delivered_cells(tmp_path):
    # vis_weight is not read: a delivered cell weighs 1 and an empty one 0
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 2), "f4")
    frames_added = np.ones((2, 3), "u1")
    frames_added[1, 2] = 0
    write_chord_n2(path, ["A1"], [400.0, 500.0], power, num_elements=2,
                   frames_added=frames_added, weight=np.full((3, 2, 2), 7.0, "f4"))
    frame = kotekan_io.read_autocorr(path, chunk=2)
    np.testing.assert_array_equal(frame.weight[..., 0], [[1, 1], [1, 0]])
    np.testing.assert_array_equal(frame.weight[..., 1], [[1, 1], [1, 0]])
