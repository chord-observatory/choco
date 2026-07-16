"""Tests for kotekan — run with `pytest`. No network needed."""

import h5py
import numpy as np

import kotekan_io
from testhelpers import write_chord_n2, write_normalized, write_visibility


def test_input_labels_reads_index_map(tmp_path):
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1", "f2"], [400.0], np.ones((1, 1, 3), "f4"))
    with h5py.File(path, "r") as f:
        labels = kotekan_io.input_labels(f)
    assert list(labels) == ["f0", "f1", "f2"]


def test_read_labels(tmp_path):
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    assert list(kotekan_io.read_labels(path)) == ["f0", "f1"]


def test_read_autocorr_normalized(tmp_path):
    path = tmp_path / "n2.h5"
    auto = np.ones((2, 4, 3), "f4") * 7.0
    auto[..., 1] = 99.0
    write_normalized(path, ["f0", "f1", "f2"], np.linspace(400, 800, 4), auto)
    frame = kotekan_io.read_autocorr(path)
    assert (frame.nfeed, frame.ntime) == (3, 2)
    assert frame.auto[0, 0, 1] == 99.0


def test_read_autocorr_visibility_diagonal(tmp_path):
    path = tmp_path / "vis.h5"
    power = np.ones((1, 2, 4), "f4") * 5.0
    power[..., 3] = 50.0
    write_visibility(path, ["a", "b", "c", "d"], np.linspace(400, 800, 2), power)
    frame = kotekan_io.read_autocorr(path)
    np.testing.assert_allclose(frame.auto[0, 0], [5, 5, 5, 50])


def test_read_autocorr_takes_recent_chunk(tmp_path):
    path = tmp_path / "n2.h5"
    write_normalized(path, ["f0", "f1"], [400.0, 500.0], np.ones((10, 2, 2), "f4"))
    frame = kotekan_io.read_autocorr(path, chunk=3)
    assert frame.ntime == 3  # only the most recent rows


def test_read_autocorr_missing_file(tmp_path):
    assert kotekan_io.read_autocorr(tmp_path / "absent.h5") is None


def test_read_labels_chord_label_fallback(tmp_path):
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1X", "A2X", "B1X"], [400.0], np.ones((1, 1, 3), "f4"))
    assert list(kotekan_io.read_labels(path)) == ["A1X", "A2X", "B1X"]


def test_read_autocorr_chord_layout(tmp_path):
    # vis[freq, prod, time], compound freq, phantom elements beyond the labels
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 4), "f4") * 5.0
    power[..., 2] = 50.0
    write_chord_n2(path, ["A1X", "A2X", "B1X", "B2X"], [400.0, 500.0], power)
    frame = kotekan_io.read_autocorr(path, chunk=2)
    assert (frame.nfeed, frame.ntime) == (4, 2)  # phantoms dropped, recent rows
    np.testing.assert_allclose(frame.auto[0, 0], [5, 5, 50, 5])
    np.testing.assert_allclose(frame.freq, [400.0, 500.0])
    assert frame.weight.min() == 1.0 and frame.valid.all()


def test_read_autocorr_chord_frames_added_validity(tmp_path):
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 2), "f4")
    frames_added = np.ones((2, 3), "u1")  # [freq, time]
    frames_added[:, 2] = 0                # newest time column never arrived
    write_chord_n2(path, ["A1X", "A1Y"], [400.0, 500.0], power, frames_added=frames_added)
    frame = kotekan_io.read_autocorr(path, chunk=2)
    np.testing.assert_array_equal(frame.valid, [[True, True], [False, False]])
