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


def test_labels_are_per_element_conventions():
    assert kotekan_io.labels_are_per_element(["A1X", "Fake", "RFI01"])
    assert kotekan_io.labels_are_per_element(["d0_pA", "d0_pB"])
    assert not kotekan_io.labels_are_per_element(["A1", "D8", "Fake", "RFI01"])
    assert not kotekan_io.labels_are_per_element(["CHORD-A01", "CHORD-H08"])


def test_expand_dish_labels_pd_order():
    # [P][D]: element = dish_idx + pol * num_dishes, pol 0 = X.
    out = list(kotekan_io.expand_dish_labels(["A1", "Fake", "A3"]))
    assert out == ["A1X", "FakeX", "A3X", "A1Y", "FakeY", "A3Y"]


def test_read_labels_per_dish_expands(tmp_path):
    # 2026-08 layout: index_map/label is per dish; the element axis is
    # num_elements wide, X block then Y block.
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1", "B1"], [400.0], np.ones((1, 1, 4), "f4"),
                   num_elements=4)
    assert list(kotekan_io.read_labels(path)) == ["A1X", "B1X", "A1Y", "B1Y"]


def test_read_autocorr_per_dish_keeps_second_pol(tmp_path):
    # Both polarization blocks are real elements now — the Y-pol autos
    # (elements num_dishes..2*num_dishes-1) must survive, not be dropped
    # as phantoms.
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
    path = tmp_path / "chord.h5"
    power = np.ones((3, 2, 2), "f4")
    frames_added = np.ones((2, 3), "u1")  # [freq, time]
    frames_added[:, 2] = 0                # newest time column never arrived
    write_chord_n2(path, ["A1X", "A1Y"], [400.0, 500.0], power, frames_added=frames_added)
    frame = kotekan_io.read_autocorr(path, chunk=2)
    np.testing.assert_array_equal(frame.valid, [[True, True], [False, False]])
