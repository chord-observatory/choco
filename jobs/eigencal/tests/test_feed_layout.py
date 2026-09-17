"""The feed layout derived from the N² file's own geometry."""

import numpy as np
import pytest

import eigencal
import n2_io

LABELS = np.array(["A1X", "A2X", "F1X", "RFIA1X", "A1Y", "A2Y", "F1Y", "RFIA1Y"])
POL = np.array([0, 0, 0, 0, 1, 1, 1, 1])
TYPE = np.array([0, 0, -1, 1, 0, 0, -1, 1])            # A: array, F: Fake, RFI: RFIDish
POS = np.array([[0.0, 0.0, 0.0], [6.3, 0.0, 0.0], [12.6, 17.0, 0.0], [44.1, 59.5, 0.0]] * 2)


def _meta(**over):
    fields = dict(path="x.h5", labels=LABELS, freq_mhz=np.array([400.0]),
                  freq_width_mhz=np.array([1.0]), time=np.array([0.0]),
                  prod_a=np.array([0]), prod_b=np.array([0]), time_first=False,
                  pol=POL, dish_type=TYPE, position_m=POS, grid_orientation=None)
    fields.update(over)
    return n2_io.N2Meta(**fields)


def test_only_array_dishes_are_calibrated():
    pols, feed_idx, ref_pos, dist = eigencal.layout_from_file(_meta(), {})
    assert pols == ["X", "Y"]
    assert [list(LABELS[i]) for i in feed_idx] == [["A1X", "A2X"], ["A1Y", "A2Y"]]
    # Default reference: the first array-dish element of each pol.
    assert ref_pos == [0, 0]
    assert np.allclose(dist[1], [6.3, 0.0])              # A2X relative to A1X
    assert np.allclose(dist[5], [6.3, 0.0])              # A2Y relative to A1Y


def test_configured_phase_reference_is_used():
    pols, feed_idx, ref_pos, dist = eigencal.layout_from_file(
        _meta(), {"X": "A2X", "Y": "A2Y"})
    assert ref_pos == [1, 1]
    assert np.allclose(dist[0], [-6.3, 0.0])             # A1X relative to A2X
    assert np.allclose(dist[1], [0.0, 0.0])


@pytest.mark.parametrize("ref", ["F1X", "RFIA1X", "A1Y", "nope"])
def test_bad_phase_reference_is_a_config_error(ref):
    """A placeholder, an RFI antenna, the other polarisation, or an
    unknown label cannot anchor pol X: ValueError -> exit 1."""
    with pytest.raises(ValueError, match="phase_reference X"):
        eigencal.layout_from_file(_meta(), {"X": ref})


def test_positions_are_rotated_to_east_north():
    th = np.radians(90.0)                                # grid x-axis = North
    rot = np.array([[np.cos(th), np.sin(th), 0.0], [-np.sin(th), np.cos(th), 0.0],
                    [0.0, 0.0, 1.0]])
    _, _, _, dist = eigencal.layout_from_file(_meta(grid_orientation=rot), {})
    assert np.allclose(dist[1], [0.0, 6.3])              # 6.3 m along grid x -> North


def test_file_without_geometry_is_degraded_not_failed():
    with pytest.raises(OSError, match="no feed geometry"):
        eigencal.layout_from_file(_meta(dish_type=None), {})


def test_yaml_override_wins(tmp_path):
    layout = tmp_path / "feeds.yaml"
    layout.write_text("phase_reference: {X: A1X}\n"
                      "feeds:\n  - {label: A1X, pol: X, ew_m: 0, ns_m: 0}\n"
                      "  - {label: F1X, pol: X, ew_m: 1, ns_m: 2}\n")
    pols, feed_idx, _, dist = eigencal.feed_layout({"feed_layout": str(layout)}, _meta())
    assert pols == ["X"]
    assert list(LABELS[feed_idx[0]]) == ["A1X", "F1X"]  # the YAML decides, type ignored
    assert np.allclose(dist[2], [1.0, 2.0])


def test_no_override_uses_the_file():
    pols, feed_idx, _, _ = eigencal.feed_layout({"feed_layout": None,
                                                 "phase_reference": {}}, _meta())
    assert pols == ["X", "Y"] and [len(i) for i in feed_idx] == [2, 2]
