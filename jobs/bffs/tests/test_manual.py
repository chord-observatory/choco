"""Tests for the manual source — run with `pytest`."""

import numpy as np

from sources import manual


def test_manual_marks_bad_labels(tmp_path):
    path = tmp_path / "manual.yaml"
    path.write_text("bad_inputs: [b]\n")
    out, rep = manual.mask({"path": str(path)}, np.array(["a", "b", "c"]), None)
    np.testing.assert_array_equal(out, [True, False, True])
    assert rep["status"] == "ok" and rep["n_measured"] == 3
    assert rep["detail"]["n_listed"] == 1 and rep["detail"]["exists"] is True


def test_manual_missing_file_is_all_good(tmp_path):
    out, rep = manual.mask({"path": str(tmp_path / "absent.yaml")}, np.array(["a", "b"]), None)
    assert out.all()
    assert rep["detail"]["exists"] is False and rep["detail"]["n_listed"] == 0


def test_manual_reports_labels_not_on_the_axis(tmp_path):
    # a typo, or a label from an older dish_inputs table: flags nothing,
    # and the page should say so rather than leave the operator guessing
    path = tmp_path / "manual.yaml"
    path.write_text("bad_inputs: [b, A1x, zz]\n")
    out, rep = manual.mask({"path": str(path)}, np.array(["a", "b"]), None)
    np.testing.assert_array_equal(out, [True, False])
    assert rep["status"] == "ok"
    assert rep["detail"]["not_on_axis"] == ["A1x", "zz"]
