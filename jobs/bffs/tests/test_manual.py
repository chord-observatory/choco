"""Tests for the manual source — run with `pytest`."""

import numpy as np

from sources import manual


def test_manual_marks_bad_labels(tmp_path):
    path = tmp_path / "manual.yaml"
    path.write_text("bad_inputs: [b]\n")
    out = manual.mask({"path": str(path)}, np.array(["a", "b", "c"]), None)
    np.testing.assert_array_equal(out, [True, False, True])


def test_manual_missing_file_is_all_good(tmp_path):
    out = manual.mask({"path": str(tmp_path / "absent.yaml")}, np.array(["a", "b"]), None)
    assert out.all()
