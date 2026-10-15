"""choco.n2file: the N² axis readers every job shares (jobs only)."""

import subprocess
import sys

import numpy as np
import pytest

h5py = pytest.importorskip("h5py")

from choco import n2file  # noqa: E402

STR = h5py.string_dtype(encoding="utf-8")


def _file(path, labels, pol=(0, 0, 1, 1), num_elements=4, extra=None):
    with h5py.File(path, "w") as f:
        f.attrs["num_elements"] = num_elements
        im = f.create_group("index_map")
        im.create_dataset("label", data=np.array(labels, dtype=object), dtype=STR)
        if pol is not None:
            im.create_dataset("pol", data=np.array(pol, dtype="i4"))
        freq = np.zeros(2, dtype=[("centre", "<f8"), ("width", "<f8")])
        freq["centre"], freq["width"] = [400.0, 400.390625], 0.390625
        im.create_dataset("freq", data=freq)
        prod = np.zeros(3, dtype=[("input_a", "<u2"), ("input_b", "<u2")])
        prod["input_a"], prod["input_b"] = [0, 0, 1], [0, 1, 1]
        im.create_dataset("prod", data=prod)
        if extra:
            extra(f)
    return h5py.File(path, "r")


def test_labels_axes(tmp_path):
    with _file(tmp_path / "a.h5", ["B4p1", "A1p1", "B4p2", "A1p2"]) as f:
        assert n2file.element_labels(f) == ["B4X", "A1X", "B4Y", "A1Y"]
        centre, width = n2file.freq_axis(f)
        assert centre.tolist() == [400.0, 400.390625] and width.tolist() == [0.390625] * 2
        a, b = n2file.prod_axis(f)
        assert a.dtype == np.int64 and (a.tolist(), b.tolist()) == ([0, 0, 1], [0, 1, 1])


@pytest.mark.parametrize("labels,pol,extra,match", [
    (["A1X", "A1Y", "B1X", "B1Y"], None, None, "no index_map/pol"),     # pre-2026-08 text
    (["A1", "B1"], (0, 0), None, "2 entries for 4 elements"),           # a per-dish table
    (["A1p1", "B1p1", "A1p2", "B1p2"], (0, 0, 1, 1),
     lambda f: f["index_map"].create_dataset("input", data=np.zeros(4)), "CHIME-style"),
])
def test_other_layouts_are_refused(tmp_path, labels, pol, extra, match):
    with _file(tmp_path / "a.h5", labels, pol, extra=extra) as f:
        with pytest.raises(ValueError, match=match):
            n2file.element_labels(f)


def test_the_web_process_imports_no_scientific_stack():
    """CLAUDE.md: the web process never imports numpy, h5py, astropy or
    matplotlib, nor the jobs-only modules that would pull them in."""
    out = subprocess.run(
        [sys.executable, "-c",
         "import sys, choco.app, choco.web; "
         "print(sorted(m for m in ('numpy', 'h5py', 'astropy', 'matplotlib', "
         "'choco.n2file', 'choco.healpix') if m in sys.modules))"],
        capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"
