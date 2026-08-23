"""The HDF5 subprocess reader.

Builds a file shaped like fpga_master's DigitalGainArchive and reads it
back the way the web layer does — as a subprocess, which is the whole
point of the module: h5py must not be importable from the gevent hub's
side of the fence, so these tests shell out exactly as ``GainArchive``
does rather than calling into it.

Skipped where h5py isn't installed: it belongs to the ``[jobs]`` extra,
which production installs and a core-only environment does not.
"""

import json
import subprocess
import sys

import pytest

h5py = pytest.importorskip("h5py")
np = pytest.importorskip("numpy")


@pytest.fixture
def gain_file(tmp_path):
    """A miniature of the real archive: same names, axes and dtypes."""
    path = tmp_path / "gain.h5"
    nfreq, ninput = 4, 3
    with h5py.File(path, "w") as f:
        f.attrs["acquisition_name"] = "20260808T053625Z_digitalgain"
        f.attrs["instrument_name"] = "chord_pathfinder"
        coeff = f.create_dataset(
            "gain_coeff",
            data=np.arange(nfreq * ninput, dtype=np.float32)
                   .reshape(1, nfreq, ninput).astype(np.complex64),
        )
        coeff.attrs["axis"] = np.array(["update_time", "freq", "input"],
                                       dtype=object)
        exp = f.create_dataset("gain_exp",
                               data=np.full((1, ninput), -1, dtype=np.int32))
        exp.attrs["axis"] = np.array(["update_time", "input"], dtype=object)
        index_map = f.create_group("index_map")
        freq = np.zeros(nfreq, dtype=[("centre", "<f8"), ("width", "<f8")])
        freq["centre"] = [400.0, 400.5, 401.0, 401.5]
        freq["width"] = 0.5
        index_map.create_dataset("freq", data=freq)
        inputs = np.zeros(ninput, dtype=[("chan_id", "<u2"),
                                         ("correlator_input", "S32")])
        inputs["chan_id"] = range(ninput)
        inputs["correlator_input"] = [b"in000", b"in001", b"in002"]
        index_map.create_dataset("input", data=inputs)
        index_map.create_dataset("update_time", data=np.array([1786167385.9]))
        f.create_dataset("update_id",
                         data=np.array([b"digitalgain_20260808T053625Z"],
                                       dtype=h5py.special_dtype(vlen=bytes)))
    return path


def run(*args):
    return subprocess.run([sys.executable, "-m", "choco.h5read", *args],
                          capture_output=True)


class TestManifest:
    def test_lists_plottable_datasets_with_axes(self, gain_file):
        proc = run("manifest", str(gain_file))
        assert proc.returncode == 0, proc.stderr
        m = json.loads(proc.stdout)
        by_name = {d["name"]: d for d in m["datasets"]}
        assert by_name["gain_coeff"]["value_type"] == "complex64"
        assert by_name["gain_coeff"]["extents"] == [1, 4, 3]
        # The axis attribute is what gives the plot its dimension names.
        assert by_name["gain_coeff"]["dimnames"] == \
            ["update_time", "freq", "input"]
        assert by_name["gain_exp"]["value_type"] == "int32"
        assert by_name["gain_coeff"]["bytes"] == 1 * 4 * 3 * 8

    def test_biggest_dataset_first(self, gain_file):
        m = json.loads(run("manifest", str(gain_file)).stdout)
        assert m["datasets"][0]["name"] == "gain_coeff"

    def test_compound_datasets_are_not_offered_for_plotting(self, gain_file):
        m = json.loads(run("manifest", str(gain_file)).stdout)
        names = [d["name"] for d in m["datasets"]]
        # index_map/freq and /input are compound: no flat dtype to plot,
        # so they are summarised instead of listed.
        assert "index_map/freq" not in names
        assert "index_map/input" not in names
        assert m["index_map"]["freq"] == {"n": 4, "first_mhz": 400.0,
                                          "last_mhz": 401.5}
        assert m["index_map"]["inputs"]["names"] == ["in000", "in001", "in002"]

    def test_attributes_and_scalars_come_through(self, gain_file):
        m = json.loads(run("manifest", str(gain_file)).stdout)
        assert m["attrs"]["instrument_name"] == "chord_pathfinder"
        # update_id is a variable-length string, unplottable but the one
        # thing that says *which* gains these are.
        assert m["scalars"]["update_id"] == ["digitalgain_20260808T053625Z"]

    def test_missing_axis_attribute_falls_back(self, tmp_path):
        path = tmp_path / "plain.h5"
        with h5py.File(path, "w") as f:
            f.create_dataset("x", data=np.zeros((2, 3), dtype="f4"))
        m = json.loads(run("manifest", str(path)).stdout)
        assert m["datasets"][0]["dimnames"] == ["dim0", "dim1"]

    def test_wrong_length_axis_attribute_is_not_trusted(self, tmp_path):
        path = tmp_path / "bad_axis.h5"
        with h5py.File(path, "w") as f:
            d = f.create_dataset("x", data=np.zeros((2, 3), dtype="f4"))
            d.attrs["axis"] = np.array(["only_one"], dtype=object)
        m = json.loads(run("manifest", str(path)).stdout)
        assert m["datasets"][0]["dimnames"] == ["dim0", "dim1"]


class TestData:
    def test_bytes_match_the_array_exactly(self, gain_file):
        proc = run("data", str(gain_file), "gain_coeff")
        assert proc.returncode == 0, proc.stderr
        with h5py.File(gain_file, "r") as f:
            expected = np.ascontiguousarray(f["gain_coeff"][()])
        assert proc.stdout == expected.tobytes()
        # C-order with the pairs interleaved is what the plotter decodes
        # complex64 as: 24 values -> 8 complex -> 12 floats... check the
        # first complex value survives the trip.
        got = np.frombuffer(proc.stdout, dtype="<c8")
        assert got[0] == expected.ravel()[0]

    def test_big_endian_is_written_little(self, tmp_path):
        path = tmp_path / "be.h5"
        with h5py.File(path, "w") as f:
            f.create_dataset("x", data=np.arange(4, dtype=">i4"))
        out = run("data", str(path), "x").stdout
        # The browser reads with typed arrays, which are native-endian.
        assert np.frombuffer(out, dtype="<i4").tolist() == [0, 1, 2, 3]

    def test_unknown_dataset_exits_2(self, gain_file):
        proc = run("data", str(gain_file), "nope")
        assert proc.returncode == 2
        assert b"no dataset" in proc.stderr

    def test_compound_dataset_is_refused(self, gain_file):
        proc = run("data", str(gain_file), "index_map/freq")
        assert proc.returncode == 2
        assert b"plottable" in proc.stderr

    def test_missing_file_exits_2(self, tmp_path):
        proc = run("manifest", str(tmp_path / "absent.h5"))
        assert proc.returncode == 2

    def test_unknown_mode_exits_1(self, gain_file):
        # A bug in the caller, not bad input: different exit code.
        assert run("wat", str(gain_file)).returncode == 1
