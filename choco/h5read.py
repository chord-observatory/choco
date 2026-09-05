"""Read an HDF5 file for the web UI — as a subprocess, never in-process.

h5py (and the numpy it drags along) does blocking C-extension work, and
the web process is a gevent hub: an import that costs ~90 ms and a read
that blocks the loop is exactly what the "jobs are separate processes"
rule exists to keep out of it.  So this module is run as

    python -m choco.h5read manifest <file>
    python -m choco.h5read data <file> <dataset>

and imports h5py *inside* main(), so importing ``choco.h5read`` from the
web process (for the constants below) costs nothing.

``manifest`` writes JSON on stdout: the plottable datasets with the same
descriptor vocabulary the buffer-plot API already speaks (value_type,
extents, dimnames), plus the file's attributes and a summary of the
index maps.  ``data`` writes one dataset's raw bytes, C-order and
little-endian, which is precisely what the plotter's byte-prefix
contract expects — so an HDF5 dataset plots through the existing stack
with no new client code.

Exit codes follow the jobs convention: 2 for "the file or dataset isn't
what we hoped" (the caller degrades), 1 for a bug here.
"""
from __future__ import annotations

import json
import sys

# numpy kind + itemsize -> the value_type names bufferplot.js decodes.
# Anything absent is simply not offered for plotting.
_VALUE_TYPES = {
    ("f", 2): "float16", ("f", 4): "float32", ("f", 8): "float64",
    ("i", 1): "int8", ("i", 2): "int16", ("i", 4): "int32", ("i", 8): "int64",
    ("u", 1): "uint8", ("u", 2): "uint16", ("u", 4): "uint32",
    ("u", 8): "uint64",
    ("c", 8): "complex64", ("c", 16): "complex128",
}

# Datasets bigger than this are still listed, but the caller is expected
# to fetch them by prefix rather than whole.
MAX_DATASET_BYTES = 256 * 1024 * 1024


def _value_type(dtype):
    """The plotter's name for a dtype, or None if it can't be plotted."""
    if dtype.names:            # compound (index_map/freq, index_map/input)
        return None
    return _VALUE_TYPES.get((dtype.kind, dtype.itemsize))


def _as_served(dset, np):
    """The dataset as the plotter will receive it: ``(array, value_type)``.

    One rule lives here, used by both ``manifest`` and ``data`` so the two
    cannot disagree: a complex dataset whose imaginary part is identically
    zero is served as its real part -- float32 for complex64, float64 for
    complex128, half the bytes.  fpga_master's gain archive is the case in
    point: its *format* is complex (``DigitalGainArchive`` allows complex
    gains) but its *content* is real -- verified against the live file,
    every imaginary part exactly 0 -- and a part selector over data that
    has no parts is noise.  The check is kept rather than stripping
    unconditionally because it costs one scan of an array already in
    hand and turns the premise failing into the correct behaviour: a
    dataset that does carry imaginary content is served complex exactly
    as before, and the plotter's part selector reappears on its own.
    """
    arr = np.ascontiguousarray(dset[()])
    if arr.dtype.kind == "c" and not np.any(arr.imag):
        arr = np.ascontiguousarray(arr.real)
    return arr, _value_type(arr.dtype)


def _dimnames(dset, ndim):
    """Axis names, from the CHIME/CHORD-style ``axis`` attribute."""
    axis = dset.attrs.get("axis")
    if axis is None:
        return [f"dim{i}" for i in range(ndim)]
    names = [a.decode() if isinstance(a, bytes) else str(a) for a in axis]
    # Trust the file only as far as it agrees with the shape.
    if len(names) != ndim:
        return [f"dim{i}" for i in range(ndim)]
    return names


def _scalar(value):
    """JSON-safe rendering of an attribute or small dataset value."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [_scalar(v) for v in value]
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


def _index_map_summary(h5py, f):
    """Human-facing facts from index_map, which is not plottable itself.

    The frequency centres and input names are the two things an operator
    reads the axes against; the plot draws index ticks, so these are
    surfaced as text rather than pretended into the axes.
    """
    out = {}
    group = f.get("index_map")
    if not isinstance(group, h5py.Group):
        return out
    freq = group.get("freq")
    if freq is not None and getattr(freq.dtype, "names", None) \
            and "centre" in freq.dtype.names and len(freq):
        centres = freq["centre"]
        out["freq"] = {
            "n": int(len(centres)),
            "first_mhz": float(centres[0]),
            "last_mhz": float(centres[-1]),
        }
    inputs = group.get("input")
    if inputs is not None and getattr(inputs.dtype, "names", None) \
            and "correlator_input" in inputs.dtype.names:
        names = [_scalar(v) for v in inputs["correlator_input"][:]]
        out["inputs"] = {"n": len(names), "names": names}
    return out


def manifest(h5py, path):
    import numpy as np

    datasets, attrs, scalars = [], {}, {}
    with h5py.File(path, "r") as f:
        for key, value in f.attrs.items():
            attrs[key] = _scalar(value)

        def visit(name, obj):
            if not isinstance(obj, h5py.Dataset):
                return
            value_type = _value_type(obj.dtype)
            if value_type is None:
                # Not plottable, but a one-element string dataset is
                # exactly how update_id is stored — keep it as a fact.
                if obj.size and obj.size <= 8:
                    scalars[name] = _scalar(obj[()])
                return
            if obj.size and obj.size <= 8 and obj.ndim <= 1:
                scalars[name] = _scalar(obj[()])
            nbytes = int(obj.dtype.itemsize) * int(obj.size)
            if obj.dtype.kind == "c" and nbytes <= MAX_DATASET_BYTES:
                # The real-content rule needs the values; only complex
                # datasets that ``data`` could serve pay for the read.
                arr, value_type = _as_served(obj, np)
                nbytes = int(arr.nbytes)
            datasets.append({
                "name": name,
                "value_type": value_type,
                "extents": [int(n) for n in obj.shape],
                "dimnames": _dimnames(obj, obj.ndim),
                "bytes": nbytes,
            })

        f.visititems(visit)
        summary = _index_map_summary(h5py, f)
    datasets.sort(key=lambda d: -d["bytes"])
    return {"datasets": datasets, "attrs": attrs, "scalars": scalars,
            "index_map": summary}


def data(h5py, path, name):
    import numpy as np

    with h5py.File(path, "r") as f:
        dset = f.get(name)
        if dset is None or not isinstance(dset, h5py.Dataset):
            raise LookupError(f"no dataset '{name}'")
        if _value_type(dset.dtype) is None:
            raise LookupError(f"dataset '{name}' has no plottable dtype")
        if dset.dtype.itemsize * dset.size > MAX_DATASET_BYTES:
            raise LookupError(f"dataset '{name}' is too large to serve")
        arr, _ = _as_served(dset, np)
    # Little-endian on the wire: the browser reads it with typed arrays,
    # which are native-endian, and every platform choco serves is LE.
    if arr.dtype.byteorder == ">":
        arr = arr.astype(arr.dtype.newbyteorder("<"))
    return arr.tobytes()


def main(argv):
    try:
        import h5py  # noqa: F401  (imported here, never at module scope)
    except ImportError as exc:
        print(f"h5py is not installed: {exc}", file=sys.stderr)
        return 2
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 1
    mode, path = argv[1], argv[2]
    try:
        if mode == "manifest":
            json.dump(manifest(h5py, path), sys.stdout)
            return 0
        if mode == "data":
            if len(argv) < 4:
                print("data mode needs a dataset name", file=sys.stderr)
                return 1
            sys.stdout.buffer.write(data(h5py, path, argv[3]))
            return 0
    except (OSError, LookupError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    print(f"unknown mode '{mode}'", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
