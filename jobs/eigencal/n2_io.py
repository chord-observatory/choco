"""n2_io — read kotekan N² output for transit calibration.

Read-only access to the same hdf5N2Write files bffs reads (the label
reading follows ``jobs/bffs/kotekan_io.py``), but eigencal needs the full
cross-correlation products over a time window, not just the newest
autocorrelation rows.  Both file flavours are handled:

* CHIME-style — ``index_map/input`` labels, ``vis[time, freq, prod]``;
* CHORD hdf5N2Write — ``index_map/label``, ``vis[freq, prod, time]``,
  compound freq, ``frames_added[freq, time]`` validity, root-level
  ``flags`` (kotekan's per-input flag state) and ``vis_weight``.

The label axis is the *element* axis: a 2026-08 per-dish label table
(one label per dish, ``A1``) is expanded to per-element labels in the
CHORD [P][D] order (``A1X`` … ``A1Y`` …), while pre-2026-08 per-element
tables are used as-is — and there products beyond the labelled feeds
(the phantom second-polarisation elements) are ignored throughout.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import h5py
import numpy as np

try:
    # Registers HDF5 compression plugins with libhdf5 on import — kotekan
    # writes vis/vis_weight bitshuffle-compressed (filter 32008).
    import hdf5plugin  # noqa: F401
except ImportError:  # uncompressed files remain readable without it
    pass


def input_labels(f: h5py.File) -> np.ndarray:
    """Ordered feed labels from the file's index map (see bffs/kotekan_io)."""
    im = f["index_map"]
    arr = (im["input"] if "input" in im else im["label"])[()]
    if arr.dtype.names:
        field = ("correlator_input" if "correlator_input" in arr.dtype.names
                 else arr.dtype.names[0])
        arr = arr[field]
    return np.array([s.decode("utf-8", "replace") if isinstance(s, bytes) else str(s)
                     for s in arr])


# Per-element vs per-dish label layouts — mirrors jobs/bffs/kotekan_io.py
# (labels_are_per_element / expand_dish_labels / element_labels); keep the
# two in step.  A label carrying a polarisation marker (``A1X``, ``d0_pA``)
# is per-element (pre-2026-08); a bare dish label (``A1``) is the 2026-08
# per-dish layout, whose element axis is [P][D]: element = dish_idx +
# pol * num_dishes, pol 0 = X.
_PER_ELEMENT_LABEL = re.compile(r"\d[XY]$|_p\w$")
POL_SUFFIXES = "XY"


def labels_are_per_element(labels) -> bool:
    """True when *labels* use the pre-2026-08 per-element convention."""
    return any(_PER_ELEMENT_LABEL.search(str(label)) for label in labels)


def expand_dish_labels(dish_labels, num_polarizations: int = 2) -> np.ndarray:
    """Per-element labels from per-dish labels, in the CHORD [P][D] order."""
    out = []
    for pol in range(int(num_polarizations)):
        suffix = POL_SUFFIXES[pol] if pol < len(POL_SUFFIXES) else f"P{pol}"
        out.extend(f"{label}{suffix}" for label in dish_labels)
    return np.array(out)


def element_labels(f: h5py.File) -> np.ndarray:
    """The element-axis labels of *f*, per-dish labels expanded.

    The polarisation count comes from the file's ``num_elements``
    attribute (default 2); CHIME-style ``index_map/input`` files are
    per-element by definition and pass through untouched.
    """
    labels = input_labels(f)
    if "input" in f["index_map"] or labels_are_per_element(labels):
        return labels
    num_elements = int(f.attrs.get("num_elements", 0) or 0)
    npol = 2
    if labels.size and num_elements >= labels.size and num_elements % labels.size == 0:
        npol = num_elements // labels.size
    return expand_dish_labels(labels, npol)


@dataclass(frozen=True)
class N2Meta:
    """Axis information for one N² file, cheap to read (index maps only)."""

    path: str
    labels: np.ndarray       # (nfeed,) str
    freq_mhz: np.ndarray     # (nfreq,)
    freq_width_mhz: np.ndarray  # (nfreq,)
    time: np.ndarray         # (ntime,) unix, integration centres
    prod_a: np.ndarray       # (nprod,) input index of each product
    prod_b: np.ndarray
    time_first: bool         # True: vis[time, freq, prod]; False: vis[freq, prod, time]


def read_meta(path: str | Path) -> N2Meta:
    with h5py.File(path, "r") as f:
        labels = element_labels(f)

        freq = f["index_map"]["freq"][()]
        if freq.dtype.names:  # kotekan freq_ctype: (centre MHz, width MHz)
            width = np.asarray(freq["width"], dtype=np.float64).reshape(-1)
            freq = np.asarray(freq["centre"], dtype=np.float64).reshape(-1)
        else:
            freq = np.asarray(freq, dtype=np.float64).reshape(-1)
            width = np.full_like(freq, np.median(np.abs(np.diff(freq))) if freq.size > 1 else 1.0)

        # Time axis: compound (fpga_count, ctime) or plain unix floats.
        # kotekan stamps the integration *start*; shift to centres.
        # VERIFY against the CHORD writer's convention.
        t = f["index_map"]["time"][()]
        if t.dtype.names:
            t = np.asarray(t["ctime"], dtype=np.float64)
        else:
            t = np.asarray(t, dtype=np.float64)
        if t.size > 1:
            t = t + 0.5 * np.median(np.abs(np.diff(t)))

        prod = f["index_map"]["prod"][()]
        if prod.dtype.names:
            a = np.asarray(prod[prod.dtype.names[0]], dtype=np.int64)
            b = np.asarray(prod[prod.dtype.names[1]], dtype=np.int64)
        else:
            prod = np.asarray(prod)
            a, b = prod[:, 0].astype(np.int64), prod[:, 1].astype(np.int64)

        vis = f["vis"]
        time_first = vis.shape[1:] == (freq.size, a.size)

    return N2Meta(path=str(path), labels=labels, freq_mhz=freq,
                  freq_width_mhz=width, time=t, prod_a=a, prod_b=b,
                  time_first=time_first)


def read_products(meta: N2Meta, prod_idx: np.ndarray, time_sel: np.ndarray,
                  freq_slice: slice) -> np.ndarray:
    """Read vis for the given (sorted) product indices -> (nt, nf, nprod_sel).

    ``time_sel`` is a sorted integer index array into the file's time axis;
    ``prod_idx`` must be strictly increasing (h5py fancy-index rule).
    """
    t0, t1 = int(time_sel[0]), int(time_sel[-1]) + 1
    trel = time_sel - t0
    with h5py.File(meta.path, "r") as f:
        vis = f["vis"]
        if meta.time_first:                      # vis[time, freq, prod]
            out = vis[t0:t1, freq_slice, prod_idx]
            out = out[trel]
        else:                                    # vis[freq, prod, time]
            out = vis[freq_slice, prod_idx, t0:t1]        # (nf, np, nt)
            out = np.moveaxis(out, -1, 0)[trel]           # (nt, nf, np)
    return np.ascontiguousarray(out)


def read_valid(meta: N2Meta, time_sel: np.ndarray, freq_slice: slice) -> np.ndarray:
    """(nt, nf) bool — which (time, freq) cells actually contain data."""
    nf = len(range(*freq_slice.indices(meta.freq_mhz.size)))
    with h5py.File(meta.path, "r") as f:
        if "frames_added" in f:                  # CHORD: frames_added[freq, time]
            fa = f["frames_added"][freq_slice, :][:, time_sel]
            return (np.asarray(fa) > 0).T
    return np.ones((time_sel.size, nf), dtype=bool)


def read_input_flags(meta: N2Meta, time_sel: np.ndarray) -> np.ndarray:
    """(nt, nfeed) bool — kotekan's per-input flag state (True = good).

    The root-level ``flags`` dataset is kotekan's own per-input flag state
    (what bffs & friends fed it).  Shape conventions vary — handle a static
    (nfeed,) vector and a per-time (ntime, nfeed) table; anything else is
    logged upstream and treated as all-good.  VERIFY once against a live
    CHORD file.
    """
    nfeed = meta.labels.size
    ones = np.ones((time_sel.size, nfeed), dtype=bool)
    with h5py.File(meta.path, "r") as f:
        flags = f.get("flags")
        if not isinstance(flags, h5py.Dataset):
            return ones
        arr = flags[()]
    if arr.ndim == 1 and arr.shape[0] >= nfeed:
        return np.broadcast_to(arr[:nfeed] > 0, (time_sel.size, nfeed)).copy()
    if arr.ndim == 2 and arr.shape[-1] >= nfeed and arr.shape[0] >= time_sel.max() + 1:
        return arr[time_sel, :nfeed] > 0
    return ones


def pol_products(meta: N2Meta, feeds: np.ndarray):
    """Products internal to one polarisation's feed set.

    Returns ``(prod_idx, ai, bi)``: sorted indices into the file's product
    axis, and for each selected product the positions of its two inputs
    within ``feeds`` — ready to scatter into an (mp, mp) matrix.
    """
    nlabel = meta.labels.size
    sub = np.full(nlabel, -1, dtype=np.int64)
    sub[feeds] = np.arange(feeds.size)
    inrange = (meta.prod_a < nlabel) & (meta.prod_b < nlabel)
    sel = inrange & (sub[np.minimum(meta.prod_a, nlabel - 1)] >= 0) \
                  & (sub[np.minimum(meta.prod_b, nlabel - 1)] >= 0)
    prod_idx = np.flatnonzero(sel)
    return prod_idx, sub[meta.prod_a[prod_idx]], sub[meta.prod_b[prod_idx]]
