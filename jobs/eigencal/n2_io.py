"""n2_io — read kotekan N² output for transit calibration.

Read-only access to the same hdf5N2Write files bffs reads (the label
reading follows ``jobs/bffs/kotekan_io.py``), but eigencal needs the full
cross-correlation products over a time window, not just the newest
autocorrelation rows.  Both visibility layouts are decoded — CHIME-style
``vis[time, freq, prod]`` and CHORD hdf5N2Write ``vis[freq, prod, time]``
with compound freq, ``frames_added[freq, time]`` validity, root-level
``flags`` (kotekan's per-input flag state) and ``vis_weight``.  The time
axis is ``index_map/time`` (CHIME-style, integration starts) or CHORD's
root-level ``time_center_t_inst_ns`` (integration centres, unix ns); see
``_time_centres``.

Per-element geometry — polarisation, kotekan ``DishType`` and position
(``feed_positions_m``, grid frame, with ``grid_orientation`` to rotate it
to East/North/Up) — is exposed on ``N2Meta`` when the file carries it;
eigencal derives its feed layout from that instead of a hand-kept table.

The label axis is the *element* axis, read from kotekan's per-element
``index_map/label`` (chord.2021.10+988, acquisitions from 2026-09-11 on:
``A1p1`` … ``A1p2`` …, one entry per element) and spelled in choco's
names (``A1X`` … ``A1Y`` …) by ``choco.dishlabels.file_element_labels``.
Any other label layout — CHIME-style ``index_map/input``, a per-dish
table, pre-2026-08 per-element labels — is refused.
"""

from __future__ import annotations

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


# kotekan's DishType (lib/utils/CHORDTelescope.hpp), as hdf5N2Write records
# it per element in index_map/type.  Only array dishes carry sky signal
# worth calibrating; Fake entries are unbuilt placeholders and RFIDish
# entries are the RFI-monitor antennas outside the main array.
DISH_FAKE, DISH_ARRAY, DISH_RFI = -1, 0, 1


def input_labels(f: h5py.File) -> np.ndarray:
    """The file's ``index_map/label`` entries as str (see bffs/kotekan_io)."""
    arr = f["index_map"]["label"][()]
    return np.array([s.decode("utf-8", "replace") if isinstance(s, bytes) else str(s)
                     for s in arr])


# The label layout lives in choco.dishlabels, shared with bffs, waterfall
# and choco's own PDB cross-check.
from choco.dishlabels import file_element_labels  # noqa: E402


def element_labels(f: h5py.File) -> np.ndarray:
    """The element-axis labels of *f* in choco's names (``A1X``).

    Only kotekan's per-element layout (chord.2021.10+988, 2026-09-11 on)
    is accepted: ``index_map/label`` names every element of the file's
    own axis as dish label + ``p1``/``p2``, cross-checked against
    ``index_map/pol`` and ``num_elements``.  Anything else — CHIME-style
    ``index_map/input``, a per-dish table, pre-2026-08 per-element
    labels — is refused with ``OSError`` so the run reports degraded
    (exit 2) rather than select feeds against a guessed axis.
    """
    im = f["index_map"]
    if "input" in im or "label" not in im:
        raise OSError(
            "N2 file has no per-element label table (index_map/label): a "
            "CHIME-style or pre-2026-08 file — refusing to guess its element axis")
    num_elements = int(f.attrs.get("num_elements", 0) or 0) or None
    pol = im["pol"][()] if "pol" in im else None
    try:
        return np.array(file_element_labels(input_labels(f), num_elements, pol))
    except ValueError as e:
        raise OSError(
            "N2 file is not in kotekan's per-element label layout "
            f"(chord.2021.10+988, 2026-09-11 on): {e}") from e


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
    # Per-element geometry, None when the file does not carry it (see
    # _feed_geometry): kotekan polarisation index, DishType, position in
    # the telescope's grid frame, and the grid frame's orientation.
    pol: np.ndarray | None = None              # (nfeed,) int
    dish_type: np.ndarray | None = None        # (nfeed,) int, DISH_*
    position_m: np.ndarray | None = None       # (nfeed, 3) grid-frame metres
    grid_orientation: np.ndarray | None = None  # (3, 3) R: v_grid = R . v_topo


def _feed_geometry(f: h5py.File, nfeed: int):
    """Per-element pol, dish type, position and frame, where the file has them.

    kotekan's hdf5N2Write records, one entry per element of the file's own
    axis, ``index_map/pol`` and ``index_map/type`` (``DishType``, see
    ``DISH_*``) and the ``feed_positions_m`` attribute: (nfeed, 3) metres
    in the telescope's GRID frame, already dish pitch x grid index plus
    the per-dish ``feed_pos_disp_m`` displacement
    (``CHORDTelescope::station_id_to_feed_position_m``), so nothing needs
    adding.  ``grid_orientation`` is the rotation R with v_grid = R . v_topo,
    topo being East/North/Up (``lib/utils/geoUtil.hpp``).  Verified against
    a live file and the kotekan source, 2026-09-13.

    Each item is None when absent.  A present item of the wrong length is
    an inconsistent file and refused with ``OSError`` (degraded exit).
    """
    im = f["index_map"]

    def per_element(arr, name):
        arr = np.asarray(arr)
        if arr.ndim == 0 or arr.shape[0] != nfeed:
            raise OSError(f"N2 file {name} has {arr.shape[0] if arr.ndim else 0} "
                          f"entries for {nfeed} elements")
        return arr

    pol = per_element(im["pol"][()], "index_map/pol").astype(np.int64) \
        if "pol" in im else None
    typ = per_element(im["type"][()], "index_map/type").astype(np.int64) \
        if "type" in im else None
    pos = None
    if "feed_positions_m" in f.attrs:
        pos = per_element(f.attrs["feed_positions_m"],
                          "feed_positions_m attribute").astype(np.float64)
        if pos.ndim != 2 or pos.shape[1] != 3:
            raise OSError(f"N2 file feed_positions_m has shape {pos.shape}, "
                          f"expected ({nfeed}, 3)")
    rot = None
    if "grid_orientation" in f.attrs:
        rot = np.asarray(f.attrs["grid_orientation"], dtype=np.float64)
        if rot.shape != (3, 3):
            raise OSError(f"N2 file grid_orientation has shape {rot.shape}, "
                          "expected (3, 3)")
    return pol, typ, pos, rot


def enu_positions_m(meta: "N2Meta") -> np.ndarray:
    """Feed positions as (nfeed, 3) East/North/Up metres.

    kotekan gives positions in the grid frame with v_grid = R . v_topo
    (R = ``grid_orientation``, orthonormal), so v_topo = R^T . v_grid — as
    row vectors, ``positions @ R``.  Without a recorded orientation the
    grid frame is taken as East/North/Up as it stands (on the live array
    the two differ by a fraction of a degree).
    """
    if meta.position_m is None:
        raise ValueError(f"{meta.path} carries no feed positions")
    if meta.grid_orientation is None:
        return meta.position_m.copy()
    return meta.position_m @ meta.grid_orientation


def _time_centres(f: h5py.File) -> np.ndarray:
    """The file's time axis as unix seconds at each integration's centre.

    CHORD hdf5N2Write files (verified against a live file, 2026-09-13)
    carry no ``index_map/time``: the time axis is the root-level
    ``time_center_t_inst_ns`` (int64 unix ns), already the integration
    centre — its first value is ``frame0_unix_ns`` + ``fpga_start_tick``
    ticks + half a ``frame_length_fpga_ticks`` frame.  CHIME-style files
    have ``index_map/time`` — compound ``(fpga_count, ctime)`` or plain
    unix floats — stamped at the integration *start*, shifted here to
    centres by half the median cadence.  A file with neither is refused
    with ``OSError`` (degraded, not failed): an unknown writer layout,
    like an unknown label layout, is not something to guess at.
    """
    im = f["index_map"]
    if "time" in im:
        t = im["time"][()]
        if t.dtype.names:
            t = np.asarray(t["ctime"], dtype=np.float64)
        else:
            t = np.asarray(t, dtype=np.float64)
        if t.size > 1:
            t = t + 0.5 * np.median(np.abs(np.diff(t)))
        return t
    if "time_center_t_inst_ns" in f:
        ns = np.asarray(f["time_center_t_inst_ns"][()], dtype=np.int64).reshape(-1)
        # Split whole seconds from the remainder so float64 keeps the
        # sub-second part exact (a bare ns/1e9 rounds at ~0.2 us).
        return (ns // 1_000_000_000).astype(np.float64) \
            + (ns % 1_000_000_000).astype(np.float64) / 1e9
    raise OSError(
        "N2 file has no time axis (neither index_map/time nor "
        "time_center_t_inst_ns) — unknown writer layout, refusing to guess")


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

        t = _time_centres(f)

        prod = f["index_map"]["prod"][()]
        if prod.dtype.names:
            a = np.asarray(prod[prod.dtype.names[0]], dtype=np.int64)
            b = np.asarray(prod[prod.dtype.names[1]], dtype=np.int64)
        else:
            prod = np.asarray(prod)
            a, b = prod[:, 0].astype(np.int64), prod[:, 1].astype(np.int64)

        vis = f["vis"]
        time_first = vis.shape[1:] == (freq.size, a.size)

        pol, dish_type, position_m, grid_orientation = _feed_geometry(f, labels.size)

    return N2Meta(path=str(path), labels=labels, freq_mhz=freq,
                  freq_width_mhz=width, time=t, prod_a=a, prod_b=b,
                  time_first=time_first, pol=pol, dish_type=dish_type,
                  position_m=position_m, grid_orientation=grid_orientation)


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
    treated as all-good.  Live CHORD files (checked 2026-09-13) write
    ``flags[freq, element, time]`` as 0/1 float32 that does vary with
    frequency, so they take the all-good path here: applying them needs a
    per-frequency treatment in the fit, not a reduction in this reader.
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
