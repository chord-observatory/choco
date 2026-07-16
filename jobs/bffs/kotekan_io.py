"""kotekan — read the feed labels and N² autocorrelation from a kotekan file.

Read-only. Shared by bffs (for the feed axis) and the power-outlier source.
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


def input_labels(f: h5py.File) -> np.ndarray:
    """Return ordered feed labels from the kotekan file's index map.

    CHIME-style files carry ``index_map/input``, written as a compound dtype
    (e.g. ``(chan_id, correlator_input)``), so the labels are a field of the
    record, not the record itself: use the ``correlator_input`` serial when
    present, else the first field. CHORD N² files (hdf5N2Write) carry
    ``index_map/label`` instead — one plain string per dish entry, in element
    order (on the pathfinder every cabled input is its own dish entry at
    polarization 0, so a label's position is its element index).
    """
    im = f["index_map"]
    arr = (im["input"] if "input" in im else im["label"])[()]
    if arr.dtype.names:
        field = "correlator_input" if "correlator_input" in arr.dtype.names else arr.dtype.names[0]
        arr = arr[field]
    # h5py yields bytes for variable-length UTF-8 strings; decode each to str.
    return np.array([s.decode("utf-8", "replace") if isinstance(s, bytes) else str(s) for s in arr])


def read_labels(path: str | Path) -> np.ndarray:
    """Feed labels (the axis) from the kotekan file's ``index_map/input``."""
    with h5py.File(path, "r") as f:
        return input_labels(f)


@dataclass(frozen=True)
class Frame:
    """The most recent block of autocorrelation data read from the kotekan file."""

    auto: np.ndarray    # (ntime, nfreq, nfeed) power
    weight: np.ndarray  # (ntime, nfreq, nfeed)
    valid: np.ndarray   # (ntime, nfreq) bool
    freq: np.ndarray    # (nfreq,)

    @property
    def ntime(self) -> int:
        return self.auto.shape[0]

    @property
    def nfeed(self) -> int:
        return self.auto.shape[2]


def read_autocorr(path: str | Path, *, chunk: int = 16) -> Frame | None:
    """Read the most recent ``chunk`` time rows of kotekan N² output as a Frame.

    Accepts an ``auto[time, freq, feed]`` dataset (per-feed power, ready to
    use) or kotekan's visibility products, whose autocorrelation diagonal is
    extracted — laid out either ``vis[time, freq, prod]`` (CHIME-style) or
    ``vis[freq, prod, time]`` (CHORD hdf5N2Write; told apart by matching the
    axes against the index map). Products beyond the labelled feeds (e.g.
    CHORD's phantom second-polarization elements) are dropped. Returns
    ``None`` if the file is missing or has no time rows.
    """
    if not Path(path).exists():
        return None
    with h5py.File(path, "r") as f:
        labels = input_labels(f)
        freq = f["index_map"]["freq"][()]
        if freq.dtype.names:  # kotekan freq_ctype: (centre MHz, width MHz)
            freq = freq["centre"]
        freq = np.asarray(freq, dtype=np.float32).reshape(-1)
        nfeed, nfreq = labels.shape[0], freq.shape[0]

        if "auto" in f:  # the `auto` layout: per-feed power, ready to use
            ntime = f["auto"].shape[0]
            if ntime == 0:
                return None
            lo = max(0, ntime - int(chunk))
            nrows = ntime - lo
            auto = np.asarray(f["auto"][lo:ntime], dtype=np.float32)
            weight = (np.asarray(f["weight"][lo:ntime], dtype=np.float32)
                      if "weight" in f else np.ones_like(auto))
            valid = (np.asarray(f["valid"][lo:ntime], dtype=bool)
                     if "valid" in f else np.ones((nrows, nfreq), dtype=bool))
            return Frame(auto=auto, weight=weight, valid=valid, freq=freq)

        # visibility products: the autocorrelation diagonal (input_a == input_b)
        # of the labelled feeds
        prod = f["index_map"]["prod"][()]
        if prod.dtype.names:
            a, b = np.asarray(prod[prod.dtype.names[0]]), np.asarray(prod[prod.dtype.names[1]])
        else:
            prod = np.asarray(prod)
            a, b = prod[:, 0], prod[:, 1]
        diag = np.nonzero((a == b) & (a < nfeed))[0]
        feed_idx = a[diag]

        vis = f["vis"]
        time_first = vis.shape[1:] == (nfreq, len(a))  # vis[time, freq, prod]
        ntime = vis.shape[0] if time_first else vis.shape[2]
        if ntime == 0:
            return None
        lo = max(0, ntime - int(chunk))
        nrows = ntime - lo

        # CHIME keeps weights in a /flags GROUP; CHORD files instead have a
        # root-level `flags` DATASET (kotekan's own per-input flag state —
        # downstream of our flagging, so deliberately unused; see the README
        # appendix on the latch problem) plus `vis_weight` at the root.  The
        # isinstance check matters: `"x" in <Dataset>` iterates the data.
        weight_ds = None
        flags = f.get("flags")
        if isinstance(flags, h5py.Group) and "vis_weight" in flags:  # CHIME
            weight_ds = flags["vis_weight"]
        elif "vis_weight" in f:  # CHORD: at the root
            weight_ds = f["vis_weight"]

        if time_first:
            power = np.real(np.asarray(vis[lo:ntime])[..., diag]).astype(np.float32)
            wdiag = (np.asarray(weight_ds[lo:ntime], dtype=np.float32)[..., diag]
                     if weight_ds is not None else None)
            valid = np.ones((nrows, nfreq), dtype=bool)
        else:  # vis[freq, prod, time] -> (time, freq, prod)
            power = np.real(vis[:, diag, lo:ntime]).astype(np.float32).transpose(2, 0, 1)
            wdiag = (np.asarray(weight_ds[:, diag, lo:ntime], dtype=np.float32).transpose(2, 0, 1)
                     if weight_ds is not None else None)
            # frames_added[freq, time] tracks which (f, t) cells have data
            valid = (np.asarray(f["frames_added"][:, lo:ntime]).T > 0
                     if "frames_added" in f else np.ones((nrows, nfreq), dtype=bool))

        auto = np.zeros((nrows, nfreq, nfeed), dtype=np.float32)
        auto[..., feed_idx] = power
        weight = np.zeros_like(auto)
        weight[..., feed_idx] = 1.0 if wdiag is None else wdiag
    return Frame(auto=auto, weight=weight, valid=valid, freq=freq)
