"""kotekan — read the feed labels and N² autocorrelation from a kotekan file.

Read-only. Shared by bffs (to check the file against the flag axis) and the
power-outlier source (which judges the file's own axis and projects onto the
flag axis by label).
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
    """The file's ``index_map/label`` entries as str, in element order.

    kotekan writes them as variable-length UTF-8, which h5py yields as
    bytes.  Raises ``KeyError`` when the file has no label table.
    """
    arr = f["index_map"]["label"][()]
    return np.array([s.decode("utf-8", "replace") if isinstance(s, bytes) else str(s)
                     for s in arr])


# -- element axis: the label layout lives in choco.dishlabels (shared with
# eigencal, waterfall and choco's PDB cross-check).
from choco.dishlabels import file_element_labels  # noqa: E402


def element_labels(f: h5py.File) -> np.ndarray:
    """The element-axis labels of *f* in choco's names (``B4X``).

    Only the per-element layout kotekan writes since chord.2021.10+988
    (acquisitions from 2026-09-11 on) is accepted: ``index_map/label``
    names every element of the file's own axis as dish label +
    ``p1``/``p2`` (or + ``X``/``Y`` since kotekan PR #1695), cross-checked
    against ``index_map/pol`` and the ``num_elements`` attribute
    (:func:`choco.dishlabels.file_element_labels`).
    Anything else — a CHIME-style ``index_map/input`` map, a per-dish
    table, pre-2026-08 ``A1X`` labels — is refused with ``OSError`` so
    the job reports degraded (exit 2) rather than flag against a guessed
    axis, and heals once files from the current writer land.
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


def read_labels(path: str | Path) -> np.ndarray:
    """The element-axis labels from the kotekan file, one per element, in
    choco's names; any other label layout raises — see :func:`element_labels`.
    """
    with h5py.File(path, "r") as f:
        return element_labels(f)


def uniquify_labels(labels) -> np.ndarray:
    """Suffix repeated labels with their element index (Missing -> Missing[7]).

    Placeholder elements share the label ``Missing`` (``MissingX``/``MissingY``
    on a per-dish axis); state diffing and per-source projection key by
    label, so duplicates must be made per-element.  Unique labels pass
    through untouched.  The core applies this to the flag axis and the
    file-based sources to a file's own axis, so the two project onto each
    other by name.
    """
    from collections import Counter
    strs = [str(label) for label in labels]
    counts = Counter(strs)
    return np.array([f"{s}[{i}]" if counts[s] > 1 else s
                     for i, s in enumerate(strs)])


@dataclass(frozen=True)
class Frame:
    """The most recent block of autocorrelation data read from the kotekan file."""

    auto: np.ndarray    # (ntime, nfreq, nfeed) power
    weight: np.ndarray  # (ntime, nfreq, nfeed)
    valid: np.ndarray   # (ntime, nfreq) bool
    freq: np.ndarray    # (nfreq,)
    # (nfeed,) the file's own element axis in choco's names
    # (:func:`element_labels`).  A ``subset/`` file is a compact
    # ``DishInputs`` frame over the wired elements only (48 of the
    # correlator's 128), so this axis is a subset of the flag axis and
    # sources project their verdicts onto it by label, never by position.
    labels: np.ndarray | None = None
    # (nfeed,) bool: which feeds the file's product list carries an
    # autocorrelation for.  None means all of them (the `auto` layout,
    # and dense-triangle files).  A subset layout (kotekan's DishInputs)
    # only correlates the wired elements — the rest have no data *by
    # construction*, which is different from a wired feed gone silent,
    # and sources must not read the gap as "dead".
    measured: np.ndarray | None = None
    # Where the window sits in the file: the file's row count, and how
    # many empty rows at its end were skipped to reach the newest row
    # that holds any frame (a stopped acquisition leaves such a tail).
    file_ntime: int | None = None
    tail_skipped: int = 0

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
    axes against the index map). The feed axis is the element axis
    (:func:`element_labels`, one label per element); products beyond it
    are dropped.

    In the CHORD layout the window ends at the newest row with any
    frame (``frames_added[freq, time] > 0``), not at the file's last
    row: a stopped acquisition leaves a tail of empty rows, and judging
    feeds on those would read every feed as dead.  ``Frame.tail_skipped``
    says how many were passed over.  Returns ``None`` if the file is
    missing, has no time rows, or (CHORD) no row holds a frame.
    """
    if not Path(path).exists():
        return None
    with h5py.File(path, "r") as f:
        labels = element_labels(f)
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
            return Frame(auto=auto, weight=weight, valid=valid, freq=freq,
                         labels=labels, file_ntime=ntime)

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
        # frames_added[freq, time] (CHORD) tracks which (f, t) cells have
        # data.  It is small (nfreq x ntime counters), so read it whole
        # and end the window at the newest row that holds any frame.
        frames_added = None
        hi = ntime
        if not time_first and "frames_added" in f:
            frames_added = np.asarray(f["frames_added"][()])
            filled = np.flatnonzero(frames_added.sum(axis=0) > 0)
            if filled.size == 0:
                return None
            hi = int(filled[-1]) + 1
        tail_skipped = ntime - hi
        lo = max(0, hi - int(chunk))
        nrows = hi - lo

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
            power = np.real(np.asarray(vis[lo:hi])[..., diag]).astype(np.float32)
            wdiag = (np.asarray(weight_ds[lo:hi], dtype=np.float32)[..., diag]
                     if weight_ds is not None else None)
            valid = np.ones((nrows, nfreq), dtype=bool)
        else:  # vis[freq, prod, time] -> (time, freq, prod)
            power = np.real(vis[:, diag, lo:hi]).astype(np.float32).transpose(2, 0, 1)
            wdiag = (np.asarray(weight_ds[:, diag, lo:hi], dtype=np.float32).transpose(2, 0, 1)
                     if weight_ds is not None else None)
            valid = (frames_added[:, lo:hi].T > 0 if frames_added is not None
                     else np.ones((nrows, nfreq), dtype=bool))

        auto = np.zeros((nrows, nfreq, nfeed), dtype=np.float32)
        auto[..., feed_idx] = power
        weight = np.zeros_like(auto)
        weight[..., feed_idx] = 1.0 if wdiag is None else wdiag
        measured = np.zeros(nfeed, dtype=bool)
        measured[feed_idx] = True
    return Frame(auto=auto, weight=weight, valid=valid, freq=freq, labels=labels,
                 measured=measured, file_ntime=ntime, tail_skipped=tail_skipped)
