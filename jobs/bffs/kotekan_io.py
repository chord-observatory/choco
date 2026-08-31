"""kotekan — read the feed labels and N² autocorrelation from a kotekan file.

Read-only. Shared by bffs (for the feed axis) and the power-outlier source.
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
    """Return ordered feed labels from the kotekan file's index map, as-is.

    CHIME-style files carry ``index_map/input``, written as a compound dtype
    (e.g. ``(chan_id, correlator_input)``), so the labels are a field of the
    record, not the record itself: use the ``correlator_input`` serial when
    present, else the first field. CHORD N² files (hdf5N2Write) carry
    ``index_map/label`` instead — one plain string per dish entry (see
    :func:`element_labels` for what "dish entry" means per layout).
    """
    im = f["index_map"]
    arr = (im["input"] if "input" in im else im["label"])[()]
    if arr.dtype.names:
        field = "correlator_input" if "correlator_input" in arr.dtype.names else arr.dtype.names[0]
        arr = arr[field]
    # h5py yields bytes for variable-length UTF-8 strings; decode each to str.
    return np.array([s.decode("utf-8", "replace") if isinstance(s, bytes) else str(s) for s in arr])


# -- element axis: per-element vs per-dish label layouts --------------------

#: A label that names a polarization is a per-element label: the wiring
#: convention's trailing X/Y after the dish number (``A1X``), or a ``_p<pol>``
#: marker (``d0_pA``, simulated configs).  A label without one names a whole
#: dish (``A1``, ``CHORD-A01``) — the 2026-08 kotekan layout.
_PER_ELEMENT_LABEL = re.compile(r"\d[XY]$|_p\w$")

#: Polarization suffixes for derived per-element labels, by polarization
#: index: 0 = X, 1 = Y.  Matches the pre-2026-08 per-element labels, so the
#: label-keyed hardware maps (pdb_map.csv, fpga_map.csv, manual overrides)
#: keep working unchanged across the layout change.
POL_SUFFIXES = "XY"


def labels_are_per_element(labels) -> bool:
    """True when *labels* use the pre-2026-08 per-element convention.

    Old ``dish_inputs`` tables (and the N² files written from them) name
    every correlator input separately, carrying a polarization marker;
    the 2026-08 kotekan layout names each *dish* once, with the
    polarizations as separate element blocks.  The two layouts are
    structurally identical — same table shape, same label count — so the
    label text is the only distinguishing mark.
    """
    return any(_PER_ELEMENT_LABEL.search(str(label)) for label in labels)


def expand_dish_labels(dish_labels, num_polarizations: int = 2) -> np.ndarray:
    """Per-element labels from per-dish labels, in the CHORD [P][D] order.

    Mirrors ``CHORDTelescope::encode_station_id``: ``element = dish_idx +
    pol * num_dishes``, so all of polarization 0 (X) comes first, then
    polarization 1 (Y) — ``A1`` at dish index i expands to ``A1X`` at
    element i and ``A1Y`` at element i + num_dishes.  Placeholder dishes
    expand like any other (``FakeX``/``FakeY``); duplicates are the
    caller's problem (bffs uniquifies by element index).
    """
    out = []
    for pol in range(int(num_polarizations)):
        suffix = POL_SUFFIXES[pol] if pol < len(POL_SUFFIXES) else f"P{pol}"
        out.extend(f"{label}{suffix}" for label in dish_labels)
    return np.array(out)


def element_labels(f: h5py.File) -> np.ndarray:
    """The element-axis labels of *f*, per-dish labels expanded.

    CHIME-style files (``index_map/input``) are per-element by
    definition.  CHORD files (``index_map/label``) carry kotekan's
    ``fill_input_maps`` output, which the 2026-08 layout made per-dish —
    one label per dish for a num_polarizations × num_dishes element axis
    — so those are expanded to per-element labels in [P][D] order, with
    the polarization count taken from the file's ``num_elements``
    attribute (default 2).  Pre-2026-08 files keep their labels as-is.
    """
    labels = input_labels(f)
    if "input" in f["index_map"] or labels_are_per_element(labels):
        return labels
    num_elements = int(f.attrs.get("num_elements", 0) or 0)
    npol = 2
    if labels.size and num_elements >= labels.size and num_elements % labels.size == 0:
        npol = num_elements // labels.size
    return expand_dish_labels(labels, npol)


def read_labels(path: str | Path) -> np.ndarray:
    """The element-axis labels from the kotekan file's index map.

    Per-dish labels (the 2026-08 CHORD layout) come back expanded to
    per-element labels — see :func:`element_labels`.
    """
    with h5py.File(path, "r") as f:
        return element_labels(f)


@dataclass(frozen=True)
class Frame:
    """The most recent block of autocorrelation data read from the kotekan file."""

    auto: np.ndarray    # (ntime, nfreq, nfeed) power
    weight: np.ndarray  # (ntime, nfreq, nfeed)
    valid: np.ndarray   # (ntime, nfreq) bool
    freq: np.ndarray    # (nfreq,)
    # (nfeed,) bool: which feeds the file's product list carries an
    # autocorrelation for.  None means all of them (the `auto` layout,
    # and dense-triangle files).  A subset layout (kotekan's DishInputs)
    # only correlates the wired elements — the rest have no data *by
    # construction*, which is different from a wired feed gone silent,
    # and sources must not read the gap as "dead".
    measured: np.ndarray | None = None

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
    (:func:`element_labels`): products beyond it are dropped — for
    pre-2026-08 CHORD files that is the phantom second-polarization
    elements, while 2026-08 per-dish files expand to the full element
    count first, so both polarizations' autos are kept. Returns
    ``None`` if the file is missing or has no time rows.
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
        measured = np.zeros(nfeed, dtype=bool)
        measured[feed_idx] = True
    return Frame(auto=auto, weight=weight, valid=valid, freq=freq,
                 measured=measured)
