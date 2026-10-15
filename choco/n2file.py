"""The axes of a kotekan N² file (hdf5N2Write), read once for every job.

h5py and numpy: jobs only, never imported by the web process (choco reads
HDF5 through the ``choco.h5read`` subprocess).  One layout is read, the one
kotekan writes: ``vis[freq, prod, time]`` with compound ``index_map/freq``
(centre, width) and ``index_map/prod`` (input_a, input_b), per-element
``index_map/label`` with ``index_map/pol``, and the ``num_elements``
attribute.  The element labels follow ``choco.dishlabels`` (see
:func:`element_labels`); what a refusal means is the caller's to decide —
bffs and eigencal exit degraded, the waterfall stores no labels.
"""

from __future__ import annotations

import h5py
# Registers HDF5 compression plugins with libhdf5 on import: kotekan
# writes vis/vis_weight bitshuffle-compressed (filter 32008).
import hdf5plugin  # noqa: F401
import numpy as np

from .dishlabels import file_element_labels


def element_labels(f: h5py.File) -> list[str]:
    """The element-axis labels of *f* in choco's names (``A1X``).

    Only the per-element layout kotekan writes since chord.2021.10+988
    (2026-09-11) is accepted: ``index_map/label`` names every element of
    the file's own axis as dish label + ``p1``/``p2`` (or + ``X``/``Y``
    since PR #1695), cross-checked against ``index_map/pol`` and the
    ``num_elements`` attribute (:func:`choco.dishlabels.file_element_labels`).
    Anything else — a CHIME-style ``index_map/input`` map, a per-dish
    table, pre-2026-08 ``A1X`` labels — raises ``ValueError``.
    """
    im = f["index_map"]
    if "input" in im or "label" not in im:
        raise ValueError(
            "no per-element label table (index_map/label): a CHIME-style or "
            "pre-2026-08 file — refusing to guess its element axis")
    num_elements = int(f.attrs.get("num_elements", 0) or 0) or None
    pol = im["pol"][()] if "pol" in im else None
    return file_element_labels(im["label"][()], num_elements, pol)


def freq_axis(f: h5py.File) -> tuple[np.ndarray, np.ndarray]:
    """``(centre, width)`` in MHz, one per frequency row of ``vis``."""
    freq = f["index_map"]["freq"][()]
    return (np.asarray(freq["centre"], dtype=np.float64).reshape(-1),
            np.asarray(freq["width"], dtype=np.float64).reshape(-1))


def prod_axis(f: h5py.File) -> tuple[np.ndarray, np.ndarray]:
    """``(input_a, input_b)``: the two elements of each product of ``vis``."""
    prod = f["index_map"]["prod"][()]
    return (np.asarray(prod["input_a"], dtype=np.int64),
            np.asarray(prod["input_b"], dtype=np.int64))
