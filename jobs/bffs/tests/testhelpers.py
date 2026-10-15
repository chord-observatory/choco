"""Shared test helpers: synthetic kotekan files + frames. Not a test module."""

import json

import h5py
import numpy as np

import kotekan_io

_STR = h5py.string_dtype(encoding="utf-8")


def element_table(dish_labels, num_elements=None):
    """kotekan's per-element label table (chord.2021.10+988) for *dish_labels*.

    Returns ``(labels, pol)``: [P][D] order, ``num_elements //
    len(dish_labels)`` polarizations (default two), each element spelled
    dish label + ``p1``/``p2`` with its polarization index alongside —
    what ``index_map/label`` and ``index_map/pol`` hold.
    """
    ndish = len(dish_labels)
    npol = (int(num_elements) // ndish) if (num_elements and ndish) else 2
    labels = [f"{d}p{p + 1}" for p in range(npol) for d in dish_labels]
    pol = [p for p in range(npol) for _ in range(ndish)]
    return labels, pol


def _write_labels(im, labels, pol):
    im.create_dataset("label", data=np.array(labels, dtype=object), dtype=_STR)
    if pol is not None:
        im.create_dataset("pol", data=np.asarray(pol, dtype="i4"))


def write_normalized(path, labels, freq, auto, weight=None):
    """A kotekan file over one polarization of the given dishes, its
    autocorrelations *auto* (ntime, nfreq, nfeed) and their *weight*.

    *labels* are bare dish labels; the file names each as its pol-0
    element (``f0`` -> ``f0p1``), which reads back as ``f0X``.  The
    layout is :func:`write_chord_n2`'s, the one kotekan writes.
    """
    write_chord_n2(path, labels, freq, auto, num_elements=len(labels), weight=weight)


def write_chord_n2(path, labels, freq, power, num_elements=None, frames_added=None,
                   products=None, element_labels=None, weight=None):
    """CHORD hdf5N2Write layout: ``vis[freq, prod, time]``, per-element
    ``index_map/label`` + ``index_map/pol``, compound freq (centre, width),
    ``vis_weight`` at the root, ``frames_added``, and the ``num_elements``
    file attribute.

    *labels* are dish labels; the file carries kotekan's per-element table
    for them (:func:`element_table`) over ``num_elements`` (default
    ``2 * len(labels)``) elements, both polarization blocks of which are
    real.  power: (ntime, nfreq, n) autocorrelation diagonal values for
    the first ``n`` elements.  ``products`` (a list of ``(input_a,
    input_b)`` pairs) overrides the dense upper triangle — the subset
    product list a ``DishInputs``-layout file has.  ``element_labels``
    writes a label table verbatim, with no ``pol`` index, for the tests
    that check other layouts are refused.  ``weight`` (shaped like
    *power*) sets the autocorrelations' ``vis_weight``; the rest are 1.
    """
    nfeed = len(labels)
    num_elements = num_elements or 2 * nfeed
    ntime, nfreq, ndata = power.shape
    pairs = (list(products) if products is not None else
             [(i, j) for i in range(num_elements) for j in range(i, num_elements)])
    vis = np.zeros((nfreq, len(pairs), ntime), dtype="c8")
    vis_weight = np.ones(vis.shape, dtype="f4")
    for k, (i, j) in enumerate(pairs):
        if i == j and i < ndata:
            vis[:, k, :] = power[:, :, i].T
            if weight is not None:
                vis_weight[:, k, :] = np.asarray(weight)[:, :, i].T
    if element_labels is None:
        table, pol = element_table(labels, num_elements)
    else:
        table, pol = list(element_labels), None
    with h5py.File(path, "w") as f:
        f.attrs["num_elements"] = num_elements
        im = f.create_group("index_map")
        _write_labels(im, table, pol)
        fr = np.zeros(nfreq, dtype=[("centre", "f8"), ("width", "f8")])
        fr["centre"], fr["width"] = np.asarray(freq, "f8"), 0.1953125
        im.create_dataset("freq", data=fr)
        prod = np.zeros(len(pairs), dtype=[("input_a", "u2"), ("input_b", "u2")])
        prod["input_a"] = [p[0] for p in pairs]
        prod["input_b"] = [p[1] for p in pairs]
        im.create_dataset("prod", data=prod)
        f.create_dataset("vis", data=vis)
        f.create_dataset("vis_weight", data=vis_weight)
        # kotekan's own per-input flag state, (freq, input, time): present
        # as in real files, and never read by bffs.
        f.create_dataset("flags", data=np.ones((nfreq, num_elements, ntime), dtype="f4"))
        f.create_dataset("frames_added", data=(np.ones((nfreq, ntime), dtype="u1")
                                               if frames_added is None else frames_added))


def write_manual(path, bad):
    """Write an operator override file marking ``bad`` (a list of labels)."""
    path.write_text(json.dumps({"bad_inputs": list(bad)}))


def frame(nfeed, *, base=10.0, bad=None, dead=None, ntime=4, nfreq=8,
          measured=None):
    amp = np.full(nfeed, base, dtype=np.float64)
    for idx, value in (bad or {}).items():
        amp[idx] = value
    for idx in dead or []:
        amp[idx] = 0.0
    auto = np.broadcast_to(amp.astype("f4"), (ntime, nfreq, nfeed)).copy()
    weight = np.ones((ntime, nfreq, nfeed), dtype="f4")
    for idx in dead or []:
        weight[..., idx] = 0.0
    if measured is not None:
        measured = np.asarray(measured, dtype=bool)
        weight[..., ~measured] = 0.0
        auto[..., ~measured] = 0.0
    return kotekan_io.Frame(
        auto=auto, weight=weight,
        valid=np.ones((ntime, nfreq), bool),
        freq=np.linspace(400.0, 800.0, nfreq, dtype="f4"),
        measured=measured,
    )
