"""Shared test helpers: synthetic kotekan files + frames. Not a test module."""

import json

import h5py
import numpy as np

import kotekan_io

_STR = h5py.string_dtype(encoding="utf-8")


def write_normalized(path, labels, freq, auto, weight=None):
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("input", data=np.array(labels, dtype=object), dtype=_STR)
        im.create_dataset("freq", data=np.asarray(freq, dtype="f4"))
        f.create_dataset("auto", data=np.asarray(auto, dtype="f4"))
        if weight is not None:
            f.create_dataset("weight", data=np.asarray(weight, dtype="f4"))


def write_visibility(path, labels, freq, power):
    """power: (ntime, nfreq, nfeed) autocorrelation diagonal values."""
    nfeed = len(labels)
    pairs = [(i, j) for i in range(nfeed) for j in range(i, nfeed)]
    ntime, nfreq, _ = power.shape
    vis = np.zeros((ntime, nfreq, len(pairs)), dtype="c8")
    for k, (i, j) in enumerate(pairs):
        if i == j:
            vis[:, :, k] = power[:, :, i]
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("input", data=np.array(labels, dtype=object), dtype=_STR)
        im.create_dataset("freq", data=np.asarray(freq, dtype="f4"))
        prod = np.zeros(len(pairs), dtype=[("input_a", "i4"), ("input_b", "i4")])
        prod["input_a"] = [p[0] for p in pairs]
        prod["input_b"] = [p[1] for p in pairs]
        im.create_dataset("prod", data=prod)
        f.create_dataset("vis", data=vis)


def write_chord_n2(path, labels, freq, power, num_elements=None, frames_added=None):
    """CHORD hdf5N2Write layout: ``vis[freq, prod, time]``, ``index_map/label``,
    compound freq (centre, width), ``vis_weight`` at the root, ``frames_added``.

    power: (ntime, nfreq, nfeed) autocorrelation diagonal values for the
    labelled feeds; ``num_elements`` (default ``2 * nfeed``) adds the phantom
    higher elements CHORD carries beyond the labelled feeds.
    """
    nfeed = len(labels)
    num_elements = num_elements or 2 * nfeed
    ntime, nfreq, _ = power.shape
    pairs = [(i, j) for i in range(num_elements) for j in range(i, num_elements)]
    vis = np.zeros((nfreq, len(pairs), ntime), dtype="c8")
    for k, (i, j) in enumerate(pairs):
        if i == j and i < nfeed:
            vis[:, k, :] = power[:, :, i].T
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("label", data=np.array(labels, dtype=object), dtype=_STR)
        fr = np.zeros(nfreq, dtype=[("centre", "f8"), ("width", "f8")])
        fr["centre"], fr["width"] = np.asarray(freq, "f8"), 0.1953125
        im.create_dataset("freq", data=fr)
        prod = np.zeros(len(pairs), dtype=[("input_a", "u2"), ("input_b", "u2")])
        prod["input_a"] = [p[0] for p in pairs]
        prod["input_b"] = [p[1] for p in pairs]
        im.create_dataset("prod", data=prod)
        f.create_dataset("vis", data=vis)
        f.create_dataset("vis_weight", data=np.ones(vis.shape, dtype="f4"))
        # Real CHORD files carry a root-level `flags` DATASET (freq, input,
        # time) — unlike CHIME, where /flags is a group.  Present here so
        # the reader's group-vs-dataset detection is exercised.
        f.create_dataset("flags", data=np.ones((nfreq, num_elements, ntime), dtype="f4"))
        f.create_dataset("frames_added", data=(np.ones((nfreq, ntime), dtype="u1")
                                               if frames_added is None else frames_added))


def write_manual(path, bad):
    """Write an operator override file marking ``bad`` (a list of labels)."""
    path.write_text(json.dumps({"bad_inputs": list(bad)}))


def frame(nfeed, *, base=10.0, bad=None, dead=None, ntime=4, nfreq=8):
    amp = np.full(nfeed, base, dtype=np.float64)
    for idx, value in (bad or {}).items():
        amp[idx] = value
    for idx in dead or []:
        amp[idx] = 0.0
    auto = np.broadcast_to(amp.astype("f4"), (ntime, nfreq, nfeed)).copy()
    weight = np.ones((ntime, nfreq, nfeed), dtype="f4")
    for idx in dead or []:
        weight[..., idx] = 0.0
    return kotekan_io.Frame(
        auto=auto, weight=weight,
        valid=np.ones((ntime, nfreq), bool),
        freq=np.linspace(400.0, 800.0, nfreq, dtype="f4"),
    )
