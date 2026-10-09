"""Per element: fraction of its cross products that are exactly zero / NaN in a mid-band block.

A bad-feed mask applied by N2Accumulate zeroes every baseline of a masked element, so this
shows the applied mask in files that do not record it.  Reads three files per acquisition.
"""
import glob
import json
import os
import sys

import h5py
import hdf5plugin  # noqa: F401
import numpy as np

ROOT = "/mnt/cs00/data/kotekan_vis_files/subset"
out = {}
for d in sorted(glob.glob(ROOT + "/acq_*")):
    files = sorted(glob.glob(d + "/*.h5"))
    if not files:
        continue
    res = []
    for p in [files[len(files) // 4], files[len(files) // 2], files[(3 * len(files)) // 4]]:
        with h5py.File(p, "r") as f:
            prod = f["index_map/prod"][()]
            ne = int(f.attrs["num_elements"])
            fr = f["index_map/freq"]["centre"]
            i0 = int(np.searchsorted(fr, 600.0))
            v = f["vis"][i0:i0 + 64, :, :]  # (64, nprod, nt)
            w = f["vis_weight"][i0:i0 + 64, :, :] if "vis_weight" in f else None
        a, b = prod["input_a"], prod["input_b"]
        live_cell = np.isfinite(v) & (v != 0)
        # cells where at least one product is nonzero (the frequency/time had data)
        has = live_cell.any(axis=1)
        frac = []
        wzero = []
        for e in range(ne):
            m = ((a == e) | (b == e)) & (a != b)
            c = live_cell[:, m, :]
            denom = has[:, None, :].repeat(m.sum(), axis=1)
            frac.append(float(1 - c[denom].mean()) if denom.any() else None)
            if w is not None:
                ww = w[:, m, :]
                wzero.append(float((ww[denom] == 0).mean()) if denom.any() else None)
        res.append({"file": os.path.basename(p), "zero_frac": frac, "weight_zero_frac": wzero,
                     "cells_with_data": float(has.mean())})
    out[os.path.basename(d)] = res
    print(os.path.basename(d), [round(x, 2) if x is not None else None for x in res[1]["zero_frac"]], flush=True)
json.dump(out, open(sys.argv[1], "w"))
