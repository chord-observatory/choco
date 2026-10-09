"""Cross-pol vs co-pol transit amplitude per element pair at a bright transit.

For the best-covered Cyg A transit (else the Sun) in an acquisition, reads all products of
one 32-channel block near 610 MHz, subtracts the stationary (crosstalk) term estimated off
source, and records the median |V - c| on source for every product.  A dish whose X and Y
feeds are swapped relative to the array shows cross-pol (X_i Y_j) amplitudes larger than
co-pol (X_i X_j) ones.
"""
import json
import os
import sys

import h5py
import hdf5plugin  # noqa: F401
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from transit import ROOT, file_start, ha_deg, src_radec  # noqa: E402
import glob  # noqa: E402

FC = 610.0


def run(acq, source):
    files = sorted(glob.glob(os.path.join(ROOT, acq, "*.h5")))
    if not files:
        return None
    starts = np.array([file_start(p) for p in files])
    grid = np.arange(starts[0], starts[-1] + 200, 300.0)
    ra, dec = src_radec(source, grid[len(grid) // 2])
    h = ha_deg(ra, grid)
    cross = np.where((h[:-1] < 0) & (h[1:] >= 0))[0]
    w = 1.2 * np.degrees(300.0 / FC / 6.0) / np.cos(np.radians(dec))  # FWHM in HA deg
    best = None
    for i in cross:
        tt = grid[i] + 300 * (-h[i]) / (h[i + 1] - h[i])
        half = 2.6 * w / 15 * 3600
        sel = np.where((starts > tt - half - 200) & (starts < tt + half))[0]
        cov = len(sel) * 200 / (2 * half)
        if best is None or cov > best[1]:
            best = (tt, cov, sel)
    if best is None or best[1] < 0.7:
        return None
    tt, cov, sel = best
    ra, dec = src_radec(source, tt)
    vs, ts = [], []
    with h5py.File(files[sel[0]], "r") as f:
        fr = f["index_map/freq"]["centre"]
        c0 = (int(np.searchsorted(fr, FC)) // 32) * 32
        prod = f["index_map/prod"][()]
    for k in sel:
        t0 = starts[k]
        tmid = t0 + 100
        hh = ha_deg(ra, tmid)
        if abs(hh) > 0.6 * w and (k % 2):
            continue  # off-source: every other file is enough
        with h5py.File(files[k], "r") as f:
            v = f["vis"][c0:c0 + 32, :, :]
            nt = v.shape[2]
            t = None
            if "time_center_t_inst_ns" in f:
                x = f["time_center_t_inst_ns"][()].astype(float) / 1e9
                if np.all(x > 1e9):
                    t = x
            if t is None:
                t = t0 + (np.arange(nt) + 0.5) * 10
        vs.append(v)
        ts.append(t)
    v = np.concatenate(vs, axis=2)  # (32, nprod, nt)
    t = np.concatenate(ts)
    hh = ha_deg(ra, t)
    on = np.abs(hh) < 0.25 * w
    off = (np.abs(hh) > 1.4 * w) & (np.abs(hh) < 2.6 * w)
    if on.sum() < 3 or off.sum() < 6:
        return None
    v = np.where(np.isfinite(v), v, np.nan)
    c = np.nanmedian(v[:, :, off].real, axis=2) + 1j * np.nanmedian(v[:, :, off].imag, axis=2)
    amp_on = np.nanmedian(np.abs(v[:, :, on] - c[:, :, None]), axis=2)  # (32, nprod)
    amp_off = np.nanmedian(np.abs(v[:, :, off] - c[:, :, None]), axis=2)
    a = np.nanmedian(amp_on, axis=0)
    b = np.nanmedian(amp_off, axis=0)
    return {"source": source, "transit_unix": tt, "coverage": cov,
            "prod_a": prod["input_a"].tolist(), "prod_b": prod["input_b"].tolist(),
            "amp_on": a.tolist(), "amp_off": b.tolist()}


if __name__ == "__main__":
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    for acq in sys.argv[2:]:
        r = None
        # before the summer re-pointing the dishes looked near Dec +20, where the Sun is the
        # bright in-beam source; afterwards Cyg A is
        order = ("Sun", "CygA") if acq < "acq_20260601" else ("CygA", "Sun")
        for s in order:
            r = run(acq, s)
            if r:
                break
        json.dump({"acq": acq, "result": r}, open(os.path.join(out, acq + ".json"), "w"))
        print(acq, r["source"] if r else None, flush=True)
