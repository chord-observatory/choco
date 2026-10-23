"""Cross-pol vs co-pol transit amplitude per element pair at a bright transit.

For the best-covered transit of each bright source in an acquisition, reads all products of
one frequency chunk (16 channels) near 610 MHz, subtracts the stationary (crosstalk) term estimated off
source, and records the median |V - c| on source for every product.  A dish whose X and Y
feeds are swapped relative to the array shows cross-pol (X_i Y_j) amplitudes larger than
co-pol (X_i X_j) ones.
"""
import glob
import os

import h5py
import hdf5plugin  # noqa: F401
import numpy as np

from extract import chunk_rows
from transit import BIN_S, file_span, file_start, ha_deg, src_radec

FC = 610.0


def run(d, source):
    files = sorted(glob.glob(os.path.join(d, "*.h5")))
    if not files:
        return None
    starts = np.array([file_start(p) for p in files])
    span = file_span(files[0])
    grid = np.arange(starts[0], starts[-1] + span, 300.0)
    ra, dec = src_radec(source, grid[len(grid) // 2])
    h = ha_deg(ra, grid)
    cross = np.where((h[:-1] < 0) & (h[1:] >= 0))[0]
    w = 1.2 * np.degrees(300.0 / FC / 6.0) / np.cos(np.radians(dec))  # FWHM in HA deg
    best = None
    for i in cross:
        tt = grid[i] + 300 * (-h[i]) / (h[i + 1] - h[i])
        half = 2.6 * w / 15 * 3600
        sel = np.where((starts > tt - half - span) & (starts < tt + half))[0]
        cov = len(sel) * span / (2 * half)
        if best is None or cov > best[1]:
            best = (tt, cov, sel)
    if best is None or best[1] < 0.7:
        return None
    tt, cov, sel = best
    ra, dec = src_radec(source, tt)
    vs, ts = [], []
    with h5py.File(files[sel[0]], "r") as f:
        fr = f["index_map/freq"]["centre"]
        cs = chunk_rows(f["vis"])                   # one chunk: it holds every product
        c0 = (int(np.searchsorted(fr, FC)) // cs) * cs
        prod = f["index_map/prod"][()]
    for k in sel:
        t0 = starts[k]
        tmid = t0 + span / 2
        hh = ha_deg(ra, tmid)
        if abs(hh) > 0.6 * w and (k % 2):
            continue  # off-source: every other file is enough
        with h5py.File(files[k], "r") as f:
            v = f["vis"][c0:c0 + cs, :, :]
            nt = v.shape[2]
            t = None
            if "time_center_t_inst_ns" in f:
                x = f["time_center_t_inst_ns"][()].astype(float) / 1e9
                if np.all(x > 1e9):
                    t = x
            if t is None:
                t = t0 + (np.arange(nt) + 0.5) * BIN_S
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
    amp_on = np.nanmedian(np.abs(v[:, :, on] - c[:, :, None]), axis=2)  # (cs, nprod)
    amp_off = np.nanmedian(np.abs(v[:, :, off] - c[:, :, None]), axis=2)
    a = np.nanmedian(amp_on, axis=0)
    b = np.nanmedian(amp_off, axis=0)
    return {"source": source, "transit_unix": tt, "coverage": cov,
            "prod_a": prod["input_a"].tolist(), "prod_b": prod["input_b"].tolist(),
            "amp_on": a.tolist(), "amp_off": b.tolist()}



def analyse(d):
    """Every bright transit with coverage, the in-beam source first.  With mixed pointing
    each source is in the beam of only some dishes (2026-10-18: rows A-B on Cyg A, the
    others on Tau A), so one transit cannot check them all.  ``result`` is the first, as
    before."""
    acq = os.path.basename(d)
    # before the summer re-pointing the dishes looked near Dec +20, where the Sun is the
    # bright in-beam source; afterwards Cyg A is
    order = ("Sun", "CygA", "TauA") if acq < "acq_20260601" else ("CygA", "TauA", "Sun")
    results = [r for r in (run(d, s) for s in order) if r]
    return {"acq": acq, "result": results[0] if results else None, "results": results}
