"""Per-element autocorrelation response to bright-source transits in one acquisition.

For each source transiting inside the acquisition (best-covered transit per source),
reads the autos of a few frequency bands around the transit and reports, per element and
band: the fractional power bump at transit, its significance, and the hour angle of the
peak (E-W beam centre).  Output: one JSON per acquisition.
"""
import datetime
import glob
import json
import os
import re
import sys

import h5py
import hdf5plugin  # noqa: F401
import numpy as np
from astropy import units as u
from astropy.coordinates import EarthLocation, SkyCoord, get_sun, AltAz
from astropy.time import Time
from astropy.utils import iers

iers.conf.auto_download = False

ROOT = "/mnt/cs00/data/kotekan_vis_files/subset"
LAT, LON = 49.32075144444, -119.62081125
SITE = EarthLocation(lat=LAT * u.deg, lon=LON * u.deg, height=545 * u.m)
SOURCES = {
    "CygA": (299.868, 40.734),
    "CasA": (350.858, 58.815),
    "TauA": (83.633, 22.015),
    "VirA": (187.706, 12.391),
    "Sun": None,
}
BANDS = [(440, 460), (600, 620), (1400, 1420)]
FNAME = re.compile(r"vis_\d+_(\d{8})T_(\d{6})_\d+\.h5$")


def file_start(p):
    m = FNAME.search(p)
    return datetime.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(
        tzinfo=datetime.timezone.utc).timestamp()


def src_radec(name, t_unix):
    if SOURCES[name] is None:
        s = get_sun(Time(t_unix, format="unix"))
        return float(s.ra.deg), float(s.dec.deg)
    return SOURCES[name]


def ha_deg(ra, t_unix):
    lst = Time(t_unix, format="unix", location=SITE).sidereal_time("apparent").deg
    return (np.asarray(lst) - ra + 180) % 360 - 180


def read_autos(path, chans_by_band):
    with h5py.File(path, "r") as f:
        prod = f["index_map/prod"][()]
        auto = np.where(prod["input_a"] == prod["input_b"])[0]
        el = prod["input_a"][auto]
        nt = f["vis"].shape[2]
        t = None
        if "time_center_t_inst_ns" in f:
            tt = f["time_center_t_inst_ns"][()].astype(float) / 1e9
            if np.all(tt > 1e9):
                t = tt
        if t is None:
            t0 = file_start(path)
            t = t0 + (np.arange(nt) + 0.5) * 10.0
        out = []
        for sl in chans_by_band:
            v = f["vis"][sl, auto.tolist(), :]
            out.append(np.abs(v))  # (nch, nel, nt)
        return t, el, out


def analyse(acq):
    d = os.path.join(ROOT, acq)
    files = sorted(glob.glob(d + "/*.h5"))
    res = {"acq": acq, "transits": []}
    if not files:
        return res
    starts = np.array([file_start(p) for p in files])
    t_end = starts[-1] + 200
    with h5py.File(files[0], "r") as f:
        fr = f["index_map/freq"]["centre"]
        chans = []
        for lo, hi in BANDS:
            idx = np.where((fr >= lo) & (fr < hi))[0]
            if idx.size:
                c0 = (int(idx.min()) // 32) * 32  # one 32-channel chunk
                chans.append(slice(c0, c0 + 32))
            else:
                chans.append(None)
        lab = [x.decode() if isinstance(x, bytes) else str(x) for x in f["index_map/label"][()]] \
            if "index_map/label" in f else None
    bands = [(b, c) for b, c in zip(BANDS, chans) if c is not None]
    res["labels_in_file"] = lab
    for name in SOURCES:
        # transit times inside the acquisition (sample every 5 min)
        grid = np.arange(starts[0], t_end, 300.0)
        ra, dec = src_radec(name, (starts[0] + t_end) / 2)
        h = ha_deg(ra, grid)
        cross = np.where((h[:-1] < 0) & (h[1:] >= 0))[0]
        best = None
        for i in cross:
            tt = grid[i] + 300 * (-h[i]) / (h[i + 1] - h[i])
            ra, dec = src_radec(name, tt)
            lam_m = 0.3 / 0.45
            w_ha = 1.2 * np.degrees(lam_m / 6.0) / max(np.cos(np.radians(dec)), 0.2)
            half = min(max(3.2 * w_ha / 15.0 * 3600, 1.6 * 3600), 2.6 * 3600)
            sel = np.where((starts > tt - half - 200) & (starts < tt + half))[0]
            cov = len(sel) * 200 / (2 * half)
            if best is None or cov > best[1]:
                best = (tt, cov, sel, half, dec, ra)
        if best is None or best[1] < 0.6:
            continue
        tt, cov, sel, half, dec, ra = best
        ts, pw = [], [[] for _ in bands]
        el = None
        for k in sel[::3]:
            try:
                t, el, outs = read_autos(files[k], [c for _, c in bands])
            except Exception as e:  # noqa
                continue
            ts.append(t)
            for j, o in enumerate(outs):
                pw[j].append(o)
        if not ts:
            continue
        t = np.concatenate(ts)
        h = ha_deg(ra, t)
        tr = {"source": name, "transit_unix": tt, "dec": dec, "coverage": cov,
              "elements": el.tolist(), "bands": []}
        for j, (band, _) in enumerate(bands):
            p = np.concatenate(pw[j], axis=2)  # (nch, nel, nt)
            p = np.where(p > 0, p, np.nan)
            # normalise each channel by its median over time, then median over channels
            with np.errstate(all="ignore"):
                pn = p / np.nanmedian(p, axis=2, keepdims=True)
                rel = np.nanmedian(pn, axis=0)  # (nel, nt)
            fc = 0.5 * (band[0] + band[1])
            w = 1.2 * np.degrees(300.0 / fc / 6.0) / max(np.cos(np.radians(dec)), 0.2)
            on = np.abs(h) < 0.15 * w
            off = (np.abs(h) > 1.5 * w) & (np.abs(h) < 3.5 * w)
            rows = []
            for e in range(rel.shape[0]):
                y = rel[e]
                ok = np.isfinite(y)
                if (ok & on).sum() < 3 or (ok & off & (h < 0)).sum() < 3 or (ok & off & (h > 0)).sum() < 3:
                    rows.append(None)
                    continue
                c = np.polyfit(h[ok & off], y[ok & off], 1)
                base = np.polyval(c, h)
                r = y / base - 1
                rms = float(np.nanstd(r[ok & off]))
                bump = float(np.nanmedian(r[ok & on]))
                # peak HA from a log-quadratic fit over the upper half of the bump
                hc = wd = None
                near = ok & (np.abs(h) < 1.2 * w)
                if bump > 5 * rms and bump > 0.005:
                    m = near & (r > 0.4 * np.nanmax(r[near]))
                    if m.sum() >= 5:
                        q = np.polyfit(h[m], np.log(np.clip(r[m], 1e-6, None)), 2)
                        if q[0] < 0:
                            hc = float(-q[1] / (2 * q[0]))
                            wd = float(np.sqrt(-4 * np.log(2) / q[0]))
                rows.append({"bump": bump, "rms": rms, "ha_peak": hc, "fwhm_ha": wd})
            tr["bands"].append({"band": band, "fwhm_ha_expected": w, "rows": rows})
        res["transits"].append(tr)
    return res


if __name__ == "__main__":
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    for acq in sys.argv[2:]:
        r = analyse(acq)
        with open(os.path.join(out, acq + ".json"), "w") as fh:
            json.dump(r, fh)
        print(acq, [(t["source"], round(t["coverage"], 2)) for t in r["transits"]], flush=True)
