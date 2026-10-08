#!/usr/bin/env python3
"""Drift-scan sky maps from kotekan N² files (choco job).

Each run folds the visibility files that have landed since the last run
into two maps on a sidereal grid, 4096 bins of RA by a strip of Dec rows
around the pointing, in colour bins that slice the spectrum and are shown
as hues from red (300 MHz) to violet (1500 MHz) in one image:

* the **sky map**, every pass over the sky accumulated, with samples
  skipped while the Sun is near the beam;
* the **daily map**, the most recent pass over each part of the sky.

Per file, the reducer (``reduce.py``) zeroes flagged elements and RFI,
accumulates the current calibrator transit's gain sums, divides out the
per-channel gains and averages channels into sub-bands; the imaging
(``imaging.py``) averages redundant baselines and scatter-adds each
sample through a precomputed beam-weighted kernel into the map
accumulators; ``render.py`` turns the accumulators into PNGs.

Calibration is self-contained: the brightest point source transiting
through the beam (Cyg A for the current pointing) gives per-channel,
per-product complex gains once per pass, and the maps are in units of
its flux density.  Until the first transit has been seen nothing can be
imaged, and the run reports degraded.

Exit codes follow the jobs convention: 0 ok or nothing to do, 2 degraded
(a mount or file unavailable, no calibration yet), 1 config error / bug.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import glob
import json
import logging
import math
import os
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import yaml

from choco.healpix import default_sky_map
from choco.jobclient import job_state_dir, write_json_atomic

import imaging as I
import reduce as R
import render

log = logging.getLogger("mapmaker")

# astropy is only used for the Sun and the calibrators' CIRS positions;
# never block a two-minute tick on the IERS servers (see jobs/skymap).
import astropy.utils.iers  # noqa: E402
astropy.utils.iers.conf.auto_download = False
astropy.utils.iers.conf.auto_max_age = None
warnings.filterwarnings("ignore", module="astropy")
warnings.filterwarnings("ignore", message=".*dubious year.*")
from astropy.coordinates import CIRS, SkyCoord, get_sun  # noqa: E402
from astropy.time import Time  # noqa: E402

DEFAULTS = {
    "roots": [],
    "pointing_dec_deg": None,
    "colour_bins": 16,
    "colour_range_mhz": [300.0, 1500.0],
    "subband_channels": 64,
    "bad_freq_mhz": [[350, 400], [600, 650], [700, 800], [850, 900], [1150, 1300]],
    "max_rfi_fraction": 0.05,
    "n_ra_bins": 4096,
    "dec_halfwidth_deg": 10.0,
    "dec_step_deg": 0.25,
    "dish_diameter_m": 6.0,
    "beam_fwhm_factor": 1.03,
    "footprint_fwhm": 1.2,
    "sun_exclusion_deg": 30.0,
    "calibrators": [
        {"name": "Cyg A", "ra_deg": 299.86815, "dec_deg": 40.73392},
        {"name": "Cas A", "ra_deg": 350.86642, "dec_deg": 58.81178},
        {"name": "Tau A", "ra_deg": 83.63308, "dec_deg": 22.01450},
        {"name": "Vir A", "ra_deg": 187.70593, "dec_deg": 12.39112},
    ],
    "calibrator_max_offset_deg": 5.0,
    "calibration_window_deg": 3.0,
    "min_cal_samples": 60,
    "dead_fraction": 0.05,
    "stretch": {"mode": "arcsinh", "floor_sigma": 3.0, "soft": 0.001, "max": 1.0},
    "exposure_floor": 0.02,
    "clean": {"gain": 0.25, "threshold_sigma": 5.0, "max_iterations": 20000,
              "beam_ra_deg": 0.6, "beam_dec_deg": 1.5,
              "stretch": {"mode": "arcsinh", "floor_sigma": 3.0, "soft": 0.001, "max": 1.0}},
    "row_repeat": 4,
    # the context image's backdrop, a HEALPix FITS ("" = no context image);
    # choco.sh install fetches the skymap job's copy, which this shares
    "background_map": default_sky_map(Path(__file__).resolve().parent.parent),
    "backfill_from": None,
    "max_files_per_run": 10,
}

#: Keys that named state paths; refused, not read (the paths are the
#: job's convention, shared with choco).
_RETIRED_KEYS = ("state_file", "lock_file", "output", "image_file", "maps_dir")
#: Renamed keys, refused with the replacement named.
_RENAMED_KEYS = {"bands": "colour_bins and colour_range_mhz (hues now run over the whole spectrum)"}

#: Files in the state directory.
MAPS_FILE = "maps.npz"
GAINS_FILE = "gains.npz"
CALSUM_FILE = "calsum.npz"
STATE_FILE = "state.json"
LOCK_FILE = "mapmaker.lock"
IMAGES = {"sky": "sky.png", "daily": "daily.png",
          "sky_strip": "sky-strip.png", "daily_strip": "daily-strip.png"}


class Degraded(Exception):
    """A dependency was unavailable.  Retries self-heal; exit 2."""


# --- config -----------------------------------------------------------------

def load_config(path) -> dict:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("config must be a mapping")
    retired = [k for k in _RETIRED_KEYS if k in raw]
    if retired:
        raise ValueError(
            f"{' and '.join(retired)} retired: remove it; the maps, gains, state.json "
            f"and the run lock live in the job's state directory "
            f"({job_state_dir('mapmaker')}; systemd's StateDirectory=choco/mapmaker, "
            f"or --state-dir)")
    renamed = [k for k in _RENAMED_KEYS if k in raw]
    if renamed:
        raise ValueError("; ".join(f"{k} retired: use {_RENAMED_KEYS[k]}" for k in renamed))
    cfg = dict(DEFAULTS)
    cfg.update(raw)
    roots = cfg.get("roots") or []
    if not roots:
        raise ValueError("no roots configured; nothing to map")
    norm = []
    for r in roots:
        if isinstance(r, str):
            r = {"path": r, "name": Path(r).name}
        if not r.get("path"):
            raise ValueError(f"root {r!r} has no path")
        norm.append({"path": str(r["path"]), "name": str(r.get("name") or Path(r["path"]).name)})
    cfg["roots"] = norm
    if cfg["pointing_dec_deg"] is None:
        raise ValueError("pointing_dec_deg is required: the declination the dishes point at")
    cfg["pointing_dec_deg"] = float(cfg["pointing_dec_deg"])
    cr = cfg["colour_range_mhz"]
    if not isinstance(cr, (list, tuple)) or len(cr) != 2 or float(cr[0]) >= float(cr[1]):
        raise ValueError("colour_range_mhz must be [lo, hi] in MHz with lo < hi")
    cfg["colour_range_mhz"] = [float(cr[0]), float(cr[1])]
    for key in ("subband_channels", "n_ra_bins", "min_cal_samples", "row_repeat",
                "max_files_per_run", "colour_bins"):
        cfg[key] = int(cfg[key])
    if cfg["colour_bins"] < 1:
        raise ValueError("colour_bins must be at least 1")
    if cfg["subband_channels"] < 1 or cfg["n_ra_bins"] < 16:
        raise ValueError("subband_channels and n_ra_bins must be positive")
    for key in ("dec_halfwidth_deg", "dec_step_deg", "dish_diameter_m", "beam_fwhm_factor",
                "footprint_fwhm", "sun_exclusion_deg", "calibrator_max_offset_deg",
                "calibration_window_deg", "dead_fraction", "exposure_floor"):
        cfg[key] = float(cfg[key])
    if cfg["max_rfi_fraction"] is not None:
        cfg["max_rfi_fraction"] = float(cfg["max_rfi_fraction"])
    if cfg["dec_step_deg"] <= 0 or cfg["dec_halfwidth_deg"] <= 0:
        raise ValueError("dec_step_deg and dec_halfwidth_deg must be positive")
    cfg["stretch"] = _stretch_cfg(cfg.get("stretch"), DEFAULTS["stretch"], "stretch")
    cl = dict(DEFAULTS["clean"])
    cl.update(cfg.get("clean") or {})
    cl["stretch"] = _stretch_cfg(cl.get("stretch"), DEFAULTS["clean"]["stretch"], "clean.stretch")
    for key in ("gain", "threshold_sigma", "beam_ra_deg", "beam_dec_deg"):
        cl[key] = float(cl[key])
    cl["max_iterations"] = int(cl["max_iterations"])
    if not 0 < cl["gain"] <= 1:
        raise ValueError("clean.gain must be in (0, 1]")
    if cl["threshold_sigma"] <= 0 or cl["max_iterations"] < 1:
        raise ValueError("clean.threshold_sigma and clean.max_iterations must be positive")
    if cl["beam_ra_deg"] <= 0 or cl["beam_dec_deg"] <= 0:
        raise ValueError("clean.beam_ra_deg and clean.beam_dec_deg must be positive")
    cfg["clean"] = cl
    cals = []
    for c in cfg["calibrators"] or []:
        if not isinstance(c, dict) or "ra_deg" not in c or "dec_deg" not in c:
            raise ValueError(f"calibrator {c!r} needs name, ra_deg and dec_deg")
        cals.append({"name": str(c.get("name") or "calibrator"),
                     "ra_deg": float(c["ra_deg"]), "dec_deg": float(c["dec_deg"])})
    cfg["calibrators"] = cals
    return cfg


def _stretch_cfg(raw, defaults: dict, name: str) -> dict:
    st = dict(defaults)
    st.update(raw or {})
    if st["mode"] not in ("equalize", "log", "arcsinh"):
        raise ValueError(f"{name}.mode must be equalize, log or arcsinh")
    return {"mode": str(st["mode"]), "floor_sigma": float(st["floor_sigma"]),
            "soft": float(st["soft"]), "max": float(st["max"])}


def grid_signature(cfg: dict) -> str:
    """What the accumulators depend on; a change starts them afresh.  The
    beam keys are in it because the kernel is part of the map's
    definition: the deconvolution rebuilds the same kernels from this
    config to get the dirty beam, so a map accumulated through one kernel
    must never be cleaned with another."""
    keys = ("pointing_dec_deg", "colour_bins", "colour_range_mhz", "n_ra_bins",
            "dec_halfwidth_deg", "dec_step_deg", "subband_channels",
            "dish_diameter_m", "beam_fwhm_factor", "footprint_fwhm")
    return json.dumps({k: cfg[k] for k in keys}, sort_keys=True)


#: The signature's keys before the beam keys joined it (2026-10-07).
_PRE_BEAM_SIGNATURE_KEYS = frozenset(("pointing_dec_deg", "colour_bins", "colour_range_mhz", "n_ra_bins",
                                      "dec_halfwidth_deg", "dec_step_deg", "subband_channels"))


def signature_matches(stored: str, current: str) -> bool:
    """Whether accumulators carrying *stored* were made for *current*.
    A file written before the beam keys joined the signature carries
    exactly the grid keys; it matches when those agree, and the next save
    writes the full signature.  Nothing else is tolerated."""
    if stored == current:
        return True
    try:
        s, c = json.loads(stored), json.loads(current)
    except ValueError:
        return False
    return isinstance(s, dict) and isinstance(c, dict) and set(s) == _PRE_BEAM_SIGNATURE_KEYS \
        and set(s) < set(c) and all(c[k] == v for k, v in s.items())


def colour_edges(cfg: dict) -> np.ndarray:
    """Bin edges in MHz, log-spaced over ``colour_range_mhz``: equal
    spectral-index leverage per bin, and equal steps of hue."""
    lo, hi = cfg["colour_range_mhz"]
    return np.geomspace(lo, hi, cfg["colour_bins"] + 1)


def colour_bin_of(freq_mhz: float, edges: np.ndarray) -> int | None:
    """The colour bin a sub-band centre falls in, or None outside the range."""
    if freq_mhz < edges[0] or freq_mhz >= edges[-1]:
        return None
    return int(np.searchsorted(edges, freq_mhz, side="right") - 1)


# --- the run lock -------------------------------------------------------------

@contextlib.contextmanager
def single_run(path):
    """Hold an exclusive lock for the duration of a run, or yield False.
    Two runs would double-count samples into the accumulators."""
    lock = Path(path)
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock, "w")
    except OSError as exc:
        log.warning("could not open lock file %s: %s", lock, exc)
        yield True
        return
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        yield True
    finally:
        fh.close()


# --- source positions ---------------------------------------------------------

def source_cirs(ra_icrs_deg: float, dec_icrs_deg: float, t_unix: float):
    """CIRS (ra, dec) of an ICRS position at *t_unix*: the frame the file's
    ``bin_ERA_deg`` hour angles refer to (precession moves Cyg A ~0.1°)."""
    c = SkyCoord(ra=ra_icrs_deg, dec=dec_icrs_deg, unit="deg", frame="icrs")
    c = c.transform_to(CIRS(obstime=Time(t_unix, format="unix")))
    return float(c.ra.deg), float(c.dec.deg)


def sun_cirs(t_unix: np.ndarray):
    """CIRS (ra, dec) arrays of the Sun at the given times."""
    t = Time(np.asarray(t_unix, dtype=float), format="unix")
    s = get_sun(t).transform_to(CIRS(obstime=t))
    return np.asarray(s.ra.deg, dtype=float), np.asarray(s.dec.deg, dtype=float)


def pick_calibrator(cfg: dict) -> dict | None:
    """The first configured calibrator whose transit passes within
    ``calibrator_max_offset_deg`` of the pointing."""
    for c in cfg["calibrators"]:
        if abs(c["dec_deg"] - cfg["pointing_dec_deg"]) <= cfg["calibrator_max_offset_deg"]:
            return c
    return None


# --- pending files ------------------------------------------------------------

def list_acquisitions(root: str) -> list[str]:
    """Acquisition directories, oldest first."""
    try:
        names = sorted(e for e in os.listdir(root)
                       if e.startswith("acq_") and os.path.isdir(os.path.join(root, e)))
    except OSError as e:
        raise Degraded(f"data root {root} unavailable: {e}") from e
    return names


def source_files(acq_dir: str) -> list[tuple[int, str]]:
    """``(index, path)`` of the completed files in an acquisition, in order."""
    out = []
    for p in glob.glob(os.path.join(acq_dir, "vis_*.h5")):
        idx = R.index_from_name(os.path.basename(p))
        if idx is not None:
            out.append((idx, p))
    return sorted(out)


def pending_files(cfg: dict, last_idx: int | None) -> tuple[list[tuple[int, str]], list[str]]:
    """Files not yet folded in, oldest first, across all roots; and the
    degraded reasons for roots that could not be listed.  Without a
    ``last_idx`` the first run starts at ``backfill_from`` (an acquisition
    name) or at the newest acquisition."""
    files, reasons = [], []
    for root in cfg["roots"]:
        try:
            acqs = list_acquisitions(root["path"])
        except Degraded as e:
            reasons.append(str(e))
            continue
        if last_idx is None:
            start = cfg.get("backfill_from")
            if start and start in acqs:
                acqs = acqs[acqs.index(start):]
            elif acqs:
                acqs = acqs[-1:]
        for acq in acqs:
            for idx, p in source_files(os.path.join(root["path"], acq)):
                if last_idx is None or idx > last_idx:
                    files.append((idx, p))
    files.sort()
    return files, reasons


# --- persistent state -----------------------------------------------------------

class Maps:
    """The map accumulators and their on-disk form.

    Besides the maps themselves the file carries what the deconvolution
    (``clean.py``) needs to rebuild the dirty beam the sky map was made
    with: ``sky_blw``, the weight each unique baseline contributed per
    imaging sub-band (the same ``w`` the scatter saw, so it follows the
    gains, the flags and the dead products exactly), and ``geometry``, the
    site, the baseline vectors and the imaged sub-bands' centres."""

    def __init__(self, grid: I.Grid, n_pol: int, n_band: int, signature: str,
                 n_sub: int = 0, n_unique: int = 0):
        self.grid = grid
        self.signature = signature
        shape = (n_pol, n_band, grid.n_dec, grid.n_bins)
        self.sky_num = np.zeros(shape, np.float32)
        self.sky_exp = np.zeros(shape, np.float32)
        self.sky_blw = np.zeros((n_sub, n_unique), np.float64)
        self.geometry: dict | None = None
        self.passes = I.PassSlots.empty(n_pol, n_band, grid.n_dec, grid.n_bins)
        self.last_file_idx: int | None = None
        self.last_sample_time: float | None = None
        self.current_pass: int | None = None
        self.n_samples = 0
        self.n_sun_skipped = 0

    @classmethod
    def load(cls, path, grid: I.Grid, n_pol: int, n_band: int, signature: str,
             n_sub: int = 0, n_unique: int = 0) -> "Maps":
        m = cls(grid, n_pol, n_band, signature, n_sub, n_unique)
        if not os.path.exists(path):
            return m
        try:
            with np.load(path, allow_pickle=False) as z:
                if not signature_matches(str(z["signature"]), signature) or z["sky_num"].shape != m.sky_num.shape:
                    log.warning("map grid or bands changed; starting the accumulators afresh")
                    return m
                m.sky_num = z["sky_num"]
                m.sky_exp = z["sky_exp"]
                m.passes = I.PassSlots(z["pass_num"], z["pass_exp"], z["pass_idx"])
                if "sky_blw" in z:
                    if z["sky_blw"].shape == m.sky_blw.shape:
                        m.sky_blw = z["sky_blw"]
                    elif m.sky_blw.size:
                        log.warning("baseline weights do not fit this file's geometry; "
                                    "accumulating them afresh")
                meta = json.loads(str(z["meta"]))
        except (OSError, ValueError, KeyError) as e:
            log.warning("could not read %s (%s); starting afresh", path, e)
            return cls(grid, n_pol, n_band, signature, n_sub, n_unique)
        m.geometry = meta.get("geometry")
        m.last_file_idx = meta.get("last_file_idx")
        m.last_sample_time = meta.get("last_sample_time")
        m.current_pass = meta.get("current_pass")
        m.n_samples = int(meta.get("n_samples", 0))
        m.n_sun_skipped = int(meta.get("n_sun_skipped", 0))
        return m

    def save(self, path) -> None:
        meta = json.dumps({"last_file_idx": self.last_file_idx,
                           "last_sample_time": self.last_sample_time,
                           "current_pass": self.current_pass,
                           "n_samples": self.n_samples,
                           "n_sun_skipped": self.n_sun_skipped,
                           "geometry": self.geometry})
        tmp = str(path) + ".tmp.npz"
        np.savez(tmp, signature=np.array(self.signature), sky_num=self.sky_num,
                 sky_exp=self.sky_exp, sky_blw=self.sky_blw, pass_num=self.passes.num,
                 pass_exp=self.passes.exp, pass_idx=self.passes.idx, meta=np.array(meta))
        os.replace(tmp, path)


def load_gains(path):
    """``(gains, meta)`` or ``(None, {})``."""
    if not os.path.exists(path):
        return None, {}
    try:
        with np.load(path, allow_pickle=False) as z:
            return z["gains"], json.loads(str(z["meta"]))
    except (OSError, ValueError, KeyError) as e:
        log.warning("could not read %s: %s", path, e)
        return None, {}


def save_gains(path, gains: np.ndarray, meta: dict) -> None:
    tmp = str(path) + ".tmp.npz"
    np.savez(tmp, gains=gains, meta=np.array(json.dumps(meta)))
    os.replace(tmp, path)


def load_calsum(path) -> I.CalAccumulator | None:
    if not os.path.exists(path):
        return None
    try:
        with np.load(path, allow_pickle=False) as z:
            meta = json.loads(str(z["meta"]))
            acc = I.CalAccumulator(z["gsum"], z["gwt"], int(meta["n_samples"]),
                                   str(meta["calibrator"]), meta.get("first_time"),
                                   meta.get("last_time"))
            return acc
    except (OSError, ValueError, KeyError) as e:
        log.warning("could not read %s: %s", path, e)
        return None


def save_calsum(path, acc: I.CalAccumulator | None) -> None:
    if acc is None or acc.n_samples == 0:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        return
    meta = json.dumps({"n_samples": acc.n_samples, "calibrator": acc.calibrator,
                       "first_time": acc.first_time, "last_time": acc.last_time})
    tmp = str(path) + ".tmp.npz"
    np.savez(tmp, gsum=acc.gsum, gwt=acc.gwt, meta=np.array(meta))
    os.replace(tmp, path)


# --- the run -----------------------------------------------------------------------

class Run:
    """One invocation: everything that is built once per run (grid,
    kernels, calibrator) and the counters that end up in state.json."""

    def __init__(self, cfg: dict, state_dir: Path):
        self.cfg = cfg
        self.state_dir = state_dir
        self.report = {"files_processed": 0, "files_imaged": 0, "files_uncalibrated": 0,
                       "backlog": 0, "skipped": [], "errors": [], "degraded": [],
                       "samples_imaged": 0, "samples_sun_skipped": 0,
                       "gains_solved": False}
        self.grid: I.Grid | None = None
        self.maps: Maps | None = None
        self.kernels: list[I.Kernel | None] = []
        self.band_of_sub: list[int | None] = []
        self.bl: I.Baselines | None = None
        self.axes_key = None
        self.gains, self.gains_meta = load_gains(state_dir / GAINS_FILE)
        self.calsum = load_calsum(state_dir / CALSUM_FILE)
        self.calibrator = pick_calibrator(cfg)
        if self.calibrator is None:
            self.report["degraded"].append(
                f"no calibrator within {cfg['calibrator_max_offset_deg']:g} deg of the pointing "
                f"(dec {cfg['pointing_dec_deg']:g}); the maps cannot be calibrated")

    # -- per-geometry setup --
    def setup(self, ax: R.FileAxes, pols: list) -> None:
        key = (ax.n_freq, ax.n_prod, ax.n_elements, round(ax.lat_deg, 6), round(ax.lon_deg, 6))
        if self.axes_key == key:
            return
        cfg = self.cfg
        self.grid = I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"], cfg["dec_halfwidth_deg"],
                                 cfg["dec_step_deg"], ax.lat_deg, ax.lon_deg)
        self.bl = I.group_baselines(pols[0].bl_xy, ax.sep_x, ax.sep_y)
        n_sub = int(math.ceil(ax.n_freq / cfg["subband_channels"]))
        edges = colour_edges(cfg)
        self.kernels, self.band_of_sub, subbands = [], [], []
        for s in range(n_sub):
            lo, hi = s * cfg["subband_channels"], min((s + 1) * cfg["subband_channels"], ax.n_freq)
            fc = float(ax.freq_mhz[lo:hi].mean())
            band = colour_bin_of(fc, edges)
            self.band_of_sub.append(band)
            if band is None or R.channel_mask(ax.freq_mhz[lo:hi], cfg["bad_freq_mhz"]).all():
                self.kernels.append(None)
                continue
            fwhm = float(I.beam_fwhm_deg(fc, cfg["dish_diameter_m"], cfg["beam_fwhm_factor"]))
            self.kernels.append(I.build_kernel(self.grid, self.bl.xy, fc, fwhm, cfg["footprint_fwhm"]))
            subbands.append([s, fc, band])
        if self.maps is None:
            self.maps = Maps.load(self.state_dir / MAPS_FILE, self.grid, len(pols),
                                  cfg["colour_bins"], grid_signature(cfg), n_sub, self.bl.n_unique)
        elif self.maps.sky_blw.shape != (n_sub, self.bl.n_unique):
            log.warning("baseline weights do not fit the new geometry; accumulating them afresh")
            self.maps.sky_blw = np.zeros((n_sub, self.bl.n_unique), np.float64)
        # what clean.py needs to rebuild these kernels without a data file
        self.maps.geometry = {"lat_deg": float(ax.lat_deg), "lon_deg": float(ax.lon_deg),
                              "baselines_xy": self.bl.xy.tolist(), "subbands": subbands}
        self.axes_key = key
        log.info("grid %d x %d, %d sub-bands (%d imaged), %d unique baselines",
                 self.grid.n_dec, self.grid.n_bins, n_sub,
                 sum(k is not None for k in self.kernels), self.bl.n_unique)

    # -- per-file --
    def calibration_window(self, ax: R.FileAxes, pols: list, era_local: np.ndarray):
        """The calibrator's window mask, delays and beam response for this
        file's samples, plus its hour angle (deg on the sky) per sample."""
        cal = self.calibrator
        ra_c, dec_c = source_cirs(cal["ra_deg"], cal["dec_deg"], float(ax.t_unix.mean()))
        H = np.mod(era_local - ra_c + 180.0, 360.0) - 180.0
        on_sky = H * math.cos(math.radians(dec_c))
        in_window = np.abs(on_sky) <= self.cfg["calibration_window_deg"]
        uE, uN, uU = I.enu(ra_c, dec_c, era_local, ax.lat_deg)
        pvec = I.pointing_vector(self.cfg["pointing_dec_deg"], ax.lat_deg)
        tau = np.stack([I.source_delays(ps.bl_xy, uE, uN) for ps in pols])      # (n_pol, n_prod, t)
        cosang = uE * pvec[0] + uN * pvec[1] + uU * pvec[2]
        return R.CalWindow(in_window, tau, cosang, self.cfg["dish_diameter_m"],
                           self.cfg["beam_fwhm_factor"]), on_sky

    def sun_ok(self, ax: R.FileAxes, era_local: np.ndarray) -> np.ndarray:
        """Samples the sky map may use: the Sun farther than
        ``sun_exclusion_deg`` from the pointing."""
        if self.cfg["sun_exclusion_deg"] <= 0:
            return np.ones(ax.n_time, bool)
        ra_s, dec_s = sun_cirs(ax.t_unix)
        uE, uN, uU = I.enu(ra_s, dec_s, era_local, ax.lat_deg)
        pvec = I.pointing_vector(self.cfg["pointing_dec_deg"], ax.lat_deg)
        cosang = uE * pvec[0] + uN * pvec[1] + uU * pvec[2]
        return np.rad2deg(np.arccos(np.clip(cosang, -1, 1))) > self.cfg["sun_exclusion_deg"]

    def process(self, path: str) -> None:
        cfg = self.cfg
        t_start = time.perf_counter()
        ax = R.read_axes(path)
        pols = R.pol_sets(ax)
        self.setup(ax, pols)
        era_local = I.era_local_deg(ax.era_deg, ax.lon_deg)
        bins = I.ra_bin(era_local, self.grid.n_bins)
        passes = I.pass_index(ax.t_unix, ax.lon_deg)

        window, on_sky = None, None
        if self.calibrator is not None:
            window, on_sky = self.calibration_window(ax, pols, era_local)
            if window.in_window.any():
                if self.calsum is None or self.calsum.gsum.shape != (ax.n_freq, len(pols), len(pols[0].prod_idx)):
                    self.calsum = I.CalAccumulator.empty(ax.n_freq, len(pols), len(pols[0].prod_idx),
                                                         self.calibrator["name"])
                elif self.calsum.last_time and ax.t_unix.min() - self.calsum.last_time > 12 * 3600:
                    log.info("discarding a stale partial calibration (%d samples)", self.calsum.n_samples)
                    self.calsum = I.CalAccumulator.empty(ax.n_freq, len(pols), len(pols[0].prod_idx),
                                                         self.calibrator["name"])

        gains = self.gains
        if gains is not None and gains.shape != (ax.n_freq, len(pols), len(pols[0].prod_idx)):
            log.warning("stored gains do not fit this file's axes; imaging uncalibrated data is "
                        "pointless, waiting for the next calibrator transit")
            gains = None

        red = R.read_reduced(ax, pols, cfg["subband_channels"], cfg["bad_freq_mhz"],
                             cfg["max_rfi_fraction"], gains,
                             cal=self.calsum if (window is not None and window.in_window.any()) else None,
                             window=window)

        # a transit that has passed: solve the gains for the passes to come
        if (self.calsum is not None and on_sky is not None and self.calsum.n_samples > 0
                and on_sky[-1] > cfg["calibration_window_deg"]):
            if self.calsum.n_samples >= cfg["min_cal_samples"]:
                self.gains = self.calsum.solve(cfg["dead_fraction"])
                dead = int((self.gains == 0).all(axis=0).sum())
                self.gains_meta = {"calibrator": self.calsum.calibrator,
                                   "solved_at": time.time(),
                                   "transit_time": self.calsum.last_time,
                                   "n_samples": self.calsum.n_samples,
                                   "n_dead_products": dead,
                                   "n_products": int(self.gains.shape[1] * self.gains.shape[2])}
                save_gains(self.state_dir / GAINS_FILE, self.gains, self.gains_meta)
                self.report["gains_solved"] = True
                log.info("solved gains on %s from %d samples; %d of %d products dead",
                         self.calsum.calibrator, self.calsum.n_samples, dead,
                         self.gains_meta["n_products"])
            else:
                log.warning("calibrator window passed with only %d samples; keeping the old gains",
                            self.calsum.n_samples)
            self.calsum = None

        self.report["files_processed"] += 1
        if not red.calibrated:
            self.report["files_uncalibrated"] += 1
        else:
            self.image(red, bins, passes, era_local, ax)
            self.report["files_imaged"] += 1
        self.maps.last_file_idx = ax.file_idx
        self.maps.last_sample_time = float(ax.t_unix.max())
        log.info("folded %s: %d samples, %s, %d in the calibration window, %.0f%% of weights zero, %.1fs",
                 os.path.basename(path), ax.n_time,
                 "calibrated" if red.calibrated else "uncalibrated",
                 int(window.in_window.sum()) if window is not None else 0,
                 100 * red.frac_flagged, time.perf_counter() - t_start)

    def image(self, red: R.Reduced, bins: np.ndarray, passes: np.ndarray,
              era_local: np.ndarray, ax: R.FileAxes) -> None:
        maps = self.maps
        sun_ok = self.sun_ok(ax, era_local)
        # (sub, pol, prod, t) -> (sub, pol, t, unique)
        V = np.moveaxis(red.V, 3, 2)
        W = np.moveaxis(red.W, 3, 2)
        Vu, Wu = I.average_redundant(V, W, self.bl)
        for t in range(ax.n_time):
            if not np.any(Wu[:, :, t, :] > 0):
                continue
            slot = maps.passes.slot_for(int(passes[t]))
            maps.current_pass = int(passes[t])
            n = int(bins[t])
            for s, kern in enumerate(self.kernels):
                if kern is None:
                    continue
                band = self.band_of_sub[s]
                for p in range(Vu.shape[1]):
                    w = Wu[s, p, t]
                    if not np.any(w > 0):
                        continue
                    v = Vu[s, p, t]
                    I.scatter(maps.passes.num[slot, p, band], maps.passes.exp[slot, p, band], kern, n, v, w)
                    if sun_ok[t]:
                        I.scatter(maps.sky_num[p, band], maps.sky_exp[p, band], kern, n, v, w)
                        maps.sky_blw[s] += w
            maps.n_samples += 1
            self.report["samples_imaged"] += 1
            if not sun_ok[t]:
                maps.n_sun_skipped += 1
                self.report["samples_sun_skipped"] += 1

    # -- outputs --
    def render(self) -> dict:
        """Write the four PNGs; returns coverage facts for the state file."""
        cfg, maps = self.cfg, self.maps
        facts = {}
        markers = [(c["name"], c["ra_deg"], c["dec_deg"]) for c in cfg["calibrators"]]
        when = render.stamp(maps.last_sample_time)
        edges = colour_edges(cfg)
        colours = render.spectrum_colours(np.sqrt(edges[:-1] * edges[1:]), edges[0], edges[-1])
        for name, num, exp in (("sky", maps.sky_num, maps.sky_exp),
                               ("daily", *maps.passes.composite(maps.current_pass if maps.current_pass is not None else -1))):
            bands, ok = render.band_maps(num, exp, cfg["exposure_floor"])
            rgb = render.to_rgb(bands, colours, cfg["stretch"])
            cover = exp.sum(axis=(0, 1, 2))
            cover = cover / cover.max() if cover.max() > 0 else cover
            facts[name] = {"coverage": float(np.mean(ok.any(axis=1), axis=-1).mean()),
                           "bins_covered": int((cover > 0).sum())}
            title = ("Sky map, all passes" if name == "sky" else "Last sidereal day") + f" — data to {when}"
            render.write_png_atomic(self.state_dir / IMAGES[name + "_strip"], rgb, cfg["row_repeat"])
            render.write_figure_atomic(self.state_dir / IMAGES[name], rgb, maps.grid.dec_deg, title,
                                       edges, colours, markers, coverage=cover)
        return facts


def run(cfg: dict, state_dir: Path, budget: int | None = None) -> dict:
    t0 = time.perf_counter()
    r = Run(cfg, state_dir)
    maps_meta_idx = None
    # the last folded index lives with the maps, so read it before listing
    probe = Maps.load(state_dir / MAPS_FILE, I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"],
                                                          cfg["dec_halfwidth_deg"], cfg["dec_step_deg"],
                                                          0.0, 0.0),
                      2, cfg["colour_bins"], grid_signature(cfg))
    maps_meta_idx = probe.last_file_idx
    files, reasons = pending_files(cfg, maps_meta_idx)
    r.report["degraded"].extend(reasons)
    budget = cfg["max_files_per_run"] if budget is None else budget
    todo = files if budget <= 0 else files[:budget]
    r.report["backlog"] = max(0, len(files) - len(todo))
    last_idx = maps_meta_idx
    for idx, path in todo:
        try:
            r.process(path)
            last_idx = idx
        except R.Unusable as e:
            r.report["skipped"].append(str(e))
            last_idx = idx
            if r.maps is not None:
                r.maps.last_file_idx = idx
        except OSError as e:
            r.report["degraded"].append(f"{os.path.basename(path)}: {e}")
            break                        # the mount is the likely cause; retry next tick
    if r.maps is not None:
        if last_idx is not None:
            r.maps.last_file_idx = max(last_idx, r.maps.last_file_idx or last_idx)
        r.maps.save(state_dir / MAPS_FILE)
        save_calsum(state_dir / CALSUM_FILE, r.calsum)
        if r.maps.n_samples > 0 or r.report["files_imaged"]:
            try:
                r.report["coverage"] = r.render()
            except (OSError, ValueError) as e:
                r.report["errors"].append(f"render: {e}")
    elif todo and last_idx is not None and last_idx != maps_meta_idx:
        # every file was unusable; remember where we got to without a grid
        probe.last_file_idx = last_idx
        probe.save(state_dir / MAPS_FILE)
    if r.gains is None and r.calibrator is not None and r.report["files_processed"]:
        acc = r.calsum.n_samples if r.calsum is not None else 0
        r.report["degraded"].append(
            f"no gains yet: waiting for a {r.calibrator['name']} transit "
            f"({acc} calibration samples so far)")
    r.report["run_seconds"] = round(time.perf_counter() - t0, 2)
    r.report["last_file_idx"] = (r.maps.last_file_idx if r.maps is not None else last_idx)
    r.report["last_sample_time"] = r.maps.last_sample_time if r.maps is not None else None
    r.report["gains"] = dict(r.gains_meta) if r.gains is not None else None
    r.report["calibrator"] = r.calibrator["name"] if r.calibrator else None
    r.report["cal_samples_pending"] = r.calsum.n_samples if r.calsum is not None else 0
    r.report["n_samples_total"] = r.maps.n_samples if r.maps is not None else 0
    r.report["current_pass"] = r.maps.current_pass if r.maps is not None else None
    return r.report


def write_state(path, cfg: dict, report: dict) -> None:
    try:
        write_json_atomic(path, {
            "updated": time.time(),
            "roots": [r["name"] for r in cfg["roots"]],
            "pointing_dec_deg": cfg["pointing_dec_deg"],
            "colour_bins": cfg["colour_bins"],
            "colour_range_mhz": cfg["colour_range_mhz"],
            "colour_edges_mhz": [round(float(e), 2) for e in colour_edges(cfg)],
            "stretch": cfg["stretch"]["mode"],
            "images": IMAGES,
            **report,
        })
    except OSError as e:
        log.warning("could not write state file %s: %s", path, e)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mapmaker", description="drift-scan sky maps, frequency as hue")
    ap.add_argument("-c", "--config", required=True, help="path to YAML config")
    ap.add_argument("--max-files", type=int, default=None,
                    help="override max_files_per_run (0 = unlimited)")
    ap.add_argument("-n", "--dry-run", action="store_true",
                    help="report what would be folded in, write nothing")
    ap.add_argument("--state-dir", default=None,
                    help="where the maps, gains, state.json and the run lock live (default: "
                         "systemd's $STATE_DIRECTORY, else /var/lib/choco/mapmaker)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING - 10 * min(args.verbose, 2),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log.setLevel(min(log.getEffectiveLevel(), logging.INFO))

    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        return 1
    state_dir = job_state_dir("mapmaker", args.state_dir)
    budget = args.max_files

    if args.dry_run:
        probe = Maps.load(state_dir / MAPS_FILE, I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"],
                                                              cfg["dec_halfwidth_deg"], cfg["dec_step_deg"],
                                                              0.0, 0.0),
                          2, cfg["colour_bins"], grid_signature(cfg))
        files, reasons = pending_files(cfg, probe.last_file_idx)
        for reason in reasons:
            log.error("%s", reason)
        print(f"{len(files)} files pending (last folded index {probe.last_file_idx})")
        for idx, p in files[:10]:
            print(f"  {idx} {p}")
        return 2 if reasons else 0

    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.error("state directory %s: %s", state_dir, e)
        return 2
    with single_run(state_dir / LOCK_FILE) as acquired:
        if not acquired:
            log.info("another run holds the lock; nothing to do")
            return 0
        report = run(cfg, state_dir, budget)
        write_state(state_dir / STATE_FILE, cfg, report)

    for e in report["errors"][:20]:
        log.warning("%s", e)
    for e in report["degraded"][:20]:
        log.warning("degraded: %s", e)
    log.info("processed %d file(s), imaged %d, in %.1fs; backlog %d; gains %s",
             report["files_processed"], report["files_imaged"], report["run_seconds"],
             report["backlog"], "solved this run" if report["gains_solved"]
             else ("present" if report["gains"] else "none"))
    return 2 if report["degraded"] else 0


if __name__ == "__main__":
    sys.exit(main())
