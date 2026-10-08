#!/usr/bin/env python3
"""Deconvolve the mapmaker's sky map: a joint-bin Högbom CLEAN against the
dirty beam the map was made with (choco job, its own timer).

Two rows of dishes give every source a raised-cosine ridge in Dec: half
the baseline weight is same-row (east-west only) products that carry no
Dec information at all, and the cross-row products add one cosine per
frequency.  That ridge is the dirty beam, and the dirty beam is known
exactly: it is the cross-correlation of the imaging kernel with itself,
weighted by what each baseline actually contributed.  So the sky map is
deconvolved the classic way (Högbom 1974): find the brightest pixel of
the exposure-weighted broadband map, subtract a fraction of it through
the per-bin dirty beams at that Dec row, repeat to the noise, then
restore the components with one Gaussian beam for every bin and add the
residual back.  The peak search runs on the broadband sum because the
ridges of the colour bins average down there, which is what keeps a
lobe from being mistaken for the source; the subtraction runs per bin
so every component keeps its spectrum and the colour survives.

Reads ``maps.npz`` (accumulators, per-sub-band baseline weights, the
geometry) from the state directory and writes ``clean.png``,
``clean-strip.png`` and ``clean.json`` beside it.  Separate from the
fold-in step because a deconvolution takes minutes and must never delay
the ingest; it self-gates on the accumulators' data time.

Exit codes follow the jobs convention: 0 ok or nothing to do, 2 degraded
(no accumulators or no geometry recorded yet, unreadable state), 1 config
error / bug.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import yaml

from choco.jobclient import job_state_dir, write_json_atomic

import imaging as I
import mapmaker as M
import render

log = logging.getLogger("mapmaker.clean")

#: Files in the state directory (names are the job's convention; choco reads them by name).
IMAGES = {"clean": "clean.png", "clean_strip": "clean-strip.png", "context": "context.png"}
FACTS_FILE = "clean.json"
LOCK_FILE = "clean.lock"
#: A bin whose beam response at the component's Dec row is below this
#: fraction of the pointing row's has no sensitivity there: its map value
#: is noise and dividing by the taper would amplify it, so the bin is
#: left untouched by that component.
MIN_TAPER = 1e-3
#: Brightest components listed in clean.json.
N_LISTED = 20


class Degraded(Exception):
    """A dependency is missing; retries self-heal; exit 2."""


# --- the dirty beam ----------------------------------------------------------

@dataclass
class DirtyBeam:
    """The per-bin dirty beam of a unit source at any Dec row, from the
    same kernels the map was accumulated through.

    For a source at row *j0* and a sample at bin *n*, the model
    visibility is ``K[b, j0, p0 - n]`` and the scatter adds
    ``Re Σ_b W_b V_b conj(K[b, j, p - n])`` to pixel ``(j, p)``, so the
    response is ``Re Σ_b W_b Σ_u K[b, j0, u] conj(K[b, j, u + δ])`` with
    ``δ = p - p0``: a correlation along the footprint, done by FFT.  The
    exposure the same samples add is ``Σ_b W_b · Σ_d E[d]`` at every
    column, the normalisation the map uses; a sub-band's weights ``W_b``
    are the ones its samples actually carried (``sky_blw``).  On the
    pointing row the peak is 1; elsewhere the peak is the taper a source
    on that row reads with (``~B⁴`` of its offset).
    """

    kernels: list            # [(sub-band index, colour bin, Kernel), ...] for the imaged sub-bands
    blw: np.ndarray          # (n_sub, n_unique) accumulated weight per unique baseline
    n_band: int
    n_dec: int
    _rows: dict = field(default_factory=dict)

    @property
    def D(self) -> int:
        """Half-width of the beam in RA bins: twice the widest footprint."""
        return 2 * max(k.M for _, _, k in self.kernels)

    def row(self, j0: int) -> tuple[np.ndarray, np.ndarray]:
        """``(psf, taper)``: ``psf`` is ``(n_band, n_dec, 2D+1)`` float32,
        column ``D`` the source's own RA bin; ``taper`` ``(n_band,)`` the
        peak per bin.  Rows are computed on demand and kept."""
        if j0 not in self._rows:
            self._rows[j0] = self._compute(j0)
        return self._rows[j0]

    def _compute(self, j0: int):
        D = self.D
        psf = np.zeros((self.n_band, self.n_dec, 2 * D + 1), np.float64)
        den = np.zeros(self.n_band)
        for s, band, k in self.kernels:
            w = self.blw[s]
            if w.sum() <= 0:
                continue
            Kc = k.Kc                                       # (nb, n_dec, L) = conj(K)
            L = Kc.shape[-1]
            N = 1 << (2 * L - 1).bit_length()
            A = np.fft.fft(Kc[:, j0, :], N, axis=-1)        # (nb, N)
            Bf = np.fft.fft(Kc, N, axis=-1)                 # (nb, n_dec, N)
            # Σ_b W_b conj(A_b) B_b, then one inverse FFT: the circular
            # correlation Σ_u conj(Kc[j0, u]) Kc[j, u + δ], δ at index δ mod N
            C = np.fft.ifft(np.einsum("b,bn,bjn->jn", w, np.conj(A), Bf), axis=-1)
            cen = np.concatenate([C[:, N - (L - 1):], C[:, :L]], axis=1).real   # δ in [-(L-1), L-1]
            psf[band, :, D - (L - 1):D + L] += cen
            den[band] += float(w.sum()) * float(k.E.sum())
        with np.errstate(invalid="ignore", divide="ignore"):
            psf = np.where(den[:, None, None] > 0, psf / np.maximum(den, 1e-300)[:, None, None], 0.0)
        return psf.astype(np.float32), psf[:, j0, D].astype(np.float64)


def build_beam(cfg: dict, geometry: dict, blw: np.ndarray, n_band: int, n_dec: int) -> DirtyBeam:
    """Rebuild the imaging kernels from the config and the recorded
    geometry (the config's beam keys are in the grid signature, so these
    are the kernels the map was accumulated through)."""
    grid = I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"], cfg["dec_halfwidth_deg"],
                        cfg["dec_step_deg"], float(geometry["lat_deg"]), float(geometry["lon_deg"]))
    if grid.n_dec != n_dec:
        raise Degraded("the accumulators' Dec grid does not match the config")
    bl_xy = np.asarray(geometry["baselines_xy"], dtype=float)
    if bl_xy.ndim != 2 or bl_xy.shape[1] != 2 or bl_xy.shape[0] != blw.shape[1]:
        raise Degraded("the recorded baselines do not fit the baseline weights")
    kernels = []
    for s, fc, band in geometry["subbands"]:
        s, band, fc = int(s), int(band), float(fc)
        if not 0 <= s < blw.shape[0] or not 0 <= band < n_band:
            raise Degraded("the recorded sub-bands do not fit the accumulators")
        fwhm = float(I.beam_fwhm_deg(fc, cfg["dish_diameter_m"], cfg["beam_fwhm_factor"]))
        kernels.append((s, band, I.build_kernel(grid, bl_xy, fc, fwhm, cfg["footprint_fwhm"])))
    if not kernels:
        raise Degraded("no imaged sub-bands recorded")
    return DirtyBeam(kernels, blw, n_band, n_dec)


# --- CLEAN ---------------------------------------------------------------------

def robust_sigma(x: np.ndarray) -> float:
    if x.size == 0:
        return 0.0
    med = float(np.median(x))
    return 1.4826 * float(np.median(np.abs(x - med)))


def _apply(m, B, search, wpix, anyvalid, scale, psf, c0, c1, q0, q1) -> None:
    """Subtract ``scale × psf[:, :, q0:q1]`` from map columns ``[c0, c1)``
    and refresh the broadband map and the peak-search array there."""
    m[:, :, c0:c1] -= scale[:, None, None] * psf[:, :, q0:q1]
    B[:, c0:c1] = (wpix[:, :, c0:c1] * m[:, :, c0:c1]).sum(axis=0)
    search[:, c0:c1] = np.where(anyvalid[:, c0:c1], B[:, c0:c1], -np.inf)


def hogbom(m: np.ndarray, valid: np.ndarray, wpix: np.ndarray, beam: DirtyBeam,
           gain: float, threshold: float, max_iterations: int) -> tuple[np.ndarray, dict]:
    """Högbom CLEAN in place on the per-bin maps *m* ``(n_band, n_dec,
    n_bins)``; returns the model (per-bin component map) and the run's
    facts.  *wpix* is each bin's share of the exposure per pixel, so
    ``Σ_c wpix·m`` is the broadband map the peaks are searched on."""
    n_band, n_dec, n_bins = m.shape
    D = beam.D
    if 2 * D + 1 >= n_bins:
        raise ValueError("the dirty beam is wider than the map")
    B = (wpix * m).sum(axis=0)
    anyvalid = valid.any(axis=0)
    search = np.where(anyvalid, B, -np.inf)
    model = np.zeros_like(m)
    if not threshold > 0:                       # an empty map: nothing to find, never spin on zeros
        threshold = float(np.finfo(np.float32).tiny)
    peak0 = float(search.max()) if anyvalid.any() else 0.0
    it = 0
    for it in range(max_iterations):
        k = int(np.argmax(search))
        j0, p0 = divmod(k, n_bins)
        peak = float(search[j0, p0])
        if not (peak >= threshold):
            break
        psf, taper = beam.row(j0)
        use = (taper > MIN_TAPER) & valid[:, j0, p0]
        a = np.where(use, m[:, j0, p0], 0.0)
        scale = np.where(use, gain * a / np.where(use, taper, 1.0), 0.0).astype(np.float32)
        model[:, j0, p0] += (gain * a).astype(np.float32)
        lo, hi = p0 - D, p0 + D + 1
        if lo >= 0 and hi <= n_bins:
            _apply(m, B, search, wpix, anyvalid, scale, psf, lo, hi, 0, 2 * D + 1)
        elif lo < 0:                                   # the window wraps past column 0
            _apply(m, B, search, wpix, anyvalid, scale, psf, lo + n_bins, n_bins, 0, -lo)
            _apply(m, B, search, wpix, anyvalid, scale, psf, 0, hi, -lo, 2 * D + 1)
        else:                                          # past the last column
            _apply(m, B, search, wpix, anyvalid, scale, psf, lo, n_bins, 0, n_bins - lo)
            _apply(m, B, search, wpix, anyvalid, scale, psf, 0, hi - n_bins, n_bins - lo, 2 * D + 1)
    else:
        it = max_iterations
    facts = {"iterations": int(it), "threshold": float(threshold), "peak_before": peak0,
             "residual_max": float(B[anyvalid].max()) if anyvalid.any() else 0.0,
             "sigma_after": robust_sigma(B[anyvalid]), "psf_rows": len(beam._rows)}
    return model, facts


def restore(model: np.ndarray, residual: np.ndarray, valid: np.ndarray,
            bin_deg: float, dec_step_deg: float, beam_ra_deg: float, beam_dec_deg: float) -> np.ndarray:
    """Components convolved with one Gaussian beam (unit peak, so a
    component reads its flux at its pixel, as in the dirty map) plus the
    residual; NaN where the dirty map had no data."""
    n_band, n_dec, n_bins = model.shape
    s_dec = beam_dec_deg / 2.3548200450309493 / dec_step_deg
    idx = np.arange(n_dec)
    G = np.exp(-0.5 * ((idx[:, None] - idx[None, :]) / s_dec) ** 2)        # (n_dec, n_dec)
    x = np.arange(n_bins)
    x = np.minimum(x, n_bins - x)                                           # circular: the RA axis wraps
    g = np.exp(-0.5 * (x / (beam_ra_deg / 2.3548200450309493 / bin_deg)) ** 2)
    F = np.fft.rfft(g)
    out = np.empty_like(model)
    for c in range(n_band):
        smooth = G @ model[c].astype(np.float64)
        out[c] = np.fft.irfft(np.fft.rfft(smooth, axis=-1) * F, n=n_bins, axis=-1)
    return np.where(valid, out + residual, np.nan).astype(np.float32)


# --- the run ----------------------------------------------------------------------

def load_maps(path):
    """``(num, exp, blw, meta)`` of the sky map, both polarizations summed."""
    with np.load(path, allow_pickle=False) as z:
        signature = str(z["signature"])
        num = z["sky_num"].sum(axis=0).astype(np.float64)
        exp = z["sky_exp"].sum(axis=0).astype(np.float64)
        blw = z["sky_blw"] if "sky_blw" in z else np.zeros((0, 0))
        meta = json.loads(str(z["meta"])) or {}
    return signature, num, exp, blw, meta


def read_facts(path) -> dict:
    try:
        with open(path) as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def run(cfg: dict, state_dir: Path, force: bool = False) -> dict:
    """One deconvolution of the current sky map; returns the facts that go
    to clean.json (``nothing`` set when there was nothing to do)."""
    t0 = time.perf_counter()
    cl = cfg["clean"]
    facts: dict = {"updated": time.time(), "degraded": [], "errors": []}
    path = state_dir / M.MAPS_FILE
    if not path.exists():
        facts["nothing"] = "no accumulators yet"
        return facts
    try:
        signature, num, exp, blw, meta = load_maps(path)
    except (OSError, ValueError, KeyError) as e:
        raise Degraded(f"could not read {path}: {e}") from e
    if not M.signature_matches(signature, M.grid_signature(cfg)):
        raise Degraded("the accumulators were made with another grid or beam; waiting for the fold-in to restart them")
    data_time = meta.get("last_sample_time")
    prev = read_facts(state_dir / FACTS_FILE)
    # the context image counts only when a backdrop is configured; a
    # configured one that is missing reruns the deconvolution each tick
    # until the map appears (the retry is the self-heal)
    wanted = [f for k, f in IMAGES.items() if k != "context" or cfg["background_map"]]
    if (not force and data_time is not None and prev.get("map_data_time") == data_time
            and all((state_dir / f).exists() for f in wanted)):
        facts["nothing"] = "the sky map has not changed since the last deconvolution"
        return facts
    geometry = meta.get("geometry")
    if not isinstance(geometry, dict) or blw.size == 0 or blw.sum() <= 0:
        raise Degraded("the accumulators carry no geometry or baseline weights yet; "
                       "the next fold-in records them")
    n_band, n_dec, n_bins = num.shape
    beam = build_beam(cfg, geometry, blw, n_band, n_dec)
    bin_deg = 360.0 / n_bins

    valid = exp > cfg["exposure_floor"] * exp.reshape(n_band, -1).max(axis=1)[:, None, None]
    with np.errstate(invalid="ignore", divide="ignore"):
        m = np.where(valid, num / np.maximum(exp, 1e-300), 0.0).astype(np.float32)
    esum = exp.sum(axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        wpix = np.where(esum > 0, exp / np.maximum(esum, 1e-300), 0.0).astype(np.float32)
    B0 = (wpix * m).sum(axis=0)
    anyvalid = valid.any(axis=0)
    sigma0 = robust_sigma(B0[anyvalid])
    threshold = cl["threshold_sigma"] * sigma0
    log.info("sky map %d bins x %d rows x %d columns: peak %.3g, robust sigma %.3g, cleaning to %.3g",
             n_band, n_dec, n_bins, float(B0[anyvalid].max()) if anyvalid.any() else 0.0, sigma0, threshold)
    model, stats = hogbom(m, valid, wpix, beam, cl["gain"], threshold, cl["max_iterations"])
    restored = restore(model, m, valid, bin_deg, cfg["dec_step_deg"], cl["beam_ra_deg"], cl["beam_dec_deg"])

    # components, merged per pixel, apparent broadband flux in the map's units
    flux = (wpix * model).sum(axis=0)
    js, ps = np.nonzero(flux)
    order = np.argsort(flux[js, ps])[::-1]
    decs = cfg["pointing_dec_deg"] + (np.arange(n_dec) - n_dec // 2) * cfg["dec_step_deg"]
    components = [{"ra_deg": round(float((ps[i] + 0.5) * bin_deg), 3),
                   "dec_deg": round(float(decs[js[i]]), 3),
                   "flux": float(flux[js[i], ps[i]])} for i in order[:N_LISTED]]

    edges = M.colour_edges(cfg)
    colours = render.spectrum_colours(np.sqrt(edges[:-1] * edges[1:]), edges[0], edges[-1])
    markers = [(c["name"], c["ra_deg"], c["dec_deg"]) for c in cfg["calibrators"]]
    cover = exp.sum(axis=(0, 1))
    cover = cover / cover.max() if cover.max() > 0 else cover
    title = f"Sky map, deconvolved ({stats['iterations']} CLEAN components) — data to {render.stamp(data_time)}"
    rgb = render.to_rgb(restored, colours, cl["stretch"])
    render.write_png_atomic(state_dir / IMAGES["clean_strip"], rgb, cfg["row_repeat"])
    render.write_figure_atomic(state_dir / IMAGES["clean"], rgb, decs, title, edges, colours, markers,
                               coverage=cover)
    # the same strip in context on the 408 MHz sky; a missing backdrop
    # costs only this image (the run reports degraded), a malformed one
    # is a config error
    if cfg["background_map"]:
        try:
            render.write_context_atomic(state_dir / IMAGES["context"], cfg["background_map"], rgb, decs,
                                        f"Sky map in context — data to {render.stamp(data_time)}", markers,
                                        valid=anyvalid)
        except OSError as e:
            facts["degraded"].append(f"no context image: {cfg['background_map']}: {e}")
            log.warning("no context image: %s", e)
    facts.update(stats)
    facts.update({"map_data_time": data_time, "sigma_before": sigma0,
                  "n_components": int(len(js)), "flux_cleaned": float(flux.sum()),
                  "gain": cl["gain"], "threshold_sigma": cl["threshold_sigma"],
                  "beam_deg": [cl["beam_ra_deg"], cl["beam_dec_deg"]],
                  "stretch": cl["stretch"]["mode"], "components": components,
                  "images": IMAGES, "run_seconds": round(time.perf_counter() - t0, 2)})
    log.info("%d iterations over %d beam rows in %.0fs: residual sigma %.3g (was %.3g), %d components",
             stats["iterations"], stats["psf_rows"], facts["run_seconds"], stats["sigma_after"],
             sigma0, facts["n_components"])
    return facts


def write_facts(path, facts: dict) -> None:
    try:
        write_json_atomic(path, facts)
    except OSError as e:
        log.warning("could not write %s: %s", path, e)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mapmaker-clean",
                                 description="deconvolve the mapmaker's sky map (Högbom CLEAN)")
    ap.add_argument("-c", "--config", required=True, help="path to the mapmaker YAML config")
    ap.add_argument("--state-dir", default=None,
                    help="the mapmaker's state directory (default: systemd's $STATE_DIRECTORY, "
                         "else /var/lib/choco/mapmaker)")
    ap.add_argument("-f", "--force", action="store_true",
                    help="deconvolve even if the sky map has not changed")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.WARNING - 10 * min(args.verbose, 2),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log.setLevel(min(log.getEffectiveLevel(), logging.INFO))

    try:
        cfg = M.load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        return 1
    state_dir = job_state_dir("mapmaker", args.state_dir)
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.error("state directory %s: %s", state_dir, e)
        return 2
    with M.single_run(state_dir / LOCK_FILE) as acquired:
        if not acquired:
            log.info("another deconvolution holds the lock; nothing to do")
            return 0
        try:
            facts = run(cfg, state_dir, force=args.force)
        except Degraded as e:
            # keep the last successful run's numbers on the page, add the reason
            facts = read_facts(state_dir / FACTS_FILE)
            facts.update({"updated": time.time(), "degraded": [str(e)], "errors": []})
            write_facts(state_dir / FACTS_FILE, facts)
            log.warning("degraded: %s", e)
            return 2
        except OSError as e:
            facts = read_facts(state_dir / FACTS_FILE)
            facts.update({"updated": time.time(), "degraded": [f"{e}"], "errors": []})
            write_facts(state_dir / FACTS_FILE, facts)
            log.warning("degraded: %s", e)
            return 2
        except ValueError as e:
            log.error("config: %s", e)
            return 1
    if facts.get("nothing"):
        log.info("nothing to do: %s", facts["nothing"])
        return 0
    write_facts(state_dir / FACTS_FILE, facts)
    return 2 if facts.get("degraded") else 0


if __name__ == "__main__":
    sys.exit(main())
