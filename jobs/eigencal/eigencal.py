#!/usr/bin/env python3
"""eigencal — one-shot point-source gain calibration from kotekan N² output.

Run once per invocation (a systemd timer sets the cadence; the script
self-gates): decide whether a calibrator transit has just completed, and
if so read the transit from kotekan's N² output files, eigendecompose the
per-polarisation visibility matrix at each (time, frequency), fit the
complex per-input response over the transit (``transit_fit.fit_transits``
— the science of CHIME's ch_cal point-source calibrator, vectorised),
archive one HDF5 file, and POST ``{update_id, start_time, gain, weight}``
through choco's group-update API to every kotekan node in the group.

No daemon, no dataset manager, no state between runs beyond the archive
directory (which doubles as the already-processed record).  Flagging is
kotekan's: the N² file's per-input ``flags`` dataset and ``frames_added``
validity are consumed as masks; eigencal derives no bad-input lists.  The
only local quality decisions are per-fit (chi^2, enough samples, dynamic
range against the off-source eigenvalue floor) and one final
don't-ship-garbage gate.

    python eigencal.py --config eigencal.example.yaml -n   # dry run

Exit codes: 0 = success or nothing-to-do; 2 = solution failed the quality
gate (not sent); 1 = error (systemd records it; the next timer tick retries).
"""

from __future__ import annotations

import argparse
import base64
import glob
import json
import logging
import os
import ssl
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import yaml

import n2_io
from transit_fit import (fit_transits, fringestop_phase, interpolate_gaps,
                         invert_no_zero)

log = logging.getLogger("eigencal")

C_MPS = 299792458.0
SIDEREAL_RATE_DEG_S = 360.9856473 / 86400.0   # LST advance per UT second


# -- config -----------------------------------------------------------------

DEFAULTS = {
    "kotekan_file": None,          # N² output glob (newest matches win); required
    "max_files": 4,                # newest matches considered when assembling the window
    "observer": {                  # DRAO; replace with the CHORD pad reference
        "latitude_deg": 49.320, "longitude_deg": -119.624, "altitude_m": 545.0,
    },
    "source": {
        "name": "CYG_A",
        "ra_deg": 299.86815, "dec_deg": 40.73392,   # J2000
        # log10(S/Jy) = sum_k c_k * log10(nu/GHz)**k
        # Perley & Butler (2017) Cyg A -- VERIFY before trusting the flux scale.
        "flux_log10_coeff": [3.3498, -1.0022, -0.2246, 0.0227, 0.0425],
    },
    "daytime": {"skip": True, "sun_alt_max_deg": -10.0},
    "run": {
        "max_age_s": 7200.0,       # process only transits completed this recently
        "archive_dir": "/var/lib/eigencal",
        "state_file": "/var/lib/eigencal/state.json",
        "gate_freq_mhz": 300.0,    # widest beam in the band, for gate sizing
    },
    "telescope": {
        "feed_layout": None,       # YAML: label -> pol/position (see example); required
        "dish_diameter_m": 6.0,
        "beam_fwhm_factor": 1.2,   # FWHM = factor * lambda / D
        "beam_peak_ha_deg": 0.0,   # beam peak hour angle (deg); 0 = on meridian
        "fringestop_sign": 1.0,    # VERIFY against the correlator convention
    },
    "analysis": {
        "nfreq_per_block": 32,     # memory ~ ntime * nf_block * nfeed_pol^2 * 8 B
        "nsigma_fit": 2.0,         # fit samples within this many beam sigma of peak
        "nsigma_off": 4.0,         # off-source samples beyond this many sigma
        "n_off_max": 64,           # cap on off-source samples eigendecomposed
        "min_off_source_samples": 10,
        "dyn_rng_threshold": 3.0,  # on/off largest-eigenvalue ratio gate
        "fit": {"poly_deg_amp": 5, "poly_deg_phi": 5, "niter": 3,
                "alpha": 0.32, "nsigma_window": 1.0},
        "chisq_per_dof_threshold": 100.0,
        "evaluate_gain_at": "transit",   # 'transit' (beam peak HA) or 'peak'
        "interpolate": {"enabled": True, "max_gap_channels": 8},
        "min_good_frac": 0.5,      # final gate before sending
    },
    "choco": {
        "url": None,               # unset -> payload summary printed, not sent
        "group": None,
        "endpoint": "updatable_config/gains",  # kotekan updatable endpoint
        "sync_delay": 5.0,         # start_time = now + sync_delay
        "transition_interval": 0.0,
    },
}


def merge_config(base, override):
    out = dict(base)
    for key, val in (override or {}).items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = merge_config(out[key], val)
        else:
            out[key] = val
    return out


def load_config(path):
    raw = yaml.safe_load(Path(path).read_text()) or {}
    cfg = merge_config(DEFAULTS, raw)
    if not cfg["kotekan_file"]:
        raise ValueError("config needs 'kotekan_file' (the kotekan N² output glob)")
    if not cfg["telescope"]["feed_layout"]:
        raise ValueError("config needs 'telescope.feed_layout' (the feed layout YAML)")
    if cfg["choco"]["url"] and not cfg["choco"]["group"]:
        raise ValueError("config needs 'choco.group' when choco.url is set")
    return cfg


# -- ephemeris (astropy; already a choco dependency via the EOP job) ---------


class Ephemeris:
    def __init__(self, obs_cfg, src_cfg):
        import astropy.units as u
        from astropy.coordinates import EarthLocation, SkyCoord

        self.u = u
        self.loc = EarthLocation(lat=obs_cfg["latitude_deg"] * u.deg,
                                 lon=obs_cfg["longitude_deg"] * u.deg,
                                 height=obs_cfg["altitude_m"] * u.m)
        self.lat_rad = np.radians(obs_cfg["latitude_deg"])
        self.src = SkyCoord(ra=src_cfg["ra_deg"] * u.deg,
                            dec=src_cfg["dec_deg"] * u.deg, frame="icrs")

    def _time(self, unix):
        from astropy.time import Time
        return Time(np.asarray(unix, dtype=float), format="unix",
                    location=self.loc)

    def apparent_radec_deg(self, unix):
        from astropy.coordinates import CIRS
        c = self.src.transform_to(CIRS(obstime=self._time(unix)))
        return float(c.ra.deg), float(c.dec.deg)

    def lst_deg(self, unix):
        return np.atleast_1d(self._time(unix).sidereal_time("apparent").deg)

    def hour_angle_rad(self, unix, ra_app_deg):
        ha = self.lst_deg(unix) - ra_app_deg
        return np.radians((ha + 180.0) % 360.0 - 180.0)

    def previous_transit(self, before_unix):
        """Most recent meridian transit before `before_unix` (fixed-point iteration)."""
        t = float(before_unix)
        for _ in range(4):
            ra, _ = self.apparent_radec_deg(t)
            dha = (float(self.lst_deg(t)[0]) - ra + 180.0) % 360.0 - 180.0
            t -= dha / SIDEREAL_RATE_DEG_S
        if t > before_unix:
            t -= 86164.0905
        return t

    def sun_alt_deg(self, unix):
        from astropy.coordinates import AltAz, get_sun
        t = self._time(unix)
        return float(get_sun(t).transform_to(
            AltAz(obstime=t, location=self.loc)).alt.deg)


# -- telescope helpers -------------------------------------------------------


def beam_sigma_rad(freq_mhz, tel_cfg):
    lam = C_MPS / (np.asarray(freq_mhz) * 1e6)
    return tel_cfg["beam_fwhm_factor"] * lam / tel_cfg["dish_diameter_m"] / 2.355


def source_flux_jy(freq_mhz, src_cfg):
    lognu = np.log10(np.asarray(freq_mhz) / 1000.0)
    return 10.0 ** sum(c * lognu ** k
                       for k, c in enumerate(src_cfg["flux_log10_coeff"]))


def load_feed_layout(path, labels):
    """Join the layout YAML to the N² file's label order.

    Returns ``(pols, feed_idx, ref_pos, dist)``: polarisation names, for
    each one the label-order indices of its feeds and the position of its
    phase-reference feed *within* that set, and (nfeed, 2) EW/NS positions
    (metres) relative to each polarisation's reference.  Feeds present in
    the file but absent from the layout are simply not fitted (their gain
    stays 0 / weight 0).
    """
    layout = yaml.safe_load(Path(path).read_text())
    by_label = {str(e["label"]): e for e in layout["feeds"]}
    refs = {str(k): str(v) for k, v in (layout.get("phase_reference") or {}).items()}

    dist = np.zeros((labels.size, 2))
    pol_of = np.array([str(by_label[l]["pol"]) if l in by_label else ""
                       for l in labels])
    for i, l in enumerate(labels):
        if l in by_label:
            dist[i] = (by_label[l]["ew_m"], by_label[l]["ns_m"])
    missing = int(np.sum(pol_of == ""))
    if missing:
        log.warning("%d of %d feeds in the N² file are not in the layout; "
                    "they will not be calibrated", missing, labels.size)

    pols, feed_idx, ref_pos = [], [], []
    label_list = list(labels)
    for pol in sorted(set(pol_of) - {""}):
        sel = np.flatnonzero(pol_of == pol)
        ref_label = refs.get(pol)
        if ref_label is None or ref_label not in label_list:
            raise ValueError(f"no phase_reference label for pol {pol!r} "
                             "(or it is not present in the N² file)")
        ref = label_list.index(ref_label)
        dist[sel] -= dist[ref]
        pols.append(pol)
        feed_idx.append(sel)
        ref_pos.append(int(np.flatnonzero(sel == ref)[0]))
    if not pols:
        raise ValueError("feed layout matched no feeds in the N² file")
    return pols, feed_idx, ref_pos, dist


# -- data collection ---------------------------------------------------------


def collect_segments(cfg, t_lo, t_hi):
    """Newest N² files whose time axes overlap [t_lo, t_hi].

    Returns ``(segments, times)``: a time-ordered list of
    ``(N2Meta, index array into that file's time axis)`` plus the
    concatenated timestamps.  All files must share the same label and
    frequency axes.
    """
    matches = glob.glob(cfg["kotekan_file"])
    if not matches:
        raise FileNotFoundError(f"no kotekan file matches {cfg['kotekan_file']!r}")
    newest_first = sorted(matches, key=os.path.getmtime, reverse=True)

    segments = []
    for path in newest_first[: int(cfg["max_files"])]:
        meta = n2_io.read_meta(path)
        if meta.time.size == 0 or meta.time.max() < t_lo or meta.time.min() > t_hi:
            continue
        sel = np.flatnonzero((meta.time >= t_lo) & (meta.time <= t_hi))
        if sel.size:
            segments.append((meta, sel))

    if not segments:
        raise ValueError("no N² data overlaps the transit window "
                         f"[{t_lo:.0f}, {t_hi:.0f}]")
    segments.sort(key=lambda s: s[0].time[s[1][0]])
    first = segments[0][0]
    for meta, _ in segments[1:]:
        if not (np.array_equal(meta.labels, first.labels)
                and np.allclose(meta.freq_mhz, first.freq_mhz)):
            raise ValueError(f"{meta.path} has a different label/freq axis "
                             f"than {first.path}")
    times = np.concatenate([m.time[s] for m, s in segments])
    return segments, times


def subselect_segments(segments, keep):
    """Restrict segment index arrays to the global boolean mask ``keep``."""
    out, pos = [], 0
    for meta, sel in segments:
        k = keep[pos:pos + sel.size]
        pos += sel.size
        if k.any():
            out.append((meta, sel[k]))
    return out


# -- the transit processing ---------------------------------------------------


def process_transit(cfg, transit_unix, eph):
    ana, tel = cfg["analysis"], cfg["telescope"]
    ra_app, dec_app = eph.apparent_radec_deg(transit_unix)
    dec_rad = np.radians(dec_app)
    beam_peak_ha = np.radians(tel["beam_peak_ha_deg"])

    # Probe the newest file for the axes, then size the selection windows.
    matches = glob.glob(cfg["kotekan_file"])
    if not matches:
        raise FileNotFoundError(f"no kotekan file matches {cfg['kotekan_file']!r}")
    probe = n2_io.read_meta(max(matches, key=os.path.getmtime))
    freq = probe.freq_mhz
    nfreq = freq.size
    sigma = beam_sigma_rad(freq, tel)
    win_fit = ana["nsigma_fit"] * sigma.max() / np.cos(dec_rad)
    win_off = ana["nsigma_off"] * sigma.max() / np.cos(dec_rad)
    omega = np.radians(SIDEREAL_RATE_DEG_S)          # rad of HA per second

    t_lo = transit_unix + (beam_peak_ha - 2 * win_off) / omega
    t_hi = transit_unix + (beam_peak_ha + 2 * win_off) / omega
    segments, times = collect_segments(cfg, t_lo, t_hi)

    ha_all = eph.hour_angle_rad(times, ra_app) - beam_peak_ha
    on = np.abs(ha_all) <= win_fit
    off = np.abs(ha_all) > win_off
    if on.sum() < 10:
        raise ValueError(f"only {int(on.sum())} on-source samples in the N² data "
                         f"(HA coverage {ha_all.min():.3f}..{ha_all.max():.3f} rad); "
                         "transit not covered")
    if off.sum() > ana["n_off_max"]:                 # decimate the off samples
        idx = np.flatnonzero(off)
        off[:] = False
        off[idx[np.linspace(0, idx.size - 1, ana["n_off_max"]).astype(int)]] = True
    use_off = int(off.sum()) >= ana["min_off_source_samples"]
    if not use_off:
        log.warning("only %d off-source samples; dynamic-range gate DISABLED",
                    int(off.sum()))

    keep = on | off
    segments = subselect_segments(segments, keep)
    ha = ha_all[keep]
    is_on = on[keep]
    nt = ha.size
    tau = float(np.median(np.abs(np.diff(times[keep]))))
    log.info("%d on-source + %d off-source samples from %d file(s); "
             "integration %.1f s", int(is_on.sum()), int(nt - is_on.sum()),
             len(segments), tau)

    meta0 = segments[0][0]
    labels = meta0.labels
    pols, feed_idx, ref_pos, dist = load_feed_layout(tel["feed_layout"], labels)
    flux = source_flux_jy(freq, cfg["source"])
    inv_bt = invert_no_zero(np.abs(meta0.freq_width_mhz) * 1e6 * tau)  # radiometer 1/(B*tau)

    in_flag = np.concatenate([n2_io.read_input_flags(m, s) for m, s in segments])
    ha_on = ha[is_on]

    ninput = labels.size
    gain = np.zeros((nfreq, ninput), dtype=np.complex64)
    weight = np.zeros((nfreq, ninput), dtype=np.float32)
    chisq_per_dof = np.zeros((2, nfreq, ninput), dtype=np.float32)

    pol_prods = [n2_io.pol_products(meta0, feeds) for feeds in feed_idx]
    nblock = int(np.ceil(nfreq / ana["nfreq_per_block"]))
    t0 = time.time()

    for bb in range(nblock):
        fsl = slice(bb * ana["nfreq_per_block"],
                    min((bb + 1) * ana["nfreq_per_block"], nfreq))
        nf = fsl.stop - fsl.start
        valid = np.concatenate([n2_io.read_valid(m, s, fsl) for m, s in segments])

        for pp, feeds in enumerate(feed_idx):
            prod_idx, ai, bi = pol_prods[pp]
            mp = feeds.size
            vis = np.concatenate(
                [n2_io.read_products(m, prod_idx, s, fsl) for m, s in segments])

            # Hermitian per-(time, freq) visibility matrix of this pol's
            # feeds, with kotekan-flagged inputs zeroed out.
            V = np.zeros((nt, nf, mp, mp), dtype=np.complex64)
            V[..., ai, bi] = vis
            V[..., bi, ai] = np.conj(vis)
            pf = in_flag[:, feeds].astype(np.float32)[:, None, :]
            V *= pf[..., :, None] * pf[..., None, :]
            del vis

            evals, evecs = np.linalg.eigh(V)
            lam = evals[..., -1]                       # largest eigenvalue
            v = evecs[..., :, -1]
            auto = np.abs(V[..., np.arange(mp), np.arange(mp)])
            del V, evals, evecs

            # Dynamic-range gate: on-source largest eigenvalue over the
            # median off-source largest eigenvalue (the noise floor).
            dyn_ok = np.ones((int(is_on.sum()), nf), dtype=bool)
            if use_off:
                lam_off = np.where(valid[~is_on], lam[~is_on], np.nan)
                with np.errstate(invalid="ignore"):
                    floor = np.nanmedian(lam_off, axis=0)
                dyn_ok = (lam[is_on] * invert_no_zero(floor)[None, :]
                          > ana["dyn_rng_threshold"])

            # Response of each input: sqrt(lambda) * eigenvector, phase-
            # referenced to this pol's reference feed.
            ref = v[..., ref_pos[pp]]
            ref_ok = np.abs(ref) > 0
            phase = np.exp(-1.0j * np.angle(np.where(ref_ok, ref, 1.0)))
            resp = (np.sqrt(np.maximum(lam, 0.0))[..., None] * v
                    * phase[..., None])[is_on]
            # Radiometer noise of each input, up to a constant absorbed by
            # the fit's chi^2/ndof covariance rescaling.
            err = np.sqrt(auto[is_on] * inv_bt[None, fsl, None])

            # Fringestop and normalise by the source flux.
            lam_inv = freq[fsl] * 1e6 / C_MPS
            u = dist[feeds, 0][None, :] * lam_inv[:, None]
            w = dist[feeds, 1][None, :] * lam_inv[:, None]
            resp *= fringestop_phase(ha_on, eph.lat_rad, dec_rad, u, w,
                                     sign=tel["fringestop_sign"])
            inv_rt_flux = invert_no_zero(np.sqrt(flux[fsl]))[None, :, None]
            resp *= inv_rt_flux
            err = err * inv_rt_flux

            flg = (valid[is_on] & dyn_ok & ref_ok[is_on])[..., None] \
                & (in_flag[is_on][:, None, feeds])

            ns = nf * mp
            window = None
            if ana["fit"]["nsigma_window"]:
                window = np.repeat(ana["fit"]["nsigma_window"]
                                   * sigma[fsl] / np.cos(dec_rad), mp)
            fit = fit_transits(
                ha_on, resp.reshape(-1, ns), err.reshape(-1, ns),
                flg.reshape(-1, ns),
                ha_eval=[0.0 if ana["evaluate_gain_at"] == "transit" else np.nan],
                window=window, **{k: ana["fit"][k] for k in
                                  ("poly_deg_amp", "poly_deg_phi", "niter", "alpha")})

            ok = fit["valid"] & np.all(
                fit["chisq_per_dof"] <= ana["chisq_per_dof_threshold"], axis=0)

            # Multiply-convention gain = 1/response; weight is the inverse
            # variance of the gain, from the model response and its
            # fractional error (as in ch_cal).  Raw-transit fallback where
            # the fit failed but the transit sample itself is fine.
            g_fit = invert_no_zero(fit["gain"][0])
            w_fit = (np.abs(fit["gain"][0])
                     * invert_no_zero(np.abs(fit["frac_err"][0]))) ** 2
            it_on = int(np.argmin(np.abs(ha_on)))
            r0 = resp.reshape(-1, ns)[it_on]
            e0 = err.reshape(-1, ns)[it_on]
            f0 = flg.reshape(-1, ns)[it_on] & (np.abs(r0) > 0) & (e0 > 0)
            g_raw = invert_no_zero(r0)
            w_raw = 0.5 * (np.abs(r0) ** 2 * invert_no_zero(e0)) ** 2

            g = np.where(ok, g_fit, np.where(f0, g_raw, 0.0))
            w2 = np.where(ok, w_fit, np.where(f0, w_raw, 0.0)).astype(np.float32)

            fi, ii = np.meshgrid(np.arange(fsl.start, fsl.stop), feeds,
                                 indexing="ij")
            gain[fi.ravel(), ii.ravel()] = g
            weight[fi.ravel(), ii.ravel()] = w2
            chisq_per_dof[:, fi.ravel(), ii.ravel()] = np.where(
                fit["valid"], fit["chisq_per_dof"], 0.0).astype(np.float32)

        if bb % 8 == 0:
            log.info("block %d/%d done (%.1f s elapsed)", bb + 1, nblock,
                     time.time() - t0)

    if ana["interpolate"]["enabled"]:
        gain, weight = interpolate_gaps(gain, weight,
                                        ana["interpolate"]["max_gap_channels"])

    good_frac = float((weight > 0).mean())
    log.info("fit complete in %.1f s; %.1f%% of (freq, input) cells good",
             time.time() - t0, 100 * good_frac)
    if good_frac < ana["min_good_frac"]:
        log.error("good fraction %.2f below threshold %.2f — not sending",
                  good_frac, ana["min_good_frac"])
        return None

    return {"gain": gain, "weight": weight, "chisq_per_dof": chisq_per_dof,
            "freq_mhz": freq, "labels": labels, "transit_time": transit_unix,
            "source": cfg["source"]["name"], "good_frac": good_frac}


# -- outputs ------------------------------------------------------------------


def encode_array(arr):
    arr = np.ascontiguousarray(arr)
    return {"shape": list(arr.shape), "dtype": str(arr.dtype),
            "encoding": "base64",
            "data": base64.b64encode(arr.tobytes()).decode("ascii")}


def build_payload(result, choco_cfg, now):
    tag = datetime.fromtimestamp(result["transit_time"],
                                 tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return {
        "update_id": f"eigencal_{tag}_{result['source'].lower()}",
        "start_time": now + float(choco_cfg["sync_delay"]),
        "transition_interval": float(choco_cfg["transition_interval"]),
        "gain": encode_array(result["gain"].astype(np.complex64)),
        "weight": encode_array(result["weight"].astype(np.float32)),
    }


def send_to_choco(choco_cfg, payload):
    """POST the gains through choco's group-update API (same path as bffs).

    choco accepts ``{"action": "updatable_config", "endpoint": ..., "values":
    ...}`` at ``POST /update/<group>`` and relays the values to every kotekan
    node in the group at ``POST /<endpoint>``.  Auth is bypassed for
    localhost callers and choco serves a self-signed certificate, so run
    eigencal on the choco host and skip TLS verification.
    """
    data = json.dumps({
        "action": "updatable_config",
        "endpoint": choco_cfg["endpoint"],
        "values": payload,
    }).encode()
    url = choco_cfg["url"].rstrip("/") + f"/update/{choco_cfg['group']}"
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"})
    ctx = None
    if url.startswith("https:"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=60.0, context=ctx) as resp:
        resp.read()


def write_archive(result, path, cfg):
    import h5py
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        f.attrs["source"] = result["source"]
        f.attrs["transit_time"] = result["transit_time"]
        f.attrs["created"] = time.time()
        f.attrs["good_frac"] = result["good_frac"]
        f.attrs["config"] = json.dumps(cfg)
        f.create_dataset("gain", data=result["gain"])
        f.create_dataset("weight", data=result["weight"])
        f.create_dataset("chisq_per_dof", data=result["chisq_per_dof"])
        f.create_dataset("index_map/freq", data=result["freq_mhz"])
        f.create_dataset("index_map/input",
                         data=np.array(result["labels"], dtype="S64"))
    log.info("wrote %s", path)


def write_state(cfg, result, sent):
    state_file = cfg["run"].get("state_file")
    if not state_file:
        return
    state = {"updated": time.time(), "transit_time": result["transit_time"],
             "source": result["source"], "good_frac": result["good_frac"],
             "sent": bool(sent)}
    p = Path(state_file)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(p)


# -- CLI ----------------------------------------------------------------------


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="eigencal",
                                description="point-source transit gain calibration")
    p.add_argument("-c", "--config", required=True, help="path to YAML config")
    p.add_argument("--transit-time", type=float, default=None,
                   help="unix time of the transit to process (default: most recent)")
    p.add_argument("-n", "--dry-run", action="store_true",
                   help="fit and archive, but send nothing")
    p.add_argument("-f", "--force", action="store_true",
                   help="ignore the daytime/age/already-done gates")
    p.add_argument("-v", "--verbose", action="count", default=0)
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING - 10 * min(args.verbose, 2),
        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log.setLevel(min(log.getEffectiveLevel(), logging.INFO))

    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, yaml.YAMLError) as e:
        log.error("bad config %s: %s", args.config, e)
        return 1

    try:
        eph = Ephemeris(cfg["observer"], cfg["source"])
        now = time.time()
        transit = args.transit_time or eph.previous_transit(now)

        # Self-gating: act only when a transit recently completed, in
        # darkness, and was not already processed.  Size the "complete"
        # time from the widest beam in the band (gate_freq_mhz).
        sig = float(beam_sigma_rad(np.array([cfg["run"]["gate_freq_mhz"]]),
                                   cfg["telescope"])[0])
        dec = np.radians(cfg["source"]["dec_deg"])
        t_done = transit + (np.radians(cfg["telescope"]["beam_peak_ha_deg"])
                            + cfg["analysis"]["nsigma_fit"] * sig / np.cos(dec)) \
            / np.radians(SIDEREAL_RATE_DEG_S)
        tag = datetime.fromtimestamp(transit, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        outfile = os.path.join(cfg["run"]["archive_dir"],
                               f"gain_{tag}_{cfg['source']['name'].lower()}.h5")

        if not args.force:
            if now < t_done:
                log.info("transit at %s not complete yet; nothing to do", tag)
                return 0
            if now - t_done > cfg["run"]["max_age_s"]:
                log.info("last transit (%s) is too old; nothing to do", tag)
                return 0
            if os.path.exists(outfile):
                log.info("transit %s already processed; nothing to do", tag)
                return 0
            if cfg["daytime"]["skip"] and \
                    eph.sun_alt_deg(transit) > cfg["daytime"]["sun_alt_max_deg"]:
                log.info("transit %s is in daytime; skipping", tag)
                return 0

        log.info("processing %s transit at %s", cfg["source"]["name"], tag)
        result = process_transit(cfg, transit, eph)
        if result is None:
            return 2

        write_archive(result, outfile, cfg)
        payload = build_payload(result, cfg["choco"], time.time())
        if args.dry_run or not cfg["choco"]["url"]:
            log.info("not sent (%s): update_id=%s, gain %s, %.1f%% good",
                     "dry run" if args.dry_run else "no choco url",
                     payload["update_id"], result["gain"].shape,
                     100 * result["good_frac"])
            write_state(cfg, result, sent=False)
        else:
            send_to_choco(cfg["choco"], payload)
            log.info("sent %s to choco group %s", payload["update_id"],
                     cfg["choco"]["group"])
            write_state(cfg, result, sent=True)
        return 0

    except (OSError, ValueError) as e:
        # Expected environmental failures (no N² file yet, choco not up,
        # transit not covered) get one useful line; -vv adds the traceback.
        log.error("%s: %s", type(e).__name__, e)
        log.debug("traceback:", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
