"""End-to-end: synthesize an N² file around a real transit, recover the gains.

Builds a CHORD-layout hdf5N2Write file containing a Cyg A transit with known
per-input complex gains — Gaussian beam, correct fringe geometry for the
configured feed positions, radiometer-level noise — and checks that
``process_transit`` returns the inverse gains.  This exercises the whole
chain: file collection, on/off selection, per-pol eigendecomposition, the
dynamic-range gate, kotekan flag ingestion, phase referencing,
fringestopping, flux normalisation, and the batched fit.
"""

import time

import h5py
import numpy as np
import pytest
import yaml

import eigencal
from eigencal import (DEFAULTS, Ephemeris, beam_sigma_rad, merge_config,
                      process_transit, source_flux_jy)
from transit_fit import invert_no_zero

NDISH = 4                      # 4 dishes x 2 pol = 8 inputs
NFEED = 2 * NDISH
FREQ_MHZ = np.array([500.0, 520.0, 540.0, 560.0])
TSTEP = 60.0

# Dish labels; the file's per-element axis is [P][D]:
# element = dish + pol * NDISH, pol 0 = X — the X block, then the Y block.
DISH_LABELS = [f"d{d:04d}" for d in range(NDISH)]
ELEM_LABELS = [f"{lbl}{p}" for p in "XY" for lbl in DISH_LABELS]
POS_EW = np.tile(np.array([0.0, 9.0, 21.0, 40.0]), 2)     # metres, per element
POS_NS = np.tile(np.array([0.0, 4.0, -7.0, 11.0]), 2)
FLAGGED_FEED = 7               # kotekan marks this input bad (d0003Y)
# The file records positions in kotekan's grid frame, here rotated 20 deg
# from East/North about Up (v_grid = R . v_topo), so the derived layout
# must rotate them back before the fringe geometry is right.
_TH = np.radians(20.0)
GRID_ORIENTATION = np.array([[np.cos(_TH), np.sin(_TH), 0.0],
                             [-np.sin(_TH), np.cos(_TH), 0.0],
                             [0.0, 0.0, 1.0]])


@pytest.fixture(params=["file", "yaml"])
def setup(request, tmp_path):
    """The two layout sources: derived from the N² file's own geometry
    (the default) or an operator's YAML override."""
    if request.param == "yaml":
        layout = {
            "phase_reference": {"X": "d0000X", "Y": "d0000Y"},
            "feeds": [{"label": lbl, "pol": lbl[-1],
                       "ew_m": float(POS_EW[i]), "ns_m": float(POS_NS[i])}
                      for i, lbl in enumerate(ELEM_LABELS)],
        }
        layout_path = tmp_path / "feeds.yaml"
        layout_path.write_text(yaml.safe_dump(layout))
        telescope = {"feed_layout": str(layout_path)}
    else:
        telescope = {"phase_reference": {"X": "d0000X", "Y": "d0000Y"}}

    cfg = merge_config(DEFAULTS, {
        "kotekan_file": str(tmp_path / "*.h5"),
        "run": {"archive_dir": str(tmp_path / "archive"),
                "state_file": None},
        "telescope": telescope,
        "analysis": {"nfreq_per_block": 4, "min_good_frac": 0.3},
        "daytime": {"skip": False},
    })

    eph = Ephemeris(cfg["observer"], cfg["source"])
    transit = eph.previous_transit(time.time() - 43200)   # a completed transit
    return cfg, eph, transit, tmp_path


def _make_file(cfg, eph, transit, tmp_path, rng):
    ra_app, dec_app = eph.apparent_radec_deg(transit)
    dec = np.radians(dec_app)
    lat = eph.lat_rad
    sign = cfg["telescope"]["fringestop_sign"]

    sigma = beam_sigma_rad(FREQ_MHZ, cfg["telescope"])    # (nf,)
    win_off = (cfg["analysis"]["nsigma_off"] * sigma.max() / np.cos(dec))
    omega = np.radians(eigencal.SIDEREAL_RATE_DEG_S)
    half_span = 1.5 * win_off / omega
    nhalf = int(half_span // TSTEP)
    times = transit + TSTEP * np.arange(-nhalf, nhalf + 1)
    nt = times.size

    ha = eph.hour_angle_rad(times, ra_app)                # (nt,)
    flux = source_flux_jy(FREQ_MHZ, cfg["source"])        # (nf,) Jy

    # Known input gains; the response in the data is r_i = g_i * fringe_i *
    # sqrt(flux * beam), so eigencal (which divides by sqrt(flux), removes
    # the fringe, and inverts) should return 1/g_i up to one constant phase
    # per polarisation (the reference feed's own gain phase).
    gain_in = (rng.uniform(0.5, 2.0, (len(FREQ_MHZ), NFEED))
               * np.exp(2j * np.pi * rng.random((len(FREQ_MHZ), NFEED))))

    e_comp = -np.cos(dec) * np.sin(ha)
    n_comp = np.cos(lat) * np.sin(dec) - np.sin(lat) * np.cos(dec) * np.cos(ha)
    lam_inv = FREQ_MHZ * 1e6 / eigencal.C_MPS
    geom = (e_comp[:, None, None] * (POS_EW[None, None, :] * lam_inv[None, :, None])
            + n_comp[:, None, None] * (POS_NS[None, None, :] * lam_inv[None, :, None]))
    fringe = np.exp(-sign * 2j * np.pi * geom)            # (nt, nf, nfeed)

    beam = np.exp(-0.5 * (ha[:, None] / (sigma[None, :] / np.cos(dec))) ** 2)
    resp = (gain_in[None] * fringe
            * np.sqrt(flux[None, :, None] * beam[..., None]))   # (nt, nf, nfeed)

    # Visibilities per pol: v_ij = r_i r_j* (cross-pol products zero), plus
    # noise well below the source but well above numerical zero (it sets the
    # off-source eigenvalue floor for the dynamic-range gate).
    a, b = np.triu_indices(NFEED)
    same_pol = (a // NDISH) == (b // NDISH)   # [P][D]: pol blocks of NDISH
    vis = np.where(same_pol[None, None, :],
                   resp[..., a] * np.conj(resp[..., b]), 0.0)
    noise_amp = 1e-3 * flux.mean()
    vis = vis + noise_amp * (rng.standard_normal(vis.shape)
                             + 1j * rng.standard_normal(vis.shape))
    vis[..., a == b] = np.abs(vis[..., a == b])           # autos stay real+

    path = tmp_path / "n2_0000.h5"
    with h5py.File(path, "w") as f:
        f.attrs["num_elements"] = NFEED
        # Per-element geometry as hdf5N2Write records it: every dish an
        # ArrayDish, positions in the grid frame (E/N/U rotated by R).
        enu = np.stack([POS_EW, POS_NS, np.zeros(NFEED)], axis=-1)
        f.attrs["feed_positions_m"] = enu @ GRID_ORIENTATION.T
        f.attrs["grid_orientation"] = GRID_ORIENTATION
        im = f.create_group("index_map")
        im.create_dataset("type", data=np.zeros(NFEED, dtype=np.int32))
        # kotekan's per-element table (chord.2021.10+988): dish label +
        # p1/p2 in [P][D] order, with the polarization index alongside.
        _dishes = [d.decode() if isinstance(d, bytes) else str(d) for d in DISH_LABELS]
        im.create_dataset("label", data=np.array(
            [f"{d}p{pol + 1}" for pol in range(2) for d in _dishes], dtype="S16"))
        im.create_dataset("pol", data=np.array(
            [pol for pol in range(2) for _ in _dishes], dtype=np.int32))
        freq = np.zeros(len(FREQ_MHZ), dtype=[("centre", "<f8"), ("width", "<f8")])
        freq["centre"], freq["width"] = FREQ_MHZ, 20.0
        im.create_dataset("freq", data=freq)
        t = np.zeros(nt, dtype=[("fpga_count", "<u8"), ("ctime", "<f8")])
        t["ctime"] = times - 0.5 * TSTEP                  # file stamps starts
        im.create_dataset("time", data=t)
        im.create_dataset("prod", data=np.stack([a, b], axis=-1).astype(np.uint16))
        f.create_dataset("vis", data=vis.astype(np.complex64).transpose(1, 2, 0))
        f.create_dataset("frames_added", data=np.ones((len(FREQ_MHZ), nt), np.int64))
        flags = np.ones(NFEED, dtype=np.float32)
        flags[FLAGGED_FEED] = 0.0
        f.create_dataset("flags", data=flags)
    return gain_in


def test_process_transit_recovers_gains(setup):
    cfg, eph, transit, tmp_path = setup
    rng = np.random.default_rng(3)
    gain_in = _make_file(cfg, eph, transit, tmp_path, rng)

    result = process_transit(cfg, transit, eph)
    assert result is not None

    g_out = result["gain"]
    w_out = result["weight"]

    # The kotekan-flagged feed is not calibrated.
    assert np.all(g_out[:, FLAGGED_FEED] == 0)
    assert np.all(w_out[:, FLAGGED_FEED] == 0)

    good = w_out > 0
    ok_feeds = np.ones(NFEED, dtype=bool)
    ok_feeds[FLAGGED_FEED] = False
    assert good[:, ok_feeds].mean() > 0.95

    # g_out * gain_in should be a constant per (freq, pol): the reference
    # feed's gain phase (unit magnitude times any per-pol phase).
    prod = g_out * gain_in
    for pol_feeds in (np.arange(NDISH), np.arange(NDISH, NFEED)):
        pf = pol_feeds[pol_feeds != FLAGGED_FEED]
        for ff in range(len(FREQ_MHZ)):
            vals = prod[ff, pf][good[ff, pf]]
            assert vals.size >= 3
            assert np.all(np.abs(np.abs(vals) - 1.0) < 0.02), \
                f"amplitude not recovered at freq {ff}: {np.abs(vals)}"
            ref = vals / vals[0]                          # constant phase?
            assert np.all(np.abs(np.angle(ref)) < 0.02), \
                f"phase not recovered at freq {ff}: {np.angle(ref)}"


def test_quality_gate_rejects_noise_only_data(setup):
    """With no source in the data the dynamic-range gate starves the fit."""
    cfg, eph, transit, tmp_path = setup
    rng = np.random.default_rng(4)
    gain_in = _make_file(cfg, eph, transit, tmp_path, rng)

    # Overwrite vis with pure noise (keep everything else).
    path = tmp_path / "n2_0000.h5"
    with h5py.File(path, "r+") as f:
        shape = f["vis"].shape
        f["vis"][...] = (0.1 * (rng.standard_normal(shape)
                                + 1j * rng.standard_normal(shape))).astype(np.complex64)

    assert process_transit(cfg, transit, eph) is None


def test_stale_files_raise_oserror_for_degraded_exit(setup):
    """Files that don't cover the transit are a data-availability problem:
    OSError -> exit 2 (degraded badge), not ValueError -> failed."""
    cfg, eph, transit, tmp_path = setup
    rng = np.random.default_rng(3)
    _make_file(cfg, eph, transit, tmp_path, rng)
    # A transit a day later than the data on disk: nothing overlaps.
    with pytest.raises(OSError, match="acquisition down or files too old"):
        eigencal.collect_segments(cfg, transit + 86400 - 600, transit + 86400 + 600)


def test_no_files_at_all_is_oserror(setup):
    cfg, eph, transit, tmp_path = setup
    with pytest.raises(OSError):
        eigencal.collect_segments(cfg, transit - 600, transit + 600)
