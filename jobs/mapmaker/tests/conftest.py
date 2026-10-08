"""Shared fixtures: a scratch state directory and a synthetic kotekan N²
file writer whose visibilities follow the job's own conventions, so the
tests check the chain end to end (file -> gains -> map) on known inputs.

``job_state_dir`` honours systemd's ``$STATE_DIRECTORY``; setting it here
keeps every ``main()`` run in the tests out of /var/lib/choco.
"""

import os

import h5py
import numpy as np
import pytest

import imaging as I

LAT, LON = 49.32075144444, -119.62081125
SEP_X, SEP_Y = 6.3, 8.5
CADENCE_S = 10.0
#: A bright "calibrator" the synthetic sky is made of (ICRS == CIRS in the
#: tests: source_cirs is patched to the identity).
SRC_RA, SRC_DEC, SRC_FLUX = 120.0, 40.8, 1.0
#: Array dishes on a 2 x 2 grid plus one RFI antenna, two polarizations.
DISHES = [(0, 0), (1, 0), (0, 1), (1, 1)]
N_RFI = 1
N_POL = 2


@pytest.fixture(autouse=True)
def scratch_state_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("STATE_DIRECTORY", str(tmp_path / "state"))
    return tmp_path / "state"


@pytest.fixture(autouse=True)
def identity_cirs(monkeypatch):
    """ICRS == CIRS and no Sun anywhere near the beam, unless a test says
    otherwise: the astropy transforms are not what is under test."""
    import mapmaker
    monkeypatch.setattr(mapmaker, "source_cirs", lambda ra, dec, t: (ra, dec))
    monkeypatch.setattr(mapmaker, "sun_cirs",
                        lambda t: (np.zeros(len(np.atleast_1d(t))), np.full(len(np.atleast_1d(t)), -60.0)))


def element_table():
    """Per-element labels, types, pols and positions in kotekan's [pol][dish] order."""
    n_dish = len(DISHES) + N_RFI
    labels, types, pols, pos = [], [], [], []
    names = ["A01", "A02", "B01", "B02"][:len(DISHES)] + [f"RFIA{i+1}" for i in range(N_RFI)]
    for p, pname in enumerate("XY"[:N_POL]):
        for d in range(n_dish):
            labels.append(names[d] + pname)
            pols.append(p)
            if d < len(DISHES):
                types.append(0)
                pos.append([DISHES[d][0] * SEP_X, DISHES[d][1] * SEP_Y, 0.0])
            else:
                types.append(1)
                pos.append([100.0 + d, -30.0, 0.0])
    return labels, np.array(types, np.int32), np.array(pols, np.int32), np.array(pos)


def products(n_el):
    a, b = np.triu_indices(n_el)
    return a.astype(np.uint16), b.astype(np.uint16)


def element_gains(n_el, n_freq, seed=0):
    """Per-element complex gains with a cable delay: amplitude 0.5–2,
    phase = constant + 2π ν τ_cable."""
    rng = np.random.default_rng(seed)
    amp = rng.uniform(0.5, 2.0, n_el)
    phi0 = rng.uniform(-np.pi, np.pi, n_el)
    tau = rng.uniform(-100e-9, 100e-9, n_el)
    return amp, phi0, tau


def write_vis_file(path, idx, t0_unix, era0_deg, *, n_time=20, n_freq=128,
                   freq_lo=400.0, freq_hi=800.0, dead_elements=(), rfi_channels=(),
                   noise=1e-3, seed=0, source=(SRC_RA, SRC_DEC, SRC_FLUX),
                   pointing_dec=SRC_DEC, dish_diameter=6.0):
    """A kotekan-shaped N² file with one point source seen through a
    Gaussian beam, element gains and noise.  Returns the sample times."""
    labels, types, pols, pos = element_table()
    n_el = len(labels)
    ia, ib = products(n_el)
    n_prod = len(ia)
    freq = np.linspace(freq_lo, freq_hi, n_freq)
    era = era0_deg + np.arange(n_time) * CADENCE_S / 86400.0 * I.ERA_DEG_PER_DAY
    t = t0_unix + np.arange(n_time) * CADENCE_S
    era_local = I.era_local_deg(era, LON)
    rng = np.random.default_rng(seed + idx)

    amp, phi0, tau_c = element_gains(n_el, n_freq)
    g = amp[None, :] * np.exp(1j * (phi0[None, :] + 2 * np.pi * freq[:, None] * 1e6 * tau_c[None, :]))  # (f, el)

    vis = (rng.normal(size=(n_freq, n_prod, n_time)) + 1j * rng.normal(size=(n_freq, n_prod, n_time))) * noise
    if source is not None:
        ra, dec, flux = source
        uE, uN, uU = I.enu(ra, dec, era_local, LAT)
        pvec = I.pointing_vector(pointing_dec, LAT)
        cosang = uE * pvec[0] + uN * pvec[1] + uU * pvec[2]
        bl = pos[ib, :2] - pos[ia, :2]                                        # (n_prod, 2)
        tau = I.source_delays(bl, uE, uN)                                     # (n_prod, t)
        for fi, f in enumerate(freq):
            fwhm = float(I.beam_fwhm_deg(f, dish_diameter))
            b2 = I.beam_sq(cosang, fwhm)                                      # (t,)
            model = flux * b2[None, :] * np.exp(2j * np.pi * f * 1e6 * tau)   # (n_prod, t)
            same_pol = pols[ia] == pols[ib]
            array = (types[ia] == 0) & (types[ib] == 0)
            vis[fi] += np.where((same_pol & array)[:, None], model, 0)
            vis[fi] *= (g[fi, ia] * np.conj(g[fi, ib]))[:, None]
    vis = vis.astype(np.complex64)
    weight = np.full((n_freq, n_prod, n_time), 1.0 / noise ** 2, np.float32)
    flags = np.ones((n_freq, n_el, n_time), np.float32)
    for e in dead_elements:
        flags[:, e, :] = 0
        dead = (ia == e) | (ib == e)
        vis[:, dead, :] = 0
    frac_rfi = np.zeros((n_freq, n_time), np.float32)
    for c in rfi_channels:
        frac_rfi[c, :] = 0.5

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with h5py.File(path, "w") as f:
        im = f.create_group("index_map")
        im.create_dataset("freq", data=np.array(list(zip(freq, np.full(n_freq, (freq_hi - freq_lo) / n_freq))),
                                                dtype=[("centre", "<f8"), ("width", "<f8")]))
        im.create_dataset("prod", data=np.array(list(zip(ia, ib)), dtype=[("input_a", "<u2"), ("input_b", "<u2")]))
        im.create_dataset("label", data=np.array(labels, dtype=h5py.string_dtype()))
        im.create_dataset("pol", data=pols)
        im.create_dataset("type", data=types)
        f.create_dataset("vis", data=vis, chunks=(min(16, n_freq), min(16, n_prod), n_time))
        f.create_dataset("vis_weight", data=weight, chunks=(min(16, n_freq), min(16, n_prod), n_time))
        f.create_dataset("flags", data=flags)
        f.create_dataset("frac_rfi", data=frac_rfi)
        f.create_dataset("bin_ERA_deg", data=era)
        f.attrs["feed_positions_m"] = pos
        f.attrs["itrs_lat_deg"] = LAT
        f.attrs["itrs_lon_deg"] = LON
        f.attrs["feed_separation_x_m"] = SEP_X
        f.attrs["feed_separation_y_m"] = SEP_Y
        f.attrs["num_elements"] = np.uint32(n_el)
        f.attrs["abs_file_idx"] = np.uint64(idx)
    return t


def file_name(idx, t_unix):
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(t_unix, tz=timezone.utc)
    return f"vis_{idx:010d}_{dt.strftime('%Y%m%dT_%H%M%S')}_{int((t_unix % 1) * 1e9):09d}.h5"


def transit_era0(ra_deg, hours_before: float) -> float:
    """The ERA at which a file starting *hours_before* a transit of
    *ra_deg* begins (local sidereal angle = RA at transit)."""
    return (ra_deg - LON - hours_before * I.ERA_DEG_PER_DAY / 24.0) % 360.0


def write_acquisition(root, acq, n_files, first_idx=1000, hours_before=0.6, t0=1_791_000_000.0, **kw):
    """*n_files* consecutive files whose span straddles the source transit
    (starting *hours_before* it).  Returns the list of paths."""
    paths = []
    era0 = transit_era0(SRC_RA, hours_before)
    for k in range(n_files):
        idx = first_idx + k
        t_start = t0 + k * 20 * CADENCE_S
        era_k = era0 + k * 20 * CADENCE_S / 86400.0 * I.ERA_DEG_PER_DAY
        p = os.path.join(root, acq, file_name(idx, t_start))
        write_vis_file(p, idx, t_start, era_k, **kw)
        paths.append(p)
    return paths


def write_cfg(path, root, **over):
    import yaml
    cfg = {"roots": [{"name": "subset", "path": str(root)}],
           "pointing_dec_deg": SRC_DEC,
           "colour_bins": 4, "colour_range_mhz": [400, 800],
           "subband_channels": 16,
           "bad_freq_mhz": [],
           "n_ra_bins": 1024, "dec_halfwidth_deg": 6.0, "dec_step_deg": 0.5,
           "calibrators": [{"name": "Test A", "ra_deg": SRC_RA, "dec_deg": SRC_DEC}],
           "calibration_window_deg": 3.0, "min_cal_samples": 20,
           "sun_exclusion_deg": 30.0,
           "max_files_per_run": 0,
           # no context image unless a test asks for one (the real map
           # may or may not be in this tree)
           "background_map": ""}
    cfg.update(over)
    path.write_text(yaml.safe_dump(cfg))
    return path


def write_healpix(path, values, nside, ordering="RING", coordsys="GALACTIC"):
    """A minimal HEALPix FITS file, laid out like LAMBDA's."""
    from astropy.io import fits
    col = fits.Column(name="TEMPERATURE", format="E", unit="K",
                      array=np.asarray(values, dtype=np.float32))
    hdu = fits.BinTableHDU.from_columns([col])
    hdu.header.update({"PIXTYPE": "HEALPIX", "ORDERING": ordering, "NSIDE": nside,
                       "COORDSYS": coordsys, "BAD_DATA": -1.6375e30})
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path)
    return path


@pytest.fixture
def tiny_sky(tmp_path):
    """An NSIDE=8 map with some structure, for the context image."""
    nside = 8
    return write_healpix(tmp_path / "tiny.fits", 20.0 + np.arange(12 * nside * nside) % 11, nside)
