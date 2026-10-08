"""The deconvolution step: the dirty beam from the kernels, Högbom CLEAN on
synthetic accumulators (a two-row array's Dec ridge), the restore, the
run's gating and exits, and the ingest's bookkeeping it depends on."""

import json
import os

import numpy as np
import pytest

import clean as C
import imaging as I
import mapmaker as M
from conftest import SRC_DEC, SRC_RA, write_acquisition, write_cfg, LAT, LON


# --- a synthetic two-row array and its kernels ---------------------------------

def two_row_cfg(tmp_path, **over):
    """A 2 x 4 grid's kind of map: a few sub-bands in two colour bins."""
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", tmp_path / "root",
                                  colour_bins=2, colour_range_mhz=[400, 800],
                                  n_ra_bins=1024, dec_halfwidth_deg=6.0, dec_step_deg=0.5, **over))
    return cfg


def two_row_geometry():
    """Unique baselines of a 2 (NS) x 4 (EW) grid, canonical orientation,
    and four imaged sub-bands, two per colour bin."""
    xy = []
    for ky in (0, 1):
        for kx in range(-3, 4):
            if kx < 0 and ky == 0 or (kx == 0 and ky == 0):
                continue
            if ky == 0 and kx < 0:
                continue
            xy.append([kx * 6.3, ky * 8.5])
    subbands = [[0, 450.0, 0], [1, 520.0, 0], [2, 650.0, 1], [3, 760.0, 1]]
    return {"lat_deg": LAT, "lon_deg": LON, "baselines_xy": xy, "subbands": subbands}


def unit_weights(geometry, n_sub=4):
    """Every baseline counted once per sub-band (a complete grid: the EW-only
    ones twice, since both rows have them)."""
    xy = np.asarray(geometry["baselines_xy"])
    w = np.where(xy[:, 1] == 0, 2.0, 1.0)
    return np.tile(w, (n_sub, 1))


def accumulate_point_source(cfg, geometry, blw, j0, p0, flux=1.0, n_pol=1):
    """Scatter a unit source at Dec row j0, RA bin p0 through the kernels
    exactly as the ingest would: ``(num, exp)`` per colour bin."""
    grid = I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"], cfg["dec_halfwidth_deg"],
                        cfg["dec_step_deg"], geometry["lat_deg"], geometry["lon_deg"])
    xy = np.asarray(geometry["baselines_xy"])
    num = np.zeros((cfg["colour_bins"], grid.n_dec, grid.n_bins), np.float32)
    exp = np.zeros_like(num)
    for s, fc, band in geometry["subbands"]:
        fwhm = float(I.beam_fwhm_deg(fc, cfg["dish_diameter_m"], cfg["beam_fwhm_factor"]))
        k = I.build_kernel(grid, xy, fc, fwhm, cfg["footprint_fwhm"])
        for n in range(grid.n_bins):                      # every sidereal bin visited once
            d = (p0 - n + grid.n_bins // 2) % grid.n_bins - grid.n_bins // 2   # the RA axis wraps
            if d < -k.M or d > k.M:                       # outside the footprint: no signal, exposure only
                V = np.zeros(len(xy), np.complex64)
            else:
                V = flux * np.conj(k.Kc[:, j0, d + k.M])  # the model visibility K[b, j0, p0 - n]
            I.scatter(num[band], exp[band], k, n, V, blw[s].astype(np.float32))
    return grid, num, exp


def test_dirty_beam_peaks_at_one_on_the_pointing_row_and_has_the_dec_ridge(tmp_path):
    cfg = two_row_cfg(tmp_path)
    geo = two_row_geometry()
    blw = unit_weights(geo)
    n_dec = int(round(2 * cfg["dec_halfwidth_deg"] / cfg["dec_step_deg"])) + 1
    beam = C.build_beam(cfg, geo, blw, cfg["colour_bins"], n_dec)
    j0 = n_dec // 2
    psf, taper = beam.row(j0)
    D = beam.D
    assert psf.shape == (2, n_dec, 2 * D + 1)
    assert taper == pytest.approx([1.0, 1.0], abs=1e-4)
    # symmetric in RA along the source's own row, and the Dec column is a
    # ridge: the same-row baselines leave a pedestal, never a clean null
    assert np.allclose(psf[:, j0, :], psf[:, j0, ::-1], atol=1e-4)
    col = psf[0, :, D]
    assert col.argmax() == j0 and col[j0 - 2] > 0.2 and col[j0 + 2] > 0.2
    # off the pointing row a source reads with the beam taper
    _, taper_off = beam.row(j0 + 6)
    assert np.all(taper_off < 0.9) and np.all(taper_off > 0)
    assert len(beam._rows) == 2                               # rows are kept


def test_clean_recovers_a_point_source_and_removes_its_ridge(tmp_path):
    cfg = two_row_cfg(tmp_path)
    geo = two_row_geometry()
    blw = unit_weights(geo)
    grid, num, exp = accumulate_point_source(cfg, geo, blw, j0=12, p0=300)
    n_band, n_dec, n_bins = num.shape
    beam = C.build_beam(cfg, geo, blw, n_band, n_dec)
    valid = exp > cfg["exposure_floor"] * exp.reshape(n_band, -1).max(1)[:, None, None]
    m = np.where(valid, num / np.maximum(exp, 1e-30), 0).astype(np.float32)
    wpix = (exp / np.maximum(exp.sum(0), 1e-30)).astype(np.float32)
    dirty_col = (wpix * m).sum(0)[:, 300].copy()
    assert dirty_col[12] == pytest.approx(1.0, abs=0.02)
    away = np.abs(np.arange(n_dec) - 12) >= 4
    assert m[0, away, 300].max() > 0.1                         # the ridge, before: a lobe 2 deg or more away
    model, facts = C.hogbom(m, valid, wpix, beam, gain=0.3, threshold=1e-3, max_iterations=5000)
    flux = (wpix * model).sum(0)
    j, p = np.unravel_index(flux.argmax(), flux.shape)
    assert (j, p) == (12, 300)
    assert flux[12, 300] == pytest.approx(1.0, abs=0.03)
    assert flux.sum() == pytest.approx(1.0, abs=0.05)           # nothing invented elsewhere
    assert facts["iterations"] > 0 and facts["residual_max"] < 1e-3 and facts["psf_rows"] >= 1
    residual_col = (wpix * m).sum(0)[:, 300]
    assert np.all(np.abs(residual_col) < 1e-3)                  # the ridge, after
    restored = C.restore(model, m, valid, 360.0 / n_bins, cfg["dec_step_deg"], 0.6, 1.5)
    assert restored.shape == num.shape
    rb = np.nansum(restored * wpix, axis=0)
    assert rb[12, 300] == pytest.approx(1.0, abs=0.03)
    assert rb[12, 300 + 8] < 0.05 and rb[12 + 4, 300] < 0.1   # restored with the clean beam, not the ridge


def test_clean_handles_a_source_at_the_ra_wrap(tmp_path):
    cfg = two_row_cfg(tmp_path)
    geo = two_row_geometry()
    blw = unit_weights(geo)
    grid, num, exp = accumulate_point_source(cfg, geo, blw, j0=12, p0=2)
    n_band, n_dec, n_bins = num.shape
    beam = C.build_beam(cfg, geo, blw, n_band, n_dec)
    valid = exp > 0
    m = np.where(valid, num / np.maximum(exp, 1e-30), 0).astype(np.float32)
    wpix = (exp / np.maximum(exp.sum(0), 1e-30)).astype(np.float32)
    model, facts = C.hogbom(m, valid, wpix, beam, gain=0.3, threshold=1e-3, max_iterations=5000)
    flux = (wpix * model).sum(0)
    assert np.unravel_index(flux.argmax(), flux.shape) == (12, 2)
    assert np.abs((wpix * m).sum(0)).max() < 1e-3               # both ends of the wrap subtracted
    restored = C.restore(model, m, valid, 360.0 / n_bins, cfg["dec_step_deg"], 2.0, 1.5)
    rb = np.nansum(restored * wpix, axis=0)
    assert rb[12, n_bins - 1] > 0.3                             # a 2 deg beam (5.7 bins) wraps round too


def test_clean_of_an_empty_map_does_nothing(tmp_path):
    cfg = two_row_cfg(tmp_path)
    geo = two_row_geometry()
    blw = unit_weights(geo)
    n_dec = int(round(2 * cfg["dec_halfwidth_deg"] / cfg["dec_step_deg"])) + 1
    beam = C.build_beam(cfg, geo, blw, cfg["colour_bins"], n_dec)
    m = np.zeros((cfg["colour_bins"], n_dec, cfg["n_ra_bins"]), np.float32)
    valid = np.ones_like(m, bool)
    wpix = np.full_like(m, 0.5)
    model, facts = C.hogbom(m, valid, wpix, beam, gain=0.25, threshold=0.0, max_iterations=1000)
    assert facts["iterations"] == 0 and not model.any() and facts["psf_rows"] == 0


def test_restore_is_a_unit_peak_gaussian_per_component():
    model = np.zeros((1, 25, 256), np.float32)
    model[0, 12, 100] = 2.0
    valid = np.ones_like(model, bool)
    out = C.restore(model, np.zeros_like(model), valid, 0.1, 0.5, beam_ra_deg=0.5, beam_dec_deg=1.0)
    assert out[0, 12, 100] == pytest.approx(2.0, rel=1e-4)
    assert out[0, 12, 100 + 5] == pytest.approx(0.125, rel=0.02)  # RA FWHM 0.5 deg = 5 bins: 2 sigma out
    assert out[0, 12, 100 - 5] == pytest.approx(0.125, rel=0.02)
    assert out[0, 13, 100] == pytest.approx(1.0, rel=0.02)       # Dec FWHM 1 deg = 2 rows: half maximum
    assert np.isnan(C.restore(model, model, ~valid, 0.1, 0.5, 0.5, 1.0)).all()


# --- the run: gating, outputs, exits ------------------------------------------------

@pytest.fixture
def acquisition(tmp_path):
    root = tmp_path / "root"
    paths = write_acquisition(str(root), "acq_20260101_000000_1", 20, hours_before=0.6)
    return root, paths


def test_ingest_records_geometry_and_baseline_weights(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    with np.load(sd / M.MAPS_FILE, allow_pickle=False) as z:
        meta = json.loads(str(z["meta"]))
        blw = z["sky_blw"]
    geo = meta["geometry"]
    assert geo["lat_deg"] == pytest.approx(LAT) and geo["lon_deg"] == pytest.approx(LON)
    assert len(geo["baselines_xy"]) == blw.shape[1] == 4            # the 2 x 2 test grid
    assert [b for _, _, b in geo["subbands"]] == sorted(b for _, _, b in geo["subbands"])
    assert blw.shape[0] == 8 and blw.sum() > 0                      # 128 channels / 16 per sub-band
    # the weights only count what reached the sky map: the same w the scatter saw
    assert np.all(blw[[s for s, _, _ in geo["subbands"]]].sum(1) > 0)


def test_run_cleans_the_synthetic_map_and_self_gates(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    facts = C.run(cfg, sd)
    assert "nothing" not in facts and not facts["degraded"]
    assert (sd / "clean.png").exists() and (sd / "clean-strip.png").exists()
    assert facts["iterations"] > 0 and facts["n_components"] >= 1
    top = facts["components"][0]
    assert abs(top["ra_deg"] - SRC_RA) < 1.0 and abs(top["dec_deg"] - SRC_DEC) < 0.6
    assert facts["sigma_after"] <= facts["sigma_before"]
    assert facts["map_data_time"] is not None
    C.write_facts(sd / C.FACTS_FILE, facts)
    again = C.run(cfg, sd)
    assert again["nothing"].startswith("the sky map has not changed")
    assert "nothing" not in C.run(cfg, sd, force=True)


def test_run_draws_the_context_image_when_the_map_is_there(acquisition, tmp_path, tiny_sky):
    root, paths = acquisition
    sd = tmp_path / "state"
    sd.mkdir()
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root, background_map=str(tiny_sky)))
    M.run(cfg, sd)
    facts = C.run(cfg, sd)
    assert (sd / "context.png").exists() and not facts["degraded"]
    # a missing map costs only that image, and says so
    (sd / "context.png").unlink()
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root, background_map=str(tmp_path / "absent.fits")))
    facts = C.run(cfg, sd, force=True)
    assert (sd / "clean.png").exists() and not (sd / "context.png").exists()
    assert "no context image" in facts["degraded"][0]


def test_main_degrades_without_the_map_and_refuses_a_malformed_one(acquisition, tmp_path, monkeypatch):
    from conftest import write_healpix
    root, paths = acquisition
    sd = tmp_path / "state"
    sd.mkdir()
    monkeypatch.setenv("STATE_DIRECTORY", str(sd))
    absent = write_cfg(tmp_path / "a.yaml", root, background_map=str(tmp_path / "absent.fits"))
    M.run(M.load_config(absent), sd)
    assert C.main(["-c", str(absent)]) == 2
    nested = write_healpix(tmp_path / "nested.fits", np.ones(12 * 16), 4, ordering="NESTED")
    bad = write_cfg(tmp_path / "b.yaml", root, background_map=str(nested))
    assert C.main(["-c", str(bad), "--force"]) == 1


def test_run_is_degraded_until_the_fold_in_records_the_geometry(tmp_path):
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", tmp_path / "root"))
    sd = tmp_path / "state"
    sd.mkdir()
    assert C.run(cfg, sd)["nothing"] == "no accumulators yet"
    # an accumulator file from before the geometry was recorded
    grid = I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"], cfg["dec_halfwidth_deg"],
                        cfg["dec_step_deg"], LAT, LON)
    maps = M.Maps(grid, 2, cfg["colour_bins"], M.grid_signature(cfg))
    maps.last_sample_time = 1.0
    maps.save(sd / M.MAPS_FILE)
    with pytest.raises(C.Degraded, match="no geometry"):
        C.run(cfg, sd)
    # another grid: the fold-in will restart the accumulators; wait for it
    maps.signature = "{}"
    maps.save(sd / M.MAPS_FILE)
    with pytest.raises(C.Degraded, match="another grid"):
        C.run(cfg, sd)


def test_main_exit_codes(acquisition, tmp_path, monkeypatch):
    root, paths = acquisition
    cfg_path = write_cfg(tmp_path / "m.yaml", root)
    sd = tmp_path / "state"
    monkeypatch.setenv("STATE_DIRECTORY", str(sd))
    assert C.main(["-c", str(tmp_path / "missing.yaml")]) == 1
    assert C.main(["-c", str(cfg_path)]) == 0                       # no accumulators: nothing to do
    assert not (sd / C.FACTS_FILE).exists()
    M.run(M.load_config(cfg_path), sd)
    assert C.main(["-c", str(cfg_path)]) == 0
    facts = json.loads((sd / C.FACTS_FILE).read_text())
    assert facts["iterations"] > 0 and facts["images"] == C.IMAGES
    assert C.main(["-c", str(cfg_path)]) == 0                       # unchanged map: no rewrite
    assert json.loads((sd / C.FACTS_FILE).read_text())["updated"] == facts["updated"]
    # a degraded run keeps the last numbers and adds the reason
    grid = I.Grid.build(1024, SRC_DEC, 6.0, 0.5, LAT, LON)
    stale = M.Maps(grid, 2, 4, M.grid_signature(M.load_config(cfg_path)))
    stale.last_sample_time = 99.0
    stale.save(sd / M.MAPS_FILE)
    assert C.main(["-c", str(cfg_path)]) == 2
    after = json.loads((sd / C.FACTS_FILE).read_text())
    assert after["iterations"] == facts["iterations"] and "no geometry" in after["degraded"][0]


def test_lock_makes_a_second_run_a_no_op(acquisition, tmp_path, monkeypatch):
    import fcntl
    root, paths = acquisition
    cfg_path = write_cfg(tmp_path / "m.yaml", root)
    sd = tmp_path / "state"
    sd.mkdir()
    monkeypatch.setenv("STATE_DIRECTORY", str(sd))
    M.run(M.load_config(cfg_path), sd)
    with open(sd / C.LOCK_FILE, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        assert C.main(["-c", str(cfg_path)]) == 0
        assert not (sd / "clean.png").exists()
