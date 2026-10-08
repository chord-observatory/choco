"""The job end to end on synthetic acquisitions: config, pending files,
calibration on a transit, imaging, the daily composite, state and exits."""

import json
import os

import numpy as np
import pytest
import yaml

import imaging as I
import mapmaker as M
from conftest import SRC_DEC, SRC_RA, write_acquisition, write_cfg, file_name, write_vis_file


# --- config ----------------------------------------------------------------

def test_load_config_applies_defaults(tmp_path):
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", tmp_path / "root"))
    assert cfg["roots"] == [{"name": "subset", "path": str(tmp_path / "root")}]
    assert cfg["dish_diameter_m"] == 6.0 and cfg["row_repeat"] == 4
    assert cfg["stretch"] == {"mode": "arcsinh", "floor_sigma": 3.0, "soft": 0.001, "max": 1.0}
    assert cfg["colour_bins"] == 4 and cfg["colour_range_mhz"] == [400.0, 800.0]
    assert cfg["clean"]["gain"] == 0.25 and cfg["clean"]["max_iterations"] == 20000
    assert cfg["clean"]["stretch"]["mode"] == "arcsinh"


def test_load_config_checks_the_clean_block(tmp_path):
    p = tmp_path / "m.yaml"
    for bad, msg in (({"gain": 0}, "clean.gain"), ({"gain": 1.5}, "clean.gain"),
                     ({"max_iterations": 0}, "max_iterations"), ({"beam_dec_deg": -1}, "beam_dec_deg"),
                     ({"stretch": {"mode": "gamma"}}, "clean.stretch.mode")):
        p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, "clean": bad}))
        with pytest.raises(ValueError, match=msg):
            M.load_config(p)
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0,
                                 "clean": {"gain": 0.5, "stretch": {"mode": "log", "soft": 0.01}}}))
    cfg = M.load_config(p)
    assert cfg["clean"]["gain"] == 0.5 and cfg["clean"]["threshold_sigma"] == 5.0
    assert cfg["clean"]["stretch"] == {"mode": "log", "floor_sigma": 3.0, "soft": 0.01, "max": 1.0}


def test_signature_tolerates_files_from_before_the_beam_keys():
    cfg = {"pointing_dec_deg": 40.8, "colour_bins": 16, "colour_range_mhz": [300.0, 1500.0],
           "n_ra_bins": 4096, "dec_halfwidth_deg": 10.0, "dec_step_deg": 0.25, "subband_channels": 64,
           "dish_diameter_m": 6.0, "beam_fwhm_factor": 1.03, "footprint_fwhm": 1.2}
    current = M.grid_signature(cfg)
    old_keys = ("pointing_dec_deg", "colour_bins", "colour_range_mhz", "n_ra_bins",
                "dec_halfwidth_deg", "dec_step_deg", "subband_channels")
    stored = json.dumps({k: cfg[k] for k in old_keys}, sort_keys=True)
    assert M.signature_matches(current, current)
    assert M.signature_matches(stored, current)                    # pre-2026-10-07 file: grid keys agree
    assert not M.signature_matches(stored.replace("4096", "2048"), current)
    assert not M.signature_matches(current, stored)                # never the other way round
    assert not M.signature_matches("not json", current)
    assert not M.signature_matches("{}", current)                  # only that one key set


def test_load_config_requires_pointing_and_roots(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump({"roots": ["/x"]}))
    with pytest.raises(ValueError, match="pointing_dec_deg"):
        M.load_config(p)
    p.write_text(yaml.safe_dump({"pointing_dec_deg": 40.0}))
    with pytest.raises(ValueError, match="roots"):
        M.load_config(p)


def test_load_config_checks_the_colour_bins(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, "colour_range_mhz": [600, 300]}))
    with pytest.raises(ValueError, match="lo < hi"):
        M.load_config(p)
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, "colour_bins": 0}))
    with pytest.raises(ValueError, match="colour_bins"):
        M.load_config(p)
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, "stretch": {"mode": "gamma"}}))
    with pytest.raises(ValueError, match="stretch.mode"):
        M.load_config(p)
    # the three-band design's key is refused, naming its replacement
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, "bands": [[300, 513]]}))
    with pytest.raises(ValueError, match="colour_bins"):
        M.load_config(p)


def test_colour_edges_are_log_spaced_and_cover_the_range():
    cfg = {"colour_range_mhz": [300.0, 1500.0], "colour_bins": 16}
    e = M.colour_edges(cfg)
    assert len(e) == 17 and e[0] == 300.0 and e[-1] == pytest.approx(1500.0)
    assert np.allclose(np.diff(np.log(e)), np.log(5) / 16)
    assert M.colour_bin_of(300.0, e) == 0 and M.colour_bin_of(1499.0, e) == 15
    assert M.colour_bin_of(299.0, e) is None and M.colour_bin_of(1500.0, e) is None


@pytest.mark.parametrize("key", ["state_file", "lock_file", "output", "image_file", "maps_dir"])
def test_retired_keys_are_refused(tmp_path, key):
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump({"roots": ["/x"], "pointing_dec_deg": 40.0, key: "/somewhere"}))
    with pytest.raises(ValueError, match="retired"):
        M.load_config(p)


def test_pick_calibrator_is_the_first_within_reach(tmp_path):
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", tmp_path / "root", pointing_dec_deg=58.0,
                                  calibrators=M.DEFAULTS["calibrators"]))
    assert M.pick_calibrator(cfg)["name"] == "Cas A"
    cfg["pointing_dec_deg"] = 0.0
    assert M.pick_calibrator(cfg) is None


# --- pending files -----------------------------------------------------------

def test_first_run_starts_at_the_newest_acquisition(tmp_path):
    root = tmp_path / "root"
    write_acquisition(str(root), "acq_20260101_000000_1", 2, first_idx=10, n_freq=16)
    write_acquisition(str(root), "acq_20260102_000000_1", 3, first_idx=20, n_freq=16)
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    files, reasons = M.pending_files(cfg, None)
    assert reasons == [] and [i for i, _ in files] == [20, 21, 22]
    cfg["backfill_from"] = "acq_20260101_000000_1"
    files, _ = M.pending_files(cfg, None)
    assert [i for i, _ in files] == [10, 11, 20, 21, 22]
    files, _ = M.pending_files(cfg, 21)
    assert [i for i, _ in files] == [22]


def test_missing_root_is_degraded(tmp_path):
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", tmp_path / "gone"))
    files, reasons = M.pending_files(cfg, None)
    assert files == [] and "unavailable" in reasons[0]


# --- the run -----------------------------------------------------------------

@pytest.fixture
def acquisition(tmp_path):
    """20 files (67 min) starting 36 min before the transit: the window
    (±3° on the sky = ±16 min = ±4.7 files here) is fully passed by the
    end, so the run solves gains."""
    root = tmp_path / "root"
    paths = write_acquisition(str(root), "acq_20260101_000000_1", 20, hours_before=0.6)
    return root, paths


def test_first_pass_solves_gains_then_images(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    report = M.run(cfg, sd)
    assert report["files_processed"] == 20
    # the window closes after file 16; the four files behind it are imaged
    # with the gains solved in the same run
    assert report["files_uncalibrated"] == 16 and report["files_imaged"] == 4
    assert report["gains_solved"] and report["gains"]["calibrator"] == "Test A"
    assert report["gains"]["n_dead_products"] == 0
    assert (sd / M.GAINS_FILE).exists() and (sd / M.MAPS_FILE).exists()
    assert not (sd / M.CALSUM_FILE).exists()        # consumed by the solve
    assert report["last_file_idx"] == 1019
    assert report["degraded"] == []                  # gains exist now


def test_solved_gains_match_the_injected_ones(acquisition, tmp_path):
    from conftest import element_gains, element_table
    import reduce as R
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    g, meta = M.load_gains(sd / M.GAINS_FILE)
    ax = R.read_axes(paths[0])
    pols = R.pol_sets(ax)
    labels, *_ = element_table()
    amp, phi0, tau_c = element_gains(len(labels), ax.n_freq)
    g_el = amp[None] * np.exp(1j * (phi0[None] + 2 * np.pi * ax.freq_mhz[:, None] * 1e6 * tau_c[None]))
    g_true = np.stack([g_el[:, ps.a] * np.conj(g_el[:, ps.b]) for ps in pols], axis=1)
    # the solve is normalised to the model beam at the calibrator, so compare
    # phases exactly and amplitudes up to a common factor
    assert np.allclose(np.angle(g * np.conj(g_true)), 0, atol=0.05)
    ratio = np.abs(g) / np.abs(g_true)
    assert np.std(ratio) / np.mean(ratio) < 0.05


def test_second_run_images_new_files_and_renders(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    # four more files, well after the transit
    write_acquisition(str(root), "acq_20260101_000000_1", 4, first_idx=1020,
                      hours_before=0.6 - 20 * 200 / 3600.0, t0=1_791_000_000.0 + 20 * 200)
    report = M.run(cfg, sd)
    assert report["files_processed"] == 4 and report["files_imaged"] == 4
    assert report["samples_imaged"] == 80 and report["samples_sun_skipped"] == 0
    assert report["coverage"]["sky"]["bins_covered"] > 0
    for name in M.IMAGES.values():
        assert (sd / name).stat().st_size > 0
    state = json.loads((sd / M.STATE_FILE).read_text()) if (sd / M.STATE_FILE).exists() else None
    assert state is None                                   # run() does not write state; main() does
    assert M.run(cfg, sd)["files_processed"] == 0          # nothing new: nothing done


def test_calibrated_map_peaks_on_the_source(acquisition, tmp_path):
    """Gains from pass one, then the same files imaged from scratch: the
    sky map's brightest pixel is the source's RA/Dec in every band."""
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    os.unlink(sd / M.MAPS_FILE)                            # keep the gains, forget the maps
    cfg["backfill_from"] = "acq_20260101_000000_1"
    report = M.run(cfg, sd)
    assert report["files_imaged"] == 20
    with np.load(sd / M.MAPS_FILE) as z:
        num, exp = z["sky_num"].sum(0), z["sky_exp"].sum(0)      # (band, dec, bins)
    grid = I.Grid.build(cfg["n_ra_bins"], cfg["pointing_dec_deg"], cfg["dec_halfwidth_deg"],
                        cfg["dec_step_deg"], 0, 0)
    for band in range(cfg["colour_bins"]):
        m = np.where(exp[band] > 0.1 * exp[band].max(), num[band] / np.maximum(exp[band], 1e-30), -np.inf)
        j, n = np.unravel_index(np.argmax(m), m.shape)
        assert abs(n - int(I.ra_bin(SRC_RA, cfg["n_ra_bins"]))) <= 1, band
        assert abs(grid.dec_deg[j] - SRC_DEC) <= cfg["dec_step_deg"] * 2, band
        # in calibrator-flux units the source is ~1 x its beam response
        assert 0.5 < m[j, n] < 1.5, band


def test_daily_map_follows_the_current_pass(acquisition, tmp_path, monkeypatch):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    os.unlink(sd / M.MAPS_FILE)
    cfg["backfill_from"] = "acq_20260101_000000_1"
    M.run(cfg, sd)
    with np.load(sd / M.MAPS_FILE) as z:
        idx = z["pass_idx"]
        pass_exp = z["pass_exp"]
        sky_exp = z["sky_exp"]
    assert (idx >= 0).sum() == 1                           # one pass seen so far
    slot = int(np.argmax(idx >= 0))
    assert np.allclose(pass_exp[slot], sky_exp)            # no Sun: identical exposure
    # a day later, the same sky: a new pass takes the other slot
    write_acquisition(str(root), "acq_20260102_000000_1", 3, first_idx=2000,
                      hours_before=0.6, t0=1_791_000_000.0 + I.SIDEREAL_DAY_S)
    M.run(cfg, sd)
    with np.load(sd / M.MAPS_FILE) as z:
        idx2 = z["pass_idx"]
    assert sorted(idx2.tolist()) == sorted([idx[slot], idx[slot] + 1])


def test_sun_near_the_beam_is_kept_out_of_the_sky_map(acquisition, tmp_path, monkeypatch):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    os.unlink(sd / M.MAPS_FILE)
    cfg["backfill_from"] = "acq_20260101_000000_1"
    # the Sun on the pointing for every sample: nothing reaches the sky map
    monkeypatch.setattr(M.Run, "sun_ok", lambda self, ax, era_local: np.zeros(ax.n_time, bool))
    report = M.run(cfg, sd)
    assert report["samples_sun_skipped"] == report["samples_imaged"] == 400
    with np.load(sd / M.MAPS_FILE) as z:
        assert not z["sky_exp"].any() and z["pass_exp"].any()


def test_dead_element_products_are_flagged_in_the_gains(tmp_path):
    root = tmp_path / "root"
    write_acquisition(str(root), "acq_20260101_000000_1", 20, hours_before=0.6, dead_elements=(1, 6))
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    report = M.run(cfg, sd)
    assert report["gains_solved"]
    # element 1 (A02X) is in 3 of the 6 XX products, element 6 (A02Y) in 3 of the YY
    assert report["gains"]["n_dead_products"] == 6


def test_unusable_files_are_skipped_once_and_the_run_moves_on(acquisition, tmp_path):
    root, paths = acquisition
    import h5py
    with h5py.File(paths[0], "r+") as f:
        del f["bin_ERA_deg"]
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    report = M.run(cfg, sd)
    assert len(report["skipped"]) == 1 and "bin_ERA_deg" in report["skipped"][0]
    assert report["files_processed"] == 19 and report["last_file_idx"] == 1019
    assert M.run(cfg, sd)["files_processed"] == 0


def test_a_changed_grid_starts_the_maps_afresh(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    M.run(cfg, sd)
    cfg2 = M.load_config(write_cfg(tmp_path / "m.yaml", root, dec_step_deg=1.0))
    report = M.run(cfg2, sd)
    assert report["files_processed"] == 20                 # last_file_idx was forgotten with the maps


# --- main() ------------------------------------------------------------------

def test_main_exits_1_on_a_bad_config(tmp_path):
    p = tmp_path / "m.yaml"
    p.write_text("roots: [/x]\n")
    assert M.main(["-c", str(p)]) == 1
    assert M.main(["-c", str(tmp_path / "missing.yaml")]) == 1


def test_main_exits_2_when_the_root_is_unavailable(tmp_path):
    cfg = write_cfg(tmp_path / "m.yaml", tmp_path / "gone")
    assert M.main(["-c", str(cfg), "--state-dir", str(tmp_path / "s")]) == 2
    state = json.loads((tmp_path / "s" / M.STATE_FILE).read_text())
    assert "unavailable" in state["degraded"][0]


def test_main_writes_state_and_exits_2_until_calibrated(tmp_path):
    root = tmp_path / "root"
    # files well before any transit: processed, uncalibrated, no gains yet
    write_acquisition(str(root), "acq_20260101_000000_1", 2, hours_before=6.0)
    cfg = write_cfg(tmp_path / "m.yaml", root)
    rc = M.main(["-c", str(cfg), "--state-dir", str(tmp_path / "s")])
    assert rc == 2
    state = json.loads((tmp_path / "s" / M.STATE_FILE).read_text())
    assert state["files_uncalibrated"] == 2 and "no gains yet" in state["degraded"][0]
    assert state["pointing_dec_deg"] == SRC_DEC and state["images"]["sky"] == "sky.png"


def test_main_exits_0_with_nothing_to_do(acquisition, tmp_path):
    root, paths = acquisition
    cfg = write_cfg(tmp_path / "m.yaml", root)
    assert M.main(["-c", str(cfg), "--state-dir", str(tmp_path / "s")]) == 0
    assert M.main(["-c", str(cfg), "--state-dir", str(tmp_path / "s")]) == 0


def test_dry_run_lists_pending_and_writes_nothing(acquisition, tmp_path, capsys):
    root, paths = acquisition
    cfg = write_cfg(tmp_path / "m.yaml", root)
    assert M.main(["-c", str(cfg), "--state-dir", str(tmp_path / "s"), "-n"]) == 0
    out = capsys.readouterr().out
    assert "20 files pending" in out
    assert not (tmp_path / "s").exists()


def test_max_files_caps_a_run_and_reports_the_backlog(acquisition, tmp_path):
    root, paths = acquisition
    cfg = M.load_config(write_cfg(tmp_path / "m.yaml", root))
    sd = tmp_path / "state"
    sd.mkdir()
    report = M.run(cfg, sd, budget=5)
    assert report["files_processed"] == 5 and report["backlog"] == 15
    report = M.run(cfg, sd, budget=5)
    assert report["files_processed"] == 5 and report["cal_samples_pending"] > 0
    assert (sd / M.CALSUM_FILE).exists()               # a transit in progress survives between runs
    report = M.run(cfg, sd, budget=0)
    assert report["files_processed"] == 10 and report["gains_solved"]


def test_lock_makes_a_second_run_a_no_op(acquisition, tmp_path):
    import fcntl
    root, paths = acquisition
    cfg = write_cfg(tmp_path / "m.yaml", root)
    sd = tmp_path / "s"
    sd.mkdir()
    with open(sd / M.LOCK_FILE, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert M.main(["-c", str(cfg), "--state-dir", str(sd)]) == 0
    assert not (sd / M.MAPS_FILE).exists()
