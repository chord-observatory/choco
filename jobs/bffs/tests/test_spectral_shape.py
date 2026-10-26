"""Tests for the spectral-shape source — run with `pytest`."""

import json

import numpy as np
import pytest

import kotekan_io
from sources import spectral_shape
from sources.spectral_shape import block_levels, judge
from testhelpers import write_chord_n2

NDISH = 10
DISHES = [f"A{d:02d}" for d in range(1, NDISH + 1)]
LABELS = [f"{d}{p}" for p in "XY" for d in DISHES]       # [P][D], as the files read back


def band(nblock=16, per=4):
    """``nblock`` runs of ``per`` adjacent channels spread over 320-1450 MHz."""
    starts = np.geomspace(320.0, 1440.0, nblock)
    return np.concatenate([s + 0.2 * np.arange(per) for s in starts]).astype("f4")


def spectra(freq, tilt=None, ntime=4, seed=0, scale=1e4):
    """(ntime, nfreq, 2 * NDISH) flat autocorrelations with a little noise;
    ``tilt`` maps an element to its slope in dex per octave about 600 MHz."""
    rng = np.random.default_rng(seed)
    x = np.log2(freq / 600.0)
    auto = scale * (1 + 0.01 * rng.standard_normal((ntime, freq.size, 2 * NDISH)))
    for e, b in (tilt or {}).items():
        auto[..., e] *= 10.0 ** (b * x)[None, :]
    return auto.astype("f4")


def frame(auto, freq):
    return kotekan_io.Frame(auto=auto, weight=np.ones_like(auto),
                            valid=np.ones(auto.shape[:2], bool), freq=freq)


P = spectral_shape._params({"min_feeds": 4})


def test_a_tilted_feed_is_flagged_and_the_flat_ones_are_not():
    f = band()
    slopes, bad, stats, why = judge(frame(spectra(f, {2: -0.35}), f), LABELS, None, P)
    assert why is None
    assert set(bad) == {"A03X"} and "dex/octave" in bad["A03X"]
    assert slopes["A03X"] == pytest.approx(-0.35, abs=0.01)
    assert stats["X"]["n"] == NDISH and stats["Y"]["n"] == NDISH


def test_a_small_tilt_stays_under_the_floor():
    # a population this tight puts 8 sigma far below min_slope
    f = band()
    _, bad, stats, _ = judge(frame(spectra(f, {2: -0.03}), f), LABELS, None, P)
    assert not bad and stats["X"]["limit"] == P["min_slope"]


def test_one_rfi_block_does_not_tilt_a_feed():
    f = band()
    auto = spectra(f)
    auto[:, 60:64, 4] *= 30                     # the last block of A05X, all four channels
    _, bad, _, _ = judge(frame(auto, f), LABELS, None, P)
    assert not bad


def test_a_few_rfi_channels_are_ignored_by_the_block_median():
    f = band()
    auto = spectra(f)
    auto[:, 0, :] *= 1000                       # one channel of the first block, every feed
    levels, centres = block_levels(frame(auto, f))
    assert levels.shape == (16, 2 * NDISH) and centres.shape == (16,)
    assert np.allclose(levels[0], 4.0, atol=0.02)


def test_dead_and_excluded_feeds_are_not_fitted():
    f = band()
    auto = spectra(f, {3: 0.5})
    auto[..., 1] = 0                            # dead
    eligible = np.ones(2 * NDISH, bool)
    eligible[3] = False                         # say, an RFI antenna
    slopes, bad, _, _ = judge(frame(auto, f), LABELS, eligible, P)
    assert "A02X" not in slopes and "A04X" not in slopes and not bad


def test_too_few_blocks_abstains():
    f = band()
    fr = frame(spectra(f), f)
    fr.valid[:, :24] = False                    # six of 16 blocks never delivered
    slopes, bad, _, why = judge(fr, LABELS, None, P)
    assert not slopes and not bad and "blocks hold data" in why


def test_a_narrow_band_abstains():
    f = np.concatenate([s + 0.2 * np.arange(4) for s in np.linspace(500.0, 900.0, 16)]).astype("f4")
    _, _, _, why = judge(frame(spectra(f), f), LABELS, None, P)
    assert "less than one" in why


def test_a_polarization_with_too_few_feeds_is_reported():
    f = band()
    auto = spectra(f)
    auto[..., NDISH + 2:] = 0                   # all but two Y feeds dead
    slopes, _, stats, why = judge(frame(auto, f), LABELS, None, P)
    assert why is None and "Y" not in stats and "Y: 2 feeds" in stats["short"][0]
    assert all(lbl.endswith("X") for lbl in slopes)


# -- the source, over a run of files -----------------------------------------


def write_file(tmp_path, idx, tilt=None, seed=0):
    f = band()
    path = tmp_path / f"vis_{idx:010d}_20261025T_233135_072787250.h5"
    write_chord_n2(path, DISHES, f, spectra(f, tilt, seed=seed))
    return str(path)


def run(tmp_path, idx, tilt=None, **src):
    labels = np.array(LABELS)
    good, rep = spectral_shape.mask({"state_dir": str(tmp_path), "min_feeds": 4,
                                     "blocks": 0, **src},
                                    labels, write_file(tmp_path, idx, tilt, seed=idx))
    return {str(lbl) for lbl, g in zip(labels, good) if not g}, rep


def test_a_feed_that_changes_shape_is_flagged_and_held(tmp_path):
    for i in range(3):
        bad, rep = run(tmp_path, 100 + i)
        assert not bad and rep["status"] == "ok"
    # A05Y tilts by 0.045: under the shape floor, over max_change
    bad, rep = run(tmp_path, 103, {NDISH + 4: 0.045})
    assert bad == {"A05Y"} and "moved" in rep["detail"]["feed_reasons"]["A05Y"]
    # it settles at once: the move shows until the lag passes it (105),
    # then the flag holds for clear_after files after the last one
    for i in range(104, 111):
        bad, rep = run(tmp_path, i, {NDISH + 4: 0.045})
        assert bad == {"A05Y"}
        if i >= 106:
            assert rep["detail"]["feed_reasons"]["A05Y"].startswith("held from file 105")
    bad, _ = run(tmp_path, 111, {NDISH + 4: 0.045})
    assert not bad


def test_the_history_is_the_cache(tmp_path, monkeypatch):
    path = write_file(tmp_path, 200, {2: -0.35})
    src = {"state_dir": str(tmp_path), "min_feeds": 4, "blocks": 0}
    _, rep = spectral_shape.mask(src, np.array(LABELS), path)
    assert not rep["detail"]["cached"]

    def no_read(*a, **k):
        raise AssertionError("read again")
    monkeypatch.setattr(spectral_shape, "read_autocorr", no_read)
    good, rep = spectral_shape.mask(src, np.array(LABELS), path)
    assert rep["detail"]["cached"] and not good[2]
    doc = json.loads((tmp_path / spectral_shape.HISTORY_NAME).read_text())
    assert [e["idx"] for e in doc["files"]] == [200]


def test_new_parameters_start_the_history_afresh(tmp_path):
    run(tmp_path, 300, {2: -0.35})
    _, rep = run(tmp_path, 300, {2: -0.35}, nsigma=6.0)
    assert not rep["detail"]["cached"]


def test_an_abstaining_file_leaves_every_feed_good(tmp_path):
    f = band()
    path = tmp_path / "vis_0000000400_20261025T_233135_072787250.h5"
    frames = np.ones((f.size, 4), "u1")
    frames[:40] = 0
    write_chord_n2(path, DISHES, f, spectra(f, {2: -0.35}), frames_added=frames)
    good, rep = spectral_shape.mask({"state_dir": str(tmp_path), "min_feeds": 4, "blocks": 0},
                                    np.array(LABELS), str(path))
    assert good.all() and rep["status"] == "degraded" and rep["n_measured"] == 0


def test_unmeasured_flag_axis_feeds_stay_good(tmp_path):
    labels = np.array(LABELS + ["Z99X", "Z99Y"])
    good, _ = spectral_shape.mask({"state_dir": str(tmp_path), "min_feeds": 4, "blocks": 0},
                                  labels, write_file(tmp_path, 500, {2: -0.35}))
    assert not good[2] and good[-1] and good[-2]
