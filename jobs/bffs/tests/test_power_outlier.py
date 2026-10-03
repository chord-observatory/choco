"""Tests for the power-outlier source — run with `pytest`."""

import numpy as np

from sources import power_outlier
from sources.power_outlier import band_coverage, power_outlier_mask
from testhelpers import frame, write_chord_n2


def test_flags_power_outlier_and_dead_feed():
    good = power_outlier_mask(frame(10, bad={3: 100.0}, dead=[7]), nsigma=5.0)
    assert not good[3] and not good[7]
    assert good[0] and good[5]


def test_uniform_power_keeps_everyone():
    assert power_outlier_mask(frame(12, base=5.0), nsigma=5.0).all()


def test_outlier_respects_freq_band():
    good = power_outlier_mask(frame(6, bad={2: 1000.0}, nfreq=8), freq_lo=400.0, freq_hi=800.0, nsigma=5.0)
    assert not good[2]


def test_absolute_power_bound():
    assert not power_outlier_mask(frame(8, base=10.0), nsigma=100.0, abs_hi=9.0).any()


def test_unmeasured_feeds_stay_good():
    """A subset-layout file never correlates the unwired elements — no
    data by construction is not a dead feed.  A feed the products DO
    cover but that reads nothing is still dead-and-bad."""
    measured = [True, True, True, False, False, False]
    good = power_outlier_mask(frame(6, dead=[1], measured=measured), nsigma=5.0)
    assert not good[1]                     # measured and silent: dead
    assert good[3] and good[4] and good[5]  # never correlated: left alone


# -- partial band: X-engine nodes down, acquisitions ending -------------------


def _knock_out_band(fr, frac):
    """Mark the first *frac* of the frame's channels as never delivered
    (frames_added 0), the way a down X-engine node's channels read."""
    n = int(round(frac * fr.valid.shape[1]))
    fr.valid[:, :n] = False
    return fr


def test_band_coverage_is_the_delivered_cell_fraction():
    fr = frame(4, nfreq=8)
    assert band_coverage(fr) == 1.0
    _knock_out_band(fr, 0.5)
    assert band_coverage(fr) == 0.5
    # the band selection narrows what counts
    assert band_coverage(fr, freq_lo=fr.freq[4]) == 1.0


def test_min_valid_frac_is_relative_to_delivered_cells():
    """Half the band missing for everyone (nodes down) must not read as
    every feed under-sampled: the fraction is of the cells that exist."""
    fr = _knock_out_band(frame(6, nfreq=8), 0.5)
    good = power_outlier_mask(fr, nsigma=5.0, min_valid_frac=0.9)
    assert good.all()


def test_min_valid_frac_still_catches_a_feed_with_dropped_weights():
    fr = _knock_out_band(frame(6, nfreq=8), 0.5)
    fr.weight[:, :, 2] = 0.0                       # this feed alone has no weight
    fr.weight[0, 4, 2] = 1.0                       # ... bar one cell
    good = power_outlier_mask(fr, nsigma=5.0, min_valid_frac=0.5)
    assert not good[2] and good[[0, 1, 3, 4, 5]].all()


def test_outlier_still_found_on_a_partial_band():
    fr = _knock_out_band(frame(8, bad={5: 400.0}, nfreq=8), 0.5)
    good = power_outlier_mask(fr, nsigma=5.0)
    assert not good[5] and good[[0, 1, 2, 3, 4, 6, 7]].all()


def test_stats_report_the_live_count_and_median():
    stats = {}
    power_outlier_mask(frame(6, base=10.0, dead=[1]), nsigma=5.0, stats=stats)
    assert stats["n_live"] == 5
    assert stats["median"] == 10.0


def _chord_file(tmp_path, frames_added, ntime=4, nfreq=8):
    path = tmp_path / "chord.h5"
    power = np.ones((ntime, nfreq, 4), "f4") * 10.0
    write_chord_n2(path, ["A1", "B1"], np.linspace(400, 800, nfreq), power,
                   num_elements=4, frames_added=frames_added)
    return str(path)


def test_mask_abstains_when_too_little_of_the_band_was_delivered(tmp_path):
    """One channel in eight with data — the 09-20 tail-file shape — must
    leave every feed good and report degraded, never 'all dead'."""
    fa = np.zeros((8, 4), "u1")
    fa[0, :] = 1
    path = _chord_file(tmp_path, fa)
    labels = np.array(["A1X", "B1X", "A1Y", "B1Y"])
    good, rep = power_outlier.mask({"kind": "power-outlier"}, labels, path)
    assert good.all()
    assert rep["status"] == "degraded"
    assert "coverage 12%" in rep["reason"]
    assert rep["n_measured"] == 0
    assert rep["detail"]["band_coverage"] == 0.125


def test_mask_judges_when_enough_of_the_band_was_delivered(tmp_path):
    fa = np.ones((8, 4), "u1")
    fa[:2, :] = 0                                   # a quarter of the band missing
    path = _chord_file(tmp_path, fa)
    labels = np.array(["A1X", "B1X", "A1Y", "B1Y"])
    good, rep = power_outlier.mask({"kind": "power-outlier"}, labels, path)
    assert good.all()
    assert rep["status"] == "ok"
    assert rep["n_measured"] == 4
    assert rep["detail"]["band_coverage"] == 0.75
    assert rep["detail"]["median_power"] == 10.0


def test_mask_min_coverage_is_configurable(tmp_path):
    fa = np.ones((8, 4), "u1")
    fa[:6, :] = 0                                   # a quarter delivered
    path = _chord_file(tmp_path, fa)
    labels = np.array(["A1X", "B1X", "A1Y", "B1Y"])
    _, rep = power_outlier.mask({"kind": "power-outlier", "min_coverage": 0.2},
                                labels, path)
    assert rep["status"] == "ok"
    _, rep = power_outlier.mask({"kind": "power-outlier", "min_coverage": 0.5},
                                labels, path)
    assert rep["status"] == "degraded"


def test_mask_reports_the_tail_rows_it_skipped(tmp_path):
    fa = np.ones((8, 6), "u1")
    fa[:, 4:] = 0                                   # acquisition ended two rows ago
    path = _chord_file(tmp_path, fa, ntime=6)
    labels = np.array(["A1X", "B1X", "A1Y", "B1Y"])
    good, rep = power_outlier.mask({"kind": "power-outlier", "chunk": 3}, labels, path)
    assert good.all() and rep["status"] == "ok"
    assert rep["detail"]["tail_rows_skipped"] == 2
    assert rep["detail"]["rows"] == 3 and rep["detail"]["rows_in_file"] == 6


def test_mask_projects_the_file_axis_onto_the_flag_axis(tmp_path):
    """A compact subset/ file holds a few of the axis's elements in its
    own order: verdicts land by label, the rest of the axis stays good
    (unmeasured) and the report counts both."""
    power = np.ones((2, 8, 4), "f4") * 10.0
    power[..., 2] = 0.0                                  # A1Y dead
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1", "B1"], np.linspace(400, 800, 8), power,
                   num_elements=4)                       # file axis A1X B1X A1Y B1Y
    labels = np.array(["C1X", "B1X", "A1X", "C1Y", "A1Y"])  # flag axis, 5 elements
    good, rep = power_outlier.mask({"kind": "power-outlier"}, labels, path)
    assert list(good) == [True, True, True, True, False]
    assert rep["status"] == "ok" and rep["n_measured"] == 3
    assert rep["detail"]["n_file_elements"] == 4
    assert rep["detail"]["n_in_file"] == 3
    assert rep["detail"]["n_not_in_file"] == 2


def test_ineligible_feeds_are_neither_compared_nor_flagged():
    """An RFI antenna reading 100x the dishes must not be flagged, and
    must not drag the median either: with it in the comparison the
    four dishes at 10 would sit 5 sigma from nothing; the real outlier
    among the dishes still is."""
    f = frame(6, bad={4: 1000.0, 5: 0.0, 2: 40.0})       # 4 = RFI hot, 5 = RFI dead, 2 = dish hot
    eligible = np.array([True, True, True, True, False, False])
    stats = {}
    good = power_outlier_mask(f, nsigma=3.0, eligible=eligible, stats=stats)
    assert list(good) == [True, True, False, True, True, True]
    assert stats["n_live"] == 4 and stats["median"] == 10.0   # the four dishes, hot one included
    # without the exclusion the dead RFI antenna would be flagged as dead
    assert not power_outlier_mask(f, nsigma=3.0)[5]


def test_mask_excludes_rfi_dishes_by_type(tmp_path):
    """The core passes each element's kotekan dish type; RFIDish elements
    are left out of the comparison and never flagged, and the report
    counts them."""
    power = np.ones((2, 8, 4), "f4") * 10.0
    power[..., 1] = 500.0                                # RFIA1X hot
    power[..., 3] = 0.0                                  # RFIA1Y dead
    path = tmp_path / "chord.h5"
    write_chord_n2(path, ["A1", "RFIA1"], np.linspace(400, 800, 8), power,
                   num_elements=4)                       # A1X RFIA1X A1Y RFIA1Y
    labels = np.array(["A1X", "RFIA1X", "A1Y", "RFIA1Y"])
    types = {"A1X": "ArrayDish", "A1Y": "ArrayDish",
             "RFIA1X": "RFIDish", "RFIA1Y": "RFIDish"}
    good, rep = power_outlier.mask({"kind": "power-outlier", "dish_types": types},
                                   labels, path)
    assert good.all()
    assert rep["n_measured"] == 2
    assert rep["detail"]["n_excluded"] == 2
    assert rep["detail"]["exclude_types"] == ["RFIDish"] and rep["detail"]["types_known"]
    # exclude_types is configurable; with none excluded the hot antenna
    # and the dead one are both flagged
    good, rep = power_outlier.mask({"kind": "power-outlier", "dish_types": types,
                                    "exclude_types": ["Nothing"]}, labels, path)
    assert list(good) == [True, False, True, False] and rep["detail"]["n_excluded"] == 0
    # no types at all: every element judged, as before
    good, rep = power_outlier.mask({"kind": "power-outlier"}, labels, path)
    assert list(good) == [True, False, True, False]
    assert rep["detail"]["n_excluded"] == 0 and not rep["detail"]["types_known"]


def test_mask_with_no_filled_rows_abstains(tmp_path):
    path = _chord_file(tmp_path, np.zeros((8, 4), "u1"))
    labels = np.array(["A1X", "B1X", "A1Y", "B1Y"])
    good, rep = power_outlier.mask({"kind": "power-outlier"}, labels, path)
    assert good.all()
    assert rep["status"] == "degraded" and "no filled time rows" in rep["reason"]
