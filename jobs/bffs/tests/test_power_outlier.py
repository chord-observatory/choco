"""Tests for the power-outlier source — run with `pytest`."""

from sources.power_outlier import power_outlier_mask
from testhelpers import frame


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
