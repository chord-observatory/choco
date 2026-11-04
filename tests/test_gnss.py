"""choco.gnss: the transit forecast monitor and what a page reads from a forecast."""

import copy
import json
import time

import pytest
import responses

from tests.helpers import TRANSIT_SAMPLE as SAMPLE, retimed
from choco.gnss import (BORESIGHT_TOL_DEG, STALE_AFTER_S, TransitMonitor, boresight_check,
                        check_forecast, fmt_in, fmt_length, new_passes, summarize, timeline)

HOST, PORT = "gnss.example", 12070
BASE = f"http://{HOST}:{PORT}"


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr("choco.http.RETRY_DELAY_S", 0)


@pytest.fixture
def forecast():
    return json.loads(SAMPLE.read_text())


class TestCheck:
    def test_the_sample_is_a_forecast(self, forecast):
        assert check_forecast(forecast) is None

    def test_another_schema_is_refused(self, forecast):
        forecast["schema"] = "chord-gnss-transit-windows/1"
        assert "schema" in check_forecast(forecast)

    def test_missing_parts_are_named(self, forecast):
        assert check_forecast([]) == "the answer is not a JSON object"
        del forecast["quiet"]
        assert check_forecast(forecast) == "no quiet windows"

    def test_a_pass_without_a_field(self, forecast):
        del forecast["transits"][0]["closest_unix"]
        assert check_forecast(forecast) == "a pass is malformed"

    @pytest.mark.parametrize("break_it", [
        lambda f: f["transits"][0].update(closest_unix=None),
        lambda f: f["transits"][0].update(bands="E5b"),
        lambda f: f["quiet_by_band"].update({"L2 1217-1238": [[1, 2, 3]]}),
        lambda f: f["quiet"].update(all=[["a", 2]]),
        lambda f: f["boresight"].pop("az_deg"),
        lambda f: f.update(created_unix=float("nan")),
    ])
    def test_types_are_checked_not_just_keys(self, forecast, break_it):
        """A malformed forecast is refused at the poll: summarize would
        otherwise raise in every request that reads the menu."""
        break_it(forecast)
        assert check_forecast(forecast) is not None

    def test_odd_optional_facts_are_dropped_not_fatal(self, forecast):
        forecast["criteria"] = {"transit_deg": "five"}
        forecast["tle_age_days"] = [["gps-ops", None], ["galileo", 0.3]]
        forecast["sun_sep_deg"] = ["x"]
        assert check_forecast(forecast) is None
        s = summarize(forecast, forecast["created_unix"] + 120, 600)
        assert s["criteria"] == {} and s["tle_age_days"] == [["galileo", 0.3]] and s["sun"] is None


class TestSummary:
    def test_the_sky_two_minutes_after_the_forecast(self, forecast):
        now = forecast["created_unix"] + 120
        s = summarize(forecast, now, 600)
        assert s["bands"] == list(forecast["quiet_by_band"])
        assert s["quiet_now"] is False and not s["active"]
        assert s["next_transit"]["name"] == "DIGUI-08" and s["next_transit"]["kind"] == "closest"
        assert s["next_transit"]["in_fmt"] == "in 1 h 01 min"
        assert [(w["start_fmt"], w["end_fmt"], w["length_fmt"]) for w in s["gain_windows"]] == [
            ("00:10", "00:53", "42 min"), ("01:26", "01:38", "12 min")]
        assert s["n_closest"] == 3 and len(s["transits"]) == 10
        assert [p["start"] for p in s["transits"]] == sorted(p["start"] for p in s["transits"])
        assert s["sun"]["sep_min"] == pytest.approx(73.4, abs=0.1)

    def test_inside_a_transit(self, forecast):
        p = next(t for t in forecast["transits"] if t["name"] == "DIGUI-08")
        s = summarize(forecast, p["start_unix"] + 5, 600)
        assert [a["name"] for a in s["active"]] == ["DIGUI-08"]
        assert s["quiet_now"] is False
        assert s["next_transit"]["name"] == "DIGUI-123"

    def test_inside_a_quiet_window_the_remainder_counts(self, forecast):
        a, b = forecast["quiet"]["all"][0]
        s = summarize(forecast, a + 600, 600)
        assert s["quiet_now"] is True
        w = s["gain_windows"][0]
        assert w["current"] and w["start_fmt"] == "now" and w["length_s"] == pytest.approx(b - a - 600)
        # the same window is too short once less than min_quiet_s remains
        s = summarize(forecast, b - 300, 600)
        assert s["gain_windows"][0]["start"] > b

    def test_pass_states(self, forecast):
        p = next(t for t in forecast["transits"] if t["name"] == "DIGUI-08")
        states = {p["name"]: p["state"] for p in summarize(forecast, p["start_unix"] + 5, 600)["transits"]}
        assert states["G17"] == "past" and states["DIGUI-08"] == "now" and states["DIGUI-52"] == "ahead"


class TestNewPasses:
    def test_the_same_forecast_has_nothing_new(self, forecast):
        assert new_passes(forecast["transits"], forecast["transits"], forecast["end_unix"]) == []

    def test_a_pass_moved_by_seconds_is_the_same_pass(self, forecast):
        moved = copy.deepcopy(forecast["transits"])
        for p in moved:
            p["closest_unix"] += 20
        assert new_passes(moved, forecast["transits"], forecast["end_unix"]) == []

    def test_a_pass_inside_the_old_horizon_is_new(self, forecast):
        later = copy.deepcopy(forecast["transits"])
        later.append({**later[0], "name": "E16", "closest_unix": forecast["end_unix"] - 100})
        assert [p["name"] for p in new_passes(later, forecast["transits"], forecast["end_unix"])] == ["E16"]

    def test_a_pass_beyond_the_old_horizon_is_the_window_moving_on(self, forecast):
        later = copy.deepcopy(forecast["transits"])
        later.append({**later[0], "name": "E16", "closest_unix": forecast["end_unix"] + 100})
        assert new_passes(later, forecast["transits"], forecast["end_unix"]) == []


class TestBoresight:
    def test_tau_a_pointing_agrees(self):
        assert boresight_check({"az_deg": 180.0, "el_deg": 62.7}, -27.3) is None

    def test_no_pointing_is_no_verdict(self):
        assert boresight_check({"az_deg": 180.0, "el_deg": 62.7}, None) is None

    def test_a_different_elevation(self):
        msg = boresight_check({"az_deg": 180.0, "el_deg": 62.7}, -30.0)
        assert "el 60" in msg and "co-elevation -30" in msg

    def test_the_other_side_of_zenith(self):
        assert "az 0" in boresight_check({"az_deg": 180.0, "el_deg": 62.7}, 27.3)

    def test_zenith_has_no_azimuth(self):
        assert boresight_check({"az_deg": 180.0, "el_deg": 90.0}, 0.0) is None

    def test_within_tolerance(self):
        assert boresight_check({"az_deg": 180.0, "el_deg": 62.7 + BORESIGHT_TOL_DEG / 2}, -27.3) is None

    def test_unreadable_boresight(self):
        assert boresight_check({"el_deg": "south"}, -27.3) == "the forecast's boresight is unreadable"


class TestFormat:
    def test_lengths(self):
        assert fmt_length(50) == "50 s"
        assert fmt_length(42 * 60 + 20) == "42 min"
        assert fmt_length(3900) == "1 h 05 min"

    def test_relative(self):
        assert fmt_in(10) == "now" and fmt_in(-10) == "now"
        assert fmt_in(720) == "in 12 min"
        assert fmt_in(-300) == "5 min ago"


class TestTimeline:
    def test_lanes_and_axis(self, forecast):
        now = forecast["created_unix"] + 120
        tl = timeline(forecast, now, 600)
        assert [lane["label"] for lane in tl["lanes"]] == ["all bands"] + list(forecast["quiet_by_band"])
        assert tl["gutter"] < tl["now_x"] < tl["plot_x1"]
        assert all(len(t["label"]) == 5 for t in tl["ticks"]) and len(tl["ticks"]) == 10
        assert tl["height"] == tl["axis_y"] + 34 and tl["tz_name"] is None
        assert all(t["local"] is None for t in tl["ticks"])

    def test_the_axis_carries_the_site_zone(self, forecast):
        from zoneinfo import ZoneInfo
        import datetime as dt
        zone, now = ZoneInfo("America/Vancouver"), forecast["created_unix"] + 120
        tl = timeline(forecast, now, 600, zone)
        # the abbreviation is the zone database's: BC stays on UTC-7 ("MST") from 2026-11
        offset = dt.datetime.fromtimestamp(now, zone).utcoffset()
        assert tl["tz_name"] == dt.datetime.fromtimestamp(now, zone).strftime("%Z")
        for t in tl["ticks"][:2]:
            utc = dt.datetime.strptime(t["label"], "%H:%M")
            assert t["local"] == (utc + offset).strftime("%H:%M")

    def test_bars_and_windows(self, forecast):
        now = forecast["created_unix"] + 120
        tl = timeline(forecast, now, 600)
        e5b = next(lane for lane in tl["lanes"] if lane["label"].startswith("E5b"))
        kinds = {b["name"]: b["kind"] for b in e5b["bars"]}
        assert kinds["DIGUI-08"] == "closest" and kinds["DIGUI-124"] == "near"
        assert all(b["x1"] > b["x0"] and "UTC" in b["title"] for b in e5b["bars"])
        gain = [q for q in tl["lanes"][0]["quiet"] if q["gain"]]
        assert len(gain) == 2
        assert not any(q["gain"] for q in e5b["quiet"])

    def test_a_short_pass_is_still_visible(self, forecast):
        p = forecast["transits"][0]
        p["end_unix"] = p["start_unix"] + 1
        p["closest_unix"] = p["start_unix"]
        tl = timeline(forecast, forecast["created_unix"] + 120, 600)
        bar = next(b for lane in tl["lanes"] for b in lane["bars"] if b["name"] == p["name"])
        assert bar["x1"] - bar["x0"] == pytest.approx(3)


class TestMonitor:
    @pytest.fixture
    def monitor(self):
        return TransitMonitor(HOST, PORT, timeout=1, min_quiet_s=600)

    @responses.activate
    def test_a_fresh_forecast_is_ok(self, monitor, forecast):
        doc = retimed(forecast, time.time() - 60)
        responses.add(responses.GET, BASE + "/transit_windows", json=doc)
        monitor.poll_once()
        assert monitor.health == "ok" and monitor.error is None
        assert monitor.forecast["created_unix"] == doc["created_unix"]
        assert monitor.last_seen and monitor.last_polled
        assert monitor.new_transits == []
        d = monitor.to_dict()
        assert d["quiet_now"] is False and d["next_transit"]["name"] == "DIGUI-08"
        assert d["gain_window"]["length_fmt"] == "42 min" and d["n_new"] == 0
        assert 50 < d["forecast_age_s"] < 70

    @responses.activate
    def test_the_next_forecast_names_its_new_passes(self, monitor, forecast):
        first = retimed(forecast, time.time() - 400)
        # the next refresh: the same passes, a later horizon with one more
        second = copy.deepcopy(first)
        second["created_unix"] = time.time() - 60
        second["end_unix"] += 300
        second["transits"].append({**second["transits"][0], "name": "E16",
                                   "closest_unix": first["end_unix"] - 100,
                                   "end_unix": first["end_unix"] - 50})
        responses.add(responses.GET, BASE + "/transit_windows", json=first)
        responses.add(responses.GET, BASE + "/transit_windows", json=second)
        monitor.poll_once()
        monitor.poll_once()
        assert [p["name"] for p in monitor.new_transits] == ["E16"]
        assert monitor.new_since and monitor.to_dict()["n_new"] == 1
        assert [p["name"] for p in monitor.summary()["new_transits"]] == ["E16"]

    @responses.activate
    def test_a_refresh_with_nothing_new_clears_the_list(self, monitor, forecast):
        first = retimed(forecast, time.time() - 400)
        second = dict(copy.deepcopy(first), created_unix=time.time() - 60)
        responses.add(responses.GET, BASE + "/transit_windows", json=first)
        responses.add(responses.GET, BASE + "/transit_windows", json=second)
        monitor.poll_once()
        monitor.new_transits, monitor.new_since = [first["transits"][0]], time.time()
        monitor.poll_once()
        assert monitor.new_transits == [] and monitor.new_since is None

    @responses.activate
    def test_an_old_forecast_is_stale(self, monitor, forecast):
        doc = retimed(forecast, time.time() - STALE_AFTER_S - 120)
        responses.add(responses.GET, BASE + "/transit_windows", json=doc)
        monitor.poll_once()
        assert monitor.health == "stale" and "min old" in monitor.error

    @responses.activate
    def test_another_schema_is_degraded_and_keeps_the_forecast(self, monitor, forecast):
        good = retimed(forecast, time.time() - 60)
        bad = {**good, "schema": "chord-gnss-transit-windows/9"}
        responses.add(responses.GET, BASE + "/transit_windows", json=good)
        responses.add(responses.GET, BASE + "/transit_windows", json=bad)
        monitor.poll_once()
        monitor.poll_once()
        assert monitor.health == "degraded" and "schema" in monitor.error
        assert monitor.forecast == good

    @responses.activate
    def test_unreachable_is_down_and_keeps_the_forecast(self, monitor, forecast):
        good = retimed(forecast, time.time() - 60)
        responses.add(responses.GET, BASE + "/transit_windows", json=good)
        monitor.poll_once()
        responses.reset()
        responses.add(responses.GET, BASE + "/transit_windows", body=ConnectionError("refused"))
        monitor.poll_once()
        assert monitor.health == "down" and HOST in monitor.error
        # the forecast is kept for the page, but it is no longer advice
        assert monitor.forecast == good and monitor.summary()["next_transit"]
        d = monitor.to_dict()
        assert d["next_transit"] is None and d["gain_window"] is None and d["quiet_now"] is None

    @responses.activate
    def test_a_dropped_request_is_retried(self, monitor, forecast):
        responses.add(responses.GET, BASE + "/transit_windows", body=ConnectionError("reset"))
        responses.add(responses.GET, BASE + "/transit_windows", json=retimed(forecast, time.time() - 60))
        monitor.poll_once()
        assert monitor.health == "ok"

    @responses.activate
    def test_the_boresight_is_checked_against_the_pointing(self, forecast):
        monitor = TransitMonitor(HOST, PORT, timeout=1, pointing=lambda: -30.0)
        responses.add(responses.GET, BASE + "/transit_windows", json=retimed(forecast, time.time() - 60))
        monitor.poll_once()
        assert monitor.health == "degraded" and "el 60" in monitor.error
        assert monitor.summary()["boresight_error"] == monitor.error

    @responses.activate
    def test_the_pointing_that_agrees(self, forecast):
        monitor = TransitMonitor(HOST, PORT, timeout=1, pointing=lambda: -27.3)
        responses.add(responses.GET, BASE + "/transit_windows", json=retimed(forecast, time.time() - 60))
        monitor.poll_once()
        assert monitor.health == "ok"

    def test_an_explicit_zero_quiet_floor_is_kept(self):
        assert TransitMonitor(HOST, PORT, min_quiet_s=0).min_quiet_s == 0.0
        assert TransitMonitor(HOST, PORT).min_quiet_s == TransitMonitor.DEFAULT_MIN_QUIET_S

    def test_unconfigured(self):
        monitor = TransitMonitor(None, None)
        assert not monitor.configured and monitor.health == "unconfigured"
        monitor.poll_once()
        assert monitor.health == "unconfigured" and monitor.summary() is None
        assert monitor.to_dict()["next_transit"] is None
