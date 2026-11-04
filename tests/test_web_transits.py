"""/transits, its partial and API, the TRANSITS badge and the gains card's hint."""

import json
import re
import time

import pytest
import responses

from tests.helpers import TRANSIT_SAMPLE as SAMPLE, configure_gain_archive, login, retimed

LOCAL = {"environ_base": {"REMOTE_ADDR": "127.0.0.1"}}


@pytest.fixture
def transits(app):
    """The monitor configured and holding a forecast made a minute ago,
    polled just now (so no page poll reaches the network)."""
    monitor = app.config["transit_monitor"]
    monitor.host, monitor.port = "gnss.example", 12070
    monitor.forecast = retimed(json.loads(SAMPLE.read_text()), time.time() - 60)
    monitor.health, monitor.error = "ok", None
    monitor.last_polled = monitor.last_seen = time.time()
    return monitor


class TestPage:
    def test_renders_the_forecast(self, client, transits):
        login(client)
        resp = client.get("/transits")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "Quiet windows for a gain measurement" in body
        assert "DIGUI-08" in body and "42 min" in body
        assert '<svg class="tl"' in body and "all bands" in body
        assert "E5b/B2b/L3 1197-1217" in body

    def test_the_partial_is_the_body(self, client, transits):
        login(client)
        resp = client.get("/partials/transits")
        assert resp.status_code == 200
        assert b"<svg" in resp.data and b"<html" not in resp.data

    def test_new_passes_are_noted(self, client, transits):
        transits.new_transits = [transits.forecast["transits"][-1]]
        transits.new_since = time.time() - 30
        login(client)
        assert "not in the forecast before it" in client.get("/transits").data.decode()

    def test_an_error_shows_with_the_forecast_kept(self, client, transits):
        transits.health, transits.error = "degraded", "the forecast is for az 180°, el 62.7°; the array points at el 60°"
        login(client)
        body = client.get("/transits").data.decode()
        assert "the array points at el 60" in body and "DIGUI-08" in body

    def test_no_forecast_yet(self, client, transits):
        transits.forecast = None
        transits.health, transits.error = "down", "gnss.example:12070: refused"
        login(client)
        body = client.get("/transits").data.decode()
        assert "refused" in body and '<svg class="tl"' not in body

    def test_unconfigured_is_404(self, client):
        login(client)
        assert client.get("/transits").status_code == 404
        assert client.get("/partials/transits").status_code == 404
        assert client.get("/api/transits").status_code == 404

    @responses.activate
    def test_a_watched_page_polls_a_stale_monitor(self, client, transits):
        transits.last_polled = time.time() - 60
        responses.add(responses.GET, "http://gnss.example:12070/transit_windows",
                      json=retimed(json.loads(SAMPLE.read_text()), time.time() - 10))
        login(client)
        assert client.get("/partials/transits").status_code == 200
        assert len(responses.calls) == 1 and time.time() - transits.last_polled < 5


class TestApi:
    def test_loopback_reads_the_forecast_and_the_summary(self, client, transits):
        resp = client.get("/api/transits", **LOCAL)
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["forecast"]["schema"] == "chord-gnss-transit-windows/0"
        assert body["summary"]["next_transit"]["name"] == "DIGUI-08"
        assert body["summary"]["gain_windows"][0]["length_fmt"] == "42 min"
        assert body["health"]["health"] == "ok"

    def test_status_carries_the_monitor(self, client, transits, jobs):
        assert client.get("/api/status", **LOCAL).get_json()["services"]["gnss"] == "ok"

    def test_metrics_carry_its_state(self, client, transits, jobs):
        text = client.get("/metrics").data.decode()
        assert 'choco_service_state{service="gnss",state="ok"} 1' in text
        assert 'choco_monitor_last_seen_timestamp_seconds{monitor="gnss"}' in text


class TestBadge:
    def test_the_strip_has_a_transits_tag(self, client, transits, jobs):
        login(client)
        body = client.get("/partials/menu").data.decode()
        assert "GNSS transits" in body and 'href="/transits"' in body
        assert "next transit: DIGUI-08 in" in body and "quiet for gains: " in body

    def test_unconfigured_has_no_tag(self, client, jobs):
        login(client)
        body = client.get("/partials/menu").data.decode()
        assert 'aria-label="GNSS transits:' not in body and 'href="/transits"' in body


class TestGainsCardHint:
    """The FPGA page's gains card says when the sky is quiet enough for a
    measurement, from the same summary."""

    def test_the_next_quiet_window(self, client, app, transits):
        configure_gain_archive(app)
        login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert "Quiet sky for a gain measurement:" in body
        assert "42 min, in" in body and 'href="/transits"' in body

    def test_a_window_already_begun(self, client, app, transits):
        configure_gain_archive(app)
        a, _b = transits.forecast["quiet"]["all"][0]
        transits.forecast = retimed(transits.forecast, transits.forecast["created_unix"] - (a - time.time()) - 60)
        login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert re.search(r"Quiet sky for a gain measurement:\s*now&ndash;", body)

    def test_no_window_in_the_forecast(self, client, app, transits):
        configure_gain_archive(app)
        transits.min_quiet_s = 7200
        login(client)
        assert "none of 2 h 00 min in the next" in client.get("/partials/fpga-gains").data.decode()

    def test_a_stale_forecast_offers_no_window(self, client, app, transits):
        configure_gain_archive(app)
        transits.health, transits.error = "stale", "the forecast is 40 min old"
        login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert "Quiet sky" not in body
        assert "No quiet window offered: the transit forecast is stale (the forecast is 40 min old)" in body
        menu = client.get("/partials/menu").data.decode()
        assert "quiet for gains" not in menu and "next transit" not in menu

    def test_local_times_beside_utc(self, client, app, transits):
        from zoneinfo import ZoneInfo
        import datetime as dt
        transits.tz = ZoneInfo("America/Vancouver")
        abbr = dt.datetime.now(transits.tz).strftime("%Z")
        login(client)
        body = client.get("/transits").data.decode()
        assert f"<th class=\"muted\">{abbr}</th>" in body and f"then the site's ({abbr})" in body
        assert re.search(rf"UTC \(\d\d:\d\d&ndash;\d\d:\d\d {abbr}\)", body)

    def test_without_the_monitor_the_card_says_nothing(self, client, app):
        configure_gain_archive(app)
        login(client)
        assert "Quiet sky" not in client.get("/partials/fpga-gains").data.decode()
