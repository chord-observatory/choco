"""choco.fengine: the F-engine service's monitor."""

from unittest.mock import patch

import pytest
import responses

from choco.fengine import FengineMonitor, parse_master, parse_psu
from tests.helpers import login


HOST = "crs.example"
PORT = 8080
BASE = f"http://{HOST}:{PORT}"

#: The supply's own JSON, as read on site (2026-11-23).
PSU = {"vin": 203.25, "vout": 12.35, "iout": 107.750, "temp_inlet": 32.3, "temp_oring": 48.0,
       "temp_outlet": 69.2, "fan_rpm": 12576, "output_on": True, "faults": []}

#: ``FpgaMasterManager.info()`` of f-engine-tools 4f5e17b.
MASTER = {"state": "running", "error": None, "pid": 5098,
          "config": "/localhome/crs/f-engine-tools/pathfinder_config.yaml:pathfinder",
          "default_config": "~/f-engine-tools/pathfinder_config.yaml:pathfinder",
          "url": "http://localhost:54321", "started_at": "2026-11-14T21:01:49+00:00",
          "log_path": "/x/fpga_master.log", "fpga_master_status": {"state": "on", "is_ready": True},
          "restart_count": 1, "power_cycle_count": 0,
          "watchdog": {"enabled": True, "paused": False}}


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr("choco.http.RETRY_DELAY_S", 0)


@pytest.fixture
def monitor():
    return FengineMonitor(host=HOST, port=PORT, timeout=1)


class TestParsers:
    def test_psu_keeps_the_numbers_and_adds_watts(self):
        psu = parse_psu(PSU)
        assert psu["output_on"] is True and psu["faults"] == []
        assert psu["vout"] == 12.35 and psu["fan_rpm"] == 12576.0
        assert psu["watts"] == pytest.approx(12.35 * 107.75)

    def test_psu_drops_what_is_not_a_number(self):
        psu = parse_psu({"output_on": 0, "vout": "12", "iout": None, "faults": ["OTP", 3]})
        assert psu == {"output_on": False, "faults": ["OTP", "3"]}

    def test_psu_refuses_another_answer(self):
        assert parse_psu({"detail": "x"}) is None
        assert parse_psu([1]) is None

    def test_master_is_the_supervisor_facts(self):
        master = parse_master(MASTER)
        assert master["state"] == "running" and master["pid"] == 5098
        assert master["restart_count"] == 1 and master["watchdog"] == "on"
        assert "fpga_master_status" not in master and "log_path" not in master
        assert parse_master({**MASTER, "watchdog": {"enabled": True, "paused": True}})["watchdog"] == "paused"
        assert parse_master({**MASTER, "watchdog": None})["watchdog"] == "off"
        assert parse_master({"ok": True}) is None


class TestPoll:
    def test_unconfigured_never_asks(self):
        m = FengineMonitor(host=None, port=None)
        assert m.health == "unconfigured"
        m.poll_once()
        assert m.health == "unconfigured" and m.psu is None

    @responses.activate
    def test_ok(self, monitor):
        responses.get(f"{BASE}/fpga-master", json=MASTER)
        responses.get(f"{BASE}/psu/metrics", json=PSU)
        monitor.poll_once()
        assert monitor.health == "ok" and monitor.error is None
        assert monitor.psu["iout"] == 107.75 and monitor.master["state"] == "running"
        assert monitor.last_seen == monitor.last_polled

    @responses.activate
    def test_supply_not_answering_is_no_psu_with_the_reason(self, monitor):
        responses.get(f"{BASE}/fpga-master", json=MASTER)
        responses.get(f"{BASE}/psu/metrics", status=502,
                      json={"detail": "PSU call get_status failed after 3 attempts"})
        monitor.poll_once()
        assert monitor.health == "no_psu" and monitor.psu is None
        assert "after 3 attempts" in monitor.psu_error
        assert monitor.master["state"] == "running"      # the supervisor still answered

    @responses.activate
    def test_service_down_clears_the_readings(self, monitor):
        responses.get(f"{BASE}/fpga-master", json=MASTER)
        responses.get(f"{BASE}/psu/metrics", json=PSU)
        monitor.poll_once()
        seen = monitor.last_seen
        responses.reset()
        responses.get(f"{BASE}/fpga-master", body=ConnectionError("refused"))
        monitor.poll_once()
        assert monitor.health == "down" and monitor.psu is None and monitor.master is None
        assert monitor.last_seen == seen

    @responses.activate
    def test_something_else_on_the_port_is_down(self, monitor):
        responses.get(f"{BASE}/fpga-master", json={"hello": "world"})
        monitor.poll_once()
        assert monitor.health == "down"


class TestWiring:
    def test_fpga_master_goes_through_a_configured_service(self, configs_dir):
        from choco.app import _DEFAULT_CONFIG, create_app
        fpga_master = {"host": "fpga.example", "port": 54321}
        direct = create_app(configs_dir=configs_dir, config={**_DEFAULT_CONFIG, "fpga_master": fpga_master})
        fpga = direct.config["fpga_monitor"]
        assert (fpga.port, fpga.via_service) == (54321, False)
        via = create_app(configs_dir=configs_dir, config={
            **_DEFAULT_CONFIG, "fpga_master": fpga_master, "fengine_service": {"host": HOST, "port": PORT}})
        fpga = via.config["fpga_monitor"]
        assert (fpga.host, fpga.port, fpga.via_service) == (HOST, PORT, True)
        assert via.config["gain_archive"].base_url == BASE


class TestPage:
    def _configure(self, app):
        fpga = app.config["fpga_monitor"]
        fpga.host, fpga.port = "fpga.example", 54321
        fengine = app.config["fengine_monitor"]
        fengine.host, fengine.port = HOST, PORT
        return fpga, fengine

    def test_absent_when_not_configured(self, client, app):
        fpga = app.config["fpga_monitor"]
        fpga.host, fpga.port = "fpga.example", 54321
        login(client)
        with patch.object(fpga, "poll_if_stale"):
            body = client.get("/partials/service-fpga").data.decode()
        assert "Crate supply" not in body

    def test_status_block_shows_supply_and_supervisor(self, client, app):
        fpga, fengine = self._configure(app)
        fengine.health, fengine.psu, fengine.master = "ok", parse_psu(PSU), parse_master(MASTER)
        login(client)
        with patch.object(fpga, "poll_if_stale"), patch.object(fengine, "poll_if_stale") as pis:
            body = client.get("/partials/service-fpga").data.decode()
            page = client.get("/fpga").data.decode()
        pis.assert_called_once_with(5)
        assert "12.35 V · 107.8 A · 1331 W" in body and "input 203 V" in body
        assert "outlet 69 °C" in body and "fan 12576 rpm" in body
        assert "<code>running</code> (pid 5098)" in body and "1 restarts, 0 power cycles" in body
        assert "Crate supply and supervisor" in page

    def test_output_off_and_faults_are_marked(self, client, app):
        fpga, fengine = self._configure(app)
        fengine.health, fengine.master = "ok", parse_master({**MASTER, "state": "stopped", "pid": None})
        fengine.psu = parse_psu({**PSU, "output_on": False})
        login(client)
        with patch.object(fpga, "poll_if_stale"), patch.object(fengine, "poll_if_stale"):
            off = client.get("/partials/service-fpga").data.decode()
            fengine.psu = parse_psu({**PSU, "faults": ["OTP"]})
            fault = client.get("/partials/service-fpga").data.decode()
        assert "tag-warn" in off and "output off" in off and "<code>stopped</code>" in off
        assert "tag-bad" in fault and "<code>OTP</code>" in fault
