"""choco.fengine: the F-engine service's monitor."""

from unittest.mock import patch

import pytest
import responses

from choco.fengine import FengineMonitor, parse_archives, parse_boards, parse_master, parse_psu
from tests.helpers import csrf, login


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


#: One board of the service's ``/boards``, as read on site (2026-11-23).
BOARD = {"key": "0228", "booted": True, "stale": False, "age_s": 0.3, "ip": "10.222.100.106",
         "board": {"part": "CRS", "serial": "0228"}, "slot": 1,
         "bootloader": "tcpipe", "bootloader_version": "1.6-dirty"}
BOARDS = {"listening": True, "error": None, "summary": {"n_boards": 2, "n_stale": 1},
          "boards": [BOARD, {**BOARD, "slot": 2, "stale": True, "board": {"serial": "0244"}},
                     {**BOARD, "slot": None}]}


class TestServiceParsers:
    def test_boards_by_crate_slot(self):
        boards = parse_boards(BOARDS)
        assert boards["n_boards"] == 3 and boards["n_booted"] == 1 and boards["unplaced"] == 1
        assert boards["by_slot"][1] == {"serial": "0228", "ip": "10.222.100.106",
                                        "bootloader": "tcpipe 1.6-dirty", "booted": True,
                                        "stale": False, "age_s": 0.3}
        assert boards["by_slot"][2]["stale"] is True
        assert parse_boards({"boards": None}) is None

    def test_archives_count_and_newest_run(self):
        files = [{"filename": "/d/20260808T053625Z_chord_pathfinder_digitalgain/00000000.h5",
                  "modified": 10.0},
                 {"filename": "/d/20261123T212958Z_chord_pathfinder_digitalgain/00000000.h5",
                  "modified": 20.0}]
        assert parse_archives(files) == {
            "count": 2, "newest": "20261123T212958Z_chord_pathfinder_digitalgain", "newest_at": 20.0}
        assert parse_archives([])["count"] == 0
        assert parse_archives({"detail": "x"}) is None


class TestSupplyControl:
    @responses.activate
    def test_on_posts_and_reports_the_output(self, monitor):
        on = responses.post(f"{BASE}/psu/on", json=PSU)
        responses.get(f"{BASE}/fpga-master", json=MASTER)
        responses.get(f"{BASE}/psu/metrics", json=PSU)
        assert monitor.psu_action("on") == (True, "output on")
        assert on.calls[0].request.body == b"{}"
        assert monitor.health == "ok"                    # it polled again

    @responses.activate
    def test_refusal_is_the_services_reason(self, monitor):
        responses.post(f"{BASE}/psu/off", status=409,
                       json={"detail": "fpga_master is running. Stop first, or use force=true"})
        ok, message = monitor.psu_action("off")
        assert ok is False and message.startswith("fpga_master is running")

    def test_unknown_action_never_asks(self, monitor):
        assert monitor.psu_action("explode")[0] is False


def _session(state, operation=None, error=None, **more):
    return {"id": "20261123T212018Z", "state": state, "operation": operation, "error": error,
            "problems": [], "reports": {}, "plots": [], "history": [], **more}


class TestGainRun:
    @pytest.fixture(autouse=True)
    def fast(self, monkeypatch):
        monkeypatch.setattr(FengineMonitor, "GAIN_POLL_S", 0)

    def _serve(self, replies):
        """GET /gains/session answers with *replies* in turn (the last one repeats)."""
        for r in replies[:-1]:
            responses.get(f"{BASE}/gains/session", **r)
        responses.get(f"{BASE}/gains/session", **replies[-1])

    @responses.activate
    def test_chain_opens_then_sends_each_step_when_the_last_finished(self, monitor):
        none = {"status": 404, "json": {"detail": "No gain session"}}
        self._serve([none,
                     {"json": _session("opening", "open")}, {"json": _session("open")},
                     {"json": _session("open", "capture")}, {"json": _session("captured")},
                     {"json": _session("computed")}, {"json": _session("applied")}])
        opened = responses.post(f"{BASE}/gains/session", json={}, status=202)
        steps = {s: responses.post(f"{BASE}/gains/session/{s}", json={}, status=202)
                 for s in ("capture", "compute", "apply")}
        assert monitor.gain_chain() == (True, "applied; ready for review")
        assert opened.calls[0].request.body == b'{"stop_fpga_master": true}'
        assert [steps[s].call_count for s in ("capture", "compute", "apply")] == [1, 1, 1]
        assert monitor.gain_run["active"] is False and monitor.gain_run["ok"] is True

    @responses.activate
    def test_a_failed_step_stops_the_chain_with_its_error(self, monitor):
        self._serve([{"json": _session("open")},
                     {"json": _session("open", error="capture failed: RuntimeError('UCAP OVERRUN')")}])
        responses.post(f"{BASE}/gains/session/capture", json={}, status=202)
        compute = responses.post(f"{BASE}/gains/session/compute", json={}, status=202)
        ok, message = monitor.gain_chain()
        assert ok is False and "UCAP OVERRUN" in message and compute.call_count == 0

    @responses.activate
    def test_a_continued_run_tries_the_failed_step_again(self, monitor):
        # The error is from before this chain: it is not this chain's failure.
        self._serve([{"json": _session("open", error="capture failed: x")},
                     {"json": _session("open", error="capture failed: x")},
                     {"json": _session("applied")}])
        capture = responses.post(f"{BASE}/gains/session/capture", json={}, status=202)
        assert monitor.gain_chain()[0] is True and capture.call_count == 1

    @responses.activate
    def test_a_refused_open_is_the_services_reason(self, monitor):
        self._serve([{"status": 404, "json": {}}])
        responses.post(f"{BASE}/gains/session", status=409,
                       json={"detail": "The PSU is off; turn it on first"})
        assert monitor.gain_chain() == (False, "The PSU is off; turn it on first")

    @responses.activate
    def test_approve_restarts_fpga_master_and_reject_does_not(self, monitor):
        approve = responses.post(f"{BASE}/gains/session/approve", json={}, status=202)
        reject = responses.post(f"{BASE}/gains/session/reject", json={})
        assert monitor.gain_decide("approve")[0] is True
        assert approve.calls[0].request.body == b'{"override": false, "restart_fpga_master": true}'
        assert monitor.gain_decide("approve-anyway")[0] is True
        assert b'"override": true' in approve.calls[1].request.body
        assert monitor.gain_decide("reject")[0] is True and reject.calls[0].request.body == b"{}"
        monitor.gain_run = {"active": True}
        assert monitor.gain_decide("approve")[0] is False      # not while the chain runs
        assert monitor.gain_decide("reject")[0] is True        # a reject cancels it

    def test_steps_follow_state_operation_and_error(self):
        from choco.fengine import gain_steps
        def words(s):
            return [x["status"] for x in gain_steps(s)]
        assert words(_session("opening", "open")) == ["running", "pending", "pending", "pending"]
        assert words(_session("captured", "compute")) == ["done", "done", "running", "pending"]
        assert words(_session("open", error="capture failed: x")) == ["done", "failed", "pending", "pending"]
        assert words(_session("applied")) == ["done"] * 4


class TestPoll:
    @responses.activate
    def test_extras_are_read_and_their_failures_are_not_the_health(self, monitor):
        responses.get(f"{BASE}/fpga-master", json=MASTER)
        responses.get(f"{BASE}/psu/metrics", json=PSU)
        responses.get(f"{BASE}/boards", json=BOARDS)
        responses.get(f"{BASE}/events", json=[{"time": "t1", "kind": "a"}, {"time": "t2", "kind": "b"}])
        responses.get(f"{BASE}/conflicts", json=[{"pid": 7, "user": "crs", "cmdline": "x"}])
        responses.get(f"{BASE}/gains/session", status=404, json={"detail": "No gain session"})
        monitor.poll_once()
        assert monitor.health == "ok" and monitor.boards["n_boards"] == 3
        assert [e["kind"] for e in monitor.events] == ["b", "a"]          # newest first
        assert monitor.conflicts[0]["pid"] == 7 and monitor.session is None
        assert monitor.archives is None                  # /gains/archives did not answer

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
    def _configure(self, app, psu=PSU, master=MASTER):
        fpga = app.config["fpga_monitor"]
        fpga.host, fpga.port = "fpga.example", 54321
        fengine = app.config["fengine_monitor"]
        fengine.host, fengine.port = HOST, PORT
        fengine.health, fengine.psu, fengine.master = "ok", parse_psu(psu), parse_master(master)
        return fpga, fengine

    def _status(self, client, fpga, fengine):
        with patch.object(fpga, "poll_if_stale"), patch.object(fengine, "poll_if_stale"):
            return client.get("/partials/service-fpga").data.decode()

    def test_without_the_service_the_page_is_fpga_masters(self, client, app):
        fpga = app.config["fpga_monitor"]
        fpga.host, fpga.port = "fpga.example", 54321
        login(client)
        with patch.object(fpga, "poll_if_stale"):
            body = client.get("/partials/service-fpga").data.decode()
            page = client.get("/fpga").data.decode()
        assert "Crate supply" not in body and "Endpoint" in body
        assert "/service/fpga/start" in page and "/service/fpga/psu/" not in page

    def test_dashboard_shows_supply_and_supervisor(self, client, app):
        fpga, fengine = self._configure(app)
        fengine.archives = {"count": 12, "newest": "20261123T212958Z_x", "newest_at": 1.0}
        login(client)
        with patch.object(fpga, "poll_if_stale"), patch.object(fengine, "poll_if_stale") as pis:
            body = client.get("/partials/service-fpga").data.decode()
            page = client.get("/fpga").data.decode()
        pis.assert_called_once_with(5)
        assert "12.35 V · 107.8 A · 1331 W" in body and "<dd>203 V</dd>" in body
        assert "outlet 69 °C" in body and "12576 rpm" in body
        assert "<code>running</code> (pid 5098)" in body and "1 restarts, 0 power cycles" in body
        assert "12, newest" in body
        assert "Crate supply" in page and page.count("/service/fpga/start") == 1

    def test_output_off_and_faults_are_marked(self, client, app):
        fpga, fengine = self._configure(app, psu={**PSU, "output_on": False},
                                        master={**MASTER, "state": "stopped", "pid": None})
        login(client)
        off = self._status(client, fpga, fengine)
        fengine.psu = parse_psu({**PSU, "faults": ["OTP"]})
        fault = self._status(client, fpga, fengine)
        assert "tag-warn" in off and "output off" in off and "<code>stopped</code>" in off
        assert "tag-bad" in fault and "<code>OTP</code>" in fault

    @staticmethod
    def _button(body, action):
        """The submit button of one control's form."""
        form = body.split(f'/service/fpga/{action}"', 1)[1]
        return form.split("<button", 1)[1].split(">", 1)[0]

    def test_start_needs_the_supply_on(self, client, app):
        stopped = {**MASTER, "state": "stopped", "pid": None}
        fpga, fengine = self._configure(app, psu={**PSU, "output_on": False}, master=stopped)
        login(client)
        off = self._status(client, fpga, fengine)
        assert "disabled" in self._button(off, "start")
        assert "Start is off: the crate supply&#39;s output is off" in off
        assert "disabled" not in self._button(off, "psu/on")
        assert "disabled" in self._button(off, "psu/off")
        fengine.psu = parse_psu(PSU)
        on = self._status(client, fpga, fengine)
        assert "disabled" not in self._button(on, "start")
        assert "disabled" in self._button(on, "stop")        # nothing to stop

    def test_supply_off_waits_for_fpga_master(self, client, app):
        fpga, fengine = self._configure(app)                  # running, output on
        login(client)
        body = self._status(client, fpga, fengine)
        assert "disabled" in self._button(body, "start")
        assert "disabled" not in self._button(body, "stop")
        assert "Stop fpga_master first" in self._button(body, "psu/off")
        assert "Stop fpga_master first" in self._button(body, "psu/power-cycle")

    def test_start_is_refused_server_side_with_the_supply_off(self, client, app):
        fpga, fengine = self._configure(app, psu={**PSU, "output_on": False})
        login(client)
        token = csrf(client)
        with patch.object(fengine, "poll_once"), patch.object(fpga, "start_master") as start:
            resp = client.post("/service/fpga/start", data={"_csrf_token": token})
        assert resp.status_code == 302
        start.assert_not_called()
        assert fpga.actions[0]["ok"] is False and "output is off" in fpga.actions[0]["message"]

    def test_supply_route_calls_the_service_and_records(self, client, app):
        fpga, fengine = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(fengine, "psu_action", return_value=(True, "output on")) as act:
            resp = client.post("/service/fpga/psu/on", data={"_csrf_token": token})
            missing = client.post("/service/fpga/psu/explode", data={"_csrf_token": token})
        assert resp.status_code == 302 and missing.status_code == 404
        act.assert_called_once_with("on")
        assert fpga.actions[0]["action"] == "supply on" and fpga.actions[0]["user"] == "tester"

    def test_supply_route_needs_control(self, client, app):
        fpga, fengine = self._configure(app)
        app.config["fengine_cfg"] = {"control": False}
        login(client)
        token = csrf(client)
        with patch.object(fengine, "psu_action") as act:
            resp = client.post("/service/fpga/psu/off", data={"_csrf_token": token})
        assert resp.status_code == 403
        act.assert_not_called()
        assert "/service/fpga/psu/" not in self._status(client, fpga, fengine)

    def test_gain_run_buttons_follow_the_session(self, client, app):
        stopped = {**MASTER, "state": "stopped", "pid": None}
        fpga, fengine = self._configure(app, master=stopped)
        login(client)
        body = self._status(client, fpga, fengine)
        assert "Start gain run" in body and "disabled" not in self._button(body, "gains/run")
        assert "gains/approve" not in body
        fengine.session = _session("captured", "compute")
        fengine.gain_run = {"active": True, "ok": None, "message": "compute", "started": 1.0}
        body = self._status(client, fpga, fengine)
        assert "disabled" in self._button(body, "gains/run") and "compute …" in body
        assert "Cancel run" in body
        fengine.gain_run["active"] = False
        fengine.session = _session("applied", plots=["target.png"], history=[1, 2],
                                   reports={"fft": {"label": "FFT", "problems": []}})
        body = self._status(client, fpga, fengine)
        assert "disabled" not in self._button(body, "gains/approve") and "gains/reject" in body
        assert "gains/run" not in body and "approve-anyway" not in body
        assert "/service/fpga/gains/plot/target.png?v=20261123T212018Z-2" in body
        fengine.session = _session("applied", problems=["fft: dropped packets"],
                                   reports={"fft": {"label": "FFT", "problems": ["dropped packets"]}})
        body = self._status(client, fpga, fengine)
        assert "disabled" in self._button(body, "gains/approve") and "gains/approve-anyway" in body

    def test_gain_run_route_spawns_the_chain_once(self, client, app):
        fpga, fengine = self._configure(app, master={**MASTER, "state": "stopped", "pid": None})
        login(client)
        token = csrf(client)
        with patch.object(fengine, "poll_once"), patch("choco.web.gevent.spawn") as spawn:
            first = client.post("/service/fpga/gains/run", data={"_csrf_token": token})
            second = client.post("/service/fpga/gains/run", data={"_csrf_token": token})
        assert first.status_code == 302 and second.status_code == 302
        assert spawn.call_count == 1 and fengine.gain_run["active"] is True
        assert "refused" in fpga.actions[0]["message"]

    def test_gain_run_needs_the_supply_on(self, client, app):
        fpga, fengine = self._configure(app, psu={**PSU, "output_on": False})
        login(client)
        token = csrf(client)
        with patch.object(fengine, "poll_once"), patch("choco.web.gevent.spawn") as spawn:
            client.post("/service/fpga/gains/run", data={"_csrf_token": token})
        spawn.assert_not_called()

    def test_gain_decision_and_plot_routes(self, client, app):
        fpga, fengine = self._configure(app)
        fengine.session = _session("applied", plots=["target.png"])
        login(client)
        token = csrf(client)
        with patch.object(fengine, "gain_decide", return_value=(True, "rejected")) as decide:
            resp = client.post("/service/fpga/gains/reject", data={"_csrf_token": token})
            assert client.post("/service/fpga/gains/explode", data={"_csrf_token": token}).status_code == 404
        decide.assert_called_once_with("reject")
        assert resp.status_code == 302 and fpga.actions[0]["action"] == "gain reject"
        with patch("choco.fengine.fetch") as fetch:
            fetch.return_value.content = b"PNG"
            png = client.get("/service/fpga/gains/plot/target.png")
            other = client.get("/service/fpga/gains/plot/secret.png")
        assert png.data == b"PNG" and png.mimetype == "image/png" and other.status_code == 404
        assert fetch.call_count == 1

    def test_boards_join_the_service_and_fpga_master(self, client, app):
        fpga, fengine = self._configure(app)
        fengine.boards = parse_boards(BOARDS)
        fengine.events = [{"time": "2026-11-23T21:29:58+00:00", "source": "gain_session",
                           "kind": "approve", "level": "warning", "message": "approve finished"}]
        fengine.conflicts = []
        fpga.hw_map = {0: ["CRS0228"]}
        login(client)
        body = client.get("/partials/fpga-boards").data.decode()
        assert "CRS0228" in body and "10.222.100.106" in body and "tcpipe 1.6-dirty" in body
        assert "silent" in body                              # slot 2: heard, not in the map
        assert "2026-11-23 21:29:58" in body and "approve finished" in body
        assert "none open" in body
