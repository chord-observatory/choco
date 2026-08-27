"""Tests for choco/services.py: the FPGA monitor and job-status helpers."""

import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses

from choco.services import (
    FpgaMonitor, PdbMonitor, decode_out_bytes, job_status, timer_status,
    read_state_json, sanitize_pipeline_svg, EOP_STALE_AFTER_S,
)


HOST = "fpga.example"
PORT = 54321
BASE = f"http://{HOST}:{PORT}"


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    """Keep the retry's real-time sleep out of the test suite."""
    monkeypatch.setattr("choco.services._RETRY_DELAY_S", 0)


@pytest.fixture
def monitor():
    return FpgaMonitor(host=HOST, port=PORT, timeout=1)


class TestFpgaMonitorPollOnce:
    @responses.activate
    def test_healthy_path(self, monitor):
        responses.get(f"{BASE}/status", json={"ok": True})
        responses.get(f"{BASE}/get-frame0-time",
                      json={"frame0_nano": 1700000000000000000, "start_ctime": 0.0})
        monitor.poll_once()
        assert monitor.health == "ok"
        assert monitor.frame0_ns == 1700000000000000000
        assert monitor.error is None
        assert monitor.last_seen is not None

    @responses.activate
    def test_status_down(self, monitor):
        # /status fails -> overall down; /get-frame0-time never reached.
        responses.get(f"{BASE}/status", status=500)
        monitor.poll_once()
        assert monitor.health == "down"
        assert monitor.frame0_ns is None
        assert monitor.error is not None

    @responses.activate
    def test_status_unreachable(self, monitor):
        # No response registered -> ConnectionError.
        monitor.poll_once()
        assert monitor.health == "down"
        assert "ConnectionError" in monitor.error

    @responses.activate
    def test_status_ok_but_timing_missing(self, monitor):
        responses.get(f"{BASE}/status", json={"ok": True})
        responses.get(f"{BASE}/get-frame0-time", status=503)
        monitor.poll_once()
        assert monitor.health == "no_timing"
        assert monitor.frame0_ns is None
        assert "503" in monitor.error or "no_timing" in monitor.error or monitor.error

    @responses.activate
    def test_status_ok_but_timing_unparseable(self, monitor):
        responses.get(f"{BASE}/status", json={"ok": True})
        responses.get(f"{BASE}/get-frame0-time", json={"not_frame0": 1})
        monitor.poll_once()
        assert monitor.health == "no_timing"

    def test_unconfigured_monitor_reports_unconfigured(self):
        mon = FpgaMonitor(host=None, port=None)
        mon.poll_once()
        assert mon.health == "unconfigured"
        assert mon.configured is False

    @responses.activate
    def test_state_and_start_result_captured(self, monitor):
        responses.get(f"{BASE}/status",
                      json={"state": "on", "is_ready": True,
                            "start_result": "Initialization complete"})
        responses.get(f"{BASE}/get-frame0-time",
                      json={"frame0_nano": 1, "start_ctime": 0.0})
        monitor.poll_once()
        assert monitor.state == "on"
        assert monitor.start_result == "Initialization complete"
        assert monitor.to_dict()["state"] == "on"

    @responses.activate
    def test_failed_start_body_surfaces_in_error(self, monitor):
        # A failed background /start makes /status itself error with the
        # exception in the body.
        responses.get(f"{BASE}/status", status=500,
                      body="RuntimeError: no ICEBoards found")
        monitor.poll_once()
        assert monitor.health == "down"
        assert monitor.state is None
        assert "no ICEBoards found" in monitor.error

    @responses.activate
    def test_status_non_dict_json_tolerated(self, monitor):
        responses.get(f"{BASE}/status", json=["not", "a", "dict"])
        responses.get(f"{BASE}/get-frame0-time",
                      json={"frame0_nano": 1, "start_ctime": 0.0})
        monitor.poll_once()
        assert monitor.health == "ok"
        assert monitor.state is None

    @responses.activate
    def test_single_dropped_request_is_retried(self, monitor):
        import requests as _requests
        # First /status attempt drops; the immediate retry succeeds —
        # the badge must not flip red on one transient blip.
        responses.get(f"{BASE}/status",
                      body=_requests.ConnectionError("blip"))
        responses.get(f"{BASE}/status", json={"state": "on"})
        responses.get(f"{BASE}/get-frame0-time",
                      json={"frame0_nano": 1, "start_ctime": 0.0})
        monitor.poll_once()
        assert monitor.health == "ok"
        assert monitor.state == "on"

    @responses.activate
    def test_persistent_failure_still_down(self, monitor):
        import requests as _requests
        # One registration serves every attempt, including the retry.
        responses.get(f"{BASE}/status",
                      body=_requests.ConnectionError("gone"))
        monitor.poll_once()
        assert monitor.health == "down"
        assert len(responses.calls) == 2  # original + retry


class TestFpgaMonitorControls:
    @responses.activate
    def test_start_master_posts_empty_config(self, monitor):
        start = responses.post(
            f"{BASE}/start",
            json="Initialization in progress. Check status for completion.")
        # start_master() re-polls; give the poll something to hit.
        responses.get(f"{BASE}/status", json={"state": "starting"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        ok, message = monitor.start_master()
        assert ok is True
        assert "Initialization in progress" in message
        assert start.calls[0].request.body == b"{}"

    @responses.activate
    def test_start_master_failure(self, monitor):
        responses.post(f"{BASE}/start", status=500)
        ok, message = monitor.start_master()
        assert ok is False
        assert message

    def test_start_master_unconfigured(self):
        ok, message = FpgaMonitor(host=None, port=None).start_master()
        assert ok is False
        assert "not configured" in message

    @responses.activate
    def test_start_master_error_body_is_failure(self, monitor):
        # wtl.rest reports handler exceptions as HTTP 200 + error body.
        responses.post(f"{BASE}/start",
                       json={"error": "RuntimeError('no config')"})
        responses.get(f"{BASE}/status", json={"state": "off"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        ok, message = monitor.start_master()
        assert ok is False
        assert "no config" in message

    @responses.activate
    def test_stop_master_uses_get_and_repolls(self, monitor):
        # GET, not POST: wtl.rest serves argument-less endpoints for GET
        # only (POST /stop is a 405 — the original silent-stop bug).
        stop = responses.get(f"{BASE}/stop", json={})
        responses.get(f"{BASE}/status", json={"state": "off"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        ok, message = monitor.stop_master()
        assert ok is True
        assert stop.call_count == 1
        assert monitor.state == "off"

    @responses.activate
    def test_stop_master_http_failure_still_repolls(self, monitor):
        responses.get(f"{BASE}/stop", status=500)
        responses.get(f"{BASE}/status", json={"state": "on"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        ok, message = monitor.stop_master()
        assert ok is False
        assert monitor.state == "on"

    @responses.activate
    def test_stop_master_remote_crash_is_failure(self, monitor):
        # The live failure mode: stop() raised remotely, wtl returned
        # 200 with the exception in an error body, state wedged.
        responses.get(f"{BASE}/stop", json={
            "error": "AttributeError(\"'FPGAMaster' object has no "
                     "attribute 'iceboard_cb'\")"})
        responses.get(f"{BASE}/status", json={"state": "stopping"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        ok, message = monitor.stop_master()
        assert ok is False
        assert "iceboard_cb" in message
        assert monitor.state == "stopping"


class TestFpgaMonitorPollIfStale:
    @responses.activate
    def test_polls_when_never_polled(self, monitor):
        responses.get(f"{BASE}/status", json={"state": "on"})
        responses.get(f"{BASE}/get-frame0-time", json={})
        monitor.poll_if_stale(10)
        assert monitor.last_polled is not None

    def test_skips_when_fresh(self, monitor):
        monitor.last_polled = time.time()
        # No responses registered: a poll would raise ConnectionError
        # and flip health to down — skipping leaves it untouched.
        monitor.poll_if_stale(10)
        assert monitor.health == "unknown"

    def test_unconfigured_never_polls(self):
        mon = FpgaMonitor(host=None, port=None)
        mon.poll_if_stale(0)
        assert mon.last_polled is None


PDB_BASE = "http://pdb.example:5000"


@pytest.fixture
def pdb():
    return PdbMonitor(host="pdb.example", port=5000, timeout=1)


class TestDecodeOutBytes:
    def test_out_bytes_are_odd_positions_from_the_end(self):
        # 2 chips -> 4 raw bytes; after reversal every even index is a
        # chip's OUT byte, chip 0 first.
        assert decode_out_bytes([0x18, 0x02, 0x18, 0x01]) == [0x01, 0x02]

    def test_live_shape_all_off(self):
        # The deployed controller reads [128, 0] per chip: status byte
        # 128, OUT byte 0 -> every channel off.
        assert decode_out_bytes([128, 0] * 4) == [0, 0, 0, 0]

    def test_empty(self):
        assert decode_out_bytes([]) == []


class TestPdbMonitorPollOnce:
    @responses.activate
    def test_healthy_path(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        # chip 0 (board 0 A): ch0 on; chip 1 (board 0 B): all off
        responses.get(f"{PDB_BASE}/channel_states", json={
            "channel_states": {"0": [0x18, 0x00, 0x18, 0x01]},
        })
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.error is None
        assert pdb.boards == {0: 1}
        rows = pdb.channels[0]
        assert rows[0] == {"board": 0, "chip": "A",
                           "channels": [True] + [False] * 7}
        assert rows[1]["chip"] == "B"
        assert not any(rows[1]["channels"])
        assert pdb.n_channels == 16
        assert pdb.n_on == 1
        assert pdb.last_seen is not None

    @responses.activate
    def test_status_down(self, pdb):
        responses.get(f"{PDB_BASE}/status", status=500)
        pdb.poll_once()
        assert pdb.health == "down"
        assert pdb.error

    @responses.activate
    def test_status_unreachable(self, pdb):
        import requests as _requests
        responses.get(f"{PDB_BASE}/status",
                      body=_requests.ConnectionError("refused"))
        pdb.poll_once()
        assert pdb.health == "down"

    @responses.activate
    def test_status_ok_but_states_missing(self, pdb):
        responses.get(f"{PDB_BASE}/status",
                      json={"active_buses": [0], "buses": {}})
        responses.get(f"{PDB_BASE}/channel_states", status=500)
        pdb.poll_once()
        assert pdb.health == "no_states"
        assert "/channel_states" in pdb.error

    @responses.activate
    def test_status_non_dict_json_tolerated(self, pdb):
        responses.get(f"{PDB_BASE}/status", json=["garbage"])
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": {"0": [128, 0]}})
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.boards == {}

    @responses.activate
    def test_states_wrong_shape_is_no_states(self, pdb):
        responses.get(f"{PDB_BASE}/status",
                      json={"active_buses": [0], "buses": {}})
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": "bogus"})
        pdb.poll_once()
        assert pdb.health == "no_states"
        assert "unexpected /channel_states shape" in pdb.error

    @responses.activate
    def test_states_garbage_bus_info_tolerated(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": "sixteen", "1": {"board_count": 2}},
        })
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": {}})
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.boards == {1: 2}

    def test_unconfigured_monitor_reports_unconfigured(self):
        m = PdbMonitor(host=None, port=None)
        assert m.configured is False
        assert m.health == "unconfigured"
        m.poll_once()
        assert m.health == "unconfigured"

    @responses.activate
    def test_to_dict(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        responses.get(f"{PDB_BASE}/channel_states", json={
            "channel_states": {"0": [0, 0xFF, 0, 0xFF]},
        })
        pdb.poll_once()
        d = pdb.to_dict()
        assert d["health"] == "ok"
        assert d["buses"] == [0]
        assert d["n_channels"] == 16
        assert d["n_on"] == 16


def _states(raw: list[int]) -> dict:
    return {"channel_states": {"0": raw}}


class TestPdbSetChannel:
    """1 board = 2 chips = 4 raw bytes; chip0's OUT byte is raw[3],
    chip1's is raw[1] (odd positions from the end)."""

    @responses.activate
    def test_turn_on(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))          # pre: all off
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))          # post: ch0 on
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is True
        assert "ch0 on" in message
        import json as _json
        payload = _json.loads(write.calls[0].request.body)
        assert payload == {"spi_bus": 0, "board_idx": 0, "chip_letter": "A",
                           "operation": "OUT", "states": "00000001"}
        # the post-write read refreshed the grid
        assert pdb.channels[0][0]["channels"][0] is True

    @responses.activate
    def test_turn_off_keeps_other_bits(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0b11]))       # ch0+ch1 on
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0b10]))
        ok, message = pdb.set_channel(0, 0, "A", 0, False)
        assert ok is True
        import json as _json
        assert _json.loads(write.calls[0].request.body)["states"] == "00000010"

    @responses.activate
    def test_already_on_skips_write(self, pdb):
        # No POST registered: a write attempt would raise ConnectionError.
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is True
        assert "already on" in message

    @responses.activate
    def test_verify_mismatch_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))          # didn't take
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "verify failed" in message
        assert "state changed underneath us" in message

    @responses.activate
    def test_write_failure(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", status=500)
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False

    @responses.activate
    def test_chip_not_present(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([]))
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "not present" in message

    @responses.activate
    def test_garbage_states_reported_not_raised(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": [1, 2, 3]})
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "ValueError" in message

    @responses.activate
    def test_controller_unreachable_reported(self, pdb):
        # no responses registered -> ConnectionError on the pre-read
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "ConnectionError" in message

    def test_invalid_address(self, pdb):
        ok, message = pdb.set_channel(0, 0, "C", 0, True)
        assert ok is False
        ok, message = pdb.set_channel(0, 0, "A", 8, True)
        assert ok is False

    def test_unconfigured(self):
        ok, message = PdbMonitor(host=None, port=None).set_channel(
            0, 0, "A", 0, True)
        assert ok is False
        assert "not configured" in message


class TestPdbSetGroup:
    """Bulk power: whole chips written as all-ones / all-zeros OUT bytes.

    Two boards = 4 chips = 8 raw bytes; chip k's OUT byte is raw[2N-1-2k],
    so [128, x3, 128, x2, 128, x1, 128, x0] holds chips 0..3 in x0..x3.
    """

    @staticmethod
    def _payloads(write):
        import json as _json
        return [_json.loads(c.request.body) for c in write.calls]

    @responses.activate
    def test_chip_scope_writes_one_chip(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0xFF, 128, 0]))
        ok, message = pdb.set_group(0, True, board=0, chip="B")
        assert ok is True
        assert self._payloads(write) == [
            {"spi_bus": 0, "board_idx": 0, "chip_letter": "B",
             "operation": "OUT", "states": "11111111"}]
        assert "8 channels on" in message

    @responses.activate
    def test_board_scope_writes_both_chips(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert [(p["board_idx"], p["chip_letter"])
                for p in self._payloads(write)] == [(0, "A"), (0, "B")]
        assert "16 channels on" in message

    @responses.activate
    def test_bus_scope_writes_every_chip(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF, 128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True)
        assert ok is True
        assert [(p["board_idx"], p["chip_letter"])
                for p in self._payloads(write)] == [
            (0, "A"), (0, "B"), (1, "A"), (1, "B")]
        assert "32 channels on" in message
        assert pdb.n_on == 32

    @responses.activate
    def test_off_writes_zero_bytes(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        ok, message = pdb.set_group(0, False, board=0)
        assert ok is True
        assert {p["states"] for p in self._payloads(write)} == {"00000000"}

    @responses.activate
    def test_chips_already_correct_are_not_written(self, pdb):
        """Only chip B needs the write; chip A is already all-on."""
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0xFF]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert [p["chip_letter"] for p in self._payloads(write)] == ["B"]
        assert "8 channels on (16 in scope)" in message

    @responses.activate
    def test_nothing_to_do_skips_writes_entirely(self, pdb):
        # No POST registered: any write attempt would raise.
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert "already on" in message

    @responses.activate
    def test_partial_write_failure_is_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", status=500)
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is False
        assert "1 write(s) failed" in message
        assert "did not take" in message
        # the successful half is still reflected in the grid
        assert pdb.channels[0][1]["channels"] == [True] * 8

    @responses.activate
    def test_verify_read_failure_is_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states", status=500)
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is False
        assert "verify read failed" in message

    @responses.activate
    def test_unknown_bus_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([128, 0]))
        ok, message = pdb.set_group(7, True)
        assert ok is False
        assert "bus not present" in message

    @responses.activate
    def test_unknown_board_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([128, 0]))
        ok, message = pdb.set_group(0, True, board=9)
        assert ok is False
        assert "no such chips" in message

    @responses.activate
    def test_controller_unreachable_reported(self, pdb):
        ok, message = pdb.set_group(0, True)
        assert ok is False
        assert "ConnectionError" in message

    def test_invalid_scope(self, pdb):
        assert pdb.set_group(0, True, board=0, chip="C")[0] is False
        assert pdb.set_group(0, True, board=-1)[0] is False
        # a chip is only addressable within a board
        assert pdb.set_group(0, True, chip="A")[0] is False

    def test_unconfigured(self):
        ok, message = PdbMonitor(host=None, port=None).set_group(0, True)
        assert ok is False
        assert "not configured" in message


UNIT = "choco-test-job.service"


def _props(**overrides) -> str:
    base = {
        "Result": "success",
        "ActiveState": "inactive",
        "SubState": "dead",
        "ExecMainStatus": "0",
        # Only tested for emptiness ("has the unit ever run") — the
        # value itself is never parsed.
        "ExecMainExitTimestamp": "Mon 2026-05-19 16:29:18 UTC",
    }
    base.update(overrides)
    return "\n".join(f"{k}={v}" for k, v in base.items())


def _patch_systemctl(stdout: str, returncode: int = 0):
    completed = MagicMock(returncode=returncode, stdout=stdout, stderr="")
    return patch("choco.services.subprocess.run", return_value=completed)


def _no_systemctl():
    return patch("choco.services.shutil.which", return_value=None)


def _with_systemctl():
    return patch("choco.services.shutil.which", return_value="/usr/bin/systemctl")


def _backdate(path: Path, age_s: float):
    old = time.time() - age_s
    os.utime(path, (old, old))


class TestJobStatusViaSystemctl:
    """systemd's Result is the failure signal; mtime carries staleness."""

    def test_success_and_fresh_state_ok(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{}")
        with _with_systemctl(), _patch_systemctl(_props()):
            out = job_status(UNIT, state_file=p,
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "ok"
        assert out["result"] == "success"
        assert out["systemd"] is True
        assert out["state_mtime"] is not None

    def test_success_without_state_file_ok(self, tmp_path):
        with _with_systemctl(), _patch_systemctl(_props()):
            out = job_status(UNIT, state_file=tmp_path / "missing.json",
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "ok"

    def test_failed_run(self, tmp_path):
        with _with_systemctl(), \
             _patch_systemctl(_props(Result="exit-code", ExecMainStatus="1")):
            out = job_status(UNIT, state_file=tmp_path / "missing.json")
        assert out["health"] == "failed"
        assert out["result"] == "exit-code"

    def test_exit_2_is_degraded_not_failed(self, tmp_path):
        # Shared job convention: exit 2 = the job is fine but a
        # dependency/input wasn't (fpga_master down, stale data).
        with _with_systemctl(), \
             _patch_systemctl(_props(Result="exit-code", ExecMainStatus="2")):
            out = job_status(UNIT, state_file=tmp_path / "missing.json")
        assert out["health"] == "degraded"
        assert out["exit_status"] == "2"

    def test_degraded_beats_stale(self, tmp_path):
        # A degraded run explains the staleness — show the cause.
        p = tmp_path / "state.json"
        p.write_text("{}")
        _backdate(p, 2 * EOP_STALE_AFTER_S)
        with _with_systemctl(), \
             _patch_systemctl(_props(Result="exit-code", ExecMainStatus="2")):
            out = job_status(UNIT, state_file=p,
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "degraded"

    def test_stale_state_beats_systemd_success(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{}")
        _backdate(p, 30 * 3600)  # past the 25h EOP threshold
        with _with_systemctl(), _patch_systemctl(_props()):
            out = job_status(UNIT, state_file=p,
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "stale"

    def test_never_run(self, tmp_path):
        with _with_systemctl(), \
             _patch_systemctl(_props(ExecMainExitTimestamp="")):
            out = job_status(UNIT, state_file=tmp_path / "missing.json")
        assert out["health"] == "never_run"

    def test_no_stale_threshold_old_state_still_ok(self, tmp_path):
        # bffs-style: state file only changes when the flag list changes,
        # so an old mtime must not degrade a succeeding job.
        p = tmp_path / "state.json"
        p.write_text("{}")
        _backdate(p, 90 * 86400)
        with _with_systemctl(), _patch_systemctl(_props()):
            out = job_status(UNIT, state_file=p)
        assert out["health"] == "ok"


class TestJobStatusWithoutSystemctl:
    """When systemctl isn't available, derive from the state file's mtime."""

    def test_missing_state_file(self, tmp_path):
        with _no_systemctl():
            out = job_status(UNIT, state_file=tmp_path / "state.json",
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "never_run"
        assert out["systemd"] is False

    def test_recent_state_file(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{}")
        with _no_systemctl():
            out = job_status(UNIT, state_file=p,
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "ok"

    def test_stale_state_file(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{}")
        _backdate(p, 30 * 3600)
        with _no_systemctl():
            out = job_status(UNIT, state_file=p,
                             stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "stale"

    def test_no_threshold_state_alone_is_inconclusive(self, tmp_path):
        # Without systemd and without a staleness rule there is no
        # success signal — report unknown rather than guessing ok.
        p = tmp_path / "state.json"
        p.write_text("{}")
        with _no_systemctl():
            out = job_status(UNIT, state_file=p)
        assert out["health"] == "unknown"

    def test_unknown_without_state_file(self):
        with _no_systemctl():
            out = job_status(UNIT, state_file=None)
        assert out["health"] == "unknown"


class TestJobLogs:
    def _patch_journalctl(self, stdout: str, returncode: int = 0):
        completed = MagicMock(returncode=returncode, stdout=stdout, stderr="")
        return patch("choco.services.subprocess.run", return_value=completed)

    def test_returns_lines(self):
        from choco.services import job_logs
        with _with_systemctl(), \
             self._patch_journalctl("line one\nline two\n"):
            out = job_logs(UNIT)
        assert out == ["line one", "line two"]

    def test_none_without_journalctl(self):
        from choco.services import job_logs
        with _no_systemctl():
            assert job_logs(UNIT) is None

    def test_none_on_failure(self):
        from choco.services import job_logs
        with _with_systemctl(), self._patch_journalctl("", returncode=1):
            assert job_logs(UNIT) is None


class TestRenderDotSvg:
    DOT = "digraph pipeline {}\n"
    SVG_DOC = ('<?xml version="1.0" encoding="UTF-8"?>\n'
               '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"\n'
               ' "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n'
               '<svg xmlns="http://www.w3.org/2000/svg"><g/></svg>\n')

    def _patch_dot(self, stdout: str, returncode: int = 0):
        completed = MagicMock(returncode=returncode, stdout=stdout, stderr="")
        return patch("choco.services.subprocess.run", return_value=completed)

    def test_renders_and_strips_prolog(self):
        from choco.services import render_dot_svg, _DOT_LAYOUT_ARGS
        with _with_systemctl(), self._patch_dot(self.SVG_DOC) as run:
            out = render_dot_svg(self.DOT)
        assert out is not None and out.startswith("<svg")
        assert "DOCTYPE" not in out
        assert run.call_args.kwargs["input"] == self.DOT
        # kotekan's labels carry `×` and `·`: the pipe is UTF-8 by contract,
        # not by whatever locale the systemd unit happens to inherit.
        assert run.call_args.kwargs["encoding"] == "utf-8"
        # First attempt renders with the layout attributes injected.
        assert run.call_count == 1
        for arg in _DOT_LAYOUT_ARGS:
            assert arg in run.call_args.args[0]

    def test_ortho_preset_leaves_nodesep_to_kotekan(self):
        # Widening nodesep aborts graphviz 2.43 in the ortho maze router on a
        # clustered CHORD graph that renders fine at kotekan's own 0.3.  The
        # retry-without-args path hides it: the operator picks ortho and
        # silently gets curves.
        from choco.services import PIPELINE_LAYOUTS
        assert not any(a.startswith("-Gnodesep")
                       for a in PIPELINE_LAYOUTS["ortho"])

    def test_layout_failure_retries_plain(self):
        from choco.services import render_dot_svg, _DOT_LAYOUT_ARGS
        ortho_fail = MagicMock(returncode=1, stdout="", stderr="ortho bug")
        plain_ok = MagicMock(returncode=0, stdout=self.SVG_DOC, stderr="")
        with _with_systemctl(), \
             patch("choco.services.subprocess.run",
                   side_effect=[ortho_fail, plain_ok]) as run:
            out = render_dot_svg(self.DOT)
        assert out is not None and out.startswith("<svg")
        assert run.call_count == 2
        # The retry drops the layout args.
        for arg in _DOT_LAYOUT_ARGS:
            assert arg not in run.call_args_list[1].args[0]

    def test_none_without_dot_binary(self):
        from choco.services import render_dot_svg
        with _no_systemctl():
            assert render_dot_svg(self.DOT) is None

    def test_none_on_render_failure(self):
        from choco.services import render_dot_svg
        with _with_systemctl(), self._patch_dot("", returncode=1):
            assert render_dot_svg(self.DOT) is None

    def test_none_on_svg_less_output(self):
        from choco.services import render_dot_svg
        with _with_systemctl(), self._patch_dot("not svg at all"):
            assert render_dot_svg(self.DOT) is None

    def test_none_on_timeout(self):
        from choco.services import render_dot_svg
        import subprocess as sp
        with _with_systemctl(), \
             patch("choco.services.subprocess.run",
                   side_effect=sp.TimeoutExpired(["dot"], 10)):
            assert render_dot_svg(self.DOT) is None


TIMER = "choco-eop-broadcast.timer"


def _timer_props(**overrides) -> str:
    base = {
        "LoadState": "loaded",
        "ActiveState": "active",
        "NextElapseUSecRealtime": "Fri 2026-07-17 12:00:00 UTC",
        "LastTriggerUSec": "Thu 2026-07-16 12:00:00 UTC",
    }
    base.update(overrides)
    return "\n".join(f"{k}={v}" for k, v in base.items())


class TestTimerStatus:
    def test_loaded_timer(self):
        with _with_systemctl(), _patch_systemctl(_timer_props()):
            out = timer_status(TIMER)
        assert out["unit"] == TIMER
        assert out["active_state"] == "active"
        assert out["next_elapse"].startswith("Fri 2026-07-17")
        assert out["last_trigger"].startswith("Thu 2026-07-16")

    def test_unknown_timer_is_none(self):
        # systemctl show of a nonexistent unit exits 0 with LoadState=not-found.
        with _with_systemctl(), \
             _patch_systemctl(_timer_props(LoadState="not-found")):
            assert timer_status(TIMER) is None

    def test_no_systemctl_is_none(self):
        with _no_systemctl():
            assert timer_status(TIMER) is None


class TestReadStateJson:
    def test_reads_dict(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text('{"bad_inputs": ["f1"], "updated": 1.0}')
        assert read_state_json(p) == {"bad_inputs": ["f1"], "updated": 1.0}

    def test_missing_file_is_none(self, tmp_path):
        assert read_state_json(tmp_path / "missing.json") is None

    def test_none_path_is_none(self):
        assert read_state_json(None) is None

    def test_invalid_json_is_none(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("{not json")
        assert read_state_json(p) is None

    def test_non_dict_json_is_none(self, tmp_path):
        p = tmp_path / "state.json"
        p.write_text("[1, 2, 3]")
        assert read_state_json(p) is None


class TestSanitizePipelineSvg:
    """Whitelist reconstruction of the graphviz pipeline SVG."""

    SVG = """<svg xmlns="http://www.w3.org/2000/svg"
         xmlns:xlink="http://www.w3.org/1999/xlink"
         width="100pt" height="50pt" viewBox="0 0 100 50">
      <g id="graph0" class="graph" transform="translate(4 46)">
        <title>pipeline</title>
        <polygon fill="white" stroke="none" points="-4,4 96,4"/>
        <g id="node1" class="node">
          <title>n2_buffer</title>
          <polygon fill="none" stroke="black" points="0,0 50,20"/>
          <text text-anchor="middle" x="25" y="10" font-size="14.00">n2_buffer</text>
        </g>
        <g id="node2" class="node">
          <title>other_buf</title>
          <ellipse fill="none" stroke="black" cx="10" cy="10" rx="5" ry="5"/>
        </g>
        <script>alert(1)</script>
        <foreignObject><div>html!</div></foreignObject>
        <g id="edge1" class="edge" onclick="alert(2)">
          <title>a-&gt;b</title>
          <path fill="none" stroke="black" d="M0,0 L10,10"/>
        </g>
        <a xlink:href="javascript:alert(3)"><text x="1" y="1">link</text></a>
      </g>
    </svg>"""

    def test_active_content_cannot_survive(self):
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert out is not None
        # Unlisted elements are unwrapped rather than copied, so none of
        # these reach the DOM -- and neither does anything that would make
        # one do something.
        for banned in ("script", "alert", "foreignObject", "onclick",
                       "xlink", "javascript", "href", "<div", "<a"):
            assert banned not in out, banned
        # The drawing itself survives.
        assert "<polygon" in out and "<path" in out
        assert "n2_buffer" in out

    def test_unlisted_wrappers_are_unwrapped_not_deleted(self):
        # The general rule, and the one that matters: an element nobody
        # listed must not be able to take the drawing down with it.  This is
        # how the graph went blank -- graphviz wraps a node's shape and text
        # in <a> as soon as it has a URL *or* a tooltip, and deleting the
        # subtree left 111 of 223 nodes as empty groups: still in the DOM,
        # still clickable, nothing drawn.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             width="100pt" height="50pt" viewBox="0 0 100 50">
          <g id="graph0" class="graph">
            <someNewGraphvizWrapper enabled="yes">
              <polygon fill="none" stroke="black" points="0,0 50,20"/>
              <text x="25" y="10">still here</text>
            </someNewGraphvizWrapper>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, set(), "cx/cx1")
        assert out is not None
        assert "someNewGraphvizWrapper" not in out and "enabled" not in out
        assert "<polygon" in out and ">still here<" in out

    def test_unwrapping_does_not_carry_over_text(self):
        # Unwrapping keeps child *elements*, never the unlisted element's own
        # text -- that is what keeps a <script> body out of the page.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             width="10pt" height="10pt" viewBox="0 0 10 10">
          <g id="graph0" class="graph">
            <script>var pwned = 1;</script>
            <style>* { display: none }</style>
            <foreignObject><div onclick="x()">html!</div></foreignObject>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, set(), "cx/cx1")
        assert out is not None
        for banned in ("pwned", "display: none", "html!", "onclick",
                       "script", "style", "foreignObject", "<div"):
            assert banned not in out, banned

    def test_link_wrapped_nodes_keep_their_drawing(self):
        # The concrete case of the rule above, in the exact shape graphviz
        # emits it.
        svg = """<svg xmlns="http://www.w3.org/2000/svg"
             xmlns:xlink="http://www.w3.org/1999/xlink"
             width="100pt" height="50pt" viewBox="0 0 100 50">
          <g id="graph0" class="graph">
            <g id="node1" class="node">
              <title>n2_buffer</title>
              <g id="a_node1"><a xlink:href="/buffer_frame?name=n2_buffer"
                                 xlink:title="n2_buffer (ndarray buffer)">
                <polygon fill="none" stroke="black" points="0,0 50,20"/>
                <text x="25" y="10">n2_buffer</text>
              </a></g>
            </g>
          </g>
        </svg>"""
        out = sanitize_pipeline_svg(svg, {"n2_buffer"}, "cx/cx1")
        assert out is not None
        # Shape and label survive; the link and its tooltip do not.
        assert "<polygon" in out
        assert ">n2_buffer<" in out
        assert "href" not in out and "xlink" not in out
        assert "/buffer_frame" not in out
        # ...and the node is still the clickable one.
        assert 'data-plot-buffer="n2_buffer"' in out

    def test_clickable_buffers_are_stamped(self):
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert 'data-plot-buffer="n2_buffer"' in out
        assert 'data-plot-node="cx/cx1"' in out
        assert "clickable-buffer" in out
        # other_buf has no peek_hold: present but not clickable.
        assert 'data-plot-buffer="other_buf"' not in out

    def test_clickable_buffers_are_keyboard_reachable(self):
        # Stamped groups double as buttons: pipeline.js turns Enter/Space
        # on a focused group into the click bufferplot.js listens for.
        out = sanitize_pipeline_svg(self.SVG, {"n2_buffer"}, "cx/cx1")
        assert 'tabindex="0"' in out
        assert 'role="button"' in out
        assert 'aria-label="plot buffer n2_buffer"' in out
        # Non-clickable nodes stay out of the tab order.
        assert out.count('tabindex="0"') == 1

    def test_edge_groups_not_stamped(self):
        out = sanitize_pipeline_svg(self.SVG, {"a->b"}, "cx/cx1")
        assert "data-plot-buffer" not in out
        assert "tabindex" not in out

    def test_unparseable_input_is_none(self):
        assert sanitize_pipeline_svg("<svg", set(), "cx/cx1") is None
        assert sanitize_pipeline_svg("not xml at all", set(), "cx/cx1") is None

    def test_non_svg_root_is_none(self):
        assert sanitize_pipeline_svg("<html>x</html>", set(), "cx/cx1") is None
