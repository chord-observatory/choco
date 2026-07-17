"""Tests for choco/services.py: the FPGA monitor and job-status helpers."""

import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses

from choco.services import (
    FpgaMonitor, PsuMonitor, decode_out_bytes, job_status, timer_status,
    read_state_json, EOP_STALE_AFTER_S,
)


HOST = "fpga.example"
PORT = 54321
BASE = f"http://{HOST}:{PORT}"


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


PSU_BASE = "http://psu.example:5000"


@pytest.fixture
def psu():
    return PsuMonitor(host="psu.example", port=5000, timeout=1)


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


class TestPsuMonitorPollOnce:
    @responses.activate
    def test_healthy_path(self, psu):
        responses.get(f"{PSU_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        # chip 0 (board 0 A): ch0 on; chip 1 (board 0 B): all off
        responses.get(f"{PSU_BASE}/channel_states", json={
            "channel_states": {"0": [0x18, 0x00, 0x18, 0x01]},
        })
        psu.poll_once()
        assert psu.health == "ok"
        assert psu.error is None
        assert psu.boards == {0: 1}
        rows = psu.channels[0]
        assert rows[0] == {"board": 0, "chip": "A",
                           "channels": [True] + [False] * 7}
        assert rows[1]["chip"] == "B"
        assert not any(rows[1]["channels"])
        assert psu.n_channels == 16
        assert psu.n_on == 1
        assert psu.last_seen is not None

    @responses.activate
    def test_status_down(self, psu):
        responses.get(f"{PSU_BASE}/status", status=500)
        psu.poll_once()
        assert psu.health == "down"
        assert psu.error

    @responses.activate
    def test_status_unreachable(self, psu):
        import requests as _requests
        responses.get(f"{PSU_BASE}/status",
                      body=_requests.ConnectionError("refused"))
        psu.poll_once()
        assert psu.health == "down"

    @responses.activate
    def test_status_ok_but_states_missing(self, psu):
        responses.get(f"{PSU_BASE}/status",
                      json={"active_buses": [0], "buses": {}})
        responses.get(f"{PSU_BASE}/channel_states", status=500)
        psu.poll_once()
        assert psu.health == "no_states"
        assert "/channel_states" in psu.error

    def test_unconfigured_monitor_reports_unconfigured(self):
        m = PsuMonitor(host=None, port=None)
        assert m.configured is False
        assert m.health == "unconfigured"
        m.poll_once()
        assert m.health == "unconfigured"

    @responses.activate
    def test_to_dict(self, psu):
        responses.get(f"{PSU_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        responses.get(f"{PSU_BASE}/channel_states", json={
            "channel_states": {"0": [0, 0xFF, 0, 0xFF]},
        })
        psu.poll_once()
        d = psu.to_dict()
        assert d["health"] == "ok"
        assert d["buses"] == [0]
        assert d["n_channels"] == 16
        assert d["n_on"] == 16


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
