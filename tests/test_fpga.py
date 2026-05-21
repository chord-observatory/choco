"""Tests for the FPGA monitor and EOP-status helpers."""

import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses

from choco.fpga import FpgaMonitor, eop_status


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


class TestEopStatusViaSystemctl:
    """When systemctl is reachable, use its output verbatim."""

    def _props(self, **overrides) -> str:
        base = {
            "Result": "success",
            "ActiveState": "inactive",
            "SubState": "dead",
            "ExecMainStatus": "0",
            # systemctl emits human-readable timestamps; the parser
            # accepts a few common formats.
            "ExecMainExitTimestamp": "Mon 2026-05-19 16:29:18 UTC",
        }
        base.update(overrides)
        return "\n".join(f"{k}={v}" for k, v in base.items())

    def _patch_systemctl(self, stdout: str, returncode: int = 0):
        completed = MagicMock(returncode=returncode, stdout=stdout, stderr="")
        return patch("choco.fpga.subprocess.run", return_value=completed)

    def test_healthy_recent_run(self, tmp_path):
        recent = time.strftime("%a %Y-%m-%d %H:%M:%S UTC",
                               time.gmtime(time.time() - 60))
        with patch("choco.fpga.shutil.which", return_value="/usr/bin/systemctl"), \
             self._patch_systemctl(self._props(ExecMainExitTimestamp=recent)):
            out = eop_status(state_file=tmp_path / "missing.json")
        assert out["health"] == "ok"
        assert out["source"] == "systemd"
        assert out["result"] == "success"
        assert out["last_run_at"] is not None

    def test_failed_run(self, tmp_path):
        with patch("choco.fpga.shutil.which", return_value="/usr/bin/systemctl"), \
             self._patch_systemctl(self._props(Result="exit-code",
                                               ExecMainStatus="1")):
            out = eop_status(state_file=tmp_path / "missing.json")
        assert out["health"] == "failed"
        assert out["result"] == "exit-code"

    def test_stale_run(self, tmp_path):
        old = time.strftime("%a %Y-%m-%d %H:%M:%S UTC",
                            time.gmtime(time.time() - 48 * 3600))
        with patch("choco.fpga.shutil.which", return_value="/usr/bin/systemctl"), \
             self._patch_systemctl(self._props(ExecMainExitTimestamp=old)):
            out = eop_status(state_file=tmp_path / "missing.json")
        assert out["health"] == "stale"


class TestEopStatusFallbackToMtime:
    """When systemctl isn't available, derive from the state file's mtime."""

    def test_missing_state_file(self, tmp_path):
        with patch("choco.fpga.shutil.which", return_value=None):
            out = eop_status(state_file=tmp_path / "eop-state.json")
        assert out["health"] == "never_run"
        assert out["source"] == "mtime"

    def test_recent_state_file(self, tmp_path):
        p = tmp_path / "eop-state.json"
        p.write_text("{}")
        with patch("choco.fpga.shutil.which", return_value=None):
            out = eop_status(state_file=p)
        assert out["health"] == "ok"
        assert out["source"] == "mtime"

    def test_stale_state_file(self, tmp_path):
        p = tmp_path / "eop-state.json"
        p.write_text("{}")
        # Backdate to 30h ago — well past the 25h stale threshold.
        old = time.time() - 30 * 3600
        import os
        os.utime(p, (old, old))
        with patch("choco.fpga.shutil.which", return_value=None):
            out = eop_status(state_file=p)
        assert out["health"] == "stale"

    def test_unknown_without_state_file(self):
        with patch("choco.fpga.shutil.which", return_value=None):
            out = eop_status(state_file=None)
        assert out["health"] == "unknown"
