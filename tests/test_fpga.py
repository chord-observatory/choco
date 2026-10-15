"""choco.fpga: the fpga_master monitor and the gain archives."""

import os
import time
from unittest.mock import patch

import pytest
import responses

from choco.fpga import FpgaMonitor


HOST = "fpga.example"


PORT = 54321


BASE = f"http://{HOST}:{PORT}"


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    """Keep the retry's real-time sleep out of the test suite."""
    monkeypatch.setattr("choco.http.RETRY_DELAY_S", 0)


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

    def test_bare_oserror_is_down_not_an_escape(self, monitor):
        """gevent can fail a connect with the builtin ConnectionError,
        which requests never wraps; it must read as down like any other."""
        with patch("choco.http.requests.request",
                   side_effect=ConnectionResetError("reset by peer")):
            monitor.poll_once()
        assert monitor.health == "down" and "ConnectionResetError" in monitor.error

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


class TestFileArchive:
    """eigencal's newest archive read through the h5read subprocess and
    cached on the file's identity and mtime."""

    @staticmethod
    def _archive(path, n=3):
        import h5py
        import numpy as np
        with h5py.File(path, "w") as f:
            f.attrs["source"] = "CYG_A"
            f.attrs["good_frac"] = 0.5
            g = f.create_dataset("gain", data=(np.arange(n * 2, dtype=np.float32)
                                               .reshape(n, 2) * (1 + 1j)).astype(np.complex64))
            g.attrs["axis"] = ["freq", "input"]
            f.create_dataset("dyn_rng", data=np.ones((n, 1), dtype=np.float32))
            f.create_dataset("index_map/freq", data=np.arange(n, dtype=np.float64))

    def test_newest_file(self, tmp_path):
        from choco.fpga import newest_file
        assert newest_file(tmp_path / "nope", "gain_*.h5") is None
        assert newest_file(tmp_path, "gain_*.h5") is None
        a = tmp_path / "gain_a.h5"; a.write_bytes(b"a")
        b = tmp_path / "gain_b.h5"; b.write_bytes(b"b")
        import os
        os.utime(a, (1, 1))
        assert newest_file(tmp_path, "gain_*.h5") == b

    def test_manifest_datasets_and_bytes(self, tmp_path):
        from choco.fpga import FileArchive
        path = tmp_path / "gain_x.h5"
        self._archive(path)
        archive = FileArchive(lambda: path)
        assert archive.refresh() is True
        names = {d["name"]: d for d in archive.to_dict()["datasets"]}
        assert names["gain"]["value_type"] == "complex64"     # imaginary part present
        assert names["gain"]["extents"] == [3, 2] and names["gain"]["dimnames"] == ["freq", "input"]
        assert "dyn_rng" in names
        raw = archive.dataset("gain")
        assert raw is not None and len(raw) == 3 * 2 * 8
        assert archive.dataset("nope") is None
        assert archive.to_dict()["attrs"]["source"] in ("CYG_A", ["CYG_A"])
        assert archive.file_bytes() == path.read_bytes()
        assert path.exists()        # never unlinked by the reader

    def test_refresh_follows_the_file(self, tmp_path, monkeypatch):
        from choco import fpga as services
        from choco.fpga import FileArchive
        path = tmp_path / "gain_x.h5"
        self._archive(path, n=3)
        archive = FileArchive(lambda: path)
        assert archive.refresh()
        calls = []
        real = services._h5read
        monkeypatch.setattr(services, "_h5read", lambda *a: (calls.append(a), real(*a))[1])
        assert archive.refresh() and calls == []           # same file, same mtime: cached
        self._archive(path, n=5)
        os.utime(path, None)
        assert archive.refresh() and len(calls) == 1       # re-read on mtime change
        assert {d["name"]: d for d in archive.to_dict()["datasets"]}["gain"]["extents"] == [5, 2]

    def test_missing_archive_is_an_error_not_an_exception(self, tmp_path):
        from choco.fpga import FileArchive
        archive = FileArchive(lambda: None)
        assert archive.refresh() is False
        assert "no gain solution archived yet" in archive.error
        assert archive.dataset("gain") is None and archive.to_dict()["datasets"] == []
