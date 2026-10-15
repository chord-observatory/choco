"""choco.jobstatus: a job's health, timer, journal and state file."""

import os
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from choco.jobstatus import EOP_STALE_AFTER_S, job_status, read_state_json, timer_status


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
    return patch("choco.hosttool.subprocess.run", return_value=completed)


def _no_systemctl():
    return patch("choco.hosttool.shutil.which", return_value=None)


def _with_systemctl():
    return patch("choco.hosttool.shutil.which", return_value="/usr/bin/systemctl")


def _backdate(path: Path, age_s: float):
    old = time.time() - age_s
    os.utime(path, (old, old))


class TestJobStatusViaSystemctl:
    """systemd's Result is the failure signal; mtime carries staleness."""

    @pytest.mark.parametrize("props,state_age_s,stale_after,expect", [
        ({}, 0, EOP_STALE_AFTER_S,
         {"health": "ok", "result": "success", "systemd": True}),
        ({}, None, EOP_STALE_AFTER_S, {"health": "ok"}),       # no state file yet
        ({"Result": "exit-code", "ExecMainStatus": "1"}, None, None,
         {"health": "failed", "result": "exit-code"}),
        # exit 2: the job is fine but a dependency or input was not
        ({"Result": "exit-code", "ExecMainStatus": "2"}, None, None,
         {"health": "degraded", "exit_status": "2"}),
        # a degraded run explains the staleness: show the cause
        ({"Result": "exit-code", "ExecMainStatus": "2"}, 2 * EOP_STALE_AFTER_S,
         EOP_STALE_AFTER_S, {"health": "degraded"}),
        ({}, 30 * 3600, EOP_STALE_AFTER_S, {"health": "stale"}),   # past 25 h
        ({"ExecMainExitTimestamp": ""}, None, None, {"health": "never_run"}),
        # bffs-style: the state file changes only with the flag list, so
        # with no threshold an old mtime never degrades a succeeding job
        ({}, 90 * 86400, None, {"health": "ok"}),
    ])
    def test_health(self, tmp_path, props, state_age_s, stale_after, expect):
        p = tmp_path / "state.json"
        if state_age_s is not None:
            p.write_text("{}")
            _backdate(p, state_age_s)
        with _with_systemctl(), _patch_systemctl(_props(**props)):
            out = job_status(UNIT, state_file=p, stale_after_s=stale_after)
        assert {k: out[k] for k in expect} == expect
        assert (out["state_mtime"] is not None) == (state_age_s is not None)

    # systemd resets Result / ExecMainStatus and blanks ExecMainExitTimestamp
    # while a oneshot runs, so the in-flight snapshot alone reads like a
    # never-run unit; a 20 s job on a 30 s timer would be grey most of the
    # time.  Verified against a real unit: a run that had just failed reads
    # Result=success, ExecMainStatus=0 during its next run.
    RUNNING = dict(ActiveState="activating", SubState="start",
                   ExecMainExitTimestamp="")

    def test_run_in_progress_keeps_the_last_completed_health(self):
        import choco.jobstatus as services_mod
        services_mod._LAST_COMPLETED.clear()
        with _with_systemctl(), _patch_systemctl(_props()):
            assert job_status(UNIT)["health"] == "ok"
        with _with_systemctl(), _patch_systemctl(_props(**self.RUNNING)):
            out = job_status(UNIT)
        assert out["health"] == "ok" and out["running"] is True
        assert out["active_state"] == "activating"      # the live fact stays

        with _with_systemctl(), _patch_systemctl(
                _props(Result="exit-code", ExecMainStatus="1", ActiveState="failed")):
            assert job_status(UNIT)["health"] == "failed"
        with _with_systemctl(), _patch_systemctl(_props(**self.RUNNING)):
            out = job_status(UNIT)
        assert out["health"] == "failed" and out["running"] is True
        assert out["result"] == "exit-code" and out["exit_status"] == "1"

    def test_run_in_progress_without_memory_is_unknown(self, tmp_path):
        import choco.jobstatus as services_mod
        services_mod._LAST_COMPLETED.clear()
        with _with_systemctl(), _patch_systemctl(_props(**self.RUNNING)):
            out = job_status(UNIT, state_file=tmp_path / "missing.json")
        assert out["health"] == "unknown" and out["running"] is True

    def test_run_in_progress_still_reports_staleness(self, tmp_path):
        import choco.jobstatus as services_mod
        services_mod._LAST_COMPLETED.clear()
        p = tmp_path / "state.json"
        p.write_text("{}")
        with _with_systemctl(), _patch_systemctl(_props()):
            assert job_status(UNIT, state_file=p,
                              stale_after_s=EOP_STALE_AFTER_S)["health"] == "ok"
        _backdate(p, 30 * 3600)
        with _with_systemctl(), _patch_systemctl(_props(**self.RUNNING)):
            out = job_status(UNIT, state_file=p, stale_after_s=EOP_STALE_AFTER_S)
        assert out["health"] == "stale" and out["running"] is True

    def test_completion_refreshes_the_memory(self):
        import choco.jobstatus as services_mod
        services_mod._LAST_COMPLETED.clear()
        with _with_systemctl(), _patch_systemctl(
                _props(Result="exit-code", ExecMainStatus="2", ActiveState="failed")):
            assert job_status(UNIT)["health"] == "degraded"
        with _with_systemctl(), _patch_systemctl(_props()):
            assert job_status(UNIT)["health"] == "ok"
        with _with_systemctl(), _patch_systemctl(_props(**self.RUNNING)):
            assert job_status(UNIT)["health"] == "ok"


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
        return patch("choco.hosttool.subprocess.run", return_value=completed)

    def test_returns_lines(self):
        from choco.jobstatus import job_logs
        with _with_systemctl(), \
             self._patch_journalctl("line one\nline two\n"):
            out = job_logs(UNIT)
        assert out == ["line one", "line two"]

    def test_none_without_journalctl(self):
        from choco.jobstatus import job_logs
        with _no_systemctl():
            assert job_logs(UNIT) is None

    def test_none_on_failure(self):
        from choco.jobstatus import job_logs
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
