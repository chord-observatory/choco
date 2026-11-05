"""``choco.metrics``: the exposition text from the status snapshots and
the jobs' files, and the ``/metrics`` route that serves it."""

import json
from types import SimpleNamespace

from choco.metrics import Gauges, render
from choco.state import Mode, NodeStatus

from tests.helpers import job_dir


def _node(status=NodeStatus.STARTED, started=True, mode=Mode.NORMAL, config_problem=None,
          error=None, version=None, queue_depth=0):
    """A node as ``metrics._nodes`` and ``updatable.node_states`` read it:
    never polled, no updatable block."""
    return SimpleNamespace(name="n", status=status, started=started, mode=mode,
                           maintenance=mode is Mode.MAINTENANCE,
                           config_problem=config_problem, updatable_load_error=None,
                           error=error, version=version, queue_depth=queue_depth,
                           live_read_at=None, live_updatable=None, live_differs=None,
                           updatable_config=None, updatable_base=lambda: {},
                           desired_updatable=lambda: {})


def _write(root, job, name, data):
    d = root / job
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(data))


def _render(tmp_path, services=None, nodes=(), workers=(), jobs=()):
    return render(started_at=1700000000.5, services=services or {}, nodes=nodes, workers=workers,
                  job_dirs={j: tmp_path / j for j in jobs})


class TestGauges:
    def test_help_and_type_once_per_name(self):
        g = Gauges()
        g.add("x_total", "X.", 1, kind="counter", a="1")
        g.add("y", "Y.", 2.5)
        g.add("x_total", "X.", 3, a="2")
        assert g.render() == ("# HELP x_total X.\n# TYPE x_total counter\n"
                              'x_total{a="1"} 1\nx_total{a="2"} 3\n'
                              "# HELP y Y.\n# TYPE y gauge\ny 2.5\n")

    def test_none_and_non_numbers_are_left_out(self):
        g = Gauges()
        g.add("a", "A.", None)
        g.add("b", "B.", "7")
        g.add("c", "C.", True)
        assert g.render() == "# HELP c C.\n# TYPE c gauge\nc 1\n"

    def test_label_values_are_escaped(self):
        g = Gauges()
        g.add("a", "A.", 1, k='q"\\\n')
        assert 'a{k="q\\"\\\\\\n"} 1' in g.render()


class TestRender:
    def test_services_one_hot_in_each_vocabulary(self, tmp_path):
        services = {"fpga": {"health": "no_timing"}, "archive": {"health": "down"},
                    "instrument": None, "bffs": {"health": "degraded"}}
        body = _render(tmp_path, services)
        assert 'choco_service_state{service="fpga",state="no_timing"} 1' in body
        assert 'choco_service_state{service="fpga",state="ok"} 0' in body
        assert 'choco_service_state{service="archive",state="down"} 1' in body
        assert 'choco_service_state{service="archive",state="unconfigured"} 0' in body
        assert 'choco_service_state{service="instrument",state="unknown"} 1' in body
        assert 'choco_service_state{service="instrument",state="never_run"}' not in body
        assert 'choco_service_state{service="bffs",state="degraded"} 1' in body
        assert "choco_start_time_seconds 1700000000.5" in body

    def test_monitors(self, tmp_path):
        services = {
            "fpga": {"health": "ok", "last_polled": 10.0, "last_seen": 9.0},
            "pdb": {"health": "ok", "last_polled": 11.0, "last_seen": None, "n_channels": 160, "n_on": 148},
            "data": {"health": "ok", "last_polled": 12.0, "last_seen": 12.0, "n_roots": 3, "n_ok": 2},
            "instrument": {"health": "degraded", "last_polled": 13.0, "last_seen": 13.0, "n_problems": 4},
            "archive": {"health": "ok", "last_polled": 14.0, "checked_at": 13.5, "dirs": 77},
            "eop": {"health": "ok", "running": False, "state_mtime": 5.0},
        }
        body = _render(tmp_path, services)
        assert 'choco_monitor_last_polled_timestamp_seconds{monitor="fpga"} 10.0' in body
        assert 'choco_monitor_last_seen_timestamp_seconds{monitor="fpga"} 9.0' in body
        assert 'choco_monitor_last_seen_timestamp_seconds{monitor="pdb"}' not in body
        assert 'choco_monitor_last_polled_timestamp_seconds{monitor="eop"}' not in body
        assert "choco_pdb_channels 160\n" in body and "choco_pdb_channels_on 148" in body
        assert "choco_data_roots 3\n" in body and "choco_data_roots_ok 2" in body
        assert "choco_instrument_problems 4" in body
        assert "choco_archive_acquisitions 77" in body
        assert "choco_archive_listing_timestamp_seconds 13.5" in body

    def test_nodes_and_workers(self, tmp_path):
        nodes = [_node(version="a+1", queue_depth=2),
                 _node(NodeStatus.DOWN, mode=Mode.MAINTENANCE, error="timeout", version="a+1"),
                 _node(NodeStatus.IDLE, started=False, config_problem="render failed", version="b+2"),
                 _node(NodeStatus.UNKNOWN, queue_depth=1, mode=Mode.DEVELOPMENT)]
        workers = [{"phase": "idle", "consecutive_failures": 0},
                   {"phase": "down", "consecutive_failures": 3},
                   {"phase": "pushing", "consecutive_failures": 0}]
        body = _render(tmp_path, nodes=nodes, workers=workers)
        assert 'choco_nodes{status="started"} 1' in body
        assert 'choco_nodes{status="down"} 1' in body
        assert 'choco_nodes{status="idle"} 1' in body
        assert 'choco_nodes{status="syncing"} 0' in body
        assert "choco_nodes_total 4" in body
        # The started node's config was never read, the idle one's file
        # does not render, the development one runs its own, the down one
        # runs nothing; none has an updatable block.
        assert 'choco_nodes_config{state="unread"} 1' in body
        assert 'choco_nodes_config{state="bad_file"} 1' in body
        assert 'choco_nodes_config{state="own"} 1' in body
        assert 'choco_nodes_config{state="none"} 1' in body
        assert 'choco_nodes_config{state="ok"} 0' in body
        assert 'choco_nodes_updatable{state="none"} 4' in body
        assert 'choco_nodes_updatable{state="differs"} 0' in body
        assert "choco_nodes_started_desired 3" in body
        assert "choco_nodes_maintenance 1" in body
        assert "choco_nodes_development 1" in body
        assert "choco_nodes_config_error 1" in body
        assert "choco_nodes_error 1" in body
        assert "choco_kotekan_versions 2" in body
        assert "choco_queued_changes 3" in body
        assert 'choco_workers{phase="idle"} 1' in body
        assert 'choco_workers{phase="down"} 1' in body
        assert 'choco_workers{phase="pushing"} 1' in body
        assert 'choco_workers{phase="queued-for-push"} 0' in body
        assert "choco_workers_failing 1" in body

    def test_job_status_and_run_files(self, tmp_path):
        services = {"bffs": {"health": "ok", "running": True, "state_mtime": 100.5},
                    "waterfall": {"health": "ok", "running": False, "state_mtime": None}}
        _write(tmp_path, "bffs", "run.json", {
            "time": 1000.0, "degraded": ["rfi: cx1 down"], "n_elements": 128, "n_bad": 16,
            "kotekan_file_age_s": 146.8,
            "sources": [{"kind": "manual", "n_measured": 128, "n_flagged": 12},
                        {"kind": "power-outlier", "n_measured": 100, "n_flagged": 0},
                        {"kind": "", "n_flagged": 9}, "junk"]})
        _write(tmp_path, "waterfall", "state.json", {
            "backlog": 34, "files_rendered": 1, "errors": ["x"], "run_seconds": 12.5, "degraded": []})
        body = _render(tmp_path, services, jobs=["bffs", "waterfall", "skymap"])
        assert 'choco_job_running{job="bffs"} 1' in body
        assert 'choco_job_running{job="waterfall"} 0' in body
        assert 'choco_job_running{job="skymap"}' not in body
        assert 'choco_job_state_timestamp_seconds{job="bffs"} 100.5' in body
        assert 'choco_job_state_timestamp_seconds{job="waterfall"}' not in body
        assert 'choco_job_run_timestamp_seconds{job="bffs"} 1000.0' in body
        assert 'choco_job_run_timestamp_seconds{job="waterfall"}' not in body
        assert 'choco_job_run_seconds{job="waterfall"} 12.5' in body
        assert 'choco_job_degraded_reasons{job="bffs"} 1' in body
        assert 'choco_job_degraded_reasons{job="waterfall"} 0' in body
        assert "choco_bffs_inputs 128" in body and "choco_bffs_bad_inputs 16" in body
        assert "choco_bffs_file_timestamp_seconds 853.2" in body
        assert 'choco_bffs_source_measured{source="manual"} 128' in body
        assert 'choco_bffs_source_flagged{source="power-outlier"} 0' in body
        assert 'source=""' not in body
        assert "choco_waterfall_backlog 34" in body
        assert "choco_waterfall_files_rendered 1" in body
        assert "choco_waterfall_errors 1" in body

    def test_the_other_jobs(self, tmp_path):
        _write(tmp_path, "eop", "state.json", {"earth_orientation_parameter_table": [
            {"t_inst_ns": 1793491200000000000}, {"t_inst_ns": 1793577600000000000}, "junk"]})
        _write(tmp_path, "eigencal", "state.json", {
            "updated": 20.0, "transit_time": 10.0, "good_frac": 0.71, "n_calibrated": 128})
        _write(tmp_path, "eigencal", "run.json", {"time": 30.0, "next_transit": 40.0, "status": "skipped"})
        _write(tmp_path, "mapmaker", "state.json", {
            "backlog": 2, "files_imaged": 3, "files_uncalibrated": 1, "errors": [], "run_seconds": 0.5,
            "gains": {"solved_at": 50.0}, "degraded": ["a", "b"]})
        _write(tmp_path, "mapmaker", "clean.json", {
            "updated": 60.0, "run_seconds": 159.3, "n_components": 8858, "sigma_after": 1.5e-05})
        _write(tmp_path, "pointing", "state.json", {"time": 70.0, "array_value": -27.3, "outliers": ["E1"]})
        _write(tmp_path, "pointing", "run.json", {"time": 80.0, "changed": False})
        _write(tmp_path, "acqnotes", "run.json", {
            "time": 90.0, "acquisitions": 76, "backlog": 0, "measured": [], "written": ["a", "b"]})
        body = _render(tmp_path, jobs=["eop", "eigencal", "mapmaker", "pointing", "acqnotes"])
        assert "choco_eop_table_entries 3" in body
        assert "choco_eop_table_end_timestamp_seconds 1793577600.0" in body
        assert "choco_eigencal_good_frac 0.71" in body
        assert "choco_eigencal_calibrated_inputs 128" in body
        assert "choco_eigencal_transit_timestamp_seconds 10.0" in body
        assert "choco_eigencal_solution_timestamp_seconds 20.0" in body
        assert "choco_eigencal_next_transit_timestamp_seconds 40.0" in body
        assert 'choco_job_run_timestamp_seconds{job="eigencal"} 30.0' in body
        assert "choco_mapmaker_backlog 2" in body
        assert "choco_mapmaker_files_imaged 3" in body
        assert "choco_mapmaker_files_uncalibrated 1" in body
        assert "choco_mapmaker_errors 0" in body
        assert "choco_mapmaker_gains_timestamp_seconds 50.0" in body
        assert 'choco_job_run_seconds{job="mapmaker"} 0.5' in body
        assert 'choco_job_degraded_reasons{job="mapmaker"} 2' in body
        assert "choco_mapmaker_clean_timestamp_seconds 60.0" in body
        assert "choco_mapmaker_clean_run_seconds 159.3" in body
        assert "choco_mapmaker_clean_components 8858" in body
        assert "choco_mapmaker_clean_residual_sigma 1.5e-05" in body
        assert "choco_pointing_coelevation_deg -27.3" in body
        assert "choco_pointing_outliers 1" in body
        assert "choco_pointing_table_timestamp_seconds 70.0" in body
        assert 'choco_job_run_timestamp_seconds{job="pointing"} 80.0' in body
        assert "choco_acqnotes_acquisitions 76" in body
        assert "choco_acqnotes_backlog 0" in body
        assert "choco_acqnotes_measured 0" in body
        assert "choco_acqnotes_written 2" in body

    def test_missing_or_malformed_files_add_nothing(self, tmp_path):
        _write(tmp_path, "mapmaker", "state.json", {"backlog": "many", "gains": "no"})
        (tmp_path / "pointing").mkdir()
        (tmp_path / "pointing" / "state.json").write_text("{not json")
        body = _render(tmp_path, jobs=["mapmaker", "pointing", "acqnotes"])
        assert "choco_mapmaker_backlog" not in body
        assert "choco_pointing" not in body
        assert "choco_acqnotes" not in body
        assert "choco_job_run_timestamp_seconds" not in body

    def test_bffs_state_file_is_never_read(self, tmp_path, monkeypatch):
        """bffs's state.json is the whole flag history, megabytes a
        scrape must not parse; its run.json has the numbers."""
        seen = []
        monkeypatch.setattr("choco.metrics.read_state_json", lambda p: seen.append(p) or None)
        _render(tmp_path, jobs=["bffs", "skymap"])
        assert seen == [tmp_path / "bffs" / "run.json"]

    def test_process(self, tmp_path):
        body = _render(tmp_path)
        assert "# TYPE process_cpu_seconds_total counter" in body
        assert "process_resident_memory_bytes " in body
        assert "process_open_fds " in body


class TestRoute:
    def test_the_route_serves_the_snapshots_and_the_files(self, client, app, jobs):
        jobs(running=False, state_mtime=100.0)
        (job_dir(app, "bffs") / "run.json").write_text(json.dumps({"time": 5.0, "n_bad": 16}))
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.mimetype == "text/plain"
        body = resp.data.decode()
        assert "choco_up 1" in body
        assert "choco_nodes_total 3" in body
        assert "choco_nodes_maintenance 3" in body          # a fresh registry
        assert 'choco_service_state{service="archive",state="unconfigured"} 1' in body
        assert 'choco_service_state{service="pdb",state="unconfigured"} 1' in body
        assert 'choco_job_running{job="bffs"} 0' in body
        assert 'choco_job_state_timestamp_seconds{job="eop"} 100.0' in body
        assert 'choco_job_run_timestamp_seconds{job="bffs"} 5.0' in body
        assert "choco_bffs_bad_inputs 16" in body
        assert 'choco_workers{phase="idle"} 0' in body       # no worker runs under the test client
        assert "process_resident_memory_bytes" in body
