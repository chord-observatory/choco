"""The /service/<name> pages and the job files behind them: logs, the job
pages, eigencal, the bffs run file and manual flags."""

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from tests.helpers import JOB_STUB as _JOB_STUB, csrf, job_dir, login


def _install_state(app, job, src, name="state.json") -> Path:
    """Put the file at *src* where choco reads *job*'s *name* from: the
    state root is the only setting; the job's file names are convention."""
    dest = job_dir(app, job) / name
    dest.write_bytes(Path(src).read_bytes())
    return dest


# --- Service logs partial ---

class TestServiceLogs:
    def test_unknown_name_is_404(self, client):
        login(client)
        resp = client.get("/partials/service-logs/not-a-service")
        assert resp.status_code == 404

    def test_renders_journal_lines(self, client):
        login(client)
        with patch("choco.web.job_logs",
                   return_value=["alpha entry", "beta entry"]) as jl:
            resp = client.get("/partials/service-logs/eop")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "alpha entry" in body
        assert "beta entry" in body
        jl.assert_called_once_with("choco-eop-broadcast.service", lines=100)

    def test_lines_query_param_clamped(self, client):
        login(client)
        with patch("choco.web.job_logs", return_value=[]) as jl:
            client.get("/partials/service-logs/eop?lines=500")
            client.get("/partials/service-logs/eop?lines=999999")
            client.get("/partials/service-logs/eop?lines=bogus")
        assert [c.kwargs["lines"] for c in jl.call_args_list] == [500, 1000, 100]

    def test_journal_unavailable_message(self, client):
        login(client)
        with patch("choco.web.job_logs", return_value=None):
            resp = client.get("/partials/service-logs/bffs")
        assert resp.status_code == 200
        assert b"Journal unavailable" in resp.data

    def test_choco_unit_viewable(self, client):
        login(client)
        with patch("choco.web.job_logs", return_value=["choco line"]) as jl:
            resp = client.get("/partials/service-logs/choco")
        assert resp.status_code == 200
        jl.assert_called_once_with("choco.service", lines=100)


# --- /service/<name> pages ---

class TestServicePage:
    def test_unknown_name_is_404(self, client):
        login(client)
        resp = client.get("/service/not-a-service")
        assert resp.status_code == 404

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal",
                                  "skymap", "mapmaker"])
    def test_job_pages_render(self, client, name, jobs):
        login(client)
        resp = client.get(f"/service/{name}")
        assert resp.status_code == 200
        assert name.upper() in resp.data.decode()

    def test_waterfall_page_is_the_files_page(self, client):
        # The renderer is folded into DATA: its old page redirects to
        # /files, keeping the journal picker's ``?lines=``.
        login(client)
        resp = client.get("/service/waterfall", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/files")
        resp = client.get("/service/waterfall?lines=500", follow_redirects=False)
        assert resp.headers["Location"].endswith("/files?lines=500")

    def test_notes_page_is_the_files_page(self, client):
        # The acquisition notes are folded into DATA too, at their section.
        login(client)
        resp = client.get("/service/acqnotes", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/files")

    def test_files_page_carries_the_notes(self, client, app):
        login(client)
        d = job_dir(app, "acqnotes")
        (d / "index.json").write_text(json.dumps({"acq_20261012_005242_957492261": {}}))
        (d / "run.json").write_text(json.dumps({
            "time": 1791862291.0, "status": "ok", "exit_code": 0, "acquisitions": 48,
            "measured": ["acq_20261012_005242_957492261"], "backlog": 45, "written": []}))
        stub = dict(_JOB_STUB, unit="choco-acqnotes.service")
        with patch("choco.web.job_status", return_value=stub), \
             patch("choco.web.timer_status", return_value=None):
            body = client.get("/files").data.decode()
        assert "Acquisition notes" in body
        assert "choco-acqnotes.service" in body
        assert "/partials/service-status/acqnotes" in body
        assert "/partials/service-logs/acqnotes" in body
        assert 'id="notes-journal"' in body and 'id="journal"' in body
        assert "1 of 48 acquisitions measured, 45 waiting" in body
        assert "no note changed" in body
        assert "/service/acqnotes" not in body

    def test_files_page_carries_the_renderer(self, client):
        login(client)
        stub = dict(_JOB_STUB, unit="choco-waterfall.service")
        with patch("choco.web.job_status", return_value=stub), \
             patch("choco.web.timer_status", return_value=None):
            body = client.get("/files?lines=500").data.decode()
        assert "Waterfall renderer" in body
        assert "choco-waterfall.service" in body
        # the status block and the journal poll the renderer's partials,
        # and the lines picker reloads this page, not /service/waterfall
        assert "/partials/service-status/waterfall" in body
        assert "/partials/service-logs/waterfall?lines=500" in body
        assert 'href="/files?lines=100#waterfall"' in body
        assert "/service/waterfall" not in body

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal",
                                  "waterfall", "skymap", "mapmaker", "acqnotes"])
    def test_status_partial_renders(self, client, name, jobs):
        login(client)
        resp = client.get(f"/partials/service-status/{name}")
        assert resp.status_code == 200
        assert "Unit" in resp.data.decode()

    def test_status_partial_unknown_404(self, client):
        login(client)
        resp = client.get("/partials/service-status/nope")
        assert resp.status_code == 404

    def test_fpga_page_renders(self, client):
        login(client)
        resp = client.get("/service/fpga")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "FPGA" in body
        assert "not configured" in body

    def test_pdb_page_renders(self, client):
        login(client)
        resp = client.get("/service/pdb")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "PDB" in body
        assert "not configured" in body

    def test_pdb_page_channel_grid(self, client, app):
        login(client)
        monitor = app.config["pdb_monitor"]
        monitor.boards = {0: 1}
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
        ]}
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert "SPI bus 0" in body
        assert "board 0" in body
        # No channel map in the test configs dir, so every cell is
        # "unmapped" and falls back to the plain on/off wording.
        assert body.count('class="chan on unmapped"') == 1
        assert body.count('class="chan off unmapped"') == 15

    def test_timer_facts_shown(self, client):
        login(client)
        timer = {"unit": "choco-eop-broadcast.timer", "active_state": "active",
                 "next_elapse": "Fri 2026-07-17 12:00:00 UTC",
                 "last_trigger": "Thu 2026-07-16 12:00:00 UTC"}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=timer) as ts:
            resp = client.get("/service/eop")
        body = resp.data.decode()
        assert "Fri 2026-07-17 12:00:00 UTC" in body
        assert "Thu 2026-07-16 12:00:00 UTC" in body
        ts.assert_called_once_with("choco-eop-broadcast.timer")

    def test_bffs_detail_from_state_file(self, client, app, tmp_path, jobs):
        login(client)
        state = {
            "updated": 1700000077.7,
            "update_id": "bffs-1700000077750",
            "bad_inputs": ["f1", "f9"],
            "history": [
                {"time": 1700000000.0, "update_id": "bffs-x",
                 "became_bad": ["f1"], "became_good": [],
                 "bad_inputs": ["f1"]},
                {"time": 1700000077.4, "update_id": "bffs-1700000077750",
                 "became_bad": ["f9"], "became_good": [],
                 "bad_inputs": ["f1", "f9"]},
            ],
        }
        state_file = tmp_path / "bffs-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "bffs", state_file)
        resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "f1, f9" in body
        assert "bffs-1700000077750" in body
        assert "Recent transitions" in body

    def test_bffs_detail_shows_flag_attribution(self, client, app, tmp_path, jobs):
        login(client)
        state = {
            "updated": 1700000077.7,
            "bad_inputs": ["A1X", "A3X"],
            "flagged_by": {"A1X": ["power-outlier", "manual"],
                           "A3X": ["rfi"]},
        }
        state_file = tmp_path / "bffs-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "bffs", state_file)
        resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "power-outlier, manual" in body
        assert "rfi" in body

    def test_bffs_detail_shows_the_reason_a_source_gave(self, client, app, tmp_path, jobs):
        login(client)
        state = {
            "updated": 1700000077.7,
            "bad_inputs": ["E01X", "A1X", "B2Y"],
            "flagged_by": {"E01X": ["power"], "A1X": ["power", "manual"], "B2Y": ["rfi"]},
            "flag_reasons": {"E01X": {"power": "not in PDB table"},
                             "A1X": {"power": "off"}},
        }
        state_file = tmp_path / "bffs-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "bffs", state_file)
        resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "power: not in PDB table" in body
        assert "power: off, manual" in body
        assert "(rfi)" in body                       # no reason given: the kind alone

    def test_bffs_detail_renders_the_element_grid(self, client, app, tmp_path, jobs):
        login(client)
        labels = [f"d{i}X" for i in range(10)] + [f"d{i}Y" for i in range(10)]
        state = {
            "updated": 1700000077.7,
            "bad_inputs": ["d0Y", "d3X"],
            "flagged_by": {"d3X": ["power-outlier"], "d0Y": ["manual"]},
            "labels": labels,
        }
        state_file = tmp_path / "bffs-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "bffs", state_file)
        app.config["bffs_cfg"] = {"control": False}      # the plain grid; toggles are tested below
        resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "2 of 20 elements" in body
        assert body.count('class="feed-bad"') == 2
        assert body.count('class="feed-good"') == 18
        assert 'title="element 3: bad (power-outlier)"' in body
        assert 'title="element 10: bad (manual)"' in body
        assert "d1, d9" not in body            # the list gave way to the grid

    def test_element_grid_runs_down_columns_of_eight(self):
        from choco.web import _element_grid
        labels = [f"e{i}" for i in range(20)]
        rows = _element_grid(labels, {"e3", "e10"}, {"e3": "rfi"})
        assert len(rows) == 8
        # column c holds elements 8c..8c+7, so row 0 is 0, 8, 16
        assert [cell["label"] for cell in rows[0]] == ["e0", "e8", "e16"]
        assert [cell["index"] for cell in rows[3][:2]] == [3, 11]
        assert rows[3][0]["bad"] and rows[3][0]["sources"] == "rfi"
        assert rows[2][1]["bad"] and rows[2][1]["sources"] == ""
        assert rows[4][2] is None and rows[7][2] is None   # ragged last column
        assert _element_grid([], set(), {}) == []

    def test_eigencal_detail_from_state_file(self, client, app, tmp_path, jobs):
        login(client)
        state = {"updated": 1700000200.0, "transit_time": 1700000100.0,
                 "source": "CYG_A", "good_frac": 0.87, "sent": True}
        state_file = tmp_path / "eigencal-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "eigencal", state_file)
        resp = client.get("/service/eigencal")
        body = resp.data.decode()
        assert "CYG_A" in body
        assert "87.0%" in body

    def test_waterfall_detail_from_state_file(self, client, app, tmp_path, jobs):
        login(client)
        state = {"updated": 1700000200.0, "roots": ["subset"],
                 "waterfalls_dir": "/mnt/cs00/data/kotekan_vis_files/waterfalls",
                 "files_rendered": 3, "acquisitions_touched": 2, "backlog": 17,
                 "run_seconds": 24.5,
                 "last_acquisition": "acq_20260723_232332_046022478",
                 "last_file_idx": 4202415, "errors": []}
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "waterfall", state_file)
        resp = client.get("/files")
        body = resp.data.decode()
        assert "17 files waiting" in body
        assert "acq_20260723_232332_046022478" in body
        assert "subset" in body

    def test_waterfall_detail_reports_being_up_to_date(self, client, app, tmp_path, jobs):
        login(client)
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(
            {"backlog": 0, "files_rendered": 0, "acquisitions_touched": 0}))
        _install_state(app, "waterfall", state_file)
        resp = client.get("/files")
        assert "up to date" in resp.data.decode()

    def test_waterfall_detail_lists_skipped_files(self, client, app, tmp_path, jobs):
        login(client)
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(
            {"backlog": 0, "files_rendered": 1, "acquisitions_touched": 1,
             "errors": [f"vis_{i}.h5: cannot be widened" for i in range(14)]}))
        _install_state(app, "waterfall", state_file)
        resp = client.get("/files")
        body = resp.data.decode()
        assert "Skipped" in body
        assert "cannot be widened" in body
        assert "14 in total" in body          # capped at 10, rest counted

    def test_mapmaker_detail_and_images(self, client, app, tmp_path, jobs):
        login(client)
        state = {"updated": 1700000200.0, "pointing_dec_deg": 40.8,
                 "colour_bins": 16, "colour_range_mhz": [300, 1500], "stretch": "equalize",
                 "last_file_idx": 4233700, "last_sample_time": 1791090000.0,
                 "files_processed": 3, "files_imaged": 3, "files_uncalibrated": 0,
                 "backlog": 12, "run_seconds": 41.2, "n_samples_total": 2400,
                 "calibrator": "Cyg A", "cal_samples_pending": 40,
                 "gains": {"calibrator": "Cyg A", "solved_at": 1791086000.0,
                           "transit_time": 1791083400.0, "n_samples": 190,
                           "n_dead_products": 36, "n_products": 240},
                 "coverage": {"sky": {"coverage": 0.3, "bins_covered": 1234},
                              "daily": {"coverage": 0.2, "bins_covered": 900}},
                 "degraded": [], "skipped": ["vis_1_x.h5: no bin_ERA_deg"], "errors": []}
        state_file = tmp_path / "mapmaker-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "mapmaker", state_file)
        png = b"\x89PNG\r\n\x1a\n" + b"\0" * 16
        (job_dir(app, "mapmaker") / "sky.png").write_bytes(png)
        body = client.get("/service/mapmaker").data.decode()
        assert "Cyg A transit" in body and "36 of 240 products dead" in body
        assert "backlog 12" in body and "file 4233700" in body
        assert "sky: 1234 bins" in body
        assert "16 colour bins, 300–1500 MHz" in body and "equalize stretch" in body
        assert "/service/mapmaker/sky.png?v=" in body       # rendered image, with its mtime
        assert "/service/mapmaker/daily.png" not in body    # not rendered yet
        assert "/service/mapmaker/clean.png" not in body    # the deconvolution has not run
        assert "Deconvolution" not in body
        assert "no bin_ERA_deg" in body
        # the image routes: allowlisted names, login, 404 until rendered
        assert client.get("/service/mapmaker/sky.png").status_code == 200
        assert client.get("/service/mapmaker/sky.png").mimetype == "image/png"
        assert client.get("/service/mapmaker/daily.png").status_code == 404
        assert client.get("/service/mapmaker/clean.png").status_code == 404
        assert client.get("/service/mapmaker/etc-passwd.png").status_code == 404
        assert client.get("/service/mapmaker/../../x.png").status_code == 404
        # the deconvolution step's facts and image, once it has run
        clean = {"updated": 1700000900.0, "map_data_time": 1791090000.0, "iterations": 10604,
                 "n_components": 2683, "threshold": 7.1e-4, "sigma_before": 1.43e-4,
                 "sigma_after": 8.7e-5, "run_seconds": 43.5, "beam_deg": [0.6, 1.5],
                 "components": [{"ra_deg": 299.751, "dec_deg": 40.8, "flux": 0.91},
                                {"ra_deg": 305.376, "dec_deg": 40.55, "flux": 0.033}],
                 "degraded": [], "errors": []}
        (job_dir(app, "mapmaker") / "clean.json").write_text(json.dumps(clean))
        (job_dir(app, "mapmaker") / "clean.png").write_bytes(png)
        body = client.get("/service/mapmaker").data.decode()
        assert "/service/mapmaker/clean.png?v=" in body and "deconvolved" in body
        assert "10604 CLEAN iterations, 2683 component pixels in 44s" in body
        assert "residual 8.7e-05 of the calibrator (was 1.4e-04)" in body
        assert "299.75&deg; +40.80&deg; (0.910)" in body and "305.38&deg;" in body
        assert "restoring beam 0.6&deg; &times; 1.5&deg;" in body
        assert client.get("/service/mapmaker/clean.png").status_code == 200
        # its context image on the 408 MHz sky, once that has rendered too
        assert "/service/mapmaker/context.png" not in body
        assert client.get("/service/mapmaker/context.png").status_code == 404
        (job_dir(app, "mapmaker") / "context.png").write_bytes(png)
        body = client.get("/service/mapmaker").data.decode()
        assert "/service/mapmaker/context.png?v=" in body and "408 MHz" in body
        assert client.get("/service/mapmaker/context.png").status_code == 200
        # a degraded deconvolution says why, keeping the last numbers
        clean["degraded"] = ["the accumulators carry no geometry or baseline weights yet; the next fold-in records them"]
        (job_dir(app, "mapmaker") / "clean.json").write_text(json.dumps(clean))
        body = client.get("/service/mapmaker").data.decode()
        assert "no geometry or baseline weights yet" in body and "10604 CLEAN iterations" in body

    @pytest.mark.parametrize("check,shown", [
        ({"kotekan_dec_deg": 40.72, "config_dec_deg": 40.8, "agrees": True},
         '<span class="muted">(kotekan: 40.72&deg;)</span>'),
        ({"kotekan_dec_deg": 22.02, "config_dec_deg": 40.8, "agrees": False},
         '<span class="t-warn">(kotekan: 22.02&deg;, disagrees)</span>'),
    ])
    def test_mapmaker_pointing_check_is_shown(self, client, app, tmp_path, jobs, check, shown):
        login(client)
        state_file = tmp_path / "s.json"
        state_file.write_text(json.dumps({"pointing_dec_deg": 40.8, "pointing_check": check}))
        _install_state(app, "mapmaker", state_file)
        assert shown in client.get("/service/mapmaker").data.decode()

    def test_mapmaker_page_without_gains_says_so(self, client, app, tmp_path, jobs):
        login(client)
        state_file = tmp_path / "s.json"
        state_file.write_text(json.dumps({"calibrator": "Cyg A", "gains": None,
                                          "degraded": ["no gains yet: waiting for a Cyg A transit (0 calibration samples so far)"],
                                          "files_processed": 2, "files_imaged": 0, "files_uncalibrated": 2}))
        _install_state(app, "mapmaker", state_file)
        body = client.get("/service/mapmaker").data.decode()
        assert "waiting for a Cyg A transit" in body and "No map rendered yet" in body

    def test_raw_state_file_shown(self, client, app, tmp_path, jobs):
        login(client)
        # a key the eigencal summary never surfaces, so finding it in the
        # response proves the raw file dump is rendered
        state = {"source": "CYG_A", "sent": True, "raw_marker": 4242}
        state_file = tmp_path / "eigencal-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "eigencal", state_file)
        resp = client.get("/service/eigencal")
        body = resp.data.decode()
        assert "State file" in body
        assert "raw_marker" in body
        assert "4242" in body

    def test_eop_detail_table_span(self, client, app, configs_dir, jobs):
        login(client)
        table = [{"t_inst_ns": 1700000000_000000000},
                 {"t_inst_ns": 1700345600_000000000}]
        state_file = configs_dir / "eop-state.json"
        state_file.write_text(
            json.dumps({"earth_orientation_parameter_table": table}))
        _install_state(app, "eop", state_file)
        resp = client.get("/service/eop")
        body = resp.data.decode()
        assert "2 entries" in body

    def test_choco_detail_counts(self, client, jobs):
        login(client)
        resp = client.get("/service/choco")
        body = resp.data.decode()
        assert "3 registered" in body
        assert "3 in maintenance" in body

    @pytest.mark.parametrize("name,state", [
        # a state file with garbage contents must degrade to "no
        # summary", never break the page
        ("eop", {"earth_orientation_parameter_table":
                 [{"t_inst_ns": "yesterday"}, "not-an-entry", None]}),
        ("eop", {"earth_orientation_parameter_table": "not-a-table"}),
        ("bffs", {"updated": "recently", "bad_inputs": 7,
                  "history": "none"}),
        ("bffs", {"bad_inputs": ["f1"],
                  "history": [42, {"time": "then", "bad_inputs": 3}]}),
        ("bffs", {"bad_inputs": ["f1"], "flagged_by": {"f1": ["power"]},
                  "flag_reasons": "lots", "labels": ["f1"]}),
        ("bffs", {"bad_inputs": ["f1"], "flagged_by": {"f1": ["power"]},
                  "flag_reasons": {"f1": "off"}, "labels": ["f1"]}),
        ("eigencal", {"updated": [], "transit_time": "noon",
                      "good_frac": "most", "sent": "yes"}),
        ("waterfall", {"updated": "just now", "roots": 7, "errors": "none",
                       "backlog": "lots"}),
        ("waterfall", {"errors": [None, {"a": 1}], "roots": [None]}),
        ("mapmaker", {"gains": "none", "coverage": 7, "colour_range_mhz": "wide", "degraded": None}),
        ("mapmaker", {"gains": {"solved_at": "yesterday"}, "colour_bins": "many",
                      "colour_range_mhz": [1], "skipped": [None, 3]}),
    ])
    def test_garbage_state_files_never_break_the_page(
            self, client, app, tmp_path, name, state, jobs):
        login(client)
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, name, state_file)
        resp = client.get(f"/service/{name}", follow_redirects=True)
        assert resp.status_code == 200

    def test_pdb_stale_grid_warning(self, client, app):
        login(client)
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.health = "down"
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [False] * 8},
        ]}
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert "Controller not readable" in body
        # grid still rendered from the last read
        assert "SPI bus 0" in body


class TestEigencalPage:
    """The EIGENCAL page explains itself from the job's run.json (every
    run) and state.json (per solution), lists the archives, and shows the
    newest solution in the plot panel through the same protocol as the
    F-engine gains."""

    RUN = {"time": 1791126511.0, "status": "skipped", "exit_code": 0,
           "reason": "last transit too old", "source": "CYG_A",
           "transit": 1791083172.0, "transit_tag": "20261004T030612Z",
           "transit_complete": 1791085900.0, "eligible_until": 1791093100.0,
           "next_transit": 1791169336.0, "sun_alt_deg": -16.1,
           "archive_only": True, "dry_run": False}
    STATE = {"updated": 1791090000.0, "transit_time": 1791083172.0, "source": "CYG_A",
             "good_frac": 0.415, "n_calibrated": 128, "n_elements": 144,
             "archive": "/var/lib/choco/eigencal/gain_20261004T030612Z_cyg_a.h5",
             "sent": False}

    def _page(self, client, app, run=None, state=None, archives=()):
        login(client)
        d = job_dir(app, "eigencal")
        if run is not None:
            (d / "run.json").write_text(json.dumps(run))
        if state is not None:
            (d / "state.json").write_text(json.dumps(state))
        for name in archives:
            (d / name).write_bytes(b"\x89HDF\r\n\x1a\n" + b"x" * 100)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            return client.get("/service/eigencal").data.decode()

    def test_strip_tooltip_says_why_when_degraded(self, client, app):
        """eigencal's one-line ``reason`` is the badge's why."""
        login(client)
        (job_dir(app, "eigencal") / "run.json").write_text(json.dumps(dict(
            self.RUN, status="degraded", exit_code=2,
            reason="OSError: no N² data overlaps the transit window")))
        degraded = dict(_JOB_STUB, health="degraded", result="exit-code", exit_status="2")
        with patch("choco.web.job_status", return_value=degraded):
            body = client.get("/partials/services").data.decode()
        assert "why: OSError: no N² data overlaps the transit window" in body

    def test_run_record_explains_a_quiet_job(self, client, app):
        body = self._page(client, app, run=self.RUN)
        assert "Last run" in body and 'class="tag tag-off run-status"' in body
        assert "last transit too old" in body
        assert "Transit considered" in body and "2026-10-04 03:06" in body
        assert "sun at -16" in body
        assert "Next transit" in body and "2026-10-05 03:02" in body
        assert "Solutions" not in body          # a run record is enough to explain

    def test_solution_and_archives_listed(self, client, app):
        body = self._page(client, app, run=dict(self.RUN, status="ok", reason="archived, not sent (no choco url)"),
                          state=self.STATE, archives=["gain_20261004T030612Z_cyg_a.h5", "gain_20261003T031008Z_cyg_a.h5"])
        assert 'class="tag tag-ok run-status"' in body
        assert "archive-only: no choco url" in body
        assert "Last solution" in body and "41.5% of 128 calibrated cells good" in body
        assert "archived, not sent" in body
        assert "Archives" in body and body.count("gain_2026100") >= 2

    def test_degraded_run_shows_the_reason(self, client, app):
        body = self._page(client, app, run=dict(self.RUN, status="degraded", exit_code=2,
                                                reason="OSError: no N² data overlaps the transit window [1, 2]"))
        assert 'class="tag tag-warn run-status"' in body
        assert "no N² data overlaps" in body

    def test_nothing_at_all_shows_only_common_facts(self, client, app):
        body = self._page(client, app)
        assert "Last run" not in body and "Archives" not in body
        assert 'id="eigencal-gains"' in body     # the solution card still loads

    def _prefill(self, app, name="gain_20261004T030612Z_cyg_a.h5"):
        d = job_dir(app, "eigencal")
        f = d / name
        f.write_bytes(b"\x89HDF\r\n\x1a\n" + b"x" * 100)
        archive = app.config["eigencal_archive"]
        archive._path, archive._mtime = f, f.stat().st_mtime
        archive._manifest = {
            "datasets": [
                {"name": "gain", "value_type": "complex64", "extents": [4, 8],
                 "dimnames": ["freq", "input"], "bytes": 4 * 8 * 8},
                {"name": "dyn_rng", "value_type": "float32", "extents": [4, 2],
                 "dimnames": ["freq", "pol"], "bytes": 4 * 2 * 4},
            ],
            "attrs": {"source": "CYG_A", "transit_time": 1791083172.0, "good_frac": 0.415},
            "scalars": {}, "index_map": {"freq": {"n": 4}},
        }
        archive._data = {"gain": bytes(range(256)), "dyn_rng": b"\x00" * 32}
        archive._fetched_at = time.time()
        return archive

    def test_solution_card_names_the_archive(self, client, app):
        self._prefill(app)
        login(client)
        body = client.get("/partials/eigencal-gains").data.decode()
        assert "gain_20261004T030612Z_cyg_a.h5" in body and "CYG_A transit 2026-10-04 03:06" in body
        assert "41.5% of calibrated cells good" in body
        assert 'value="gain"' in body and 'value="dyn_rng"' in body
        assert "/api/eigencal/gain-data?dataset=" in body

    def test_solution_card_without_an_archive(self, client, app):
        login(client)
        body = client.get("/partials/eigencal-gains").data.decode()
        assert "no gain solution archived yet" in body.lower()

    def test_gain_data_descriptor_and_bytes(self, client, app):
        self._prefill(app)
        login(client)
        desc = client.get("/api/eigencal/gain-data?dataset=gain&len=0").get_json()
        assert desc["frame_desc"] == {"value_type": "complex64", "extents": [4, 8], "dimnames": ["freq", "input"]}
        assert desc["frame_id"].startswith("gain_20261004T030612Z_cyg_a.h5@")
        assert desc["metadata"]["attrs"]["source"] == "CYG_A"
        resp = client.get("/api/eigencal/gain-data?dataset=gain&len=64")
        assert resp.status_code == 200 and resp.data == bytes(range(64))
        assert resp.headers["X-Frame-Size"] == "256"
        # the manifest is the allowlist: an unknown name, path-like or not,
        # is a 404; only characters outside the dataset-name alphabet are 400
        assert client.get("/api/eigencal/gain-data?dataset=nope&len=0").status_code == 404
        assert client.get("/api/eigencal/gain-data?dataset=../x&len=0").status_code == 404
        assert client.get("/api/eigencal/gain-data?dataset=bad%20name&len=0").status_code == 400

    def test_gain_data_without_an_archive_is_404(self, client, app):
        login(client)
        resp = client.get("/api/eigencal/gain-data?dataset=gain&len=0")
        assert resp.status_code == 404 and "archived" in resp.get_json()["error"]

    def test_plot_page_and_download(self, client, app):
        self._prefill(app)
        login(client)
        body = client.get("/service/eigencal/plot?dataset=gain").data.decode()
        assert 'data-source-url="/api/eigencal/gain-data?dataset=gain"' in body
        assert 'data-source-id="eigencal-gain|gain"' in body
        resp = client.get("/service/eigencal/gain.h5")
        assert resp.status_code == 200 and resp.mimetype == "application/x-hdf5"
        assert resp.headers["Content-Disposition"].endswith('gain_20261004T030612Z_cyg_a.h5"')


class TestBffsRunFile:
    """The BFFS page's last-run block and Sources table, and the badge
    reasons, all read from the job's per-run file."""

    RUN = {
        "time": 1790873972.8, "dry_run": False, "status": "degraded",
        "exit_code": 2, "error": None,
        "degraded": ["no usable kotekan file — skipped: power-outlier",
                     "rfi: 2 of 14 /sk endpoints unreachable"],
        "kotekan_file": "/mnt/cs00/data/kotekan_vis_files/full/acq_x/vis_0004227923.h5",
        "kotekan_file_age_s": 937000.0,
        "kotekan_file_reason": "last written 260.3 h ago, older than max_age 3600 s",
        "n_elements": 128, "n_bad": 36, "sent": True,
        "update_id": "bffs-1790873972777",
        "sources": [
            {"kind": "manual", "status": "ok", "reason": None,
             "n_measured": 128, "n_flagged": 0,
             "detail": {"path": "/data/bffs/manual_overrides.yaml",
                        "exists": False, "n_listed": 0}},
            {"kind": "power-outlier", "status": "skipped",
             "reason": "no usable kotekan file: last written 260.3 h ago, "
                       "older than max_age 3600 s",
             "n_measured": 0, "n_flagged": 0, "detail": {}},
            {"kind": "power", "status": "ok", "reason": None,
             "n_measured": 128, "n_flagged": 36,
             "detail": {"url": "http://10.222.0.30:5000", "channels_read": 256,
                        "n_mapped": 136, "n_watched": 128, "n_unmapped": 0,
                        "n_unpowered": 36, "map_source": "choco master table",
                        "map_check": "ok"}},
            {"kind": "rfi", "status": "degraded",
             "reason": "2 of 14 /sk endpoints unreachable",
             "n_measured": 48, "n_flagged": 0,
             "detail": {"n_endpoints": 14, "n_failed": 2, "n_stale": 0,
                        "skipped_nodes": [{"node": "cx47", "status": "down"}],
                        "sk_bounds": [0.7, 1.5], "endpoints": []}},
        ],
    }
    STATE = {"updated": 1790000000.0, "update_id": "bffs-1790000000000",
             "bad_inputs": ["B01X"], "labels": ["B01X", "B02X"],
             "flagged_by": {"B01X": ["power"]}}

    def _page(self, client, app, tmp_path, run=None, state=None):
        login(client)
        d = job_dir(app, "bffs")
        if state is not None:
            (d / "state.json").write_text(json.dumps(state))
        if run is not None:
            (d / "run.json").write_text(run if isinstance(run, str) else json.dumps(run))
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/bffs")
        assert resp.status_code == 200
        return resp.data.decode()

    def test_sources_table_from_the_run_file(self, client, app, tmp_path):
        body = self._page(client, app, tmp_path, run=self.RUN, state=self.STATE)
        assert "Sources" in body
        # one row per configured source, with its status pill
        for kind in ("manual", "power-outlier", "power", "rfi"):
            assert f"<code>{kind}</code>" in body
        assert body.count(">skipped</span>") == 1
        # the reasons and the per-kind detail lines
        assert "2 of 14 /sk endpoints unreachable" in body
        assert "not polled: cx47 (down)" in body
        assert "12 of 14 /sk endpoints used" in body
        assert "map: choco master table" in body
        assert "128 feeds watched" in body and "36 unpowered" in body
        assert "override file absent" in body
        assert "no usable kotekan file: last written 260.3 h ago" in body
        # the last-run facts
        assert "36 bad of 128" in body
        assert "sent to kotekan" in body
        assert "written 10.8 d ago" in body
        assert "older than max_age 3600 s" in body
        # the state summary is still there beside it
        assert "1 of 2 elements" in body

    def test_run_file_alone_renders_without_a_state_summary(self, client, app, tmp_path):
        body = self._page(client, app, tmp_path, run=self.RUN)
        assert "Last run" in body and "Sources" in body
        assert "Bad feeds" not in body
        assert "Recent transitions" not in body

    def test_both_files_come_from_the_state_root_alone(self, client, app, tmp_path):
        # nothing in the bffs config block names a file
        app.config["bffs_cfg"] = {}
        body = self._page(client, app, tmp_path, run=self.RUN, state=self.STATE)
        assert "Sources" in body and "not polled: cx47 (down)" in body
        assert "1 of 2 elements" in body

    @pytest.mark.parametrize("run", [
        "{ not json",
        {"sources": "nope", "time": "then"},
        {"time": 1.0, "sources": [{"kind": "x", "n_flagged": "lots"}]},
        {"time": 1.0, "sources": [None, 7, {"kind": "x", "detail": "flat"}],
         "degraded": None, "kotekan_file_age_s": "old"},
    ])
    def test_garbage_run_file_keeps_the_state_summary(self, client, app, tmp_path, run):
        body = self._page(client, app, tmp_path, run=run, state=self.STATE)
        assert "Bad feeds" in body
        assert "1 of 2 elements" in body
        if not isinstance(run, dict) or "kind" not in json.dumps(run) or "lots" in json.dumps(run) or "old" in json.dumps(run):
            assert "Sources" not in body

    def test_unknown_source_kind_falls_back_to_key_values(self, client, app, tmp_path):
        run = dict(self.RUN, sources=[{"kind": "noise", "status": "ok",
                                       "n_measured": 3, "n_flagged": 1,
                                       "detail": {"sigma": 2.5, "window": "10m"}}])
        body = self._page(client, app, tmp_path, run=run)
        assert "<code>noise</code>" in body
        assert "sigma=2.5" in body and "window=10m" in body

    def test_strip_tooltip_says_why_when_degraded(self, client, app, tmp_path, jobs):
        login(client)
        (job_dir(app, "bffs") / "run.json").write_text(json.dumps(self.RUN))
        degraded = dict(_JOB_STUB, health="degraded", result="exit-code", exit_status="2")
        with patch("choco.web.job_status", return_value=degraded):
            body = client.get("/partials/services").data.decode()
        assert "why: no usable kotekan file — skipped: power-outlier" in body
        assert "why: rfi: 2 of 14 /sk endpoints unreachable" in body
        # an ok badge carries no reasons, whatever the run file says
        body = client.get("/partials/services").data.decode()
        assert "why:" not in body

    def test_landing_table_carries_the_reasons(self, client, app, tmp_path):
        login(client)
        (job_dir(app, "bffs") / "run.json").write_text(json.dumps(self.RUN))
        degraded = dict(_JOB_STUB, health="degraded", result="exit-code", exit_status="2")
        with patch("choco.web.job_status", return_value=degraded), \
             patch("choco.web.timer_status", return_value=None):
            body = client.get("/partials/landing-services").data.decode()
        assert "rfi: 2 of 14 /sk endpoints unreachable" in body

    def test_failed_run_error_is_a_reason(self, client, app, tmp_path):
        login(client)
        (job_dir(app, "bffs") / "run.json").write_text(json.dumps({
            "status": "failed", "exit_code": 1,
            "error": "ValueError: unknown source kind 'nope'",
            "degraded": [], "sources": []}))
        failed = dict(_JOB_STUB, health="failed", result="exit-code", exit_status="1")
        with patch("choco.web.job_status", return_value=failed):
            body = client.get("/partials/services").data.decode()
        assert "why: ValueError: unknown source kind" in body

    def test_api_nodes_carries_the_live_status(self, client):
        data = client.get("/api/nodes").get_json()
        nodes = [n for group in data["groups"].values() for n in group]
        assert nodes and all(n["status"] == "unknown" for n in nodes)

    def test_source_summary_lines(self):
        from choco.web import _bffs_source_summary as line
        assert line("power-outlier", {"band_coverage": 0.125, "rows": 16,
                                      "tail_rows_skipped": 4, "median_power": 1234.5678,
                                      "file": "/a/b/vis_0001.h5"}) == (
            "band coverage 12% · 16 rows (4 empty tail rows skipped) · "
            "median power 1235 · vis_0001.h5")
        # a compact subset/ file: say how much of the axis it carries
        assert line("power-outlier", {"band_coverage": 1.0, "rows": 16,
                                      "n_in_file": 48, "n_not_in_file": 80}) == (
            "band coverage 100% · 16 rows · 48 of 128 elements in the file")
        # a full file carries the whole axis: nothing to say about it
        assert line("power-outlier", {"rows": 16, "n_in_file": 128,
                                      "n_not_in_file": 0}) == "16 rows"
        assert line("power-outlier", {"rows": 16, "n_excluded": 16,
                                      "exclude_types": ["RFIDish"]}) == (
            "16 rows · 16 RFIDish elements not compared")
        assert line("dish-type", {"type_counts": {"ArrayDish": 32, "Missing": 80,
                                                  "RFIDish": 16},
                                  "bad_types": ["Missing"], "n_untyped": 0}) == (
            "32 ArrayDish / 80 Missing / 16 RFIDish · bad: Missing")
        assert line("dish-type", {"type_counts": {"ArrayDish": 2}, "bad_types": ["Missing"],
                                  "n_untyped": 3}) == (
            "2 ArrayDish · bad: Missing · 3 untyped (left good)")
        assert line("rfi", {"n_endpoints": 14, "n_failed": 1, "n_stale": 1,
                            "skipped_nodes": [{"node": "cx47", "status": "idle"}],
                            "sk_bounds": [0.7, 1.5]}) == (
            "12 of 14 /sk endpoints used · 1 unreachable · 1 stale · "
            "not polled: cx47 (idle) · SK bounds 0.7–1.5")
        assert line("power", {"n_watched": 128, "n_unpowered": 36, "n_unmapped": 2,
                              "unmapped_flagged": True,
                              "map_source": "choco master table",
                              "map_check": "disagrees with the cx kotekan config: 1 of 3 dish inputs mapped"}) == (
            "128 feeds watched · 36 unpowered · 2 not in the PDB table (flagged absent) · "
            "map: choco master table · disagrees with the cx kotekan config: 1 of 3 dish inputs mapped")
        assert line("power", {"n_watched": 80, "n_unpowered": 0, "n_unmapped": 48,
                              "unmapped_flagged": False, "map_source": "config CSV"}) == (
            "80 feeds watched · 0 unpowered · 48 not in the PDB table (left good) · "
            "map: config CSV")
        assert line("manual", {"n_listed": 2, "exists": True, "not_on_axis": ["A1x"]}) == (
            "2 listed · not on the axis: A1x")
        assert line("fpga", {"n_watched": 8, "channels_sampled": 32}) == (
            "8 feeds watched · 32 channels sampled")
        assert line("power-outlier", {}) == ""

    def test_fmt_age(self):
        from choco.web import _fmt_age
        assert _fmt_age(30) == "30 s"
        assert _fmt_age(600) == "10 min"
        assert _fmt_age(7200) == "2.0 h"
        assert _fmt_age(937000) == "10.8 d"


class TestBffsManualFlags:
    """Clicking the element grid adds or removes a manual flag: a CSRF
    form per cell posting to /service/bffs/manual, the label checked
    against the state file's axis, the file cross-checked against the
    one the job reports reading."""

    LABELS = ["A1X", "B1X", "A1Y", "B1Y"]

    def _setup(self, app, tmp_path, *, manual=True, run="match", state=True,
               manual_text=None):
        d = job_dir(app, "bffs")
        if state:
            (d / "state.json").write_text(json.dumps(
                {"bad_inputs": ["B1X"], "labels": self.LABELS,
                 "flagged_by": {"B1X": ["power"]}}))
        manual_file = d / "manual_overrides.yaml"
        # bffs.control is the one switch: off, the grid is read-only
        cfg = {} if manual else {"control": False}
        if manual_text is not None:
            manual_file.write_text(manual_text)
        if run is not None:
            job_path = {"match": str(manual_file), "other": "/elsewhere/flags.yaml",
                        "none": None}[run]
            sources = ([{"kind": "manual", "status": "ok", "n_measured": 4, "n_flagged": 0,
                         "detail": {"path": job_path, "exists": False, "n_listed": 0}}]
                       if job_path else [{"kind": "power", "status": "ok"}])
            (d / "run.json").write_text(json.dumps(
                {"time": 1.0, "status": "ok", "exit_code": 0,
                 "degraded": [], "sources": sources}))
        app.config["bffs_cfg"] = cfg
        return manual_file

    def _post(self, client, token, label, htmx=False):
        headers = {"HX-Request": "true"} if htmx else {}
        return client.post("/service/bffs/manual",
                           data={"_csrf_token": token, "label": label},
                           headers=headers)

    def _page(self, client):
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            return client.get("/service/bffs").data.decode()

    def test_control_off_403(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, manual=False)
        login(client)
        assert self._post(client, csrf(client), "A1X").status_code == 403
        assert not manual_file.exists()

    @pytest.mark.parametrize("label", ["", "Z9X", "A1X; rm -rf /", "x" * 65, "../etc"])
    def test_label_must_be_on_the_axis(self, client, app, tmp_path, label):
        manual_file = self._setup(app, tmp_path)
        login(client)
        assert self._post(client, csrf(client), label).status_code == 400
        assert not manual_file.exists()

    def test_no_axis_on_record_400(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, state=False)
        login(client)
        assert self._post(client, csrf(client), "A1X").status_code == 400
        assert not manual_file.exists()

    def test_toggle_adds_then_removes(self, client, app, tmp_path, caplog):
        manual_file = self._setup(app, tmp_path)
        login(client)
        token = csrf(client)
        resp = self._post(client, token, "A1X")
        assert resp.status_code == 302 and resp.headers["Location"].endswith("/service/bffs")
        assert manual_file.exists()
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1X"]}
        assert manual_file.read_text().startswith("# bffs manual overrides")
        assert "bffs manual flag: A1X -> bad requested by tester" in caplog.text

        self._post(client, token, "B1Y")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1X", "B1Y"]}
        self._post(client, token, "A1X")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["B1Y"]}
        assert "bffs manual flag: A1X -> good requested by tester" in caplog.text

    def test_other_keys_and_bare_lists_survive(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path,
                                  manual_text="note: keep me\nbad_inputs: [B1X]\n")
        login(client)
        token = csrf(client)
        self._post(client, token, "A1X")
        assert yaml.safe_load(manual_file.read_text()) == {
            "note": "keep me", "bad_inputs": ["A1X", "B1X"]}
        manual_file.write_text("[B1X, A1Y]\n")                # the bare-list shape
        self._post(client, token, "B1X")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1Y"]}

    def test_htmx_reply_swaps_the_grid_and_flashes(self, client, app, tmp_path, jobs):
        self._setup(app, tmp_path)
        login(client)
        token = csrf(client)
        resp = self._post(client, token, "A1X", htmx=True)
        body = resp.data.decode()
        assert resp.status_code == 200
        assert 'id="service-flash" hx-swap-oob="true"' in body
        assert "A1X flagged manually; the job applies it on its next run" in body
        # the grid comes back with the new flag marked, ahead of the state file
        assert 'class="feed-good feed-manual"' in body
        assert "manual flag set, applied on the next run" in body

    def test_job_reading_another_file_refuses(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, run="other")
        login(client)
        resp = self._post(client, csrf(client), "A1X", htmx=True)
        body = resp.data.decode()
        assert resp.status_code == 200
        assert "manual source reads /elsewhere/flags.yaml" in body
        assert not manual_file.exists()

    def test_job_without_a_manual_source_refuses(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, run="none")
        login(client)
        resp = self._post(client, csrf(client), "A1X", htmx=True)
        assert "runs with no manual source" in resp.data.decode()
        assert not manual_file.exists()

    def test_no_run_file_means_nothing_to_check_against(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, run=None)
        login(client)
        self._post(client, csrf(client), "A1X")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1X"]}

    def test_unparseable_file_is_not_overwritten(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, manual_text="bad_inputs: [A1X\n")
        login(client)
        resp = self._post(client, csrf(client), "B1Y", htmx=True)
        assert "manual file not rewritten" in resp.data.decode()
        assert manual_file.read_text() == "bad_inputs: [A1X\n"

    def test_grid_cells_are_forms_when_enabled(self, client, app, tmp_path):
        self._setup(app, tmp_path, manual_text="bad_inputs: [A1Y]\n")
        login(client)
        body = self._page(client)
        assert body.count('hx-post="/service/bffs/manual"') == 4
        assert body.count('name="label"') == 4
        assert 'class="feed-good feed-manual"' in body            # A1Y: pending
        assert 'click to remove the manual flag' in body
        assert 'click to flag it manually' in body
        assert "Click a cell to add or remove a manual flag" in body
        assert 'data-confirm="Flag A1X as bad manually?"' in body
        assert 'data-confirm="Remove the manual flag on A1Y?"' in body

    def test_grid_is_plain_when_control_is_off(self, client, app, tmp_path):
        self._setup(app, tmp_path, manual=False)
        login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert 'feed-manual"' not in body                        # no cell, no key swatch
        assert 'title="element 1: bad (power)"' in body         # unchanged tooltip

    def test_grid_read_only_on_a_path_mismatch(self, client, app, tmp_path):
        self._setup(app, tmp_path, run="other")
        login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert "but the bffs job reads <code>/elsewhere/flags.yaml</code>" in body

    def test_grid_read_only_on_an_unreadable_file(self, client, app, tmp_path):
        self._setup(app, tmp_path, manual_text="bad_inputs: [A1X\n")
        login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert "Manual flags cannot be edited" in body
