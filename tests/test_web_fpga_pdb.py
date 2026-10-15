"""The F-engine and power-board pages: the gain archive, the controls, the
grid layouts and the master PDB map."""

import re
from unittest.mock import patch

import pytest
import yaml

from tests.helpers import csrf, login


def _pdb(app, chips, control=True, **cfg):
    """The PDB monitor of *app* pointed at a controller and holding *chips*
    on bus 0: ``(board, chip, channels)`` rows, channels a string of
    ``1``/``0`` per channel (``"1" + "0" * 7`` is channel 0 on)."""
    monitor = app.config["pdb_monitor"]
    monitor.host, monitor.port = "pdb.example", 5000
    monitor.channels = {0: [{"board": b, "chip": c, "channels": [x == "1" for x in bits]}
                            for b, c, bits in chips]}
    app.config["pdb_cfg"] = {"host": monitor.host, "port": monitor.port,
                             "control": control, **cfg}
    return monitor


class TestFpgaGains:
    """The digital-gain archive: manifest, data protocol, page, download."""

    MANIFEST = {
        "datasets": [
            {"name": "gain_coeff", "value_type": "complex64",
             "extents": [1, 8192, 128],
             "dimnames": ["update_time", "freq", "input"], "bytes": 8388608},
            {"name": "gain_exp", "value_type": "int32", "extents": [1, 128],
             "dimnames": ["update_time", "input"], "bytes": 512},
        ],
        "attrs": {"acquisition_name": "20260808T053625Z_digitalgain"},
        "scalars": {"update_id": ["digitalgain_20260808T053625.917371Z"],
                    "index_map/update_time": [1786167385.917371]},
        "index_map": {"freq": {"n": 8192, "first_mhz": 0.0,
                               "last_mhz": 1599.8046875},
                      "inputs": {"n": 128, "names": ["chord_pathfinder000000"]}},
    }

    def _configure(self, app, payload=b"\x01\x02\x03\x04"):
        """A gain archive with its cache pre-filled — no chive, no h5py."""
        archive = app.config["gain_archive"]
        archive.base_url = "http://fpga.example:54321"
        archive._manifest = self.MANIFEST
        archive._data = {"gain_exp": payload}
        archive._fetched_at = 1e12          # never stale during a test
        monitor = app.config["fpga_monitor"]
        monitor.host, monitor.port = "fpga.example", 54321
        app.config["fpga_cfg"] = {"host": monitor.host, "port": monitor.port}
        return archive

    def test_descriptor_speaks_the_buffer_protocol(self, client, app):
        self._configure(app)
        login(client)
        body = client.get("/api/fpga/gain-data?dataset=gain_coeff&len=0").get_json()
        # Exactly the shape bufferplot.js already understands, which is
        # what lets the whole plotting stack work on an HDF5 dataset.
        assert body["frame_desc"] == {
            "value_type": "complex64", "extents": [1, 8192, 128],
            "dimnames": ["update_time", "freq", "input"],
        }
        assert body["frame_id"] == "digitalgain_20260808T053625.917371Z"
        assert body["frame_size"] == 8388608
        assert body["metadata"]["index_map"]["freq"]["n"] == 8192

    def test_data_is_raw_bytes_with_the_update_id(self, client, app):
        self._configure(app, payload=b"abcdefgh")
        login(client)
        resp = client.get("/api/fpga/gain-data?dataset=gain_exp&len=4")
        assert resp.status_code == 200
        assert resp.mimetype == "application/octet-stream"
        assert resp.data == b"abcd"            # honours the len prefix
        assert resp.headers["X-Frame-Id"] == "digitalgain_20260808T053625.917371Z"
        assert resp.headers["X-Frame-Size"] == "8"
        assert resp.headers["Cache-Control"] == "no-store"

    def test_unknown_dataset_404(self, client, app):
        self._configure(app)
        login(client)
        assert client.get("/api/fpga/gain-data?dataset=nope&len=0").status_code == 404

    def test_bad_dataset_name_rejected(self, client, app):
        self._configure(app)
        login(client)
        # The name reaches h5py, so it gets the same allowlist treatment
        # as a buffer name or a journal unit.
        for name in ['"><script>', "a b", "", "x;y"]:
            resp = client.get("/api/fpga/gain-data",
                              query_string={"dataset": name, "len": 0})
            assert resp.status_code == 400, name

    def test_negative_len_rejected(self, client, app):
        self._configure(app)
        login(client)
        resp = client.get("/api/fpga/gain-data?dataset=gain_exp&len=-1")
        assert resp.status_code == 400

    def test_unconfigured_is_404_not_a_crash(self, client, app):
        login(client)
        assert client.get("/api/fpga/gain-data?dataset=x&len=0").status_code == 404

    def test_page_defers_the_card(self, client, app):
        self._configure(app)
        login(client)
        body = client.get("/service/fpga").data.decode()
        # Filling the card means pulling 8.4 MB from fpga_master, so the
        # page must not wait for it — it asks for the card separately
        # and loads the plot module ready for when it lands.
        assert 'hx-get="/partials/fpga-gains"' in body
        assert "bufferplot.js" in body
        assert "digitalgain_20260808T053625.917371Z" not in body

    def test_card_shows_the_archive(self, client, app):
        self._configure(app)
        login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert "Digital gains" in body
        assert "digitalgain_20260808T053625.917371Z" in body
        assert 'id="gain-dataset"' in body
        assert "gain_coeff" in body and "gain_exp" in body
        assert "1599.8" in body                       # the frequency span
        assert "/service/fpga/gain.h5" in body        # download link

    def test_card_reports_an_unreachable_archive(self, client, app):
        archive = self._configure(app)
        archive._manifest = None
        archive._fetched_at = None
        archive.error = "ConnectionError: no route to host"
        # refresh() will try and fail; the card says so rather than 500.
        archive.refresh = lambda force=False: False
        login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert "unavailable" in body
        assert "no route to host" in body

    def test_page_without_an_archive_still_renders(self, client, app):
        login(client)
        app.config["fpga_monitor"].host = "fpga.example"
        app.config["fpga_monitor"].port = 54321
        body = client.get("/service/fpga").data.decode()
        card = client.get("/partials/fpga-gains").data.decode()
        assert "Digital gains" not in card            # nothing to show
        assert "F-engine" in body or "FPGA" in body   # ...page is fine

    def test_fullscreen_page_reuses_the_plot_template(self, client, app):
        self._configure(app)
        login(client)
        body = client.get("/service/fpga/plot?dataset=gain_coeff").data.decode()
        assert 'data-source-url="/api/fpga/gain-data?dataset=gain_coeff"' in body
        assert 'data-source-id="fpga-gain|gain_coeff"' in body
        assert 'data-fullscreen="1"' in body
        assert "bufferplot.js" in body

    def test_fullscreen_bad_dataset_redirects(self, client, app):
        self._configure(app)
        login(client)
        resp = client.get("/service/fpga/plot?dataset=a b",
                          follow_redirects=False)
        assert resp.status_code == 302
        assert "/service/fpga" in resp.headers["Location"]

    def test_download_serves_the_file(self, client, app, tmp_path):
        archive = self._configure(app)
        h5 = tmp_path / "gain.h5"
        h5.write_bytes(b"\x89HDF\r\n\x1a\n rest")
        archive._path = h5
        login(client)
        resp = client.get("/service/fpga/gain.h5")
        assert resp.status_code == 200
        assert resp.data.startswith(b"\x89HDF")
        assert "attachment" in resp.headers["Content-Disposition"]


class TestFpgaControl:
    def _configure(self, app, control=True):
        monitor = app.config["fpga_monitor"]
        monitor.host, monitor.port = "fpga.example", 54321
        app.config["fpga_cfg"] = {"host": monitor.host, "port": monitor.port,
                                  "control": control}
        return monitor

    def test_unknown_action_404(self, client, app):
        self._configure(app)
        login(client)
        token = csrf(client)
        resp = client.post("/service/fpga/reboot",
                           data={"_csrf_token": token})
        assert resp.status_code == 404

    def test_control_disabled_403(self, client, app):
        monitor = self._configure(app, control=False)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "start_master") as sm:
            resp = client.post("/service/fpga/start",
                               data={"_csrf_token": token})
        assert resp.status_code == 403
        sm.assert_not_called()

    def test_start_calls_monitor(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "start_master",
                          return_value=(True, "Initialization in progress")) as sm:
            resp = client.post("/service/fpga/start",
                               data={"_csrf_token": token},
                               follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/service/fpga")
        sm.assert_called_once_with()
        # the action lands in the visible trail with user + outcome
        assert monitor.actions[0]["action"] == "start"
        assert monitor.actions[0]["user"] == "tester"
        assert monitor.actions[0]["ok"] is True

    def test_stop_spawns_greenlet(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "stop_master",
                          return_value=(True, "stopped")) as sm:
            resp = client.post("/service/fpga/stop",
                               data={"_csrf_token": token},
                               follow_redirects=False)
            import gevent
            gevent.sleep(0)  # let the spawned greenlet run
        assert resp.status_code == 302
        sm.assert_called_once_with()
        # two trail entries: the in-flight request, then its completion
        assert [a["action"] for a in monitor.actions] == ["stop", "stop"]
        assert monitor.actions[1]["ok"] is None
        assert monitor.actions[0]["ok"] is True
        assert monitor.actions[0]["message"] == "stopped"

    def test_actions_rendered_in_status_partial(self, client, app):
        monitor = self._configure(app)
        monitor.record_action("start", "tester", True, "Initialization in progress")
        login(client)
        with patch.object(monitor, "poll_if_stale"):
            resp = client.get("/partials/service-fpga")
        body = resp.data.decode()
        assert "Recent actions" in body
        assert "tester" in body
        assert "Initialization in progress" in body

    def test_action_trail_is_capped(self, app):
        monitor = app.config["fpga_monitor"]
        for i in range(15):
            monitor.record_action("start", "t", True, f"m{i}")
        assert len(monitor.actions) == monitor.MAX_ACTIONS
        assert monitor.actions[0]["message"] == "m14"  # newest first

    def test_controls_rendered_when_enabled(self, client, app):
        self._configure(app)
        login(client)
        resp = client.get("/service/fpga")
        body = resp.data.decode()
        assert "/service/fpga/start" in body
        assert "/service/fpga/stop" in body
        assert "new frame0" in body

    def test_controls_hidden_when_disabled(self, client, app):
        self._configure(app, control=False)
        login(client)
        resp = client.get("/service/fpga")
        body = resp.data.decode()
        assert "/service/fpga/start" not in body

    def test_status_partial_renders_and_polls(self, client, app):
        monitor = self._configure(app)
        monitor.state = "on"
        login(client)
        with patch.object(monitor, "poll_if_stale") as pis:
            resp = client.get("/partials/service-fpga")
        assert resp.status_code == 200
        assert "on" in resp.data.decode()
        pis.assert_called_once_with(5)


class TestPdbControl:
    def _configure(self, app, control=True):
        return _pdb(app, [(0, "A", "10000000"), (0, "B", "00000000")], control)

    def _form(self, token, **overrides):
        form = {"_csrf_token": token, "bus": "0", "board": "0",
                "chip": "A", "channel": "3", "state": "on"}
        form.update(overrides)
        return form

    def test_control_disabled_403(self, client, app):
        monitor = self._configure(app, control=False)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channel") as sc:
            resp = client.post("/service/pdb/set", data=self._form(token))
        assert resp.status_code == 403
        sc.assert_not_called()

    @pytest.mark.parametrize("bad", [
        {"chip": "C"}, {"channel": "8"}, {"channel": "-1"},
        {"board": "-2"}, {"bus": "zero"},
    ])
    def test_bad_params_400(self, client, app, bad):
        self._configure(app)
        login(client)
        token = csrf(client)
        resp = client.post("/service/pdb/set", data=self._form(token, **bad))
        assert resp.status_code == 400

    def test_toggle_calls_set_channel(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(True, "bus 0 board 0 chip A ch3 on")) as sc:
            resp = client.post("/service/pdb/set",
                               data=self._form(token),
                               follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/service/pdb")
        sc.assert_called_once_with(0, 0, "A", 3, True)

    def test_verify_failure_flashes_error(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(False, "verify failed")):
            resp = client.post("/service/pdb/set",
                               data=self._form(token, state="off"),
                               follow_redirects=True)
        assert b"verify failed" in resp.data

    def test_grid_buttons_when_control_enabled(self, client, app):
        self._configure(app)
        login(client)
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert '/service/pdb/set' in body
        assert body.count("<button") >= 16
        assert 'name="channel"' in body

    def test_grid_readonly_when_control_disabled(self, client, app):
        self._configure(app, control=False)
        login(client)
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert '/service/pdb/set' not in body
        assert '<span class="chan' in body

    def test_status_partial_renders_and_polls(self, client, app):
        monitor = self._configure(app)
        login(client)
        with patch.object(monitor, "poll_if_stale") as pis:
            resp = client.get("/partials/service-pdb")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "SPI bus 0" in body
        assert '/service/pdb/set' in body  # toggles live inside the partial
        pis.assert_called_once_with(5)

    def test_htmx_toggle_swaps_in_place_instead_of_redirecting(
            self, client, app):
        """The page must not reload (and so must not jump to the top)."""
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(True, "ch3 on")):
            resp = client.post("/service/pdb/set", data=self._form(token),
                               headers={"HX-Request": "true"})
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "SPI bus 0" in body                 # the fresh grid
        assert 'id="pdb-flash" hx-swap-oob="true"' in body
        assert "PDB: ch3 on" in body

    def test_htmx_toggle_failure_is_an_error_notice(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(False, "verify failed")):
            resp = client.post("/service/pdb/set", data=self._form(token),
                               headers={"HX-Request": "true"})
        body = resp.data.decode()
        assert "flash-error" in body
        assert "verify failed" in body


class TestPdbGridLayout:
    """The grid is drawn as the frame is mounted (web._pdb_layout): boards
    15..8 across the top reading chip B then A with channel 0 at the top,
    boards 0..7 below reading A then B with channel 7 at the top."""

    def test_layout_rows(self):
        from choco.web import _pdb_layout
        rows = _pdb_layout(set(range(16)))
        assert [r["boards"] for r in rows] == [list(range(15, 7, -1)), list(range(8))]
        assert [r["chips"] for r in rows] == [["B", "A"], ["A", "B"]]
        assert [r["channels"] for r in rows] == [list(range(8)), list(range(7, -1, -1))]

    def test_top_row_only_when_an_upper_board_reports(self):
        from choco.web import _pdb_layout
        rows = _pdb_layout({0, 1})
        assert len(rows) == 1 and rows[0]["boards"] == list(range(8))
        rows = _pdb_layout({0, 8})
        assert [r["boards"][0] for r in rows] == [15, 0]

    def test_boards_beyond_the_frame_get_their_own_rows(self):
        from choco.web import _pdb_layout
        rows = _pdb_layout({3, 16, 17})
        assert [r["boards"] for r in rows] == [list(range(8)), [16, 17]]

    def test_rendered_order_matches_the_frame(self, client, app):
        login(client)
        monitor = app.config["pdb_monitor"]
        monitor.boards = {0: 16}
        monitor.channels = {0: [{"board": b, "chip": c, "channels": [False] * 8}
                                for b in range(16) for c in "AB"]}
        body = client.get("/service/pdb").data.decode()
        heads = re.findall(r'class="boardhead[^"]*"[^>]*>board (\d+)<', body)
        assert heads == [str(b) for b in range(15, 7, -1)] + [str(b) for b in range(8)]
        chips = re.findall(r'class="chiphead[^"]*">([AB])<', body)
        assert chips[:4] == ["B", "A", "B", "A"] and chips[16:20] == ["A", "B", "A", "B"]
        chans = re.findall(r'class="chanhead">ch(\d)<', body)
        assert chans == [str(c) for c in range(8)] + [str(c) for c in range(7, -1, -1)]
        # Every cell is addressed by its own slot: the top-left cell is
        # board 15 chip B channel 0, the bottom block opens with board 0
        # chip A channel 7.
        cells = re.findall(r'title="bus 0 board (\d+) chip ([AB]) ch(\d)', body)
        assert len(cells) == 256
        assert cells[0] == ("15", "B", "0") and cells[1] == ("15", "A", "0")
        assert cells[16] == ("15", "B", "1")
        assert cells[128] == ("0", "A", "7") and cells[129] == ("0", "B", "7")
        assert 'class="absent"' not in body and ' absent"' not in body

    def test_missing_board_leaves_its_slot_empty(self, client, app):
        login(client)
        monitor = app.config["pdb_monitor"]
        monitor.boards = {0: 2}
        monitor.channels = {0: [{"board": b, "chip": c, "channels": [False] * 8}
                                for b in (0, 1) for c in "AB"]}
        body = client.get("/service/pdb").data.decode()
        heads = re.findall(r'class="boardhead( absent)?"[^>]*>board (\d+)<', body)
        assert [h[1] for h in heads] == [str(b) for b in range(8)]
        assert [bool(h[0]) for h in heads] == [False, False] + [True] * 6
        assert body.count('<td class="absent"></td>') == 6 * 2 * 8


class TestPdbDishLayout:
    """The PDB page's second grid: dishes as they stand in the field, with
    per-row, per-pol power buttons whose channels come from the map."""

    MAP = (
        "spi_bus,board,chip,channel,dish_input,amplifier,notes\n"
        "0,0,A,0,A01X,,\n0,0,A,1,A01Y,,\n0,0,A,2,A02X,,\n0,0,A,3,A02Y,,\n"
        "0,0,B,0,RFIA1X,,\n0,0,B,1,RFIA1Y,,\n")

    def _configure(self, app, configs_dir, control=True):
        (configs_dir / "pdb_map.csv").write_text(self.MAP)
        return _pdb(app, [(0, "A", "10100000"), (0, "B", "10000000")], control)

    def test_dish_layout_renders_the_field(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        login(client)
        body = client.get("/service/pdb?layout=dish").data.decode()
        assert 'class="dish-grid"' in body and 'class="pdb-grid"' not in body
        assert re.findall(r'class="rowhead">([^<]*)<', body) == list("ABCDEFGH") + ["RFI"]
        assert re.findall(r'class="dish-name">([^<]*)<', body) == ["A01", "A02", "RFIA1"]
        # A01X and A02X are on, their Y pols off; RFIA1X on
        assert body.count(">● X<") == 3 and body.count(">○ Y<") == 3
        # every frame slot the map does not name is drawn empty
        assert body.count('<td class="absent"></td>') == 64 - 2
        # the poll and every control post carry the layout
        assert "hx-vals='{\"layout\": \"dish\"}'" in body
        assert 'aria-current="page"' in body

    def test_bulkhead_is_the_default_and_the_fallback(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        login(client)
        for url in ("/service/pdb", "/service/pdb?layout=nope"):
            body = client.get(url).data.decode()
            assert 'class="pdb-grid"' in body and 'class="dish-grid"' not in body
            assert "hx-vals='{\"layout\": \"bulkhead\"}'" in body

    def test_partial_follows_the_layout(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        login(client)
        body = client.get("/partials/service-pdb?layout=dish").data.decode()
        assert 'class="dish-grid"' in body

    def test_row_power_buttons_per_pol(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        login(client)
        body = client.get("/service/pdb?layout=dish").data.decode()
        assert "/service/pdb/set-row" in body
        # rows A and RFI have both pols mapped; empty rows get no buttons
        assert body.count(">X on<") == 2 and body.count(">X off<") == 2
        assert body.count(">Y on<") == 2 and body.count(">Y off<") == 2
        assert "all 2 X channels of row A?" in body

    def test_set_row_resolves_channels_through_the_map(self, client, app, configs_dir):
        monitor = self._configure(app, configs_dir)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channels",
                          return_value=(True, "row A pol X: 2 channels off")) as sc:
            resp = client.post("/service/pdb/set-row",
                               data={"_csrf_token": token, "row": "A", "pol": "X",
                                     "state": "off", "layout": "dish"},
                               headers={"HX-Request": "true"})
        sc.assert_called_once_with([(0, 0, "A", 0), (0, 0, "A", 2)], False, "row A pol X")
        body = resp.data.decode()
        assert 'class="dish-grid"' in body
        assert "PDB: row A pol X: 2 channels off" in body

    def test_set_row_with_nothing_mapped_writes_nothing(self, client, app, configs_dir):
        monitor = self._configure(app, configs_dir)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_channels") as sc:
            resp = client.post("/service/pdb/set-row",
                               data={"_csrf_token": token, "row": "B", "pol": "X",
                                     "state": "on"}, headers={"HX-Request": "true"})
        sc.assert_not_called()
        assert "no channels in the map" in resp.data.decode()

    @pytest.mark.parametrize("form", [
        {"row": "Z", "pol": "X", "state": "on"},      # not a row of the layout
        {"row": "A", "pol": "Q", "state": "on"},      # not a polarization
        {"row": "A", "pol": "X"},                     # no state
        {"row": "../etc", "pol": "X", "state": "on"},
    ])
    def test_set_row_bad_params_400(self, client, app, configs_dir, form):
        self._configure(app, configs_dir)
        login(client)
        token = csrf(client)
        resp = client.post("/service/pdb/set-row", data={"_csrf_token": token, **form})
        assert resp.status_code == 400

    def test_set_row_requires_csrf_and_control(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        login(client)
        resp = client.post("/service/pdb/set-row", data={"row": "A", "pol": "X", "state": "on"})
        assert resp.status_code == 403
        self._configure(app, configs_dir, control=False)
        token = csrf(client)
        resp = client.post("/service/pdb/set-row",
                           data={"_csrf_token": token, "row": "A", "pol": "X", "state": "on"})
        assert resp.status_code == 403


class TestPdbGroupControl:
    """Bulk power buttons: per chip, per board, and per SPI bus."""

    def _configure(self, app, control=True):
        return _pdb(app, [(0, "A", "10000000"), (0, "B", "00000000"),
                          (1, "A", "00000000"), (1, "B", "00000000")], control)

    def test_control_disabled_403(self, client, app):
        monitor = self._configure(app, control=False)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_group") as sg:
            resp = client.post("/service/pdb/set-group", data={
                "_csrf_token": token, "bus": "0", "state": "on"})
        assert resp.status_code == 403
        sg.assert_not_called()

    @pytest.mark.parametrize("form,expected", [
        ({"bus": "0", "state": "on"}, (0, True, None, None)),
        ({"bus": "0", "board": "1", "state": "on"}, (0, True, 1, None)),
        ({"bus": "0", "board": "1", "chip": "B", "state": "off"},
         (0, False, 1, "B")),
    ])
    def test_scope_widens_with_the_form(self, client, app, form, expected):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_group",
                          return_value=(True, "done")) as sg:
            client.post("/service/pdb/set-group",
                        data={"_csrf_token": token, **form})
        bus, on, board, chip = expected
        sg.assert_called_once_with(bus, on, board=board, chip=chip)

    @pytest.mark.parametrize("form", [
        {"bus": "zero", "state": "on"},
        {"bus": "0", "board": "-1", "state": "on"},
        {"bus": "0", "board": "0", "chip": "C", "state": "on"},
        {"bus": "0", "chip": "A", "state": "on"},   # chip without a board
        {"state": "on"},                            # no bus
    ])
    def test_bad_params_400(self, client, app, form):
        self._configure(app)
        login(client)
        token = csrf(client)
        resp = client.post("/service/pdb/set-group",
                           data={"_csrf_token": token, **form})
        assert resp.status_code == 400

    def test_htmx_reply_swaps_the_grid(self, client, app):
        monitor = self._configure(app)
        login(client)
        token = csrf(client)
        with patch.object(monitor, "set_group",
                          return_value=(True, "bus 0: 32 channels on")):
            resp = client.post(
                "/service/pdb/set-group",
                data={"_csrf_token": token, "bus": "0", "state": "on"},
                headers={"HX-Request": "true"})
        body = resp.data.decode()
        assert "SPI bus 0" in body
        assert "PDB: bus 0: 32 channels on" in body

    def test_buttons_rendered_at_each_scope(self, client, app):
        self._configure(app)
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert '/service/pdb/set-group' in body
        assert "bus all on" in body and "bus all off" in body
        # one chip-level pair per chip; no per-board button (the chip
        # column is the same two clicks, so the board button was dropped)
        assert body.count(">all on<") == 4
        assert body.count(">all off<") == 4
        assert "board 0" in body and "board 1" in body

    def test_no_bulk_buttons_when_control_disabled(self, client, app):
        self._configure(app, control=False)
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert '/service/pdb/set-group' not in body


class TestPdbChannelMap:
    """The master dish-input <-> channel table and its kotekan cross-check."""

    MAP = ("spi_bus,board,chip,channel,dish_input,amplifier,notes\n"
           "0,0,A,0,A1X,AMP-1,\n"
           "0,0,A,1,A1Y,AMP-2,\n")

    def _configure(self, app, configs_dir, map_text=None, dish_inputs=None):
        monitor = _pdb(app, [(0, "A", "10000000"), (0, "B", "00000000")],
                       kotekan_group="cx")
        if map_text is not None:
            (configs_dir / "pdb_map.csv").write_text(map_text)
        if dish_inputs is not None:
            (configs_dir / "cx" / "cx1.yaml").write_text(
                yaml.safe_dump({"dish_inputs": dish_inputs}))
            app.config["registry"].reload()
        return monitor

    def test_grid_cells_carry_the_dish_input(self, client, app, configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP)
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "A1X" in body and "A1Y" in body
        # mapped cells lose the "unmapped" styling; unmapped ones keep it
        assert 'class="chan on"' in body
        assert 'class="chan off unmapped"' in body

    def test_map_problems_are_shown_not_fatal(self, client, app, configs_dir):
        self._configure(app, configs_dir,
                        map_text=self.MAP + "0,0,C,0,BAD,,\n")
        login(client)
        resp = client.get("/service/pdb")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "1 bad row in the channel map" in body
        assert "chip must be A/B" in body
        assert "A1X" in body           # the good rows still label the grid

    def test_cross_check_agreement(self, client, app, configs_dir):
        # One connected dish (A1): the map's A1X/A1Y rows cover it.
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A1", "type": "ArrayDish"},
        ])
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "agreed" in body

    def test_cross_check_reports_disagreements(self, client, app,
                                               configs_dir):
        # A2 is connected but unmapped (both pols); the map's rows are
        # for A1, which kotekan has never heard of -> stale.
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A2", "type": "ArrayDish"},
        ])
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "disagreements" in body
        assert "2 in kotekan but not in the map" in body
        assert "2 in the map but not in kotekan" in body
        assert "A2X" in body           # kotekan knows it, the map doesn't
        assert "A1Y" in body           # the map knows it, kotekan doesn't

    def test_unconnected_dish_wiring_is_not_stale(self, client, app,
                                                  configs_dir):
        """Map rows for a dish that exists but is not on the correlator
        (type Fake, real label) are legitimate, not disagreements."""
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A1", "type": "Fake"},
        ])
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "agreed" in body

    def test_old_style_table_reports_a_migration_reason(self, client, app,
                                                        configs_dir):
        """A pre-2026-08 per-element table is refused, never checked."""
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A1X"},
            {"dish_idx": 1, "label": "A1Y"},
        ])
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "Not cross-checked against kotekan" in body
        assert "migrate the config" in body

    def test_no_dish_inputs_degrades_to_a_reason(self, client, app,
                                                 configs_dir):
        """The stock test config has no dish_inputs table."""
        self._configure(app, configs_dir, map_text=self.MAP)
        login(client)
        body = client.get("/service/pdb").data.decode()
        assert "Not cross-checked against kotekan" in body
        assert "has no dish_inputs table" in body

    def test_api_serves_the_master_table(self, client, app, configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP)
        resp = client.get("/api/pdb/map")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["n_entries"] == 2
        assert data["errors"] == []
        first = data["channels"][0]
        assert first["dish_input"] == "A1X"
        # bffs's CSV loader keys on correlator_input; both names are served
        assert first["correlator_input"] == "A1X"
        assert data["check"]["available"] is False   # no dish_inputs table

    def test_api_includes_the_cross_check(self, client, app, configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A1", "type": "ArrayDish"},
        ])
        data = client.get("/api/pdb/map").get_json()
        assert data["check"]["available"] is True
        assert data["check"]["ok"] is True
        assert data["check"]["group"] == "cx"

    def test_missing_map_file_is_not_an_error_page(self, client, app,
                                                   configs_dir):
        self._configure(app, configs_dir)      # no CSV written
        login(client)
        resp = client.get("/service/pdb")
        assert resp.status_code == 200
        assert "FileNotFoundError" in resp.data.decode()

    def test_map_is_reread_when_the_file_changes(self, client, app,
                                                 configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP)
        login(client)
        assert "A1X" in client.get("/service/pdb").data.decode()
        (configs_dir / "pdb_map.csv").write_text(
            "spi_bus,board,chip,channel,dish_input\n0,0,A,0,RENAMED\n")
        body = client.get("/service/pdb").data.decode()
        assert "RENAMED" in body
        assert "A1X" not in body
