"""The beam page: its events and the waterfall rows it serves."""

import json
from unittest.mock import PropertyMock, patch

from choco import beam
from choco.state import Node

from tests.helpers import login
from tests.test_beam import SEARCH, candidate, frame


def config(tmp_path, **fields):
    return {"incoherent_beam_search": dict(SEARCH, candidate_file=str(tmp_path / "c.jsonl"),
                                           **fields)}


def desired(value):
    return patch.object(Node, "desired_config", new_callable=PropertyMock, return_value=value)


class TestBeamPage:
    def test_requires_login(self, client):
        assert client.get("/beam", follow_redirects=False).status_code == 302
        assert client.get("/beam/waterfall", follow_redirects=False).status_code == 302

    def test_menu_has_beam_under_acquisitions(self, client):
        login(client)
        body = client.get("/files").data.decode()
        assert body.index('href="/files"') < body.index('href="/beam"') < body.index("Sky maps")

    def test_page_without_a_search(self, client):
        login(client)
        body = client.get("/beam").data.decode()
        assert "nothing to draw" in body and "static/beam.js" not in body
        assert "no search with a candidate_file" in body

    def test_page_with_a_search(self, client, app, tmp_path):
        login(client)
        (tmp_path / "c.jsonl").write_text(json.dumps(candidate(1795807533.4, cutout=3)) + "\n")
        log = app.config["state_dir"] / "server" / "baseband_triggers.jsonl"
        log.parent.mkdir(parents=True)
        log.write_text(json.dumps({"time": "2026-11-27T17:07:40.094+00:00", "sender": "chime",
                                   "result": "forwarded", "event_id": 10103,
                                   "request": {"dm": 242.4}, "nodes": {"cx/cx1": {}}}) + "\n")
        with desired(config(tmp_path)):
            body = client.get("/beam").data.decode()
            partial = client.get("/partials/beam/events").data.decode()
        assert 'id="beam-waterfall"' in body and "static/beam.js" in body
        assert 'data-column-mhz="1.5625"' in body
        for text in (body, partial):
            assert "19:25:33" in text and "300.5" in text        # the candidate
            assert "10103" in text and "242.4" in text and "0 / 1" in text
        assert "<html" not in partial


class TestBeamWaterfall:
    def get(self, client, tmp_path, url, frames, **fields):
        with desired(config(tmp_path, **fields)), \
                patch.object(Node, "get_buffer_frame", side_effect=frames) as peek:
            resp = client.get(url)
        return resp, peek

    def test_rows_after_a_number(self, client, tmp_path):
        login(client)
        resp, peek = self.get(client, tmp_path, "/beam/waterfall?after=1001", [frame()])
        assert resp.status_code == 200 and resp.content_type == "application/octet-stream"
        assert resp.data == bytes(range(6, 12))
        assert resp.headers["X-Beam-First"] == "1002"
        assert resp.headers["X-Beam-Newest"] == "1003"
        assert resp.headers["X-Beam-Columns"] == "3"
        assert abs(float(resp.headers["X-Beam-Row-Seconds"]) - 8192 * 5.12e-6) < 1e-12
        assert "X-Beam-End-Unix" not in resp.headers
        assert resp.headers["Cache-Control"] == "no-store"
        peek.assert_called_once_with("incoherent_waterfall_buffer")

    def test_everything_without_after_and_dated_by_a_candidate(self, client, tmp_path):
        login(client)
        (tmp_path / "c.jsonl").write_text(json.dumps(candidate(5000.0, fpga_seq=8192 * 1000)) + "\n")
        with desired(config(tmp_path)):
            client.get("/partials/beam/events")          # reads the candidate file
        resp, _ = self.get(client, tmp_path, "/beam/waterfall", [frame()])
        assert resp.data == bytes(range(12)) and resp.headers["X-Beam-First"] == "1000"
        # The newest row, 1003, ends four rows after the candidate's sequence number.
        assert abs(float(resp.headers["X-Beam-End-Unix"]) - (5000.0 + 4 * 8192 * 5.12e-6)) < 2e-3

    def test_viewers_share_one_read(self, client, tmp_path):
        login(client)
        with desired(config(tmp_path)), \
                patch.object(Node, "get_buffer_frame", side_effect=[frame()]) as peek:
            assert client.get("/beam/waterfall?after=1000").data == bytes(range(3, 12))
            assert client.get("/beam/waterfall?after=1002").data == bytes(range(9, 12))
        assert peek.call_count == 1

    def test_errors(self, client, app, tmp_path):
        login(client)
        assert client.get("/beam/waterfall").status_code == 404
        resp, _ = self.get(client, tmp_path, "/beam/waterfall?after=-1", [frame()])
        assert resp.status_code == 400
        resp, _ = self.get(client, tmp_path, "/beam/waterfall?after=1e3", [frame()])
        assert resp.status_code == 400
        resp, _ = self.get(client, tmp_path, "/beam/waterfall", [None])
        assert resp.status_code == 502 and resp.get_json() == {"error": "kotekan unreachable"}
        app.config.pop("beam_frames")
        resp, _ = self.get(client, tmp_path, "/beam/waterfall",
                           [{"error": "no full frame currently in buffer"}])
        assert resp.status_code == 404
        app.config.pop("beam_frames")
        resp, peek = self.get(client, tmp_path, "/beam/waterfall", [frame()],
                              waterfall_buf="../etc")
        assert resp.status_code == 404 and peek.call_count == 0
        assert beam.FRAME_TTL_S < 0.34          # one read per search pass at most
