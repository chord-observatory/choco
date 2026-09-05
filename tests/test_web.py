"""Tests for the nodes.yaml editor and group-config editor routes."""

import contextlib
import hashlib
import json
import logging
import re

import pytest
import yaml

from choco.app import create_app
from choco.auth import save_user, _users
from choco.sync import ChangeType


@pytest.fixture(autouse=True)
def clear_users():
    _users.clear()
    yield
    _users.clear()


def _login(client):
    user = save_user("cn=tester,dc=example", "tester")
    with client.session_transaction() as sess:
        sess["_user_id"] = user.get_id()


def _csrf(client):
    """Establish a session and return its CSRF token.

    The token is seeded lazily by the ``csrf_token`` context processor,
    so this must fetch a page that renders a CSRF-carrying form — the
    dashboard, not the landing page.
    """
    client.get("/nodes")
    with client.session_transaction() as sess:
        return sess["_csrf_token"]


# --- GET /nodes/edit ---

class TestNodesEditGet:
    def test_requires_login(self, client):
        resp = client.get("/nodes/edit", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_renders_groups(self, client):
        _login(client)
        resp = client.get("/nodes/edit")
        assert resp.status_code == 200
        body = resp.data.decode()
        # Toolbar sanity + the seeded groups/nodes appear as editable rows.
        assert 'value="cx"' in body
        assert 'value="recv"' in body
        assert 'value="cx1"' in body
        assert 'value="cx1.example"' in body
        # Warning banner is present so the user knows this is disruptive.
        # New text mentions both the maintenance pause and the rediscovery.
        body_lower = body.lower()
        assert "maintenance" in body_lower
        assert "rebuilds the node registry" in body_lower


# --- POST /nodes/edit ---

class TestNodesSave:
    def test_requires_csrf(self, client):
        _login(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": {}}),
            content_type="application/json",
        )
        assert resp.status_code == 403

    def test_bad_csrf_rejected(self, client):
        _login(client)
        _csrf(client)  # ensure session has a token
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": {}}),
            content_type="application/json",
            headers={"X-CSRF-Token": "not-the-token"},
        )
        assert resp.status_code == 403

    def test_rejects_non_dict_groups(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": []}),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400

    def test_rejects_invalid_group_name(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": {"bad/name": []}}),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "invalid group name" in resp.get_json()["error"].lower()

    def test_rejects_missing_host(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({
                "groups": {"g": [{"name": "n1", "host": "", "port": 12048}]}
            }),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400

    def test_rejects_duplicate_node(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({
                "groups": {
                    "g": [
                        {"name": "n1", "host": "a", "port": 12048},
                        {"name": "n1", "host": "b", "port": 12048},
                    ]
                }
            }),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400

    def test_save_rewrites_yaml_and_reloads(self, client, app, configs_dir):
        _login(client)
        token = _csrf(client)
        new_payload = {
            "groups": {
                "only": [
                    {"name": "n1", "host": "n1.example", "port": 9000},
                ]
            }
        }
        resp = client.post(
            "/nodes/edit",
            data=json.dumps(new_payload),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok"}

        # nodes.yaml on disk matches.
        on_disk = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        assert on_disk == {
            "groups": {
                "only": {"n1": {"host": "n1.example", "port": 9000}}
            }
        }
        # Registry was fully rebuilt.
        registry = app.config["registry"]
        assert set(registry.nodes.keys()) == {"only/n1"}
        assert registry.get_node("only/n1").port == 9000

    def test_save_forces_maintenance_and_rediscovers_started(self, client, app):
        """After a save, every rebuilt node is in maintenance and
        ``started`` reflects what discovery probed (not the prior
        in-memory toggle).
        """
        from unittest.mock import patch
        from choco.state import Node, NodeStatus

        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        # Pre-save: flip maintenance off and force a started value so
        # we can prove neither leaks across the rebuild.
        for n in registry.nodes.values():
            n.maintenance = False
        registry.get_node("cx/cx1").started = False

        # Make discovery deterministic: pretend cx1 is currently running.
        with patch.object(Node, "get_status", return_value=NodeStatus.STARTED):
            resp = client.post(
                "/nodes/edit",
                data=json.dumps({
                    "groups": {
                        "cx": [{"name": "cx1", "host": "cx1.example", "port": 12048}],
                    }
                }),
                content_type="application/json",
                headers={"X-CSRF-Token": token},
            )
        assert resp.status_code == 200

        cx1 = registry.get_node("cx/cx1")
        # Every rebuilt node lands in maintenance — the save is a pause.
        assert cx1.maintenance is True
        # And started follows reality (probe returned STARTED), not the
        # cold default.
        assert cx1.started is True


# --- Landing page (/) ---

class TestLandingPage:
    def test_requires_login(self, client):
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_renders_service_table(self, client):
        _login(client)
        body = client.get("/").data.decode()
        # One row per badge: choco itself, the cluster, and the jobs.
        for label in ("CHOCO", "NODES", "EOP", "BFFS", "EIGENCAL", "WF"):
            assert label in body
        # The nodes row summarizes the registry (3 seeded nodes).
        assert "3 nodes" in body
        # The door to node management, now that the dashboard left /.
        assert 'href="/nodes"' in body

    def test_dashboard_no_longer_at_root(self, client):
        _login(client)
        body = client.get("/").data.decode()
        assert "dashboard-table" not in body
        # The dashboard still answers at /nodes.
        nodes_body = client.get("/nodes").data.decode()
        assert "dashboard-table" in nodes_body

    def test_partial_refreshes_table(self, client):
        _login(client)
        body = client.get("/partials/landing-services").data.decode()
        assert "<table" in body and "EOP" in body

    def test_partial_requires_login(self, client):
        resp = client.get("/partials/landing-services", follow_redirects=False)
        assert resp.status_code == 302


# --- Sky map: /skymap.png (unauthenticated) + landing card ---

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


class TestSkymap:
    def _configure(self, app, tmp_path, write=True):
        path = tmp_path / "skymap.png"
        if write:
            path.write_bytes(PNG_MAGIC + b"fake image data")
        app.config["skymap_cfg"] = {"image_file": str(path)}
        return path

    def test_unconfigured_404(self, client):
        # No login on purpose: the route must answer (with 404 here)
        # without a session.
        assert client.get("/skymap.png").status_code == 404

    def test_serves_image_without_login(self, client, app, tmp_path):
        path = self._configure(app, tmp_path)
        resp = client.get("/skymap.png")
        assert resp.status_code == 200
        assert resp.mimetype == "image/png"
        assert resp.data == path.read_bytes()

    def test_missing_file_404(self, client, app, tmp_path):
        self._configure(app, tmp_path, write=False)
        assert client.get("/skymap.png").status_code == 404

    def test_conditional_get_304(self, client, app, tmp_path):
        self._configure(app, tmp_path)
        first = client.get("/skymap.png")
        etag = first.headers.get("ETag")
        assert etag
        resp = client.get("/skymap.png", headers={"If-None-Match": etag})
        assert resp.status_code == 304

    def test_partial_requires_login(self, client, app, tmp_path):
        self._configure(app, tmp_path)
        resp = client.get("/partials/skymap", follow_redirects=False)
        assert resp.status_code == 302

    def test_partial_carries_mtime_busted_url(self, client, app, tmp_path):
        path = self._configure(app, tmp_path)
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert f'/skymap.png?v={int(path.stat().st_mtime)}' in body

    def test_partial_without_render_yet(self, client, app, tmp_path):
        self._configure(app, tmp_path, write=False)
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert "No sky map rendered yet" in body

    def test_landing_card_only_when_configured(self, client, app, tmp_path):
        _login(client)
        assert 'id="skymap"' not in client.get("/").data.decode()
        self._configure(app, tmp_path)
        assert 'id="skymap"' in client.get("/").data.decode()

    def test_load_config_carries_skymap_block(self, configs_dir, tmp_path):
        """The production path: config.yaml -> load_config -> create_app.

        load_config copies each known section explicitly, so a section
        missing from that list is parsed and then silently dropped --
        exactly how a correct skymap: block in the deployed config.yaml
        still 404'd /skymap.png.  Every other test reaches create_app
        directly and never sees that filter.
        """
        from choco.app import load_config
        png = tmp_path / "skymap.png"
        png.write_bytes(PNG_MAGIC + b"fake")
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            f"configs_dir: {configs_dir}\n"
            "server: {secret_key: kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk}\n"
            f"skymap:\n  image_file: {png}\n"
        )
        config = load_config(cfg_file)
        assert config["skymap"] == {"image_file": str(png)}
        app = create_app(config=config)
        app.config["TESTING"] = True
        resp = app.test_client().get("/skymap.png")
        assert resp.status_code == 200


# --- POST /nodes/set-started-group/<group>/<action> ---

class TestSetStartedGroup:
    def test_start_scopes_to_group(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        # Seed: nothing started.
        for node in registry.nodes.values():
            node.started = False

        resp = client.post(
            "/nodes/set-started-group/cx/start",
            data={"_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert registry.get_node("cx/cx1").started is True
        assert registry.get_node("cx/cx2").started is True
        # Other groups untouched.
        assert registry.get_node("recv/recv1").started is False

    def test_unknown_group_404(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/set-started-group/nope/start",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 404

    def test_bad_action_400(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/set-started-group/cx/frobnicate",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 400

    def test_bad_csrf_rejected(self, client):
        _login(client)
        _csrf(client)
        resp = client.post(
            "/nodes/set-started-group/cx/start",
            data={"_csrf_token": "bogus"},
        )
        assert resp.status_code == 403


class TestPipelinePage:
    """Full-page interactive pipeline view (/pipeline/<key>)."""

    DOT = 'digraph pipeline {\n"n2_buffer" -> "stage_x";\n}'
    # Graphviz-shaped SVG: one held buffer, one without peek_hold.
    SVG = ('<svg xmlns="http://www.w3.org/2000/svg" width="10pt" height="10pt" '
           'viewBox="0 0 10 10"><g class="graph">'
           '<g id="node1" class="node"><title>n2_buffer</title>'
           '<polygon fill="none" stroke="black" points="0,0 5,5"/></g>'
           '<g id="node2" class="node"><title>host_voltage_buffer_0</title>'
           '<polygon fill="none" stroke="black" points="0,0 5,5"/></g>'
           '</g></svg>')
    BUFFERS = {
        "n2_buffer": {"num_full_frame": 1, "peek_hold": True},
        "host_voltage_buffer_0": {"num_full_frame": 0},
    }

    def test_requires_login(self, client):
        resp = client.get("/pipeline/cx/cx1", follow_redirects=False)
        assert resp.status_code == 302

    def test_unknown_node_redirects(self, client):
        _login(client)
        resp = client.get("/pipeline/cx/nope", follow_redirects=False)
        assert resp.status_code == 302

    def test_page_structure(self, client):
        _login(client)
        body = client.get("/pipeline/cx/cx1").data.decode()
        # Standalone full-viewport page with slim nav, not base.html.
        assert 'id="pipeline-graph"' in body
        assert "/partials/node-pipeline-svg/cx/cx1" in body
        assert 'class="brand-pill"' in body and 'href="/"' in body
        assert "/nodes/edit/cx/cx1" in body
        # The status page is gone; nothing may still link to it.
        assert "/status/cx/cx1" not in body
        # Plot popup overlay + layout preset selector + theme toggle.
        assert 'id="buffer-plot"' in body
        assert 'data-node-key="cx/cx1"' in body
        assert 'name="layout"' in body
        # curves is the default layout (first option in the selector).
        assert body.index('value="curves"') < body.index('value="ortho"')
        assert 'id="pg-theme"' in body
        assert 'data-theme="dark"' in body  # dark is the default
        assert "bufferplot.js" in body and "pipeline.js" in body

    def test_page_layout_selection_round_trips(self, client):
        _login(client)
        # ?layout= preselects a preset, so a refresh or a shared link
        # comes back with the same routing...
        body = client.get("/pipeline/cx/cx1?layout=ortho").data.decode()
        assert '<option value="ortho" selected>' in body
        assert '<option value="curves">' in body
        # ...and an unknown value falls back to curves, never reaching dot.
        body = client.get("/pipeline/cx/cx1?layout=../evil").data.decode()
        assert '<option value="curves" selected>' in body
        assert "evil" not in body
        # The initial graph fetch includes the selector, so a browser
        # restoring a select value across a reload can't disagree with
        # what gets rendered.
        assert 'hx-include="#pg-layout"' in body

    def test_partial_layout_presets_are_allowlisted(self, client):
        from unittest.mock import patch
        from choco.state import Node
        from choco.services import PIPELINE_LAYOUTS
        _login(client)
        for requested, expected in (
            ("curves", PIPELINE_LAYOUTS["curves"]),
            ("ortho", PIPELINE_LAYOUTS["ortho"]),
            ("../evil", PIPELINE_LAYOUTS["curves"]),  # unknown -> default
        ):
            with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
                 patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
                 patch("choco.web.render_dot_svg",
                       return_value=self.SVG) as rds:
                resp = client.get(
                    f"/partials/node-pipeline-svg/cx/cx1?layout={requested}")
            assert resp.status_code == 200
            rds.assert_called_once_with(self.DOT, layout_args=expected)

    def test_partial_stamps_only_held_buffers(self, client):
        from unittest.mock import patch
        from choco.state import Node
        _login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert 'data-plot-buffer="n2_buffer"' in body
        assert 'data-plot-node="cx/cx1"' in body
        assert 'data-plot-buffer="host_voltage_buffer_0"' not in body
        # Inline SVG, not the base64 <img> of the status page.
        assert "<svg" in body and "data:image/svg+xml" not in body

    def test_partial_notes_when_nothing_is_clickable(self, client):
        from unittest.mock import patch
        from choco.state import Node
        _login(client)
        # A failed /buffers read: the graph renders, but nothing could be
        # marked — say so rather than silently showing an amber-less graph.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=None), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "buffer list unavailable" in body
        assert "data-plot-buffer" not in body
        # A reachable kotekan with no peek_hold buffers is a different
        # (also worth stating) case.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers",
                          return_value={"host_voltage_buffer_0": {"num_full_frame": 0}}), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "no <code>peek_hold</code> buffers" in body
        # ...and neither note appears when buffers are clickable.
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch.object(Node, "get_buffers", return_value=self.BUFFERS), \
             patch("choco.web.render_dot_svg", return_value=self.SVG):
            body = client.get("/partials/node-pipeline-svg/cx/cx1").data.decode()
        assert "unavailable" not in body and "nothing is clickable" not in body

    def test_partial_falls_back_to_dot_text(self, client):
        from unittest.mock import patch
        from choco.state import Node
        _login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=self.DOT), \
             patch("choco.web.render_dot_svg", return_value=None):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        body = resp.data.decode()
        assert "graphviz" in body
        assert "&#34;n2_buffer&#34;" in body  # escaped dot text

    def test_partial_unreachable(self, client):
        from unittest.mock import patch
        from choco.state import Node
        _login(client)
        with patch.object(Node, "get_pipeline_dot", return_value=None):
            resp = client.get("/partials/node-pipeline-svg/cx/cx1")
        assert b"unreachable" in resp.data

    def test_partial_unknown_node_404(self, client):
        _login(client)
        assert client.get("/partials/node-pipeline-svg/cx/nope").status_code == 404


class TestPlotPage:
    """Full-viewport single-buffer plot (/plot/<key>?buffer=)."""

    def test_requires_login(self, client):
        resp = client.get("/plot/cx/cx1?buffer=n2_buffer", follow_redirects=False)
        assert resp.status_code == 302

    def test_unknown_node_redirects(self, client):
        _login(client)
        resp = client.get("/plot/cx/nope?buffer=n2_buffer", follow_redirects=False)
        assert resp.status_code == 302

    def test_page_structure(self, client):
        _login(client)
        body = client.get("/plot/cx/cx1?buffer=n2_buffer").data.decode()
        # Standalone page: the same container bufferplot.js renders into
        # on the pipeline page, plus the attributes that name the source
        # and make it open full screen instead of waiting for a click.
        assert 'id="buffer-plot"' in body
        assert 'data-source-url="/api/node-buffer-data/cx/cx1?buffer=n2_buffer"' in body
        assert 'data-source-id="cx/cx1|n2_buffer"' in body
        assert 'data-fullscreen="1"' in body
        assert "bufferplot.js" in body
        # Slim nav, no base.html chrome, dark by default.
        assert 'class="brand-pill"' in body
        assert "/pipeline/cx/cx1" in body and "/nodes/edit/cx/cx1" in body
        assert 'data-theme="dark"' in body
        # The view lives in the fragment, so the server renders nothing
        # for it — no query-string plumbing to get wrong.
        assert "pipeline.js" not in body

    def test_bad_buffer_name_rejected(self, client):
        _login(client)
        # Same allowlist as the data API: a name that could break out of
        # an attribute never reaches the template.
        for name in ['"><script>', "../etc", "a b", ""]:
            resp = client.get(
                "/plot/cx/cx1", query_string={"buffer": name},
                follow_redirects=False,
            )
            assert resp.status_code == 302, name
            assert "/pipeline/cx/cx1" in resp.headers["Location"]

    def test_fragment_is_not_the_server_s_business(self, client):
        _login(client)
        # A fragment never reaches the server at all, but the route must
        # not care if one is somehow passed through as a query either.
        resp = client.get("/plot/cx/cx1?buffer=n2_buffer&dims=F:x,E:y&zoom=1:9")
        assert resp.status_code == 200


class TestNodeBufferDataApi:
    """The frame-data proxy behind the live buffer plots.

    Requests come from 127.0.0.1 so the localhost bypass applies (no
    login needed), same as the other /api/ routes.
    """

    RAW = bytes(range(16))

    @classmethod
    def frame(cls, **extra):
        import base64
        f = {
            "buffer": "n2_buffer", "frame_id": 7, "frame_size": 100756,
            "data_length": len(cls.RAW),
            "data": base64.b64encode(cls.RAW).decode("ascii"),
            "encoding": "base64",
            "metadata": {"fpga_seq_start": 12345},
            "frame_desc": {"frame_desc_type": "ndarray",
                           "value_type": "int32", "extents": [4, 2]},
        }
        f.update(extra)
        return f

    def test_unknown_node_404(self, client):
        resp = client.get("/api/node-buffer-data/cx/nope?buffer=b")
        assert resp.status_code == 404

    def test_bad_buffer_name_400(self, client):
        for bad in ("", "a/b", "a b", "a%2Fb/../kill"):
            resp = client.get(f"/api/node-buffer-data/cx/cx1?buffer={bad}")
            assert resp.status_code == 400, bad

    def test_bad_len_400(self, client):
        for bad in ("abc", "-1", "1.5"):
            resp = client.get(
                f"/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len={bad}")
            assert resp.status_code == 400, bad

    def test_len_defaults_and_clamps(self, client):
        from unittest.mock import patch
        from choco.state import Node
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()) as gbf:
            client.get("/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        gbf.assert_called_once_with("n2_buffer", length=4 * 1024 * 1024)
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()) as gbf:
            client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=999999999")
        gbf.assert_called_once_with("n2_buffer", length=32 * 1024 * 1024)

    def test_len_zero_returns_descriptor_json(self, client):
        from unittest.mock import patch
        from choco.state import Node
        frame = self.frame()
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame",
                          return_value=frame) as gbf:
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=0")
        gbf.assert_called_once_with("n2_buffer", length=0)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["frame_desc"]["value_type"] == "int32"
        assert data["frame_id"] == 7

    def test_data_returned_as_raw_bytes(self, client):
        from unittest.mock import patch
        from choco.state import Node
        with patch.object(Node, "get_buffer_frame",
                          return_value=self.frame()):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=16")
        assert resp.status_code == 200
        assert resp.content_type == "application/octet-stream"
        assert resp.data == self.RAW
        assert resp.headers["X-Frame-Id"] == "7"
        assert resp.headers["X-Frame-Size"] == "100756"

    def test_no_full_frame_404_json(self, client):
        from unittest.mock import patch
        from choco.state import Node
        with patch.object(Node, "get_buffer_frame",
                          return_value={"error": "no full frame currently in buffer"}):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        assert resp.status_code == 404
        assert "no full frame" in resp.get_json()["error"]

    def test_unreachable_502(self, client):
        from unittest.mock import patch
        from choco.state import Node
        with patch.object(Node, "get_buffer_frame", return_value=None):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer")
        assert resp.status_code == 502

    def test_missing_data_field_502(self, client):
        from unittest.mock import patch
        from choco.state import Node
        frame = self.frame()
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame", return_value=frame):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_buffer&len=16")
        assert resp.status_code == 502
        assert "base64" in resp.get_json()["error"]


class TestServicesPartial:
    def test_requires_login(self, client):
        resp = client.get("/partials/services", follow_redirects=False)
        assert resp.status_code == 302

    def test_renders_strip_when_logged_in(self, client, app):
        from unittest.mock import patch
        _login(client)
        # No fpga_master configured in the test app, so monitor is in
        # 'unknown' / unconfigured state.  Stub job_status so we don't
        # depend on the host's systemd or fs state.
        with patch("choco.web.job_status",
                   return_value={"health": "ok", "state_mtime": None,
                                 "result": "success", "systemd": True,
                                 "active_state": None, "sub_state": None,
                                 "exit_status": None, "state_file": None,
                                 "unit": "test.service"}):
            resp = client.get("/partials/services")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "EOP" in body
        assert "BFFS" in body
        assert "EIGENCAL" in body
        # Monitor badges are only rendered if a monitor is set on
        # app.config.  The test app installs both (unconfigured).
        assert "FPGA" in body
        assert "PDB" in body
        # Badges link to the service pages.
        assert '/service/eop' in body
        assert '/service/fpga' in body
        assert '/service/pdb' in body
        # The cluster itself is a badge too, linking to the dashboard.
        assert "NODES" in body

    # --- The NODES badge: green all up, red all down, yellow between ---

    _JOB_STUB = {"health": "ok", "state_mtime": None, "result": "success",
                 "systemd": True, "active_state": None, "sub_state": None,
                 "exit_status": None, "state_file": None,
                 "unit": "test.service"}

    def _nodes_badge(self, client):
        """(background colour, label) of the strip's NODES badge."""
        from unittest.mock import patch
        with patch("choco.web.job_status", return_value=dict(self._JOB_STUB)):
            body = client.get("/partials/services").data.decode()
        color = re.search(
            r'<a href="/nodes"[^>]*background: (#[0-9a-f]{3,6})', body)
        label = re.search(r'<strong>NODES</strong> <span>([^<]*)</span>', body)
        assert color and label
        return color.group(1), label.group(1)

    def _set_statuses(self, app, statuses):
        from choco.state import NodeStatus
        nodes = list(app.config["registry"].nodes.values())
        assert len(statuses) == len(nodes)
        for node, status in zip(nodes, statuses):
            node.status = NodeStatus[status]

    def test_nodes_badge_all_up_is_green(self, client, app):
        _login(client)
        self._set_statuses(app, ["STARTED", "STARTED", "STARTED"])
        assert self._nodes_badge(client) == ("#008000", "all up")

    def test_nodes_badge_all_down_is_red(self, client, app):
        _login(client)
        self._set_statuses(app, ["DOWN", "DOWN", "DOWN"])
        assert self._nodes_badge(client) == ("#ff4136", "all down")

    def test_nodes_badge_partial_up_is_yellow(self, client, app):
        _login(client)
        self._set_statuses(app, ["STARTED", "DOWN", "IDLE"])
        assert self._nodes_badge(client) == ("#ffdc00", "1/3 up")

    def test_nodes_badge_all_idle_is_yellow(self, client, app):
        _login(client)
        self._set_statuses(app, ["IDLE", "IDLE", "IDLE"])
        assert self._nodes_badge(client) == ("#ffdc00", "idle")

    def test_nodes_badge_unpolled_is_grey(self, client, app):
        # Fresh registry: every node still UNKNOWN until the first poll.
        _login(client)
        assert self._nodes_badge(client) == ("#aaa", "unknown")


class TestMaintenanceToggles:
    def test_toggle_flips_per_node(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        node = registry.get_node("cx/cx1")
        node.maintenance = False

        resp = client.post(
            "/nodes/toggle-maintenance/cx/cx1",
            data={"_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert node.maintenance is True

    def test_set_group_scopes_to_group(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.maintenance = False

        resp = client.post(
            "/nodes/set-maintenance-group/cx/on",
            data={"_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert registry.get_node("cx/cx1").maintenance is True
        assert registry.get_node("cx/cx2").maintenance is True
        # recv group untouched.
        assert registry.get_node("recv/recv1").maintenance is False

    def test_set_all_flips_every_node(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.maintenance = False

        client.post(
            "/nodes/set-maintenance-all/on",
            data={"_csrf_token": token},
        )
        assert all(n.maintenance for n in registry.nodes.values())

        client.post(
            "/nodes/set-maintenance-all/off",
            data={"_csrf_token": token},
        )
        assert not any(n.maintenance for n in registry.nodes.values())

    def test_bad_action_400(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/set-maintenance-all/frobnicate",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 400

    def test_json_api_set_maintenance_per_node(self, client, app):
        _login(client)
        registry = app.config["registry"]
        node = registry.get_node("cx/cx1")
        node.maintenance = False

        resp = client.post(
            "/update/cx/cx1",
            json={"action": "set_maintenance", "maintenance": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["maintenance"] is True
        assert node.maintenance is True

    def test_json_api_rejects_non_bool(self, client):
        _login(client)
        resp = client.post(
            "/update/cx/cx1",
            json={"action": "set_maintenance", "maintenance": "yes"},
        )
        assert resp.status_code == 400


    def test_json_api_flags_wake_the_worker(self, client, app):
        """set_started / set_maintenance through /update enqueue a POLL,
        exactly as the dashboard toggles do, so the change takes effect
        now rather than at the node's next scheduled check (which for a
        backed-off node is up to max_retry_interval away)."""
        submitted = []
        orch = app.config["orchestrator"]
        orch.submit_node = lambda key, item: submitted.append((key, item))
        orch
        assert client.post("/update/cx/cx1", json={
            "action": "set_started", "started": True}).status_code == 200
        assert client.post("/update/cx", json={
            "action": "set_maintenance", "maintenance": False}).status_code == 200
        assert client.post("/update/recv/recv1", json={
            "action": "set_maintenance", "maintenance": False}).status_code == 200
        assert client.post("/update/recv", json={
            "action": "set_started", "started": False}).status_code == 200
        assert [(i.type, k) for k, i in submitted] == [
            (ChangeType.POLL, "cx/cx1"),
            (ChangeType.POLL, "cx/cx1"), (ChangeType.POLL, "cx/cx2"),
            (ChangeType.POLL, "recv/recv1"),
            (ChangeType.POLL, "recv/recv1"),
        ]
        # A rejected value wakes nothing.
        submitted.clear()
        assert client.post("/update/cx", json={
            "action": "set_started", "started": "yes"}).status_code == 400
        assert submitted == []


# --- POST /oneshot/<group>[/<node>] ---

class TestOneshot:
    """Start a config on paused, idle nodes without recording it.

    Requests come from 127.0.0.1 so the localhost bypass applies; tests
    log in anyway so the audit line names a user.  Node REST methods are
    patched class-wide rather than per instance: the route's fan-out
    yields to the hub, which lets the real sync workers run a cycle, and
    those must not reach the network either.
    """

    CONTENT = "num_elements: 4096\nlog_level: debug\n"
    RENDERED = {"num_elements": 4096, "log_level": "debug"}

    @contextlib.contextmanager
    def _cluster(self, app, statuses=None, start_ok=True):
        """Probe answers per node key (default IDLE); every REST write
        is recorded instead of sent.

        Only writes from *this* app's nodes are recorded: every test
        builds an app whose sync workers outlive it, and those stale
        workers see the class-level patch too -- a leftover, unpaused
        cx1 told it is "running" would /kill itself and pollute the
        record.
        """
        from unittest.mock import patch
        from choco.state import Node, NodeStatus
        statuses = statuses or {}
        mine = {id(n) for n in app.config["registry"].nodes.values()}
        calls = {"start": [], "kill": []}

        def get_status(node):
            return statuses.get(node.key, NodeStatus.IDLE)

        def start(node, config, *, override_maintenance=False):
            if id(node) in mine:
                calls["start"].append((node.key, config, override_maintenance))
            return start_ok

        def kill(node):
            if id(node) in mine:
                calls["kill"].append(node.key)
            return True

        with patch.object(Node, "get_status", get_status), \
             patch.object(Node, "start", start), \
             patch.object(Node, "kill", kill), \
             patch.object(Node, "get_version_info", lambda self: None):
            yield calls

    @staticmethod
    def _pause(app, *keys):
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.maintenance = n.key in keys

    def test_unknown_targets_404(self, client):
        _login(client)
        assert client.post("/oneshot/nope",
                           json={"config_content": self.CONTENT}).status_code == 404
        assert client.post("/oneshot/cx/nope",
                           json={"config_content": self.CONTENT}).status_code == 404

    def test_unrenderable_text_is_400_and_contacts_nothing(self, client, app):
        _login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app) as calls:
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": "not: [valid"})
        assert resp.status_code == 400
        assert "Invalid config" in resp.get_json()["error"]
        assert calls["start"] == []

    def test_empty_body_is_400(self, client, app):
        _login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app) as calls:
            resp = client.post("/oneshot/cx/cx1", json={})
        assert resp.status_code == 400
        assert calls["start"] == []

    def test_skips_node_not_in_maintenance(self, client, app):
        _login(client)
        self._pause(app)  # nobody paused
        with self._cluster(app) as calls:
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 409
        body = resp.get_json()
        assert body["started"] == []
        assert body["skipped"] == {"cx/cx1": "not in maintenance"}
        assert calls["start"] == []

    def test_skips_running_and_unreachable_never_kills(self, client, app):
        from choco.state import NodeStatus
        _login(client)
        self._pause(app, "cx/cx1", "cx/cx2")
        with self._cluster(app, {"cx/cx1": NodeStatus.STARTED,
                            "cx/cx2": NodeStatus.DOWN}) as calls:
            resp = client.post("/oneshot/cx",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 409
        assert resp.get_json()["skipped"] == {"cx/cx1": "running",
                                              "cx/cx2": "unreachable"}
        assert calls["start"] == []
        assert calls["kill"] == []

    def test_starts_idle_node_and_records_nothing(self, client, app, configs_dir, caplog):
        _login(client)
        self._pause(app, "cx/cx1")
        node = app.config["registry"].get_node("cx/cx1")
        file_before = (configs_dir / "cx" / "cx1.yaml").read_text()
        base_before, rendered_before = node.base_content, node.rendered_config
        submitted = []
        app.config["orchestrator"].submit_node = lambda key, item: submitted.append((key, item))

        with self._cluster(app) as calls, \
             caplog.at_level(logging.WARNING, logger="choco.web"):
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["started"] == ["cx/cx1"]
        assert body["skipped"] == {}

        # The rendered text reached kotekan, through the override.
        assert calls["start"] == [("cx/cx1", self.RENDERED, True)]
        assert calls["kill"] == []

        # Nothing recorded: file, updatable store, in-memory config.
        assert (configs_dir / "cx" / "cx1.yaml").read_text() == file_before
        assert not (configs_dir / "cx" / ".updatable").exists()
        assert node.base_content == base_before
        assert node.rendered_config == rendered_before
        assert node.started is False

        # The audit line is the only trace, so it carries the hash.
        digest = hashlib.sha256(self.CONTENT.encode()).hexdigest()[:12]
        assert body["sha256"] == digest
        line = next(r.getMessage() for r in caplog.records
                    if "oneshot by" in r.getMessage())
        assert "oneshot by tester" in line
        assert digest in line and "cx/cx1" in line

        # The worker is asked to look, not told what it will see.
        assert [(i.type, k) for k, i in submitted] == \
            [(ChangeType.POLL, "cx/cx1")]

    def test_group_fans_out_per_node(self, client, app):
        from choco.state import NodeStatus
        _login(client)
        self._pause(app, "cx/cx1", "cx/cx2", "recv/recv1")
        with self._cluster(app, {"cx/cx2": NodeStatus.STARTED}) as calls:
            resp = client.post("/oneshot/cx",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["started"] == ["cx/cx1"]
        assert body["skipped"] == {"cx/cx2": "running"}
        # Only the group's idle node; recv1 is paused and idle but not asked.
        assert [c[0] for c in calls["start"]] == ["cx/cx1"]

    def test_start_failure_is_reported(self, client, app):
        _login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app, start_ok=False):
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 409
        assert resp.get_json()["skipped"] == {"cx/cx1": "/start failed"}

    def test_edit_page_offers_the_button(self, client):
        _login(client)
        body = client.get("/nodes/edit/cx/cx1").data.decode()
        assert 'value="oneshot"' in body

    def test_edit_page_button_starts_without_saving(self, client, app, configs_dir):
        _login(client)
        token = _csrf(client)
        self._pause(app, "cx/cx1")
        file_before = (configs_dir / "cx" / "cx1.yaml").read_text()
        with self._cluster(app) as calls:
            resp = client.post(
                "/nodes/edit/cx/cx1",
                data={"_csrf_token": token, "action": "oneshot",
                      "config_content": self.CONTENT},
                follow_redirects=True,
            )
        assert resp.status_code == 200
        assert calls["start"] == [("cx/cx1", self.RENDERED, True)]
        assert (configs_dir / "cx" / "cx1.yaml").read_text() == file_before
        assert "One-off config started on cx/cx1" in resp.data.decode()

    def test_edit_page_button_explains_a_skip(self, client, app):
        _login(client)
        token = _csrf(client)
        self._pause(app)  # cx1 not paused
        with self._cluster(app) as calls:
            resp = client.post(
                "/nodes/edit/cx/cx1",
                data={"_csrf_token": token, "action": "oneshot",
                      "config_content": self.CONTENT},
                follow_redirects=True,
            )
        assert calls["start"] == []
        assert "not in maintenance" in resp.data.decode()


# --- GET / POST /edit-group/<group> ---

class TestGroupEdit:
    def test_requires_login(self, client):
        resp = client.get("/nodes/edit-group/cx", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_unknown_group_redirects(self, client):
        _login(client)
        resp = client.get("/nodes/edit-group/nope", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].rstrip("/").endswith("")  # → "/"

    def test_get_renders_empty_textarea(self, client):
        _login(client)
        resp = client.get("/nodes/edit-group/cx")
        assert resp.status_code == 200
        body = resp.data.decode()
        # The seeded cx1.yaml has `num_elements` — it must NOT leak into the form.
        assert "num_elements" not in body
        # Empty textarea (the placeholder text is ok, but the value between tags is empty).
        assert "<textarea" in body

    def test_post_invalid_redisplays_form(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit-group/cx",
            data={"config_content": "not_a_mapping", "_csrf_token": token},
        )
        assert resp.status_code == 200
        assert b"Invalid config" in resp.data
        assert b"not_a_mapping" in resp.data

    def test_post_queues_and_redirects(self, client, app):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes/edit-group/cx",
            data={"config_content": "num_elements: 512\n", "_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert resp.headers["Location"].rstrip("/").endswith("")  # → "/"

        # Every cx node has a BASE_CONFIG item queued; recv is untouched.
        registry = app.config["registry"]
        for key in ("cx/cx1", "cx/cx2"):
            node = registry.get_node(key)
            assert not node.queue_empty
            item = node.queue_pop()
            assert item.type == ChangeType.BASE_CONFIG
            assert item.config_content == "num_elements: 512\n"
        assert registry.get_node("recv/recv1").queue_empty

    def test_post_bad_csrf_rejected(self, client):
        _login(client)
        _csrf(client)
        resp = client.post(
            "/nodes/edit-group/cx",
            data={"config_content": "num_elements: 1\n", "_csrf_token": "bogus"},
        )
        assert resp.status_code == 403


# --- Service logs partial ---

_JOB_OK = {"health": "ok", "state_mtime": None, "result": "success",
           "systemd": True, "active_state": None, "sub_state": None,
           "exit_status": None, "state_file": None, "unit": "test.service"}


class TestServiceLogs:
    def test_requires_login(self, client):
        resp = client.get("/partials/service-logs/eop", follow_redirects=False)
        assert resp.status_code == 302

    def test_unknown_name_is_404(self, client):
        _login(client)
        resp = client.get("/partials/service-logs/not-a-service")
        assert resp.status_code == 404

    def test_renders_journal_lines(self, client):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_logs",
                   return_value=["alpha entry", "beta entry"]) as jl:
            resp = client.get("/partials/service-logs/eop")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "alpha entry" in body
        assert "beta entry" in body
        jl.assert_called_once_with("choco-eop-broadcast.service", lines=100)

    def test_lines_query_param_clamped(self, client):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_logs", return_value=[]) as jl:
            client.get("/partials/service-logs/eop?lines=500")
            client.get("/partials/service-logs/eop?lines=999999")
            client.get("/partials/service-logs/eop?lines=bogus")
        assert [c.kwargs["lines"] for c in jl.call_args_list] == [500, 1000, 100]

    def test_journal_unavailable_message(self, client):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_logs", return_value=None):
            resp = client.get("/partials/service-logs/bffs")
        assert resp.status_code == 200
        assert b"Journal unavailable" in resp.data

    def test_choco_unit_viewable(self, client):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_logs", return_value=["choco line"]) as jl:
            resp = client.get("/partials/service-logs/choco")
        assert resp.status_code == 200
        jl.assert_called_once_with("choco.service", lines=100)


# --- /service/<name> pages ---

_JOB_STUB = {
    "health": "ok", "state_mtime": None, "result": "success",
    "systemd": True, "active_state": "inactive", "sub_state": "dead",
    "exit_status": "0", "state_file": None, "unit": "test.service",
}


class TestServicePage:
    def test_requires_login(self, client):
        resp = client.get("/service/eop", follow_redirects=False)
        assert resp.status_code == 302

    def test_unknown_name_is_404(self, client):
        _login(client)
        resp = client.get("/service/not-a-service")
        assert resp.status_code == 404

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal",
                                  "waterfall"])
    def test_job_pages_render(self, client, name):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get(f"/service/{name}")
        assert resp.status_code == 200
        assert name.upper() in resp.data.decode()

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal",
                                  "waterfall"])
    def test_status_partial_renders(self, client, name):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get(f"/partials/service-status/{name}")
        assert resp.status_code == 200
        assert "Unit" in resp.data.decode()

    def test_status_partial_unknown_404(self, client):
        _login(client)
        resp = client.get("/partials/service-status/nope")
        assert resp.status_code == 404

    def test_fpga_page_renders(self, client):
        _login(client)
        resp = client.get("/service/fpga")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "FPGA" in body
        assert "not configured" in body

    def test_pdb_page_renders(self, client):
        _login(client)
        resp = client.get("/service/pdb")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "PDB" in body
        assert "not configured" in body

    def test_pdb_page_channel_grid(self, client, app):
        _login(client)
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

    def test_psu_page_redirects_to_pdb(self, client):
        """The page was /service/psu before the rename."""
        _login(client)
        resp = client.get("/service/psu", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/service/pdb")

    def test_timer_facts_shown(self, client):
        from unittest.mock import patch
        _login(client)
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

    def test_bffs_detail_from_state_file(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
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
        app.config["bffs_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "f1, f9" in body
        assert "bffs-1700000077750" in body
        assert "Recent transitions" in body

    def test_bffs_detail_shows_flag_attribution(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state = {
            "updated": 1700000077.7,
            "bad_inputs": ["A1X", "A3X"],
            "flagged_by": {"A1X": ["power-outlier", "manual"],
                           "A3X": ["rfi"]},
        }
        state_file = tmp_path / "bffs-state.json"
        state_file.write_text(json.dumps(state))
        app.config["bffs_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "power-outlier, manual" in body
        assert "rfi" in body

    def test_eigencal_detail_from_state_file(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state = {"updated": 1700000200.0, "transit_time": 1700000100.0,
                 "source": "CYG_A", "good_frac": 0.87, "sent": True}
        state_file = tmp_path / "eigencal-state.json"
        state_file.write_text(json.dumps(state))
        app.config["eigencal_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/eigencal")
        body = resp.data.decode()
        assert "CYG_A" in body
        assert "87.0%" in body

    def test_waterfall_detail_from_state_file(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state = {"updated": 1700000200.0, "roots": ["subset"],
                 "waterfalls_dir": "/mnt/cs00/data/kotekan_vis_files/waterfalls",
                 "files_rendered": 3, "acquisitions_touched": 2, "backlog": 17,
                 "run_seconds": 24.5,
                 "last_acquisition": "acq_20260723_232332_046022478",
                 "last_file_idx": 4202415, "errors": []}
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(state))
        app.config["waterfall_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/waterfall")
        body = resp.data.decode()
        assert "17 files waiting" in body
        assert "acq_20260723_232332_046022478" in body
        assert "subset" in body

    def test_waterfall_detail_reports_being_up_to_date(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(
            {"backlog": 0, "files_rendered": 0, "acquisitions_touched": 0}))
        app.config["waterfall_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/waterfall")
        assert "up to date" in resp.data.decode()

    def test_waterfall_detail_lists_skipped_files(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state_file = tmp_path / "waterfall-state.json"
        state_file.write_text(json.dumps(
            {"backlog": 0, "files_rendered": 1, "acquisitions_touched": 1,
             "errors": [f"vis_{i}.h5: cannot be widened" for i in range(14)]}))
        app.config["waterfall_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/waterfall")
        body = resp.data.decode()
        assert "Skipped" in body
        assert "cannot be widened" in body
        assert "14 in total" in body          # capped at 10, rest counted

    def test_raw_state_file_shown(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        # a key the eigencal summary never surfaces, so finding it in the
        # response proves the raw file dump is rendered
        state = {"source": "CYG_A", "sent": True, "raw_marker": 4242}
        state_file = tmp_path / "eigencal-state.json"
        state_file.write_text(json.dumps(state))
        app.config["eigencal_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/eigencal")
        body = resp.data.decode()
        assert "State file" in body
        assert "raw_marker" in body
        assert "4242" in body

    def test_eop_detail_table_span(self, client, app, configs_dir):
        from unittest.mock import patch
        _login(client)
        table = [{"t_inst_ns": 1700000000_000000000},
                 {"t_inst_ns": 1700345600_000000000}]
        (configs_dir / "eop-state.json").write_text(
            json.dumps({"earth_orientation_parameter_table": table}))
        app.config["eop_cfg"] = {"state_file": "eop-state.json"}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/eop")
        body = resp.data.decode()
        assert "2 entries" in body

    def test_choco_detail_counts(self, client):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)):
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
        ("eigencal", {"updated": [], "transit_time": "noon",
                      "good_frac": "most", "sent": "yes"}),
        ("waterfall", {"updated": "just now", "roots": 7, "errors": "none",
                       "backlog": "lots"}),
        ("waterfall", {"errors": [None, {"a": 1}], "roots": [None]}),
    ])
    def test_garbage_state_files_never_break_the_page(
            self, client, app, tmp_path, name, state):
        from unittest.mock import patch
        _login(client)
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(state))
        if name == "eop":
            app.config["eop_cfg"] = {"state_file": state_file.name}
            app.config["configs_dir"] = tmp_path
        else:
            app.config[f"{name}_cfg"] = {"state_file": str(state_file)}
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get(f"/service/{name}")
        assert resp.status_code == 200

    def test_pdb_stale_grid_warning(self, client, app):
        _login(client)
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.health = "down"
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [False] * 8},
        ]}
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert "isn't currently readable" in body
        # grid still rendered from the last read
        assert "SPI bus 0" in body


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

    def test_requires_login(self, client):
        resp = client.get("/api/fpga/gain-data?dataset=gain_exp",
                          follow_redirects=False)
        assert resp.status_code == 302

    def test_descriptor_speaks_the_buffer_protocol(self, client, app):
        self._configure(app)
        _login(client)
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
        _login(client)
        resp = client.get("/api/fpga/gain-data?dataset=gain_exp&len=4")
        assert resp.status_code == 200
        assert resp.mimetype == "application/octet-stream"
        assert resp.data == b"abcd"            # honours the len prefix
        assert resp.headers["X-Frame-Id"] == "digitalgain_20260808T053625.917371Z"
        assert resp.headers["X-Frame-Size"] == "8"
        assert resp.headers["Cache-Control"] == "no-store"

    def test_unknown_dataset_404(self, client, app):
        self._configure(app)
        _login(client)
        assert client.get("/api/fpga/gain-data?dataset=nope&len=0").status_code == 404

    def test_bad_dataset_name_rejected(self, client, app):
        self._configure(app)
        _login(client)
        # The name reaches h5py, so it gets the same allowlist treatment
        # as a buffer name or a journal unit.
        for name in ['"><script>', "a b", "", "x;y"]:
            resp = client.get("/api/fpga/gain-data",
                              query_string={"dataset": name, "len": 0})
            assert resp.status_code == 400, name

    def test_negative_len_rejected(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.get("/api/fpga/gain-data?dataset=gain_exp&len=-1")
        assert resp.status_code == 400

    def test_unconfigured_is_404_not_a_crash(self, client, app):
        _login(client)
        assert client.get("/api/fpga/gain-data?dataset=x&len=0").status_code == 404

    def test_page_defers_the_card(self, client, app):
        self._configure(app)
        _login(client)
        body = client.get("/service/fpga").data.decode()
        # Filling the card means pulling 8.4 MB from fpga_master, so the
        # page must not wait for it — it asks for the card separately
        # and loads the plot module ready for when it lands.
        assert 'hx-get="/partials/fpga-gains"' in body
        assert "bufferplot.js" in body
        assert "digitalgain_20260808T053625.917371Z" not in body

    def test_card_shows_the_archive(self, client, app):
        self._configure(app)
        _login(client)
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
        _login(client)
        body = client.get("/partials/fpga-gains").data.decode()
        assert "unavailable" in body
        assert "no route to host" in body

    def test_page_without_an_archive_still_renders(self, client, app):
        _login(client)
        app.config["fpga_monitor"].host = "fpga.example"
        app.config["fpga_monitor"].port = 54321
        body = client.get("/service/fpga").data.decode()
        card = client.get("/partials/fpga-gains").data.decode()
        assert "Digital gains" not in card            # nothing to show
        assert "F-engine" in body or "FPGA" in body   # ...page is fine

    def test_fullscreen_page_reuses_the_plot_template(self, client, app):
        self._configure(app)
        _login(client)
        body = client.get("/service/fpga/plot?dataset=gain_coeff").data.decode()
        assert 'data-source-url="/api/fpga/gain-data?dataset=gain_coeff"' in body
        assert 'data-source-id="fpga-gain|gain_coeff"' in body
        assert 'data-fullscreen="1"' in body
        assert "bufferplot.js" in body

    def test_fullscreen_bad_dataset_redirects(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.get("/service/fpga/plot?dataset=a b",
                          follow_redirects=False)
        assert resp.status_code == 302
        assert "/service/fpga" in resp.headers["Location"]

    def test_download_serves_the_file(self, client, app, tmp_path):
        archive = self._configure(app)
        h5 = tmp_path / "gain.h5"
        h5.write_bytes(b"\x89HDF\r\n\x1a\n rest")
        archive._path = h5
        _login(client)
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

    def test_requires_login(self, client):
        resp = client.post("/service/fpga/start", follow_redirects=False)
        assert resp.status_code == 302

    def test_requires_csrf(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.post("/service/fpga/start", data={})
        assert resp.status_code == 403

    def test_unknown_action_404(self, client, app):
        self._configure(app)
        _login(client)
        token = _csrf(client)
        resp = client.post("/service/fpga/reboot",
                           data={"_csrf_token": token})
        assert resp.status_code == 404

    def test_control_disabled_403(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app, control=False)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "start_master") as sm:
            resp = client.post("/service/fpga/start",
                               data={"_csrf_token": token})
        assert resp.status_code == 403
        sm.assert_not_called()

    def test_start_calls_monitor(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
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
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
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
        _login(client)
        from unittest.mock import patch
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
        _login(client)
        resp = client.get("/service/fpga")
        body = resp.data.decode()
        assert "/service/fpga/start" in body
        assert "/service/fpga/stop" in body
        assert "new frame0" in body

    def test_controls_hidden_when_disabled(self, client, app):
        self._configure(app, control=False)
        _login(client)
        resp = client.get("/service/fpga")
        body = resp.data.decode()
        assert "/service/fpga/start" not in body

    def test_status_partial_renders_and_polls(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        monitor.state = "on"
        _login(client)
        with patch.object(monitor, "poll_if_stale") as pis:
            resp = client.get("/partials/service-fpga")
        assert resp.status_code == 200
        assert "on" in resp.data.decode()
        pis.assert_called_once_with(5)


class TestPdbControl:
    def _configure(self, app, control=True):
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
        ]}
        app.config["pdb_cfg"] = {"host": monitor.host, "port": monitor.port,
                                 "control": control}
        return monitor

    def _form(self, token, **overrides):
        form = {"_csrf_token": token, "bus": "0", "board": "0",
                "chip": "A", "channel": "3", "state": "on"}
        form.update(overrides)
        return form

    def test_requires_login(self, client):
        resp = client.post("/service/pdb/set", follow_redirects=False)
        assert resp.status_code == 302

    def test_requires_csrf(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.post("/service/pdb/set", data={"bus": "0"})
        assert resp.status_code == 403

    def test_control_disabled_403(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app, control=False)
        _login(client)
        token = _csrf(client)
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
        _login(client)
        token = _csrf(client)
        resp = client.post("/service/pdb/set", data=self._form(token, **bad))
        assert resp.status_code == 400

    def test_toggle_calls_set_channel(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(True, "bus 0 board 0 chip A ch3 on")) as sc:
            resp = client.post("/service/pdb/set",
                               data=self._form(token),
                               follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/service/pdb")
        sc.assert_called_once_with(0, 0, "A", 3, True)

    def test_verify_failure_flashes_error(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(False, "verify failed")):
            resp = client.post("/service/pdb/set",
                               data=self._form(token, state="off"),
                               follow_redirects=True)
        assert b"verify failed" in resp.data

    def test_grid_buttons_when_control_enabled(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert '/service/pdb/set' in body
        assert body.count("<button") >= 16
        assert 'name="channel"' in body

    def test_grid_readonly_when_control_disabled(self, client, app):
        self._configure(app, control=False)
        _login(client)
        resp = client.get("/service/pdb")
        body = resp.data.decode()
        assert '/service/pdb/set' not in body
        assert '<span class="chan' in body

    def test_status_partial_renders_and_polls(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
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
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
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
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(False, "verify failed")):
            resp = client.post("/service/pdb/set", data=self._form(token),
                               headers={"HX-Request": "true"})
        body = resp.data.decode()
        assert "flash-error" in body
        assert "verify failed" in body


class TestPdbGroupControl:
    """Bulk power buttons: per chip, per board, and per SPI bus."""

    def _configure(self, app, control=True):
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
            {"board": 1, "chip": "A", "channels": [False] * 8},
            {"board": 1, "chip": "B", "channels": [False] * 8},
        ]}
        app.config["pdb_cfg"] = {"host": monitor.host, "port": monitor.port,
                                 "control": control}
        return monitor

    def test_requires_login(self, client):
        resp = client.post("/service/pdb/set-group", follow_redirects=False)
        assert resp.status_code == 302

    def test_requires_csrf(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.post("/service/pdb/set-group",
                           data={"bus": "0", "state": "on"})
        assert resp.status_code == 403

    def test_control_disabled_403(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app, control=False)
        _login(client)
        token = _csrf(client)
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
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
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
        _login(client)
        token = _csrf(client)
        resp = client.post("/service/pdb/set-group",
                           data={"_csrf_token": token, **form})
        assert resp.status_code == 400

    def test_htmx_reply_swaps_the_grid(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
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
        _login(client)
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
        _login(client)
        body = client.get("/service/pdb").data.decode()
        assert '/service/pdb/set-group' not in body


class TestPdbChannelMap:
    """The master dish-input <-> channel table and its kotekan cross-check."""

    MAP = ("spi_bus,board,chip,channel,dish_input,amplifier,notes\n"
           "0,0,A,0,A1X,AMP-1,\n"
           "0,0,A,1,A1Y,AMP-2,\n")

    def _configure(self, app, configs_dir, map_text=None, dish_inputs=None):
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
        ]}
        app.config["pdb_cfg"] = {"host": monitor.host, "port": monitor.port,
                                 "control": True, "kotekan_group": "cx"}
        if map_text is not None:
            (configs_dir / "pdb_map.csv").write_text(map_text)
        if dish_inputs is not None:
            (configs_dir / "cx" / "cx1.yaml").write_text(
                yaml.safe_dump({"dish_inputs": dish_inputs}))
            app.config["registry"].reload()
        return monitor

    def test_grid_cells_carry_the_dish_input(self, client, app, configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP)
        _login(client)
        body = client.get("/service/pdb").data.decode()
        assert "A1X" in body and "A1Y" in body
        # mapped cells lose the "unmapped" styling; unmapped ones keep it
        assert 'class="chan on"' in body
        assert 'class="chan off unmapped"' in body

    def test_map_problems_are_shown_not_fatal(self, client, app, configs_dir):
        self._configure(app, configs_dir,
                        map_text=self.MAP + "0,0,C,0,BAD,,\n")
        _login(client)
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
        _login(client)
        body = client.get("/service/pdb").data.decode()
        assert "agreed" in body

    def test_cross_check_reports_disagreements(self, client, app,
                                               configs_dir):
        # A2 is connected but unmapped (both pols); the map's rows are
        # for A1, which kotekan has never heard of -> stale.
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A2", "type": "ArrayDish"},
        ])
        _login(client)
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
        _login(client)
        body = client.get("/service/pdb").data.decode()
        assert "agreed" in body

    def test_old_style_table_reports_a_migration_reason(self, client, app,
                                                        configs_dir):
        """A pre-2026-08 per-element table is refused, never checked."""
        self._configure(app, configs_dir, map_text=self.MAP, dish_inputs=[
            {"dish_idx": 0, "label": "A1X"},
            {"dish_idx": 1, "label": "A1Y"},
        ])
        _login(client)
        body = client.get("/service/pdb").data.decode()
        assert "Not cross-checked against kotekan" in body
        assert "migrate the config" in body

    def test_no_dish_inputs_degrades_to_a_reason(self, client, app,
                                                 configs_dir):
        """The stock test config has no dish_inputs table."""
        self._configure(app, configs_dir, map_text=self.MAP)
        _login(client)
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
        _login(client)
        resp = client.get("/service/pdb")
        assert resp.status_code == 200
        assert "FileNotFoundError" in resp.data.decode()

    def test_map_is_reread_when_the_file_changes(self, client, app,
                                                 configs_dir):
        self._configure(app, configs_dir, map_text=self.MAP)
        _login(client)
        assert "A1X" in client.get("/service/pdb").data.decode()
        (configs_dir / "pdb_map.csv").write_text(
            "spi_bus,board,chip,channel,dish_input\n0,0,A,0,RENAMED\n")
        body = client.get("/service/pdb").data.decode()
        assert "RENAMED" in body
        assert "A1X" not in body


# --- Status API + metrics ---
# The test client's requests come from 127.0.0.1, so the JSON API's
# localhost bypass applies (no login needed).

class TestStatusApi:
    def test_api_status_is_a_summary(self, client):
        from unittest.mock import patch
        with patch("choco.web.job_status", return_value=dict(_JOB_OK)):
            resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["up"] is True
        assert data["services"]["eop"] == "ok"
        assert data["services"]["bffs"] == "ok"
        assert data["services"]["eigencal"] == "ok"
        assert "fpga" in data["services"]
        assert data["services"]["pdb"] == "unconfigured"
        assert data["nodes"]["total"] == 3
        # Fresh Registry constructs every node in maintenance mode.
        assert data["nodes"]["maintenance"] == 3
        # No per-node detail here — that moved to /api/nodes/status.
        assert "nodes" not in data.get("summary", {})

    def test_api_nodes_status_is_detailed(self, client):
        resp = client.get("/api/nodes/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["summary"]["total"] == 3
        assert len(data["nodes"]) == 3
        keys = {n["key"] for n in data["nodes"]}
        assert keys == {"cx/cx1", "cx/cx2", "recv/recv1"}

    def test_api_group_config_returns_desired_config(self, client):
        resp = client.get("/api/config/cx")
        assert resp.status_code == 200
        # The fixture's cx configs carry num_elements; a sample node's
        # rendered config represents the whole group.
        assert resp.get_json()["num_elements"] == 2048

    def test_api_group_config_unknown_group_404(self, client):
        resp = client.get("/api/config/nope")
        assert resp.status_code == 404

    def test_api_group_config_load_error_503(self, client, app):
        node = app.config["registry"].get_node("cx/cx1")
        node._base_load_error = "boom"
        node.base_content = None
        node.rendered_config = None
        resp = client.get("/api/config/cx")
        assert resp.status_code == 503


class TestMetrics:
    def test_no_auth_required(self, client):
        from unittest.mock import patch
        with patch("choco.web.job_status", return_value=dict(_JOB_OK)):
            resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.mimetype == "text/plain"

    def test_exposition_content(self, client):
        from unittest.mock import patch
        failed = dict(_JOB_OK, health="failed")
        with patch("choco.web.job_status", return_value=failed):
            resp = client.get("/metrics")
        body = resp.data.decode()
        assert "choco_up 1" in body
        assert "choco_start_time_seconds" in body
        assert 'choco_service_state{service="eop",state="failed"} 1' in body
        assert 'choco_service_state{service="eop",state="ok"} 0' in body
        assert 'choco_service_state{service="eop",state="degraded"} 0' in body
        assert 'choco_service_state{service="eigencal",state="failed"} 1' in body
        assert 'choco_service_state{service="pdb",state="unconfigured"} 1' in body
        assert 'choco_service_state{service="pdb",state="no_states"} 0' in body
        assert "choco_nodes_total 3" in body
        assert "choco_nodes_maintenance 3" in body
        # One-hot node counts by status are present for every status.
        assert 'choco_nodes{status="unknown"} 3' in body


class TestNodeStatusPartial:
    def test_renders_cached_status_without_probing(self, client, app):
        """The worker is the only writer of node.status; the partial
        must render what it last recorded, not probe kotekan itself."""
        from unittest.mock import patch
        from choco.state import Node, NodeStatus

        _login(client)
        node = app.config["registry"].get_node("cx/cx1")
        node.status = NodeStatus.STARTED
        node.version = "2026.09"
        with patch.object(Node, "get_status") as probe:
            resp = client.get("/nodes/partials/node-status/cx/cx1")
        assert resp.status_code == 200
        probe.assert_not_called()
        body = resp.data.decode()
        assert "status-started" in body
        assert "2026.09" in body

    def test_unknown_node_404s(self, client):
        _login(client)
        assert client.get("/nodes/partials/node-status/cx/nope").status_code == 404
