"""Tests for the web routes: the nodes.yaml editor, node pages, the config
library, services and the JSON API."""

import contextlib
import hashlib
import json
import logging
import re
import time
from pathlib import Path

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


def _job_dir(app, job) -> Path:
    """<state_dir>/<job>/ of the test app, created."""
    d = Path(app.config["state_dir"]) / job
    d.mkdir(parents=True, exist_ok=True)
    return d


def _install_state(app, job, src, name="state.json") -> Path:
    """Put the file at *src* where choco reads *job*'s *name* from: the
    state root is the only setting; the job's file names are convention."""
    dest = _job_dir(app, job) / name
    dest.write_bytes(Path(src).read_bytes())
    return dest


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


class TestVendoredFonts:
    """The site's typefaces ship with the package (static/fonts, OFL); no
    page may reach an outside font host.  Guards the package-data glob."""

    def test_font_files_served(self, client):
        for name in ("ibm-plex-sans-latin-400", "ibm-plex-sans-latin-500",
                     "ibm-plex-sans-latin-600", "ibm-plex-mono-latin-400",
                     "ibm-plex-mono-latin-600"):
            resp = client.get(f"/static/fonts/{name}.woff2")
            assert resp.status_code == 200, name
            assert resp.data[:4] == b"wOF2", name
        assert client.get("/static/fonts/OFL.txt").status_code == 200

    def test_stylesheet_declares_them_and_nothing_external(self, client):
        css = client.get("/static/choco.css").data.decode()
        assert css.count("@font-face") == 5
        assert 'url("fonts/ibm-plex-sans-latin-400.woff2")' in css
        assert "fonts.googleapis.com" not in css and "https://" not in css
        import re, pathlib
        pyproject = pathlib.Path(__file__).parent.parent / "pyproject.toml"
        assert '"static/fonts/*"' in pyproject.read_text()


class TestPageChrome:
    """The review round of 2026-10: one title format, the dashboard's
    desired-vs-actual note, one config line per uniform group, and the
    wall-display mode."""

    def test_titles_name_the_page_then_the_site(self, client, app):
        _login(client)
        for url, title in [("/", "Services — CHOCO"), ("/nodes", "Nodes — CHOCO"),
                           ("/nodes/edit", "Edit nodes — CHOCO"), ("/nodes/edit/cx/cx1", "cx/cx1 — CHOCO"),
                           ("/configs", "Configs — CHOCO"), ("/files", "Data files — CHOCO"),
                           ("/service/eop", "EOP — CHOCO"), ("/pipeline/cx/cx1", "cx/cx1 pipeline — CHOCO")]:
            body = client.get(url).data.decode()
            assert f"<title>{title}</title>" in body, url
        # a logged-in client is bounced off /login; a fresh one sees the form
        assert "<title>Sign in — CHOCO</title>" in app.test_client().get("/login").data.decode()

    def test_wants_note_when_desired_differs_from_actual(self, client, app):
        from choco.state import NodeStatus
        _login(client)
        nodes = list(app.config["registry"].nodes.values())
        nodes[0].started, nodes[0].status = True, NodeStatus.DOWN
        nodes[1].started, nodes[1].status = False, NodeStatus.STARTED
        nodes[2].started, nodes[2].status = True, NodeStatus.STARTED
        body = client.get("/partials/dashboard-table").data.decode()
        assert body.count('class="muted wants">wants started<') == 1
        assert body.count('class="muted wants">wants idle<') == 1
        node_page = client.get("/nodes/partials/node-status/cx/cx1").data.decode()
        assert "wants started" in node_page

    def test_uniform_group_names_its_file_once(self, client, app):
        _login(client)
        body = client.get("/partials/dashboard-table").data.decode()
        # cx: cx1.yaml and cx2.yaml differ -> a Config column; recv: one
        # node, one file -> named in the group bar, no column
        cx = body[body.index("group <code>cx</code>"):body.index("group <code>recv</code>")]
        recv = body[body.index("group <code>recv</code>"):]
        assert "<th>Config</th>" in cx and "<code>cx/cx1.yaml</code>" in cx and "<code>cx/cx2.yaml</code>" in cx
        assert "<th>Config</th>" not in recv
        assert 'renders <code>recv/recv1.yaml</code>' in recv

    def test_wall_mode_drops_the_chrome(self, client):
        _login(client)
        plain = client.get("/").data.decode()
        wall = client.get("/?wall=1").data.decode()
        assert '<body class="wall">' in wall and '<body class="wall">' not in plain
        assert "landing-services" in wall and 'id="skymap"' in wall

    def test_landing_table_has_one_timing_column_and_no_unit_names(self, client):
        from unittest.mock import patch
        _login(client)
        stub = dict(_JOB_STUB, unit="choco-eop-broadcast.service", state_mtime=time.time() - 120)
        with patch("choco.web.job_status", return_value=stub), \
             patch("choco.web.timer_status", return_value={"unit": "x.timer", "next_elapse": "Mon 2026-10-05 06:00:00 UTC"}):
            body = client.get("/partials/landing-services").data.decode()
        heads = re.findall(r"<th>([^<]*)</th>", body)
        assert heads == ["Service", "Status", "Detail", "Timing"]
        assert "choco-eop-broadcast.service" not in body
        assert "next Mon 2026-10-05 06:00:00 UTC" in body


class TestSkymap:
    def _configure(self, app, tmp_path, write=True, night=False):
        # The images live where the job writes them, <state_dir>/skymap/;
        # nothing in config.yaml names them.
        path = _job_dir(app, "skymap") / "skymap.png"
        if write:
            path.write_bytes(PNG_MAGIC + b"fake image data")
        if night and write:
            (path.parent / "skymap-night.png").write_bytes(PNG_MAGIC + b"fake night data")
        return path

    def test_not_rendered_yet_404(self, client):
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

    def test_night_image_served_without_login(self, client, app, tmp_path):
        path = self._configure(app, tmp_path, night=True)
        resp = client.get("/skymap-night.png")
        assert resp.status_code == 200
        assert resp.mimetype == "image/png"
        assert resp.data == (path.parent / "skymap-night.png").read_bytes()
        assert resp.data != path.read_bytes()

    def test_night_image_404_until_configured_and_rendered(
            self, client, app, tmp_path):
        # Day only configured: the night route is simply absent ...
        self._configure(app, tmp_path)
        assert client.get("/skymap-night.png").status_code == 404
        # ... and configured but not yet rendered is the same 404.
        self._configure(app, tmp_path, write=False, night=True)
        assert client.get("/skymap-night.png").status_code == 404

    def test_partial_requires_login(self, client, app, tmp_path):
        self._configure(app, tmp_path)
        resp = client.get("/partials/skymap", follow_redirects=False)
        assert resp.status_code == 302

    def test_partial_names_the_night_render_for_dark_mode(self, client, app, tmp_path):
        # base.html swaps the card's src to data-night-src when the theme is
        # dark; the attribute is there only once a night render exists.
        self._configure(app, tmp_path, night=True)
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert 'data-day-src="/skymap.png?v=' in body
        assert 'data-night-src="/skymap-night.png?v=' in body
        self._configure(app, tmp_path, night=False)
        (_job_dir(app, "skymap") / "skymap-night.png").unlink()
        body = client.get("/partials/skymap").data.decode()
        assert "data-night-src" not in body

    def test_partial_carries_mtime_busted_url(self, client, app, tmp_path):
        path = self._configure(app, tmp_path)
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert f'/skymap.png?v={int(path.stat().st_mtime)}' in body

    def test_partial_links_night_image_only_when_rendered(
            self, client, app, tmp_path):
        _login(client)
        self._configure(app, tmp_path)
        assert "skymap-night" not in client.get("/partials/skymap").data.decode()
        path = self._configure(app, tmp_path, night=True)
        night_mtime = int((path.parent / "skymap-night.png").stat().st_mtime)
        body = client.get("/partials/skymap").data.decode()
        assert f'/skymap-night.png?v={night_mtime}' in body

    def test_partial_without_render_yet(self, client, app, tmp_path):
        self._configure(app, tmp_path, write=False)
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert "No sky map rendered yet" in body

    def test_landing_card_always_present(self, client, app, tmp_path):
        # the job is part of the install, so the card is unconditional:
        # it says "not rendered yet" until the first PNG lands
        _login(client)
        assert 'id="skymap"' in client.get("/").data.decode()
        _login(client)
        body = client.get("/partials/skymap").data.decode()
        assert "No sky map rendered yet" in body

    def test_load_config_carries_skymap_block(self, configs_dir, tmp_path):
        """The production path: config.yaml -> load_config -> create_app.

        load_config copies each known section explicitly, so a section
        it forgot would stay invisible until the page is loaded for real.
        The state root the skymap PNGs are read from travels the same way.
        """
        from choco.app import create_app, load_config
        cfg = tmp_path / "config.yaml"
        cfg.write_text(yaml.safe_dump({
            "configs_dir": str(configs_dir),
            "server": {"secret_key": "k" * 32},  # a placeholder key is refused at load
            "state_dir": str(tmp_path / "state"),
            "skymap": {"service_unit": "choco-skymap.service"},
        }))
        loaded = load_config(cfg)
        assert loaded["skymap"]["service_unit"] == "choco-skymap.service"
        assert loaded["state_dir"] == str(tmp_path / "state")
        app = create_app(config=loaded)
        assert app.config["state_dir"] == tmp_path / "state"
        assert app.config["skymap_cfg"]["service_unit"] == "choco-skymap.service"
        (tmp_path / "state" / "skymap").mkdir(parents=True)
        (tmp_path / "state" / "skymap" / "skymap.png").write_bytes(PNG_MAGIC + b"x")
        assert app.test_client().get("/skymap.png").status_code == 200

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
        assert 'class="brand"' in body and 'href="/"' in body
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
        assert 'class="brand"' in body
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

    DISH_TABLE = [{"label": "B4", "type": "ArrayDish"},
                  {"label": "Missing", "type": "Missing"},
                  {"label": "A1", "type": "ArrayDish"},
                  {"label": "RFIA1", "type": "RFIDish"}]

    def _desc_reply(self, client, desc, config):
        from unittest.mock import patch, PropertyMock
        from choco.state import Node
        frame = self.frame(frame_desc=desc)
        del frame["data"], frame["encoding"]
        with patch.object(Node, "get_buffer_frame", return_value=frame), \
                patch.object(Node, "desired_config", new_callable=PropertyMock,
                             return_value=config):
            resp = client.get(
                "/api/node-buffer-data/cx/cx1?buffer=n2_subset_buffer&len=0")
        assert resp.status_code == 200
        return resp.get_json()["frame_desc"]

    def test_dish_inputs_descriptor_gets_the_implicit_input_list(self, client):
        # kotekan develop sends a compact DishInputs descriptor with only
        # the element count; the identities are implied by the dish table
        # choco pushed, so the reply spells them out for the plotter.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 6, "num_ev": 0}
        config = {"telescope": {"num_polarizations": 2,
                                "dish_inputs": self.DISH_TABLE}}
        out = self._desc_reply(client, desc, config)
        assert out["input_list"] == [0, 2, 3, 4, 6, 7]
        assert out["num_elements"] == 6

    def test_implicit_input_list_needs_the_count_to_agree(self, client):
        # A table that does not reproduce num_elements means the rule has
        # drifted: better no identities than wrong ones.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 48, "num_ev": 0}
        config = {"telescope": {"dish_inputs": self.DISH_TABLE}}
        assert "input_list" not in self._desc_reply(client, desc, config)
        # No table at all (config failed to load, or no telescope block).
        assert "input_list" not in self._desc_reply(client, desc, None)
        assert "input_list" not in self._desc_reply(client, desc, {"a": 1})

    def test_explicit_wire_forms_are_left_alone(self, client):
        config = {"telescope": {"dish_inputs": self.DISH_TABLE}}
        # The kv-branch build already names its elements.
        desc = {"frame_desc_type": "N2", "n2_layout": "DishInputs",
                "num_elements": 6, "num_ev": 0, "input_list": [1, 2, 3, 4, 5, 6]}
        assert self._desc_reply(client, desc, config)["input_list"] == \
            [1, 2, 3, 4, 5, 6]
        # Dense layouts and non-N2 descriptors carry no list.
        desc = {"frame_desc_type": "N2", "n2_layout": "FullUpperTri",
                "num_elements": 6, "num_ev": 0}
        assert "input_list" not in self._desc_reply(client, desc, config)
        desc = {"frame_desc_type": "ndarray", "value_type": "int32",
                "extents": [6]}
        assert "input_list" not in self._desc_reply(client, desc, config)

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
        """(tone, label) of the strip's NODES badge.

        The tone is the ``tag-<tone>`` class the tag macro emits (painted
        by static/choco.css); the label is what the badge says, read from
        aria-label because a quiet (ok) badge shows no word at all.
        """
        from unittest.mock import patch
        with patch("choco.web.job_status", return_value=dict(self._JOB_STUB)):
            body = client.get("/partials/services").data.decode()
        m = re.search(
            r'<a href="/nodes" class="tag tag-([a-z]+)( quiet)?[^"]*"'
            r' aria-label="NODES: ([^"]*)"', body)
        assert m, body
        return m.group(1), m.group(3)

    def _set_statuses(self, app, statuses):
        from choco.state import NodeStatus
        nodes = list(app.config["registry"].nodes.values())
        assert len(statuses) == len(nodes)
        for node, status in zip(nodes, statuses):
            node.status = NodeStatus[status]

    def test_nodes_badge_all_up_is_green(self, client, app):
        _login(client)
        self._set_statuses(app, ["STARTED", "STARTED", "STARTED"])
        assert self._nodes_badge(client) == ("ok", "all up")

    def test_nodes_badge_all_down_is_red(self, client, app):
        _login(client)
        self._set_statuses(app, ["DOWN", "DOWN", "DOWN"])
        assert self._nodes_badge(client) == ("bad", "all down")

    def test_nodes_badge_quiet_when_all_up(self, client, app):
        # A nominal badge shows label and dot only: the word lives in the
        # tooltip and aria-label, and the tint goes neutral.
        from unittest.mock import patch
        _login(client)
        self._set_statuses(app, ["STARTED", "STARTED", "STARTED"])
        with patch("choco.web.job_status", return_value=dict(self._JOB_STUB)):
            body = client.get("/partials/services").data.decode()
        nodes = re.search(r'<a href="/nodes"[^>]*>(.*?)</a>', body, re.S)
        assert nodes and 'class="tag tag-ok quiet"' in nodes.group(0)
        assert "<strong>NODES</strong>" in nodes.group(1)
        assert 'class="word"' not in nodes.group(1)

    def test_running_job_is_a_quiet_blue_dot(self, client, app):
        # A run in flight with no verdict yet is info, and info is quiet in
        # the strip like ok: dot only, "running" in the tooltip/aria-label.
        # Otherwise the word pushed the nine-service strip onto two lines.
        from unittest.mock import patch
        _login(client)
        running = dict(self._JOB_STUB, health="never_run", running=True,
                       unit="choco-eop.service")
        with patch("choco.web.job_status", return_value=running):
            body = client.get("/partials/services").data.decode()
        eop = re.search(r'<a href="/service/eop"[^>]*>(.*?)</a>', body, re.S)
        assert eop and 'class="tag tag-info quiet"' in eop.group(0)
        assert 'aria-label="EOP: running"' in eop.group(0)
        assert 'class="word"' not in eop.group(1)

    def test_nodes_badge_worded_when_not_ok(self, client, app):
        from unittest.mock import patch
        _login(client)
        self._set_statuses(app, ["DOWN", "DOWN", "DOWN"])
        with patch("choco.web.job_status", return_value=dict(self._JOB_STUB)):
            body = client.get("/partials/services").data.decode()
        nodes = re.search(r'<a href="/nodes"[^>]*>(.*?)</a>', body, re.S)
        assert nodes and "quiet" not in nodes.group(0)
        assert '<span class="word">all down</span>' in nodes.group(1)

    def test_nodes_badge_partial_up_is_yellow(self, client, app):
        _login(client)
        self._set_statuses(app, ["STARTED", "DOWN", "IDLE"])
        assert self._nodes_badge(client) == ("warn", "1/3 up")

    def test_nodes_badge_all_idle_is_yellow(self, client, app):
        _login(client)
        self._set_statuses(app, ["IDLE", "IDLE", "IDLE"])
        assert self._nodes_badge(client) == ("warn", "idle")

    def test_nodes_badge_unpolled_is_grey(self, client, app):
        # Fresh registry: every node still UNKNOWN until the first poll.
        _login(client)
        assert self._nodes_badge(client) == ("off", "unknown")


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
        assert 'id="oneshot-select"' in body and 'value="cx/cx1.yaml"' in body

    def test_edit_page_button_starts_a_library_file_without_saving(
            self, client, app, configs_dir):
        _login(client)
        token = _csrf(client)
        self._pause(app, "cx/cx1")
        (configs_dir / "trial.j2").write_text(self.CONTENT)
        file_before = (configs_dir / "cx" / "cx1.yaml").read_text()
        with self._cluster(app) as calls:
            resp = client.post(
                "/nodes/edit/cx/cx1",
                data={"_csrf_token": token, "action": "oneshot",
                      "config": "trial.j2"},
                follow_redirects=True,
            )
        assert resp.status_code == 200
        assert calls["start"] == [("cx/cx1", self.RENDERED, True)]
        assert (configs_dir / "cx" / "cx1.yaml").read_text() == file_before
        assert "One-off trial.j2 started on cx/cx1" in resp.data.decode()
        # The node still renders its own file: nothing was recorded.
        assert app.config["registry"].get_node("cx/cx1").config_filename == "cx/cx1.yaml"

    def test_edit_page_button_explains_a_skip(self, client, app):
        _login(client)
        token = _csrf(client)
        self._pause(app)  # cx1 not paused
        with self._cluster(app) as calls:
            resp = client.post(
                "/nodes/edit/cx/cx1",
                data={"_csrf_token": token, "action": "oneshot",
                      "config": "cx/cx2.yaml"},
                follow_redirects=True,
            )
        assert calls["start"] == []
        assert "not in maintenance" in resp.data.decode()

    def test_edit_page_button_refuses_a_bad_or_missing_file(self, client, app):
        _login(client)
        token = _csrf(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app) as calls:
            for bad in ("../x.yaml", "cx/absent.yaml", "nodes.yaml"):
                resp = client.post(
                    "/nodes/edit/cx/cx1",
                    data={"_csrf_token": token, "action": "oneshot",
                          "config": bad},
                    follow_redirects=True,
                )
                assert "One-off not started" in resp.data.decode()
        assert calls["start"] == []


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
                                  "waterfall", "skymap"])
    def test_job_pages_render(self, client, name):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get(f"/service/{name}")
        assert resp.status_code == 200
        assert name.upper() in resp.data.decode()

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal",
                                  "waterfall", "skymap"])
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
        _install_state(app, "bffs", state_file)
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
        _install_state(app, "bffs", state_file)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "power-outlier, manual" in body
        assert "rfi" in body

    def test_bffs_detail_shows_the_reason_a_source_gave(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
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
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get("/service/bffs")
        body = resp.data.decode()
        assert "power: not in PDB table" in body
        assert "power: off, manual" in body
        assert "(rfi)" in body                       # no reason given: the kind alone

    def test_bffs_detail_renders_the_element_grid(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
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
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
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

    def test_eigencal_detail_from_state_file(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        state = {"updated": 1700000200.0, "transit_time": 1700000100.0,
                 "source": "CYG_A", "good_frac": 0.87, "sent": True}
        state_file = tmp_path / "eigencal-state.json"
        state_file.write_text(json.dumps(state))
        _install_state(app, "eigencal", state_file)
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
        _install_state(app, "waterfall", state_file)
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
        _install_state(app, "waterfall", state_file)
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
        _install_state(app, "waterfall", state_file)
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
        _install_state(app, "eigencal", state_file)
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
        state_file = configs_dir / "eop-state.json"
        state_file.write_text(
            json.dumps({"earth_orientation_parameter_table": table}))
        _install_state(app, "eop", state_file)
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
        ("bffs", {"bad_inputs": ["f1"], "flagged_by": {"f1": ["power"]},
                  "flag_reasons": "lots", "labels": ["f1"]}),
        ("bffs", {"bad_inputs": ["f1"], "flagged_by": {"f1": ["power"]},
                  "flag_reasons": {"f1": "off"}, "labels": ["f1"]}),
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
        _install_state(app, name, state_file)
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
        assert "Controller not readable" in body
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
        _login(client)
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
        _login(client)
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
        monitor = app.config["pdb_monitor"]
        monitor.host, monitor.port = "pdb.example", 5000
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True, False, True, False] + [False] * 4},
            {"board": 0, "chip": "B", "channels": [True] + [False] * 7},
        ]}
        app.config["pdb_cfg"] = {"host": monitor.host, "port": monitor.port,
                                 "control": control}
        (configs_dir / "pdb_map.csv").write_text(self.MAP)
        return monitor

    def test_dish_layout_renders_the_field(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        _login(client)
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
        _login(client)
        for url in ("/service/pdb", "/service/pdb?layout=nope"):
            body = client.get(url).data.decode()
            assert 'class="pdb-grid"' in body and 'class="dish-grid"' not in body
            assert "hx-vals='{\"layout\": \"bulkhead\"}'" in body

    def test_partial_follows_the_layout(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        _login(client)
        body = client.get("/partials/service-pdb?layout=dish").data.decode()
        assert 'class="dish-grid"' in body

    def test_row_power_buttons_per_pol(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        _login(client)
        body = client.get("/service/pdb?layout=dish").data.decode()
        assert "/service/pdb/set-row" in body
        # rows A and RFI have both pols mapped; empty rows get no buttons
        assert body.count(">X on<") == 2 and body.count(">X off<") == 2
        assert body.count(">Y on<") == 2 and body.count(">Y off<") == 2
        assert "all 2 X channels of row A?" in body

    def test_set_row_resolves_channels_through_the_map(self, client, app, configs_dir):
        from unittest.mock import patch
        monitor = self._configure(app, configs_dir)
        _login(client)
        token = _csrf(client)
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
        from unittest.mock import patch
        monitor = self._configure(app, configs_dir)
        _login(client)
        token = _csrf(client)
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
        _login(client)
        token = _csrf(client)
        resp = client.post("/service/pdb/set-row", data={"_csrf_token": token, **form})
        assert resp.status_code == 400

    def test_set_row_requires_csrf_and_control(self, client, app, configs_dir):
        self._configure(app, configs_dir)
        _login(client)
        resp = client.post("/service/pdb/set-row", data={"row": "A", "pol": "X", "state": "on"})
        assert resp.status_code == 403
        self._configure(app, configs_dir, control=False)
        token = _csrf(client)
        resp = client.post("/service/pdb/set-row",
                           data={"_csrf_token": token, "row": "A", "pol": "X", "state": "on"})
        assert resp.status_code == 403


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
        from unittest.mock import patch
        _login(client)
        d = _job_dir(app, "bffs")
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

    def test_strip_tooltip_says_why_when_degraded(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        (_job_dir(app, "bffs") / "run.json").write_text(json.dumps(self.RUN))
        degraded = dict(_JOB_STUB, health="degraded", result="exit-code", exit_status="2")
        with patch("choco.web.job_status", return_value=degraded):
            body = client.get("/partials/services").data.decode()
        assert "why: no usable kotekan file — skipped: power-outlier" in body
        assert "why: rfi: 2 of 14 /sk endpoints unreachable" in body
        # an ok badge carries no reasons, whatever the run file says
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)):
            body = client.get("/partials/services").data.decode()
        assert "why:" not in body

    def test_landing_table_carries_the_reasons(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        (_job_dir(app, "bffs") / "run.json").write_text(json.dumps(self.RUN))
        degraded = dict(_JOB_STUB, health="degraded", result="exit-code", exit_status="2")
        with patch("choco.web.job_status", return_value=degraded), \
             patch("choco.web.timer_status", return_value=None):
            body = client.get("/partials/landing-services").data.decode()
        assert "rfi: 2 of 14 /sk endpoints unreachable" in body

    def test_failed_run_error_is_a_reason(self, client, app, tmp_path):
        from unittest.mock import patch
        _login(client)
        (_job_dir(app, "bffs") / "run.json").write_text(json.dumps({
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
                              "unmapped_flagged": False, "map_source": "bundled placeholder"}) == (
            "80 feeds watched · 0 unpowered · 48 not in the PDB table (left good) · "
            "map: bundled placeholder")
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
        d = _job_dir(app, "bffs")
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
        from unittest.mock import patch
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            return client.get("/service/bffs").data.decode()

    def test_requires_login(self, client, app, tmp_path):
        self._setup(app, tmp_path)
        resp = client.post("/service/bffs/manual", data={"label": "A1X"},
                           follow_redirects=False)
        assert resp.status_code == 302

    def test_requires_csrf(self, client, app, tmp_path):
        self._setup(app, tmp_path)
        _login(client)
        assert client.post("/service/bffs/manual", data={"label": "A1X"}).status_code == 403

    def test_control_off_403(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, manual=False)
        _login(client)
        assert self._post(client, _csrf(client), "A1X").status_code == 403
        assert not manual_file.exists()

    @pytest.mark.parametrize("label", ["", "Z9X", "A1X; rm -rf /", "x" * 65, "../etc"])
    def test_label_must_be_on_the_axis(self, client, app, tmp_path, label):
        manual_file = self._setup(app, tmp_path)
        _login(client)
        assert self._post(client, _csrf(client), label).status_code == 400
        assert not manual_file.exists()

    def test_no_axis_on_record_400(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, state=False)
        _login(client)
        assert self._post(client, _csrf(client), "A1X").status_code == 400
        assert not manual_file.exists()

    def test_toggle_adds_then_removes(self, client, app, tmp_path, caplog):
        manual_file = self._setup(app, tmp_path)
        _login(client)
        token = _csrf(client)
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
        _login(client)
        token = _csrf(client)
        self._post(client, token, "A1X")
        assert yaml.safe_load(manual_file.read_text()) == {
            "note": "keep me", "bad_inputs": ["A1X", "B1X"]}
        manual_file.write_text("[B1X, A1Y]\n")                # the bare-list shape
        self._post(client, token, "B1X")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1Y"]}

    def test_htmx_reply_swaps_the_grid_and_flashes(self, client, app, tmp_path):
        from unittest.mock import patch
        self._setup(app, tmp_path)
        _login(client)
        token = _csrf(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
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
        _login(client)
        resp = self._post(client, _csrf(client), "A1X", htmx=True)
        body = resp.data.decode()
        assert resp.status_code == 200
        assert "manual source reads /elsewhere/flags.yaml" in body
        assert not manual_file.exists()

    def test_job_without_a_manual_source_refuses(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, run="none")
        _login(client)
        resp = self._post(client, _csrf(client), "A1X", htmx=True)
        assert "runs with no manual source" in resp.data.decode()
        assert not manual_file.exists()

    def test_no_run_file_means_nothing_to_check_against(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, run=None)
        _login(client)
        self._post(client, _csrf(client), "A1X")
        assert yaml.safe_load(manual_file.read_text()) == {"bad_inputs": ["A1X"]}

    def test_unparseable_file_is_not_overwritten(self, client, app, tmp_path):
        manual_file = self._setup(app, tmp_path, manual_text="bad_inputs: [A1X\n")
        _login(client)
        resp = self._post(client, _csrf(client), "B1Y", htmx=True)
        assert "manual file not rewritten" in resp.data.decode()
        assert manual_file.read_text() == "bad_inputs: [A1X\n"

    def test_grid_cells_are_forms_when_enabled(self, client, app, tmp_path):
        self._setup(app, tmp_path, manual_text="bad_inputs: [A1Y]\n")
        _login(client)
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
        _login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert 'feed-manual"' not in body                        # no cell, no key swatch
        assert 'title="element 1: bad (power)"' in body         # unchanged tooltip

    def test_grid_read_only_on_a_path_mismatch(self, client, app, tmp_path):
        self._setup(app, tmp_path, run="other")
        _login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert "but the bffs job reads <code>/elsewhere/flags.yaml</code>" in body

    def test_grid_read_only_on_an_unreadable_file(self, client, app, tmp_path):
        self._setup(app, tmp_path, manual_text="bad_inputs: [A1X\n")
        _login(client)
        body = self._page(client)
        assert "/service/bffs/manual" not in body
        assert "Manual flags cannot be edited" in body


# --- Config library ------------------------------------------------------

from unittest.mock import patch as _patch  # noqa: E402
from choco.state import Node, NodeStatus  # noqa: E402


class TestConfigLibrary:
    @pytest.fixture
    def library(self, configs_dir, app):
        """A shared chord/ library with cx1 rendering chord/pathfinder.j2,
        which includes chord/telescope.j2; cx2 and recv1 stay on their
        per-node files."""
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "telescope.j2" %}\n')
        (chord / "telescope.j2").write_text("telescope: {name: a}\n")
        data = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/pathfinder.j2"
        (configs_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        app.config["registry"].reload()
        return configs_dir

    def test_requires_login(self, client, library):
        assert client.get("/configs").status_code == 302
        assert client.get("/configs/edit/chord/telescope.j2").status_code == 302

    def test_page_lists_files_and_users(self, client, library):
        _login(client)
        body = client.get("/configs").get_data(as_text=True)
        assert "chord/pathfinder.j2" in body and "chord/telescope.j2" in body
        assert "cx/cx1.yaml" in body and "recv/recv1.yaml" in body
        assert 'href="/configs/edit/.updatable' not in body
        assert 'href="/configs/edit/nodes.yaml"' not in body
        row = body[body.index("chord/telescope.j2"):]
        row = row[:row.index("</tr>")]
        assert "cx/cx1" in row

    @pytest.mark.parametrize("path", [
        "chord/nope.j2", "nodes.yaml", ".updatable/cx/cx1.json",
        "chord/telescope.txt", "chord/.hidden.j2",
    ])
    def test_bad_or_missing_path_is_404(self, client, library, path):
        _login(client)
        assert client.get("/configs/edit/" + path).status_code == 404

    def test_editor_shows_file_and_users(self, client, library):
        _login(client)
        body = client.get("/configs/edit/chord/telescope.j2").get_data(as_text=True)
        assert "telescope: {name: a}" in body
        assert "Included by: cx/cx1" in body
        body = client.get("/configs/edit/chord/pathfinder.j2").get_data(as_text=True)
        assert "Rendered by: cx/cx1" in body

    def test_save_rerenders_every_user_now(self, client, app, library):
        _login(client)
        token = _csrf(client)
        orch = app.config["orchestrator"]
        orch._file_mtimes = orch._config_file_mtimes()  # scan baseline
        polled = []
        orch.submit_node = lambda key, item: polled.append((key, item.type))
        resp = client.post("/configs/edit/chord/telescope.j2",
                           data={"_csrf_token": token,
                                 "content": "telescope: {name: b}\r\n"})
        assert resp.status_code == 302
        # CRLF normalised, written atomically, no temp file left behind.
        assert (library / "chord" / "telescope.j2").read_text() == \
            "telescope: {name: b}\n"
        assert not list((library / "chord").glob("*.tmp"))
        node = app.config["registry"].get_node("cx/cx1")
        assert node.rendered_config["telescope"] == {"name": "b"}
        assert polled == [("cx/cx1", ChangeType.POLL)]
        # The scan does not take choco's own write for an external edit.
        polled.clear()
        orch.check_config_files()
        assert polled == []

    def test_save_refused_when_a_user_would_not_render(self, client, app, library):
        _login(client)
        token = _csrf(client)
        before = (library / "chord" / "telescope.j2").read_text()
        resp = client.post("/configs/edit/chord/telescope.j2",
                           data={"_csrf_token": token,
                                 "content": '{% include "absent.j2" %}\n'})
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Not saved" in body and "cx/cx1" in body
        assert (library / "chord" / "telescope.j2").read_text() == before
        assert app.config["registry"].get_node("cx/cx1").rendered_config[
            "telescope"] == {"name": "a"}

    def test_unused_file_only_needs_template_syntax(self, client, library):
        _login(client)
        token = _csrf(client)
        (library / "chord" / "spare.j2").write_text("x: 1\n")
        resp = client.post("/configs/edit/chord/spare.j2",
                           data={"_csrf_token": token, "content": "{% if %}"})
        assert resp.status_code == 200 and "Not saved" in resp.get_data(as_text=True)
        resp = client.post("/configs/edit/chord/spare.j2",
                           data={"_csrf_token": token, "content": "not: [a mapping\n"})
        assert resp.status_code == 302  # YAML is not checked for a fragment

    def test_vars_yaml_must_be_a_mapping(self, client, app, library):
        _login(client)
        token = _csrf(client)
        (library / "vars.yaml").write_text("n: 1\n")
        app.config["registry"].reload()
        resp = client.post("/configs/edit/vars.yaml",
                           data={"_csrf_token": token, "content": "- a\n"})
        assert resp.status_code == 200 and "mapping" in resp.get_data(as_text=True)
        body = client.get("/configs").get_data(as_text=True)
        assert "every node" in body

    def test_new_file(self, client, library):
        _login(client)
        token = _csrf(client)
        resp = client.post("/configs/new",
                           data={"_csrf_token": token, "name": "chord/extra.yaml"})
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/configs/edit/chord/extra.yaml")
        assert (library / "chord" / "extra.yaml").read_text() == ""
        resp = client.post("/configs/new",
                           data={"_csrf_token": token, "name": "../x.yaml"},
                           follow_redirects=True)
        assert "Not created" in resp.get_data(as_text=True)
        assert not (library / "x.yaml").exists()

    def test_csrf_required(self, client, library):
        _login(client)
        _csrf(client)
        resp = client.post("/configs/edit/chord/telescope.j2",
                           data={"_csrf_token": "bogus", "content": "a: 1\n"})
        assert resp.status_code == 403


class TestNodeConfigSelection:
    @pytest.fixture
    def library(self, configs_dir, app):
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text("num_elements: 128\n")
        return configs_dir

    def test_node_page_offers_the_library(self, client, library):
        _login(client)
        body = client.get("/nodes/edit/cx/cx1").get_data(as_text=True)
        assert 'name="action" value="set_config"' in body
        assert '<option value="chord/pathfinder.j2"' in body
        assert "shared with" not in body
        # No text editor on the node page: the file is edited in the library.
        assert "<textarea" not in body and 'value="save_config"' not in body
        assert 'href="/configs/edit/cx/cx1.yaml"' in body
        assert 'title="Edit cx/cx1.yaml in the config library"' in body
        assert 'value="push_config"' in body

    def test_dashboard_buttons(self, client, library):
        _login(client)
        body = client.get("/nodes").get_data(as_text=True)
        assert 'href="/configs"' in body and ">Edit configs<" in body
        assert 'href="/nodes/edit"' in body and ">Edit nodes<" in body
        # The table names each node's file but carries no per-row editor
        # button: the library is one click away in the header.
        assert "<code>cx/cx1.yaml</code>" in body
        assert "/configs/edit/" not in body
        assert "edit-group" not in body
        assert client.get("/nodes/edit-group/cx").status_code == 404

    def test_library_page_has_edit_buttons(self, client, library):
        _login(client)
        body = client.get("/configs").get_data(as_text=True)
        assert 'href="/configs/edit/chord/pathfinder.j2" role="button"' in body
        assert "no node uses it yet" in body

    def test_use_rewrites_nodes_yaml_and_rebuilds(self, client, app, library):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.maintenance = False
        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/nodes/edit/cx/cx1",
                               data={"_csrf_token": token, "action": "set_config",
                                     "config": "chord/pathfinder.j2"},
                               follow_redirects=True)
        assert resp.status_code == 200
        assert "now renders chord/pathfinder.j2" in resp.get_data(as_text=True)
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert on_disk["groups"]["cx"]["cx1"]["config"] == "chord/pathfinder.j2"
        assert "config" not in on_disk["groups"]["cx"]["cx2"]
        node = registry.get_node("cx/cx1")
        assert node.config_filename == "chord/pathfinder.j2"
        assert node.rendered_config == {"num_elements": 128}
        assert all(n.maintenance for n in registry.nodes.values())

        # Both nodes on the file: the page says so.
        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            client.post("/nodes/edit/cx/cx2",
                        data={"_csrf_token": token, "action": "set_config",
                              "config": "chord/pathfinder.j2"})
        body = client.get("/nodes/edit/cx/cx1").get_data(as_text=True)
        assert "shared with cx/cx2" in body

        # Back to the per-node file.
        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            client.post("/nodes/edit/cx/cx1",
                        data={"_csrf_token": token, "action": "set_config",
                              "config": ""})
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert "config" not in on_disk["groups"]["cx"]["cx1"]
        assert registry.get_node("cx/cx1").config_filename == "cx/cx1.yaml"

    def test_missing_or_bad_file_refused(self, client, app, library):
        _login(client)
        token = _csrf(client)
        before = (library / "nodes.yaml").read_text()
        for bad in ("chord/absent.j2", "../x.j2", "nodes.yaml"):
            resp = client.post("/nodes/edit/cx/cx1",
                               data={"_csrf_token": token, "action": "set_config",
                                     "config": bad}, follow_redirects=True)
            assert resp.status_code == 200
        assert (library / "nodes.yaml").read_text() == before
        assert app.config["registry"].get_node("cx/cx1").explicit_config is None

    def test_nodes_editor_round_trips_config(self, client, app, library):
        _login(client)
        token = _csrf(client)
        payload = {"groups": {"cx": [
            {"name": "cx1", "host": "cx1.example", "port": 12048,
             "config": "chord/pathfinder.j2"},
            {"name": "cx2", "host": "cx2.example", "port": 12048, "config": ""},
        ]}}
        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/nodes/edit", data=json.dumps(payload),
                               content_type="application/json",
                               headers={"X-CSRF-Token": token})
        assert resp.status_code == 200
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert on_disk["groups"]["cx"] == {
            "cx1": {"host": "cx1.example", "port": 12048,
                    "config": "chord/pathfinder.j2"},
            "cx2": {"host": "cx2.example", "port": 12048},
        }
        body = client.get("/nodes/edit").get_data(as_text=True)
        assert 'value="chord/pathfinder.j2"' in body
        assert '<datalist id="config-files">' in body

        payload["groups"]["cx"][0]["config"] = "../../etc/x.yaml"
        resp = client.post("/nodes/edit", data=json.dumps(payload),
                           content_type="application/json",
                           headers={"X-CSRF-Token": token})
        assert resp.status_code == 400
        assert "cx/cx1" in resp.get_json()["error"]


class TestConfigLibraryApi:
    """The loopback JSON API the CLI and a deploy script use."""

    @pytest.fixture
    def library(self, configs_dir, app):
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "telescope.j2" %}\n')
        (chord / "telescope.j2").write_text("telescope: {name: a}\n")
        data = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/pathfinder.j2"
        (configs_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        app.config["registry"].reload()
        return configs_dir

    def test_list_and_get(self, client, library):
        files = client.get("/api/configs").get_json()["files"]
        by_path = {f["path"]: f for f in files}
        assert by_path["chord/pathfinder.j2"]["used_by"] == ["cx/cx1"]
        assert by_path["chord/telescope.j2"]["included_by"] == ["cx/cx1"]
        assert "nodes.yaml" not in by_path
        got = client.get("/api/configs/chord/telescope.j2").get_json()
        assert got["content"] == "telescope: {name: a}\n"
        assert got["included_by"] == ["cx/cx1"]
        assert client.get("/api/configs/chord/absent.j2").status_code == 404
        assert client.get("/api/configs/../x.yaml").status_code in (400, 404)
        assert client.get("/api/configs/nodes.yaml").status_code == 400

    def test_put_creates_validates_and_reloads(self, client, app, library):
        orch = app.config["orchestrator"]
        polled = []
        orch.submit_node = lambda key, item: polled.append(key)
        resp = client.put("/api/configs/chord/common.j2",
                          json={"content": "log_level: WARN\n"})
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "saved", "path": "chord/common.j2",
                                   "created": True, "reloaded": []}
        assert (library / "chord" / "common.j2").read_text() == "log_level: WARN\n"

        resp = client.put("/api/configs/chord/telescope.j2",
                          json={"content": "telescope: {name: api}\n"})
        assert resp.get_json()["reloaded"] == ["cx/cx1"]
        assert resp.get_json()["created"] is False
        assert app.config["registry"].get_node("cx/cx1").rendered_config[
            "telescope"] == {"name": "api"}
        assert polled == ["cx/cx1"]

        resp = client.put("/api/configs/chord/telescope.j2",
                          json={"content": "{% include 'absent.j2' %}"})
        assert resp.status_code == 400 and "cx/cx1" in resp.get_json()["error"]
        assert (library / "chord" / "telescope.j2").read_text() == \
            "telescope: {name: api}\n"

        assert client.put("/api/configs/chord/x.j2", json={}).status_code == 400
        assert client.put("/api/configs/../x.j2",
                          json={"content": "a: 1"}).status_code in (400, 404)
        assert client.put("/api/configs/.updatable/cx/cx1.json",
                          json={"content": "{}"}).status_code == 400

    def test_set_config_through_update(self, client, app, library):
        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/update/cx/cx2",
                               json={"action": "set_config",
                                     "config": "chord/pathfinder.j2"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "reloaded" and body["node"] == "cx/cx2"
        assert body["maintenance"] is True
        registry = app.config["registry"]
        assert registry.get_node("cx/cx2").config_filename == "chord/pathfinder.j2"
        assert all(n.maintenance for n in registry.nodes.values())

        with _patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/update/cx", json={"action": "set_config",
                                                   "config": None})
        assert resp.status_code == 200 and resp.get_json()["group"] == "cx"
        assert registry.get_node("cx/cx1").explicit_config is None
        assert registry.get_node("cx/cx2").explicit_config is None

        resp = client.post("/update/cx/cx1", json={"action": "set_config",
                                                   "config": 42})
        assert resp.status_code == 400
        resp = client.post("/update/cx/cx1", json={"action": "set_config",
                                                   "config": "chord/absent.j2"})
        assert resp.status_code == 404

    def test_node_config_endpoint(self, client, app, library):
        assert client.get("/api/config/cx/cx1").get_json() == {
            "num_elements": 128, "telescope": {"name": "a"}}
        assert client.get("/api/config/cx/cx2").get_json() == {"num_elements": 2048}
        assert client.get("/api/config/cx/nope").status_code == 404
        (library / "chord" / "telescope.j2").unlink()
        app.config["registry"].get_node("cx/cx1").load_config()
        resp = client.get("/api/config/cx/cx1")
        assert resp.status_code == 503 and "telescope.j2" in resp.get_json()["error"]

    def test_nodes_api_carries_config(self, client, library):
        nodes = client.get("/api/nodes").get_json()["groups"]["cx"]
        assert {n["name"]: n["config"] for n in nodes} == {
            "cx1": "chord/pathfinder.j2", "cx2": "cx/cx2.yaml"}
        status = client.get("/api/nodes/status").get_json()["nodes"]
        assert any(n["config"] == "chord/pathfinder.j2" for n in status)
