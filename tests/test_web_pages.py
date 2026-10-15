"""What every page shares: the landing page, page chrome and fonts,
presence, the service strip, the sky-map routes, /api/status and /metrics."""

import re
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from choco.state import NodeStatus

from tests.helpers import JOB_STUB as _JOB_STUB, job_dir, login


# --- Landing page (/) ---

class TestLandingPage:
    def test_renders_service_table(self, client):
        login(client)
        body = client.get("/").data.decode()
        # One row per badge: choco itself, the cluster, and the jobs.
        for label in ("CHOCO", "NODES", "DATA", "EOP", "BFFS", "EIGENCAL", "MAP"):
            assert label in body
        assert "/service/mapmaker" in body
        # The waterfall renderer rides in the DATA row (one badge, one
        # page), never a row of its own; the skymap in the MAP row.
        assert "waterfalls:" in body
        assert ">WF<" not in body and "/service/waterfall" not in body
        assert "skymap:" in body
        assert ">SKYMAP<" not in body and "/service/skymap" not in body
        # The nodes row summarizes the registry (3 seeded nodes).
        assert "3 nodes" in body
        # The door to node management, now that the dashboard left /.
        assert 'href="/nodes"' in body

    def test_dashboard_no_longer_at_root(self, client):
        login(client)
        body = client.get("/").data.decode()
        assert "dashboard-table" not in body
        # The dashboard still answers at /nodes.
        nodes_body = client.get("/nodes").data.decode()
        assert "dashboard-table" in nodes_body

    def test_partial_refreshes_table(self, client):
        login(client)
        body = client.get("/partials/landing-services").data.decode()
        assert "<table" in body and "EOP" in body


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
        pyproject = Path(__file__).parent.parent / "pyproject.toml"
        assert '"static/fonts/*"' in pyproject.read_text()


class TestPageChrome:
    """The review round of 2026-10: one title format, the dashboard's
    desired-vs-actual note, one config line per uniform group, and the
    wall-display mode."""

    def test_titles_name_the_page_then_the_site(self, client, app):
        login(client)
        for url, title in [("/", "Services — CHOCO"), ("/nodes", "Nodes — CHOCO"),
                           ("/nodes/edit", "Edit nodes — CHOCO"), ("/nodes/edit/cx/cx1", "cx/cx1 — CHOCO"),
                           ("/configs", "Configs — CHOCO"), ("/files", "Data files — CHOCO"),
                           ("/service/eop", "EOP — CHOCO"), ("/pipeline/cx/cx1", "cx/cx1 pipeline — CHOCO")]:
            body = client.get(url).data.decode()
            assert f"<title>{title}</title>" in body, url
        # a logged-in client is bounced off /login; a fresh one sees the form
        assert "<title>Sign in — CHOCO</title>" in app.test_client().get("/login").data.decode()

    def test_wants_note_when_desired_differs_from_actual(self, client, app):
        login(client)
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
        login(client)
        body = client.get("/partials/dashboard-table").data.decode()
        # cx: cx1.yaml and cx2.yaml differ -> a Config column; recv: one
        # node, one file -> named in the group bar, no column
        cx = body[body.index("group <code>cx</code>"):body.index("group <code>recv</code>")]
        recv = body[body.index("group <code>recv</code>"):]
        assert "<th>Config</th>" in cx and "<code>cx/cx1.yaml</code>" in cx and "<code>cx/cx2.yaml</code>" in cx
        assert "<th>Config</th>" not in recv
        assert 'renders <code>recv/recv1.yaml</code>' in recv

    def test_wall_mode_drops_the_chrome(self, client):
        login(client)
        plain = client.get("/").data.decode()
        wall = client.get("/?wall=1").data.decode()
        assert '<body class="wall">' in wall and '<body class="wall">' not in plain
        assert "landing-services" in wall and 'id="skymap"' in wall

    def test_landing_table_has_one_timing_column_and_no_unit_names(self, client):
        login(client)
        stub = dict(_JOB_STUB, unit="choco-eop-broadcast.service", state_mtime=time.time() - 120)
        with patch("choco.web.job_status", return_value=stub), \
             patch("choco.web.timer_status", return_value={"unit": "x.timer", "next_elapse": "Mon 2026-10-05 06:00:00 UTC"}):
            body = client.get("/partials/landing-services").data.decode()
        heads = re.findall(r"<th>([^<]*)</th>", body)
        assert heads == ["Service", "Status", "Detail", "Timing"]
        assert "choco-eop-broadcast.service" not in body
        assert "next Mon 2026-10-05 06:00:00 UTC" in body


class TestPresence:
    """Who is here: any authenticated request is presence, a page load or
    POST is an action, a partial poll is "page open"; the nav line names
    the operator and counts the others active in the last ten minutes."""

    def _seen(self, app):
        return app.config.get("presence", {})

    def test_requests_are_recorded_with_their_page(self, client, app):
        login(client)
        client.get("/nodes")
        me = self._seen(app)["tester"]
        assert me["page"] == "/nodes" and me["last_action"] is not None
        # a poll keeps presence fresh and names the page it belongs to,
        # but is not an action
        action = me["last_action"]
        client.get("/partials/services", headers={"HX-Current-URL": "https://x/service/pdb"})
        me = self._seen(app)["tester"]
        assert me["page"] == "/service/pdb" and me["last_action"] == action

    def test_unauthenticated_and_static_requests_are_not_presence(self, client, app):
        client.get("/login")
        client.get("/static/choco.css")
        assert "tester" not in self._seen(app)

    def test_nav_line_counts_the_others(self, client, app):
        from choco.web import PRESENCE_WINDOW_S
        login(client)
        client.get("/")
        now = time.time()
        app.config["presence"]["alice"] = {"last_seen": now - 120, "last_action": now - 180, "page": "/service/pdb"}
        app.config["presence"]["bob"] = {"last_seen": now - 30, "last_action": None, "page": "/nodes"}
        app.config["presence"]["gone"] = {"last_seen": now - PRESENCE_WINDOW_S - 5, "last_action": None, "page": "/"}
        body = client.get("/partials/presence").data.decode()
        assert "tester" in body and "2 others" in body
        assert "alice (you)" not in body and "tester (you)" in body
        assert "alice — service pdb, acting 3m ago" in body
        assert "bob — nodes, page open" in body
        assert "gone" not in body and "gone" not in app.config["presence"]

    def test_alone_shows_only_the_name(self, client, app):
        login(client)
        body = client.get("/partials/presence").data.decode()
        assert "tester" in body and "other" not in body

    def test_nav_polls_the_line(self, client):
        login(client)
        body = client.get("/").data.decode()
        assert 'id="presence" hx-get="/partials/presence"' in body


# --- Sky map: /skymap.png (unauthenticated) + landing card ---

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


class TestSkymap:
    def _configure(self, app, tmp_path, write=True, night=False):
        # The images live where the job writes them, <state_dir>/skymap/;
        # nothing in config.yaml names them.
        path = job_dir(app, "skymap") / "skymap.png"
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

    def test_partial_names_the_night_render_for_dark_mode(self, client, app, tmp_path):
        # base.html swaps the card's src to data-night-src when the theme is
        # dark; the attribute is there only once a night render exists.
        self._configure(app, tmp_path, night=True)
        login(client)
        body = client.get("/partials/skymap").data.decode()
        assert 'data-day-src="/skymap.png?v=' in body
        assert 'data-night-src="/skymap-night.png?v=' in body
        self._configure(app, tmp_path, night=False)
        (job_dir(app, "skymap") / "skymap-night.png").unlink()
        body = client.get("/partials/skymap").data.decode()
        assert "data-night-src" not in body

    def test_partial_carries_mtime_busted_url(self, client, app, tmp_path):
        path = self._configure(app, tmp_path)
        login(client)
        body = client.get("/partials/skymap").data.decode()
        assert f'/skymap.png?v={int(path.stat().st_mtime)}' in body

    def test_partial_links_night_image_only_when_rendered(
            self, client, app, tmp_path):
        login(client)
        self._configure(app, tmp_path)
        assert "skymap-night" not in client.get("/partials/skymap").data.decode()
        path = self._configure(app, tmp_path, night=True)
        night_mtime = int((path.parent / "skymap-night.png").stat().st_mtime)
        body = client.get("/partials/skymap").data.decode()
        assert f'/skymap-night.png?v={night_mtime}' in body

    def test_partial_without_render_yet(self, client, app, tmp_path):
        self._configure(app, tmp_path, write=False)
        login(client)
        body = client.get("/partials/skymap").data.decode()
        assert "No sky map rendered yet" in body

    def test_landing_card_always_present(self, client, app, tmp_path):
        # the job is part of the install, so the card is unconditional:
        # it says "not rendered yet" until the first PNG lands
        login(client)
        assert 'id="skymap"' in client.get("/").data.decode()
        login(client)
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


class TestServicesPartial:
    def test_renders_strip_when_logged_in(self, client, app, jobs):
        # No fpga_master configured in the test app, so monitor is in
        # 'unknown' / unconfigured state.
        login(client)
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

    def _nodes_badge(self, client):
        """(tone, label) of the strip's NODES badge.

        The tone is the ``tag-<tone>`` class the tag macro emits (painted
        by static/choco.css); the label is what the badge says, read from
        aria-label because a strip badge shows no word at all.  The caller
        holds the ``jobs`` fixture.
        """
        body = client.get("/partials/services").data.decode()
        m = re.search(
            r'<a href="/nodes" class="tag tag-([a-z]+)( quiet)?[^"]*"'
            r' aria-label="NODES: ([^"]*)"', body)
        assert m, body
        return m.group(1), m.group(3)

    def _set_statuses(self, app, statuses):
        nodes = list(app.config["registry"].nodes.values())
        assert len(statuses) == len(nodes)
        for node, status in zip(nodes, statuses):
            node.status = NodeStatus[status]

    @pytest.mark.parametrize("statuses,badge", [
        (["STARTED", "STARTED", "STARTED"], ("ok", "all up")),
        (["DOWN", "DOWN", "DOWN"], ("bad", "all down")),
        (["STARTED", "DOWN", "IDLE"], ("warn", "1/3 up")),
        (["IDLE", "IDLE", "IDLE"], ("warn", "idle")),
        (None, ("off", "unknown")),          # fresh registry: nothing polled yet
    ])
    def test_nodes_badge_tone_and_word(self, client, app, jobs, statuses, badge):
        login(client)
        if statuses:
            self._set_statuses(app, statuses)
        assert self._nodes_badge(client) == badge

    def test_nodes_badge_quiet_when_all_up(self, client, app, jobs):
        # A nominal badge shows label and dot only: the word lives in the
        # tooltip and aria-label, and the tint goes neutral.
        login(client)
        self._set_statuses(app, ["STARTED", "STARTED", "STARTED"])
        body = client.get("/partials/services").data.decode()
        nodes = re.search(r'<a href="/nodes"[^>]*>(.*?)</a>', body, re.S)
        assert nodes and 'class="tag tag-ok quiet"' in nodes.group(0)
        assert "<strong>NODES</strong>" in nodes.group(1)
        assert 'class="word"' not in nodes.group(1)

    def test_running_job_is_a_quiet_blue_dot(self, client, app):
        # A run in flight with no verdict yet is info, and info is quiet in
        # the strip like ok: dot only, "running" in the tooltip/aria-label.
        # Otherwise the word pushed the nine-service strip onto two lines.
        login(client)
        running = dict(_JOB_STUB, health="never_run", running=True,
                       unit="choco-eop.service")
        with patch("choco.web.job_status", return_value=running):
            body = client.get("/partials/services").data.decode()
        eop = re.search(r'<a href="/service/eop"[^>]*>(.*?)</a>', body, re.S)
        assert eop and 'class="tag tag-info quiet"' in eop.group(0)
        assert 'aria-label="EOP: running"' in eop.group(0)
        assert 'class="word"' not in eop.group(1)

    # --- The DATA badge: the mounts and the waterfall renderer, one tag ---

    def _data_badge(self, client, jobs, wf):
        """(tone, word) of the strip's DATA badge with the renderer's
        job_status stubbed to *wf*; the test app's mounts are
        unconfigured (off), so the renderer wins whenever it is worse."""
        jobs(**wf)
        body = client.get("/partials/services").data.decode()
        m = re.search(
            r'<a href="/files" class="tag tag-([a-z]+)( quiet)?[^"]*"'
            r' aria-label="DATA: ([^"]*)"', body)
        assert m, body
        assert ">WF<" not in body          # no badge of its own
        return m.group(1), m.group(3)

    def test_data_badge_shows_a_failed_renderer(self, client, jobs):
        login(client)
        assert self._data_badge(client, jobs, {"health": "failed"}) == ("bad", "waterfall failed")

    def test_data_badge_shows_a_degraded_renderer(self, client, jobs):
        login(client)
        assert self._data_badge(client, jobs, {"health": "degraded"}) == ("warn", "waterfall degraded")

    def test_data_badge_prefers_the_mounts_when_worse(self, client, app, jobs):
        # A dead mount is what explains a degraded renderer: the mounts'
        # word wins over the renderer's at equal or worse severity.
        login(client)
        scan = app.config["datafile_scan"]
        scan.roots = [Path("/nowhere/a")]
        scan.health = "down"
        assert self._data_badge(client, jobs, {"health": "degraded"}) == ("bad", "down")
        scan.health = "ok"
        assert self._data_badge(client, jobs, {"health": "ok"}) == ("ok", "up")
        assert self._data_badge(client, jobs, {"health": "failed"}) == ("bad", "waterfall failed")

    def test_data_badge_tooltip_names_both_halves(self, client, app):
        login(client)
        scan = app.config["datafile_scan"]
        scan.roots = [Path("/nowhere/a")]
        scan.health = "ok"
        stub = dict(_JOB_STUB, health="degraded", unit="choco-waterfall.service",
                    reasons=["subset/ unreadable"])
        with patch("choco.web.job_status", return_value=stub):
            body = client.get("/partials/services").data.decode()
        tag = re.search(r'<a href="/files"[^>]*title="([^"]*)"', body)
        assert tag, body
        tip = tag.group(1)
        assert "mounts: ok" in tip
        assert "waterfall (choco-waterfall.service): degraded" in tip
        assert "why: subset/ unreadable" in tip

    def test_data_badge_carries_the_notes_job(self, client, app):
        # The acquisition notes ride in DATA too: a failed notes run turns
        # the tag red with its own word while the renderer is fine, and
        # neither job has a badge of its own.
        login(client)
        scan = app.config["datafile_scan"]
        scan.roots = [Path("/nowhere/a")]
        scan.health = "ok"

        def status(unit, **kw):
            health = "failed" if unit == "choco-acqnotes.service" else "ok"
            return dict(_JOB_STUB, health=health, unit=unit)
        with patch("choco.web.job_status", side_effect=status):
            body = client.get("/partials/services").data.decode()
        m = re.search(r'<a href="/files" class="tag tag-([a-z]+)[^"]*" aria-label="DATA: ([^"]*)"'
                      r'[^>]*title="([^"]*)"', body)
        assert m, body
        assert (m.group(1), m.group(2)) == ("bad", "notes failed")
        assert "notes (choco-acqnotes.service): failed" in m.group(3)

    def test_map_badge_carries_the_skymap_job(self, client, app):
        # The skymap rides in MAP the same way: a failed render turns the
        # tag red with its own word, the tooltip names both jobs, and the
        # skymap has no badge of its own.
        login(client)

        def status(unit, **kw):
            health = "failed" if unit == "choco-skymap.service" else "ok"
            return dict(_JOB_STUB, health=health, unit=unit)
        with patch("choco.web.job_status", side_effect=status):
            body = client.get("/partials/services").data.decode()
        m = re.search(r'<a href="/service/mapmaker" class="tag tag-([a-z]+)[^"]*" aria-label="MAP: ([^"]*)"'
                      r'[^>]*title="([^"]*)"', body)
        assert m, body
        assert (m.group(1), m.group(2)) == ("bad", "skymap failed")
        assert "MAP job (choco-mapmaker.service)" in m.group(3)
        assert "skymap (choco-skymap.service): failed" in m.group(3)
        assert ">SKYMAP<" not in body and "/service/skymap" not in body
        # the host's own state, unprefixed, when it is the worse of the two
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB, health="degraded")):
            body = client.get("/partials/services").data.decode()
        assert 'aria-label="MAP: degraded"' in body
        assert ">NOTES<" not in body and "/service/acqnotes" not in body

    def test_nodes_badge_tinted_but_wordless_when_not_ok(self, client, app, jobs):
        # A bad badge keeps its tint (no `quiet` class) but still shows no
        # word: the strip's layout must not shift with the state, so the
        # word lives only in the tooltip and aria-label.
        login(client)
        self._set_statuses(app, ["DOWN", "DOWN", "DOWN"])
        body = client.get("/partials/services").data.decode()
        nodes = re.search(r'<a href="/nodes"[^>]*>(.*?)</a>', body, re.S)
        assert nodes and 'class="tag tag-bad"' in nodes.group(0)
        assert 'aria-label="NODES: all down"' in nodes.group(0)
        assert "<strong>NODES</strong>" in nodes.group(1)
        assert 'class="word"' not in nodes.group(1)

    def test_degraded_job_badge_is_tinted_and_wordless(self, client, app):
        login(client)
        degraded = dict(_JOB_STUB, health="degraded", unit="choco-bffs.service")
        with patch("choco.web.job_status", return_value=degraded):
            body = client.get("/partials/services").data.decode()
        bffs = re.search(r'<a href="/service/bffs"[^>]*>(.*?)</a>', body, re.S)
        assert bffs and 'class="tag tag-warn"' in bffs.group(0)
        assert 'aria-label="BFFS: degraded"' in bffs.group(0)
        assert 'class="word"' not in bffs.group(1)


# --- Status API + metrics ---
# The test client's requests come from 127.0.0.1, so the JSON API's
# localhost bypass applies (no login needed).

class TestStatusApi:
    def test_api_status_is_a_summary(self, client, jobs):
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
    def test_no_auth_required(self, client, jobs):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.mimetype == "text/plain"

    def test_exposition_content(self, client):
        failed = dict(_JOB_STUB, health="failed")
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
