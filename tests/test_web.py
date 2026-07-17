"""Tests for the nodes.yaml editor and group-config editor routes."""

import json

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


@pytest.fixture
def configs_dir(tmp_path):
    """Temporary configs directory with a starting set of two groups."""
    nodes = {
        "groups": {
            "cx": {
                "cx1": {"host": "cx1.example", "port": 12048},
                "cx2": {"host": "cx2.example", "port": 12048},
            },
            "recv": {
                "recv1": {"host": "recv1.example", "port": 12048},
            },
        }
    }
    (tmp_path / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "cx").mkdir()
    (tmp_path / "cx" / "cx1.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "cx" / "cx2.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "recv").mkdir()
    (tmp_path / "recv" / "recv1.yaml").write_text("buffer_depth: 12\n")
    return tmp_path


@pytest.fixture
def app(configs_dir):
    app = create_app(configs_dir=configs_dir)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def _login(client):
    user = save_user("cn=tester,dc=example", "tester")
    with client.session_transaction() as sess:
        sess["_user_id"] = user.get_id()


def _csrf(client):
    """Establish a session and return its CSRF token."""
    client.get("/")
    with client.session_transaction() as sess:
        return sess["_csrf_token"]


# --- GET /nodes ---

class TestNodesEditGet:
    def test_requires_login(self, client):
        resp = client.get("/nodes", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_renders_groups(self, client):
        _login(client)
        resp = client.get("/nodes")
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


# --- POST /nodes ---

class TestNodesSave:
    def test_requires_csrf(self, client):
        _login(client)
        resp = client.post(
            "/nodes",
            data=json.dumps({"groups": {}}),
            content_type="application/json",
        )
        assert resp.status_code == 403

    def test_bad_csrf_rejected(self, client):
        _login(client)
        _csrf(client)  # ensure session has a token
        resp = client.post(
            "/nodes",
            data=json.dumps({"groups": {}}),
            content_type="application/json",
            headers={"X-CSRF-Token": "not-the-token"},
        )
        assert resp.status_code == 403

    def test_rejects_non_dict_groups(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes",
            data=json.dumps({"groups": []}),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400

    def test_rejects_invalid_group_name(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/nodes",
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
            "/nodes",
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
            "/nodes",
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
            "/nodes",
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
                "/nodes",
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


# --- POST /set-started-group/<group>/<action> ---

class TestSetStartedGroup:
    def test_start_scopes_to_group(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        # Seed: nothing started.
        for node in registry.nodes.values():
            node.started = False

        resp = client.post(
            "/set-started-group/cx/start",
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
            "/set-started-group/nope/start",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 404

    def test_bad_action_400(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/set-started-group/cx/frobnicate",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 400

    def test_bad_csrf_rejected(self, client):
        _login(client)
        _csrf(client)
        resp = client.post(
            "/set-started-group/cx/start",
            data={"_csrf_token": "bogus"},
        )
        assert resp.status_code == 403


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
        assert "PSU" in body
        # Badges link to the service pages.
        assert '/service/eop' in body
        assert '/service/fpga' in body
        assert '/service/psu' in body


class TestMaintenanceToggles:
    def test_toggle_flips_per_node(self, client, app):
        _login(client)
        token = _csrf(client)
        registry = app.config["registry"]
        node = registry.get_node("cx/cx1")
        node.maintenance = False

        resp = client.post(
            "/toggle-maintenance/cx/cx1",
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
            "/set-maintenance-group/cx/on",
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
            "/set-maintenance-all/on",
            data={"_csrf_token": token},
        )
        assert all(n.maintenance for n in registry.nodes.values())

        client.post(
            "/set-maintenance-all/off",
            data={"_csrf_token": token},
        )
        assert not any(n.maintenance for n in registry.nodes.values())

    def test_bad_action_400(self, client):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/set-maintenance-all/frobnicate",
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


# --- GET / POST /edit-group/<group> ---

class TestGroupEdit:
    def test_requires_login(self, client):
        resp = client.get("/edit-group/cx", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_unknown_group_redirects(self, client):
        _login(client)
        resp = client.get("/edit-group/nope", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].rstrip("/").endswith("")  # → "/"

    def test_get_renders_empty_textarea(self, client):
        _login(client)
        resp = client.get("/edit-group/cx")
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
            "/edit-group/cx",
            data={"config_content": "not_a_mapping", "_csrf_token": token},
        )
        assert resp.status_code == 200
        assert b"Invalid config" in resp.data
        assert b"not_a_mapping" in resp.data

    def test_post_queues_and_redirects(self, client, app):
        _login(client)
        token = _csrf(client)
        resp = client.post(
            "/edit-group/cx",
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
            "/edit-group/cx",
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

    @pytest.mark.parametrize("name", ["choco", "eop", "bffs", "eigencal"])
    def test_job_pages_render(self, client, name):
        from unittest.mock import patch
        _login(client)
        with patch("choco.web.job_status", return_value=dict(_JOB_STUB)), \
             patch("choco.web.timer_status", return_value=None):
            resp = client.get(f"/service/{name}")
        assert resp.status_code == 200
        assert name.upper() in resp.data.decode()

    def test_fpga_page_renders(self, client):
        _login(client)
        resp = client.get("/service/fpga")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "FPGA" in body
        assert "not configured" in body

    def test_psu_page_renders(self, client):
        _login(client)
        resp = client.get("/service/psu")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "PSU" in body
        assert "not configured" in body

    def test_psu_page_channel_grid(self, client, app):
        from choco.services import PsuMonitor
        _login(client)
        monitor = app.config["psu_monitor"]
        monitor.boards = {0: 1}
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
        ]}
        resp = client.get("/service/psu")
        body = resp.data.decode()
        assert "SPI bus 0" in body
        assert "board 0" in body
        assert body.count('class="chan on"') == 1
        assert body.count('class="chan off"') == 15

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
        pis.assert_called_once_with(10)


class TestPsuControl:
    def _configure(self, app, control=True):
        monitor = app.config["psu_monitor"]
        monitor.host, monitor.port = "psu.example", 5000
        monitor.channels = {0: [
            {"board": 0, "chip": "A", "channels": [True] + [False] * 7},
            {"board": 0, "chip": "B", "channels": [False] * 8},
        ]}
        app.config["psu_cfg"] = {"host": monitor.host, "port": monitor.port,
                                 "control": control}
        return monitor

    def _form(self, token, **overrides):
        form = {"_csrf_token": token, "bus": "0", "board": "0",
                "chip": "A", "channel": "3", "state": "on"}
        form.update(overrides)
        return form

    def test_requires_login(self, client):
        resp = client.post("/service/psu/set", follow_redirects=False)
        assert resp.status_code == 302

    def test_requires_csrf(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.post("/service/psu/set", data={"bus": "0"})
        assert resp.status_code == 403

    def test_control_disabled_403(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app, control=False)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel") as sc:
            resp = client.post("/service/psu/set", data=self._form(token))
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
        resp = client.post("/service/psu/set", data=self._form(token, **bad))
        assert resp.status_code == 400

    def test_toggle_calls_set_channel(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(True, "bus 0 board 0 chip A ch3 on")) as sc:
            resp = client.post("/service/psu/set",
                               data=self._form(token),
                               follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/service/psu")
        sc.assert_called_once_with(0, 0, "A", 3, True)

    def test_verify_failure_flashes_error(self, client, app):
        from unittest.mock import patch
        monitor = self._configure(app)
        _login(client)
        token = _csrf(client)
        with patch.object(monitor, "set_channel",
                          return_value=(False, "verify failed")):
            resp = client.post("/service/psu/set",
                               data=self._form(token, state="off"),
                               follow_redirects=True)
        assert b"verify failed" in resp.data

    def test_grid_buttons_when_control_enabled(self, client, app):
        self._configure(app)
        _login(client)
        resp = client.get("/service/psu")
        body = resp.data.decode()
        assert '/service/psu/set' in body
        assert body.count("<button") >= 16
        assert 'name="channel"' in body

    def test_grid_readonly_when_control_disabled(self, client, app):
        self._configure(app, control=False)
        _login(client)
        resp = client.get("/service/psu")
        body = resp.data.decode()
        assert '/service/psu/set' not in body
        assert '<span class="chan' in body


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
        assert data["services"]["psu"] == "unconfigured"
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
        assert 'choco_service_state{service="eigencal",state="failed"} 1' in body
        assert 'choco_service_state{service="psu",state="unconfigured"} 1' in body
        assert 'choco_service_state{service="psu",state="no_states"} 0' in body
        assert "choco_nodes_total 3" in body
        assert "choco_nodes_maintenance 3" in body
        # One-hot node counts by status are present for every status.
        assert 'choco_nodes{status="unknown"} 3' in body
