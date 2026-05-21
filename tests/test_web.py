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
        # 'unknown' / unconfigured state.  Stub eop_status so we don't
        # depend on the host's systemd or fs state.
        with patch("choco.web.eop_status",
                   return_value={"health": "ok",
                                 "last_run_at": None,
                                 "source": "mtime"}):
            resp = client.get("/partials/services")
        assert resp.status_code == 200
        body = resp.data.decode()
        assert "EOP" in body
        # FPGA badge is only rendered if a monitor is set on app.config.
        # The test app does install one (unconfigured), so it shows up.
        assert "FPGA" in body


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
