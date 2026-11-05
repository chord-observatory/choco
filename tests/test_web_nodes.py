"""Node control: the nodes.yaml editor, the per-node and group toggles,
one-off runs and the node status partial."""

import contextlib
import hashlib
import json
import logging
from unittest.mock import patch

import pytest
import yaml

from choco.state import Mode, Node, NodeStatus
from choco.sync import ChangeType

from tests.helpers import csrf, login


# --- GET /nodes/edit ---

class TestNodesEditGet:
    def test_renders_groups(self, client):
        login(client)
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
    def test_bad_csrf_rejected(self, client):
        login(client)
        csrf(client)  # ensure session has a token
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": {}}),
            content_type="application/json",
            headers={"X-CSRF-Token": "not-the-token"},
        )
        assert resp.status_code == 403

    def test_rejects_non_dict_groups(self, client):
        login(client)
        token = csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": []}),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400

    def test_rejects_invalid_group_name(self, client):
        login(client)
        token = csrf(client)
        resp = client.post(
            "/nodes/edit",
            data=json.dumps({"groups": {"bad/name": []}}),
            content_type="application/json",
            headers={"X-CSRF-Token": token},
        )
        assert resp.status_code == 400
        assert "invalid group name" in resp.get_json()["error"].lower()

    def test_rejects_missing_host(self, client):
        login(client)
        token = csrf(client)
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
        login(client)
        token = csrf(client)
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
        login(client)
        token = csrf(client)
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

        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        # Pre-save: flip maintenance off and force a started value so
        # we can prove neither leaks across the rebuild.
        for n in registry.nodes.values():
            n.mode = Mode.NORMAL
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

    def test_save_keeps_the_development_declaration(self, client, app, configs_dir):
        login(client)
        token = csrf(client)

        def save(groups):
            with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
                return client.post("/nodes/edit", data=json.dumps({"groups": groups}),
                                   content_type="application/json",
                                   headers={"X-CSRF-Token": token})

        resp = save({"cx": [
            {"name": "cx1", "host": "cx1.example", "port": 12048, "development": True},
            {"name": "cx2", "host": "cx2.example", "port": 12048, "development": False}]})
        assert resp.status_code == 200
        on_disk = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        assert on_disk["groups"]["cx"]["cx1"] == {"host": "cx1.example", "port": 12048,
                                                  "development": True}
        assert "development" not in on_disk["groups"]["cx"]["cx2"]
        registry = app.config["registry"]
        cx1 = registry.get_node("cx/cx1")
        assert cx1.maintenance and cx1.development and cx1.rest_mode is Mode.DEVELOPMENT
        assert not registry.get_node("cx/cx2").development
        page = client.get("/nodes/edit").get_data(as_text=True)
        assert page.count('class="node-development" checked') == 1
        resp = save({"cx": [{"name": "cx1", "host": "cx1.example", "development": "yes"}]})
        assert resp.status_code == 400
        assert "development must be a boolean" in resp.get_json()["error"]


# --- POST /nodes/set-started-group/<group>/<action> ---

class TestSetStartedGroup:
    def test_start_scopes_to_group(self, client, app):
        login(client)
        token = csrf(client)
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
        login(client)
        token = csrf(client)
        resp = client.post(
            "/nodes/set-started-group/nope/start",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 404

    def test_bad_action_400(self, client):
        login(client)
        token = csrf(client)
        resp = client.post(
            "/nodes/set-started-group/cx/frobnicate",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 400

    @pytest.mark.parametrize("token", ["bogus", "tökén"])   # non-ASCII: 403, not a 500
    def test_bad_csrf_rejected(self, client, token):
        login(client)
        csrf(client)
        resp = client.post(
            "/nodes/set-started-group/cx/start",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 403


class TestMaintenanceToggles:
    def test_set_mode_per_node(self, client, app):
        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        node = registry.get_node("cx/cx1")
        node.mode = Mode.NORMAL

        resp = client.post(
            "/nodes/set-mode/maintenance/cx/cx1",
            data={"_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert node.maintenance is True
        # htmx gets the control back, with the new mode filled in.
        resp = client.post("/nodes/set-mode/development/cx/cx1",
                           data={"_csrf_token": token}, headers={"HX-Request": "true"})
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert node.mode is Mode.DEVELOPMENT
        assert 'class="seg-btn tone-dev current"' in body
        assert "/nodes/set-mode/maintenance/cx/cx1" in body
        assert "/nodes/set-mode/normal/cx/cx1" not in body   # not offered from development
        # Development straight to normal is refused (a stale page's click:
        # htmx gets the control as it stands); through maintenance it goes.
        resp = client.post("/nodes/set-mode/normal/cx/cx1",
                           data={"_csrf_token": token}, headers={"HX-Request": "true"})
        assert resp.status_code == 200 and node.mode is Mode.DEVELOPMENT
        assert 'class="seg-btn tone-dev current"' in resp.get_data(as_text=True)
        resp = client.post("/nodes/set-mode/normal/cx/cx1",
                           data={"_csrf_token": token}, follow_redirects=True)
        assert "cx/cx1: in development; put it in maintenance first" in resp.get_data(as_text=True)
        assert node.mode is Mode.DEVELOPMENT
        client.post("/nodes/set-mode/maintenance/cx/cx1", data={"_csrf_token": token})
        client.post("/nodes/set-mode/normal/cx/cx1", data={"_csrf_token": token})
        assert node.mode is Mode.NORMAL
        assert client.post("/nodes/set-mode/sideways/cx/cx1",
                           data={"_csrf_token": token}).status_code == 400
        assert client.post("/nodes/set-mode/normal/cx/nope",
                           data={"_csrf_token": token}).status_code == 404

    def test_a_declared_development_node_is_not_offered_normal(self, client, app):
        login(client)
        token = csrf(client)
        node = app.config["registry"].get_node("cx/cx1")
        node.development = True
        node.mode = Mode.DEVELOPMENT
        body = client.post("/nodes/set-mode/development/cx/cx1",
                           data={"_csrf_token": token},
                           headers={"HX-Request": "true"}).get_data(as_text=True)
        assert "/nodes/set-mode/normal/cx/cx1" not in body
        assert "/nodes/set-mode/maintenance/cx/cx1" in body
        assert "nodes.yaml declares it" in body

    def test_lifting_returns_each_node_to_its_resting_mode(self, client, app):
        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        cx1, cx2, recv1 = (registry.get_node(k) for k in ("cx/cx1", "cx/cx2", "recv/recv1"))
        cx1.development = True
        cx2.mode = Mode.DEVELOPMENT          # a runtime handover, not declared
        recv1.mode = Mode.NORMAL
        # ▼ is the stop button: every node, development included.
        client.post("/nodes/set-mode-all/maintenance", data={"_csrf_token": token})
        assert all(n.maintenance for n in registry.nodes.values())
        client.post("/nodes/set-mode-all/normal", data={"_csrf_token": token})
        assert cx1.mode is Mode.DEVELOPMENT
        assert cx2.mode is Mode.NORMAL and recv1.mode is Mode.NORMAL
        # ▲ leaves a node not in maintenance alone.
        cx2.mode = Mode.DEVELOPMENT
        client.post("/nodes/set-mode-group/cx/normal", data={"_csrf_token": token})
        assert cx2.mode is Mode.DEVELOPMENT
        # dev puts every node in scope in development.
        client.post("/nodes/set-mode-group/cx/development", data={"_csrf_token": token})
        assert cx1.mode is Mode.DEVELOPMENT and cx2.mode is Mode.DEVELOPMENT
        assert recv1.mode is Mode.NORMAL

    def test_set_group_scopes_to_group(self, client, app):
        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.mode = Mode.NORMAL

        resp = client.post(
            "/nodes/set-mode-group/cx/maintenance",
            data={"_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert registry.get_node("cx/cx1").maintenance is True
        assert registry.get_node("cx/cx2").maintenance is True
        # recv group untouched.
        assert registry.get_node("recv/recv1").maintenance is False

    def test_set_all_flips_every_node(self, client, app):
        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.mode = Mode.NORMAL

        client.post(
            "/nodes/set-mode-all/maintenance",
            data={"_csrf_token": token},
        )
        assert all(n.maintenance for n in registry.nodes.values())

        client.post(
            "/nodes/set-mode-all/normal",
            data={"_csrf_token": token},
        )
        assert not any(n.maintenance for n in registry.nodes.values())

    def test_bad_action_400(self, client):
        login(client)
        token = csrf(client)
        resp = client.post(
            "/nodes/set-mode-all/frobnicate",
            data={"_csrf_token": token},
        )
        assert resp.status_code == 400

    def test_json_api_set_maintenance_per_node(self, client, app):
        login(client)
        registry = app.config["registry"]
        node = registry.get_node("cx/cx1")
        node.mode = Mode.NORMAL

        resp = client.post(
            "/update/cx/cx1",
            json={"action": "set_maintenance", "maintenance": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["maintenance"] is True
        assert node.maintenance is True

    def test_json_api_rejects_non_bool(self, client):
        login(client)
        resp = client.post(
            "/update/cx/cx1",
            json={"action": "set_maintenance", "maintenance": "yes"},
        )
        assert resp.status_code == 400

    def test_json_api_set_mode(self, client, app):
        login(client)
        registry = app.config["registry"]
        cx1, cx2 = registry.get_node("cx/cx1"), registry.get_node("cx/cx2")
        resp = client.post("/update/cx/cx1", json={"action": "set_mode", "mode": "development"})
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "ok", "node": "cx/cx1", "mode": "development"}
        assert cx1.mode is Mode.DEVELOPMENT and cx2.maintenance
        # All or none: one development node refuses the group's step to normal.
        resp = client.post("/update/cx", json={"action": "set_mode", "mode": "normal"})
        assert resp.status_code == 409
        assert resp.get_json()["refused"] == ["cx/cx1"]
        assert resp.get_json()["error"] == "cx/cx1: in development; put it in maintenance first"
        assert cx1.mode is Mode.DEVELOPMENT and cx2.maintenance
        for bad in ("sideways", ["normal"], {"mode": "normal"}, None):
            resp = client.post("/update/cx/cx1", json={"action": "set_mode", "mode": bad})
            assert resp.status_code == 400, bad
        # A declared development node is refused normal from maintenance too.
        cx2.development = True
        resp = client.post("/update/cx/cx2", json={"action": "set_mode", "mode": "normal"})
        assert resp.status_code == 409 and "nodes.yaml" in resp.get_json()["error"]
        assert cx2.maintenance
        cx2.development = False
        # set_maintenance false is the lift: each node to its resting mode.
        cx2.development = True
        client.post("/update/cx", json={"action": "set_maintenance", "maintenance": True})
        assert cx1.maintenance and cx2.maintenance
        resp = client.post("/update/cx", json={"action": "set_maintenance", "maintenance": False})
        assert resp.status_code == 200 and resp.get_json()["maintenance"] is False
        assert cx1.mode is Mode.NORMAL and cx2.mode is Mode.DEVELOPMENT
        status = client.get("/api/nodes/status").get_json()["nodes"]
        by_key = {n["key"]: n for n in status}
        assert by_key["cx/cx2"]["mode"] == "development" and by_key["cx/cx2"]["development"]
        assert by_key["cx/cx1"]["mode"] == "normal" and not by_key["cx/cx1"]["maintenance"]


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
            n.mode = Mode.MAINTENANCE if n.key in keys else Mode.NORMAL

    def test_unknown_targets_404(self, client):
        login(client)
        assert client.post("/oneshot/nope",
                           json={"config_content": self.CONTENT}).status_code == 404
        assert client.post("/oneshot/cx/nope",
                           json={"config_content": self.CONTENT}).status_code == 404

    def test_unrenderable_text_is_400_and_contacts_nothing(self, client, app):
        login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app) as calls:
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": "not: [valid"})
        assert resp.status_code == 400
        assert "Invalid config" in resp.get_json()["error"]
        assert calls["start"] == []

    def test_empty_body_is_400(self, client, app):
        login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app) as calls:
            resp = client.post("/oneshot/cx/cx1", json={})
        assert resp.status_code == 400
        assert calls["start"] == []

    def test_skips_node_not_in_maintenance(self, client, app):
        login(client)
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
        login(client)
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
        login(client)
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
        login(client)
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
        login(client)
        self._pause(app, "cx/cx1")
        with self._cluster(app, start_ok=False):
            resp = client.post("/oneshot/cx/cx1",
                               json={"config_content": self.CONTENT})
        assert resp.status_code == 409
        assert resp.get_json()["skipped"] == {"cx/cx1": "/start failed"}

    def test_edit_page_offers_the_button(self, client):
        login(client)
        body = client.get("/nodes/edit/cx/cx1").data.decode()
        assert 'value="oneshot"' in body
        assert 'id="oneshot-select"' in body and 'value="cx/cx1.yaml"' in body

    def test_edit_page_button_starts_a_library_file_without_saving(
            self, client, app, configs_dir):
        login(client)
        token = csrf(client)
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
        login(client)
        token = csrf(client)
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
        login(client)
        token = csrf(client)
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


class TestNodeStatusPartial:
    def test_renders_cached_status_without_probing(self, client, app):
        """The worker is the only writer of node.status; the partial
        must render what it last recorded, not probe kotekan itself."""

        login(client)
        node = app.config["registry"].get_node("cx/cx1")
        node.status = NodeStatus.STARTED
        node.version_info = {"kotekan_version": "2026.09"}
        with patch.object(Node, "get_status") as probe:
            resp = client.get("/nodes/partials/node-status/cx/cx1")
        assert resp.status_code == 200
        probe.assert_not_called()
        body = resp.data.decode()
        assert "status-started" in body
        assert "2026.09" in body

    def test_unknown_node_404s(self, client):
        login(client)
        assert client.get("/nodes/partials/node-status/cx/nope").status_code == 404
