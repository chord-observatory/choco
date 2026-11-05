"""The X-engine page's updatable panel: the rows (choco.updatable), the
worker's live reads behind them (NodeWorker._note_live) and the partial."""

import json
import time
from unittest.mock import MagicMock

import pytest
import yaml

from choco.state import Mode, NodeStatus, Registry
from choco.sync import LIVE_READ_MAINTENANCE_S, ChangeItem, ChangeType, NodeWorker, Orchestrator
from choco.updatable import (LIVE_MAX_AGE_S, NODE_STATES, desired_blocks, fleet_rows, is_switch,
                             node_states, summarize)

from tests.helpers import csrf, login

CX = """\
fringestop_enabled:
    kotekan_update_endpoint: json
    enabled: false
    valid_at_time_ns: 0
rfi_second_stage_thresholds:
    kotekan_update_endpoint: json
    thresholds: []
    valid_at_time_ns: 0
updatable_config:
    bad_inputs:
        kotekan_update_endpoint: json
        bad_inputs: []
        update_id: initial
"""
RECV = """\
earth_rotation_data:
    kotekan_update_endpoint: json
    earth_orientation_parameter_table: []
"""
FS_OFF = {"enabled": False, "valid_at_time_ns": 0}
THRESHOLDS = {"thresholds": [], "valid_at_time_ns": 0}
BAD = {"bad_inputs": [1, 4, 5, 7, 23], "update_id": "bffs-1"}
OWNERS = {"updatable_config/bad_inputs": "bffs", "earth_rotation_data": "eop"}


@pytest.fixture
def configs_dir(tmp_path):
    """cx1 and cx2 render the same blocks; cx2 stores an override of the
    switch and both a bffs list; recv1 has the EOP table only."""
    nodes = {"groups": {
        "cx": {"cx1": {"host": "cx1.example", "port": 12048},
               "cx2": {"host": "cx2.example", "port": 12048}},
        "recv": {"recv1": {"host": "recv1.example", "port": 12048}},
    }}
    (tmp_path / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    for group, name, text in (("cx", "cx1", CX), ("cx", "cx2", CX), ("recv", "recv1", RECV)):
        (tmp_path / group).mkdir(exist_ok=True)
        (tmp_path / group / f"{name}.yaml").write_text(text)
    store = tmp_path / ".updatable" / "cx"
    store.mkdir(parents=True)
    (store / "cx1.json").write_text(json.dumps({"updatable_config/bad_inputs": BAD}))
    (store / "cx2.json").write_text(json.dumps({
        "updatable_config/bad_inputs": BAD,
        "fringestop_enabled": {"enabled": False, "valid_at_time_ns": 1}}))
    return tmp_path


@pytest.fixture
def registry(configs_dir):
    return Registry(configs_dir)


def _running(node, live: dict, now: float, age: float = 1.0):
    """*node* as its worker leaves it after reading *live* blocks."""
    node.status = NodeStatus.STARTED
    node.live_updatable = live
    node.live_read_at = now - age


def _row(rows, endpoint):
    return next(r for r in rows if r.endpoint == endpoint)


def _states(row):
    return {c.name: c.state for c in row.cells}


class TestBlocks:
    def test_switch_shape(self):
        assert is_switch(FS_OFF)
        assert is_switch({"enabled": True})
        assert not is_switch(THRESHOLDS)
        assert not is_switch({"enabled": 1, "valid_at_time_ns": 0})
        assert not is_switch({"enabled": True, "valid_at_time_ns": 0, "extra": 1})

    def test_summaries(self):
        assert summarize(FS_OFF) == "off"
        assert summarize({"enabled": True, "valid_at_time_ns": 7}) == "on, valid_at_time_ns 7"
        assert summarize(BAD) == "bad_inputs: [5 items], update_id: bffs-1"
        assert summarize({"bad_inputs": [1, 2], "start_time": 1.5}) == "bad_inputs: [1, 2], start_time: 1.5"

    def test_desired_merges_the_override_over_the_rendered_block(self, registry):
        cx2 = registry.get_node("cx/cx2")
        blocks = desired_blocks(cx2)
        assert blocks["fringestop_enabled"] == {"enabled": False, "valid_at_time_ns": 1}
        assert blocks["rfi_second_stage_thresholds"] == THRESHOLDS
        # the same merge desired_config does
        assert cx2.desired_config["fringestop_enabled"] == {
            "kotekan_update_endpoint": "json", "enabled": False, "valid_at_time_ns": 1}


class TestRows:
    def test_rows_in_config_order_with_owners(self, registry):
        rows = fleet_rows(list(registry.nodes.values()), OWNERS)
        assert [r.endpoint for r in rows] == [
            "fringestop_enabled", "rfi_second_stage_thresholds",
            "updatable_config/bad_inputs", "earth_rotation_data"]
        assert [r.switch for r in rows] == [True, False, False, False]
        assert [r.owner for r in rows] == [None, None, "bffs", "eop"]

    def test_mixed_values_name_their_nodes(self, registry):
        # cx2's stray valid_at_time_ns: two desired values, one node each
        fs = _row(fleet_rows(list(registry.nodes.values()), OWNERS), "fringestop_enabled")
        assert [(s, n) for s, _, n in fs.values] == [
            ("off", ["cx1"]), ("off, valid_at_time_ns 1", ["cx2"])]
        assert fs.overrides == ["cx2"] and fs.rendered_on == 2

    def test_shared_value_and_every_node_overridden(self, registry):
        bad = _row(fleet_rows(list(registry.nodes.values()), OWNERS), "updatable_config/bad_inputs")
        assert len(bad.values) == 1 and bad.values[0][2] == ["cx1", "cx2"]
        assert bad.overrides == ["cx1", "cx2"]

    def test_live_states(self, registry):
        now = time.time()
        cx1, cx2, recv1 = (registry.get_node(k) for k in ("cx/cx1", "cx/cx2", "recv/recv1"))
        # cx1 runs everything it should; cx2 runs the switch without its
        # stray time and lacks the thresholds; recv1 runs a block no file renders
        _running(cx1, {"fringestop_enabled": FS_OFF, "rfi_second_stage_thresholds": THRESHOLDS,
                       "updatable_config/bad_inputs": BAD}, now)
        _running(cx2, {"fringestop_enabled": FS_OFF, "updatable_config/bad_inputs": BAD}, now)
        _running(recv1, {"earth_rotation_data": {"earth_orientation_parameter_table": []},
                         "gnss_gain": {"x": 1}}, now)
        cx2.mode = Mode.MAINTENANCE
        rows = fleet_rows([cx1, cx2, recv1], OWNERS, now=now)
        assert _states(_row(rows, "fringestop_enabled")) == {"cx1": "live", "cx2": "differs"}
        assert "Maintenance" in _row(rows, "fringestop_enabled").cells[1].note
        assert _states(_row(rows, "rfi_second_stage_thresholds")) == {"cx1": "live", "cx2": "absent"}
        extra = _row(rows, "gnss_gain")
        assert _states(extra) == {"recv1": "extra"}
        assert extra.rendered_on == 0 and not extra.values

    def test_no_current_read_is_unread(self, registry):
        now = time.time()
        cx1, cx2, recv1 = (registry.get_node(k) for k in ("cx/cx1", "cx/cx2", "recv/recv1"))
        _running(cx1, {"fringestop_enabled": FS_OFF}, now, age=LIVE_MAX_AGE_S + 1)
        _running(cx2, {"fringestop_enabled": FS_OFF}, now)
        cx2.status = NodeStatus.DOWN
        recv1.status = NodeStatus.STARTED  # never read
        rows = fleet_rows([cx1, cx2, recv1], OWNERS, now=now)
        cells = {c.name: c for c in _row(rows, "fringestop_enabled").cells}
        assert {n: c.state for n, c in cells.items()} == {"cx1": "unread", "cx2": "unread"}
        assert "s ago" in cells["cx1"].note and "down" in cells["cx2"].note
        assert _states(_row(rows, "earth_rotation_data")) == {"recv1": "unread"}

    def test_failed_read_is_unread(self, registry):
        cx1 = registry.get_node("cx/cx1")
        cx1.status, cx1.live_updatable, cx1.live_read_at = NodeStatus.STARTED, None, time.time()
        fs = _row(fleet_rows([cx1], OWNERS), "fringestop_enabled")
        assert fs.cells[0].state == "unread" and "failed" in fs.cells[0].note

    def test_a_development_node_desires_only_its_stored_values(self, registry):
        now = time.time()
        cx2 = registry.get_node("cx/cx2")
        cx2.mode = Mode.DEVELOPMENT
        # Its own config: the switch with a field the stored value lacks
        # (so the stored value fits), bffs's endpoint, and a block of its
        # own; the thresholds its file renders are not run and not desired.
        _running(cx2, {"fringestop_enabled": {**FS_OFF, "extra": 1},
                       "updatable_config/bad_inputs": {"bad_inputs": [], "update_id": "x"},
                       "gnss_gain": {"x": 1}}, now)
        rows = fleet_rows([cx2], OWNERS, now=now)
        assert [r.endpoint for r in rows] == [
            "updatable_config/bad_inputs", "fringestop_enabled", "gnss_gain"]
        bad = _row(rows, "updatable_config/bad_inputs")
        assert _states(bad) == {"cx2": "differs"}
        assert bad.values[0][2] == ["cx2"] and bad.overrides == ["cx2"]
        fs = _row(rows, "fringestop_enabled")
        assert _states(fs) == {"cx2": "differs"}
        assert fs.values[0][1] == '{"enabled": false,"valid_at_time_ns": 1,"extra": 1}'
        gain = _row(rows, "gnss_gain")
        assert _states(gain) == {"cx2": "extra"} and "development" in gain.cells[0].note
        # A stored value with a field the running block lacks is not pushed, and says so.
        cx2.live_updatable["fringestop_enabled"] = {"enabled": False}
        fs = _row(fleet_rows([cx2], OWNERS, now=now), "fringestop_enabled")
        assert _states(fs) == {"cx2": "differs"} and "not pushed" in fs.cells[0].note
        assert not fs.values
        # A stale read: the desired set follows the last read, shown unread.
        cx2.live_read_at = now - 1000
        bad = _row(fleet_rows([cx2], OWNERS, now=now), "updatable_config/bad_inputs")
        assert _states(bad) == {"cx2": "unread"}


class TestWorkerLiveRead:
    @pytest.fixture
    def orchestrator(self, registry):
        return Orchestrator(registry, poll_interval=1, max_concurrent_pushes=2)

    def _poll(self, node, orchestrator):
        node.queue_put(ChangeItem(type=ChangeType.POLL))
        NodeWorker(node, orchestrator).process()

    def _node(self, orchestrator, maintenance: bool):
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        node.mode = Mode.MAINTENANCE if maintenance else Mode.NORMAL
        node.get_status = MagicMock(return_value=NodeStatus.STARTED)
        node.get_version_info = MagicMock(return_value={})
        node.get_config = MagicMock(return_value=node.desired_config)
        node.kill, node.start, node.push_updatable = MagicMock(), MagicMock(), MagicMock()
        return node

    def test_every_cycle_outside_maintenance(self, orchestrator):
        node = self._node(orchestrator, maintenance=False)
        self._poll(node, orchestrator)
        self._poll(node, orchestrator)
        assert node.get_config.call_count == 2
        assert node.live_updatable["updatable_config/bad_inputs"] == BAD
        assert time.time() - node.live_read_at < 5
        # The base of the live config is kept and compared with the file's.
        assert node.live_base == node.rendered_base and node.live_differs is False
        node.start.assert_not_called()

    def test_a_changed_file_is_recompared_with_the_last_read(self, orchestrator):
        node = self._node(orchestrator, maintenance=True)
        self._poll(node, orchestrator)
        assert node.live_differs is False
        node.rendered_config = {**node.rendered_config, "num_elements": 4096}
        assert node.live_differs is True
        node.rendered_config = None
        assert node.live_differs is None

    def test_maintenance_reads_but_never_writes(self, orchestrator):
        node = self._node(orchestrator, maintenance=True)
        self._poll(node, orchestrator)
        self._poll(node, orchestrator)       # within LIVE_READ_MAINTENANCE_S: no second read
        assert node.get_config.call_count == 1
        assert node.live_updatable["fringestop_enabled"] == FS_OFF
        node.live_read_at -= LIVE_READ_MAINTENANCE_S
        self._poll(node, orchestrator)
        assert node.get_config.call_count == 2
        node.kill.assert_not_called()
        node.start.assert_not_called()
        node.push_updatable.assert_not_called()

    def test_maintenance_skips_a_kotekan_that_is_not_running(self, orchestrator):
        node = self._node(orchestrator, maintenance=True)
        node.get_status.return_value = NodeStatus.IDLE
        self._poll(node, orchestrator)
        node.get_config.assert_not_called()
        assert node.live_read_at is None

    def test_failed_read_is_kept_as_none(self, orchestrator):
        node = self._node(orchestrator, maintenance=True)
        node.get_config.return_value = None
        self._poll(node, orchestrator)
        assert node.live_updatable is None and node.live_read_at is not None
        assert node.live_base is None and node.live_differs is None


class TestNodeStates:
    """A node's config and updatable states (updatable.node_states): the
    dashboard's two columns beside the kotekan status."""

    def _live(self, node, now, base=None, blocks=None, age=1.0):
        """*node* as its worker leaves it after reading a live config whose
        base is *base* (default: the file's) and whose updatable blocks are
        *blocks* (default: the desired values)."""
        node.status = NodeStatus.STARTED
        node.live_base = node.rendered_base if base is None else base
        node.live_updatable = desired_blocks(node) if blocks is None else blocks
        node.compare_live()
        node.live_read_at = now - age

    def test_running_the_file_and_the_values(self, registry):
        now = time.time()
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.NORMAL
        self._live(cx1, now)
        config, updatable = node_states(cx1, now)
        assert (config.state, config.tone, config.word) == ("ok", "ok", "ok")
        assert config.note == "cx1 runs the config its file renders (read 1 s ago)."
        assert (updatable.state, updatable.tone) == ("ok", "ok")
        assert updatable.note == "cx1 runs the desired value at 3 endpoints (read 1 s ago)."

    def test_another_base_config(self, registry):
        now = time.time()
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.MAINTENANCE
        self._live(cx1, now, base={**cx1.rendered_base, "num_elements": 4096})
        config, updatable = node_states(cx1, now)
        assert (config.state, config.tone) == ("differs", "warn")
        assert "Lifting maintenance restarts it onto the file" in config.note
        assert updatable.state == "ok"
        cx1.mode = Mode.NORMAL
        config, _ = node_states(cx1, now)
        assert config.state == "differs" and "a lasting difference is a failed push" in config.note

    def test_values_the_node_does_not_run(self, registry):
        now = time.time()
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.MAINTENANCE
        blocks = desired_blocks(cx1)
        blocks["updatable_config/bad_inputs"] = {"bad_inputs": [], "update_id": "initial"}
        self._live(cx1, now, blocks=blocks)
        config, updatable = node_states(cx1, now)
        assert config.state == "ok"
        assert (updatable.state, updatable.tone) == ("differs", "warn")
        assert updatable.note == ("cx1: 1 of 3 endpoints differ (read 1 s ago): "
                                  "/updatable_config/bad_inputs differs. "
                                  "Maintenance: choco pushes nothing to it.")
        # An endpoint the running config lacks counts too.
        del blocks["fringestop_enabled"]
        cx1.mode = Mode.NORMAL
        updatable = node_states(cx1, now)[1]
        assert "2 of 3 endpoints differ" in updatable.note
        assert "/fringestop_enabled absent" in updatable.note and "Maintenance" not in updatable.note

    def test_development(self, registry):
        now = time.time()
        cx2 = registry.get_node("cx/cx2")
        cx2.mode = Mode.DEVELOPMENT
        # cx2 stores the bffs list (which the running config has) and the
        # switch with valid_at_time_ns 1 (which it runs at 0).
        self._live(cx2, now, base={"their": "config"},
                   blocks={"updatable_config/bad_inputs": dict(BAD), "fringestop_enabled": FS_OFF})
        config, updatable = node_states(cx2, now)
        assert (config.state, config.tone) == ("own", "dev")
        assert "in development the file does not apply" in config.note
        assert updatable.state == "differs" and "1 of 2 endpoints differ" in updatable.note
        assert "/fringestop_enabled differs" in updatable.note
        # A stored value the running block cannot take is a difference too.
        self._live(cx2, now, base={"their": "config"},
                   blocks={"updatable_config/bad_inputs": {"bad_inputs": "none"},
                           "fringestop_enabled": {"enabled": False, "valid_at_time_ns": 1}})
        updatable = node_states(cx2, now)[1]
        assert "1 of 2 endpoints differ" in updatable.note
        assert "/updatable_config/bad_inputs differs" in updatable.note
        # No stored value at all: nothing to compare.
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.DEVELOPMENT
        cx1.updatable_config = {}
        self._live(cx1, now, base={}, blocks={"fringestop_enabled": FS_OFF})
        updatable = node_states(cx1, now)[1]
        assert updatable.state == "none" and "no stored value its running config takes" in updatable.note

    def test_not_running_not_read_or_stale(self, registry):
        now = time.time()
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.NORMAL
        cx1.status = NodeStatus.IDLE
        config, updatable = node_states(cx1, now)
        assert (config.state, config.tone) == ("none", "off") and "kotekan is idle" in config.note
        assert updatable.state == "none" and "kotekan is idle" in updatable.note
        cx1.status = NodeStatus.SYNCING
        config, updatable = node_states(cx1, now)
        assert (config.state, config.tone) == ("pushing", "info") and updatable.state == "pushing"
        cx1.status = NodeStatus.STARTED
        config, updatable = node_states(cx1, now)
        assert (config.state, config.tone) == ("unread", "off") and "not read yet" in config.note
        assert updatable.state == "unread" and "not read yet" in updatable.note
        self._live(cx1, now, age=LIVE_MAX_AGE_S + 1)
        config, updatable = node_states(cx1, now)
        assert config.state == "unread" and "last read" in config.note
        assert updatable.state == "unread"
        cx1.live_read_at = now
        cx1.live_updatable = cx1.live_base = None
        cx1.compare_live()
        assert "the last read of /config failed" in node_states(cx1, now)[0].note

    def test_a_file_or_store_that_does_not_load(self, registry, configs_dir):
        now = time.time()
        cx1 = registry.get_node("cx/cx1")
        cx1.mode = Mode.MAINTENANCE
        self._live(cx1, now)
        (configs_dir / ".updatable" / "cx" / "cx1.json").write_text("{not json")
        cx1.load_updatable()
        config, updatable = node_states(cx1, now)
        assert config.state == "ok"
        assert (updatable.state, updatable.tone, updatable.word) == ("bad_store", "bad", "bad store")
        assert "cx1.json" in updatable.note
        (configs_dir / "cx" / "cx1.yaml").write_text("not: [valid")
        cx1.load_config()
        config = node_states(cx1, now)[0]
        assert (config.state, config.tone, config.word) == ("bad_file", "bad", "bad file")
        assert "cx1.yaml" in config.note

    def test_no_updatable_block_in_the_file(self, registry):
        now = time.time()
        recv1 = registry.get_node("recv/recv1")
        recv1.mode = Mode.NORMAL
        recv1.rendered_config = {"buffer_depth": 12}
        self._live(recv1, now)
        config, updatable = node_states(recv1, now)
        assert config.state == "ok"
        assert updatable.state == "none" and "its file renders no updatable block" in updatable.note

    def test_every_state_has_a_tone(self):
        from choco.updatable import TONES
        assert all(word in TONES for word in NODE_STATES)


class TestPanel:
    def test_nodes_page_and_partial(self, client, app):
        login(client)
        page = client.get("/nodes").data.decode()
        assert 'id="updatable-table"' in page and "/partials/updatable" in page
        assert 'id="updatable-flash"' in page

    def test_the_set_controls(self, client, app):
        login(client)
        body = client.get("/partials/updatable").get_data(as_text=True)
        # The switch: on, off and edit, and library for cx2's override;
        # the thresholds: edit alone; bffs's and eop's rows: the jobs'.
        assert body.count('"action": "on"') == 1 and body.count('"action": "off"') == 1
        assert body.count('"action": "library"') == 1
        assert 'hx-confirm="Drop the override of /fringestop_enabled on cx2?"' in body
        assert 'popovertarget="up-fringestop_enabled"' in body
        assert 'popovertarget="up-rfi_second_stage_thresholds"' in body
        assert 'popovertarget="up-updatable_config-bad_inputs"' not in body
        assert 'hx-post="/nodes/updatable/updatable_config/bad_inputs"' not in body
        assert 'hx-post="/nodes/updatable/earth_rotation_data"' not in body
        assert "The bffs job sets it" in body
        # The edit form starts from the most common desired value (the
        # quotes escaped in the source; the browser decodes them in a textarea).
        assert ('<textarea name="values" rows="4" spellcheck="false">'
                '{&#34;thresholds&#34;: [],&#34;valid_at_time_ns&#34;: 0}</textarea>') in body

    def test_set_a_switch_on_every_node_that_has_it(self, client, app):
        login(client)
        token = csrf(client)
        registry, orch = app.config["registry"], app.config["orchestrator"]
        submitted = []
        orch.submit_node = lambda key, item: submitted.append((key, item))
        resp = client.post("/nodes/updatable/fringestop_enabled",
                           data={"_csrf_token": token, "action": "on"},
                           headers={"HX-Request": "true"})
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert 'id="updatable-flash"' in body and "flash-success" in body
        assert "/fringestop_enabled: on queued on cx/cx1, cx/cx2" in body
        assert [(k, i.type, i.endpoint, i.values) for k, i in submitted] == [
            ("cx/cx1", ChangeType.UPDATABLE_CONFIG, "fringestop_enabled",
             {"enabled": True, "valid_at_time_ns": 0}),
            ("cx/cx2", ChangeType.UPDATABLE_CONFIG, "fringestop_enabled",
             {"enabled": True, "valid_at_time_ns": 0})]
        # A node in development takes it from its running block, which
        # here has no valid_at_time_ns; its file (the EOP table) does not count.
        submitted.clear()
        recv1 = registry.get_node("recv/recv1")
        recv1.mode = Mode.DEVELOPMENT
        recv1.live_updatable = {"fringestop_enabled": {"enabled": True}}
        client.post("/nodes/updatable/fringestop_enabled",
                    data={"_csrf_token": token, "action": "off"})
        assert [(k, i.values) for k, i in submitted] == [
            ("cx/cx1", {"enabled": False, "valid_at_time_ns": 0}),
            ("cx/cx2", {"enabled": False, "valid_at_time_ns": 0}),
            ("recv/recv1", {"enabled": False})]

    def test_edit_any_value_and_drop_the_overrides(self, client, app):
        login(client)
        token = csrf(client)
        orch = app.config["orchestrator"]
        submitted = []
        orch.submit_node = lambda key, item: submitted.append((key, item))

        def post(endpoint, **form):
            return client.post(f"/nodes/updatable/{endpoint}",
                               data={"_csrf_token": token, **form},
                               headers={"HX-Request": "true"})

        resp = post("rfi_second_stage_thresholds", values='{"thresholds": [1, 2]}')
        assert resp.status_code == 200
        assert "value queued on cx/cx1, cx/cx2" in resp.get_data(as_text=True)
        assert [(k, i.values) for k, i in submitted] == [
            ("cx/cx1", {"thresholds": [1, 2]}), ("cx/cx2", {"thresholds": [1, 2]})]
        submitted.clear()
        # A value the blocks cannot take is queued nowhere, and said.
        body = post("rfi_second_stage_thresholds", values='{"thresholds": "x"}').get_data(as_text=True)
        assert "queued on no node; skipped cx/cx1, cx/cx2" in body and "flash-error" in body
        assert not submitted
        assert "invalid JSON" in post("rfi_second_stage_thresholds", values="{").get_data(as_text=True)
        assert "must be a JSON object" in post("rfi_second_stage_thresholds", values="[1]").get_data(as_text=True)
        assert post("rfi_second_stage_thresholds").status_code == 400
        # An endpoint no node has: nothing queued.
        assert "queued on no node" in post("nothing", values="{}").get_data(as_text=True)
        # library drops the stored override where there is one: bffs's list on both cx nodes.
        body = post("updatable_config/bad_inputs", action="library").get_data(as_text=True)
        assert "override dropped queued on cx/cx1, cx/cx2" in body
        assert [(k, i.endpoint, i.values) for k, i in submitted] == [
            ("cx/cx1", "updatable_config/bad_inputs", None),
            ("cx/cx2", "updatable_config/bad_inputs", None)]
        # A plain form post flashes and returns to the dashboard.
        resp = client.post("/nodes/updatable/fringestop_enabled",
                           data={"_csrf_token": token, "action": "on"})
        assert resp.status_code == 302 and resp.headers["Location"].endswith("/nodes")
        cx1 = app.config["registry"].get_node("cx/cx1")
        _running(cx1, {"fringestop_enabled": FS_OFF}, time.time())
        body = client.get("/partials/updatable").data.decode()
        assert "<code>/fringestop_enabled</code>" in body and ">switch<" in body
        # the job's label, linked to its page; the partial is under the app root
        assert 'href="/service/bffs"' in body and ">Bad feeds<" in body
        assert 'href="/service/eop"' in body
        assert "override <small" in body and "cx2" in body
        assert 'class="tag tag-ok"' in body and 'class="tag tag-off"' in body
