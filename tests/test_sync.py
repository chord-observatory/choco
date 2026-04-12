"""Tests for the queue-based sync system."""

import pytest
from unittest.mock import MagicMock

import yaml

from choco.state import Registry, Node, NodeStatus
from choco.sync import (
    ChangeType, ChangeItem, InputQueue, Orchestrator,
)


@pytest.fixture
def configs_dir(tmp_path):
    """Temporary configs directory with two groups."""
    nodes = {
        "groups": {
            "cx": {
                "cx1": {"host": "cx1.chord.ca", "port": 12048},
                "cx2": {"host": "cx2.chord.ca", "port": 12048},
            },
            "recv": {
                "recv1": {"host": "recv1.chord.ca", "port": 12048},
            },
        }
    }
    with open(tmp_path / "nodes.yaml", "w") as f:
        yaml.dump(nodes, f)

    cx_dir = tmp_path / "cx"
    cx_dir.mkdir()
    config = {"num_elements": 2048, "log_level": "info"}
    with open(cx_dir / "cx1.yaml", "w") as f:
        yaml.dump(config, f)
    with open(cx_dir / "cx2.yaml", "w") as f:
        yaml.dump(config, f)

    recv_dir = tmp_path / "recv"
    recv_dir.mkdir()
    with open(recv_dir / "recv1.yaml", "w") as f:
        yaml.dump({"buffer_depth": 12}, f)

    return tmp_path


@pytest.fixture
def registry(configs_dir):
    return Registry(configs_dir)


@pytest.fixture
def orchestrator(registry):
    return Orchestrator(registry, socketio=None, poll_interval=1, num_workers=2)


class TestNodeQueue:
    def test_put_and_pop(self, orchestrator):
        node = orchestrator.registry.get_node("cx/cx1")
        item = ChangeItem(type=ChangeType.POLL, node_key="cx/cx1")
        node.queue_put(item)
        assert not node.queue_empty
        assert node.queue_pop() is item
        assert node.queue_empty

    def test_pop_empty(self, orchestrator):
        node = orchestrator.registry.get_node("cx/cx1")
        assert node.queue_pop() is None

    def test_fifo_order(self, orchestrator):
        node = orchestrator.registry.get_node("cx/cx1")
        items = [
            ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"),
            ChangeItem(type=ChangeType.RESYNC, node_key="cx/cx1"),
            ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"),
        ]
        for item in items:
            node.queue_put(item)
        for expected in items:
            assert node.queue_pop() is expected

    def test_try_lock_and_unlock(self, orchestrator):
        node = orchestrator.registry.get_node("cx/cx1")
        assert node.queue_try_lock() is True
        assert node.queue_try_lock() is False
        node.queue_unlock()
        assert node.queue_try_lock() is True
        node.queue_unlock()


class TestInputQueue:
    def test_submit_node(self, orchestrator):
        iq = orchestrator.input_queue
        item = ChangeItem(type=ChangeType.POLL, node_key="cx/cx1")
        iq.submit_node(item)

        cx1 = orchestrator.registry.get_node("cx/cx1")
        cx2 = orchestrator.registry.get_node("cx/cx2")
        recv1 = orchestrator.registry.get_node("recv/recv1")
        assert not cx1.queue_empty
        assert cx2.queue_empty
        assert recv1.queue_empty

    def test_submit_node_unknown_key(self, orchestrator):
        iq = orchestrator.input_queue
        item = ChangeItem(type=ChangeType.POLL, node_key="nonexistent/node")
        iq.submit_node(item)  # should not raise

    def test_submit_group(self, orchestrator):
        iq = orchestrator.input_queue
        iq.submit_group(
            "cx",
            lambda key: ChangeItem(type=ChangeType.POLL, node_key=key),
        )

        cx1 = orchestrator.registry.get_node("cx/cx1")
        cx2 = orchestrator.registry.get_node("cx/cx2")
        recv1 = orchestrator.registry.get_node("recv/recv1")
        assert not cx1.queue_empty
        assert not cx2.queue_empty
        assert recv1.queue_empty

    def test_submit_group_nonexistent(self, orchestrator):
        iq = orchestrator.input_queue
        iq.submit_group(
            "nonexistent",
            lambda key: ChangeItem(type=ChangeType.POLL, node_key=key),
        )
        for node in orchestrator.registry.nodes.values():
            assert node.queue_empty

    def test_submit_all(self, orchestrator):
        iq = orchestrator.input_queue
        iq.submit_all(
            lambda key: ChangeItem(type=ChangeType.POLL, node_key=key),
        )
        for node in orchestrator.registry.nodes.values():
            assert not node.queue_empty


class TestOrchestratorQueues:
    def test_submit_base_config(self, orchestrator):
        orchestrator.submit_base_config("cx/cx1", "num_elements: 1024\n")
        node = orchestrator.registry.get_node("cx/cx1")
        item = node.queue_pop()
        assert item.type == ChangeType.BASE_CONFIG
        assert item.node_key == "cx/cx1"
        assert item.config_content == "num_elements: 1024\n"

    def test_submit_updatable_config(self, orchestrator):
        orchestrator.submit_updatable_config(
            "cx/cx1", "updatable_config/gains", {"start_time": 100},
        )
        node = orchestrator.registry.get_node("cx/cx1")
        item = node.queue_pop()
        assert item.type == ChangeType.UPDATABLE_CONFIG
        assert item.endpoint == "updatable_config/gains"
        assert item.values == {"start_time": 100}

    def test_submit_resync(self, orchestrator):
        orchestrator.submit_resync("cx/cx1")
        node = orchestrator.registry.get_node("cx/cx1")
        item = node.queue_pop()
        assert item.type == ChangeType.RESYNC

    def test_submit_group_base_config(self, orchestrator):
        orchestrator.submit_group_base_config("cx", "num_elements: 512\n")
        cx1 = orchestrator.registry.get_node("cx/cx1")
        cx2 = orchestrator.registry.get_node("cx/cx2")
        recv1 = orchestrator.registry.get_node("recv/recv1")
        assert not cx1.queue_empty
        assert not cx2.queue_empty
        assert recv1.queue_empty

        for node in (cx1, cx2):
            item = node.queue_pop()
            assert item.type == ChangeType.BASE_CONFIG
            assert item.config_content == "num_elements: 512\n"

    def test_submit_group_updatable_config(self, orchestrator):
        orchestrator.submit_group_updatable_config(
            "cx", "updatable_config/gains", {"start_time": 200},
        )
        cx1 = orchestrator.registry.get_node("cx/cx1")
        cx2 = orchestrator.registry.get_node("cx/cx2")
        recv1 = orchestrator.registry.get_node("recv/recv1")
        for node in (cx1, cx2):
            item = node.queue_pop()
            assert item.type == ChangeType.UPDATABLE_CONFIG
            assert item.endpoint == "updatable_config/gains"
            assert item.values == {"start_time": 200}
        assert recv1.queue_empty


class TestProcessNode:
    def test_poll_node_down(self, orchestrator):
        """POLL item for an unreachable node sets status to DOWN."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        node.get_status = MagicMock(return_value=NodeStatus.DOWN)

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        assert node.status == NodeStatus.DOWN
        assert node.error == "Unreachable"

    def test_poll_node_up_no_drift(self, orchestrator):
        """POLL item for a node with matching config sets status to UP."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        rendered = node.rendered_config

        node.get_status = MagicMock(return_value=NodeStatus.STARTED)
        node.get_config = MagicMock(return_value=rendered)
        node.get_version = MagicMock(return_value="2024.11")

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        assert node.status == NodeStatus.STARTED

    def test_poll_node_drift_triggers_push(self, orchestrator):
        """POLL detects drift and pushes config (kill -> start)."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True

        node.get_status = MagicMock(side_effect=[
            NodeStatus.STARTED,      # _sync_node probe
            NodeStatus.STARTED,      # _push_config probe (not idle, so kill)
            NodeStatus.STOPPED,    # wait loop
            NodeStatus.STOPPED,    # post-loop check
        ])
        node.get_config = MagicMock(return_value={"wrong": "config"})
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock(return_value=True)
        node.start = MagicMock(return_value=True)

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_called_once()
        node.start.assert_called_once()
        assert node.status == NodeStatus.STARTED

    def test_base_config_change_forces_restart(self, orchestrator):
        """BASE_CONFIG item writes to disk and triggers restart."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        desired_after = {"num_elements": 512}

        node.get_status = MagicMock(side_effect=[
            NodeStatus.STARTED,      # _sync_node probe
            NodeStatus.STARTED,      # _push_config probe (not idle, so kill)
            NodeStatus.STOPPED,    # wait loop check
            NodeStatus.STOPPED,    # post-loop check
        ])
        node.get_config = MagicMock(return_value=desired_after)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock(return_value=True)
        node.start = MagicMock(return_value=True)

        node.queue_put(ChangeItem(
            type=ChangeType.BASE_CONFIG,
            node_key="cx/cx1",
            config_content="num_elements: 512\n",
        ))
        orchestrator._process_node(node)

        # Config was written to disk
        assert node.rendered_config == desired_after
        # And kotekan was restarted
        node.kill.assert_called_once()
        node.start.assert_called_once()

    def test_updatable_only_no_restart(self, orchestrator):
        """UPDATABLE_CONFIG item saves to store and syncs without restart."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        rendered = node.rendered_config

        node.get_status = MagicMock(return_value=NodeStatus.STARTED)
        node.get_config = MagicMock(return_value=rendered)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock()
        node.push_updatable = MagicMock(return_value=True)

        node.queue_put(ChangeItem(
            type=ChangeType.UPDATABLE_CONFIG,
            node_key="cx/cx1",
            endpoint="updatable_config/gains",
            values={"start_time": 100},
        ))
        orchestrator._process_node(node)

        node.kill.assert_not_called()
        assert node.status == NodeStatus.STARTED
        assert node.updatable_config == {
            "updatable_config/gains": {"start_time": 100}
        }

    def test_stale_updatable_not_pushed(self, orchestrator):
        """Stored updatable endpoint removed from base config is not pushed."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        rendered = node.rendered_config

        # Store an updatable override for an endpoint not in the base config.
        node.save_updatable("updatable_config/removed", {"val": 1})

        node.get_status = MagicMock(return_value=NodeStatus.STARTED)
        node.get_config = MagicMock(return_value=rendered)
        node.get_version = MagicMock(return_value="2024.11")
        node.push_updatable = MagicMock(return_value=True)

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.push_updatable.assert_not_called()

    def test_resync_forces_restart(self, orchestrator):
        """RESYNC item forces a restart even with no config changes."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True
        rendered = node.rendered_config

        node.get_status = MagicMock(side_effect=[
            NodeStatus.STARTED,      # _sync_node probe
            NodeStatus.STARTED,      # _push_config probe (not idle, so kill)
            NodeStatus.STOPPED,    # wait loop check
            NodeStatus.STOPPED,    # post-loop check
        ])
        node.get_config = MagicMock(return_value=rendered)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock(return_value=True)
        node.start = MagicMock(return_value=True)

        node.queue_put(ChangeItem(type=ChangeType.RESYNC, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_called_once()
        node.start.assert_called_once()

    def test_idle_node_started_without_kill(self, orchestrator):
        """An idle node should receive /start directly, no /kill."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True

        node.get_status = MagicMock(side_effect=[
            NodeStatus.STOPPED,    # _sync_node probe
            NodeStatus.STOPPED,    # _push_config probe (already idle)
        ])
        node.get_config = MagicMock(return_value=None)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock()
        node.start = MagicMock(return_value=True)

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_not_called()
        node.start.assert_called_once()
        assert node.status == NodeStatus.STARTED

    def test_multiple_items_batched(self, orchestrator):
        """Multiple items are drained before a single sync."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = True

        node.get_status = MagicMock(side_effect=[
            NodeStatus.STARTED,      # _sync_node probe
            NodeStatus.STARTED,      # _push_config probe (not idle, so kill)
            NodeStatus.STOPPED,    # wait loop check
            NodeStatus.STOPPED,    # post-loop check
        ])
        node.get_config = MagicMock(return_value={"wrong": "config"})
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock(return_value=True)
        node.start = MagicMock(return_value=True)

        node.queue_put(ChangeItem(
            type=ChangeType.BASE_CONFIG,
            node_key="cx/cx1",
            config_content="num_elements: 256\n",
        ))
        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))

        orchestrator._process_node(node)

        assert node.rendered_config == {"num_elements": 256}
        node.kill.assert_called_once()
        node.start.assert_called_once()

    def test_stopped_node_kills_running_kotekan(self, orchestrator):
        """A node with started=False kills kotekan if it is running."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = False

        node.get_status = MagicMock(return_value=NodeStatus.STARTED)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock(return_value=True)
        node.start = MagicMock()

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_called_once()
        node.start.assert_not_called()
        assert node.status == NodeStatus.STOPPED

    def test_stopped_node_leaves_stopped_alone(self, orchestrator):
        """A node with started=False does nothing if already stopped."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = False

        node.get_status = MagicMock(return_value=NodeStatus.STOPPED)
        node.get_version = MagicMock(return_value="2024.11")
        node.kill = MagicMock()
        node.start = MagicMock()

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_not_called()
        node.start.assert_not_called()
        assert node.status == NodeStatus.STOPPED

    def test_stopped_node_does_not_push_updatable(self, orchestrator):
        """A node with started=False never pushes updatable config."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = False
        node.save_updatable("updatable_config/gains", {"start_time": 100})

        node.get_status = MagicMock(return_value=NodeStatus.STOPPED)
        node.get_version = MagicMock(return_value="2024.11")
        node.push_updatable = MagicMock()

        node.queue_put(ChangeItem(
            type=ChangeType.UPDATABLE_CONFIG,
            node_key="cx/cx1",
            endpoint="updatable_config/gains",
            values={"start_time": 200},
        ))
        orchestrator._process_node(node)

        node.push_updatable.assert_not_called()

    def test_stopped_node_down_stays_down(self, orchestrator):
        """A node with started=False that is down stays down."""
        node = orchestrator.registry.get_node("cx/cx1")
        node.started = False

        node.get_status = MagicMock(return_value=NodeStatus.DOWN)
        node.kill = MagicMock()
        node.start = MagicMock()

        node.queue_put(ChangeItem(type=ChangeType.POLL, node_key="cx/cx1"))
        orchestrator._process_node(node)

        node.kill.assert_not_called()
        node.start.assert_not_called()
        assert node.status == NodeStatus.DOWN
