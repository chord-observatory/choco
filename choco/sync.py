"""Queue-based sync system for pushing configs to kotekan nodes.

Architecture:

    Input Queue (serialized) --> Per-Node Queues (FIFO, on Node) --> Worker Pool

Changes enter through the single serialized input queue, which fans them out
to per-node queues (each Node holds its own).  A pool of worker greenlets
scans nodes; when a worker finds an unlocked, non-empty queue it locks it,
drains all pending changes (writing base configs to YAML files and updatable
configs to the JSON store), then syncs the result to the remote kotekan
instance: a full restart (kill -> start) if any base-config changes were
applied, or just updatable-endpoint POSTs otherwise.

Periodic polling adds POLL items for every node so remote drift is detected
even when no local changes are made.
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum

import gevent
from gevent.lock import BoundedSemaphore

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .state import (
    Node, Registry, NodeStatus,
    strip_updatable_values, find_updatable_blocks,
)

logger = logging.getLogger(__name__)


# --- Queue data types ---

class ChangeType(Enum):
    """Types of changes that flow through the queue system."""
    BASE_CONFIG = "base_config"
    UPDATABLE_CONFIG = "updatable_config"
    POLL = "poll"
    RESYNC = "resync"


@dataclass
class ChangeItem:
    """A single queued change destined for one node."""
    type: ChangeType
    node_key: str
    config_content: str | None = None  # BASE_CONFIG: base config text (YAML/Jinja2)
    endpoint: str | None = None        # UPDATABLE_CONFIG: REST path
    values: dict | None = None         # UPDATABLE_CONFIG: JSON payload


# --- Serialized input queue ---

class InputQueue:
    """Single serialized entry point that distributes items to node queues.

    Every public method acquires the same lock, so only one caller submits
    at a time.
    """

    def __init__(self, registry: Registry):
        self._lock = BoundedSemaphore()
        self.registry = registry

    def submit_node(self, item: ChangeItem):
        """Submit a change for one node."""
        with self._lock:
            node = self.registry.get_node(item.node_key)
            if node is not None:
                node.queue_put(item)
            else:
                logger.warning(f"No node for key {item.node_key}")

    def submit_group(self, group: str, make_item):
        """Submit a change for every node in *group*.

        *make_item(node_key)* is called once per matching node to create the
        ChangeItem.
        """
        with self._lock:
            for key, node in self.registry.nodes.items():
                if node.group == group:
                    node.queue_put(make_item(key))

    def submit_all(self, make_item):
        """Submit a change for every registered node."""
        with self._lock:
            for key, node in self.registry.nodes.items():
                node.queue_put(make_item(key))


# --- File-system watcher ---

class ConfigFileHandler(FileSystemEventHandler):
    """Detect on-disk config changes and feed them into the queue."""

    def __init__(self, orchestrator: "Orchestrator"):
        self._orchestrator = orchestrator

    def _handle(self, event, action):
        path = event.src_path
        if path.endswith((".yaml", ".yml", ".j2")):
            logger.info(f"Config file {action}: {path}")
            self._orchestrator.on_file_changed(path)
        elif path.endswith(".json") and "/.updatable/" in path:
            logger.info(f"Updatable config file {action}: {path}")
            self._orchestrator.on_file_changed(path)

    def on_modified(self, event):
        self._handle(event, "changed")

    def on_created(self, event):
        self._handle(event, "created")

    def on_deleted(self, event):
        self._handle(event, "deleted")


# --- Sync loop (orchestrator) ---

class Orchestrator:
    """Manages the input queue and worker pool.

    Each :class:`Node` holds its own change queue.  Call :meth:`run` to
    start the worker pool and periodic polling (blocks until :meth:`stop`
    is called).  Use the ``submit_*`` helpers to feed changes from web
    routes or other callers.
    """

    def __init__(self, registry: Registry, socketio=None,
                 poll_interval: int = 5, restart_timeout: int = 10,
                 num_workers: int = 4):
        self.registry = registry
        self.socketio = socketio
        self.poll_interval = poll_interval
        self.restart_timeout = restart_timeout
        self.num_workers = num_workers
        self._observer: Observer | None = None
        self._running = False

        self.input_queue = InputQueue(registry)
        self._assign_queue_locks()

    def _assign_queue_locks(self):
        """Ensure every Node has a gevent-aware queue lock."""
        for node in self.registry.nodes.values():
            if node._queue_lock is None:
                node._queue_lock = BoundedSemaphore()

    # --- File-watcher callbacks ---

    def start_file_watcher(self):
        handler = ConfigFileHandler(self)
        self._observer = Observer()
        self._observer.schedule(
            handler, str(self.registry.configs_dir), recursive=True,
        )
        self._observer.daemon = True
        self._observer.start()
        logger.info(f"Watching config directory: {self.registry.configs_dir}")

    def stop_file_watcher(self):
        if self._observer:
            self._observer.stop()
            self._observer.join()

    def on_file_changed(self, path: str):
        """Reload the affected node's config from disk and queue a poll.

        If vars.yaml changed, all nodes are re-rendered.  If nodes.yaml
        changed, the registry is fully reloaded (clear and rebuild).
        """
        from pathlib import Path
        p = Path(path)
        configs_dir = self.registry.configs_dir

        # nodes.yaml is a full registry reset — clear and rebuild.
        if p.name == "nodes.yaml" and p.parent == configs_dir:
            self.apply_nodes_update()
            return

        # vars.yaml affects all nodes — reload template vars and re-render.
        if p.name == "vars.yaml":
            template_vars = self.registry._load_vars()
            for node in self.registry.nodes.values():
                node.template_vars = template_vars
                node.load_config()
            self._emit("config_reloaded", {})
            self.input_queue.submit_all(
                lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
            )
            return

        # Resolve path to a node key: strip configs_dir prefix, .updatable/
        # prefix, and file extension to get <group>/<name>.
        try:
            rel = p.relative_to(configs_dir)
        except ValueError:
            return
        rel_str = str(rel)
        if rel_str.startswith(".updatable/"):
            rel = Path(rel_str.removeprefix(".updatable/"))
        node_key = str(rel.with_suffix(""))

        node = self.registry.get_node(node_key)
        if node is None:
            return

        node.load_config()
        node.load_updatable()
        self._emit("config_reloaded", {})
        self.input_queue.submit_node(
            ChangeItem(type=ChangeType.POLL, node_key=node_key)
        )

    # --- Main loop ---

    def run(self):
        """Start the worker pool and periodic polling.  Blocks."""
        self._running = True
        self.start_file_watcher()

        for _ in range(self.num_workers):
            gevent.spawn(self._worker_loop)

        logger.info(
            f"Sync loop started ({self.num_workers} workers, "
            f"polling every {self.poll_interval}s, "
            f"{len(self.registry.nodes)} nodes)"
        )

        while self._running:
            gevent.sleep(self.poll_interval)
            self.input_queue.submit_all(
                lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
            )

    def stop(self):
        self._running = False
        self.stop_file_watcher()

    # --- Worker pool ---

    def _worker_loop(self):
        """Continuously scan node queues for work."""
        while self._running:
            found_work = False
            # Snapshot the node list: a concurrent registry reload may
            # replace ``self.registry.nodes`` in place, and iterating a
            # dict that mutates mid-loop would raise RuntimeError.
            for node in list(self.registry.nodes.values()):
                if node.queue_try_lock():
                    if node.queue_empty:
                        node.queue_unlock()
                        continue
                    try:
                        self._process_node(node)
                        found_work = True
                    finally:
                        node.queue_unlock()
            if not found_work:
                gevent.sleep(0.1)

    def _process_node(self, node: Node):
        """Drain all items from a node's queue, then sync to remote."""
        had_base_change = False

        # 1. Drain queue -- apply each item to on-disk files.
        while True:
            item = node.queue_pop()
            if item is None:
                break
            if item.type == ChangeType.BASE_CONFIG:
                if item.config_content is not None:
                    node.save_base(item.config_content)
                    logger.info(f"Wrote base config for {node.key}")
                had_base_change = True
            elif item.type == ChangeType.UPDATABLE_CONFIG:
                if item.endpoint and item.values is not None:
                    node.save_updatable(item.endpoint, item.values)
                    logger.info(f"Wrote updatable config for {node.key} "
                                f"at /{item.endpoint}")
            elif item.type == ChangeType.RESYNC:
                had_base_change = True  # force restart
            # POLL: no file changes

        # 2. Sync to remote kotekan instance.
        prev_status = node.status
        self._sync_node(node, had_base_change)
        if node.status != prev_status:
            self._emit("node_status_changed", {
                "node": node.key,
                "status": node.status.value,
                "last_seen": node.last_seen_ago,
            })

    # --- Remote sync ---

    def _sync_node(self, node: Node, had_base_change: bool):
        """Compare desired state with the remote node and reconcile."""
        if node.status == NodeStatus.SYNCING:
            return

        probe = node.get_status()
        node.error = None

        if probe == NodeStatus.DOWN:
            node.status = NodeStatus.DOWN
            node.error = "Unreachable"
            return

        if probe == NodeStatus.UNKNOWN:
            node.status = NodeStatus.UNKNOWN
            node.error = "Unknown state"
            return

        node.last_seen = time.time()
        node.version = node.get_version()

        # If the node's desired state is not started, ensure kotekan is not running.
        if not node.started:
            if probe == NodeStatus.STARTED:
                logger.info(f"Node {node.key} should be idle; sending /kill")
                node.kill()
                node.status = NodeStatus.IDLE
            else:
                node.status = probe
            return

        desired = node.desired_config
        if desired is None:
            node.error = f"No config file ({node.config_filename})"
            return

        actual = node.get_config()

        # Node idle with no config -> start it.
        if probe == NodeStatus.IDLE and actual is None:
            self._push_config(node, desired)
            return

        if actual is None:
            node.status = NodeStatus.UNKNOWN
            node.error = "Unable to get remote config; status indeterminate."
            return

        base_drift = (strip_updatable_values(actual)
                      != strip_updatable_values(desired))

        if had_base_change or base_drift:
            self._push_config(node, desired)
        else:
            node.status = NodeStatus.STARTED
            self._sync_updatable(node, actual)

    def _push_config(self, node: Node, desired: dict) -> bool:
        """Kill -> wait for idle -> start with *desired* config.

        *desired* should already include updatable overrides (as returned
        by ``Node.desired_config``).
        """
        key = node.key
        node.status = NodeStatus.SYNCING
        self._emit("node_status_changed", {
            "node": key, "status": node.status.value,
        })

        probe = node.get_status()
        if probe == NodeStatus.DOWN:
            logger.warning(f"Cannot push config to {key}: kotekan down")
            node.status = probe
            node.error = "Unreachable"
            return False

        if probe != NodeStatus.IDLE:
            logger.info(f"Sending /kill to {key}")
            node.kill()
            logger.info(f"Waiting for {key} to reach idle state")
            for _ in range(10):
                gevent.sleep(self.restart_timeout // 10)
                if node.get_status() == NodeStatus.IDLE:
                    break
            else:
                logger.warning(
                    f"Timed out waiting for {key} to become idle"
                )

            probe = node.get_status()
            if probe != NodeStatus.IDLE:
                node.status = probe
                node.error = (f"Status is {probe.value}, "
                              f"failed to push config")
                return False

        logger.info(f"Sending config to {key} via /start")
        success = node.start(desired)
        if success:
            logger.info(f"Successfully pushed config to {key}")
            node.status = NodeStatus.STARTED
            node.error = None
        else:
            logger.error(f"Failed to push config to {key}")
            node.status = NodeStatus.UNKNOWN
            node.error = "Failed to push config via /start"
        return success

    def _sync_updatable(self, node: Node, live_config: dict):
        """Push stored updatable values that differ from the live config."""
        stored = node.updatable_config
        if not stored:
            return
        # Only push endpoints that still exist in the rendered base config.
        rendered_blocks = find_updatable_blocks(node.rendered_config) if node.rendered_config else {}
        live_blocks = find_updatable_blocks(live_config)
        for endpoint, values in stored.items():
            if endpoint not in rendered_blocks:
                continue
            if live_blocks.get(endpoint) != values:
                logger.info(f"Updatable config drift on {node.key} "
                            f"at /{endpoint}")
                if not node.push_updatable(f"/{endpoint}", values):
                    logger.warning(f"Failed to sync updatable "
                                   f"/{endpoint} to {node.key}")

    def _emit(self, event: str, data: dict):
        if self.socketio:
            self.socketio.emit(event, data, namespace="/")

    # --- Public API (called by web routes) ---

    def submit_base_config(self, node_key: str, config_content: str):
        """Queue a base-config file change for one node."""
        self.input_queue.submit_node(ChangeItem(
            type=ChangeType.BASE_CONFIG,
            node_key=node_key,
            config_content=config_content,
        ))

    def submit_updatable_config(self, node_key: str, endpoint: str,
                                values: dict):
        """Queue an updatable-config change for one node."""
        self.input_queue.submit_node(ChangeItem(
            type=ChangeType.UPDATABLE_CONFIG,
            node_key=node_key,
            endpoint=endpoint,
            values=values,
        ))

    def submit_resync(self, node_key: str):
        """Queue a forced full config re-push for one node."""
        self.input_queue.submit_node(ChangeItem(
            type=ChangeType.RESYNC,
            node_key=node_key,
        ))

    def submit_group_base_config(self, group: str, config_content: str):
        """Queue a base-config file change for every node in *group*."""
        self.input_queue.submit_group(
            group,
            lambda key: ChangeItem(
                type=ChangeType.BASE_CONFIG,
                node_key=key,
                config_content=config_content,
            ),
        )

    def submit_group_updatable_config(self, group: str, endpoint: str,
                                      values: dict):
        """Queue an updatable-config change for every node in *group*."""
        self.input_queue.submit_group(
            group,
            lambda key: ChangeItem(
                type=ChangeType.UPDATABLE_CONFIG,
                node_key=key,
                endpoint=endpoint,
                values=values,
            ),
        )

    def apply_nodes_update(self, new_data: dict | None = None):
        """Replace the node registry.

        If *new_data* is given, it is written to ``nodes.yaml`` first;
        otherwise the current on-disk file is used (for file-watcher
        reloads).  The registry is then rebuilt from scratch — all
        existing :class:`Node` objects, pending queue items, and runtime
        ``started`` toggles are discarded.  Held under the input-queue
        lock so in-flight submissions don't race the rebuild.
        """
        with self.input_queue._lock:
            if new_data is not None:
                self.registry.save_nodes_yaml(new_data)
            self.registry.reload()
            self._assign_queue_locks()
        self._emit("config_reloaded", {})
