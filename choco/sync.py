"""Background sync loop: polls kotekan instances and reconciles config."""

import collections
import logging
import time

import gevent
from gevent.lock import BoundedSemaphore

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .state import (
    Node, Registry, NodeStatus,
    strip_updatable_values, find_updatable_blocks,
)

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 5 # seconds
DEFAULT_RESTART_TIMEOUT = 10 # seconds

class ConfigFileHandler(FileSystemEventHandler):
    """Watch the configs directory for changes and trigger a reload."""

    def __init__(self, sync_loop: "SyncLoop"):
        self._sync_loop = sync_loop

    def on_modified(self, event):
        if event.src_path.endswith((".yaml", ".yml", ".j2")):
            logger.info(f"Config file changed: {event.src_path}")
            self._sync_loop.on_config_changed()

    def on_created(self, event):
        if event.src_path.endswith((".yaml", ".yml", ".j2")):
            logger.info(f"Config file created: {event.src_path}")
            self._sync_loop.on_config_changed()

    def on_deleted(self, event):
        if event.src_path.endswith((".yaml", ".yml", ".j2")):
            logger.info(f"Config file deleted: {event.src_path}")
            self._sync_loop.on_config_changed()


class SyncLoop:
    """Background loop that polls kotekan instances and reconciles state."""

    def __init__(self, registry: Registry, socketio=None,
                 poll_interval: float = DEFAULT_POLL_INTERVAL):
        self.registry = registry
        self.socketio = socketio
        self.poll_interval = poll_interval
        self._observer: Observer | None = None
        self._running = False
        self._push_locks: dict[str, BoundedSemaphore] = collections.defaultdict(BoundedSemaphore)

    def start_file_watcher(self):
        handler = ConfigFileHandler(self)
        self._observer = Observer()
        self._observer.schedule(
            handler, str(self.registry.configs_dir), recursive=True
        )
        self._observer.daemon = True
        self._observer.start()
        logger.info(f"Watching config directory: {self.registry.configs_dir}")

    def stop_file_watcher(self):
        if self._observer:
            self._observer.stop()
            self._observer.join()

    def on_config_changed(self):
        """Called when a config file changes on disk. Auto-pushes changed configs."""
        old_configs = self.registry.config_store.desired_configs
        self.registry.config_store.reload()
        self.registry._reload_config_overrides()
        self._emit("config_reloaded", {})

        # Push changed configs to affected nodes (fire-and-forget — the poll
        # loop and per-node locks handle convergence)
        new_configs = self.registry.config_store.desired_configs
        changed = {k for k in new_configs if new_configs.get(k) != old_configs.get(k)}
        for key in self.registry.nodes:
            config_name = self.registry.get_config_name(key)
            if config_name in changed:
                logger.info(f"Config '{config_name}' changed on disk, pushing to {key}")
                gevent.spawn(self.push_config, key)

    def run(self):
        """Main sync loop. Intended to run in a background thread."""
        self._running = True
        self.start_file_watcher()
        logger.info(
            f"Sync loop started (polling every {self.poll_interval}s, "
            f"{len(self.registry.nodes)} nodes)"
        )

        while self._running:
            self.poll_all()
            gevent.sleep(self.poll_interval)

    def stop(self):
        self._running = False
        self.stop_file_watcher()

    def poll_all(self):
        """Poll every registered node and update state."""
        for key, node in self.registry.nodes.items():
            prev_status = node.status
            self._poll_node(key, node)
            if node.status != prev_status:
                self._emit("node_status_changed", {
                    "node": key,
                    "status": node.status.value,
                    "last_seen": node.last_seen_ago,
                })

    def _poll_node(self, key: str, node):
        """Poll a single node: check status and detect config drift."""
        
        if node.status == NodeStatus.SYNCING:
            # Waiting for push_config to complete.
            return
        else:
            probe = node.get_status()

            if probe == NodeStatus.DOWN:
                node.status = NodeStatus.DOWN
                node.error = "Unreachable"
                return

            if probe == NodeStatus.UNKNOWN:
                node.status = NodeStatus.UNKNOWN
                node.error = "Unknown State"

            node.last_seen = time.time()
            node.version = node.get_version()

            if probe == NodeStatus.IDLE:
                node.status = NodeStatus.IDLE
                node.error = "Not running" # not an error necessarily

            node.error = None

            # Try to validate the node is running the correct config.

            config_name = self.registry.get_config_name(key)
            desired = self.registry.config_store.get_desired_config(config_name)
            if desired is None:
                node.error = "No valid desired config"
                return

            actual = node.get_config()
            if actual is None:
                node.error = "Unable to get remote node config"

            if strip_updatable_values(actual) == strip_updatable_values(desired):
                # Base config matches.
                if actual is not None:
                    node.status = NodeStatus.UP
                    # Also sync any stored updatable config values.
                    self._sync_updatable(key, node, actual)
            else:
                logger.info(f"Config drift detected on {key}, pushing desired config")
                node.status = NodeStatus.SYNCING
                gevent.spawn(self.push_config, key)

    def push_config(self, key: str) -> bool:
        """Push the desired config to a node (stop + start with new config).

        Uses a per-node lock so concurrent pushes to different nodes run in
        parallel, but only one push per node runs at a time.
        """
        node = self.registry.get_node(key)
        if node is None:
            logger.error(f"Node {key} not found")
            return False

        lock = self._push_locks[key]
        if not lock.acquire(blocking=False):
            logger.debug(f"Push already in progress for {key}, skipping")
            return False

        node.status = NodeStatus.SYNCING
        self._emit("node_status_changed", {
            "node": key,
            "status": node.status.value,
        })
        try:
            return self._push_config(node)
        finally:
            lock.release()

    def _push_config(self, node: Node) -> bool:
        """Internal push implementation (caller must hold the per-node lock)."""
        key = node.key
        config_name = self.registry.get_config_name(key)
        desired = self.registry.config_store.get_desired_config(config_name)
        if desired is None:
            logger.warning(f"No config '{config_name}' for {key}")
            node.status = NodeStatus.UNKNOWN
            node.error = f"No config '{config_name}'"
            return False

        probe = node.get_status()
        if probe == NodeStatus.DOWN :
            logger.warning(f"Unable to send config to {key} due to kotekan down")
            node.status = probe
            return False
        else :
            # Kill the running instance; the daemon restarts kotekan, then we
            # provide the new config via /start.
            logger.info(f"Sending /kill to {key}")
            node.kill()

        # Wait for the daemon to restart kotekan into idle state.
        # After kill, kotekan may be briefly unreachable before the
        # daemon restarts it — that's expected, keep waiting.
        logger.info(f"Waiting for {key} to restart into idle state")
        for _ in range(10):
            gevent.sleep(DEFAULT_RESTART_TIMEOUT//10)
            probe = node.get_status()
            if probe == NodeStatus.IDLE:
                break
            # UP: kill hasn't taken effect yet; DOWN: daemon hasn't
            # restarted kotekan yet. Either way, keep waiting.
        else :
            logger.warning(f"Timed out waiting for {key} to become idle after kill.")

        probe = node.get_status()
        if probe != NodeStatus.IDLE :
            node.status = probe
            node.error = "Current status is: " + node.status.value + ", failed to push config."
            return False

        logger.info(f"Sending config to {key} via /start")
        success = node.start(desired)
        if success:
            logger.info(f"Successfully pushed config to {key}")
            node.status = NodeStatus.UP
            node.error = None
            # Re-apply stored updatable config values after restart.
            stored = self.registry.updatable_store.get(key)
            if stored:
                for endpoint, values in stored.items():
                    if not node.update_config(f"/{endpoint}", values):
                        logger.warning(
                            f"Failed to re-apply updatable config /{endpoint} to {key}"
                        )
        else:
            logger.error(f"Failed to push config to {key}")
            node.status = NodeStatus.DOWN
            node.error = "Failed to push config via /start"
        return success

    def _sync_updatable(self, key: str, node: Node, live_config: dict):
        """Push any stored updatable config values that differ from live."""
        stored = self.registry.updatable_store.get(key)
        if not stored:
            return
        live_blocks = find_updatable_blocks(live_config)
        for endpoint, values in stored.items():
            if live_blocks.get(endpoint) != values:
                logger.info(f"Updatable config drift on {key} at /{endpoint}")
                node.update_config(f"/{endpoint}", values)

    def _emit(self, event: str, data: dict):
        if self.socketio:
            self.socketio.emit(event, data, namespace="/")
