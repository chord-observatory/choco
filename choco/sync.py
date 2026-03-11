"""Background sync loop: polls kotekan instances and reconciles config."""

import logging
import time

import gevent

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .state import Registry, NodeStatus

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 5  # seconds


class ConfigFileHandler(FileSystemEventHandler):
    """Watch the configs directory for changes and trigger a reload."""

    def __init__(self, sync_loop: "SyncLoop"):
        self._sync_loop = sync_loop

    def on_modified(self, event):
        if event.src_path.endswith((".yaml", ".yml")):
            logger.info(f"Config file changed: {event.src_path}")
            self._sync_loop.on_config_changed()

    def on_created(self, event):
        if event.src_path.endswith((".yaml", ".yml")):
            logger.info(f"Config file created: {event.src_path}")
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
        old_hashes = dict(self.registry.config_store._desired_hashes)
        self.registry.config_store.reload()
        self.registry.deploy_store.reload()

        # Find configs whose hash changed and push to affected nodes
        new_hashes = self.registry.config_store._desired_hashes
        changed = {k for k in new_hashes if new_hashes.get(k) != old_hashes.get(k)}
        for key in self.registry.nodes:
            config_name = self.registry.deploy_store.get_config_name(key)
            if config_name in changed:
                logger.info(f"Config '{config_name}' changed on disk, pushing to {key}")
                self.push_config(key)

        self._emit("config_reloaded", {})

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
            prev_status = node.state.status
            self._poll_node(key, node)
            if node.state.status != prev_status:
                self._emit("node_status_changed", {
                    "node": key,
                    "status": node.state.status.value,
                    "last_seen": node.state.last_seen_ago,
                })

    def _poll_node(self, key: str, node):
        """Poll a single node: check status and detect config drift."""
        status = node.client.get_status()

        if not status.reachable:
            node.state.status = NodeStatus.DOWN
            node.state.error = "Unreachable"
            return

        node.state.last_seen = time.time()
        node.state.error = None

        if not status.ok:
            node.state.status = NodeStatus.DOWN
            node.state.error = "Not running"
            return

        # Resolve which config this node uses
        config_name = self.registry.deploy_store.get_config_name(key)
        desired_hash = self.registry.config_store.get_desired_hash(config_name)
        if desired_hash is None:
            node.state.status = NodeStatus.UP
            return

        actual_hash = node.client.get_config_hash()
        node.state.config_hash = actual_hash

        if actual_hash is None:
            node.state.status = NodeStatus.UP
            return

        if actual_hash == desired_hash:
            node.state.status = NodeStatus.UP
        else:
            node.state.status = NodeStatus.DRIFT
            logger.info(
                f"Config drift detected on {key}: "
                f"desired={desired_hash[:8]}... actual={actual_hash[:8]}..."
            )

    def push_config(self, key: str) -> bool:
        """Push the desired config to a node (stop + start with new config)."""
        node = self.registry.get_node(key)
        if node is None:
            logger.error(f"Node {key} not found")
            return False

        config_name = self.registry.deploy_store.get_config_name(key)
        desired = self.registry.config_store.get_desired_config(config_name)
        if desired is None:
            logger.warning(f"No config '{config_name}' for {key}")
            return False

        # Kotekan has no "replace config" endpoint — must stop then start.
        status = node.client.get_status()
        if status.ok:
            node.client.stop()

        success = node.client.start(desired)
        if success:
            logger.info(f"Pushed config to {key}")
        else:
            logger.error(f"Failed to push config to {key}")
        return success

    def _emit(self, event: str, data: dict):
        if self.socketio:
            self.socketio.emit(event, data, namespace="/")

    def get_template_context(self) -> dict:
        """Get context for dashboard templates."""
        return {
            "nodes": self.registry.nodes,
            "deploy": self.registry.deploy_store,
        }
