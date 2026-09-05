"""Queue-based sync system for pushing configs to kotekan nodes.

Architecture:

    Serialized submit --> Per-Node Queues (FIFO, on Node) --> One owner
                                                              greenlet
                                                              per node

Changes enter through the orchestrator's ``submit_*`` methods, which share
one lock (so only one caller submits at a time) and fan items out to
per-node queues (each Node holds its own), waking that node's worker.
Each :class:`NodeWorker` greenlet owns exactly one node: it drains all
pending changes (writing base configs to YAML files and updatable configs
to the JSON store), then syncs the result to the remote kotekan instance —
a full restart (kill -> start) if any base-config changes were applied, or
just updatable-endpoint POSTs otherwise.  Single ownership means a node's
queue needs no lock (producers only append; greenlets don't preempt) and
gives each node visible per-worker state: phase, failure count, timing.

Remote drift is detected even when no local changes are made: each worker
schedules its own periodic check (every ``poll_interval``, backing off
towards ``max_retry_interval`` while its node is unreachable).  How many
nodes *restart* at once is bounded separately by ``max_concurrent_pushes``
— polling concurrency scales with the cluster and needs no cap.  The
orchestrator's own loop only scans config-file mtimes, so local edits to
the config directory are picked up within one poll interval (no inotify
machinery; a stat scan also works on NFS).
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import gevent
from gevent.event import Event
from gevent.lock import BoundedSemaphore

from .state import (
    Node, Registry, NodeStatus,
    strip_updatable_values, find_updatable_blocks,
    _CONFIG_SUFFIXES,
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


# --- Per-node worker ---

class WorkerPhase(Enum):
    """What a node's worker greenlet is currently doing.

    Distinct from :class:`NodeStatus`, which is what the *node* is: a
    node can be STARTED while its worker is IDLE (parked until the next
    check), or DOWN while its worker is QUEUED_FOR_PUSH.
    """
    IDLE = "idle"                        # parked, next check scheduled
    DOWN = "down"                        # parked; node unreachable, backing off
    DRAINING = "draining"                # applying queued changes to disk
    PROBING = "probing"                  # reading remote status / config
    QUEUED_FOR_PUSH = "queued-for-push"  # restart needed, waiting for a turn
    PUSHING = "pushing"                  # /kill or /start in flight
    AWAITING_IDLE = "awaiting-idle"      # killed, waiting for kotekan to go idle


class NodeWorker:
    """One greenlet owning one node for that node's lifetime.

    A node has exactly one owner, so its queue needs no lock: producers
    only ``append``, this greenlet is the only consumer, and greenlets
    never switch mid-bytecode.  Ownership is also what makes per-node
    state possible — phase, timing and failure counts live here and are
    surfaced through :meth:`snapshot`.

    Workers are never force-killed: :meth:`stop` sets a flag and wakes
    the greenlet, which finishes any in-flight cycle first.  A config
    push therefore always completes its kill -> start sequence instead
    of aborting between the two and leaving kotekan down.
    """

    def __init__(self, node: Node, orchestrator: "Orchestrator"):
        self.node = node
        self.orch = orchestrator
        self.wake = Event()
        self.greenlet: gevent.Greenlet | None = None
        self._stop = False

        # Monitoring state.  Written only by this greenlet; read freely
        # by request greenlets without a lock, since each read is a
        # single attribute load and greenlets do not preempt.
        self.phase = WorkerPhase.IDLE
        self.phase_since = time.time()
        self.consecutive_failures = 0
        self.last_cycle_s: float | None = None
        self.next_check = 0.0  # 0 => the first check runs immediately
        self.cycles = 0

    def set_phase(self, phase: WorkerPhase):
        if phase is not self.phase:
            self.phase = phase
            self.phase_since = time.time()

    def snapshot(self) -> dict:
        """Monitoring view of this worker, for dashboards and the API."""
        return {
            "phase": self.phase.value,
            "phase_age_s": round(time.time() - self.phase_since, 1),
            "consecutive_failures": self.consecutive_failures,
            "last_cycle_s": (round(self.last_cycle_s, 3)
                             if self.last_cycle_s is not None else None),
            "next_check_in_s": round(max(0.0, self.next_check - time.time()), 1),
            "queued": self.node.queue_depth,
            "cycles": self.cycles,
        }

    def start(self):
        self.greenlet = gevent.spawn(self.run)

    def stop(self):
        """Ask the worker to exit after any in-flight cycle."""
        self._stop = True
        self.wake.set()

    def run(self):
        while self.orch._running and not self._stop:
            # Clear before testing the queue.  A producer appending
            # between the clear and the test is caught by the test; one
            # appending after it breaks the wait.  Clearing after the
            # wait instead would discard a signal raised during the
            # preceding cycle, parking a pending change until the next
            # scheduled check — up to max_retry_interval away.  Nothing
            # between the clear and the wait yields to the hub, so no
            # producer can interleave inside the sequence itself.
            self.wake.clear()
            now = time.time()
            if self.node.queue_empty and now < self.next_check:
                down = self.node.status in (NodeStatus.DOWN,
                                            NodeStatus.UNKNOWN)
                self.set_phase(WorkerPhase.DOWN if down else WorkerPhase.IDLE)
                self.wake.wait(timeout=self.next_check - now)
                continue
            self._cycle()

    def _cycle(self):
        started = time.time()
        try:
            self.orch._process_node(self.node, worker=self)
        except Exception:
            # A worker must outlive any single failure: with one owner
            # per node, an unhandled exception here would strand this
            # node for the life of the process.
            logger.exception(f"Sync cycle failed for {self.node.key}")
            self.node.error = "Internal error during sync (see log)"
            self.consecutive_failures += 1
        else:
            # _sync_node leaves node.status reflecting this cycle's
            # probe on every path, so it doubles as the "did the node
            # answer" signal.  Back off on unreachability only: a node
            # that answers but has a bad config file keeps its normal
            # cadence so its status stays fresh, while a node that
            # cannot be reached costs a full request timeout per
            # attempt, which is the case worth spacing out.
            if self.node.status in (NodeStatus.DOWN, NodeStatus.UNKNOWN):
                self.consecutive_failures += 1
            else:
                self.consecutive_failures = 0
        finally:
            self.last_cycle_s = time.time() - started
            self.cycles += 1
            self.next_check = time.time() + self._interval()

    def _interval(self) -> float:
        """Seconds until this node's next scheduled check."""
        base = self.orch.poll_interval
        if not self.consecutive_failures:
            return base
        # consecutive_failures is unbounded, so clamp the exponent — a
        # node down overnight would otherwise build a huge integer only
        # to discard it against the ceiling.
        doublings = min(self.consecutive_failures, 8)
        return min(base * 2 ** doublings, self.orch.max_retry_interval)


# --- Sync loop (orchestrator) ---

class Orchestrator:
    """Manages change submission and the per-node workers.

    Each :class:`Node` holds its own change queue, drained by its own
    :class:`NodeWorker` greenlet.  Call :meth:`run` to spawn the workers
    and the config-file scan (blocks until :meth:`stop` is called).
    Feed changes in from web routes or other callers with the
    ``submit_*`` methods, which construct nothing themselves — they take
    :class:`ChangeItem` objects (or a ``make_item`` factory for fan-out)
    and distribute them to node queues under one shared lock, waking
    each queue's worker.
    """

    def __init__(self, registry: Registry,
                 poll_interval: int = 5, restart_timeout: int = 10,
                 max_concurrent_pushes: int = 4,
                 max_retry_interval: int = 60):
        self.registry = registry
        self.poll_interval = poll_interval
        self.restart_timeout = restart_timeout
        self.max_concurrent_pushes = max_concurrent_pushes
        self.max_retry_interval = max_retry_interval
        self._running = False
        self._file_mtimes: dict[Path, float] = {}

        # Serializes all submissions (and pauses them during a registry
        # rebuild in apply_nodes_update).
        self._submit_lock = BoundedSemaphore()
        # Bounds how many nodes may be mid-restart at once; polling
        # concurrency is deliberately unbounded (one greenlet per node).
        self._push_semaphore = BoundedSemaphore(max_concurrent_pushes)
        self._workers: dict[str, NodeWorker] = {}

    def worker_status(self, key: str) -> dict | None:
        """The node's worker snapshot, or None if no worker is running."""
        worker = self._workers.get(key)
        return worker.snapshot() if worker is not None else None

    # --- Submissions (serialized entry point) ---

    def _enqueue(self, node: Node, item: ChangeItem):
        """Append *item* and wake the node's worker.

        Caller holds ``_submit_lock``.  A node without a worker (before
        :meth:`run`, or mid-reload) just keeps the item queued: workers
        start with an immediate first check and drain it then.
        """
        node.queue_put(item)
        worker = self._workers.get(node.key)
        if worker is not None:
            worker.wake.set()

    def submit_node(self, item: ChangeItem):
        """Submit a change for one node."""
        with self._submit_lock:
            node = self.registry.get_node(item.node_key)
            if node is not None:
                self._enqueue(node, item)
            else:
                logger.warning(f"No node for key {item.node_key}")

    def submit_group(self, group: str, make_item):
        """Submit a change for every node in *group*.

        *make_item(node_key)* is called once per matching node to create the
        ChangeItem.
        """
        with self._submit_lock:
            for key, node in self.registry.nodes.items():
                if node.group == group:
                    self._enqueue(node, make_item(key))

    def submit_all(self, make_item):
        """Submit a change for every registered node."""
        with self._submit_lock:
            for key, node in self.registry.nodes.items():
                self._enqueue(node, make_item(key))

    # --- Config-directory change detection ---

    def _config_file_mtimes(self) -> dict[Path, float]:
        """Snapshot mtimes of every config file under configs_dir.

        Covers base configs (``.yaml`` / ``.yml`` / ``.j2`` anywhere,
        which includes ``nodes.yaml`` and ``vars.yaml``) and the
        ``.updatable/`` JSON store.
        """
        mtimes: dict[Path, float] = {}
        root = self.registry.configs_dir
        if not root.is_dir():
            return mtimes
        for path in root.rglob("*"):
            if not (path.suffix in _CONFIG_SUFFIXES
                    or (path.suffix == ".json" and ".updatable" in path.parts)):
                continue
            try:
                if path.is_file():
                    mtimes[path] = path.stat().st_mtime
            except OSError:
                continue
        return mtimes

    def check_config_files(self):
        """Detect changed / created / deleted config files by mtime.

        Called once per poll tick.  Each detected path is handed to
        :meth:`on_file_changed`.
        """
        current = self._config_file_mtimes()
        changed = [p for p, m in current.items()
                   if self._file_mtimes.get(p) != m]
        deleted = [p for p in self._file_mtimes if p not in current]
        self._file_mtimes = current
        for path in changed + deleted:
            action = "deleted" if path in deleted else "changed"
            logger.info(f"Config file {action}: {path}")
            self.on_file_changed(str(path))

    def on_file_changed(self, path: str):
        """Reload the affected node's config from disk and queue a poll.

        If vars.yaml changed, all nodes are re-rendered.  If nodes.yaml
        changed, the registry is fully reloaded (clear and rebuild).
        """
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
            self.submit_all(
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
        self.submit_node(
            ChangeItem(type=ChangeType.POLL, node_key=node_key)
        )

    # --- Main loop ---

    def _discover_one(self, node: Node):
        """Probe ``node`` once and set its runtime state from the result."""
        status = node.get_status()
        node.started = (status == NodeStatus.STARTED)
        node.status = status
        if status in (NodeStatus.STARTED, NodeStatus.IDLE):
            node.last_seen = time.time()

    def discover_node_states(self):
        """Probe every node once and set ``node.started`` from reality.

        Called at startup so the runtime ``started`` desired state
        reflects what kotekan is actually doing rather than overwriting
        it with a default.  Probes happen in parallel via gevent
        greenlets.  Unreachable / indeterminate nodes fall back to
        ``started=False`` (idle).
        """
        if not self.registry.nodes:
            return

        greenlets = [gevent.spawn(self._discover_one, n)
                     for n in list(self.registry.nodes.values())]
        gevent.joinall(greenlets)

        running = sum(1 for n in self.registry.nodes.values() if n.started)
        logger.info(
            f"State discovery: {running}/{len(self.registry.nodes)} "
            f"nodes running"
        )

    def run(self):
        """Spawn the per-node workers and scan config files.  Blocks."""
        self._running = True

        # Baseline the config-file mtimes: the registry already loaded
        # the current on-disk state, so only *subsequent* edits count.
        self._file_mtimes = self._config_file_mtimes()

        # Set node.started from each node's actual runtime state before
        # any worker acts on it.  All nodes default to maintenance=True
        # so this is purely an observation pass; no pushes occur until
        # the operator takes nodes out of maintenance.
        self.discover_node_states()

        with self._submit_lock:
            self._respawn_workers()

        logger.info(
            f"Sync loop started ({len(self._workers)} node workers, "
            f"polling every {self.poll_interval}s, up to "
            f"{self.max_concurrent_pushes} concurrent restarts)"
        )

        # Workers schedule their own periodic checks, so this loop's
        # only job is detecting local config-file edits.
        while self._running:
            gevent.sleep(self.poll_interval)
            self.check_config_files()

    def stop(self):
        self._running = False
        with self._submit_lock:
            self._stop_workers()

    # --- Worker lifecycle ---

    def _stop_workers(self):
        """Signal every worker to exit.  Caller holds ``_submit_lock``.

        Workers finish any in-flight cycle before exiting (they are
        never force-killed), so a config push in progress completes its
        kill -> start sequence rather than leaving kotekan down.
        """
        for worker in self._workers.values():
            worker.stop()
        self._workers.clear()

    def _respawn_workers(self):
        """Replace the worker set to match the current registry.

        Caller holds ``_submit_lock``.  Always stops the existing set
        first, so concurrent callers (a web save racing the file
        watcher) end with exactly one worker per node.
        """
        self._stop_workers()
        if not self._running:
            return
        for node in self.registry.nodes.values():
            worker = NodeWorker(node, self)
            self._workers[node.key] = worker
            worker.start()

    def _process_node(self, node: Node, worker: NodeWorker | None = None):
        """Drain all items from a node's queue, then sync to remote."""
        if worker:
            worker.set_phase(WorkerPhase.DRAINING)
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
        self._sync_node(node, had_base_change, worker=worker)

    # --- Remote sync ---

    def _sync_node(self, node: Node, had_base_change: bool,
                   worker: NodeWorker | None = None):
        """Compare desired state with the remote node and reconcile.

        Every path through here leaves ``node.status`` reflecting this
        cycle's probe — :meth:`NodeWorker._cycle` reads it afterwards to
        decide whether the node answered.
        """
        if worker:
            worker.set_phase(WorkerPhase.PROBING)

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
        node.version_info = node.get_version_info()
        node.version = (node.version_info or {}).get("kotekan_version")

        # If the node's desired state is not started, ensure kotekan is not running.
        if not node.started:
            if probe == NodeStatus.STARTED and not node.maintenance:
                logger.info(f"Node {node.key} should be idle; sending /kill")
                node.kill()
                node.status = NodeStatus.IDLE
            else:
                node.status = probe
            return

        desired = node.desired_config
        if desired is None:
            node.status = probe
            node.error = (node.load_error
                          or f"No config file ({node.config_filename})")
            return

        # Refuse to push anything while a config file failed to load — the
        # in-memory desired_config is incomplete (e.g. updatable overrides
        # are missing because the JSON store is corrupt) and pushing it
        # would silently reset runtime state on the node.
        if node.load_error:
            node.status = probe
            node.error = node.load_error
            return

        # Maintenance mode: poll status but make no changes.  Push paths
        # are skipped here and kill is skipped above so choco never
        # mutates a node the operator has paused.
        if node.maintenance:
            node.status = probe
            return

        actual = node.get_config()

        # Node idle with no config -> start it.
        if probe == NodeStatus.IDLE and actual is None:
            self._push_config(node, desired, worker=worker)
            return

        if actual is None:
            node.status = NodeStatus.UNKNOWN
            node.error = "Unable to get remote config; status indeterminate."
            return

        base_drift = (strip_updatable_values(actual)
                      != strip_updatable_values(desired))

        if had_base_change or base_drift:
            self._push_config(node, desired, worker=worker)
        else:
            node.status = NodeStatus.STARTED
            self._sync_updatable(node, actual)

    def _push_config(self, node: Node, desired: dict,
                     worker: NodeWorker | None = None) -> bool:
        """Kill -> wait for idle -> start with *desired* config.

        *desired* should already include updatable overrides (as returned
        by ``Node.desired_config``).
        """
        key = node.key
        # Restarts are the disruptive operation, so they are the one
        # thing bounded cluster-wide; a worker waits its turn here while
        # plain polling continues everywhere else.
        if worker:
            worker.set_phase(WorkerPhase.QUEUED_FOR_PUSH)
        with self._push_semaphore:
            if worker:
                worker.set_phase(WorkerPhase.PUSHING)
            node.status = NodeStatus.SYNCING

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
                if worker:
                    worker.set_phase(WorkerPhase.AWAITING_IDLE)
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
                if worker:
                    worker.set_phase(WorkerPhase.PUSHING)

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

    def apply_nodes_update(self, new_data: dict | None = None):
        """Replace the node registry.

        If *new_data* is given, it is written to ``nodes.yaml`` first;
        otherwise the current on-disk file is used (for file-watcher
        reloads).  The registry is then rebuilt from scratch — all
        existing :class:`Node` objects, pending queue items, and runtime
        toggles are discarded.  Held under the submit lock so in-flight
        submissions don't race the rebuild.

        Freshly-built nodes default to ``maintenance=True`` (set by
        :meth:`Registry.reload`), so the registry edit acts as a pause:
        choco won't push anything until the operator takes nodes back
        out of maintenance.  After the rebuild we run state discovery
        so each new :class:`Node`'s ``started`` flag matches the actual
        kotekan runtime state rather than the cold default.
        """
        with self._submit_lock:
            if new_data is not None:
                self.registry.save_nodes_yaml(new_data)
            self._stop_workers()
            self.registry.reload()
        # Discovery runs before the new workers spawn so their first
        # cycle acts on observed runtime state, not the cold ``started``
        # defaults from nodes.yaml — the same order run() uses.  It sits
        # outside the lock (it can take a full request timeout);
        # submissions landing in the gap stay queued on the new Node
        # objects and are drained by the fresh workers' first check.
        self.discover_node_states()
        with self._submit_lock:
            self._respawn_workers()
