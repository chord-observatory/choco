"""Node registry and runtime state tracking."""

import copy
import json
import logging
import time
from collections import deque
from enum import Enum
from pathlib import Path

import jinja2
import requests
import yaml

logger = logging.getLogger(__name__)

# Config file extensions (order matters: later wins if both exist for same key)
_CONFIG_SUFFIXES = (".yaml", ".yml", ".j2")

_UPDATABLE_MARKER = "kotekan_update_endpoint"


def strip_updatable_values(config: dict) -> dict:
    """Return a deep copy of *config* with updatable config values removed.

    Any sub-dict (at any depth) that contains the key
    ``kotekan_update_endpoint`` is replaced with just that marker key,
    dropping the mutable value keys that kotekan may change at runtime.
    This lets two configs that differ only in updatable values compare as
    equal.
    """
    out = {}
    if not config:
        return out
    for key, value in config.items():
        if isinstance(value, dict):
            if _UPDATABLE_MARKER in value:
                out[key] = {_UPDATABLE_MARKER: value[_UPDATABLE_MARKER]}
            else:
                out[key] = strip_updatable_values(value)
        else:
            out[key] = value
    return out


def find_updatable_blocks(config: dict, _prefix: str = "") -> dict[str, dict]:
    """Find all updatable config blocks and return their endpoint paths + values.

    Walks *config* recursively.  Any sub-dict containing the
    ``kotekan_update_endpoint`` key is collected; its path (joined with ``/``)
    becomes the key and the values (without the marker) become the value.

    For example, this might return something like::

        {"updatable_config/flagging": {"start_time": …, …},
         "updatable_config/gains":    {"start_time": …, …},
         "updatable_config/26m_gated": {"enabled": False}}
    """
    blocks: dict[str, dict] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            path = f"{_prefix}/{key}" if _prefix else key
            if _UPDATABLE_MARKER in value:
                blocks[path] = {
                    k: v for k, v in value.items() if k != _UPDATABLE_MARKER
                }
            else:
                blocks.update(find_updatable_blocks(value, path))
    return blocks


class NodeStatus(Enum):
    UNKNOWN = "unknown"
    DOWN = "down"       # Unreachable
    IDLE = "idle"       # Reachable but kotekan not running (ready for /start)
    STARTED = "started" # Running with correct config
    SYNCING = "syncing" # Push in progress (kill -> wait -> start with new config)


class Node:
    """A kotekan instance on the cluster.

    Each node owns its identity (name, group, host, port), its config
    state (base config file on disk, rendered config, updatable overrides),
    a FIFO change queue (used by the sync worker pool), and an HTTP
    client for the kotekan REST API.

    Config lifecycle:
        - **base_content** — the on-disk file text (YAML or Jinja2)
        - **rendered_config** — base rendered through Jinja2 and parsed
        - **updatable_config** — runtime-mutable overrides stored in JSON
        - **desired_config** — rendered + updatable merged; what gets pushed

    REST methods return ``None`` / ``False`` on connection failure rather
    than raising, so callers can treat unreachable nodes as a normal state.

    The *configs_dir* and *template_vars* parameters are optional so that
    the REST client can be used standalone in tests without a config
    directory.
    """

    def __init__(self, name: str, group: str, host: str,
                 port: int = 12048, timeout: int = 10, *,
                 started: bool = False,
                 configs_dir: Path | None = None,
                 template_vars: dict | None = None):
        # Identity
        self.name = name
        self.group = group
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started = started
        self._base_url = f"http://{host}:{port}"

        # Config state (loaded from disk by load_config / load_updatable)
        self.configs_dir = configs_dir
        self.template_vars: dict = template_vars or {}
        self.base_content: str | None = None
        self.rendered_config: dict | None = None
        self._file_suffix: str = ".yaml"
        self.updatable_config: dict[str, dict] | None = None

        # Runtime state (ephemeral, rebuilt from polling)
        self.status: NodeStatus = NodeStatus.UNKNOWN
        self.last_seen: float | None = None
        self.error: str | None = None
        self.version: str | None = None

        # Change queue (used by the sync worker pool)
        self._queue: deque = deque()
        self._queue_lock: object | None = None  # set by Orchestrator (gevent semaphore)

    @property
    def key(self) -> str:
        return f"{self.group}/{self.name}"

    @property
    def last_seen_ago(self) -> str | None:
        """Human-readable time since last seen."""
        if self.last_seen is None:
            return None
        delta = time.time() - self.last_seen
        if delta < 60:
            return f"{int(delta)}s ago"
        if delta < 3600:
            return f"{int(delta / 60)}m ago"
        return f"{int(delta / 3600)}h ago"

    def __repr__(self) -> str:
        return f"Node({self.key}, {self.host}:{self.port}, {self.status.value})"

    # --- Change queue ---

    def queue_put(self, item):
        """Append a ChangeItem to this node's queue."""
        self._queue.append(item)

    def queue_pop(self):
        """Pop the next ChangeItem, or None if empty."""
        try:
            return self._queue.popleft()
        except IndexError:
            return None

    def queue_try_lock(self) -> bool:
        """Try to acquire exclusive access to this node's queue."""
        if self._queue_lock is None:
            return False
        return self._queue_lock.acquire(blocking=False)

    def queue_unlock(self):
        """Release exclusive access to this node's queue."""
        if self._queue_lock is not None:
            self._queue_lock.release()

    @property
    def queue_empty(self) -> bool:
        return len(self._queue) == 0

    # --- Config state ---

    @property
    def config_filename(self) -> str:
        """Relative path of this node's base config file."""
        return f"{self.group}/{self.name}{self._file_suffix}"

    @property
    def desired_config(self) -> dict | None:
        """Rendered config with updatable overrides applied.

        Computed from the current ``_rendered_config`` and ``_updatable``
        on every access — no separate cache.  Returns a fresh deep copy
        safe to mutate, or None if no base config exists.
        """
        if self.rendered_config is None:
            return None
        desired = copy.deepcopy(self.rendered_config)
        if self.updatable_config:
            blocks = find_updatable_blocks(desired)
            for endpoint, values in self.updatable_config.items():
                if endpoint in blocks:
                    target = desired
                    for part in endpoint.split("/"):
                        target = target[part]
                    target.update(values)
        return desired

    def load_config(self):
        """Load (or reload) the base config from disk and render it."""
        if self.configs_dir is None:
            return
        for suffix in _CONFIG_SUFFIXES:
            path = self.configs_dir / self.group / f"{self.name}{suffix}"
            if path.exists():
                self._file_suffix = suffix
                self.base_content = path.read_text()
                self.rendered_config = self.render(self.base_content)
                return
        self.base_content = None
        self.rendered_config = None

    def load_updatable(self):
        """Load updatable overrides from the JSON store on disk."""
        if self.configs_dir is None:
            self.updatable_config = None
            return
        path = self.configs_dir / ".updatable" / self.group / f"{self.name}.json"
        if not path.exists():
            self.updatable_config = None
            return
        with open(path) as f:
            self.updatable_config = json.load(f)

    def save_base(self, base_content: str):
        """Validate, write base config to disk, and update caches."""
        rendered = self.render(base_content)
        path = self.configs_dir / self.group / f"{self.name}{self._file_suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(base_content)
        self.base_content = base_content
        self.rendered_config = rendered

    def save_updatable(self, endpoint: str, values: dict):
        """Save updatable values for one endpoint to memory and disk."""
        if self.updatable_config is None:
            self.updatable_config = {}
        self.updatable_config[endpoint] = values
        if self.configs_dir is not None:
            path = self.configs_dir / ".updatable" / self.group / f"{self.name}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(self.updatable_config, f, indent=2)

    def render(self, base_content: str) -> dict:
        """Render base config text through Jinja2 and parse as YAML.

        Also serves as validation — raises on invalid content.
        """
        rendered = jinja2.Template(base_content).render(self.template_vars)
        config = yaml.safe_load(rendered)
        if not isinstance(config, dict):
            raise ValueError("Config must render to a YAML mapping")
        return config

    # --- Kotekan REST API ---

    def _request(self, method: str, path: str, **kwargs) -> requests.Response | None:
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            resp = requests.request(method, url, timeout=self.timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, ConnectionError):
            logger.debug(f"Connection failed: {url}")
        except requests.Timeout:
            logger.debug(f"Timeout: {url}")
        except requests.HTTPError as e:
            logger.warning(f"HTTP error from {url}: {e}")
        return None

    def get_status(self) -> NodeStatus:
        """Probe kotekan: returns DOWN, IDLE, STARTED, or UNKNOWN."""
        resp = self._request("GET", "/status")
        if resp is None:
            return NodeStatus.DOWN
        try:
            data = resp.json()
            return NodeStatus.STARTED if data.get("running", False) else NodeStatus.IDLE
        except Exception:
            return NodeStatus.UNKNOWN

    def get_config(self) -> dict | None:
        """Get the live config from kotekan.  Returns None if unreachable."""
        resp = self._request("GET", "/config")
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
            logger.warning(f"Failed to parse config JSON from {self._base_url}")
            return None

    def push_updatable(self, path: str, values: dict) -> bool:
        """Push values to an updatable config endpoint on kotekan."""
        return self._request("POST", path, json=values) is not None

    def start(self, desired_config: dict) -> bool:
        """Start kotekan with the desired config via POST /start."""
        return self._request("POST", "/start", json=desired_config) is not None

    def kill(self) -> bool:
        """Kill the kotekan process. The daemon restarts it into an idle state.

        This is the reliable way to stop a running config — the ``/stop``
        endpoint is unreliable, so we always use ``/kill`` instead.
        """
        return self._request("GET", "/kill") is not None

    def get_version(self) -> str | None:
        """Get the kotekan version string."""
        resp = self._request("GET", "/version")
        if resp is None:
            return None
        try:
            return resp.json().get("kotekan_version")
        except Exception:
            return None


class Registry:
    """Node registry: loads node definitions from nodes.yaml and provides lookup.

    Each :class:`Node` owns its own config state (base config file,
    rendered config, updatable overrides).  The registry creates them
    and loads shared Jinja2 template variables from ``vars.yaml``.
    """

    def __init__(self, configs_dir: Path, kotekan_timeout: int = 10):
        self.configs_dir = Path(configs_dir)
        self.kotekan_timeout = kotekan_timeout
        self.nodes: dict[str, Node] = {}
        self.reload()

    def _load_vars(self) -> dict:
        vars_file = self.configs_dir / "vars.yaml"
        if vars_file.exists():
            with open(vars_file) as f:
                return yaml.safe_load(f) or {}
        return {}

    def reload(self):
        """Rebuild ``self.nodes`` from ``nodes.yaml`` on disk.

        Clears and repopulates the registry; all existing :class:`Node`
        objects are discarded along with any pending queue items or
        runtime state.  Callers that need to synchronise with the sync
        worker pool should hold the orchestrator's input-queue lock
        around this call.
        """
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            logger.warning(f"No nodes.yaml found at {nodes_file}")
            self.nodes.clear()
            return

        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}

        template_vars = self._load_vars()

        self.nodes.clear()
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in (members or {}).items():
                key = f"{group_name}/{node_name}"
                host = node_info.get("host", node_name)
                port = node_info.get("port", 12048)
                started = node_info.get("started", False)
                node = Node(
                    node_name, group_name, host, port,
                    timeout=self.kotekan_timeout,
                    started=started,
                    configs_dir=self.configs_dir,
                    template_vars=template_vars,
                )
                node.load_config()
                node.load_updatable()
                self.nodes[key] = node

        logger.info(f"Loaded {len(self.nodes)} nodes")

    def save_nodes_yaml(self, data: dict):
        """Write *data* to ``nodes.yaml`` atomically (temp file + rename)."""
        nodes_file = self.configs_dir / "nodes.yaml"
        nodes_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = nodes_file.with_name(nodes_file.name + ".tmp")
        with open(tmp, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
        tmp.replace(nodes_file)

    def get_node(self, key: str) -> Node | None:
        return self.nodes.get(key)
