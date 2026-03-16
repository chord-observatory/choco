"""Node registry and runtime state tracking."""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import jinja2
import requests
import yaml

logger = logging.getLogger(__name__)

# Files in configs_dir that are not kotekan configs
_META_FILES = {"nodes.yaml", "vars.yaml"}

# Config file extensions (order matters: later wins if both exist for same key)
_CONFIG_SUFFIXES = (".yaml", ".yml", ".j2")

DEFAULT_TIMEOUT = 5  # seconds


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

    # Empty config / nothing to strip.
    if not config :
        return out

    for key, value in config.items():
        if isinstance(value, dict):
            if _UPDATABLE_MARKER in value:
                # Keep only the marker so the block still "exists" in both.
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

    For cx27.yaml this returns something like::

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
    UP = "up"           # Running with correct config
    SYNCING = "syncing" # Push in progress (kill -> wait -> start with new config)


@dataclass
class Node:
    """A kotekan node managed by choco.

    Combines node identity, runtime state, and the HTTP client for
    communicating with the kotekan REST API on this node.
    """

    name: str
    group: str
    host: str
    port: int = 12048
    timeout: float = DEFAULT_TIMEOUT

    # Runtime state (ephemeral, rebuilt from polling)
    status: NodeStatus = NodeStatus.UNKNOWN
    last_seen: float | None = None
    error: str | None = None
    version: str | None = None

    _base_url: str = field(init=False, repr=False)

    def __post_init__(self):
        self._base_url = f"http://{self.host}:{self.port}"

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
        """Probe kotekan: returns DOWN, IDLE, UP, or UNKNOWN."""
        resp = self._request("GET", "/status")
        if resp is None:
            return NodeStatus.DOWN
        try:
            data = resp.json()
            return NodeStatus.UP if data.get("running", False) else NodeStatus.IDLE
        except Exception:
            return NodeStatus.UNKNOWN

    def get_config(self) -> dict | None:
        """Get the full current config. Returns None if unreachable."""
        resp = self._request("GET", "/config")
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
            logger.warning(f"Failed to parse config JSON from {self._base_url}")
            return None

    def update_config(self, path: str, values: dict) -> bool:
        """Push a config update to an updatable config block."""
        return self._request("POST", path, json=values) is not None

    def start(self, config: dict) -> bool:
        """Start kotekan with the given config via POST /start."""
        return self._request("POST", "/start", json=config) is not None

    def stop(self) -> bool:
        """Stop the running kotekan config."""
        return self._request("GET", "/stop") is not None

    def kill(self) -> bool:
        """Kill the kotekan process. The daemon will restart it."""
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

    def __repr__(self) -> str:
        return f"Node({self.key}, {self.host}:{self.port}, {self.status.value})"


class ConfigStore:
    """Manages desired configs stored as YAML/Jinja2 files on disk.

    All config files are rendered through Jinja2 (using vars.yaml as context)
    and parsed as YAML before being pushed to kotekan as JSON.

    Directory structure:
        configs_dir/
            nodes.yaml
            vars.yaml           (optional shared template variables)
            <group>/
                <node_id>.yaml  (or .j2)
    """

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self._desired_configs: dict[str, dict] = {}
        self._raw_contents: dict[str, str] = {}
        self._file_suffixes: dict[str, str] = {}
        self._vars: dict = {}
        self.reload()

    def _load_vars(self):
        vars_file = self.configs_dir / "vars.yaml"
        if vars_file.exists():
            with open(vars_file) as f:
                self._vars = yaml.safe_load(f) or {}
        else:
            self._vars = {}

    def reload(self):
        """Reload all desired configs from disk."""
        self._desired_configs.clear()
        self._raw_contents.clear()
        self._file_suffixes.clear()
        self._load_vars()

        for config_file in self.configs_dir.rglob("*"):
            if not config_file.is_file():
                continue
            if config_file.name in _META_FILES:
                continue
            suffix = config_file.suffix
            if suffix not in _CONFIG_SUFFIXES:
                continue
            rel = config_file.relative_to(self.configs_dir)
            key = str(rel.with_suffix(""))
            try:
                raw = config_file.read_text()
                self._raw_contents[key] = raw
                self._file_suffixes[key] = suffix
                config = self._render(raw)
                self._desired_configs[key] = config
                logger.debug(f"Loaded desired config for {key}")
            except Exception as e:
                logger.error(f"Failed to load config {config_file}: {e}")

    def _render(self, raw: str) -> dict:
        """Render Jinja2 template with shared vars and parse as YAML."""
        rendered = jinja2.Template(raw).render(self._vars)
        config = yaml.safe_load(rendered)
        if not isinstance(config, dict):
            raise ValueError("Config must render to a YAML mapping")
        return config

    @property
    def desired_configs(self) -> dict[str, dict]:
        """All desired configs (config_name -> parsed config dict)."""
        return dict(self._desired_configs)

    @property
    def config_names(self) -> list[str]:
        """All available config names."""
        return list(self._desired_configs.keys())

    def get_desired_config(self, config_name: str) -> dict | None:
        return self._desired_configs.get(config_name)

    def get_raw_content(self, config_name: str) -> str | None:
        """Get the raw file content (for editing in the web UI)."""
        return self._raw_contents.get(config_name)

    def get_file_suffix(self, config_name: str) -> str:
        """Get the file extension for a config (e.g. '.yaml' or '.j2')."""
        return self._file_suffixes.get(config_name, ".yaml")

    def save_raw(self, config_name: str, content: str):
        """Save raw content to disk, render, and update caches."""
        suffix = self._file_suffixes.get(config_name, ".yaml")
        path = self.configs_dir / f"{config_name}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        config = self._render(content)
        path.write_text(content)
        self._raw_contents[config_name] = content
        self._desired_configs[config_name] = config

    def save_config(self, config_name: str, config: dict):
        """Save a config dict as YAML."""
        path = self.configs_dir / f"{config_name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = yaml.dump(config, default_flow_style=False)
        path.write_text(raw)
        self._raw_contents[config_name] = raw
        self._file_suffixes[config_name] = ".yaml"
        self._desired_configs[config_name] = config


class UpdatableStore:
    """Persists per-node updatable config values as JSON files.

    Storage layout::

        configs_dir/.updatable/<group>/<node>.json

    Each file maps endpoint paths to their current values, e.g.::

        {"updatable_config/gains": {"start_time": …, …}}

    Files are only created when a user explicitly sets values via the web UI.
    """

    def __init__(self, configs_dir: Path):
        self._dir = Path(configs_dir) / ".updatable"

    def _path(self, node_key: str) -> Path:
        return self._dir / f"{node_key}.json"

    def get(self, node_key: str) -> dict[str, dict] | None:
        """Load stored updatable values for a node.  Returns None if no file."""
        path = self._path(node_key)
        if not path.exists():
            return None
        with open(path) as f:
            return json.load(f)

    def save(self, node_key: str, endpoint: str, values: dict):
        """Merge-save one endpoint's values into the node's store file."""
        existing = self.get(node_key) or {}
        existing[endpoint] = values
        self._write(node_key, existing)

    def save_all(self, node_key: str, blocks: dict[str, dict]):
        """Overwrite all stored blocks for a node."""
        self._write(node_key, blocks)

    def _write(self, node_key: str, data: dict):
        path = self._path(node_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)


class Registry:
    """Node registry: loads node definitions and tracks their state."""

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self.nodes: dict[str, Node] = {}
        self.config_store = ConfigStore(configs_dir)
        self.updatable_store = UpdatableStore(configs_dir)
        self._config_overrides: dict[str, str] = {}
        self._load_nodes()

    @staticmethod
    def _parse_config_overrides(data: dict) -> dict[str, str]:
        """Extract config overrides from parsed nodes.yaml data."""
        overrides = {}
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in members.items():
                config = node_info.get("config")
                if config:
                    overrides[f"{group_name}/{node_name}"] = config
        return overrides

    def _load_nodes(self):
        """Load node definitions from nodes.yaml."""
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            logger.warning(f"No nodes.yaml found at {nodes_file}")
            return

        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}

        self.nodes.clear()
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in members.items():
                key = f"{group_name}/{node_name}"
                host = node_info.get("host", node_name)
                port = node_info.get("port", 12048)
                self.nodes[key] = Node(
                    name=node_name, group=group_name, host=host, port=port
                )

        self._config_overrides = self._parse_config_overrides(data)
        logger.info(f"Loaded {len(self.nodes)} nodes")

    def _reload_config_overrides(self):
        """Re-read config overrides from nodes.yaml without rebuilding nodes."""
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            self._config_overrides.clear()
            return
        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}
        self._config_overrides = self._parse_config_overrides(data)

    def get_config_name(self, key: str) -> str:
        """Get the config name for a node (falls back to node key)."""
        return self._config_overrides.get(key, key)

    def set_config_name(self, key: str, config_name: str):
        """Update the config override for a node and save to nodes.yaml."""
        if config_name == key:
            self._config_overrides.pop(key, None)
        else:
            self._config_overrides[key] = config_name
        self._save_nodes()

    def _save_nodes(self):
        """Write current node definitions back to nodes.yaml."""
        nodes_file = self.configs_dir / "nodes.yaml"
        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in members.items():
                key = f"{group_name}/{node_name}"
                if key in self._config_overrides:
                    node_info["config"] = self._config_overrides[key]
                else:
                    node_info.pop("config", None)
        with open(nodes_file, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

    def reload(self):
        """Reload node definitions and configs."""
        self._load_nodes()
        self.config_store.reload()

    def get_config_filename(self, key: str) -> str:
        """Get the config filename for a node (e.g. 'cx/cx27.yaml')."""
        name = self.get_config_name(key)
        suffix = self.config_store.get_file_suffix(name)
        return f"{name}{suffix}"

    def get_node(self, key: str) -> Node | None:
        return self.nodes.get(key)
