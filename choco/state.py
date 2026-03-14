"""Node registry and runtime state tracking."""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import jinja2
import yaml

from .kotekan import KotekanClient

logger = logging.getLogger(__name__)

# Files in configs_dir that are not kotekan configs
_META_FILES = {"nodes.yaml", "vars.yaml"}

# Config file extensions (order matters: later wins if both exist for same key)
_CONFIG_SUFFIXES = (".yaml", ".yml", ".j2")


class NodeStatus(Enum):
    UNKNOWN = "unknown"
    UP = "up"
    DOWN = "down"
    DRIFT = "drift"  # Running but config doesn't match desired


@dataclass
class NodeState:
    """Runtime state for a single node (ephemeral, rebuilt from polling)."""

    status: NodeStatus = NodeStatus.UNKNOWN
    last_seen: float | None = None
    error: str | None = None
    version: str | None = None

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


@dataclass
class Node:
    """A kotekan node managed by choco."""

    name: str
    group: str
    host: str
    port: int
    client: KotekanClient = field(init=False, repr=False)
    state: NodeState = field(default_factory=NodeState)

    def __post_init__(self):
        self.client = KotekanClient(self.host, self.port)

    @property
    def key(self) -> str:
        return f"{self.group}/{self.name}"


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


class Registry:
    """Node registry: loads node definitions and tracks their state."""

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self.nodes: dict[str, Node] = {}
        self.config_store = ConfigStore(configs_dir)
        self._config_overrides: dict[str, str] = {}
        self._load_nodes()

    def _load_nodes(self):
        """Load node definitions from nodes.yaml."""
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            logger.warning(f"No nodes.yaml found at {nodes_file}")
            return

        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}

        groups = data.get("groups", {})
        self.nodes.clear()
        self._config_overrides.clear()

        for group_name, members in groups.items():
            for node_name, node_info in members.items():
                key = f"{group_name}/{node_name}"
                host = node_info.get("host", node_name)
                port = node_info.get("port", 12048)
                self.nodes[key] = Node(
                    name=node_name, group=group_name, host=host, port=port
                )
                config = node_info.get("config")
                if config:
                    self._config_overrides[key] = config

        logger.info(f"Loaded {len(self.nodes)} nodes")

    def _reload_config_overrides(self):
        """Re-read config overrides from nodes.yaml without rebuilding nodes."""
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            self._config_overrides.clear()
            return
        with open(nodes_file) as f:
            data = yaml.safe_load(f) or {}
        self._config_overrides.clear()
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in members.items():
                config = node_info.get("config")
                if config:
                    self._config_overrides[f"{group_name}/{node_name}"] = config

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
