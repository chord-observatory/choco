"""Node registry and runtime state tracking."""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import yaml

from .kotekan import KotekanClient

logger = logging.getLogger(__name__)

# Files in configs_dir that are not kotekan configs
_META_FILES = {"nodes.yaml", "deploy.yaml"}


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
    config_hash: str | None = None
    error: str | None = None

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


class DeployStore:
    """Manages deploy.yaml: default branch and per-node branch/config overrides.

    File format:
        default_branch: main
        nodes:
          cx/cx27:
            branch: develop
            config: some-config-name
    """

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self._path = self.configs_dir / "deploy.yaml"
        self.default_branch: str = "main"
        self._nodes: dict[str, dict] = {}
        self.reload()

    def reload(self):
        if not self._path.exists():
            self.default_branch = "main"
            self._nodes = {}
            return
        with open(self._path) as f:
            data = yaml.safe_load(f) or {}
        self.default_branch = data.get("default_branch", "main")
        self._nodes = data.get("nodes") or {}

    def _save(self):
        data = {"default_branch": self.default_branch, "nodes": self._nodes}
        with open(self._path, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

    def get_branch(self, node_key: str) -> str:
        """Get the branch for a node (falls back to default_branch)."""
        entry = self._nodes.get(node_key) or {}
        return entry.get("branch") or self.default_branch

    def get_config_name(self, node_key: str) -> str:
        """Get the config name for a node (falls back to node key)."""
        entry = self._nodes.get(node_key) or {}
        return entry.get("config") or node_key

    def set_node(self, node_key: str, branch: str | None = None,
                 config: str | None = None):
        """Update deploy settings for a node and save to disk."""
        entry = self._nodes.setdefault(node_key, {})
        if branch is not None:
            if branch == self.default_branch:
                entry.pop("branch", None)
            else:
                entry["branch"] = branch
        if config is not None:
            if config == node_key:
                entry.pop("config", None)
            else:
                entry["config"] = config
        # Remove empty entries
        if not entry:
            self._nodes.pop(node_key, None)
        self._save()


class ConfigStore:
    """Manages desired configs stored as YAML files on disk.

    Directory structure:
        configs_dir/
            nodes.yaml
            deploy.yaml
            <group>/
                <node_id>.yaml
    """

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self._desired_configs: dict[str, dict] = {}
        self._desired_hashes: dict[str, str] = {}
        self.reload()

    def reload(self):
        """Reload all desired configs from disk."""
        self._desired_configs.clear()
        self._desired_hashes.clear()

        for yaml_file in self.configs_dir.rglob("*.yaml"):
            if yaml_file.name in _META_FILES:
                continue
            rel = yaml_file.relative_to(self.configs_dir)
            key = str(rel.with_suffix(""))
            try:
                with open(yaml_file) as f:
                    config = yaml.safe_load(f) or {}
                self._desired_configs[key] = config
                self._desired_hashes[key] = self._hash_config(config)
                logger.debug(f"Loaded desired config for {key}")
            except Exception as e:
                logger.error(f"Failed to load config {yaml_file}: {e}")

    @property
    def config_names(self) -> list[str]:
        """All available config names."""
        return list(self._desired_configs.keys())

    def get_desired_config(self, config_name: str) -> dict | None:
        return self._desired_configs.get(config_name)

    def get_desired_hash(self, config_name: str) -> str | None:
        return self._desired_hashes.get(config_name)

    def save_config(self, config_name: str, config: dict):
        path = self.configs_dir / f"{config_name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(config, f, default_flow_style=False)
        self._desired_configs[config_name] = config
        self._desired_hashes[config_name] = self._hash_config(config)

    @staticmethod
    def _hash_config(config: dict) -> str:
        serialized = yaml.dump(config, default_flow_style=False, sort_keys=True)
        return hashlib.md5(serialized.encode()).hexdigest()


class Registry:
    """Node registry: loads node definitions and tracks their state."""

    def __init__(self, configs_dir: Path):
        self.configs_dir = Path(configs_dir)
        self.nodes: dict[str, Node] = {}
        self.config_store = ConfigStore(configs_dir)
        self.deploy_store = DeployStore(configs_dir)
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

        for group_name, members in groups.items():
            for node_name, node_info in members.items():
                key = f"{group_name}/{node_name}"
                host = node_info.get("host", node_name)
                port = node_info.get("port", 12048)
                self.nodes[key] = Node(
                    name=node_name, group=group_name, host=host, port=port
                )

        logger.info(f"Loaded {len(self.nodes)} nodes")

    def reload(self):
        """Reload node definitions, configs, and deploy settings."""
        self._load_nodes()
        self.config_store.reload()
        self.deploy_store.reload()

    def get_node(self, key: str) -> Node | None:
        return self.nodes.get(key)
