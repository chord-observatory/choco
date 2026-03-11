"""Tests for node registry and state tracking."""

import pytest
from pathlib import Path

import yaml

from choco.state import Registry, ConfigStore, DeployStore, NodeStatus


@pytest.fixture
def configs_dir(tmp_path):
    """Create a temporary configs directory with test data."""
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

    return tmp_path


class TestRegistry:
    def test_loads_nodes(self, configs_dir):
        registry = Registry(configs_dir)
        assert len(registry.nodes) == 3
        assert "cx/cx1" in registry.nodes
        assert "cx/cx2" in registry.nodes
        assert "recv/recv1" in registry.nodes

    def test_node_properties(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node is not None
        assert node.name == "cx1"
        assert node.group == "cx"
        assert node.host == "cx1.chord.ca"
        assert node.port == 12048
        assert node.key == "cx/cx1"

    def test_initial_status(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.state.status == NodeStatus.UNKNOWN

    def test_missing_node(self, configs_dir):
        registry = Registry(configs_dir)
        assert registry.get_node("nonexistent/node") is None


class TestConfigStore:
    def test_loads_configs(self, configs_dir):
        store = ConfigStore(configs_dir)
        config = store.get_desired_config("cx/cx1")
        assert config is not None
        assert config["num_elements"] == 2048

    def test_no_config(self, configs_dir):
        store = ConfigStore(configs_dir)
        assert store.get_desired_config("cx/cx2") is None

    def test_hash(self, configs_dir):
        store = ConfigStore(configs_dir)
        h = store.get_desired_hash("cx/cx1")
        assert h is not None
        assert len(h) == 32  # md5 hex

    def test_save_config(self, configs_dir):
        store = ConfigStore(configs_dir)
        new_config = {"log_level": "debug", "num_elements": 1024}
        store.save_config("cx/cx2", new_config)
        assert store.get_desired_config("cx/cx2") == new_config
        path = configs_dir / "cx" / "cx2.yaml"
        assert path.exists()
        with open(path) as f:
            on_disk = yaml.safe_load(f)
        assert on_disk == new_config

    def test_save_creates_directory(self, configs_dir):
        store = ConfigStore(configs_dir)
        store.save_config("newgroup/node1", {"key": "val"})
        assert (configs_dir / "newgroup" / "node1.yaml").exists()

    def test_reload(self, configs_dir):
        store = ConfigStore(configs_dir)
        recv_dir = configs_dir / "recv"
        recv_dir.mkdir(exist_ok=True)
        with open(recv_dir / "recv1.yaml", "w") as f:
            yaml.dump({"buffer_depth": 12}, f)
        store.reload()
        assert store.get_desired_config("recv/recv1") is not None

    def test_skips_meta_files(self, configs_dir):
        """deploy.yaml and nodes.yaml should not be loaded as configs."""
        with open(configs_dir / "deploy.yaml", "w") as f:
            yaml.dump({"default_branch": "main"}, f)
        store = ConfigStore(configs_dir)
        assert store.get_desired_config("deploy") is None
        assert store.get_desired_config("nodes") is None

    def test_config_names(self, configs_dir):
        store = ConfigStore(configs_dir)
        assert "cx/cx1" in store.config_names


class TestDeployStore:
    def test_defaults(self, tmp_path):
        store = DeployStore(tmp_path)
        assert store.default_branch == "main"
        assert store.get_branch("cx/cx1") == "main"
        assert store.get_config_name("cx/cx1") == "cx/cx1"

    def test_loads_from_file(self, tmp_path):
        data = {
            "default_branch": "develop",
            "nodes": {
                "cx/cx1": {"branch": "feature-x", "config": "shared-config"},
            },
        }
        with open(tmp_path / "deploy.yaml", "w") as f:
            yaml.dump(data, f)
        store = DeployStore(tmp_path)
        assert store.default_branch == "develop"
        assert store.get_branch("cx/cx1") == "feature-x"
        assert store.get_config_name("cx/cx1") == "shared-config"
        # Node without override falls back to defaults
        assert store.get_branch("cx/cx2") == "develop"
        assert store.get_config_name("cx/cx2") == "cx/cx2"

    def test_set_node(self, tmp_path):
        store = DeployStore(tmp_path)
        store.set_node("cx/cx1", branch="feature-y")
        assert store.get_branch("cx/cx1") == "feature-y"
        # Verify persisted to disk
        with open(tmp_path / "deploy.yaml") as f:
            data = yaml.safe_load(f)
        assert data["nodes"]["cx/cx1"]["branch"] == "feature-y"

    def test_set_default_branch_clears_override(self, tmp_path):
        store = DeployStore(tmp_path)
        store.set_node("cx/cx1", branch="develop")
        assert store.get_branch("cx/cx1") == "develop"
        # Setting to the default branch should remove the override
        store.set_node("cx/cx1", branch="main")
        assert store.get_branch("cx/cx1") == "main"
        with open(tmp_path / "deploy.yaml") as f:
            data = yaml.safe_load(f)
        assert "cx/cx1" not in (data.get("nodes") or {})

    def test_reload(self, tmp_path):
        store = DeployStore(tmp_path)
        with open(tmp_path / "deploy.yaml", "w") as f:
            yaml.dump({"default_branch": "v2", "nodes": {}}, f)
        store.reload()
        assert store.default_branch == "v2"
