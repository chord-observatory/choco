"""Tests for node registry and state tracking."""

import pytest
from pathlib import Path

import yaml

from choco.state import (
    Registry, ConfigStore, NodeStatus, UpdatableStore,
    strip_updatable_values, find_updatable_blocks,
)


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
        assert node.status == NodeStatus.UNKNOWN

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
        """nodes.yaml and vars.yaml should not be loaded as configs."""
        with open(configs_dir / "vars.yaml", "w") as f:
            yaml.dump({"some_var": "val"}, f)
        store = ConfigStore(configs_dir)
        assert store.get_desired_config("nodes") is None
        assert store.get_desired_config("vars") is None

    def test_config_names(self, configs_dir):
        store = ConfigStore(configs_dir)
        assert "cx/cx1" in store.config_names

    def test_loads_j2_config(self, configs_dir):
        cx_dir = configs_dir / "cx"
        (cx_dir / "cx2.j2").write_text("num_elements: 1024\nlog_level: debug\n")
        store = ConfigStore(configs_dir)
        config = store.get_desired_config("cx/cx2")
        assert config == {"num_elements": 1024, "log_level": "debug"}
        assert store.get_file_suffix("cx/cx2") == ".j2"

    def test_j2_renders_with_vars(self, configs_dir):
        with open(configs_dir / "vars.yaml", "w") as f:
            yaml.dump({"n_elem": 2048}, f)
        cx_dir = configs_dir / "cx"
        (cx_dir / "cx2.j2").write_text("num_elements: {{ n_elem }}\n")
        store = ConfigStore(configs_dir)
        config = store.get_desired_config("cx/cx2")
        assert config["num_elements"] == 2048

    def test_yaml_renders_with_vars(self, configs_dir):
        with open(configs_dir / "vars.yaml", "w") as f:
            yaml.dump({"level": "debug"}, f)
        cx_dir = configs_dir / "cx"
        (cx_dir / "cx1.yaml").write_text("log_level: {{ level }}\n")
        store = ConfigStore(configs_dir)
        config = store.get_desired_config("cx/cx1")
        assert config["log_level"] == "debug"

    def test_raw_content(self, configs_dir):
        store = ConfigStore(configs_dir)
        raw = store.get_raw_content("cx/cx1")
        assert raw is not None
        assert "num_elements" in raw

    def test_save_raw(self, configs_dir):
        store = ConfigStore(configs_dir)
        store.save_raw("cx/cx1", "num_elements: 512\nlog_level: warn\n")
        assert store.get_desired_config("cx/cx1") == {"num_elements": 512, "log_level": "warn"}
        assert store.get_raw_content("cx/cx1") == "num_elements: 512\nlog_level: warn\n"

    def test_save_raw_j2(self, configs_dir):
        cx_dir = configs_dir / "cx"
        (cx_dir / "cx2.j2").write_text("num_elements: 1024\n")
        store = ConfigStore(configs_dir)
        store.save_raw("cx/cx2", "num_elements: 2048\n")
        # Should preserve the .j2 extension
        assert (cx_dir / "cx2.j2").read_text() == "num_elements: 2048\n"
        assert not (cx_dir / "cx2.yaml").exists()

    def test_save_raw_invalid_raises(self, configs_dir):
        store = ConfigStore(configs_dir)
        with pytest.raises(ValueError):
            store.save_raw("cx/cx1", "not_a_mapping")


class TestStripUpdatableValues:
    def test_no_updatable_blocks(self):
        config = {"log_level": "info", "num_elements": 2048}
        assert strip_updatable_values(config) == config

    def test_strips_updatable_values(self):
        config = {
            "log_level": "info",
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1500000000,
                    "update_id": "gains1500000000",
                    "transition_interval": 10.0,
                },
            },
        }
        result = strip_updatable_values(config)
        assert result["log_level"] == "info"
        assert result["updatable_config"]["gains"] == {
            "kotekan_update_endpoint": "json"
        }

    def test_differing_updatable_values_compare_equal(self):
        desired = {
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1500000000,
                    "update_id": "old",
                },
            },
            "other": "value",
        }
        actual = {
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 9999999999,
                    "update_id": "new",
                },
            },
            "other": "value",
        }
        assert strip_updatable_values(desired) == strip_updatable_values(actual)

    def test_non_updatable_diff_still_detected(self):
        a = {
            "log_level": "info",
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1,
                },
            },
        }
        b = {
            "log_level": "debug",
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1,
                },
            },
        }
        assert strip_updatable_values(a) != strip_updatable_values(b)

    def test_deeply_nested_updatable(self):
        config = {
            "pipeline": {
                "stage1": {
                    "tuning": {
                        "kotekan_update_endpoint": "json",
                        "param": 42,
                    }
                }
            }
        }
        result = strip_updatable_values(config)
        assert result["pipeline"]["stage1"]["tuning"] == {
            "kotekan_update_endpoint": "json"
        }

    def test_does_not_mutate_original(self):
        config = {
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1,
                },
            },
        }
        strip_updatable_values(config)
        assert "start_time" in config["updatable_config"]["gains"]


class TestFindUpdatableBlocks:
    def test_no_updatable_blocks(self):
        config = {"log_level": "info", "num_elements": 2048}
        assert find_updatable_blocks(config) == {}

    def test_single_block(self):
        config = {
            "updatable_config": {
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 1500000000,
                    "update_id": "g1",
                },
            },
        }
        result = find_updatable_blocks(config)
        assert result == {
            "updatable_config/gains": {
                "start_time": 1500000000,
                "update_id": "g1",
            },
        }

    def test_multiple_blocks(self):
        config = {
            "updatable_config": {
                "flagging": {
                    "kotekan_update_endpoint": "json",
                    "bad_inputs": [1, 2],
                },
                "gains": {
                    "kotekan_update_endpoint": "json",
                    "start_time": 100,
                },
            },
        }
        result = find_updatable_blocks(config)
        assert "updatable_config/flagging" in result
        assert "updatable_config/gains" in result
        assert "kotekan_update_endpoint" not in result["updatable_config/flagging"]

    def test_deeply_nested(self):
        config = {
            "pipeline": {
                "stage": {
                    "tuning": {
                        "kotekan_update_endpoint": "json",
                        "param": 42,
                    }
                }
            }
        }
        result = find_updatable_blocks(config)
        assert result == {"pipeline/stage/tuning": {"param": 42}}


class TestUpdatableStore:
    def test_get_missing(self, tmp_path):
        store = UpdatableStore(tmp_path)
        assert store.get("cx/cx1") is None

    def test_save_and_get(self, tmp_path):
        store = UpdatableStore(tmp_path)
        values = {"start_time": 100, "update_id": "g1"}
        store.save("cx/cx1", "updatable_config/gains", values)
        result = store.get("cx/cx1")
        assert result == {"updatable_config/gains": values}

    def test_save_merges(self, tmp_path):
        store = UpdatableStore(tmp_path)
        store.save("cx/cx1", "updatable_config/gains", {"start_time": 100})
        store.save("cx/cx1", "updatable_config/flagging", {"bad_inputs": [1]})
        result = store.get("cx/cx1")
        assert "updatable_config/gains" in result
        assert "updatable_config/flagging" in result

    def test_save_all(self, tmp_path):
        store = UpdatableStore(tmp_path)
        blocks = {
            "updatable_config/gains": {"start_time": 100},
            "updatable_config/flagging": {"bad_inputs": [1, 2]},
        }
        store.save_all("cx/cx1", blocks)
        assert store.get("cx/cx1") == blocks

    def test_save_overwrites_endpoint(self, tmp_path):
        store = UpdatableStore(tmp_path)
        store.save("cx/cx1", "updatable_config/gains", {"start_time": 100})
        store.save("cx/cx1", "updatable_config/gains", {"start_time": 200})
        result = store.get("cx/cx1")
        assert result["updatable_config/gains"]["start_time"] == 200


class TestConfigOverrides:
    def test_default_config_name(self, configs_dir):
        registry = Registry(configs_dir)
        assert registry.get_config_name("cx/cx1") == "cx/cx1"

    def test_loads_override_from_nodes_yaml(self, configs_dir):
        # Rewrite nodes.yaml with a config override
        nodes = {
            "groups": {
                "cx": {
                    "cx1": {"host": "cx1.chord.ca", "port": 12048,
                            "config": "shared-config"},
                    "cx2": {"host": "cx2.chord.ca", "port": 12048},
                },
            }
        }
        with open(configs_dir / "nodes.yaml", "w") as f:
            yaml.dump(nodes, f)
        registry = Registry(configs_dir)
        assert registry.get_config_name("cx/cx1") == "shared-config"
        assert registry.get_config_name("cx/cx2") == "cx/cx2"

    def test_set_config_name(self, configs_dir):
        registry = Registry(configs_dir)
        registry.set_config_name("cx/cx1", "shared-config")
        assert registry.get_config_name("cx/cx1") == "shared-config"
        # Verify persisted to nodes.yaml
        with open(configs_dir / "nodes.yaml") as f:
            data = yaml.safe_load(f)
        assert data["groups"]["cx"]["cx1"]["config"] == "shared-config"

    def test_set_default_clears_override(self, configs_dir):
        registry = Registry(configs_dir)
        registry.set_config_name("cx/cx1", "shared-config")
        assert registry.get_config_name("cx/cx1") == "shared-config"
        # Setting back to node key should remove the override
        registry.set_config_name("cx/cx1", "cx/cx1")
        assert registry.get_config_name("cx/cx1") == "cx/cx1"
        with open(configs_dir / "nodes.yaml") as f:
            data = yaml.safe_load(f)
        assert "config" not in data["groups"]["cx"]["cx1"]

    def test_reload_config_overrides(self, configs_dir):
        registry = Registry(configs_dir)
        # Edit nodes.yaml to add an override
        with open(configs_dir / "nodes.yaml") as f:
            data = yaml.safe_load(f)
        data["groups"]["cx"]["cx1"]["config"] = "other"
        with open(configs_dir / "nodes.yaml", "w") as f:
            yaml.dump(data, f)
        registry._reload_config_overrides()
        assert registry.get_config_name("cx/cx1") == "other"
