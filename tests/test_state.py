"""Tests for node registry and state tracking."""

import pytest
from pathlib import Path

import yaml

from choco.state import (
    Registry, Node, NodeStatus,
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

    def test_loads_config_on_init(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.rendered_config is not None
        assert node.rendered_config["num_elements"] == 2048

    def test_node_without_config_file(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        assert node.rendered_config is None
        assert node.base_content is None

    def test_reload_node_config(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        assert node.rendered_config is None
        # Create a config file after init, then reload just that node
        (configs_dir / "cx" / "cx2.yaml").write_text("num_elements: 1024\n")
        node.load_config()
        assert node.rendered_config == {"num_elements": 1024}


class TestNodeConfig:
    def test_base_content(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.base_content is not None
        assert "num_elements" in node.base_content

    def test_config_filename(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.config_filename == "cx/cx1.yaml"

    def test_save_base(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.save_base("num_elements: 512\nlog_level: warn\n")
        assert node.rendered_config == {"num_elements": 512, "log_level": "warn"}
        assert node.base_content == "num_elements: 512\nlog_level: warn\n"
        # Verify on disk
        on_disk = yaml.safe_load((configs_dir / "cx" / "cx1.yaml").read_text())
        assert on_disk == {"num_elements": 512, "log_level": "warn"}

    def test_save_base_creates_directory(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("recv/recv1")
        node.save_base("buffer_depth: 12\n")
        assert (configs_dir / "recv" / "recv1.yaml").exists()

    def test_save_base_invalid_raises(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        with pytest.raises(ValueError):
            node.save_base("not_a_mapping")

    def test_j2_config(self, configs_dir):
        (configs_dir / "cx" / "cx2.j2").write_text(
            "num_elements: 1024\nlog_level: debug\n"
        )
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        assert node.rendered_config == {"num_elements": 1024, "log_level": "debug"}
        assert node.config_filename == "cx/cx2.j2"

    def test_j2_renders_with_vars(self, configs_dir):
        with open(configs_dir / "vars.yaml", "w") as f:
            yaml.dump({"n_elem": 2048}, f)
        (configs_dir / "cx" / "cx2.j2").write_text("num_elements: {{ n_elem }}\n")
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        assert node.rendered_config["num_elements"] == 2048

    def test_yaml_renders_with_vars(self, configs_dir):
        with open(configs_dir / "vars.yaml", "w") as f:
            yaml.dump({"level": "debug"}, f)
        (configs_dir / "cx" / "cx1.yaml").write_text("log_level: {{ level }}\n")
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.rendered_config["log_level"] == "debug"

    def test_save_base_preserves_j2_suffix(self, configs_dir):
        (configs_dir / "cx" / "cx2.j2").write_text("num_elements: 1024\n")
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        node.save_base("num_elements: 2048\n")
        assert (configs_dir / "cx" / "cx2.j2").read_text() == "num_elements: 2048\n"
        assert not (configs_dir / "cx" / "cx2.yaml").exists()

    def test_render(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        result = node.render("key: value\n")
        assert result == {"key": "value"}

    def test_render_invalid_raises(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        with pytest.raises(ValueError):
            node.render("not_a_mapping")


class TestNodeUpdatable:
    def test_no_updatable(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.updatable_config is None

    def test_save_and_load(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.save_updatable("updatable_config/gains", {"start_time": 100})
        assert node.updatable_config == {
            "updatable_config/gains": {"start_time": 100}
        }
        # Reload from disk
        node.load_updatable()
        assert node.updatable_config["updatable_config/gains"]["start_time"] == 100

    def test_save_merges(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.save_updatable("updatable_config/gains", {"start_time": 100})
        node.save_updatable("updatable_config/flagging", {"bad_inputs": [1]})
        assert "updatable_config/gains" in node.updatable_config
        assert "updatable_config/flagging" in node.updatable_config

    def test_save_overwrites_endpoint(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.save_updatable("updatable_config/gains", {"start_time": 100})
        node.save_updatable("updatable_config/gains", {"start_time": 200})
        assert node.updatable_config["updatable_config/gains"]["start_time"] == 200


class TestDesiredConfig:
    def test_no_updatable(self, configs_dir):
        """desired_config equals rendered_config when no updatable overrides."""
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.desired_config == node.rendered_config

    def test_with_updatable(self, configs_dir):
        """desired_config merges updatable overrides into rendered config."""
        (configs_dir / "cx" / "cx1.yaml").write_text(
            "updatable_config:\n"
            "  gains:\n"
            "    kotekan_update_endpoint: json\n"
            "    start_time: 0\n"
        )
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.save_updatable("updatable_config/gains", {"start_time": 100})

        desired = node.desired_config
        assert desired["updatable_config"]["gains"]["start_time"] == 100
        # rendered_config should still have the original value
        assert node.rendered_config["updatable_config"]["gains"]["start_time"] == 0

    def test_no_config_file(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx2")
        assert node.desired_config is None


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
