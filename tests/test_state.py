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

    def test_default_started_is_false(self, configs_dir):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.started is False

    def test_started_from_nodes_yaml(self, tmp_path):
        nodes = {
            "groups": {
                "cx": {
                    "cx1": {"host": "cx1.chord.ca", "port": 12048, "started": True},
                    "cx2": {"host": "cx2.chord.ca", "port": 12048},
                },
            }
        }
        with open(tmp_path / "nodes.yaml", "w") as f:
            yaml.dump(nodes, f)
        registry = Registry(tmp_path)
        assert registry.get_node("cx/cx1").started is True
        assert registry.get_node("cx/cx2").started is False

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


class TestRegistryReload:
    """Registry.reload() clears and rebuilds; save_nodes_yaml() persists edits."""

    def _rewrite_nodes(self, configs_dir, data):
        with open(configs_dir / "nodes.yaml", "w") as f:
            yaml.dump(data, f)

    def test_reload_picks_up_added_group(self, configs_dir):
        registry = Registry(configs_dir)
        assert "new/n1" not in registry.nodes
        self._rewrite_nodes(configs_dir, {
            "groups": {
                "new": {"n1": {"host": "n1.example", "port": 12048}},
            }
        })
        registry.reload()
        assert set(registry.nodes.keys()) == {"new/n1"}

    def test_reload_drops_removed_nodes(self, configs_dir):
        registry = Registry(configs_dir)
        assert "cx/cx1" in registry.nodes
        self._rewrite_nodes(configs_dir, {"groups": {}})
        registry.reload()
        assert registry.nodes == {}

    def test_reload_resets_runtime_state(self, configs_dir):
        """Reload is a full reset — runtime ``started`` toggles are dropped."""
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        node.started = True  # simulated runtime toggle
        registry.reload()
        # A fresh Node is constructed; started defaults to False.
        assert registry.get_node("cx/cx1").started is False

    def test_reload_handles_empty_group(self, tmp_path):
        """A group with no members (YAML null) must not crash reload."""
        (tmp_path / "nodes.yaml").write_text("groups:\n  empty_grp:\n")
        registry = Registry(tmp_path)
        assert registry.nodes == {}

    def test_reload_missing_file_clears_registry(self, configs_dir):
        registry = Registry(configs_dir)
        assert registry.nodes  # populated
        (configs_dir / "nodes.yaml").unlink()
        registry.reload()
        assert registry.nodes == {}

    def test_save_nodes_yaml_roundtrip(self, configs_dir):
        registry = Registry(configs_dir)
        new_data = {
            "groups": {
                "g1": {"n1": {"host": "n1.example", "port": 12048}},
                "g2": {"n2": {"host": "n2.example", "port": 9000}},
            }
        }
        registry.save_nodes_yaml(new_data)
        on_disk = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        assert on_disk == new_data
        registry.reload()
        assert set(registry.nodes.keys()) == {"g1/n1", "g2/n2"}
        assert registry.get_node("g2/n2").port == 9000

    def test_save_nodes_yaml_is_atomic(self, configs_dir):
        """save_nodes_yaml writes via temp+rename; no .tmp left behind."""
        registry = Registry(configs_dir)
        registry.save_nodes_yaml({"groups": {}})
        leftovers = list(configs_dir.glob("nodes.yaml*"))
        assert leftovers == [configs_dir / "nodes.yaml"]


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


class TestLoadResilience:
    """Corrupt config files must not crash registry init/reload."""

    def _write_bad_updatable(self, configs_dir):
        path = configs_dir / ".updatable" / "cx" / "cx1.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"updatable_config/gains": {"start_time": 100}\n')
        return path

    def test_corrupt_updatable_does_not_raise(self, configs_dir, caplog):
        self._write_bad_updatable(configs_dir)
        with caplog.at_level("ERROR"):
            registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node is not None
        assert node.updatable_config is None
        assert node.load_error is not None
        assert "Bad updatable JSON" in node.load_error
        # Path is logged so the user can find the bad file.
        assert any("cx1.json" in r.message for r in caplog.records)

    def test_corrupt_updatable_other_nodes_unaffected(self, configs_dir):
        self._write_bad_updatable(configs_dir)
        registry = Registry(configs_dir)
        # cx2 has no updatable and a clean (missing) config; loads cleanly.
        assert registry.get_node("cx/cx2") is not None
        assert registry.get_node("cx/cx2").load_error is None

    def test_corrupt_base_config_does_not_raise(self, configs_dir, caplog):
        (configs_dir / "cx" / "cx1.yaml").write_text("not_a_mapping")
        with caplog.at_level("ERROR"):
            registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.rendered_config is None
        assert node.load_error is not None
        assert "Bad base config" in node.load_error
        assert any("cx1.yaml" in r.message for r in caplog.records)

    def test_corrupt_j2_template_does_not_raise(self, configs_dir, caplog):
        (configs_dir / "cx" / "cx1.yaml").unlink()
        (configs_dir / "cx" / "cx1.j2").write_text("num_elements: {{ unterminated\n")
        with caplog.at_level("ERROR"):
            registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.rendered_config is None
        assert node.load_error is not None
        assert "cx1.j2" in node.load_error

    def test_corrupt_vars_yaml_does_not_raise(self, configs_dir, caplog):
        (configs_dir / "vars.yaml").write_text("key: [unclosed\n")
        with caplog.at_level("ERROR"):
            registry = Registry(configs_dir)
        # Registry still populated — vars just defaulted to empty.
        assert "cx/cx1" in registry.nodes
        assert any("vars.yaml" in r.message for r in caplog.records)

    def test_corrupt_nodes_yaml_does_not_raise(self, configs_dir, caplog):
        (configs_dir / "nodes.yaml").write_text("groups: [unclosed\n")
        with caplog.at_level("ERROR"):
            registry = Registry(configs_dir)
        assert registry.nodes == {}
        assert any("nodes.yaml" in r.message for r in caplog.records)

    def test_reload_clears_load_error_on_success(self, configs_dir):
        path = self._write_bad_updatable(configs_dir)
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.load_error is not None
        # Fix the file and reload just this node.
        path.write_text('{"updatable_config/gains": {"start_time": 100}}')
        node.load_updatable()
        # Note: load_updatable does not clear an error set by load_config.
        # Here only load_updatable set the error, so a clean reload of
        # the same file should leave the override populated.
        assert node.updatable_config == {
            "updatable_config/gains": {"start_time": 100}
        }

    def test_save_updatable_warns_when_overwriting_broken_file(
        self, configs_dir, caplog,
    ):
        self._write_bad_updatable(configs_dir)
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.load_error is not None

        with caplog.at_level("WARNING"):
            node.save_updatable("updatable_config/gains", {"start_time": 5})

        assert node.load_error is None
        assert any(
            "Overwriting previously-unreadable" in r.message
            and "cx1.json" in r.message
            for r in caplog.records
        )

    def test_save_updatable_no_warning_on_clean_save(self, configs_dir, caplog):
        registry = Registry(configs_dir)
        node = registry.get_node("cx/cx1")
        assert node.load_error is None

        with caplog.at_level("WARNING"):
            node.save_updatable("updatable_config/gains", {"start_time": 5})

        assert not any(
            "Overwriting" in r.message for r in caplog.records
        )


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


# --- Config library: nodes.yaml ``config:``, includes, dependencies ---

from choco.state import resolve_config_path, list_config_files  # noqa: E402


@pytest.fixture
def library_dir(tmp_path):
    """A configs directory with a shared library: two cx nodes on one
    chord/pathfinder.j2 that includes chord/telescope.j2, one recv node
    on its legacy per-node file, and a stray file nobody uses."""
    nodes = {
        "groups": {
            "cx": {
                "cx1": {"host": "cx1.chord.ca", "port": 12048,
                        "config": "chord/pathfinder.j2"},
                "cx2": {"host": "cx2.chord.ca", "port": 12048,
                        "config": "chord/pathfinder.j2"},
            },
            "recv": {
                "recv1": {"host": "recv1.chord.ca", "port": 12048},
            },
        }
    }
    (tmp_path / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    chord = tmp_path / "chord"
    chord.mkdir()
    (chord / "pathfinder.j2").write_text(
        "{% set num_dishes = 64 %}\n"
        "num_elements: {{ 2 * num_dishes }}\n"
        '{% include "telescope.j2" %}\n'
    )
    (chord / "telescope.j2").write_text(
        "telescope:\n    name: CHORDTelescope\n    num_dishes: {{ num_dishes }}\n"
    )
    (chord / "spare.yaml").write_text("unused: true\n")
    (tmp_path / "recv").mkdir()
    (tmp_path / "recv" / "recv1.yaml").write_text("buffer_depth: 12\n")
    return tmp_path


class TestResolveConfigPath:
    def test_plain_relative_path(self, tmp_path):
        assert resolve_config_path(tmp_path, "chord/pathfinder.j2") == \
            tmp_path.resolve() / "chord" / "pathfinder.j2"

    @pytest.mark.parametrize("bad", [
        "", "/etc/passwd.yaml", "../x.yaml", "chord/../../x.j2", ".hidden/x.yaml",
        ".updatable/cx/cx1.json", "chord/x.txt", "chord\\x.j2", "nodes.yaml",
        "chord//x.j2", "x.j2\n", 42,
    ])
    def test_rejected(self, tmp_path, bad):
        with pytest.raises(ValueError):
            resolve_config_path(tmp_path, bad)


class TestListConfigFiles:
    def test_library_listing(self, library_dir):
        (library_dir / "vars.yaml").write_text("a: 1\n")
        upd = library_dir / ".updatable" / "cx"
        upd.mkdir(parents=True)
        (upd / "cx1.json").write_text("{}")
        (library_dir / "chord" / "notes.txt").write_text("no")
        assert list_config_files(library_dir) == [
            "chord/pathfinder.j2", "chord/spare.yaml", "chord/telescope.j2",
            "recv/recv1.yaml", "vars.yaml",
        ]

    def test_missing_dir_is_empty(self, tmp_path):
        assert list_config_files(tmp_path / "nope") == []


class TestConfigKey:
    def test_shared_config_renders_with_include(self, library_dir):
        registry = Registry(library_dir)
        for key in ("cx/cx1", "cx/cx2"):
            node = registry.get_node(key)
            assert node.config_filename == "chord/pathfinder.j2"
            assert node.explicit_config == "chord/pathfinder.j2"
            assert node.config_abspath == library_dir / "chord" / "pathfinder.j2"
            assert node.rendered_config == {
                "num_elements": 128,
                "telescope": {"name": "CHORDTelescope", "num_dishes": 64},
            }
            assert node.dependencies == {library_dir / "chord" / "telescope.j2"}
            assert node.load_error is None

    def test_legacy_node_unchanged(self, library_dir):
        node = Registry(library_dir).get_node("recv/recv1")
        assert node.explicit_config is None
        assert node.config_filename == "recv/recv1.yaml"
        assert node.rendered_config == {"buffer_depth": 12}
        assert node.dependencies == set()

    def test_include_from_configs_root(self, library_dir):
        """A name the file's own directory lacks resolves from the root."""
        (library_dir / "chord" / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "recv/recv1.yaml" %}\n')
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config == {"num_elements": 128, "buffer_depth": 12}
        assert node.dependencies == {library_dir / "recv" / "recv1.yaml"}

    def test_missing_include_is_load_error(self, library_dir, caplog):
        (library_dir / "chord" / "telescope.j2").unlink()
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config is None
        assert "telescope.j2" in node.load_error
        # Recorded where the loader would look, so creating it reloads.
        assert node.dependencies == {library_dir / "chord" / "telescope.j2"}

    def test_nested_includes_are_dependencies(self, library_dir):
        (library_dir / "chord" / "telescope.j2").write_text(
            'telescope: {name: x}\n{% include "common.j2" %}\n')
        (library_dir / "chord" / "common.j2").write_text("log_level: WARN\n")
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config["log_level"] == "WARN"
        assert node.dependencies == {library_dir / "chord" / "telescope.j2",
                                     library_dir / "chord" / "common.j2"}

    def test_parent_traversal_in_include_is_refused(self, library_dir):
        (library_dir / "secret.yaml").write_text("x: 1\n")
        (library_dir / "chord" / "pathfinder.j2").write_text(
            '{% include "../../secret.yaml" %}\n')
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config is None
        assert node.load_error

    def test_bad_config_path_is_load_error_not_a_path(self, library_dir):
        data = yaml.safe_load((library_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "../../etc/passwd.yaml"
        (library_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config is None
        assert node.config_abspath is None
        assert "Bad config path" in node.load_error
        with pytest.raises(ValueError):
            node.save_base("a: 1\n")

    def test_missing_config_file_is_no_config(self, library_dir):
        data = yaml.safe_load((library_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/absent.j2"
        (library_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        node = Registry(library_dir).get_node("cx/cx1")
        assert node.rendered_config is None
        assert node.load_error is None
        assert node.config_filename == "chord/absent.j2"

    def test_save_base_writes_the_shared_file(self, library_dir):
        registry = Registry(library_dir)
        registry.get_node("cx/cx1").save_base("num_elements: 256\n")
        assert (library_dir / "chord" / "pathfinder.j2").read_text() == \
            "num_elements: 256\n"
        assert registry.get_node("cx/cx1").dependencies == set()

    def test_overlay_render_does_not_touch_disk(self, library_dir):
        registry = Registry(library_dir)
        node = registry.get_node("cx/cx1")
        tel = library_dir / "chord" / "telescope.j2"
        before = tel.read_text()
        out = node.render(node.base_content,
                          overlay={tel: "telescope: {name: overlaid}\n"})
        assert out["telescope"] == {"name": "overlaid"}
        assert tel.read_text() == before
        with pytest.raises(Exception):
            node.render(node.base_content, overlay={tel: "{% bogus %}"})

    def test_users_of(self, library_dir):
        registry = Registry(library_dir)
        direct, includers = registry.users_of("chord/pathfinder.j2")
        assert [n.key for n in direct] == ["cx/cx1", "cx/cx2"]
        assert includers == []
        direct, includers = registry.users_of("chord/telescope.j2")
        assert direct == []
        assert [n.key for n in includers] == ["cx/cx1", "cx/cx2"]
        assert registry.users_of("chord/spare.yaml") == ([], [])
        assert registry.config_files() == [
            "chord/pathfinder.j2", "chord/spare.yaml", "chord/telescope.j2",
            "recv/recv1.yaml",
        ]

    def test_bare_node_renders_without_a_loader(self):
        node = Node("n", "g", "h")
        assert node.render("a: 1\n") == {"a": 1}
        with pytest.raises(Exception):
            node.render('{% include "x.j2" %}')
