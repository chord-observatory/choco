"""Shared fixtures: a choco app over a small two-group registry.

``configs_dir`` / ``app`` / ``client`` serve every route-level suite
(test_web, test_cli, ...).  A module that needs a different registry
defines its own fixture of the same name, which overrides these.
"""

import pytest
import yaml

from choco.app import create_app


@pytest.fixture
def configs_dir(tmp_path):
    """Temporary configs directory with a starting set of two groups."""
    nodes = {
        "groups": {
            "cx": {
                "cx1": {"host": "cx1.example", "port": 12048},
                "cx2": {"host": "cx2.example", "port": 12048},
            },
            "recv": {
                "recv1": {"host": "recv1.example", "port": 12048},
            },
        }
    }
    (tmp_path / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "cx").mkdir()
    (tmp_path / "cx" / "cx1.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "cx" / "cx2.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "recv").mkdir()
    (tmp_path / "recv" / "recv1.yaml").write_text("buffer_depth: 12\n")
    return tmp_path


@pytest.fixture
def app(configs_dir):
    app = create_app(configs_dir=configs_dir)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()
