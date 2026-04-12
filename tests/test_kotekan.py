"""Tests for the kotekan REST API methods on Node."""

import json

import pytest
import responses

from choco.state import Node, NodeStatus

HOST = "localhost"
PORT = 12048
BASE = f"http://{HOST}:{PORT}"


@pytest.fixture
def node():
    return Node(name="test", group="test", host=HOST, port=PORT, timeout=1)


class TestGetStatus:
    @responses.activate
    def test_running(self, node):
        responses.get(f"{BASE}/status", json={"running": True})
        assert node.get_status() == NodeStatus.STARTED

    @responses.activate
    def test_not_running(self, node):
        responses.get(f"{BASE}/status", json={"running": False})
        assert node.get_status() == NodeStatus.STOPPED

    @responses.activate
    def test_unreachable(self, node):
        responses.get(f"{BASE}/status", body=ConnectionError("refused"))
        assert node.get_status() == NodeStatus.DOWN


class TestGetConfig:
    @responses.activate
    def test_returns_config(self, node):
        config = {"num_elements": 2048, "log_level": "info"}
        responses.get(f"{BASE}/config", json=config)
        result = node.get_config()
        assert result == config

    @responses.activate
    def test_unreachable(self, node):
        responses.get(f"{BASE}/config", body=ConnectionError())
        assert node.get_config() is None


class TestPushUpdatable:
    @responses.activate
    def test_success(self, node):
        responses.post(f"{BASE}/foo/bar", json={"status": "ok"})
        assert node.push_updatable("foo/bar", {"val": 42}) is True

    @responses.activate
    def test_failure(self, node):
        responses.post(f"{BASE}/foo/bar", body=ConnectionError())
        assert node.push_updatable("foo/bar", {"val": 42}) is False


class TestLifecycle:
    @responses.activate
    def test_start(self, node):
        responses.post(f"{BASE}/start", json={"status": "ok"})
        assert node.start({"config": "data"}) is True

    @responses.activate
    def test_version(self, node):
        responses.get(f"{BASE}/version", json={"kotekan_version": "2024.11"})
        assert node.get_version() == "2024.11"
