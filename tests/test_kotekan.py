"""Tests for the kotekan REST API client."""

import json

import pytest
import responses

from choco.kotekan import KotekanClient, KotekanStatus

HOST = "localhost"
PORT = 12048
BASE = f"http://{HOST}:{PORT}"


@pytest.fixture
def client():
    return KotekanClient(HOST, PORT, timeout=1)


class TestGetStatus:
    @responses.activate
    def test_running(self, client):
        responses.get(f"{BASE}/status", json={"running": True})
        status = client.get_status()
        assert status.reachable is True
        assert status.running is True
        assert status.ok is True

    @responses.activate
    def test_not_running(self, client):
        responses.get(f"{BASE}/status", json={"running": False})
        status = client.get_status()
        assert status.reachable is True
        assert status.running is False
        assert status.ok is False

    @responses.activate
    def test_unreachable(self, client):
        responses.get(f"{BASE}/status", body=ConnectionError("refused"))
        status = client.get_status()
        assert status.reachable is False
        assert status.ok is False


class TestGetConfig:
    @responses.activate
    def test_returns_config(self, client):
        config = {"num_elements": 2048, "log_level": "info"}
        responses.get(f"{BASE}/config", json=config)
        result = client.get_config()
        assert result == config

    @responses.activate
    def test_unreachable(self, client):
        responses.get(f"{BASE}/config", body=ConnectionError())
        assert client.get_config() is None


class TestGetConfigHash:
    @responses.activate
    def test_returns_hash(self, client):
        responses.get(f"{BASE}/config_md5sum", json={"md5sum": "abc123"})
        assert client.get_config_hash() == "abc123"

    @responses.activate
    def test_unreachable(self, client):
        responses.get(f"{BASE}/config_md5sum", body=ConnectionError())
        assert client.get_config_hash() is None


class TestUpdateConfig:
    @responses.activate
    def test_success(self, client):
        responses.post(f"{BASE}/foo/bar", json={"status": "ok"})
        assert client.update_config("foo/bar", {"val": 42}) is True

    @responses.activate
    def test_failure(self, client):
        responses.post(f"{BASE}/foo/bar", body=ConnectionError())
        assert client.update_config("foo/bar", {"val": 42}) is False


class TestLifecycle:
    @responses.activate
    def test_start(self, client):
        responses.post(f"{BASE}/start", json={"status": "ok"})
        assert client.start({"config": "data"}) is True

    @responses.activate
    def test_stop(self, client):
        responses.get(f"{BASE}/stop", json={"status": "ok"})
        assert client.stop() is True

    @responses.activate
    def test_version(self, client):
        responses.get(f"{BASE}/version", json={"kotekan_version": "2024.11"})
        assert client.get_version() == "2024.11"
