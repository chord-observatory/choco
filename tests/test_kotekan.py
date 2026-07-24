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
        assert node.get_status() == NodeStatus.IDLE

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

    @responses.activate
    def test_version_info_full(self, node):
        payload = {
            "kotekan_version": "2024.11",
            "branch": "main",
            "git_commit_hash": "abcdef1234567890",
            "cmake_build_settings": {"CMAKE_BUILD_TYPE": "Release"},
            "available_stages": ["stage_a", "stage_b"],
        }
        responses.get(f"{BASE}/version", json=payload)
        assert node.get_version_info() == payload
        assert node.get_version() == "2024.11"

    @responses.activate
    def test_version_info_unreachable(self, node):
        responses.get(f"{BASE}/version", body=ConnectionError())
        assert node.get_version_info() is None
        assert node.get_version() is None

    @responses.activate
    def test_version_info_missing_fields(self, node):
        responses.get(f"{BASE}/version", json={"kotekan_version": "2024.11"})
        info = node.get_version_info()
        assert info == {"kotekan_version": "2024.11"}
        assert info.get("branch") is None


class TestGetBuffers:
    @responses.activate
    def test_returns_buffer_table(self, node):
        payload = {
            "n2_buffer": {
                "num_full_frame": 3, "frames": [1, 1, 1, 0],
                "frame_size": 100756, "peek_hold": True,
            },
            "ring_buf": {"consumers": {}, "producers": {}},
        }
        responses.get(f"{BASE}/buffers", json=payload)
        assert node.get_buffers() == payload

    @responses.activate
    def test_unreachable(self, node):
        responses.get(f"{BASE}/buffers", body=ConnectionError())
        assert node.get_buffers() is None

    @responses.activate
    def test_non_dict_reply(self, node):
        responses.get(f"{BASE}/buffers", json=["not", "a", "dict"])
        assert node.get_buffers() is None


class TestGetBufferFrame:
    @responses.activate
    def test_returns_frame_reply(self, node):
        payload = {
            "buffer": "n2_buffer", "frame_id": 2, "frame_size": 100756,
            "data_length": 0, "metadata": {"fpga_seq": 12345},
            "frame_desc": {"frame_desc_type": "N2"},
        }
        responses.get(f"{BASE}/buffer/n2_buffer/frame", json=payload)
        assert node.get_buffer_frame("n2_buffer", length=0) == payload
        assert responses.calls[0].request.url.endswith("?len=0")

    @responses.activate
    def test_no_length_omits_len_param(self, node):
        responses.get(f"{BASE}/buffer/n2_buffer/frame", json={"frame_id": 0})
        node.get_buffer_frame("n2_buffer")
        assert "len=" not in responses.calls[0].request.url

    @responses.activate
    def test_no_full_frame_is_error_reply_not_none(self, node):
        # kotekan reports a peek miss as HTTP 402; that's a meaningful
        # reply ("try again" / "enable peek_hold"), not an outage.
        responses.get(f"{BASE}/buffer/n2_buffer/frame", status=402)
        frame = node.get_buffer_frame("n2_buffer", length=0)
        assert frame is not None
        assert "no full frame" in frame["error"]

    @responses.activate
    def test_unreachable(self, node):
        responses.get(f"{BASE}/buffer/n2_buffer/frame", body=ConnectionError())
        assert node.get_buffer_frame("n2_buffer") is None
