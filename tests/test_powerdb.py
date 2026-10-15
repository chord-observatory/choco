"""choco.powerdb: the power_db monitor and its power controls (and the
wire conventions it shares with bffs, choco.pdbmap)."""

from unittest.mock import patch

import pytest
import responses

from choco.pdbmap import decode_out_bytes
from choco.powerdb import PdbMonitor


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    """Keep the retry's real-time sleep out of the test suite."""
    monkeypatch.setattr("choco.http.RETRY_DELAY_S", 0)


PDB_BASE = "http://pdb.example:5000"


@pytest.fixture
def pdb():
    return PdbMonitor(host="pdb.example", port=5000, timeout=1)


class TestDecodeOutBytes:
    def test_out_bytes_are_odd_positions_from_the_end(self):
        # 2 chips -> 4 raw bytes; after reversal every even index is a
        # chip's OUT byte, chip 0 first.
        assert decode_out_bytes([0x18, 0x02, 0x18, 0x01]) == [0x01, 0x02]

    def test_live_shape_all_off(self):
        # The deployed controller reads [128, 0] per chip: status byte
        # 128, OUT byte 0 -> every channel off.
        assert decode_out_bytes([128, 0] * 4) == [0, 0, 0, 0]

    def test_empty(self):
        assert decode_out_bytes([]) == []


class TestPdbMonitorPollOnce:
    @responses.activate
    def test_healthy_path(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        # chip 0 (board 0 A): ch0 on; chip 1 (board 0 B): all off
        responses.get(f"{PDB_BASE}/channel_states", json={
            "channel_states": {"0": [0x18, 0x00, 0x18, 0x01]},
        })
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.error is None
        assert pdb.boards == {0: 1}
        rows = pdb.channels[0]
        assert rows[0] == {"board": 0, "chip": "A",
                           "channels": [True] + [False] * 7}
        assert rows[1]["chip"] == "B"
        assert not any(rows[1]["channels"])
        assert pdb.n_channels == 16
        assert pdb.n_on == 1
        assert pdb.last_seen is not None

    @responses.activate
    def test_status_down(self, pdb):
        responses.get(f"{PDB_BASE}/status", status=500)
        pdb.poll_once()
        assert pdb.health == "down"
        assert pdb.error

    @responses.activate
    def test_status_unreachable(self, pdb):
        import requests as _requests
        responses.get(f"{PDB_BASE}/status",
                      body=_requests.ConnectionError("refused"))
        pdb.poll_once()
        assert pdb.health == "down"

    @responses.activate
    def test_status_ok_but_states_missing(self, pdb):
        responses.get(f"{PDB_BASE}/status",
                      json={"active_buses": [0], "buses": {}})
        responses.get(f"{PDB_BASE}/channel_states", status=500)
        pdb.poll_once()
        assert pdb.health == "no_states"
        assert "/channel_states" in pdb.error

    @responses.activate
    def test_status_non_dict_json_tolerated(self, pdb):
        responses.get(f"{PDB_BASE}/status", json=["garbage"])
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": {"0": [128, 0]}})
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.boards == {}

    @responses.activate
    def test_states_wrong_shape_is_no_states(self, pdb):
        responses.get(f"{PDB_BASE}/status",
                      json={"active_buses": [0], "buses": {}})
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": "bogus"})
        pdb.poll_once()
        assert pdb.health == "no_states"
        assert "unexpected /channel_states shape" in pdb.error

    @responses.activate
    def test_states_garbage_bus_info_tolerated(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": "sixteen", "1": {"board_count": 2}},
        })
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": {}})
        pdb.poll_once()
        assert pdb.health == "ok"
        assert pdb.boards == {1: 2}

    def test_unconfigured_monitor_reports_unconfigured(self):
        m = PdbMonitor(host=None, port=None)
        assert m.configured is False
        assert m.health == "unconfigured"
        m.poll_once()
        assert m.health == "unconfigured"

    @responses.activate
    def test_to_dict(self, pdb):
        responses.get(f"{PDB_BASE}/status", json={
            "active_buses": [0],
            "buses": {"0": {"board_count": 1, "chip_count": 2}},
        })
        responses.get(f"{PDB_BASE}/channel_states", json={
            "channel_states": {"0": [0, 0xFF, 0, 0xFF]},
        })
        pdb.poll_once()
        d = pdb.to_dict()
        assert d["health"] == "ok"
        assert d["buses"] == [0]
        assert d["n_channels"] == 16
        assert d["n_on"] == 16


def _states(raw: list[int]) -> dict:
    return {"channel_states": {"0": raw}}


class TestPdbSetChannel:
    """1 board = 2 chips = 4 raw bytes; chip0's OUT byte is raw[3],
    chip1's is raw[1] (odd positions from the end)."""

    @responses.activate
    def test_turn_on(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))          # pre: all off
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))          # post: ch0 on
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is True
        assert "ch0 on" in message
        import json as _json
        payload = _json.loads(write.calls[0].request.body)
        assert payload == {"spi_bus": 0, "board_idx": 0, "chip_letter": "A",
                           "operation": "OUT", "states": "00000001"}
        # the post-write read refreshed the grid
        assert pdb.channels[0][0]["channels"][0] is True

    @responses.activate
    def test_turn_off_keeps_other_bits(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0b11]))       # ch0+ch1 on
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0b10]))
        ok, message = pdb.set_channel(0, 0, "A", 0, False)
        assert ok is True
        import json as _json
        assert _json.loads(write.calls[0].request.body)["states"] == "00000010"

    @responses.activate
    def test_already_on_skips_write(self, pdb):
        # No POST registered: a write attempt would raise ConnectionError.
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 1]))
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is True
        assert "already on" in message

    @responses.activate
    def test_verify_mismatch_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))          # didn't take
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "verify failed" in message
        assert "state changed underneath us" in message

    @responses.activate
    def test_write_failure(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", status=500)
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False

    @responses.activate
    def test_chip_not_present(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([]))
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "not present" in message

    @responses.activate
    def test_garbage_states_reported_not_raised(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json={"channel_states": [1, 2, 3]})
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "ValueError" in message

    @responses.activate
    def test_controller_unreachable_reported(self, pdb):
        # no responses registered -> ConnectionError on the pre-read
        ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False
        assert "ConnectionError" in message

    def test_bare_oserror_is_reported_not_raised(self, pdb):
        with patch("choco.http.requests.request",
                   side_effect=ConnectionResetError("reset by peer")):
            ok, message = pdb.set_channel(0, 0, "A", 0, True)
        assert ok is False and "ConnectionResetError" in message

    def test_invalid_address(self, pdb):
        ok, message = pdb.set_channel(0, 0, "C", 0, True)
        assert ok is False
        ok, message = pdb.set_channel(0, 0, "A", 8, True)
        assert ok is False

    def test_unconfigured(self):
        ok, message = PdbMonitor(host=None, port=None).set_channel(
            0, 0, "A", 0, True)
        assert ok is False
        assert "not configured" in message


class TestPdbSetChannels:
    """Arbitrary channel sets (a dish row in one pol) folded into one
    OUT-byte read-modify-write per chip, set_group's rules throughout.

    Two boards = 4 chips = 8 raw bytes; chip k's OUT byte is raw[2N-1-2k],
    so [128, x3, 128, x2, 128, x1, 128, x0] holds chips 0..3 in x0..x3.
    """

    @staticmethod
    def _payloads(write):
        import json as _json
        return [_json.loads(c.request.body) for c in write.calls]

    @responses.activate
    def test_masks_fold_per_chip_and_keep_other_bits(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0x01]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x80, 128, 0x00, 128, 0x00, 128, 0x07]))
        ok, message = pdb.set_channels(
            [(0, 0, "A", 1), (0, 0, "A", 2), (0, 1, "B", 7)], True, "row A pol X")
        assert ok is True, message
        # chip 0 keeps its bit 0 and gains 1 and 2; chip 3 gains bit 7;
        # chips 1 and 2 are not touched
        assert self._payloads(write) == [
            {"spi_bus": 0, "board_idx": 0, "chip_letter": "A",
             "operation": "OUT", "states": "00000111"},
            {"spi_bus": 0, "board_idx": 1, "chip_letter": "B",
             "operation": "OUT", "states": "10000000"}]
        assert message == "row A pol X: 3 channels on"
        assert pdb.n_on == 4

    @responses.activate
    def test_already_right_writes_nothing(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0x03]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        ok, message = pdb.set_channels([(0, 0, "A", 0), (0, 0, "A", 1)], True, "row A pol X")
        assert ok is True
        assert write.call_count == 0
        assert "already on" in message

    @responses.activate
    def test_off_clears_only_the_masked_bits(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0xFF]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0xFE]))
        ok, message = pdb.set_channels([(0, 0, "A", 0)], False, "row A pol Y")
        assert ok is True, message
        assert self._payloads(write)[0]["states"] == "11111110"
        assert message == "row A pol Y: 1 channels off"

    @responses.activate
    def test_verify_mismatch_is_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0x00]))
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00, 128, 0x00, 128, 0x00]))
        ok, message = pdb.set_channels([(0, 0, "A", 0)], True, "row A pol X")
        assert ok is False
        assert "did not take" in message and "board 0 chip A" in message

    @responses.activate
    def test_missing_chip_is_reported_before_any_write(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0x00, 128, 0x00]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        ok, message = pdb.set_channels([(0, 0, "A", 0), (0, 5, "A", 0)], True, "row A pol X")
        assert ok is False
        assert "not present on the controller" in message and "board 5 chip A" in message
        assert write.call_count == 0

    def test_bad_address_and_empty_set_rejected(self, pdb):
        assert pdb.set_channels([(0, 0, "C", 0)], True, "x")[0] is False
        assert pdb.set_channels([(0, 0, "A", 8)], True, "x")[0] is False
        assert pdb.set_channels([], True, "x")[0] is False

    def test_unconfigured(self):
        ok, message = PdbMonitor(host=None, port=None).set_channels([(0, 0, "A", 0)], True, "x")
        assert ok is False and "not configured" in message


class TestPdbSetGroup:
    """Bulk power: whole chips written as all-ones / all-zeros OUT bytes.

    Two boards = 4 chips = 8 raw bytes; chip k's OUT byte is raw[2N-1-2k],
    so [128, x3, 128, x2, 128, x1, 128, x0] holds chips 0..3 in x0..x3.
    """

    @staticmethod
    def _payloads(write):
        import json as _json
        return [_json.loads(c.request.body) for c in write.calls]

    @responses.activate
    def test_chip_scope_writes_one_chip(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0xFF, 128, 0]))
        ok, message = pdb.set_group(0, True, board=0, chip="B")
        assert ok is True
        assert self._payloads(write) == [
            {"spi_bus": 0, "board_idx": 0, "chip_letter": "B",
             "operation": "OUT", "states": "11111111"}]
        assert "8 channels on" in message

    @responses.activate
    def test_board_scope_writes_both_chips(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert [(p["board_idx"], p["chip_letter"])
                for p in self._payloads(write)] == [(0, "A"), (0, "B")]
        assert "16 channels on" in message

    @responses.activate
    def test_bus_scope_writes_every_chip(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0, 128, 0, 128, 0]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF, 128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True)
        assert ok is True
        assert [(p["board_idx"], p["chip_letter"])
                for p in self._payloads(write)] == [
            (0, "A"), (0, "B"), (1, "A"), (1, "B")]
        assert "32 channels on" in message
        assert pdb.n_on == 32

    @responses.activate
    def test_off_writes_zero_bytes(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        ok, message = pdb.set_group(0, False, board=0)
        assert ok is True
        assert {p["states"] for p in self._payloads(write)} == {"00000000"}

    @responses.activate
    def test_chips_already_correct_are_not_written(self, pdb):
        """Only chip B needs the write; chip A is already all-on."""
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0xFF]))
        write = responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert [p["chip_letter"] for p in self._payloads(write)] == ["B"]
        assert "8 channels on (16 in scope)" in message

    @responses.activate
    def test_nothing_to_do_skips_writes_entirely(self, pdb):
        # No POST registered: any write attempt would raise.
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0xFF]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is True
        assert "already on" in message

    @responses.activate
    def test_partial_write_failure_is_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", status=500)
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0xFF, 128, 0]))
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is False
        assert "1 write(s) failed" in message
        assert "did not take" in message
        # the successful half is still reflected in the grid
        assert pdb.channels[0][1]["channels"] == [True] * 8

    @responses.activate
    def test_verify_read_failure_is_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states",
                      json=_states([128, 0, 128, 0]))
        responses.post(f"{PDB_BASE}/write_command", json={"message": "ok"})
        responses.get(f"{PDB_BASE}/channel_states", status=500)
        ok, message = pdb.set_group(0, True, board=0)
        assert ok is False
        assert "verify read failed" in message

    @responses.activate
    def test_unknown_bus_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([128, 0]))
        ok, message = pdb.set_group(7, True)
        assert ok is False
        assert "bus not present" in message

    @responses.activate
    def test_unknown_board_reported(self, pdb):
        responses.get(f"{PDB_BASE}/channel_states", json=_states([128, 0]))
        ok, message = pdb.set_group(0, True, board=9)
        assert ok is False
        assert "no such chips" in message

    @responses.activate
    def test_controller_unreachable_reported(self, pdb):
        ok, message = pdb.set_group(0, True)
        assert ok is False
        assert "ConnectionError" in message

    def test_invalid_scope(self, pdb):
        assert pdb.set_group(0, True, board=0, chip="C")[0] is False
        assert pdb.set_group(0, True, board=-1)[0] is False
        # a chip is only addressable within a board
        assert pdb.set_group(0, True, chip="A")[0] is False

    def test_unconfigured(self):
        ok, message = PdbMonitor(host=None, port=None).set_group(0, True)
        assert ok is False
        assert "not configured" in message
