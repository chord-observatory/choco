"""Tests for the power source — run with `pytest`. No network needed."""

import pytest
import numpy as np

from sources import power
from sources.common import load_map, project

# The exact /channel_states buffer captured from the live server (bus 0, 16
# boards): every chip's OUT reads 0x00 except one reading 0x02.
_LIVE_BUF = [128, 0] * 16 + [128, 2] + [128, 0] * 15


def test_decode_simple():
    state = power.decode_channel_states([128, 2, 128, 0], spi_bus=0)
    assert state[(0, 0, "B", 1)] is True
    assert state[(0, 0, "A", 0)] is False
    assert sum(state.values()) == 1


def test_decode_live_buffer_one_channel_on():
    state = power.decode_channel_states(_LIVE_BUF, spi_bus=0)
    on = [ch for ch, powered in state.items() if powered]
    assert on == [(0, 7, "B", 1)]
    assert len(state) == 32 * 8


def test_load_power_map(tmp_path):
    csv_file = tmp_path / "map.csv"
    csv_file.write_text(
        "spi_bus,board,chip,channel,amplifier,correlator_input\n"
        "0,7,b,1,AMP-0121,feed_0121\n"      # lower-case chip -> normalized to 'B'
        "0,0,A,0,AMP-0000,feed_0000\n"
    )
    m = load_map(csv_file, power._key)
    assert m[(0, 7, "B", 1)] == "feed_0121"
    assert m[(0, 0, "A", 0)] == "feed_0000"
    assert len(m) == 2                      # 'amplifier' column ignored, not a key


def test_load_map_accepts_chocos_column_name(tmp_path):
    """choco's master table calls the column dish_input."""
    csv_file = tmp_path / "map.csv"
    csv_file.write_text("spi_bus,board,chip,channel,dish_input\n0,0,A,0,A1X\n")
    assert load_map(csv_file, power._key) == {(0, 0, "A", 0): "A1X"}


def test_load_map_without_an_input_column_is_an_error(tmp_path):
    csv_file = tmp_path / "map.csv"
    csv_file.write_text("spi_bus,board,chip,channel,amplifier\n0,0,A,0,AMP-1\n")
    with pytest.raises(ValueError, match="no correlator-input column"):
        load_map(csv_file, power._key)


class TestResolveMap:
    """Where the channel->input map comes from: config, choco, or bundled."""

    CHOCO_PAYLOAD = {
        "channels": [{"spi_bus": 0, "board": 7, "chip": "b", "channel": 1,
                      "dish_input": "A1X", "correlator_input": "A1X"}],
        "errors": [],
        "check": {"available": True, "ok": True, "group": "cx",
                  "n_matched": 1, "n_kotekan": 1},
    }

    def test_explicit_map_wins(self, tmp_path, monkeypatch):
        csv_file = tmp_path / "map.csv"
        csv_file.write_text("spi_bus,board,chip,channel,dish_input\n0,0,A,0,LOCAL\n")
        monkeypatch.setattr(power, "choco_pdb_map",
                            lambda *a, **k: pytest.fail("choco was consulted"))
        m = power.resolve_map({"map": str(csv_file),
                               "choco_url": "https://localhost:5000"})
        assert m == {(0, 0, "A", 0): "LOCAL"}

    def test_choco_master_table_used_by_default(self, monkeypatch):
        monkeypatch.setattr(power, "choco_pdb_map",
                            lambda url, **k: self.CHOCO_PAYLOAD)
        m = power.resolve_map({"choco_url": "https://localhost:5000"})
        assert m == {(0, 7, "B", 1): "A1X"}     # chip letter normalized

    def test_choco_unreachable_falls_back_to_the_bundled_csv(self, monkeypatch,
                                                             caplog):
        def boom(url, **k):
            raise OSError("connection refused")
        monkeypatch.setattr(power, "choco_pdb_map", boom)
        m = power.resolve_map({"choco_url": "https://localhost:5000"})
        assert m == load_map(power._DEFAULT_MAP, power._key)
        assert "no PDB map from choco" in caplog.text

    def test_no_choco_url_uses_the_bundled_csv(self):
        assert power.resolve_map({}) == load_map(power._DEFAULT_MAP, power._key)

    def test_disagreement_with_kotekan_is_logged_not_fatal(self, monkeypatch,
                                                           caplog):
        payload = dict(self.CHOCO_PAYLOAD,
                       check={"available": True, "ok": False, "group": "cx",
                              "n_matched": 1, "n_kotekan": 3,
                              "missing_in_map": ["A2X", "A3X"],
                              "unknown_to_kotekan": [],
                              "duplicate_labels": {}})
        monkeypatch.setattr(power, "choco_pdb_map", lambda url, **k: payload)
        m = power.resolve_map({"choco_url": "https://localhost:5000"})
        assert m == {(0, 7, "B", 1): "A1X"}     # still usable
        assert "disagrees with the cx kotekan config" in caplog.text

    def test_bad_rows_reported(self, monkeypatch, caplog):
        payload = dict(self.CHOCO_PAYLOAD, errors=["line 4: unparseable"])
        monkeypatch.setattr(power, "choco_pdb_map", lambda url, **k: payload)
        power.resolve_map({"choco_url": "https://localhost:5000"})
        assert "has 1 bad row" in caplog.text


def test_power_join_projects_onto_labels():
    state = power.decode_channel_states(_LIVE_BUF, spi_bus=0)  # feed_0121's channel is on
    power_map = {(0, 7, "B", 1): "feed_0121", (0, 0, "A", 0): "feed_0000"}
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    labels = np.array(["feed_0000", "feed_0121", "feed_9999"])
    # feed_0000 unpowered -> bad, feed_0121 powered -> good, feed_9999 uncovered -> good
    np.testing.assert_array_equal(project(input_good, labels), [False, True, True])
