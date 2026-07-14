"""Tests for the power source — run with `pytest`. No network needed."""

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


def test_power_join_projects_onto_labels():
    state = power.decode_channel_states(_LIVE_BUF, spi_bus=0)  # feed_0121's channel is on
    power_map = {(0, 7, "B", 1): "feed_0121", (0, 0, "A", 0): "feed_0000"}
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    labels = np.array(["feed_0000", "feed_0121", "feed_9999"])
    # feed_0000 unpowered -> bad, feed_0121 powered -> good, feed_9999 uncovered -> good
    np.testing.assert_array_equal(project(input_good, labels), [False, True, True])
