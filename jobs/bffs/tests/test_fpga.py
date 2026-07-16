"""Tests for the fpga source — run with `pytest`. No network/FPGA needed."""

import math

import numpy as np

from sources import fpga
from sources.common import load_map, project

# A synthetic raw_acq monitoring payload: two channels plus node-level noise.
_METRICS = """\
# a comment line
raw_acq_node_cpu_percent 12.0
raw_acq_adc_averaged_rms{crate="0",slot="3",chan="5"} 14.2
raw_acq_fft_scaler_overflows{crate="0",slot="3",chan="5"} 0
raw_acq_adc_frames{crate="0",slot="3",chan="5"} 1000
raw_acq_adc_averaged_rms{crate="0",slot="3",chan="6"} 0.2
raw_acq_fft_scaler_overflows{crate="0",slot="3",chan="6"} 7
raw_acq_adc_frames{crate="0",slot="3",chan="6"} 0
"""


def test_parse_channel_health():
    h = fpga.parse_channel_health(_METRICS)
    assert h[(0, 3, 5)] == {"overflows": 0, "frames": 1000, "adc_rms": 14.2}
    assert h[(0, 3, 6)] == {"overflows": 7, "frames": 0, "adc_rms": 0.2}
    assert len(h) == 2          # node-level lines ignored


def test_health_mask_each_signal():
    health = {
        (0, 0, 0): {"overflows": 0, "frames": 1000, "adc_rms": 14.2},   # healthy
        (0, 0, 1): {"overflows": 7, "frames": 1000, "adc_rms": 14.2},   # saturating
        (0, 0, 2): {"overflows": 0, "frames": 0, "adc_rms": 14.2},      # not streaming
        (0, 0, 3): {"overflows": 0, "frames": 1000, "adc_rms": 0.2},    # dead (low RMS)
        (0, 0, 4): {"overflows": 0, "frames": 1000, "adc_rms": 150.0},  # hot (high RMS)
        (0, 0, 5): {"overflows": 0, "frames": 1000, "adc_rms": math.nan},  # RMS absent
    }
    good = fpga.health_mask(health)
    assert good[(0, 0, 0)] is True
    assert good[(0, 0, 1)] is False   # overflow
    assert good[(0, 0, 2)] is False   # no frames
    assert good[(0, 0, 3)] is False   # RMS too low
    assert good[(0, 0, 4)] is False   # RMS too high
    assert good[(0, 0, 5)] is True    # NaN RMS -> judged on overflow/frames only


def test_health_mask_thresholds_configurable():
    health = {(0, 0, 1): {"overflows": 3, "frames": 1000, "adc_rms": 14.2}}
    assert fpga.health_mask(health)[(0, 0, 1)] is False             # default max_overflows=0
    assert fpga.health_mask(health, max_overflows=5)[(0, 0, 1)] is True


def test_load_fpga_map(tmp_path):
    csv_file = tmp_path / "map.csv"
    csv_file.write_text(
        "crate,slot,chan,note,correlator_input\n"
        "0,3,5,wet?,feed_0121\n"
        "1,0,0,,feed_0256\n"
    )
    m = load_map(csv_file, fpga._key)
    assert m[(0, 3, 5)] == "feed_0121"
    assert m[(1, 0, 0)] == "feed_0256"
    assert len(m) == 2           # 'note' column ignored, not a key


def test_shipped_map_matches_wiring_table():
    m = load_map(fpga._DEFAULT_MAP, fpga._key)
    assert len(m) == 32                                   # 4 slots x 8 ADCs
    assert set(m.values()) == {f"{g}{n}{p}" for g in "AB" for n in range(1, 9) for p in "XY"}
    assert all(c == 0 and 0 <= s <= 3 and 0 <= ch <= 7 for c, s, ch in m)
    # corners of the table: metric slot/chan are 0-based (Slot N -> N-1, ADC N -> N-1)
    assert m[(0, 0, 0)] == "A1X"      # Slot 1, ADC1
    assert m[(0, 2, 4)] == "B5X"      # Slot 3, ADC5
    assert m[(0, 3, 7)] == "B8Y"      # Slot 4, ADC8


def test_fpga_join_projects_onto_labels():
    good = fpga.health_mask(fpga.parse_channel_health(_METRICS))
    fpga_map = {(0, 3, 5): "feed_0121", (0, 3, 6): "feed_0122"}
    input_good = {inp: good.get(ch, False) for ch, inp in fpga_map.items()}
    labels = np.array(["feed_0121", "feed_0122", "feed_9999"])
    # ch5 healthy -> good, ch6 saturating/no-frames/low-RMS -> bad, feed_9999 uncovered -> good
    np.testing.assert_array_equal(project(input_good, labels), [True, False, True])
