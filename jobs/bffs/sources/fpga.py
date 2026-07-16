"""fpga source — flag correlator inputs from F-engine (pychfpga) health.

Read-only. Turns per-channel signal health published by the raw acquisition
server (pychfpga ``raw_acq``) into a good-mask over correlator inputs. Three
independent digital-health signals, none visible in the N² band power that
``power-outlier`` uses:

  - overflows : FFT/scaler saturation (clipping)        -> bad if > max_overflows
  - frames    : ADC frames received (is it streaming?)  -> bad if < min_frames
  - adc_rms   : ADC RMS level                           -> bad if outside [lo, hi]

A feed is good only if all three pass.

The map (``fpga_map.csv``) is the rack wiring table (Slot 1-4 x ADC1-8 -> feed),
keyed by the coordinates raw_acq puts in its metric labels: the stream id encodes
``(crate << 8) | (slot - 1) << 4 | chan`` with a 0-based channelizer index
(pychfpga ``ucap.py``/``raw_acq.unpack_stream_id``) — so physical Slot N is
``slot`` N-1 and ADC N is assumed ``chan`` N-1. The ``_M_*`` metric names match
the pychfpga raw_acq source.

PROVISIONAL (the FPGA is off — confirm against live output when it is up): the
crate number (assumed 0, ucap's no-crate fallback), ADC N -> chan N-1, the
``/get-monitoring-data`` endpoint/gzip format, and the default RMS bounds.
Read-only.

Standalone diagnostic: ``python -m sources.fpga --url http://<fpga-node>:<port>``
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import urllib.request
from pathlib import Path

from .common import iter_metrics, load_map, project

# An F-engine channel is addressed by (crate, slot, chan); see pychfpga raw_acq.
Channel = tuple[int, int, int]
_DEFAULT_MAP = str(Path(__file__).with_name("fpga_map.csv"))

# raw_acq metric names (match the pychfpga raw_acq source; confirm live).
_M_OVERFLOW = "raw_acq_fft_scaler_overflows"
_M_FRAMES = "raw_acq_adc_frames"
_M_RMS = "raw_acq_adc_averaged_rms"


def _key(row: dict) -> Channel:
    return (int(row["crate"]), int(row["slot"]), int(row["chan"]))


def parse_channel_health(text: str) -> dict[Channel, dict]:
    """Parse raw_acq Prometheus metrics into per-(crate,slot,chan) health fields.

    Reads the three ``_M_*`` metrics, keyed by their crate/slot/chan labels;
    other lines (node-level metrics, comments) are ignored. Returns
    ``{channel: {overflows, frames, adc_rms}}`` (adc_rms NaN if absent).
    """
    fields: dict[Channel, dict[str, float]] = {}
    for name, labels, value in iter_metrics(text):
        if name not in (_M_OVERFLOW, _M_FRAMES, _M_RMS):
            continue
        try:
            ch = (int(labels["crate"]), int(labels["slot"]), int(labels["chan"]))
        except KeyError:
            continue
        fields.setdefault(ch, {})[name] = value
    return {
        ch: {"overflows": int(d.get(_M_OVERFLOW, 0)),
             "frames": int(d.get(_M_FRAMES, 0)),
             "adc_rms": d.get(_M_RMS, math.nan)}
        for ch, d in fields.items()
    }


def read_channel_health(base_url: str) -> dict[Channel, dict]:
    """GET /get-monitoring-data (read-only), gunzip if needed, and parse."""
    url = base_url.rstrip("/") + "/get-monitoring-data"
    with urllib.request.urlopen(url, timeout=10.0) as resp:
        raw = resp.read()
    try:
        text = gzip.decompress(raw).decode()
    except OSError:        # not gzip-encoded
        text = raw.decode()
    return parse_channel_health(text)


def health_mask(health: dict[Channel, dict], *, max_overflows: int = 0,
                min_frames: int = 1, rms_lo: float | None = 1.0,
                rms_hi: float | None = 100.0) -> dict[Channel, bool]:
    """Per-channel good-mask (``True`` = good). Bad if any signal trips.

    A channel whose RMS was absent (NaN) is judged on overflows/frames only —
    the RMS bounds are skipped rather than failing it.
    """
    out: dict[Channel, bool] = {}
    for ch, h in health.items():
        good = h["overflows"] <= max_overflows and h["frames"] >= min_frames
        rms = h["adc_rms"]
        if good and not math.isnan(rms):
            if rms_lo is not None:
                good = rms >= rms_lo
            if good and rms_hi is not None:
                good = rms <= rms_hi
        out[ch] = good
    return out


def mask(src: dict, labels, kotekan_file: str):
    """Good-mask over ``labels`` from F-engine per-channel health.

    A mapped channel absent from the health sample counts as bad (fail-safe).
    """
    fpga_map = load_map(src.get("map", _DEFAULT_MAP), _key)
    good = health_mask(read_channel_health(src["url"]))
    input_good = {inp: good.get(ch, False) for ch, inp in fpga_map.items()}
    return project(input_good, labels)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sources.fpga",
                                description="flag correlator inputs from F-engine health (read-only)")
    p.add_argument("-m", "--map", default=_DEFAULT_MAP, help="(crate,slot,chan)->input map CSV")
    p.add_argument("-u", "--url", required=True, help="raw_acq base URL, e.g. http://<fpga-node>:<port>")
    args = p.parse_args(argv)

    fpga_map = load_map(args.map, _key)
    health = read_channel_health(args.url)
    good = health_mask(health)
    input_good = {inp: good.get(ch, False) for ch, inp in fpga_map.items()}

    reasons = {}        # why each mapped-bad input failed
    for ch, inp in fpga_map.items():
        if input_good[inp]:
            continue
        reasons[inp] = health.get(ch, "no health sample")

    print(json.dumps({
        "n_mapped": len(input_good),
        "bad_inputs": sorted(i for i, g in input_good.items() if not g),
        "reasons": reasons,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
