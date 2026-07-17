"""power source — feeds whose amplifier the power controller reports unpowered.

Read-only. Joins power_db's live ``/channel_states`` with a channel->input map.
An independent "is this feed powered" signal that does not depend on bffs's own
flagging (no latch), and self-heals when a feed powers back on.

PROVISIONAL: the channel->input map (``power_map.csv``) is assumed — real wiring
comes from the hardware database (padloper). The board/chip byte framing in
``decode_channel_states`` is verified against the live controller (2026-07-17,
via choco's PSU toggle test: a ch0 write to board 0 chip A moves exactly the
last raw byte). Only ever issues ``GET /channel_states``.

Standalone diagnostic: ``python -m sources.power --url http://10.222.0.30:5000``
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

from .common import load_map, project

# A power channel is addressed by (spi_bus, board, chip, channel); see power_db_v3.
Channel = tuple[int, int, str, int]
_DEFAULT_MAP = str(Path(__file__).with_name("power_map.csv"))


def _key(row: dict) -> Channel:
    return (int(row["spi_bus"]), int(row["board"]), row["chip"].strip().upper(), int(row["channel"]))


def decode_channel_states(raw: list[int], spi_bus: int) -> dict[Channel, bool]:
    """Decode one bus's raw ``/channel_states`` buffer to per-channel power.

    Mirrors power_db's own daisy-chain decode (spi_ops._write_and_verify_all):
    reverse the flat byte list, then every even index is a chip's ``OUT`` byte,
    with chip_num = i//2, board = chip_num//2, chip 'A' if chip_num even else 'B'.
    Each ``OUT`` bit c (0..7) is channel c; 1 = powered.
    """
    resp = list(raw)
    resp.reverse()
    out: dict[Channel, bool] = {}
    for i in range(0, len(resp), 2):
        chip_num = i // 2
        board, chip = chip_num // 2, ("A" if chip_num % 2 == 0 else "B")
        out_byte = resp[i]
        for ch in range(8):
            out[(spi_bus, board, chip, ch)] = bool(out_byte & (1 << ch))
    return out


def read_power_state(base_url: str) -> dict[Channel, bool]:
    """GET /channel_states (read-only) and decode every active bus."""
    url = base_url.rstrip("/") + "/channel_states"
    with urllib.request.urlopen(url, timeout=10.0) as resp:
        data = json.loads(resp.read())
    states: dict[Channel, bool] = {}
    for bus_str, raw in data["channel_states"].items():
        states.update(decode_channel_states(raw, int(bus_str)))
    return states


def mask(src: dict, labels, kotekan_file: str):
    """Good-mask over ``labels``: a feed whose power channel reads off is bad.

    A mapped channel absent from the live read counts as unpowered (fail-safe).
    """
    power_map = load_map(src.get("map", _DEFAULT_MAP), _key)
    state = read_power_state(src["url"])
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    return project(input_good, labels)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sources.power",
                                description="report CHORD feed power by correlator input (read-only)")
    p.add_argument("-m", "--map", default=_DEFAULT_MAP, help="channel->input map CSV")
    p.add_argument("-u", "--url", default="http://10.222.0.30:5000", help="power_db base URL")
    args = p.parse_args(argv)

    power_map = load_map(args.map, _key)
    state = read_power_state(args.url)
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    unmapped_powered = sorted(ch for ch, on in state.items() if on and ch not in power_map)

    print(json.dumps({
        "powered": sorted(i for i, on in input_good.items() if on),
        "unpowered": sorted(i for i, on in input_good.items() if not on),
        "n_mapped": len(input_good),
        "unmapped_powered_channels": [list(ch) for ch in unmapped_powered],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
