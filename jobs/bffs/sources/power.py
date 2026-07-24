"""power source — feeds whose amplifier the power controller reports unpowered.

Read-only. Joins power_db's live ``/channel_states`` with a channel->input map.
An independent "is this feed powered" signal that does not depend on bffs's own
flagging (no latch), and self-heals when a feed powers back on.

The channel->input map comes from choco's master PDB table
(``GET /api/pdb/map``, one CSV beside choco's ``nodes.yaml``) so this job and
choco's own PDB page agree on which breaker feeds which feed; choco
cross-checks that table against kotekan's ``dish_inputs`` and reports the
verdict in the same payload, which is logged here. Setting ``map:`` in the
source config overrides it with a local CSV, and the bundled placeholder is
the last resort (dry runs, choco down).

PROVISIONAL: the wiring itself is assumed until the hardware database
(padloper) provides the real table. The board/chip byte framing in
``decode_channel_states`` is verified against the live controller (2026-07-17,
via choco's PDB toggle test: a ch0 write to board 0 chip A moves exactly the
last raw byte). Only ever issues ``GET /channel_states``.

Standalone diagnostic: ``python -m sources.power --url http://10.222.0.30:5000``
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request
from pathlib import Path

from .common import choco_pdb_map, load_map, project

log = logging.getLogger("bffs.power")

# A power channel is addressed by (spi_bus, board, chip, channel); see power_db_v3.
Channel = tuple[int, int, str, int]
_DEFAULT_MAP = str(Path(__file__).with_name("power_map.csv"))


def _key(row: dict) -> Channel:
    return (int(row["spi_bus"]), int(row["board"]), row["chip"].strip().upper(), int(row["channel"]))


def resolve_map(src: dict) -> dict[Channel, str]:
    """The channel -> correlator-input map for one run.

    An explicit ``map:`` is an operator override and wins.  Otherwise
    choco's master table is used when choco is reachable (the normal
    path), falling back to the bundled placeholder CSV so dry runs and a
    choco outage still produce a mask rather than failing the source.
    """
    if src.get("map"):
        return load_map(src["map"], _key)
    choco_url = src.get("choco_url")
    if choco_url:
        try:
            payload = choco_pdb_map(choco_url)
        except (OSError, ValueError) as e:
            log.warning("no PDB map from choco (%s); using %s",
                        e, _DEFAULT_MAP)
        else:
            _log_check(payload.get("check") or {}, payload.get("errors") or [])
            rows = payload.get("channels") or []
            return {(int(r["spi_bus"]), int(r["board"]),
                     str(r["chip"]).strip().upper(), int(r["channel"])):
                    str(r.get("dish_input") or r.get("correlator_input", "")).strip()
                    for r in rows}
    return load_map(_DEFAULT_MAP, _key)


def _log_check(check: dict, errors: list) -> None:
    """Surface choco's map-vs-kotekan verdict in the bffs journal.

    A disagreement is loud but not fatal: a channel whose label is not on
    the current element axis simply projects onto nothing, so a stale
    wiring row cannot mis-flag a feed — it just leaves one unwatched.
    """
    if errors:
        log.warning("choco's PDB map has %d bad row(s): %s",
                    len(errors), "; ".join(str(e) for e in errors[:3]))
    if not check.get("available"):
        log.info("PDB map not cross-checked against kotekan: %s",
                 check.get("reason"))
    elif not check.get("ok"):
        log.warning(
            "PDB map disagrees with the %s kotekan config: %d of %d dish "
            "inputs mapped (%d unmapped, %d unknown to kotekan, %d "
            "duplicated) — those feeds' power is not being watched",
            check.get("group"), check.get("n_matched", 0),
            check.get("n_kotekan", 0), len(check.get("missing_in_map") or []),
            len(check.get("unknown_to_kotekan") or []),
            len(check.get("duplicate_labels") or {}))


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
    power_map = resolve_map(src)
    state = read_power_state(src["url"])
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    return project(input_good, labels)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sources.power",
                                description="report CHORD feed power by correlator input (read-only)")
    p.add_argument("-m", "--map", default=None,
                   help="channel->input map CSV (default: choco's master table, "
                        "or the bundled placeholder if --choco is unset)")
    p.add_argument("-c", "--choco", default=None,
                   help="choco base URL to read /api/pdb/map from")
    p.add_argument("-u", "--url", default="http://10.222.0.30:5000", help="power_db base URL")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    power_map = resolve_map({"map": args.map, "choco_url": args.choco})
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
