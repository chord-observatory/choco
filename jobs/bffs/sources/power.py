"""power source — feeds whose amplifier is unpowered, or that have no amplifier.

Read-only. Joins power_db's live ``/channel_states`` with a channel->input map.
An independent "is this feed powered" signal that does not depend on bffs's own
flagging (no latch), and self-heals when a feed powers back on.  The map is
also the inventory of what is wired at all: a feed on the element axis with
no channel in it (the E/F/G/H dishes before they are installed, placeholder
elements) has nothing to power and is flagged *absent* — ch_flag's ``layout``
signal — provided the table can be trusted (see :func:`mask`).

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

from .common import choco_pdb_map, load_map, project, report

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
    return resolve_map_info(src)[0]


def resolve_map_info(src: dict) -> tuple[dict[Channel, str], dict]:
    """:func:`resolve_map` plus where the map came from, for the report.

    The info dict carries ``map_source`` (``config CSV``, ``choco master
    table`` or ``bundled placeholder``), ``map_path`` for the CSVs, and
    for choco's table its ``check`` verdict against kotekan's
    ``dish_inputs`` and the number of unparseable rows.
    """
    if src.get("map"):
        return load_map(src["map"], _key), {
            "map_source": "config CSV", "map_path": str(src["map"])}
    choco_url = src.get("choco_url")
    fallback_reason = None
    if choco_url:
        try:
            payload = choco_pdb_map(choco_url)
        except (OSError, ValueError) as e:
            log.warning("no PDB map from choco (%s); using %s",
                        e, _DEFAULT_MAP)
            fallback_reason = f"no PDB map from choco: {e}"
        else:
            check = payload.get("check") or {}
            errors = payload.get("errors") or []
            _log_check(check, errors)
            rows = payload.get("channels") or []
            table = {(int(r["spi_bus"]), int(r["board"]),
                      str(r["chip"]).strip().upper(), int(r["channel"])):
                     str(r.get("dish_input") or r.get("correlator_input", "")).strip()
                     for r in rows}
            info = {"map_source": "choco master table",
                    "map_bad_rows": len(errors),
                    "map_check_available": bool(check.get("available")),
                    "map_unknown_to_kotekan": len(check.get("unknown_to_kotekan") or [])}
            if check.get("available"):
                info["map_check"] = "ok" if check.get("ok") else (
                    f"disagrees with the {check.get('group')} kotekan config: "
                    f"{check.get('n_matched', 0)} of {check.get('n_kotekan', 0)} "
                    f"dish inputs mapped")
            else:
                info["map_check"] = f"not cross-checked ({check.get('reason')})"
            return table, info
    info = {"map_source": "bundled placeholder", "map_path": _DEFAULT_MAP}
    if fallback_reason:
        info["map_fallback_reason"] = fallback_reason
    return load_map(_DEFAULT_MAP, _key), info


def table_trusted(info: dict) -> tuple[bool, str | None]:
    """Whether the map may be read as the inventory of wired feeds.

    Returns ``(trusted, why_not)``.  An operator's ``map:`` CSV is taken
    at its word.  choco's master table is trusted when choco could
    cross-check it against kotekan's ``dish_inputs`` and found no row
    naming a dish input kotekan does not know — a stale or renamed label
    there would make real feeds look unmapped, and flagging *those*
    absent is the one mistake this rule must not make.  The bundled
    placeholder never qualifies.
    """
    source = info.get("map_source")
    if source == "config CSV":
        return True, None
    if source == "choco master table":
        if not info.get("map_check_available"):
            return False, "choco could not cross-check the table against kotekan"
        unknown = int(info.get("map_unknown_to_kotekan") or 0)
        if unknown:
            return False, (f"the table names {unknown} dish input(s) kotekan does "
                           f"not know (stale rows?)")
        return True, None
    return False, "the map is the bundled placeholder"


def _log_check(check: dict, errors: list) -> None:
    """Surface choco's map-vs-kotekan verdict in the bffs journal.

    A disagreement is loud but not fatal: unmapped feeds are flagged
    absent only when the table is trusted (:func:`table_trusted`), so a
    stale wiring row leaves feeds unjudged and reported, never mis-flagged.
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
    """Good-mask over ``labels``: a feed is bad when its power channel
    reads off, or when the table has no channel for it at all.

    The second rule is the connectivity signal: the master table is the
    inventory of what is wired, so a feed absent from it has no amplifier
    to power and is flagged ``not in PDB table`` — but only when the table
    is trusted (:func:`table_trusted`); otherwise those feeds are left
    good and the run reports ``degraded`` saying how many and why.

    A mapped channel absent from the live read counts as unpowered
    (fail-safe) and is reported ``degraded`` too: that is a read problem
    being turned into a flag, and the page should say so.  The report's
    ``feed_reasons`` names each flagged feed's reason (``off``, ``unread``,
    ``not in PDB table``), which the core carries into the state file for
    the element grid's hover text.
    """
    power_map, info = resolve_map_info(src)
    state = read_power_state(src["url"])
    input_good = {inp: state.get(ch, False) for ch, inp in power_map.items()}
    axis = [str(lbl) for lbl in labels]
    watched = [lbl for lbl in axis if lbl in input_good]
    unmapped = [lbl for lbl in axis if lbl not in input_good]
    unread = sorted(inp for ch, inp in power_map.items()
                    if ch not in state and inp in set(axis))
    reasons = {lbl: ("unread" if lbl in unread else "off")
               for lbl in watched if not input_good[lbl]}
    trusted, why_not = table_trusted(info)
    if unmapped and trusted:
        for lbl in unmapped:
            input_good[lbl] = False
            reasons[lbl] = "not in PDB table"
    detail = {
        "url": src["url"],
        "channels_read": len(state),
        "n_mapped": len(power_map),
        "n_watched": len(watched),
        "n_unpowered": sum(1 for lbl in watched if not input_good[lbl]),
        "n_unmapped": len(unmapped),
        "unmapped_flagged": bool(unmapped and trusted),
        "feed_reasons": reasons,
        **info,
    }
    problems = []
    if unread:
        detail["unread"] = unread[:10]
        problems.append(f"{len(unread)} mapped channel(s) absent from the "
                        f"controller's read; their feeds are judged unpowered")
        log.warning("power: %s: %s", problems[-1], ", ".join(unread[:10]))
    if unmapped and not trusted:
        problems.append(f"{len(unmapped)} feed(s) not in the PDB table left "
                        f"unjudged: {why_not}")
        log.warning("power: %s", problems[-1])
    n_measured = len(watched) + (len(unmapped) if trusted else 0)
    return project(input_good, labels), report(
        "degraded" if problems else "ok", "; ".join(problems) or None,
        n_measured=n_measured, **detail)


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
