"""The power distribution boards: power_db's monitor and power controls.

``PdbMonitor`` polls the power_db controller (a Pi fronting the PDB SPI
buses) for the POWER badge and the ``/service/power`` page, and switches
channels: one channel, an arbitrary set (a dish row in one
polarization), or whole chips, boards and buses.  Every write is the
same read-modify-write-verify (:meth:`PdbMonitor._write_masks`); the
three entry points differ only in what they address and how they word
the outcome.  The wire conventions live in ``choco.pdbmap``, shared with
the bffs power source.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from .http import fetch
from .monitor import HttpMonitor
from .pdbmap import board_chip, chip_index, decode_out_bytes, valid_channel_address

logger = logging.getLogger(__name__)

#: What a read or a decode of the controller's reply can raise: transport,
#: a malformed body, a missing or wrongly typed key.
_PDB_ERRORS = (OSError, ValueError, KeyError, TypeError)


def _summarize(items: list[str], limit: int = 4) -> str:
    """First few items plus a count of the rest — bulk writes can fail wide."""
    head = ", ".join(items[:limit])
    extra = len(items) - limit
    return f"{head} and {extra} more" if extra > 0 else head


@dataclass
class _ChipWrite:
    """One chip's part of a write: its OUT byte now and as wanted."""
    bus: int
    chip_num: int
    board: int
    chip: str
    current: int
    wanted: int

    @property
    def needed(self) -> bool:
        return self.current != self.wanted

    def name(self, with_bus: bool) -> str:
        head = f"bus {self.bus} " if with_bus else ""
        return f"{head}board {self.board} chip {self.chip}"


@dataclass
class _Outcome:
    """How a write went, for the caller to word."""
    error: str | None = None            # the read before the write failed
    missing: list[str] = field(default_factory=list)   # chips not on the controller
    plan: list[_ChipWrite] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)  # "<chip> (<exception>)"
    verify_error: str | None = None     # the read after the write failed
    readback: dict | None = None        # bus -> OUT bytes after the write
    stale: list[str] = field(default_factory=list)     # chips whose OUT is not as wanted

    @property
    def todo(self) -> list[_ChipWrite]:
        return [w for w in self.plan if w.needed]


class PdbMonitor(HttpMonitor):
    """Background poller for the power_db PDB (power distribution boards).

    Hits ``/status`` and ``/channel_states`` on a slow interval and
    decodes the per-channel power state.  Same shape as
    :class:`~choco.fpga.FpgaMonitor`: latest result held in attributes,
    surfaced by the services strip and the ``/service/power`` page.

    Health levels:
      * ``ok``        — both endpoints responded and decoded.
      * ``no_states`` — /status responded but /channel_states did not.
      * ``down``      — /status did not respond.
      * ``unconfigured`` — no pdb.host / port in config.
    """

    def __init__(self, host: str | None, port: int | None, timeout: float = 5.0):
        super().__init__(host, port, timeout)
        # {bus: [{"board": int, "chip": "A"|"B", "channels": [bool]*8}]}
        self.channels: dict[int, list[dict]] = {}
        self.boards: dict[int, int] = {}   # bus -> board count (from /status)
        self.last_seen: float | None = None

    @property
    def n_channels(self) -> int:
        return sum(8 * len(rows) for rows in self.channels.values())

    @property
    def n_on(self) -> int:
        return sum(sum(r["channels"]) for rows in self.channels.values()
                   for r in rows)

    # --- reading ----------------------------------------------------------

    def _get_json(self, path: str):
        """GET with one retry, like every monitor probe, decoded."""
        return fetch("GET", f"{self.base_url}{path}", timeout=self.timeout, retries=1).json()

    def _fetch_states(self) -> dict[int, list[int]]:
        """GET /channel_states, decoded to per-chip OUT bytes per bus.

        Raises ValueError on an unexpected response shape so every
        caller's error path treats "garbage" the same as "unreachable".
        """
        data = self._get_json("/channel_states")
        states = data.get("channel_states") if isinstance(data, dict) else None
        if not isinstance(states, dict):
            raise ValueError(f"unexpected /channel_states shape: {data!r:.80}")
        return {int(bus): decode_out_bytes(raw) for bus, raw in states.items()}

    @staticmethod
    def _rows(out_bytes: list[int]) -> list[dict]:
        rows = []
        for k, out in enumerate(out_bytes):
            board, chip = board_chip(k)
            rows.append({
                "board": board, "chip": chip,
                "channels": [bool(out & (1 << c)) for c in range(8)],
            })
        return rows

    def _adopt(self, states: dict[int, list[int]], seen: float | None = None) -> None:
        """Take a fresh read as the grid the page shows."""
        self.channels = {bus: self._rows(outs) for bus, outs in states.items()}
        self.last_seen = time.time() if seen is None else seen

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "pdb.host / port not set"
            return

        # /status: reachability + board counts
        try:
            status = self._get_json("/status")
            buses = status.get("buses") if isinstance(status, dict) else None
            self.boards = {
                int(bus): int(info.get("board_count", 0))
                for bus, info in (buses or {}).items()
                if isinstance(info, dict)
            } if isinstance(buses, dict) else {}
        except (OSError, ValueError, TypeError) as e:
            self.health = "down"
            self.error = f"{type(e).__name__}: {e}"
            return

        # /channel_states: raw OUT-register bytes per bus
        try:
            self._adopt(self._fetch_states(), seen=self.last_polled)
            self.health = "ok"
            self.error = None
        except _PDB_ERRORS as e:
            self.health = "no_states"
            self.error = f"/channel_states: {type(e).__name__}: {e}"

    # --- writing ----------------------------------------------------------

    def _write_out_byte(self, bus: int, board: int, chip: str, value: int) -> None:
        """POST one chip's whole OUT register.  Raises on a failed write."""
        fetch("POST", f"{self.base_url}/write_command",
              json={"spi_bus": bus, "board_idx": board,
                    "chip_letter": chip, "operation": "OUT",
                    "states": format(value, "08b")},
              timeout=self.timeout)

    def _write_masks(self, masks: dict[tuple[int, int], int], on: bool, *,
                     states: dict[int, list[int]] | None = None,
                     with_bus: bool = True) -> _Outcome:
        """Set (*on*) or clear the bits in *masks* (``(bus, chip_num) -> bits``)
        with one OUT-byte read-modify-write per chip, then verify.

        power_db has no per-channel endpoint — a chip's OUT register is
        written whole — and other writers exist (power_db's own CLI), so
        the current bytes are read immediately before the write (or
        taken from *states*, a read the caller just made) and the result
        is confirmed by a fresh read afterwards.  A chip already in the
        wanted state is not written; every chip that needs it is written
        even if another fails; and a partial result is reported, never
        silently retried.  The read after the write also refreshes the
        monitor's grid, so the page shows the new truth.  *with_bus*
        says whether chip names carry their bus.
        """
        out = _Outcome()
        if states is None:
            try:
                states = self._fetch_states()
            except _PDB_ERRORS as e:
                out.error = f"{type(e).__name__}: {e}"
                return out
        for (bus, chip_num), mask in sorted(masks.items()):
            board, chip = board_chip(chip_num)
            outs = states.get(bus)
            if outs is None or chip_num >= len(outs):
                out.missing.append(_ChipWrite(bus, chip_num, board, chip, 0, 0).name(with_bus))
                continue
            current = outs[chip_num]
            wanted = current | mask if on else current & ~mask
            out.plan.append(_ChipWrite(bus, chip_num, board, chip, current, wanted))
        if out.missing or not out.todo:
            return out
        for w in out.todo:
            try:
                self._write_out_byte(w.bus, w.board, w.chip, w.wanted)
            except (OSError, ValueError) as e:
                out.failures.append(f"{w.name(with_bus)} ({type(e).__name__})")
        try:
            out.readback = self._fetch_states()
        except _PDB_ERRORS as e:
            out.verify_error = f"{type(e).__name__}: {e}"
            return out
        self._adopt(out.readback)
        out.stale = [w.name(with_bus) for w in out.plan
                     if w.chip_num >= len(out.readback.get(w.bus, []))
                     or out.readback[w.bus][w.chip_num] != w.wanted]
        return out

    @staticmethod
    def _problems(out: _Outcome) -> str:
        parts = []
        if out.failures:
            parts.append(f"{len(out.failures)} write(s) failed: " + _summarize(out.failures))
        if out.stale:
            parts.append(f"{len(out.stale)} chip(s) did not take: " + _summarize(out.stale))
        return "; ".join(parts)

    def set_channel(self, bus: int, board: int, chip: str, channel: int,
                    on: bool) -> tuple[bool, str]:
        """Power one channel on/off (:meth:`_write_masks` on one bit).

        The write itself doesn't depend on the read decode (the states
        string goes straight to the chip register), so if the byte
        framing assumed by ``pdbmap.decode_out_bytes`` were ever wrong,
        the first toggle would fail its verify loudly rather than flip
        the wrong channel.
        """
        if not self.configured:
            return False, "pdb is not configured"
        if not valid_channel_address(board, chip, channel):
            return False, f"invalid channel address: board {board} " \
                          f"chip {chip} ch{channel}"
        label = f"bus {bus} board {board} chip {chip} ch{channel}"
        state_word = "on" if on else "off"
        out = self._write_masks({(bus, chip_index(board, chip)): 1 << channel}, on)
        if out.error:
            return False, f"{label}: {out.error}"
        if out.missing:
            return False, f"{label}: not present on the controller"
        w = out.plan[0]
        if not w.needed:
            return True, f"{label} was already {state_word}"
        if out.failures:
            return False, f"{label}: write failed {out.failures[0][out.failures[0].rindex('('):]}"
        if out.verify_error:
            return False, f"{label}: {out.verify_error}"
        if out.stale:
            got = out.readback[bus][w.chip_num]
            return False, (
                f"verify failed on {label}: OUT reads {got:#04x}, expected "
                f"{w.wanted:#04x} — state changed underneath us (another "
                f"writer?); check the grid and retry")
        logger.info(f"pdb: {label} -> {state_word}")
        return True, f"{label} {state_word}"

    def set_channels(self, targets: list[tuple[int, int, str, int]],
                     on: bool, scope: str) -> tuple[bool, str]:
        """Power an arbitrary set of channels, one OUT-byte write per chip.

        For scopes the hardware does not have — a row of dishes in one
        polarization, whose channels are scattered over the boards — the
        caller resolves addresses through the channel map (its allowlist)
        and this folds them into a mask per chip.  *scope* names the set
        for the messages and the audit line.
        """
        if not self.configured:
            return False, "pdb is not configured"
        masks: dict[tuple[int, int], int] = {}   # (bus, chip_num) -> bits
        for bus, board, chip, channel in targets:
            if not valid_channel_address(board, chip, channel):
                return False, (f"{scope}: invalid channel address: board "
                               f"{board} chip {chip} ch{channel}")
            key = (bus, chip_index(board, chip))
            masks[key] = masks.get(key, 0) | (1 << channel)
        if not masks:
            return False, f"{scope}: no channels to write"
        n = sum(bin(m).count("1") for m in masks.values())
        state_word = "on" if on else "off"

        logger.warning(f"pdb: {scope} -> {n} channels {state_word}")
        out = self._write_masks(masks, on)
        if out.error:
            return False, f"{scope}: {out.error}"
        if out.missing:
            return False, (f"{scope}: not present on the controller: "
                           + _summarize(out.missing))
        if not out.todo:
            return True, f"{scope}: all {n} channels were already {state_word}"
        if out.verify_error:
            return False, (
                f"{scope}: wrote {len(out.todo) - len(out.failures)} of "
                f"{len(out.todo)} chips but the verify read failed "
                f"({out.verify_error}) — check the grid")
        if out.failures or out.stale:
            return False, f"{scope}: {self._problems(out)} — check the grid"
        return True, f"{scope}: {n} channels {state_word}"

    def set_group(self, bus: int, on: bool, board: int | None = None,
                  chip: str | None = None) -> tuple[bool, str]:
        """Power every channel of a chip, a board, or a whole bus.

        The scope widens as arguments are dropped: ``chip`` given ->
        that chip's 8 channels; only ``board`` -> both of its chips;
        neither -> every chip on ``bus``.  A chip is an all-ones or
        all-zeros OUT byte: the same read-modify-write as
        :meth:`set_channel` with the "modify" being the whole byte.
        """
        if not self.configured:
            return False, "pdb is not configured"
        if chip is not None and chip not in ("A", "B"):
            return False, f"invalid chip: {chip}"
        if board is not None and board < 0:
            return False, f"invalid board: {board}"
        if chip is not None and board is None:
            return False, "a chip scope needs a board"

        if chip is not None:
            scope = f"bus {bus} board {board} chip {chip}"
        elif board is not None:
            scope = f"bus {bus} board {board}"
        else:
            scope = f"bus {bus} (all boards)"
        state_word = "on" if on else "off"

        # which chips the scope names is only known from a read
        try:
            states = self._fetch_states()
        except _PDB_ERRORS as e:
            return False, f"{scope}: {type(e).__name__}: {e}"
        outs = states.get(bus)
        if outs is None:
            return False, f"{scope}: bus not present on the controller"
        masks = {}
        for chip_num in range(len(outs)):
            b, c = board_chip(chip_num)
            if (board is None or b == board) and (chip is None or c == chip):
                masks[(bus, chip_num)] = 0xFF
        if not masks:
            return False, f"{scope}: no such chips on the controller"

        logger.warning(f"pdb: {scope} -> all {state_word}")
        out = self._write_masks(masks, on, states=states, with_bus=False)
        n_scope = len(out.plan) * 8
        if not out.todo:
            return True, f"{scope}: all {n_scope} channels were already {state_word}"
        if out.verify_error:
            return False, (
                f"{scope}: wrote {len(out.todo) - len(out.failures)} of "
                f"{len(out.todo)} chips but the verify read failed "
                f"({out.verify_error}) — check the grid")
        if out.failures or out.stale:
            return False, f"{scope}: turning all {state_word} — " + self._problems(out)
        return True, (f"{scope}: {len(out.todo) * 8} channels {state_word} "
                      f"({n_scope} in scope)")

    def to_dict(self) -> dict:
        return {
            "configured": self.configured,
            "host": self.host,
            "port": self.port,
            "health": self.health,
            "buses": sorted(self.channels),
            "n_channels": self.n_channels,
            "n_on": self.n_on,
            "last_polled": self.last_polled,
            "last_seen": self.last_seen,
            "error": self.error,
        }
