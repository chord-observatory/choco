"""The master PDB channel map — dish input <-> power board/chip/channel.

The wiring between the analog power distribution boards (PDB, driven by
power_db) and the correlator's dish inputs lives in one CSV next to
``nodes.yaml``.  It is the *master* table: choco labels the
``/service/pdb`` grid from it, serves it at ``/api/pdb/map`` so jobs
share one copy instead of vendoring their own, and cross-checks it
against the labels kotekan actually reports in its ``dish_inputs``
table — the same table kotekan indexes its bad-input mask with, so a
disagreement means a flag (or a power cut) would land on the wrong feed.

The file is read on demand and reloaded whenever its mtime changes (the
same "edit the file, no restart" rule as the kotekan configs).  Rows are
validated individually: a malformed row is collected as an error and
skipped, never raised — a typo in the wiring table must not take the
page down, and the rows that *are* readable stay useful.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from .dishlabels import (
    PLACEHOLDER_LABEL, expand_dish_labels, find_dish_inputs,
    labels_are_per_element,
)

logger = logging.getLogger(__name__)

DEFAULT_MAP_FILENAME = "pdb_map.csv"

# The dish-input column has gone by a few names (bffs's vendored
# power_map.csv calls it correlator_input); accept them all, write the
# first.
_LABEL_COLUMNS = ("dish_input", "correlator_input", "label")
_ADDRESS_COLUMNS = ("spi_bus", "board", "chip", "channel")


@dataclass(frozen=True)
class PdbMapEntry:
    """One row: a power channel and the dish input it feeds."""

    bus: int
    board: int
    chip: str
    channel: int
    dish_input: str
    amplifier: str = ""
    notes: str = ""

    @property
    def address(self) -> tuple[int, int, str, int]:
        return (self.bus, self.board, self.chip, self.channel)

    @property
    def address_label(self) -> str:
        return (f"bus {self.bus} board {self.board} chip {self.chip} "
                f"ch{self.channel}")

    def to_dict(self) -> dict:
        return {
            "spi_bus": self.bus,
            "board": self.board,
            "chip": self.chip,
            "channel": self.channel,
            "dish_input": self.dish_input,
            # bffs's CSV loader keys on correlator_input; emit both so
            # either name works for a consumer of /api/pdb/map.
            "correlator_input": self.dish_input,
            "amplifier": self.amplifier,
            "notes": self.notes,
        }


class PdbMap:
    """A parsed master map: entries by address, plus any parse errors."""

    def __init__(self, path=None, entries=(), errors=(), mtime=None):
        self.path = str(path) if path else None
        self.entries: dict[tuple, PdbMapEntry] = {
            e.address: e for e in entries}
        self.errors: list[str] = list(errors)
        self.mtime = mtime

    @property
    def n_entries(self) -> int:
        return len(self.entries)

    def entry(self, bus, board, chip, channel) -> PdbMapEntry | None:
        """The row for one channel address, or None if unmapped.

        Called once per grid cell, so it stays a plain dict lookup, and
        it tolerates junk arguments rather than raising mid-render.
        """
        try:
            key = (int(bus), int(board), str(chip), int(channel))
        except (TypeError, ValueError):
            return None
        return self.entries.get(key)

    def label(self, bus, board, chip, channel) -> str | None:
        """The dish input fed by one channel, or None if unmapped."""
        entry = self.entry(bus, board, chip, channel)
        return entry.dish_input if entry is not None else None

    def to_list(self) -> list[dict]:
        return [e.to_dict() for e in sorted(self.entries.values(),
                                            key=lambda e: e.address)]


def load_pdb_map(path) -> PdbMap:
    """Parse the master map CSV at *path*.  Never raises.

    An unreadable file yields an empty map carrying the reason in
    ``errors`` — the PDB page then renders an unlabelled grid rather
    than a 500.
    """
    p = Path(path)
    try:
        mtime = p.stat().st_mtime
        text = p.read_text()
    except OSError as e:
        return PdbMap(path=p, errors=[f"{type(e).__name__}: {e}"])
    entries, errors = _parse(text)
    return PdbMap(path=p, entries=entries, errors=errors, mtime=mtime)


def _parse(text: str) -> tuple[list[PdbMapEntry], list[str]]:
    """Rows and errors from the CSV body.

    Blank lines and ``#`` comments are stripped first: the table is
    maintained by hand and wants section headings.  Line numbers in
    errors refer to the original file so they can be jumped to.
    """
    numbered = [(i, ln) for i, ln in enumerate(text.splitlines(), start=1)
                if ln.strip() and not ln.lstrip().startswith("#")]
    if not numbered:
        return [], ["file is empty"]
    linenos = [i for i, _ in numbered]
    reader = csv.DictReader([ln for _, ln in numbered])
    fields = [(f or "").strip() for f in (reader.fieldnames or [])]
    missing = [c for c in _ADDRESS_COLUMNS if c not in fields]
    label_col = next((c for c in _LABEL_COLUMNS if c in fields), None)
    if missing or label_col is None:
        want = ", ".join(_ADDRESS_COLUMNS + (_LABEL_COLUMNS[0],))
        return [], [f"header must have columns: {want} "
                    f"(found: {', '.join(fields) or 'nothing'})"]

    entries: list[PdbMapEntry] = []
    errors: list[str] = []
    seen: dict[tuple, int] = {}
    # linenos[0] is the header row; the data rows pair with the rest.
    for lineno, row in zip(linenos[1:], reader):
        clean = {(k or "").strip(): (v or "").strip()
                 for k, v in row.items() if isinstance(k, str)}
        try:
            bus = int(clean["spi_bus"])
            board = int(clean["board"])
            chip = clean["chip"].upper()
            channel = int(clean["channel"])
        except (KeyError, ValueError, AttributeError):
            errors.append(f"line {lineno}: unparseable channel address")
            continue
        if bus < 0 or board < 0 or chip not in ("A", "B") \
                or not 0 <= channel < 8:
            errors.append(
                f"line {lineno}: address out of range (bus {bus} board "
                f"{board} chip {chip} ch{channel}); chip must be A/B and "
                f"channel 0-7")
            continue
        dish_input = clean.get(label_col, "")
        if not dish_input:
            errors.append(f"line {lineno}: no {label_col} value")
            continue
        address = (bus, board, chip, channel)
        if address in seen:
            errors.append(
                f"line {lineno}: duplicate address bus {bus} board {board} "
                f"chip {chip} ch{channel} (first on line {seen[address]}); "
                f"keeping the first")
            continue
        seen[address] = lineno
        entries.append(PdbMapEntry(
            bus=bus, board=board, chip=chip, channel=channel,
            dish_input=dish_input,
            amplifier=clean.get("amplifier", ""),
            notes=clean.get("notes", ""),
        ))
    return entries, errors


class PdbMapFile:
    """Lazily loaded, mtime-refreshed view of the master map file.

    Held on ``app.config`` and asked for the current map on each render,
    so editing the CSV takes effect without restarting choco.
    """

    def __init__(self, path):
        self.path = Path(path)
        self._map: PdbMap | None = None
        self._stamp = None

    def get(self) -> PdbMap:
        try:
            st = self.path.stat()
            stamp = (st.st_mtime, st.st_size)
        except OSError:
            stamp = None
        if self._map is None or stamp != self._stamp:
            self._map = load_pdb_map(self.path)
            self._stamp = stamp
            if self._map.errors:
                logger.warning("pdb map %s: %s", self.path,
                               "; ".join(self._map.errors[:5]))
            else:
                logger.info("pdb map %s: %d channels mapped",
                            self.path, self._map.n_entries)
        return self._map


# --- cross-check against kotekan's own dish_inputs table -----------------

def kotekan_dish_labels(config) -> dict | None:
    """Per-element label sets from a per-dish ``dish_inputs`` table.

    Returns ``{"live", "known"}``, each a set of per-element labels
    (dish label + X/Y): ``live`` covers the connected dishes (``type:
    ArrayDish``) — feeds that must have a known breaker — while
    ``known`` covers every real-labeled entry, connected or not, so a
    map row for a dish that exists but is not on the correlator yet
    (the pathfinder's C/D rows) is not flagged as stale.  Placeholder
    entries (label ``Fake``) are dropped from both.  Only the names
    matter here: the cross-check compares *which* feeds exist, not
    where they sit on the element axis (that indexing is bffs's
    problem, and bffs reads the table itself).

    Returns ``None`` when the config has no usable table.  Raises
    ``ValueError`` for a pre-2026-08 per-element table (labels like
    ``A1X``): its element ordering was wrong, so the check reports it
    as unavailable with a migrate-this-config reason instead of
    checking against untrustworthy data.
    """
    entries = [(str(e.get("label", "")).strip(), str(e.get("type", "")).strip())
               for e in (find_dish_inputs(config) or [])
               if isinstance(e, dict)]
    entries = [(label, typ) for label, typ in entries
               if label and label != PLACEHOLDER_LABEL]
    if not entries:
        return None
    if labels_are_per_element(label for label, _ in entries):
        raise ValueError(
            "pre-2026-08 per-element dish_inputs table (labels like A1X) — "
            "its element ordering is untrustworthy; migrate the config to "
            "the per-dish layout")

    return {
        "live": set(expand_dish_labels(l for l, typ in entries if typ == "ArrayDish")),
        "known": set(expand_dish_labels(l for l, _ in entries)),
    }


def cross_check(pdb_map: PdbMap, label_sets: dict) -> dict:
    """Compare the master map against a config's dish-input label sets.

    *label_sets* is :func:`kotekan_dish_labels` output.  The two sides
    deliberately use different sets:
      * ``missing_in_map``     — a **live** (ArrayDish) feed the map
        doesn't place, so nobody can find its breaker.  Dishes that are
        not on the correlator don't count: an unbuilt row is not a
        missing breaker.
      * ``unknown_to_kotekan`` — the map names a position kotekan has
        never heard of under any type; a stale row, or a typo that
        hides a real channel.  Wiring recorded for a dish that exists
        but is not connected yet is legitimate, not stale.
      * ``duplicate_labels``   — one dish input claimed by two channels;
        at most one can be right.
    """
    by_label: dict[str, list[PdbMapEntry]] = {}
    for entry in pdb_map.entries.values():
        by_label.setdefault(entry.dish_input, []).append(entry)
    duplicates = {
        label: sorted(e.address_label for e in entries)
        for label, entries in by_label.items() if len(entries) > 1
    }
    live = {str(label) for label in (label_sets or {}).get("live", ())}
    known = {str(label) for label in (label_sets or {}).get("known", ())} | live
    mapped = set(by_label)
    missing = sorted(live - mapped)
    unknown = sorted(mapped - known)
    return {
        "n_kotekan": len(live),
        "n_mapped": len(mapped),
        "n_matched": len(live & mapped),
        "missing_in_map": missing,
        "unknown_to_kotekan": unknown,
        "duplicate_labels": duplicates,
        "ok": bool(known) and not missing and not unknown
        and not duplicates,
    }
