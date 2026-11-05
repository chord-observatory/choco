"""The fleet's updatable config, endpoint by endpoint (no Flask).

For each endpoint, what choco means every node to run (its desired value:
the block its file renders with the node's stored override merged over
it, as ``Node.desired_config`` merges them; in development only the
stored values, over the running config's blocks, as
``Node.desired_updatable`` has it) and whether that node's kotekan runs
it, from the worker's last read of the live ``/config``
(``NodeWorker._note_live``).  The X-engine page shows one row per endpoint.
"""

import json
import time
from dataclasses import dataclass

from .state import Mode, NodeStatus, fits_block
from .sync import LIVE_READ_MAINTENANCE_S

#: A live read older than this is not shown.  The worker reads every poll
#: outside maintenance and every LIVE_READ_MAINTENANCE_S in it, so an
#: older read means it stopped reading (no config to compare, a stopped
#: node, a load error).
LIVE_MAX_AGE_S = 3 * LIVE_READ_MAINTENANCE_S

#: A list longer than this is summarised by its length.
_SHORT_LIST = 4

# A cell's state -> its tag's tone.
TONES = {"live": "ok", "differs": "warn", "absent": "warn", "unread": "off", "extra": "info"}


@dataclass
class Cell:
    """One node's part of a row: the tag under "Live"."""
    name: str
    state: str           # a TONES key
    note: str            # the tooltip

    @property
    def tone(self) -> str:
        return TONES[self.state]


@dataclass
class Row:
    endpoint: str
    switch: bool
    owner: str | None              # the job that pushes it, if any
    values: list[tuple[str, str, list[str]]]  # (summary, full JSON, nodes), most nodes first
    overrides: list[str]           # nodes whose stored value stands over the library's
    rendered_on: int               # how many nodes render it
    cells: list[Cell]


def is_switch(values) -> bool:
    """An on/off block: a bool ``enabled``, and ``valid_at_time_ns`` at most."""
    return (isinstance(values, dict) and isinstance(values.get("enabled"), bool)
            and set(values) <= {"enabled", "valid_at_time_ns"})


def switch_values(block: dict, on: bool) -> dict:
    """The update that sets a switch *block* on or off now: ``enabled``,
    and ``valid_at_time_ns: 0`` (at once) where the block has it."""
    values = {"enabled": on}
    if "valid_at_time_ns" in block:
        values["valid_at_time_ns"] = 0
    return values


def summarize(values: dict) -> str:
    """One short line for a block: a switch as on/off, any other field as
    ``key: value`` with a long list as its length."""
    if is_switch(values):
        word = "on" if values["enabled"] else "off"
        at = values.get("valid_at_time_ns", 0)
        return word if not at else f"{word}, valid_at_time_ns {at}"
    parts = []
    for key, value in values.items():
        if isinstance(value, list) and len(value) > _SHORT_LIST:
            text = f"[{len(value)} items]"
        elif isinstance(value, str):
            text = value
        else:
            text = json.dumps(value)
        parts.append(f"{key}: {text}")
    return ", ".join(parts)


def desired_blocks(node) -> dict[str, dict]:
    """The node's desired value of every block its file renders, the
    stored value merged over it; in development only the stored values
    the running config can take (``Node.desired_updatable``)."""
    if node.mode is Mode.DEVELOPMENT:
        return node.desired_updatable()
    stored = node.updatable_config or {}
    return {endpoint: {**values, **stored[endpoint]} if endpoint in stored else values
            for endpoint, values in node.updatable_base().items()}


def endpoint_block(node, endpoint: str) -> dict | None:
    """The block a new value at *endpoint* would merge over on *node*
    (``Node.updatable_base``); None when the node has no such endpoint."""
    return node.updatable_base().get(endpoint)


def _live_blocks(node, now: float) -> dict[str, dict] | None:
    """The live blocks the worker last read, or None when there is no
    current read of a running kotekan to show."""
    if (node.status != NodeStatus.STARTED or node.live_updatable is None
            or node.live_read_at is None or now - node.live_read_at > LIVE_MAX_AGE_S):
        return None
    return node.live_updatable


def _compact(values) -> str:
    return json.dumps(values, separators=(",", ": "))


def _cell(node, endpoint: str, desired: dict | None,
          live: dict[str, dict] | None, now: float) -> Cell | None:
    name = node.name
    if live is None:
        if desired is None:
            return None
        if node.status != NodeStatus.STARTED:
            why = f"kotekan is {node.status.value}"
        elif node.live_read_at is None:
            why = "not read yet"
        elif node.live_updatable is None:
            why = "the last read of /config failed"
        else:
            why = f"last read {now - node.live_read_at:.0f} s ago"
        return Cell(name, "unread", f"{name}: no live value ({why})")
    age = f"read {now - node.live_read_at:.0f} s ago"
    held = " Maintenance: choco pushes nothing to it." if node.maintenance else ""
    value = live.get(endpoint)
    if desired is None:
        if value is None:
            return None
        stored = (node.updatable_config or {}).get(endpoint)
        if (node.mode is Mode.DEVELOPMENT and stored is not None
                and not fits_block(stored, value)):
            # desired_updatable left the stored value out: say why rather
            # than show the endpoint as merely extra.
            return Cell(name, "differs",
                        f"{name} runs {_compact(value)} ({age}). The stored value "
                        f"{_compact(stored)} does not fit the running block (a "
                        f"field it lacks, or of another type), so it is not pushed.")
        why = ("choco does not set it on a node in development"
               if node.mode is Mode.DEVELOPMENT else "its config file does not render")
        return Cell(name, "extra", f"{name} runs {_compact(value)}, which {why} ({age}).")
    if value is None:
        return Cell(name, "absent", f"{name}: the running config has no "
                                    f"/{endpoint} ({age}).{held}")
    if value == desired:
        return Cell(name, "live", f"{name} runs the desired value ({age}).")
    return Cell(name, "differs", f"{name} runs {_compact(value)} ({age}).{held}")


def fleet_rows(nodes, owners: dict[str, str], now: float | None = None) -> list[Row]:
    """One row per endpoint that any node renders or runs, in config
    order (the nodes' order, then the endpoints only a live config has).

    *nodes* in the registry's order; *owners* maps an endpoint to the
    job that pushes it.
    """
    now = time.time() if now is None else now
    live = {node.key: _live_blocks(node, now) for node in nodes}
    # A development node's desired set follows its last read, current or
    # not, so a stale read shows its endpoints as unread rather than as none.
    desired = {node.key: desired_blocks(node) for node in nodes}
    endpoints = list(dict.fromkeys(
        [e for node in nodes for e in desired[node.key]]
        + [e for node in nodes for e in (live[node.key] or {})]))
    rows = []
    for endpoint in endpoints:
        groups: dict[str, list[str]] = {}
        texts: dict[str, dict] = {}
        overrides = []
        cells = []
        for node in nodes:
            value = desired[node.key].get(endpoint)
            if value is not None:
                key = json.dumps(value, sort_keys=True)
                groups.setdefault(key, []).append(node.name)
                texts[key] = value
                if endpoint in (node.updatable_config or {}):
                    overrides.append(node.name)
            cell = _cell(node, endpoint, value, live[node.key], now)
            if cell is not None:
                cells.append(cell)
        values = [(summarize(texts[key]), _compact(texts[key]), names)
                  for key, names in sorted(groups.items(), key=lambda kv: -len(kv[1]))]
        rows.append(Row(
            endpoint=endpoint,
            switch=bool(texts) and all(is_switch(v) for v in texts.values()),
            owner=owners.get(endpoint),
            values=values,
            overrides=overrides,
            rendered_on=sum(len(names) for names in groups.values()),
            cells=cells,
        ))
    return rows
