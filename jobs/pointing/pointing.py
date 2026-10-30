#!/usr/bin/env python3
"""Dish pointing from the site's angle table into the config library.

Fetches the per-foundation antenna angles the site publishes as CSV, takes
the value the array dishes share, and writes it as ``dish_coelev_deg`` in
the library's ``vars.yaml``, the Jinja context every node renders with.
kotekan's ``telescope.j2`` reads it there; a change is a config change, so
the nodes that render it restart onto it when the operator lifts the
maintenance this job puts them in.

The CSV is untrusted input.  The fetch is bounded (``max_bytes``), the
text must be ASCII, every field must match a fixed pattern, and the 64
pathfinder foundations must each appear once.  Nothing read from the CSV
is written anywhere: ``vars.yaml`` gets a float Python formats from the
parsed value, and the history gets floats and the labels that matched.
Anything short of a fully valid table changes nothing and exits degraded,
so the last applied value stands until a valid table arrives.
"""
import argparse
import csv
import io
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.request

import yaml

from choco.jobclient import (STATE_NAME, Degraded, failure, get_json, job_state_dir,
                             put_json, setup_logging, single_run, write_json_atomic,
                             write_run)

log = logging.getLogger("pointing")

DEFAULTS = {
    "choco_url": "https://localhost:5000",
    "url": "http://px.chord-observatory.ca/chord-angle-csv",
    "timeout_s": 30,
    "max_bytes": 65536,
    "min_agree": 56,
    "pause": True,
}
#: The exact header the site publishes; any other table is refused.
HEADER = ["Foundation", "Antenna angle", "N / S", "Ant angle", "Antenna ID"]
#: The pathfinder's 64 foundations, in kotekan's dish-table spelling.
FOUNDATIONS = [f"{row}{col}" for row in "ABCDEFGH" for col in range(1, 9)]
_FOUNDATION_RE = re.compile(r"^[A-J][1-9][0-9]?$")
_ANGLE_RE = re.compile(r"^[0-9]{1,2}\.[0-9]{4}$")
_SIGNED_RE = re.compile(r"^-?[0-9]{1,2}\.[0-9]{4}$")
_ID_RE = re.compile(r"^[0-9]{3}$")
#: What vars.yaml carries; the key telescope.j2 reads.
VAR = "dish_coelev_deg"
#: The head of vars.yaml, put there on the first write and kept since: what
#: the file is for and that this key is not for hand edits.
VARS_HEADER = """\
# vars.yaml: the Jinja context every node's config renders with.  Each
# top-level key is a variable the library's templates can use as
# {{ name }}.  Edit it by hand for a value every node shares and no job
# keeps.  A change re-renders every node, and the sync loop restarts a
# node whose rendered config changed.
#
# dish_coelev_deg is written by the pointing job (choco-pointing.timer)
# from the site's angle table.  Do not edit it by hand: the next run puts
# the table's value back.
"""
#: Every fetch, one line each; state.json is the latest table and changes
#: only when the table does, so its mtime is "last change".
HISTORY_NAME = "history.jsonl"


class InvalidTable(Degraded):
    """The fetched text is not the table this job knows."""


def fetch(url: str, timeout_s: float, max_bytes: int) -> str:
    """The CSV text, read at most *max_bytes* and decoded as ASCII.

    ``OSError`` when the site cannot be reached; :class:`InvalidTable`
    when the reply is too long or not ASCII.
    """
    # The site answers 406 to urllib's default User-Agent.
    req = urllib.request.Request(url, headers={"User-Agent": "choco-pointing/1"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise InvalidTable(f"reply longer than {max_bytes} bytes")
    try:
        return raw.decode("ascii")
    except UnicodeDecodeError as e:
        raise InvalidTable(f"reply is not ASCII: {e}") from None


def parse(text: str) -> dict[str, float]:
    """``{foundation: signed angle}`` for every row of a valid table.

    Every field must match its pattern and the row must be consistent:
    the signed angle is the unsigned one, negative when ``N / S`` is
    ``S`` and positive otherwise (the site leaves the side blank for
    north and for zero).  The site's signed field carries a line break
    after the sign, which the CSV reader keeps inside the quotes; it is
    removed before the match.
    """
    try:
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except csv.Error as e:
        raise InvalidTable(f"not CSV: {e}") from None
    if not rows or rows[0] != HEADER:
        raise InvalidTable(f"unexpected header: {rows[0] if rows else 'empty'}")
    table: dict[str, float] = {}
    for n, row in enumerate(rows[1:], 2):
        if len(row) != len(HEADER):
            raise InvalidTable(f"line {n}: {len(row)} fields")
        foundation, angle, ns, signed, ident = row
        signed = signed.replace("\n", "")
        if not (_FOUNDATION_RE.match(foundation) and _ANGLE_RE.match(angle)
                and ns in ("N", "S", "") and _SIGNED_RE.match(signed)
                and _ID_RE.match(ident)):
            raise InvalidTable(f"line {n}: field outside the known patterns")
        value = float(signed)
        expect = -float(angle) if ns == "S" else float(angle)
        if value != expect or abs(value) > 90.0:
            raise InvalidTable(f"line {n}: {foundation} angle {signed} does not "
                               f"agree with {angle} {ns or 'no'} side")
        if foundation in table:
            raise InvalidTable(f"line {n}: {foundation} listed twice")
        table[foundation] = value
    missing = [f for f in FOUNDATIONS if f not in table]
    if missing:
        raise InvalidTable(f"foundations missing: {', '.join(missing)}")
    return {f: table[f] for f in FOUNDATIONS}


def array_value(table: dict[str, float], min_agree: int) -> tuple[float | None, list[str]]:
    """The angle at least *min_agree* of the 64 dishes share, and the
    dishes that differ from it.  ``None`` when no value has that many:
    a table read while the dishes move, or one half the array disagrees
    with, applies nothing."""
    counts: dict[float, int] = {}
    for v in table.values():
        counts[v] = counts.get(v, 0) + 1
    value, n = max(counts.items(), key=lambda kv: kv[1])
    if n < min_agree:
        return None, sorted(table)
    return value, sorted(f for f, v in table.items() if v != value)


def current_value(choco_url: str, timeout: float) -> float | None:
    """``dish_coelev_deg`` from the library's vars.yaml, or None when the
    file or the key is absent.  Raises ``OSError`` when choco is down and
    ``ValueError`` when the file is not the mapping choco requires."""
    try:
        entry = get_json(choco_url, "/api/configs/vars.yaml", timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    data = yaml.safe_load(entry["content"]) or {}
    if not isinstance(data, dict):
        raise ValueError("vars.yaml is not a mapping")
    value = data.get(VAR)
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"vars.yaml {VAR} is not a number: {value!r}")
    return float(value)


def updated_text(text: str, value: float) -> str:
    """vars.yaml's text with ``dish_coelev_deg`` set to *value*: the key's
    line replaced, or appended when absent, and the header put at the top
    when it is not there.  Every other byte is kept, so hand-written keys
    and comments survive.  ``ValueError`` when the result is not a mapping
    that carries the value (a block value under the key, say)."""
    line = f"{VAR}: {value}"
    if re.search(rf"^{VAR}:", text, re.M):
        new = re.sub(rf"^{VAR}:[^\n]*$", line, text, count=1, flags=re.M)
    else:
        new = (text if text.endswith("\n") or not text else text + "\n") + line + "\n"
    if not new.startswith("# vars.yaml:"):
        new = VARS_HEADER + "\n" + new
    try:
        data = yaml.safe_load(new)
    except yaml.YAMLError as e:
        raise ValueError(f"vars.yaml would not parse after the edit: {e}") from None
    if not isinstance(data, dict) or data.get(VAR) != value:
        raise ValueError(f"vars.yaml would not carry {VAR} = {value} after the edit")
    return new


def write_value(choco_url: str, value: float, pause: bool, timeout: float) -> dict:
    """Set ``dish_coelev_deg`` in vars.yaml through the library API, keeping
    the rest of the file.  With *pause*, the nodes that render it are put
    in maintenance first, as a pull does, so the change restarts nothing
    until the operator lifts it.  Returns choco's reply."""
    text = ""
    try:
        text = get_json(choco_url, "/api/configs/vars.yaml", timeout=timeout)["content"]
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    return put_json(choco_url, "/api/configs/vars.yaml",
                    {"content": updated_text(text, value), "pause": pause},
                    timeout=timeout)


def load_config(path) -> dict:
    cfg = dict(DEFAULTS)
    if path:
        with open(path) as f:
            loaded = yaml.safe_load(f) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"{path}: top level must be a mapping")
        cfg.update({k: v for k, v in loaded.items() if v is not None})
    for key in ("timeout_s", "max_bytes", "min_agree"):
        if not isinstance(cfg[key], (int, float)) or cfg[key] <= 0:
            raise ValueError(f"{key} must be a positive number")
    if not 1 <= cfg["min_agree"] <= len(FOUNDATIONS):
        raise ValueError(f"min_agree must be 1..{len(FOUNDATIONS)}")
    return cfg


def run(cfg: dict, state_dir, dry_run: bool = False) -> tuple[int, dict]:
    """One run: ``(exit code, facts for run.json)``."""
    text = fetch(cfg["url"], cfg["timeout_s"], cfg["max_bytes"])
    table = parse(text)
    value, outliers = array_value(table, cfg["min_agree"])
    facts = {"url": cfg["url"], "array_value": value, "outliers": outliers}
    if value is None:
        raise Degraded(f"no angle shared by {cfg['min_agree']} dishes; "
                       f"{len(set(table.values()))} distinct values")
    current = current_value(cfg["choco_url"], cfg["timeout_s"])
    facts["previous"] = current
    changed = current != value
    facts["changed"] = changed
    if outliers:
        log.warning("%d dishes differ from the array value %.4f: %s",
                    len(outliers), value, " ".join(outliers))
    if not changed:
        log.info("%s = %.4f, unchanged", VAR, value)
    elif dry_run:
        log.warning("%s %s -> %.4f (dry run, not written)", VAR, current, value)
    else:
        reply = write_value(cfg["choco_url"], value, cfg["pause"], cfg["timeout_s"])
        facts["reloaded"] = reply.get("reloaded", [])
        facts["paused"] = reply.get("paused", [])
        log.warning("%s %s -> %.4f written to vars.yaml; nodes re-rendered: %s; "
                    "put in maintenance: %s", VAR, current, value,
                    ", ".join(facts["reloaded"]) or "none",
                    ", ".join(facts["paused"]) or "none")
    if not dry_run:
        now = time.time()
        with open(state_dir / HISTORY_NAME, "a") as f:
            f.write(json.dumps({"time": now, "array_value": value,
                                "applied": changed, "table": table}) + "\n")
        state_file = state_dir / STATE_NAME
        try:
            previous_table = json.loads(state_file.read_text()).get("table")
        except (OSError, ValueError, AttributeError):
            previous_table = None
        if table != previous_table:
            write_json_atomic(state_file, {"time": now, "array_value": value,
                                           "outliers": outliers, "table": table})
    return 0, facts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=None, help="pointing.yaml")
    parser.add_argument("--state-dir", default=None,
                        help="history and run.json (default: systemd's "
                             "$STATE_DIRECTORY, else /var/lib/choco/pointing)")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="fetch and compare, write nothing")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)
    setup_logging(args.verbose, log)
    state_dir = job_state_dir("pointing", args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    try:
        cfg = load_config(args.config)
    except (ValueError, yaml.YAMLError) as e:
        rc, reason = failure(e, log)
        write_run(state_dir, rc, reason=reason)
        return rc

    with single_run(state_dir / "pointing.lock") as acquired:
        if not acquired:
            log.warning("another run holds the lock; nothing done")
            return 0
        try:
            rc, facts = run(cfg, state_dir, dry_run=args.dry_run)
        except (Degraded, OSError, ValueError, yaml.YAMLError) as e:
            rc, reason = failure(e, log)
            facts = {"reason": reason}
        if not args.dry_run:
            write_run(state_dir, rc, **facts)
    return rc


if __name__ == "__main__":
    sys.exit(main())
