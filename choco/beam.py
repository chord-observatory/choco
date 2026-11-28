"""The beam page's data: the incoherent beam's live waterfall and the
recent events (no Flask).

The waterfall is kotekan's: ``IncoherentBeamSearch`` writes the samples
of the last seconds, as the search saw them after its masks and binned
for a display, to its ``waterfall_buf`` once per search pass.  The
buffer is peek-held, so reading it never holds the search, and every
frame carries the whole image with the FPGA sequence number of its
first row.  A reader that asks more often than the image is long
therefore sees every row once: rows are numbered by their sequence
number and the browser asks for the rows after the last one it drew.

The events are two files: the search's candidate file and choco's own
record of the baseband dump triggers it relayed.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from datetime import datetime

from .acqtree import COARSE_CHANNEL_MHZ

#: One FPGA frame (a step of ``fpga_seq``): 16384 samples at 3200 MHz.
FPGA_SEQ_S = 16384 / 3200e6

#: The stage's defaults (kotekan ``lib/stages/IncoherentBeamSearch.cpp``).
DEFAULT_FREQ_BIN = 8

#: A peeked frame is reused for this long: the stage writes one every
#: search pass (0.34 s), so every viewer shares one read per pass.
FRAME_TTL_S = 0.25
#: The most bytes of a frame asked of kotekan, which sends 64 KiB when not
#: told: above the 20 s image (0.4 MB) with room for other bin sizes.
MAX_FRAME_BYTES = 1 << 22

#: The candidate file is read again at most this often, and this much of
#: its end: about half an hour of lines at the 2026-11 rate.
EVENTS_TTL_S = 4.0
CANDIDATE_TAIL_BYTES = 1 << 20
TRIGGER_TAIL_BYTES = 1 << 18

#: The waterfall value of a bin with nothing in use.
NO_DATA = 0x80


def search_stage(config) -> dict | None:
    """The first ``IncoherentBeamSearch`` block of *config*, or None."""
    if not isinstance(config, dict):
        return None
    if config.get("kotekan_stage") == "IncoherentBeamSearch":
        return config
    for value in config.values():
        found = search_stage(value)
        if found is not None:
            return found
    return None


def find_search(nodes):
    """``(node, stage)`` of the node that runs the search, preferring one
    whose stage writes a waterfall; ``(None, None)`` without one."""
    first = (None, None)
    for node in nodes:
        stage = search_stage(node.desired_config)
        if stage is None:
            continue
        if isinstance(stage.get("waterfall_buf"), str):
            return node, stage
        if first[0] is None:
            first = (node, stage)
    return first


def _positive_int(value, default: int | None = None) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


def freq_axis(stage: dict) -> dict | None:
    """Where the waterfall's columns are: ``low_mhz`` (the lower edge of
    column 0) and ``column_mhz``, from the stage's block.  None for a
    block without usable numbers."""
    base = stage.get("freq_id_base")
    freq_bin = _positive_int(stage.get("waterfall_freq_bin"), DEFAULT_FREQ_BIN)
    if isinstance(base, bool) or not isinstance(base, int) or base < 0 or freq_bin is None:
        return None
    return {"low_mhz": (base - 0.5) * COARSE_CHANNEL_MHZ,
            "column_mhz": freq_bin * COARSE_CHANNEL_MHZ}


def decode_waterfall(frame: dict) -> dict | None:
    """A peeked ``waterfall_buf`` frame as ``{first, step, rows, columns,
    data}``, or None when the reply is not one.

    ``first`` is the number of the image's first row: its FPGA sequence
    number over ``step``, the sequence numbers per row.  Row numbers
    rise by one per row for as long as kotekan runs, and across a
    restart of the search.
    """
    desc, meta = frame.get("frame_desc"), frame.get("metadata")
    if not isinstance(desc, dict) or not isinstance(meta, dict):
        return None
    extents = desc.get("extents")
    if (desc.get("value_type") != "int8" or not isinstance(extents, list) or len(extents) != 2
            or _positive_int(extents[0]) is None or _positive_int(extents[1]) is None):
        return None
    rows, columns = extents
    seq, step = meta.get("FPGA_SEQ_NUM"), _positive_int(meta.get("TIME_DOWNSAMPLING_FPGA"))
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0 or step is None:
        return None
    if frame.get("encoding") != "base64" or not isinstance(frame.get("data"), str):
        return None
    try:
        data = base64.b64decode(frame["data"], validate=True)
    except ValueError:
        return None
    if len(data) != rows * columns:
        return None
    return {"first": seq // step, "step": step, "rows": rows, "columns": columns, "data": data}


def rows_after(image: dict, after: int | None) -> tuple[int, bytes]:
    """The image's rows numbered above *after* (all of them for None):
    the number of the first one returned and their bytes."""
    skip = 0 if after is None else min(max(after + 1 - image["first"], 0), image["rows"])
    return image["first"] + skip, image["data"][skip * image["columns"]:]


class FrameCache:
    """The newest decoded waterfall frame, read from kotekan at most once
    per ``FRAME_TTL_S`` however many browsers ask."""

    def __init__(self):
        self._lock = threading.Lock()
        self._key = None
        self._at = 0.0
        self._value: dict | None = None

    def get(self, node, buffer_name: str) -> dict:
        """``{"image": ...}`` or ``{"error": text, "status": code}``."""
        with self._lock:
            key = (node.key, buffer_name)
            now = time.monotonic()
            if self._value is not None and self._key == key and now - self._at < FRAME_TTL_S:
                return self._value
            frame = node.get_buffer_frame(buffer_name, MAX_FRAME_BYTES)
            if frame is None:
                value = {"error": "kotekan unreachable", "status": 502}
            elif frame.get("error"):
                value = {"error": frame["error"], "status": 404}
            else:
                image = decode_waterfall(frame)
                value = ({"image": image} if image is not None
                         else {"error": "unexpected kotekan reply: not a waterfall frame",
                               "status": 502})
            self._key, self._at, self._value = key, time.monotonic(), value
            return value


def _tail_records(path, max_bytes: int) -> list[dict]:
    """The JSON objects on the last lines of *path*, oldest first: at most
    *max_bytes* of its end, without the line the cut falls in.  Blocking."""
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        f.seek(max(size - max_bytes, 0))
        lines = f.read().split(b"\n")
    if size > max_bytes:
        lines = lines[1:]
    records = []
    for line in lines:
        try:
            rec = json.loads(line)
        except ValueError:          # the line being written, or damage
            continue
        if isinstance(rec, dict):
            records.append(rec)
    return records


def _number(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def read_candidates(path, limit: int) -> dict:
    """The newest events of the search's candidate file, newest first,
    and the file's newest time stamp.  Blocking (the file is on NFS):
    call it through ``datafiles.run_blocking``.

    The file lists every candidate from the reporting cut, about one a
    few seconds of thermal noise.  An event here is a candidate that
    passed the intensity dump cut (its line has ``cutout``) or has no
    flag.  ``clock`` is ``(unix_time, fpga_seq)`` of the newest line that
    has both, which dates the waterfall's rows.
    """
    events, clock = [], None
    for rec in reversed(_tail_records(path, CANDIDATE_TAIL_BYTES)):
        unix, seq = _number(rec.get("unix_time")), rec.get("fpga_seq")
        if clock is None and unix is not None and isinstance(seq, int):
            clock = (unix, seq)
        flags = rec.get("flags")
        flags = [f for f in flags if isinstance(f, str)] if isinstance(flags, list) else []
        dump = rec.get("cutout")
        if len(events) >= limit or (flags and not isinstance(dump, int)):
            if len(events) >= limit and clock is not None:
                break
            continue
        events.append({
            "unix": unix,
            "snr": _number(rec.get("snr")),
            "dm": _number(rec.get("dm")),
            "width": _number(rec.get("width")),
            "flags": flags,
            "dump": dump if isinstance(dump, int) and not isinstance(dump, bool) else None,
        })
    return {"events": events, "clock": clock}


def read_triggers(path, limit: int) -> list[dict]:
    """The newest lines of choco's baseband trigger record
    (``trigger.TriggerLog``), newest first.  ``dumps`` counts the nodes
    that answered with files, of the ``asked`` it went to."""
    try:
        records = _tail_records(path, TRIGGER_TAIL_BYTES)
    except OSError:
        return []
    out = []
    for rec in reversed(records[-limit:]):
        try:
            unix = datetime.fromisoformat(rec.get("time")).timestamp()
        except (TypeError, ValueError):
            unix = None
        request, nodes = rec.get("request"), rec.get("nodes")
        event_id = rec.get("event_id")
        out.append({
            "unix": unix,
            "sender": rec.get("sender") if isinstance(rec.get("sender"), str) else None,
            "event_id": event_id if isinstance(event_id, int) else None,
            "result": rec.get("result") if isinstance(rec.get("result"), str) else "?",
            "dm": _number(request.get("dm")) if isinstance(request, dict) else None,
            "dumps": (sum(1 for reply in nodes.values() if reply)
                      if isinstance(nodes, dict) else None),
            "asked": len(nodes) if isinstance(nodes, dict) else None,
        })
    return out


class EventsCache:
    """The candidate file's newest events, read at most once per
    ``EVENTS_TTL_S``.  ``read`` is the blocking reader, already wrapped
    for the threadpool by the caller."""

    def __init__(self):
        self._lock = threading.Lock()
        self._key = None
        self._at = 0.0
        self._value: dict = {"events": [], "clock": None, "error": None}

    def get(self, path, read) -> dict:
        with self._lock:
            now = time.monotonic()
            if self._key == path and now - self._at < EVENTS_TTL_S:
                return self._value
            try:
                value = dict(read(path), error=None)
            except OSError as e:
                value = {"events": [], "clock": self._value["clock"] if self._key == path else None,
                         "error": str(e)}
            self._key, self._at, self._value = path, time.monotonic(), value
            return value

    def clock(self, path):
        """The last ``clock`` read for *path*, without reading."""
        with self._lock:
            return self._value["clock"] if self._key == path else None


def row_unix(clock, image: dict, row: int) -> float | None:
    """The UTC (unix seconds) at which row number *row* starts, from a
    candidate's ``clock``; None without one."""
    if clock is None:
        return None
    unix, seq = clock
    return unix + (row * image["step"] - seq) * FPGA_SEQ_S
