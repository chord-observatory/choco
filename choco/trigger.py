"""Baseband dump triggers from outside telescopes (no Flask).

An outside telescope that sees a burst asks for a baseband dump with
kotekan's own ``POST /baseband`` body.  The request reaches choco through
the site proxy, which has already checked the sender's address and token
and names the sender in a header.  This module is choco's part: who may
call (:func:`authorized`), what a request must look like
(:func:`parse_request`), and the record of what was asked
(:class:`TriggerLog`).  The route (``web.trigger_baseband``) forwards an
accepted request to the running nodes.

The body is parsed strictly and rebuilt from the parsed values; the
sender's bytes are never forwarded.  ``file_path`` is always choco's: a
sender's value is checked and then replaced, since kotekan joins it into
the path it writes.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import math
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

#: Sent by the proxy with every trigger; the value is
#: ``baseband_trigger.secret``.  Not the proxy sign-in secret: this one
#: opens the trigger route and nothing else.
SECRET_HEADER = "X-Choco-Trigger-Secret"
#: Set by the proxy to the sender it authenticated (one of ``senders``).
SENDER_HEADER = "X-Choco-Trigger-Sender"

#: Larger bodies are refused unread.  kotekan's example is ~250 bytes.
MAX_BODY_BYTES = 2048

FIELDS = ("event_id", "start_unix_seconds", "start_unix_nano", "duration_nano",
          "dm", "dm_error", "file_path")

DEFAULTS = {
    "peer": None,
    "secret": None,
    "senders": [],
    "groups": ["cx"],           # node groups a trigger is forwarded to
    "max_duration_s": 10.0,
    "max_dm": 10000.0,
    "max_dm_error": 500.0,
    "max_age_s": 120.0,         # how far in the past the start may be
    "max_ahead_s": 10.0,        # ... and in the future (clock error)
    "node_timeout_s": 2.0,
}

_MIN_SECRET_LEN = 32
_NAME_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
_PATH_RE = re.compile(r"[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*")
_MAX_PATH_LEN = 128
_UINT64_MAX = 2 ** 64 - 1


class Refused(ValueError):
    """A request choco does not forward.  ``reason`` is a short fixed
    word for the record and the counter; the message is for the sender."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


def parse_config(raw, proxy_secret: str | None = None) -> dict | None:
    """Validate the ``baseband_trigger`` block, or None when it is absent
    or names no peer (the default: the route answers 404).  Raises
    ``ValueError`` naming the problem, so a typo is a startup error."""
    if not raw or not raw.get("peer"):
        return None
    unknown = set(raw) - set(DEFAULTS)
    if unknown:
        raise ValueError(f"baseband_trigger: unknown key(s) {', '.join(sorted(map(str, unknown)))}")
    cfg = {**DEFAULTS, **raw}
    try:
        peer = ipaddress.ip_address(str(cfg["peer"]).strip())
    except ValueError:
        raise ValueError("baseband_trigger.peer must be the proxy's IP address "
                         f"as choco sees it, got {cfg['peer']!r}") from None
    if peer.is_loopback:
        raise ValueError("baseband_trigger.peer may not be loopback")
    secret = str(cfg["secret"] or "")
    if len(secret) < _MIN_SECRET_LEN:
        raise ValueError(f"baseband_trigger.secret must be at least {_MIN_SECRET_LEN} characters")
    if proxy_secret and hmac.compare_digest(secret, proxy_secret):
        raise ValueError("baseband_trigger.secret must differ from server.proxy_auth.secret: "
                         "that one signs people in")
    for key in ("senders", "groups"):
        names = cfg[key]
        if (not isinstance(names, list) or not names
                or not all(isinstance(n, str) and _NAME_RE.fullmatch(n) for n in names)):
            raise ValueError(f"baseband_trigger.{key} must be a list of names "
                             f"(lower case, digits, - and _), got {names!r}")
    out = {"peer": peer, "secret": secret,
           "senders": frozenset(cfg["senders"]), "groups": frozenset(cfg["groups"])}
    for key in ("max_duration_s", "max_dm", "max_dm_error", "max_age_s",
                "max_ahead_s", "node_timeout_s"):
        value = cfg[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) \
                or not math.isfinite(value) or value <= 0:
            raise ValueError(f"baseband_trigger.{key} must be a positive number, got {value!r}")
        out[key] = float(value)
    return out


def authorized(cfg: dict, remote_addr: str | None, supplied: str | None) -> bool:
    """Whether a request comes from the proxy: its address *and* the
    trigger secret."""
    try:
        peer = ipaddress.ip_address(remote_addr or "")
    except ValueError:
        return False
    return peer == cfg["peer"] and bool(supplied) and hmac.compare_digest(
        supplied.encode("latin-1", "replace"), cfg["secret"].encode())


def _no_duplicates(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"duplicate key {key!r}")
        out[key] = value
    return out


def _no_constants(name):
    raise ValueError(f"{name} is not a number")


def _integer(body: dict, key: str, low: int, high: int) -> int:
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise Refused("malformed", f"{key} must be an integer")
    if not low <= value <= high:
        raise Refused("out_of_range", f"{key} must be between {low} and {high}")
    return value


def _number(body: dict, key: str, low: float, high: float) -> float:
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Refused("malformed", f"{key} must be a number")
    try:
        value = float(value)
    except OverflowError:
        value = math.inf
    if not low <= value <= high:        # also refuses an infinity
        raise Refused("out_of_range", f"{key} must be between {low:g} and {high:g}")
    return value


def parse_request(data: bytes, cfg: dict, sender: str, now: float) -> dict:
    """The request to forward, rebuilt from *data*, or :class:`Refused`.

    Exactly kotekan's seven fields, each of its type and inside the
    configured limits; the start inside the window around *now* (kotekan
    reads a negative start as "now", which a relayed trigger never
    means).  ``file_path`` becomes ``<sender>/YYYY/MM/DD/event_<id>``
    from the start time.
    """
    if sender not in cfg["senders"]:
        raise Refused("unknown_sender", "unknown sender")
    if len(data) > MAX_BODY_BYTES:
        raise Refused("malformed", "body too large")
    try:
        body = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicates,
                          parse_constant=_no_constants)
    except (ValueError, RecursionError) as e:
        raise Refused("malformed", f"not valid JSON: {e}") from None
    if not isinstance(body, dict) or set(body) != set(FIELDS):
        raise Refused("malformed", f"the body must be an object with exactly: {', '.join(FIELDS)}")

    event_id = _integer(body, "event_id", 1, _UINT64_MAX)
    seconds = _integer(body, "start_unix_seconds", 0, 2 ** 62)
    nano = _integer(body, "start_unix_nano", 0, 999_999_999)
    duration = _integer(body, "duration_nano", 1, int(cfg["max_duration_s"] * 1e9))
    dm = _number(body, "dm", 0.0, cfg["max_dm"])
    dm_error = _number(body, "dm_error", 0.0, cfg["max_dm_error"])
    path = body["file_path"]
    if not isinstance(path, str) or len(path) > _MAX_PATH_LEN or not _PATH_RE.fullmatch(path):
        raise Refused("malformed", "file_path must be plain names (letters, digits, - and _) "
                                   "separated by /")

    start = seconds + nano / 1e9
    if not now - cfg["max_age_s"] <= start <= now + cfg["max_ahead_s"]:
        raise Refused("outside_window",
                      f"start is {now - start:+.1f} s before now; accepted from "
                      f"{cfg['max_age_s']:g} s in the past to {cfg['max_ahead_s']:g} s ahead")
    day = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y/%m/%d")
    return {"event_id": event_id, "start_unix_seconds": seconds, "start_unix_nano": nano,
            "duration_nano": duration, "dm": dm, "dm_error": dm_error,
            "file_path": f"{sender}/{day}/event_{event_id}"}


class TriggerLog:
    """The record of every trigger: one JSON object per line, appended.

    It is also the memory of which ``(sender, event_id)`` were forwarded,
    read back at start so a restart does not let a repeat through, and
    the counts ``/metrics`` shows (since this process started).
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.counts: dict[str, int] = {}
        self._forwarded: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        try:
            with open(self.path) as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if rec.get("result") == "forwarded":
                        self._forwarded.add((rec.get("sender"), rec.get("event_id")))
        except OSError:
            pass

    def claim(self, sender: str, event_id: int) -> bool:
        """Mark ``(sender, event_id)`` as forwarded; False when it already
        was.  Claimed before the nodes are asked, so a repeat that arrives
        while the first is still going out is refused too."""
        with self._lock:
            if (sender, event_id) in self._forwarded:
                return False
            self._forwarded.add((sender, event_id))
            return True

    def record(self, result: str, **fields) -> None:
        """Append one line and count *result* (``forwarded`` or a
        :class:`Refused` reason).  A record that cannot be written is
        logged, never raised: the trigger matters more than its line."""
        rec = {"time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
               "result": result, **fields}
        with self._lock:
            self.counts[result] = self.counts.get(result, 0) + 1
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.path, "a") as f:
                    f.write(json.dumps(rec, sort_keys=True) + "\n")
            except OSError as e:
                logger.error(f"baseband trigger record not written to {self.path}: {e}")
