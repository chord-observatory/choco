"""FPGA-master poller and job-status helpers.

choco exposes service-status badges in the page header:

* **FPGA** — derived from a slow background poll of the fpga_master
  daemon's ``/status`` and ``/get-frame0-time`` HTTP endpoints.
* **Jobs** (EOP broadcast, bffs, ...) — derived on demand by
  :func:`job_status` from the job's systemd unit (``systemctl show``)
  and/or the mtime of the job's state file.

Neither poller blocks the request path; the FPGA poller runs in its own
gevent greenlet and a job query is one ``systemctl`` subprocess plus one
``stat()`` call.
"""

import json
import logging
import shutil
import subprocess
import time
from pathlib import Path

import gevent
import requests

logger = logging.getLogger(__name__)


_RETRY_DELAY_S = 1.0


def _get_with_retry(url: str, timeout: float) -> requests.Response:
    """GET with one quick retry.

    The monitors poll every 30s, so a single dropped or slow request
    would otherwise show a service as down for a whole interval — one
    retry turns a transient blip back into a non-event while a real
    outage still fails within ~2*timeout.
    """
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.RequestException:
        gevent.sleep(_RETRY_DELAY_S)
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp


def _wtl_result(resp: requests.Response) -> tuple[bool, str]:
    """Interpret a wtl.rest control response.

    fpga_master's server (wtl.rest) reports handler exceptions as HTTP
    200 with an ``{"error": ...}`` JSON body — a status-code check alone
    reads a crashed stop as success.  Returns ``(ok, message)``.
    """
    try:
        data = resp.json()
    except ValueError:
        return True, resp.text[:300]
    if isinstance(data, dict) and "error" in data:
        return False, str(data["error"])[:300]
    return True, str(data)[:300]


# --- FPGA master --------------------------------------------------------

class FpgaMonitor:
    """Background poller for the fpga_master daemon.

    Hits ``/status`` and ``/get-frame0-time`` on a slow interval.  The
    latest result is held in attributes and surfaced to the web UI; no
    callback / event mechanism is needed because the page polls a
    partial template on its own cadence.

    Health levels:
      * ``ok``        — both endpoints responded; ``frame0_nano`` parsed.
      * ``no_timing`` — /status responded but /get-frame0-time did not.
      * ``down``      — /status did not respond (or fpga_master is not configured).

    Also carries the daemon's own run state (``state`` / ``start_result``
    from ``/status``) and thin ``start_master`` / ``stop_master`` control
    wrappers for the ``/service/fpga`` page.
    """

    POLL_INTERVAL_S = 30
    # /stop awaits the full F-engine shutdown before responding — call
    # stop_master from a greenlet, never a request handler.
    STOP_TIMEOUT_S = 300

    def __init__(self, host: str | None, port: int | None,
                 timeout: float = 5.0):
        self.host = host
        self.port = int(port) if port else None
        self.timeout = float(timeout)
        # Unconfigured monitors never poll, so reflect that up front
        # instead of sitting on "unknown" forever.
        self.health: str = "unknown" if self.configured else "unconfigured"
        self.state: str | None = None      # fpga_master's own state string
        self.start_result = None           # last /start outcome, if reported
        self.frame0_ns: int | None = None
        self.last_polled: float | None = None
        self.last_seen: float | None = None
        self.error: str | None = None
        # Recent control actions (newest first, ephemeral) — the page's
        # visible trail of who started/stopped what and how it went.
        self.actions: list[dict] = []
        self._running = False

    @property
    def configured(self) -> bool:
        return bool(self.host) and bool(self.port)

    @property
    def base_url(self) -> str | None:
        if not self.configured:
            return None
        return f"http://{self.host}:{self.port}"

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "fpga_master.host / port not set"
            return

        # /status: reachability + the daemon's own run state
        try:
            resp = _get_with_retry(f"{self.base_url}/status", self.timeout)
            try:
                data = resp.json()
            except ValueError:
                data = {}
            if not isinstance(data, dict):
                data = {}
            self.state = data.get("state")
            self.start_result = data.get("start_result") or None
        except requests.RequestException as e:
            self.health = "down"
            self.state = None
            err = f"{type(e).__name__}: {e}"
            # A failed background /start makes /status itself error; the
            # response body then carries the start exception.
            body = getattr(getattr(e, "response", None), "text", "") or ""
            if body:
                err += f" — {body[:300]}"
            self.error = err
            return

        # /get-frame0-time: parse {frame0_nano, ...}
        try:
            resp = _get_with_retry(f"{self.base_url}/get-frame0-time",
                                   self.timeout)
            data = resp.json()
            self.frame0_ns = int(data["frame0_nano"])
            self.health = "ok"
            self.error = None
            self.last_seen = self.last_polled
        except (requests.RequestException, ValueError, KeyError, TypeError) as e:
            self.health = "no_timing"
            self.error = f"/get-frame0-time: {type(e).__name__}: {e}"

    def poll_if_stale(self, max_age_s: float = 10.0) -> None:
        """Poll now if the last poll is older than ``max_age_s``.

        Lets the /service/fpga page tighten the effective cadence while
        an operator is actually watching, without touching the 30s loop.
        """
        if not self.configured:
            return
        if (self.last_polled is None
                or time.time() - self.last_polled > max_age_s):
            self.poll_once()

    MAX_ACTIONS = 10

    def record_action(self, action: str, user: str, ok: bool | None,
                      message: str) -> None:
        """Prepend one control-action entry to the visible trail.

        ``ok=None`` marks an action still in flight (an async stop);
        its completion is recorded as a second entry.  The durable
        audit trail is choco's own journal — this list is ephemeral
        display state, like everything else on the monitor.
        """
        self.actions.insert(0, {
            "time": time.time(),
            "action": action,
            "user": user,
            "ok": ok,
            "message": message,
        })
        del self.actions[self.MAX_ACTIONS:]

    def start_master(self) -> tuple[bool, str]:
        """POST ``/start`` with no config overrides.

        fpga_master reuses the config it was launched with, guards
        against double-starts ("already started"), and initializes in
        the background — completion or failure shows up in ``/status``
        via the poller.  Returns ``(ok, message)``.
        """
        if not self.configured:
            return False, "fpga_master is not configured"
        try:
            resp = requests.post(f"{self.base_url}/start", json={},
                                 timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            return False, f"{type(e).__name__}: {e}"
        ok, message = _wtl_result(resp)
        self.poll_once()
        return ok, message

    def stop_master(self) -> tuple[bool, str]:
        """GET ``/stop`` and wait for the shutdown to complete.

        GET, not POST: wtl.rest registers argument-less endpoints for
        GET only (a POST gets 405 Method Not Allowed).  Blocks up to
        ``STOP_TIMEOUT_S`` — run from a greenlet.  The outcome is
        logged and the state re-polled either way, so the page reflects
        reality on its next refresh.
        """
        if not self.configured:
            return False, "fpga_master is not configured"
        try:
            resp = requests.get(f"{self.base_url}/stop",
                                timeout=self.STOP_TIMEOUT_S)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"fpga_master stop failed: {e}")
            self.poll_once()
            return False, f"{type(e).__name__}: {e}"
        ok, message = _wtl_result(resp)
        if ok:
            logger.info(f"fpga_master stop completed: {message}")
        else:
            logger.error(f"fpga_master stop failed remotely: {message}")
        self.poll_once()
        return ok, message

    def run(self) -> None:
        """Poll forever on ``POLL_INTERVAL_S``.  Designed for gevent.spawn."""
        self._running = True
        while self._running:
            try:
                self.poll_once()
            except Exception:
                logger.exception("FpgaMonitor.poll_once raised")
            gevent.sleep(self.POLL_INTERVAL_S)

    def stop(self) -> None:
        self._running = False

    def to_dict(self) -> dict:
        return {
            "configured": self.configured,
            "host": self.host,
            "port": self.port,
            "health": self.health,
            "state": self.state,
            "start_result": self.start_result,
            "frame0_ns": self.frame0_ns,
            "last_polled": self.last_polled,
            "last_seen": self.last_seen,
            "error": self.error,
        }


# --- PSU (power_db) -------------------------------------------------------

def decode_out_bytes(raw: list[int]) -> list[int]:
    """Per-chip OUT bytes from one bus's raw ``/channel_states`` buffer.

    Mirrors power_db's own daisy-chain decode (``spi_ops``; the same
    convention as ``jobs/bffs/sources/power.py``): reverse the flat byte
    list, then every even index is a chip's OUT byte — chip ``k`` of the
    chain, i.e. board ``k // 2``, chip ``'A'`` if ``k`` is even else
    ``'B'``.  Each OUT bit ``c`` (0..7) is channel ``c``; 1 = powered.

    Verified against the live controller (2026-07-17): writing
    ``00000001`` to board 0 chip A moves exactly the last raw byte from
    0 to 1, i.e. chip ``k``'s OUT byte is ``raw[2N-1-2k]``.
    """
    resp = list(raw)
    resp.reverse()
    return [resp[i] for i in range(0, len(resp), 2)]


class PsuMonitor:
    """Background poller for the power_db PSU controller.

    Hits ``/status`` and ``/channel_states`` on a slow interval and
    decodes the per-channel power state.  Same shape as
    :class:`FpgaMonitor`: latest result held in attributes, surfaced by
    the services strip and the ``/service/psu`` page.

    Health levels:
      * ``ok``        — both endpoints responded and decoded.
      * ``no_states`` — /status responded but /channel_states did not.
      * ``down``      — /status did not respond.
      * ``unconfigured`` — no psu.host / port in config.
    """

    POLL_INTERVAL_S = 30

    def __init__(self, host: str | None, port: int | None,
                 timeout: float = 5.0):
        self.host = host
        self.port = int(port) if port else None
        self.timeout = float(timeout)
        self.health: str = "unknown" if self.configured else "unconfigured"
        # {bus: [{"board": int, "chip": "A"|"B", "channels": [bool]*8}]}
        self.channels: dict[int, list[dict]] = {}
        self.boards: dict[int, int] = {}   # bus -> board count (from /status)
        self.last_polled: float | None = None
        self.last_seen: float | None = None
        self.error: str | None = None
        self._running = False

    @property
    def configured(self) -> bool:
        return bool(self.host) and bool(self.port)

    @property
    def base_url(self) -> str | None:
        if not self.configured:
            return None
        return f"http://{self.host}:{self.port}"

    @property
    def n_channels(self) -> int:
        return sum(8 * len(rows) for rows in self.channels.values())

    @property
    def n_on(self) -> int:
        return sum(sum(r["channels"]) for rows in self.channels.values()
                   for r in rows)

    def _fetch_states(self) -> dict[int, list[int]]:
        """GET /channel_states, decoded to per-chip OUT bytes per bus.

        Raises ValueError on an unexpected response shape so every
        caller's error path treats "garbage" the same as "unreachable".
        """
        resp = _get_with_retry(f"{self.base_url}/channel_states",
                               self.timeout)
        data = resp.json()
        states = data.get("channel_states") if isinstance(data, dict) else None
        if not isinstance(states, dict):
            raise ValueError(f"unexpected /channel_states shape: {data!r:.80}")
        return {int(bus): decode_out_bytes(raw)
                for bus, raw in states.items()}

    @staticmethod
    def _rows(out_bytes: list[int]) -> list[dict]:
        return [{"board": k // 2,
                 "chip": "A" if k % 2 == 0 else "B",
                 "channels": [bool(out & (1 << c)) for c in range(8)]}
                for k, out in enumerate(out_bytes)]

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "psu.host / port not set"
            return

        # /status: reachability + board counts
        try:
            resp = _get_with_retry(f"{self.base_url}/status", self.timeout)
            status = resp.json()
            buses = status.get("buses") if isinstance(status, dict) else None
            self.boards = {
                int(bus): int(info.get("board_count", 0))
                for bus, info in (buses or {}).items()
                if isinstance(info, dict)
            } if isinstance(buses, dict) else {}
        except (requests.RequestException, ValueError, TypeError) as e:
            self.health = "down"
            self.error = f"{type(e).__name__}: {e}"
            return

        # /channel_states: raw OUT-register bytes per bus
        try:
            states = self._fetch_states()
            self.channels = {bus: self._rows(outs)
                             for bus, outs in states.items()}
            self.health = "ok"
            self.error = None
            self.last_seen = self.last_polled
        except (requests.RequestException, ValueError, KeyError,
                TypeError) as e:
            self.health = "no_states"
            self.error = f"/channel_states: {type(e).__name__}: {e}"

    def set_channel(self, bus: int, board: int, chip: str, channel: int,
                    on: bool) -> tuple[bool, str]:
        """Power one channel on/off, read-modify-write on the chip's OUT byte.

        power_db has no per-channel endpoint — a chip's OUT register is
        written whole — and other writers exist (power_db's own CLI), so
        the current byte is read immediately before the write and the
        result is confirmed by a fresh read afterwards: a mismatch is
        reported, never silently retried.  The post-write read also
        refreshes the monitor's grid, so the page shows the new truth.

        The write itself doesn't depend on the read decode (the states
        string goes straight to the chip register), so if the byte
        framing assumed by :func:`decode_out_bytes` were ever wrong, the
        first toggle would fail its verify loudly rather than flip the
        wrong channel.
        """
        if not self.configured:
            return False, "psu is not configured"
        if chip not in ("A", "B") or not 0 <= channel < 8 or board < 0:
            return False, f"invalid channel address: board {board} " \
                          f"chip {chip} ch{channel}"
        chip_num = board * 2 + (0 if chip == "A" else 1)
        label = f"bus {bus} board {board} chip {chip} ch{channel}"
        try:
            outs = self._fetch_states().get(bus)
            if outs is None or chip_num >= len(outs):
                return False, f"{label}: not present on the controller"
            current = outs[chip_num]
            wanted = (current | (1 << channel) if on
                      else current & ~(1 << channel))
            if wanted != current:
                resp = requests.post(
                    f"{self.base_url}/write_command",
                    json={"spi_bus": bus, "board_idx": board,
                          "chip_letter": chip, "operation": "OUT",
                          "states": format(wanted, "08b")},
                    timeout=self.timeout)
                resp.raise_for_status()
            readback = self._fetch_states()
            self.channels = {b: self._rows(o) for b, o in readback.items()}
            self.last_seen = time.time()
            got = readback[bus][chip_num]
        except (requests.RequestException, ValueError, KeyError,
                TypeError, IndexError) as e:
            return False, f"{label}: {type(e).__name__}: {e}"
        if got != wanted:
            return False, (
                f"verify failed on {label}: OUT reads {got:#04x}, expected "
                f"{wanted:#04x} — state changed underneath us (another "
                f"writer?); check the grid and retry")
        if wanted == current:
            return True, f"{label} was already {'on' if on else 'off'}"
        logger.info(f"psu: {label} -> {'on' if on else 'off'}")
        return True, f"{label} {'on' if on else 'off'}"

    def run(self) -> None:
        """Poll forever on ``POLL_INTERVAL_S``.  Designed for gevent.spawn."""
        self._running = True
        while self._running:
            try:
                self.poll_once()
            except Exception:
                logger.exception("PsuMonitor.poll_once raised")
            gevent.sleep(self.POLL_INTERVAL_S)

    def stop(self) -> None:
        self._running = False

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


# --- Job status (EOP broadcast, bffs, ...) -------------------------------

# The handful of ``systemctl show`` properties consumed by job_status.
# ``Result`` is the only health signal; ``ExecMainExitTimestamp`` is used
# purely as an emptiness test ("has this unit ever run") — its value is
# never parsed.  The rest are carried into the result for tooltips.
_SYSTEMCTL_PROPS = (
    "Result",
    "ActiveState",
    "SubState",
    "ExecMainStatus",
    "ExecMainExitTimestamp",
)

# How old the EOP state file can be before we call it "stale".  The
# job runs daily, so anything more than a day plus a little slop is a
# signal that the last run didn't update the state.
EOP_STALE_AFTER_S = 25 * 3600


def _systemctl_show(unit: str, timeout: float = 5.0,
                    props: tuple[str, ...] = _SYSTEMCTL_PROPS,
                    ) -> dict[str, str] | None:
    """Return ``systemctl show <unit>`` properties as a dict, or None on failure."""
    if shutil.which("systemctl") is None:
        return None
    try:
        result = subprocess.run(
            ["systemctl", "show", unit,
             "--property=" + ",".join(props),
             "--no-pager"],
            capture_output=True, text=True, timeout=timeout,
        )
    except (subprocess.SubprocessError, OSError) as e:
        logger.debug(f"systemctl show {unit} failed: {e}")
        return None
    if result.returncode != 0:
        return None
    props: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            props[k] = v
    return props


def job_logs(service_unit: str, lines: int = 50,
             timeout: float = 5.0) -> list[str] | None:
    """Recent journal lines for a unit, newest last.

    Returns ``None`` when the journal can't be read (no journalctl on
    the host, or the call failed) — the caller distinguishes "no log
    access" from "unit has no entries" (an empty-ish but valid list).
    """
    if shutil.which("journalctl") is None:
        return None
    try:
        result = subprocess.run(
            ["journalctl", "-u", service_unit, "-n", str(int(lines)),
             "--no-pager", "-o", "short-iso"],
            capture_output=True, text=True, timeout=timeout,
        )
    except (subprocess.SubprocessError, OSError) as e:
        logger.debug(f"journalctl -u {service_unit} failed: {e}")
        return None
    if result.returncode != 0:
        logger.debug(f"journalctl -u {service_unit} rc={result.returncode}: "
                     f"{result.stderr.strip()}")
        return None
    return result.stdout.splitlines()


# Schedule facts for a job's timer.  The timestamp values are systemd's
# own human-readable strings — displayed as-is, never parsed.
_TIMER_PROPS = (
    "LoadState",
    "ActiveState",
    "NextElapseUSecRealtime",
    "LastTriggerUSec",
)


def timer_status(timer_unit: str) -> dict | None:
    """Schedule facts for a job's systemd timer, for display only.

    Returns None when the timer isn't known to systemd (no systemctl on
    the host, or the unit isn't loaded).
    """
    props = _systemctl_show(timer_unit, props=_TIMER_PROPS)
    if props is None or props.get("LoadState") != "loaded":
        return None
    return {
        "unit": timer_unit,
        "active_state": props.get("ActiveState") or None,
        "next_elapse": props.get("NextElapseUSecRealtime") or None,
        "last_trigger": props.get("LastTriggerUSec") or None,
    }


def read_state_json(path: Path | str | None) -> dict | None:
    """A job's JSON state file as a dict, or None (missing/unreadable/invalid).

    The service pages treat a missing state file the same as an empty
    one — the job may simply never have run on this host.
    """
    if not path:
        return None
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        logger.debug(f"state file {path}: {e}")
        return None
    return data if isinstance(data, dict) else None


def job_status(service_unit: str, state_file: Path | None = None,
               stale_after_s: float | None = None) -> dict:
    """Best-effort health snapshot of a oneshot job.

    Combines two cheap signals, either of which may be unavailable:

    * ``systemctl show <service_unit>`` — ``Result`` says whether the
      last run failed; an empty ``ExecMainExitTimestamp`` says the unit
      has never run.  No timestamp values are parsed.
    * ``state_file`` mtime — for a job that rewrites its state file on
      every successful run (EOP), the mtime is "last successful run"
      and ``stale_after_s`` turns an old mtime into ``stale`` health.
      Without ``stale_after_s`` the mtime is informational only (bffs
      rewrites state only when the bad-feed list *changes*, so its age
      says nothing about job health).

    Jobs share an exit-code convention: 0 = ok, **2 = degraded** (the
    job itself is fine but a dependency or input wasn't — fpga_master
    unreachable, stale data, choco down; retries self-heal), anything
    else = failed (config error or bug — needs a human).  A failing
    unit whose last exit status is 2 therefore reports ``degraded``
    rather than ``failed``.

    Returns a dict with ``health`` (``ok`` / ``degraded`` / ``stale`` /
    ``failed`` / ``never_run`` / ``unknown``), ``state_mtime`` (epoch or
    ``None``), and raw systemd fields for the tooltip.
    """
    now = time.time()
    props = _systemctl_show(service_unit)

    mtime = None
    if state_file is not None:
        try:
            mtime = state_file.stat().st_mtime
        except OSError:
            mtime = None

    result = (props or {}).get("Result", "").strip()
    ran = (props or {}).get("ExecMainExitTimestamp", "").strip() not in ("", "n/a")

    failed = props is not None and result not in ("", "n/a", "success")
    degraded = failed and (props or {}).get("ExecMainStatus", "").strip() == "2"
    stale = (stale_after_s is not None and mtime is not None
             and now - mtime > stale_after_s)
    ran_ok = props is not None and result == "success" and ran
    fresh = mtime is not None and stale_after_s is not None and not stale

    if degraded:
        health = "degraded"
    elif failed:
        health = "failed"
    elif stale:
        health = "stale"
    elif ran_ok or fresh:
        health = "ok"
    elif mtime is not None:
        health = "unknown"  # state exists, but nothing confirms success
    elif props is not None or state_file is not None:
        health = "never_run"
    else:
        health = "unknown"

    return {
        "health": health,
        "state_mtime": mtime,
        "result": result or None,
        "active_state": (props or {}).get("ActiveState") or None,
        "sub_state": (props or {}).get("SubState") or None,
        "exit_status": (props or {}).get("ExecMainStatus") or None,
        "systemd": props is not None,
        "unit": service_unit,
        "state_file": str(state_file) if state_file else None,
    }
