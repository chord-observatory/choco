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
    """

    POLL_INTERVAL_S = 30

    def __init__(self, host: str | None, port: int | None,
                 timeout: float = 5.0):
        self.host = host
        self.port = int(port) if port else None
        self.timeout = float(timeout)
        # Unconfigured monitors never poll, so reflect that up front
        # instead of sitting on "unknown" forever.
        self.health: str = "unknown" if self.configured else "unconfigured"
        self.frame0_ns: int | None = None
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

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "fpga_master.host / port not set"
            return

        # /status: cheap reachability check
        try:
            resp = requests.get(f"{self.base_url}/status",
                                timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            self.health = "down"
            self.error = f"{type(e).__name__}: {e}"
            return

        # /get-frame0-time: parse {frame0_nano, ...}
        try:
            resp = requests.get(f"{self.base_url}/get-frame0-time",
                                timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            self.frame0_ns = int(data["frame0_nano"])
            self.health = "ok"
            self.error = None
            self.last_seen = self.last_polled
        except (requests.RequestException, ValueError, KeyError, TypeError) as e:
            self.health = "no_timing"
            self.error = f"/get-frame0-time: {type(e).__name__}: {e}"

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

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "psu.host / port not set"
            return

        # /status: reachability + board counts
        try:
            resp = requests.get(f"{self.base_url}/status",
                                timeout=self.timeout)
            resp.raise_for_status()
            status = resp.json()
            self.boards = {
                int(bus): int((info or {}).get("board_count", 0))
                for bus, info in (status.get("buses") or {}).items()
            }
        except (requests.RequestException, ValueError, TypeError) as e:
            self.health = "down"
            self.error = f"{type(e).__name__}: {e}"
            return

        # /channel_states: raw OUT-register bytes per bus
        try:
            resp = requests.get(f"{self.base_url}/channel_states",
                                timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            channels: dict[int, list[dict]] = {}
            for bus_str, raw in (data["channel_states"] or {}).items():
                rows = []
                for k, out in enumerate(decode_out_bytes(raw)):
                    rows.append({
                        "board": k // 2,
                        "chip": "A" if k % 2 == 0 else "B",
                        "channels": [bool(out & (1 << c)) for c in range(8)],
                    })
                channels[int(bus_str)] = rows
            self.channels = channels
            self.health = "ok"
            self.error = None
            self.last_seen = self.last_polled
        except (requests.RequestException, ValueError, KeyError,
                TypeError) as e:
            self.health = "no_states"
            self.error = f"/channel_states: {type(e).__name__}: {e}"

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

    Returns a dict with ``health`` (``ok`` / ``stale`` / ``failed`` /
    ``never_run`` / ``unknown``), ``state_mtime`` (epoch or ``None``),
    and raw systemd fields for the tooltip.
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
    stale = (stale_after_s is not None and mtime is not None
             and now - mtime > stale_after_s)
    ran_ok = props is not None and result == "success" and ran
    fresh = mtime is not None and stale_after_s is not None and not stale

    if failed:
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
