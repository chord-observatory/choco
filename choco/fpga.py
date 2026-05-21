"""FPGA-master poller and EOP-job status helpers.

choco exposes two service-status badges in the page header:

* **FPGA** — derived from a slow background poll of the fpga_master
  daemon's ``/status`` and ``/get-frame0-time`` HTTP endpoints.
* **EOP** — derived on demand from the ``choco-eop-broadcast.service``
  systemd unit (queried via ``systemctl show``), falling back to the
  mtime of the EOP state file if systemd is unavailable.

Neither poller blocks the request path; the FPGA poller runs in its own
gevent greenlet and the EOP query is a single ``systemctl`` subprocess
(or one ``stat()`` call in the fallback path).
"""

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
        self.health: str = "unknown"
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


# --- EOP broadcast service ---------------------------------------------

# A oneshot service's last-run state is exposed by ``systemctl show``.
# We only consume a handful of properties; the rest are ignored.
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
_EOP_STALE_AFTER_S = 25 * 3600


def _systemctl_show(unit: str, timeout: float = 5.0) -> dict[str, str] | None:
    """Return ``systemctl show <unit>`` properties as a dict, or None on failure."""
    if shutil.which("systemctl") is None:
        return None
    try:
        result = subprocess.run(
            ["systemctl", "show", unit,
             "--property=" + ",".join(_SYSTEMCTL_PROPS),
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


def _parse_systemd_timestamp(raw: str) -> float | None:
    """Convert systemd's ExecMainExitTimestamp string to a Unix epoch float.

    The format is locale-dependent text like ``Mon 2026-05-19 16:29:18 UTC``;
    rather than parsing it we use ``systemctl show ... --property=
    ExecMainExitTimestampMonotonic`` style timestamps when available.
    Empty / "n/a" inputs return None.
    """
    raw = (raw or "").strip()
    if not raw or raw == "n/a":
        return None
    # systemd accepts and emits Unix timestamps for *Timestamp* with
    # ``--property=…TimestampMonotonic``, but we ask for the plain
    # variant which is human-readable.  Try a handful of common formats.
    import datetime
    for fmt in (
        "%a %Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S %Z",
        "%a %Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.datetime.strptime(raw, fmt)
            return dt.timestamp()
        except ValueError:
            continue
    return None


def eop_status(state_file: Path | None = None,
               service_unit: str = "choco-eop-broadcast.service") -> dict:
    """Best-effort health snapshot of the EOP broadcast job.

    Prefers ``systemctl show`` against the configured unit.  Falls back
    to the mtime of ``state_file`` when systemd is unreachable (e.g.,
    dev runs outside the unit).

    Returns a dict with: ``health`` (one of ``ok``, ``stale``,
    ``failed``, ``never_run``, ``unknown``), ``last_run_at`` (epoch or
    ``None``), ``source`` (``systemd`` / ``mtime`` / ``none``), plus
    raw details for the tooltip.
    """
    now = time.time()
    props = _systemctl_show(service_unit)
    if props is not None:
        result = props.get("Result", "")
        last_run = _parse_systemd_timestamp(
            props.get("ExecMainExitTimestamp", "")
        )
        if result == "" or result == "n/a":
            health = "unknown"
        elif result != "success":
            health = "failed"
        elif last_run is None:
            health = "never_run"
        elif now - last_run > _EOP_STALE_AFTER_S:
            health = "stale"
        else:
            health = "ok"
        return {
            "health": health,
            "last_run_at": last_run,
            "result": result or None,
            "active_state": props.get("ActiveState") or None,
            "sub_state": props.get("SubState") or None,
            "exit_status": props.get("ExecMainStatus") or None,
            "source": "systemd",
            "unit": service_unit,
        }

    # systemctl unavailable — fall back to state-file mtime.
    if state_file is None:
        return {"health": "unknown", "source": "none"}
    try:
        mtime = state_file.stat().st_mtime
    except FileNotFoundError:
        return {"health": "never_run", "source": "mtime",
                "state_file": str(state_file)}
    except OSError as e:
        return {"health": "unknown", "source": "mtime",
                "error": f"stat: {e}"}
    health = "stale" if now - mtime > _EOP_STALE_AFTER_S else "ok"
    return {
        "health": health,
        "last_run_at": mtime,
        "source": "mtime",
        "state_file": str(state_file),
    }
