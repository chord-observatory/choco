"""The F-engine service: the crate supply and fpga_master's supervisor.

``FengineMonitor`` polls f-engine-tools' ``f-engine-service`` on
crs-control, which holds the crate's power supply (a t0 PSU, through
``psucontrol``) and starts, stops and watches fpga_master.  choco reads
the supply, the supervisor's state, the boards the service hears on the
network, its event log and its gain archives from it, and switches the
supply through it; it never opens the supply itself.
"""

from __future__ import annotations

import time

import requests

from .fpga import _failure
from .http import fetch
from .monitor import HttpMonitor

#: The supply's readings, as its ``/psu`` JSON names them.
_PSU_NUMBERS = ("vin", "vout", "iout", "temp_inlet", "temp_oring", "temp_outlet", "fan_rpm")
#: What the supervisor says of fpga_master (``FpgaMasterManager.info``).
_MASTER_KEYS = ("state", "error", "pid", "config", "started_at", "restart_count", "power_cycle_count")


def parse_psu(data) -> dict | None:
    """``/psu/metrics`` (the supply's own JSON, passed through) as the
    numbers that parse, ``output_on``, ``faults`` and ``watts``; ``None``
    when it is not the supply's answer."""
    if not isinstance(data, dict) or "output_on" not in data:
        return None
    psu: dict = {"output_on": bool(data["output_on"])}
    for key in _PSU_NUMBERS:
        value = data.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            psu[key] = float(value)
    faults = data.get("faults")
    psu["faults"] = [str(f) for f in faults] if isinstance(faults, list) else []
    if "vout" in psu and "iout" in psu:
        psu["watts"] = psu["vout"] * psu["iout"]
    return psu


def parse_master(data) -> dict | None:
    """``/fpga-master`` as the supervisor's facts, or ``None``."""
    if not isinstance(data, dict) or "state" not in data:
        return None
    master = {key: data.get(key) for key in _MASTER_KEYS}
    watchdog = data.get("watchdog")
    watchdog = watchdog if isinstance(watchdog, dict) else {}
    master["watchdog"] = ("off" if not watchdog.get("enabled")
                          else "paused" if watchdog.get("paused") else "on")
    return master


def parse_boards(data) -> dict | None:
    """``/boards`` (the service's discover.py table: the boards whose boot
    beacons it hears) as its summary counts and, by crate slot (from 1),
    each board's serial, address, bootloader and beacon age."""
    if not isinstance(data, dict) or not isinstance(data.get("boards"), list):
        return None
    by_slot: dict[int, dict] = {}
    unplaced = 0
    for b in data["boards"]:
        if not isinstance(b, dict):
            continue
        slot = b.get("slot")
        if not isinstance(slot, int) or isinstance(slot, bool):
            unplaced += 1
            continue
        board = b.get("board") if isinstance(b.get("board"), dict) else {}
        loader = " ".join(str(b[k]) for k in ("bootloader", "bootloader_version") if b.get(k))
        by_slot[slot] = {"serial": board.get("serial"), "ip": b.get("ip"), "bootloader": loader,
                         "booted": bool(b.get("booted")), "stale": bool(b.get("stale")),
                         "age_s": b.get("age_s") if isinstance(b.get("age_s"), (int, float)) else None}
    summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
    return {"listening": bool(data.get("listening")), "error": data.get("error"),
            "n_boards": len(by_slot) + unplaced, "unplaced": unplaced,
            "n_booted": sum(b["booted"] and not b["stale"] for b in by_slot.values()),
            "n_stale": summary.get("n_stale") or 0, "by_slot": by_slot}


def parse_archives(data) -> dict | None:
    """``/gains/archives`` as the count and the newest file's run (its
    directory's name) and time."""
    if not isinstance(data, list):
        return None
    files = [f for f in data if isinstance(f, dict) and isinstance(f.get("filename"), str)]
    if not files:
        return {"count": 0, "newest": None, "newest_at": None}
    newest = max(files, key=lambda f: f.get("modified") or 0)
    parts = newest["filename"].rstrip("/").split("/")
    return {"count": len(files), "newest": parts[-2] if len(parts) > 1 else parts[-1],
            "newest_at": newest.get("modified")}


class FengineMonitor(HttpMonitor):
    """Background poller for the F-engine service.

    Health levels:
      * ``ok``     — the service answered and read the supply.
      * ``no_psu`` — the service answered but could not read the supply.
      * ``down``   — the service did not answer.

    The supply's output being off, or fpga_master stopped, is a fact the
    page shows, not the service's health.
    """

    PSU_ACTIONS = ("on", "off", "power-cycle", "clear-faults")
    # A power cycle answers after the off and boot delays (25 s as configured).
    PSU_ACTION_TIMEOUT_S = 180
    MAX_EVENTS = 15
    ARCHIVES_EVERY_S = 60

    def __init__(self, host: str | None, port: int | None, timeout: float = 5.0):
        super().__init__(host, port, timeout)
        self.master: dict | None = None
        self.psu: dict | None = None
        self.psu_error: str | None = None
        self.last_seen: float | None = None
        # The rest of what the service knows; each is None when its read
        # failed, and none of them changes the health.
        self.boards: dict | None = None
        self.events: list[dict] = []
        self.conflicts: list[dict] | None = None
        self.session: dict | None = None        # the open gain session, if any
        self.archives: dict | None = None
        self._archives_at = 0.0

    def _get(self, path: str, **kwargs) -> requests.Response:
        return fetch("GET", f"{self.base_url}{path}", timeout=self.timeout, retries=1, **kwargs)

    def poll_once(self) -> None:
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "fengine_service.host / port not set"
            return

        try:
            master = parse_master(self._get("/fpga-master").json())
            if master is None:
                raise ValueError("not the service's /fpga-master")
        except (OSError, ValueError) as e:
            self.health = "down"
            self.error = f"{type(e).__name__}: {e}"[:300]
            self.master, self.psu, self.psu_error = None, None, None
            self.boards, self.conflicts, self.session = None, None, None
            return
        self.master = master
        self.last_seen = self.last_polled
        self._poll_extras()

        # The service answers 502 with the reason when the supply does not.
        try:
            resp = self._get("/psu/metrics", accept_statuses=(502,))
            data = resp.json()
            if resp.status_code == 502:
                detail = data.get("detail") if isinstance(data, dict) else None
                raise ValueError(detail or "the supply did not answer")
            self.psu = parse_psu(data)
            if self.psu is None:
                raise ValueError("not the supply's status")
        except (OSError, ValueError) as e:
            self.psu = None
            self.psu_error = f"{e}"[:300]
            self.health = "no_psu"
            self.error = f"/psu/metrics: {self.psu_error}"
            return
        self.psu_error = None
        self.health = "ok"
        self.error = None

    def _poll_extras(self) -> None:
        """The boards, events, conflicts, gain session and archives."""
        try:
            self.boards = parse_boards(self._get("/boards").json())
        except (OSError, ValueError):
            self.boards = None
        try:
            events = self._get(f"/events?limit={self.MAX_EVENTS}").json()
            self.events = [e for e in events if isinstance(e, dict)][::-1]
        except (OSError, ValueError, TypeError):
            pass
        try:
            conflicts = self._get("/conflicts").json()
            self.conflicts = [c for c in conflicts if isinstance(c, dict)]
        except (OSError, ValueError, TypeError):
            self.conflicts = None
        # No session is a 404.
        try:
            resp = self._get("/gains/session", accept_statuses=(404,))
            data = resp.json()
            self.session = data if resp.status_code == 200 and isinstance(data, dict) else None
        except (OSError, ValueError):
            self.session = None
        # The archive list is a glob on the data disk and changes with a
        # gain session only.
        if self.last_polled - self._archives_at >= self.ARCHIVES_EVERY_S:
            try:
                self.archives = parse_archives(self._get("/gains/archives").json())
                self._archives_at = self.last_polled
            except (OSError, ValueError):
                self.archives = None

    def psu_action(self, action: str) -> tuple[bool, str]:
        """Switch the supply through the service: ``on``, ``off``,
        ``power-cycle`` or ``clear-faults``.  The service refuses ``off``
        and ``power-cycle`` (409, with the reason) while fpga_master runs
        or a gain session is open; choco never forces.  A power cycle
        answers after the boards' boot delay, so call it from a greenlet.
        Returns ``(ok, message)``."""
        if not self.configured:
            return False, "the F-engine service is not configured"
        if action not in self.PSU_ACTIONS:
            return False, f"no such supply action: {action}"
        try:
            resp = fetch("POST", f"{self.base_url}/psu/{action}", json={},
                         timeout=self.PSU_ACTION_TIMEOUT_S)
        except OSError as e:
            self.poll_once()
            return False, _failure(e)
        try:
            psu = parse_psu(resp.json())
        except ValueError:
            psu = None
        self.poll_once()
        if psu is None:
            return True, "done"
        return True, f"output {'on' if psu['output_on'] else 'off'}"

    def to_dict(self) -> dict:
        return {
            "boards": self.boards,
            "events": self.events,
            "conflicts": self.conflicts,
            "session": self.session,
            "archives": self.archives,
            "configured": self.configured,
            "host": self.host,
            "port": self.port,
            "health": self.health,
            "error": self.error,
            "last_polled": self.last_polled,
            "last_seen": self.last_seen,
            "master": self.master,
            "psu": self.psu,
            "psu_error": self.psu_error,
        }
