"""The F-engine service: the crate supply and fpga_master's supervisor.

``FengineMonitor`` polls f-engine-tools' ``f-engine-service`` on
crs-control, which holds the crate's power supply (a t0 PSU, through
``psucontrol``) and starts, stops and watches fpga_master.  choco reads
the supply and the supervisor's state from it and never opens the supply
itself.
"""

from __future__ import annotations

import time

import requests

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


class FengineMonitor(HttpMonitor):
    """Background poller for the F-engine service.

    Health levels:
      * ``ok``     — the service answered and read the supply.
      * ``no_psu`` — the service answered but could not read the supply.
      * ``down``   — the service did not answer.

    The supply's output being off, or fpga_master stopped, is a fact the
    page shows, not the service's health.
    """

    def __init__(self, host: str | None, port: int | None, timeout: float = 5.0):
        super().__init__(host, port, timeout)
        self.master: dict | None = None
        self.psu: dict | None = None
        self.psu_error: str | None = None
        self.last_seen: float | None = None

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
            return
        self.master = master
        self.last_seen = self.last_polled

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

    def to_dict(self) -> dict:
        return {
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
