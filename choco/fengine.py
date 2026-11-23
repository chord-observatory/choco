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

import gevent
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


#: A gain session's states in the order a run passes them, and the step
#: that takes each to the next (``GainSessionManager``).
GAIN_STEPS = ("open", "capture", "compute", "apply")
_GAIN_NEXT = {"open": "capture", "captured": "compute", "computed": "apply"}
_GAIN_DONE = {"opening": 0, "open": 1, "captured": 2, "computed": 3, "applied": 4, "archived": 4}
#: The states of a session that is over.
GAIN_ENDED = ("archived", "rejected", "failed")


def gain_session_live(session: dict | None) -> bool:
    """Whether the service holds a gain session that is not over."""
    return bool(session) and session.get("state") not in GAIN_ENDED


def gain_steps(session: dict | None) -> list[dict]:
    """The four steps of a gain run as ``{name, status}`` with status
    done, running, failed or pending, from the session's state, the
    operation it is running and its error (``"<step> failed: ..."``)."""
    session = session or {}
    done = _GAIN_DONE.get(session.get("state"), 0)
    running = session.get("operation")
    error = session.get("error") or ""
    steps = []
    for i, name in enumerate(GAIN_STEPS):
        if running == name:
            status = "running"
        elif error.startswith(f"{name} failed"):
            status = "failed"
        elif i < done:
            status = "done"
        else:
            status = "pending"
        steps.append({"name": name, "status": status})
    return steps


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
    # A gain run: the open answers after fpga_master's shutdown; a step
    # (programming the boards, a capture) is given this long to finish.
    GAIN_POST_TIMEOUT_S = 30
    GAIN_OPEN_TIMEOUT_S = 300
    GAIN_STEP_TIMEOUT_S = 900
    GAIN_POLL_S = 2
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
        # choco's own chain through a gain session's steps, while it runs
        # and its outcome after: {"active", "ok", "message", "started"}.
        self.gain_run: dict | None = None

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
        try:
            self._read_session()
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

    def _read_session(self) -> dict | None:
        """The service's gain session now (no session is a 404)."""
        resp = self._get("/gains/session", accept_statuses=(404,))
        data = resp.json()
        self.session = data if resp.status_code == 200 and isinstance(data, dict) else None
        return self.session

    def _gain_post(self, step: str, body: dict, timeout: float | None = None) -> None:
        path = "/gains/session" if step == "open" else f"/gains/session/{step}"
        fetch("POST", f"{self.base_url}{path}", json=body, timeout=timeout or self.GAIN_POST_TIMEOUT_S)

    def gain_chain(self) -> tuple[bool, str]:
        """Take a gain session to ``applied``: open one if none is live
        (the service stops fpga_master first), then capture, compute and
        apply with the service's defaults, each sent when the one before
        has finished.  A live session is continued from where it stands,
        so a failed step can be tried again.  Blocks for minutes; run it
        in a greenlet.  The outcome is also left in ``gain_run``."""
        self.gain_run = {"active": True, "ok": None, "message": "starting", "started": time.time()}
        ok, message = self._gain_chain()
        self.gain_run.update(active=False, ok=ok, message=message)
        try:
            self.poll_once()
        except Exception:               # the outcome stands whatever the poll does
            pass
        return ok, message

    def _gain_chain(self) -> tuple[bool, str]:
        if not self.configured:
            return False, "the F-engine service is not configured"
        try:
            session = self._read_session()
            sent = None                 # the step this chain sent last
            if not gain_session_live(session):
                self.gain_run["message"] = "open"
                # The answer waits for fpga_master's shutdown.
                self._gain_post("open", {"stop_fpga_master": True}, timeout=self.GAIN_OPEN_TIMEOUT_S)
                sent = "open"
            deadline = time.time() + self.GAIN_STEP_TIMEOUT_S
            while True:
                session = self._read_session()
                if session is None:
                    return False, "the gain session is gone"
                state = session.get("state")
                if session.get("operation"):
                    if time.time() > deadline:
                        return False, f"{session['operation']} did not finish in {self.GAIN_STEP_TIMEOUT_S} s"
                    gevent.sleep(self.GAIN_POLL_S)
                    continue
                # An error is the last operation's; one from before this
                # chain is what a continued run tries again.
                if sent and session.get("error"):
                    return False, str(session["error"])[:300]
                if state == "applied":
                    return True, "applied; ready for review"
                step = _GAIN_NEXT.get(state)
                if step is None:
                    return False, f"the gain session is {state}"
                self.gain_run["message"] = step
                self._gain_post(step, {})
                sent = step
                deadline = time.time() + self.GAIN_STEP_TIMEOUT_S
        except (OSError, ValueError) as e:
            return False, _failure(e) if isinstance(e, OSError) else f"{type(e).__name__}: {e}"

    def gain_decide(self, action: str) -> tuple[bool, str]:
        """End the session: ``approve`` (write the archive and start
        fpga_master again; the service refuses if a capture had problems),
        ``approve-anyway`` (the same with ``override``) or ``reject``
        (discard; fpga_master stays stopped).  Returns ``(ok, message)``."""
        if not self.configured:
            return False, "the F-engine service is not configured"
        if action != "reject" and self.gain_run and self.gain_run.get("active"):
            return False, "the gain run is still in progress"
        try:
            # A reject also cancels the step in progress; the chain then
            # finds the session rejected and ends.
            if action == "reject":
                self._gain_post("reject", {})
                message = "rejected; fpga_master stays stopped"
            elif action in ("approve", "approve-anyway"):
                self._gain_post("approve", {"override": action == "approve-anyway",
                                            "restart_fpga_master": True})
                message = "approved; the archive is written and fpga_master starts"
            else:
                return False, f"no such gain action: {action}"
        except OSError as e:
            return False, _failure(e)
        self.poll_once()
        return True, message

    def gain_plot(self, name: str) -> bytes | None:
        """One of the live session's plots (a PNG), or None."""
        if not self.configured or name not in ((self.session or {}).get("plots") or []):
            return None
        try:
            return fetch("GET", f"{self.base_url}/gains/session/plots/{name}",
                         timeout=self.timeout).content
        except OSError:
            return None

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
            "gain_run": self.gain_run,
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
