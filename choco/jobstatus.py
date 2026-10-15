"""The jobs as systemd and their state files see them.

``job_status`` is a oneshot job's health for its badge, from ``systemctl
show`` and its state file's age; ``timer_status`` its timer's schedule;
``job_logs`` its journal; ``read_state_json`` the file it publishes.
The host tools run through ``choco.hosttool``.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from .hosttool import run_tool

logger = logging.getLogger(__name__)


# The handful of ``systemctl show`` properties consumed by job_status.
# ``Result`` is the only health signal; ``ExecMainExitTimestamp`` is used
# purely as an emptiness test ("has this unit ever run") — its value is
# never parsed; ``ActiveState`` marks a run in progress.  The rest are
# carried into the result for tooltips.
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


#: The last *completed* snapshot per unit (health, result, exit status).
#: systemd resets ``Result`` and ``ExecMainStatus`` and blanks
#: ``ExecMainExitTimestamp`` the moment a run starts — a unit that just
#: failed reads ``success`` while its next run is in flight — so a run in
#: progress carries no verdict, and this is the only place the previous
#: one survives.  Process-local and rebuilt by the status poll: runtime
#: state, never persisted.
_LAST_COMPLETED: dict[str, dict] = {}


def _systemctl_show(unit: str, timeout: float = 5.0,
                    props: tuple[str, ...] = _SYSTEMCTL_PROPS,
                    ) -> dict[str, str] | None:
    """Return ``systemctl show <unit>`` properties as a dict, or None on failure."""
    result = run_tool(["systemctl", "show", unit, "--property=" + ",".join(props),
                       "--no-pager"], timeout=timeout)
    if result is None or result.returncode != 0:
        return None
    out: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


def job_logs(service_unit: str, lines: int = 50,
             timeout: float = 5.0) -> list[str] | None:
    """Recent journal lines for a unit, newest last.

    Returns ``None`` when the journal can't be read (no journalctl on
    the host, or the call failed) — the caller distinguishes "no log
    access" from "unit has no entries" (an empty-ish but valid list).
    """
    result = run_tool(["journalctl", "-u", service_unit, "-n", str(int(lines)),
                       "--no-pager", "-o", "short-iso"], timeout=timeout)
    if result is None:
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

    A run in progress (``ActiveState`` ``activating``) is reported as
    the unit's last completed snapshot, flagged ``running``, because
    systemd tells nothing about the previous outcome while a run is in
    flight (see ``_LAST_COMPLETED``); without one — a fresh process —
    it reads ``unknown`` until the first completion it sees.  Staleness
    is a live fact about the state file and still applies.

    Jobs share an exit-code convention: 0 = ok, **2 = degraded** (the
    job itself is fine but a dependency or input wasn't — fpga_master
    unreachable, stale data, choco down; retries self-heal), anything
    else = failed (config error or bug — needs a human).  A failing
    unit whose last exit status is 2 therefore reports ``degraded``
    rather than ``failed``.

    Returns a dict with ``health`` (``ok`` / ``degraded`` / ``stale`` /
    ``failed`` / ``never_run`` / ``unknown``), ``running``,
    ``state_mtime`` (epoch or ``None``), and raw systemd fields for the
    tooltip.
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

    out = {
        "health": health,
        "running": False,
        "state_mtime": mtime,
        "result": result or None,
        "active_state": (props or {}).get("ActiveState") or None,
        "sub_state": (props or {}).get("SubState") or None,
        "exit_status": (props or {}).get("ExecMainStatus") or None,
        "systemd": props is not None,
        "unit": service_unit,
        "state_file": str(state_file) if state_file else None,
    }
    if (out["active_state"] or "").strip() == "activating":
        out["running"] = True
        last = _LAST_COMPLETED.get(service_unit)
        if last is None:
            out["health"] = "stale" if stale else "unknown"
        else:
            out["health"] = "stale" if stale else last["health"]
            out["result"], out["exit_status"] = last["result"], last["exit_status"]
    elif props is not None:
        _LAST_COMPLETED[service_unit] = {
            "health": health, "result": out["result"],
            "exit_status": out["exit_status"]}
    return out
