"""The F-engine: fpga_master's monitor and controls, and the gain archives.

``FpgaMonitor`` polls the fpga_master daemon for the FPGA badge and the
``/service/fpga`` page and carries its start/stop controls.
``GainArchive`` fetches and caches the F-engine's current digital-gain
HDF5 file; ``FileArchive`` reads a local one (eigencal's newest
solution) the same way.  Both serve their datasets in the buffer-plot
protocol, parsed by the ``choco.h5read`` subprocess so h5py never enters
the gevent hub.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests
from gevent.lock import BoundedSemaphore

from .http import fetch
from .monitor import HttpMonitor

logger = logging.getLogger(__name__)


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


class FpgaMonitor(HttpMonitor):
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

    # /stop awaits the full F-engine shutdown before responding — call
    # stop_master from a greenlet, never a request handler.
    STOP_TIMEOUT_S = 300
    MAX_ACTIONS = 10

    def __init__(self, host: str | None, port: int | None, timeout: float = 5.0):
        super().__init__(host, port, timeout)
        self.state: str | None = None      # fpga_master's own state string
        self.start_result = None           # last /start outcome, if reported
        self.frame0_ns: int | None = None
        self.last_seen: float | None = None
        # Recent control actions (newest first, ephemeral) — the page's
        # visible trail of who started/stopped what and how it went.
        self.actions: list[dict] = []

    def _get(self, path: str) -> requests.Response:
        """GET with one retry, like every monitor probe."""
        return fetch("GET", f"{self.base_url}{path}", timeout=self.timeout, retries=1)

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "fpga_master.host / port not set"
            return

        # /status: reachability + the daemon's own run state
        try:
            resp = self._get("/status")
            try:
                data = resp.json()
            except ValueError:
                data = {}
            if not isinstance(data, dict):
                data = {}
            self.state = data.get("state")
            self.start_result = data.get("start_result") or None
        except OSError as e:
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

        # /get-frame0-time: the timing reference the F-engine is running on
        try:
            resp = self._get("/get-frame0-time")
            data = resp.json()
            self.frame0_ns = int(data["frame0_nano"])
            self.health = "ok"
            self.error = None
            self.last_seen = self.last_polled
        except (OSError, ValueError, KeyError, TypeError) as e:
            self.health = "no_timing"
            self.error = f"/get-frame0-time: {type(e).__name__}: {e}"

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
            resp = fetch("POST", f"{self.base_url}/start", json={}, timeout=self.timeout)
        except OSError as e:
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
            resp = fetch("GET", f"{self.base_url}/stop", timeout=self.STOP_TIMEOUT_S)
        except OSError as e:
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


# --- gain archives ----------------------------------------------------------

class _H5Archive:
    """An HDF5 gain archive, served in the buffer-plot protocol.

    The datasets are C-order arrays with named axes, which is exactly the
    shape the buffer-plot API already speaks, so the web layer hands them
    to the existing plotter untouched.  The HDF5 parsing runs as a
    **subprocess** (``choco.h5read``): h5py is a blocking C extension
    whose import alone costs ~90 ms, and this process is a gevent hub —
    the same reason the timer jobs are separate processes.  A subclass
    says where the file comes from and when it is stale (:meth:`refresh`).
    """

    def __init__(self):
        self._path: Path | None = None      # the .h5 on disk
        self._manifest: dict | None = None
        self._data: dict[str, bytes] = {}   # dataset name -> raw bytes
        self._fetched_at: float | None = None
        self.error: str | None = None
        self._lock = BoundedSemaphore(1)

    @property
    def configured(self) -> bool:
        return True

    @property
    def path(self) -> Path | None:
        """The file last read, if any."""
        return self._path

    def refresh(self) -> bool:
        """Re-read if stale.  True when a manifest is available."""
        raise NotImplementedError

    def manifest(self) -> dict | None:
        self.refresh()
        return self._manifest

    def describe(self, name: str) -> dict | None:
        """The manifest's entry for dataset *name*, or None.

        The name is checked against the manifest rather than passed
        through: the same never-hand-the-caller's-string-to-the-tool
        rule as the journalctl allowlist.
        """
        if not self.refresh() or self._manifest is None:
            return None
        return next((d for d in self._manifest["datasets"] if d["name"] == name), None)

    def dataset(self, name: str) -> bytes | None:
        """Raw little-endian C-order bytes of one dataset, or None."""
        if self.describe(name) is None:
            return None
        if name not in self._data:
            if self._path is None:
                return None
            try:
                self._data[name] = _h5read("data", str(self._path), name)
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                logger.warning(f"gain dataset '{name}' unreadable: {self.error}")
                return None
        return self._data[name]

    def file_bytes(self) -> bytes | None:
        """The archive itself, for the page's download link."""
        if not self.refresh() or self._path is None:
            return None
        try:
            return self._path.read_bytes()
        except OSError as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            return None

    def to_dict(self) -> dict:
        manifest = self._manifest or {}
        return {
            "configured": self.configured,
            "error": self.error,
            "fetched_at": self._fetched_at,
            "datasets": manifest.get("datasets", []),
            "attrs": manifest.get("attrs", {}),
            "scalars": manifest.get("scalars", {}),
            "index_map": manifest.get("index_map", {}),
        }


class GainArchive(_H5Archive):
    """The F-engine's current digital-gain file, fetched and cached.

    fpga_master serves ``/get-current-gain-file`` as an HDF5 archive
    (``pychfpga.digital_gain.DigitalGainArchive``): ``gain_coeff``
    [update_time, freq, input] -- complex64 in the file, served as
    float32 because its imaginary part is identically zero (the rule
    lives in ``h5read._as_served``) -- plus small per-input datasets
    and an ``index_map``.

    The result is **cached** for ``ttl_s``: the plot panel polls every
    few seconds, while the gains change when someone recalibrates, so
    without a cache every poll would re-download 8.4 MB and pay for a
    fresh interpreter.  The download itself is cooperative (``requests``
    under ``monkey.patch_all()``), so only the parse needed isolating.
    """

    DEFAULT_TTL_S = 30.0
    FETCH_TIMEOUT_S = 60.0

    def __init__(self, base_url: str | None, ttl_s: float = DEFAULT_TTL_S):
        super().__init__()
        self.base_url = base_url
        self.ttl_s = float(ttl_s)

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def _stale(self) -> bool:
        return (self._fetched_at is None
                or time.time() - self._fetched_at > self.ttl_s)

    def refresh(self) -> bool:
        """Re-download and re-read if the cache is stale.  True on success.

        Serialised: with several viewers on the page, concurrent misses
        would otherwise each pull the whole file.  The waiters re-check
        staleness after the lock and normally find the work already done.
        """
        if not self.configured:
            self.error = "fpga_master.host / port not set"
            return False
        with self._lock:
            if not self._stale() and self._manifest is not None:
                return True
            try:
                self._load()
            except Exception as exc:            # network, subprocess, JSON
                self.error = f"{type(exc).__name__}: {exc}"
                logger.warning(f"gain archive refresh failed: {self.error}")
                # Keep whatever was cached: a stale gain table still
                # tells the operator more than an empty page does.
                return self._manifest is not None
            self.error = None
            return True

    def _load(self) -> None:
        resp = fetch("GET", f"{self.base_url}/get-current-gain-file",
                     timeout=self.FETCH_TIMEOUT_S)
        fd, tmp = tempfile.mkstemp(prefix="choco-gain-", suffix=".h5")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(resp.content)
            manifest = json.loads(_h5read("manifest", tmp).decode())
        except Exception:
            Path(tmp).unlink(missing_ok=True)
            raise
        if self._path is not None:              # the previous download
            self._path.unlink(missing_ok=True)
        self._path = Path(tmp)
        self._manifest = manifest
        self._data = {}
        self._fetched_at = time.time()


def newest_file(directory, pattern: str) -> Path | None:
    """The most recently modified file matching *pattern* in *directory*,
    or None (no directory, no match, or a mount that will not answer)."""
    try:
        files = sorted(Path(directory).glob(pattern),
                       key=lambda f: f.stat().st_mtime, reverse=True)
    except OSError:
        return None
    return files[0] if files else None


class FileArchive(_H5Archive):
    """A local HDF5 archive -- eigencal's newest gain solution -- read the
    way :class:`GainArchive` reads fpga_master's.  *path_fn* returns the
    file to show (the newest ``gain_*.h5`` in the job's state directory)
    or None; the cache is keyed on that path and its mtime, so a new
    solution shows up on the next look and nothing is re-parsed while
    the file stands still.  The file is the job's record and is never
    copied or deleted here.
    """

    def __init__(self, path_fn):
        super().__init__()
        self._path_fn = path_fn
        self._mtime: float | None = None

    @property
    def mtime(self) -> float | None:
        """The mtime of the file last read, if any."""
        return self._mtime

    def refresh(self) -> bool:
        with self._lock:
            try:
                path = self._path_fn()
                if path is None:
                    self.error = "no gain solution archived yet"
                    self._path, self._manifest, self._data = None, None, {}
                    return False
                path = Path(path)
                mtime = path.stat().st_mtime
                if (self._manifest is not None
                        and self._path == path and self._mtime == mtime):
                    return True
                manifest = json.loads(_h5read("manifest", str(path)).decode())
            except Exception as exc:
                self.error = f"{type(exc).__name__}: {exc}"
                logger.warning(f"eigencal archive unreadable: {self.error}")
                return self._manifest is not None
            self._path, self._mtime = path, mtime
            self._manifest, self._data = manifest, {}
            self._fetched_at = time.time()
            self.error = None
            return True


def _h5read(mode: str, path: str, *args: str) -> bytes:
    """Run choco.h5read in a subprocess and return its stdout."""
    cmd = [sys.executable, "-m", "choco.h5read", mode, path, *args]
    proc = subprocess.run(cmd, capture_output=True, timeout=120)
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip() or "no detail"
        raise RuntimeError(f"h5read {mode} failed: {detail}")
    return proc.stdout
