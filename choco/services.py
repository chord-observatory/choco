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
import os
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import gevent
import requests
from gevent.lock import BoundedSemaphore

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


# --- FPGA digital gains ---------------------------------------------------

class GainArchive:
    """The F-engine's current digital-gain file, fetched and cached.

    fpga_master serves ``/get-current-gain-file`` as an HDF5 archive
    (``pychfpga.digital_gain.DigitalGainArchive``): ``gain_coeff``
    [update_time, freq, input] -- complex64 in the file, served as
    float32 because its imaginary part is identically zero (the rule
    lives in ``h5read._as_served``) -- plus small per-input datasets
    and an ``index_map``.  Its datasets are C-order arrays with named
    axes, which is exactly the shape the buffer-plot API already speaks,
    so the web layer can hand them to the existing plotter untouched.

    Two things are deliberate here.  The HDF5 parsing runs as a
    **subprocess** (``choco.h5read``): h5py is a blocking C extension
    whose import alone costs ~90 ms, and this process is a gevent hub —
    the same reason the timer jobs are separate processes.  And the
    result is **cached** for ``ttl_s``: the plot panel polls every few
    seconds, while the gains change when someone recalibrates, so
    without a cache every poll would re-download 8.4 MB and pay for a
    fresh interpreter.

    The download itself is plain ``requests`` and therefore cooperative
    — ``monkey.patch_all()`` runs at startup — so only the parse needed
    isolating.
    """

    DEFAULT_TTL_S = 30.0
    FETCH_TIMEOUT_S = 60.0

    def __init__(self, base_url: str | None, ttl_s: float = DEFAULT_TTL_S):
        self.base_url = base_url
        self.ttl_s = float(ttl_s)
        self._path: Path | None = None      # the cached .h5 on disk
        self._manifest: dict | None = None
        self._data: dict[str, bytes] = {}   # dataset name -> raw bytes
        self._fetched_at: float | None = None
        self.error: str | None = None
        self._lock = BoundedSemaphore(1)

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def _stale(self) -> bool:
        return (self._fetched_at is None
                or time.time() - self._fetched_at > self.ttl_s)

    def refresh(self, force: bool = False) -> bool:
        """Re-download and re-read if the cache is stale.  True on success.

        Serialised: with several viewers on the page, concurrent misses
        would otherwise each pull the whole file.  The waiters re-check
        staleness after the lock and normally find the work already done.
        """
        if not self.configured:
            self.error = "fpga_master.host / port not set"
            return False
        with self._lock:
            if not force and not self._stale() and self._manifest is not None:
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
        resp = requests.get(f"{self.base_url}/get-current-gain-file",
                            timeout=self.FETCH_TIMEOUT_S)
        resp.raise_for_status()
        fd, tmp = tempfile.mkstemp(prefix="choco-gain-", suffix=".h5")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(resp.content)
            manifest = json.loads(_h5read("manifest", tmp).decode())
        except Exception:
            Path(tmp).unlink(missing_ok=True)
            raise
        self._discard_file()
        self._path = Path(tmp)
        self._manifest = manifest
        self._data = {}
        self._fetched_at = time.time()

    def _discard_file(self) -> None:
        if self._path is not None:
            self._path.unlink(missing_ok=True)
            self._path = None

    def manifest(self) -> dict | None:
        self.refresh()
        return self._manifest

    def dataset(self, name: str) -> bytes | None:
        """Raw little-endian C-order bytes of one dataset, or None.

        The name is checked against the manifest rather than passed
        through: the same never-hand-the-caller's-string-to-the-tool
        rule as the journalctl allowlist.
        """
        if not self.refresh() or self._manifest is None:
            return None
        if not any(d["name"] == name for d in self._manifest["datasets"]):
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

    def describe(self, name: str) -> dict | None:
        manifest = self.manifest()
        if manifest is None:
            return None
        for dataset in manifest["datasets"]:
            if dataset["name"] == name:
                return dataset
        return None

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


def _h5read(mode: str, path: str, *args: str) -> bytes:
    """Run choco.h5read in a subprocess and return its stdout."""
    cmd = [sys.executable, "-m", "choco.h5read", mode, path, *args]
    proc = subprocess.run(cmd, capture_output=True, timeout=120)
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip() or "no detail"
        raise RuntimeError(f"h5read {mode} failed: {detail}")
    return proc.stdout


# --- PDB (power_db) -------------------------------------------------------

def _summarize(items: list[str], limit: int = 4) -> str:
    """First few items plus a count of the rest — bulk writes can fail wide."""
    head = ", ".join(items[:limit])
    extra = len(items) - limit
    return f"{head} and {extra} more" if extra > 0 else head


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


class PdbMonitor:
    """Background poller for the power_db PDB (power distribution boards).

    Hits ``/status`` and ``/channel_states`` on a slow interval and
    decodes the per-channel power state.  Same shape as
    :class:`FpgaMonitor`: latest result held in attributes, surfaced by
    the services strip and the ``/service/pdb`` page.

    Health levels:
      * ``ok``        — both endpoints responded and decoded.
      * ``no_states`` — /status responded but /channel_states did not.
      * ``down``      — /status did not respond.
      * ``unconfigured`` — no pdb.host / port in config.
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

    # Position in the daisy chain <-> (board, chip).  Every read and every
    # write goes through this pair, so the convention is stated once: get
    # it wrong and the UI labels — or powers — the wrong amplifier.
    @staticmethod
    def _chip_num(board: int, chip: str) -> int:
        return board * 2 + (0 if chip == "A" else 1)

    @staticmethod
    def _board_chip(chip_num: int) -> tuple[int, str]:
        return chip_num // 2, ("A" if chip_num % 2 == 0 else "B")

    @classmethod
    def _rows(cls, out_bytes: list[int]) -> list[dict]:
        rows = []
        for k, out in enumerate(out_bytes):
            board, chip = cls._board_chip(k)
            rows.append({
                "board": board, "chip": chip,
                "channels": [bool(out & (1 << c)) for c in range(8)],
            })
        return rows

    def poll_once(self) -> None:
        """Probe both endpoints once and update in-memory state."""
        self.last_polled = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = "pdb.host / port not set"
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

    def poll_if_stale(self, max_age_s: float = 10.0) -> None:
        """Poll now if the last poll is older than ``max_age_s``.

        Lets the /service/pdb page tighten the effective cadence while
        an operator is actually watching, without touching the 30s loop.
        """
        if not self.configured:
            return
        if (self.last_polled is None
                or time.time() - self.last_polled > max_age_s):
            self.poll_once()

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
            return False, "pdb is not configured"
        if chip not in ("A", "B") or not 0 <= channel < 8 or board < 0:
            return False, f"invalid channel address: board {board} " \
                          f"chip {chip} ch{channel}"
        chip_num = self._chip_num(board, chip)
        label = f"bus {bus} board {board} chip {chip} ch{channel}"
        try:
            outs = self._fetch_states().get(bus)
            if outs is None or chip_num >= len(outs):
                return False, f"{label}: not present on the controller"
            current = outs[chip_num]
            wanted = (current | (1 << channel) if on
                      else current & ~(1 << channel))
            if wanted != current:
                self._write_out_byte(bus, board, chip, wanted)
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
        logger.info(f"pdb: {label} -> {'on' if on else 'off'}")
        return True, f"{label} {'on' if on else 'off'}"

    def _write_out_byte(self, bus: int, board: int, chip: str,
                        value: int) -> None:
        """POST one chip's whole OUT register.  Raises on a failed write."""
        resp = requests.post(
            f"{self.base_url}/write_command",
            json={"spi_bus": bus, "board_idx": board,
                  "chip_letter": chip, "operation": "OUT",
                  "states": format(value, "08b")},
            timeout=self.timeout)
        resp.raise_for_status()

    def set_group(self, bus: int, on: bool, board: int | None = None,
                  chip: str | None = None) -> tuple[bool, str]:
        """Power every channel of a chip, a board, or a whole bus.

        The scope widens as arguments are dropped: ``chip`` given ->
        that chip's 8 channels; only ``board`` -> both of its chips;
        neither -> every chip on ``bus``.  A chip is an all-ones or
        all-zeros OUT byte, so this is the same read-modify-write as
        :meth:`set_channel` with the "modify" being the whole byte —
        and the same rules apply: chips already in the wanted state are
        not written, and the result is confirmed by one fresh read.

        Writes are issued per chip because power_db has no bulk
        endpoint.  If some fail the rest still go out (the grid then
        shows exactly which took), and the message names the failures —
        a partial result is reported, never retried silently.
        """
        if not self.configured:
            return False, "pdb is not configured"
        if chip is not None and chip not in ("A", "B"):
            return False, f"invalid chip: {chip}"
        if board is not None and board < 0:
            return False, f"invalid board: {board}"
        if chip is not None and board is None:
            return False, "a chip scope needs a board"

        if chip is not None:
            scope = f"bus {bus} board {board} chip {chip}"
        elif board is not None:
            scope = f"bus {bus} board {board}"
        else:
            scope = f"bus {bus} (all boards)"
        wanted = 0xFF if on else 0x00
        state_word = "on" if on else "off"

        try:
            outs = self._fetch_states().get(bus)
        except (requests.RequestException, ValueError, KeyError,
                TypeError) as e:
            return False, f"{scope}: {type(e).__name__}: {e}"
        if outs is None:
            return False, f"{scope}: bus not present on the controller"

        targets = []   # (chip_num, board, chip, current OUT byte)
        for chip_num, current in enumerate(outs):
            b, c = self._board_chip(chip_num)
            if board is not None and b != board:
                continue
            if chip is not None and c != chip:
                continue
            targets.append((chip_num, b, c, current))
        if not targets:
            return False, f"{scope}: no such chips on the controller"

        todo = [t for t in targets if t[3] != wanted]
        if not todo:
            return True, (f"{scope}: all {len(targets) * 8} channels were "
                          f"already {state_word}")

        logger.warning(
            f"pdb: {scope} -> all {state_word} "
            f"({len(todo)} of {len(targets)} chips need writing)")
        failures = []
        for _chip_num, b, c, _current in todo:
            try:
                self._write_out_byte(bus, b, c, wanted)
            except (requests.RequestException, ValueError) as e:
                failures.append(f"board {b} chip {c} ({type(e).__name__})")

        try:
            readback = self._fetch_states()
            self.channels = {b: self._rows(o) for b, o in readback.items()}
            self.last_seen = time.time()
            got = readback[bus]
        except (requests.RequestException, ValueError, KeyError,
                TypeError) as e:
            return False, (
                f"{scope}: wrote {len(todo) - len(failures)} of "
                f"{len(todo)} chips but the verify read failed "
                f"({type(e).__name__}: {e}) — check the grid")
        stale = [f"board {b} chip {c}" for chip_num, b, c, _ in targets
                 if chip_num >= len(got) or got[chip_num] != wanted]

        if failures or stale:
            parts = []
            if failures:
                parts.append(f"{len(failures)} write(s) failed: "
                             + _summarize(failures))
            if stale:
                parts.append(f"{len(stale)} chip(s) did not take: "
                             + _summarize(stale))
            return False, f"{scope}: turning all {state_word} — " \
                          + "; ".join(parts)
        return True, (f"{scope}: {len(todo) * 8} channels {state_word} "
                      f"({len(targets) * 8} in scope)")

    def run(self) -> None:
        """Poll forever on ``POLL_INTERVAL_S``.  Designed for gevent.spawn."""
        self._running = True
        while self._running:
            try:
                self.poll_once()
            except Exception:
                logger.exception("PdbMonitor.poll_once raised")
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


# Layout attributes injected at render time.  kotekan ships its own
# rankdir/nodesep/ranksep/mclimit/newrank now, and a -G on the command line
# silently overrides the graph's own value, so anything set here is a
# deliberate override of kotekan's choice.  mclimit raises the crossing-
# minimisation effort and newrank improves ranking — together they laid
# out the 645-line cx19 pipeline ~11% narrower (fewer edge crossings)
# for well under a second of extra layout time.  Splines (curves) are
# the default routing: ortho was the original pick when default splines
# were spaghetti, but mclimit/newrank tamed that, curves lay out ~10%
# more compact, and the operator prefers them; ortho stays a preset.
_DOT_LAYOUT_ARGS = ("-Gsplines=true", "-Granksep=1.0", "-Gnodesep=0.5",
                    "-Gmclimit=8", "-Gnewrank=true")

# Selectable presets for the full-page pipeline view: edge-routing style
# is a taste call on a graph this size, so the page lets the operator
# flip between them live.  Keys are the allowlist for the ``?layout=``
# query parameter — never pass raw values to the dot command line.
#
# ortho deliberately leaves nodesep to kotekan (0.3).  Widening it aborts
# graphviz 2.43 outright — `chkSgraph: Assertion np->cells[0] failed` in
# the ortho maze router — on a clustered CHORD graph that renders fine at
# kotekan's own value; 0.4 survived and 0.5 did not, so the threshold is
# graph-dependent and the safe move is not to override it at all.  The
# crash is caught by the retry-without-args path, but the operator picks
# "ortho" and silently gets curves after a wasted render.
PIPELINE_LAYOUTS = {
    "curves": _DOT_LAYOUT_ARGS,
    "ortho": ("-Gsplines=ortho", "-Granksep=1.2",
              "-Gmclimit=8", "-Gnewrank=true"),
    "polyline": ("-Gsplines=polyline", "-Granksep=1.0", "-Gnodesep=0.5",
                 "-Gmclimit=8", "-Gnewrank=true"),
}


def render_dot_svg(dot_text: str, timeout: float = 10.0,
                   layout_args: tuple[str, ...] = _DOT_LAYOUT_ARGS) -> str | None:
    """Render graphviz dot text to an SVG document string.

    Shells out to the graphviz CLI (same host-tool-over-Python-dependency
    choice as the openssl TLS fallback), injecting ``_DOT_LAYOUT_ARGS``;
    orthogonal routing is the fussiest graphviz code path, so a failed
    render is retried once without the layout args before giving up.
    Returns None when it can't render at all (no ``dot`` binary on the
    host, render failure, timeout) — the caller falls back to showing the
    raw dot text.  The output is sliced down to the ``<svg`` element: the
    XML prolog and DOCTYPE would be invalid embedded in an HTML page.

    The pipe is UTF-8 explicitly rather than by locale: kotekan's layout
    lines carry ``×`` and ``·``, and choco runs as a systemd unit with no
    LANG to speak of — a C-locale interpreter would raise UnicodeEncodeError
    here, which is not a ``SubprocessError`` and would 500 the route.
    """
    if shutil.which("dot") is None:
        return None
    for extra_args in (layout_args, ()):
        try:
            result = subprocess.run(
                ["dot", "-Tsvg", *extra_args],
                input=dot_text, capture_output=True, text=True,
                encoding="utf-8", timeout=timeout,
            )
        except (subprocess.SubprocessError, OSError) as e:
            logger.warning(f"dot -Tsvg {' '.join(extra_args)} failed: {e}")
            continue
        if result.returncode != 0:
            logger.warning(f"dot -Tsvg {' '.join(extra_args)} "
                           f"rc={result.returncode}: "
                           f"{result.stderr.strip()[:200]}")
            continue
        start = result.stdout.find("<svg")
        if start < 0:
            logger.warning("dot -Tsvg produced no <svg> element")
            continue
        return result.stdout[start:]
    return None


# --- Pipeline SVG sanitizer (clickable inline graph) ---
#
# The pipeline page needs the SVG *live* in the DOM to make buffer
# nodes clickable, which means kotekan-supplied markup lands in choco's
# authenticated UI.  It goes through this whitelist RECONSTRUCTION: a
# brand-new tree is built and only
# known-inert graphviz output elements/attributes are copied over —
# <script>, event handlers, <foreignObject>, xlink:href and anything
# else unexpected can't survive because nothing is copied by default.
#
# An element that is not on the list is UNWRAPPED, not deleted: it is left
# out of the output and its children are rebuilt in its place.  That keeps
# deny-by-default exactly as strict — the output can still only ever contain
# listed elements carrying listed attributes, and an unknown wrapper reaches
# the DOM no more than a <script> does — while making the failure mode
# lossless.  Deleting the subtree instead is a silent one: graphviz wraps a
# node's shape and text in <a> as soon as the node carries a URL *or* a
# tooltip, kotekan sets a tooltip on every buffer and every stage, and the
# result was 111 of 223 nodes rendering as empty groups — present in the DOM
# and still clickable, but with nothing drawn.  Whichever element graphviz
# starts emitting next, the worst this can now do is lose its styling.

_SVG_NS = "http://www.w3.org/2000/svg"
# Process-global ET state: makes SVG the *default* namespace on
# serialization (``<svg xmlns=...>`` rather than ``<ns0:svg
# xmlns:ns0=...>``, which browsers would still honour but no CSS
# selector here would match).  services.py is choco's only ElementTree
# user; a second one serializing a different vocabulary would need its
# own register_namespace call.
ET.register_namespace("", _SVG_NS)

_SVG_ALLOWED = {
    "svg": {"width", "height", "viewBox"},
    "g": {"id", "class", "transform"},
    "title": set(),
    "polygon": {"fill", "stroke", "stroke-width", "stroke-dasharray", "points"},
    "polyline": {"fill", "stroke", "stroke-width", "stroke-dasharray", "points"},
    "ellipse": {"fill", "stroke", "stroke-width", "stroke-dasharray",
                "cx", "cy", "rx", "ry"},
    "circle": {"fill", "stroke", "stroke-width", "stroke-dasharray",
               "cx", "cy", "r"},
    "path": {"fill", "stroke", "stroke-width", "stroke-dasharray", "d"},
    "text": {"text-anchor", "x", "y", "font-family", "font-size", "fill"},
}


def _svg_local(tag_or_attr: str) -> str:
    """Local name of a possibly namespace-qualified tag/attribute."""
    return tag_or_attr.rsplit("}", 1)[-1]


def sanitize_pipeline_svg(svg_text: str, clickable: set[str],
                          node_key: str) -> str | None:
    """Rebuild a graphviz pipeline SVG through the whitelist above.

    Graphviz wraps every dot node in ``<g class="node">`` whose first
    ``<title>`` child is the exact dot node name; groups whose title is
    in ``clickable`` (the peek_hold buffer names) are stamped with
    ``data-plot-buffer``/``data-plot-node`` so bufferplot.js's existing
    delegated click handler opens their live plot — no JS changes — plus
    ``tabindex``/``role``/``aria-label`` so they are reachable by
    keyboard (pipeline.js turns Enter/Space into a click).
    Returns the sanitized SVG markup, or None if the input doesn't
    parse (caller falls back to the raw dot text), or if the root is
    not an ``<svg>`` — the root is the one element that cannot be
    unwrapped, since there would be nothing to put the graph in.
    """
    try:
        root = ET.fromstring(svg_text)
    except ET.ParseError as e:
        logger.warning(f"pipeline SVG parse failed: {e}")
        return None
    if _svg_local(root.tag) != "svg":
        return None

    unwrapped: set[str] = set()

    def rebuild(src: ET.Element, dst_parent: ET.Element) -> None:
        """Copy `src` under `dst_parent` if it is listed; else unwrap it."""
        tag = _svg_local(src.tag)
        allowed = _SVG_ALLOWED.get(tag)
        if allowed is None:
            # Not copied, so it never reaches the DOM — but its children
            # get their own turn under the same rule, so a wrapper cannot
            # take the drawing down with it.  Text is deliberately not
            # carried over: that is what keeps <script> contents out.
            unwrapped.add(tag)
            for child in src:
                rebuild(child, dst_parent)
            return
        attrs = {name: value for raw, value in src.attrib.items()
                 if (name := _svg_local(raw)) in allowed}
        dst = ET.SubElement(dst_parent, f"{{{_SVG_NS}}}{tag}", attrs)
        if tag in ("text", "title"):
            dst.text = src.text
        for child in src:
            rebuild(child, dst)

    root_attrs = {name: value for raw, value in root.attrib.items()
                  if (name := _svg_local(raw)) in _SVG_ALLOWED["svg"]}
    out = ET.Element(f"{{{_SVG_NS}}}svg", root_attrs)
    for child in root:
        rebuild(child, out)
    if unwrapped:
        # Not an error -- graphviz is free to wrap things -- but worth
        # saying, since it means the output lost whatever those elements
        # were styling.
        logger.info("pipeline SVG: unwrapped unlisted elements: "
                    f"{', '.join(sorted(unwrapped))}")

    for group in out.iter(f"{{{_SVG_NS}}}g"):
        if "node" not in group.get("class", "").split():
            continue
        title = group.find(f"{{{_SVG_NS}}}title")
        name = (title.text or "").strip() if title is not None else ""
        if name in clickable:
            group.set("data-plot-buffer", name)
            group.set("data-plot-node", node_key)
            group.set("class", group.get("class", "") + " clickable-buffer")
            group.set("tabindex", "0")
            group.set("role", "button")
            group.set("aria-label", f"plot buffer {name}")
    return ET.tostring(out, encoding="unicode")


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
