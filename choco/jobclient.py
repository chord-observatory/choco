"""What every job shares: the loopback client for choco's JSON API, the
state-directory convention, atomic writes, and the run plumbing (exit
codes, logging, the run lock, ``run.json``).

Stdlib only, because its callers are the jobs and the CLI, which talk to
choco on the same host.  The conventions live here so they are written
once: the ``/update``, ``/oneshot`` and ``/api/*`` routes bypass login for
loopback callers, and choco's certificate is self-signed, so an ``https``
URL is fetched **unverified** — nothing on that wire leaves the host.
Transport failures surface as ``urllib.error.URLError`` / ``HTTPError``,
both ``OSError`` subclasses, which is what the jobs' exit-code convention
turns into "degraded".
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import ssl
import tempfile
import time
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)


def ssl_context(url: str) -> ssl.SSLContext | None:
    """An unverified TLS context for an ``https`` URL, None for plain HTTP."""
    if not url.startswith("https:"):
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def request_json(base_url: str, path: str, body=None, timeout: float = 10.0,
                 method: str | None = None):
    """GET (``body`` None) or POST (``body`` JSON-encoded) one endpoint;
    *method* overrides the verb (``PUT`` for a library write).

    Returns the decoded JSON reply, or None for an empty body.  Raises
    ``urllib.error.HTTPError`` for a non-2xx reply and ``URLError`` when
    choco cannot be reached.
    """
    url = base_url.rstrip("/") + path
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method=method or ("POST" if body is not None else "GET"))
    with urllib.request.urlopen(req, timeout=timeout,
                                context=ssl_context(url)) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


def get_json(base_url: str, path: str, timeout: float = 10.0):
    return request_json(base_url, path, timeout=timeout)


def post_json(base_url: str, path: str, body, timeout: float = 10.0):
    return request_json(base_url, path, body=body, timeout=timeout)


def put_json(base_url: str, path: str, body, timeout: float = 10.0):
    return request_json(base_url, path, body=body, timeout=timeout, method="PUT")


def push_updatable(base_url: str, group: str, endpoint: str, values,
                   timeout: float = 10.0):
    """Hand *values* for one kotekan updatable *endpoint* to choco, which
    relays them to every node in *group* (``POST /update/<group>``)."""
    return post_json(base_url, f"/update/{group}", {
        "action": "updatable_config", "endpoint": endpoint, "values": values,
    }, timeout=timeout)


#: Root of the jobs' state, as the systemd units lay it out
#: (``StateDirectory=choco/<job>``).  choco reads ``<state_dir>/<job>/...``
#: from its one ``state_dir`` setting, which defaults to this.
STATE_ROOT = Path("/var/lib/choco")

#: Files choco reads from ``<state_dir>/<job>/`` by name: the job's state
#: (changes only with what the job publishes) and its per-run record
#: (rewritten every run, see :func:`write_run`).
STATE_NAME = "state.json"
RUN_NAME = "run.json"
#: eigencal's archived solutions, beside its state.json (gain_<transit>_<source>.h5).
EIGENCAL_ARCHIVE_GLOB = "gain_*.h5"

#: The bffs manual override file: written by choco's BFFS page (the
#: element grid) and read by the job's ``manual`` source, so the name is
#: shared rather than configured twice.
MANUAL_OVERRIDES_NAME = "manual_overrides.yaml"


def job_state_dir(name: str, override=None) -> Path:
    """Where job *name* keeps its files.

    This is a convention, not configuration: the unit fixes it
    (``StateDirectory=choco/<name>``, exported to the job as
    ``$STATE_DIRECTORY``) and choco reads the same place.  *override* (a
    ``--state-dir`` flag) serves hand runs, dev instances and tests;
    after it comes ``$STATE_DIRECTORY``, then ``/var/lib/choco/<name>``.
    """
    if override:
        return Path(override)
    env = os.environ.get("STATE_DIRECTORY")
    if env:
        # systemd joins several StateDirectory= entries with ':'; ours have one
        return Path(env.split(":")[0])
    return STATE_ROOT / name


def refuse_retired(raw: dict, keys, job: str, files: str, *,
                   prefix: str = "") -> None:
    """Raise ``ValueError`` if *raw* still sets any of *keys*.

    The keys named state paths, which are now the job's convention
    (:func:`job_state_dir`), shared with choco: a key that is silently
    ignored leaves a config that lies about what runs, so it is refused
    with where *files* live now.  *prefix* names the section the keys sat
    in (``"state."``).
    """
    found = [prefix + k for k in keys if k in raw]
    if found:
        raise ValueError(
            f"{' and '.join(found)} retired: remove it; {files} in the job's "
            f"state directory ({job_state_dir(job)}; systemd's "
            f"StateDirectory=choco/{job}, or --state-dir)")


@contextlib.contextmanager
def atomic_output(path, *, suffix: str = "", mode: int = 0o644,
                  fsync: bool = False):
    """Yield a temp path beside *path*; rename it over *path* on a clean
    exit, remove it on any error, so a reader never sees a torn file.

    The temp name is hidden, unique and ends ``.tmp`` + *suffix*: the
    config mtime scan ignores ``.tmp``, and *suffix* serves writers that
    pick a format by extension (``".npz"`` for numpy, ``".png"`` for
    matplotlib).  *mode* is applied before the rename (mkstemp makes the
    file private, and choco and the jobs run as different users); *fsync*
    makes the data durable before the rename, for a commit protocol that
    depends on it.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{p.name}.", suffix=".tmp" + suffix,
                               dir=p.parent)
    os.close(fd)
    try:
        yield Path(tmp)
        os.chmod(tmp, mode)
        if fsync:
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        os.replace(tmp, p)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def write_text_atomic(path, text: str, **kw) -> None:
    """Write *text* to *path* through :func:`atomic_output`."""
    with atomic_output(path, **kw) as tmp:
        tmp.write_text(text)


def write_json_atomic(path, obj, indent: int = 2) -> None:
    """Write *obj* as JSON through :func:`atomic_output`, so a reader
    (choco's ``read_state_json``, the next run of the job) never sees a
    torn file."""
    write_text_atomic(path, json.dumps(obj, indent=indent))


# --- the run --------------------------------------------------------------
#
# Exit codes, read by choco's badge: 0 ok or nothing to do; 2 degraded (a
# dependency or input was unavailable, the next run retries); 1 failed (a
# config error or a bug, needs a human).

EXIT_OK, EXIT_FAILED, EXIT_DEGRADED = 0, 1, 2

#: ``run.json``'s ``status`` for each exit code.
RUN_STATUS = {EXIT_OK: "ok", EXIT_DEGRADED: "degraded", EXIT_FAILED: "failed"}


class Degraded(Exception):
    """A dependency or input was unavailable; retries self-heal.  Exit 2."""


def setup_logging(verbose: int = 0, log: logging.Logger | None = None) -> None:
    """The jobs' logging: warnings by default, ``-v`` info, ``-vv`` debug.

    *log*, a job's own logger, is held at info or below, for a job whose
    one-line summary of a run belongs in the journal every run.
    """
    logging.basicConfig(level=logging.WARNING - 10 * min(verbose, 2),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if log is not None:
        log.setLevel(min(log.getEffectiveLevel(), logging.INFO))


def failure(exc: BaseException, log: logging.Logger) -> tuple[int, str]:
    """``(exit code, reason)`` for an exception that ended a run.

    :class:`Degraded` and ``OSError`` (no data yet, a mount or a service
    down) are environmental: 2.  Anything else the caller chose to catch
    (``ValueError``, a YAML error) is a config error: 1.  Logs one line;
    ``-vv`` adds the traceback.
    """
    reason = f"{type(exc).__name__}: {exc}"
    log.error("%s", reason)
    log.debug("traceback:", exc_info=exc)
    if isinstance(exc, (Degraded, OSError)):
        return EXIT_DEGRADED, reason
    return EXIT_FAILED, reason


@contextlib.contextmanager
def single_run(path):
    """Hold an exclusive lock on *path* for a run, or yield False if another
    run holds it.

    systemd never starts a unit twice, but nothing stops a hand run racing
    the timer, and a job that appends to its outputs (the waterfall's
    PNGs, the mapmaker's accumulators) would corrupt them.  A lock file
    that cannot be opened yields True: a missing lock must not stop the
    job.
    """
    lock = Path(path)
    try:
        lock.parent.mkdir(parents=True, exist_ok=True)
        fh = open(lock, "w")
    except OSError as exc:
        logger.warning("could not open lock file %s: %s", lock, exc)
        yield True
        return
    try:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        yield True
    finally:
        fh.close()


def write_run(state_dir, rc: int, *, status: str | None = None, **facts) -> None:
    """Rewrite ``<state_dir>/run.json``: how this run went, every run.

    ``time``, ``status`` (from *rc*, unless the job has a finer word such
    as eigencal's ``skipped``) and ``exit_code``, then the job's *facts*.
    Why a run was not plain ok goes in ``reason`` (one line), ``error``
    or ``degraded`` (a list): choco's badge shows all three.  It is a
    window onto the run, not part of it, so a failed write is logged and
    never raises.
    """
    path = Path(state_dir) / RUN_NAME
    record = {"time": time.time(), "status": status or RUN_STATUS.get(rc, "failed"),
              "exit_code": rc, **facts}
    try:
        write_json_atomic(path, record)
    except OSError as exc:
        logger.warning("could not write %s: %s", path, exc)
