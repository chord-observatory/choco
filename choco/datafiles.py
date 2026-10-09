"""Acquisition-directory scan for the data-files page.

kotekan writes visibility files into per-acquisition directories under a
few well-known roots (``.../kotekan_vis_files/full``, ``.../subset``).
This module answers one question about them — which directories exist,
and how much HDF5 data has landed in each — for ``/files``.

Three things shape the implementation.

The scan is **one level deep, by design**.  A root's children are
acquisitions and an acquisition's ``.h5`` files sit directly inside it,
so the count means "files in this acquisition" rather than "files
somewhere beneath this path".  That deliberately excludes kotekan's
``.partial`` staging subdirectory: a file still being written is not
yet part of the acquisition, and counting it would make a directory
look one file larger than it can actually be read as.  It also means a
root whose data is nested deeper (the archived ``old/`` layout) reports
zeros rather than silently descending into a different shape.

The scan runs in gevent's **threadpool**.  These roots are NFS mounts,
so ``stat`` is not the cheap local call it looks like — ~20k of them
cost a few hundred ms warm — and, more to the point, a wedged mount
blocks for as long as it likes.  In the hub that would take the sync
loop, the monitors and every other request down with it; in a thread it
costs one thread.  This is the same reasoning that put h5py in a
subprocess, one step down in weight because there is no C extension to
isolate, only a blocking syscall.

And it is **cached** for a TTL, like the gain archive: several viewers
on the page cost one scan, and file counts change on the timescale of a
kotekan acquisition, not of a page load.
"""

import logging
import os
import re
import stat as stat_mod
import time
from pathlib import Path

import gevent
import yaml
from gevent.lock import BoundedSemaphore
from markupsafe import Markup, escape

logger = logging.getLogger(__name__)

#: Files counted by the scan.
H5_SUFFIX = ".h5"
#: An acquisition's notes (tools/acqnotes), shown behind the table's info button.
NOTES_NAME = "README.md"
#: Notes larger than this are refused rather than rendered.
NOTES_MAX_BYTES = 256 * 1024
#: A root's acquisition history (tools/acqnotes/timeline.yaml), behind the
#: root header's Timeline button.
TIMELINE_NAME = "timeline.yaml"


def scan_dir(path: str) -> dict:
    """Count the ``.h5`` files sitting directly in one directory.

    Returns ``{"files", "bytes", "oldest", "newest", "notes", "error"}``.
    ``newest`` is the mtime of the most recently modified counted file
    (``None`` when there are none), which is what says whether an
    acquisition is still growing; ``oldest`` with it gives the span the
    files cover.  ``notes`` says whether the directory carries a
    ``README.md`` (written by ``tools/acqnotes``), noticed from the same
    listing at no extra cost.
    """
    files = 0
    total = 0
    oldest = newest = None
    notes = False
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.name == NOTES_NAME:
                    notes = True
                    continue
                if not entry.name.endswith(H5_SUFFIX):
                    continue
                try:
                    if not entry.is_file():
                        continue
                    st = entry.stat()
                except OSError:
                    # A file that vanished mid-scan (kotekan rotating a
                    # file out) is not an error worth reporting — it is
                    # simply not there any more.
                    continue
                files += 1
                total += st.st_size
                if newest is None or st.st_mtime > newest:
                    newest = st.st_mtime
                if oldest is None or st.st_mtime < oldest:
                    oldest = st.st_mtime
    except OSError as exc:
        return {"files": files, "bytes": total, "oldest": oldest,
                "newest": newest, "notes": notes,
                "error": f"{type(exc).__name__}: {exc}"}
    return {"files": files, "bytes": total, "oldest": oldest,
            "newest": newest, "notes": notes, "error": None}


def scan_root(root: str) -> dict:
    """Scan one root: its immediate subdirectories, newest first.

    Failures are collected, never raised, in the same spirit as
    ``Registry.reload``: an unreadable acquisition costs that row, and a
    missing root costs that section — neither costs the page.
    """
    path = str(root)
    started = time.monotonic()
    timeline = False
    try:
        with os.scandir(path) as it:
            entries = []
            for e in it:
                if e.name == TIMELINE_NAME:
                    timeline = True
                elif e.is_dir():
                    entries.append(e)
    except OSError as exc:
        logger.warning(f"data-files: cannot list {path}: {exc}")
        return {"path": path, "error": f"{type(exc).__name__}: {exc}",
                "dirs": [], "files": 0, "bytes": 0, "duration_s": 0.0,
                "timeline": False}

    dirs = []
    for entry in sorted(entries, key=lambda e: e.name):
        row = scan_dir(entry.path)
        row["name"] = entry.name
        row["path"] = entry.path
        dirs.append(row)

    # Newest acquisition first: on a page whose whole point is "what is
    # landing right now", the current one should not be at the bottom of
    # a 30-row table.  Directories with no files yet sort by name.
    dirs.sort(key=lambda d: (d["newest"] is not None, d["newest"] or 0,
                             d["name"]), reverse=True)
    return {
        "path": path,
        "error": None,
        "dirs": dirs,
        "timeline": timeline,
        "files": sum(d["files"] for d in dirs),
        "bytes": sum(d["bytes"] for d in dirs),
        "duration_s": time.monotonic() - started,
    }


def probe_root(path: str) -> dict:
    """Liveness probe for one root: is it there and does it answer?

    Deliberately a ``readdir`` and not just a ``stat``.  On NFS a bare
    ``stat`` of a mount point is frequently served from the attribute
    cache and keeps answering long after the server has stopped, so it
    would report a wedged mount as healthy.  Reading a single directory
    entry forces a round trip while staying O(1) — the count is the
    scan's job, not the badge's.
    """
    try:
        st = os.stat(path)
        if not stat_mod.S_ISDIR(st.st_mode):
            return {"path": path, "ok": False, "error": "not a directory"}
        with os.scandir(path) as it:
            next(iter(it), None)
    except OSError as exc:
        return {"path": path, "ok": False,
                "error": f"{type(exc).__name__}: {exc}"}
    return {"path": path, "ok": True, "error": None}


def probe_roots(paths) -> list[dict]:
    return [probe_root(p) for p in paths]


class DataFileScan:
    """The configured data roots, scanned on demand and cached.

    ``roots`` is the explicit list from ``config.yaml`` (``vis_files.roots``);
    an empty list leaves the page saying it is unconfigured rather than
    guessing at a mount point.
    """

    DEFAULT_TTL_S = 30.0

    def __init__(self, roots=(), ttl_s: float = DEFAULT_TTL_S):
        self.roots = [Path(r) for r in (roots or ())]
        self.ttl_s = float(ttl_s)
        self._result: dict | None = None
        self._lock = BoundedSemaphore(1)
        # Badge state, kept by the health probe rather than the scan:
        # the strip polls every 30 s and must never walk the mount.
        self.health: str = "unknown" if self.configured else "unconfigured"
        self.root_status: list[dict] = []
        self.last_checked: float | None = None
        self.last_ok: float | None = None
        self.error: str | None = None
        self._probing = False
        self._running = False

    @property
    def configured(self) -> bool:
        return bool(self.roots)

    def _stale(self) -> bool:
        if self._result is None:
            return True
        return time.time() - self._result["scanned_at"] > self.ttl_s

    def get(self, force: bool = False) -> dict:
        """Return the cached scan, refreshing it if stale.

        Serialised the way ``GainArchive.refresh`` is: concurrent misses
        would otherwise each walk the mount.  Waiters re-check staleness
        after the lock and normally find the work already done.
        """
        if not self.configured:
            return {"configured": False, "roots": [], "scanned_at": None,
                    "duration_s": 0.0, "files": 0, "bytes": 0, "dirs": 0}
        with self._lock:
            if not force and not self._stale():
                return self._result
            self._result = self._scan()
            return self._result

    def _scan(self) -> dict:
        started = time.monotonic()
        # One thread per root, so two mounts are walked at once and a
        # slow one doesn't serialise behind a fast one.
        pool = gevent.get_hub().threadpool
        roots = list(pool.map(scan_root, [str(p) for p in self.roots]))
        duration = time.monotonic() - started
        logger.info(
            f"data-files: scanned {len(roots)} root(s), "
            f"{sum(r['files'] for r in roots)} files in {duration:.2f}s"
        )
        return {
            "configured": True,
            "roots": roots,
            "scanned_at": time.time(),
            "duration_s": duration,
            "dirs": sum(len(r["dirs"]) for r in roots),
            "files": sum(r["files"] for r in roots),
            "bytes": sum(r["bytes"] for r in roots),
        }


    # --- health probe (the DATA badge) -----------------------------------
    #
    # Separate from the scan on purpose.  The badge polls on the header
    # strip's cadence and only ever answers "is the filesystem there",
    # so it must cost one readdir per root, never a walk.

    CHECK_INTERVAL_S = 30
    # A wedged NFS mount blocks in the kernel and cannot be interrupted,
    # so the probe runs in a thread and the *greenlet* gives up waiting.
    # The thread stays stuck until the mount recovers; ``_probing`` is
    # what keeps a stuck one from being joined by a new one every tick.
    CHECK_TIMEOUT_S = 10.0

    def check_once(self) -> None:
        """Probe every root and update the badge state."""
        self.last_checked = time.time()
        if not self.configured:
            self.health = "unconfigured"
            self.error = None
            return
        if self._probing:
            # The previous probe never came back.  That *is* the answer;
            # spawning another would just pile up blocked threads.
            self.health = "down"
            self.error = (f"no response from the filesystem in "
                          f"{self.CHECK_TIMEOUT_S:.0f}s (mount not responding)")
            return

        paths = [str(p) for p in self.roots]
        self._probing = True
        result = gevent.get_hub().threadpool.spawn(probe_roots, paths)
        result.rawlink(self._probe_done)
        try:
            statuses = result.get(timeout=self.CHECK_TIMEOUT_S)
        except gevent.Timeout:
            self.health = "down"
            self.error = (f"no response from the filesystem in "
                          f"{self.CHECK_TIMEOUT_S:.0f}s (mount not responding)")
            logger.warning(f"data-files: probe timed out on {paths}")
            return
        except Exception as exc:                     # never kill the loop
            self.health = "down"
            self.error = f"{type(exc).__name__}: {exc}"
            return

        self.root_status = statuses
        n_ok = sum(1 for s in statuses if s["ok"])
        if n_ok == len(statuses):
            self.health = "ok"
            self.error = None
            self.last_ok = self.last_checked
        elif n_ok:
            # Some roots answered: the box is up, one mount is not.
            self.health = "degraded"
            self.error = "; ".join(f"{s['path']}: {s['error']}"
                                   for s in statuses if not s["ok"])
        else:
            self.health = "down"
            self.error = "; ".join(f"{s['path']}: {s['error']}"
                                   for s in statuses)

    def _probe_done(self, _result) -> None:
        self._probing = False

    def check_if_stale(self, max_age_s: float = 10.0) -> None:
        """Probe now if the last probe is older than ``max_age_s``."""
        if not self.configured:
            return
        if (self.last_checked is None
                or time.time() - self.last_checked > max_age_s):
            self.check_once()

    def run(self) -> None:
        """Probe forever on ``CHECK_INTERVAL_S``.  For gevent.spawn."""
        self._running = True
        while self._running:
            try:
                self.check_once()
            except Exception:
                logger.exception("DataFileScan.check_once raised")
            gevent.sleep(self.CHECK_INTERVAL_S)

    def stop(self) -> None:
        self._running = False

    def to_dict(self) -> dict:
        """Badge-shaped health, matching the hardware monitors."""
        return {
            "configured": self.configured,
            "health": self.health,
            "roots": [str(p) for p in self.roots],
            "root_status": self.root_status,
            "n_roots": len(self.roots),
            "n_ok": sum(1 for s in self.root_status if s["ok"]),
            "last_checked": self.last_checked,
            "last_seen": self.last_ok,
            "error": self.error,
        }


def read_notes(path: str, name: str = NOTES_NAME) -> str:
    """The text of one notes file (an acquisition's ``README.md``, a root's
    ``timeline.yaml``).  Blocking: run it in the threadpool.  Raises
    ``OSError`` (missing, unreadable, too large)."""
    with open(os.path.join(path, name), "rb") as fh:
        data = fh.read(NOTES_MAX_BYTES + 1)
    if len(data) > NOTES_MAX_BYTES:
        raise OSError(f"{name} is larger than {NOTES_MAX_BYTES} bytes")
    return data.decode("utf-8", errors="replace")


_CODE = re.compile(r"`([^`]+)`")
_BOLD = re.compile(r"\*\*(.+?)\*\*")
_EM = re.compile(r"(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?![*\w])")


def _inline(text: str) -> str:
    """Escape, then mark up `code`, **bold** and *emphasis*: nothing the
    file says is ever passed through as HTML."""
    s = str(escape(text))
    s = _CODE.sub(r"<code>\1</code>", s)
    s = _BOLD.sub(r"<strong>\1</strong>", s)
    return _EM.sub(r"<em>\1</em>", s)


def notes_html(text: str) -> Markup:
    """The Markdown subset the acquisition notes use, as HTML: headings,
    paragraphs, ``-`` lists, pipe tables, `code`, **bold** and *emphasis*.

    Deliberately not a Markdown library (see dependencies.md): every line
    is escaped before the few constructs are recognised, so a README can
    only ever produce these elements.
    """
    out, para, items, rows = [], [], [], []

    def flush():
        if para:
            out.append(f"<p>{_inline(' '.join(para))}</p>")
            para.clear()
        if items:
            out.append("<ul>" + "".join(f"<li>{_inline(i)}</li>" for i in items) + "</ul>")
            items.clear()
        if rows:
            head, body = rows[0], rows[1:]
            html = ["<table><thead><tr>"]
            html += [f"<th>{_inline(c)}</th>" for c in head]
            html.append("</tr></thead><tbody>")
            for r in body:
                html.append("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>")
            html.append("</tbody></table>")
            out.append("".join(html))
            rows.clear()

    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("|"):
            if para or items:
                flush()
            cells = [c.strip() for c in line.strip("|").split("|")]
            if all(set(c) <= set("-: ") and c for c in cells):
                continue  # the header separator row
            rows.append(cells)
            continue
        if rows:
            flush()
        if not line:
            flush()
        elif line.startswith("#"):
            flush()
            level = len(line) - len(line.lstrip("#"))
            tag = f"h{min(level + 3, 6)}"
            out.append(f"<{tag}>{_inline(line[level:].strip())}</{tag}>")
        elif line.startswith("- "):
            if para:
                flush()
            items.append(line[2:])
        elif items and raw.startswith("  "):
            items[-1] += " " + line  # a wrapped list item
        else:
            para.append(line)
    flush()
    return Markup("\n".join(out))


def timeline_rows(text: str) -> list[dict]:
    """``timeline.yaml`` as display rows, oldest first.

    Each row: ``when`` (the period, or "listed acquisitions"), ``topic``,
    ``text`` and ``source`` as safe HTML (via ``_inline``), ``swapped``
    and ``acqs`` as lists of plain strings.  Raises ``ValueError`` on a
    file that is not the expected shape.
    """
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"not valid YAML: {exc}") from None
    entries = doc.get("entries") if isinstance(doc, dict) else None
    if not isinstance(entries, list):
        raise ValueError("no `entries:` list")
    rows = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        start, end = e.get("from"), e.get("to")
        acqs = [str(a) for a in e.get("acqs") or []]
        if acqs:
            when = "listed acquisitions"
            m = re.match(r"acq_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})", min(acqs))
            key = "%s-%s-%s %s:%s" % m.groups() if m else min(acqs)
        else:
            when = f"{_when(start)} → {_when(end) if end else 'now'}"
            key = _when(start)
        rows.append({
            "key": key,
            "when": when,
            "topic": str(e.get("topic") or ""),
            "text": Markup(_inline(str(e.get("text") or ""))),
            "source": Markup(_inline(str(e.get("source") or ""))),
            "swapped": [str(s) for s in e.get("swapped") or []],
            "acqs": acqs,
        })
    rows.sort(key=lambda r: r["key"])
    return rows


def _when(v) -> str:
    """A timeline bound (YAML date, datetime or string) as 'YYYY-MM-DD[ HH:MM]'."""
    s = str(v or "").replace("T", " ")
    return s[:16] if len(s) > 10 else s


def human_bytes(n) -> str:
    """1024-based size for display.  ``None`` and 0 both read as '0 B'."""
    n = float(n or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"
