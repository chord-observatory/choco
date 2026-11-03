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

import json
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

from .acqtree import COARSE_CHANNEL_MHZ, MERGE_LOG_NAME, MERGED_NAME_RE
from .h5read import run as h5read
from .monitor import PolledMonitor

logger = logging.getLogger(__name__)

#: Files counted by the scan.
H5_SUFFIX = ".h5"
#: An acquisition's notes (the acqnotes job, jobs/acqnotes), shown behind the table's info button.
NOTES_NAME = "README.md"
#: Notes larger than this are refused rather than rendered.
NOTES_MAX_BYTES = 256 * 1024
#: A root's acquisition history (jobs/acqnotes/timeline.yaml), behind the
#: root header's Timeline button.
TIMELINE_NAME = "timeline.yaml"


def run_blocking(fn, *args, timeout: float):
    """``fn(*args)`` in gevent's threadpool, waited on for at most *timeout*.

    The one way the web process touches NFS.  A timeout raises
    ``TimeoutError`` (an ``OSError``), so a caller treats a wedged mount
    like any other unreadable file.  Only the greenlet gives up: a thread
    blocked in the kernel cannot be interrupted and stays until the mount
    answers.
    """
    try:
        return gevent.get_hub().threadpool.spawn(fn, *args).get(timeout=timeout)
    except gevent.Timeout:
        raise TimeoutError(
            f"no answer from the filesystem in {timeout:.0f}s") from None


def scan_dir(path: str, notes_name: str = NOTES_NAME) -> dict:
    """Count the ``.h5`` files sitting directly in one directory.

    Returns ``{"files", "bytes", "oldest", "newest", "notes", "h5", "error"}``.
    ``newest`` is the mtime of the most recently modified counted file
    (``None`` when there are none), which is what says whether an
    acquisition is still growing; ``oldest`` with it gives the span the
    files cover.  ``h5`` is each counted file's ``(bytes, mtime)`` by
    name, what the archive's listing is joined on.  ``notes`` says
    whether the directory carries a *notes_name* file (``README.md``,
    written by the acqnotes job), noticed from the same listing at no
    extra cost.
    """
    files = 0
    total = 0
    oldest = newest = None
    notes = False
    h5 = {}
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.name == notes_name:
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
                h5[entry.name] = (st.st_size, int(st.st_mtime))
                if newest is None or st.st_mtime > newest:
                    newest = st.st_mtime
                if oldest is None or st.st_mtime < oldest:
                    oldest = st.st_mtime
    except OSError as exc:
        return {"files": files, "bytes": total, "oldest": oldest,
                "newest": newest, "notes": notes, "h5": h5,
                "error": f"{type(exc).__name__}: {exc}"}
    return {"files": files, "bytes": total, "oldest": oldest,
            "newest": newest, "notes": notes, "h5": h5, "error": None}


def scan_root(root: str, dir_re: re.Pattern | None = None,
              notes_name: str = NOTES_NAME) -> dict:
    """Scan one root: its immediate subdirectories, newest first.

    *dir_re* keeps only the subdirectories it matches (the baseband
    root holds the raw per-node dumps beside the merged ones); *notes_name*
    is the file that gives a row its info button.

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
                elif e.is_dir() and (dir_re is None or dir_re.match(e.name)):
                    entries.append(e)
    except OSError as exc:
        logger.warning(f"data-files: cannot list {path}: {exc}")
        return {"path": path, "error": f"{type(exc).__name__}: {exc}",
                "dirs": [], "files": 0, "bytes": 0, "duration_s": 0.0,
                "timeline": False}

    dirs = []
    for entry in sorted(entries, key=lambda e: e.name):
        row = scan_dir(entry.path, notes_name)
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


class DataFileScan(PolledMonitor):
    """The configured data roots, scanned on demand and cached, and the
    DATA badge's liveness probe of them, polled.

    ``roots`` is the explicit list from ``config.yaml`` (``vis_files.roots``);
    an empty list leaves the page saying it is unconfigured rather than
    guessing at a mount point.  ``baseband`` (``vis_files.baseband``) is
    the dumps' root, scanned for its ``*_merged`` directories the same
    way and reported apart from the acquisition roots.
    """

    DEFAULT_TTL_S = 30.0
    #: How long one scan waits for a root's walk.  A root that does not
    #: finish in time reports an error row; its walk keeps its thread and
    #: is picked up, not restarted, by the next scan.
    SCAN_TIMEOUT_S = 30.0

    def __init__(self, roots=(), ttl_s: float = DEFAULT_TTL_S, baseband=None):
        self.roots = [Path(r) for r in (roots or ())]
        self.baseband = Path(baseband) if baseband else None
        self.ttl_s = float(ttl_s)
        self._result: dict | None = None
        self._lock = BoundedSemaphore(1)
        self._walks: dict = {}                  # root -> its unfinished walk
        # Badge state, kept by the health probe rather than the scan:
        # the strip polls every 30 s and must never walk the mount.
        super().__init__()
        self.root_status: list[dict] = []
        self.last_ok: float | None = None
        self._probing = False

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
            return {"configured": False, "roots": [], "baseband": None, "scanned_at": None,
                    "duration_s": 0.0, "files": 0, "bytes": 0, "dirs": 0}
        with self._lock:
            if not force and not self._stale():
                return self._result
            self._result = self._scan()
            return self._result

    def _scan(self) -> dict:
        started = time.monotonic()
        # One thread per root, so two mounts are walked at once and a
        # slow one doesn't serialise behind a fast one.  A walk still
        # running from an earlier scan is waited on again rather than
        # joined by a second one: a wedged mount holds one thread, not
        # one per page load (the pool is shared with gevent's resolver).
        pool = gevent.get_hub().threadpool
        paths = [str(p) for p in self.roots]
        for p in paths:
            if p not in self._walks:
                self._walks[p] = pool.spawn(scan_root, p)
        # The baseband root is one more walk of the same kind, kept apart
        # in the result: its directories are dumps, not acquisitions.
        baseband = str(self.baseband) if self.baseband else None
        if baseband and baseband not in self._walks:
            self._walks[baseband] = pool.spawn(scan_root, baseband, MERGED_NAME_RE, MERGE_LOG_NAME)
        deadline = started + self.SCAN_TIMEOUT_S
        roots = []
        for p in paths + ([baseband] if baseband else []):
            walk = self._walks[p]
            try:
                roots.append(walk.get(timeout=max(0.0, deadline - time.monotonic())))
            except gevent.Timeout:
                logger.warning(f"data-files: scan of {p} timed out")
                roots.append({"path": p, "dirs": [], "files": 0, "bytes": 0,
                              "duration_s": self.SCAN_TIMEOUT_S, "timeline": False,
                              "error": f"no answer from the filesystem in "
                                       f"{self.SCAN_TIMEOUT_S:.0f}s"})
                continue
            except Exception as exc:                 # never break the page
                roots.append({"path": p, "dirs": [], "files": 0, "bytes": 0,
                              "duration_s": 0.0, "timeline": False,
                              "error": f"{type(exc).__name__}: {exc}"})
            del self._walks[p]
        duration = time.monotonic() - started
        logger.info(
            f"data-files: scanned {len(roots)} root(s), "
            f"{sum(r['files'] for r in roots)} files in {duration:.2f}s"
        )
        baseband_root = roots.pop() if baseband else None
        return {
            "configured": True,
            "roots": roots,
            "baseband": baseband_root,
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

    INTERVAL_S = 30
    # A wedged NFS mount blocks in the kernel and cannot be interrupted,
    # so the probe runs in a thread and the *greenlet* gives up waiting.
    # The thread stays stuck until the mount recovers; ``_probing`` is
    # what keeps a stuck one from being joined by a new one every tick.
    CHECK_TIMEOUT_S = 10.0

    def poll_once(self) -> None:
        """Probe every root and update the badge state."""
        self.last_polled = time.time()
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

        paths = [str(p) for p in self.roots + ([self.baseband] if self.baseband else [])]
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
            self.last_ok = self.last_polled
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

    def to_dict(self) -> dict:
        """Badge-shaped health, matching the hardware monitors."""
        return {
            "configured": self.configured,
            "health": self.health,
            "roots": [str(p) for p in self.roots + ([self.baseband] if self.baseband else [])],
            "root_status": self.root_status,
            "n_roots": len(self.roots) + (1 if self.baseband else 0),
            "n_ok": sum(1 for s in self.root_status if s["ok"]),
            "last_polled": self.last_polled,
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


#: The merge log's lines worth showing: the stamped start and end, what
#: was scanned, and the merged shape.  The rest is progress.
_MERGE_LOG_KEEP = re.compile(r"^(=== |scanning |merged: |split into |\d+ node\(s\))")
#: The merged dump's data: one part file's dataset, ``[T, F, P, D]``.
BASEBAND_DATASET = "baseband_subset"
#: Manifests of the dumps' first parts, keyed by path with the file's
#: (mtime, size): a dump is read once, however often its info is opened.
_baseband_manifests: dict[str, tuple[tuple, dict]] = {}


def read_baseband_dir(path: str) -> dict:
    """What a merged dump's directory says about itself.  Blocking: run
    it in the threadpool.  Returns the merge log's stamped lines
    (``log``) and the first part file (``first``: path, mtime, size),
    either absent when missing.
    """
    out = {"log": [], "first": None}
    names = sorted(n for n in os.listdir(path) if n.endswith(H5_SUFFIX))
    if names:
        st = os.stat(os.path.join(path, names[0]))
        out["first"] = (os.path.join(path, names[0]), st.st_mtime, st.st_size)
    try:
        with open(os.path.join(path, MERGE_LOG_NAME), "rb") as fh:
            text = fh.read(4 << 20).decode("utf-8", errors="replace")
    except OSError:
        return out
    out["log"] = [l.rstrip() for l in text.splitlines() if _MERGE_LOG_KEEP.match(l)][:40]
    return out


def baseband_facts(first: tuple | None, n_files: int) -> dict:
    """The facts a merged dump's part file carries, read once through
    the h5read subprocess (cooperative under the monkey-patching, so
    not the threadpool).  *first* is ``read_baseband_dir``'s; None, or
    a file without the dataset, gives ``{}``.  Raises ``RuntimeError``
    when the read fails.
    """
    if first is None:
        return {}
    path, mtime, size = first
    key = (mtime, size)
    cached = _baseband_manifests.get(path)
    if cached is None or cached[0] != key:
        manifest = json.loads(h5read("manifest", path).decode())
        _baseband_manifests[path] = (key, manifest)
    else:
        manifest = cached[1]
    dset = next((d for d in manifest.get("datasets") or [] if d["name"] == BASEBAND_DATASET), None)
    if dset is None:
        return {}
    # The attributes are whatever the file carries: each is used only in
    # the shape expected, so an odd file gives blanks, not a traceback.
    a = dset.get("attrs") or {}
    number = lambda v: v if isinstance(v, (int, float)) and not isinstance(v, bool) else None
    extents = [n for n in dset.get("extents") or [] if isinstance(n, int)]
    samples = extents[0] if extents else None
    seq_ns = number(a.get("seq_length_nsec"))
    start_ns = number(a.get("fpga_seq_time_nsec"))
    coarse = [c for c in (a.get("coarse_freq") if isinstance(a.get("coarse_freq"), list) else [])
              if number(c) is not None]
    dim_names = a.get("dim_names")
    facts = {
        "start": start_ns / 1e9 if start_ns else None,
        "samples": samples,
        "sample_ns": seq_ns,
        "file_s": samples * seq_ns / 1e9 if samples and seq_ns else None,
        "channels": len(coarse) or (extents[1] if len(extents) > 1 else None),
        "chan_first": coarse[0] if coarse else None,
        "chan_last": coarse[-1] if coarse else None,
        "mhz_first": coarse[0] * COARSE_CHANNEL_MHZ if coarse else None,
        "mhz_last": (coarse[-1] + 1) * COARSE_CHANNEL_MHZ if coarse else None,
        "pols": a.get("num_polarizations"),
        "dishes": a.get("num_dishes"),
        "sample_type": a.get("type"),
        "shape": extents,
        "dim_names": dim_names if isinstance(dim_names, list) else None,
        "bytes": dset.get("bytes"),
        "sources": a.get("merge_num_sources"),
        "merge_tool": a.get("merge_tool"),
        "gains_file": os.path.basename(str(manifest.get("attrs", {}).get("digital_gains_source_file") or "")) or None,
        "n_files": n_files,
    }
    facts["total_s"] = facts["file_s"] * n_files if facts["file_s"] and n_files else None
    facts["start_utc"] = (time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(facts["start"]))
                          if facts["start"] else None)
    return facts


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


def utc_minute(ts, short: bool = False) -> str:
    """An mtime as 'YYYY-MM-DD HH:MM' UTC (``short``: 'MM-DD HH:MM')."""
    if not ts:
        return "—"
    return time.strftime("%m-%d %H:%M" if short else "%Y-%m-%d %H:%M", time.gmtime(ts))


def human_bytes(n) -> str:
    """1024-based size for display.  ``None`` and 0 both read as '0 B'."""
    n = float(n or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"
