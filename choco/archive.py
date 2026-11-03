"""What the remote archive holds, polled through the transfer host.

The transfer host (alpenhorn) pushes finished acquisitions and merged
baseband dumps to Fir with rsync and holds the only key that can ask
Fir anything.  Its ``kotekan-vis-verify`` socket (chord-sysops,
``roles/kotekan_vis_transfer``) answers one request per connection; the
one asked here is ``list -l``: one ``ls`` on Fir, one line per file it
holds, ``<dir>/<file> <bytes> <mtime>``, over every acquisition and
merged dump there (or ``error <text>``).

:class:`ArchiveMonitor` asks on a timer, like the header strip's
monitors, and keeps the latest listing; the ``/files`` page reads that
and never asks on its own.  Fir is read on its own, not through what is
here, so the listing also names what was deleted on site; joined to the
local scan by file name, size and mtime (rsync's quick check) it says of
each directory how much of it the archive holds.  A failed poll keeps
the previous listing and says when it was taken.
"""

from __future__ import annotations

import logging
import re
import socket
import time

from .acqtree import MERGED_NAME_RE
from .datafiles import H5_SUFFIX
from .monitor import PolledMonitor

logger = logging.getLogger(__name__)

#: The names the transfer host lists: an acquisition directory (its own
#: strict spelling) and a merged baseband dump (``acqtree.MERGED_NAME_RE``).
#: Any other first path component fails the answer, which a listing from
#: anything but that script would.
ACQ_NAME_RE = re.compile(r"^acq_[0-9]{8}_[0-9]{6}_[0-9]+$")
#: Longest answer read: ~100 bytes a file, tens of thousands of files.
REPLY_MAX_BYTES = 64 << 20
#: The request: the per-file listing of everything the remote holds.
LIST_REQUEST = "list -l\n"


class ArchiveMonitor(PolledMonitor):
    """The remote archive's listing, polled through the verify socket.

    ``host``/``port`` is the socket; ``root`` the configured data root the
    archive mirrors (the transfer's source directory).  Any part missing
    leaves the monitor unconfigured and the page without an Archive
    column.  ``dirs`` is the last listing that arrived, ``checked_at``
    when; ``health``/``error`` say how the latest poll went.
    """

    INTERVAL_S = 600
    DEFAULT_TIMEOUT_S = 300.0

    def __init__(self, host=None, port=None, root=None,
                 timeout_s: float = DEFAULT_TIMEOUT_S, interval_s: float | None = None):
        self.host = str(host) if host else None
        self.port = int(port) if port else None
        self.root = str(root).rstrip("/") if root else None
        self.timeout_s = float(timeout_s)
        if interval_s:
            self.INTERVAL_S = float(interval_s)
        super().__init__()
        self.dirs: dict[str, dict[str, tuple[int, int]]] = {}
        self.checked_at: float | None = None

    @property
    def configured(self) -> bool:
        return bool(self.host and self.port and self.root)

    def covers(self, root_path) -> bool:
        """Is *root_path* the root this archive mirrors?"""
        return self.root is not None and str(root_path).rstrip("/") == self.root

    def poll_once(self) -> None:
        """One request; a failure keeps the previous listing."""
        self.last_polled = time.time()
        try:
            raw = self._exchange(LIST_REQUEST)
            dirs = parse_listing(raw)
        except OSError as exc:
            self.health, self.error = "down", f"{self.host}:{self.port}: {exc}"
            logger.warning(f"archive: {self.error}")
            return
        except ValueError as exc:
            self.health, self.error = "down", str(exc)
            logger.warning(f"archive: {self.error}")
            return
        self.dirs, self.checked_at = dirs, self.last_polled
        self.health, self.error = "ok", None
        logger.info(f"archive: {len(dirs)} directories listed on {self.host}")

    def _exchange(self, request: str) -> str:
        """Send *request*, close our side, read the answer to EOF."""
        with socket.create_connection((self.host, self.port),
                                      timeout=self.timeout_s) as conn:
            conn.sendall(request.encode())
            conn.shutdown(socket.SHUT_WR)
            chunks = []
            size = 0
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                size += len(chunk)
                if size > REPLY_MAX_BYTES:
                    raise OSError("answer too long")
                chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", "replace")

    def to_dict(self) -> dict:
        return {
            "configured": self.configured,
            "health": self.health,
            "host": self.host,
            "port": self.port,
            "root": self.root,
            "interval_s": self.INTERVAL_S,
            "checked_at": self.checked_at,
            "last_polled": self.last_polled,
            "error": self.error,
            "dirs": len(self.dirs),
        }


def parse_listing(text: str) -> dict[str, dict[str, tuple[int, int]]]:
    """``{dir: {file path: (bytes, mtime)}}`` from a ``list -l`` answer.

    An ``error`` line or a line of another shape is a ``ValueError``
    naming the problem.  An empty answer is an empty archive.
    """
    dirs: dict[str, dict[str, tuple[int, int]]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("error "):
            raise ValueError(f"the transfer host says: {line[6:]}")
        parts = line.rsplit(None, 2)
        name, _, path = parts[0].partition("/") if len(parts) == 3 else ("", "", "")
        if not path or not (ACQ_NAME_RE.match(name) or MERGED_NAME_RE.match(name)):
            raise ValueError(f"unexpected answer line: {line[:80]!r}")
        try:
            size, mtime = int(parts[1]), int(parts[2])
        except ValueError:
            raise ValueError(f"unexpected answer line: {line[:80]!r}") from None
        dirs.setdefault(name, {})[path] = (size, mtime)
    return dirs


def compare_dir(listing: dict | None, local: dict | None) -> dict:
    """What the archive holds of one directory.

    *listing* is the directory's files on the archive (``parse_listing``),
    *local* the scan's ``h5`` map of the files here (``{name: (bytes,
    mtime)}``); either may be None.  Returns ``files``/``bytes``/
    ``oldest``/``newest`` for the ``.h5`` files sitting directly in the
    archive's copy (the scan's own rule, so the two counts compare) and
    ``held``, how many of the local files the archive has at the same
    size and mtime: what rsync's quick check would not send again.
    """
    listing = listing or {}
    files = total = 0
    oldest = newest = None
    for path, (size, mtime) in listing.items():
        if "/" in path or not path.endswith(H5_SUFFIX):
            continue
        files += 1
        total += size
        oldest = mtime if oldest is None or mtime < oldest else oldest
        newest = mtime if newest is None or mtime > newest else newest
    held = 0
    for name, (size, mtime) in (local or {}).items():
        there = listing.get(name)
        if there is not None and there[0] == size and there[1] == int(mtime):
            held += 1
    return {"files": files, "bytes": total, "oldest": oldest, "newest": newest,
            "held": held}


def join_root(root: dict, dirs: dict, name_re: re.Pattern) -> tuple[list[dict], dict]:
    """The scan's rows for one root joined with the archive's listing.

    Returns ``(rows, summary)``: a row per directory on either side, in
    the scan's order (newest first), each a copy of the scan's row (or,
    for a directory only the archive has, a row shaped like one with
    ``on_site`` False and the archive's count, size and span in its
    place) plus ``archive`` (``compare_dir``); and the summary's counts
    of what the archive holds in ``full``, in ``part``, ``none`` of, and
    ``archive_only``.  *name_re* picks the listing's names that belong
    to this root.
    """
    rows = []
    summary = {"full": 0, "part": 0, "none": 0, "archive_only": 0}
    seen = set()
    for d in root.get("dirs") or []:
        seen.add(d["name"])
        a = compare_dir(dirs.get(d["name"]), d.get("h5"))
        rows.append({**d, "on_site": True, "archive": a})
        if not d["files"]:
            continue
        summary["full" if a["held"] == d["files"] else "part" if a["held"] else "none"] += 1
    for name in dirs:
        if name in seen or not name_re.match(name):
            continue
        a = compare_dir(dirs[name], None)
        rows.append({"name": name, "path": None, "files": a["files"], "bytes": a["bytes"],
                     "oldest": a["oldest"], "newest": a["newest"], "notes": False,
                     "error": None, "h5": {}, "on_site": False, "archive": a})
        summary["archive_only"] += 1
    rows.sort(key=lambda d: (d["newest"] is not None, d["newest"] or 0, d["name"]),
              reverse=True)
    return rows, summary
