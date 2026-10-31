"""The remote archive's live word on the acquisitions, through the transfer host.

The transfer host (alpenhorn) pushes finished acquisitions to Fir with
rsync and holds the only key that can ask Fir anything.  Its
``kotekan-vis-verify`` socket (chord-sysops, ``roles/kotekan_vis_transfer``)
runs one dry-run rsync per connection and answers, per acquisition, how
many files are there and how many a transfer would still send: missing
on Fir, or of another size or mtime.  Nothing is recorded on either side,
so "all there" means Fir said so a moment ago, which is what a person
about to delete the local copy wants to know; a flag written at transfer
time would only say that a copy existed once.

The protocol is one line of acquisition names in, one line per
acquisition out (``<name> <files> <missing>``), or ``error <text>``.
Only names the cached ``/files`` scan listed go out, never a caller's.
The socket is a plain gevent-cooperative connection; the answer takes
as long as the dry-run rsync (seconds for the whole tree).
"""

from __future__ import annotations

import logging
import re
import socket
import time

logger = logging.getLogger(__name__)

#: What the transfer host accepts as an acquisition directory name.  Any
#: other scan row is left out of the question: one bad name fails the
#: whole answer there.
ACQ_NAME_RE = re.compile(r"^acq_[0-9]{8}_[0-9]{6}_[0-9]+$")
#: Longest answer read, in bytes: a short line per acquisition.
REPLY_MAX_BYTES = 1 << 20


class ArchiveCheck:
    """One root's archive, asked live.

    ``host``/``port`` is the verify socket; ``root`` the configured data
    root the archive mirrors (the transfer's source directory).  Any
    part missing leaves the check unconfigured and the page without an
    Archive column.
    """

    DEFAULT_TIMEOUT_S = 120.0

    def __init__(self, host=None, port=None, root=None,
                 timeout_s: float = DEFAULT_TIMEOUT_S):
        self.host = str(host) if host else None
        self.port = int(port) if port else None
        self.root = str(root).rstrip("/") if root else None
        self.timeout_s = float(timeout_s)

    @property
    def configured(self) -> bool:
        return bool(self.host and self.port and self.root)

    def covers(self, root_path) -> bool:
        """Is *root_path* the root this archive mirrors?"""
        return self.root is not None and str(root_path).rstrip("/") == self.root

    def query(self, names) -> dict:
        """Ask about *names* (acquisition directory names).

        Returns ``{"ok", "error", "dirs", "checked_at"}``; ``dirs`` maps
        each answered name to ``{"files", "missing"}``.  Names that are
        not acquisition names are not asked about and are absent from
        ``dirs``.  A failure (no connection, an ``error`` line, an
        answer that does not parse) is ``ok=False`` with the reason.
        """
        asked = [n for n in names if ACQ_NAME_RE.match(n)]
        result = {"ok": False, "error": None, "dirs": {},
                  "checked_at": time.time()}
        if not asked:
            result["ok"] = True
            return result
        try:
            raw = self._exchange(" ".join(asked) + "\n")
        except OSError as exc:
            result["error"] = f"{self.host}:{self.port}: {exc}"
            logger.warning(f"archive check: {result['error']}")
            return result
        try:
            result["dirs"] = parse_reply(raw)
        except ValueError as exc:
            result["error"] = str(exc)
            logger.warning(f"archive check: {exc}")
            return result
        result["ok"] = True
        return result

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


def parse_reply(text: str) -> dict:
    """``{name: {"files", "missing"}}`` from the socket's answer.

    An ``error`` line, an empty answer or a line of another shape is a
    ``ValueError`` naming the problem.
    """
    dirs = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("error "):
            raise ValueError(f"the transfer host says: {line[6:]}")
        parts = line.split()
        if len(parts) != 3 or not ACQ_NAME_RE.match(parts[0]):
            raise ValueError(f"unexpected answer line: {line[:80]!r}")
        try:
            files, missing = int(parts[1]), int(parts[2])
        except ValueError:
            raise ValueError(f"unexpected answer line: {line[:80]!r}") from None
        dirs[parts[0]] = {"files": files, "missing": missing}
    if not dirs:
        raise ValueError("empty answer from the transfer host")
    return dirs
