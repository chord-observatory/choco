"""Run a host tool -- ``systemctl``, ``journalctl``, ``dot`` -- the one way.

choco prefers a host tool to a Python dependency (CLAUDE.md), and every
caller ran one the same way: look it up on PATH, run it with a timeout,
log a failure.  The argv is always built from choco's own allowlists,
never from a caller's string.
"""

from __future__ import annotations

import logging
import shutil
import subprocess

logger = logging.getLogger(__name__)


def run_tool(argv, *, timeout: float, input: str | None = None,
             log_level: int = logging.DEBUG) -> subprocess.CompletedProcess | None:
    """Run *argv* and return its result, or None when the tool is not on
    PATH or the call itself failed (timeout, exec error).  A non-zero
    exit comes back for the caller to judge.

    The pipes are UTF-8 explicitly rather than by locale: kotekan's dot
    output carries ``×`` and ``·``, and choco runs as a systemd unit with
    no LANG to speak of, so a C-locale interpreter would raise
    UnicodeEncodeError, which no ``except SubprocessError`` catches.
    """
    argv = [str(a) for a in argv]
    if shutil.which(argv[0]) is None:
        return None
    try:
        return subprocess.run(argv, input=input, capture_output=True, text=True,
                              encoding="utf-8", timeout=timeout)
    except (subprocess.SubprocessError, OSError) as e:
        logger.log(log_level, f"{' '.join(argv[:3])} failed: {e}")
        return None
