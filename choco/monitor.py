"""What the header strip's background pollers share.

Each monitor polls one thing on a slow interval from its own greenlet,
holds the latest result in attributes, and is read by the page on its
own cadence; no callbacks, no events.  A page watching one monitor can
tighten its cadence with :meth:`PolledMonitor.poll_if_stale`.
"""

from __future__ import annotations

import logging
import time

import gevent

logger = logging.getLogger(__name__)


class PolledMonitor:
    """One thing polled every ``INTERVAL_S`` seconds, for ``gevent.spawn``."""

    INTERVAL_S = 30

    def __init__(self):
        # Unconfigured monitors never poll, so reflect that up front
        # instead of sitting on "unknown" forever.
        self.health: str = "unknown" if self.configured else "unconfigured"
        self.last_polled: float | None = None
        self.error: str | None = None

    @property
    def configured(self) -> bool:
        return True

    def poll_once(self) -> None:
        """One probe; leaves ``health`` / ``error`` / ``last_polled`` set."""
        raise NotImplementedError

    def poll_if_stale(self, max_age_s: float = 10.0) -> None:
        """Poll now if the last poll is older than *max_age_s*: a page
        being watched tightens the cadence without touching the loop."""
        if not self.configured:
            return
        if self.last_polled is None or time.time() - self.last_polled > max_age_s:
            self.poll_once()

    def run(self) -> None:
        """Poll forever.  A raising probe is logged, never fatal."""
        while True:
            try:
                self.poll_once()
            except Exception:
                logger.exception(f"{type(self).__name__}.poll_once raised")
            gevent.sleep(self.INTERVAL_S)


class HttpMonitor(PolledMonitor):
    """A monitor of one HTTP service at ``host:port``."""

    def __init__(self, host: str | None, port: int | None, timeout: float = 5.0):
        self.host = host
        self.port = int(port) if port else None
        self.timeout = float(timeout)
        super().__init__()

    @property
    def configured(self) -> bool:
        return bool(self.host) and bool(self.port)

    @property
    def base_url(self) -> str | None:
        return f"http://{self.host}:{self.port}" if self.configured else None
