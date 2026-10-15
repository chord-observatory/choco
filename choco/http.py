"""One way to make an HTTP request from the web process.

Every caller used ``requests`` directly, each with its own retry and its
own idea of what to catch; this is the one place those live.  Failures
are ``OSError``: requests' own exceptions are OSErrors
(``RequestException`` subclasses ``IOError``), and under gevent a
connection can also fail with a bare ``ConnectionError`` that requests
never wraps -- catching only ``RequestException`` once let that one
escape a poll (stale health) and a PDB route (a 500).  A reply with an
error status raises ``requests.HTTPError`` (an OSError too) unless the
caller accepts that status.
"""

from __future__ import annotations

import logging

import gevent
import requests

logger = logging.getLogger(__name__)

#: Pause between a failed attempt and its retry.  A monitor polls every
#: 30 s, so a single dropped request would otherwise show a service as
#: down for a whole interval; one retry a second later turns a blip back
#: into a non-event while a real outage still fails within ~2*timeout.
RETRY_DELAY_S = 1.0


def fetch(method: str, url: str, *, timeout: float, retries: int = 0,
          retry_delay: float | None = None, accept_statuses=(),
          check: bool = True, **kwargs) -> requests.Response:
    """``requests.request`` with *retries* further attempts after a failure.

    An error status raises unless it is in *accept_statuses* or *check*
    is off (the caller reads the status itself).  *retry_delay* is the
    pause between attempts (default :data:`RETRY_DELAY_S`); the node
    probes pass 0, since connection-refused is instant and a blackhole
    already costs the timeout.  Raises ``OSError`` after the last attempt.
    """
    delay = RETRY_DELAY_S if retry_delay is None else retry_delay
    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if check and resp.status_code not in accept_statuses:
                resp.raise_for_status()
            return resp
        except OSError as e:
            if attempt >= retries:
                raise
            logger.debug(f"{method} {url}: {type(e).__name__}: {e}; retrying")
            if delay:
                gevent.sleep(delay)
    raise AssertionError("unreachable")
