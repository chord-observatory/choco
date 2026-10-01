"""Loopback client for choco's JSON API, plus the jobs' atomic state write.

Stdlib only, because its callers are the jobs and the CLI, which talk to
choco on the same host.  Two conventions live here so they are written
once: the ``/update``, ``/oneshot`` and ``/api/*`` routes bypass login for
loopback callers, and choco's certificate is self-signed, so an ``https``
URL is fetched **unverified** — nothing on that wire leaves the host.
Transport failures surface as ``urllib.error.URLError`` / ``HTTPError``,
both ``OSError`` subclasses, which is what the jobs' exit-code convention
turns into "degraded".
"""

from __future__ import annotations

import json
import os
import ssl
import urllib.request
from pathlib import Path


def ssl_context(url: str) -> ssl.SSLContext | None:
    """An unverified TLS context for an ``https`` URL, None for plain HTTP."""
    if not url.startswith("https:"):
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def request_json(base_url: str, path: str, body=None, timeout: float = 10.0):
    """GET (``body`` None) or POST (``body`` JSON-encoded) one endpoint.

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
                                 method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout,
                                context=ssl_context(url)) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


def get_json(base_url: str, path: str, timeout: float = 10.0):
    return request_json(base_url, path, timeout=timeout)


def post_json(base_url: str, path: str, body, timeout: float = 10.0):
    return request_json(base_url, path, body=body, timeout=timeout)


#: Root of the jobs' state, as the systemd units lay it out
#: (``StateDirectory=choco/<job>``).  choco reads ``<state_dir>/<job>/...``
#: from its one ``state_dir`` setting, which defaults to this.
STATE_ROOT = Path("/var/lib/choco")

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


def write_json_atomic(path, obj, indent: int = 2) -> None:
    """Write *obj* as JSON via a temp file and rename, so a reader (choco's
    ``read_state_json``, the next run of the job) never sees a torn file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=indent))
    os.replace(tmp, p)
