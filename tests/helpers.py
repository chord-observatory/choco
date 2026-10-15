"""Helpers the route-level suites share; the fixtures live in conftest.py."""

from pathlib import Path

from choco.auth import save_user

#: What ``job_status`` reports for a healthy oneshot job between runs
#: (the ``jobs`` fixture's default).
JOB_STUB = {
    "health": "ok", "state_mtime": None, "result": "success",
    "systemd": True, "active_state": "inactive", "sub_state": "dead",
    "exit_status": "0", "state_file": None, "unit": "test.service",
}


def login(client):
    """Log *client* in as a test user: the session Flask-Login reads."""
    user = save_user("cn=tester,dc=example", "tester")
    with client.session_transaction() as sess:
        sess["_user_id"] = user.get_id()


def csrf(client, **kw):
    """Establish a session and return its CSRF token.

    The token is seeded lazily by the ``csrf_token`` context processor,
    so this must fetch a page that renders a CSRF-carrying form — the
    dashboard, not the landing page.  *kw* goes to that request (an
    ``environ_base`` naming the peer).
    """
    client.get("/nodes", **kw)
    with client.session_transaction() as sess:
        return sess["_csrf_token"]


def job_dir(app, job) -> Path:
    """<state_dir>/<job>/ of the test app, created."""
    d = Path(app.config["state_dir"]) / job
    d.mkdir(parents=True, exist_ok=True)
    return d
