"""Helpers the route-level suites share; the fixtures live in conftest.py."""

import copy
from pathlib import Path

from choco.auth import save_user

#: A transit forecast as the gnss-fengine service wrote it on 2026-11-04
#: (2 h, 10 passes); ``retimed`` moves it to the present.
TRANSIT_SAMPLE = Path(__file__).parent / "data" / "transit_windows.json"

#: The F-engine gain archive's manifest, as choco.h5read reports it.
GAIN_MANIFEST = {
    "datasets": [
        {"name": "gain_coeff", "value_type": "complex64",
         "extents": [1, 8192, 128],
         "dimnames": ["update_time", "freq", "input"], "bytes": 8388608},
        {"name": "gain_exp", "value_type": "int32", "extents": [1, 128],
         "dimnames": ["update_time", "input"], "bytes": 512},
    ],
    "attrs": {"acquisition_name": "20260808T053625Z_digitalgain"},
    "scalars": {"update_id": ["digitalgain_20260808T053625.917371Z"],
                "index_map/update_time": [1786167385.917371]},
    "index_map": {"freq": {"n": 8192, "first_mhz": 0.0,
                           "last_mhz": 1599.8046875},
                  "inputs": {"n": 128, "names": ["chord_pathfinder000000"]}},
}

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


def configure_gain_archive(app, payload=b"\x01\x02\x03\x04"):
    """A gain archive with its cache pre-filled, and the FPGA monitor
    configured to match: no fpga_master, no h5py."""
    archive = app.config["gain_archive"]
    archive.base_url = "http://fpga.example:54321"
    archive._manifest = GAIN_MANIFEST
    archive._data = {"gain_exp": payload}
    archive._fetched_at = 1e12          # never stale during a test
    monitor = app.config["fpga_monitor"]
    monitor.host, monitor.port = "fpga.example", 54321
    app.config["fpga_cfg"] = {"host": monitor.host, "port": monitor.port}
    return archive


def retimed(forecast: dict, created: float) -> dict:
    """A transit forecast moved in time so that it was made at *created*."""
    out = copy.deepcopy(forecast)
    shift = created - forecast["created_unix"]
    for k in ("created_unix", "start_unix", "end_unix"):
        out[k] += shift
    for p in out["transits"]:
        for k in ("start_unix", "end_unix", "closest_unix"):
            p[k] += shift
    out["quiet"] = {b: [[a + shift, c + shift] for a, c in w] for b, w in out["quiet"].items()}
    out["quiet_by_band"] = {b: [[a + shift, c + shift] for a, c in w] for b, w in out["quiet_by_band"].items()}
    return out
