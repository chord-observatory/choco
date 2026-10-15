"""Every route's exposure, swept from the URL map rather than written per
route, so a route added without its decorator fails here, not in review.

The rules (CLAUDE.md, "Auth and exposure"): every route needs a login
except the cross-host aggregate images and ``/metrics``; ``/update/*``,
``/oneshot/*`` and ``/api/*`` also admit loopback callers (the jobs and the
CLI); every state-changing request from anywhere else checks the CSRF
token, and on those three prefixes must be JSON as well.
"""

import re

import pytest
import responses

from choco.app import create_app
from tests.helpers import csrf, login

#: Reachable without a login, from anywhere.
PUBLIC = {"web.metrics", "web.skymap_png", "web.skymap_night_png", "web.login", "static"}

#: The routes ``localhost_or_login_required`` opens to loopback callers.
LOOPBACK_PREFIXES = ("/update/", "/oneshot/", "/api/")

#: Under /api/ but login-only, deliberately: nothing on the host reads the
#: F-engine's gains, so loopback callers get no more than they need.
LOGIN_ONLY_API = {"/api/fpga/gain-data"}

#: Path arguments that name things the conftest registry has (a ``path:``
#: converter may carry a slash, a plain one may not).
ARGS = {"group": "cx", "node": "cx1", "node_key": "cx/cx1", "action": "start",
        "name": "bffs", "path:name": "cx/cx1.yaml", "root": "subset", "acq": "acq_x",
        "shard": "e0000", "root_idx": "0", "filename": "choco.css"}

REMOTE = {"environ_base": {"REMOTE_ADDR": "10.222.0.54"}}
LOCAL = {"environ_base": {"REMOTE_ADDR": "127.0.0.1"}}


def _routes():
    """``(method, rule, endpoint, url)`` for every route, built once."""
    app = create_app(configs_dir="/nonexistent")
    out = []
    for r in app.url_map.iter_rules():
        url = re.sub(r"<(?:([^:>]+):)?([^>]+)>",
                     lambda m: ARGS.get(f"{m.group(1)}:{m.group(2)}", ARGS.get(m.group(2), "x")),
                     r.rule)
        for method in sorted(r.methods - {"HEAD", "OPTIONS"}):
            out.append((method, r.rule, r.endpoint, url))
    return sorted(out)


ROUTES = _routes()
GUARDED = [r for r in ROUTES if r[2] not in PUBLIC]
_id = "{0[0]} {0[1]}".format


def _to_login(resp) -> bool:
    return resp.status_code == 302 and "/login" in resp.headers.get("Location", "")


@responses.activate                        # no request leaves the test
@pytest.mark.parametrize("route", GUARDED, ids=[_id(r) for r in GUARDED])
def test_a_remote_peer_must_log_in(client, route):
    method, _rule, _endpoint, url = route
    assert _to_login(client.open(url, method=method, **REMOTE))


@responses.activate
@pytest.mark.parametrize("route", [r for r in ROUTES if r[2] in PUBLIC and r[0] == "GET"],
                         ids=lambda r: _id(r))
def test_the_public_routes_need_no_login(client, jobs, route):
    method, _rule, _endpoint, url = route
    assert not _to_login(client.open(url, method=method, **REMOTE))


LOOPBACK_GETS = [r for r in GUARDED if r[1].startswith(LOOPBACK_PREFIXES)
                 and r[0] == "GET" and r[1] not in LOGIN_ONLY_API]


@responses.activate
@pytest.mark.parametrize("route", LOOPBACK_GETS, ids=[_id(r) for r in LOOPBACK_GETS])
def test_loopback_reaches_the_api_without_a_login(client, jobs, route):
    method, _rule, _endpoint, url = route
    assert not _to_login(client.open(url, method=method, **LOCAL))


NOT_LOOPBACK = [r for r in GUARDED if r[0] == "GET"
                and (not r[1].startswith(LOOPBACK_PREFIXES) or r[1] in LOGIN_ONLY_API)]


@responses.activate
@pytest.mark.parametrize("route", NOT_LOOPBACK, ids=[_id(r) for r in NOT_LOOPBACK])
def test_nothing_else_is_open_to_loopback(client, route):
    method, _rule, _endpoint, url = route
    assert _to_login(client.open(url, method=method, **LOCAL))


MUTATING = [r for r in GUARDED if r[0] in ("POST", "PUT")]


@responses.activate
@pytest.mark.parametrize("route", MUTATING, ids=[_id(r) for r in MUTATING])
def test_a_state_change_needs_the_csrf_token(client, jobs, route, monkeypatch):
    """The 403 must be the token check's own, not some other refusal (and
    it comes first: nothing runs, a pull included)."""
    from choco import auth
    checked = []
    real = auth.require_csrf_token
    monkeypatch.setattr(auth, "require_csrf_token",
                        lambda token: (checked.append(token), real(token)))
    method, _rule, _endpoint, url = route
    login(client)
    csrf(client, **REMOTE)                 # the session has a token; the request omits it
    resp = client.open(url, method=method, json={}, **REMOTE)
    assert checked == [""] and resp.status_code == 403


API_WRITES = [r for r in MUTATING if r[1].startswith(LOOPBACK_PREFIXES)]


@responses.activate
@pytest.mark.parametrize("route", API_WRITES, ids=[_id(r) for r in API_WRITES])
def test_a_remote_api_write_must_be_json(client, jobs, route):
    """A cross-site HTML form can carry neither the header nor a JSON body;
    with the token but a form body the write is still refused."""
    method, _rule, _endpoint, url = route
    login(client)
    token = csrf(client, **REMOTE)
    resp = client.open(url, method=method, data={"dry_run": "1"},
                       headers={"X-CSRF-Token": token}, **REMOTE)
    assert resp.status_code == 415


def test_a_remote_api_write_with_token_and_json_goes_through(client, configs_dir):
    login(client)
    token = csrf(client, **REMOTE)
    resp = client.put("/api/configs/cx/cx1.yaml", json={"content": "num_elements: 4\n"},
                      headers={"X-CSRF-Token": token}, **REMOTE)
    assert resp.status_code == 200
    assert (configs_dir / "cx" / "cx1.yaml").read_text() == "num_elements: 4\n"


def test_loopback_writes_need_neither_login_nor_token(client, configs_dir):
    """The jobs and the CLI: unchanged."""
    resp = client.put("/api/configs/cx/cx1.yaml", json={"content": "num_elements: 8\n"},
                      **LOCAL)
    assert resp.status_code == 200
