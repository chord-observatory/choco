"""Tests for ``server.proxy_auth``: signing in through the site proxy.

choco is served at ``https://<site>/choco/`` behind nginx and Authelia.  A
request counts as proxied only from the configured peer *and* with the
shared secret; it is then served under the prefix and signed in as the
proxy's ``Remote-User``.  The peer address is never rewritten, so the
loopback bypass and ``trusted_hosts`` still judge the real TCP peer.  The
sweep at the end renders every route as a proxied request and fails on any
root-relative URL that escapes the prefix -- the guard for templates and
scripts that build a URL by hand.
"""

import copy
import re

import pytest
import responses
import yaml

from choco.app import _DEFAULT_CONFIG, create_app, load_config
from choco.auth import ProxyMiddleware, parse_proxy_auth, parse_trusted_hosts
from tests.test_route_auth import ROUTES

PROXY = "10.220.1.13"
SECRET = "s3cret-" * 6
PREFIX = "/choco"
PROXY_AUTH = {"peer": PROXY, "secret": SECRET, "prefix": PREFIX}


def _config(configs_dir, **server):
    cfg = copy.deepcopy(_DEFAULT_CONFIG)
    cfg["server"].update(server)
    cfg["configs_dir"] = str(configs_dir)
    return cfg


@pytest.fixture
def app(configs_dir):
    """Overrides conftest's: choco configured for the site proxy."""
    app = create_app(configs_dir=configs_dir,
                     config=_config(configs_dir, proxy_auth=PROXY_AUTH))
    app.config["TESTING"] = True
    app.config["state_dir"] = configs_dir / "state"   # never the host's /var/lib/choco
    return app


def _via(user="alice", groups="choco-users", secret=SECRET, peer=PROXY):
    """A request as the proxy sends it (Authelia's headers + the secret)."""
    headers = {"Remote-User": user, "Remote-Groups": groups}
    if secret is not None:
        headers["X-Choco-Proxy-Secret"] = secret
    return {"environ_base": {"REMOTE_ADDR": peer}, "headers": headers}


def _to_login(resp) -> bool:
    return resp.status_code == 302 and "/login" in resp.headers.get("Location", "")


class TestConfig:
    def test_absent_is_off(self):
        assert parse_proxy_auth(None, []) is None
        assert parse_proxy_auth({}, []) is None

    def test_accepts_a_valid_block(self):
        p = parse_proxy_auth(PROXY_AUTH, [])
        assert (str(p["peer"]), p["prefix"], p["logout_url"]) == \
            (PROXY, PREFIX, "/authelia/logout")

    @pytest.mark.parametrize("change, message", [
        ({"peer": "127.0.0.1"}, "loopback"),
        ({"peer": "proxy.example"}, "IP address"),
        ({"secret": "short"}, "at least"),
        ({"prefix": "/choco/"}, "prefix"),
        ({"prefix": "choco"}, "prefix"),
        ({"prefix": "/cho co"}, "prefix"),
        ({"logout_url": "//evil.example/logout"}, "logout_url"),
        ({"logout_url": "https://evil.example/"}, "logout_url"),
        ({"extra": 1}, "unknown"),
    ])
    def test_refuses(self, change, message):
        with pytest.raises(ValueError, match=message):
            parse_proxy_auth({**PROXY_AUTH, **change}, [])

    def test_refuses_a_non_mapping(self):
        with pytest.raises(ValueError, match="mapping"):
            parse_proxy_auth("10.220.1.13", [])

    def test_refuses_a_peer_inside_trusted_hosts(self):
        trusted = parse_trusted_hosts({"10.220.1.0/24": "ctc"})
        with pytest.raises(ValueError, match="trusted_hosts"):
            parse_proxy_auth(PROXY_AUTH, trusted)

    def _write(self, tmp_path, **server):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump({"server": {"secret_key": "k" * 32, **server}}))
        path.chmod(0o600)
        return path

    def test_load_config_checks_it_against_trusted_hosts(self, tmp_path):
        path = self._write(tmp_path, proxy_auth=PROXY_AUTH,
                           trusted_hosts={PROXY: "proxy"})
        with pytest.raises(ValueError, match="trusted_hosts"):
            load_config(path)

    def test_load_config_refuses_it_with_dev_auth(self, tmp_path):
        path = self._write(tmp_path, proxy_auth=PROXY_AUTH, dev_auth="dev",
                           host="127.0.0.1")
        with pytest.raises(ValueError, match="dev_auth"):
            load_config(path)


class TestMiddleware:
    def _call(self, environ):
        seen = {}

        def app(env, start_response):
            seen.update(env)
            return []
        ProxyMiddleware(app, parse_proxy_auth(PROXY_AUTH, []))(environ, None)
        return seen

    def test_a_vouched_request_is_served_under_the_prefix(self):
        env = self._call({"REMOTE_ADDR": PROXY, "PATH_INFO": "/nodes",
                          "HTTP_X_CHOCO_PROXY_SECRET": SECRET, "wsgi.url_scheme": "http"})
        assert env["choco.proxied"] is True
        assert (env["SCRIPT_NAME"], env["PATH_INFO"], env["wsgi.url_scheme"]) == \
            (PREFIX, "/nodes", "https")
        assert env["REMOTE_ADDR"] == PROXY, "the peer address is never rewritten"
        assert "HTTP_X_CHOCO_PROXY_SECRET" not in env, "the secret is not passed on"

    def test_an_unstripped_prefix_is_tolerated(self):
        env = self._call({"REMOTE_ADDR": PROXY, "PATH_INFO": "/choco/nodes",
                          "HTTP_X_CHOCO_PROXY_SECRET": SECRET})
        assert (env["SCRIPT_NAME"], env["PATH_INFO"]) == (PREFIX, "/nodes")

    @pytest.mark.parametrize("peer, secret", [
        (PROXY, "wrong"), (PROXY, ""), ("10.222.0.54", SECRET), ("127.0.0.1", SECRET)])
    def test_anything_else_is_left_alone(self, peer, secret):
        env = self._call({"REMOTE_ADDR": peer, "PATH_INFO": "/nodes",
                          "HTTP_X_CHOCO_PROXY_SECRET": secret})
        assert env["choco.proxied"] is False
        assert "SCRIPT_NAME" not in env and env["PATH_INFO"] == "/nodes"
        assert "HTTP_X_CHOCO_PROXY_SECRET" not in env


class TestSignIn:
    def test_the_proxy_user_is_signed_in(self, client, jobs):
        resp = client.get("/", **_via("alice"))
        assert resp.status_code == 200
        assert "alice" in resp.get_data(as_text=True)

    @pytest.mark.parametrize("kw", [
        {"secret": None}, {"secret": "wrong"}, {"peer": "10.222.0.54"}])
    def test_without_the_proxys_word_it_is_an_ordinary_request(self, client, kw):
        resp = client.get("/nodes", **_via("alice", **kw))
        assert _to_login(resp)
        assert resp.headers["Location"].startswith("/login"), "not served under the prefix"

    @pytest.mark.parametrize("user", ["", "a b", "x)(uid=*", "ålice"])
    def test_an_implausible_username_is_refused(self, client, user):
        assert client.get("/nodes", **_via(user)).status_code == 403

    def test_the_session_follows_the_header(self, client, jobs):
        assert "alice" in client.get("/", **_via("alice")).get_data(as_text=True)
        page = client.get("/", **_via("bob")).get_data(as_text=True)
        assert "bob" in page and ">alice<" not in page

    def test_a_proxied_api_write_is_a_browser_write(self, client):
        """REMOTE_ADDR stays the proxy, so no loopback bypass: a write
        needs the session's CSRF token like any other browser write."""
        resp = client.post("/api/configs/pull", json={}, **_via("alice"))
        assert resp.status_code == 403

    def test_logout_signs_out_of_the_proxy_too(self, client):
        client.get("/nodes", **_via("alice"))
        resp = client.get("/logout", **_via("alice"))
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/authelia/logout"

    def test_next_targets_keep_the_prefix(self, client):
        resp = client.get("/login?next=/files", **_via("alice"))
        assert resp.status_code == 302 and resp.headers["Location"] == "/choco/files"

    def test_direct_access_is_unchanged(self, client, jobs):
        """Reached directly (no proxy), URLs stay at the root."""
        from tests.helpers import login
        login(client)
        page = client.get("/", environ_base={"REMOTE_ADDR": "10.222.0.54"}).get_data(as_text=True)
        assert '<meta name="choco-root" content="">' in page
        assert 'href="/nodes"' in page


# --- every page, rendered behind the proxy ------------------------------------

#: An attribute that carries a URL, and a quoted root-relative path in a script.
_ATTR_URL = re.compile(r'\b(?:href|src|action|hx-[a-z-]+|data-[a-z-]+)="(/[^"]*)"')
_SCRIPT = re.compile(r"<script\b[^>]*>(.*?)</script>", re.S)
_SCRIPT_PATH = re.compile(r"""["'`](/(?!/)[A-Za-z_][^"'`\s]*)""")


def _escapes(url: str) -> bool:
    """A root-relative URL outside the prefix (``//host`` is not root-relative)."""
    return (url.startswith("/") and not url.startswith("//")
            and url != PREFIX and not url.startswith(PREFIX + "/"))


GETS = [r for r in ROUTES if r[0] == "GET"]


@responses.activate                        # no request leaves the test
@pytest.mark.parametrize("route", GETS, ids=["GET " + r[1] for r in GETS])
def test_nothing_escapes_the_prefix(client, jobs, route):
    _method, _rule, _endpoint, url = route
    resp = client.get(url, **_via("alice"))
    location = resp.headers.get("Location") or resp.headers.get("HX-Redirect")
    if location:
        assert not _escapes(location) or location == "/authelia/logout", location
    if resp.mimetype != "text/html" or resp.status_code in (301, 302, 303, 307, 308):
        return                             # a redirect's body is Werkzeug's, not ours
    page = resp.get_data(as_text=True)
    bad = [u for u in _ATTR_URL.findall(page) if _escapes(u)]
    for script in _SCRIPT.findall(page):
        bad += [u for u in _SCRIPT_PATH.findall(script) if _escapes(u)]
    assert not bad, f"root-relative URLs outside {PREFIX}: {sorted(set(bad))}"


def test_the_landing_page_is_prefixed(client, jobs):
    page = client.get("/", **_via("alice")).get_data(as_text=True)
    assert f'<meta name="choco-root" content="{PREFIX}">' in page
    assert f'href="{PREFIX}/nodes"' in page
    assert f'hx-get="{PREFIX}/partials/' in page


# --- and every template and script, as written --------------------------------
#
# The sweep above only sees pages that render without data (the waterfall
# pages 404 here), so the sources are checked too: a quoted string that
# starts at the app root must be prefixed (request.script_root in a
# template, the page's choco-root in a script) or be a suffix appended to
# something else.

import choco  # noqa: E402

_PKG = __import__("pathlib").Path(choco.__file__).parent
_SOURCES = sorted(_PKG.glob("templates/*.html")) + sorted(
    p for p in _PKG.glob("static/*.js") if not p.name.endswith(".min.js"))
#: A quote opening a string that starts at the root: "/x", '/x', `/x`
#: (not "//host", not a lone "/" used as a separator, not the "/> closing
#: an SVG element after a quoted attribute).
_ROOT_LITERAL = re.compile(r"""["'`]/(?![/"'`>])""")
#: What may stand right before such a string.
_PREFIXED = re.compile(r"(request\.script_root\s*~|\+)\s*$")


@pytest.mark.parametrize("path", _SOURCES, ids=[p.name for p in _SOURCES])
def test_sources_build_urls_from_the_root(path):
    bad = []
    for n, line in enumerate(path.read_text().splitlines(), 1):
        for m in _ROOT_LITERAL.finditer(line):
            if not _PREFIXED.search(line[:m.start()]):
                bad.append(f"{path.name}:{n}: {line.strip()[:100]}")
    assert not bad, "\n".join(bad)


# --- read-only users ----------------------------------------------------------

from tests.helpers import csrf  # noqa: E402
from tests.test_route_auth import MUTATING  # noqa: E402

PUBLIC_URL = "https://site.example/choco/"
ROLES = {**PROXY_AUTH, "write_groups": ["choco-users", "choco-admins"],
         "public_url": PUBLIC_URL}
CHIVE = "10.222.0.54"


@pytest.fixture
def roles_client(configs_dir):
    """choco with write groups, a public URL, no LDAP, and chive trusted."""
    app = create_app(configs_dir=configs_dir, config=_config(
        configs_dir, proxy_auth=ROLES, trusted_hosts={CHIVE: "chive"}))
    app.config["TESTING"] = True
    app.config["state_dir"] = configs_dir / "state"
    return app.test_client()


def _went_through(resp) -> bool:
    """A permitted toggle: refused would be 403, unauthenticated a login
    redirect; a plain POST is answered with a redirect back to the page."""
    return resp.status_code in (200, 302) and not _to_login(resp)


class TestRoleConfig:
    @pytest.mark.parametrize("change, message", [
        ({"write_groups": "choco-users"}, "write_groups"),
        ({"write_groups": ["choco-users", ""]}, "write_groups"),
        ({"public_url": "http://site.example/choco/"}, "public_url"),
    ])
    def test_refuses(self, change, message):
        with pytest.raises(ValueError, match=message):
            parse_proxy_auth({**ROLES, **change}, [])

    def test_unset_write_groups_means_everyone_may(self, client):
        token = csrf(client, **_via("alice", groups="chord-users"))
        resp = client.post("/nodes/toggle-maintenance/cx/cx1",
                           data={"_csrf_token": token}, **_via("alice", groups="chord-users"))
        assert _went_through(resp)


class TestReadOnly:
    def test_a_reader_sees_the_pages_marked_read_only(self, roles_client, jobs):
        page = roles_client.get("/", **_via("rita", groups="chord-users")).get_data(as_text=True)
        assert "read-only" in page and "Read-only access" in page

    @responses.activate
    @pytest.mark.parametrize("route", MUTATING, ids=["{0[0]} {0[1]}".format(r) for r in MUTATING])
    def test_a_reader_changes_nothing(self, roles_client, route):
        method, _rule, _endpoint, url = route
        via = _via("rita", groups="chord-users")
        token = csrf(roles_client, **via)
        resp = roles_client.open(url, method=method, data={"_csrf_token": token},
                                 headers={**via["headers"], "X-CSRF-Token": token},
                                 environ_base=via["environ_base"])
        assert resp.status_code == 403

    def test_a_choco_user_may_write(self, roles_client):
        via = _via("cara", groups="chord-users,choco-users")
        token = csrf(roles_client, **via)
        resp = roles_client.post("/nodes/toggle-maintenance/cx/cx1",
                                 data={"_csrf_token": token}, **via)
        assert _went_through(resp)
        assert "Read-only access" not in roles_client.get("/nodes", **via).get_data(as_text=True)

    def test_a_group_change_applies_on_the_next_request(self, roles_client):
        reader = _via("cara", groups="chord-users")
        token = csrf(roles_client, **reader)
        url = "/nodes/toggle-maintenance/cx/cx1"
        assert roles_client.post(url, data={"_csrf_token": token}, **reader).status_code == 403
        writer = _via("cara", groups="chord-users,choco-admins")
        assert _went_through(roles_client.post(url, data={"_csrf_token": token}, **writer))

    def test_trusted_hosts_keep_full_access(self, roles_client):
        chive = {"environ_base": {"REMOTE_ADDR": CHIVE}}
        token = csrf(roles_client, **chive)
        resp = roles_client.post("/nodes/toggle-maintenance/cx/cx1",
                                 data={"_csrf_token": token}, **chive)
        assert _went_through(resp)

    def test_the_login_page_points_to_the_proxy(self, roles_client):
        page = roles_client.get("/login", environ_base={"REMOTE_ADDR": "10.0.0.9"}).get_data(as_text=True)
        assert PUBLIC_URL in page and 'name="password"' not in page
