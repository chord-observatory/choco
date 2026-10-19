"""Tests for login sessions and the login page.

choco has no login of its own (the site proxy signs people in:
test_proxy_auth; trusted hosts: test_trusted_hosts).  Here: the user
store, the session, where an unauthenticated request is sent, and the
``?next=`` rules ``/login`` applies to a request that is already signed in.
"""

from urllib.parse import quote

import pytest

from choco.app import create_app
from choco.auth import User, save_user, _users


@pytest.fixture
def app(tmp_path):
    """A test app with the shipped defaults."""
    nodes_yaml = tmp_path / "nodes.yaml"
    nodes_yaml.write_text("groups: {}\n")
    app = create_app(configs_dir=tmp_path)
    app.config["TESTING"] = True
    app.config["state_dir"] = tmp_path / "state"   # never the host's /var/lib/choco
    return app


@pytest.fixture
def client(app):
    return app.test_client()


class TestUser:
    def test_user_properties(self):
        user = User("cn=test,dc=example", "test")
        assert user.username == "test"
        assert user.dn == "cn=test,dc=example"
        assert user.get_id() == "cn=test,dc=example"
        assert user.is_authenticated is True

    def test_save_user(self):
        user = save_user("cn=test,dc=example", "test")
        assert _users["cn=test,dc=example"] is user
        assert user.username == "test"

    def test_save_user_overwrites(self):
        save_user("cn=test,dc=example", "test")
        user2 = save_user("cn=test,dc=example", "renamed")
        assert _users["cn=test,dc=example"] is user2
        assert user2.username == "renamed"


class TestSession:
    """The session is the signed cookie's ``_user_id``, looked up in the
    in-memory store on each request."""

    def test_unknown_id_is_nobody(self, client):
        # a cookie from before a restart, or a trusted peer's synthetic
        # user, names an id the store does not have: anonymous
        with client.session_transaction() as sess:
            sess["_user_id"] = "cn=gone,dc=example"
        resp = client.get("/nodes", follow_redirects=False)
        assert resp.status_code == 302 and "/login" in resp.headers["Location"]

    def test_logout_forgets_the_id(self, client):
        user = save_user("cn=test,dc=example", "testuser")
        with client.session_transaction() as sess:
            sess["_user_id"] = user.get_id()
        assert client.get("/nodes").status_code == 200
        client.get("/logout")
        with client.session_transaction() as sess:
            assert "_user_id" not in sess
        assert client.get("/nodes", follow_redirects=False).status_code == 302


class TestLoginPage:
    """choco has no login form: the page points to the site proxy."""

    def test_login_page_accessible(self, client):
        resp = client.get("/login")
        assert resp.status_code == 200
        assert b'name="password"' not in resp.data
        assert b"ssh -L 5000:127.0.0.1:5000" in resp.data

    def test_there_is_nothing_to_post_to(self, client):
        assert client.post("/login", data={"username": "a", "password": "b"}).status_code == 405

    def test_session_cookie_is_hardened(self, client):
        """Secure (server.ssl is on by default), HttpOnly, SameSite=Lax."""
        user = save_user("proxy:alice", "alice")
        with client.session_transaction() as sess:
            sess["_user_id"] = user.get_id()
        resp = client.get("/nodes")          # seeds the CSRF token: the session is rewritten
        cookie = next(h for h in resp.headers.getlist("Set-Cookie")
                      if h.startswith("choco_session="))
        assert "Secure" in cookie
        assert "HttpOnly" in cookie
        assert "SameSite=Lax" in cookie


class TestLoginNextRedirect:
    """A request that needs a login carries where it was going (?next=),
    and /login sends a signed-in request there -- same-site paths only."""

    def _signed_in_get(self, client, url):
        user = save_user("proxy:alice", "alice")
        with client.session_transaction() as sess:
            sess["_user_id"] = user.get_id()
        return client.get(url, follow_redirects=False)

    def test_unauthorized_get_carries_next(self, client):
        resp = client.get("/nodes", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/login?next=%2Fnodes"

    def test_unauthorized_get_keeps_query(self, client):
        resp = client.get("/nodes?a=b", follow_redirects=False)
        assert resp.headers["Location"] == "/login?next=%2Fnodes%3Fa%3Db"

    def test_unauthorized_post_gets_no_next(self, client):
        # Replaying a POST-only URL as a GET after login would 405.
        resp = client.post("/nodes/toggle-started/cx/cx1",
                           follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/login"

    def test_htmx_unauthorized_returns_to_the_page(self, client):
        # The polled partial is not a place a person can be; the page
        # they were on (HX-Current-URL) is.
        resp = client.get(
            "/partials/services",
            headers={"HX-Request": "true",
                     "HX-Current-URL": "https://choco.example/nodes"},
        )
        assert resp.status_code == 200
        assert resp.headers["HX-Redirect"] == "/login?next=%2Fnodes"

    def test_login_redirects_to_next(self, client):
        resp = self._signed_in_get(client, "/login?next=/nodes")
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/nodes"

    @pytest.mark.parametrize("evil", [
        "https://evil.example", "//evil.example", "javascript:alert(1)",
        # browsers resolve these as //evil.example
        "/\\evil.example", "/\\/evil.example", "/\t/evil.example", "/\r\n/evil.example"])
    def test_login_rejects_offsite_next(self, client, evil):
        resp = self._signed_in_get(client, f"/login?next={quote(evil, safe='')}")
        assert resp.headers["Location"] == "/", repr(evil)


class TestAuthenticatedAccess:
    """Test that authenticated users can access routes."""

    def _login(self, client, app):
        """Helper to simulate a logged-in user."""
        user = save_user("cn=test,dc=example", "testuser")
        with client.session_transaction() as sess:
            # the signed session carries the user's id (auth.login_user)
            sess["_user_id"] = user.get_id()

    def test_dashboard_accessible(self, client, app):
        self._login(client, app)
        resp = client.get("/")
        assert resp.status_code == 200

    def test_partials_accessible(self, client, app):
        self._login(client, app)
        resp = client.get("/partials/dashboard-table")
        assert resp.status_code == 200

    def test_logout(self, client, app):
        self._login(client, app)
        resp = client.get("/logout", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]
        # After logout, dashboard should redirect to login
        resp2 = client.get("/", follow_redirects=False)
        assert resp2.status_code == 302
        assert "/login" in resp2.headers["Location"]

    def test_already_logged_in_redirects_from_login(self, client, app):
        self._login(client, app)
        resp = client.get("/login", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/"
