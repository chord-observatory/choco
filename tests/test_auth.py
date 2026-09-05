"""Tests for LDAP authentication."""

import pytest
from unittest.mock import MagicMock, patch

from ldap3.core.exceptions import LDAPInvalidCredentialsResult
import copy
import ssl
from urllib.parse import quote

from choco.app import _DEFAULT_CONFIG, create_app

from choco.app import create_app
from choco.auth import LdapAuthenticator, User, save_user, _users


@pytest.fixture(autouse=True)
def clear_users():
    """Clear the in-memory user store between tests."""
    _users.clear()
    yield
    _users.clear()


@pytest.fixture
def app(tmp_path):
    """Create a test app with LDAP disabled."""
    nodes_yaml = tmp_path / "nodes.yaml"
    nodes_yaml.write_text("groups: {}\n")
    app = create_app(configs_dir=tmp_path)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


class TestUser:
    def test_user_properties(self):
        user = User("cn=test,dc=example", "test", {"email": "test@example.com"})
        assert user.username == "test"
        assert user.dn == "cn=test,dc=example"
        assert user.get_id() == "cn=test,dc=example"
        assert user.is_authenticated is True

    def test_save_user(self):
        user = save_user("cn=test,dc=example", "test")
        assert _users["cn=test,dc=example"] is user
        assert user.username == "test"

    def test_save_user_overwrites(self):
        save_user("cn=test,dc=example", "test", {"v": 1})
        user2 = save_user("cn=test,dc=example", "test", {"v": 2})
        assert _users["cn=test,dc=example"] is user2
        assert user2.data == {"v": 2}


class TestUnauthenticatedAccess:
    """All routes should redirect to login when not authenticated."""

    def test_dashboard_redirects(self, client):
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_node_edit_redirects(self, client):
        resp = client.get("/nodes/edit/cx/cx1", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_partials_redirect(self, client):
        resp = client.get("/partials/dashboard-table", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_login_page_accessible(self, client):
        resp = client.get("/login")
        assert resp.status_code == 200
        assert b"Log in" in resp.data


class TestLoginFlow:
    def _get_csrf(self, client):
        """GET /login to establish a session, then extract the CSRF token."""
        client.get("/login")
        with client.session_transaction() as sess:
            return sess["_csrf_token"]

    def test_login_missing_fields(self, client):
        token = self._get_csrf(client)
        resp = client.post("/login", data={"username": "", "password": "", "_csrf_token": token})
        assert resp.status_code == 200
        assert b"required" in resp.data

    def test_login_ldap_not_configured(self, client):
        token = self._get_csrf(client)
        resp = client.post(
            "/login", data={"username": "test", "password": "pass", "_csrf_token": token},
            follow_redirects=True,
        )
        assert b"LDAP is not configured" in resp.data

    def _enable_ldap(self, app):
        """Wire a stub authenticator in as if ldap.host were configured."""
        app.config["LDAP_ENABLED"] = True
        authenticator = MagicMock(spec=LdapAuthenticator)
        app.config["ldap_authenticator"] = authenticator
        return authenticator

    def test_login_success_creates_session(self, client, app):
        authenticator = self._enable_ldap(app)
        authenticator.authenticate.return_value = \
            "uid=alice,cn=users,cn=accounts,dc=example"
        token = self._get_csrf(client)
        resp = client.post(
            "/login",
            data={"username": "alice", "password": "pw", "_csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/"
        authenticator.authenticate.assert_called_once_with("alice", "pw")
        # the session is live: a protected page now renders
        assert client.get("/").status_code == 200

    def test_session_cookie_is_hardened(self, client, app):
        """Secure (server.ssl is on by default), HttpOnly, SameSite=Lax."""
        authenticator = self._enable_ldap(app)
        authenticator.authenticate.return_value = \
            "uid=alice,cn=users,cn=accounts,dc=example"
        token = self._get_csrf(client)
        resp = client.post(
            "/login",
            data={"username": "alice", "password": "pw", "_csrf_token": token},
            follow_redirects=False,
        )
        cookie = next(h for h in resp.headers.getlist("Set-Cookie")
                      if h.startswith("session="))
        assert "Secure" in cookie
        assert "HttpOnly" in cookie
        assert "SameSite=Lax" in cookie

    def test_login_failure_flashes_error(self, client, app):
        authenticator = self._enable_ldap(app)
        authenticator.authenticate.return_value = None
        token = self._get_csrf(client)
        resp = client.post(
            "/login",
            data={"username": "alice", "password": "wrong", "_csrf_token": token},
            follow_redirects=True,
        )
        assert b"Invalid username or password" in resp.data
        # still locked out
        assert client.get("/", follow_redirects=False).status_code == 302


class TestLoginNextRedirect:
    """Login returns the user to the page they were trying to reach."""

    def _login(self, client, app, url="/login"):
        app.config["LDAP_ENABLED"] = True
        authenticator = MagicMock(spec=LdapAuthenticator)
        authenticator.authenticate.return_value = \
            "uid=alice,cn=users,cn=accounts,dc=example"
        app.config["ldap_authenticator"] = authenticator
        client.get("/login")
        with client.session_transaction() as sess:
            token = sess["_csrf_token"]
        return client.post(
            url,
            data={"username": "alice", "password": "pw", "_csrf_token": token},
            follow_redirects=False,
        )

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

    def test_login_redirects_to_next(self, client, app):
        resp = self._login(client, app, "/login?next=/nodes")
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/nodes"

    def test_login_rejects_offsite_next(self, client, app):
        for evil in ("https://evil.example", "//evil.example",
                     "javascript:alert(1)",
                     # browsers resolve these as //evil.example
                     "/\\evil.example", "/\\/evil.example",
                     "/\t/evil.example", "/\r\n/evil.example"):
            _users.clear()
            resp = self._login(client, app,
                               f"/login?next={quote(evil, safe='')}")
            assert resp.headers["Location"] == "/", repr(evil)
            client.get("/logout")

    def test_already_logged_in_login_honors_next(self, client, app):
        self._login(client, app)
        resp = client.get("/login?next=/files", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["Location"] == "/files"


class TestLdapAuthenticator:
    def _auth(self, **kwargs):
        defaults = dict(host="ldaps://ipa.example", port=636, use_ssl=True,
                        base_dn="dc=example,dc=ca",
                        user_dn="cn=users,cn=accounts", login_attr="uid")
        defaults.update(kwargs)
        return LdapAuthenticator(**defaults)

    def test_tls_verifies_the_server_certificate(self):
        """ldap3's own default, Server(use_ssl=True) with no tls=, is
        Tls(validate=CERT_NONE): any certificate from any host, so an
        on-path attacker could answer "bind succeeded" to any password."""
        auth = self._auth()
        assert auth.server.ssl is True
        assert auth.server.tls.validate == ssl.CERT_REQUIRED
        assert auth.server.tls.ca_certs_file is None  # the system CA store

    def test_ca_cert_is_passed_through(self, tmp_path):
        pem = tmp_path / "ipa-ca.pem"
        pem.write_text("not a real certificate\n")
        assert self._auth(ca_cert=str(pem)).server.tls.ca_certs_file == str(pem)

    def test_ldaps_url_overrides_use_ssl_false_and_still_verifies(self):
        """ldap3 lets the URL scheme win.  The first version built the
        strict Tls only when use_ssl was true, so this exact config got
        ldap3's CERT_NONE default back -- the test that caught it."""
        auth = self._auth(host="ldaps://ipa.example", use_ssl=False)
        assert auth.server.ssl is True
        assert auth.server.tls.validate == ssl.CERT_REQUIRED

    def test_plain_ldap_is_plain(self):
        auth = self._auth(host="ldap://ipa.example", use_ssl=False, port=389)
        assert auth.server.ssl is False
        assert auth.server.tls.validate == ssl.CERT_REQUIRED  # inert, but never CERT_NONE

    def test_user_dn_construction(self):
        auth = self._auth()
        assert auth.user_dn_for("alice") == \
            "uid=alice,cn=users,cn=accounts,dc=example,dc=ca"

    def test_user_dn_without_base_dn(self):
        auth = self._auth(base_dn="")
        assert auth.user_dn_for("alice") == "uid=alice,cn=users,cn=accounts"

    def test_user_dn_escapes_dn_metacharacters(self):
        """A crafted username must not splice components into the DN."""
        auth = self._auth()
        dn = auth.user_dn_for("alice,cn=admins")
        assert dn == "uid=alice\\,cn\\=admins,cn=users,cn=accounts,dc=example,dc=ca"

    def test_successful_bind_returns_dn(self):
        auth = self._auth()
        with patch("choco.auth.ldap3.Connection") as conn:
            conn.return_value.__enter__ = MagicMock()
            conn.return_value.__exit__ = MagicMock(return_value=False)
            dn = auth.authenticate("alice", "pw")
        assert dn == "uid=alice,cn=users,cn=accounts,dc=example,dc=ca"
        kwargs = conn.call_args.kwargs
        assert kwargs["user"] == dn
        assert kwargs["password"] == "pw"
        assert kwargs["raise_exceptions"] is True

    def test_bad_credentials_returns_none(self):
        auth = self._auth()
        with patch("choco.auth.ldap3.Connection",
                   side_effect=LDAPInvalidCredentialsResult):
            assert auth.authenticate("alice", "wrong") is None

    def test_empty_password_rejected_without_contacting_ldap(self):
        """An empty password would be an anonymous bind — the classic
        LDAP pitfall where 'authentication' succeeds with no credentials."""
        auth = self._auth()
        with patch("choco.auth.ldap3.Connection") as conn:
            assert auth.authenticate("alice", "") is None
            assert auth.authenticate("", "pw") is None
        conn.assert_not_called()


class TestAuthenticatedAccess:
    """Test that authenticated users can access routes."""

    def _login(self, client, app):
        """Helper to simulate a logged-in user."""
        user = save_user("cn=test,dc=example", "testuser")
        with client.session_transaction() as sess:
            # Flask-Login stores user_id in the session under _user_id
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


class TestInitAuthLdapConfig:
    """init_auth's handling of the ldap: block."""

    def _app(self, tmp_path, **ldap):
        (tmp_path / "nodes.yaml").write_text("groups: {}\n")
        cfg = copy.deepcopy(_DEFAULT_CONFIG)
        cfg["ldap"] = {"host": "ldaps://ipa.example", **ldap}
        return create_app(configs_dir=tmp_path, config=cfg)

    def test_ca_cert_reaches_the_authenticator(self, tmp_path):
        pem = tmp_path / "ipa-ca.pem"
        pem.write_text("not a real certificate\n")
        app = self._app(tmp_path, ca_cert=str(pem))
        tls = app.config["ldap_authenticator"].server.tls
        assert tls.validate == ssl.CERT_REQUIRED
        assert tls.ca_certs_file == str(pem)

    def test_missing_ca_cert_file_refuses_to_start(self, tmp_path):
        """ldap3 would log an error and silently verify against the system
        store instead -- the operator asked for a specific CA."""
        with pytest.raises(ValueError, match="ldap.ca_cert"):
            self._app(tmp_path, ca_cert=str(tmp_path / "nope.pem"))

    def test_empty_ca_cert_means_system_store(self, tmp_path):
        app = self._app(tmp_path, ca_cert="")
        assert app.config["ldap_authenticator"].server.tls.ca_certs_file is None

    def test_plain_ldap_is_loud(self, tmp_path, caplog):
        with caplog.at_level("WARNING", logger="choco.auth"):
            app = self._app(tmp_path, host="ldap://ipa.example",
                            use_ssl=False, port=389)
        assert app.config["ldap_authenticator"].server.ssl is False
        assert any("cleartext" in r.getMessage() for r in caplog.records)

    def test_ldaps_url_with_use_ssl_false_is_not_plaintext(self, tmp_path, caplog):
        with caplog.at_level("WARNING", logger="choco.auth"):
            app = self._app(tmp_path, use_ssl=False)  # host is ldaps://
        assert app.config["ldap_authenticator"].server.ssl is True
        assert not any("cleartext" in r.getMessage() for r in caplog.records)
