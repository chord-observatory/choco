"""Tests for LDAP authentication."""

import pytest
from unittest.mock import MagicMock, patch

from choco.app import create_app
from choco.auth import User, save_user, _users


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
        resp = client.get("/edit/cx/cx1", follow_redirects=False)
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
