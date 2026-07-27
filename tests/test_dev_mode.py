"""Tests for development mode (``server.dev_auth`` / ``server.ssl``).

Dev mode removes both authentication and CSRF, so the tests that matter
most are the negative ones: that neither comes off unless dev_auth is
explicitly set, and that choco refuses to start dev mode on a
network-reachable address.
"""

import copy

import pytest
import yaml
from werkzeug.exceptions import Forbidden

from choco import web
from choco.app import _DEFAULT_CONFIG, _make_ssl_context, create_app, load_config
from choco.auth import _users


@pytest.fixture(autouse=True)
def clear_users():
    _users.clear()
    yield
    _users.clear()


def _config(configs_dir, **server):
    cfg = copy.deepcopy(_DEFAULT_CONFIG)
    cfg["server"].update(server)
    cfg["configs_dir"] = str(configs_dir)
    return cfg


@pytest.fixture
def configs_dir(tmp_path):
    (tmp_path / "nodes.yaml").write_text("groups: {}\n")
    return tmp_path


@pytest.fixture
def dev_app(configs_dir):
    """App as ./choco.sh develop starts it."""
    app = create_app(
        configs_dir=configs_dir,
        config=_config(configs_dir, host="127.0.0.1", dev_auth="dev", ssl=False),
    )
    app.config["TESTING"] = True
    return app


@pytest.fixture
def prod_app(configs_dir):
    """App with the shipped defaults — dev_auth unset."""
    app = create_app(configs_dir=configs_dir, config=_config(configs_dir))
    app.config["TESTING"] = True
    return app


class TestDevAuthOff:
    """The default. These are the regressions that would matter."""

    def test_dashboard_still_requires_login(self, prod_app):
        resp = prod_app.test_client().get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_csrf_still_enforced(self, prod_app):
        with prod_app.test_request_context("/", method="POST", data={}):
            with pytest.raises(Forbidden):
                web._check_csrf()

    def test_csrf_header_still_enforced(self, prod_app):
        with prod_app.test_request_context("/", method="POST"):
            with pytest.raises(Forbidden):
                web._check_csrf_header()

    def test_dev_banner_absent(self, prod_app):
        resp = prod_app.test_client().get("/login")
        assert b"DEV MODE" not in resp.data


class TestDevAuthOn:
    def test_dashboard_needs_no_login(self, dev_app):
        resp = dev_app.test_client().get("/", follow_redirects=False)
        assert resp.status_code == 200

    def test_csrf_skipped(self, dev_app):
        with dev_app.test_request_context("/", method="POST", data={}):
            assert web._check_csrf() is None
            assert web._check_csrf_header() is None

    def test_dev_banner_shown(self, dev_app):
        resp = dev_app.test_client().get("/")
        assert b"DEV MODE" in resp.data

    def test_auto_login_survives_a_missing_cookie(self, dev_app):
        """Each request re-establishes the user, so a client that never
        returns the session cookie still gets in — the whole point of
        doing this in before_request rather than via login_user alone."""
        client = dev_app.test_client()
        assert client.get("/").status_code == 200
        client.delete_cookie("session")
        assert client.get("/").status_code == 200


class TestDevModeGuardrails:
    def _write(self, tmp_path, server):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump({"server": server}))
        return path

    def test_refuses_non_loopback_bind(self, tmp_path):
        path = self._write(tmp_path, {"host": "0.0.0.0", "dev_auth": "dev"})
        with pytest.raises(ValueError, match="loopback"):
            load_config(path)

    def test_refuses_public_bind(self, tmp_path):
        path = self._write(tmp_path, {"host": "10.222.0.30", "dev_auth": "dev"})
        with pytest.raises(ValueError, match="loopback"):
            load_config(path)

    @pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
    def test_allows_loopback(self, tmp_path, host):
        path = self._write(tmp_path, {"host": host, "dev_auth": "dev"})
        assert load_config(path)["server"]["dev_auth"] == "dev"

    def test_non_loopback_fine_without_dev_auth(self, tmp_path):
        path = self._write(tmp_path, {"host": "0.0.0.0"})
        assert load_config(path)["server"]["host"] == "0.0.0.0"

    def test_ssl_false_disables_tls(self):
        assert _make_ssl_context({"ssl": False}) is None

    def test_ssl_defaults_on(self, tmp_path):
        path = self._write(tmp_path, {})
        assert load_config(path)["server"]["ssl"] is True
