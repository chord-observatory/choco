"""Tests for ``server.trusted_hosts``: credential-free login for listed peers.

The property that matters most is the negative one -- the default is
empty, and Flask's test client presents as 127.0.0.1, so every other
suite's ``test_requires_login`` already guards that.  Here: a listed peer
gets the whole UI as a synthetic user; CSRF stays on; the cookie it mints
is worthless from any other address; a real login is left alone; and a
malformed mapping is a startup error.
"""

import copy

import pytest
import yaml
from werkzeug.exceptions import Forbidden

from choco import web
from choco.app import _DEFAULT_CONFIG, create_app, load_config
from choco.auth import (_users, parse_trusted_hosts, save_user,
                        trusted_host_label)

CHIVE = "10.222.0.54"
LAB_NET = "10.222.0.0/24"
STRANGER = "10.0.0.9"

TRUSTED = {"127.0.0.1": "localhost", "::1": "localhost",
           LAB_NET: "lab", CHIVE: "chive"}


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
def trusted_app(configs_dir):
    app = create_app(configs_dir=configs_dir,
                     config=_config(configs_dir, trusted_hosts=TRUSTED))
    app.config["TESTING"] = True
    return app


@pytest.fixture
def plain_app(configs_dir):
    """Shipped defaults: no trusted hosts."""
    app = create_app(configs_dir=configs_dir, config=_config(configs_dir))
    app.config["TESTING"] = True
    return app


def _from(addr):
    return {"environ_base": {"REMOTE_ADDR": addr}}


def _csrf(client, **kw):
    client.get("/nodes", **kw)
    with client.session_transaction() as sess:
        return sess["_csrf_token"]


class TestDefaultOff:
    def test_loopback_still_redirects_to_login(self, plain_app):
        resp = plain_app.test_client().get("/nodes", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_no_hook_installed(self, plain_app):
        assert plain_app.config["TRUSTED_PEERS"] == []


class TestTrustedPeer:
    def test_loopback_gets_the_ui(self, trusted_app):
        resp = trusted_app.test_client().get("/nodes", follow_redirects=False)
        assert resp.status_code == 200
        assert b"<small>localhost</small>" in resp.data
        assert b"htmx.min.js" in resp.data  # the UI is live, not a static shell

    def test_chive_gets_the_ui_under_its_own_label(self, trusted_app):
        resp = trusted_app.test_client().get("/nodes", **_from(CHIVE))
        assert resp.status_code == 200
        assert b"<small>chive</small>" in resp.data

    def test_network_entry_labels_the_rest_of_the_subnet(self, trusted_app):
        resp = trusted_app.test_client().get("/nodes", **_from("10.222.0.7"))
        assert resp.status_code == 200
        assert b"<small>lab</small>" in resp.data

    def test_unlisted_peer_redirects_to_login(self, trusted_app):
        resp = trusted_app.test_client().get(
            "/nodes", follow_redirects=False, **_from(STRANGER))
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_json_api_opens_to_chive_too(self, trusted_app):
        client = trusted_app.test_client()
        assert client.get("/api/status", **_from(CHIVE)).status_code == 200
        assert client.get("/api/status", follow_redirects=False,
                          **_from(STRANGER)).status_code == 302

    def test_logout_link_hidden_for_synthetic_user(self, trusted_app):
        resp = trusted_app.test_client().get("/nodes")
        assert b'href="/logout"' not in resp.data

    def test_login_page_bounces_a_trusted_peer_home(self, trusted_app):
        resp = trusted_app.test_client().get("/login", follow_redirects=False)
        assert resp.status_code == 302

    def test_synthetic_user_is_never_stored(self, trusted_app):
        trusted_app.test_client().get("/nodes")
        assert _users == {}

    def test_dev_banner_absent(self, trusted_app):
        resp = trusted_app.test_client().get("/nodes")
        assert b"DEV MODE" not in resp.data


class TestCsrfStaysOn:
    def test_check_csrf_still_raises(self, trusted_app):
        with trusted_app.test_request_context(
                "/", method="POST", data={},
                environ_base={"REMOTE_ADDR": CHIVE}):
            with pytest.raises(Forbidden):
                web._check_csrf()
            with pytest.raises(Forbidden):
                web._check_csrf_header()

    def test_post_without_token_is_forbidden(self, trusted_app):
        action = next(iter(web._MAINTENANCE_ACTIONS))
        client = trusted_app.test_client()
        client.get("/nodes")  # logged in as localhost, session established
        resp = client.post(f"/nodes/set-maintenance-all/{action}", data={})
        assert resp.status_code == 403

    def test_post_with_token_goes_through(self, trusted_app):
        action = next(iter(web._MAINTENANCE_ACTIONS))
        client = trusted_app.test_client()
        token = _csrf(client)
        resp = client.post(f"/nodes/set-maintenance-all/{action}",
                           data={"_csrf_token": token},
                           headers={"HX-Request": "true"})
        assert resp.status_code == 200  # the dashboard partial, not a 403


class TestCookieIsBoundToTheAddress:
    def test_replay_from_elsewhere_is_anonymous(self, trusted_app):
        client = trusted_app.test_client()
        assert client.get("/nodes").status_code == 200
        with client.session_transaction() as sess:
            assert sess["_user_id"] == "trusted:127.0.0.1"
        # Same cookie jar, different peer: the id loads as nobody.
        resp = client.get("/nodes", follow_redirects=False, **_from(STRANGER))
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_re_login_each_request_survives_a_missing_cookie(self, trusted_app):
        client = trusted_app.test_client()
        assert client.get("/nodes").status_code == 200
        client.delete_cookie("session")
        assert client.get("/nodes").status_code == 200


class TestRealLoginWins:
    def test_ldap_session_keeps_its_own_name(self, trusted_app):
        client = trusted_app.test_client()
        user = save_user("cn=tester,dc=example", "tester")
        with client.session_transaction() as sess:
            sess["_user_id"] = user.get_id()
        resp = client.get("/nodes", **_from(CHIVE))
        assert resp.status_code == 200
        assert b"<small>tester</small>" in resp.data
        assert b"<small>chive</small>" not in resp.data
        assert b'href="/logout"' in resp.data


class TestParsing:
    def test_empty_and_none(self):
        assert parse_trusted_hosts(None) == []
        assert parse_trusted_hosts({}) == []

    def test_most_specific_first(self):
        parsed = parse_trusted_hosts({LAB_NET: "lab", CHIVE: "chive"})
        assert [label for _, label in parsed] == ["chive", "lab"]
        assert trusted_host_label(parsed, CHIVE) == "chive"
        assert trusted_host_label(parsed, "10.222.0.7") == "lab"
        assert trusted_host_label(parsed, STRANGER) is None

    def test_ipv6_loopback(self):
        parsed = parse_trusted_hosts({"::1": "localhost"})
        assert trusted_host_label(parsed, "::1") == "localhost"
        assert trusted_host_label(parsed, "127.0.0.1") is None

    def test_ipv4_mapped_ipv6_peer_is_unmapped(self):
        parsed = parse_trusted_hosts({CHIVE: "chive"})
        assert trusted_host_label(parsed, f"::ffff:{CHIVE}") == "chive"
        assert trusted_host_label(parsed, f"::ffff:{STRANGER}") is None

    def test_garbage_remote_addr_is_untrusted(self):
        parsed = parse_trusted_hosts(TRUSTED)
        assert trusted_host_label(parsed, None) is None
        assert trusted_host_label(parsed, "unix:") is None

    def test_yaml_scalar_keys_are_stringified(self):
        # YAML may hand over non-str keys; they must be judged as text.
        parsed = parse_trusted_hosts({"10.222.0.54 ": " chive "})
        assert parsed[0][1] == "chive"
        assert trusted_host_label(parsed, CHIVE) == "chive"

    @pytest.mark.parametrize("raw, match", [
        (["127.0.0.1"], "mapping"),
        ({"chive.example": "chive"}, "not an IP"),
        ({"10.222.0.54": ""}, "label"),
        ({"10.222.0.54": None}, "label"),
        ({"10.222.0.54": 7}, "label"),
    ])
    def test_refuses_malformed(self, raw, match):
        with pytest.raises(ValueError, match=match):
            parse_trusted_hosts(raw)


class TestLoadConfigGuard:
    def _write(self, tmp_path, trusted_hosts):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump({"server": {
            "secret_key": "k" * 32, "trusted_hosts": trusted_hosts}}))
        return path

    def test_accepts_addresses_and_networks(self, tmp_path):
        cfg = load_config(self._write(tmp_path, TRUSTED))
        assert cfg["server"]["trusted_hosts"] == TRUSTED

    def test_default_is_empty(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump({"server": {"secret_key": "k" * 32}}))
        assert load_config(path)["server"]["trusted_hosts"] == {}

    def test_refuses_a_list(self, tmp_path):
        with pytest.raises(ValueError, match="trusted_hosts"):
            load_config(self._write(tmp_path, ["127.0.0.1"]))

    def test_refuses_a_hostname(self, tmp_path):
        with pytest.raises(ValueError, match="chive.example"):
            load_config(self._write(tmp_path, {"chive.example": "chive"}))
