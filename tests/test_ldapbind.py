"""Tests for choco.ldapbind: the hand-rolled LDAP simple bind.

The wire format is checked byte for byte against RFC 4511's BER, and the
whole bind against a small fake LDAP server on loopback -- plain and
TLS, the TLS one with a throwaway certificate so a wrong CA, a wrong
hostname and the system store can each be shown to refuse it.
"""

import shutil
import socket
import ssl
import subprocess
import threading

import pytest

from choco import ldapbind as L


# --- encoding ------------------------------------------------------------------------

def test_bind_request_bytes():
    # LDAPMessage{ id 1, [APP 0]{ version 3, "cn=a", [0] "b" } }
    assert L.bind_request("cn=a", "b") == bytes.fromhex(
        "3011" "020101" "600c" "020103" "0404" "636e3d61" "8001" "62")


def test_long_values_use_long_form_lengths():
    msg = L.bind_request("cn=" + "x" * 300, "p" * 200)
    assert msg[:2] == b"\x30\x82"                      # two length bytes follow
    tag, body, end = L._read_tlv(msg, 0)
    assert tag == 0x30 and end == len(msg)
    # the password is the last element, in full
    assert body.endswith(b"\x80\x81\xc8" + b"p" * 200)


def test_unbind_request_bytes():
    assert L.unbind_request() == bytes.fromhex("3005" "020102" "4200")


def test_utf8_dn_and_password():
    msg = L.bind_request("uid=zoë,dc=x", "pässwörd")
    assert "zoë".encode() in msg and "pässwörd".encode() in msg


# --- parsing responses ----------------------------------------------------------------

def _response(code, message_id=1, diag=b"", tag=0x61):
    result = L._tlv(0x0A, bytes([code])) + L._tlv(0x04, b"") + L._tlv(0x04, diag)
    return L._tlv(0x30, L._integer(message_id) + L._tlv(tag, result))


def test_success_parses():
    L.parse_bind_response(_response(0))


def test_invalid_credentials_is_rejected_with_its_code():
    with pytest.raises(L.BindRejected) as e:
        L.parse_bind_response(_response(49, diag=b"INVALID_CREDENTIALS"))
    assert e.value.code == 49 and "invalidCredentials" in str(e.value)
    assert e.value.diagnostic == "INVALID_CREDENTIALS"


@pytest.mark.parametrize("message, match", [
    (_response(0, message_id=7), "expected 1"),
    (_response(0, message_id=0, tag=0x78), "disconnection"),
    (_response(0, tag=0x65), "BindResponse"),
    (bytes.fromhex("3003020101"), "truncated"),
    (bytes.fromhex("3080020101"), "indefinite"),
    (bytes.fromhex("0403616263"), "one LDAPMessage"),
    (_response(0) + b"\x00", "one LDAPMessage"),
    (L._tlv(0x30, L._integer(1) + L._tlv(0x61, L._tlv(0x04, b""))), "result code"),
])
def test_anything_but_a_clean_success_is_an_error(message, match):
    with pytest.raises(L.LdapError, match=match):
        L.parse_bind_response(message)


def test_a_rejection_is_never_mistaken_for_success_when_diagnostics_are_missing():
    bare = L._tlv(0x30, L._integer(1) + L._tlv(0x61, L._tlv(0x0A, b"\x31")))
    with pytest.raises(L.BindRejected) as e:
        L.parse_bind_response(bare)
    assert e.value.code == 49


# --- escaping --------------------------------------------------------------------------

@pytest.mark.parametrize("raw, escaped", [
    ("alice", "alice"),
    ("alice,cn=admins", "alice\\,cn\\=admins"),
    ("a+b<c>d;e\"f\\g", "a\\+b\\<c\\>d\\;e\\\"f\\\\g"),
    ("#x", "\\#x"), (" x", "\\ x"), ("x ", "x\\ "), (" ", "\\ "),
    ("a\\ ", "a\\\\\\ "),                       # backslash then a trailing space: both escaped
    ("a\x00b", "a\\00b"),
    ("x#y z", "x#y z"),                         # # and space are only special at the ends
])
def test_escape_rdn(raw, escaped):
    assert L.escape_rdn(raw) == escaped


# --- the server address ------------------------------------------------------------------

@pytest.mark.parametrize("host, port, use_ssl, want", [
    ("ldaps://ipa.example", 636, True, ("ipa.example", 636, True)),
    ("ldaps://ipa.example", 636, False, ("ipa.example", 636, True)),   # the scheme wins
    ("ldap://ipa.example", 389, True, ("ipa.example", 389, False)),
    ("ipa.example", None, True, ("ipa.example", 636, True)),
    ("ipa.example", None, False, ("ipa.example", 389, False)),
    ("ldaps://ipa.example:1636", 636, True, ("ipa.example", 1636, True)),
    ("ldaps://ipa.example/", 636, True, ("ipa.example", 636, True)),
    ("ldaps://[::1]:1636", 636, True, ("::1", 1636, True)),
    ("ldaps://[::1]", 636, True, ("::1", 636, True)),
])
def test_server_address(host, port, use_ssl, want):
    s = L.LdapServer(host, port=port, use_ssl=use_ssl)
    assert (s.host, s.port, s.ssl) == want


@pytest.mark.parametrize("host", ["ldapi:///run/slapd", "http://x", "ldaps://x:port",
                                  "ldaps://[::1", "ldaps://", "ldaps://x:70000"])
def test_bad_server_addresses_are_refused(host):
    with pytest.raises(ValueError):
        L.LdapServer(host)


def test_tls_context_always_verifies():
    ctx = L.LdapServer("ldaps://ipa.example").ssl_context()
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname is True


# --- binds against a fake server -----------------------------------------------------------

class FakeLdap:
    """A one-connection-at-a-time LDAP server on loopback that answers a
    bind with *reply(request_bytes)* and records what it was sent."""

    def __init__(self, reply, tls=None):
        self.reply, self.tls = reply, tls
        self.requests = []
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            try:
                if self.tls:
                    conn = self.tls.wrap_socket(conn, server_side=True)
                conn.settimeout(5)
                req = L._recv_message(conn)
                self.requests.append(req)
                out = self.reply(req)
                if out:
                    conn.sendall(out)
                    try:
                        self.requests.append(L._recv_message(conn))    # the unbind
                    except (L.LdapError, OSError):
                        pass
            except (OSError, L.LdapError, ssl.SSLError):
                pass
            finally:
                conn.close()

    def close(self):
        self.sock.close()


def test_bind_succeeds_then_unbinds():
    srv = FakeLdap(lambda req: _response(0))
    try:
        L.simple_bind(L.LdapServer("127.0.0.1", port=srv.port, use_ssl=False), "uid=a,dc=x", "pw")
        srv.thread.join(0.5)
        assert srv.requests[0] == L.bind_request("uid=a,dc=x", "pw")
        assert srv.requests[1] == L.unbind_request()
    finally:
        srv.close()


def test_bind_rejected():
    srv = FakeLdap(lambda req: _response(49))
    try:
        with pytest.raises(L.BindRejected):
            L.simple_bind(L.LdapServer("127.0.0.1", port=srv.port, use_ssl=False), "uid=a,dc=x", "bad")
    finally:
        srv.close()


def test_silent_server_times_out():
    srv = FakeLdap(lambda req: None)
    hold = threading.Event()
    srv.reply = lambda req: hold.wait(3) and None
    try:
        with pytest.raises(L.LdapError):
            L.simple_bind(L.LdapServer("127.0.0.1", port=srv.port, use_ssl=False), "uid=a,dc=x", "pw",
                          timeout=0.3)
    finally:
        hold.set()
        srv.close()


def test_nothing_listening_is_an_ldap_error():
    s = socket.create_server(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    with pytest.raises(L.LdapError, match="cannot connect"):
        L.simple_bind(L.LdapServer("127.0.0.1", port=port, use_ssl=False), "uid=a,dc=x", "pw")


def test_empty_password_never_reaches_the_wire():
    srv = FakeLdap(lambda req: _response(0))
    try:
        with pytest.raises(ValueError):
            L.simple_bind(L.LdapServer("127.0.0.1", port=srv.port, use_ssl=False), "uid=a,dc=x", "")
        assert srv.requests == []
    finally:
        srv.close()


@pytest.fixture(scope="module")
def cert(tmp_path_factory):
    """A self-signed certificate for ``localhost`` (DNS name only)."""
    if not shutil.which("openssl"):
        pytest.skip("openssl not available")
    d = tmp_path_factory.mktemp("tls")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-keyout", str(d / "key.pem"), "-out", str(d / "cert.pem"),
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
                   check=True, capture_output=True)
    return d / "cert.pem", d / "key.pem"


@pytest.fixture
def tls_server(cert):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(*map(str, cert))
    srv = FakeLdap(lambda req: _response(0), tls=ctx)
    yield srv
    srv.close()


def test_tls_bind_with_the_right_ca_and_name(tls_server, cert):
    server = L.LdapServer(f"ldaps://localhost:{tls_server.port}", ca_cert=str(cert[0]))
    L.simple_bind(server, "uid=a,dc=x", "pw")
    assert tls_server.requests[0] == L.bind_request("uid=a,dc=x", "pw")


def test_tls_refuses_a_hostname_the_certificate_does_not_name(tls_server, cert):
    # the certificate is for "localhost"; by IP the name check fails
    server = L.LdapServer(f"ldaps://127.0.0.1:{tls_server.port}", ca_cert=str(cert[0]))
    with pytest.raises(L.LdapError, match="TLS"):
        L.simple_bind(server, "uid=a,dc=x", "pw")
    assert tls_server.requests == []


def test_tls_refuses_a_certificate_outside_the_trust_store(tls_server):
    server = L.LdapServer(f"ldaps://localhost:{tls_server.port}")       # system store only
    with pytest.raises(L.LdapError, match="TLS"):
        L.simple_bind(server, "uid=a,dc=x", "pw")
    assert tls_server.requests == []
