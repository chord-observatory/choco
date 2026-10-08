"""LDAP simple bind on the standard library (RFC 4511).

choco authenticates by binding as the user (``auth.LdapAuthenticator``):
one BindRequest, one BindResponse, an UnbindRequest, close.  That is all
of LDAP it needs, so this module speaks exactly that much -- BER-encoded
by hand, over ``ssl``/``socket`` -- in place of ldap3, whose last stable
release is from 2021 and which calls a pyasn1 API that is already
deprecated (docs/design/auth.md).

TLS is ``ssl.create_default_context``: certificate *and* hostname
verified, against ``ca_cert`` or the system store.  Nothing here can turn
verification off.  Under gevent the sockets are cooperative.
"""

from __future__ import annotations

import socket
import ssl

#: LDAPv3 result codes worth a name in the log (RFC 4511 §4.1.9).
RESULT_NAMES = {
    0: "success", 1: "operationsError", 2: "protocolError", 7: "authMethodNotSupported",
    8: "strongerAuthRequired", 13: "confidentialityRequired", 19: "constraintViolation",
    32: "noSuchObject", 34: "invalidDNSyntax", 48: "inappropriateAuthentication",
    49: "invalidCredentials", 50: "insufficientAccessRights", 51: "busy", 52: "unavailable",
    53: "unwillingToPerform", 80: "other",
}

#: A BindResponse is a few dozen bytes; anything claiming more is not one.
MAX_RESPONSE = 64 * 1024

_BIND_ID = 1
_UNBIND_ID = 2


class LdapError(Exception):
    """The bind did not succeed: connection, TLS, protocol or result."""


class BindRejected(LdapError):
    """The server answered the bind with a non-zero result code."""

    def __init__(self, code: int, diagnostic: str = ""):
        self.code = code
        self.diagnostic = diagnostic
        super().__init__(f"{RESULT_NAMES.get(code, 'result')} ({code})"
                         + (f": {diagnostic}" if diagnostic else ""))


# --- the server address --------------------------------------------------------

class LdapServer:
    """Where to bind: ``host`` as config.yaml gives it -- ``ldaps://h``,
    ``ldap://h``, ``h``, any of them with ``:port``, IPv6 in brackets --
    resolved the way ldap3 resolved it, so a deployed config means what
    it meant: the URL scheme decides TLS whatever *use_ssl* says, a bare
    host follows *use_ssl*, a port in the string beats *port*, and no
    port at all is 389 or 636."""

    def __init__(self, host: str, port: int | None = None, use_ssl: bool = True,
                 ca_cert: str | None = None):
        h = host.strip()
        low = h.lower()
        if low.startswith("ldaps://"):
            h, use_ssl = h[8:], True
        elif low.startswith("ldap://"):
            h, use_ssl = h[7:], False
        elif "://" in low:
            raise ValueError(f"ldap.host {host!r}: only ldap:// and ldaps:// are supported")
        h = h.rstrip("/")
        if h.startswith("["):                                   # [v6]:port
            name, sep, rest = h[1:].partition("]")
            if not sep or (rest and not (rest.startswith(":") and rest[1:].isdecimal())):
                raise ValueError(f"ldap.host {host!r}: not a valid [IPv6]:port address")
            h, port = name, (int(rest[1:]) if rest else port)
        elif h.count(":") == 1:
            h, _, p = h.partition(":")
            if not p.isdecimal():
                raise ValueError(f"ldap.host {host!r}: the port must be a number")
            port = int(p) or port
        if not h:
            raise ValueError(f"ldap.host {host!r}: no host name")
        self.host = h
        self.ssl = bool(use_ssl)
        self.port = int(port) if port else (636 if self.ssl else 389)
        if not 0 < self.port < 65536:
            raise ValueError(f"ldap port {self.port} is out of range")
        self.ca_cert = ca_cert or None

    def ssl_context(self) -> ssl.SSLContext:
        """CERT_REQUIRED with hostname checking, against ``ca_cert`` or
        the system store (an IPA-enrolled host has the IPA CA there)."""
        return ssl.create_default_context(cafile=self.ca_cert)

    def __repr__(self) -> str:
        return f"LdapServer({'ldaps' if self.ssl else 'ldap'}://{self.host}:{self.port})"


# --- BER, as much as a bind needs ------------------------------------------------

def _length(n: int) -> bytes:
    if n < 0x80:
        return bytes([n])
    b = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(b)]) + b


def _tlv(tag: int, value: bytes) -> bytes:
    return bytes([tag]) + _length(len(value)) + value


def _integer(v: int) -> bytes:
    return _tlv(0x02, v.to_bytes(max(1, (v.bit_length() + 8) // 8), "big", signed=True))


def bind_request(dn: str, password: str, message_id: int = _BIND_ID) -> bytes:
    """LDAPMessage{ messageID, BindRequest{ version 3, name, simple } }."""
    body = _integer(3) + _tlv(0x04, dn.encode("utf-8")) + _tlv(0x80, password.encode("utf-8"))
    return _tlv(0x30, _integer(message_id) + _tlv(0x60, body))


def unbind_request(message_id: int = _UNBIND_ID) -> bytes:
    return _tlv(0x30, _integer(message_id) + b"\x42\x00")


def _read_tlv(buf: bytes, i: int) -> tuple[int, bytes, int]:
    """``(tag, value, next_offset)`` of the element at *buf[i:]*; definite
    lengths only, every bound checked."""
    if i + 2 > len(buf):
        raise LdapError("truncated response")
    tag, first = buf[i], buf[i + 1]
    i += 2
    if first == 0x80:
        raise LdapError("indefinite length is not allowed in LDAP")
    if first & 0x80:
        n = first & 0x7F
        if n > 4 or i + n > len(buf):
            raise LdapError("malformed length")
        length = int.from_bytes(buf[i:i + n], "big")
        i += n
    else:
        length = first
    if i + length > len(buf):
        raise LdapError("truncated response")
    return tag, buf[i:i + length], i + length


def parse_bind_response(message: bytes, message_id: int = _BIND_ID) -> None:
    """Return quietly if *message* (one whole LDAPMessage, outer tag
    included) is a successful BindResponse to *message_id*; raise
    :class:`BindRejected` for a result code, :class:`LdapError` for
    anything else -- a Notice of Disconnection, another message, junk."""
    tag, body, end = _read_tlv(message, 0)
    if tag != 0x30 or end != len(message):
        raise LdapError("response is not one LDAPMessage")
    tag, mid, i = _read_tlv(body, 0)
    if tag != 0x02 or not mid:
        raise LdapError("response has no message ID")
    got_id = int.from_bytes(mid, "big", signed=True)
    tag, op, _ = _read_tlv(body, i)
    if tag == 0x78 and got_id == 0:
        raise LdapError("server sent a notice of disconnection")
    if tag != 0x61:
        raise LdapError(f"expected a BindResponse, got tag 0x{tag:02x}")
    if got_id != message_id:
        raise LdapError(f"BindResponse for message {got_id}, expected {message_id}")
    tag, code, j = _read_tlv(op, 0)
    if tag != 0x0A or not code:
        raise LdapError("BindResponse has no result code")
    result = int.from_bytes(code, "big", signed=True)
    diagnostic = ""
    try:
        _, _, j = _read_tlv(op, j)                                   # matchedDN
        tag, diag, _ = _read_tlv(op, j)                              # diagnosticMessage
        if tag == 0x04:
            diagnostic = diag.decode("utf-8", "replace")[:200]
    except LdapError:
        pass                                                         # optional detail only
    if result != 0:
        raise BindRejected(result, diagnostic)


def _recv_exact(sock, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise LdapError("connection closed by the server")
        out += chunk
    return bytes(out)


def _recv_message(sock) -> bytes:
    """One LDAPMessage off the wire, length-checked before it is read."""
    head = _recv_exact(sock, 2)
    if head[0] != 0x30:
        raise LdapError(f"response does not start an LDAPMessage (0x{head[0]:02x})")
    first = head[1]
    if first == 0x80:
        raise LdapError("indefinite length is not allowed in LDAP")
    if first & 0x80:
        n = first & 0x7F
        if n > 4:
            raise LdapError("malformed length")
        lb = _recv_exact(sock, n)
        length = int.from_bytes(lb, "big")
        head += lb
    else:
        length = first
    if length > MAX_RESPONSE:
        raise LdapError(f"response claims {length} bytes")
    return head + _recv_exact(sock, length)


# --- the bind ----------------------------------------------------------------------

def simple_bind(server: LdapServer, dn: str, password: str, timeout: float = 10.0) -> None:
    """Bind as *dn* with *password*; return on success, raise
    :class:`LdapError` (:class:`BindRejected` for a refused bind)
    otherwise.  An empty password is refused here too: on the wire it
    is an anonymous bind, which most servers accept."""
    if not dn or not password:
        raise ValueError("a simple bind needs a DN and a non-empty password")
    try:
        raw = socket.create_connection((server.host, server.port), timeout=timeout)
    except OSError as e:
        raise LdapError(f"cannot connect to {server.host}:{server.port}: {e}") from e
    sock = raw
    try:
        if server.ssl:
            try:
                sock = server.ssl_context().wrap_socket(raw, server_hostname=server.host)
            except (ssl.SSLError, ssl.CertificateError, OSError) as e:
                raise LdapError(f"TLS to {server.host}: {e}") from e
        try:
            sock.sendall(bind_request(dn, password))
            message = _recv_message(sock)
        except OSError as e:                                         # timeouts included
            raise LdapError(f"bind to {server.host}: {e}") from e
        parse_bind_response(message)
        try:
            sock.sendall(unbind_request())
        except OSError:
            pass                                                     # the bind already succeeded
    finally:
        if sock is not raw:
            sock.close()
        raw.close()


def escape_rdn(value: str) -> str:
    """RFC 4514 escaping for an attribute value inside a DN, so a crafted
    username cannot splice extra components in: the specials anywhere, a
    leading ``#`` or space, a trailing space, NUL as ``\\00``."""
    out = []
    last = len(value) - 1
    for i, ch in enumerate(value):
        if ch == "\x00":
            out.append("\\00")
        elif ch in '\\,+"<>;=' or (i == 0 and ch in "# ") or (i == last and ch == " "):
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)
