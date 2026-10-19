"""Login sessions and LDAP authentication for choco.

A session is Flask's signed cookie carrying the user's id under
``_user_id``; each request looks it up in the in-memory ``_users``
(:func:`current_user`), so a restart logs everyone out and an id the
store does not know -- a trusted peer's synthetic user, a stale cookie --
loads as nobody.  Flask-Login did this until 2026-10 (dependencies.md).
"""

import hmac
import ipaddress
import logging
import os
import re
from functools import wraps
from urllib.parse import urlencode, urlsplit

from flask import Flask, Response, abort, current_app, g, request, session, url_for
from werkzeug.local import LocalProxy

from .ldapbind import LdapError, LdapServer, escape_rdn, simple_bind

logger = logging.getLogger(__name__)

# In-memory user store: DN -> User
_users: dict[str, "User"] = {}


class User:
    """Authenticated user backed by LDAP, or -- ``trusted=True`` -- the
    synthetic user a ``server.trusted_hosts`` peer is logged in as."""

    is_authenticated = True

    def __init__(self, dn: str, username: str, *, trusted: bool = False):
        self.dn = dn
        self.username = username
        self.trusted = trusted

    def get_id(self) -> str:
        return self.dn

    def __repr__(self) -> str:
        return f"User({self.username})"


class _Anonymous:
    """Nobody: what ``current_user`` is before login."""

    is_authenticated = False
    trusted = False
    username = None
    dn = None


ANONYMOUS = _Anonymous()


def save_user(dn: str, username: str) -> User:
    """Create or update a user in the in-memory store."""
    user = User(dn, username)
    _users[dn] = user
    return user


def login_user(user: User) -> None:
    """Start *user*'s session: the id goes into the signed cookie, and the
    user is current for the rest of this request."""
    session["_user_id"] = user.get_id()
    g.user = user


def logout_user() -> None:
    session.pop("_user_id", None)
    g.user = ANONYMOUS


def _current_user():
    if "user" not in g:
        user_id = session.get("_user_id")
        g.user = (_users.get(user_id) if user_id else None) or ANONYMOUS
    return g.user


#: The request's user (``User`` or ``ANONYMOUS``), resolved once per
#: request from the session; a template global too (``init_auth``).
current_user = LocalProxy(_current_user)


def unauthorized() -> Response:
    """The reply to a request that needs a login: redirect to the login
    page, carrying the attempted URL as ?next= so a successful login
    returns there (the login view validates it as a same-site path).

    For htmx requests, HX-Redirect makes the browser do a full-page
    navigation instead of swapping login HTML into a partial — and the
    place to return to is the *page* the user was on (HX-Current-URL),
    not the polled partial that happened to hit the 302.  Only the
    header's path+query are kept, so it cannot smuggle in another host.
    A plain POST (an expired session's toggle) gets no next: replaying it
    as a GET after login would just 405.
    """
    next_path = None
    if request.headers.get("HX-Request"):
        current = urlsplit(request.headers.get("HX-Current-URL", ""))
        path = current.path
        # The browser's URL carries the proxy's prefix; ?next= is relative
        # to the app root (``_next_target`` puts the prefix back).
        root = request.script_root
        if root and (path == root or path.startswith(root + "/")):
            path = path[len(root):] or "/"
        if path.startswith("/"):
            next_path = path + (
                f"?{current.query}" if current.query else "")
    elif request.method == "GET":
        query = request.query_string.decode()
        next_path = request.path + (f"?{query}" if query else "")

    login_url = url_for("web.login")
    if next_path and next_path != "/" and not next_path.startswith("//"):
        login_url += "?" + urlencode({"next": next_path})
    if request.headers.get("HX-Request"):
        return Response(status=200, headers={"HX-Redirect": login_url})
    return Response(status=302, headers={"Location": login_url})


def login_required(f):
    """Route decorator: the request's user must be logged in."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return unauthorized()
        return f(*args, **kwargs)
    return decorated


def parse_trusted_hosts(raw) -> list[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, str]]:
    """Validate ``server.trusted_hosts`` into ``[(network, label), ...]``.

    The config value is a mapping of address or CIDR network to the label
    the peer is logged in as (``127.0.0.1: localhost``, ``10.222.0.54:
    chive``).  Keys are stringified before parsing so YAML's scalar
    guessing cannot change their meaning; a bare address is a /32 (or
    /128).  The result is sorted most-specific first so a host entry
    inside a listed network wins.  Raises ``ValueError`` on anything
    malformed -- called from ``load_config`` so a typo is a startup
    error, not a silently open or silently closed door.
    """
    if not raw:
        return []
    if not isinstance(raw, dict):
        raise ValueError(
            "server.trusted_hosts must be a mapping of address (or CIDR "
            "network) to label, e.g. {127.0.0.1: localhost}; "
            f"got {type(raw).__name__}"
        )
    out = []
    for key, label in raw.items():
        try:
            net = ipaddress.ip_network(str(key).strip(), strict=False)
        except ValueError as e:
            raise ValueError(
                f"server.trusted_hosts: {key!r} is not an IP address or "
                f"CIDR network ({e})"
            ) from None
        if not isinstance(label, str) or not label.strip():
            raise ValueError(
                f"server.trusted_hosts: {key!r} needs a non-empty label "
                f"(the name audit lines record), got {label!r}"
            )
        out.append((net, label.strip()))
    out.sort(key=lambda item: item[0].prefixlen, reverse=True)
    return out


def peer_address(remote_addr: str | None):
    """*remote_addr* as an ``ipaddress`` object, or None if it is not one.

    A dual-stack bind ("::") reports IPv4 peers as ::ffff:a.b.c.d; those
    are judged by the IPv4 address, the one an operator writes down and
    the one a local job connects from.
    """
    try:
        addr = ipaddress.ip_address(remote_addr or "")
    except ValueError:
        return None
    if addr.version == 6 and addr.ipv4_mapped is not None:
        addr = addr.ipv4_mapped
    return addr


def is_loopback(remote_addr: str | None) -> bool:
    """Whether the peer is this host (the jobs and the CLI)."""
    addr = peer_address(remote_addr)
    return addr is not None and addr.is_loopback


def trusted_host_label(trusted: list, remote_addr: str | None) -> str | None:
    """The label for *remote_addr* if it is inside a trusted network."""
    addr = peer_address(remote_addr) if trusted else None
    if addr is None:
        return None
    for net, label in trusted:
        if addr.version == net.version and addr in net:
            return label
    return None


#: Sent by the reverse proxy with every request it has authenticated; the
#: value is ``server.proxy_auth.secret``.
PROXY_SECRET_HEADER = "X-Choco-Proxy-Secret"
#: Set by Authelia (through the proxy) to the signed-in username.
PROXY_USER_HEADER = "Remote-User"

_PROXY_PREFIX_RE = re.compile(r"(/[A-Za-z0-9._-]+)+")
_PROXY_USERNAME_RE = re.compile(r"[A-Za-z0-9._-]+")
_PROXY_KEYS = {"peer", "secret", "prefix", "logout_url"}
_MIN_PROXY_SECRET_LEN = 32


def parse_proxy_auth(raw, trusted: list) -> dict | None:
    """Validate ``server.proxy_auth`` into ``{peer, secret, prefix,
    logout_url}``, or None when it is absent (the default: no proxy).

    The proxy is trusted by address *and* secret: only a request whose TCP
    peer is ``peer`` and that carries ``secret`` is served under ``prefix``
    and signed in as the proxy's ``Remote-User``.  The peer may not be
    loopback (loopback callers already skip login on the API) nor inside
    ``trusted_hosts`` (every proxied request would be logged in as that
    host's label).  Raises ``ValueError`` naming the problem, so a typo is
    a startup error -- called from ``load_config`` and ``init_auth``.
    """
    if not raw:
        return None
    if not isinstance(raw, dict):
        raise ValueError("server.proxy_auth must be a mapping "
                         f"({', '.join(sorted(_PROXY_KEYS))})")
    unknown = set(raw) - _PROXY_KEYS
    if unknown:
        raise ValueError(f"server.proxy_auth: unknown key(s) {', '.join(sorted(map(str, unknown)))}")
    peer = peer_address(str(raw.get("peer") or "").strip())
    if peer is None:
        raise ValueError("server.proxy_auth.peer must be the proxy's IP address "
                         f"as choco sees it, got {raw.get('peer')!r}")
    if peer.is_loopback:
        raise ValueError("server.proxy_auth.peer may not be loopback: loopback "
                         "callers already skip login on the API")
    label = trusted_host_label(trusted, str(peer))
    if label:
        raise ValueError(f"server.proxy_auth.peer {peer} is inside "
                         f"server.trusted_hosts (as {label!r}), which would log "
                         "every proxied request in as that host")
    secret = str(raw.get("secret") or "")
    if len(secret) < _MIN_PROXY_SECRET_LEN:
        raise ValueError("server.proxy_auth.secret must be at least "
                         f"{_MIN_PROXY_SECRET_LEN} characters")
    prefix = str(raw.get("prefix") or "")
    if not _PROXY_PREFIX_RE.fullmatch(prefix):
        raise ValueError("server.proxy_auth.prefix must be a path like /choco "
                         f"(no trailing slash), got {prefix!r}")
    logout_url = str(raw.get("logout_url") or "/authelia/logout")
    if not logout_url.startswith("/") or logout_url.startswith("//"):
        raise ValueError("server.proxy_auth.logout_url must be a path on the "
                         f"proxy's site, got {logout_url!r}")
    return {"peer": peer, "secret": secret, "prefix": prefix,
            "logout_url": logout_url}


class ProxyMiddleware:
    """WSGI middleware: a request from ``proxy_auth.peer`` carrying the
    secret is marked proxied (``environ["choco.proxied"]``) and served
    under the proxy's path prefix (``SCRIPT_NAME``) over HTTPS, so
    ``url_for`` and ``request.script_root`` build the URLs the browser
    sees.  ``REMOTE_ADDR`` is never rewritten -- no ``X-Forwarded-For``
    is read -- so the loopback bypass and ``trusted_hosts`` still judge
    the real TCP peer.  The secret header is removed from every request.
    """

    def __init__(self, app, settings: dict):
        self.app = app
        self.peer = settings["peer"]
        self.secret = settings["secret"].encode()
        self.prefix = settings["prefix"]

    def __call__(self, environ, start_response):
        supplied = environ.pop("HTTP_" + PROXY_SECRET_HEADER.upper().replace("-", "_"), "")
        proxied = bool(supplied) and peer_address(environ.get("REMOTE_ADDR")) == self.peer \
            and hmac.compare_digest(supplied.encode("latin-1"), self.secret)
        environ["choco.proxied"] = proxied
        if proxied:
            environ["SCRIPT_NAME"] = self.prefix
            path = environ.get("PATH_INFO", "")
            # nginx strips the prefix (location /choco/ -> proxy_pass .../);
            # tolerate one that does not.
            if path == self.prefix or path.startswith(self.prefix + "/"):
                environ["PATH_INFO"] = path[len(self.prefix):] or "/"
            environ["wsgi.url_scheme"] = "https"
        return self.app(environ, start_response)


def is_proxied() -> bool:
    """Whether this request came through the authenticating proxy."""
    return bool(request.environ.get("choco.proxied"))


class LdapAuthenticator:
    """Direct-bind LDAP authentication, FreeIPA-flavoured defaults.

    The login attribute doubles as the RDN under a single user container,
    so a user's DN can be strung together
    (``uid=<user>,cn=users,cn=accounts,<base_dn>``) and bound directly
    with their password — the bind itself proves the credentials, no
    service account involved.
    """

    def __init__(self, host: str, port: int | None = None, use_ssl: bool = True,
                 base_dn: str = "", user_dn: str = "cn=users,cn=accounts",
                 login_attr: str = "uid", ca_cert: str | None = None,
                 timeout: float = 10.0):
        self.host = host
        # TLS verifies the certificate chain *and* the hostname, against
        # ca_cert or the system store (choco.ldapbind; there is no way to
        # turn it off).  The URL scheme decides TLS whatever use_ssl says
        # -- an ldaps:// host is TLS even with use_ssl false -- as it did
        # under ldap3, so the cleartext warning keys off server.ssl.  A
        # missing ca_cert file must be caught by the caller (init_auth):
        # it would only surface at the first login, as a TLS failure.
        # No port is ldapbind's rule (389, or 636 under TLS); a default
        # here would send an ldap:// bind in cleartext to the LDAPS port.
        self.server = LdapServer(host, port=None if port is None else int(port),
                                 use_ssl=use_ssl, ca_cert=ca_cert)
        self.timeout = float(timeout)
        self.login_attr = login_attr
        parts = [p for p in (user_dn.strip(), base_dn.strip()) if p]
        self.user_container = ",".join(parts)

    def user_dn_for(self, username: str) -> str:
        """The DN the user binds as; RDN-escaped, so a crafted username
        cannot splice extra components into the DN."""
        return f"{self.login_attr}={escape_rdn(username)},{self.user_container}"

    def authenticate(self, username: str, password: str) -> str | None:
        """The user's DN on success, None on any failure.  Never raises.

        The empty-password guard matters: LDAP treats a bind with a DN
        and no password as an *anonymous* bind, which "succeeds" on most
        servers — without the guard, a blank password would log anyone in.
        (``simple_bind`` refuses one too.)
        """
        if not username or not password:
            return None
        dn = self.user_dn_for(username)
        try:
            simple_bind(self.server, dn, password, timeout=self.timeout)
        except LdapError as e:
            logger.info(f"LDAP bind failed for '{username}': {e}")
            return None
        return dn


def dev_auth_active() -> bool:
    """True when running with ``server.dev_auth`` (loopback-only, no auth).

    CSRF has to come off with login rather than stay on: the token lives
    in the session cookie, so with auto-login re-established per request
    a stale cookie would 403 every POST — toggles, edits, PDB writes —
    with no login flow left to reset it.
    """
    return bool(current_app.config.get("DEV_AUTH"))


def require_csrf_token(token: str) -> None:
    """403 unless *token* is this session's CSRF token (constant-time)."""
    if dev_auth_active():
        return
    expected = session.get("_csrf_token") or ""
    # bytes: compare_digest refuses a non-ASCII str, which a forged form can send
    if not token or not hmac.compare_digest(token.encode(), expected.encode()):
        abort(403)


#: Methods that change state.
_WRITES = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def localhost_or_login_required(f):
    """Like @login_required, but loopback callers (the jobs and the CLI)
    need no login.

    A state-changing request from anywhere else is a browser session, and
    needs what every other browser write needs: the session's CSRF token
    (``X-CSRF-Token``).  It must also be JSON, which no cross-site HTML
    form can send, so an empty-bodied forgery cannot reach a route that
    acts on no body at all (``/api/configs/pull``).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if is_loopback(request.remote_addr):
            return f(*args, **kwargs)
        if not current_user.is_authenticated:
            return unauthorized()
        if request.method in _WRITES:
            require_csrf_token(request.headers.get("X-CSRF-Token", ""))
            if not request.is_json:
                abort(415)
        return f(*args, **kwargs)
    return decorated


def init_auth(app: Flask, config: dict):
    """Set up the login session, dev / trusted-host logins and the LDAP
    authenticator on the app.  LDAP settings are read from config["ldap"].
    """
    app.jinja_env.globals["current_user"] = current_user

    # Dev mode: log every request in as a synthetic user, so a developer
    # with no FreeIPA reachable still gets the whole UI.  Guarded by
    # load_config, which refuses to start unless the server is bound to
    # loopback (see the dev_auth check there) — this is not a role, it
    # is the absence of authentication.
    #
    # Deliberately re-established per request rather than left to
    # login_user's session cookie: dev instances are reached over ssh
    # tunnels, where the browser's cookie jar is shared across every
    # localhost port (cookies ignore the port), so a cookie minted by a
    # different choco on the same port would otherwise lock the
    # developer out with a CSRF 403 and no way to log back in.
    dev_user = (config.get("server") or {}).get("dev_auth")
    app.config["DEV_AUTH"] = dev_user
    if dev_user:
        logger.warning(
            f"DEV MODE: server.dev_auth={dev_user!r} — login and CSRF "
            f"checks are DISABLED for every request. Loopback only."
        )

        @app.before_request
        def _dev_auto_login():
            if not current_user.is_authenticated:
                login_user(save_user(f"dev:{dev_user}", dev_user))

    # Trusted hosts: a peer inside ``server.trusted_hosts`` is logged in as
    # a synthetic user named by its label, so the whole UI (which gates
    # htmx and the toggles on ``is_authenticated``) works with no LDAP.
    # Three properties keep this narrower than dev mode:
    #
    # * CSRF stays on.  The session cookie round-trips normally, so the
    #   token works; and a hostile page in the operator's browser could
    #   otherwise POST to localhost unchallenged.
    # * The user is built with ``User(...)``, never ``save_user``, so it
    #   is not in ``_users``: the cookie this mints loads as *nobody* on
    #   the next request and only a trusted peer gets logged back in.
    #   Replaying it from any other address is worthless.
    # * A real login is left alone -- ``is_authenticated`` is tested
    #   first -- so audit lines name the person when there is one.
    #
    # Stored as TRUSTED_PEERS, not TRUSTED_HOSTS: Flask >= 3.1 reads the
    # latter as its Host-header allowlist and would reject every request.
    trusted = parse_trusted_hosts((config.get("server") or {}).get("trusted_hosts"))
    app.config["TRUSTED_PEERS"] = trusted
    if trusted:
        logger.warning(
            "server.trusted_hosts: requests from %s are logged in without "
            "credentials",
            ", ".join(f"{net} as {label!r}" for net, label in trusted),
        )

        @app.before_request
        def _trusted_host_login():
            if current_user.is_authenticated:
                return
            label = trusted_host_label(trusted, request.remote_addr)
            if label:
                login_user(User(f"trusted:{request.remote_addr}", label,
                                trusted=True))

    # Proxy sign-in: a request the authenticating reverse proxy vouched for
    # (ProxyMiddleware: right peer *and* secret) is signed in as the
    # proxy's Remote-User, a real user in ``_users`` so audit lines name
    # the person.  The header decides on every request: a session for
    # someone else is replaced.  Checked after trusted hosts, which can
    # never match the proxy (parse_proxy_auth refuses that).
    proxy = parse_proxy_auth((config.get("server") or {}).get("proxy_auth"), trusted)
    app.config["PROXY_AUTH"] = proxy
    if proxy:
        app.wsgi_app = ProxyMiddleware(app.wsgi_app, proxy)
        logger.info("server.proxy_auth: requests from %s with the proxy secret "
                    "are served under %s and signed in as %s",
                    proxy["peer"], proxy["prefix"], PROXY_USER_HEADER)

        @app.before_request
        def _proxy_login():
            if not is_proxied():
                return
            username = request.headers.get(PROXY_USER_HEADER, "").strip()
            if not _PROXY_USERNAME_RE.fullmatch(username):
                abort(403)
            if (current_user.is_authenticated and not current_user.trusted
                    and current_user.username == username):
                return
            session.clear()
            authenticator = current_app.config.get("ldap_authenticator")
            dn = authenticator.user_dn_for(username) if authenticator else f"proxy:{username}"
            login_user(save_user(dn, username))

    # LDAP setup: direct bind, so no service account and no search filter.
    ldap = config.get("ldap", {}) or {}
    ldap_host = ldap.get("host")
    if not ldap_host:
        logger.warning(
            "ldap.host not set in config. LDAP authentication will not work."
        )
        app.config["LDAP_ENABLED"] = False
        return

    use_ssl = bool(ldap.get("use_ssl", True))
    ca_cert = ldap.get("ca_cert") or None
    if ca_cert and not os.path.isfile(ca_cert):
        # Refuse at startup: the operator asked for a specific CA, and a
        # missing file would only show up as every login failing TLS.
        raise ValueError(
            f"ldap.ca_cert is set to {ca_cert!r} but no such file exists.")
    app.config["LDAP_ENABLED"] = True
    authenticator = LdapAuthenticator(
        host=ldap_host,
        port=ldap.get("port"),
        use_ssl=use_ssl,
        base_dn=ldap.get("base_dn", ""),
        user_dn=ldap.get("user_dn", "cn=users,cn=accounts"),
        login_attr=ldap.get("user_login_attr", "uid"),
        ca_cert=ca_cert,
    )
    app.config["ldap_authenticator"] = authenticator
    # The *effective* mode, not the flag: an ldaps:// host overrides
    # use_ssl.
    if not authenticator.server.ssl:
        logger.warning(
            "LDAP connection is plaintext (ldap.use_ssl false, host not "
            "ldaps://): user passwords will cross the network in cleartext "
            "on every login.")
    logger.info(f"LDAP authentication configured (server: {ldap_host})")
