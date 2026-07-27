"""LDAP authentication for choco."""

import logging
from functools import wraps

import ldap3
from ldap3.core.exceptions import LDAPException
from ldap3.utils.dn import escape_rdn
from flask import Flask, Response, request, url_for
from flask_login import LoginManager, UserMixin, current_user, login_user

logger = logging.getLogger(__name__)

# In-memory user store: DN -> User
_users: dict[str, "User"] = {}


class User(UserMixin):
    """Authenticated user backed by LDAP."""

    def __init__(self, dn: str, username: str, data: dict | None = None):
        self.dn = dn
        self.username = username
        self.data = data or {}

    def get_id(self) -> str:
        return self.dn

    def __repr__(self) -> str:
        return f"User({self.username})"


def save_user(dn: str, username: str, data: dict | None = None) -> User:
    """Create or update a user in the in-memory store."""
    user = User(dn, username, data)
    _users[dn] = user
    return user


class LdapAuthenticator:
    """Direct-bind LDAP authentication, FreeIPA-flavoured defaults.

    The login attribute doubles as the RDN under a single user container,
    so a user's DN can be strung together
    (``uid=<user>,cn=users,cn=accounts,<base_dn>``) and bound directly
    with their password — the bind itself proves the credentials, no
    service account involved.

    This replaces flask-ldap3-login with the one code path choco ever
    exercised: with the RDN attribute always set equal to the login
    attribute, the wrapper's search-bind mode (the reason for its
    ``bind_dn`` service account) was unreachable dead weight, along with
    its Flask-WTF/WTForms dependencies.
    """

    def __init__(self, host: str, port: int = 636, use_ssl: bool = True,
                 base_dn: str = "", user_dn: str = "cn=users,cn=accounts",
                 login_attr: str = "uid"):
        self.host = host
        self.server = ldap3.Server(host, port=int(port), use_ssl=use_ssl)
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
        """
        if not username or not password:
            return None
        dn = self.user_dn_for(username)
        try:
            # Context manager: binds on enter (raising on bad
            # credentials), unbinds on exit.
            with ldap3.Connection(self.server, user=dn, password=password,
                                  authentication=ldap3.SIMPLE,
                                  raise_exceptions=True):
                pass
        except LDAPException as e:
            logger.info(f"LDAP bind failed for '{username}': "
                        f"{type(e).__name__}")
            return None
        return dn


def localhost_or_login_required(f):
    """Like @login_required, but skip auth for requests from localhost."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.remote_addr in ("127.0.0.1", "::1"):
            return f(*args, **kwargs)
        if not current_user.is_authenticated:
            from flask import current_app
            return current_app.login_manager.unauthorized()
        return f(*args, **kwargs)
    return decorated


def init_auth(app: Flask, config: dict):
    """Initialize Flask-Login and the LDAP authenticator on the app.

    LDAP settings are read from config["ldap"].
    """
    # Flask-Login setup
    login_manager = LoginManager()
    login_manager.login_view = "web.login"
    login_manager.login_message = "Please log in to access choco."
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return _users.get(user_id)

    @login_manager.unauthorized_handler
    def unauthorized():
        """Redirect to login; for htmx requests use HX-Redirect for a
        full-page navigation instead of swapping login HTML into a partial."""
        login_url = url_for("web.login")
        if request.headers.get("HX-Request"):
            return Response(status=200, headers={"HX-Redirect": login_url})
        return Response(status=302, headers={"Location": login_url})

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

    # LDAP setup.  Legacy keys from the flask-ldap3-login era (bind_dn,
    # bind_password, user_object_filter, user_search_scope) are simply
    # ignored — they only fed the search-bind path choco never took, so
    # a deployed config.yaml keeps working (and the bind account's
    # credential can be deleted from it).
    ldap = config.get("ldap", {}) or {}
    ldap_host = ldap.get("host")
    if not ldap_host:
        logger.warning(
            "ldap.host not set in config. LDAP authentication will not work."
        )
        app.config["LDAP_ENABLED"] = False
        return

    app.config["LDAP_ENABLED"] = True
    app.config["ldap_authenticator"] = LdapAuthenticator(
        host=ldap_host,
        port=int(ldap.get("port", 636)),
        use_ssl=bool(ldap.get("use_ssl", True)),
        base_dn=ldap.get("base_dn", ""),
        user_dn=ldap.get("user_dn", "cn=users,cn=accounts"),
        login_attr=ldap.get("user_login_attr", "uid"),
    )
    logger.info(f"LDAP authentication configured (server: {ldap_host})")
