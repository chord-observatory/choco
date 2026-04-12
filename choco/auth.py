"""LDAP authentication for choco."""

import logging
from functools import wraps

from flask import Flask, Response, request, url_for
from flask_login import LoginManager, UserMixin, current_user

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
    """Initialize Flask-Login and Flask-LDAP3-Login on the app.

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

    # Flask-LDAP3-Login setup
    ldap = config.get("ldap", {}) or {}
    ldap_host = ldap.get("host")
    if not ldap_host:
        logger.warning(
            "ldap.host not set in config. LDAP authentication will not work."
        )
        app.config["LDAP_ENABLED"] = False
        return

    app.config["LDAP_ENABLED"] = True
    app.config["LDAP_HOST"] = ldap_host
    app.config["LDAP_PORT"] = int(ldap.get("port", 636))
    app.config["LDAP_USE_SSL"] = ldap.get("use_ssl", True)
    app.config["LDAP_BASE_DN"] = ldap.get("base_dn", "")
    app.config["LDAP_USER_DN"] = ldap.get("user_dn", "cn=users,cn=accounts")
    app.config["LDAP_USER_SEARCH_SCOPE"] = ldap.get("user_search_scope", "SUBTREE")
    app.config["LDAP_USER_LOGIN_ATTR"] = ldap.get("user_login_attr", "uid")
    app.config["LDAP_USER_RDN_ATTR"] = ldap.get("user_login_attr", "uid")
    app.config["LDAP_USER_OBJECT_FILTER"] = ldap.get(
        "user_object_filter", "(objectclass=posixaccount)"
    )

    # Disable group searching (FreeIPA uses posixgroup, not AD's "group")
    app.config["LDAP_GROUP_DN"] = ""
    app.config["LDAP_GROUP_OBJECT_FILTER"] = ""

    # Service account for searching (required for FreeIPA — no anonymous bind)
    bind_dn = ldap.get("bind_dn")
    if bind_dn:
        app.config["LDAP_BIND_USER_DN"] = bind_dn
        app.config["LDAP_BIND_USER_PASSWORD"] = ldap.get("bind_password", "")

    from flask_ldap3_login import LDAP3LoginManager

    ldap_manager = LDAP3LoginManager(app)
    app.config["ldap_manager"] = ldap_manager
    logger.info(f"LDAP authentication configured (server: {ldap_host})")
