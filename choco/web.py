"""Flask routes for the choco web UI."""

import logging
import secrets

from flask import (
    Blueprint, render_template, request, redirect, url_for, flash,
    current_app, session, abort,
)
from flask_login import login_required, login_user, logout_user, current_user

from .auth import save_user

logger = logging.getLogger(__name__)

bp = Blueprint("web", __name__)


def _csrf_token() -> str:
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_hex(32)
    return session["_csrf_token"]


def _check_csrf():
    token = request.form.get("_csrf_token", "")
    if not token or token != session.get("_csrf_token"):
        abort(403)


@bp.app_context_processor
def inject_csrf():
    return {"csrf_token": _csrf_token}


def _registry():
    return current_app.config["registry"]


def _sync_loop():
    return current_app.config["sync_loop"]


# --- Authentication routes ---

@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("web.dashboard"))

    if request.method == "POST":
        _check_csrf()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Username and password are required.", "error")
            return render_template("login.html")

        if not current_app.config.get("LDAP_ENABLED"):
            flash("LDAP is not configured. Set ldap.host in config.yaml.", "error")
            return render_template("login.html")

        ldap_manager = current_app.config["ldap_manager"]
        result = ldap_manager.authenticate(username, password)

        if result.status.name == "success":
            user = save_user(result.user_dn, result.user_id, result.user_info)
            login_user(user)
            next_page = request.args.get("next", "")
            if not next_page or next_page.startswith(("//", "http:", "https:")):
                next_page = url_for("web.dashboard")
            return redirect(next_page)
        else:
            logger.warning(f"Login failed for '{username}': {result.status.name}")
            flash("Invalid username or password.", "error")

    return render_template("login.html")


@bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "success")
    return redirect(url_for("web.login"))


# --- Main routes (all require login) ---

@bp.route("/")
@login_required
def dashboard():
    ctx = _sync_loop().get_template_context()
    return render_template("dashboard.html", **ctx)


@bp.route("/edit/<path:node_key>", methods=["GET", "POST"])
@login_required
def node_edit(node_key):
    """Edit config settings for a node."""
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))

    if request.method == "POST":
        _check_csrf()
        action = request.form.get("action", "save")

        if action == "save":
            config_name = request.form.get("config", "").strip()
            registry.set_config_name(node_key, config_name or node_key)
            flash(f"Settings saved for {node_key}", "success")

        elif action == "push_config":
            success = _sync_loop().push_config(node_key)
            if success:
                flash(f"Config pushed to {node_key}", "success")
            else:
                flash(f"Failed to push config to {node_key}", "error")

        elif action == "save_config":
            content = request.form.get("config_content", "")
            config_name = registry.get_config_name(node_key)
            try:
                registry.config_store.save_raw(config_name, content)
            except Exception as e:
                flash(f"Invalid config: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            flash(f"Config saved for {node_key}.", "success")

        return redirect(url_for("web.node_edit", node_key=node_key))

    config_name = registry.get_config_name(node_key)
    config_content = registry.config_store.get_raw_content(config_name) or ""
    config_filename = registry.get_config_filename(node_key)

    return render_template(
        "edit.html",
        node=node,
        node_key=node_key,
        config_name=config_name,
        config_filename=config_filename,
        config_names=registry.config_store.config_names,
        config_content=config_content,
        registry=registry,
    )


# --- htmx partial endpoints for live updates ---

@bp.route("/partials/dashboard-table")
@login_required
def partial_dashboard_table():
    ctx = _sync_loop().get_template_context()
    return render_template("_dashboard_table.html", **ctx)
