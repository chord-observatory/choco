"""Flask routes for the choco web UI."""

import logging
import secrets

import yaml

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


@bp.route("/node/<path:node_key>/edit", methods=["GET", "POST"])
@login_required
def node_edit(node_key):
    """Edit deploy settings for a node: branch, config, reinstall."""
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))

    deploy = registry.deploy_store

    if request.method == "POST":
        _check_csrf()
        action = request.form.get("action", "save")

        if action == "save":
            branch = request.form.get("branch", "").strip()
            config_name = request.form.get("config", "").strip()
            old_branch = deploy.get_branch(node_key)
            # Empty branch means "use default"
            new_branch = branch or deploy.default_branch
            deploy.set_node(
                node_key,
                branch=new_branch,
                config=config_name or node_key,
            )
            flash(f"Settings saved for {node_key}", "success")

            # Auto-reinstall if branch changed
            if new_branch != old_branch:
                return redirect(url_for("web.node_reinstall", node_key=node_key))

        elif action == "push_config":
            success = _sync_loop().push_config(node_key)
            if success:
                flash(f"Config pushed to {node_key}", "success")
            else:
                flash(f"Failed to push config to {node_key}", "error")

        elif action == "save_push":
            content = request.form.get("config_content", "")
            try:
                config_dict = yaml.safe_load(content)
                if not isinstance(config_dict, dict):
                    raise ValueError("Config must be a YAML mapping.")
            except Exception as e:
                flash(f"Invalid YAML: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            config_name = deploy.get_config_name(node_key)
            registry.config_store.save_config(config_name, config_dict)
            success = _sync_loop().push_config(node_key)
            if success:
                flash(f"Config saved and pushed to {node_key}.", "success")
            else:
                flash(f"Config saved but push failed for {node_key}.", "error")

        return redirect(url_for("web.node_edit", node_key=node_key))

    config_name = deploy.get_config_name(node_key)
    config_dict = registry.config_store.get_desired_config(config_name)
    config_content = yaml.dump(config_dict, default_flow_style=False) if config_dict else ""

    return render_template(
        "edit.html",
        node=node,
        node_key=node_key,
        branch=deploy.get_branch(node_key),
        config_name=config_name,
        config_names=registry.config_store.config_names,
        config_content=config_content,
        default_branch=deploy.default_branch,
    )


@bp.route("/node/<path:node_key>/reinstall", methods=["GET", "POST"])
@login_required
def node_reinstall(node_key):
    """Reinstall kotekan on a node (clone + build + install + restart)."""
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))

    installing = current_app.config["_installing"]

    if request.method == "POST":
        _check_csrf()
        if node_key in installing:
            flash(f"Installation already in progress on {node_key}.", "error")
            return redirect(url_for("web.node_reinstall", node_key=node_key))

        from .ssh import SSHConfig, install_kotekan
        from .app import socketio

        ssh_cfg = SSHConfig.from_config(current_app.config.get("_raw_config", {}))
        branch = registry.deploy_store.get_branch(node_key)
        installing.add(node_key)

        def _do_install(app, nk, host, br, cfg):
            with app.app_context():
                success = install_kotekan(host, br, cfg)
                app.config["_installing"].discard(nk)
                socketio.emit("install_complete", {
                    "node": nk, "success": success,
                }, namespace="/")

        socketio.start_background_task(
            _do_install, current_app._get_current_object(),
            node_key, node.host, branch, ssh_cfg,
        )
        return redirect(url_for("web.node_reinstall", node_key=node_key))

    branch = registry.deploy_store.get_branch(node_key)
    return render_template(
        "reinstall.html",
        node=node,
        node_key=node_key,
        branch=branch,
        installing=node_key in installing,
    )


# --- htmx partial endpoints for live updates ---

@bp.route("/partials/dashboard-table")
@login_required
def partial_dashboard_table():
    ctx = _sync_loop().get_template_context()
    return render_template("_dashboard_table.html", **ctx)
