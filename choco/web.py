"""Flask routes for the choco web UI."""

import json
import logging
import secrets
import time

from flask import (
    Blueprint, render_template, request, redirect, url_for, flash,
    current_app, session, abort,
)
from flask_login import login_required, login_user, logout_user, current_user

from .auth import save_user, localhost_or_login_required
from .state import NodeStatus, find_updatable_blocks

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


def _orchestrator():
    return current_app.config["orchestrator"]


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
    registry = _registry()
    return render_template("dashboard.html", nodes=registry.nodes)


@bp.route("/edit/<path:node_key>", methods=["GET", "POST"])
@login_required
def node_edit(node_key):
    """Edit base config or updatable config for a node."""
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))

    if request.method == "POST":
        _check_csrf()
        orchestrator = _orchestrator()
        action = request.form.get("action", "push_config")

        if action == "push_config":
            orchestrator.submit_resync(node_key)
            flash(f"Config re-push queued for {node_key}", "success")

        elif action == "save_config":
            content = request.form.get("config_content", "")
            try:
                node.render(content)
            except Exception as e:
                flash(f"Invalid config: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            orchestrator.submit_base_config(node_key, content)
            flash(f"Config change queued for {node_key}.", "success")

        elif action == "update_config":  # updatable_config change
            endpoint = request.form.get("endpoint", "")
            raw_json = request.form.get("updatable_content", "")
            try:
                values = json.loads(raw_json)
            except json.JSONDecodeError as e:
                flash(f"Invalid JSON: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            orchestrator.submit_updatable_config(node_key, endpoint, values)
            flash(f"Update queued for /{endpoint}", "success")

        return redirect(url_for("web.node_edit", node_key=node_key))

    config_content = node.base_content or ""

    # Extract updatable config blocks from the live config.
    # Pre-serialize to compact JSON strings so Jinja2 auto-escaping
    # safely handles quotes inside HTML attributes.
    live_config = node.get_config()
    updatable_blocks = find_updatable_blocks(live_config) if live_config else {}
    updatable_json = {
        endpoint: json.dumps(values, separators=(",", ": "))
        for endpoint, values in updatable_blocks.items()
    }

    return render_template(
        "edit.html",
        node=node,
        node_key=node_key,
        config_content=config_content,
        updatable_json=updatable_json,
    )


# --- htmx partial endpoints for live updates ---

@bp.route("/partials/node-status/<path:node_key>")
@login_required
def partial_node_status(node_key):
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
    # Light probe so the edit page gets fresh data between sync loop polls.
    probe = node.get_status()
    if probe != node.status:
        node.status = probe
    if probe not in (NodeStatus.DOWN, NodeStatus.UNKNOWN):
        node.last_seen = time.time()
    return render_template("_node_status.html", node=node)


@bp.route("/partials/dashboard-table")
@login_required
def partial_dashboard_table():
    registry = _registry()
    return render_template("_dashboard_table.html", nodes=registry.nodes)


# --- JSON API endpoints for queue-based updates ---

@bp.route("/update/<group>", methods=["POST"])
@localhost_or_login_required
def update_group(group):
    """Queue a config change for all nodes in a group."""
    registry = _registry()
    orchestrator = _orchestrator()

    # Find a node in the group (for validation and to confirm group exists).
    sample_node = next(
        (n for n in registry.nodes.values() if n.group == group), None
    )
    if sample_node is None:
        return {"error": f"Group '{group}' not found"}, 404

    data = request.get_json(silent=True) or {}
    action = data.get("action", "")

    if action == "base_config":
        content = data.get("config_content", "")
        try:
            sample_node.render(content)
        except Exception as e:
            return {"error": f"Invalid config: {e}"}, 400
        orchestrator.submit_group_base_config(group, content)
        return {"status": "queued", "group": group, "action": action}

    if action == "updatable_config":
        endpoint = data.get("endpoint", "")
        values = data.get("values")
        if not endpoint or values is None:
            return {"error": "endpoint and values are required"}, 400
        orchestrator.submit_group_updatable_config(group, endpoint, values)
        return {"status": "queued", "group": group, "action": action}

    return {"error": f"Unknown action '{action}'"}, 400


@bp.route("/update/<group>/<node>", methods=["POST"])
@localhost_or_login_required
def update_node(group, node):
    """Queue a config change for a single node."""
    registry = _registry()
    orchestrator = _orchestrator()
    node_key = f"{group}/{node}"

    node_obj = registry.get_node(node_key)
    if node_obj is None:
        return {"error": f"Node '{node_key}' not found"}, 404

    data = request.get_json(silent=True) or {}
    action = data.get("action", "")

    if action == "base_config":
        content = data.get("config_content", "")
        try:
            node_obj.render(content)
        except Exception as e:
            return {"error": f"Invalid config: {e}"}, 400
        orchestrator.submit_base_config(node_key, content)
        return {"status": "queued", "node": node_key, "action": action}

    if action == "updatable_config":
        endpoint = data.get("endpoint", "")
        values = data.get("values")
        if not endpoint or values is None:
            return {"error": "endpoint and values are required"}, 400
        orchestrator.submit_updatable_config(node_key, endpoint, values)
        return {"status": "queued", "node": node_key, "action": action}

    return {"error": f"Unknown action '{action}'"}, 400
