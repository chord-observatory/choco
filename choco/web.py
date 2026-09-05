"""Flask routes for the choco web UI."""

import base64
import hashlib
import json
import logging
import re
import secrets
import time
from pathlib import Path
from urllib.parse import quote

import gevent

from flask import (
    Blueprint, Response, render_template, request, redirect, url_for, flash,
    current_app, session, abort, send_file,
)
from flask_login import login_required, login_user, logout_user, current_user

from .auth import save_user, localhost_or_login_required
from .datafiles import human_bytes
from .pdbmap import PdbMap, cross_check, kotekan_dish_labels
from .waterfalls import (
    IMAGE_RE as WF_IMAGE_RE, freq_ticks as wf_freq_ticks, open_stream,
    palette_gradient as wf_palette_gradient, parse_elements,
    time_ticks as wf_time_ticks, triangle as wf_triangle,
    value_ticks as wf_value_ticks,
)
from .services import (
    job_status, job_logs, timer_status, read_state_json, render_dot_svg,
    sanitize_pipeline_svg, EOP_STALE_AFTER_S, PIPELINE_LAYOUTS,
)
from .state import NodeStatus, find_updatable_blocks
from .sync import ChangeItem, ChangeType

logger = logging.getLogger(__name__)

bp = Blueprint("web", __name__)

# Module import happens once at startup, so this doubles as the process
# start time (surfaced via /api/status and /metrics — a restart implies
# maintenance mode was re-engaged cluster-wide).
_STARTED_AT = time.time()


def _csrf_token() -> str:
    if "_csrf_token" not in session:
        session["_csrf_token"] = secrets.token_hex(32)
    return session["_csrf_token"]


def _dev_auth() -> bool:
    """True when running with ``server.dev_auth`` (loopback-only, no auth).

    CSRF has to come off with login rather than stay on: the token lives
    in the session cookie, so with auto-login re-established per request
    a stale cookie would 403 every POST — toggles, edits, PDB writes —
    with no login flow left to reset it.
    """
    return bool(current_app.config.get("DEV_AUTH"))


def _check_csrf():
    if _dev_auth():
        return
    token = request.form.get("_csrf_token", "")
    if not token or token != session.get("_csrf_token"):
        abort(403)


def _check_csrf_header():
    """CSRF check for JSON POSTs: token comes in via an ``X-CSRF-Token`` header."""
    if _dev_auth():
        return
    token = request.headers.get("X-CSRF-Token", "")
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

def _next_target() -> str:
    """The post-login destination: ?next= when it is a same-site path.

    Anything else — empty, scheme-relative (//host), an absolute URL, or
    a path carrying a backslash or control character — falls back to the
    landing page, so a crafted login link cannot bounce a fresh session
    to another site.  The backslash rule is what makes the ``//`` rule
    hold: browsers resolve ``/\\host`` in a Location header exactly as
    ``//host``.  The login form posts to its own URL (no action
    attribute), so ?next= survives failed attempts too.
    """
    next_page = request.args.get("next", "")
    if (not next_page.startswith("/") or next_page.startswith("//")
            or "\\" in next_page
            or any(ord(c) < 0x20 or c == "\x7f" for c in next_page)):
        return url_for("web.landing")
    return next_page


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(_next_target())

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

        authenticator = current_app.config["ldap_authenticator"]
        user_dn = authenticator.authenticate(username, password)

        if user_dn:
            user = save_user(user_dn, username)
            login_user(user)
            return redirect(_next_target())
        else:
            logger.warning(f"Login failed for '{username}'")
            flash("Invalid username or password.", "error")

    return render_template("login.html")


@bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.", "success")
    return redirect(url_for("web.login"))


# --- Main routes (all require login) ---

# Job services shown on the landing page (and their table order there).
_LANDING_JOBS = ("eop", "bffs", "eigencal", "waterfall")


def _landing_context() -> dict:
    """Everything the landing page's service table shows.

    The same health snapshots as the header strip, plus the per-service
    facts the strip only carries in a tooltip: timer schedules and the
    state-file summaries (both already tolerant of missing systemd or
    unusable state files).
    """
    services = _services_health()
    svc_registry = _service_registry()
    timers = {}
    details = {}
    for name in _LANDING_JOBS:
        svc = svc_registry[name]
        timers[name] = timer_status(svc["timer"]) if svc["timer"] else None
        details[name] = _service_detail(name, svc)
    return {
        "services": services,
        "svc": svc_registry,
        "timers": timers,
        "details": details,
        "choco": _service_detail("choco", svc_registry["choco"]),
        "nodes": _nodes_health(),
        "skymap_configured": _skymap_file() is not None,
        "now_ts": time.time(),
    }


@bp.route("/")
@login_required
def landing():
    """CHOCO landing page: the services overview table.

    Node management lives under /nodes/*; this page is the front door.
    """
    return render_template("landing.html", **_landing_context())


@bp.route("/partials/landing-services")
@login_required
def partial_landing_services():
    """The landing page's service table, htmx-refreshed in place."""
    return render_template("_landing_services.html", **_landing_context())


def _skymap_file() -> Path | None:
    """The rendered sky-map PNG, if the skymap config block names one."""
    cfg = current_app.config.get("skymap_cfg") or {}
    path = cfg.get("image_file")
    return Path(path) if path else None


@bp.route("/skymap.png")
def skymap_png():
    """The current-sky strip image, rendered by jobs/skymap.

    Deliberately unauthenticated (like /metrics, and for the same
    cross-host reason: wall displays and external dashboards can't do
    LDAP sessions).  It is an aggregate sky rendering whose only
    cluster fact is the pointing declination.  ``conditional`` +
    ``max_age=0`` make the browser revalidate each time and get a 304
    until the 5-minute job rewrites the file.
    """
    path = _skymap_file()
    if path is None or not path.is_file():
        abort(404)
    return send_file(path, mimetype="image/png", conditional=True, max_age=0)


@bp.route("/partials/skymap")
@login_required
def partial_skymap():
    """The landing page's sky-map card, htmx-refreshed every 5 min.

    The image URL carries the file's mtime, so a refresh swaps in a new
    render exactly when one has landed and is a no-op otherwise.
    """
    path = _skymap_file()
    mtime = None
    if path is not None:
        try:
            mtime = int(path.stat().st_mtime)
        except OSError:
            mtime = None
    return render_template("_skymap.html", mtime=mtime, now_ts=time.time())


@bp.route("/nodes")
@login_required
def dashboard():
    registry = _registry()
    return render_template("dashboard.html", nodes=registry.nodes)


@bp.route("/nodes/edit/<path:node_key>", methods=["GET", "POST"])
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
            orchestrator.submit_node(ChangeItem(
                type=ChangeType.RESYNC, node_key=node_key,
            ))
            flash(f"Config re-push queued for {node_key}", "success")

        elif action == "save_config":
            content = request.form.get("config_content", "")
            try:
                node.render(content)
            except Exception as e:
                flash(f"Invalid config: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            orchestrator.submit_node(ChangeItem(
                type=ChangeType.BASE_CONFIG, node_key=node_key,
                config_content=content,
            ))
            flash(f"Config change queued for {node_key}.", "success")

        elif action == "oneshot":
            # The textarea's text, started but never saved (see
            # _run_oneshot).  The helper's JSON body is folded into a
            # flash message since this caller is a form post.
            body, status = _run_oneshot(
                [node], request.form.get("config_content", ""),
                _audit_user())
            if status == 400:
                flash(body["error"], "error")
            elif node_key in body["started"]:
                flash(f"One-off config started on {node_key} "
                      f"(sha256 {body['sha256']}); nothing saved.",
                      "success")
            else:
                flash(f"One-off not started on {node_key}: "
                      f"{body['skipped'][node_key]}", "error")

        elif action == "update_config":  # updatable_config change
            endpoint = request.form.get("endpoint", "")
            raw_json = request.form.get("updatable_content", "")
            try:
                values = json.loads(raw_json)
            except json.JSONDecodeError as e:
                flash(f"Invalid JSON: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            orchestrator.submit_node(ChangeItem(
                type=ChangeType.UPDATABLE_CONFIG, node_key=node_key,
                endpoint=endpoint, values=values,
            ))
            flash(f"Update queued for /{endpoint}", "success")

        return redirect(url_for("web.node_edit", node_key=node_key))

    config_content = node.base_content or ""

    # Extract updatable config blocks from the desired config (rendered
    # base + stored overrides). Using desired_config instead of the live
    # kotekan config means the UI still shows fields when a node is down
    # or when a newly added updatable block hasn't been pushed yet.
    # Pre-serialize to compact JSON strings so Jinja2 auto-escaping
    # safely handles quotes inside HTML attributes.
    desired = node.desired_config
    updatable_blocks = find_updatable_blocks(desired) if desired else {}
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


@bp.route("/pipeline/<path:node_key>")
@login_required
def node_pipeline_page(node_key):
    """Full-page interactive pipeline view.

    The graph pane fills the viewport (drag to pan, scroll to zoom,
    Fit/1:1 buttons) and buffer nodes with ``peek_hold`` are clickable —
    a click opens the live plot in a popup overlay pinned to the
    bottom-right corner, so the graph keeps the whole viewport.  The SVG
    is inlined after ``sanitize_pipeline_svg`` whitelist reconstruction,
    unlike the status page's inert base64 ``<img>``.

    ``?layout=`` preselects an edge-routing preset (allowlisted against
    PIPELINE_LAYOUTS) so a layout choice survives a refresh and can be
    bookmarked; pipeline.js keeps the URL in step with the selector.
    """
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))
    layout = request.args.get("layout", "")
    if layout not in PIPELINE_LAYOUTS:
        layout = "curves"
    return render_template(
        "pipeline.html", node=node, node_key=node_key,
        layout=layout, layouts=list(PIPELINE_LAYOUTS),
    )


@bp.route("/plot/<path:node_key>")
@login_required
def node_plot_page(node_key):
    """Full-viewport live plot of one buffer.

    The same panel the pipeline page opens in its corner overlay, given
    the whole window: ``bufferplot.js`` renders into ``#buffer-plot``
    either way, and the ``data-buffer`` attribute here is what tells it
    to open full screen instead of waiting for a click.

    The view — dimension dispositions, series selection, mode, zoom —
    rides in the URL *fragment*, which is why nothing here parses it:
    it never reaches the server, and the page is bookmarkable and
    shareable without a round trip.  The buffer name is validated
    against the same allowlist the data API uses before it is handed to
    a template that puts it in an attribute.
    """
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        flash(f"Node {node_key} not found", "error")
        return redirect(url_for("web.dashboard"))
    buffer_name = request.args.get("buffer", "")
    if not _BUFFER_NAME_RE.fullmatch(buffer_name):
        flash("Invalid buffer name", "error")
        return redirect(url_for("web.node_pipeline_page", node_key=node_key))
    return render_template(
        "plot.html", node=node, node_key=node_key, title=buffer_name,
        subtitle=node.key,
        source_url="/api/node-buffer-data/" + node_key
                   + "?buffer=" + quote(buffer_name, safe=""),
        source_id=node_key + "|" + buffer_name,
        back_url=url_for("web.node_pipeline_page", node_key=node_key),
        back_label="pipeline",
    )


@bp.route("/partials/node-pipeline-svg/<path:node_key>")
@login_required
def partial_node_pipeline_svg(node_key):
    """Sanitized inline SVG for the full-page pipeline view.

    ``?layout=`` selects an edge-routing preset; the value is an
    allowlist key into PIPELINE_LAYOUTS, never a raw dot argument.
    """
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
    layout = request.args.get("layout", "curves")
    layout_args = PIPELINE_LAYOUTS.get(layout, PIPELINE_LAYOUTS["curves"])
    dot = node.get_pipeline_dot()
    svg_markup = None
    # Which buffers are clickable needs a second kotekan read, and it can
    # fail on its own: distinguish "asked, got nothing" (buffers_ok, an
    # idle kotekan or a pipeline with no peek_hold) from "couldn't ask"
    # so the template doesn't leave the page promising amber buffers
    # that were never marked.
    buffers_ok = True
    clickable = set()
    if dot is not None:
        svg = render_dot_svg(dot, layout_args=layout_args)
        if svg is not None:
            buffers = node.get_buffers()
            buffers_ok = buffers is not None
            clickable = {name for name, info in (buffers or {}).items()
                         if isinstance(info, dict) and info.get("peek_hold")}
            svg_markup = sanitize_pipeline_svg(svg, clickable, node_key)
    return render_template(
        "_node_pipeline_svg.html",
        node_key=node_key, dot=dot, svg_markup=svg_markup,
        buffers_ok=buffers_ok, clickable_count=len(clickable),
    )


# --- Nodes-registry editor (nodes.yaml) ---

@bp.route("/nodes/edit", methods=["GET"])
@login_required
def nodes_edit():
    """Render the nodes.yaml editor with drag-and-drop groups."""
    registry = _registry()
    groups: dict[str, list] = {}
    for node in registry.nodes.values():
        groups.setdefault(node.group, []).append(node)
    return render_template("nodes.html", groups=groups)


@bp.route("/nodes/edit", methods=["POST"])
@login_required
def nodes_save():
    """Validate the posted JSON structure, save nodes.yaml, and reload."""
    _check_csrf_header()

    payload = request.get_json(silent=True) or {}
    groups_in = payload.get("groups")
    if not isinstance(groups_in, dict):
        return {"error": "'groups' must be an object"}, 400

    new_data: dict = {"groups": {}}
    seen_keys: set[str] = set()

    for group_name, items in groups_in.items():
        if not isinstance(group_name, str):
            return {"error": "Group name must be a string"}, 400
        group_name = group_name.strip()
        if not group_name or "/" in group_name or group_name.startswith("."):
            return {"error": f"Invalid group name {group_name!r}"}, 400
        if not isinstance(items, list):
            return {"error": f"Group {group_name!r} must be a list"}, 400
        if group_name in new_data["groups"]:
            return {"error": f"Duplicate group {group_name!r}"}, 400

        members: dict = {}
        for item in items:
            if not isinstance(item, dict):
                return {"error": f"Entry in {group_name!r} must be an object"}, 400
            name = str(item.get("name", "")).strip()
            host = str(item.get("host", "")).strip()
            if not name or "/" in name or name.startswith("."):
                return {"error": f"Invalid node name {name!r} in {group_name!r}"}, 400
            if not host:
                return {"error": f"Node {group_name}/{name} missing host"}, 400
            try:
                port = int(item.get("port", 12048))
            except (TypeError, ValueError):
                return {"error": f"Node {group_name}/{name} invalid port"}, 400
            key = f"{group_name}/{name}"
            if key in seen_keys:
                return {"error": f"Duplicate node {key!r}"}, 400
            seen_keys.add(key)
            members[name] = {"host": host, "port": port}

        new_data["groups"][group_name] = members

    try:
        _orchestrator().apply_nodes_update(new_data)
    except Exception as e:
        logger.exception("Failed to apply nodes update")
        return {"error": str(e)}, 500

    flash("Node registry saved; all nodes placed in maintenance mode.",
          "success")
    return {"status": "ok"}


# --- Group config editor (push one config to every node in a group) ---

@bp.route("/nodes/edit-group/<group>", methods=["GET", "POST"])
@login_required
def group_edit(group):
    """Edit a single config to broadcast to every node in *group*."""
    registry = _registry()
    sample_node = next(
        (n for n in registry.nodes.values() if n.group == group), None
    )
    if sample_node is None:
        flash(f"Group {group!r} not found", "error")
        return redirect(url_for("web.dashboard"))

    if request.method == "POST":
        _check_csrf()
        content = request.form.get("config_content", "")
        try:
            sample_node.render(content)
        except Exception as e:
            flash(f"Invalid config: {e}", "error")
            return render_template(
                "edit_group.html", group=group, config_content=content,
            )
        _orchestrator().submit_group(group, lambda key: ChangeItem(
            type=ChangeType.BASE_CONFIG, node_key=key,
            config_content=content,
        ))
        flash(f"Config change queued for group {group!r}.", "success")
        return redirect(url_for("web.dashboard"))

    return render_template("edit_group.html", group=group, config_content="")


# --- htmx partial endpoints for live updates ---

@bp.route("/nodes/toggle-started/<path:node_key>", methods=["POST"])
@login_required
def toggle_started(node_key):
    """Toggle the started/idle desired state for a node."""
    _check_csrf()
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
    node.started = not node.started
    _orchestrator().submit_node(
        ChangeItem(type=ChangeType.POLL, node_key=node_key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_toggle_started.html", node=node, key=node_key)
    flash(f"{node_key} {'started' if node.started else 'stopped'}", "success")
    return redirect(request.referrer or url_for("web.dashboard"))


@bp.route("/nodes/set-started-all/<action>", methods=["POST"])
@login_required
def set_started_all(action):
    """Set all nodes to started or stopped."""
    _check_csrf()
    if action not in ("start", "stop"):
        abort(400)
    registry = _registry()
    started = action == "start"
    for node in registry.nodes.values():
        node.started = started
    _orchestrator().submit_all(
        lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_dashboard_table.html", nodes=registry.nodes)
    return redirect(url_for("web.dashboard"))


@bp.route("/nodes/set-started-group/<group>/<action>", methods=["POST"])
@login_required
def set_started_group(group, action):
    """Set every node in *group* to started or stopped."""
    _check_csrf()
    if action not in ("start", "stop"):
        abort(400)
    registry = _registry()
    group_nodes = [n for n in registry.nodes.values() if n.group == group]
    if not group_nodes:
        abort(404)
    started = action == "start"
    for node in group_nodes:
        node.started = started
    _orchestrator().submit_group(
        group, lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_dashboard_table.html", nodes=registry.nodes)
    return redirect(url_for("web.dashboard"))


@bp.route("/nodes/toggle-maintenance/<path:node_key>", methods=["POST"])
@login_required
def toggle_maintenance(node_key):
    """Toggle maintenance mode for a single node."""
    _check_csrf()
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
    node.maintenance = not node.maintenance
    _orchestrator().submit_node(
        ChangeItem(type=ChangeType.POLL, node_key=node_key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_toggle_maintenance.html",
                               node=node, key=node_key)
    flash(f"{node_key} maintenance "
          f"{'on' if node.maintenance else 'off'}", "success")
    return redirect(request.referrer or url_for("web.dashboard"))


@bp.route("/nodes/set-maintenance-all/<action>", methods=["POST"])
@login_required
def set_maintenance_all(action):
    """Put every node into or out of maintenance mode."""
    _check_csrf()
    if action not in ("on", "off"):
        abort(400)
    registry = _registry()
    maintenance = action == "on"
    for node in registry.nodes.values():
        node.maintenance = maintenance
    _orchestrator().submit_all(
        lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_dashboard_table.html", nodes=registry.nodes)
    return redirect(url_for("web.dashboard"))


@bp.route("/nodes/set-maintenance-group/<group>/<action>", methods=["POST"])
@login_required
def set_maintenance_group(group, action):
    """Put every node in *group* into or out of maintenance mode."""
    _check_csrf()
    if action not in ("on", "off"):
        abort(400)
    registry = _registry()
    group_nodes = [n for n in registry.nodes.values() if n.group == group]
    if not group_nodes:
        abort(404)
    maintenance = action == "on"
    for node in group_nodes:
        node.maintenance = maintenance
    _orchestrator().submit_group(
        group, lambda key: ChangeItem(type=ChangeType.POLL, node_key=key)
    )
    if request.headers.get("HX-Request"):
        return render_template("_dashboard_table.html", nodes=registry.nodes)
    return redirect(url_for("web.dashboard"))


@bp.route("/nodes/partials/node-status/<path:node_key>")
@login_required
def partial_node_status(node_key):
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
    # Light probe so the status/edit pages get fresh data between sync
    # loop polls.
    probe = node.get_status()
    if probe != node.status:
        node.status = probe
    if probe not in (NodeStatus.DOWN, NodeStatus.UNKNOWN):
        node.last_seen = time.time()
    return render_template("_node_status.html", node=node)


# Kotekan buffer names are config keys; anything else is rejected before
# the name is placed in a kotekan URL path.
_BUFFER_NAME_RE = re.compile(r"[A-Za-z0-9_.\-]+")


# Bounds for the buffer-data proxy: the default keeps a 5 s poll cheap;
# the cap keeps one request from streaming a whole 400 MB voltage frame
# through choco.  These are byte counts of frame *prefix* — every pathfinder
# buffer is C-order with time leading, so a prefix is complete early
# timesamples with all inner structure intact.
_BUFFER_DATA_DEFAULT_LEN = 4 * 1024 * 1024
_BUFFER_DATA_MAX_LEN = 32 * 1024 * 1024


@bp.route("/api/node-buffer-data/<path:node_key>")
@localhost_or_login_required
def api_node_buffer_data(node_key):
    """Frame-data proxy for the live buffer plots (``bufferplot.js``).

    ``?len=0`` returns kotekan's JSON reply as-is (frame descriptor,
    metadata, frame id) — the plotter fetches it once to learn the
    value type and extents.  ``?len>0`` (clamped, default 4 MiB)
    returns the newest frame's leading bytes as raw
    ``application/octet-stream`` — kotekan base64-encodes frame data
    inside its JSON reply, so choco decodes here rather than making
    the browser do it — with the frame id in ``X-Frame-Id`` so the
    poller can spot a stalled pipeline serving the same held frame.

    Errors are JSON: 400 bad buffer name / ``len``, 404 unknown node
    or no full frame in the buffer, 502 kotekan unreachable or reply
    malformed.
    """
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        return {"error": f"Node '{node_key}' not found"}, 404
    buffer_name = request.args.get("buffer", "")
    if not _BUFFER_NAME_RE.fullmatch(buffer_name):
        return {"error": "bad buffer name"}, 400
    try:
        length = int(request.args.get("len", _BUFFER_DATA_DEFAULT_LEN))
    except ValueError:
        return {"error": "'len' must be a non-negative integer"}, 400
    if length < 0:
        return {"error": "'len' must be a non-negative integer"}, 400
    length = min(length, _BUFFER_DATA_MAX_LEN)

    frame = node.get_buffer_frame(buffer_name, length=length)
    if frame is None:
        return {"error": "kotekan unreachable"}, 502
    if frame.get("error"):
        return {"error": frame["error"]}, 404
    if length == 0:
        return frame

    encoded = frame.get("data")
    if not isinstance(encoded, str) or frame.get("encoding") != "base64":
        return {"error": "unexpected kotekan reply: no base64 frame data"}, 502
    try:
        raw = base64.b64decode(encoded)
    except Exception:
        return {"error": "unexpected kotekan reply: bad base64 frame data"}, 502
    resp = Response(raw, mimetype="application/octet-stream")
    # int-check before echoing kotekan-supplied values into headers —
    # Werkzeug rejects control characters with a 500, so don't let a
    # malformed reply get that far.
    frame_id = frame.get("frame_id")
    frame_size = frame.get("frame_size")
    resp.headers["X-Frame-Id"] = str(frame_id) if isinstance(frame_id, int) else ""
    resp.headers["X-Frame-Size"] = str(frame_size) if isinstance(frame_size, int) else ""
    resp.headers["Cache-Control"] = "no-store"
    return resp


_GAIN_DATASET_RE = re.compile(r"[A-Za-z0-9_./\-]+")
# The archive changes when someone recalibrates, not every few seconds,
# and gain_coeff is 8.4 MB — so the panel polls slowly and asks for the
# whole dataset (a byte prefix would cut the frequency axis in half).
GAIN_POLL_MS = 30000
GAIN_FETCH_BYTES = 16 * 1024 * 1024


@bp.route("/api/fpga/gain-data")
@login_required
def api_fpga_gain_data():
    """The F-engine's digital gains, in the buffer-plot protocol.

    Deliberately the *same* wire format as ``/api/node-buffer-data``:
    ``?len=0`` returns a descriptor (``value_type`` / ``extents`` /
    ``dimnames``) plus metadata, ``?len>0`` returns leading bytes as
    ``application/octet-stream``.  An HDF5 dataset is already a C-order
    array with named axes, so speaking this protocol means the whole
    plotting stack — dimension table, folds, series, zoom, full-screen
    page — works on gains with no client-side special case.

    ``X-Frame-Id`` carries the archive's ``update_id``, so the plotter's
    staleness note reads as "the gains have not changed" rather than
    pretending to a liveness the file does not have.
    """
    archive = current_app.config.get("gain_archive")
    if archive is None or not archive.configured:
        return {"error": "fpga_master is not configured"}, 404
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        return {"error": "bad dataset name"}, 400
    try:
        length = int(request.args.get("len", _BUFFER_DATA_DEFAULT_LEN))
    except ValueError:
        return {"error": "'len' must be a non-negative integer"}, 400
    if length < 0:
        return {"error": "'len' must be a non-negative integer"}, 400
    length = min(length, _BUFFER_DATA_MAX_LEN)

    desc = archive.describe(dataset)
    if desc is None:
        return {"error": archive.error or f"no dataset '{dataset}'"}, 404
    info = archive.to_dict()
    update_id = _first_scalar(info["scalars"].get("update_id"))

    if length == 0:
        return {
            "frame_desc": {
                "value_type": desc["value_type"],
                "extents": desc["extents"],
                "dimnames": desc["dimnames"],
            },
            "frame_id": update_id,
            "frame_size": desc["bytes"],
            "metadata": {
                "dataset": dataset,
                "attrs": info["attrs"],
                "scalars": info["scalars"],
                "index_map": info["index_map"],
                "fetched_at": info["fetched_at"],
            },
        }

    raw = archive.dataset(dataset)
    if raw is None:
        return {"error": archive.error or "gain archive unreadable"}, 502
    resp = Response(raw[:length], mimetype="application/octet-stream")
    resp.headers["X-Frame-Id"] = str(update_id or "")
    resp.headers["X-Frame-Size"] = str(len(raw))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.route("/partials/fpga-gains")
@login_required
def partial_fpga_gains():
    """The gain card, loaded after the page paints.

    Rendering it pulls the archive from fpga_master — 8.4 MB and a
    subprocess on a cold cache.  That must not be in the way of a page
    whose first job is showing whether the F-engine is up, so the card
    arrives on its own (``hx-trigger="load"``) and a failure degrades to
    a note inside it.
    """
    return render_template("_fpga_gains.html", gains=_gain_context(),
                           poll_ms=GAIN_POLL_MS, fetch_bytes=GAIN_FETCH_BYTES)


@bp.route("/service/fpga/gain.h5")
@login_required
def fpga_gain_file():
    """The gain archive itself, proxied so the browser needs no chive route."""
    archive = current_app.config.get("gain_archive")
    if archive is None or not archive.configured:
        abort(404)
    raw = archive.file_bytes()
    if raw is None:
        flash(f"Gain file unavailable: {archive.error or 'unknown error'}",
              "error")
        return redirect(url_for("web.service_page", name="fpga"))
    resp = Response(raw, mimetype="application/x-hdf5")
    resp.headers["Content-Disposition"] = \
        'attachment; filename="current_gain.h5"'
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.route("/service/fpga/plot")
@login_required
def fpga_gain_plot_page():
    """Full-viewport plot of one gain dataset.

    The same ``plot.html`` the node buffers use — only the source
    attributes on the container differ, which is the whole point of the
    plot panel taking a source rather than a node and a buffer.
    """
    archive = current_app.config.get("gain_archive")
    if archive is None or not archive.configured:
        abort(404)
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        flash("Invalid dataset name", "error")
        return redirect(url_for("web.service_page", name="fpga"))
    return render_template("plot.html", title=dataset, subtitle="F-engine gains",
                           source_url="/api/fpga/gain-data?dataset="
                                      + quote(dataset, safe=""),
                           source_id="fpga-gain|" + dataset,
                           back_url=url_for("web.service_page", name="fpga"),
                           back_label="fpga",
                           poll_ms=GAIN_POLL_MS, fetch_bytes=GAIN_FETCH_BYTES)


def _gain_context() -> dict:
    """What the FPGA page shows about the gain archive.

    Fetching is on demand and cached, so rendering the page is what
    pulls the file the first time.  A failure degrades to a note: the
    gains are an extra on this page, never the reason it loads.
    """
    archive = current_app.config.get("gain_archive")
    if archive is None or not archive.configured:
        return {"configured": False, "datasets": [], "attrs": {},
                "index_map": {}, "error": None}
    archive.refresh()          # to_dict reports the cache; this fills it
    info = archive.to_dict()
    update_time = _first_scalar(info["scalars"].get("index_map/update_time"))
    info["update_id"] = _first_scalar(info["scalars"].get("update_id"))
    info["update_fmt"] = (_fmt_utc(update_time)
                          if isinstance(update_time, (int, float)) else None)
    return info


def _first_scalar(value):
    """h5py hands back one-element lists for scalar datasets."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


@bp.route("/partials/dashboard-table")
@login_required
def partial_dashboard_table():
    registry = _registry()
    return render_template("_dashboard_table.html", nodes=registry.nodes)


@bp.app_template_filter("filesize")
def _filesize(n) -> str:
    return human_bytes(n)


def _datafile_scan():
    return current_app.config.get("datafile_scan")


@bp.route("/files")
@login_required
def files_page():
    """What kotekan has written to the data roots.

    The scan itself is a *lazily loaded partial*, for the same reason
    the FPGA gain card is: walking an NFS mount takes a few hundred ms,
    and the page has nothing to wait for it for.
    """
    scan = _datafile_scan()
    return render_template(
        "files.html",
        configured=bool(scan is not None and scan.configured),
        roots=[str(r) for r in (scan.roots if scan is not None else [])],
    )


def _waterfall_store():
    return current_app.config.get("waterfall_store")


@bp.route("/partials/files")
@login_required
def partial_files():
    """The scan table.  ``?refresh=1`` bypasses the cache."""
    scan = _datafile_scan()
    if scan is None or not scan.configured:
        result = {"configured": False, "roots": []}
    else:
        result = scan.get(force=request.args.get("refresh") == "1")

    # What has been rendered, keyed the way the template can look it up:
    # the scan knows a root by its path, the image tree by its last
    # component, which is the name jobs/waterfall writes under.
    # Keyed by the acquisition's own source path where the job recorded
    # one, so two roots sharing a basename cannot collide; the name-based
    # key stays as a fallback for trees rendered before that was written.
    store = _waterfall_store()
    rendered = {}
    if store is not None and store.configured:
        for (root_name, acq), summary in store.summaries(
                force=request.args.get("refresh") == "1").items():
            summary = {**summary, "root": root_name, "acq": acq}
            rendered.setdefault(f"{root_name}/{acq}", summary)
            if summary.get("source_path"):
                rendered[f"{summary['source_path'].rstrip('/')}/{acq}"] = summary
    return render_template("_files_table.html", scan=result,
                           rendered=rendered, now_ts=time.time())


@bp.route("/files/<root>/<acq>/triangle")
@login_required
def waterfall_triangle(root, acq):
    """The upper-triangle contact sheet for one acquisition."""
    store = _waterfall_store()
    if store is None or not store.configured:
        abort(404)
    index = store.index(root, acq)
    if index is None:
        abort(404)
    grid = wf_triangle(index, parse_elements(request.args.get("elements")))
    return render_template(
        "waterfall_triangle.html",
        root=root, acq=acq, index=index, grid=grid,
        n_files=len(index.get("files") or []),
        elements_arg=request.args.get("elements") or "",
    )


@bp.route("/waterfall/<root>/<acq>/<shard>/<name>")
@login_required
def waterfall_image(root, acq, shard, name):
    """One rendered image.

    Every path component is matched against the pattern the writer uses
    before it is joined — the caller's string never reaches the
    filesystem unchecked.  A short max-age keeps a 500-cell contact
    sheet from revalidating every thumbnail on each reload, while
    staying far below the ~3 min cadence at which an image can grow.

    The file is opened, stat-ed and streamed off the gevent hub: a
    contact sheet is one request per cell (528 today, 5050 at 100
    elements) and a full-resolution image can be a hundred megabytes, so
    neither the NFS open nor the reads may block the sync loop, and the
    response may not be buffered.
    """
    store = _waterfall_store()
    if store is None or not store.configured:
        abort(404)
    resolved = store.image_file(root, acq, shard, name)
    if resolved is None:
        abort(404)
    directory, filename = resolved
    opened = open_stream(directory / filename)
    if opened is None:
        abort(404)
    size, mtime, chunks = opened
    resp = Response(chunks, mimetype="image/png")
    resp.headers["Content-Length"] = str(size)
    resp.last_modified = mtime
    resp.cache_control.max_age = 30
    return resp


@bp.route("/waterfall/<root>/<acq>/view/<name>")
@login_required
def waterfall_view(root, acq, name):
    """One full-resolution image with its axes drawn around it.

    The axes live here, at display time, and never in the PNG: the image
    is append-only data, and pixels spent on labels could not be revised
    when the acquisition grows.  Time comes from times.bin, frequency
    from freq.npy, and the |V| colorbar from the palette inside the PNG
    itself plus the frozen lo/hi in the index — each read hub-safe and
    each degrading to a plainer page rather than a 500 when missing.
    """
    store = _waterfall_store()
    if store is None or not store.configured:
        abort(404)
    m = WF_IMAGE_RE.match(name or "")
    if not m or not name.startswith("wf_"):
        abort(404)
    index = store.index(root, acq)
    if index is None:
        abort(404)
    a, b = int(m.group(1)), int(m.group(2))
    prod = next((p for p in index.get("products") or []
                 if isinstance(p, dict)
                 and p.get("name") == f"e{a:04d}xe{b:04d}"), None)
    if prod is None or not prod.get("rows"):
        abort(404)
    shard = f"e{a:04d}"
    n_freq = int(index.get("n_freq") or 0)

    labels = index.get("labels") or []

    def label(e: int) -> str:
        return labels[e] if e < len(labels) else str(e)

    head = store.image_head(root, acq, shard, name)
    pal = head["palette"] if head else None
    # The tick fractions must describe the pixels the browser shows: on a
    # live acquisition the served PNG can be an append ahead of the
    # index's committed row count, so its own IHDR height wins.
    rows = int(head["height"]) if head and head.get("height") else int(prod["rows"])
    # The image renders at page width, so its on-screen height scales
    # with rows/width — a squat early-acquisition image has room for a
    # handful of time labels where a night-long one fits dozens.
    time_target = max(3, min(40, round(43 * rows / max(n_freq, 1))))
    return render_template(
        "waterfall_view.html",
        root=root, acq=acq, shard=shard, name=name,
        a=a, b=b, label_a=label(a), label_b=label(b),
        rows=rows, n_freq=n_freq,
        freq_axis=wf_freq_ticks(store.freq_axis(root, acq), n_freq),
        time_axis=wf_time_ticks(
            store.times(root, acq), rows, target=time_target,
            tz=(current_app.config.get("waterfall_cfg") or {}).get("timezone")),
        value_ticks=wf_value_ticks(prod.get("lo"), prod.get("hi")),
        gradient=wf_palette_gradient(pal),
        n_files=len(index.get("files") or []),
    )


def _service_registry() -> dict[str, dict]:
    """Everything the services strip and /service/<name> pages know.

    Keyed by page slug.  ``unit`` doubles as the allowlist for the
    journal viewer: only these units can be read through the web UI.
    Jobs follow the choco-<name>.service / choco-<name>.timer naming,
    so the timer unit is derived from the service unit.
    """
    eop_cfg = current_app.config.get("eop_cfg") or {}
    bffs_cfg = current_app.config.get("bffs_cfg") or {}
    eigencal_cfg = current_app.config.get("eigencal_cfg") or {}
    waterfall_cfg = current_app.config.get("waterfall_cfg") or {}
    configs_dir = current_app.config.get("configs_dir")

    # EOP rewrites its state file on every successful (daily) run, so
    # the mtime doubles as "last successful run" and goes stale.  The
    # path is absolute (the /var/lib/choco/eop default) or, for backward
    # compatibility, relative to configs_dir (the legacy layout).
    eop_state = None
    if eop_cfg.get("state_file"):
        p = Path(str(eop_cfg["state_file"]))
        if p.is_absolute():
            eop_state = p
        elif configs_dir:
            eop_state = Path(configs_dir) / p

    def job(unit: str, state_file, stale_after_s=None,
            mtime_label="last run") -> dict:
        return {
            "unit": unit,
            "timer": unit.replace(".service", ".timer"),
            "state_file": state_file,
            "stale_after_s": stale_after_s,
            "mtime_label": mtime_label,
        }

    return {
        "choco": {"unit": "choco.service", "timer": None, "state_file": None,
                  "stale_after_s": None, "mtime_label": None},
        "eop": job(eop_cfg.get("service_unit") or "choco-eop-broadcast.service",
                   eop_state, EOP_STALE_AFTER_S, "last run"),
        # bffs rewrites its state file only when the bad-feed list
        # changes, so no staleness threshold — the mtime is "last change".
        "bffs": job(bffs_cfg.get("service_unit") or "choco-bffs-flag.service",
                    Path(str(bffs_cfg["state_file"]))
                    if bffs_cfg.get("state_file") else None,
                    None, "last change"),
        # eigencal rewrites its state file once per processed transit;
        # transits skipped for daytime are silent by design, so an old
        # mtime is informational, not a health downgrade.
        "eigencal": job(eigencal_cfg.get("service_unit")
                        or "choco-eigencal.service",
                        Path(str(eigencal_cfg["state_file"]))
                        if eigencal_cfg.get("state_file") else None,
                        None, "last calibration"),
        # waterfall rewrites its state file on every run, but a run with
        # nothing to render is the normal case between acquisitions, so
        # the mtime is "last run" and never a health downgrade.
        "waterfall": job(waterfall_cfg.get("service_unit")
                         or "choco-waterfall.service",
                         Path(str(waterfall_cfg["state_file"]))
                         if waterfall_cfg.get("state_file") else None,
                         None, "last run"),
    }


def _service_units() -> dict[str, str]:
    """Name -> systemd unit for the units whose journals may be viewed."""
    return {name: s["unit"] for name, s in _service_registry().items()}


def _services_health() -> dict:
    """Health snapshots for the hardware monitors and the oneshot jobs.

    Shared by the header strip, /api/status, and /metrics.
    """
    fpga = current_app.config.get("fpga_monitor")
    pdb = current_app.config.get("pdb_monitor")
    data = current_app.config.get("datafile_scan")
    registry = _service_registry()
    health = {
        "fpga": fpga.to_dict() if fpga is not None else None,
        "pdb": pdb.to_dict() if pdb is not None else None,
        "data": data.to_dict() if data is not None else None,
    }
    for name in ("eop", "bffs", "eigencal", "waterfall"):
        svc = registry[name]
        health[name] = job_status(svc["unit"], state_file=svc["state_file"],
                                  stale_after_s=svc["stale_after_s"])
    return health


def _nodes_health() -> dict:
    """Cluster-level roll-up of per-node status for the NODES badge.

    Green when every node is STARTED, red when every node is DOWN, grey
    when there are no nodes or nothing has been polled yet (all
    UNKNOWN); anything in between — some up, all idle, a mix — is
    yellow.  ``node.status`` is a plain attribute the sync loop keeps
    fresh, so this is an in-memory sweep, not a probe.
    """
    registry = _registry()
    counts = {s: 0 for s in NodeStatus}
    for node in registry.nodes.values():
        counts[node.status] += 1
    total = len(registry.nodes)
    started = counts[NodeStatus.STARTED]
    idle = counts[NodeStatus.IDLE]
    down = counts[NodeStatus.DOWN]
    if total == 0:
        health, label = "unknown", "no nodes"
    elif started == total:
        health, label = "ok", "all up"
    elif down == total:
        health, label = "down", "all down"
    elif counts[NodeStatus.UNKNOWN] == total:
        health, label = "unknown", "unknown"
    elif started:
        health, label = "degraded", f"{started}/{total} up"
    elif idle == total:
        health, label = "degraded", "idle"
    else:
        # No node started, not all idle: some mix of idle, down,
        # syncing and unknown.  The tooltip carries the exact counts.
        health, label = "degraded", "mixed"
    return {
        "health": health,
        "label": label,
        "total": total,
        "started": started,
        "idle": idle,
        "down": down,
        "unknown": counts[NodeStatus.UNKNOWN],
        "syncing": counts[NodeStatus.SYNCING],
        "maintenance": sum(
            1 for n in registry.nodes.values() if n.maintenance),
    }


@bp.route("/partials/services")
@login_required
def partial_services():
    """Render the NODES + monitor (FPGA, PDB, DATA) + job (EOP, bffs,
    eigencal, waterfall) strip."""
    services = _services_health()
    return render_template(
        "_services_status.html",
        nodes=_nodes_health(),
        fpga=services["fpga"],
        pdb=services["pdb"],
        data=services["data"],
        eop=services["eop"],
        bffs=services["bffs"],
        eigencal=services["eigencal"],
        waterfall=services["waterfall"],
        now_ts=time.time(),
    )


def _journal_lines_arg(default: int = 100) -> int:
    try:
        nlines = int(request.args.get("lines", default))
    except (TypeError, ValueError):
        nlines = default
    return max(10, min(nlines, 1000))


@bp.route("/partials/service-logs/<name>")
@login_required
def partial_service_logs(name):
    """Recent journal lines for one of the known service units."""
    unit = _service_units().get(name)
    if unit is None:
        abort(404)
    nlines = _journal_lines_arg()
    lines = job_logs(unit, lines=nlines)
    return render_template(
        "_service_logs.html",
        name=name, unit=unit, lines=lines, nlines=nlines, now_ts=time.time(),
    )


def _fmt_utc(ts) -> str | None:
    if not ts:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))


def _service_detail(name: str, svc: dict) -> dict | None:
    """Per-service extras for the /service/<name> page.

    Jobs are summarized from their JSON state file (missing or invalid
    state -> None, and the page shows only the common facts); choco is
    summarized from the live registry.  A state file with unexpected
    contents must degrade to "no summary", never break the page.
    """
    try:
        return _service_detail_inner(name, svc)
    except (TypeError, ValueError, KeyError, AttributeError,
            IndexError) as e:
        logger.warning(f"service detail for {name}: "
                       f"unusable state file contents: {e}")
        return None


def _service_detail_inner(name: str, svc: dict) -> dict | None:
    if name == "choco":
        registry = _registry()
        return {
            "started_at": _fmt_utc(_STARTED_AT),
            "nodes_total": len(registry.nodes),
            "maintenance": sum(
                1 for n in registry.nodes.values() if n.maintenance),
            "started_desired": sum(
                1 for n in registry.nodes.values() if n.started),
            "queued": sum(len(n._queue) for n in registry.nodes.values()),
            "configs_dir": str(current_app.config.get("configs_dir")),
        }

    state = read_state_json(svc.get("state_file"))
    if state is None:
        return None

    if name == "eop":
        table = state.get("earth_orientation_parameter_table") or []
        stamps = sorted(e["t_inst_ns"] for e in table
                        if isinstance(e, dict) and "t_inst_ns" in e)
        if not stamps:
            return None
        # t_inst_ns is instrument time (frame0-anchored TAI ns), within
        # leap-seconds of unix time — fine for a span display.
        return {
            "entries": len(table),
            "first": _fmt_utc(stamps[0] / 1e9),
            "last": _fmt_utc(stamps[-1] / 1e9),
        }

    if name == "bffs":
        history = [h for h in (state.get("history") or [])
                   if isinstance(h, dict)]
        flagged_by = state.get("flagged_by") or {}
        return {
            "updated": _fmt_utc(state.get("updated")),
            "update_id": state.get("update_id"),
            "bad_inputs": list(state.get("bad_inputs") or []),
            # which source(s) flagged each feed (absent for state files
            # written before attribution existed)
            "flagged_by": {str(label): ", ".join(map(str, kinds or []))
                           for label, kinds in flagged_by.items()}
            if isinstance(flagged_by, dict) else {},
            # pre-shape everything the template touches, so a malformed
            # entry fails here (-> detail None) and not mid-render
            "history": [{
                "time_fmt": _fmt_utc(h.get("time")),
                "became_bad": list(h.get("became_bad") or []),
                "became_good": list(h.get("became_good") or []),
                "n_bad": len(h.get("bad_inputs") or []),
            } for h in reversed(history[-10:])],
            "history_total": len(history),
        }

    if name == "eigencal":
        try:
            good_frac = float(state.get("good_frac"))
        except (TypeError, ValueError):
            good_frac = None
        return {
            "updated": _fmt_utc(state.get("updated")),
            "transit_time": _fmt_utc(state.get("transit_time")),
            "source": state.get("source"),
            "good_frac": good_frac,
            "sent": state.get("sent"),
        }

    if name == "waterfall":
        errors = [str(e) for e in (state.get("errors") or [])]
        return {
            "updated": _fmt_utc(state.get("updated")),
            "roots": ", ".join(str(r) for r in (state.get("roots") or [])),
            "waterfalls_dir": state.get("waterfalls_dir"),
            "files_rendered": state.get("files_rendered"),
            "acquisitions_touched": state.get("acquisitions_touched"),
            "backlog": state.get("backlog"),
            "run_seconds": state.get("run_seconds"),
            "last_acquisition": state.get("last_acquisition"),
            "last_file_idx": state.get("last_file_idx"),
            "errors": errors[:10],
            "errors_total": len(errors),
        }

    return None


@bp.route("/service/psu")
def legacy_psu_page():
    """The PDB page lived at /service/psu before the rename.

    A URL alias, not a service page — kept out of ``service_page`` so
    that function stays "render the page for this service".
    """
    return redirect(url_for("web.service_page", name="pdb"))


@bp.route("/service/<name>")
@login_required
def service_page(name):
    """Detail page for one service badge: health, schedule, state, journal."""
    if name == "fpga":
        monitor = current_app.config.get("fpga_monitor")
        if monitor is None:
            abort(404)
        fpga = monitor.to_dict()
        fpga_cfg = current_app.config.get("fpga_cfg") or {}
        return render_template(
            "service_fpga.html", fpga=fpga,
            frame0_fmt=_fmt_utc((fpga["frame0_ns"] or 0) / 1e9),
            control=bool(fpga_cfg.get("control", True)),
            actions=monitor.actions,
            now_ts=time.time(),
        )
    if name == "pdb":
        if current_app.config.get("pdb_monitor") is None:
            abort(404)
        context = _pdb_context()
        return render_template(
            "service_pdb.html", **context,
            check=_pdb_cross_check(context["pdb_map"]),
        )
    svc = _service_registry().get(name)
    if svc is None:
        abort(404)
    job = job_status(svc["unit"], state_file=svc["state_file"],
                     stale_after_s=svc["stale_after_s"])
    timer = timer_status(svc["timer"]) if svc["timer"] else None
    # Pretty-printed raw state file, shown verbatim in a collapsed
    # <details> below the curated summary. Values already came from
    # json.load, so re-serializing them can't raise.
    state = read_state_json(svc["state_file"])
    state_json = json.dumps(state, indent=2) if state is not None else None
    return render_template(
        "service.html",
        name=name, svc=svc, job=job, timer=timer,
        detail=_service_detail(name, svc),
        state_json=state_json,
        nlines=_journal_lines_arg(),
        now_ts=time.time(),
    )


@bp.route("/partials/service-fpga")
@login_required
def partial_service_fpga():
    """Live status block for the FPGA page.

    The page polls this every 10s; poll_if_stale tightens the monitor's
    effective cadence only while someone is actually watching.
    """
    monitor = current_app.config.get("fpga_monitor")
    if monitor is None:
        abort(404)
    monitor.poll_if_stale(5)
    fpga = monitor.to_dict()
    return render_template(
        "_service_fpga_status.html", fpga=fpga,
        frame0_fmt=_fmt_utc((fpga["frame0_ns"] or 0) / 1e9),
        actions=monitor.actions,
        now_ts=time.time(),
    )


def _pdb_buses(monitor) -> list[dict]:
    """Per-bus grid data: boards, their chips, and channel counts.

    The monitor stores one flat row per chip in daisy-chain order; the
    grid wants them grouped by board (a board is two chips, and the
    board-level power buttons span both rows), and the bus header wants
    the counts.  Shaped here rather than in Jinja so the template stays
    a table and not arithmetic.
    """
    buses = []
    for bus, rows in sorted(monitor.channels.items()):
        boards: list[dict] = []
        for row in rows:
            if not boards or boards[-1]["board"] != row["board"]:
                boards.append({"board": row["board"], "chips": []})
            boards[-1]["chips"].append(row)
        buses.append({
            "bus": bus,
            "boards": boards,
            # /status knows the configured board count; fall back to
            # what actually came back on the wire.
            "n_boards": monitor.boards.get(bus, len(boards)),
            "n_channels": sum(len(r["channels"]) for r in rows),
            "n_on": sum(1 for r in rows for c in r["channels"] if c),
        })
    return buses


def _pdb_context() -> dict:
    """Template variables shared by the PDB page, poll, and control replies.

    The grid, its labels, and the bulk controls all render from the same
    dict, so a control reply can hand back exactly what the poll would.
    """
    monitor = current_app.config["pdb_monitor"]
    pdb_cfg = current_app.config.get("pdb_cfg") or {}
    return {
        "pdb": monitor.to_dict(),
        "buses": _pdb_buses(monitor),
        # .get() re-reads the CSV if its mtime changed, so edits show up
        # on the next render without a restart.
        "pdb_map": current_app.config["pdb_map"].get(),
        "control": bool(pdb_cfg.get("control", True)),
        "now_ts": time.time(),
    }


def _pdb_cross_check(pdb_map: PdbMap) -> dict:
    """Check the master map against a kotekan group's dish_inputs table.

    kotekan's table is the naming authority (it is what the bad-input
    mask is indexed against), so anything the map and it disagree on is
    a wiring-table bug worth showing.  Every way this can come up empty
    — no groups, an unreadable config, no dish_inputs — degrades to a
    printed reason, never a 500.
    """
    cfg = current_app.config.get("pdb_cfg") or {}
    registry = _registry()
    group = cfg.get("kotekan_group")
    if not group:
        # Single-group sites are the norm; take the first group rather
        # than making the check opt-in.
        group = next((n.group for n in registry.nodes.values()), None)
    result = {"available": False, "group": group, "reason": None}
    if not group:
        result["reason"] = "no node groups in nodes.yaml"
        return result
    sample = next((n for n in registry.nodes.values() if n.group == group),
                  None)
    if sample is None:
        result["reason"] = f"group '{group}' is not in nodes.yaml"
        return result
    try:
        desired = sample.desired_config
    except Exception as e:                      # noqa: BLE001 — never 500
        logger.exception("pdb cross-check: %s config failed to render", group)
        result["reason"] = f"{group} config failed to render: " \
                           f"{type(e).__name__}: {e}"
        return result
    if desired is None:
        result["reason"] = (sample.load_error
                            or f"no config file ({sample.config_filename})")
        return result
    try:
        labels = kotekan_dish_labels(desired)
    except ValueError as e:
        # A pre-2026-08 per-element table: its element ordering was
        # wrong, so checking against it would be checking against
        # untrustworthy data.  Reported as a migrate-this-config nudge.
        result["reason"] = str(e)
        return result
    if not labels:
        result["reason"] = f"the {group} kotekan config has no dish_inputs " \
                           f"table to check against"
        return result
    result.update(cross_check(pdb_map, labels), available=True)
    return result


@bp.route("/partials/service-pdb")
@login_required
def partial_service_pdb():
    """Live status + channel grid for the PDB page (5s htmx poll).

    poll_if_stale tightens the monitor's effective cadence only while
    someone is actually watching.  The channel map's cross-check is
    deliberately *not* here — it re-renders a kotekan config, which is
    far too much work for a 5s poll, and wiring doesn't change while
    you watch.
    """
    monitor = current_app.config.get("pdb_monitor")
    if monitor is None:
        abort(404)
    monitor.poll_if_stale(5)
    return render_template("_service_pdb_status.html", **_pdb_context())


@bp.route("/partials/service-status/<name>")
@login_required
def partial_service_status(name):
    """Live facts + state-file summary for a job page (5s htmx poll)."""
    svc = _service_registry().get(name)
    if svc is None:
        abort(404)
    job = job_status(svc["unit"], state_file=svc["state_file"],
                     stale_after_s=svc["stale_after_s"])
    timer = timer_status(svc["timer"]) if svc["timer"] else None
    return render_template(
        "_service_status.html",
        name=name, svc=svc, job=job, timer=timer,
        detail=_service_detail(name, svc),
        now_ts=time.time(),
    )


@bp.route("/service/fpga/<action>", methods=["POST"])
@login_required
def fpga_control(action):
    """Start or stop the FPGA master from the /service/fpga page.

    /start returns immediately (fpga_master initializes in the
    background, reusing its launch config); /stop blocks until the
    F-engine is down, so it runs in a greenlet and the page's status
    poll shows the transition.
    """
    if action not in ("start", "stop"):
        abort(404)
    _check_csrf()
    monitor = current_app.config.get("fpga_monitor")
    fpga_cfg = current_app.config.get("fpga_cfg") or {}
    if monitor is None or not fpga_cfg.get("control", True):
        abort(403)
    user = getattr(current_user, "username", "?")
    logger.warning(f"fpga_master {action} requested by {user}")
    if action == "start":
        ok, message = monitor.start_master()
        monitor.record_action("start", user, ok, message)
        # fpga_master acknowledges a start before checking its state; a
        # no-op start ("already started") only surfaces afterwards in
        # the status block's "Last start result".
        flash(f"FPGA start: {message}", "success" if ok else "error")
    else:
        monitor.record_action("stop", user, None,
                              "requested — shutdown in progress")

        def _stop_and_record():
            ok, message = monitor.stop_master()
            monitor.record_action("stop", user, ok, message)

        gevent.spawn(_stop_and_record)
        flash("FPGA stop requested — the state below follows the shutdown.",
              "success")
    return redirect(url_for("web.service_page", name="fpga"))


def _pdb_control_monitor():
    """The PDB monitor, or abort 403 when control is off / absent."""
    monitor = current_app.config.get("pdb_monitor")
    pdb_cfg = current_app.config.get("pdb_cfg") or {}
    if monitor is None or not pdb_cfg.get("control", True):
        abort(403)
    return monitor


def _pdb_result(ok: bool, message: str):
    """Reply to a power write.

    From htmx (the normal path) the grid is swapped in place and the
    outcome rides along as an out-of-band notice, so the page does not
    reload and does not jump back to the top — the point of doing this
    over htmx at all.  A plain form POST (no JS) still gets the old
    flash-and-redirect, so the controls degrade rather than break.
    """
    if request.headers.get("HX-Request"):
        return render_template("_service_pdb_result.html",
                               ok=ok, message=message, **_pdb_context())
    flash(f"PDB: {message}", "success" if ok else "error")
    return redirect(url_for("web.service_page", name="pdb"))


@bp.route("/service/pdb/set", methods=["POST"])
@login_required
def pdb_control():
    """Toggle one PDB channel from the /service/pdb page."""
    _check_csrf()
    monitor = _pdb_control_monitor()
    try:
        bus = int(request.form["bus"])
        board = int(request.form["board"])
        chip = request.form["chip"]
        channel = int(request.form["channel"])
        on = request.form["state"] == "on"
    except (KeyError, ValueError):
        abort(400)
    if chip not in ("A", "B") or not 0 <= channel < 8 or board < 0:
        abort(400)
    logger.warning(
        f"pdb: bus {bus} board {board} chip {chip} ch{channel} -> "
        f"{'on' if on else 'off'} requested by "
        f"{getattr(current_user, 'username', '?')}")
    ok, message = monitor.set_channel(bus, board, chip, channel, on)
    return _pdb_result(ok, message)


@bp.route("/service/pdb/set-group", methods=["POST"])
@login_required
def pdb_control_group():
    """Power a whole chip, board, or SPI bus on or off.

    Same audit trail as a single channel — one warning line naming the
    scope and the operator — because this is the same write, just wider.
    """
    _check_csrf()
    monitor = _pdb_control_monitor()
    form = request.form
    try:
        bus = int(form["bus"])
        board = int(form["board"]) if form.get("board", "") != "" else None
        on = form["state"] == "on"
    except (KeyError, ValueError):
        abort(400)
    chip = form.get("chip") or None
    if chip is not None and chip not in ("A", "B"):
        abort(400)
    if board is not None and board < 0:
        abort(400)
    if chip is not None and board is None:
        abort(400)
    scope = (f"bus {bus}" + (f" board {board}" if board is not None else "")
             + (f" chip {chip}" if chip else ""))
    logger.warning(
        f"pdb: {scope} -> all {'on' if on else 'off'} requested by "
        f"{getattr(current_user, 'username', '?')}")
    ok, message = monitor.set_group(bus, on, board=board, chip=chip)
    return _pdb_result(ok, message)


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
        orchestrator.submit_group(group, lambda key: ChangeItem(
            type=ChangeType.BASE_CONFIG, node_key=key,
            config_content=content,
        ))
        return {"status": "queued", "group": group, "action": action}

    if action == "updatable_config":
        endpoint = data.get("endpoint", "")
        values = data.get("values")
        if not endpoint or values is None:
            return {"error": "endpoint and values are required"}, 400
        orchestrator.submit_group(group, lambda key: ChangeItem(
            type=ChangeType.UPDATABLE_CONFIG, node_key=key,
            endpoint=endpoint, values=values,
        ))
        return {"status": "queued", "group": group, "action": action}

    if action == "set_started":
        started = data.get("started")
        if not isinstance(started, bool):
            return {"error": "started must be a boolean"}, 400
        for node in registry.nodes.values():
            if node.group == group:
                node.started = started
        # Wake the workers, as the dashboard toggles do, so the change
        # takes effect now rather than at the next scheduled check.
        orchestrator.submit_group(
            group, lambda key: ChangeItem(type=ChangeType.POLL, node_key=key))
        return {"status": "ok", "group": group, "started": started}

    if action == "set_maintenance":
        maintenance = data.get("maintenance")
        if not isinstance(maintenance, bool):
            return {"error": "maintenance must be a boolean"}, 400
        for node in registry.nodes.values():
            if node.group == group:
                node.maintenance = maintenance
        orchestrator.submit_group(
            group, lambda key: ChangeItem(type=ChangeType.POLL, node_key=key))
        return {"status": "ok", "group": group, "maintenance": maintenance}

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
        orchestrator.submit_node(ChangeItem(
            type=ChangeType.BASE_CONFIG, node_key=node_key,
            config_content=content,
        ))
        return {"status": "queued", "node": node_key, "action": action}

    if action == "updatable_config":
        endpoint = data.get("endpoint", "")
        values = data.get("values")
        if not endpoint or values is None:
            return {"error": "endpoint and values are required"}, 400
        orchestrator.submit_node(ChangeItem(
            type=ChangeType.UPDATABLE_CONFIG, node_key=node_key,
            endpoint=endpoint, values=values,
        ))
        return {"status": "queued", "node": node_key, "action": action}

    if action == "set_started":
        started = data.get("started")
        if not isinstance(started, bool):
            return {"error": "started must be a boolean"}, 400
        node_obj.started = started
        orchestrator.submit_node(
            ChangeItem(type=ChangeType.POLL, node_key=node_key))
        return {"status": "ok", "node": node_key, "started": started}

    if action == "set_maintenance":
        maintenance = data.get("maintenance")
        if not isinstance(maintenance, bool):
            return {"error": "maintenance must be a boolean"}, 400
        node_obj.maintenance = maintenance
        orchestrator.submit_node(
            ChangeItem(type=ChangeType.POLL, node_key=node_key))
        return {"status": "ok", "node": node_key, "maintenance": maintenance}

    return {"error": f"Unknown action '{action}'"}, 400


# --- One-off configs: start without recording ---

_ONESHOT_SKIP_REASONS = {
    NodeStatus.DOWN: "unreachable",
    NodeStatus.STARTED: "running",
    NodeStatus.UNKNOWN: "unknown state",
}


def _audit_user() -> str:
    """Who to name in an audit line: the session user, else the caller's
    address (the localhost bypass has no session)."""
    return getattr(current_user, "username", None) or request.remote_addr


def _run_oneshot(nodes: list, content: str, user: str) -> tuple[dict, int]:
    """Start *content* on every node in *nodes* that is paused and idle,
    recording nothing.

    This is a control, not a config change: the text is rendered, POSTed
    to ``/start`` on each eligible node, and forgotten -- no base file,
    no updatable store, no in-memory desired state.  The only trace is
    the audit line, which carries the config's sha256 because nothing
    else does.  Two preconditions, both checked per node:

    - **maintenance on** -- outside it the next poll would see base
      drift and restart the node onto its recorded config within one
      poll interval, so the one-off could not stick.  Lifting
      maintenance later reverts it the same way, which scopes a one-off
      to the maintenance window by construction.
    - **kotekan IDLE**, from a fresh probe rather than the cached status
      (up to a poll interval stale, or ``max_retry_interval`` for a node
      that was backing off) -- a one-off never kills, so the "no
      destructive writes while paused" rule holds.

    The fan-out is spawn-and-join like startup discovery, not bounded by
    the restart semaphore: that semaphore limits kill-and-start
    sequences because they are disruptive, and there is no kill here.
    Each started node gets a POLL item so its worker observes the new
    state now rather than a poll interval from now; the route never sets
    ``node.status`` itself -- if kotekan accepted the POST but the
    pipeline failed to come up, the poll is what tells the truth.

    Returns ``(body, status)``: 400 if the text does not render (nothing
    contacted), 409 if every node was skipped, 200 otherwise; the body
    carries ``started`` and per-node ``skipped`` reasons either way.
    """
    try:
        rendered = nodes[0].render(content)  # template vars are shared
    except Exception as e:
        return {"error": f"Invalid config: {e}"}, 400

    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
    started: list[str] = []
    skipped: dict[str, str] = {}

    def one(node):
        if not node.maintenance:
            skipped[node.key] = "not in maintenance"
            return
        probe = node.get_status()
        if probe != NodeStatus.IDLE:
            skipped[node.key] = _ONESHOT_SKIP_REASONS.get(probe, probe.value)
            return
        if node.start(rendered, override_maintenance=True):
            started.append(node.key)
        else:
            skipped[node.key] = "/start failed"

    gevent.joinall([gevent.spawn(one, n) for n in nodes])
    started.sort()

    logger.warning(
        f"oneshot by {user}: sha256 {digest}, {len(content)} bytes; "
        f"started {started}; skipped {skipped}"
    )
    orchestrator = _orchestrator()
    for key in started:
        orchestrator.submit_node(ChangeItem(type=ChangeType.POLL, node_key=key))

    body = {"started": started, "skipped": skipped, "sha256": digest}
    return body, (200 if started else 409)


@bp.route("/oneshot/<group>", methods=["POST"])
@localhost_or_login_required
def oneshot_group(group):
    """Start a config on the group's paused, idle nodes without recording it."""
    nodes = [n for n in _registry().nodes.values() if n.group == group]
    if not nodes:
        return {"error": f"Group '{group}' not found"}, 404
    data = request.get_json(silent=True) or {}
    return _run_oneshot(nodes, data.get("config_content", ""), _audit_user())


@bp.route("/oneshot/<group>/<node>", methods=["POST"])
@localhost_or_login_required
def oneshot_node(group, node):
    """Start a config on one paused, idle node without recording it."""
    node_key = f"{group}/{node}"
    node_obj = _registry().get_node(node_key)
    if node_obj is None:
        return {"error": f"Node '{node_key}' not found"}, 404
    data = request.get_json(silent=True) or {}
    return _run_oneshot([node_obj], data.get("config_content", ""),
                        _audit_user())


# --- JSON API endpoints for read-only status (localhost bypass) ---

def _node_to_dict(node) -> dict:
    return {
        "key": node.key,
        "group": node.group,
        "name": node.name,
        "host": node.host,
        "port": node.port,
        "started": node.started,
        "maintenance": node.maintenance,
        "status": node.status.value,
        "last_seen": node.last_seen,
        "last_seen_ago": node.last_seen_ago,
        "version": node.version,
        "version_info": node.version_info,
        "error": node.error,
        "queue_depth": len(node._queue),
    }


def _status_summary() -> dict:
    """Aggregate choco health: services plus node counts.

    Shared by /api/status (JSON) and /metrics (Prometheus text).
    """
    registry = _registry()
    services = _services_health()
    counts = {s.value: 0 for s in NodeStatus}
    for node in registry.nodes.values():
        counts[node.status.value] += 1
    return {
        "up": True,
        "started_at": _STARTED_AT,
        "services": {
            name: (health or {}).get("health", "unknown")
            for name, health in services.items()
        },
        "nodes": {
            "total": len(registry.nodes),
            "started_desired": sum(
                1 for n in registry.nodes.values() if n.started),
            "maintenance": sum(
                1 for n in registry.nodes.values() if n.maintenance),
            **counts,
        },
    }


@bp.route("/api/status", methods=["GET"])
@localhost_or_login_required
def api_status():
    """Simple overall health: choco itself, services, and node counts.

    The detailed per-node dump lives at /api/nodes/status.
    """
    return _status_summary()


# Health states each service can report, for one-hot /metrics gauges.
_JOB_STATES = ("ok", "degraded", "stale", "failed", "never_run", "unknown")
_MONITOR_STATES = {
    "fpga": ("ok", "no_timing", "down", "unconfigured", "unknown"),
    "pdb": ("ok", "no_states", "down", "unconfigured", "unknown"),
    "data": ("ok", "degraded", "down", "unconfigured", "unknown"),
}


@bp.route("/metrics", methods=["GET"])
def metrics():
    """Prometheus metrics.

    Deliberately unauthenticated (Prometheus scrapes from another
    host and speaks neither LDAP sessions nor our CSRF); it exposes
    only aggregate health — no node names, hosts, or configs.
    """
    summary = _status_summary()
    nodes = summary["nodes"]
    lines = [
        "# HELP choco_up 1 while choco is serving requests.",
        "# TYPE choco_up gauge",
        "choco_up 1",
        "# HELP choco_start_time_seconds Unix time the choco process "
        "started (a restart re-engages cluster-wide maintenance mode).",
        "# TYPE choco_start_time_seconds gauge",
        f"choco_start_time_seconds {_STARTED_AT:.3f}",
        "# HELP choco_service_state Service health, one-hot per state.",
        "# TYPE choco_service_state gauge",
    ]
    for name, health in summary["services"].items():
        states = _MONITOR_STATES.get(name, _JOB_STATES)
        for state in states:
            value = 1 if health == state else 0
            lines.append(
                f'choco_service_state{{service="{name}",state="{state}"}} '
                f"{value}")
    lines += [
        "# HELP choco_nodes Node count by kotekan runtime status.",
        "# TYPE choco_nodes gauge",
    ]
    for status in NodeStatus:
        lines.append(
            f'choco_nodes{{status="{status.value}"}} {nodes[status.value]}')
    lines += [
        "# HELP choco_nodes_total Number of registered nodes.",
        "# TYPE choco_nodes_total gauge",
        f"choco_nodes_total {nodes['total']}",
        "# HELP choco_nodes_started_desired Nodes whose desired state is started.",
        "# TYPE choco_nodes_started_desired gauge",
        f"choco_nodes_started_desired {nodes['started_desired']}",
        "# HELP choco_nodes_maintenance Nodes currently in maintenance mode.",
        "# TYPE choco_nodes_maintenance gauge",
        f"choco_nodes_maintenance {nodes['maintenance']}",
    ]
    return Response("\n".join(lines) + "\n",
                    mimetype="text/plain; version=0.0.4")


@bp.route("/api/nodes/status", methods=["GET"])
@localhost_or_login_required
def api_nodes_status():
    """Per-node runtime status plus an aggregate summary."""
    registry = _registry()
    orchestrator = _orchestrator()
    nodes = [_node_to_dict(n) | {"worker": orchestrator.worker_status(n.key)}
             for n in registry.nodes.values()]
    summary = {s.value: 0 for s in NodeStatus}
    summary["total"] = len(nodes)
    summary["started_desired"] = sum(1 for n in registry.nodes.values() if n.started)
    for n in nodes:
        summary[n["status"]] += 1
    return {"summary": summary, "nodes": nodes}


@bp.route("/api/nodes", methods=["GET"])
@localhost_or_login_required
def api_nodes():
    """Node registry (the nodes.yaml contents) as JSON."""
    registry = _registry()
    groups: dict[str, list] = {}
    for node in registry.nodes.values():
        groups.setdefault(node.group, []).append({
            "name": node.name,
            "host": node.host,
            "port": node.port,
            "started": node.started,
        })
    return {"groups": groups}


@bp.route("/api/config/<group>", methods=["GET"])
@localhost_or_login_required
def api_group_config(group):
    """A sample node's desired kotekan config for *group*, as JSON.

    Group configs are broadcast together, so any member represents the
    group.  Jobs use this to learn the element layout (``dish_inputs``)
    that kotekan's bad-input mask is indexed against, without touching
    data files.
    """
    registry = _registry()
    sample = next(
        (n for n in registry.nodes.values() if n.group == group), None
    )
    if sample is None:
        return {"error": f"Group '{group}' not found"}, 404
    desired = sample.desired_config
    if desired is None:
        return {"error": sample.load_error
                or f"No config file ({sample.config_filename})"}, 503
    return desired


@bp.route("/api/pdb/map", methods=["GET"])
@localhost_or_login_required
def api_pdb_map():
    """The master dish-input <-> power-channel table, plus its cross-check.

    One table, served rather than copied: bffs's power source reads it
    from here so choco's file stays the single authority for which
    breaker feeds which feed.  ``check`` is the same comparison against
    kotekan's ``dish_inputs`` that the /service/pdb page shows, so a
    consumer can refuse to act on a map kotekan disagrees with.
    """
    pdb_map = current_app.config["pdb_map"].get()
    return {
        "path": pdb_map.path,
        "mtime": pdb_map.mtime,
        "n_entries": pdb_map.n_entries,
        "errors": pdb_map.errors,
        "channels": pdb_map.to_list(),
        "check": _pdb_cross_check(pdb_map),
    }


@bp.route("/api/files", methods=["GET"])
@localhost_or_login_required
def api_files():
    """The same scan as /files, as JSON.  ``?refresh=1`` bypasses the cache.

    Sizes are bytes and times are unix timestamps — the page's own
    formatting is display, not data.
    """
    scan = _datafile_scan()
    if scan is None or not scan.configured:
        return {"configured": False, "roots": []}
    return scan.get(force=request.args.get("refresh") == "1")
