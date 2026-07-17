"""Flask routes for the choco web UI."""

import json
import logging
import secrets
import time
from pathlib import Path

import gevent

from flask import (
    Blueprint, Response, render_template, request, redirect, url_for, flash,
    current_app, session, abort,
)
from flask_login import login_required, login_user, logout_user, current_user

from .auth import save_user, localhost_or_login_required
from .services import (
    job_status, job_logs, timer_status, read_state_json, EOP_STALE_AFTER_S,
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


def _check_csrf():
    token = request.form.get("_csrf_token", "")
    if not token or token != session.get("_csrf_token"):
        abort(403)


def _check_csrf_header():
    """CSRF check for JSON POSTs: token comes in via an ``X-CSRF-Token`` header."""
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


# --- Nodes-registry editor (nodes.yaml) ---

@bp.route("/nodes", methods=["GET"])
@login_required
def nodes_edit():
    """Render the nodes.yaml editor with drag-and-drop groups."""
    registry = _registry()
    groups: dict[str, list] = {}
    for node in registry.nodes.values():
        groups.setdefault(node.group, []).append(node)
    return render_template("nodes.html", groups=groups)


@bp.route("/nodes", methods=["POST"])
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

@bp.route("/edit-group/<group>", methods=["GET", "POST"])
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

@bp.route("/toggle-started/<path:node_key>", methods=["POST"])
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


@bp.route("/set-started-all/<action>", methods=["POST"])
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


@bp.route("/set-started-group/<group>/<action>", methods=["POST"])
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


@bp.route("/toggle-maintenance/<path:node_key>", methods=["POST"])
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


@bp.route("/set-maintenance-all/<action>", methods=["POST"])
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


@bp.route("/set-maintenance-group/<group>/<action>", methods=["POST"])
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
    configs_dir = current_app.config.get("configs_dir")

    # EOP rewrites its state file on every successful (daily) run, so
    # the mtime doubles as "last successful run" and goes stale.
    eop_state = None
    if configs_dir and eop_cfg.get("state_file"):
        eop_state = Path(configs_dir) / str(eop_cfg["state_file"])

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
    }


def _service_units() -> dict[str, str]:
    """Name -> systemd unit for the units whose journals may be viewed."""
    return {name: s["unit"] for name, s in _service_registry().items()}


def _services_health() -> dict:
    """Health snapshots for the hardware monitors and the oneshot jobs.

    Shared by the header strip, /api/status, and /metrics.
    """
    fpga = current_app.config.get("fpga_monitor")
    psu = current_app.config.get("psu_monitor")
    registry = _service_registry()
    health = {
        "fpga": fpga.to_dict() if fpga is not None else None,
        "psu": psu.to_dict() if psu is not None else None,
    }
    for name in ("eop", "bffs", "eigencal"):
        svc = registry[name]
        health[name] = job_status(svc["unit"], state_file=svc["state_file"],
                                  stale_after_s=svc["stale_after_s"])
    return health


@bp.route("/partials/services")
@login_required
def partial_services():
    """Render the monitor (FPGA, PSU) + job (EOP, bffs, eigencal) strip."""
    services = _services_health()
    return render_template(
        "_services_status.html",
        fpga=services["fpga"],
        psu=services["psu"],
        eop=services["eop"],
        bffs=services["bffs"],
        eigencal=services["eigencal"],
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

    return None


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
    if name == "psu":
        monitor = current_app.config.get("psu_monitor")
        if monitor is None:
            abort(404)
        psu_cfg = current_app.config.get("psu_cfg") or {}
        return render_template(
            "service_psu.html", psu=monitor.to_dict(),
            channels=monitor.channels, boards=monitor.boards,
            control=bool(psu_cfg.get("control", True)),
            now_ts=time.time(),
        )
    svc = _service_registry().get(name)
    if svc is None:
        abort(404)
    job = job_status(svc["unit"], state_file=svc["state_file"],
                     stale_after_s=svc["stale_after_s"])
    timer = timer_status(svc["timer"]) if svc["timer"] else None
    return render_template(
        "service.html",
        name=name, svc=svc, job=job, timer=timer,
        detail=_service_detail(name, svc),
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
    monitor.poll_if_stale(10)
    fpga = monitor.to_dict()
    return render_template(
        "_service_fpga_status.html", fpga=fpga,
        frame0_fmt=_fmt_utc((fpga["frame0_ns"] or 0) / 1e9),
        actions=monitor.actions,
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


@bp.route("/service/psu/set", methods=["POST"])
@login_required
def psu_control():
    """Toggle one PSU channel from the /service/psu page."""
    _check_csrf()
    monitor = current_app.config.get("psu_monitor")
    psu_cfg = current_app.config.get("psu_cfg") or {}
    if monitor is None or not psu_cfg.get("control", True):
        abort(403)
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
        f"psu: bus {bus} board {board} chip {chip} ch{channel} -> "
        f"{'on' if on else 'off'} requested by "
        f"{getattr(current_user, 'username', '?')}")
    ok, message = monitor.set_channel(bus, board, chip, channel, on)
    flash(f"PSU: {message}", "success" if ok else "error")
    return redirect(url_for("web.service_page", name="psu"))


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
        return {"status": "ok", "group": group, "started": started}

    if action == "set_maintenance":
        maintenance = data.get("maintenance")
        if not isinstance(maintenance, bool):
            return {"error": "maintenance must be a boolean"}, 400
        for node in registry.nodes.values():
            if node.group == group:
                node.maintenance = maintenance
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
        return {"status": "ok", "node": node_key, "started": started}

    if action == "set_maintenance":
        maintenance = data.get("maintenance")
        if not isinstance(maintenance, bool):
            return {"error": "maintenance must be a boolean"}, 400
        node_obj.maintenance = maintenance
        return {"status": "ok", "node": node_key, "maintenance": maintenance}

    return {"error": f"Unknown action '{action}'"}, 400


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
    "psu": ("ok", "no_states", "down", "unconfigured", "unknown"),
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
    nodes = [_node_to_dict(n) for n in registry.nodes.values()]
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
