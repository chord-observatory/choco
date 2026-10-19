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
from markupsafe import Markup

from flask import (
    Blueprint, Response, render_template, request, redirect, url_for, flash,
    current_app, session, abort, send_file,
)

from . import auth, library
from .auth import (current_user, localhost_or_login_required, login_required, login_user,
                   logout_user, save_user)
from .jobclient import MANUAL_OVERRIDES_NAME, RUN_NAME, STATE_NAME, STATE_ROOT
from .datafiles import (TIMELINE_NAME, human_bytes, notes_html, read_notes,
                        run_blocking, timeline_rows, utc_minute)
from . import dishlabels
from .pdbmap import (PdbMap, cross_check, dish_layout, kotekan_dish_labels, row_entries,
                     valid_channel_address)
from .waterfalls import (coarse_image as wf_coarse_image, coarse_view_context as wf_coarse_view_context,
                         matrix_context as wf_matrix_context, matrix_image as wf_matrix_image,
                         open_stream, overview_of, parse_elements, rendered_index,
                         strips as wf_strips, triangle as wf_triangle,
                         view_context as wf_view_context)
from .jobdetail import (JOB_DETAILS, MAPMAKER_IMAGES, file_mtime, float_or_none, fmt_utc,
                        job_manual_path, same_path)
from .jobstatus import (EOP_STALE_AFTER_S, job_logs, job_status, read_state_json,
                        timer_status)
from .manualflags import toggle_manual_flag
from .pipeline_svg import PIPELINE_LAYOUTS, render_dot_svg, sanitize_pipeline_svg
from .state import NodeStatus, find_updatable_blocks, validate_nodes_payload
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
    auth.require_csrf_token(request.form.get("_csrf_token", ""))


def _check_csrf_header():
    """CSRF check for JSON POSTs: token comes in via an ``X-CSRF-Token`` header."""
    auth.require_csrf_token(request.headers.get("X-CSRF-Token", ""))


@bp.app_context_processor
def inject_csrf():
    return {"csrf_token": _csrf_token}


def _registry():
    return current_app.config["registry"]


def _orchestrator():
    return current_app.config["orchestrator"]


def _flag_response(nodes, attr: str, value: bool):
    """Reply to a bulk toggle: the dashboard table for htmx, else a redirect."""
    _orchestrator().set_flag(nodes, attr, value)
    if request.headers.get("HX-Request"):
        return render_template("_dashboard_table.html", nodes=_registry().nodes)
    return redirect(url_for("web.dashboard"))


# URL action words of the bulk toggle routes -> flag value.
_STARTED_ACTIONS = {"start": True, "stop": False}
_MAINTENANCE_ACTIONS = {"on": True, "off": False}


# --- Authentication routes ---

def _next_target() -> str:
    """The post-login destination: ?next= when it is a same-site path.

    Anything else — empty, scheme-relative (//host), an absolute URL, or
    a path carrying a backslash or control character — falls back to the
    landing page, so a crafted login link cannot bounce a fresh session
    to another site.  The backslash rule is what makes the ``//`` rule
    hold: browsers resolve ``/\\host`` in a Location header exactly as
    ``//host``.  The login form posts to its own URL (no action
    attribute), so ?next= survives failed attempts too.  ?next= is
    relative to the app root; behind the site proxy the prefix
    (``request.script_root``) goes back on here.
    """
    next_page = request.args.get("next", "")
    if (not next_page.startswith("/") or next_page.startswith("//")
            or "\\" in next_page
            or any(ord(c) < 0x20 or c == "\x7f" for c in next_page)):
        return url_for("web.landing")
    return request.script_root + next_page


# --- who is here -----------------------------------------------------------
#
# Per-user last-seen, in process (ephemeral like every other runtime fact):
# any authenticated request is presence, a page load or a POST is an
# action, and a partial poll (htmx) is neither more nor less than "has a
# page open".  The nav shows the operator's own name and how many others
# were active in the last PRESENCE_WINDOW_S, with the list in a tooltip.

PRESENCE_WINDOW_S = 600
_POLL_PREFIXES = ("/partials/", "/nodes/partials/")


def _presence() -> dict:
    return current_app.config.setdefault("presence", {})


@bp.before_app_request
def _note_presence():
    if not getattr(current_user, "is_authenticated", False):
        return
    path = request.path
    if path.startswith("/static/"):
        return
    poll = path.startswith(_POLL_PREFIXES)
    if poll:
        # the page the poll belongs to, from htmx's own header
        page = request.headers.get("HX-Current-URL", "")
        page = re.sub(r"^https?://[^/]+", "", page) or None
    else:
        page = path
    now = time.time()
    entry = _presence().setdefault(current_user.username,
                                   {"last_seen": now, "last_action": None, "page": None})
    entry["last_seen"] = now
    if page:
        entry["page"] = page
    if not poll:
        entry["last_action"] = now


_PAGE_NAMES = (("/nodes/edit/", "node"), ("/nodes/edit", "edit nodes"),
               ("/nodes", "nodes"), ("/configs", "configs"), ("/files", "files"),
               ("/waterfall", "waterfall"), ("/pipeline/", "pipeline"), ("/plot/", "plot"),
               ("/service/", "service"), ("/", "services"))


def _page_name(path: str | None) -> str:
    """A short name for where someone is, from the path prefix."""
    if not path:
        return "?"
    for prefix, name in _PAGE_NAMES:
        if path.startswith(prefix):
            if name in ("node", "pipeline", "plot", "service"):
                rest = path[len(prefix):].split("?")[0].strip("/")
                return f"{name} {rest}" if rest else name
            return name
    return path


def _active_users(now: float | None = None) -> list[dict]:
    """Everyone seen within the window, most recent first; stale entries
    are dropped on the way so the table never grows past the roster."""
    now = time.time() if now is None else now
    table = _presence()
    for name in [n for n, e in table.items()
                 if now - e["last_seen"] > PRESENCE_WINDOW_S]:
        del table[name]
    out = []
    for name, e in table.items():
        out.append({"name": name, "page": _page_name(e.get("page")),
                    "seen_ago": int(now - e["last_seen"]),
                    "action_ago": (int(now - e["last_action"])
                                   if e.get("last_action") else None)})
    out.sort(key=lambda u: u["seen_ago"])
    return out


@bp.route("/partials/presence")
@login_required
def partial_presence():
    """The nav's who-is-here line (60 s htmx poll)."""
    return render_template("_presence.html", users=_active_users(),
                           me=current_user.username,
                           window_min=PRESENCE_WINDOW_S // 60)


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
    # Behind the site proxy the session comes from Authelia: sign out
    # there too, or the next request signs straight back in.
    if auth.is_proxied():
        return redirect(current_app.config["PROXY_AUTH"]["logout_url"])
    flash("You have been logged out.", "success")
    return redirect(url_for("web.login"))


# --- Main routes (all require login) ---

def _landing_context() -> dict:
    """Everything the landing page's service table shows.

    The same health snapshots as the header strip, plus the per-service
    facts the strip only carries in a tooltip: timer schedules and the
    state-file summaries (both already tolerant of missing systemd or
    unusable state files).
    """
    svc_registry = _service_registry()
    services = _services_health(svc_registry)
    timers = {}
    details = {}
    for name, svc in svc_registry.items():
        if svc["kind"] != "job":
            continue
        timers[name] = timer_status(svc["timer"])
        details[name] = _service_detail(name, svc)
    return {
        "services": services,
        "svc": svc_registry,
        "folds": {host: [[n, services[n]] for n in names]
                  for host, names in _folded(svc_registry).items()},
        "timers": timers,
        "details": details,
        "choco": _service_detail("choco", svc_registry["choco"]),
        "nodes": _nodes_health(),
        "now_ts": time.time(),
    }


@bp.route("/")
@login_required
def landing():
    """CHOCO landing page: the services overview table.

    Node management lives under /nodes/*; this page is the front door.
    """
    # ?wall=1: a wall display -- no chrome, larger tags (base.html / css).
    return render_template("landing.html", wall=request.args.get("wall") == "1",
                           **_landing_context())


@bp.route("/partials/landing-services")
@login_required
def partial_landing_services():
    """The landing page's service table, htmx-refreshed in place."""
    return render_template("_landing_services.html", **_landing_context())


def _state_root() -> Path:
    """Where the jobs keep their files: ``state_dir`` (default
    ``/var/lib/choco``), the root systemd's ``StateDirectory=choco/<job>``
    lays out.  Everything choco reads back from a job sits under it."""
    return Path(current_app.config.get("state_dir") or STATE_ROOT)


def _skymap_file(night: bool = False) -> Path:
    """The rendered sky-map PNG (or its night-palette twin), where the
    skymap job writes it: ``<state_dir>/skymap/skymap[-night].png``."""
    return _state_root() / "skymap" / ("skymap-night.png" if night else "skymap.png")


def _send_png(path: Path | None):
    """A job's PNG as it stands, revalidated on every request."""
    if path is None or not path.is_file():
        abort(404)
    return send_file(path, mimetype="image/png", conditional=True, max_age=0)


def _component(key: str):
    """The app's *key* -- a monitor, an archive, the scan, the store --
    when it is there and configured, else None: one test per route, not
    two."""
    obj = current_app.config.get(key)
    return obj if obj is not None and obj.configured else None


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
    return _send_png(_skymap_file())


@bp.route("/skymap-night.png")
def skymap_night_png():
    """The same render in the job's night palette, for wall displays in
    a dim control room.  Unauthenticated for the same reason as
    /skymap.png and carrying the same single cluster fact; 404 until
    the job has rendered it (``night: true`` in skymap.yaml).
    """
    return _send_png(_skymap_file(night=True))


@bp.route("/service/mapmaker/<name>.png")
@login_required
def mapmaker_png(name):
    """One of the mapmaker job's PNGs (sky, daily, clean and their -strip
    forms), read from its state directory; 404 until rendered.
    The name is looked up in an allowlist, never joined into a path."""
    fname = MAPMAKER_IMAGES.get(name)
    if fname is None:
        abort(404)
    return _send_png(_state_root() / "mapmaker" / fname)


@bp.route("/partials/skymap")
@login_required
def partial_skymap():
    """The landing page's sky-map card, htmx-refreshed every 5 min.

    The image URL carries the file's mtime, so a refresh swaps in a new
    render exactly when one has landed and is a no-op otherwise.  The
    card links the night-palette image when one has been rendered.
    """
    return render_template("_skymap.html",
                           mtime=file_mtime(_skymap_file()),
                           night_mtime=file_mtime(_skymap_file(night=True)),
                           now_ts=time.time())


@bp.route("/nodes")
@login_required
def dashboard():
    registry = _registry()
    return render_template("dashboard.html", nodes=registry.nodes)


@bp.route("/nodes/edit/<path:node_key>", methods=["GET", "POST"])
@login_required
def node_edit(node_key):
    """A node's page: desired state and maintenance, live status, which
    library file it renders (the selector; a nodes.yaml edit), re-push
    and one-off controls, and its updatable config values.  The config
    text itself is edited in the library (/configs/edit/<path>)."""
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
            orchestrator.submit_node(node_key, ChangeItem(ChangeType.RESYNC))
            flash(f"Config re-push queued for {node_key}", "success")

        elif action == "oneshot":
            # A library file's text, started but never recorded as this
            # node's config (see _run_oneshot).  The helper's JSON body
            # is folded into a flash message since this caller is a
            # form post.  The path is validated before it is read.
            rel = request.form.get("config", "")
            try:
                path = library.file_path(registry, rel)
            except ValueError as e:
                flash(f"One-off not started: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            if not path.is_file():
                flash(f"One-off not started: no such config file {rel}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            body, status = _run_oneshot([node], path.read_text(), _audit_user())
            if status == 400:
                flash(body["error"], "error")
            elif node_key in body["started"]:
                flash(f"One-off {rel} started on {node_key} "
                      f"(sha256 {body['sha256']}); nothing saved.",
                      "success")
            else:
                flash(f"One-off not started on {node_key}: "
                      f"{body['skipped'][node_key]}", "error")

        elif action == "set_config":
            # Which library file this node renders: a nodes.yaml edit,
            # so a registry rebuild (see library.set_node_configs).
            body, status = library.set_node_configs(
                _registry(), _orchestrator(), [node], request.form.get("config") or None,
                _audit_user())
            if status == 200:
                fresh = registry.get_node(node_key)
                flash(f"{node_key} now renders {fresh.config_filename}; "
                      "the registry was rebuilt, so every node is in "
                      "maintenance mode.", "success")
            else:
                flash(body["error"], "error")

        elif action == "update_config":  # updatable_config change
            endpoint = request.form.get("endpoint", "")
            raw_json = request.form.get("updatable_content", "")
            try:
                values = json.loads(raw_json)
            except json.JSONDecodeError as e:
                flash(f"Invalid JSON: {e}", "error")
                return redirect(url_for("web.node_edit", node_key=node_key))
            orchestrator.submit_node(node_key, ChangeItem(
                ChangeType.UPDATABLE_CONFIG, endpoint=endpoint, values=values))
            flash(f"Update queued for /{endpoint}", "success")

        return redirect(url_for("web.node_edit", node_key=node_key))

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

    # Other nodes rendering the same file: a save here is a save for
    # them too, which the page says beside the file name.
    direct, _ = registry.users_of(node.config_filename)
    shared_with = [n.key for n in direct if n is not node]

    return render_template(
        "edit.html",
        node=node,
        node_key=node_key,
        updatable_json=updatable_json,
        shared_with=shared_with,
        config_files=registry.config_files(),
    )


@bp.route("/pipeline/<path:node_key>")
@login_required
def node_pipeline_page(node_key):
    """Full-page interactive pipeline view.

    The graph pane fills the viewport (drag to pan, scroll to zoom,
    Fit/1:1 buttons) and buffer nodes with ``peek_hold`` are clickable —
    a click opens the live plot in a popup overlay pinned to the
    bottom-right corner, so the graph keeps the whole viewport.  The SVG
    is inlined after ``sanitize_pipeline_svg`` whitelist reconstruction.

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
        "plot.html", node_key=node_key, title=buffer_name,
        subtitle=node.key,
        source_url=request.script_root + "/api/node-buffer-data/" + node_key
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
    return render_template("nodes.html", groups=groups,
                           config_files=registry.config_files())


@bp.route("/nodes/edit", methods=["POST"])
@login_required
def nodes_save():
    """Validate the posted JSON structure, save nodes.yaml, and reload."""
    _check_csrf_header()

    try:
        new_data = validate_nodes_payload(request.get_json(silent=True) or {},
                                          _registry().configs_dir)
    except ValueError as e:
        return {"error": str(e)}, 400
    try:
        _orchestrator().apply_nodes_update(new_data)
    except Exception as e:
        logger.exception("Failed to apply nodes update")
        return {"error": str(e)}, 500

    flash("Node registry saved; all nodes placed in maintenance mode.",
          "success")
    return {"status": "ok"}


@bp.route("/configs")
@login_required
def configs_page():
    """The config library: every config file under configs_dir, with
    the nodes that render or include each."""
    registry = _registry()
    files = [library.entry(registry, rel) for rel in registry.config_files()]
    return render_template("configs.html", files=files,
                           upstream=current_app.config.get("upstream"))


def _library_pull(dry_run: bool) -> tuple[dict, int]:
    """``library.pull`` with the app's upstream, by the request's user."""
    return library.pull(_registry(), _orchestrator(), current_app.config.get("upstream"),
                        dry_run, _audit_user())


@bp.route("/configs/pull", methods=["POST"])
@login_required
def configs_pull():
    """The library page's "Pull <dir> from GitHub" button: mirror the
    upstream config directory and say what changed (``library.pull``)."""
    _check_csrf()
    result, status = _library_pull(dry_run=False)
    if status != 200:
        flash(f"Not pulled: {result['error']}", "error")
    else:
        flash(result["summary"], "success")
    return redirect(url_for("web.configs_page"))


@bp.route("/configs/new", methods=["POST"])
@login_required
def config_new():
    """Create an empty library file and open it in the editor."""
    _check_csrf()
    registry = _registry()
    name = request.form.get("name", "").strip()
    try:
        path = library.file_path(registry, name)
    except ValueError as e:
        flash(f"Not created: {e}", "error")
        return redirect(url_for("web.configs_page"))
    if path.exists():
        flash(f"{name} already exists", "error")
    else:
        library.write_file(registry, _orchestrator(), name, "", _audit_user())
        flash(f"Created {name}", "success")
    return redirect(url_for("web.config_edit", name=name))


@bp.route("/configs/edit/<path:name>", methods=["GET", "POST"])
@login_required
def config_edit(name):
    """Edit one library file.

    A save is rendered through every node that uses the file before it
    is written; a change that would break any of them is refused with
    that node's error.  The path comes from the URL, so it is validated
    (``resolve_config_path``) before anything is opened: 404 otherwise.
    """
    registry = _registry()
    try:
        path = library.file_path(registry, name)
    except ValueError:
        abort(404)
    if not path.is_file():
        abort(404)
    entry = library.entry(registry, name)
    if request.method == "POST":
        _check_csrf()
        # Browsers submit textarea text with CRLF line endings.
        content = request.form.get("content", "").replace("\r\n", "\n")
        affected, error = library.check_save(registry, name, content)
        if error:
            flash(f"Not saved: {error}", "error")
            return render_template("config_edit.html", entry=entry,
                                   content=content)
        library.write_file(registry, _orchestrator(), name, content, _audit_user())
        flash(f"Saved {name}"
              + (f"; re-rendered {', '.join(affected)}" if affected else ""),
              "success")
        return redirect(url_for("web.config_edit", name=name))
    return render_template("config_edit.html", entry=entry,
                           content=path.read_text())


# --- htmx partial endpoints for live updates ---

def _toggle(node_key: str, flag: str, words: tuple[str, str]):
    """Flip one node's desired *flag*.  htmx gets the switch back in
    place; a plain form post a flash (*words* for on / off) and a redirect."""
    _check_csrf()
    node = _registry().get_node(node_key)
    if node is None:
        abort(404)
    _orchestrator().set_flag([node], flag, not getattr(node, flag))
    if request.headers.get("HX-Request"):
        return render_template(f"_toggle_{flag}.html", node=node, key=node_key)
    flash(f"{node_key} {words[0] if getattr(node, flag) else words[1]}", "success")
    return redirect(request.referrer or url_for("web.dashboard"))


def _bulk(flag: str, actions: dict, action: str, group: str | None):
    """Set *flag* on every node, or on *group*'s, from the URL's action word."""
    _check_csrf()
    if action not in actions:
        abort(400)
    registry = _registry()
    nodes = registry.in_group(group) if group is not None else list(registry.nodes.values())
    if group is not None and not nodes:
        abort(404)
    return _flag_response(nodes, flag, actions[action])


@bp.route("/nodes/toggle-started/<path:node_key>", methods=["POST"])
@login_required
def toggle_started(node_key):
    """Toggle the started/idle desired state for a node."""
    return _toggle(node_key, "started", ("started", "stopped"))


@bp.route("/nodes/set-started-all/<action>", methods=["POST"])
@bp.route("/nodes/set-started-group/<group>/<action>", methods=["POST"])
@login_required
def set_started(action, group=None):
    """Set every node, or every node in *group*, to started or stopped."""
    return _bulk("started", _STARTED_ACTIONS, action, group)


@bp.route("/nodes/toggle-maintenance/<path:node_key>", methods=["POST"])
@login_required
def toggle_maintenance(node_key):
    """Toggle maintenance mode for a single node."""
    return _toggle(node_key, "maintenance", ("maintenance on", "maintenance off"))


@bp.route("/nodes/set-maintenance-all/<action>", methods=["POST"])
@bp.route("/nodes/set-maintenance-group/<group>/<action>", methods=["POST"])
@login_required
def set_maintenance(action, group=None):
    """Put every node, or every node in *group*, into or out of maintenance."""
    return _bulk("maintenance", _MAINTENANCE_ACTIONS, action, group)


@bp.route("/nodes/partials/node-status/<path:node_key>")
@login_required
def partial_node_status(node_key):
    """The edit page's status rows, rendered from the worker's last probe.

    Deliberately no probe of its own: the node's worker is the only
    writer of ``node.status``, and its cadence (``poll_interval`` while
    reachable) already bounds how fresh this can be.
    """
    registry = _registry()
    node = registry.get_node(node_key)
    if node is None:
        abort(404)
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
    length, error = _len_arg()
    if error:
        return error

    frame = node.get_buffer_frame(buffer_name, length=length)
    if frame is None:
        return {"error": "kotekan unreachable"}, 502
    if frame.get("error"):
        return {"error": frame["error"]}, 404
    if length == 0:
        _fill_implicit_input_list(node, frame)
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


def _fill_implicit_input_list(node, frame: dict) -> None:
    """Spell out the element identities a compact ``DishInputs`` descriptor implies.

    kotekan (develop since PR #1658, 2026-09-11) compacts a ``DishInputs``
    frame to the telescope's connected elements and sends only their
    count: which fiducial inputs they are is implied by the
    ``dish_inputs`` table it runs with — the one choco pushed.  Derive
    the list the same way (``dishlabels.connected_elements``) from the
    node's desired config and hand it to the plotter as ``input_list``,
    the wire form the kv-branch builds send explicitly (a descriptor that
    already carries one is left alone).  The result is used only when it
    is exactly ``num_elements`` long: a rule drift between kotekan and
    choco then shows as a frame without identities (the plotter renders
    it generically) rather than as mislabelled inputs.
    """
    desc = frame.get("frame_desc")
    if (not isinstance(desc, dict) or desc.get("frame_desc_type") != "N2"
            or desc.get("n2_layout") != "DishInputs" or "input_list" in desc):
        return
    num_elements = desc.get("num_elements")
    if isinstance(num_elements, bool) or not isinstance(num_elements, int):
        return
    try:
        inputs = dishlabels.connected_elements(node.desired_config)
    except ValueError:                    # a table kotekan would refuse too
        return
    if inputs is not None and len(inputs) == num_elements:
        desc["input_list"] = inputs


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
    archive = _component("gain_archive")
    if archive is None:
        return {"error": "fpga_master is not configured"}, 404
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        return {"error": "bad dataset name"}, 400
    length, error = _len_arg()
    if error:
        return error

    info = archive.to_dict()
    update_id = _first_scalar(info["scalars"].get("update_id"))
    return _archive_data_response(archive, dataset, length, update_id)


def _len_arg():
    """The ``len`` query parameter of a data request, clamped to the cap,
    with the error reply when it is not a non-negative integer."""
    try:
        length = int(request.args.get("len", _BUFFER_DATA_DEFAULT_LEN))
    except ValueError:
        length = -1
    if length < 0:
        return None, ({"error": "'len' must be a non-negative integer"}, 400)
    return min(length, _BUFFER_DATA_MAX_LEN), None


def _archive_data_response(archive, dataset: str, length: int, frame_id):
    """One HDF5 dataset in the buffer-plot wire format (see
    api_fpga_gain_data); *frame_id* is what the plotter's staleness note
    compares, the archive's update id or its file identity."""
    desc = archive.describe(dataset)
    if desc is None:
        return {"error": archive.error or f"no dataset '{dataset}'"}, 404
    info = archive.to_dict()
    if length == 0:
        return {
            "frame_desc": {
                "value_type": desc["value_type"],
                "extents": desc["extents"],
                "dimnames": desc["dimnames"],
            },
            "frame_id": frame_id,
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
        return {"error": archive.error or "archive unreadable"}, 502
    resp = Response(raw[:length], mimetype="application/octet-stream")
    resp.headers["X-Frame-Id"] = str(frame_id or "")
    resp.headers["X-Frame-Size"] = str(len(raw))
    resp.headers["Cache-Control"] = "no-store"
    return resp


# --- eigencal's archived solution, the same way ------------------------------

def _eigencal_gain_context() -> dict:
    """What the EIGENCAL page's solution card shows: the newest archive's
    datasets and headline attributes, or why there is nothing to show."""
    archive = _component("eigencal_archive")
    if archive is None:
        return {"available": False, "error": "no archive reader", "datasets": []}
    if not archive.refresh():
        return {"available": False, "error": archive.error, "datasets": []}
    info = archive.to_dict()
    attrs = info.get("attrs") or {}
    transit = _first_scalar(attrs.get("transit_time"))
    return {
        "available": True, "error": None,
        "datasets": info["datasets"],
        "filename": archive.path.name if archive.path else None,
        "source": _first_scalar(attrs.get("source")),
        "transit_fmt": fmt_utc(transit) if isinstance(transit, (int, float)) else None,
        "good_frac": float_or_none(_first_scalar(attrs.get("good_frac"))),
    }


@bp.route("/partials/eigencal-gains")
@login_required
def partial_eigencal_gains():
    """The solution card, loaded after the page paints: reading the
    archive is a subprocess and must not delay the facts above it."""
    return render_template("_eigencal_gains.html", gains=_eigencal_gain_context(),
                           poll_ms=GAIN_POLL_MS, fetch_bytes=GAIN_FETCH_BYTES)


@bp.route("/api/eigencal/gain-data")
@localhost_or_login_required
def api_eigencal_gain_data():
    """The newest eigencal solution's datasets, in the buffer-plot protocol
    (see api_fpga_gain_data).  The frame id is the file and its mtime, so
    the plotter's staleness note means "no new solution"."""
    archive = _component("eigencal_archive")
    if archive is None:
        return {"error": "no archive reader"}, 404
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        return {"error": "bad dataset name"}, 400
    length, error = _len_arg()
    if error:
        return error
    if not archive.refresh():
        return {"error": archive.error or "no gain solution archived yet"}, 404
    frame_id = f"{archive.path.name}@{archive.mtime:.0f}" if archive.path else None
    return _archive_data_response(archive, dataset, length, frame_id)


@bp.route("/service/eigencal/gain.h5")
@login_required
def eigencal_gain_file():
    """The newest archived solution itself."""
    archive = _component("eigencal_archive")
    raw = archive.file_bytes() if archive is not None else None
    if raw is None:
        flash(f"Gain solution unavailable: {archive.error if archive else 'no archive reader'}",
              "error")
        return redirect(url_for("web.service_page", name="eigencal"))
    resp = Response(raw, mimetype="application/x-hdf5")
    resp.headers["Content-Disposition"] = \
        f'attachment; filename="{archive.path.name}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.route("/service/eigencal/plot")
@login_required
def eigencal_gain_plot_page():
    """Full-viewport plot of one dataset of the newest solution."""
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        flash("Invalid dataset name", "error")
        return redirect(url_for("web.service_page", name="eigencal"))
    return render_template("plot.html", title=dataset, subtitle="eigencal solution",
                           source_url=request.script_root + "/api/eigencal/gain-data?dataset="
                                      + quote(dataset, safe=""),
                           source_id="eigencal-gain|" + dataset,
                           back_url=url_for("web.service_page", name="eigencal"),
                           back_label="eigencal",
                           node_key=None, poll_ms=GAIN_POLL_MS,
                           fetch_bytes=GAIN_FETCH_BYTES)


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
    """The gain archive itself, proxied so the browser needs no route to fpga_master."""
    archive = _component("gain_archive")
    if archive is None:
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
    archive = _component("gain_archive")
    if archive is None:
        abort(404)
    dataset = request.args.get("dataset", "")
    if not _GAIN_DATASET_RE.fullmatch(dataset):
        flash("Invalid dataset name", "error")
        return redirect(url_for("web.service_page", name="fpga"))
    return render_template("plot.html", title=dataset, subtitle="F-engine gains",
                           source_url=request.script_root + "/api/fpga/gain-data?dataset="
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
    archive = _component("gain_archive")
    if archive is None:
        return {"configured": False, "datasets": [], "attrs": {},
                "index_map": {}, "error": None}
    archive.refresh()          # to_dict reports the cache; this fills it
    info = archive.to_dict()
    update_time = _first_scalar(info["scalars"].get("index_map/update_time"))
    info["update_id"] = _first_scalar(info["scalars"].get("update_id"))
    info["update_fmt"] = (fmt_utc(update_time)
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


bp.add_app_template_filter(human_bytes, "filesize")
bp.add_app_template_filter(utc_minute, "utc_minute")


@bp.route("/files")
@login_required
def files_page():
    """What kotekan has written to the data roots.

    The scan itself is a *lazily loaded partial*, for the same reason
    the FPGA gain card is: walking an NFS mount takes a few hundred ms,
    and the page has nothing to wait for it for.
    """
    scan = current_app.config.get("datafile_scan")
    # The waterfall renderer lives on this page (see _service_registry),
    # so the title carries the joint DATA tag and the page ends with the
    # renderer's status, state file and journal.
    # The acquisition notes job is folded in the same way, as a second
    # section with its own status block.
    registry = _service_registry()
    return render_template(
        "files.html",
        configured=bool(scan is not None and scan.configured),
        roots=[str(r) for r in (scan.roots if scan is not None else [])],
        data=scan.to_dict() if scan is not None else None,
        name="waterfall", nlines=_journal_lines_arg(),
        notes=_service_page_context("acqnotes", registry["acqnotes"]),
        **_service_page_context("waterfall", registry["waterfall"]),
    )


@bp.route("/partials/files")
@login_required
def partial_files():
    """The scan table.  ``?refresh=1`` bypasses the cache."""
    scan = _component("datafile_scan")
    if scan is None:
        result = {"configured": False, "roots": []}
    else:
        result = scan.get(force=request.args.get("refresh") == "1")

    store = _component("waterfall_store")
    rendered = (rendered_index(store, force=request.args.get("refresh") == "1")
                if store is not None else {})
    return render_template("_files_table.html", scan=result,
                           rendered=rendered, now_ts=time.time())


#: How long a notes read may block on NFS before the popover says so.
NOTES_TIMEOUT_S = 5.0


def _scan_root(root_idx: int) -> dict:
    """The cached scan's row for root *root_idx*, else a 404.  The index
    into the configured roots is the allowlist: the caller's string never
    becomes a path."""
    scan = _component("datafile_scan")
    if scan is None:
        abort(404)
    roots = scan.get().get("roots") or []
    if not 0 <= root_idx < len(roots):
        abort(404)
    return roots[root_idx]


@bp.route("/files/notes/<int:root_idx>/<acq>")
@login_required
def files_notes(root_idx, acq):
    """One acquisition's README.md, rendered for the table's info popover.

    The root is an index into the configured roots and the acquisition
    must be a directory the scan itself listed with notes: the caller's
    string never becomes a path.  The read runs in the threadpool with a
    timeout, like every other NFS access from the web process.
    """
    row = next((d for d in _scan_root(root_idx)["dirs"]
                if d["name"] == acq and d.get("notes")), None)
    if row is None:
        abort(404)
    try:
        text = run_blocking(read_notes, row["path"], timeout=NOTES_TIMEOUT_S)
    except OSError as exc:
        return render_template("_files_notes.html", error=str(exc))
    return render_template("_files_notes.html", html=notes_html(text))


@bp.route("/files/timeline/<int:root_idx>")
@login_required
def files_timeline(root_idx):
    """A root's ``timeline.yaml`` (the acquisition history that
    the acqnotes job renders the READMEs from), as a table for the root
    header's popover.  Same allowlist and threadpool read as the notes:
    an index into the configured roots whose cached scan saw the file."""
    root = _scan_root(root_idx)
    if not root.get("timeline"):
        abort(404)
    try:
        text = run_blocking(read_notes, root["path"], TIMELINE_NAME, timeout=NOTES_TIMEOUT_S)
        rows = timeline_rows(text)
    except (OSError, ValueError) as exc:
        return render_template("_files_timeline.html", error=str(exc))
    return render_template("_files_timeline.html", rows=rows, path=root["path"])


@bp.route("/files/<root>/<acq>/triangle")
@login_required
def waterfall_triangle(root, acq):
    """The upper-triangle contact sheet for one acquisition."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    index = store.index(root, acq)
    if index is None:
        abort(404)
    grid = wf_triangle(index, parse_elements(request.args.get("elements")))
    # a selected acquisition reads as strips; the whole triangle as a triangle
    layout = request.args.get("layout")
    strips = None if layout == "triangle" else wf_strips(index)
    if layout == "strips" and strips is None:
        strips = wf_strips({**index, "n_elements": 0}) or None
    return render_template(
        "waterfall_triangle.html",
        root=root, acq=acq, index=index, grid=grid, strips=strips,
        overview=overview_of(index) is not None,
        n_files=len(index.get("files") or []),
        elements_arg=request.args.get("elements") or "",
    )


@bp.route("/files/<root>/<acq>/matrix")
@login_required
def waterfall_matrix(root, acq):
    """The element x element coherence matrix, one frame per source file
    (``waterfalls.matrix_context``)."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    context = wf_matrix_context(store, root, acq, request.args.get("band"), request.args.get("row"),
                                tz=(current_app.config.get("waterfall_cfg") or {}).get("timezone"))
    if context is None:
        abort(404)
    return render_template("waterfall_matrix.html", **context)


def _png_response(png: bytes | None) -> Response:
    if png is None:
        abort(404)
    resp = Response(png, mimetype="image/png")
    resp.cache_control.max_age = 30
    return resp


@bp.route("/waterfall/<root>/<acq>/matrix/<band>/<row>.png")
@login_required
def waterfall_matrix_image(root, acq, band, row):
    """One frame of the matrix, rendered from ``matrix.dat`` on request.
    Band and row are parsed as integers after the login check and
    bounded by the index's record; the cube is read off the hub and
    encoded with the stdlib."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    return _png_response(wf_matrix_image(store, root, acq, band, row))


@bp.route("/waterfall/<root>/<acq>/coarse/<name>")
@login_required
def waterfall_coarse_image(root, acq, name):
    """Any product of the file as a coarse waterfall, rendered from
    ``coarse.dat`` on request.  The name is matched like an image name
    and resolved to a product through the index's recorded order."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    return _png_response(wf_coarse_image(store, root, acq, name))


@bp.route("/waterfall/<root>/<acq>/coarse-view/<name>")
@login_required
def waterfall_coarse_view(root, acq, name):
    """A coarse waterfall with its axes (``waterfalls.coarse_view_context``)."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    context = wf_coarse_view_context(store, root, acq, name,
                                     tz=(current_app.config.get("waterfall_cfg") or {}).get("timezone"))
    if context is None:
        abort(404)
    return render_template("waterfall_view.html", **context)


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
    store = _component("waterfall_store")
    if store is None:
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
    """One full-resolution image with its axes drawn around it
    (``waterfalls.view_context``)."""
    store = _component("waterfall_store")
    if store is None:
        abort(404)
    context = wf_view_context(store, root, acq, name,
                              tz=(current_app.config.get("waterfall_cfg") or {}).get("timezone"))
    if context is None:
        abort(404)
    return render_template("waterfall_view.html", **context)


#: The jobs the services strip and the landing table show, in badge
#: order; ``label`` is the badge text.  ``unit`` is the service unit
#: unless ``<name>_cfg.service_unit`` says otherwise; the timer is the
#: same name with ``.timer``.  ``state``, ``run`` and ``manual`` name the
#: files read back from ``<state_dir>/<name>/``, the layout systemd's
#: ``StateDirectory=choco/<name>`` gives the jobs: conventions, not
#: settings.  ``mtime_label`` says what the state file's mtime means on
#: the page; only a job that rewrites its state file on every successful
#: run gets a ``stale_after_s``.  ``page`` is where a folded job is
#: shown (see ``_service_registry``) and ``card`` a partial that page
#: loads above the job's status block; ``description`` heads the page,
#: or the folded job's section.
_JOBS: dict[str, dict] = {
    # EOP rewrites its state file on every successful (daily) run, so
    # the mtime doubles as "last successful run" and goes stale.
    "eop": {
        "label": "EOP", "unit": "choco-eop-broadcast.service",
        "state": STATE_NAME, "stale_after_s": EOP_STALE_AFTER_S, "mtime_label": "last run",
        "description": "Earth-orientation-parameter broadcast — merges IERS data and "
                       "pushes the EOP table to kotekan daily.",
    },
    # bffs rewrites its state file only when the bad-feed list changes,
    # so no staleness threshold: the mtime is "last change".  It also
    # rewrites a run file on *every* run -- what each source measured,
    # what it abstained from and why the exit status was what it was --
    # and its ``manual`` source reads the override file the element grid
    # toggles flags in (``bffs.control: false`` makes the grid
    # read-only, the switch the PDB page has).
    "bffs": {
        "label": "BFFS", "unit": "choco-bffs-flag.service",
        "state": STATE_NAME, "run": RUN_NAME, "manual": MANUAL_OVERRIDES_NAME,
        "mtime_label": "last change",
        "description": "Feed flagging — combines per-feed health sources and pushes the "
                       "bad-input list to kotekan on change.",
    },
    # eigencal rewrites its state file once per processed transit;
    # transits skipped for daytime are silent by design, so an old
    # mtime is informational, not a health downgrade.
    "eigencal": {
        "label": "EIGENCAL", "unit": "choco-eigencal.service",
        "state": STATE_NAME, "run": RUN_NAME, "mtime_label": "last calibration",
        "description": "Point-source gain calibration — fits per-input complex gains once "
                       "per calibrator transit, at night.",
    },
    # waterfall rewrites its state file on every run, but a run with
    # nothing to render is the normal case between acquisitions, so the
    # mtime is "last run" and never a health downgrade.  The renderer
    # reads the mounts the DATA badge probes, so the two share one badge
    # and one page: /files shows the mounts' scan with the renderer's
    # status below it.
    "waterfall": {
        "label": "WF", "unit": "choco-waterfall.service",
        "state": STATE_NAME, "mtime_label": "last run", "page": "/files",
    },
    # skymap keeps no state file: the image is the record and its title
    # carries the render time, so the badge is systemd only.  The strip
    # plot ends the MAP page, so the job is folded into MAP the way the
    # waterfall is into DATA.
    "skymap": {
        "label": "SKYMAP", "unit": "choco-skymap.service", "mtime_label": "last render",
        "page": "/service/mapmaker", "card": "/partials/skymap",
        "description": Markup(
            "Current-sky strip plot — renders the beam's drift-scan strip with the Sun, "
            "Moon and pointing every 5 minutes, in a day and a night palette; "
            '<a href="{root}/">also on the landing page</a>.'),
    },
    # mapmaker rewrites its state file on every run; a run with nothing
    # new to fold in is the normal case between files, so the mtime is
    # "last run" and never a health downgrade.
    "mapmaker": {
        "label": "MAP", "unit": "choco-mapmaker.service",
        "state": STATE_NAME, "mtime_label": "last run",
        "description": Markup(
            "Drift-scan sky maps — folds each new N&sup2; file into a map of every pass, "
            "shown deconvolved in context on the 408 MHz sky, and a map of the last "
            "sidereal day, frequency as hue from red to violet, calibrated on the "
            'brightest source transiting the beam. <a href="{root}/files">The data it reads</a>.'),
    },
    # acqnotes rewrites its index only when it measures an acquisition,
    # so the mtime is "last measurement"; between acquisitions an hourly
    # run that finds nothing new is normal.  It writes the notes /files
    # shows, so it is folded into DATA like the waterfall.
    "acqnotes": {
        "label": "NOTES", "unit": "choco-acqnotes.service",
        "state": "index.json", "run": RUN_NAME, "mtime_label": "last measurement",
        "page": "/files",
    },
}


def _service_registry() -> dict[str, dict]:
    """Everything the services strip and /service/<name> pages know:
    choco itself, then ``_JOBS`` with their files placed under the state
    directory and their units as configured.  Keyed by page slug;
    ``unit`` doubles as the allowlist for the journal viewer: only these
    units can be read through the web UI.

    ``page`` says where a job's status is shown.  None: its own
    /service/<name> page, a badge in the strip and a row in the landing
    table.  A path: the job is folded into that page's badge and row
    (the waterfall into DATA, the skymap into MAP) and /service/<name>
    redirects there; it keeps its entry for the journal viewer, the
    status partial, /api/status and /metrics, which report it on its own.
    """
    root = _state_root()
    registry = {"choco": {"kind": "choco", "label": "CHOCO", "unit": "choco.service",
                          "timer": None, "state_file": None, "stale_after_s": None,
                          "mtime_label": None, "run_file": None, "manual_file": None,
                          "page": None, "description": "The config orchestrator itself."}}
    for name, job in _JOBS.items():
        cfg = current_app.config.get(f"{name}_cfg") or {}
        unit = cfg.get("service_unit") or job["unit"]
        files = {key: root / name / job[key] for key in ("state", "run", "manual") if key in job}
        registry[name] = {
            "kind": "job",
            "label": job["label"],
            "unit": unit,
            "timer": unit.replace(".service", ".timer"),
            "state_file": files.get("state"),
            "stale_after_s": job.get("stale_after_s"),
            "mtime_label": job["mtime_label"],
            "run_file": files.get("run"),
            "manual_file": files.get("manual") if cfg.get("control", True) else None,
            "page": job.get("page"),
            "card": job.get("card"),
            "description": job.get("description"),
        }
    return registry


def _folded(registry: dict) -> dict[str, list[str]]:
    """The jobs shown on another job's page, by host slug: every entry
    whose ``page`` is ``/service/<host>`` (the skymap on the MAP page).
    Its state rides in the host's badge and landing row, the way the
    waterfall's rides in DATA, and its status block ends the host's page."""
    folded: dict[str, list[str]] = {}
    for name, svc in registry.items():
        host = (svc.get("page") or "").removeprefix("/service/")
        if svc.get("page") and host in registry:
            folded.setdefault(host, []).append(name)
    return folded

def _services_health(registry: dict | None = None) -> dict:
    """Health snapshots for the hardware monitors and the oneshot jobs.

    Shared by the header strip, the landing table, /api/status and
    /metrics.  *registry* is ``_service_registry()``, passed in when the
    caller already has one.
    """
    fpga = current_app.config.get("fpga_monitor")
    pdb = current_app.config.get("pdb_monitor")
    data = current_app.config.get("datafile_scan")
    health = {
        "fpga": fpga.to_dict() if fpga is not None else None,
        "pdb": pdb.to_dict() if pdb is not None else None,
        "data": data.to_dict() if data is not None else None,
    }
    for name, svc in (registry or _service_registry()).items():
        if svc["kind"] == "job":
            # a copy: the reasons below belong to this job alone
            health[name] = dict(job_status(svc["unit"], state_file=svc["state_file"],
                                           stale_after_s=svc["stale_after_s"]))
            if svc.get("run_file") and health[name]["health"] in ("degraded", "failed"):
                health[name]["reasons"] = _run_reasons(svc["run_file"])
    return health


def _run_reasons(run_file) -> list[str]:
    """Why a job's last run was not plain ok, in the job's own words.

    Read from the job's per-run file (``degraded`` reasons, ``error``,
    and the one-line ``reason`` eigencal gives), so a yellow badge can say
    *which* input was unavailable instead of just "degraded".  A missing
    or malformed file contributes nothing; the verdict itself stays
    systemd's.
    """
    run = read_state_json(run_file)
    if not isinstance(run, dict):
        return []
    reasons = run.get("degraded")
    out = [str(r) for r in reasons] if isinstance(reasons, list) else []
    out += [str(run[k]) for k in ("error", "reason") if run.get(k)]
    return out[:5]


def _node_counts(registry) -> dict:
    """Every roll-up's node numbers: the count per runtime status, the
    total, and how many are desired started / in maintenance."""
    counts = {s.value: 0 for s in NodeStatus}
    for node in registry.nodes.values():
        counts[node.status.value] += 1
    return {
        "total": len(registry.nodes),
        "started_desired": sum(1 for n in registry.nodes.values() if n.started),
        "maintenance": sum(1 for n in registry.nodes.values() if n.maintenance),
        **counts,
    }


def _nodes_health() -> dict:
    """Cluster-level roll-up of per-node status for the NODES badge.

    Green when every node is STARTED, red when every node is DOWN, grey
    when there are no nodes or nothing has been polled yet (all
    UNKNOWN); anything in between — some up, all idle, a mix — is
    yellow.  ``node.status`` is a plain attribute the sync loop keeps
    fresh, so this is an in-memory sweep, not a probe.
    """
    counts = _node_counts(_registry())
    total, started, idle, down = counts["total"], counts["started"], counts["idle"], counts["down"]
    if total == 0:
        health, label = "unknown", "no nodes"
    elif started == total:
        health, label = "ok", "all up"
    elif down == total:
        health, label = "down", "all down"
    elif counts["unknown"] == total:
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
        "unknown": counts["unknown"],
        "syncing": counts["syncing"],
        "maintenance": counts["maintenance"],
    }


@bp.route("/partials/services")
@login_required
def partial_services():
    """Render the NODES + monitor (FPGA, PDB, DATA) + job (EOP, bffs,
    eigencal, MAP) strip.  DATA covers the mounts and the jobs that read
    them; MAP carries the skymap (``_folded``)."""
    registry = _service_registry()
    services = _services_health(registry)
    return render_template(
        "_services_status.html",
        nodes=_nodes_health(),
        fpga=services["fpga"],
        pdb=services["pdb"],
        data=services["data"],
        services=services,
        svc=registry,
        folds={host: [[n, services[n]] for n in names]
               for host, names in _folded(registry).items()},
        now_ts=time.time(),
    )


def _journal_lines_arg() -> int:
    """How many journal lines a page asked for (``?lines=``), 10 to 1000."""
    try:
        nlines = int(request.args.get("lines", 100))
    except (TypeError, ValueError):
        nlines = 100
    return max(10, min(nlines, 1000))


@bp.route("/partials/service-logs/<name>")
@login_required
def partial_service_logs(name):
    """Recent journal lines for one of the known service units."""
    svc = _service_registry().get(name)
    if svc is None:
        abort(404)
    unit = svc["unit"]
    nlines = _journal_lines_arg()
    lines = job_logs(unit, lines=nlines)
    return render_template(
        "_service_logs.html",
        name=name, unit=unit, lines=lines, nlines=nlines, now_ts=time.time(),
    )


def _choco_detail(state, run, svc) -> dict:
    """choco's own page: the live registry, no state file."""
    registry = _registry()
    counts = _node_counts(registry)
    return {
        "started_at": fmt_utc(_STARTED_AT),
        "nodes_total": counts["total"],
        "maintenance": counts["maintenance"],
        "started_desired": counts["started_desired"],
        "queued": sum(n.queue_depth for n in registry.nodes.values()),
        "configs_dir": str(current_app.config.get("configs_dir")),
    }


#: Each service's summariser (``jobdetail.JOB_DETAILS`` plus choco's own).
_SERVICE_DETAILS = {"choco": _choco_detail, **JOB_DETAILS}


def _service_detail(name: str, svc: dict) -> dict | None:
    """Per-service extras for the /service/<name> page.

    Jobs are summarized from their JSON state file (missing or invalid
    state -> None, and the page shows only the common facts); choco is
    summarized from the live registry.  A state file with unexpected
    contents must degrade to "no summary", never break the page.
    """
    summarize = _SERVICE_DETAILS.get(name)
    if summarize is None:
        return None
    try:
        return summarize(read_state_json(svc["state_file"]), read_state_json(svc["run_file"]), svc)
    except (TypeError, ValueError, KeyError, AttributeError,
            IndexError) as e:
        logger.warning(f"service detail for {name}: "
                       f"unusable state file contents: {e}")
        return None


@bp.route("/service/<name>")
@login_required
def service_page(name):
    """Detail page for one service badge: health, schedule, state, journal."""
    if name == "fpga":
        monitor = current_app.config.get("fpga_monitor")
        if monitor is None:
            abort(404)
        fpga_cfg = current_app.config.get("fpga_cfg") or {}
        return render_template("service_fpga.html", control=bool(fpga_cfg.get("control", True)),
                               **_fpga_context(monitor))
    if name == "pdb":
        if current_app.config.get("pdb_monitor") is None:
            abort(404)
        context = _pdb_context()
        return render_template(
            "service_pdb.html", **context,
            check=_pdb_cross_check(context["pdb_map"]),
        )
    registry = _service_registry()
    svc = registry.get(name)
    if svc is None:
        abort(404)
    if svc.get("page"):
        # Folded into another page's badge (the waterfall into DATA, the
        # skymap into MAP): old links land there, ``?lines=`` and all.
        query = request.query_string.decode()
        return redirect(svc["page"] + ("?" + query if query else ""))
    # The jobs folded into this page end it, each with its own status
    # block and journal (service.html).
    folded = {other: _service_page_context(other, registry[other])
              for other in _folded(registry).get(name, [])}
    return render_template(
        "service.html", name=name, nlines=_journal_lines_arg(), folded=folded,
        **_service_page_context(name, svc))


def _service_page_context(name: str, svc: dict) -> dict:
    """What a job's page renders: live facts, timer, state-file summary
    and the pretty-printed raw state file.  Shared by /service/<name>
    and the page a folded job is shown on (the waterfall on /files)."""
    job = job_status(svc["unit"], state_file=svc["state_file"],
                     stale_after_s=svc["stale_after_s"])
    timer = timer_status(svc["timer"]) if svc["timer"] else None
    # Pretty-printed raw state file, shown verbatim in a collapsed
    # <details> below the curated summary. Values already came from
    # json.load, so re-serializing them can't raise.
    state = read_state_json(svc["state_file"])
    state_json = json.dumps(state, indent=2) if state is not None else None
    return {
        "svc": svc, "job": job, "timer": timer,
        "detail": _service_detail(name, svc),
        "state_json": state_json,
        "now_ts": time.time(),
    }


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
    return render_template("_service_fpga_status.html", **_fpga_context(monitor))


def _fpga_context(monitor) -> dict:
    """What the FPGA page and its status poll render from the monitor."""
    fpga = monitor.to_dict()
    return {"fpga": fpga, "frame0_fmt": fmt_utc((fpga["frame0_ns"] or 0) / 1e9),
            "actions": monitor.actions, "now_ts": time.time()}


def _pdb_layout(present: set[int]) -> list[dict]:
    """The physical frame, as rows of board slots for the grid.

    Sixteen boards sit in two rows of eight.  The top row holds boards
    15 down to 8 and is mounted the other way up, so it reads chip B
    then A left to right with channel 0 at the top; the bottom row holds
    boards 0 to 7, chip A then B, channel 7 at the top.  A row is drawn
    only if one of its boards reported in, and a slot whose board did
    not is drawn empty so the columns stay where the hardware is.
    Boards above 15 (no such frame today) go in extra rows of eight in
    the bottom row's orientation rather than being dropped.
    """
    rows = []
    top = list(range(15, 7, -1))
    bottom = list(range(0, 8))
    if any(b in present for b in top):
        rows.append({"boards": top, "chips": ["B", "A"],
                     "channels": list(range(8))})
    if any(b in present for b in bottom):
        rows.append({"boards": bottom, "chips": ["A", "B"],
                     "channels": list(range(7, -1, -1))})
    extra = sorted(b for b in present if b > 15)
    for i in range(0, len(extra), 8):
        rows.append({"boards": extra[i:i + 8], "chips": ["A", "B"],
                     "channels": list(range(7, -1, -1))})
    return rows


def _pdb_buses(monitor) -> list[dict]:
    """Per-bus grid data: boards, their chips, the frame layout and counts.

    The monitor stores one flat row per chip in daisy-chain order; the
    grid wants them by board and chip (``by_board[board][chip]``) laid
    out as the frame is mounted (``layout``, see ``_pdb_layout``), and
    the bus header wants the counts.  Shaped here rather than in Jinja
    so the template stays a table and not arithmetic.
    """
    buses = []
    for bus, rows in sorted(monitor.channels.items()):
        boards: list[dict] = []
        by_board: dict[int, dict[str, dict]] = {}
        for row in rows:
            if not boards or boards[-1]["board"] != row["board"]:
                boards.append({"board": row["board"], "chips": []})
            boards[-1]["chips"].append(row)
            by_board.setdefault(row["board"], {})[row["chip"]] = row
        buses.append({
            "bus": bus,
            "boards": boards,
            "by_board": by_board,
            "layout": _pdb_layout(set(by_board)),
            # /status knows the configured board count; fall back to
            # what actually came back on the wire.
            "n_boards": monitor.boards.get(bus, len(boards)),
            "n_channels": sum(len(r["channels"]) for r in rows),
            "n_on": sum(1 for r in rows for c in r["channels"] if c),
        })
    return buses


#: The PDB page's two grids: the channels as they come through the
#: bulkhead into the computing chamber (boards, chips, channels; see
#: _pdb_layout) and as the dishes stand in the field (rows A..H of eight
#: dishes, X and Y per dish, the RFI antennas a row of their own; see
#: pdbmap.dish_layout).  Chosen with ?layout=, carried on the poll and
#: on every control post by hx-vals on #pdb-status.
PDB_LAYOUTS = ("bulkhead", "dish")


def _pdb_layout_name() -> str:
    """The requested grid layout, defaulting to bulkhead for anything
    not in PDB_LAYOUTS -- the name only picks a template branch."""
    name = request.values.get("layout", "")
    return name if name in PDB_LAYOUTS else PDB_LAYOUTS[0]


def _pdb_dish_grid(pdb_map, buses: list[dict]) -> dict:
    """The field layout (pdbmap.dish_layout) joined with the live states.

    Each pol becomes ``{"pol", "label", "bus", "board", "chip",
    "channel", "on"}`` with ``on`` None when the controller did not
    report that board; each row gets per-pol counts for its power
    buttons.
    """
    by_bus = {b["bus"]: b["by_board"] for b in buses}

    def state(entry):
        chips = by_bus.get(entry.bus, {}).get(entry.board)
        row = chips.get(entry.chip) if chips else None
        return row["channels"][entry.channel] if row else None

    layout = dish_layout(pdb_map)
    rows = []
    n_total = n_on = 0
    for r in layout["rows"]:
        counts = {}
        cells = []
        for cell in r["cells"]:
            if cell is None:
                cells.append(None)
                continue
            pols = []
            for pol, entry in cell["pols"].items():
                if entry is None:
                    pols.append({"pol": pol, "label": None, "on": None})
                    continue
                on = state(entry)
                pols.append({"pol": pol, "label": entry.dish_input,
                             "bus": entry.bus, "board": entry.board,
                             "chip": entry.chip, "channel": entry.channel,
                             "on": on})
                c = counts.setdefault(pol, {"n": 0, "n_on": 0})
                c["n"] += 1
                n_total += 1
                if on:
                    c["n_on"] += 1
                    n_on += 1
            cells.append({"name": cell["name"], "pols": pols})
        rows.append({"name": r["name"], "cells": cells, "counts": counts})
    return {"cols": layout["cols"], "rows": rows,
            "n_channels": n_total, "n_on": n_on}


def _pdb_context() -> dict:
    """Template variables shared by the PDB page, poll, and control replies.

    The grid, its labels, and the bulk controls all render from the same
    dict, so a control reply can hand back exactly what the poll would.
    """
    monitor = current_app.config["pdb_monitor"]
    pdb_cfg = current_app.config.get("pdb_cfg") or {}
    # .get() re-reads the CSV if its mtime changed, so edits show up on
    # the next render without a restart.
    pdb_map = current_app.config["pdb_map"].get()
    buses = _pdb_buses(monitor)
    layout = _pdb_layout_name()
    return {
        "pdb": monitor.to_dict(),
        "buses": buses,
        "pdb_map": pdb_map,
        "layout": layout,
        "dish_grid": _pdb_dish_grid(pdb_map, buses) if layout == "dish" else None,
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
        result["reason"] = sample.config_problem
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
    return render_template("_service_status.html", name=name,
                           **_service_page_context(name, svc))


def _bffs_manual_result(ok: bool, message: str):
    """Reply to a manual-flag toggle, the same two ways as a PDB write:
    over htmx the status block is swapped in place with the outcome as an
    out-of-band notice; a plain form POST gets flash-and-redirect."""
    if request.headers.get("HX-Request"):
        svc = _service_registry()["bffs"]
        return render_template("_service_bffs_result.html", ok=ok, message=message,
                               name="bffs", **_service_page_context("bffs", svc))
    flash(f"BFFS: {message}", "success" if ok else "error")
    return redirect(url_for("web.service_page", name="bffs"))


@bp.route("/service/bffs/manual", methods=["POST"])
@login_required
def bffs_manual_toggle():
    """Add or remove one element's manual flag from the BFFS element grid.

    Writes ``manual_overrides.yaml`` in the job's state directory, the
    file its ``manual`` source reads by default; the job applies it on
    its next run.  The
    label is accepted only if it is on the element axis the state file
    records — the allowlist for this write — and the file is written
    only if the run file says the job's manual source reads that same
    file (or there is no run file to check against).  Login + CSRF, and
    one audit line naming the operator, like the PDB controls.
    """
    _check_csrf()
    svc = _service_registry()["bffs"]
    manual_file = svc.get("manual_file")
    if manual_file is None:
        abort(403)
    label = request.form.get("label", "")
    state = read_state_json(svc.get("state_file")) or {}
    axis = state.get("labels")
    if (not label or len(label) > 64 or not isinstance(axis, list)
            or label not in [str(x) for x in axis]):
        abort(400)
    run = read_state_json(svc.get("run_file"))
    job_path = job_manual_path(run)
    if job_path == "":
        return _bffs_manual_result(
            False, "the bffs job runs with no manual source; a flag written "
                   "here would never be read")
    if job_path is not None and not same_path(job_path, manual_file):
        return _bffs_manual_result(
            False, f"choco edits {manual_file} but the bffs job's manual source "
                   f"reads {job_path}; drop the source's path: (or point it at "
                   f"the state directory) so the two agree")
    try:
        flagged = toggle_manual_flag(manual_file, label)
    except ValueError as e:
        return _bffs_manual_result(False, f"manual file not rewritten: {e}")
    except OSError as e:
        return _bffs_manual_result(False, f"could not write {manual_file}: {e}")
    logger.warning(
        f"bffs manual flag: {label} -> {'bad' if flagged else 'good'} "
        f"requested by {_audit_user()} ({manual_file})")
    return _bffs_manual_result(
        True, f"{label} {'flagged' if flagged else 'unflagged'} manually; "
              f"the job applies it on its next run")


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
    user = _audit_user()
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
    if not valid_channel_address(board, chip, channel):
        abort(400)
    logger.warning(
        f"pdb: bus {bus} board {board} chip {chip} ch{channel} -> "
        f"{'on' if on else 'off'} requested by "
        f"{_audit_user()}")
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
        f"{_audit_user()}")
    ok, message = monitor.set_group(bus, on, board=board, chip=chip)
    return _pdb_result(ok, message)


@bp.route("/service/pdb/set-row", methods=["POST"])
@login_required
def pdb_control_row():
    """Power one polarization of a whole row of dishes on or off.

    The form names a row of the field layout ("A".."H", "RFI") and a
    pol; both become channel addresses only through the channel map
    (``pdbmap.row_entries``), so nothing the browser sent reaches the
    controller.  Same audit line as the other bulk writes.
    """
    _check_csrf()
    monitor = _pdb_control_monitor()
    form = request.form
    row = form.get("row", "")
    pol = form.get("pol", "")
    try:
        on = form["state"] == "on"
    except KeyError:
        abort(400)
    if pol not in ("X", "Y"):
        abort(400)
    pdb_map = current_app.config["pdb_map"].get()
    entries = row_entries(dish_layout(pdb_map), row, pol)
    if entries is None:
        abort(400)
    scope = f"row {row} pol {pol}"
    if not entries:
        return _pdb_result(False, f"{scope}: no channels in the map")
    logger.warning(
        f"pdb: {scope} -> all {'on' if on else 'off'} "
        f"({len(entries)} channels) requested by "
        f"{_audit_user()}")
    ok, message = monitor.set_channels([e.address for e in entries], on, scope)
    return _pdb_result(ok, message)


# --- JSON API endpoints for queue-based updates ---

def _apply_update(nodes: list, data: dict, scope: dict) -> tuple[dict, int]:
    """One ``/update`` body applied to *nodes* (a group or a single node).

    ``base_config`` and ``updatable_config`` queue one :class:`ChangeItem`
    on every node; ``set_started`` / ``set_maintenance`` set the flag and
    wake the workers exactly as the dashboard toggles do; ``set_config``
    rewrites nodes.yaml and rebuilds the registry.  *scope* is echoed in
    the reply (``{"group": g}`` or ``{"node": key}``).
    """
    action = data.get("action", "")
    if action == "base_config":
        content = data.get("config_content", "")
        try:
            nodes[0].render(content)  # template vars are shared
        except Exception as e:
            return {"error": f"Invalid config: {e}"}, 400
        item = ChangeItem(ChangeType.BASE_CONFIG, config_content=content)
    elif action == "updatable_config":
        endpoint = data.get("endpoint", "")
        values = data.get("values")
        if not endpoint or values is None:
            return {"error": "endpoint and values are required"}, 400
        item = ChangeItem(ChangeType.UPDATABLE_CONFIG,
                          endpoint=endpoint, values=values)
    elif action in ("set_started", "set_maintenance"):
        attr = action.removeprefix("set_")
        value = data.get(attr)
        if not isinstance(value, bool):
            return {"error": f"{attr} must be a boolean"}, 400
        _orchestrator().set_flag(nodes, attr, value)
        return {"status": "ok", **scope, attr: value}, 200
    elif action == "set_config":
        # Which library file the nodes render (null: their per-node
        # files).  Not a queue item: a nodes.yaml edit and registry
        # rebuild, which pauses the cluster (see library.set_node_configs).
        config = data.get("config")
        if config is not None and not isinstance(config, str):
            return {"error": "config must be a string or null"}, 400
        body, status = library.set_node_configs(_registry(), _orchestrator(), nodes,
                                                config or None, _audit_user())
        return (body | scope if status == 200 else body), status
    else:
        return {"error": f"Unknown action '{action}'"}, 400
    orchestrator = _orchestrator()
    for node in nodes:
        orchestrator.submit_node(node.key, item)
    return {"status": "queued", **scope, "action": action}, 200


def _targets(group: str, node: str | None):
    """The nodes a group or node route addresses and the scope its reply
    names, or the error reply."""
    registry = _registry()
    if node is None:
        nodes = registry.in_group(group)
        if not nodes:
            return None, None, ({"error": f"Group '{group}' not found"}, 404)
        return nodes, {"group": group}, None
    key = f"{group}/{node}"
    obj = registry.get_node(key)
    if obj is None:
        return None, None, ({"error": f"Node '{key}' not found"}, 404)
    return [obj], {"node": key}, None


@bp.route("/update/<group>", methods=["POST"])
@bp.route("/update/<group>/<node>", methods=["POST"])
@localhost_or_login_required
def update(group, node=None):
    """Queue a config change for every node in a group, or for one node."""
    nodes, scope, error = _targets(group, node)
    if error:
        return error
    return _apply_update(nodes, request.get_json(silent=True) or {}, scope)


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
        orchestrator.submit_node(key, ChangeItem(ChangeType.POLL))

    body = {"started": started, "skipped": skipped, "sha256": digest}
    return body, (200 if started else 409)


@bp.route("/oneshot/<group>", methods=["POST"])
@bp.route("/oneshot/<group>/<node>", methods=["POST"])
@localhost_or_login_required
def oneshot(group, node=None):
    """Start a config on a group's paused, idle nodes, or on one such
    node, without recording it."""
    nodes, _scope, error = _targets(group, node)
    if error:
        return error
    data = request.get_json(silent=True) or {}
    return _run_oneshot(nodes, data.get("config_content", ""), _audit_user())


# --- JSON API endpoints for read-only status (localhost bypass) ---

def _node_to_dict(node) -> dict:
    return {
        "key": node.key,
        "group": node.group,
        "name": node.name,
        "host": node.host,
        "port": node.port,
        "config": node.config_filename,
        "started": node.started,
        "maintenance": node.maintenance,
        "status": node.status.value,
        "last_seen": node.last_seen,
        "last_seen_ago": node.last_seen_ago,
        "version": node.version,
        "version_info": node.version_info,
        "error": node.error,
        "queue_depth": node.queue_depth,
    }


def _status_summary() -> dict:
    """Aggregate choco health: services plus node counts.

    Shared by /api/status (JSON) and /metrics (Prometheus text).
    """
    services = _services_health()
    return {
        "up": True,
        "started_at": _STARTED_AT,
        "services": {
            name: (health or {}).get("health", "unknown")
            for name, health in services.items()
        },
        "nodes": _node_counts(_registry()),
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


def _gauge(name: str, help_text: str, value=None) -> list[str]:
    """One Prometheus gauge's lines: HELP, TYPE and, given, its value
    (a labelled series appends its own lines)."""
    lines = [f"# HELP {name} {help_text}", f"# TYPE {name} gauge"]
    return lines + ([f"{name} {value}"] if value is not None else [])


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
        *_gauge("choco_up", "1 while choco is serving requests.", 1),
        *_gauge("choco_start_time_seconds", "Unix time the choco process started "
                "(a restart re-engages cluster-wide maintenance mode).", f"{_STARTED_AT:.3f}"),
        *_gauge("choco_service_state", "Service health, one-hot per state."),
    ]
    for name, health in summary["services"].items():
        states = _MONITOR_STATES.get(name, _JOB_STATES)
        for state in states:
            value = 1 if health == state else 0
            lines.append(
                f'choco_service_state{{service="{name}",state="{state}"}} '
                f"{value}")
    lines += _gauge("choco_nodes", "Node count by kotekan runtime status.")
    for status in NodeStatus:
        lines.append(
            f'choco_nodes{{status="{status.value}"}} {nodes[status.value]}')
    lines += [
        *_gauge("choco_nodes_total", "Number of registered nodes.", nodes["total"]),
        *_gauge("choco_nodes_started_desired", "Nodes whose desired state is started.",
                nodes["started_desired"]),
        *_gauge("choco_nodes_maintenance", "Nodes currently in maintenance mode.",
                nodes["maintenance"]),
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
    return {"summary": _node_counts(registry), "nodes": nodes}


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
            "config": node.config_filename,
            "started": node.started,
            # the sync loop's last probe, so a job can tell a node that
            # is down from one worth polling (bffs's rfi source)
            "status": node.status.value,
        })
    return {"groups": groups}


@bp.route("/api/config/<group>", methods=["GET"])
@bp.route("/api/config/<group>/<node>", methods=["GET"])
@localhost_or_login_required
def api_config(group, node=None):
    """A node's desired kotekan config, as JSON: what its worker would
    push.  For a group, its first member's: group configs are broadcast
    together, so any member represents the group, and jobs use this to
    learn the element layout (``dish_inputs``) that kotekan's bad-input
    mask is indexed against, without touching data files.  The check
    that moving a node onto a library file changed nothing is a diff of
    this before and after."""
    nodes, _scope, error = _targets(group, node)
    if error:
        return error
    sample = nodes[0]
    if sample.desired_config is None:
        return {"error": sample.config_problem}, 503
    return sample.desired_config


@bp.route("/api/configs", methods=["GET"])
@localhost_or_login_required
def api_configs():
    """The config library: every file under configs_dir with the nodes
    that render or include it (what /configs shows)."""
    registry = _registry()
    return {"files": [library.entry(registry, rel)
                      for rel in registry.config_files()]}


@bp.route("/api/configs/<path:name>", methods=["GET"])
@localhost_or_login_required
def api_config_get(name):
    """One library file's text, with its users."""
    registry = _registry()
    try:
        path = library.file_path(registry, name)
    except ValueError as e:
        return {"error": str(e)}, 400
    if not path.is_file():
        return {"error": f"No such config file: {name}"}, 404
    return library.entry(registry, name) | {"content": path.read_text()}


@bp.route("/api/configs/<path:name>", methods=["PUT"])
@localhost_or_login_required
def api_config_put(name):
    """Write a library file (``{"content": ...}``), creating it if
    needed, after the check the editor makes: every node that uses the
    file must still render.  This is how a config reaches the configs
    directory without a root shell (``choco config put``)."""
    registry = _registry()
    try:
        library.file_path(registry, name)
    except ValueError as e:
        return {"error": str(e)}, 400
    data = request.get_json(silent=True) or {}
    content = data.get("content")
    if not isinstance(content, str):
        return {"error": "'content' must be a string"}, 400
    affected, error = library.check_save(registry, name, content)
    if error:
        return {"error": error}, 400
    created = not (registry.configs_dir / name).exists()
    library.write_file(registry, _orchestrator(), name, content, _audit_user())
    return {"status": "saved", "path": name, "created": created,
            "reloaded": affected}


@bp.route("/api/configs/pull", methods=["POST"])
@localhost_or_login_required
def api_configs_pull():
    """``choco config pull [-n]``: mirror the upstream config directory
    into the library and report what changed as JSON; ``{"dry_run":
    true}`` reports the plan and writes nothing."""
    data = request.get_json(silent=True) or {}
    return _library_pull(dry_run=bool(data.get("dry_run")))


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
    scan = _component("datafile_scan")
    if scan is None:
        return {"configured": False, "roots": []}
    return scan.get(force=request.args.get("refresh") == "1")
