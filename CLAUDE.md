# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**choco** (CHORD Config Orchestrator) manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes for the CHORD telescope. It replaces the older **coco** system (reference code in `coco/`) with a simpler, more maintainable architecture.

## Architecture

Single-process Flask app with three concerns:
- **Flask** — web UI with live updates (htmx polling of partial templates; no WebSockets), LDAP auth, JSON API, served by gevent's WSGIServer
- **Queue-based sync system** — a serialized submit path (one lock on the Orchestrator) fans changes out to per-node FIFO queues, processed by a worker pool; workers apply file changes then sync to remote (full restart for base config changes, endpoint POSTs for updatable-only changes)
- **Config directory** — YAML files on disk as source of truth for desired state

Kotekan is deployed and managed on nodes by Ansible. choco only handles monitoring and config management via kotekan's own REST API (port 12048): `/status`, `/config`, `/start`, `/kill`, updatable config endpoints.

## Build & Run

The `choco.sh` script wraps common commands. **Keep this script up to date** when install steps, dependencies, or run commands change.

```bash
./choco.sh install   # create venv, install deps, copy config template (--overwrite-configs / --keep-configs)
./choco.sh run       # start choco (extra args forwarded, e.g. ./choco.sh run /path/to/config.yaml)
./choco.sh test      # run tests (extra args forwarded to pytest)
```

Configuration is in `config.yaml` (gitignored; copy from `config.yaml.template`). Sections: `server`, `configs_dir`, `ldap`. See template for all options.

## Code Structure

```
choco/
├── app.py          # Flask app factory, gevent WSGI server, SSL, entry point
├── web.py          # Flask routes (blueprint): dashboard, node edit, /update/* JSON API
├── state.py        # Node (identity, config state, change queue, kotekan REST client), Registry
├── sync.py         # Queue-based sync: ChangeItem, Orchestrator (serialized submit + worker pool)
├── fpga.py         # FpgaMonitor (background poll) + job_status (systemd/mtime job health)
├── auth.py         # LDAP auth (Flask-Login + Flask-LDAP3-Login, in-memory user store)
├── templates/      # Jinja2 templates (base, dashboard, edit, login)
└── static/         # Vendored assets: pico.min.css, htmx.min.js, idiomorph-ext.min.js, Sortable.min.js
jobs/
├── choco.service               # Main systemd service (Type=notify)
├── choco-eop-broadcast.service # EOP update job (runs on choco start + daily timer)
├── choco-eop-broadcast.timer   # Daily at 12:00 UTC
├── eop-broadcast.sh            # Wrapper: finds venv, calls eop_update.py
├── eop_update.py               # EOP pipeline: generate table, merge with state, push to choco
├── eop_utils.py                # Vendored from kotekan (do not modify — update from upstream)
├── choco-bffs-flag.service     # bffs bad-feed flag job (runs on choco start + 30 s timer)
├── choco-bffs-flag.timer       # Every 30 s
└── bffs-flag.sh                # Wrapper: finds venv, calls bffs/bffs.py
bffs/               # Feed-flagging script run by the bffs job (see bffs/README.md)
configs/
├── nodes.yaml      # Node registry: groups → nodes → {host, port, started}
├── vars.yaml       # (optional) Shared Jinja2 template variables
├── <group>/
│   └── <node>.yaml # Base kotekan config per node (or .j2 for Jinja2 templates)
tests/
├── test_kotekan.py # Mock-based tests for kotekan REST client (Node class)
├── test_state.py   # Tests for registry, node config/updatable state
├── test_sync.py    # Tests for queue system and sync logic
└── test_auth.py    # Tests for auth (redirects, login flow, session)
```

## Key Design Decisions

- **No database** — runtime state is ephemeral (rebuilt from polling); desired config lives in YAML files that can be edited locally or via the web UI
- **Started/stopped toggle** — each node has a ``started`` boolean representing the desired runtime state. ``nodes.yaml``'s ``started`` field is read as the pre-discovery default, but at startup ``Orchestrator.discover_node_states()`` probes every node in parallel and overwrites ``node.started`` from the actual runtime state (STARTED → ``True``; IDLE / DOWN / UNKNOWN / unreachable → ``False``, i.e. idle is the default whenever the probe doesn't clearly say "running"). The same discovery pass also runs from ``Orchestrator.apply_nodes_update`` after every ``nodes.yaml`` save / file-watcher reload, so registry rebuilds preserve runtime state in the same way startup does. When ``started=False``, the sync loop will kill kotekan if found running and will never start it or push updatable configs (subject to the maintenance gate below). Config changes are still tracked on disk. Runtime toggles are ephemeral (lost on choco restart; the next startup re-discovers).
- **Maintenance mode** — each node has a ``maintenance`` boolean (default ``True`` at registry construction, ephemeral, never persisted). Because ``Registry.reload`` is what forces ``maintenance=True``, both choco startup *and* every ``/nodes`` save / file-watcher reload land the entire cluster back in maintenance — the registry edit becomes an automatic pause. When ``maintenance=True``, every REST call that mutates the node is blocked: ``Node.push_updatable``, ``Node.start``, **and** ``Node.kill`` log and return ``False``, and ``Orchestrator._sync_node`` short-circuits before reaching them (including the kill-on-not-started branch). Polling, status display, version reads, and config-file writes are unaffected — the rule is "no REST writes", and ``started=False`` is no longer enforced while a node is paused. Toggles live in ``web.py`` (per-node ``/toggle-maintenance/<key>``, group ``/set-maintenance-group/<group>/<on|off>``, all ``/set-maintenance-all/<on|off>``, JSON API ``set_maintenance``) and templates ``_toggle_maintenance.html`` / ``_toggle_maintenance_all.html``. The orange/blue colour pair distinguishes it from the green/yellow started toggle, and the per-node slider is mirrored relative to the started toggle (maintenance = thumb on the *left*, normal = thumb on the *right*) so "thumb-right = choco active" reads consistently across both. The ``Node`` ``maintenance`` kwarg defaults to ``False`` so direct test instantiation stays in normal mode; ``Registry.reload`` is what forces it ``True`` for production.
- **Config terminology** — four distinct types used throughout the codebase:
  - **base_config** — config file text on disk (YAML or Jinja2), not yet rendered
  - **rendered_config** — base config rendered through Jinja2 and parsed as a dict, no updatable overrides
  - **desired_config** — rendered config with updatable overrides applied; what gets pushed to kotekan
  - **updatable_config** — runtime-mutable fields (marked with `kotekan_update_endpoint`) stored separately in `.updatable/` JSON files
- **Config drift detection** — fetches the running config via `GET /config` and compares it against the desired config; `strip_updatable_values` ignores updatable blocks for the base-drift comparison
- **Node owns its state** — each ``Node`` holds its base config file, rendered config, updatable overrides, and a FIFO change queue; ``node.desired_config`` returns the merged result.  ``Registry`` just loads ``nodes.yaml`` and provides lookup.
- **File-based config** — each node's base config is `<group>/<node>.yaml` (or `.j2`); local edits are picked up automatically
- **No inotify, no WebSockets** — both "live" mechanisms ride the existing poll cadence instead of a second event transport. Local config edits are detected by an mtime scan (``Orchestrator.check_config_files``, run each sync tick; stat-ing the config dir is cheap, works on NFS, and needs no watcher thread). Browser freshness comes from htmx polling partial templates (dashboard table every 2 s, node status on the edit page every 0.5 s, services strip every 30 s) — the underlying data is only as fresh as the 5 s sync poll anyway, so push added no real latency benefit. UI assets (pico.css, htmx, idiomorph, Sortable) are vendored in `choco/static/` so the UI works without internet access.
- **Jinja2 rendering** — all config files (both `.yaml` and `.j2`) are rendered through Jinja2 using shared variables from `vars.yaml`, then sent to kotekan as JSON
- **JSON API for updates** — `POST /update/<group>` and `POST /update/<group>/<node>` accept JSON to queue base-config, updatable-config, or started-state changes; the web UI also submits through the queue
- **YAML config file** — all settings in `config.yaml` (gitignored); `config.yaml.template` checked in with defaults. No environment variables.
- **LDAP-only auth (FreeIPA)** — all routes require login via Flask-Login; users authenticated against FreeIPA LDAP (no local fallback). No roles yet — all authenticated users have full access. Defaults tuned for FreeIPA: `cn=users,cn=accounts` user DN, `posixaccount` object class, LDAPS on port 636.
- **Kotekan managed by Ansible** — choco does not deploy, build, or restart kotekan on nodes. That is handled entirely by Ansible. choco only monitors nodes and pushes config updates via the REST API.
- **No /stop endpoint** — kotekan's ``/stop`` endpoint is unreliable. All stopping is done via ``/kill``, which terminates the process; the daemon restarts it into a stopped (idle) state. The ``Node.stop()`` method has been removed.
- **Config-load resilience** — broken files do not crash startup. ``Registry.reload`` catches errors from each per-file load (``nodes.yaml``, ``vars.yaml``, every base config, every ``.updatable/*.json``), logs the full path, and continues. Per-node failures populate ``Node._base_load_error`` / ``Node._updatable_load_error`` (combined into the ``Node.load_error`` property). The sync loop **refuses to push** to a node whose ``load_error`` is set — ``desired_config`` would be incomplete (e.g. updatable overrides silently missing) and pushing it could regress kotekan's runtime state. Successful ``load_config`` / ``load_updatable`` clear their own slot; ``save_base`` / ``save_updatable`` clear theirs too (and ``save_updatable`` logs a WARNING when overwriting a previously-unreadable file). Stopped nodes still get ``/kill`` regardless — load errors don't override the user's stop intent.
- **Service status strip** — ``choco/fpga.py`` holds a tiny ``FpgaMonitor`` (HTTP poll of ``/status`` + ``/get-frame0-time`` every 30s on its own gevent greenlet) and a generic on-demand ``job_status(service_unit, state_file, stale_after_s)`` helper for the oneshot jobs (EOP, bffs).  ``job_status`` combines two cheap signals without parsing any timestamps: ``systemctl show``'s ``Result`` (the only reliable "last run failed" signal; ``ExecMainExitTimestamp`` is tested for emptiness only, to detect never-run) and the job state file's mtime.  With ``stale_after_s`` set (EOP: 25 h — the job rewrites its state file on every successful daily run) an old mtime downgrades health to ``stale``; without it (bffs — state is rewritten only when the bad-feed list *changes*) the mtime is informational only.  All badges are surfaced by ``/partials/services``, rendered into ``_services_status.html``, and included from ``base.html`` above the nav for authenticated users (htmx polls every 30s).  The FPGA monitor is instantiated unconditionally so the UI is uniform; if ``fpga_master.host/port`` are absent the badge renders as ``unconfigured`` and the greenlet doesn't spawn.  The ``fpga_master`` block is **top-level** in ``config.yaml`` (was nested under ``eop`` historically); both ``app.load_config`` and ``jobs/eop_update.py`` accept the legacy ``eop.fpga_master_host`` / ``eop.fpga_master_port`` keys but log a deprecation warning.  The ``bffs`` config block (``service_unit``, ``state_file``) feeds the BFFS badge.
- **Jobs pattern** — a "job" is a standalone script that pushes through choco's localhost JSON API (auth is bypassed for localhost callers), keeps its state in a single JSON file, and ships as ``choco-<name>.service`` (oneshot) + ``choco-<name>.timer``, installed and enabled by the ``choco-*.{service,timer}`` glob in ``choco.sh install``.  Jobs deliberately run as separate processes, not in-process greenlets: they do blocking C-extension work (astropy IERS downloads, h5py reads of files kotekan is writing) that would stall the gevent hub, and keeping them out of the choco process means job deploys/crashes don't restart choco (a choco restart re-engages cluster-wide maintenance mode).  In-process greenlets are reserved for cheap UI-facing monitoring (``FpgaMonitor``).  A header badge costs one ``job_status`` call in ``web.partial_services`` plus one pill in ``_services_status.html``.
- **EOP merge policy** — ``jobs/eop_update.py::merge_tables`` is **append-only and no-overwrite**. Stored entries are immutable: past and future values, once committed, are never replaced (IERS refinements don't propagate to already-stored entries). Fresh entries are added only when their ``t_inst_ns`` is strictly greater than the latest surviving stored entry — gaps inside the stored range are preserved (kotekan may be interpolating across them) and nothing is prepended before the first stored entry. Truncation of entries older than ``intervals_before`` days is **conditional**: only applied if the surviving stored entries still contain at least one timestamp ``<= now`` *and* one ``>= now``. If truncation would break that bracketing, it is skipped and the old entries are preserved. The astropy/numpy machinery (frame0 read, IERS download, time math) is in ``compute_lower_cutoff_ns`` / ``compute_now_inst_ns`` / ``build_fresh_table``; the policy itself (``merge_tables``) operates on plain integer timestamps so it is unit-tested without astropy in ``tests/test_eop_update.py``.

## Reference: coco (old system, in `coco/`)

The original coco is preserved for reference. It was significantly more complex:
- Redis task queue + separate worker process
- Elaborate reply checking (5 check types with on_failure actions)
- Endpoint chaining (before/after hooks, coco-to-coco forwarding)
- Slack notifications, Comet broker integration
- Sanic web framework + aiohttp client

choco preserves the core functionality (monitoring nodes, pushing config updates) but drops the over-engineered abstractions.
