# Sync loop, node state and config model

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Config terminology

four distinct types used throughout the codebase:
  - **base_config** — config file text on disk (YAML or Jinja2), not yet
    rendered
  - **rendered_config** — base config rendered through Jinja2 and parsed as a
    dict, no updatable overrides
  - **desired_config** — rendered config with updatable overrides applied;
    what gets pushed to kotekan
  - **updatable_config** — runtime-mutable fields (marked with
    `kotekan_update_endpoint`) stored separately in `.updatable/` JSON files

## No database

runtime state is ephemeral (rebuilt from polling); desired config lives in
YAML files that can be edited locally or via the web UI

## Node owns its state

each ``Node`` holds its base config file, rendered config, updatable
overrides, and a FIFO change queue; ``node.desired_config`` returns the merged
result.  ``Registry`` just loads ``nodes.yaml`` and provides lookup.

## File-based config

each node's base config is `<group>/<node>.yaml` (or `.j2`); local edits are
picked up automatically

## Jinja2 rendering

all config files (both `.yaml` and `.j2`) are rendered through Jinja2 using
shared variables from `vars.yaml`, then sent to kotekan as JSON

## Config drift detection

fetches the running config via `GET /config` and compares it against the
desired config; `strip_updatable_values` ignores updatable blocks for the
base-drift comparison, and `_sync_updatable` separately re-pushes stored
updatable values that differ from the live config.  This drift path is also
the delivery guarantee for job pushes (bffs flags, EOP tables, eigencal
gains): a node that is down when a group update lands keeps the values as on-
disk desired state and catches up on the first successful poll after it
returns — no job-side re-send needed (an unreachable node's checks back off
towards ``sync.max_retry_interval``, default 60 s, so "first poll after it
returns" can be up to that far behind the return)

## One worker greenlet per node

every ``Node`` has exactly one owner (``sync.NodeWorker``) that drains its
queue and reconciles, so per-node operations are serialized by construction
(no queue locks, no ``SYNCING`` re-entry guard) and each node carries visible
worker state: a ``WorkerPhase`` (idle / down / draining / probing / queued-
for-push / pushing / awaiting-idle), consecutive-failure count, last cycle
duration and next check time, surfaced per node in ``/api/nodes/status`` as
``worker``.  Submissions wake the owner (``Orchestrator._enqueue``), so a
queued change is picked up immediately rather than on the next scheduled
check; the wake protocol clears the event *before* testing the queue and
nothing between the clear and the wait yields to the hub, which is what makes
a lost wakeup unrepresentable — do not add yielding calls (logging to a socket
handler included) to that window.  Each worker schedules its own drift check:
every ``sync.poll_interval`` while the node answers, doubling toward
``sync.max_retry_interval`` (default 60 s) while it is unreachable, so dead
nodes stop consuming request timeouts at full cadence.  Restart concurrency is
bounded separately by ``sync.max_concurrent_pushes`` (the deprecated
``sync.num_workers`` is read as its alias with a warning): polling parallelism
scales with the cluster, but only that many kill→start sequences run at once.
Workers are never force-killed — ``stop()`` is a flag plus a wake, and an in-
flight cycle (a restart especially) always completes first, so a registry
reload cannot strand a node between ``/kill`` and ``/start``;
``apply_nodes_update`` stops the old workers, reloads, runs state discovery,
then spawns the new set (discovery before spawn, same order as startup, so
first cycles act on observed state).  Every path through ``_sync_node`` leaves
``node.status`` reflecting that cycle's probe — the load-error early-returns
included — because the worker reads it afterwards to distinguish "node
answered" (normal cadence) from "unreachable" (back off); a reachable node
with a broken config file therefore keeps fresh status and normal cadence
rather than masquerading as down.

## No inotify, no WebSockets

both "live" mechanisms ride the existing poll cadence instead of a second
event transport. Local config edits are detected by an mtime scan
(``Orchestrator.check_config_files``, run each sync tick; stat-ing the config
dir is cheap, works on NFS, and needs no watcher thread). Browser freshness
comes from htmx polling partial templates (dashboard table every 2 s, node
status on the edit page every 0.5 s, service-page status blocks every 5 s,
services strip and landing service table every 30 s) — the underlying data is
only as fresh as each node's sync check anyway (``poll_interval`` while
reachable, backed off toward ``max_retry_interval`` while down), so push added
no real latency benefit. htmx is also the *write* transport where a full-page
reload would lose the operator's place (the PDB grid: ``hx-post`` + a morph
swap of the same partial the poll targets), so those actions never round-trip
through a redirect either. UI assets (pico.css, htmx, idiomorph, Sortable) are
vendored in `choco/static/` so the UI works without internet access.

## Started/stopped toggle

each node has a ``started`` boolean representing the desired runtime state.
``nodes.yaml``'s ``started`` field is read as the pre-discovery default, but
at startup ``Orchestrator.discover_node_states()`` probes every node in
parallel and overwrites ``node.started`` from the actual runtime state
(STARTED → ``True``; IDLE / DOWN / UNKNOWN / unreachable → ``False``, i.e.
idle is the default whenever the probe doesn't clearly say "running"). The
same discovery pass also runs from ``Orchestrator.apply_nodes_update`` after
every ``nodes.yaml`` save / file-watcher reload, so registry rebuilds preserve
runtime state in the same way startup does. When ``started=False``, the sync
loop will kill kotekan if found running and will never start it or push
updatable configs (subject to the maintenance gate below). Config changes are
still tracked on disk. Runtime toggles are ephemeral (lost on choco restart;
the next startup re-discovers).

## Maintenance mode

each node has a ``maintenance`` boolean (default ``True`` at registry
construction, ephemeral, never persisted). Because ``Registry.reload`` is what
forces ``maintenance=True``, both choco startup *and* every ``/nodes/edit``
save / file-watcher reload land the entire cluster back in maintenance — the
registry edit becomes an automatic pause. When ``maintenance=True``, every
REST call that mutates the node is blocked: ``Node.push_updatable``,
``Node.start``, **and** ``Node.kill`` log and return ``False``, and
``Orchestrator._sync_node`` short-circuits before reaching them (including the
kill-on-not-started branch). Polling, status display, version reads, and
config-file writes are unaffected — the rule is "no REST writes", and
``started=False`` is no longer enforced while a node is paused. Toggles live
in ``web.py`` (per-node ``/nodes/toggle-maintenance/<key>``, group
``/nodes/set-maintenance-group/<group>/<on|off>``, all ``/nodes/set-
maintenance-all/<on|off>``, JSON API ``set_maintenance``) and templates
``_toggle_maintenance.html`` / ``_toggle_maintenance_all.html``. The
orange/blue colour pair distinguishes it from the green/yellow started toggle,
and the per-node slider is mirrored relative to the started toggle
(maintenance = thumb on the *left*, normal = thumb on the *right*) so "thumb-
right = choco active" reads consistently across both. The ``Node``
``maintenance`` kwarg defaults to ``False`` so direct test instantiation stays
in normal mode; ``Registry.reload`` is what forces it ``True`` for production.

## One-off configs

``POST /oneshot/<group>`` and ``POST /oneshot/<group>/<node>`` (body
``{"config_content": ...}``; localhost bypass like ``/update``), plus a
**Start as one-off** button on the node edit page that posts the textarea's
text, start a supplied config on nodes that are **in maintenance and idle**
without recording it anywhere: no base file, no ``.updatable`` store, no in-
memory desired state — the only trace is the audit ``logger.warning`` line,
which carries the config's sha256 prefix because nothing else does
(``web._run_oneshot``).  It is a *control* in the FPGA Start/Stop family, not
a change through the queue: in maintenance the worker is read-only, so there
is nothing for a direct ``/start`` to collide with, and a synchronous per-node
result (``started`` / ``skipped`` with reasons; 400 unrenderable, 409 nothing
started, else 200) beats an async "queued".  Both preconditions are checked
per node against a **fresh probe**, not the cached status: maintenance because
outside it the next poll sees base drift and restarts the node onto its
recorded config within a poll interval (so lifting maintenance later *reverts*
a one-off — it is scoped to the maintenance window by construction), and IDLE
because a one-off never kills.  The fan-out is spawn-and-join like discovery,
not bounded by the restart semaphore (no kill, nothing disruptive to bound);
each started node gets a ``POLL`` item so its worker observes the new state
now, and the route never sets ``node.status`` itself — if kotekan accepted the
POST but the pipeline failed to come up, the poll is what tells the truth.
``Node.start(..., override_maintenance=True)`` is the one hole in the
maintenance guard, named for the guard it steps past; the sync loop never
passes it.  The posted text is rendered exactly as supplied — stored updatable
overrides are *not* merged in (paste them if wanted) — and since YAML is a
superset of JSON a script can post the output of ``/api/config/<group>`` back
as text.  Getting a *running* node idle inside maintenance has no choco path
(toggle started off, lift maintenance briefly so the loop kills it, re-enter,
post); a manual kill control is the obvious follow-up if that is felt.

## JSON API for updates

`POST /update/<group>` and `POST /update/<group>/<node>` accept JSON to queue
base-config, updatable-config, or started-state changes; the web UI also
submits through the queue, and the ``set_started`` / ``set_maintenance``
actions enqueue a ``POLL`` so the worker acts now, as the dashboard toggles do
(the CLI bullet has the history)

## Config-load resilience

broken files do not crash startup. YAML parsing uses libyaml's ``CSafeLoader``
when available (``state._yaml_load``; same safe subset and ``yaml.YAMLError``
subclasses, ~7× faster, and — unlike the pure-Python parser, which raises
``RecursionError`` past ~500 nesting levels — it cannot throw a
non-``YAMLError`` that would escape the guards below). ``Registry.reload``
catches errors from each per-file load (``nodes.yaml``, ``vars.yaml``, every
base config, every ``.updatable/*.json``), logs the full path, and continues.
Per-node failures populate ``Node._base_load_error`` /
``Node._updatable_load_error`` (combined into the ``Node.load_error``
property). The sync loop **refuses to push** to a node whose ``load_error`` is
set — ``desired_config`` would be incomplete (e.g. updatable overrides
silently missing) and pushing it could regress kotekan's runtime state.
Successful ``load_config`` / ``load_updatable`` clear their own slot;
``save_base`` / ``save_updatable`` clear theirs too (and ``save_updatable``
logs a WARNING when overwriting a previously-unreadable file). Stopped nodes
still get ``/kill`` regardless — load errors don't override the user's stop
intent.

## No /stop endpoint

kotekan's ``/stop`` endpoint is unreliable. All stopping is done via
``/kill``, which terminates the process; the daemon restarts it into a stopped
(idle) state. The ``Node.stop()`` method has been removed.

## Kotekan managed by Ansible

choco does not deploy, build, or restart kotekan on nodes. That is handled
entirely by Ansible. choco only monitors nodes and pushes config updates via
the REST API.
