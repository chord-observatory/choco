# Landing page, service strip, service pages and monitoring endpoints

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Landing page & URL layout

``/`` is a services overview (the landing page), not the node dashboard: node
management lives under ``/nodes/*`` — the dashboard at ``/nodes``, the
``nodes.yaml`` registry editor at ``/nodes/edit``, the per-node config editor
at ``/nodes/edit/<key>``, the group editor at ``/nodes/edit-group/<group>``,
the started/maintenance toggles, and the node-status partial.
``landing.html`` renders one table row per header badge (CHOCO itself plus
NODES / FPGA / PDB / DATA / EOP / BFFS / EIGENCAL / WF) with the detail the
strip only carries in a tooltip: monitor host:port and error, job unit and
failure result, a one-line state-file summary (``web._service_detail`` — the
same tolerant summariser the service pages use, so corrupt state degrades to
no summary), and the timer's next run (``services.timer_status``, systemd's
own strings, displayed never parsed).  The table is htmx-refreshed every 30 s
via ``/partials/landing-services`` — the cadence of the strip whose data it
mirrors; each refresh costs four ``systemctl show`` and four state-file reads,
the price the service pages already pay at 5 s.  Below the table sits the sky-
map card (see the sky-map bullet), refreshed every 5 min and rendered only
when the ``skymap:`` block is configured.  Login lands on ``/`` and the CHOCO
brand pill links there; the way to the dashboard is the strip's NODES badge
(see the service-status-strip bullet) — plus the landing table's NODES row
and, on the standalone pipeline/plot pages, which have no strip, a plain Nodes
nav pill (``.brand-pill.secondary``).  One test-visible consequence: the CSRF
token is seeded lazily by the ``csrf_token`` context processor, so a session
is established by fetching a page that renders a CSRF form (the dashboard),
not the landing page.

## Service status strip

``choco/services.py`` holds two small hardware monitors (and
``datafiles.DataFileScan`` supplies a third badge, DATA — see the data-file
bullet) — ``FpgaMonitor`` (HTTP poll of fpga_master's ``/status`` + ``/get-
frame0-time`` every 30s on its own gevent greenlet; also records the daemon's
``state`` / ``start_result``) and ``PdbMonitor`` (same shape; polls power_db's
``/status`` + ``/channel_states`` and decodes per-channel power via
``decode_out_bytes``, the same daisy-chain convention as
``jobs/bffs/sources/power.py``, verified against the live controller) — and a
generic on-demand ``job_status(service_unit, state_file, stale_after_s)``
helper for the oneshot jobs (EOP, bffs, eigencal).  ``job_status`` combines
two cheap signals without parsing any timestamps: ``systemctl show``'s
``Result`` (the only reliable "last run failed" signal;
``ExecMainExitTimestamp`` is tested for emptiness only, to detect never-run)
and the job state file's mtime — plus ``ExecMainStatus`` to split failures by
the shared exit-code convention: exit 2 reports ``degraded`` (yellow —
dependency/input trouble, self-heals), anything else ``failed`` (red — needs a
human).  With ``stale_after_s`` set (EOP: 25 h — the job rewrites its state
file on every successful daily run) an old mtime downgrades health to
``stale``; without it (bffs — state rewritten only when the bad-feed list
*changes*; eigencal — daytime transits are silently skipped by design) the
mtime is informational only.  The cluster itself is the strip's first badge:
NODES (``web._nodes_health``, linking to the ``/nodes`` dashboard) rolls per-
node status up to one colour — green when every node is STARTED, red when
every node is DOWN, grey with no nodes or nothing polled yet (all UNKNOWN),
and yellow for everything between (some up, all idle, or a mix), with the
exact started/idle/down/maintenance counts in the tooltip; ``node.status`` is
kept fresh by the sync poll, so this is an in-memory sweep.  All badges are
surfaced by ``/partials/services``, rendered into ``_services_status.html``
(shared pill/colour macros in ``_service_macros.html``), and included from
``base.html`` above the nav for authenticated users (htmx polls every 30s).
Both monitors are instantiated unconditionally so the UI is uniform; if
``fpga_master.host/port`` (or ``pdb.host/port``) are absent the badge renders
as ``unconfigured`` and the greenlet doesn't spawn.  The ``fpga_master`` and
``pdb`` blocks are **top-level** in ``config.yaml`` (``fpga_master`` was
nested under ``eop`` historically, and ``pdb`` was called ``psu`` before the
rename); ``app.load_config`` accepts the legacy ``psu:`` block and (with
``jobs/eop/eop_update.py``) the legacy ``eop.fpga_master_host`` /
``eop.fpga_master_port`` keys, logging a deprecation warning for each.  The
``bffs`` and ``eigencal`` config blocks (``service_unit``, ``state_file``)
feed their badges.

## Service pages

each badge links to a ``/service/<name>`` detail page, all keyed off the
``web._service_registry()`` allowlist (``choco`` / ``eop`` / ``bffs`` /
``eigencal``, plus the monitor pages ``fpga`` / ``pdb``) — page slugs are
looked up there, never passed to journalctl raw.  A job page
(``service.html``) shows common unit facts (``job_status`` detail plus the
timer's next/last run via ``services.timer_status`` — systemd's own timestamp
strings, displayed never parsed), a per-service summary read from the job's
JSON state file (``services.read_state_json``: bffs bad-feed list + recent
transitions, EOP table span, eigencal last transit; assembled in
``web._service_detail``), a collapsed ``<details>`` dump of the **raw** state-
file JSON (pretty-printed once at page-load, kept out of the 5 s status poll
so a large EOP table isn't re-sent), and an htmx-refreshed journal viewer
(``job_logs``, one ``journalctl -u`` subprocess; ``?lines=`` clamped to
10–1000).  Every page's status block refreshes itself every **5 s while
open**: job pages poll ``/partials/service-status/<name>`` (facts + state-file
summary), the PDB page ``/partials/service-pdb`` (facts + grid, toggles
included), the FPGA page ``/partials/service-fpga`` — the monitor partials
call ``poll_if_stale(5)`` so the hardware polls tighten only while someone is
watching; the journal keeps its own 30 s cadence.  Failure handling is
deliberate: monitor GETs get **one quick retry** (``_get_with_retry``) so a
single dropped request doesn't flip a badge red for a whole 30 s interval;
JSON of unexpected shape is tolerated (non-dict payloads ignored,
``_fetch_states`` raises ValueError so garbage and unreachable take the same
error path); a PDB poll failure keeps the last-known grid with an explicit
"showing stale states" banner; and ``web._service_detail`` wraps all state-
file summarising so corrupt job state degrades to "no summary", never a 500.

## Monitoring endpoints

``/api/status`` (localhost bypass) is the simple overall-health JSON: choco
``up``/``started_at``, one health string per service (from
``web._status_summary`` / ``_services_health``, ``data`` among them), and node
counts including ``maintenance``.  The detailed per-node dump lives at
``/api/nodes/status``; the registry at ``/api/nodes``; a group's sample
desired kotekan config at ``/api/config/<group>`` (bffs derives its feed
labels from the config's ``dish_inputs`` table there — the same table kotekan
indexes its bad-input mask with); the master PDB channel map plus its cross-
check at ``/api/pdb/map``; and the data-file scan at ``/api/files``
(``?refresh=1`` bypasses its cache).  ``/metrics`` re-renders the same summary
as hand-rolled Prometheus exposition text (no client library) and is
unauthenticated — Prometheus scrapes cross-host and can't do LDAP sessions —
so it must stay aggregate-only: no node names, hosts, or configs in metric
labels.  The only other unauthenticated route is ``/skymap.png`` (see the sky-
map bullet), open for the same cross-host reason; everything else requires
login.  ``choco_start_time_seconds`` exists specifically because a choco
restart re-engages cluster-wide maintenance (alertable via ``changes()``).
