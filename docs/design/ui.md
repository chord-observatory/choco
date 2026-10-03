# Landing page, service strip, service pages and monitoring endpoints

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Landing page & URL layout

``/`` is a services overview (the landing page), not the node dashboard: node
management lives under ``/nodes/*`` — the dashboard at ``/nodes``, the
``nodes.yaml`` registry editor at ``/nodes/edit``, the per-node config editor
at ``/nodes/edit/<key>``, the group editor at ``/nodes/edit-group/<group>``,
the started/maintenance toggles, and the node-status partial — and the config
library under ``/configs`` (the files under the configs directory with the
nodes that render or include each, a New-file form) and
``/configs/edit/<path>`` (one file; a save is checked against every node that
uses it first, see [sync.md](sync.md)).  The node page names the file it
renders, says which other nodes share it, links it into the library, and has
the selector that changes it (a nodes.yaml edit, so the cluster is paused as
the registry editor's banner says); the dashboard's Config column links each
file the same way, and the landing table's NODES row links the library beside
the dashboard.
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
human).  A run in progress (``ActiveState`` ``activating``) carries no verdict
at all — systemd resets ``Result`` and ``ExecMainStatus`` and blanks
``ExecMainExitTimestamp`` the moment a run starts, verified against a unit
that had just failed — so ``job_status`` remembers each unit's last completed
snapshot (``services._LAST_COMPLETED``, process-local, rebuilt by the 5 s
poll) and reports that, flagged ``running`` for the tooltip, instead of
dropping to grey for the length of every run (bffs runs ~6-20 s of every 30 s,
the waterfall ~50 s of every 2 min); a fresh process shows ``running`` until
the first completion it sees.  With ``stale_after_s`` set (EOP: 25 h — the job rewrites its state
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
jobs' files are read from ``<state_dir>/<job>/`` by name (``state_dir`` in
config.yaml, default ``/var/lib/choco``; see jobs.md); the per-job blocks
carry only ``service_unit`` (and ``bffs.control``).

## Service pages

each badge links to a ``/service/<name>`` detail page, all keyed off the
``web._service_registry()`` allowlist (``choco`` / ``eop`` / ``bffs`` /
``eigencal``, plus the monitor pages ``fpga`` / ``pdb``) — page slugs are
looked up there, never passed to journalctl raw.  A job page
(``service.html``) shows common unit facts (``job_status`` detail plus the
timer's next/last run via ``services.timer_status`` — systemd's own timestamp
strings, displayed never parsed), a per-service summary read from the job's
JSON state file (``services.read_state_json``: bffs element grid — every
element of the recorded axis, eight to a column in kotekan's order, good/bad
with the flagging source on hover, ``web._element_grid``; the bad list alone
for a state file that predates the axis — + recent transitions, EOP table
span, eigencal last transit; assembled in ``web._service_detail``), a collapsed ``<details>`` dump of the **raw** state-
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

The BFFS page additionally reads the job's per-run file (``run.json`` in
its state directory; see jobs.md) through
``web._bffs_detail`` / ``_bffs_run_summary``: a **Last run** fact (status
pill, time, bad count, whether it was sent, the degraded reasons or the
error) and an **N² file** fact (the file used, its age, or why none was),
then a **Sources** table — one row per configured source with a status
pill (``source_color``: ok green, degraded/skipped yellow), the counts it
flagged and judged, its reason, and a one-line per-kind digest of its detail
(``_bffs_source_summary``; unknown kinds fall back to ``key=value``).  Either
file may be missing; the page renders what it has.  The same reasons reach
the strip's badge tooltip ("why: …") and the landing table's job row via
``_services_health`` → ``_run_reasons``, attached only when systemd's verdict
is degraded or failed, so the badge colour and its explanation cannot
disagree.  ``/api/nodes`` entries carry the sync loop's live ``status``
beside the desired ``started``.

**Manual flags from the grid (2026-10).**  Unless ``bffs.control`` is
false, every cell of the element grid is a one-button form posting to
``POST /service/bffs/manual`` (login + CSRF, like the PDB toggles; one
``logger.warning`` audit line naming the operator), which adds the label to
``manual_overrides.yaml`` in the job's state directory — the file the job's
``manual`` source reads by default — or removes it (``services.toggle_manual_flag``:
other keys kept, comments not, temp-file-and-rename so the job never reads
a torn file).  The label is accepted only if it is on the element axis the
state file records — the allowlist for this write, so nothing a browser
sends reaches the file unchecked.  Before writing, the route compares that
file with the one the job's ``manual`` source reported reading in
``run.json`` (a source given an explicit ``path:`` reads elsewhere) and
refuses a mismatch, or a job with no manual source, with an error notice,
since the click would otherwise land where the job never looks; with no run
file there is nothing to check against and the write proceeds.
A cell in the file but not yet in the state's bad list (the job runs every
30 s) shows a yellow inset border ("manual flag set, applied on the next
run"), so the click is visible at once and the red follows.  The reply is
the status block swapped in place plus an out-of-band notice into
``#service-flash`` (outside the polled region), or flash-and-redirect for a
plain form POST; a once-per-session confirm gate in ``service.html`` mirrors
the PDB page's.  An unreadable override file or a path mismatch renders the
grid read-only with the reason in the caption.

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
labels.  The only other unauthenticated routes are ``/skymap.png`` and its
night-palette twin ``/skymap-night.png`` (see the sky-map bullet), open for the
same cross-host reason; everything else requires login.  ``choco_start_time_seconds`` exists specifically because a choco
restart re-engages cluster-wide maintenance (alertable via ``changes()``).
