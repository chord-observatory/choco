# Landing page, service strip, service pages and monitoring endpoints

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Landing page & URL layout

``/`` is a services overview (the landing page), not the node dashboard: node
management lives under ``/nodes/*`` — the dashboard at ``/nodes``, the
``nodes.yaml`` registry editor at ``/nodes/edit``, the node page at
``/nodes/edit/<key>``, the started/maintenance toggles, and the node-status
partial — and the config library under ``/configs`` (the files under the
configs directory with the nodes that render or include each, a New-file
form) and ``/configs/edit/<path>`` (one file; a save is checked against every
node that uses it first, see [sync.md](sync.md)).  Config *text* is edited
only in the library: the node page has no textarea (2026-10; the textarea's
prominent Save button next to the file selector's small Use button invited
saving the old text instead of applying the selection).  The node page names
the file it renders, says which other nodes share it, and carries the
selector that changes it (a nodes.yaml edit, so the cluster is paused as the
registry editor's banner says), a Re-push control, and a one-off that starts
a chosen library file without recording it.  The group editor
(``/nodes/edit-group/<group>``, one textarea broadcast to a group) was
removed with it: a group shares one library file now.  Everything that opens
an editor is an explicit button with a tooltip — "Edit config" beside the
file name on the node page, "Edit" per row on the library page, "Edit
nodes" / "Edit configs" in the dashboard header — never a bare clickable
name.  The dashboard table itself only names each node's file: a per-row
editor button made the page too busy, and the header button is one click
away.  The landing table's NODES row links the library beside the dashboard.
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
brand mark links there; the way to the dashboard is the strip's NODES badge
(see the service-status-strip bullet) — plus the landing table's NODES row
and, on the standalone pipeline/plot pages, which have no strip, a plain Nodes
nav link (``.brand.secondary``); the dashboard table only names each
node and its file, with the Pipeline and Node buttons per row.  One
test-visible consequence: the CSRF
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
(the shared tone macros and the ``tag()`` component in
``_service_macros.html``; see the palette bullet below), and included from
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
tag (``source_tone``: ok, degraded/skipped warn), the counts it
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

## Palette and status tags

Restyled 2026-10 after an audit: the original badges were fully rounded
pills in the clrs.cc web primaries (``#008000`` / ``#ff4136`` / ``#ffdc00``
/ ``#0074d9`` / ``#aaa``) with white text, which read as a row of loud
capsules even when everything was healthy, and three of the fills failed
WCAG AA (red 3.5:1, grey 2.3:1, orange 2.4:1; the strip's yellow was
white-on-yellow at 1.4:1 because ``pill_style`` hard-coded white ink).  The
strip, the dashboard and the service pages also disagreed on the badge
shape, and the colours lived as hex literals in eleven templates.

**One stylesheet, six tones.**  ``static/choco.css`` is loaded by
``base.html`` and by the standalone pipeline/plot pages, so all three share
it.  It defines the palette as tokens on ``:root`` with a ``[data-theme=
"dark"]`` block (every choco page sets ``data-theme`` explicitly, so no
``prefers-color-scheme`` block is needed): six status tones — ``ok``,
``bad``, ``warn``, ``info`` (syncing / running / not in maintenance),
``maint`` and ``off`` (unknown, not run, idle, channel off) — each in a
*solid* form (fill + ink, for switches, dots and bad grid cells) and a
*soft* form (tint + dark ink of the same hue, for tags and good cells), a
neutral ``--surface``, and the grey brand mark.  Neutrals otherwise come
from Pico's own variables (``--pico-muted-color``,
``--pico-muted-border-color``, ``--pico-background-color``) so they follow
its themes.  Every pairing meets AA at the sizes used.  Templates never
carry a hex literal for a state: the ``*_tone`` macros in
``_service_macros.html`` (``monitor_tone``, ``job_tone``, ``source_tone``,
``node_tone``) map each subsystem's health strings onto the six tones, and
the ``tag()`` macro turns a tone into ``class="tag tag-<tone>"``.
``_nodes_health`` still decides the strip's roll-up (all idle is ``warn``);
``node_tone`` makes a single idle node ``off``, since idle is a chosen
state, not a warning.

**Page chrome (2026-10 audit).**  One header pattern on every page,
``.page-head``: the title (with its status tag, if any) left and the page's
actions right; one note style, ``.note``, for explanatory prose (the
configs, config-editor and node pages used ``<p><small>`` and three pages
each had their own muted class); section headings are ``h3`` everywhere,
sized down to 1.15 rem in ``choco.css`` so they sit under the 1.75 rem page
title (the service pages had skipped to ``h4`` while the dashboard used
``h3``); one compact button, ``.btn-sm`` on ``secondary outline``, for every
row-level action (Pipeline, Node, Edit, Rescan, the journal refresh), with
Pico's filled primary reserved for each page's single main action (Save,
Save & Reload, Create, Apply, Log in, the FPGA Start) — the dashboard row
had mixed azure outlines, a round grey "i" and the group buttons; static
notices are ``.notice`` with ``.notice-warn`` / ``.notice-bad`` (the nodes
editor's warning, the PDB map's bad-row list, an unreachable PDB
controller, the FPGA restart caution, a failed data root), distinct from
the transient ``.flash``; ``.table-sm`` and ``.scroll-x`` replace the inline
table and figure styles.  The build-details popover opens from the version
string itself (``.version-trigger``, a dotted underline) on both the
dashboard and the node page, replacing the "i" button and the "Show build
details" button.  The dashboard row leads with the node name and lost the
Synced column (it repeated Status).  The node page's facts are three
``dl.node-facts`` lists with one fixed label column — desired flags, the
polled runtime facts (``_node_status.html`` emits dt/dd pairs into the
polled ``dl``), the file — and its forms are ``.action-row`` rows with a
fixed label.  The journal is a plain section, not the one card on the site.

**One theme choice.**  ``base.html`` pins light and the pipeline/plot pages
dark by default, but a toggle (◐, in the nav and in the standalone headers)
stores one choice under ``localStorage.chocoTheme`` that every page applies
before first paint (``chocoPipelineTheme``, the older key, is still read).
Until the operator toggles, nothing changes from the per-page defaults; once
they do, all pages follow.  Pico handles its own dark theme and
``choco.css`` carries the dark tokens, so no template needs a second set of
colours.

**Type (2026-10).**  IBM Plex Sans for the UI and IBM Plex Mono for
labels and code, vendored as latin-subset woff2 files under
``static/fonts/`` (SIL OFL, licence beside them; ~100 KB in all, five
faces: Sans 400/500/600, Mono 400/600) and declared in ``choco.css``, which
also points Pico's two family variables at them.  Vendored rather than
linked because the site must not reach an outside font host (operators'
browsers at the site may have no internet, and nothing else on the site
loads from elsewhere) and because ``system-ui`` is Segoe, SF, Ubuntu or
DejaVu depending on the desk, with very different weights and widths —
the "too bold" pills were the strip's ``<strong>`` labels rendering at 700
in whatever the OS had.  Plex has even kerning, true tabular figures (so
``table { font-variant-numeric: tabular-nums }`` lines digits up
everywhere) and a mono sibling, so dish labels line up in both faces.
Plex at 700 reads heavy, so **600 is the site's bold**: headings, ``strong``,
table headers, the tag label and the brand mark; tag words are 400 and
the tag body 500.  ``tests/test_web.py::TestVendoredFonts`` serves every
file and checks the stylesheet names no external host — and that
``pyproject.toml`` lists ``static/fonts/*``, since setuptools' ``static/*``
glob does not descend.

**Section rules.**  Every ``h3`` in ``main`` carries a hairline rule below
it (``--surface-border``) with a larger top margin, so the blocks of a page
separate without boxes or tinted bars; a heading that shares a row with
controls (the PDB bus and dish heads, the journal head with its lines
picker, the files page's root headers) puts the rule on the row instead
via ``.section-head``, and the dashboard's tinted ``.group-bar`` keeps its
own treatment because it carries the group controls.

**Night sky map in the dark.**  The landing card's ``<img>`` carries
``data-day-src`` and, once a night render exists, ``data-night-src``;
``base.html``'s theme script swaps ``src`` to match the theme on load, on
toggle and after every htmx swap (the card is replaced every five
minutes).  Done client-side because the theme lives in ``localStorage``
and the server never knows it.

**Scale and density (2026-10 review).**  Pico grows the base font with
the viewport — 16 px on a phone, 20 px at 1280 and 21 px on a wide monitor
— which suits a marketing page and made the control panel enormous: a
dashboard heading was about 36 px.  ``choco.css`` pins ``--pico-font-size``
to 100% at every width — the browser's default, 16 px unless the operator
has set otherwise, so font preference, zoom and display scaling still apply
(15 px was tried and read a shade small; a fixed px size was never used) —
h2 to 1.4 rem, h3 to 1.15 rem, table cells to
0.4 × 0.6 rem (Pico's 0.5 × 1 rem), code in table cells, fact lists and
the group bar to plain monospace (Pico's tint made the configs table a
wall of chips), and the switches to 2 × 1.125 rem.  Pico's buttons are full-width blocks;
``choco.css`` makes every button as wide as its label, since a Save or a
Log in stretched across the page read as a banner.  Do not re-enable the
fluid scale: the sixteen-board PDB grid and a forty-row dashboard are
sized to this.

**Orientation.**  A page row under the strip naming the four sections
was tried in the review round and removed the same day: one more line of
chrome than the pages needed, and the strip's NODES / DATA tags plus the
dashboard's Edit nodes / Edit configs buttons already reach everything.
Deep pages carry a breadcrumb inside the title
(``.crumb`` / ``.crumb-sep``): Nodes / cx47, Configs / path, Services /
BFFS, Files / acq / A01X × B02Y.  ``<title>`` is "Page — CHOCO" everywhere
(it was four formats).  ``?wall=1`` on the landing page is the wall-display
mode the night sky map hinted at: ``body.wall`` hides the nav and the
notes, widens the container and enlarges the table and its tags.

**The dashboard says what is wrong.**  Every choco restart puts the whole
cluster in maintenance, which silently blocks every push, so the table now
opens with a ``.notice-warn`` whenever any node is in maintenance — the
count, why it matters, "every node starts this way after a choco restart"
when it is all of them, and a "Lift maintenance on all" button — rendered
inside the polled table so it tracks the switches.  The Start switch says
what the operator asked for and the tag what is true; when they differ a
muted "wants started" / "wants idle" (``node_wants`` in
``_service_macros.html``) sits beside the tag on the dashboard and the node
page, which is what the removed Synced column was reaching for.  A group
whose nodes all render one file says so once in its bar ("renders
chord/pathfinder.j2") and drops the Config column; the column appears only
for a group whose nodes differ.  The landing table folded Last activity and
Next run into one Timing column ("state 2m ago" over "next …") and dropped
the systemd unit names from Detail — they live on the service page and in
the strip's tooltip.  The explanatory prose on the configs page and the
nodes editor's warning were cut to the consequential facts in bold.

**The tag.**  Rectangular (3 px radius), a small dot in the tone's ink, a
1 px border mixed from the ink, the font size inherited from its context
(the strip is 0.85 em, a service-page heading halves it, table cells
shrink it a little).  ``tag(tone, word, label=, href=, title=, quiet=,
extra=)`` renders an ``<a>`` with ``href`` else a ``<span>``; ``label`` is
the service name in the strip and also puts ``aria-label="LABEL: word"``
on the element so the state stays available to assistive tech.  The strip
passes ``quiet=true``: an **ok**, **info** or **off** tag drops the word and
its tint and keeps the label and a dot in its tone on the neutral surface,
so a healthy cluster is a quiet row and the word plus the tint appear only
for **warn** and **bad** (the tooltip carries the full health line either
way).  Info and off are quiet too because a worded ``running`` or ``not
run`` pushed the nine-service strip onto a second nav line (Pico's nav list
does not wrap, the strip item does); the strip also uses tighter tag
padding than the rest of the page for the same reason.  The landing table,
the dashboard status column and the service pages keep the word, since
there the row has no other status text; the node badges add a
``status-<value>`` class as a stable hook.  The CHOCO mark is ``.brand``,
a grey rectangle (``--brand-bg``), not a health colour; ``.brand.secondary``
is the outlined Nodes link on the standalone pages.

**Large surfaces in the dark.**  The dark soft tints are tuned for tags;
a notice or a flash at that strength is a big block that glows against
Pico's near-black page.  Under ``[data-theme="dark"]`` the notices and
flashes take a faint wash — the tint mixed 40–60 % into the page colour —
and the coloured left rule carries the tone.

**Controls are not status.**  The started/maintenance switches take the
tone of what they enable when on (``--ok``, ``--info``) and are neutral
when off, except maintenance, the one off-state that silently stops pushes,
which stays ``--maint``; an in-flight htmx request dims the switch rather
than greying it, so idle and "request pending" look different.  The
group-wide ▲▼ buttons (``.group-ctl``) are a muted caption ("maint",
"start") plus a pair of outlined glyph buttons whose *text* takes the tone
of the switch state it sets — amber ▼ into maintenance and blue ▲ out,
green ▲ started and grey ▼ idle — so the association with the switch
columns below is learnable; two uncaptioned neutral pairs were
indistinguishable.  They are styled with ``all: unset`` rather than Pico's
button classes because Pico's hover rules outrank any tone colour.  The
PDB page's bulk buttons follow the same text-colour rule.  The bffs element grid paints good cells in the ``ok`` tint and
bad cells in solid ``--bad`` so the flagged feeds are the figure and the
rest the ground; the manual-flag inset border is ``--warn``.  The PDB
channel grid is ``ok`` tint for on and ``off`` tint for off, with the
●/○ glyph carrying the state as well.  Flash banners, the dev-mode banner
(``--warn``), the nodes editor's warning box and the files table's error
notes all use the same tokens; ``.err`` / ``.t-bad`` / ``.t-warn`` /
``.t-ok`` / ``.muted`` are the text-only helpers.

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
