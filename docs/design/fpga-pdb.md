# F-engine gains, FPGA and PDB controls, the master PDB map

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## F-engine digital gains

the FPGA page also shows the *current gain archive*, read-only.  fpga_master
serves it at ``/get-current-gain-file`` as an HDF5 ``DigitalGainArchive``:
``gain_coeff`` [update_time, freq, input] complex64 in the file (8.4 MB; see
below for what is served) plus ``gain_exp`` / ``compute_time`` per input and
an ``index_map`` giving frequency centres in MHz and the correlator input
names.  The whole design turns on one observation: **an HDF5 dataset already
speaks the buffer-plot protocol** — a C-order array with named axes — so
``/api/fpga/gain-data?dataset=&len=`` returns exactly what ``/api/node-buffer-
data`` does (``len=0`` a descriptor, ``len>0`` leading bytes, the archive's
``update_id`` as ``X-Frame-Id``) and the entire plotting stack works on gains
with no client-side special case: dimension table, folds, index selection,
series picker, zoom, ticks, and a full-screen page at
``/service/fpga/plot?dataset=``.  What made that reuse possible was
generalising the panel from a node+buffer to a **source** (``{id, title, url,
page}``); a kotekan buffer and a gain dataset differ only there, and
``window.chocoPlot`` is the one small export that lets the card's dataset
selector point the panel somewhere new.  Two things are genuinely new:
**complex64** decoding, and a ``part`` selector for it outside N2 frames
(``mag | phase | real | imag | components``); changing ``part`` re-derives
from the bytes already in hand exactly as the ``bits`` selector does, and a
derived part drops the component dimension before the axes are composed.  The
gains themselves no longer exercise either: the archive's *format* is complex
(``DigitalGainArchive`` allows complex gains) but its *content* is real —
verified against the live file, every one of the 1,048,576 imaginary parts
exactly zero, and expected to stay so — so ``h5read`` applies one rule in one
place (``_as_served``, shared by ``manifest`` and ``data`` so the two cannot
disagree): a complex dataset whose imaginary part is identically zero is
served as its real part, float32 at half the bytes (4.2 MB per poll instead of
8.4), and the plotter sees a plain real dataset — no part selector, no re/im
dimension, no client code involved.  The manifest reads the values of complex
datasets to apply the rule (metadata alone for everything else); the check is
kept rather than stripping unconditionally because it costs one scan of an
array already in hand and turns the premise failing into the correct behaviour
— a dataset that does carry imaginary content is served complex as before and
the selector reappears.  ``gain_exp`` is deliberately **not** folded into
``gain_coeff``: the coefficients are raw fixed-point spanning five orders of
magnitude (hence ``log``), and inventing a scaling convention here would be
guessing.  **h5py runs in a subprocess** (``choco.h5read``, imported inside
``main()``): the import alone costs ~90 ms and the parse blocks, and this
process is a gevent hub — the same reason the timer jobs are separate
processes; the download is cooperative because ``monkey.patch_all()`` runs at
startup, so only the parse needed isolating.  ``services.GainArchive`` caches
the manifest and per-dataset bytes for 30 s behind a semaphore, so several
viewers cost one download, and the panel polls at 30 s rather than 5 s and
asks for the whole dataset since a byte prefix would cut the frequency axis in
half.  The card is a **lazily loaded partial** (``/partials/fpga-gains``,
``hx-trigger="load"``) because filling it means pulling 8.4 MB while the
page's first job is saying whether the F-engine is up — measured against the
live daemon, the page paints in 19 ms and the card follows in ~265 ms cold, 1
ms cached.  Dataset names are checked against the manifest before reaching
h5py, the same never-pass-the-caller's-string rule as the journalctl
allowlist.  There is **no write path**: nothing here applies or uploads gains.
The ``index_map`` also hands over a real MHz axis and per-input names, which
are *not* wired into the plot yet — real-unit axes need tick generation over a
non-integer axis and a zoom window that is not in indices, so for now they are
reported as text in the card.

## FPGA & PDB controls

the FPGA page (``service_fpga.html``) carries **Start/Stop controls**:
confirm-dialog forms POSTing to ``/service/fpga/<action>``, gated by the
``fpga_master.control`` config flag (default true), audit-logged with the
username.  ``/start`` POSTs ``{}`` so fpga_master reuses its launch config and
initializes in the background (it acknowledges before checking its state, so a
no-op "already started" only surfaces afterwards via ``start_result``);
``/stop`` must be a **GET** — wtl.rest serves argument-less endpoints for GET
only (POST gets a 405) — and blocks remotely, so ``FpgaMonitor.stop_master``
runs in a greenlet; both control wrappers parse the body through
``_wtl_result`` because wtl.rest reports handler exceptions as **HTTP 200 with
an ``{"error": ...}`` body** (a crashed remote stop would otherwise read as
success); the page's status block htmx-polls ``/partials/service-fpga`` every
10 s, whose ``poll_if_stale`` tightens the monitor cadence only while someone
is watching.  Every action lands in ``FpgaMonitor.actions`` (ephemeral,
newest-first, capped) rendered as the page's **Recent actions** table — an
async stop appears first as in-flight, then as its completion — while the
durable audit trail is the ``logger.warning`` line in choco's own journal.  A
restart assigns a new frame0 — the page warns about it.  The PDB page
(``service_pdb.html``) renders the per-bus board/chip/channel grid whose cells
are **toggle buttons** (``POST /service/pdb/set`` →
``PdbMonitor.set_channel``), alongside **bulk power buttons** (``POST
/service/pdb/set-group`` → ``PdbMonitor.set_group``): per chip at the end of
each row, and per SPI bus in the bus heading — a per-board button was dropped
as redundant with the chip-level column (two clicks vs one), though
``set_group`` and its route still take a board-only scope, reachable directly
if a caller wants it.  All are gated by ``pdb.control`` and audit-logged.
Writes go **over htmx** (``hx-post`` → ``hx-swap="morph:innerHTML"`` into
``#pdb-status``), not as form POST + redirect, so the page never reloads and
never jumps to the top; the route branches on the ``HX-Request`` header,
returning ``_service_pdb_result.html`` (the grid plus an ``hx-swap-oob``
notice into ``#pdb-flash``, which lives *outside* the polled region so the
outcome survives the 5 s refresh) and falling back to the old flash-and-
redirect for a plain POST.  idiomorph rather than plain innerHTML matters
here: it keeps the cell ``<form>`` nodes (each carrying a stable ``id``) alive
across a poll swap, so a poll landing mid-toggle can't cancel the in-flight
write.  Confirmation is hooked into htmx's own ``htmx:confirm`` event (an
``onsubmit`` handler would fire too late): a single-channel toggle asks once
per browsing session (``sessionStorage`` ack, so bulk work isn't nagged),
while every ``data-confirm-always`` bulk write asks **every** time — a bus-
wide power-up is hundreds of amplifiers at once.  On the write side, power_db
has no per-channel endpoint, so a toggle is a read-modify-write of the chip's
whole OUT byte — the current byte is re-read immediately before the write and
confirmed by a fresh read after (other writers exist: power_db's own CLI),
with a mismatch reported as "state changed underneath us", never retried
silently.  ``set_group`` is the same operation with the whole byte as the
"modify" (0xFF / 0x00), one write per chip, skipping chips already correct and
verifying all of them in one readback; partial failures are reported with the
offending chips named, never silently retried.  The write side never depends
on the read decode, so a framing regression would surface as a loud verify
failure, not a wrong channel.

## Master PDB channel map

the dish-input ↔ power-channel wiring lives in **one CSV**
(``<configs_dir>/pdb_map.csv``, overridable via ``pdb.map_file``), parsed by
``choco/pdbmap.py`` and held as a ``PdbMapFile`` on ``app.config`` that re-
reads on mtime/size change (same "edit the file, no restart" rule as the
kotekan configs).  It is the *master* table, deliberately not copied: its
``dish_input`` values label every grid cell on the PDB page (``● A1X`` rather
than ``on``), and it is served at ``GET /api/pdb/map`` — where
``jobs/bffs/sources/power.py`` now reads it from (explicit ``map:`` in the
bffs source config still overrides; the bundled ``power_map.csv`` is the
fallback for dry runs and choco outages).  The label column is canonically
``dish_input`` with ``correlator_input`` / ``label`` accepted as aliases, and
``to_dict`` emits both names so either consumer works.  Parsing is row-at-a-
time resilient in the same spirit as ``Registry.reload``: ``#``-comments and
blank lines are stripped (the table is hand-maintained), and a bad address /
missing label / duplicate address is collected into ``errors`` with the
**original file line number** and skipped, so one typo costs one channel, not
the page.  ``cross_check`` compares the map against ``kotekan_dish_labels`` —
the ``dish_inputs`` table of a group's rendered config (``pdb.kotekan_group``,
else the first group), the same table kotekan indexes its bad-input mask with.
Only the 2026-08 per-dish layout is accepted (each dish named once, per-
element labels derived as label + X/Y; a pre-2026-08 per-element table is
refused with a migrate-this-config reason, since its element ordering was
wrong).  The two sides deliberately use different label sets:
``missing_in_map`` counts only **live** (``type: ArrayDish``) feeds — a
connected feed nobody can find the breaker for; ``unknown_to_kotekan`` checks
against **every** real-labeled dish, connected or not, so wiring recorded for
a not-yet-connected dish (the pathfinder's C/D rows, typed ``Fake`` with real
labels) is legitimate rather than stale; ``duplicate_labels`` (one feed
claimed by two channels) is map-internal.  Both the page and the API carry the
verdict, and bffs logs it; a disagreement is loud but never fatal, because a
label off the current element axis simply projects onto nothing — a stale row
leaves a feed unwatched, it cannot mis-flag one.  The cross-check renders
**once per page load**, outside the 5 s status poll, because it re-renders a
kotekan config.  The deployed CSV is **deployment data, not a repo artifact**
— there may be no upstream source for the wiring, so the deployed copy can be
the only authoritative record.  ``choco.sh install`` therefore gives it the
``config.yaml`` treatment, not the kotekan-config treatment:
``copy_repo_configs`` excludes it from every copy (including ``--overwrite-
configs``) and ``deploy_pdb_map`` seeds it once, afterwards staging a
differing repo copy as ``pdb_map.csv.new`` rather than replacing it.  It must
be seeded *after* the configs block, since seeding first would make a fresh
install's configs dir look non-empty and turn the first install into an
overwrite prompt.  The sync loop's mtime scan ignores both files (it globs
only ``.yaml``/``.yml``/``.j2`` plus ``.updatable/*.json``), so a staged
``.new`` sitting in configs_dir is inert.
