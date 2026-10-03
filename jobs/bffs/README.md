# bffs

`bffs` is a **feed-flagging script**. Run once per invocation — by a systemd
timer, a cron job, or by hand — it reads the feed labels and autocorrelation
data from one kotekan N² output file, decides which feeds (correlator inputs)
are **bad**, and POSTs that bad-feed list to **choco** (the CHORD config
orchestrator), which relays it to every `kotekan` node in the configured group
so downstream processing can exclude them.

There is no daemon: one pass — read the data, combine, send — then exit. A small
JSON file records the feed change history and lets it send to choco only when the
bad list changes; a second one, rewritten every run, records how the run went
(which sources measured, which abstained and why — see
[Run file](#run-file)). It is a small package — a core (`bffs.py`), the kotekan reader
(`kotekan_io.py`), and one module per source under `sources/` — on the standard
library plus four packages (`numpy`, `h5py`, `hdf5plugin`, `PyYAML`).

`bffs` keeps the *source ideas* of CHIME's `ch_flag` (see the
[prior-art appendix](#appendix-prior-art--chimes-ch_flag)) but shares none of its
code — `ch_flag` is a long-running tornado/asyncio server with a REST API, an
HDF5 archive, and per-source hysteresis; `bffs` is the same job done as a
stateless script.

## Run

bffs lives in the [choco](../../README.md) repo and runs from choco's venv
(numpy, h5py, hdf5plugin, and PyYAML come with it):

```sh
../../.venv/bin/python bffs.py --config bffs.example.yaml  # read data, combine, POST to choco
../../.venv/bin/python bffs.py -c bffs.example.yaml -n     # dry run: print the payload, send nothing
../../.venv/bin/pytest                                  # the tests (or: ../../choco.sh test)
```

`-v`/`-vv` raise the log level; `--kotekan-file` overrides the kotekan N² output
path. With no `choco.url` set (or `--dry-run`) the script prints the JSON payload
to stdout instead of sending it. See `bffs.example.yaml` for an annotated config.

Exit codes (the shared choco job convention): 0 = success, 2 = *degraded* —
the script is fine but a dependency or input wasn't (no/stale kotekan data so
file-based sources were skipped, choco or every node unreachable; the next
timer tick retries), 1 = a config error or bug that needs a human. choco's
BFFS badge renders these as green / yellow / red.

### As a systemd timer

choco ships the units: `choco-bffs-flag.service` (oneshot) paired with
`choco-bffs-flag.timer` (every 30 s), installed and enabled by
`choco.sh install`. The service runs `jobs/bffs/bffs-flag.sh` against
`/etc/choco/bffs.yaml` (seeded from `bffs.example.yaml` on first install).
The timer interval is the flagging cadence — there is no internal scheduling.

## How it works

```
   kotekan N² output (hdf5N2Write) — feed labels (index map) + autocorr data
                          │
                          ▼  kotekan_io.read_labels()  (the feed axis)
   ┌──────────────────── sources/ ───────────────────┐
   │ manual · power-outlier · power · fpga · rfi       │  each → per-feed good/bad mask + a report
   └────────────────────────┬─────────────────────────┘
                          ▼  AND the good masks  (a feed is bad if ANY source flags it)
                 diff vs state.json ── unchanged ─▶ done ─┐
                          │ changed                       │
                          ▼                               ▼
              push {update_id, start_time,    + append the transition     write run.json
              bad_inputs} → choco → kotekan     to state.json's history   (every run)
```

The flag values are tiny: `{update_id, start_time, bad_inputs}`. `start_time`
is `now + sync_delay` (slightly in the future) so every consumer switches flags
at the same moment. All the real work is in *how each source decides what is
bad*. (`bad_inputs` are positions in the file's feed-label list — for CHORD,
element indices in the `[pol][dish]` order, which `kotekan`'s
`bufferBadInputs` stage turns into the RFI-kernel feed mask.)

They travel through choco's group-update API: bffs POSTs
`{"action": "updatable_config", "endpoint": "updatable_config/bad_inputs",
"values": {…}}` to `<choco.url>/update/<choco.group>`, and choco pushes the
values to `POST /updatable_config/bad_inputs` on every kotekan node in the
group (kotekan validates that all three value keys are present). choco
bypasses auth for localhost callers and serves a self-signed certificate
(bffs skips TLS verification), so run bffs on the choco host.

Per-node delivery is choco's job, and it absorbs node outages: the group
POST fans out to per-node queues, so a down node never blocks its peers;
the flag values persist in choco's `.updatable/` store as that node's
desired state; and choco's poll loop re-pushes them as updatable-config
drift once the node is reachable again — a recovering node catches up
within one poll interval without bffs re-sending. If choco itself is
unreachable, the run fails (red badge) and, because the state file is
written only after a successful send, the next timer tick retries.

**Feed labels** come from the kotekan config's `dish_inputs` table, fetched
through choco (`GET /api/config/<group>`). Only the 2026-08 per-dish layout
is accepted: the table names each *dish* once (`A1`) and the element axis is
`[P][D]` (`element = dish_idx + pol * num_dishes`, `num_dishes`/
`num_polarizations` read from the config), so bffs derives per-element
labels as label + `X`/`Y` (pol 0 = X) — the same names the label-keyed
hardware maps (pdb_map.csv, fpga_map.csv, manual overrides) use. A
pre-2026-08 per-element table (a polarization marker in the label text —
`A1X`, `d0_pA`; `choco.dishlabels.labels_are_per_element`) is REFUSED with a
degraded exit: those tables carried a wrong element ordering, so indexing
kotekan's bad-input mask with them would flag the wrong feeds; the run
resumes by itself once the config is migrated. The labels shown name
exactly the elements the `bad_inputs` indices address. The N² file's own
label table is the fallback (dry runs, choco down). Duplicate placeholder
labels are made per-element (`Fake[7]`) so label-keyed state stays exact.

The file's axis and the flag axis are two different things, joined by
label and never by position. The `subset/` files bffs reads in production
are compact `DishInputs` frames over the wired elements only — 48 of the
correlator's 128 (16 dishes and 8 RFI antennas, both polarizations), in
the receiver's own order — so the power-outlier source judges the file's
own axis and projects each element's verdict onto the flag axis by name;
the 80 feeds the file does not carry are unmeasured and stay good (the
run file counts them, `n_in_file` / `n_not_in_file`). Every label the file
carries must be on the flag axis (`bffs.file_axis_mismatch`): one that is
not means the file was written under a different `dish_inputs` table (it
predates the running config), and then the file is sidelined like a stale
one — the file-based sources are skipped with the reason in the run file,
the other sources still flag — rather than trusting the labels that happen
to match.

bffs reads CHORD `hdf5N2Write` output (`index_map/label`, `vis[freq,
prod, time]`, compound freq, `frames_added` validity). Since kotekan
chord.2021.10+988 (acquisitions from 2026-09-11 on) `index_map/label` is
per *element* — one entry per element of the file's axis, with
`index_map/pol` alongside, spelled dish label + `p1`/`p2` up to kotekan
PR #1695 and dish label + `X`/`Y` from then on (files from 2026-10-01) —
and `kotekan_io.read_labels` spells either in the same `X`/`Y` names the
config derives (`choco.dishlabels.file_element_labels`), so the two lists
compare directly. Any other file layout (a per-dish table, CHIME-style
`index_map/input`, pre-2026-08 `A1X` labels with no `index_map/pol` to
vouch for them) is refused with a degraded exit rather than expanded or
guessed at.
Products beyond the element axis are ignored, and elements the file's
product list never correlates (unwired slots in a `DishInputs`-layout file)
are reported as unmeasured — `power-outlier` leaves them good rather than
flagging 96 unwired slots as "dead" (kotekan's baseline mask already covers
them; a feed the products *do* cover but that reads nothing is still
dead-and-bad). `kotekan_file` may be a glob, spanning directories if needed
(`subset/acq_*/*.h5`) — each run reads the newest match by mtime, i.e. the
most recently written file of the current acquisition (`bffs.newest_file`
compares the matches' directories first and stats only the newest one's
files, so the lookup does not grow with the 15,000-file archive). kotekan
writes a file in `.partial/` and renames it into the acquisition directory
when it is complete, ~3.5 min of frames later, so the newest file is
complete and the verdict lags the sky by up to that much. If that newest
file is missing or older than `max_age` seconds (default 3600; 0 disables),
it is treated as unusable — a stopped acquisition's empty tail rows would
mark every feed dead — and the **file-based sources (power-outlier) are
skipped with a warning while the rest still flag**. Only when *every*
configured source ends up skipped does the run fail (red badge): nothing
measurable is a systematic problem, not an all-good.

### Sources

A source produces a length-`nfeed` boolean good-mask (`True` = good). They are
AND-ed: a feed is bad if any source flags it.  Beside the mask a source returns
a **report** (`sources.common.report`): `ok` or `degraded`, a reason, how many
feeds it actually judged (`n_measured`), and free-form detail.  The rule every
source follows: **a feed it cannot measure is left good and the shortfall is
reported, never flagged.**  No data in the window, an endpoint down, too little
of the band present — none of these is evidence about a feed, and turning them
into flags is how a stopped acquisition once marked all 128 elements bad for a
day (see the run-file section).  The core adds `skipped` reports for sources it
did not run and fails the run only when *every* source is skipped or measured
nothing.

| Source | Evidence | What it flags |
|---|---|---|
| `manual` | a watched override file (`bad_inputs:` list of labels), edited by hand or by clicking choco's BFFS element grid | feeds an operator marked bad |
| `dish-type` | the kotekan config's `dish_inputs` `type` per dish, via choco | every element of a `Missing` dish — a slot with no dish behind it is bad by construction (`bad_types`; RFI antennas are not flagged) |
| `power-outlier` | kotekan N² output (`hdf5N2write`) → per-feed band-averaged power | main-array feeds whose power is an outlier across the other main-array feeds (`RFIDish` elements are neither compared nor flagged) |
| `power` | the power controller's live `/channel_states` (power_db) joined to choco's master PDB table | feeds whose amplifier is unpowered, and feeds the table has no channel for at all (not installed: flagged *absent*) |
| `fpga` *(provisional)* | the F-engine `raw_acq` metrics (pychfpga) | feeds with FFT overflow, no frames, or out-of-range ADC RMS |
| `rfi` *(provisional)* | kotekan's per-feed spectral kurtosis (RfiSKMetrics `/sk` endpoints) | feeds whose SK sits persistently away from 1 (RFI or a broken signal chain) |

Each source is a module under `sources/` exposing `mask(src, labels, kotekan_file)`
(`True` = good); `bffs.combine_sources` dispatches via `sources.get(kind)`.
`manual` reads its override file — YAML or JSON, `bad_inputs: [A1X, ...]`
or a bare list.  By default it is `manual_overrides.yaml` in the state
directory, the file choco's BFFS page edits when an element in the grid is
clicked (choco checks the label against the element axis and the path
against what this source reported reading in the run file, and rewrites the
file atomically, keeping other keys); `path:` names another file instead. `power-outlier` reads the kotekan
file. `power`, `fpga`, and `rfi` poll an external service. `power` and `fpga` join to the feed
labels through a channel→input map: `fpga`'s map is the real rack wiring table
(Slot 1–4 × ADC1–8 → feed), keyed to `raw_acq`'s 0-based slot/chan metric labels
(remaining assumptions — crate 0, ADC N → chan N−1 — await a live F-engine);
`power`'s map is choco's **master PDB channel table** (`GET /api/pdb/map`,
backed by one CSV beside choco's `nodes.yaml`), so this job and choco's PDB
page read the same wiring instead of each keeping a copy; choco cross-checks
that table against kotekan's `dish_inputs` and the verdict is logged on every
run. The table is also read as the inventory of what is wired: a feed on the
element axis with no channel in it (the E/F/G/H dishes before they are
installed, placeholder elements) has nothing to power and is flagged **not in
PDB table** — ch_flag's `layout` idea — so the grid shows it as bad with that
reason on hover. That rule applies only when the table can be trusted: an
operator's `map:` CSV, or choco's table when the cross-check finds no row
naming a dish input kotekan does not know (a stale or renamed label would
otherwise make real feeds look unmapped). With a stale table, or the bundled
`sources/power_map.csv` fallback (dry runs, choco down), unmapped feeds are
left good and the run reports degraded saying how many and why. Each flagged
feed carries its reason (`off`, `unread`, `not in PDB table`) in the report's
`feed_reasons`; the core copies it into the state file's `flag_reasons` for
the page. `rfi` needs no map: kotekan's `RfiSKMetrics` stage serves a JSON
`/sk` endpoint whose arrays are indexed by element — the feed's position in the
label list. By default `rfi` derives its endpoints from choco's node registry
(`GET /api/nodes`): every *started* node of the broadcast group, polled at each
`sk_paths` entry (which must match the kotekan config's RfiSKMetrics
instances); explicit `urls` override. A node choco's sync loop reports `down`
or `idle` is not polled at all (its band is simply unmeasured this run), and an
endpoint that fails anyway is skipped — one down X-engine node doesn't stall
flagging for the rest — but both are reported `degraded` with the node named.
The `/sk` values are moving averages that freeze when a stage stops receiving
frames, so each node's `/metrics` is read once for the SK gauges' last-update
timestamps and an instance older than `max_stale_s` (default 60 s; 0
disables) is ignored and reported *stale*; with no `/metrics` the freshness is
unknown and the readings are used. Nothing measurable at all (no node up,
every endpoint down or stale) is a `degraded` report with nothing judged; the
core fails the run only if every source ends up that way. kotekan computes the
single-feed SK for every feed regardless of the current bad-feed mask, so an
`rfi`-flagged feed keeps being measured and heals on recovery. `power` and
`rfi` are live in the deployed config; `fpga` is built and tested (runnable
standalone, `python -m sources.fpga`) but awaits the F-engine.

`dish-type` is inventory rather than measurement, like the power source's
*absent* rule: the config types every dish `ArrayDish`, `RFIDish` or
`Missing`, and an element of a `Missing` dish (the C–H rows today, 80
elements) has nothing to receive with, so it is bad whatever any data says
— and the `subset/` files do not carry those elements at all, so without
this source nothing would ever flag them. kotekan's own baseline mask
already excludes them; bffs flags them too so its bad list, the grid and
the history say what is a feed. The core hands every source the config's
`{label: type}` map (`dish_types`); with no config the source abstains.

The main heuristic, `power_outlier_mask`, reduces the most recent *filled* time
rows to one power level per feed (a weighted average over time and a frequency
band), then flags any feed sitting more than `nsigma` from the median of the
other feeds — using the median absolute deviation as the spread, so a few bad
feeds don't skew the threshold — plus any dead feed (no valid/positive data) or
any feed outside the absolute bounds. Only main-array dishes take part:
elements whose dish type is in `exclude_types` (default `RFIDish`) enter
neither the median nor the spread and are never flagged — the RFI-monitor
antennas are real receivers pointed at the horizon, and their power says
nothing about them as feeds of the array (16 of the subset file's 48
elements; left in, a hot one would be flagged and a dead one too).  Two guards keep a thin data stream from
reading as dead feeds.  The reader ends the window at the newest row holding
any frame, skipping the empty tail a stopped acquisition leaves.  Then, before
any feed is judged, the source measures **band coverage** — the fraction of
(time, frequency) cells in the band the receiver actually filled, a property
of the stream (which X-engine nodes delivered), not of any feed — and
abstains below `min_coverage` (default 0.25: one node is 1/8 of the band, so
the source keeps working with most of the cluster down, while a tail file with
one live channel in thousands abstains).  `min_valid_frac` is per feed and
relative to the delivered cells, so a band only partly delivered costs every
feed the same cells and counts against none of them.

### State & change history

bffs keeps a small JSON file, `state.json` in its state directory
(`/var/lib/choco/bffs`, systemd's `StateDirectory`; `--state-dir` redirects a
hand run), recording
the feed change history — and sends to choco only when the bad list changes. It
holds the current bad list (by stable feed *label*, not index), the element
axis those labels sit on (`labels`, one per element in kotekan's order — what
the payload's indices address, and what choco's service page draws its
element grid from; a state file from before it was recorded gains it on the
next run) and an append-only `history` of transitions:

```json
{
  "updated": 1700000077.7,
  "update_id": "bffs-1700000077750",
  "bad_inputs": ["A1X"],
  "labels": ["A1X", "A2X", "A1Y", "A2Y"],
  "flagged_by": {"A1X": ["power-outlier", "manual"]},
  "history": [
    {"time": 1700000077.4, "update_id": "bffs-...", "became_bad": ["A1X"], "became_good": [], "bad_inputs": ["A1X"]}
  ]
}
```

`flagged_by` records which source(s) flagged each currently-bad feed (as of
the last change), and `flag_reasons` the reason a source gave where it gave
one (`{"E01X": {"power": "not in PDB table"}}`) — display bookkeeping for
choco's BFFS page, whose grid shows "bad (power: not in PDB table)" on hover.
The payload
sent to kotekan is unchanged: exactly `{update_id, start_time, bad_inputs}`
with integer element indices.

Each run diffs the current bad set against the file: a change POSTs to choco and
then appends one history entry, rewriting the file (atomically); an unchanged
run does nothing. The send comes *before* the state write, so a failed send
leaves the state untouched and the next run retries. `--force` re-sends the
current list even when unchanged (e.g. to re-sync choco after a restart);
`max_history` caps the kept entries (0 = keep all).

### Run file

The state file says what the flags *are*; it cannot say how they were measured,
because it changes only when they do.  So every non-dry run also rewrites
`run.json` beside it with how it went:

```json
{
  "time": 1790873972.8, "status": "degraded", "exit_code": 2, "error": null,
  "degraded": ["no usable kotekan file — skipped: power-outlier"],
  "kotekan_file": ".../vis_0004227923_20260920T_203902_494116150.h5",
  "kotekan_file_age_s": 938880.0,
  "kotekan_file_reason": "last written 260.8 h ago, older than max_age 3600 s",
  "n_elements": 128, "n_bad": 36, "sent": false, "update_id": "bffs-1790873972777",
  "sources": [
    {"kind": "manual", "status": "ok", "reason": null, "n_measured": 128, "n_flagged": 0,
     "detail": {"path": "/data/bffs/manual_overrides.yaml", "exists": false, "n_listed": 0}},
    {"kind": "power-outlier", "status": "skipped", "reason": "no usable kotekan file: ...",
     "n_measured": 0, "n_flagged": 0, "detail": {}},
    {"kind": "power", "status": "ok", "n_measured": 80, "n_flagged": 36,
     "detail": {"n_mapped": 80, "n_watched": 80, "n_unpowered": 36,
                "n_unmapped": 48, "unmapped_flagged": true,
                "feed_reasons": {"B01X": "off", "E01X": "not in PDB table", "...": "..."},
                "map_source": "choco master table", "map_check": "ok", "...": "..."}},
    {"kind": "rfi", "status": "ok", "n_measured": 47, "n_flagged": 0,
     "detail": {"n_endpoints": 12, "n_failed": 0, "n_stale": 0, "skipped_nodes": [],
                "endpoints": [{"url": "http://cx19...:12048/rfi_sk_metrics/sk_metrics_0/sk",
                               "ok": true, "age_s": 0.0, "n_measured": 47, "n_flagged": 0}, "..."]}}
  ]
}
```

`status` follows the exit code (`ok` / `degraded` / `failed`); `error` is the
one-line exception of a failed run, with whatever sources had reported before
it.  `n_measured` is what tells "ran and flagged nothing" from "had nothing to
measure".  choco's BFFS page renders the file as a *Last run* line and a
*Sources* table, and puts the `degraded` reasons in the badge's tooltip, so a
yellow badge says which input was unavailable.  Writing it is best-effort: a
failure is logged and never changes the exit code.

### Config

A single YAML file (see `bffs.example.yaml`): the `kotekan_file` (the one N²
output that supplies both the feed labels and the autocorrelation data), a `choco`
block (`url` + `sync_delay`), an optional `state` block (`max_history`), and a
list of `sources`, each a `kind` plus its parameters.  Where the state lives is
not in the file: see the run-file section.  There
is no per-source cadence or hysteresis — the timer sets the cadence, and each run
is independent.

## What it deliberately isn't

`bffs` trades features for simplicity. Each of these was in the design and was
cut; the noted upgrade is small if you ever need it:

- **No daemon / no async** — systemd (or cron) drives the cadence; the script is
  synchronous, top to bottom.
- **No hysteresis** — a borderline feed can flip good/bad between runs. Debouncing
  needs memory across runs; the state file is there, so this is the natural next
  add if real flag chatter shows up.
- **No full HDF5 archive** — kotekan records the bad-feed lists it applies. bffs
  keeps only the lightweight JSON change history above, not a per-sample archive.
- **No runtime re-indexing** — the feed list is whatever the kotekan file's
  `index_map/input` says this run; a changed feed list is just picked up next run.
- **No connectivity from kotekan's `enabled` flag** — it's downstream of our own
  flagging (a latch); see the appendix. Live connectivity instead comes from the
  `power` source (the independent power controller).

## Appendix: prior art — CHIME's `ch_flag`

`bffs` generalizes CHIME's `ch_flag`, a CHIME-specific real-time correlator-input
flagging server. `ch_flag`'s ten sources are the menu of *ideas* `bffs` draws on;
the telescope-specific acquisition behind each is **not** carried over.

| `ch_flag` source | Idea | `bffs` status |
|---|---|---|
| `layout` | which feeds are connected/on | kotekan `enabled` dropped (latch); **`power`**: unpowered, or absent from the master PDB table |
| `manual` | operator overrides | `manual` (watched file) |
| `autovar` | outlier band-averaged autocorrelation deviation | **`power-outlier`** |
| `ampvar` | band-averaged gain-amplitude variance | covered by `power-outlier` |
| `power` | inputs whose amplifiers aren't powered | **`power`** (power_db, provisional map) |
| `rms` | low/high input RMS | partial: `power-outlier` abs bounds + **`fpga`** ADC RMS |
| `raw` | raw-ADC histogram/spectrum classifier | partial: **`fpga`** FFT overflow/saturation (live); full classifier not built |
| `rfi` / `noise` | RFI / radiometric-noise outliers | **`rfi`** (kotekan per-feed SK; noise variant not built) |
| `calibration` | gain-calibration failures | not built |

**Why no `connectivity`/`layout` source.** An earlier bffs draft derived
connectivity from the kotekan output's own `enabled` flag — but that flag is
*downstream* of flagging, so a feed disabled there would never be re-examined,
latching it bad forever. ch_flag avoided this by querying an independent layout
database; bffs gets the equivalent from the **`power`** source (the independent
power controller — not downstream of flagging, so no latch). The static-exclusion
job (permanently removing known-bad or non-antenna inputs) stays with the `manual`
override file, and `power-outlier` catches dead/disconnected feeds — all of these
self-heal when a feed recovers.

How `ch_flag` sent updates to coco (its choco): **on change**, not on a fixed
timer — it re-POSTed only when the combined bad-input list differed, detected by a
loop that ticked every `0.1 × min(source cadence)` (≈4 s with its defaults). bffs
keeps the "compute and hand off the bad list" core and drops the server,
hysteresis, archive, ephemeris excludes, REST API, and `ch_util`/`wtl`
dependencies.
