# bffs

`bffs` is a **feed-flagging script**. Run once per invocation — by a systemd
timer, a cron job, or by hand — it reads the feed labels and autocorrelation
data from one kotekan N² output file, decides which feeds (correlator inputs)
are **bad**, and POSTs that bad-feed list to **choco** (the CHORD config
orchestrator), which relays it to every `kotekan` node in the configured group
so downstream processing can exclude them.

There is no daemon: one pass — read the data, combine, send — then exit. A small
JSON file records the feed change history and lets it send to choco only when the
bad list changes. It is a small package — a core (`bffs.py`), the kotekan reader
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
   │ manual · power-outlier · power · fpga             │  each → per-feed good/bad mask
   └────────────────────────┬─────────────────────────┘
                          ▼  AND the good masks  (a feed is bad if ANY source flags it)
                 diff vs state.json ── unchanged ─▶ done
                          │ changed
                          ▼
              push {update_id, start_time,    + append the transition
              bad_inputs} → choco → kotekan     to state.json's history
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
through choco (`GET /api/config/<group>`), and both table layouts are
handled. Pre-2026-08 tables name every element: bffs rebuilds the same
element table kotekan did — every slot `Fake` by default, each `dish_inputs`
entry's `label` at its `dish_idx`. The 2026-08 kotekan layout names each
*dish* once (`A1`) and the element axis is `[P][D]` (`element = dish_idx +
pol * num_dishes`, `num_dishes`/`num_polarizations` read from the config),
so bffs derives per-element labels as label + `X`/`Y` (pol 0 = X) —
reproducing the old names, which keeps the label-keyed hardware maps
(pdb_map.csv, fpga_map.csv, manual overrides) working unchanged. The two
layouts are structurally identical, so they are told apart by the label
text: a polarization marker (`A1X`, `d0_pA`) means per-element
(`kotekan_io.labels_are_per_element`). Either way the labels shown name
exactly the elements the `bad_inputs` indices address. The N² file's own
index map is the fallback (dry runs, choco down); when both are available
they must agree — a mismatch (the file predates the running config) fails
the run rather than sending ambiguous indices. Duplicate placeholder labels
are made per-element (`Fake[7]`) so label-keyed state stays exact.

bffs reads both N² file flavours: CHIME-style (`index_map/input` labels,
`vis[time, freq, prod]`) and CHORD `hdf5N2Write` output (`index_map/label`,
`vis[freq, prod, time]`, compound freq, `frames_added` validity). A CHORD
file's per-dish label table is expanded to the element axis the same way as
the config's. Products beyond the element axis (the pre-2026-08 files'
phantom second-polarization elements) are ignored, and elements the file's
product list never correlates (unwired slots in a `DishInputs`-layout file)
are reported as unmeasured — `power-outlier` leaves them good rather than
flagging 96 unwired slots as "dead" (kotekan's baseline mask already covers
them; a feed the products *do* cover but that reads nothing is still
dead-and-bad). `kotekan_file` may be a glob, spanning directories if needed
(e.g. `full/acq_*/*.h5`) — each run reads the newest match by mtime, i.e.
the most recently written file of the current acquisition. If that newest
file is missing or older than `max_age` seconds (default 3600; 0 disables),
it is treated as unusable — a stopped acquisition's empty tail rows would
mark every feed dead — and the **file-based sources (power-outlier) are
skipped with a warning while the rest still flag**. Only when *every*
configured source ends up skipped does the run fail (red badge): nothing
measurable is a systematic problem, not an all-good.

### Sources

A source produces a length-`nfeed` boolean good-mask (`True` = good). They are
AND-ed: a feed is bad if any source flags it.

| Source | Evidence | What it flags |
|---|---|---|
| `manual` | a watched override file (`bad_inputs:` list of labels) | feeds an operator marked bad |
| `power-outlier` | kotekan N² output (`hdf5N2write`) → per-feed band-averaged power | feeds whose power is an outlier across the other feeds |
| `power` *(provisional)* | the power controller's live `/channel_states` (power_db) | feeds whose amplifier is unpowered |
| `fpga` *(provisional)* | the F-engine `raw_acq` metrics (pychfpga) | feeds with FFT overflow, no frames, or out-of-range ADC RMS |
| `rfi` *(provisional)* | kotekan's per-feed spectral kurtosis (RfiSKMetrics `/sk` endpoints) | feeds whose SK sits persistently away from 1 (RFI or a broken signal chain) |

Each source is a module under `sources/` exposing `mask(src, labels, kotekan_file)`
(`True` = good); `bffs.combine_sources` dispatches via `sources.get(kind)`.
`manual` and `power-outlier` read the kotekan file and work today. `power`,
`fpga`, and `rfi` poll an external service. `power` and `fpga` join to the feed
labels through a channel→input map: `fpga`'s map is the real rack wiring table
(Slot 1–4 × ADC1–8 → feed), keyed to `raw_acq`'s 0-based slot/chan metric labels
(remaining assumptions — crate 0, ADC N → chan N−1 — await a live F-engine);
`power`'s map is choco's **master PDB channel table** (`GET /api/pdb/map`,
backed by one CSV beside choco's `nodes.yaml`), so this job and choco's PDB
page read the same wiring instead of each keeping a copy; choco cross-checks
that table against kotekan's `dish_inputs` and the verdict is logged on every
run (a mismatch is a warning, not a failure — a label off the element axis
projects onto nothing, so a stale row leaves a feed unwatched rather than
mis-flagging one). Setting `map:` in the source config overrides it with a
local CSV, and the bundled `sources/power_map.csv` is the fallback for dry
runs and choco outages. The wiring itself is still a placeholder —
**pending the hardware database (padloper)**. `rfi` needs no map: kotekan's `RfiSKMetrics` stage serves a JSON
`/sk` endpoint whose arrays are indexed by element — the feed's position in the
label list. By default `rfi` derives its endpoints from choco's node registry
(`GET /api/nodes`): every *started* node of the broadcast group, polled at each
`sk_paths` entry (which must match the kotekan config's RfiSKMetrics
instances); explicit `urls` override. An unreachable node is skipped with a
warning — one down node doesn't stall flagging — but nothing measurable at
all fails the run (red badge): every endpoint unreachable, or no started
nodes in the group. kotekan computes the single-feed SK for every feed regardless of the
current bad-feed mask, so an `rfi`-flagged feed keeps being measured and heals
on recovery. All three are built and tested (and runnable standalone, e.g.
`python -m sources.power`) but not yet wired into a live config.

The main heuristic, `power_outlier_mask`, reduces the most recent time rows to one
power level per feed (a weighted average over time and a frequency band), then
flags any feed sitting more than `nsigma` from the median of the other feeds —
using the median absolute deviation as the spread, so a few bad feeds don't skew
the threshold — plus any dead feed (no valid/positive data) or any feed outside
the absolute bounds.

### State & change history

If the config has a `state.path`, bffs keeps a small JSON file there recording
the feed change history — and sends to choco only when the bad list changes. It
holds the current bad list (by stable feed *label*, not index) and an append-only
`history` of transitions:

```json
{
  "updated": 1700000077.7,
  "update_id": "bffs-1700000077750",
  "bad_inputs": ["A1X"],
  "flagged_by": {"A1X": ["power-outlier", "manual"]},
  "history": [
    {"time": 1700000077.4, "update_id": "bffs-...", "became_bad": ["A1X"], "became_good": [], "bad_inputs": ["A1X"]}
  ]
}
```

`flagged_by` records which source(s) flagged each currently-bad feed (as of
the last change) — display bookkeeping for choco's BFFS page. The payload
sent to kotekan is unchanged: exactly `{update_id, start_time, bad_inputs}`
with integer element indices.

Each run diffs the current bad set against the file: a change POSTs to choco and
then appends one history entry, rewriting the file (atomically); an unchanged
run does nothing. The send comes *before* the state write, so a failed send
leaves the state untouched and the next run retries. `--force` re-sends the
current list even when unchanged (e.g. to re-sync choco after a restart);
`max_history` caps the kept entries (0 = keep all). Omit the `state` block to
run stateless — every invocation sends.

### Config

A single YAML file (see `bffs.example.yaml`): the `kotekan_file` (the one N²
output that supplies both the feed labels and the autocorrelation data), a `choco`
block (`url` + `sync_delay`), an optional `state` block (`path` + `max_history`),
and a list of `sources`, each a `kind` plus its parameters. There is no per-source
cadence or hysteresis — the timer sets the cadence, and each run is independent.

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
| `layout` | which feeds are connected/on | kotekan `enabled` dropped (latch); live connectivity via **`power`** |
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
