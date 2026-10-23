# acqnotes: per-acquisition READMEs for the N² subset data

An hourly choco job that writes a `README.md` and an `acq_info.yaml` into every
`/mnt/cs00/data/kotekan_vis_files/subset/acq_*` directory and copies `timeline.yaml` into
`subset/`.  choco's `/files` table shows each README behind an Info button beside the
acquisition, and the timeline behind the root's Timeline button.  Each README states what is
known about that kotekan run: start and end time (UTC and Penticton local), kotekan builds,
element axis and whether its labels are physical, X/Y swaps, configured vs. measured
pointing, RFI excision, fringestopping, and whether the bad-feed mask matched the sky.

The job only reads the data; its scientific stack is the `[jobs]` extra (h5py,
hdf5plugin, numpy, astropy, PyYAML).

## Pieces

| file | does |
|---|---|
| `acqnotes.py` | The job: finds new or grown acquisitions, runs the measurements, renders, writes the notes that changed. |
| `timeline.yaml` | Curated history: dish-table changes, swaps, deployments, audits.  **Edit this** when something new is learned. |
| `extract.py` | Metadata of five sampled files per acquisition: attributes, labels, embedded kotekan configs, flags, RFI fractions, autos. |
| `transit.py` | Autocorrelation rise of every input at the Cyg A, Cas A, Tau A, Vir A and Sun transits (440-460, 600-620, 1400-1420 MHz) and the hour angle of the peak. |
| `polswap.py` | Cross-pol vs co-pol amplitude of every product at each Cyg A, Tau A and Sun transit, 610 MHz. |
| `zerovis.py` | Per input, the fraction of its baselines that are zeroed (an applied mask in files that do not record one). |
| `verify.py` | Reads every file of an acquisition (attributes, all embedded configs, flags at 610 MHz, RFI fractions, gains id), so a mask that changes in a run is given as a fraction.  Files already read are not read again. |
| `render.py` | Joins the measurements with `timeline.yaml` and renders the notes. |

## How a run goes

1. List the `acq_*` directories and their `vis_*.h5` files (`.partial/` is ignored).
2. Measure, newest first and at most `max_acqs_per_run`: every acquisition not yet
   measured, every finished one whose files changed, and the newest one (kotekan may still
   be writing it) when its files changed and `remeasure_hours` have passed.  Each step's
   result is `<state dir>/<step>/<acq>.json`; `index.json` records which files it saw.
3. Render every measured acquisition together (missing cx configs and the fringestop state
   are inferred from neighbours) and write each `README.md`, `acq_info.yaml` and the root's
   `timeline.yaml` only if its text changed.  The README's generation date alone is not a
   change.  The newest acquisition's README says that it can still grow.
4. Write `run.json` (measured, backlog, written, degraded) for its section of choco's `/files` page (folded into the DATA tag).

Exit codes follow the jobs convention: 2 when the root or a file is unreadable (that
acquisition is retried next run), 1 for a config error or a measurement bug (the other
acquisitions are still done).

The acquisition directories belong to `nobody:nogroup` (kotekan's root, squashed by NFS),
mode 775, so the unit runs as root, which NFS squashes to the same `nobody`.  The state
directory is `/var/lib/choco/acqnotes`.

## Wording

The README text is written in ASD-STE100 style: one statement per sentence (at most
about 25 words), active voice, simple verbs, the same term for the same thing.  Keep the
sentences natural and keep technical names as they are.  This applies to the `text` of
`timeline.yaml` entries and to the fixed strings in `render.py`.

## Running by hand

```bash
jobs/run-job.sh acqnotes/acqnotes.py /etc/choco/acqnotes.yaml -v                # one run, as the timer does
jobs/run-job.sh acqnotes/acqnotes.py /etc/choco/acqnotes.yaml --out /tmp/notes  # render elsewhere to review
jobs/run-job.sh acqnotes/acqnotes.py /etc/choco/acqnotes.yaml --remeasure acq_20261012_005242_957492261
```

After editing `timeline.yaml` or `render.py`, reinstall: the next run rewrites every note
whose text changed, without re-measuring.  After changing a measurement script, name the
acquisitions it affects with `--remeasure` (or delete `index.json` to re-measure all; a full
backfill is roughly 5-10 min of NFS reads per acquisition).

## Methods and limits

- **Times** come from the file names (UTC); local is America/Vancouver.  A file holds 20
  ten-second bins until 2026-10 and 40 from 2026-10-10; the scripts read the count from the
  files.
- **Labels**: the 2026-09-09 to 2026-10-01 20:18 files used a dish table reversed within
  each group of eight; `render.py` maps them to physical names (sky audit 2026-09-21).
- **Pointing**: which sources raise the autocorrelations at transit.  Cyg A in the main
  beam raises them ~60 % at 440-620 MHz; the Sun near the beam centre raises them 2-10x.
  E-W beam centres per dish are relative to the array median (the Cygnus region biases the
  absolute peak by a few degrees of hour angle).  Per-dish north-south offsets cannot be
  separated from per-input system temperature with one in-beam source, so a low sky
  response is reported as "weak", not as a pointing number.
- **Mixed pointing**: from 2026-10-17 the dishes do not all point at the same declination
  (rows A-B at Cyg A, most of rows C-H at Tau A).  Each sky test therefore works on the
  dishes that see its transit, not on the array.  The pointing and flag tests use the dishes
  whose autocorrelations rise at the transit; elsewhere they say nothing, because a dish that
  points elsewhere and a dead dish look the same.  The cross-pol check takes the dishes
  whose fringes at a transit are within 4x of the strongest dish's, and judges each dish
  at the first transit that covers it.  A dish is swapped above cross/co 2 (healthy feeds
  give 0.07-0.45, swaps 4-10).  At the 2026-10-18 Tau A transit the Tau A dishes have
  fringes, but their autocorrelations show no rise.
- **Bad-feed flags**: an input is "dead" when it shows no transit at > 3 sigma while at least
  40 % of the array does; the mask is "correct" when it covers exactly the dead inputs
  (hot inputs excepted).
- **Reads are priced per chunk.**  Since 2026-10 kotekan chunks `vis` as 16 channels by
  every product by every time bin (42 MB at 128 elements), and HDF5 decompresses a chunk
  whole.  Each measurement therefore reads one aligned chunk where it needs a band: the
  autocorrelation medians come from three chunks at 1/4, 1/2 and 3/4 of the band, the
  transit curves from one chunk per band, the pol check and the zero-visibility check from
  one chunk near 600 MHz.  Reading the middle 3/4 of the band for the autos cost 12 GB a
  file.
- **RFI**: config values are start-up defaults (first-stage excision is updatable since
  2026-09-17); the measured `frac_rfi` is the evidence of what was applied.
