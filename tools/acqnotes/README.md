# acqnotes: per-acquisition READMEs for the N² subset data

Writes a `README.md` and an `acq_info.yaml` into every
`/mnt/cs00/data/kotekan_vis_files/subset/acq_*` directory; choco's `/files` table shows
each README behind an Info button beside the acquisition.  `render.py` also copies
`timeline.yaml` into `subset/`, where the root's Timeline button shows it.  Each README states what is
known about that kotekan run: start and end time (UTC and Penticton local), kotekan builds, element axis and whether its labels are
physical, X/Y swaps, configured vs. measured pointing, RFI excision, fringestopping, and
whether the bad-feed mask matched the sky.

Not part of the choco package: these are offline scripts that run in a venv with the
`[jobs]` extra (h5py, hdf5plugin, numpy, astropy, PyYAML) and only read the data.

## Pieces

| file | does |
|---|---|
| `timeline.yaml` | Curated history: dish-table changes, swaps, deployments, audits.  **Edit this** when something new is learned. |
| `extract.py` | Metadata of five sampled files per acquisition: attributes, labels, embedded kotekan configs, flags, RFI fractions, autos. |
| `transit.py` | Autocorrelation rise of every input at the Cyg A, Cas A, Tau A, Vir A and Sun transits (440-460, 600-620, 1400-1420 MHz) and the hour angle of the peak. |
| `polswap.py` | Cross-pol vs co-pol amplitude of every product at the Cyg A (else Sun) transit, 610 MHz. |
| `zerovis.py` | Per input, the fraction of its baselines that are zeroed (an applied mask in files that do not record one). |
| `render.py` | Joins the above with `timeline.yaml` and writes the READMEs. |

The acquisition directories belong to `nobody:nogroup`; write them with sudo (render into
`--dry-run` output, then `sudo cp -r $W/out/. /mnt/cs00/data/kotekan_vis_files/subset/`).

## Wording

The README text is written in ASD-STE100 style: one statement per sentence (at most
about 25 words), active voice, simple verbs, the same term for the same thing.  Keep the
sentences natural and keep technical names as they are.  This applies to the `text` of
`timeline.yaml` entries and to the fixed strings in `render.py`.

## Running

```bash
PY=/home/jmertens/choco/.venv/bin/python   # any venv with the [jobs] extra
W=/path/to/workdir
ls /mnt/cs00/data/kotekan_vis_files/subset | xargs -P 4 -n 1 $PY -I extract.py $W/meta
ls /mnt/cs00/data/kotekan_vis_files/subset | xargs -P 3 -n 1 $PY -I transit.py $W/transit
ls /mnt/cs00/data/kotekan_vis_files/subset | xargs -P 2 -n 1 $PY -I polswap.py $W/pol
$PY -I zerovis.py $W/zerovis.json
$PY render.py --work $W --dry-run     # writes into $W/out for review
$PY render.py --work $W               # writes into the acquisition directories
```

The measurement steps take about an hour over NFS for ~45 acquisitions and only need
re-running for new acquisitions; `render.py` takes seconds and is what to re-run after
editing `timeline.yaml`.  An acquisition without a `meta/<acq>.json` (e.g. one still being
written) is skipped.

## Methods and limits

- **Times** come from the file names (UTC); local is America/Vancouver.
- **Labels**: the 2026-09-09 to 2026-10-01 20:18 files used a dish table reversed within
  each group of eight; `render.py` maps them to physical names (sky audit 2026-09-21).
- **Pointing**: which sources raise the autocorrelations at transit.  Cyg A in the main
  beam raises them ~60 % at 440-620 MHz; the Sun near the beam centre raises them 2-10x.
  E-W beam centres per dish are relative to the array median (the Cygnus region biases the
  absolute peak by a few degrees of hour angle).  Per-dish north-south offsets cannot be
  separated from per-input system temperature with one in-beam source, so a low sky
  response is reported as "weak", not as a pointing number.
- **Bad-feed flags**: an input is "dead" when it shows no transit at > 3 sigma while at least
  40 % of the array does; the mask is "correct" when it covers exactly the dead inputs
  (hot inputs excepted).
- **RFI**: config values are start-up defaults (first-stage excision is updatable since
  2026-09-17); the measured `frac_rfi` is the evidence of what was applied.
