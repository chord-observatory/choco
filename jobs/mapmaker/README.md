# mapmaker

Drift-scan sky maps, frequency as colour, from the kotekan N² files under
`/mnt/cs00/data/kotekan_vis_files/subset/`, shown on choco's
`/service/mapmaker` page (the MAP badge).

Every run (choco-mapmaker.timer, every 2 minutes) folds the files that
landed since the last run into two maps on a sidereal grid, 4096 bins of
RA by a strip of Dec rows around the pointing, in log-spaced colour bins
that slice the whole spectrum and are displayed as hues from red (300 MHz)
to violet (1500 MHz) in one image:

- the **sky map**: every pass over the sky, with samples skipped while the
  Sun is within `sun_exclusion_deg` of the pointing;
- the **daily map**: the most recent pass over each part of the sky.

```sh
./mapmaker.sh /etc/choco/mapmaker.yaml          # one pass
python mapmaker.py -c mapmaker.yaml -n          # what is pending
python mapmaker.py -c mapmaker.yaml --max-files 0   # fold in everything pending
python mapmaker.py -c mapmaker.yaml --state-dir /tmp/maps -vv   # a hand run, elsewhere
```

## What a run does

1. **Pending files** are decided from filenames: kotekan's `vis_<idx>_…`
   index against the last index folded in (kept with the maps).  The first
   run starts at the newest acquisition, or at `backfill_from`.
2. **Reduce** (`reduce.py`): each file is swept once in 64-channel blocks.
   Per block, the array dishes' cross products are taken per polarization
   (RFI antennas and Missing elements excluded), weights are zeroed by the
   element flags kotekan applied, by the per-sample RFI fraction and by the
   configured bad frequency ranges; the calibrator transit's gain sums are
   accumulated per channel; the per-channel gains are divided out; the
   channels are averaged into one visibility per sub-band.
3. **Image** (`imaging.py`): redundant baselines are averaged (22 unique
   vectors for the 2 × 8 grid), then each sample is scatter-added through a
   precomputed kernel `K = B² exp(iφ)` over the beam's footprint into the
   `map` and `exposure` accumulators of its colour bin, for the sky map
   (Sun permitting) and for the current sidereal pass.
4. **Render** (`render.py`): `map / exposure` per colour bin, one stretch
   curve shared by every bin (`equalize` above a noise floor by default,
   or `log` / `arcsinh`), each bin painted in its hue and the hues summed
   per pixel over the bins that have data there, RA increasing to the
   left, as a raw strip PNG and a labelled figure with the frequency key;
   the daily map is the current pass where it has exposure and the
   previous pass elsewhere.

## Deconvolution

With two rows of dishes every source carries a raised-cosine ridge in
Dec: the same-row (east-west only) products are half the weight and have
no Dec information, the cross-row products add one cosine per frequency.
`clean.py` (`choco-mapmaker-clean.timer`, every 15 min, self-gating on the
accumulators' data time) removes it by Högbom CLEAN against the dirty
beam the map was made with — computed from the same kernels and the
per-baseline weights the fold-in accumulated (`sky_blw` in `maps.npz`),
never fitted.  The peak search runs on the exposure-weighted broadband
map (where the ridges average down), the subtraction per colour bin (so
each component keeps its spectrum), and the components are restored with
one 0.6° × 1.5° Gaussian beam for every bin plus the residual.  The
`clean:` block of the config sets the gain, the stopping threshold in
robust sigma, the iteration cap, the restoring beam and the stretch.

```sh
./clean.sh /etc/choco/mapmaker.yaml            # one deconvolution, if the map changed
python clean.py -c mapmaker.yaml --state-dir /tmp/maps -f -vv   # a hand run, forced
```

Outputs `clean.png`, `clean-strip.png` and `clean.json` (iterations,
threshold, residual sigma, the brightest components) beside the maps;
choco shows the deconvolved map first.  Until the fold-in has recorded
the geometry and baseline weights (the first run after an install) it
exits 2 and says so.

## Calibration

Self-contained.  The first configured calibrator whose transit passes
within `calibrator_max_offset_deg` of the pointing (Cyg A at dec 40.8) is
used: the samples within `calibration_window_deg` of it on the sky feed
per-channel, per-product sums `Σ w B² V e^{-iφ}` / `Σ w B⁴`, solved once the
window has passed and kept in `gains.npz` until the next transit.  A
product fainter than `dead_fraction` of its channel's median is a dead feed
and is excluded.  The normalisation makes `V / g` a unit-flux source through
the model beam, so the maps are in units of the calibrator's flux density
and it appears white in the composite; colour is spectral index relative
to it (steeper redder, flatter bluer).

Until the first transit has been seen the run exits 2 (degraded, "no
gains yet") and nothing is imaged; files are still consumed, so a backlog
run only loses the data before its first transit.  Gains are per product
(baseline), not per antenna: enough for a dirty map, and no closure
assumptions.  When eigencal's per-antenna solutions are consumed by
kotekan this job should switch to them.

## Why it is shaped this way

**Incremental, not batch.**  The time integration is a sum over sidereal
bins with a per-pixel phase, so it is linear in the visibilities and can be
done one sample at a time.  radivs' `DirtymapFromVisibilities` computes the
same map as a circular FFT over the whole day and cannot take a file at a
time; it is the reference the imaging was checked against, not the engine.
Kotekan's `RingMapMaker` is the per-sample step only, CHIME-specific and
in-memory.

**Per-channel gains, sub-band imaging, colour from maps.**  Cable delays
wind the phase by a turn every few MHz, so gains have to be applied per
channel before any averaging; the geometric phase winds with frequency on
the long baselines, so visibilities can only be averaged over ~12 MHz (a
64-channel sub-band) before sources at the beam edge smear; and a colour
bin is an average of sub-band *maps*, never of visibilities.

**Two rows of dishes have Dec grating lobes.**  The 8.5 m row spacing puts
a copy of every source every λ/8.5 m in Dec (4.8° at 420 MHz, 1.4° at 1400
MHz).  Per colour bin the Dec axis is ambiguous; only the sum over the
spectrum localises, because the lobes fall in different places per
frequency.  In the composite they show as rainbow fans above and below
each source, red outermost, tapered by the beam.

**Normalisation by the pointing row's beam.**  The exposure a sample adds
is `Σ W · B⁴` along the pointing row, the same for every Dec row.  That
keeps the map insensitive to how often a pixel was covered while leaving
the dirty map's natural taper: a source on the pointing row reads its
flux, a grating lobe reads `B²` of its offset.  Dividing by `Σ W B²(j, d)`
instead flattens every Dec lobe to the height of its source (the taper
cancels), and dividing by `Σ W B⁴(j, d)`, the maximum-likelihood
normalisation, amplifies them: in the prototype that put the peak one
lobe south of Cyg A.

**Conventions, fixed against a Cyg A transit (2026-10-05).**  Hour angle is
the file's `bin_ERA_deg` plus the site longitude minus the CIRS right
ascension; a baseline is `pos[input_b] − pos[input_a]`; a source at ENU
direction `u` has phase `−k (b_x u_E + b_y u_N)`.  That sign choice gave
0.91 coherence against 0.07 for the alternatives.  N2Accumulate fringestops
each 10 s bin to the pointing at the bin's own ERA, which is exactly the
frozen-sky-per-dump model the kernel assumes.

## Files

```
jobs/mapmaker/
├── mapmaker.py            # config, lock, pending files, calibration windows, state, exit codes
├── reduce.py              # one N² file -> masked, calibrated sub-band visibilities
├── imaging.py             # geometry, beam, kernels, scatter, redundancy, self-cal, passes
├── render.py              # accumulators -> RGB strip PNGs and labelled figures
├── clean.py               # the deconvolution: dirty beam from the kernels, joint-bin CLEAN, restore
├── mapmaker.example.yaml  # seeded to /etc/choco/mapmaker.yaml by choco.sh install
├── mapmaker.sh, clean.sh  # wrappers the units run
├── choco-mapmaker.{service,timer}         # fold-in, every 2 min
├── choco-mapmaker-clean.{service,timer}   # deconvolution, every 15 min
└── tests/                 # synthetic N² files with a known source, gains and dead feeds
```

State (`/var/lib/choco/mapmaker/`, systemd's StateDirectory; not a setting):
`maps.npz` (accumulators, per-sub-band baseline weights, geometry, the
last folded index), `gains.npz`, `calsum.npz` (a transit in progress),
`state.json` (per fold-in run; what choco's badge and page read),
`sky.png`, `daily.png`, `sky-strip.png`, `daily-strip.png`, and from the
deconvolution `clean.png`, `clean-strip.png`, `clean.json`; one lock per
step.  Changing the pointing, colour bins, grid or beam keys in the config
starts the accumulators afresh.

Exit codes (both steps): 0 ok or nothing to do; 2 degraded (a root or file
unavailable, no gains yet, no geometry recorded yet); 1 config error or
bug.
