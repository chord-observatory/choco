# Drift-scan sky maps (the mapmaker job)

Design rationale for ``jobs/mapmaker/`` (2026-10-05).  Historical: the
measurements are from the day it was built, on the subset acquisition of
2026-10-04 and the Cyg A transit in it.

## What it is for

An operator's quick look at the sky the pathfinder is seeing: a strip of
RA by Dec with frequency as hue, red at 300 MHz through violet at 1500 MHz
(16 log-spaced colour bins by default, every clean channel imaged), accumulated over
every pass (the **sky map**, Sun excluded) and over the last sidereal day
(the **daily map**, Sun included).  The daily map is as much a data-quality
display as a sky image — its difference from the sky map is RFI, dead feeds
and gain drift — which puts it in the same family as the waterfall and the
bffs grid.  Others produce maps at full spectral resolution; this job keeps
only what the picture needs: a few tens of colour bins, nothing persisted
finer.  (It began as three fixed RGB bands; the user asked the same day for
the whole clean spectrum folded in with hues running over the band, and
for a stretch that shows bright and faint sources together.)

## Why incremental scatter-add, not radivs or RingMapMaker

The drift-scan dirty map is ``Re Σ_t Σ_b w_b V_b(t) exp(-i k b·ŝ(t))``: a
sum over time with a per-pixel phase, linear in the visibilities.  Written
per *input bin* instead of per output pixel, a sample at sidereal bin *n*
affects only the pixels within a beam width, through a kernel
``K[f, b, θ, d] = B²(θ, d) exp(iφ_b(θ, d))`` that depends on geometry,
frequency and Dec but never on the day.  So the kernel is built once per
run and every new sample is a scatter-add into ``map`` and ``exposure``
arrays; stacking days, masking the Sun and dropping flagged bins fall out
of the exposure bookkeeping for free.

radivs (``chord-mapmaking``, ``DirtymapFromVisibilities``) computes the
same map as a circular FFT over 4096 hard-coded sidereal bins of the whole
day: every pixel depends on every bin, so it cannot take a file at a time,
and at full resolution it is an hour per polarization per day with ~180 GB
of memory.  It remains the batch reference the imaging agrees with.
Kotekan's ``RingMapMaker`` is the per-sample step alone — a matrix-vector
multiply per frame into a (time, sin za) ring buffer — written for CHIME's
north-south feed axis, 511 pixels and four-pol products, with a REST
endpoint and no file output; the idea ports, the stage does not.

## Conventions, validated on Cyg A

Fixed against files 11–50 of ``acq_20261004_012627_261398483`` (scripts in
``~/tmp/mapmaker-prototype/`` at the time):

* **Hour angle** from the file's own ``bin_ERA_deg`` plus the site
  longitude (``itrs_lon_deg``) minus the source's CIRS right ascension.
  CIRS, not ICRS: precession moves Cyg A ~0.1° and the self-cal would
  absorb it, but a consistent frame costs nothing.
* **Baselines** are ``feed_positions_m[input_b] − feed_positions_m[input_a]``
  with grid x east and y north; a source at ENU ``u`` phases as
  ``−k (b_x u_E + b_y u_N)``.  Of the four sign choices this gave 0.91
  coherence over the transit against 0.07 for the two with the EW sign
  flipped and 0.87 with only the NS sign flipped (two rows constrain it
  weakly, through the curvature of the fringe over the transit).
* **Fringestopping.** ``N2Accumulate`` stops each 10 s bin to the pointing
  centre at the bin's own ERA (``Telescope::fill_fringestop_phases_1d``):
  the sky frozen at the bin centre, exactly the kernel's model.  Nothing
  needs undoing.
* **The beam**: a Gaussian with FWHM 1.03 λ/D.  Cyg A's transit measured
  4.9°, 3.2° and 2.0° at 677, 1002 and 1402 MHz against 4.4°, 2.9° and 2.1°.
* **Pointing**: Cyg A transits 8.5° south of zenith, so the dishes point
  at dec ≈ 40.8, not the −27.3° co-elevation (Tau A) kotekan's config
  says.  The pointing is therefore a config value, not read from kotekan.
* A small unexplained offset: the transit arrives ~0.2° (50 s) early
  against the CIRS/ERA model in the upper bands.  Two pixels; harmless for
  a quick look, to be understood before absolute positions are trusted.

## Calibration

No gains exist for these data (eigencal has solutions in progress and
kotekan no consumer), and without per-channel phase calibration the
matched-filter sum over baselines is incoherent and the map is noise.  The
job therefore calibrates itself on the brightest point source transiting
through the beam.  Within ``calibration_window_deg`` (3° on the sky,
±16 min) of the calibrator the reducer accumulates, per channel and per
product, ``Σ w B² V e^{-iφ}`` and ``Σ w B⁴`` with the per-channel model beam
(one width across the band left a ±12% frequency-dependent amplitude bias
in the synthetic tests); when a sample past the window arrives the ratio
is the gain, kept until the next transit.  The normalisation makes a unit
source through the model beam the imaging template, so the maps are in
units of the calibrator's flux and the calibrator is white in the
composite — colour is spectral index relative to it.  Products below
``dead_fraction`` of their channel's median amplitude are dead feeds and
drop out (gain 0 → weight 0), which is what "flags are not perfect" asked
for: in the 10-04 data B01, B02 and B04 were flagged by kotekan and five of
twelve long EW baselines showed no transit.

Gains are per product, not per antenna (no closure assumption; enough
for a dirty map).  Uncalibrated delays were ~45° rms across a 3 MHz slab,
so gains must be applied per channel before the 64-channel sub-band
average, and the sub-band average must precede the redundant average.
The sub-band width is itself bounded by bandwidth smearing: the geometric
phase winds with frequency on the 44 m baselines, and 12 MHz keeps a
source at the beam edge coherent.  A colour bin is an average of sub-band
*maps*, never of visibilities.

Until the first transit has passed nothing can be imaged; the run exits 2
with "no gains yet" and consumes the files anyway, so a backlog loses only
the data before its first transit.  When kotekan consumes eigencal's
per-antenna gains, this job should read them instead.

## Two rows have Dec grating lobes

With dishes in two rows 8.5 m apart, every source has a copy every
λ/8.5 m in Dec: 4.8° at 420 MHz, 1.4° at 1400 MHz, inside a beam whose
FWHM is 0.69 of that spacing at every frequency.  Per colour bin the Dec
axis is ambiguous; the sum over the spectrum localises because the lobes
land in different places per frequency, and the composite shows them as
rainbow fans above and below each source, red outermost.  The RA axis is clean: the
44 m east-west aperture gives 0.4° at 1 GHz, with grating lobes at
λ/(6.3 m cos δ) well separated from the main lobe.

The normalisation matters here, and took two tries.  The maximum-
likelihood ``Σ W B⁴(j, d)`` per pixel amplifies lobes outside the beam and
put the prototype's peak one lobe south of Cyg A in every band.
``Σ W B²(j, d)`` looked right on Cyg A but the synthetic tests showed why
it is not: the beam taper cancels exactly, so every Dec lobe inside the
footprint is as tall as its source, and with a narrow colour bin (few
sub-bands to smear them) the argmax lands on a lobe.  The exposure is
therefore ``Σ W B⁴`` along the *pointing row*, the same for every Dec
row: insensitive to coverage like the others, a source on the pointing
row reads its flux, a lobe reads ``B²`` of its offset (14% at 420 MHz,
24% at 1400), a source off the pointing row reads ``~B⁴`` of its offset.
That is the dirty map's own taper, which is what the eye expects.

## RFI

In the 10-04 transit file kotekan's per-channel excision fraction was
33% at 350–400 MHz, 35% at 600–650, 49% at 700–750 (LTE), 14% at 750–800,
36% at 850–900 and ~10% with zero weights at 1150–1300; a slab at 742 MHz
was dominated by a stationary signal (constant phase on a 44 m baseline for
four hours) and never showed Cyg A's fringes.  Those ranges are excluded
in the example config, and ``frac_rfi`` per (channel, sample) handles the
rest.  Terrestrial RFI maps to horizontal stripes: constant in RA, periodic
in Dec through the NS baselines.

## Cost and placement

Reading 16-channel slabs of 70 files took seconds (``vis`` is chunked
``(16, 16, 20)``); a whole 1.6 GB file is ~16 s off NFS.  Kernels for 64
imaged sub-bands, 22 baselines, 81 Dec rows and footprints of 36–177 bins
are ~300 MB and build in a few seconds; the scatter is 20 samples × 64
sub-bands × 2 polarizations of small einsums per file, a few seconds.  The
accumulators are ~50 MB.  The job therefore runs on the choco host next to
the waterfall renderer, which already reads every new file within ~50 s of
each 2 min tick; the NFS read is the only real cost, and
``max_files_per_run`` the only effective throttle.

## Colour and stretch

Each colour bin is painted in the sRGB of the visible wavelength it maps
onto (Bruton's piecewise fit), frequency taken logarithmically from
650 nm at the bottom of the range to 440 nm at the top so no bin is
nearly black, and a pixel's colour is ``Σ_c y_c · rgb_c / Σ_c rgb_c`` over
the bins that have data there — a flat spectrum in the map's units is
white, a missing bin (an RFI range, exposure below the floor) leaves no
tint rather than a hole in the colour sum.  ``y_c`` is one stretch curve
shared by every bin, which is what keeps colour meaningful; per-bin
equalisation was rejected because it would make every bin's distribution
the same and erase the spectral colour.

The default curve was histogram equalisation above a noise floor
(median + 3 robust sigma) until 2026-10-07, chosen so that Cyg A and a
source at the floor both get their share of the range.  On the real map
it was what made Cyg A a bright vertical bar: the map is 1e5 deep, the
floor came out at 1e-4 of Cyg A, 98% of the pixels above it are fainter
than 2% of Cyg A, and a rank stretch therefore paints a 1% grating lobe
at 0.85–1.0.  Sixteen bins each painting their Dec ridge at full
intensity, at interleaved positions, fill a stripe.  The default is now
``arcsinh`` with the knee at 1e-3 of the calibrator, which keeps a 24%
lobe near 0.6 and a 1% one near 0.2; ``log`` and ``equalize`` remain
options.

## Passes and the daily map

Each bin of the sky is visited once per sidereal day, so "the last day" is
the current pass where it has exposure and the previous pass elsewhere.
``imaging.PassSlots`` keeps one accumulator pair per parity of the pass
index (``pass_index``: full turns of the local sidereal angle since J2000,
so the boundary is where the RA axis wraps); a pass more than one behind
is cleared when its slot is reused.  No ring buffer of visibilities and no
subtraction.

## Deconvolution (2026-10-07)

The vertical bar through Cyg A was reported as "something wrong".  Half of
it was the stretch (above); the other half is physics the stretch
exposed.  Per colour bin, the Dec column through Cyg A in the live
accumulators is exactly the two-row dirty beam: a central peak and a
cosine ridge under the beam envelope with lobes every λ/8.5 m at 1–30% of
the peak.  The ridge is continuous rather than a row of ghosts because
the same-row products — purely east-west baselines — carry no Dec
information at all and are 49% of the live weight (38 of 78 products per
polarization with B01, B02 and B04 dead); their contribution is the source
smeared over the primary beam in Dec, a pedestal of ~0.5 that the
cross-row cosine rides on.  The broadband sum over the bins does localise
(1.00 / 0.32 / 0.15 / 0.10 at 0 / 1 / 2 / 3°) because the ridges have
different periods, but the renderer never formed it.

There is no model-free filter for this: with one north-south spacing the
per-frequency Dec information is one cosine.  What removes it is
deconvolution, and the dirty beam is known exactly.  ``clean.py`` runs a
joint-bin Högbom CLEAN (Högbom 1974; the multi-frequency form is what
WSClean's joined-channel mode does):

* **The dirty beam from the kernels.**  For a source at Dec row ``j0``
  the response at ``(j, p0 + δ)`` is
  ``Re Σ_b W_b Σ_u K[b, j0, u] conj(K[b, j, u + δ])``, a correlation along
  the footprint done by FFT per sub-band, normalised by the exposure the
  same samples add, ``Σ_b W_b · Σ_d E[d]``.  ``W_b`` must be what the
  samples actually carried — gains squared, dead products, flags — so the
  fold-in now accumulates ``sky_blw[s, b]``, the weight per sub-band and
  unique baseline that reached the sky map, and records the geometry
  (site, baseline vectors, imaged sub-band centres) in ``maps.npz``.  The
  deconvolution rebuilds the kernels from that and the config without a
  data file.  The beam keys (``dish_diameter_m``, ``beam_fwhm_factor``,
  ``footprint_fwhm``) joined the grid signature for the same reason: a map
  accumulated through one kernel must never be cleaned with another.
  Files from before carry only the grid keys and are accepted once
  (``signature_matches``); the next save writes the full signature.
* **Joint-bin CLEAN.**  The peak search runs on the exposure-weighted
  broadband map, where the ridges average down, so a lobe is not mistaken
  for its source; at each peak the per-bin amplitudes are read and the
  per-bin dirty beams subtracted, so a component keeps its spectrum and
  the colour survives.  A bin with less than 0.1% of the pointing row's
  response at that Dec is left alone by the component (its value there is
  noise and dividing by the taper would amplify it).  Gain 0.25, stop at
  five robust sigma of the broadband map.
* **Restore** with one Gaussian beam for every bin (0.6° × 1.5°, unit
  peak so a component reads its flux as in the dirty map), plus the
  residual.  One beam for all bins is also what removes the colour
  fringing on point sources.

On the live accumulators (two sidereal days, 2026-10-07): Cyg A's column
goes from 1.00 / 0.35 / 0.11 / 0.13 at 0 / 1 / 2 / 3° to 0.99 / 0.31 /
0.05 / 0.03 (the 0.31 is the restoring beam), the residual there is
≤ 1e-3, and the γ Cygni supernova remnant lands within a pixel of its
catalogue position.  10,600 iterations over 47 beam rows take 44 s on the
choco VM (the dirty-beam rows are computed on demand and kept; the
subtraction uses slices, never fancy indexing).  It is still minutes
rather than seconds, so it is a separate unit, ``choco-mapmaker-clean``,
on a 15 min timer that reads the accumulators and self-gates on their
data time; the fold-in is never delayed.  Its facts go to ``clean.json``
(a degraded run keeps the last numbers and adds the reason) and choco
shows the deconvolved map first, the dirty maps beneath it.

Caveats.  The diffuse emission east of Cyg A (RA 300–310) is where Cyg X
and the Galactic plane are, but 1–2% of Cyg A is also where a slightly
wrong dirty beam would leave artefacts, since the per-product self-cal
only guarantees the model at Cyg A itself; check it against a 408 MHz map
before trusting it.  Negative residuals are not cleaned.  The daily map
is not deconvolved: it is the data-quality display and the ridge is
part of what it shows.  Clark major cycles, multi-scale components for
Cyg X and per-antenna gains from eigencal are the known next steps.

## Context image (2026-10-08)

`context.png`, drawn by the deconvolution step after `clean.png`, puts the
strip in its sky.  The backdrop is the 408 MHz all-sky map (the skymap's
backdrop, read through `choco.healpix`) on an RA × Dec grid at 0.25°.  RA
runs 360 → 0 like the strips, and Dec covers −40° to +90°, everything that
rises at DRAO.  It uses inferno on log T_b and is not faded.  The deconvolved
colour composite is pasted into its band with the band's edges outlined.  A
pixel with no data in any colour bin (`anyvalid`) is left transparent, so an
unobserved stretch of RA shows the 408 MHz sky rather than black.  The two
use different colour schemes on purpose: the strip's hue is frequency, the
backdrop's is brightness.

The strip's RA is the sidereal angle of date and the backdrop's is ICRS.
They differ by precession, about 0.36° in 2026, which is under two of the
backdrop's pixels.  The ICRS → Galactic rotation is the Hipparcos matrix,
with no astropy in the loop.

`background_map` names the file and defaults to the skymap job's copy
(`jobs/skymap/`, which `choco.sh install` fetches); `""` turns the image off.
A missing map costs only this image: the run reports it degraded (exit 2)
and retries every tick, since the self-gate wants every configured image.
A map in another layout is a config error (exit 1).  The fold-in never
touches it.

## Not done, by decision

Finer-band persistence ("reduce once, map many times") was dropped:
others produce full-resolution maps.  Matching the colour bins' resolution
before compositing (the upper bins smoothed to the lowest bin's beam) is a
display option left for later; the colour fringing on point sources is
accepted for now.
The maps are per pointing: a repointed array needs a fresh accumulator set
(any change to ``pointing_dec_deg`` starts one).
