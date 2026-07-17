# eigencal

`eigencal` is a **point-source gain-calibration script**. Run once per
invocation — by a systemd timer or by hand — it decides whether a calibrator
transit (Cyg A by default) has just completed, reads the transit from one or
more kotekan N² output files, fits a complex gain for every correlator input,
archives one HDF5 file, and POSTs the gains to **choco** (the CHORD config
orchestrator), which relays them to every `kotekan` node in the configured
group.

There is no daemon: one pass — read, eigendecompose, fit, send — then exit.
The archive directory doubles as the already-processed record, so the timer
can fire every ten minutes and the script exits in seconds on all but one run
per transit. It is a small package — the orchestration (`eigencal.py`), the
N² reader (`n2_io.py`), and the pure-numpy fit (`transit_fit.py`) — on
choco's existing dependencies (`numpy`, `h5py`, `hdf5plugin`, `PyYAML`,
`astropy`); no scipy, no CHIME packages.

`eigencal` keeps the *science* of CHIME's `ch_cal` point-source calibrator
(see the [prior-art appendix](#appendix-prior-art--chimes-ch_cal)) but shares
none of its infrastructure — `ch_cal` was a Python 2 tornado broker with
threads, a REST API, HDF5 archives, and four companion services; `eigencal`
is the same measurement done as a stateless script.

## Run

eigencal lives in the [choco](../../README.md) repo and runs from choco's venv:

```sh
../../.venv/bin/python eigencal.py --config eigencal.example.yaml     # real run
../../.venv/bin/python eigencal.py -c eigencal.example.yaml -n        # dry run: fit + archive, send nothing
../../.venv/bin/python eigencal.py -c ... -f --transit-time 1789000000  # reprocess a specific transit
../../.venv/bin/pytest                                                # the tests (or: ../../choco.sh test)
```

`-v`/`-vv` raise the log level. Exit codes (the shared choco job convention):
0 = success *or* nothing-to-do, 2 = *degraded* — the solution failed the
quality gate (archived but not sent) or a dependency wasn't available (no N²
data covering the transit, choco not up; the next tick retries), 1 = a config
error or bug that needs a human. choco's EIGENCAL badge renders these as
green / yellow / red. See `eigencal.example.yaml` for an annotated config and
`eigencal_feeds.example.yaml` for the feed layout file.

### As a systemd timer

choco ships the units: `choco-eigencal.service` (oneshot) paired with
`choco-eigencal.timer` (every 10 min), installed and enabled by
`choco.sh install`. The service runs `jobs/eigencal/eigencal.sh` against
`/etc/choco/eigencal.yaml` (seeded from `eigencal.example.yaml` on first
install). The timer is *not* the calibration cadence — the script self-gates
and does real work only once per transit, at night.

## How it works

```
                 previous transit of the calibrator (astropy)
                          │  completed recently? at night? not yet processed?
                          ▼
   kotekan N² output (hdf5N2Write) — newest file(s) covering the transit
                          │  n2_io: labels, freq, time, vis products,
                          │  frames_added, per-input `flags`  (kotekan's masks)
                          ▼
   per (time, freq, pol): visibility matrix → eigh → response = √λ · v
                          │  dynamic-range gate: λ_on / median λ_off
                          │  phase-reference · fringestop · ÷√(source flux)
                          ▼
   transit_fit.fit_transits — polynomial fit of log-amp & phase vs hour angle
   (batched over all freq × input), χ²-gated, raw-transit fallback
                          │
                          ▼  gap-fill over frequency, final good-fraction gate
        one HDF5 in archive_dir  +  POST {update_id, start_time,
                                          gain, weight} → choco → kotekan
```

The gains use the multiply convention (multiply visibilities by
`g_i · g_j*` to calibrate, amplitudes referenced to the source flux so
calibrated data come out in Jy); `weight` is the per-cell inverse variance
from the fit, `0` marking cells with no valid gain. Arrays travel
base64-encoded (`{shape, dtype, encoding, data}`) inside choco's group-update
API: eigencal POSTs `{"action": "updatable_config", "endpoint":
"updatable_config/gains", "values": {…}}` to `<choco.url>/update/<group>`,
and choco pushes the values to `POST /updatable_config/gains` on every
kotekan node in the group. `start_time` is `now + sync_delay` so every
consumer switches gains at the same moment. As with bffs, run it on the choco
host (localhost callers bypass auth; TLS to choco's self-signed cert goes
unverified).

### What it measures

For each polarisation the visibility matrix of an unresolved dominant source
is rank one: `V_ij ≈ r_i r_j*`, where `r_i` is input *i*'s complex response
to the source. Eigendecomposing the per-pol matrix at every (time, freq)
gives `r = √λ·v`; the **dynamic-range gate** (largest eigenvalue on-source
over its median off-source, both from the same file read) keeps only samples
where the source truly dominates. The response is then phase-referenced to a
chosen feed, **fringestopped** (the source's geometric phase is removed using
the feed positions, so gains are boresight-referenced), and normalised by
`√flux`. What remains varies over the transit only through the primary beam
— so log-amplitude and phase are each fit with a polynomial in hour angle
(iteratively reweighted least squares in a moving window around the fitted
peak, batched over every (freq, input) series at once), and the gain is the
inverse of the fitted response evaluated at the beam peak. Fits failing the
χ²/dof gate fall back to the raw transit sample; channels flagged only
during the transit are filled by linear interpolation over short frequency
gaps so they stay calibrated for the rest of the day.

### Flagging is kotekan's job

eigencal derives no bad-input lists (that's [bffs](../bffs/README.md), and
kotekan applies the result). It *consumes* the masks kotekan writes into the
N² file — the per-input `flags` dataset and `frames_added` validity — plus
its own per-sample data-quality cuts (dynamic range, fit χ²). A
kotekan-flagged input simply comes out with gain 0 / weight 0. The one
solution-level decision is the final gate: if fewer than `min_good_frac` of
the (freq, input) cells produced a valid gain, the run exits 2 and sends
nothing — with no transition machinery downstream, a bad update must not
reach the beamformer.

### Config

A single YAML file (see `eigencal.example.yaml`): the `kotekan_file` glob, an
`observer` block (site position), a `source` block (name, J2000 coordinates,
flux polynomial — apparent place is computed per transit), a `telescope`
block (dish size and the `feed_layout` file), an `analysis` block (windows,
gates, fit degrees), and a `choco` block (`url` + `group` + `endpoint`).
The **feed layout file** (`eigencal_feeds.example.yaml`) maps the N² file's
feed labels to polarisation and EW/NS position and names one phase-reference
feed per polarisation — it replaces CHIME's layout database, and it is the
one place where "row *i* of the gain array is feed label *L*" is pinned down.

### Things to VERIFY on first live use

These encode conventions that cannot be checked without real CHORD data
(all marked `VERIFY` in the code):

- **fringestop sign** (`telescope.fringestop_sign`) — with the wrong sign the
  fitted phase winds rapidly with hour angle instead of sitting flat;
- **kotekan gain endpoint** (`choco.endpoint`) and the exact payload keys the
  kotekan gain-apply stage expects;
- the N² file's **time convention** (integration start vs centre) and the
  **shape of the `flags` dataset**;
- the **flux coefficients** (Perley & Butler 2017 values are pre-filled for
  Cyg A).

## What it deliberately isn't

Each of these was in `ch_cal` and was cut; the noted upgrade is small if it
is ever needed:

- **No timing/noise-source calibration** — `ch_cal`'s fast (10 s) delay
  tracking needed an injected calibration signal and a coefficient model.
  If CHORD adds one, it is a *separate* job POSTing a second updatable
  config; the gains here multiply cleanly with it.
- **No daemon / no server** — systemd drives it; there is no REST API, no
  in-memory state to query. The HDF5 archive is the record.
- **No gain transitions** — CHIME blended old→new gains over 300 s server-side.
  Here `transition_interval` is forwarded in the payload for kotekan to honour
  if its gain stage supports blending; otherwise gains step once per day.
- **No multi-calibrator ladder, no beam-ratio files** — one configured source.
  A second source is a second config + timer pointing at the same archive dir.
- **No Gaussian-process frequency interpolation** — linear fill over short
  gaps instead; swap in something smarter if wide RFI bands leave holes.
- **No historical-amplitude bad-input detection** — that idea now lives in
  bffs (`power-outlier` et al.), where flagging belongs.

## Appendix: prior art — CHIME's `ch_cal`

`ch_cal` (github.com/chime-experiment/ch_cal) ran CHIME's real-time complex
gain calibration as a tornado "calibration broker": a point-source calibrator
and a noise-source timing calibrator staged updates into a combining loop
that archived every update, dumped update files, and notified consumers
(via coco) with an `update_id` they then pulled. The point-source *analysis*
carried over here almost whole — eigenvector response extraction with an
off-source dynamic-range reference, phase referencing, fringestopping, flux
normalisation, the iteratively-reweighted polynomial transit fit with
χ²-gating and error propagation (`ch_cal/utils.py:fit_point_source_transit`,
re-implemented batched in `transit_fit.py`), gain interpolation over flagged
channels, and a validity gate before applying. Everything else — the broker
process, dataset-manager (comet) bookkeeping, shared-memory readers, gain
transitions, the flagging chain, layout-database queries, and the `wtl.*`
framework — is replaced by this directory plus choco.
