# The jobs pattern and the EOP merge policy

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Jobs pattern

a "job" is a standalone script that pushes through choco's localhost JSON API
(auth is bypassed for localhost callers), keeps its state under the shared
``/var/lib/choco/<name>/`` namespace (``StateDirectory=choco/<name>``, so
systemd owns creation and ownership and exports the path to the job as
``$STATE_DIRECTORY``), and lives in its own ``jobs/<name>/`` directory (units and code together;
every unit's ``ExecStart`` is the one ``jobs/run-job.sh <name>/<script>.py
<config>``, which finds the venv), shipping as ``choco-<name>.service``
(oneshot) + ``choco-<name>.timer``, installed and enabled by the
``jobs/*/choco-*.{service,timer}`` glob in ``choco.sh install``.  Jobs share
an **exit-code convention**, read back by ``job_status`` via systemd's
``ExecMainStatus``: **0 = ok** (success or nothing-to-do), **2 = degraded** —
the job itself is fine but a dependency or input wasn't
(fpga_master/IERS/choco unreachable, no or stale N² data, an eigencal solution
failing its quality gate); retries self-heal, badge yellow — and **1 =
failed** — a config error or bug that needs a human; badge red.  Rule of thumb
inside a job's ``main``: ``OSError`` (network, missing files) → 2,
``ValueError``/``yaml.YAMLError`` (config, consistency) → 1.  The plumbing
every job needs lives once, in ``choco.jobclient`` (2026-10; until then each
job carried its own copy, and the copies had drifted): ``Degraded`` and
``failure`` (the exit-code mapping, one log line), ``setup_logging``,
``single_run`` (the flock that keeps a hand run from racing the timer),
``refuse_retired``, ``atomic_output`` / ``write_json_atomic`` (hidden unique
temp file, mode 0644, optional fsync, rename), ``push_updatable`` and
``write_run`` (``run.json``: ``time``, ``status``, ``exit_code`` and the
job's facts, its why in ``reason``, ``error`` or ``degraded``, all three
shown on the badge).  Jobs
deliberately run as separate processes, not in-process greenlets: they do
blocking C-extension work (astropy IERS downloads, h5py reads of files kotekan
is writing) that would stall the gevent hub, and keeping them out of the choco
process means job deploys/crashes don't restart choco (a choco restart re-
engages cluster-wide maintenance mode).  In-process greenlets are reserved for
cheap UI-facing monitoring (``FpgaMonitor``, ``PdbMonitor``).  A header badge
+ detail page costs one entry in ``web._JOBS`` (the strip and the landing
table loop over the registry it builds) and, optionally, a summariser in
``jobdetail.JOB_DETAILS`` with its ``_detail_<name>.html`` partial, which
``_service_status.html`` includes by name.

**State paths are a convention (2026-10).**  Until then every job had a
``state_file`` (or ``state.path``, ``archive_dir``, ``lock_file``,
``output``) in its own YAML *and* a matching key in choco's config.yaml, and
the only thing the second copy ever did was drift — the bffs manual-flag
route grew a path cross-check purely to catch it.  Now ``choco.jobclient.job_state_dir(name)``
resolves a job's directory (``--state-dir`` > ``$STATE_DIRECTORY`` >
``/var/lib/choco/<name>``), the files inside have fixed names (``state.json``,
``run.json``, ``manual_overrides.yaml``, ``waterfall.lock``,
``gain_<tag>_<source>.h5``, ``skymap.png`` / ``skymap-night.png``, the mapmaker's
``maps.npz`` / ``gains.npz`` / ``sky.png`` / ``daily.png`` / ``clean.png`` /
``clean.json``), and choco
reads them from its one ``state_dir`` root (``web._state_root``; the test
app and ``./choco.sh develop`` point it at a scratch directory).  The old
keys are refused on both sides with the fix named — choco's
``_RETIRED_KEYS``, each job's ``load_config`` — never read; there were no
deployments worth a migration shim.

**Per-run files (bffs, 2026-10).**  The exit code says *that* a run was
degraded; it cannot say which input was missing, and a state file that is
rewritten only on change cannot either.  bffs therefore keeps two files.
``state.json`` is what the flags *are* — bad list, element axis,
transitions — and changes only when the bad list does, so its mtime stays
"last change".  ``run.json`` beside it is rewritten on every non-dry run, whatever the outcome, with ``status`` /
``exit_code`` / ``error``, the ``degraded`` reasons, the N² file chosen (or
why none was), and one report per configured source.  Each source's
``mask()`` may return ``(mask, report)`` — ``sources.common.report``: ``ok``
or ``degraded``, a reason, ``n_measured`` (feeds it actually judged) and
free-form ``detail`` — and the core adds ``skipped`` entries for sources it
did not run and ``n_flagged`` for all.  ``n_measured`` is what tells "ran and
flagged nothing" from "had nothing to measure"; a run in which every source
is skipped or measured nothing still fails (``OSError``, exit 2) rather than
pass as all-good.  The web side reads the file in ``jobdetail._bffs_run_summary``
(its own guard: a malformed run file costs only the last-run block) and in
``web._run_reasons`` for the badge tooltip and landing table, only when
systemd's verdict is degraded or failed — the verdict stays systemd's; the
file supplies the words.

**The N² file is read by chunk (bffs, 2026-10-17).**  When the subset grew
to all 64 dishes the files went from 2.8 GB (48 elements, 1176 products) to
12 GB (128 elements, 8256 products) every 400 s, and ``vis`` is chunked
``(16 freq, every product, every time)``: 42 MB a chunk, bitshuffle on
disk.  HDF5 decompresses a chunk whole however little of it a read keeps,
so the power-outlier source's ``vis[:, diag, lo:hi]`` — 128 autocorrelations
over 16 rows — decompressed every chunk of ``vis`` and ``vis_weight``, 24
GB, and took 3.7 min on a 30 s timer over a link that delivers 36–59 MB/s
(measured with ``dd`` against the live file; kotekan writes 31 MB/s into
the same server).  Three changes brought a run under 5 s.  The reader takes
``blocks`` (8) chunk-aligned 16-channel blocks spread over the band
(``kotekan_io.channel_blocks``), 113 channels for 8 chunks, 3.5 s cold.  It
no longer reads ``vis_weight``: on the live files every delivered cell of an
unflagged input has a positive weight, so ``frames_added`` (0.5 MB) carries
the same validity, and the band mean is a plain mean over delivered cells.
And the verdict is cached per file beside ``run.json``
(``power_outlier.json``, keyed on path, mtime, size and the source's
parameters): a kotekan file is complete when it is renamed into the
acquisition and never changes, while the timer fires some 13 times before
the next one lands, so the same read repeated 13 times for one answer.  The
report's ``cached`` flag says which it was; a changed parameter or
``dish_types`` map is a miss.

The ``spectral-shape`` source (2026-10-26) reads more: 48 blocks over
320-1450 MHz, 2 GB and ~20 s a file, once per file through its history
file (``spectral_shape.json``).  A slope over 16 blocks varied by 0.05-0.09
for single feeds whenever a block landed in RFI that one feed saw more
than its partners (B03Y read -0.09 against -0.02 at 48 blocks); at 48 a
working feed's slope varies by ~0.003 between files, against the 0.04 the
instability test flags.

**Abstain, never flag for lack of data.**  Measured 2026-10-01 against the
state history: the stopped-acquisition tail file (20 rows, frames in 1 of
6145 channels) had twice flagged all 128 elements bad (09-19 02:30, 09-20
20:40 — for a day), because ``max_age`` does not trip on a just-written file
and ``min_valid_frac`` was measured against the whole band, so every feed
looked dead.  The same arithmetic would have flagged everything with four of
the eight X-engine nodes (each 2 GPUs × 384 of the 6144 channels) down.  The
rule now: the reader ends the window at the newest row holding any frame
(``Frame.tail_skipped``); power-outlier gates on ``band_coverage`` (the
fraction of (time, band-freq) cells delivered; ``min_coverage`` 0.25) and
measures ``min_valid_frac`` relative to the delivered cells; rfi leaves a
down or idle node's band unmeasured, ignores an instance whose gauges
(``/metrics`` timestamps on ``kotekan_rfi_sk_per_feed_valid_frac``) are
older than ``max_stale_s`` — the ``/sk`` EMAs freeze when frames stop — and
no longer raises when every endpoint fails: it abstains with
``n_measured`` 0 and lets the core decide whether the run as a whole measured
anything.  ``/api/nodes`` carries each node's live ``status`` for this.

The one deliberate exception is the power source's *absent* rule (2026-10):
a feed the master PDB table has no channel for is flagged ``not in PDB
table``, because the table is the inventory of what is wired, not a
measurement that failed — the E/F/G/H dishes before installation, which
kotekan already types as not live.  The guard is ``power.table_trusted``:
an operator's ``map:`` CSV, or choco's table when the cross-check found no
``unknown_to_kotekan`` rows (``missing_in_map`` counts only live feeds, so
unbuilt dishes do not trip it, while a renamed label would, and *that* is
the case where unmapped feeds must be left good and reported).  Sources may
put a per-feed reason in ``detail.feed_reasons``; the core lifts it into
``run.json`` and the state file's ``flag_reasons`` so the grid's hover text
reads ``power: not in PDB table`` rather than ``power``.

**The N² file is joined to the flag axis by label (2026-10-03).**  bffs had
read ``full/acq_*/*.h5`` and required the file's element axis to *equal*
the config's, position for position (a mismatch was exit 1).  ``full/`` is
written on demand and its newest file was 13 days old, so the
power-outlier source had been skipped as stale since 2026-09-20 while the
receiver wrote ``subset/`` continuously — compact ``DishInputs`` frames over
the 48 wired elements (16 dishes + 8 RFI antennas × 2 pol; the file's
``input_list`` attribute names their positions on the 128-element axis,
its ``index_map/label`` their names).  The source now judges the file's
own axis (``Frame.labels``) and projects onto the flag axis by name
(``sources.common.project``), the 80 feeds the file does not carry staying
good and counted (``n_in_file`` / ``n_not_in_file``, on the page as "48 of
128 elements in the file" — not ``degraded``: it is the file's design, not
a shortfall, the same way rfi measures 47 of 128 and reports ok).  The
positional check became ``bffs.file_axis_mismatch``: every file label must
be on the axis (both sides uniquified, so a full file with placeholders
still matches and a subset file with a duplicated label does not), and a
file that fails it is sidelined like a stale one — file sources skipped,
reason in ``run.json`` — rather than failing the run, since no index is
derived from the file any more.  Measured on the live tree: 6.8 s per run
(1.6 GB file, diagonal of 1176 products, newest 16 of 20 rows), three
feeds flagged (B04X hot, B04Y dead, RFIB1X hot) at the right indices.
``bffs.newest_file`` compares the matches' directories by mtime before
stat'ing files: a flat stat of ``subset/``'s 15,000 files cost 2.6 s per
30 s tick and grows ~400 files a day.  kotekan renames a file out of
``.partial/`` when complete, so the verdict lags the sky by up to one
file (~3.5 min); reading the partial file was not attempted.

**Dish types (2026-10-03, same day).**  With the subset files in use the
first dry run flagged B04X, B04Y and RFIB1X, and left C–H good — the
opposite of what an operator expects.  Two rules settle it, both reading
the kotekan config's ``dish_inputs`` ``type`` (``Missing`` / ``ArrayDish``
/ ``RFIDish``, kotekan's ``DishType``), which the core now derives per
element beside the labels (``bffs.config_axis`` over
``dishlabels.dish_table``, the one reader of that table; ``FlagAxis.types``) and hands to every source as ``dish_types``.  (1) A new
``dish-type`` source flags every element of a ``Missing`` dish, 80 of 128
today: a slot with no dish behind it is bad by construction, and since the
subset files do not carry those elements no data-driven source could ever
say so.  It duplicates kotekan's baseline mask on purpose — the bad list
should say what is a feed — and the power source's *absent* rule, which
stays for the case the PDB table and the config disagree.  (2)
power-outlier takes an ``eligible`` mask and leaves ``RFIDish`` elements out
of the median and the verdict (``exclude_types``): the RFI antennas are
receivers pointed at the horizon, so their power is no evidence about them
as feeds, and with 16 of the subset file's 48 elements they were a third
of the statistic.  Both rules abstain without a config (choco down, the
file's own axis), where ``dish_types`` is None.

## eigencal's run record (2026-10)

eigencal writes ``run.json`` on **every** exit, as bffs does: ``status``
(``ok`` / ``skipped`` / ``degraded`` / ``failed``), ``exit_code``, a one-line
``reason`` ("transit not complete yet", "last transit too old", "transit in
daytime", "transit already processed", "archived, not sent (no choco url)",
"sent <id> to group <g>", the quality gate's numbers, or the OSError /
ValueError text), the transit it considered (``transit``, ``transit_tag``,
``transit_complete``, ``eligible_until``, ``sun_alt_deg``), ``next_transit``,
and ``archive_only`` / ``dry_run``.  ``state.json`` still changes only when a
solution is produced, and now names the ``archive`` and the calibrated
element count.  The archive carries, beside ``gain`` / ``weight`` /
``chisq_per_dof``, the per-(freq, pol) eigenvalue diagnostics ``lam_peak``
(largest eigenvalue at the transit peak) and ``dyn_rng`` (its ratio to the
off-source floor, the dynamic-range gate's own number) with
``index_map/pol``.  Motivation: on 2026-10-04 twelve runs in the transit
window exited "no N² data overlaps" and the page could only say *ok*,
because exit 0 after a skipped transit and exit 0 after a sent solution are
the same to systemd; the journal had the answer and needed sudo.
``jobdetail.eigencal_detail`` reads both files plus the ``gain_*.h5`` list, and
``fpga.FileArchive`` serves the newest archive to the shared plot panel
(``/api/eigencal/gain-data``, the F-engine gains' protocol).

## acqnotes: notes written into the data (2026-10)

The per-acquisition READMEs began (2026-10-09) as ``tools/acqnotes``, scripts
run by hand into a work directory and copied into ``subset/`` with sudo; by
10-12 seven acquisitions had none.  The job keeps the same scripts, each now a
``analyse(acq_dir)`` function, and adds only scheduling and write-out.  Three
choices differ from the other jobs.  (1) It writes *into* the acquisition
directories, not a sibling tree like the waterfall: ``/files`` finds the notes
there, and they are meant to travel with the data.  Those directories are
``nobody:nogroup`` 775 (kotekan's squashed root), so the unit runs as root,
which NFS squashes to the same ``nobody``; ``User=choco`` cannot write there.
It needs nothing from choco's API, so it does not ``Require`` choco.service.
(2) Measurements are incremental: ``index.json`` records the file count and
newest file each acquisition was measured at, so a run measures only new or
grown acquisitions, at most ``max_acqs_per_run`` (newest first, about 5-10
minutes of NFS reads each).  The newest acquisition is re-measured at most
every ``remeasure_hours``, and ``verify`` reuses its per-file records.  (3)
Rendering is cheap (seconds) and covers every acquisition, because a timeline
edit or a new neighbour can change any note, but a file is written only when
its text changed, ignoring the README's generation date.  A run with nothing
new therefore writes nothing to the data mount.  ``run.json`` and ``index.json``
(whose mtime is its "last measurement") feed the job's section of
``/files``; like the waterfall it has no badge of its own and rides in DATA.

The 10-10 acquisitions also showed why the scripts must not assume a file
length: kotekan went from 20 to 40 ten-second bins per file, and the
hard-coded 200 s halved every transit's coverage below the 0.6 gate, so no
source was found, and every file boundary was reported as a gap.  The bin
count now comes from the files' ``vis`` shape.

The 10-17 files (128 elements, 12 GB) showed the other assumption: that a
narrow read is cheap.  ``vis`` is chunked over every product, so the
metadata summary's read of the autocorrelations over the middle 3/4 of the
band at one time bin decompressed 288 chunks, 12 GB, for 128 numbers — on
five files per acquisition, every three hours for the live one.  Every
measurement now reads whole aligned chunks and as few as it needs
(``extract.chunk_blocks``): three for the autocorrelation medians, one per
band for the transit curves (which already sample every third file), one
near 600 MHz for the pol and zero-visibility checks; the summary went from
over 100 s a file to 12 s.

## EOP merge policy

``jobs/eop/eop_update.py::merge_tables`` is **append-only and no-overwrite**.
Stored entries are immutable: past and future values, once committed, are
never replaced (IERS refinements don't propagate to already-stored entries).
Fresh entries are added only when their ``t_inst_ns`` is strictly greater than
the latest surviving stored entry — gaps inside the stored range are preserved
(kotekan may be interpolating across them) and nothing is prepended before the
first stored entry. Truncation of entries older than ``intervals_before`` days
is **conditional**: only applied if the surviving stored entries still contain
at least one timestamp ``<= now`` *and* one ``>= now``. If truncation would
break that bracketing, it is skipped and the old entries are preserved. The
astropy/numpy machinery (frame0 read, IERS download, time math) is in
``compute_lower_cutoff_ns`` / ``compute_now_inst_ns`` / ``build_fresh_table``;
the policy itself (``merge_tables``) operates on plain integer timestamps so
it is unit-tested without astropy in ``tests/test_eop_update.py``.
