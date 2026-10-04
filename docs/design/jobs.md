# The jobs pattern and the EOP merge policy

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Jobs pattern

a "job" is a standalone script that pushes through choco's localhost JSON API
(auth is bypassed for localhost callers), keeps its state under the shared
``/var/lib/choco/<name>/`` namespace (``StateDirectory=choco/<name>``, so
systemd owns creation and ownership and exports the path to the job as
``$STATE_DIRECTORY``), and lives in its own ``jobs/<name>/`` directory (units,
wrapper script, and code together), shipping as ``choco-<name>.service``
(oneshot) + ``choco-<name>.timer``, installed and enabled by the
``jobs/*/choco-*.{service,timer}`` glob in ``choco.sh install``.  Jobs share
an **exit-code convention**, read back by ``job_status`` via systemd's
``ExecMainStatus``: **0 = ok** (success or nothing-to-do), **2 = degraded** —
the job itself is fine but a dependency or input wasn't
(fpga_master/IERS/choco unreachable, no or stale N² data, an eigencal solution
failing its quality gate); retries self-heal, badge yellow — and **1 =
failed** — a config error or bug that needs a human; badge red.  Rule of thumb
inside a job's ``main``: ``OSError`` (network, missing files) → 2,
``ValueError``/``yaml.YAMLError`` (config, consistency) → 1.  Jobs
deliberately run as separate processes, not in-process greenlets: they do
blocking C-extension work (astropy IERS downloads, h5py reads of files kotekan
is writing) that would stall the gevent hub, and keeping them out of the choco
process means job deploys/crashes don't restart choco (a choco restart re-
engages cluster-wide maintenance mode).  In-process greenlets are reserved for
cheap UI-facing monitoring (``FpgaMonitor``, ``PdbMonitor``).  A header badge
+ detail page costs one entry in ``web._service_registry()``, one pill in
``_services_status.html``, and (optionally) a state-file summary branch in
``web._service_detail``.

**State paths are a convention (2026-10).**  Until then every job had a
``state_file`` (or ``state.path``, ``archive_dir``, ``lock_file``,
``output``) in its own YAML *and* a matching key in choco's config.yaml, and
the only thing the second copy ever did was drift — the bffs manual-flag
route grew a path cross-check purely to catch it.  Now ``choco.jobclient.job_state_dir(name)``
resolves a job's directory (``--state-dir`` > ``$STATE_DIRECTORY`` >
``/var/lib/choco/<name>``), the files inside have fixed names (``state.json``,
``run.json``, ``manual_overrides.yaml``, ``waterfall.lock``,
``gain_<tag>_<source>.h5``, ``skymap.png`` / ``skymap-night.png``), and choco
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
pass as all-good.  The web side reads the file in ``web._bffs_run_summary``
(its own guard: a malformed run file costs only the last-run block) and in
``web._run_reasons`` for the badge tooltip and landing table, only when
systemd's verdict is degraded or failed — the verdict stays systemd's; the
file supplies the words.

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
element beside the labels (``bffs.element_types_from_config``,
``FlagAxis.types``) and hands to every source as ``dish_types``.  (1) A new
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

eigencal writes ``run.json`` on **every** exit (bffs's convention): ``status``
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
``web._eigencal_detail`` reads both files plus the ``gain_*.h5`` list, and
``services.FileArchive`` serves the newest archive to the shared plot panel
(``/api/eigencal/gain-data``, the F-engine gains' protocol).

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
