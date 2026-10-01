# The jobs pattern and the EOP merge policy

Design rationale moved out of CLAUDE.md (2026-09).  Historical: the
measurements and dates are from when each part was built.

## Jobs pattern

a "job" is a standalone script that pushes through choco's localhost JSON API
(auth is bypassed for localhost callers), keeps its state under the shared
``/var/lib/choco/<name>/`` namespace (``StateDirectory=choco/<name>``, so
systemd owns creation and ownership; ``choco.sh install`` migrates pre-
namespace ``/var/lib/<name>`` directories and warns about stale paths in the
deployed configs), and lives in its own ``jobs/<name>/`` directory (units,
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

**Per-run files (bffs, 2026-10).**  The exit code says *that* a run was
degraded; it cannot say which input was missing, and a state file that is
rewritten only on change cannot either.  bffs therefore keeps two files.
``state.json`` is what the flags *are* — bad list, element axis,
transitions — and changes only when the bad list does, so its mtime stays
"last change".  ``run.json`` (``state.run_path``, default a sibling) is
rewritten on every non-dry run, whatever the outcome, with ``status`` /
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
