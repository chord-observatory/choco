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
