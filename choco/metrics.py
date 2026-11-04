"""``/metrics``: Prometheus exposition text from the status snapshots.

:func:`render` turns what the web layer already holds -- the services'
health dicts, the node registry, the workers' snapshots and the jobs'
own run and state files -- into gauges.  The route is unauthenticated
and read across hosts, so nothing here names a node, a host or a
config: states, counts, timestamps and the jobs' own numbers only.

Timestamps are unix seconds (``*_timestamp_seconds``), durations
seconds; a value the source does not have is left out rather than
reported as zero.  No client library: the text format is a few lines.
"""

from __future__ import annotations

import os
from pathlib import Path

from .jobclient import RUN_NAME, STATE_NAME
from .jobstatus import read_state_json
from .state import NodeStatus
from .sync import WorkerPhase

#: Health states each service can report, for the one-hot
#: ``choco_service_state`` gauges (alert rules key on these names).
JOB_STATES = ("ok", "degraded", "stale", "failed", "never_run", "unknown")
MONITOR_STATES = {
    "fpga": ("ok", "no_timing", "down", "unconfigured", "unknown"),
    "pdb": ("ok", "no_states", "down", "unconfigured", "unknown"),
    "data": ("ok", "degraded", "down", "unconfigured", "unknown"),
    "instrument": ("ok", "degraded", "down", "unconfigured", "unknown"),
    "archive": ("ok", "down", "unconfigured", "unknown"),
    "gnss": ("ok", "stale", "degraded", "down", "unconfigured", "unknown"),
}

#: The files read per job, by name, for the job's own numbers.  bffs's
#: ``state.json`` is the whole flag history (megabytes) and the skymap
#: keeps no state: neither is read.
_JOB_FILES = {
    "eop": (STATE_NAME,),
    "bffs": (RUN_NAME,),
    "eigencal": (RUN_NAME, STATE_NAME),
    "waterfall": (STATE_NAME,),
    "mapmaker": (STATE_NAME, "clean.json"),
    "pointing": (RUN_NAME, STATE_NAME),
    "acqnotes": (RUN_NAME,),
}


class Gauges:
    """The series of one exposition, grouped by metric name in the
    order first added; ``HELP`` and ``TYPE`` once per name."""

    def __init__(self):
        self._metrics: dict[str, tuple[str, str, list[tuple[dict, str]]]] = {}

    def add(self, name: str, help_text: str, value, kind: str = "gauge", **labels) -> None:
        """One sample.  ``None`` adds nothing: the source has no value."""
        if value is None:
            return
        if isinstance(value, bool):
            value = int(value)
        if not isinstance(value, (int, float)):
            return
        if name not in self._metrics:
            self._metrics[name] = (help_text, kind, [])
        self._metrics[name][2].append((labels, repr(value) if isinstance(value, float) else str(value)))

    def render(self) -> str:
        lines = []
        for name, (help_text, kind, samples) in self._metrics.items():
            lines += [f"# HELP {name} {help_text}", f"# TYPE {name} {kind}"]
            for labels, value in samples:
                if labels:
                    inner = ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items())
                    lines.append(f"{name}{{{inner}}} {value}")
                else:
                    lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


def _escape(value) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _num(value):
    """A number as is; a bool as 0/1; anything else None."""
    if isinstance(value, bool):
        return int(value)
    return value if isinstance(value, (int, float)) else None


def _count(value):
    """A list's length, a number as is (the jobs record ``skipped`` and
    ``errors`` either way), else None."""
    if isinstance(value, list):
        return len(value)
    return _num(value)


def render(*, started_at: float, services: dict, nodes, workers, job_dirs: dict[str, Path]) -> str:
    """The whole exposition.

    *services* is ``web._services_health()``: name to health dict (or
    None); *nodes* the registry's nodes; *workers* their workers'
    snapshots; *job_dirs* each job's state directory by name.
    """
    g = Gauges()
    g.add("choco_up", "1 while choco is serving requests.", 1)
    g.add("choco_start_time_seconds", "Unix time the choco process started "
          "(a restart re-engages cluster-wide maintenance mode).", float(started_at))
    _services(g, services)
    _nodes(g, list(nodes), list(workers))
    for name, root in job_dirs.items():
        _job(g, name, root, services.get(name))
    _process(g)
    return g.render()


def _services(g: Gauges, services: dict) -> None:
    for name, health in services.items():
        state = (health or {}).get("health", "unknown")
        for s in MONITOR_STATES.get(name, JOB_STATES):
            g.add("choco_service_state", "Service health, one-hot per state.",
                  int(state == s), service=name, state=s)
    for name, health in services.items():
        if not health or "last_polled" not in health:
            continue
        g.add("choco_monitor_last_polled_timestamp_seconds",
              "When the monitor last probed its service.", _num(health.get("last_polled")), monitor=name)
        g.add("choco_monitor_last_seen_timestamp_seconds",
              "When the monitor last got a good answer.", _num(health.get("last_seen")), monitor=name)
    pdb = services.get("pdb") or {}
    g.add("choco_pdb_channels", "PDB power channels the last poll listed.", _num(pdb.get("n_channels")))
    g.add("choco_pdb_channels_on", "PDB power channels that are on.", _num(pdb.get("n_on")))
    data = services.get("data") or {}
    g.add("choco_data_roots", "Data roots configured (the N\u00b2 and baseband mounts).", _num(data.get("n_roots")))
    g.add("choco_data_roots_ok", "Data roots the last probe could read.", _num(data.get("n_ok")))
    inst = services.get("instrument") or {}
    g.add("choco_instrument_problems", "Link problems the instrument monitor sees "
          "(switch ports and X-engine NICs).", _num(inst.get("n_problems")))
    arc = services.get("archive") or {}
    g.add("choco_archive_acquisitions", "Directories the remote archive's last listing held.",
          _num(arc.get("dirs")))
    g.add("choco_archive_listing_timestamp_seconds", "When the remote archive was last listed.",
          _num(arc.get("checked_at")))


def _nodes(g: Gauges, nodes: list, workers: list) -> None:
    counts = {s: 0 for s in NodeStatus}
    for node in nodes:
        counts[node.status] += 1
    for status in NodeStatus:
        g.add("choco_nodes", "Node count by kotekan runtime status.", counts[status], status=status.value)
    g.add("choco_nodes_total", "Number of registered nodes.", len(nodes))
    g.add("choco_nodes_started_desired", "Nodes whose desired state is started.",
          sum(1 for n in nodes if n.started))
    g.add("choco_nodes_maintenance", "Nodes currently in maintenance mode.",
          sum(1 for n in nodes if n.maintenance))
    g.add("choco_nodes_config_error", "Nodes whose config failed to render or load (never pushed to).",
          sum(1 for n in nodes if n.config_problem))
    g.add("choco_nodes_error", "Nodes whose last sync cycle left an error.",
          sum(1 for n in nodes if n.error))
    g.add("choco_kotekan_versions", "Distinct kotekan versions among the nodes that report one.",
          len({n.version for n in nodes if n.version}))
    g.add("choco_queued_changes", "Config changes queued for the workers, over all nodes.",
          sum(n.queue_depth for n in nodes))
    phases = {p: 0 for p in WorkerPhase}
    for w in workers:
        phase = next((p for p in WorkerPhase if p.value == w.get("phase")), None)
        if phase is not None:
            phases[phase] += 1
    for phase in WorkerPhase:
        g.add("choco_workers", "Sync workers by phase.", phases[phase], phase=phase.value)
    g.add("choco_workers_failing", "Workers whose last cycles failed in a row (backing off).",
          sum(1 for w in workers if w.get("consecutive_failures")))


def _job(g: Gauges, name: str, root: Path, health: dict | None) -> None:
    health = health or {}
    g.add("choco_job_running", "1 while the job's unit is running.", _num(health.get("running")), job=name)
    g.add("choco_job_state_timestamp_seconds",
          "Modification time of the job's state file (what the badge calls last run or last change).",
          _num(health.get("state_mtime")), job=name)
    files = {f: read_state_json(root / f) for f in _JOB_FILES.get(name, ())}
    run, state = files.get(RUN_NAME) or {}, files.get(STATE_NAME) or {}
    g.add("choco_job_run_timestamp_seconds", "When the job's last run finished (its run file).",
          _num(run.get("time")), job=name)
    g.add("choco_job_run_seconds", "How long the job's last run took.",
          _num((run or state).get("run_seconds")), job=name)
    degraded = run.get("degraded") if "degraded" in run else state.get("degraded")
    g.add("choco_job_degraded_reasons", "Reasons the last run reported itself degraded.",
          _count(degraded), job=name)
    fn = globals().get(f"_job_{name}")
    if fn is not None:
        fn(g, run, state, files)


def _job_eop(g: Gauges, run: dict, state: dict, files: dict) -> None:
    table = state.get("earth_orientation_parameter_table")
    if not isinstance(table, list):
        return
    g.add("choco_eop_table_entries", "Rows in the EOP table last pushed to kotekan.", len(table))
    instants = [_num(r.get("t_inst_ns")) for r in table if isinstance(r, dict)]
    instants = [t for t in instants if t is not None]
    if instants:
        g.add("choco_eop_table_end_timestamp_seconds", "The EOP table's last instant.", max(instants) / 1e9)


def _job_bffs(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_bffs_inputs", "Inputs the last bffs run judged.", _num(run.get("n_elements")))
    g.add("choco_bffs_bad_inputs", "Inputs the last bffs run flagged bad (what kotekan masks).",
          _num(run.get("n_bad")))
    t, age = _num(run.get("time")), _num(run.get("kotekan_file_age_s"))
    if t is not None and age is not None:
        g.add("choco_bffs_file_timestamp_seconds",
              "Modification time of the newest N\u00b2 file the last bffs run read.", t - age)
    for src in run.get("sources") or []:
        if not isinstance(src, dict) or not src.get("kind"):
            continue
        kind = str(src["kind"])
        g.add("choco_bffs_source_measured", "Inputs each bffs source could judge in the last run.",
              _num(src.get("n_measured")), source=kind)
        g.add("choco_bffs_source_flagged", "Inputs each bffs source flagged in the last run.",
              _num(src.get("n_flagged")), source=kind)


def _job_eigencal(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_eigencal_good_frac", "Fraction of the last solution's inputs that calibrated.",
          _num(state.get("good_frac")))
    g.add("choco_eigencal_calibrated_inputs", "Inputs the last solution calibrated.",
          _num(state.get("n_calibrated")))
    g.add("choco_eigencal_transit_timestamp_seconds", "Transit of the last solution's calibrator.",
          _num(state.get("transit_time")))
    g.add("choco_eigencal_solution_timestamp_seconds", "When the last solution was written.",
          _num(state.get("updated")))
    g.add("choco_eigencal_next_transit_timestamp_seconds",
          "The next calibrator transit the job is waiting for.", _num(run.get("next_transit")))


def _job_waterfall(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_waterfall_backlog", "Files not yet rendered after the last run.", _num(state.get("backlog")))
    g.add("choco_waterfall_files_rendered", "Files the last run rendered.", _num(state.get("files_rendered")))
    g.add("choco_waterfall_errors", "Files the last run failed on.", _count(state.get("errors")))


def _job_mapmaker(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_mapmaker_backlog", "Files not yet folded into the map after the last run.",
          _num(state.get("backlog")))
    g.add("choco_mapmaker_files_imaged", "Files the last run folded into the map.",
          _num(state.get("files_imaged")))
    g.add("choco_mapmaker_files_uncalibrated", "Files the last run skipped for want of gains.",
          _num(state.get("files_uncalibrated")))
    g.add("choco_mapmaker_errors", "Files the last run failed on.", _count(state.get("errors")))
    gains = state.get("gains") if isinstance(state.get("gains"), dict) else {}
    g.add("choco_mapmaker_gains_timestamp_seconds", "When the map's self-calibration gains were solved.",
          _num(gains.get("solved_at")))
    clean = files.get("clean.json") or {}
    g.add("choco_mapmaker_clean_timestamp_seconds", "When the deconvolution last ran.",
          _num(clean.get("updated")))
    g.add("choco_mapmaker_clean_run_seconds", "How long the last deconvolution took.",
          _num(clean.get("run_seconds")))
    g.add("choco_mapmaker_clean_components", "CLEAN components of the last deconvolution.",
          _num(clean.get("n_components")))
    g.add("choco_mapmaker_clean_residual_sigma", "Residual noise after the last deconvolution.",
          _num(clean.get("sigma_after")))


def _job_pointing(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_pointing_coelevation_deg", "The array's co-elevation from the site's angle table "
          "(what vars.yaml carries as dish_coelev_deg).", _num(state.get("array_value")))
    g.add("choco_pointing_outliers", "Dishes whose angle differs from the array's.",
          _count(state.get("outliers")))
    g.add("choco_pointing_table_timestamp_seconds", "When the angle table last changed.",
          _num(state.get("time")))


def _job_acqnotes(g: Gauges, run: dict, state: dict, files: dict) -> None:
    g.add("choco_acqnotes_acquisitions", "Acquisitions the last run found under the data roots.",
          _num(run.get("acquisitions")))
    g.add("choco_acqnotes_backlog", "Acquisitions still to measure after the last run.",
          _num(run.get("backlog")))
    g.add("choco_acqnotes_measured", "Acquisitions the last run measured.", _count(run.get("measured")))
    g.add("choco_acqnotes_written", "Notes the last run wrote.", _count(run.get("written")))


def _process(g: Gauges) -> None:
    """The web process itself, the way the Prometheus client libraries
    report it (``/proc`` is read; nothing is reported where it is not)."""
    times = os.times()
    g.add("process_cpu_seconds_total", "Total user and system CPU time spent in seconds.",
          times.user + times.system, kind="counter")
    try:
        with open("/proc/self/statm") as fh:
            resident_pages = int(fh.read().split()[1])
        g.add("process_resident_memory_bytes", "Resident memory size in bytes.",
              resident_pages * os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, IndexError):
        pass
    try:
        g.add("process_open_fds", "Number of open file descriptors.", len(os.listdir("/proc/self/fd")))
    except OSError:
        pass
