#!/usr/bin/env python3
"""
Generate an EOP table and push it to choco as an updatable config. The EOP
table values are tabulated at each midnight (sets snap_to_grid=True in
eop_utils.)

If no table exists yet, this script builds a new EOP table from eop_utils and
stores the result to STATE_FILENAME.

If a table exists, then entries at or before the next midnight boundary are
used from the stored table. New entries are only ever *appended* after that
point, so currently-used EOP values should never change.

The state is kept in a single file (STATE_FILENAME) so all nodes should receive
an identical table regardless of individual node state.

Reads fpga_master and eop settings from choco's config.yaml; the node groups
come from choco itself (``GET /api/nodes``).
"""
import json
import logging
import sys
import time
from pathlib import Path

import yaml
from astropy.time import Time
import astropy.utils.iers
import astropy.utils.data

from choco.jobclient import get_json, post_json, write_json_atomic

sys.path.insert(0, str(Path(__file__).parent))
import eop_utils  # noqa: E402

astropy.utils.iers.conf.auto_download = True
astropy.utils.iers.conf.auto_max_age = 10.0

log = logging.getLogger("eop")

INTERVAL_LENGTH_DAYS = 1.0
EOP_REQUIRED_KEYS = [
    "intervals_before", "intervals_after",
    "endpoint", "state_file",
]


class Degraded(Exception):
    """A dependency was unavailable (fpga_master, choco).  Exit 2."""


def build_fresh_table(frame0_ns: int, n_before: int, n_after: int) -> list[dict]:
    """Build a fresh EOP table from IERS data."""
    t_ref = Time.now()
    t_ref.precision = 9
    log.info("Reference time: %s (UTC)", t_ref.utc.isot)

    times = eop_utils.build_time_array(
        t_ref, n_before, n_after, INTERVAL_LENGTH_DAYS, snap_to_grid=True,
    )
    log.info("Fresh table: %s to %s (%d entries)",
             times[0].isot, times[-1].isot, len(times))

    iers = astropy.utils.iers.IERS_Auto.open()
    table = eop_utils.build_EOP_table(times, frame0_ns, iers)
    iers.close()
    return table


def _to_inst_ns(t: Time, frame0_ns: int) -> int:
    """Convert an astropy ``Time`` to instrument-ns relative to ``frame0_ns``."""
    t0 = eop_utils.calc_astropy_time_from_unix_ns(frame0_ns)
    return frame0_ns + eop_utils.calc_tai_ns_from_dt(t - t0)


def compute_lower_cutoff_ns(frame0_ns: int, n_before: int,
                            t_now: Time | None = None) -> int:
    """Instrument-ns timestamp of UTC midnight ``n_before`` days before ``t_now``.

    Stored entries with ``t_inst_ns`` strictly less than this value are
    considered "too old" and are eligible for truncation by
    :func:`merge_tables` — though the truncation may be skipped if it
    would leave the stored table without entries on both sides of now.
    ``t_now`` is parameterised mostly for tests; production callers let
    it default to ``Time.now()``.
    """
    if t_now is None:
        t_now = Time.now()
    lower_mjd = int(t_now.utc.mjd) - n_before
    t_lower = Time(lower_mjd, format="mjd", scale="utc", precision=9)
    return _to_inst_ns(t_lower, frame0_ns)


def compute_now_inst_ns(frame0_ns: int, t_now: Time | None = None) -> int:
    """Instrument-ns timestamp for ``t_now`` (defaults to ``Time.now()``).

    Used by :func:`merge_tables` to decide whether a candidate
    truncation still leaves stored entries bracketing the current
    instant.
    """
    if t_now is None:
        t_now = Time.now()
    return _to_inst_ns(t_now, frame0_ns)


def merge_tables(stored: list[dict], fresh: list[dict],
                 lower_inst_ns: int, now_inst_ns: int) -> list[dict]:
    """Combine *stored* and *fresh* EOP tables under an append-only policy.

    Rules:
      * Stored entries are preserved verbatim — never overwritten and
        never reordered.
      * Truncation: stored entries with ``t_inst_ns < lower_inst_ns``
        are dropped, **but only if** the surviving stored entries still
        contain at least one timestamp ``<= now_inst_ns`` *and* one
        ``>= now_inst_ns``.  If truncation would leave the table
        without an anchor on either side of now, no truncation happens
        — kotekan's interpolation at the current instant takes
        precedence over tidy bookkeeping.
      * Fresh entries are added only when their ``t_inst_ns`` is
        strictly greater than the latest surviving stored entry.  Gaps
        inside the stored range are never filled, and nothing is
        inserted before the first stored entry: kotekan may already be
        interpolating across those segments.

    The result is sorted by ``t_inst_ns``.  Both inputs are assumed to
    use the same ``frame0_ns`` so equal timestamps compare as integers.
    """
    kept_truncated = [e for e in stored if e["t_inst_ns"] >= lower_inst_ns]
    has_before = any(e["t_inst_ns"] <= now_inst_ns for e in kept_truncated)
    has_after = any(e["t_inst_ns"] >= now_inst_ns for e in kept_truncated)
    truncated_ok = has_before and has_after
    kept = kept_truncated if truncated_ok else list(stored)

    if kept:
        last_kept_ns = max(e["t_inst_ns"] for e in kept)
        added = [e for e in fresh if e["t_inst_ns"] > last_kept_ns]
    else:
        # Stored was empty to begin with; nothing to anchor against, so
        # just take fresh whole.  Caller is expected to have built fresh
        # for the desired window.
        added = list(fresh)

    merged = sorted(kept + added, key=lambda e: e["t_inst_ns"])

    log.info("Merge: %d stored - %d truncated + %d appended fresh = %d total (%s)",
             len(stored), len(stored) - len(kept), len(added), len(merged),
             "truncation applied" if truncated_ok else "truncation skipped")
    return merged


def wait_for_choco(choco_url: str, timeout: int = 30) -> None:
    """Wait for choco to answer ``/api/status`` (this unit starts with
    choco.service).  Raises :class:`Degraded` after *timeout* seconds."""
    log.info("Waiting for choco at %s ...", choco_url)
    for _ in range(timeout):
        try:
            get_json(choco_url, "/api/status", timeout=2)
            return
        except OSError:
            time.sleep(1)
    raise Degraded(f"choco at {choco_url} did not come up in {timeout} s")


def push_to_choco(choco_url: str, groups: list[str],
                  eop_table: list[dict], endpoint: str) -> bool:
    """POST the EOP table to choco. Returns True if all groups succeeded."""
    payload = {
        "action": "updatable_config",
        "endpoint": endpoint,
        "values": {"earth_orientation_parameter_table": eop_table},
    }
    failures = 0
    for group in groups:
        try:
            post_json(choco_url, f"/update/{group}", payload, timeout=30)
            log.info("POST /update/%s OK", group)
        except OSError as e:
            log.error("POST /update/%s FAILED: %s", group, e)
            failures += 1
    return failures == 0


def main() -> int:
    # Find config
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    if config_path is None:
        for candidate in ["/etc/choco/config.yaml", "config.yaml"]:
            if Path(candidate).exists():
                config_path = candidate
                break
    if config_path is None:
        raise ValueError("no config.yaml found")

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}
    log.info("Config: %s", config_path)

    # EOP settings (all required)
    eop_cfg = config.get("eop") or {}
    missing = [k for k in EOP_REQUIRED_KEYS if k not in eop_cfg]
    if missing:
        raise ValueError(f"missing eop config keys: {', '.join(missing)}")
    fpga_cfg = config.get("fpga_master") or {}
    fpga_host, fpga_port = fpga_cfg.get("host"), fpga_cfg.get("port")
    if fpga_host is None or fpga_port is None:
        raise ValueError("fpga_master.host / fpga_master.port not set in config")
    fpga_port = int(fpga_port)
    n_before = int(eop_cfg["intervals_before"])
    n_after = int(eop_cfg["intervals_after"])
    endpoint = eop_cfg["endpoint"]

    # The table is append-only across runs, so where it lives is not a
    # detail: a relative path would resolve against whatever the current
    # directory happens to be and quietly start a fresh table.
    state_file = Path(eop_cfg["state_file"])
    if not state_file.is_absolute():
        raise ValueError(
            f"eop.state_file must be an absolute path, not {state_file!s}; "
            f"move the table to /var/lib/choco/eop/state.json and point the "
            f"key there")

    # Frame0
    log.info("Reading frame0 from fpga_master at %s:%d ...", fpga_host, fpga_port)
    try:
        frame0_ns = eop_utils.read_fpga_master_frame0_ns(fpga_host, fpga_port, 30.0)
    except Exception as e:
        raise Degraded(f"fpga_master not reachable: {e}") from e
    t0 = eop_utils.calc_astropy_time_from_unix_ns(frame0_ns)
    log.info("frame0: %d ns  (%s UTC)", frame0_ns, t0.utc.isot)

    # Build fresh table
    fresh_table = build_fresh_table(frame0_ns, n_before, n_after)

    # Merge with stored state if it exists.  Truncate stored entries older
    # than n_before days; preserve everything else verbatim.
    if state_file.exists():
        log.info("Loading stored state from %s", state_file)
        with open(state_file) as f:
            stored = json.load(f)
        stored_table = stored["earth_orientation_parameter_table"]
        t_now = Time.now()
        lower_inst_ns = compute_lower_cutoff_ns(frame0_ns, n_before, t_now)
        now_inst_ns = compute_now_inst_ns(frame0_ns, t_now)
        final_table = merge_tables(stored_table, fresh_table,
                                   lower_inst_ns, now_inst_ns)
    else:
        log.info("No stored state - using fresh table as-is")
        final_table = fresh_table

    # Push to choco: every group it knows.
    server = config.get("server") or {}
    choco_url = f"https://localhost:{int(server.get('port', 5000))}"
    wait_for_choco(choco_url)
    groups = list((get_json(choco_url, "/api/nodes").get("groups") or {}).keys())
    if not groups:
        raise ValueError("choco has no node groups to push to")

    log.info("Pushing to %d group(s) ...", len(groups))
    if not push_to_choco(choco_url, groups, final_table, endpoint):
        raise Degraded("some groups failed - state NOT updated")

    write_json_atomic(state_file,
                      {"earth_orientation_parameter_table": final_table},
                      indent=None)
    log.info("State saved to %s", state_file)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # Exit codes (shared job convention, read by choco's badge):
    #   0 ok; 2 degraded — the job is fine but a dependency wasn't
    #   (fpga_master unreachable, IERS download down, choco/groups not
    #   accepting; the unit's Restart / next timer tick self-heals);
    #   1 failed — config error or bug, needs a human.
    try:
        sys.exit(main())
    except (Degraded, OSError) as e:
        # Environmental: IERS download unreachable, network errors
        # (urllib errors are OSError).
        log.error("%s: %s", type(e).__name__, e)
        sys.exit(2)
    except (ValueError, yaml.YAMLError) as e:
        # Config or state-file problems (JSONDecodeError is a
        # ValueError) — needs a human.  Anything else is a bug and
        # keeps its traceback.
        log.error("%s: %s", type(e).__name__, e)
        sys.exit(1)
