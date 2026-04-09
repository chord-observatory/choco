#!/usr/bin/env python3
"""Generate an EOP table and push it to choco as an updatable config.

Builds a fresh EOP table from IERS data using eop_utils (vendored from kotekan).
Merges with previously pushed state to preserve continuity: entries at or before
the next midnight boundary are kept from the stored table, fresh entries are
appended after that point. This ensures currently-interpolated EOP values never
change, only future values are updated.

State is kept in a single file (eop-state.json) so all nodes get a consistent
table regardless of individual node state.

Reads fpga_master, server, and node settings from choco's config.yaml.
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import requests
import urllib3
import yaml
from astropy.time import Time
import astropy.utils.iers
import astropy.utils.data

sys.path.insert(0, str(Path(__file__).parent))
import eop_utils

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
astropy.utils.iers.conf.auto_download = True
astropy.utils.iers.conf.auto_max_age = 10.0

N_INTERVALS_BEFORE = 2
N_INTERVALS_AFTER = 3
INTERVAL_LENGTH_DAYS = 1.0
EOP_ENDPOINT = "earth_rotation_data"
STATE_FILENAME = "eop-state.json"


def build_fresh_table(frame0_ns: int) -> list[dict]:
    """Build a fresh EOP table from IERS data."""
    t_ref = Time.now()
    t_ref.precision = 9
    print(f"Reference time: {t_ref.utc.isot} (UTC)")

    times = eop_utils.build_time_array(
        t_ref, N_INTERVALS_BEFORE, N_INTERVALS_AFTER,
        INTERVAL_LENGTH_DAYS, snap_to_grid=True,
    )
    print(f"Fresh table: {times[0].isot} to {times[-1].isot} ({len(times)} entries)")

    iers = astropy.utils.iers.IERS_Auto.open()
    table = eop_utils.build_EOP_table(times, frame0_ns, iers)
    iers.close()
    return table


def merge_tables(stored: list[dict], fresh: list[dict], frame0_ns: int) -> list[dict]:
    """Merge stored and fresh tables at the next midnight boundary.

    Entries are on a daily grid (snap_to_grid + 1-day intervals = UTC midnight).
    The "current interval" is bounded by the two midnight entries surrounding now.
    We keep all stored entries up through the END of the current interval (the next
    midnight), ensuring currently-interpolated values are untouched. Fresh entries
    after that point are appended.
    """
    t_now = Time.now()

    # Next midnight in instrument time
    next_midnight_mjd = int(t_now.utc.mjd) + 1
    t_next_midnight = Time(next_midnight_mjd, format="mjd", scale="utc", precision=9)
    t0 = eop_utils.calc_astropy_time_from_unix_ns(frame0_ns)
    dt_ns = eop_utils.calc_tai_ns_from_dt(t_next_midnight - t0)
    cutoff_inst_ns = frame0_ns + dt_ns

    print(f"Merge cutoff (next midnight): {t_next_midnight.isot} "
          f"(inst_ns: {cutoff_inst_ns})")

    # Keep stored entries up to and including the cutoff
    stored_times = np.array([e["t_inst_ns"] for e in stored])
    keep_count = int(np.searchsorted(stored_times, cutoff_inst_ns, side="right"))
    kept = stored[:keep_count]

    # Append fresh entries strictly after the last kept entry
    if kept:
        last_kept_ns = kept[-1]["t_inst_ns"]
        fresh_times = np.array([e["t_inst_ns"] for e in fresh])
        start_idx = int(np.searchsorted(fresh_times, last_kept_ns, side="right"))
        appended = fresh[start_idx:]
    else:
        appended = fresh

    merged = kept + appended
    print(f"Merge result: {len(kept)} kept + {len(appended)} new = {len(merged)} total")
    return merged


def wait_for_choco(choco_url: str, timeout: int = 30):
    """Wait for choco to be ready (handles startup race with choco.service)."""
    print(f"Waiting for choco at {choco_url} ...", end="", flush=True)
    for i in range(timeout):
        try:
            requests.get(f"{choco_url}/login", timeout=2, verify=False)
            print(" ready")
            return
        except requests.RequestException:
            pass
        time.sleep(1)
    print(" timed out", file=sys.stderr)
    sys.exit(1)


def push_to_choco(choco_url: str, groups: list[str],
                  eop_table: list[dict]) -> bool:
    """POST the EOP table to choco. Returns True if all groups succeeded."""
    payload = {
        "action": "updatable_config",
        "endpoint": EOP_ENDPOINT,
        "values": {"earth_orientation_parameter_table": eop_table},
    }
    failures = 0
    for group in groups:
        url = f"{choco_url}/update/{group}"
        print(f"  POST {url} ...", end="")
        try:
            resp = requests.post(url, json=payload, timeout=30, verify=False)
            resp.raise_for_status()
            print(f" {resp.status_code} OK")
        except requests.RequestException as e:
            print(f" FAILED: {e}")
            failures += 1
    return failures == 0


def main():
    # Find config
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    if config_path is None:
        for candidate in ["/etc/choco/config.yaml", "config.yaml"]:
            if Path(candidate).exists():
                config_path = candidate
                break
    if config_path is None:
        print("Error: no config.yaml found", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f) or {}
    print(f"Config: {config_path}")

    # Resolve paths
    configs_dir = Path(config.get("configs_dir", "configs"))
    if not configs_dir.is_absolute():
        configs_dir = Path(config_path).parent / configs_dir
    state_file = configs_dir / STATE_FILENAME

    # Frame0
    fpga_cfg = config.get("fpga_master") or {}
    fpga_host = fpga_cfg.get("host")
    fpga_port = int(fpga_cfg.get("port", 54321))
    if not fpga_host:
        print("Error: fpga_master.host not set in config", file=sys.stderr)
        sys.exit(1)
    print(f"Reading frame0 from fpga_master at {fpga_host}:{fpga_port} ...")
    frame0_ns = eop_utils.read_fpga_master_frame0_ns(fpga_host, fpga_port, 30.0)
    t0 = eop_utils.calc_astropy_time_from_unix_ns(frame0_ns)
    print(f"frame0: {frame0_ns} ns  ({t0.utc.isot} UTC)")

    # Build fresh table
    fresh_table = build_fresh_table(frame0_ns)

    # Merge with stored state if it exists
    if state_file.exists():
        print(f"Loading stored state from {state_file}")
        with open(state_file) as f:
            stored = json.load(f)
        stored_table = stored["earth_orientation_parameter_table"]
        final_table = merge_tables(stored_table, fresh_table, frame0_ns)
    else:
        print("No stored state - using fresh table as-is")
        final_table = fresh_table

    # Load groups
    with open(configs_dir / "nodes.yaml") as f:
        nodes = yaml.safe_load(f)
    groups = list((nodes.get("groups") or {}).keys())
    if not groups:
        print(f"Error: no groups in {configs_dir / 'nodes.yaml'}", file=sys.stderr)
        sys.exit(1)

    # Push to choco
    server = config.get("server") or {}
    port = int(server.get("port", 5000))
    choco_url = f"https://localhost:{port}"

    wait_for_choco(choco_url)
    print(f"Pushing to {len(groups)} group(s) ...")
    success = push_to_choco(choco_url, groups, final_table)

    if success:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file, "w") as f:
            json.dump({"earth_orientation_parameter_table": final_table}, f)
        print(f"State saved to {state_file}")
    else:
        print("Some groups failed - state NOT updated", file=sys.stderr)
        sys.exit(1)

    print("Done")


if __name__ == "__main__":
    main()
