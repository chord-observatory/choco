#!/usr/bin/env bash
#
# Run eigencal (the point-source transit gain calibration, in this repo's
# jobs/eigencal/ directory) once.  eigencal self-gates: unless a calibrator
# transit just completed and is unprocessed it exits immediately, so this is
# safe to run from a frequent timer.  On a real run it reads the transit from
# the kotekan N² output, fits per-input complex gains, archives one HDF5, and
# POSTs the gains to choco's group-update API for relay to every kotekan node
# in the group (see jobs/eigencal/README.md).
#
# Thin wrapper that finds the Python venv and calls the eigencal.py next to it.
# Usage: ./jobs/eigencal/eigencal.sh [/path/to/eigencal.yaml]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG="${1:-/etc/choco/eigencal.yaml}"

# Use installed (preferred) or local venv python
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

cd "$SCRIPT_DIR"
exec "$PYTHON" "$SCRIPT_DIR/eigencal.py" --config "$CONFIG"
