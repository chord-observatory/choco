#!/usr/bin/env bash
#
# Deconvolve the drift-scan sky map (CLEAN) from the mapmaker's accumulators (in this
# repo's jobs/mapmaker/ directory) once.  The script self-gates: with no new
# data folded in since the last deconvolution it exits immediately, so it is safe to run from a
# timer.  See jobs/mapmaker/README.md.
#
# Thin wrapper that finds the Python venv and calls the clean.py next to it.
# Extra arguments are forwarded (e.g. -f, --state-dir, -v).
# Usage: ./jobs/mapmaker/clean.sh [/path/to/mapmaker.yaml] [args...]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG="${1:-/etc/choco/mapmaker.yaml}"
if [ $# -gt 0 ]; then shift; fi

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
exec "$PYTHON" "$SCRIPT_DIR/clean.py" --config "$CONFIG" "$@"
