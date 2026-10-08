#!/usr/bin/env bash
#
# Fold newly landed kotekan N² files into the drift-scan sky maps (in this
# repo's jobs/mapmaker/ directory) once.  The script self-gates: without new
# source files it exits immediately, so it is safe to run from a frequent
# timer.  See jobs/mapmaker/README.md.
#
# Thin wrapper that finds the Python venv and calls the mapmaker.py next to it.
# Extra arguments are forwarded (e.g. -n, --max-files, --state-dir).
# Usage: ./jobs/mapmaker/mapmaker.sh [/path/to/mapmaker.yaml] [args...]
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
exec "$PYTHON" "$SCRIPT_DIR/mapmaker.py" --config "$CONFIG" "$@"
