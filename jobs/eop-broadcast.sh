#!/usr/bin/env bash
#
# Generate an EOP table and push it to choco as an updatable config.
#
# Thin wrapper that finds the Python venv and calls eop_update.py.
# Usage: ./jobs/eop-broadcast.sh [/path/to/choco/config.yaml]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

# Use installed (preferred) or local venv python
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

exec "$PYTHON" "$SCRIPT_DIR/eop_update.py" "$@"
