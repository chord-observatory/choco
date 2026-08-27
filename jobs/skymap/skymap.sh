#!/usr/bin/env bash
#
# Render the current-sky strip plot (jobs/skymap) once: Mollweide sky
# map with the CHORD beam strip at the live pointing declination (read
# from choco's /api/config) and the current Sun / Moon / beam positions.
# The PNG is written atomically for choco's /skymap.png route to serve.
#
# Thin wrapper that finds the Python venv and calls the skymap.py next to it.
# Usage: ./jobs/skymap/skymap.sh [/path/to/skymap.yaml]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG="${1:-/etc/choco/skymap.yaml}"

# Use installed (preferred) or local venv python
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

exec "$PYTHON" "$SCRIPT_DIR/skymap.py" --config "$CONFIG"
