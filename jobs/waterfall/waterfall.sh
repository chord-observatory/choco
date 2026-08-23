#!/usr/bin/env bash
#
# Render kotekan N² visibility files as waterfall images (in this repo's
# jobs/waterfall/ directory) once.  The script self-gates: unless source
# files have landed that are not yet folded in, it exits immediately, so
# this is safe to run from a frequent timer.  On a real run it appends
# each new file's time samples to every product's growing PNG under
# <waterfalls_dir>, then re-derives the display palettes and thumbnails
# (see jobs/waterfall/README.md).
#
# Thin wrapper that finds the Python venv and calls the waterfall.py next to it.
# Extra arguments are forwarded (e.g. --repalette, --acq, -n).
# Usage: ./jobs/waterfall/waterfall.sh [/path/to/waterfall.yaml] [args...]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG="${1:-/etc/choco/waterfall.yaml}"
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
exec "$PYTHON" "$SCRIPT_DIR/waterfall.py" --config "$CONFIG" "$@"
