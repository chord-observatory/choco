#!/usr/bin/env bash
#
# Run bffs (the feed-flagging script, in this repo's jobs/bffs/ directory)
# once. bffs reads the latest kotekan N2 output, decides which feeds are bad,
# and — only when the list changes — POSTs it to choco's group-update API,
# which relays it to every kotekan node in the group at
# /updatable_config/bad_inputs (see jobs/bffs/README.md).
#
# Thin wrapper that finds the Python venv and calls the bffs.py next to it.
# Usage: ./jobs/bffs/bffs-flag.sh [/path/to/bffs.yaml]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG="${1:-/etc/choco/bffs.yaml}"

# Use installed (preferred) or local venv python
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

exec "$PYTHON" "$SCRIPT_DIR/bffs.py" --config "$CONFIG"
