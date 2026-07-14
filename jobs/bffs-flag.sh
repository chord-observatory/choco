#!/usr/bin/env bash
#
# Run bffs (the feed-flagging script, in this repo's bffs/ directory) once.
# bffs reads the latest kotekan N2 output, decides which feeds are bad, and —
# only when the list changes — POSTs it to choco's group-update API, which
# relays it to every kotekan node in the group at /updatable_config/bad_inputs
# (see bffs/README.md).
#
# Thin wrapper that finds the venv and the bffs directory.
# Usage: ./jobs/bffs-flag.sh [/path/to/bffs.yaml]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG="${1:-/etc/choco/bffs.yaml}"

# Use installed (preferred) or local venv python, with the matching bffs dir
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
    BFFS_DIR=/opt/choco/bffs
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
    BFFS_DIR="$REPO_DIR/bffs"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

exec "$PYTHON" "$BFFS_DIR/bffs.py" --config "$CONFIG"
