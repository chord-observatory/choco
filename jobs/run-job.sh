#!/usr/bin/env bash
#
# Run one job script once, from the choco venv: what every job unit's
# ExecStart calls, and the way to run a job by hand.
#
# Usage: jobs/run-job.sh <job>/<script>.py <config> [args...]
#   e.g. jobs/run-job.sh waterfall/waterfall.py /etc/choco/waterfall.yaml -n
# The config goes to the script as --config; extra arguments (-n, -v,
# --state-dir, ...) are forwarded.  The script runs in its own directory.
#
set -euo pipefail

JOBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$JOBS_DIR")"
if [ $# -lt 2 ]; then
    echo "Usage: $0 <job>/<script>.py <config> [args...]" >&2
    exit 1
fi
SCRIPT="$JOBS_DIR/$1"
CONFIG="$2"
shift 2
if [ ! -f "$SCRIPT" ]; then
    echo "Error: no job script $SCRIPT" >&2
    exit 1
fi

# Use installed (preferred) or local venv python
if [ -x /opt/choco/.venv/bin/python ]; then
    PYTHON=/opt/choco/.venv/bin/python
elif [ -x "$REPO_DIR/.venv/bin/python" ]; then
    PYTHON="$REPO_DIR/.venv/bin/python"
else
    echo "Error: no choco venv found" >&2
    exit 1
fi

cd "$(dirname "$SCRIPT")"
exec "$PYTHON" "$SCRIPT" --config "$CONFIG" "$@"
