#!/usr/bin/env bash
#
# Generate an EOP table and push it to choco as an updatable config.
#
# Calls generateEOPTable.py (vendored from kotekan) to build the table,
# then POSTs it to choco's /update/<group> API via curl.
#
# Reads fpga_master, server, and node settings from choco's config.yaml.
# Usage: ./jobs/eop-broadcast.sh [/path/to/choco/config.yaml] [extra generateEOPTable.py args...]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

yaml_val() {
    "$PYTHON" -c "
import yaml, sys
d = yaml.safe_load(open(sys.argv[1]))
for k in sys.argv[2].split('.'):
    d = d.get(k) if isinstance(d, dict) else None
print(d if d is not None else (sys.argv[3] if len(sys.argv) > 3 else ''))
" "$@"
}

yaml_keys() {
    "$PYTHON" -c "
import yaml, sys
d = yaml.safe_load(open(sys.argv[1]))
for k in sys.argv[2].split('.'):
    d = d.get(k) if isinstance(d, dict) else None
print(' '.join(d.keys()) if isinstance(d, dict) else '')
" "$@"
}


# Find config file: first arg if given, else standard locations
if [ $# -gt 0 ] && [[ "$1" != --* ]]; then
    CONFIG="$1"; shift
elif [ -f /etc/choco/config.yaml ]; then
    CONFIG=/etc/choco/config.yaml
elif [ -f "$REPO_DIR/config.yaml" ]; then
    CONFIG="$REPO_DIR/config.yaml"
else
    echo "Error: no config.yaml found" >&2
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

# Read settings from choco config

PORT=$(yaml_val "$CONFIG" server.port 5000)
FPGA_HOST=$(yaml_val "$CONFIG" fpga_master.host)
FPGA_PORT=$(yaml_val "$CONFIG" fpga_master.port 54321)
CONFIGS_DIR=$(yaml_val "$CONFIG" configs_dir configs)
[[ "$CONFIGS_DIR" != /* ]] && CONFIGS_DIR="$(dirname "$CONFIG")/$CONFIGS_DIR"
GROUPS=$(yaml_keys "$CONFIGS_DIR/nodes.yaml" groups)

if [ -z "$GROUPS" ]; then
    echo "Error: no groups found in $CONFIGS_DIR/nodes.yaml" >&2
    exit 1
fi

if [ -z "$FPGA_HOST" ]; then
    echo "Error: fpga_master.host not set in $CONFIG" >&2
    exit 1
fi

# Generate EOP table

TMPFILE=$(mktemp --suffix=.json)
trap 'rm -f "$TMPFILE"' EXIT

echo "Generating EOP table (fpga_master: $FPGA_HOST:$FPGA_PORT) ..."
export PYTHONPATH="$SCRIPT_DIR"
"$PYTHON" "$SCRIPT_DIR/generateEOPTable.py" \
    --frame0-src fpga_master \
    --fpga-master-host "$FPGA_HOST" \
    --fpga-master-port "$FPGA_PORT" \
    --enforce-continuity yes \
    -o "$TMPFILE" \
    "$@"

# POST to choco

CHOCO_URL="https://localhost:${PORT}"
EOP_ENDPOINT="earth_rotation_data"

# Wait for choco to be ready (handles startup race with choco.service)
echo -n "Waiting for choco at $CHOCO_URL ..."
for i in $(seq 1 30); do
    if curl -s -o /dev/null --insecure "$CHOCO_URL/login" 2>/dev/null; then
        echo " ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo " timed out" >&2
        exit 1
    fi
    sleep 1
done

FAILURES=0

echo "Pushing to choco ..."
for group in $GROUPS; do
    URL="${CHOCO_URL}/update/${group}"
    echo -n "  POST $URL ..."
    HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' --fail \
        -X POST "$URL" \
        -H 'Content-Type: application/json' \
        -d "{\"action\": \"updatable_config\", \"endpoint\": \"$EOP_ENDPOINT\", \"values\": $(cat "$TMPFILE")}" \
        --insecure) || true
    echo " $HTTP_CODE"
    if [ "$HTTP_CODE" != "200" ]; then
        FAILURES=$((FAILURES + 1))
    fi
done

if [ "$FAILURES" -gt 0 ]; then
    echo "Error: $FAILURES group(s) failed" >&2
    exit 1
fi

echo "Done"
