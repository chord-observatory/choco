#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

cmd_install() {
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install -e "$SCRIPT_DIR[dev]"
    if [ ! -f "$SCRIPT_DIR/config.yaml" ]; then
        cp "$SCRIPT_DIR/config.yaml.template" "$SCRIPT_DIR/config.yaml"
        echo "Created config.yaml from template — edit it before running."
    fi
}

cmd_run() {
    exec "$VENV_DIR/bin/choco" "$@"
}

cmd_test() {
    exec "$VENV_DIR/bin/pytest" "$SCRIPT_DIR/tests" -v "$@"
}

cmd_setup_ports() {
    # Redirect privileged ports to choco's unprivileged ports.
    # 443 -> 5000 (HTTPS), 80 -> 8080 (HTTP, redirects to HTTPS).
    # Requires root. Use iptables-persistent to survive reboots.
    # Set http_redirect_port: 8080 in config.yaml to enable the HTTP redirect server.
    echo "Adding iptables rules: 443 -> 5000 (HTTPS), 80 -> 8080 (HTTP redirect)..."
    sudo iptables -t nat -A PREROUTING -p tcp --dport 443 -j REDIRECT --to-port 5000
    sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080
    echo "Done. To make persistent: sudo apt install iptables-persistent && sudo netfilter-persistent save"
}

cmd_help() {
    echo "Usage: ./choco.sh <command> [args...]"
    echo ""
    echo "Commands:"
    echo "  install   Create venv, install dependencies, copy config template"
    echo "  run       Start choco (pass extra args to choco, e.g. ./choco.sh run /path/to/config.yaml)"
    echo "  test      Run tests (pass extra args to pytest)"
    echo "  setup-ports  Redirect ports 443/80 -> 5000 via iptables (requires root)"
    echo "  help      Show this message"
}

case "${1:-help}" in
    install) cmd_install ;;
    run)     shift; cmd_run "$@" ;;
    test)    shift; cmd_test "$@" ;;
    setup-ports) cmd_setup_ports ;;
    help|*)  cmd_help ;;
esac
