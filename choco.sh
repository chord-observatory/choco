#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INSTALL_DIR=/opt/choco
VENV_DIR="$INSTALL_DIR/.venv"
CONFIG_DIR=/etc/choco
CONFIGS_DIR="$CONFIG_DIR/configs"

check_config() {
    # Warn about required config fields that aren't filled out.
    local config="$CONFIG_DIR/config.yaml"
    local warnings=()

    # secret_key: must not be default
    if grep -qE '^\s*secret_key:\s*change-me\s*$' "$config" 2>/dev/null; then
        warnings+=("server.secret_key is still the default 'change-me'")
    fi

    # LDAP fields: required if host is set, but warn if host is empty too
    local ldap_fields=("host" "base_dn" "bind_dn" "bind_password")
    for field in "${ldap_fields[@]}"; do
        # Match lines where the value is empty (just the key with optional comment)
        if grep -qE "^\s*${field}:\s*(#.*)?$" "$config" 2>/dev/null; then
            warnings+=("ldap.$field is not set")
        fi
    done

    if [ ${#warnings[@]} -gt 0 ]; then
        echo ""
        echo "Warning: $config needs attention:"
        for w in "${warnings[@]}"; do
            echo "  - $w"
        done
        echo "  Edit with: sudo \$EDITOR $config"
        echo ""
    fi
}

cmd_install() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Error: install must be run as root (sudo ./choco.sh install)"
        exit 1
    fi

    # Local dev venv (owned by the invoking user)
    sudo -u "$SUDO_USER" python3 -m venv "$SCRIPT_DIR/.venv"
    sudo -u "$SUDO_USER" "$SCRIPT_DIR/.venv/bin/pip" install -e "$SCRIPT_DIR[dev]"

    # System install
    sudo mkdir -p "$INSTALL_DIR"
    tmp_src="$(mktemp -d)"
    rsync -a --exclude='.venv' --exclude='.git' --exclude='.ssl' "$SCRIPT_DIR/" "$tmp_src/"
    sudo python3 -m venv "$VENV_DIR"
    sudo "$VENV_DIR/bin/pip" install "$tmp_src"
    rm -rf "$tmp_src"

    # Config files
    sudo mkdir -p "$CONFIGS_DIR"
    if [ -f "$SCRIPT_DIR/config.yaml" ]; then
        sudo cp "$SCRIPT_DIR/config.yaml" "$CONFIG_DIR/config.yaml"
    else
        sudo cp "$SCRIPT_DIR/config.yaml.template" "$CONFIG_DIR/config.yaml"
    fi
    sudo sed -i 's|^configs_dir:.*|configs_dir: /etc/choco/configs|' "$CONFIG_DIR/config.yaml"
    sudo chmod 600 "$CONFIG_DIR/config.yaml"
    check_config

    # Seed kotekan configs from repo if system dir is empty
    if [ -z "$(ls -A "$CONFIGS_DIR" 2>/dev/null)" ] && [ -d "$SCRIPT_DIR/configs" ]; then
        sudo cp -r "$SCRIPT_DIR/configs/." "$CONFIGS_DIR/"
        echo "Copied initial configs to $CONFIGS_DIR"
    fi

    # iptables port redirects (443 -> 5000, 80 -> 8080)
    echo "Setting up iptables rules: 443 -> 5000 (HTTPS), 80 -> 8080 (HTTP redirect)..."
    sudo iptables -t nat -C PREROUTING -p tcp --dport 443 -j REDIRECT --to-port 5000 2>/dev/null \
        || sudo iptables -t nat -A PREROUTING -p tcp --dport 443 -j REDIRECT --to-port 5000
    sudo iptables -t nat -C PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080 2>/dev/null \
        || sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080
    # To make persistent: sudo apt install iptables-persistent && sudo netfilter-persistent save
    sudo apt install iptables-persistent
    sudo netfilter-persistent save

    # systemd service
    sudo cp "$SCRIPT_DIR/choco.service" /etc/systemd/system/choco.service
    sudo systemctl daemon-reload
    sudo systemctl enable choco
    sudo systemctl restart choco
    echo ""
    echo "choco installed and running."
    echo "  Config:  $CONFIG_DIR/config.yaml"
    echo "  Configs: $CONFIGS_DIR/"
    echo "  Status:  sudo systemctl status choco"
    echo "  Logs:    sudo journalctl -u choco -f"
}

cmd_run() {
    if systemctl is-active --quiet choco 2>/dev/null; then
        echo "Warning: choco systemd service is running and may conflict."
        echo "  Stop it first:  sudo systemctl stop choco"
        echo ""
    fi
    if [ "${1:-}" = "local" ]; then
        shift
        exec "$SCRIPT_DIR/.venv/bin/choco" "$@"
    fi
    exec "$VENV_DIR/bin/choco" "$@"
}

cmd_test() {
    exec "$SCRIPT_DIR/.venv/bin/pytest" "$SCRIPT_DIR/tests" -v "$@"
}

cmd_help() {
    echo "Usage: ./choco.sh <command> [args...]"
    echo ""
    echo "Commands:"
    echo "  install   Install to $INSTALL_DIR, configure, and start systemd service"
    echo "  run       Start choco from system install (extra args forwarded)"
    echo "  run local Start choco from local repo code (for development)"
    echo "  test      Run tests (pass extra args to pytest)"
    echo "  help      Show this message"
}

case "${1:-help}" in
    install) cmd_install ;;
    run)     shift; cmd_run "$@" ;;
    test)    shift; cmd_test "$@" ;;
    help|*)  cmd_help ;;
esac
