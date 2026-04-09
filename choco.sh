#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INSTALL_DIR=/opt/choco
CONFIG_DIR=/etc/choco

# --- Helpers ---

ensure_local_venv() {
    local venv="$SCRIPT_DIR/.venv"
    # Run as invoking user when under sudo so venv stays user-owned
    local as_user=""
    if [ -n "${SUDO_USER:-}" ]; then
        as_user="sudo -u $SUDO_USER"
    fi
    if [ ! -x "$venv/bin/choco" ] || ! $as_user "$venv/bin/python" -c "import choco" 2>/dev/null; then
        echo "Setting up local venv..."
        $as_user python3 -m venv "$venv"
        $as_user "$venv/bin/pip" install -e "$SCRIPT_DIR[dev]"
    fi
}

ensure_iptables() {
    for pair in 443:5000 80:8080; do
        local from="${pair%%:*}" to="${pair##*:}"
        if ! iptables -t nat -C PREROUTING -p tcp --dport "$from" -j REDIRECT --to-port "$to" 2>/dev/null; then
            echo "Adding iptables redirect: $from -> $to"
            iptables -t nat -A PREROUTING -p tcp --dport "$from" -j REDIRECT --to-port "$to"
        fi
    done
}

check_ports() {
    local busy=()
    for port in "$@"; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            busy+=("$port")
        fi
    done
    if [ ${#busy[@]} -gt 0 ]; then
        echo "Error: port(s) ${busy[*]} already in use:"
        for port in "${busy[@]}"; do
            ss -tlnp 2>/dev/null | grep ":${port} " | sed 's/^/  /'
        done
        exit 1
    fi
}

check_config() {
    local config="$1"
    local warnings=()

    if grep -qE '^\s*secret_key:\s*change-me\s*$' "$config" 2>/dev/null; then
        warnings+=("server.secret_key is still the default 'change-me'")
    fi
    for field in host base_dn bind_dn bind_password; do
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

# --- Commands ---

cmd_install() {
    local overwrite_configs=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --overwrite-configs)  overwrite_configs=yes; shift ;;
            --keep-configs)       overwrite_configs=no; shift ;;
            *) echo "Unknown install option: $1"; exit 1 ;;
        esac
    done

    if [ "$(id -u)" -ne 0 ]; then
        echo "Error: install must be run as root (sudo ./choco.sh install)"
        exit 1
    fi

    # System venv + package
    mkdir -p "$INSTALL_DIR"
    local tmp_src
    tmp_src="$(mktemp -d)"
    rsync -a --exclude='.venv' --exclude='.git' --exclude='.ssl' "$SCRIPT_DIR/" "$tmp_src/"
    python3 -m venv "$INSTALL_DIR/.venv"
    "$INSTALL_DIR/.venv/bin/pip" install "$tmp_src"
    rm -rf "$tmp_src"

    # Job scripts (timers, wrapper scripts, Python helpers)
    rsync -a "$SCRIPT_DIR/jobs/" "$INSTALL_DIR/jobs/"
    chmod +x "$INSTALL_DIR"/jobs/*.sh 2>/dev/null || true

    # Config
    mkdir -p "$CONFIG_DIR/configs"
    if [ -f "$SCRIPT_DIR/config.yaml" ]; then
        echo "Found local config.yaml, copying..."
        cp "$SCRIPT_DIR/config.yaml" "$CONFIG_DIR/config.yaml"
    else
        echo "No local config.yaml, copying template..."
        cp "$SCRIPT_DIR/config.yaml.template" "$CONFIG_DIR/config.yaml"
    fi
    sed -i "s|^configs_dir:.*|configs_dir: $CONFIG_DIR/configs|" "$CONFIG_DIR/config.yaml"
    chmod 600 "$CONFIG_DIR/config.yaml"
    check_config "$CONFIG_DIR/config.yaml"

    # Seed or overwrite kotekan configs from repo
    if [ -d "$SCRIPT_DIR/configs" ]; then
        if [ -z "$(ls -A "$CONFIG_DIR/configs" 2>/dev/null)" ]; then
            cp -r "$SCRIPT_DIR/configs/." "$CONFIG_DIR/configs/"
            echo "Copied initial configs to $CONFIG_DIR/configs"
        else
            if [ -z "$overwrite_configs" ]; then
                read -rp "Configs already exist in $CONFIG_DIR/configs. Overwrite? [y/N] " answer
                case "$answer" in
                    [yY]*) overwrite_configs=yes ;;
                    *)     overwrite_configs=no ;;
                esac
            fi
            if [ "$overwrite_configs" = "yes" ]; then
                cp -r "$SCRIPT_DIR/configs/." "$CONFIG_DIR/configs/"
                echo "Overwritten configs in $CONFIG_DIR/configs"
            else
                echo "Keeping existing configs in $CONFIG_DIR/configs"
            fi
        fi
    fi

    # Network
    ensure_iptables
    apt install -y iptables-persistent
    netfilter-persistent save

    # systemd service
    # systemd units: main service + any job timers
    cp "$SCRIPT_DIR/jobs/choco.service" /etc/systemd/system/
    cp "$SCRIPT_DIR"/jobs/choco-*.{service,timer} /etc/systemd/system/ 2>/dev/null || true
    systemctl daemon-reload
    systemctl enable choco
    systemctl restart choco
    for timer in "$SCRIPT_DIR"/jobs/choco-*.timer; do
        [ -f "$timer" ] && systemctl enable --now "$(basename "$timer")"
    done

    echo ""
    echo "choco installed and running."
    echo "  Config:  $CONFIG_DIR/config.yaml"
    echo "  Configs: $CONFIG_DIR/configs/"
    echo "  Status:  sudo systemctl status choco"
    echo "  Logs:    sudo journalctl -u choco -f"
}

cmd_uninstall() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Error: uninstall must be run as root (sudo ./choco.sh uninstall)"
        exit 1
    fi

    # systemd
    if systemctl is-active --quiet choco 2>/dev/null; then
        systemctl stop choco
    fi
    systemctl disable choco 2>/dev/null || true
    for timer in /etc/systemd/system/choco-*.timer; do
        [ -f "$timer" ] && systemctl disable --now "$(basename "$timer")" 2>/dev/null || true
    done
    rm -f /etc/systemd/system/choco.service
    rm -f /etc/systemd/system/choco-*.{service,timer}
    systemctl daemon-reload

    # iptables
    iptables -t nat -D PREROUTING -p tcp --dport 443 -j REDIRECT --to-port 5000 2>/dev/null || true
    iptables -t nat -D PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8080 2>/dev/null || true
    netfilter-persistent save 2>/dev/null || true

    # Application
    rm -rf "$INSTALL_DIR"

    echo "Removed systemd service, iptables rules, and $INSTALL_DIR"
    echo "Config preserved at $CONFIG_DIR (remove manually if desired)"
}

cmd_run() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Error: run must be run as root (sudo ./choco.sh run)"
        exit 1
    fi

    ensure_local_venv

    if [ ! -f "$SCRIPT_DIR/config.yaml" ]; then
        echo "Error: config.yaml not found (copy from config.yaml.template)"
        exit 1
    fi

    check_ports 5000 8080
    ensure_iptables

    # Drop back to invoking user so choco can access user-owned files (.ssl, etc.)
    exec sudo -u "$SUDO_USER" "$SCRIPT_DIR/.venv/bin/choco" "${@:-$SCRIPT_DIR/config.yaml}"
}

cmd_test() {
    ensure_local_venv
    exec "$SCRIPT_DIR/.venv/bin/pytest" "$SCRIPT_DIR/tests" -v "$@"
}

cmd_help() {
    echo "Usage: ./choco.sh <command> [args...]"
    echo ""
    echo "Commands:"
    echo "  install     System install to $INSTALL_DIR and start daemon (requires root)"
    echo "                --overwrite-configs   Overwrite existing configs without prompting"
    echo "                --keep-configs        Keep existing configs without prompting"
    echo "  uninstall   Remove daemon, iptables rules, and $INSTALL_DIR (requires root)"
    echo "  run         Run choco locally for development (requires root; extra args forwarded)"
    echo "  test        Run tests (extra args forwarded to pytest)"
    echo "  help        Show this message"
}

case "${1:-help}" in
    install)   shift; cmd_install "$@" ;;
    uninstall) cmd_uninstall ;;
    run)       shift; cmd_run "$@" ;;
    test)      shift; cmd_test "$@" ;;
    help|*)    cmd_help ;;
esac
