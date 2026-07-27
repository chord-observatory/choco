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
        $as_user python3 -m venv --upgrade-deps "$venv"
        # Locked deps first so dev runs the same versions a deploy gets;
        # the editable install then only adds choco itself + dev tools.
        if [ -f "$SCRIPT_DIR/requirements.lock" ]; then
            $as_user "$venv/bin/pip" install --require-hashes -r "$SCRIPT_DIR/requirements.lock"
        fi
        $as_user "$venv/bin/pip" install -e "$SCRIPT_DIR[dev,jobs]"
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
    # bind_dn/bind_password deliberately absent: direct-bind LDAP needs
    # no service account, so those legacy keys are ignored if present.
    for field in host base_dn; do
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

# The deployed master PDB channel map (dish input <-> power board/chip/
# channel). It may be the only authoritative record of that wiring, so it
# is treated like config.yaml rather than like a kotekan config: seeded
# once, never replaced by an install, and if the repo copy differs it is
# staged alongside for the operator to merge.
PDB_MAP_NAME="pdb_map.csv"

copy_repo_configs() {
    # tar rather than cp -r so the map can be excluded; "." carries the
    # hidden .updatable/ tree along with the group directories.
    tar -C "$SCRIPT_DIR/configs" --exclude="./$PDB_MAP_NAME" -cf - . \
        | tar -C "$CONFIG_DIR/configs" -xf -
}

deploy_pdb_map() {
    local src="$SCRIPT_DIR/configs/$PDB_MAP_NAME"
    local dest="$CONFIG_DIR/configs/$PDB_MAP_NAME"
    [ -f "$src" ] || return 0
    mkdir -p "$CONFIG_DIR/configs"
    if [ ! -f "$dest" ]; then
        cp "$src" "$dest"
        echo "Seeded $dest -- fill in the real dish-input wiring before use"
    elif cmp -s "$src" "$dest"; then
        rm -f "$dest.new"
    else
        cp "$src" "$dest.new"
        echo "Kept existing $dest (the master PDB channel map, deployment data);"
        echo "  the repo copy is at $dest.new -- merge by hand if you want it."
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
    python3 -m venv --upgrade-deps "$INSTALL_DIR/.venv"
    # Install dependencies from the lock so a deploy gets exactly the
    # reviewed versions, then choco itself with --no-deps so nothing
    # floats past the pins.  ([jobs] is the scientific stack the timer
    # jobs need; the lock covers core + jobs.)  Fall back to a floating
    # resolve only if the lock is missing.
    if [ -f "$tmp_src/requirements.lock" ]; then
        "$INSTALL_DIR/.venv/bin/pip" install --require-hashes -r "$tmp_src/requirements.lock"
        "$INSTALL_DIR/.venv/bin/pip" install --no-deps "$tmp_src"
    else
        echo "Warning: requirements.lock missing; installing unpinned versions."
        "$INSTALL_DIR/.venv/bin/pip" install "$tmp_src[jobs]"
    fi
    rm -rf "$tmp_src"

    # Jobs (one subdir per job: units, wrapper script, Python code)
    rsync -a --exclude='__pycache__' --exclude='.pytest_cache' \
        "$SCRIPT_DIR/jobs/" "$INSTALL_DIR/jobs/"
    find "$INSTALL_DIR/jobs" -name '*.sh' -exec chmod +x {} +

    # Config
    mkdir -p "$CONFIG_DIR/configs"
    # Deploy config.yaml: seed on first install, but NEVER silently
    # overwrite a deployed one — it is the production config, edited in
    # place (secrets, service blocks). When the incoming copy differs it
    # is staged next to it as config.yaml.new for manual merging.
    src_config="$SCRIPT_DIR/config.yaml"
    [ -f "$src_config" ] || src_config="$SCRIPT_DIR/config.yaml.template"
    if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
        echo "Seeding $CONFIG_DIR/config.yaml from ${src_config##*/}..."
        cp "$src_config" "$CONFIG_DIR/config.yaml"
        sed -i "s|^configs_dir:.*|configs_dir: $CONFIG_DIR/configs|" "$CONFIG_DIR/config.yaml"
        chmod 600 "$CONFIG_DIR/config.yaml"
    else
        cp "$src_config" "$CONFIG_DIR/config.yaml.new"
        sed -i "s|^configs_dir:.*|configs_dir: $CONFIG_DIR/configs|" "$CONFIG_DIR/config.yaml.new"
        chmod 600 "$CONFIG_DIR/config.yaml.new"
        if cmp -s "$CONFIG_DIR/config.yaml" "$CONFIG_DIR/config.yaml.new"; then
            rm -f "$CONFIG_DIR/config.yaml.new"
        else
            echo "Kept existing $CONFIG_DIR/config.yaml (differs from the repo copy);"
            echo "  the incoming version is at $CONFIG_DIR/config.yaml.new -- merge by hand."
        fi
    fi
    check_config "$CONFIG_DIR/config.yaml"

    # Seed the bffs config on first install; never overwrite an edited one
    if [ ! -f "$CONFIG_DIR/bffs.yaml" ]; then
        cp "$SCRIPT_DIR/jobs/bffs/bffs.example.yaml" "$CONFIG_DIR/bffs.yaml"
        echo "Seeded $CONFIG_DIR/bffs.yaml from jobs/bffs/bffs.example.yaml -- edit before use"
    fi

    # Seed the eigencal configs on first install; never overwrite edited ones
    if [ ! -f "$CONFIG_DIR/eigencal.yaml" ]; then
        cp "$SCRIPT_DIR/jobs/eigencal/eigencal.example.yaml" "$CONFIG_DIR/eigencal.yaml"
        echo "Seeded $CONFIG_DIR/eigencal.yaml from jobs/eigencal/eigencal.example.yaml -- edit before use"
    fi
    if [ ! -f "$CONFIG_DIR/eigencal_feeds.yaml" ]; then
        cp "$SCRIPT_DIR/jobs/eigencal/eigencal_feeds.example.yaml" "$CONFIG_DIR/eigencal_feeds.yaml"
        echo "Seeded $CONFIG_DIR/eigencal_feeds.yaml -- fill in the real feed layout before use"
    fi

    # Seed or overwrite kotekan configs from repo.  pdb_map.csv is excluded
    # from every copy here and handled separately below -- it is deployment
    # data, not a repo artifact, so even an explicit --overwrite-configs
    # must not replace it.
    if [ -d "$SCRIPT_DIR/configs" ]; then
        if [ -z "$(ls -A "$CONFIG_DIR/configs" 2>/dev/null)" ]; then
            copy_repo_configs
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
                copy_repo_configs
                echo "Overwritten configs in $CONFIG_DIR/configs"
            else
                echo "Keeping existing configs in $CONFIG_DIR/configs"
            fi
        fi
    fi
    deploy_pdb_map

    # Network
    ensure_iptables
    apt install -y iptables-persistent
    netfilter-persistent save

    # systemd units: main service + any job units (jobs/<name>/choco-*.{service,timer})
    cp "$SCRIPT_DIR/jobs/choco.service" /etc/systemd/system/
    cp "$SCRIPT_DIR"/jobs/*/choco-*.{service,timer} /etc/systemd/system/ 2>/dev/null || true
    systemctl daemon-reload

    # Enable job services WITHOUT starting them (enable only creates their
    # WantedBy=choco.service links): the jobs run when choco (re)starts
    # below and on their timers.  Starting a oneshot here would block the
    # install on a full job run — indefinitely, for a unit that combines
    # Restart= with no start-rate limit (oneshot units have no start
    # timeout) — and a job failing for environmental reasons (data file
    # not there yet, FPGA master unreachable) must not abort an install.
    for unit in "$SCRIPT_DIR"/jobs/*/choco-*.service; do
        [ -f "$unit" ] && systemctl enable "$(basename "$unit")"
    done
    # Timers are safe to start: that only schedules the job.
    for unit in "$SCRIPT_DIR"/jobs/*/choco-*.timer; do
        [ -f "$unit" ] && systemctl enable --now "$(basename "$unit")"
    done

    systemctl enable choco
    systemctl restart choco

    # Optional host tool: node pages render kotekan's pipeline graph
    # through the graphviz CLI, falling back to raw dot text without it.
    if ! command -v dot >/dev/null 2>&1; then
        echo ""
        echo "Note: graphviz (dot) not found -- pipeline graphs on node pages"
        echo "  will show raw dot text.  apt install graphviz to render them."
    fi

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
    for unit in /etc/systemd/system/choco-*.{service,timer}; do
        [ -f "$unit" ] && systemctl disable --now "$(basename "$unit")" 2>/dev/null || true
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

cmd_develop() {
    # Unlike `run`, this needs no root: dev mode binds loopback, so there
    # are no iptables redirects to install and no privileged port to
    # claim.  If invoked under sudo anyway, drop back to the real user
    # so .venv/ and dev/ don't silently become root-owned (which would
    # then break `./choco.sh test`).
    if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ]; then
        echo "develop needs no root — re-running as $SUDO_USER"
        exec sudo -u "$SUDO_USER" "$SCRIPT_DIR/choco.sh" develop "$@"
    fi

    ensure_local_venv

    local dev_dir="$SCRIPT_DIR/dev"
    local dev_config="$dev_dir/config.yaml"
    local dev_configs="$dev_dir/configs"

    # Seeded once, then left alone — same rule as config.yaml and
    # pdb_map.csv.  Edit the files to change the dev setup; there are no
    # flags, because the file is the source of truth everywhere else in
    # choco too.
    mkdir -p "$dev_configs/dev"

    if [ ! -f "$dev_config" ]; then
        echo "Seeding $dev_config"
        cat > "$dev_config" <<EOF
# choco development config — created by ./choco.sh develop.
# Gitignored; edit freely.  Delete it to have develop re-seed.
server:
  host: 127.0.0.1                 # loopback only: dev_auth means no auth
  port: 5000
  secret_key: dev-only-not-a-secret
  log_level: DEBUG
  ssl: false                      # plain HTTP — no cert warning
  dev_auth: dev                   # auto-login, CSRF off (loopback only)
  http_redirect_port:             # empty: no second listener

configs_dir: $dev_configs

kotekan:
  timeout: 10

sync:
  poll_interval: 5
  restart_timeout: 10
  num_workers: 2

# fpga_master / pdb are deliberately absent: a dev instance should not
# poll (let alone control) the real F-engine or the power boards.  Their
# badges render as "unconfigured" and no greenlet spawns.

ldap:
  host:                           # disabled — dev_auth logs you in
EOF
    fi

    if [ ! -f "$dev_configs/nodes.yaml" ]; then
        echo "Seeding $dev_configs/nodes.yaml"
        cat > "$dev_configs/nodes.yaml" <<'EOF'
# Dev registry: one node, pointed at a kotekan you run locally.
# `started` is only the pre-discovery default — discover_node_states()
# overwrites it from the node's real /status at startup.
groups:
  dev:
    local:
      host: 127.0.0.1
      port: 12048
      started: false
EOF
    fi

    if [ ! -f "$dev_configs/dev/local.yaml" ]; then
        echo "Seeding $dev_configs/dev/local.yaml"
        cat > "$dev_configs/dev/local.yaml" <<'EOF'
# Base kotekan config for the local dev node.  Placeholder: replace with
# a real pipeline config (kotekan/config/*.yaml) once kotekan is
# listening on 127.0.0.1:12048.  It only needs to be loadable YAML for
# the node to appear in the UI.
log_level: info
EOF
    fi

    # The config file is authoritative for the port, so read it back
    # rather than assuming 5000 — the operator may well have edited it.
    local port
    port=$("$SCRIPT_DIR/.venv/bin/python" -c \
        "import yaml,sys; print((yaml.safe_load(open(sys.argv[1])) or {}).get('server',{}).get('port',5000))" \
        "$dev_config")
    check_ports "$port"

    echo ""
    echo "Starting choco in DEV MODE (no auth, no CSRF, loopback only)."
    echo "  URL:      http://127.0.0.1:$port"
    echo "  Tunnel:   ssh -N -L 8443:localhost:$port <user>@<this-host>"
    echo "            then browse http://localhost:8443"
    echo "  Configs:  $dev_configs"
    echo ""
    exec "$SCRIPT_DIR/.venv/bin/choco" "$dev_config"
}

cmd_test() {
    ensure_local_venv
    "$SCRIPT_DIR/.venv/bin/pytest" "$SCRIPT_DIR/tests" -v "$@"
    # the jobs have their own pytest.ini (pythonpath, testpaths)
    (cd "$SCRIPT_DIR/jobs/bffs" && "$SCRIPT_DIR/.venv/bin/pytest" -v "$@")
    (cd "$SCRIPT_DIR/jobs/eigencal" && "$SCRIPT_DIR/.venv/bin/pytest" -v "$@")
}

cmd_lock() {
    # Regenerate requirements.lock: the pinned production dependency set
    # (core + [jobs], no dev tools), resolved fresh in a scratch venv so
    # dev-only packages can't leak in — then hash-locked: each pin lists
    # the sha256 of every artifact PyPI serves for that version (all
    # platform wheels + the sdist), so installs run --require-hashes and
    # refuse a substituted or tampered file even at the right version,
    # while still installing on any platform.  Run after editing
    # pyproject dependencies, review the diff, and commit the result.
    local tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    echo "Resolving production dependency set in a scratch venv..."
    python3 -m venv --upgrade-deps "$tmp/venv"
    "$tmp/venv/bin/pip" install -q "$SCRIPT_DIR[jobs]"
    # --all keeps pip itself in the freeze: pip is the tool that verifies
    # the hashes, so it gets pinned, hash-locked, and audited like any
    # other dependency (the scratch venv's --upgrade-deps means the pin
    # is current, not whatever the OS seeded).
    "$tmp/venv/bin/pip" freeze --all --exclude-editable \
        | grep -vE '^(choco(==| @ )|setuptools==|wheel==)' > "$tmp/pins"
    echo "Fetching artifact hashes from PyPI for $(wc -l < "$tmp/pins") pins..."
    python3 - "$tmp/pins" > "$SCRIPT_DIR/requirements.lock" <<'PYEOF'
import json, re, sys, urllib.request

pins = [ln.strip() for ln in open(sys.argv[1]) if "==" in ln]
print("# Pinned production dependencies (core + [jobs] extra), hash-locked.")
print("# Each pin lists the sha256 of every artifact PyPI serves for that")
print("# version, so `pip install --require-hashes` (what choco.sh runs)")
print("# refuses a substituted or tampered file even at the same version.")
print("# Regenerate with: ./choco.sh lock   (then review the diff and commit)")
for pin in sorted(pins, key=str.lower):
    name, version = pin.split("==")
    url = (f"https://pypi.org/pypi/"
           f"{re.sub(r'[-_.]+', '-', name).lower()}/{version}/json")
    with urllib.request.urlopen(url, timeout=30) as resp:
        files = json.load(resp)["urls"]
    hashes = sorted({f["digests"]["sha256"] for f in files})
    if not hashes:
        raise SystemExit(f"no artifacts on PyPI for {pin}")
    print(f"{pin} \\")
    print(" \\\n".join(f"    --hash=sha256:{h}" for h in hashes))
PYEOF
    echo "Wrote $SCRIPT_DIR/requirements.lock:"
    grep -c '==' "$SCRIPT_DIR/requirements.lock" | xargs echo "  pinned packages:"
    grep -c -- '--hash=' "$SCRIPT_DIR/requirements.lock" | xargs echo "  artifact hashes:"
}

cmd_audit() {
    # Check every requirements.lock pin against the latest PyPI release
    # and the OSV vulnerability database (osv.dev aggregates PyPI's
    # advisory feed + GitHub advisories).  Read-only and stdlib-only —
    # nothing is installed or changed.  To act on findings: bump with
    # ./choco.sh lock, review the diff, run tests, commit.
    python3 - "$SCRIPT_DIR/requirements.lock" <<'PYEOF'
import json, re, sys, urllib.request

def get(url, payload=None):
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"} if payload else {})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)

pins = []
for ln in open(sys.argv[1]):
    m = re.match(r"^([A-Za-z0-9_.-]+)==(\S+?)\s*\\?$", ln)
    if m:
        pins.append(m.groups())

stale, vulnerable = [], []
for name, version in pins:
    canon = re.sub(r"[-_.]+", "-", name).lower()
    latest = get(f"https://pypi.org/pypi/{canon}/json")["info"]["version"]
    osv = get("https://api.osv.dev/v1/query", json.dumps(
        {"package": {"name": canon, "ecosystem": "PyPI"},
         "version": version}).encode())
    vulns = [v["id"] for v in (osv.get("vulns") or [])]
    notes = []
    if vulns:
        notes.append("VULNERABLE: " + ", ".join(vulns))
        vulnerable.append(name)
    if latest != version:
        notes.append(f"latest: {latest}")
        stale.append(name)
    print(f"  {name + '==' + version:<44s} {'; '.join(notes) or 'ok'}")

print()
if vulnerable:
    print(f"{len(vulnerable)} pin(s) have known advisories: "
          f"{', '.join(vulnerable)} — run ./choco.sh lock to bump, "
          f"review, test, commit.")
if stale:
    print(f"{len(stale)} pin(s) behind latest: {', '.join(stale)}")
if not vulnerable and not stale:
    print("All pins are the latest release with no known advisories.")
sys.exit(1 if vulnerable else 0)
PYEOF
}

cmd_help() {
    echo "Usage: ./choco.sh <command> [args...]"
    echo ""
    echo "Commands:"
    echo "  install     System install to $INSTALL_DIR and start daemon (requires root)"
    echo "                --overwrite-configs   Overwrite existing configs without prompting"
    echo "                --keep-configs        Keep existing configs without prompting"
    echo "  uninstall   Remove daemon, iptables rules, and $INSTALL_DIR (requires root)"
    echo "  run         Run choco locally against config.yaml (requires root; extra args forwarded)"
    echo "  develop     Run a loopback-only dev instance: no auth, no TLS, dev/ configs (no root)"
    echo "  test        Run tests (extra args forwarded to pytest)"
    echo "  lock        Regenerate requirements.lock (pinned production deps)"
    echo "  audit       Check lock pins against latest PyPI + OSV advisories"
    echo "  help        Show this message"
}

case "${1:-help}" in
    install)   shift; cmd_install "$@" ;;
    uninstall) cmd_uninstall ;;
    run)       shift; cmd_run "$@" ;;
    develop)   shift; cmd_develop "$@" ;;
    test)      shift; cmd_test "$@" ;;
    lock)      cmd_lock ;;
    audit)     cmd_audit ;;
    help|*)    cmd_help ;;
esac
