# choco

**CHORD COntroller** - monitors and manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes.

choco provides a web UI that shows the live status of every kotekan instance, detects when their configs drift from the desired state, and lets you push config updates. It talks to kotekan's built-in REST API, so no agent software is needed on the nodes.

Kotekan itself is deployed and managed on nodes by Ansible. choco only handles monitoring and config management.

## Requirements

- Python 3.10+
- A FreeIPA server for LDAP authentication (e.g. `ipa1.auth.chord-observatory.ca`)
- Kotekan instances reachable over HTTP (default port 12048)

## Installation

Requires root (uses sudo internally):

```bash
git clone <this repo>
cd choco

sudo ./choco.sh install            # install system + local dev venv, configure iptables, start service
sudo $EDITOR /etc/choco/config.yaml  # edit LDAP settings + secret_key
sudo systemctl restart choco
```

This installs choco as a system service with the following layout:

| Path | Contents |
|---|---|
| `/opt/choco/.venv/` | System Python venv with choco installed |
| `/etc/choco/config.yaml` | choco configuration (chmod 600) |
| `/etc/choco/configs/` | Kotekan config files (nodes.yaml, group dirs, `.updatable/`) |

The install script also:
- Creates a local `.venv` in the repo directory (editable install, owned by invoking user) for development
- Sets up iptables rules to redirect ports 443 -> 5000 and 80 -> 8080 (persisted via `iptables-persistent`)
- Installs and enables a systemd service that starts on boot and restarts on failure
- Seeds `/etc/choco/configs/` from the repo's `configs/` directory on first install

Re-running `sudo ./choco.sh install` is safe - it won't overwrite existing config files, and iptables rules are deduplicated.

### Service management

```bash
sudo systemctl status choco        # check status
sudo systemctl restart choco       # restart after config changes
sudo journalctl -u choco -f        # follow logs
```

### Running manually

```bash
./choco.sh run                     # run from system install (/opt/choco)
./choco.sh run local               # run from local repo code (for development)
```

Both will warn if the systemd service is already running.

### Development

The install script creates a local `.venv` with an editable install, so code changes in the repo are picked up immediately:

```bash
./choco.sh run local               # run local code against /etc/choco/config.yaml
./choco.sh test                    # run tests (extra args forwarded to pytest)
./choco.sh test -k test_kotekan   # run specific tests
```

## Configuration

choco is configured via a `config.yaml` file and a config directory containing node/kotekan YAML files.

### `config.yaml`

The install script creates `/etc/choco/config.yaml` from the template. Edit it:

```yaml
server:
  host: 0.0.0.0
  port: 5000
  secret_key: change-me           # Change this in production!
  log_level: INFO

configs_dir: configs

ldap:
  host:                           # e.g. ldaps://ipa1.auth.chord-observatory.ca
  port: 636
  use_ssl: true
  base_dn:                       # e.g. dc=auth,dc=chord-observatory,dc=ca
  user_dn: cn=users,cn=accounts
  user_login_attr: uid
  user_object_filter: "(objectclass=posixaccount)"
  bind_dn:                       # e.g. uid=choco,cn=users,cn=accounts,dc=auth,dc=chord-observatory,dc=ca
  bind_password:
```

`config.yaml` contains secrets and is chmod 600. Only `config.yaml.template` is checked into the repo.

#### LDAP Authentication (FreeIPA)

choco authenticates against a FreeIPA LDAP directory. FreeIPA does not allow anonymous binds, so a bind account is required for user searches. The `bind_dn` can be a dedicated user account (e.g. `uid=choco,cn=users,cn=accounts,...`). The defaults are tuned for FreeIPA (`cn=users,cn=accounts` user DN, `posixaccount` object class, LDAPS on port 636).

### Config Directory

The config directory (`/etc/choco/configs/`) is the source of truth for which nodes choco manages and what their desired configs should be.

```
/etc/choco/configs/
├── nodes.yaml          # Node registry
├── vars.yaml           # (optional) Shared Jinja2 template variables
├── .updatable/         # Per-node updatable config overrides (JSON)
│   └── cx/
│       └── cx27.json   # Updatable values for cx27
├── cx/
│   └── cx27.yaml       # Desired kotekan config for cx27
└── recv/
    └── recv1.j2        # Desired kotekan config (Jinja2 template)
```

#### `nodes.yaml` - Node Registry

Defines the kotekan instances choco should monitor, organized into groups. An optional `config` field overrides which config file a node uses (default: `<group>/<node>.yaml`):

```yaml
groups:
  cx:
    cx27: {host: cx27.site.chord-observatory.ca, port: 12048}
  recv:
    recv1: {host: recv1.site.chord-observatory.ca, port: 12048, config: cx/cx27}
```

#### Per-Node Config Files

Each file at `<group>/<node>.yaml` (or `<group>/<node>.j2`) contains the desired kotekan config for that node. All config files are rendered through Jinja2 using variables from `vars.yaml` (if present), then sent to kotekan as JSON.

For example, a Jinja2 template `cx/cx27.j2` might reference shared variables:

```yaml
num_elements: {{ n_elem }}
log_level: info
```

These files can be edited directly on disk - choco watches for changes and picks them up automatically.

#### Updatable Config Overrides

Kotekan configs can contain updatable blocks - sections marked with `kotekan_update_endpoint` that can be changed at runtime without restarting kotekan. When updatable values are set (via the web UI or by editing files on disk), they are stored as JSON files under `.updatable/<group>/<node>.json`:

```json
{"updatable_config/gains": {"start_time": 1234, "coeff": 1.0}}
```

When a config is pushed, stored updatable values are merged into the config before sending to kotekan, so it boots with the correct values immediately. These files are also watched - editing them on disk triggers an immediate push of the updatable values to the running kotekan instance (without a restart).

## Running

After installation, choco runs as a systemd service. Open `https://<hostname>` in a browser and log in with your LDAP credentials.

To run manually (e.g. for debugging):

```bash
sudo systemctl stop choco
/opt/choco/.venv/bin/choco /etc/choco/config.yaml
```

## Web UI

### Dashboard

The main page shows a table of all registered nodes with live-updating columns: node name, status, config, sync state, and an Edit link.

Status indicators:
- **Green (up)** - kotekan is running and config matches the desired state
- **Orange (drift)** - kotekan is running but its config differs from the desired state
- **Red (down)** - kotekan is unreachable or not running
- **Grey (unknown)** - not yet polled

Status updates are pushed to the browser in real time via WebSockets - no need to refresh.

### Node Edit

Click Edit on a node to manage its settings:
- **Config selector** - which desired config file to use for this node.
- **Config editor** - edit the desired kotekan config YAML. "Save & Push" saves to disk and pushes to the node. "Re-push Current" re-pushes without editing.

## How Sync Works

A background process runs continuously:

1. Every 5 seconds, it polls each registered kotekan instance via `GET /status`
2. It fetches the running config via `GET /config` and compares it against the desired config
3. If the configs differ, the node is marked as drifted
4. If kotekan is unreachable, the node is marked as down
5. Any status change is pushed to all connected browsers via WebSocket

Config pushes (triggered by drift detection, the web UI, or file changes on disk) kill kotekan and restart it with the desired config via `POST /start`. Stored updatable config values are merged into the config before sending, so kotekan boots with the correct values.

The config directory is watched for file changes:
- **YAML/J2 files** - triggers a config reload and auto-push (kill + restart) to all affected nodes
- **`.updatable/` JSON files** - triggers an immediate push of updatable values to the running kotekan instance (no restart)

## Tests

```bash
./choco.sh test
```

Or manually:

```bash
source .venv/bin/activate
pytest tests/ -v
```

## Project Structure

```
choco/
├── app.py          # Flask app factory, SocketIO setup, entry point
├── auth.py         # LDAP authentication (Flask-Login + Flask-LDAP3-Login)
├── web.py          # Flask routes: dashboard, node edit, login/logout
├── kotekan.py      # HTTP client for kotekan's REST API
├── state.py        # Node registry, config store, config overrides, runtime state tracking
├── sync.py         # Background sync loop + file watcher
├── templates/      # Jinja2 templates (Pico CSS + htmx)
└── static/         # Static assets
```
