# choco

**CHORD COntroller** - monitors and manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes.

choco provides a web UI that shows the live status of every kotekan instance, detects when their configs drift from the desired state, and lets you push config updates. It talks to kotekan's built-in REST API, so no agent software is needed on the nodes.

Kotekan itself is deployed and managed on nodes by Ansible. choco only handles monitoring and config management.

## Requirements

- Python 3.10+
- A FreeIPA server for LDAP authentication (e.g. `ipa1.auth.chord-observatory.ca`)
- Kotekan instances reachable over HTTP (default port 12048)

## Quick Start

A convenience script `choco.sh` wraps common commands:

```bash
git clone <this repo>
cd choco

./choco.sh install   # create venv, install deps (including dev), copy config template
$EDITOR config.yaml  # edit LDAP settings + secret_key
./choco.sh run       # start the server
```

You can also pass extra arguments: `./choco.sh run /path/to/config.yaml`.

To run tests: `./choco.sh test` (extra args forwarded to pytest, e.g. `./choco.sh test -k test_kotekan`).

### Manual Installation

If you prefer not to use the script:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Configuration

choco is configured via a `config.yaml` file and a config directory containing node/kotekan YAML files.

### `config.yaml`

Copy the template and edit it:

```bash
cp config.yaml.template config.yaml
```

The template contains all settings with defaults and comments:

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

`config.yaml` is gitignored (it contains secrets). Only `config.yaml.template` is checked in.

#### LDAP Authentication (FreeIPA)

choco authenticates against a FreeIPA LDAP directory. FreeIPA does not allow anonymous binds, so a bind account is required for user searches. The `bind_dn` can be a dedicated user account (e.g. `uid=choco,cn=users,cn=accounts,...`). The defaults are tuned for FreeIPA (`cn=users,cn=accounts` user DN, `posixaccount` object class, LDAPS on port 636).

### Config Directory

The config directory is the source of truth for which nodes choco manages and what their desired configs should be.

```
configs/
├── nodes.yaml          # Node registry
├── vars.yaml           # (optional) Shared Jinja2 template variables
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

These files can be edited directly on disk — choco watches for changes and picks them up automatically.

## Running

```bash
./choco.sh run
```

Or manually:

```bash
source .venv/bin/activate
choco                          # or: choco /path/to/config.yaml
```

Then open `http://localhost:5000` in a browser. You'll be prompted to log in with your LDAP credentials.

## Web UI

### Dashboard

The main page shows a table of all registered nodes with live-updating columns: node name, status, config, sync state, and an Edit link.

Status indicators:
- **Green (up)** — kotekan is running and config matches the desired state
- **Orange (drift)** — kotekan is running but its config differs from the desired state
- **Red (down)** — kotekan is unreachable or not running
- **Grey (unknown)** — not yet polled

Status updates are pushed to the browser in real time via WebSockets — no need to refresh.

### Node Edit

Click Edit on a node to manage its settings:
- **Config selector** — which desired config file to use for this node.
- **Config editor** — edit the desired kotekan config YAML. "Save & Push" saves to disk and pushes to the node. "Re-push Current" re-pushes without editing.

## How Sync Works

A background process runs continuously:

1. Every 5 seconds, it polls each registered kotekan instance via `GET /status`
2. It fetches the running config via `GET /config` and compares it against the desired config
3. If the configs differ, the node is marked as drifted
4. If kotekan is unreachable, the node is marked as down
5. Any status change is pushed to all connected browsers via WebSocket

Config pushes (triggered from the web UI) stop kotekan and restart it with the desired config via `POST /start`.

The config directory is also watched for local file changes — editing a YAML file on disk triggers an immediate reload and auto-push to all affected nodes.

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
