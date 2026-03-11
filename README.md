# choco

**CHORD COntroller** - monitors and manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes.

choco provides a web UI that shows the live status of every kotekan instance, detects when their configs drift from the desired state, and lets you push config updates. It talks to kotekan's built-in REST API, so no agent software is needed on the nodes.

## Requirements

- Python 3.10+
- A FreeIPA server for LDAP authentication (e.g. `ipa1.auth.chord-observatory.ca`)
- Kotekan instances reachable over HTTP (default port 12048)
- FreeIPA sudo rules for the `choco` user on managed nodes (see [Node Setup](#node-setup))

## Installation

```bash
git clone <this repo>
cd choco

python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

For development/testing:

```bash
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
├── cx/
│   └── cx27.yaml       # Desired kotekan config for cx27
└── recv/
    └── recv1.yaml      # Desired kotekan config for recv1
```

#### `nodes.yaml` - Node Registry

Defines the kotekan instances choco should monitor, organized into groups:

```yaml
groups:
  cx:
    cx27: {host: cx27.site.chord-observatory.ca, port: 12048}
  recv:
    recv1: {host: recv1.site.chord-observatory.ca, port: 12048}
```

#### Per-Node Config Files

Each file at `<group>/<node>.yaml` contains the desired kotekan config for that node. For example, `cx/cx27.yaml` is the desired config for node `cx27` in the `cx` group.

These files can be edited directly on disk — choco watches for changes and picks them up automatically.

## Running

```bash
# Create config from template and edit LDAP settings + secret_key
cp config.yaml.template config.yaml
$EDITOR config.yaml

# Start the server
choco
```

You can also pass a custom config path: `choco /path/to/config.yaml`.

Then open `http://localhost:5000` in a browser. You'll be prompted to log in with your LDAP credentials.

## Web UI

### Dashboard

The main page shows a table of all registered nodes with live-updating columns: node name, status, branch, config, sync state, and an Edit link.

Status indicators:
- **Green (up)** — kotekan is running and config matches the desired state
- **Orange (drift)** — kotekan is running but its config differs from the desired state
- **Red (down)** — kotekan is unreachable or not running
- **Grey (unknown)** — not yet polled

Status updates are pushed to the browser in real time via WebSockets — no need to refresh.

### Node Edit

Click Edit on a node to manage its deploy settings:
- **Branch** — which git branch of kotekan to build. Changing the branch prompts a reinstall.
- **Config selector** — which desired config file to use for this node.
- **Config editor** — edit the desired kotekan config YAML. "Save & Push" saves to disk and pushes to the node. "Re-push Current" re-pushes without editing.
- **Reinstall** — clone/update, build, `make install`, and restart the kotekan systemd service on the remote node via SSH.

## How Sync Works

A background process runs continuously:

1. Every 5 seconds, it polls each registered kotekan instance via `GET /status`
2. It checks `GET /config_md5sum` for a fast hash comparison against the desired config
3. If the hash differs, the node is marked as drifted
4. If kotekan is unreachable, the node is marked as down
5. Any status change is pushed to all connected browsers via WebSocket

Config pushes (triggered from the web UI) stop kotekan and restart it with the desired config via `POST /start`.

The config directory is also watched for local file changes — editing a YAML file on disk triggers an immediate reload and auto-push to all affected nodes.

## Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Node Setup

Each managed node needs the following (typically provisioned by ansible):

1. **Directories**: `/kotekan` owned by `choco` user (source + build), `/var/lib/kotekan` (systemd WorkingDirectory)
2. **Build dependencies**: `build-essential`, `cmake`, `git`, `libevent-dev`, `libssl-dev`, `libyaml-cpp-dev`, `python3`, `python3-yaml`, `python3-jinja2`
3. **FreeIPA sudo rules** for the `choco` user (see below)

### FreeIPA Sudo Configuration

The `choco` user needs passwordless sudo for a limited set of commands on managed nodes. Configure this via the FreeIPA web UI:

**1. Create sudo commands** (Policy → Sudo → Sudo Commands → Add):

| Command |
|---------|
| `/usr/bin/make install` |
| `/usr/bin/systemctl start kotekan` |
| `/usr/bin/systemctl stop kotekan` |
| `/usr/bin/systemctl restart kotekan` |
| `/usr/bin/systemctl daemon-reload` |

**2. Create a sudo command group** (Policy → Sudo → Sudo Command Groups → Add):
- Name: `choco-kotekan-mgmt`
- Add all five commands above to the group

**3. Create a sudo rule** (Policy → Sudo → Sudo Rules → Add):
- Rule name: `choco-kotekan-mgmt`
- **Who**: User → add `choco`
- **Access this host**: Category "All" (or a specific host group for kotekan nodes)
- **Run Commands**: Allow → add the `choco-kotekan-mgmt` command group
- **As whom**: RunAsUser → `root`
- **Options**: add `!authenticate` (this enables NOPASSWD)

**4. Verify** from the choco server:
```bash
ssh choco@<node> sudo systemctl status kotekan
```

## Project Structure

```
choco/
├── app.py          # Flask app factory, SocketIO setup, entry point
├── auth.py         # LDAP authentication (Flask-Login + Flask-LDAP3-Login)
├── web.py          # Flask routes: dashboard, node edit, reinstall, login/logout
├── kotekan.py      # HTTP client for kotekan's REST API
├── state.py        # Node registry, config store, runtime state tracking
├── sync.py         # Background sync loop + file watcher
├── ssh.py          # SSH client for remote kotekan management (Kerberos/GSSAPI)
├── templates/      # Jinja2 templates (Pico CSS + htmx)
└── static/         # Static assets
```
