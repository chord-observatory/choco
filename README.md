# choco

**CHORD Config Orchestrator** — monitors and manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes.

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

sudo ./choco.sh install                   # install; prompts to overwrite existing configs
sudo ./choco.sh install --overwrite-configs  # overwrite configs without prompting
sudo ./choco.sh install --keep-configs       # keep existing configs without prompting
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
- Seeds `/etc/choco/configs/` from the repo's `configs/` directory on first install; on subsequent installs, prompts whether to overwrite (use `--overwrite-configs` or `--keep-configs` to skip the prompt)

Re-running `sudo ./choco.sh install` is safe — it always syncs `config.yaml` from the local copy (with `configs_dir` rewritten to `/etc/choco/configs`), and iptables rules are deduplicated. If configs already exist you'll be prompted before overwriting.

### Service management

```bash
sudo systemctl status choco        # check status
sudo systemctl restart choco       # restart after config changes
sudo journalctl -u choco -f        # follow logs
```

### Running manually

```bash
./choco.sh run                     # run locally for development (extra args forwarded)
```

### Development

The install script creates a local `.venv` with an editable install, so code changes in the repo are picked up immediately:

```bash
./choco.sh run                     # run local code against config.yaml
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

The config directory (`/etc/choco/configs/`) is the source of truth for which nodes choco manages and what their base configs are.

```
/etc/choco/configs/
├── nodes.yaml          # Node registry
├── vars.yaml           # (optional) Shared Jinja2 template variables
├── .updatable/         # Per-node updatable config overrides (JSON)
│   └── cx/
│       └── cx27.json   # Updatable values for cx27
├── cx/
│   └── cx27.yaml       # Base kotekan config for cx27
└── recv/
    └── recv1.j2        # Base kotekan config (Jinja2 template)
```

#### `nodes.yaml` - Node Registry

Defines the kotekan instances choco should monitor, organized into groups. Each node's base config lives at `<group>/<node>.yaml` (or `.j2`):

```yaml
groups:
  cx:
    cx27: {host: cx27.site.chord-observatory.ca, port: 12048}
  recv:
    recv1: {host: recv1.site.chord-observatory.ca, port: 12048}
```

#### Per-Node Config Files

Each file at `<group>/<node>.yaml` (or `<group>/<node>.j2`) contains the base kotekan config for that node. All base config files are rendered through Jinja2 using variables from `vars.yaml` (if present) to produce rendered configs, which are then merged with any updatable overrides to form the desired config that gets pushed to kotekan as JSON.

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

When a config is pushed, stored updatable values are merged into the rendered config to produce the desired config, which is sent to kotekan so it boots with the correct values immediately. These files are also watched - editing them on disk triggers an immediate push of the updatable values to the running kotekan instance (without a restart).

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
- **Green (up)** — kotekan is running and config matches the desired state
- **Yellow (idle)** — kotekan is reachable but not running (ready for `/start`)
- **Blue (syncing)** — config push in progress (kill → restart → start)
- **Red (down)** — kotekan is unreachable
- **Grey (unknown)** — not yet polled or state indeterminate

Status updates are pushed to the browser in real time via WebSockets - no need to refresh.

### Node Edit

Click Edit on a node to manage its settings:
- **Config selector** — which base config file to use for this node.
- **Config editor** — edit the base config YAML. Save queues a base-config change (write to disk + restart). "Re-push Current" queues a forced re-push.
- **Updatable config** — edit individual updatable blocks. Changes are queued and pushed to kotekan's updatable endpoints without a restart.

### JSON API

Config changes can also be submitted programmatically:

- `POST /update/<group>` — queue a change for all nodes in a group
- `POST /update/<group>/<node>` — queue a change for a single node

Both accept JSON with `{"action": "base_config", "config_content": "..."}` or `{"action": "updatable_config", "endpoint": "...", "values": {...}}`.

## How Sync Works

Changes flow through a two-tier queue system:

```
Producers (web UI, API, file watcher, poll timer)
    → Input Queue (serialized — one submission at a time)
        → Node Queues (FIFO, each Node holds its own)
            → Worker Pool (locks a node's queue, drains items, syncs to remote)
```

**Input queue** — a single serialized entry point. Accepts changes for individual nodes or entire groups (fan-out). Submissions block each other so only one caller modifies the queues at a time.

**Node queues** — each Node holds a FIFO change queue. A pool of worker greenlets scans nodes for unlocked, non-empty queues. A worker locks a node's queue, drains all pending items (writing base config or updatable values to disk), then syncs to the remote kotekan instance:
- **Base config changes** — kill kotekan, wait for idle, start with new config via `POST /start`
- **Updatable-only changes** — POST new values directly to updatable endpoints (no restart)
- **Poll (no changes)** — compare desired config vs. running config; push if drift is detected

**Periodic polling** — every 5 seconds, a poll item is submitted for every node. This detects drift and unreachable nodes even when no local changes are made. Status changes are pushed to browsers via WebSocket.

**File watcher** — the config directory is watched for changes:
- **YAML/J2 files** — reloads the affected node's config and queues a poll for it (``vars.yaml`` changes re-render all nodes)
- **`.updatable/` JSON files** — reloads the affected node's updatable store and queues a poll

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
├── web.py          # Flask routes: dashboard, node edit, login/logout, /update/* JSON API
├── state.py        # Node (identity, config state, change queue, kotekan REST client), Registry
├── sync.py         # Queue-based sync: ChangeItem, InputQueue, Orchestrator worker pool
├── templates/      # Jinja2 templates (Pico CSS + htmx)
└── static/         # Static assets
```
