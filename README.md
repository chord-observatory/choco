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

fpga_master:
  host: chive.site.chord-observatory.ca
  port: 54321
  timeout: 5                     # HTTP request timeout (seconds)

eop:
  intervals_before: 2             # Days of past entries (older stored entries are truncated on merge)
  intervals_after: 2              # Days of future entries (later stored entries are kept, never overwritten)
  endpoint: earth_rotation_data   # Kotekan updatable config endpoint name
  state_file: eop-state.json     # State file name (stored in configs_dir)
  service_unit: choco-eop-broadcast.service  # systemd unit for last-run status

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
    cx42: {host: cx42.site.chord-observatory.ca, port: 12048, started: true}
  recv:
    recv1: {host: recv1.site.chord-observatory.ca, port: 12048}
```

The optional `started` field is a pre-discovery default for the desired runtime state. On startup, choco polls every node and **preserves whatever kotekan is actually doing** — reachable nodes that are running come up with `started=True`, idle ones with `started=False`, and unreachable nodes fall back to `started=False`. The `nodes.yaml` value is overwritten by this observation. The started state can also be toggled at runtime via the dashboard or the JSON API. Runtime toggles are ephemeral (reset on choco restart, at which point the discovery pass runs again).

#### Per-Node Config Files

Each file at `<group>/<node>.yaml` (or `<group>/<node>.j2`) contains the base kotekan config for that node. All base config files are rendered through Jinja2 using variables from `vars.yaml` (if present) to produce rendered configs, which are then merged with any updatable overrides to form the desired config that gets pushed to kotekan as JSON.

For example, a Jinja2 template `cx/cx27.j2` might reference shared variables:

```yaml
num_elements: {{ n_elem }}
log_level: info
```

These files can be edited directly on disk - choco compares file mtimes on every sync tick and picks up edits within one poll interval (5 seconds by default).

#### Updatable Config Overrides

Kotekan configs can contain updatable blocks - sections marked with `kotekan_update_endpoint` that can be changed at runtime without restarting kotekan. When updatable values are set (via the web UI or by editing files on disk), they are stored as JSON files under `.updatable/<group>/<node>.json`:

```json
{"updatable_config/gains": {"start_time": 1234, "coeff": 1.0}}
```

When a config is pushed, stored updatable values are merged into the rendered config to produce the desired config, which is sent to kotekan so it boots with the correct values immediately. These files are also picked up when edited on disk, triggering a push of the updatable values to the running kotekan instance (without a restart).

## Running

After installation, choco runs as a systemd service. Open `https://<hostname>` in a browser and log in with your LDAP credentials.

To run manually (e.g. for debugging):

```bash
sudo systemctl stop choco
/opt/choco/.venv/bin/choco /etc/choco/config.yaml
```

## Web UI

### Service status strip

Every page (for logged-in users) shows a thin strip above the nav with pill badges:

- **FPGA** — colour-coded readout from the `fpga_master` daemon. Green when `/status` responds and `/get-frame0-time` parses (timing is good); yellow when `/status` is reachable but timing can't be read; red when the daemon is unreachable; grey when no `fpga_master` block is configured. The tooltip carries the host, last-seen, error, and current `frame0_ns`.
- **EOP** — health of the EOP broadcast job. Red when `systemctl show choco-eop-broadcast.service` reports the last run failed; yellow when the `eop-state.json` mtime is older than ~25 hours (the job rewrites it on every successful daily run); green when the last run succeeded and the state file is fresh; grey when the unit has never run or health can't be determined.
- **BFFS** — health of the bffs feed-flagging job (`choco-bffs-flag.service`), from the same generic helper. Green/red from the last run's systemd result; the state-file mtime is shown in the tooltip as "last change" but doesn't age the badge, since bffs only rewrites its state when the bad-feed list changes.

Job health combines two cheap signals — the unit's `Result` from `systemctl show` and the job state file's mtime — with no timestamp parsing. The strip is refreshed every 30 seconds via htmx; the FPGA poller runs as a single gevent greenlet on the same cadence.

**Clicking a job badge** (EOP, BFFS) opens a log panel under the header with the unit's 50 most recent journal lines (`journalctl -u <unit>`), so a red badge can be diagnosed without leaving the browser. The panel has refresh/close buttons; only the known service units can be viewed (`choco`, `eop`, `bffs` — an allowlist, not arbitrary units).

### Dashboard

The main page shows a table of all registered nodes with live-updating columns: node name, status, config, sync state, and an Edit link.

Status indicators:
- **Green (started)** — kotekan is running and config matches the desired state
- **Yellow (stopped)** — kotekan is reachable but not running (ready for `/start`)
- **Blue (syncing)** — config push in progress (kill → restart → start)
- **Red (down)** — kotekan is unreachable
- **Grey (unknown)** — not yet polled or state indeterminate

Each node also has two toggles:

- **Started/stopped** (green/yellow) — desired runtime state. On startup choco discovers the actual state and sets this from what kotekan reports; the toggle then controls whether choco keeps kotekan running.
- **Maintenance** (orange = on, blue = normal) — when on, choco observes the node but never writes to it (no `/start`, no `/kill`, no updatable POSTs). Useful when an operator is intervening on a node manually. **Every node starts in maintenance mode** after a choco restart; flip it off (per node, per group, or with the cluster-wide toggle) when you're ready for choco to reconcile drift.

Each scope (group header, dashboard header) has paired ▲/▼ and M/N buttons that flip every node in scope at once.

The dashboard table refreshes itself every 2 seconds via htmx polling - no need to refresh the page.

### Node Edit

Click Edit on a node to manage its settings:
- **Config selector** — which base config file to use for this node.
- **Config editor** — edit the base config YAML. Save queues a base-config change (write to disk + restart). "Re-push Current" queues a forced re-push.
- **Updatable config** — edit individual updatable blocks. Changes are queued and pushed to kotekan's updatable endpoints without a restart.

### Edit Nodes (registry)

The **Edit nodes** button on the dashboard opens `/nodes`, a drag-and-drop editor for `nodes.yaml`. Saving rewrites the YAML, rebuilds the in-memory registry from scratch (dropping queued changes), then automatically puts **every** node into maintenance mode and re-runs state discovery so each node's started/idle flag is set from the live kotekan runtime rather than a cold default. Take nodes back out of maintenance individually or via the cluster-wide toggle once you've reviewed the new layout. Config files on disk are *not* moved when nodes change groups — that's an operator task.

### JSON API

Config changes can also be submitted programmatically:

- `POST /update/<group>` — queue a change for all nodes in a group
- `POST /update/<group>/<node>` — queue a change for a single node

Both accept JSON with:
- `{"action": "base_config", "config_content": "..."}`
- `{"action": "updatable_config", "endpoint": "...", "values": {...}}`
- `{"action": "set_started", "started": true}` — set the started/stopped state
- `{"action": "set_maintenance", "maintenance": true}` — put the node(s) into or out of maintenance mode

Read-only status endpoints:
- `GET /api/status` — simple overall health: choco itself (`up`, `started_at`), each service's health string (`fpga`, `eop`, `bffs`), and node counts by status (plus `total`, `started_desired`, `maintenance`)
- `GET /api/nodes/status` — per-node runtime status plus an aggregate summary
- `GET /api/nodes` — the node registry (groups/hosts/ports) as JSON
- `GET /metrics` — the same overall health in Prometheus exposition format (see below)

The `/update/*` and `/api/*` endpoints bypass auth when called from `localhost`, so from the choco host you can use curl directly (use `-k` since the cert is typically self-signed):

```bash
# Start a single node
curl -ks -X POST https://localhost:5000/update/<group>/<node> -H 'Content-Type: application/json' -d '{"action":"set_started","started":true}'

# Stop (idle) a single node
curl -ks -X POST https://localhost:5000/update/<group>/<node> -H 'Content-Type: application/json' -d '{"action":"set_started","started":false}'

# Start a whole group
curl -ks -X POST https://localhost:5000/update/<group> -H 'Content-Type: application/json' -d '{"action":"set_started","started":true}'

# Stop a whole group
curl -ks -X POST https://localhost:5000/update/<group> -H 'Content-Type: application/json' -d '{"action":"set_started","started":false}'

# Check status
curl -ks https://localhost:5000/api/status | jq .        # overall health
curl -ks https://localhost:5000/api/nodes/status | jq .  # per-node detail
```

### Prometheus metrics

`GET /metrics` serves Prometheus exposition text and is the one **unauthenticated**
endpoint (Prometheus scrapes from another host and speaks neither LDAP sessions
nor CSRF tokens). It deliberately exposes only aggregate health — no node names,
hosts, or configs:

- `choco_up` — 1 while choco is serving requests (Prometheus's own `up` metric
  covers total outage)
- `choco_start_time_seconds` — process start time; an increase means choco
  restarted, which also means **the whole cluster re-entered maintenance mode**
  (worth alerting on)
- `choco_service_state{service,state}` — one-hot health per service (`fpga`:
  ok / no_timing / down / unconfigured / unknown; `eop`, `bffs`: ok / stale /
  failed / never_run / unknown)
- `choco_nodes{status}`, `choco_nodes_total`, `choco_nodes_started_desired`,
  `choco_nodes_maintenance` — node counts

Example alerts: `choco_service_state{service="eop",state="ok"} != 1` for a
day, `choco_nodes{status="down"} > 0`, or `changes(choco_start_time_seconds[1h]) > 0`.

## How Sync Works

Changes flow through a two-tier queue system:

```
Producers (web UI, API, config-file scan, poll timer)
    → Serialized submit (one lock — one submission at a time)
        → Node Queues (FIFO, each Node holds its own)
            → Worker Pool (locks a node's queue, drains items, syncs to remote)
```

**Serialized submit** — the orchestrator's `submit_node` / `submit_group` / `submit_all` methods all share a single lock, so only one caller modifies the queues at a time (and a registry rebuild can pause submissions by holding the same lock). Group and all submissions fan one change out to every matching node.

**Node queues** — each Node holds a FIFO change queue. A pool of worker greenlets scans nodes for unlocked, non-empty queues. A worker locks a node's queue, drains all pending items (writing base config or updatable values to disk), then syncs to the remote kotekan instance:
- **Base config changes** — kill kotekan, wait for stopped, start with new config via `POST /start`
- **Updatable-only changes** — POST new values directly to updatable endpoints (no restart)
- **Poll (no changes)** — compare desired config vs. running config; push if drift is detected

**Periodic polling** — every 5 seconds, a poll item is submitted for every node. This detects drift and unreachable nodes even when no local changes are made.

**Config-file scan** — the same 5-second tick compares the mtime of every config file against the previous scan (a plain stat sweep — no inotify, works on NFS). Changed, created, or deleted files are handled by type:
- **YAML/J2 files** — reloads the affected node's config and queues a poll for it (``vars.yaml`` changes re-render all nodes; ``nodes.yaml`` changes rebuild the registry)
- **`.updatable/` JSON files** — reloads the affected node's updatable store and queues a poll

**Load errors are surfaced, not fatal.** If a base config or updatable JSON file fails to parse, the affected node loads with a ``load_error`` and the service still starts. The dashboard shows the specific error (including the file name), and the sync loop **refuses to push any config to that node** until the error clears — pushing an incomplete `desired_config` could silently regress kotekan's runtime state. Errors clear automatically when the file becomes parseable again (file watcher reload) or when a fresh config is submitted via the UI / API (`save_base` / `save_updatable`). Stopped nodes (`started: false`) are still killed normally — load errors don't override the user's intent to stop a node.

**Startup state discovery.** When choco starts, `Orchestrator.discover_node_states()` probes every node in parallel and sets each `node.started` from the actual runtime state (STARTED → `True`, IDLE → `False`, unreachable → `False`). This happens before the regular poll loop and worker pool engage, so choco never "resets" a running node back to idle just because the local default was `False`.

**Maintenance mode.** Every node has a `maintenance` flag that defaults to **on** at startup. When maintenance is on, all REST calls that mutate the node — `Node.push_updatable`, `Node.start`, and `Node.kill` — are no-ops (they log and return `False`), and `Orchestrator._sync_node` short-circuits before reaching them. Drift is still observed and the dashboard reflects the node's actual state, but choco never writes to a paused node, even to enforce `started=False`. Operators flip nodes out of maintenance once they're ready for choco to reconcile. Maintenance state is ephemeral; a choco restart puts everything back into maintenance and re-runs state discovery.

## EOP Broadcast

A companion oneshot service generates an Earth Orientation Parameter (EOP) table from IERS data and pushes it to every group as `updatable_config` under the `earth_rotation_data` endpoint.

**Schedule** — `choco-eop-broadcast.service` runs once on `choco.service` startup (`After=choco.service`, `WantedBy=choco.service`) and again daily at 12:00 UTC via `choco-eop-broadcast.timer` (`Persistent=true`, so a missed firing catches up on boot). One-off runs: `sudo systemctl start choco-eop-broadcast.service`.

**Pipeline** (`jobs/eop/eop_update.py`):
1. Read `frame0_ns` from `fpga_master` over TCP.
2. Build a fresh EOP table on the UTC-midnight grid using `astropy` + IERS auto-download, covering `(now − intervals_before, now + intervals_after)` days.
3. If `configs_dir/eop-state.json` exists, merge with stored state (policy below).
4. Wait for choco's web port, then `POST /update/<group>` for every group in `nodes.yaml`.
5. If *all* groups succeed, write the merged table back to `eop-state.json`. On any failure, the state file is left alone so the next run merges from a known-good baseline.

**Merge policy** — `eop-state.json` is the source of truth for what kotekan has been told; the merge protects continuity of any value that has already been pushed:

- **No overwrite.** Stored entries are never replaced, even if IERS data has been refined since they were committed. Past *and* future values are immutable once stored.
- **No gap filling, no prepending.** Fresh entries are added only when their timestamp is strictly greater than the latest surviving stored entry. We never insert between two existing stored entries — kotekan may be interpolating across that segment — and we never insert before the first stored entry.
- **Conditional truncation.** Stored entries older than `intervals_before` days are dropped, but **only if** the surviving stored set still contains at least one entry on either side of "now". If truncation would leave the table without an anchor before or after now, no truncation happens — preserving kotekan's ability to interpolate at the current instant takes priority over tidy bookkeeping.

The net effect is that the on-disk table grows forward over time (one new entry per day) and is trimmed from the past only when it's safe to do so.

## Bad-feed flag broadcast (bffs)

A companion oneshot service runs [bffs](jobs/bffs/) — the feed-flagging script
that lives in this repo's `jobs/bffs/` directory — every 30 seconds via
`choco-bffs-flag.timer`. bffs reads the newest kotekan N² output file, decides
which feeds are bad, and (only when the list changes) POSTs
`{"action": "updatable_config", "endpoint": "updatable_config/bad_inputs",
"values": {update_id, start_time, bad_inputs}}` to `POST /update/<group>` on
localhost. choco then relays the values to every kotekan node in the group at
`POST /updatable_config/bad_inputs`, where the `bufferBadInputs` stage turns
them into the RFI-kernel feed mask.

bffs runs from the choco venv (`jobs/bffs/bffs-flag.sh` finds `/opt/choco` or
the local checkout) with its config at `/etc/choco/bffs.yaml`, seeded from
`jobs/bffs/bffs.example.yaml` on first install — see
[jobs/bffs/README.md](jobs/bffs/README.md)
for the config format and flagging sources. The service runs once on choco
startup and on every timer tick; bffs records its state only after a
successful POST, so a run that fails while choco is still coming up is retried
on the next tick. Note that nodes sitting in **maintenance mode** receive no
pushes — choco stores the flags and reconciles them once maintenance is
lifted.

```bash
sudo systemctl status choco-bffs-flag.timer   # cadence
sudo journalctl -u choco-bffs-flag -f          # per-run logs
```

The header's **BFFS** badge tracks this job; its `bffs:` block in choco's
`config.yaml` (`service_unit`, `state_file`) tells choco where to look.

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
├── app.py          # Flask app factory, gevent WSGI server, entry point
├── auth.py         # LDAP authentication (Flask-Login + Flask-LDAP3-Login)
├── web.py          # Flask routes: dashboard, node edit, login/logout, /update/* JSON API
├── state.py        # Node (identity, config state, change queue, kotekan REST client), Registry
├── sync.py         # Queue-based sync: ChangeItem, Orchestrator (serialized submit + worker pool)
├── services.py     # Service-status helpers: FpgaMonitor (background poll) + job_status (systemd/mtime job health)
├── templates/      # Jinja2 templates (Pico CSS + htmx)
└── static/         # Vendored assets: pico.min.css, htmx.min.js, idiomorph-ext.min.js, Sortable.min.js
jobs/                               # One subdir per job: units, wrapper, code
├── choco.service               # Main systemd service (Type=notify)
├── eop/                        # EOP broadcast job
│   ├── choco-eop-broadcast.service # Runs on choco start + daily timer
│   ├── choco-eop-broadcast.timer   # Daily at 12:00 UTC
│   ├── eop-broadcast.sh            # Wrapper: finds venv, calls eop_update.py
│   ├── eop_update.py               # EOP pipeline: generate table, merge with state, push to choco
│   └── eop_utils.py                # Vendored from kotekan (do not modify — update from upstream)
└── bffs/                       # Feed-flagging job
    ├── choco-bffs-flag.service     # Runs on choco start + 30 s timer
    ├── choco-bffs-flag.timer       # Every 30 s
    ├── bffs-flag.sh                # Wrapper: finds venv, calls bffs.py
    ├── bffs.py                     # The feed-flagging script (see jobs/bffs/README.md)
    ├── kotekan_io.py               # kotekan N² file reader
    ├── sources/                    # Flagging sources (manual, power-outlier, power, fpga, rfi)
    └── tests/                      # bffs test suite (run by ./choco.sh test)
```

`eop_utils.py` is vendored from [kotekan](https://github.com/kotekan/kotekan/) (`tools/earth_orientation/eop_utils.py`). It should not be modified in this repo.
