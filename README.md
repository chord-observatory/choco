# choco

**CHORD Config Orchestrator** — monitors and manages [kotekan](https://github.com/kotekan/kotekan/) instances running on a cluster of nodes.

choco provides a web UI that shows the live status of every kotekan instance, detects when their configs drift from the desired state, and lets you push config updates. It talks to kotekan's built-in REST API, so no agent software is needed on the nodes.

Kotekan itself is deployed and managed on nodes by Ansible. choco only handles monitoring and config management.

Around that core, choco also fronts the observatory's companion services: header badges and `/service/<name>` pages for the **fpga_master** daemon (with start/stop controls) and the **power_db** power distribution boards (with per-channel and bulk power toggles), plus a family of timer-driven **jobs** that push through choco's API — EOP broadcast, bad-feed flagging (bffs), and point-source gain calibration (eigencal).

## Requirements

- Python 3.10+
- A FreeIPA server for LDAP authentication (e.g. `ipa1.auth.chord-observatory.ca`)
- Kotekan instances reachable over HTTP (default port 12048)
- graphviz (`dot`) — optional; renders the pipeline graph on node pipeline pages
  (without it the raw dot text is shown instead)

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
| `/etc/choco/configs/` | Kotekan config files (nodes.yaml, group dirs, `.updatable/`) plus `pdb_map.csv` |

The install script also:
- Creates a local `.venv` in the repo directory (editable install, owned by invoking user) for development
- Installs Python dependencies **pinned and hash-locked by `requirements.lock`** — pip runs with `--require-hashes`, and each pin lists the sha256 of every artifact PyPI serves for that version, so a deploy gets exactly the reviewed versions and refuses a substituted or tampered file even at the same version number (choco itself goes on top with `--no-deps`); regenerate the lock with `./choco.sh lock` after changing `pyproject.toml`, review the diff, and commit it. `./choco.sh audit` checks every pin (including pip itself, which is in the lock) against the latest PyPI releases and the OSV vulnerability database without changing anything — it exits non-zero if a pinned version has a known advisory, so it can run from cron or CI
- Symlinks the `choco` command-line client (the venv's `choco` console script; see JSON API below) to `/usr/local/bin/choco`; the daemon's entry point is `choco-server`
- Sets up iptables rules to redirect ports 443 -> 5000 and 80 -> 8080 (persisted via `iptables-persistent`)
- Installs and enables a systemd service that starts on boot and restarts on failure
- Installs every job's units from `jobs/*/choco-*.{service,timer}` (EOP, bffs, eigencal), enabling the services and starting the timers
- Seeds `/etc/choco/config.yaml` (from the repo's local `config.yaml` or the template) and `/etc/choco/configs/` from the repo's `configs/` directory on first install, and each job's config (`bffs.yaml`, `eigencal.yaml`, `eigencal_feeds.yaml`) from its example file; on subsequent installs, prompts whether to overwrite kotekan configs (use `--overwrite-configs` or `--keep-configs` to skip the prompt) and **never overwrites** the deployed `config.yaml`, edited job configs, or `configs/pdb_map.csv` — a diverged repo copy is staged as `config.yaml.new` / `pdb_map.csv.new` instead

Re-running `sudo ./choco.sh install` is safe — **it never overwrites a deployed `/etc/choco/config.yaml`**. On first install the config is seeded from the repo's local `config.yaml` (or the template) with `configs_dir` rewritten to `/etc/choco/configs`; on later installs, if the repo copy differs from what's deployed, the incoming version is staged as `/etc/choco/config.yaml.new` for manual merging and the deployed file is left alone. Kotekan configs prompt before overwriting; `configs/pdb_map.csv` — the master PDB channel map, which may be the only authoritative record of that wiring — is excluded from that overwrite entirely and gets the same seed-once-then-stage-a-`.new` treatment as `config.yaml`; iptables rules are deduplicated.

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
  secret_key: change-me           # Placeholder: choco refuses to start with it (install seeds a random key)
  log_level: INFO
  ssl_cert:                       # Leave empty to auto-generate a self-signed cert
  ssl_key:
  http_redirect_port: 8080        # HTTP listener that redirects to HTTPS

configs_dir: configs

kotekan:
  timeout: 10                     # HTTP request timeout (seconds) for kotekan REST calls

sync:
  poll_interval: 5                # Seconds between polling all nodes for drift
  restart_timeout: 10             # Seconds to wait for kotekan to restart after /kill
  num_workers: 4                  # Worker greenlets processing node queues

fpga_master:
  host: chive.site.chord-observatory.ca
  port: 54321
  timeout: 5                      # HTTP request timeout (seconds)
  control: true                   # show Start/Stop controls on /service/fpga

pdb:                              # power_db power distribution boards
  host: 10.222.0.30
  port: 5000
  timeout: 5
  control: true                   # allow power toggles on /service/pdb
  map_file: pdb_map.csv           # master dish-input <-> channel table (in configs_dir)
  # kotekan_group: cx             # group whose dish_inputs the map is checked against

eop:
  intervals_before: 2             # Days of past entries (older stored entries are truncated on merge)
  intervals_after: 2              # Days of future entries (later stored entries are kept, never overwritten)
  endpoint: earth_rotation_data   # Kotekan updatable config endpoint name
  state_file: /var/lib/eop/state.json  # Absolute; rewritten on every successful run (a relative path resolves against configs_dir — legacy layout)
  service_unit: choco-eop-broadcast.service  # systemd unit for last-run status

bffs:
  service_unit: choco-bffs-flag.service
  state_file: /var/lib/bffs/state.json

eigencal:
  service_unit: choco-eigencal.service
  state_file: /var/lib/eigencal/state.json

ldap:
  host:                           # e.g. ldaps://ipa1.auth.chord-observatory.ca
  port: 636
  use_ssl: true
  base_dn:                        # e.g. dc=auth,dc=chord-observatory,dc=ca
  user_dn: cn=users,cn=accounts
  user_login_attr: uid
```

`config.yaml` contains secrets and is chmod 600. Only `config.yaml.template` is checked into the repo.

#### LDAP Authentication (FreeIPA)

choco authenticates against a FreeIPA LDAP directory by **direct bind**: the user's DN is strung together as `<user_login_attr>=<username>,<user_dn>,<base_dn>` and bound with their own password — the bind itself proves the credentials, so **no service account is needed**. The defaults are tuned for FreeIPA (`cn=users,cn=accounts` user DN, `uid` login attribute, LDAPS on port 636). Legacy keys from the old search-bind implementation (`bind_dn`, `bind_password`, `user_object_filter`, `user_search_scope`) are ignored if present, so an existing `config.yaml` keeps working — and the bind account's credential can be removed from it.

The LDAPS connection **verifies the server's certificate and hostname** (ldap3's own default would accept any certificate, which lets anyone on the path to the IPA server answer "bind succeeded" to any password). By default it verifies against the system CA store, where `ipa-client-install` has already placed the IPA CA; `ldap.ca_cert` points at a PEM bundle instead, and a path to a missing file is a startup error rather than a silent fallback. `use_ssl: false` is allowed but logged as a warning, since passwords would then cross the network in cleartext.

Two other startup-time guardrails sit alongside: `server.secret_key` may not be a placeholder or shorter than 16 characters (it signs the session cookie, so a guessable key is a login bypass; `choco.sh install` generates one), and the session cookie is issued `Secure` (when `server.ssl` is on), `HttpOnly` and `SameSite=Lax`.

### Config Directory

The config directory (`/etc/choco/configs/`) is the source of truth for which nodes choco manages and what their base configs are.

```
/etc/choco/configs/
├── nodes.yaml          # Node registry
├── vars.yaml           # (optional) Shared Jinja2 template variables
├── pdb_map.csv         # Master dish-input <-> PDB channel table (deployment data)
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

#### `pdb_map.csv` - Master PDB Channel Map

Which power channel feeds which dish input. One CSV, hand-maintained, alongside `nodes.yaml`:

```csv
# Blank lines and #-comments are ignored, so keep section headings here.
spi_bus,board,chip,channel,dish_input,amplifier,notes
0,0,A,0,A1X,AMP-0000,
0,0,A,1,A1Y,AMP-0001,
```

`spi_bus,board,chip,channel` is the power_db channel address (`chip` is `A` or `B`, `channel` `0`–`7`); `dish_input` is the kotekan `dish_inputs` label (`correlator_input` and `label` are accepted as column aliases for older tables); `amplifier` and `notes` are free text shown on the page. Rows are validated one at a time — a bad address, a missing label, or a duplicate address is reported on the PDB page with its file line number and skipped, so one typo costs one channel rather than the whole table. Edits are picked up on the next page render, no restart needed.

This is the **single authority** for that wiring: it labels the `/service/pdb` grid, is served at `/api/pdb/map` (where bffs's `power` source reads it instead of keeping its own copy), and is cross-checked against a kotekan group's `dish_inputs` table — set `pdb.kotekan_group` to pick the group, otherwise the first group in `nodes.yaml` is used. Read that cross-check after every edit; it is the only thing that will tell you the table has drifted from what kotekan believes.

Because there may be no upstream source for this wiring, **treat the deployed copy as data, not as something the repo can regenerate**. `choco.sh install` seeds it once and then never replaces it — not even with `--overwrite-configs` — staging a differing repo copy as `pdb_map.csv.new` for you to merge by hand. Back it up with the rest of `/etc/choco`; the copy in the repo is a starting point, not a backup.

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
- **PDB** — colour-coded readout from the power_db power distribution boards. Green when `/status` responds and `/channel_states` decodes (the tooltip shows how many channels are powered); yellow when the controller is up but channel states can't be read; red when it's unreachable; grey when no `pdb` block is configured.
- **EOP** — health of the EOP broadcast job. Green (`ok`) when the last run succeeded and the EOP state file is fresh; yellow `degraded` when the run couldn't do its job for external reasons (fpga_master unreachable for `frame0`, IERS download down, choco not accepting); yellow `stale` when the state file is older than ~25 hours (the job rewrites it on every successful daily run); red `failed` for config errors or bugs; grey when the unit has never run.
- **BFFS** — health of the bffs feed-flagging job (`choco-bffs-flag.service`). Green `ok`, yellow `degraded` when flagging ran with reduced coverage or couldn't run for external reasons (no/stale kotekan data, nodes or choco unreachable), red `failed` for config errors. The state-file mtime is "last change" in the tooltip but doesn't age the badge, since bffs only rewrites its state when the bad-feed list changes.
- **EIGENCAL** — health of the eigencal gain-calibration job (`choco-eigencal.service`). Yellow `degraded` covers both dependency trouble and a solution that failed its quality gate (archived, not sent); red `failed` means a real error. The state-file mtime is "last calibration" in the tooltip but doesn't age the badge, since daytime transits are skipped by design.

Job health combines the unit's `Result` and `ExecMainStatus` from `systemctl show` with the job state file's mtime — no timestamp parsing. All jobs share one exit-code convention: **0 = ok, 2 = degraded (the job is fine, a dependency or input wasn't — retries self-heal), 1 = failed (config error or bug — needs a human)**. The strip is refreshed every 30 seconds via htmx; the FPGA poller runs as a single gevent greenlet on the same cadence.

### Service pages

**Clicking a badge** opens that service's detail page at `/service/<name>` (`choco`, `eop`, `bffs`, `eigencal`, `fpga`, `pdb` — an allowlist, not arbitrary units). Each job page shows the unit's last result, the timer's last/next run, a summary from the job's state file — the current bad-feed list and recent transitions for bffs, the EOP table's time span, the last processed transit for eigencal — and the unit's recent journal lines (`journalctl -u <unit>`, auto-refreshed, 50–1000 lines), so a red badge can be diagnosed without leaving the browser. Every service page's status block refreshes itself every ~5 s while the page is open (the journal refreshes every 30 s). The FPGA page shows the monitor's live view of the `fpga_master` daemon (health, run state, `frame0`, last start result) plus **Start/Stop controls**: Start hands fpga_master its own launch config (initialization runs in the background; the state block follows the progress), Stop waits out the F-engine shutdown in the background. Both ask for confirmation and are logged with the requesting username; set `fpga_master.control: false` to hide them. A **Recent actions** table on the page records each start/stop with who requested it and the outcome (a stop shows up first as in-flight, then as its completion) — note that fpga_master acknowledges a start before checking its state, so starting an already-running F-engine is a silent no-op whose "already started" verdict appears under *Last start result*. The page warns that a restart assigns a new `frame0`, which kotekan and the EOP table are anchored to. The PDB page shows the full per-bus board/chip/channel power grid, with **bulk power buttons** for a chip (8 channels, at the end of each row) and for a whole SPI bus (in the bus heading). Every cell is itself a toggle. All of it is logged with the requesting username, and `pdb.control: false` makes the grid read-only. Writes go over htmx and swap the grid in place, so the page never reloads or jumps; the outcome appears as a notice above the grid and stays there until the next action. A single-channel toggle asks for confirmation once per browsing session; **every** bulk write asks each time (a bus-wide power-up is hundreds of amplifiers at once). A toggle re-reads the chip's output register immediately before writing (other tools can also drive the controller), writes the changed byte, and confirms with a fresh read — if the state changed underneath, it reports the conflict instead of retrying. Bulk writes work the same way, one whole-byte write per chip, skipping chips already in the wanted state; if some writes fail the rest still go out and the message names what didn't take.

Below the grid, the page shows the **master channel map** — the dish-input ↔ power-channel wiring table (`configs/pdb_map.csv`, a CSV alongside `nodes.yaml`). Its `dish_input` values label the grid cells, so a channel reads `● A1X` rather than just `on`. choco re-reads the file whenever it changes and **cross-checks it against the `dish_inputs` table of a kotekan group's config** — the same table kotekan indexes its bad-input mask with — reporting dish inputs kotekan knows but the map doesn't place, rows naming feeds kotekan doesn't have, and any dish input claimed by two channels. Bad rows in the CSV are listed and skipped rather than taking the page down. The deployed copy is **treated as data, not as a repo artifact**: since there may be no upstream source for this wiring, `choco.sh install` seeds it once and then never replaces it — not even with `--overwrite-configs` — staging a differing repo copy alongside as `pdb_map.csv.new` for manual merging, exactly as it does for `config.yaml`. Back it up with the rest of `/etc/choco`. The table is served as JSON at `/api/pdb/map` (with the cross-check attached), which is where bffs's `power` source reads it from, so the file is the single authority instead of every consumer keeping a copy.

### Dashboard

The main page shows a table of all registered nodes with live-updating columns: node name, status, config, sync state, and Pipeline/Edit links.

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

### Node Pipeline (`/pipeline/<group>/<node>`)

Click **Pipeline** on a node (or "Live pipeline" from its edit page) for the live view — everything here is a kotekan read, so it works in maintenance mode too.

A standalone full-viewport page: slim header with CHOCO/edit links instead of the usual chrome, dark by default with a theme toggle. The graph fills the screen — drag to pan, scroll to zoom around the cursor (trackpad pinch works too; shift+scroll stays a plain scroll), Fit/1:1 buttons, and a layout selector (curves (default) / ortho / polyline edge routing, an allowlisted `?layout=` preset that sticks in the URL, since crossing-heavy graphs read differently in each style). Zoom survives a refresh or a layout change.

The graph is a snapshot at fetch time, drawn from the running pipeline: each buffer's array layout and fullness (`3/24 (12.5%)` = full frames / total frames — a high fraction means the consumer is falling behind), measured frame and byte rates, per-stage CPU and GPU kernel times, with a red outline on a buffer that has no free frame left and a dashed one on a buffer nothing has ever flowed through. Stages sit in the config section they were declared in; GPU stages instead group by the card they drive, one region per `gpu_id`. The whole thing is recolored for dark mode via CSS on the inline SVG — fills go dark, outlines brighten, each node type keeping its hue, so blue buffers, amber CPU stages, orange GPU regions, violet I/O, magenta device memory and teal endpoints stay apart in either theme.

Buffers with `peek_hold` carry a persistent amber outline marking them clickable — click one (or tab to it and press Enter) and its live plot opens as a popup overlay in the corner, keeping the graph in view; the overlay's metadata button shows that frame's descriptor and metadata JSON. If the buffer list can't be read the page says so, rather than quietly showing a graph with nothing amber in it. The SVG is inlined after a whitelist-reconstruction sanitizer, so kotekan-supplied content can't run script.

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
- `GET /api/status` — simple overall health: choco itself (`up`, `started_at`), each service's health string (`fpga`, `pdb`, `eop`, `bffs`, `eigencal`), and node counts by status (plus `total`, `started_desired`, `maintenance`)
- `GET /api/nodes/status` — per-node runtime status plus an aggregate summary
- `GET /api/nodes` — the node registry (groups/hosts/ports) as JSON
- `GET /api/config/<group>` — a sample node's desired kotekan config for the group (jobs use this to learn the `dish_inputs` element layout)
- `GET /api/pdb/map` — the master dish-input ↔ power-channel table plus its cross-check against kotekan's `dish_inputs` (bffs's `power` source reads it from here)
- `GET /metrics` — the same overall health in Prometheus exposition format (see below)

One-off configs go the same way: `POST /oneshot/<group>` or `POST /oneshot/<group>/<node>` with `{"config_content": "..."}` starts the supplied config on nodes that are in maintenance and idle, recording nothing (200 with `started`/`skipped` per node, 409 if nothing started, 400 if the text does not render).

The `/update/*`, `/oneshot/*` and `/api/*` endpoints bypass auth when called from `localhost`, and the `choco` command wraps them — `choco/cli.py`, stdlib only, installed to `/usr/local/bin/choco` by `choco.sh install` (the daemon itself is `choco-server`):

```bash
choco status                        # overall health
choco nodes                         # per-node table (-j for the JSON)
choco get /api/config/cx            # any GET endpoint: /api/pdb/map, /api/files, /metrics, ...
choco stop cx recv/recv1            # desired state -> idle; targets are <group> or <group>/<node>
choco start cx/cx19
choco maint off cx                  # leave maintenance mode (on = pause)
choco push cx/cx19 new.yaml         # queue a base config (- reads stdin)
choco set cx updatable_config/bad_inputs '{"bad_inputs": [3, 7]}'   # or @values.json, or -
choco oneshot cx/cx19 once.yaml     # start an unrecorded config on a paused, idle node
choco help [command]                # usage (bare `choco` prints it too)
```

Every reply is printed as JSON. Exit status: 0 ok, 1 rejected (the server's error on stderr) or misused, 2 choco unreachable. `--url` or `CHOCO_URL` point it elsewhere (a `./choco.sh develop` instance prints the line to use). By hand it is plain `curl -ks` against `https://localhost:5000` with a JSON body, e.g. `-d '{"action":"set_started","started":false}'`.

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
  ok / no_timing / down / unconfigured / unknown; `pdb`: ok / no_states / down /
  unconfigured / unknown; `eop`, `bffs`, `eigencal`: ok / degraded / stale /
  failed / never_run / unknown — degraded = the job exited 2, meaning a
  dependency or input problem rather than a broken job)
- `choco_nodes{status}`, `choco_nodes_total`, `choco_nodes_started_desired`,
  `choco_nodes_maintenance` — node counts

Example alerts: `choco_service_state{service="eop",state="ok"} != 1` for a
day, `choco_nodes{status="down"} > 0`, or `changes(choco_start_time_seconds[1h]) > 0`.

> **Note:** the power controller's service label changed from `psu` to `pdb`
> when the service was renamed. Any dashboard or alert matching
> `service="psu"` needs updating; the old series simply stops being reported.

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

**Schedule** — `choco-eop-broadcast.service` runs once on `choco.service` startup (`After=choco.service`, `WantedBy=choco.service`) and again daily at 12:00 UTC via `choco-eop-broadcast.timer` (`Persistent=true`, so a missed firing catches up on boot). One-off runs: `sudo systemctl start --no-block choco-eop-broadcast.service` (use `--no-block`: the unit retries forever on failure — deliberately, so it self-recovers when the FPGA master comes back — and a plain `start` would wait through those retries indefinitely).

**Pipeline** (`jobs/eop/eop_update.py`):
1. Read `frame0_ns` from `fpga_master` over TCP.
2. Build a fresh EOP table on the UTC-midnight grid using `astropy` + IERS auto-download, covering `(now − intervals_before, now + intervals_after)` days.
3. If the state file (`eop.state_file`, default `/var/lib/eop/state.json`; a relative path resolves against `configs_dir`) exists, merge with stored state (policy below).
4. Wait for choco's web port, then `POST /update/<group>` for every group in `nodes.yaml`.
5. If *all* groups succeed, write the merged table back to the state file. On any failure, it is left alone so the next run merges from a known-good baseline.

**Merge policy** — the state file is the source of truth for what kotekan has been told; the merge protects continuity of any value that has already been pushed:

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

bffs reads two tables from choco rather than keeping its own copies: the feed
labels its flags are indexed against come from a group's kotekan `dish_inputs`
via `GET /api/config/<group>`, and the `power` source's channel→feed wiring
comes from the master PDB map via `GET /api/pdb/map`. Each run logs choco's
cross-check of that map against kotekan — a disagreement is a warning, not a
failure, since a label off the current element axis projects onto nothing (a
stale row leaves a feed unwatched; it cannot mis-flag one).

## Point-source gain calibration (eigencal)

A companion oneshot service runs [eigencal](jobs/eigencal/) every 10 minutes
via `choco-eigencal.timer`. The script **self-gates**: it exits in seconds on
all but one run per calibrator transit (Cyg A by default, at night), when it
reads the transit from the kotekan N² output, fits a complex gain for every
correlator input, archives one HDF5 file, and POSTs the gains to
`POST /update/<group>` for relay to kotekan.

Exit codes carry meaning: 0 is success *or* nothing-to-do, 2 means the
solution failed the quality gate (archived but not sent), 1 is an error —
so a red **EIGENCAL** badge means a real failure, not an idle daytime tick.
Its config lives at `/etc/choco/eigencal.yaml` (plus the feed-layout file
`eigencal_feeds.yaml`, which must be filled in before real use), seeded from
the examples in `jobs/eigencal/` on first install — see
[jobs/eigencal/README.md](jobs/eigencal/README.md) for the science and
config details.

```bash
sudo systemctl status choco-eigencal.timer   # cadence
sudo journalctl -u choco-eigencal -f          # per-run logs
```

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
├── cli.py          # `choco` command: stdlib client for the localhost JSON API
├── auth.py         # LDAP authentication (Flask-Login sessions + direct ldap3 bind)
├── web.py          # Flask routes: dashboard, node edit, /service/* pages, /update/* JSON API
├── state.py        # Node (identity, config state, change queue, kotekan REST client), Registry
├── sync.py         # Queue-based sync: ChangeItem, Orchestrator (serialized submit + worker pool)
├── services.py     # FpgaMonitor + PdbMonitor (polls, control wrappers) + job-status helpers
├── pdbmap.py       # Master dish-input <-> PDB channel table (CSV load, kotekan cross-check)
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
├── bffs/                       # Feed-flagging job
│   ├── choco-bffs-flag.service     # Runs on choco start + 30 s timer
│   ├── choco-bffs-flag.timer       # Every 30 s
│   ├── bffs-flag.sh                # Wrapper: finds venv, calls bffs.py
│   ├── bffs.py                     # The feed-flagging script (see jobs/bffs/README.md)
│   ├── kotekan_io.py               # kotekan N² file reader
│   ├── sources/                    # Flagging sources (manual, power-outlier, power, fpga, rfi)
│   └── tests/                      # bffs test suite (run by ./choco.sh test)
└── eigencal/                   # Point-source gain calibration
    ├── choco-eigencal.service      # Runs every 10 min; self-gates to one real run per transit
    ├── choco-eigencal.timer
    ├── eigencal.sh                 # Wrapper: finds venv, calls eigencal.py
    ├── eigencal.py                 # Orchestration: gates, ephemeris, fit, send (see jobs/eigencal/README.md)
    ├── n2_io.py                    # kotekan N² reader (full products)
    ├── transit_fit.py              # The batched transit fit (pure numpy)
    └── tests/                      # eigencal test suite (run by ./choco.sh test)
```

`eop_utils.py` is vendored from [kotekan](https://github.com/kotekan/kotekan/) (`tools/earth_orientation/eop_utils.py`). It should not be modified in this repo.
