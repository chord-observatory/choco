# CLAUDE.md

Guidance for Claude Code when working in this repository.  This file holds
the rules that must keep holding; the reasoning behind them, with the
measurements and history, lives in `docs/design/` and is linked from each
bullet.  Add a sentence here only if a future change would be wrong without
it.

## Project Overview

**choco** (CHORD Config Orchestrator) manages [kotekan](https://github.com/kotekan/kotekan/)
instances on the CHORD telescope cluster.  It replaces the older **coco**
(gitignored reference copy in `coco/`) with a single-process Flask app:

- **Flask + gevent** — web UI with htmx polling of partial templates (no
  WebSockets), sign-in through the site proxy (Authelia), JSON API, served by gevent's WSGIServer.
- **Per-node sync workers** — one greenlet owns each node, drains its FIFO
  change queue, and reconciles the node against its desired config.
- **Config directory** — YAML on disk is the source of truth for desired
  state; runtime state is ephemeral and rebuilt from polling.

Kotekan is deployed by Ansible; choco only monitors and pushes config through
kotekan's REST API (port 12048: `/status`, `/config`, `/start`, `/kill`,
updatable endpoints).  choco also fronts **fpga_master** (F-engine,
crs-control:54321) and **power_db** (PDB power boards, a Pi on port 5000), and hosts
a family of timer-driven **jobs**.

## Build & Run

`choco.sh` wraps every operation.  **Keep it in step** with any change to
install steps, dependencies or run commands.

```bash
./choco.sh install   # venv, hash-locked deps, config seeding, systemd units (root)
./choco.sh run       # run against ./config.yaml (root, for the iptables redirects)
./choco.sh develop   # loopback-only dev instance: no auth, no TLS, dev/ configs
./choco.sh test      # main suite + the job suites (args forwarded to pytest)
./choco.sh lock      # regenerate requirements.lock after editing pyproject.toml
./choco.sh audit     # check pins, and the installed venv's leftovers, against PyPI and OSV (read-only)
```

Configuration is `config.yaml` (gitignored; start from `config.yaml.template`,
which documents every key).  Console scripts: `choco-server` (the daemon) and
`choco` (the CLI).

## Code Structure

```
choco/
├── app.py          # load_config, create_app, TLS, gevent server, `choco-server`
├── cli.py          # `choco`: stdlib client for the localhost JSON API
├── web.py          # Flask blueprint: all routes and partials
├── library.py      # the config library: entries, the save check (every user re-rendered), writes, the upstream pull
├── jobdetail.py    # what each job's page summarises from its files (JOB_DETAILS; no Flask)
├── state.py        # Node (config state, queue, kotekan REST client), Registry
├── sync.py         # ChangeItem, NodeWorker, Orchestrator
├── upstream.py     # GitHub mirror of kotekan's config/chord into the library (plan by blob sha, fetch, verify)
├── fpga.py         # FpgaMonitor (fpga_master poll + controls), GainArchive, FileArchive (h5read subprocess)
├── powerdb.py      # PdbMonitor: power_db poll and the one read-modify-write-verify behind every power control
├── pipeline_svg.py # dot layout presets, render_dot_svg, the SVG whitelist sanitizer
├── jobstatus.py    # job_status / timer_status / job_logs / read_state_json (systemd + state files)
├── manualflags.py  # the bffs manual override file, read and edited by one rule (shared with bffs)
├── monitor.py      # PolledMonitor / HttpMonitor: the header strip's pollers' shared lifecycle
├── http.py         # fetch(): the one HTTP request helper (retries, OSError) for state, fpga, powerdb, upstream
├── hosttool.py     # run_tool(): systemctl / journalctl / dot, the one way
├── pdbmap.py       # Master dish-input <-> PDB channel CSV + kotekan cross-check + power_db wire conventions (stdlib; shared with bffs)
├── instrument.py   # / (front page): wiring.csv, boards from dish_inputs + crs_board_remap, InstrumentMonitor (kotekan /metrics, switch eAPI, Prometheus), flow_diagram
├── datafiles.py    # /files scan and the DATA badge probe (threadpooled NFS access)
├── waterfalls.py   # Read side of the waterfall image tree (hub-safe, cached)
├── h5read.py       # h5py subprocess for the gain archive (never imported for its deps)
├── dishlabels.py   # dish_inputs table (dish_table, DishType) + per-element label layout (stdlib; shared with the jobs)
├── acqtree.py      # the data trees and their names: acquisitions, vis files, waterfall shards (stdlib)
├── observatory.py  # the site and the bright sources, once (stdlib; jobs only)
├── healpix.py      # 408 MHz HEALPix map: FITS reader + RING lookup (numpy; jobs only, never the web)
├── n2file.py       # kotekan N² file axes: element labels, freq, prod (h5py; jobs only, never the web)
├── jobclient.py    # loopback JSON client, state dirs, atomic writes, run plumbing (stdlib; jobs + CLI)
├── auth.py         # login sessions (signed cookie + in-memory users), proxy sign-in and roles, trusted hosts, localhost bypass decorator
├── templates/      # Jinja2; _*.html are htmx partials; pipeline/plot are standalone pages
└── static/         # pico.css, htmx, idiomorph, Sortable (vendored); bufferplot.js, pipeline.js
jobs/               # One dir per job: systemd units, code, tests; run-job.sh runs any of them
├── choco.service   # Main service (Type=notify)
├── eop/            # EOP table broadcast (on choco start + daily); eop_utils.py is vendored
├── bffs/           # Bad-feed flagging (30 s timer); sources/ = one module per signal
├── eigencal/       # Point-source gain calibration (10 min timer, self-gating)
├── waterfall/      # Append-only visibility waterfall PNGs (2 min timer)
├── skymap/         # Current-sky Mollweide plot, day + night PNGs (5 min timer)
├── mapmaker/       # Drift-scan sky maps, frequency as hue, sky + last day (2 min timer); CLEAN of the sky map (15 min timer)
├── pointing/       # Dish pointing from the site's angle table into vars.yaml (10 min timer); pauses the nodes it re-renders
└── acqnotes/       # Per-acquisition README + acq_info.yaml written into subset/ (hourly); timeline.yaml is curated history
configs/            # nodes.yaml, vars.yaml, pdb_map.csv, wiring.csv, the config library (<dir>/<file>.j2, legacy <group>/<node>.yaml|.j2)
tests/              # pytest; test_<module>.py per module, test_web_<area>.py for routes, test_route_auth.py sweeps every route's exposure
docs/design/        # Design rationale, one file per subsystem (see the end of this file)
```

## Rules that must keep holding

**Config model** ([sync.md](docs/design/sync.md))
- Four terms, used consistently: **base_config** (file text, YAML or Jinja2),
  **rendered_config** (rendered and parsed, no overrides), **desired_config**
  (rendered + updatable overrides; what is pushed), **updatable_config**
  (blocks marked `kotekan_update_endpoint`, stored in `.updatable/*.json`).
- No database.  Desired state is the YAML on disk; runtime state (status,
  started, maintenance) is ephemeral and rebuilt by polling.
- A node renders the file nodes.yaml's `config:` names (a library file,
  typically shared), else its legacy `<group>/<node>.{yaml,yml,j2}`.
  Includes resolve against the file's own directory first, then the configs
  root — kotekan's own loader order, so the same files render in both trees.
  A library save is rendered through every node that uses the file before
  it is written; selecting a node's file is a nodes.yaml edit and pauses the
  cluster like any registry rebuild.  Only the nodes whose file or include
  changed re-render, never the fleet.
- `chord/` is a mirror of kotekan's `config/chord/` (`upstream:` in
  config.yaml; the `/configs` Pull button, `choco config pull`).  A pull is
  one validated set, never a file at a time: the texts are rendered together
  through every node that uses any of them, every write lands before the
  sync loop hears of any, and a file removed upstream that a node still uses
  is kept and reported, never deleted.  The nodes a pull re-renders are put
  in maintenance first: a pull never restarts a node.
- Local edits are picked up by an mtime scan each tick; browser freshness is
  htmx polling.  Do not add inotify or WebSockets.
- A node whose config failed to load is never pushed to: `desired_config`
  would be incomplete.  Stopped nodes are still killed.

**Node ownership** ([sync.md](docs/design/sync.md))
- Each `Node` has exactly one owner, its `NodeWorker`.  Only the worker
  writes `status`, `last_seen`, `version`, `error`.  Request handlers set the
  desired flags (`started`, `maintenance`) and enqueue a `POLL`; they never
  probe kotekan to write status themselves.
- Every path through `NodeWorker._sync` leaves `node.status` reflecting that
  cycle's probe; the worker reads it to choose its cadence.
- The wake protocol in `NodeWorker.run` clears the event before testing the
  queue and nothing between them may yield to the hub.  Do not add logging
  to a socket handler or any other yielding call inside that window.
- Workers are never force-killed; `stop()` is a flag plus a wake.  A
  replacement worker joins the one it retires before its first cycle;
  that join lives in the worker, not in `apply_nodes_update`.
- Restart concurrency is bounded by `sync.max_concurrent_pushes`; polling
  concurrency is unbounded by design.

**Maintenance and started** ([sync.md](docs/design/sync.md))
- `maintenance=True` means no REST write to the node: `push_updatable`,
  `start` and `kill` all return `False`.  Every registry build (startup and
  every `nodes.yaml` reload) puts the whole cluster in maintenance.
- `Node.start(override_maintenance=True)` exists only for the operator's
  one-off start (`web._run_oneshot`).  The sync loop never passes it.
- `started` is discovered from kotekan at startup and after each registry
  rebuild; `nodes.yaml`'s value is only the pre-discovery default.
- Stopping is always `/kill`; kotekan's `/stop` is unreliable.  There is no
  `Node.stop()`.
- Drift detection is the delivery guarantee for job pushes: a node that was
  down when a group update landed catches up on its first successful poll.

**Never hand a caller's string to a tool or a path**
- Journal units come from `web._service_registry()`; dot layout presets from
  `pipeline_svg.PIPELINE_LAYOUTS`; a config path from nodes.yaml or a URL goes
  through `state.resolve_config_path` (relative, plain components, config
  suffix, inside the configs directory); buffer names must match `_BUFFER_NAME_RE`;
  gain dataset names are checked against the manifest; waterfall path parts
  against `NAME_RE` / `SHARD_RE` / `IMAGE_RE`; a manual bffs flag's label
  against the element axis in the job's state file; a PDB row-power
  request's row and polarization become channel addresses only through
  `pdbmap.row_entries`; an acquisition-notes request is a root index
  plus a directory the cached `/files` scan listed with notes, and a
  timeline request a root index whose scan saw `timeline.yaml`, and an
  archive check a root index the archive mirrors, asking the transfer host
  only about the names the scan listed; a GitHub
  listing's file names go through `resolve_config_path` too, and the raw
  URL is built from the validated name at the resolved commit, never from
  the listing's `download_url`.
  Extend the allowlist, never bypass it.
- kotekan-supplied markup reaches the DOM only through
  `pipeline_svg.sanitize_pipeline_svg` (whitelist reconstruction; unknown
  elements are unwrapped, never copied).  Plot panel DOM is built with
  `createElement`/`textContent`, never `innerHTML`.

**The instrument page** ([instrument.md](docs/design/instrument.md)) — the front page, `/`
- Read-only, and direct where choco can: the NIC counters from each node's
  kotekan `/metrics`; the switch over eAPI only with a `show`-only account,
  and `SwitchEapi` refuses any command that is not a `show` before sending
  it — never add another verb.  The PDU, the iDRACs, the dish supply and
  node_exporter come from Prometheus (at most every 30 s); a detail it lacks
  is added to its exporter in chord-sysops.
- The page polls every 5 s while open (`PAGE_POLL_S`); the 30 s background
  loop is for the header badge.  Overlapping polls are skipped, not queued.
- Feeds are placed on boards from kotekan's own `dish_inputs` and
  `crs_board_remap`, never from a table of choco's; only the switch wiring,
  which nothing reports, is kept by hand (`wiring.csv`, deployment data like
  `pdb_map.csv`) and is checked against the live rates, never trusted silently.

**Auth and exposure** ([auth.md](docs/design/auth.md), [ui.md](docs/design/ui.md))
- Every route requires login except `/metrics`, `/skymap.png` and
  `/skymap-night.png`, which are cross-host and must stay aggregate-only: no
  node names, hosts or configs.  `tests/test_route_auth.py` sweeps the URL
  map for this, the loopback bypass and CSRF; a new exception goes in its
  lists, deliberately.
- `/update/*`, `/oneshot/*` and `/api/*` (including the config library's
  `/api/configs`) bypass login for loopback callers
  (`localhost_or_login_required`); the jobs and the CLI use only these.  From
  any other address a write there needs a login, the CSRF token
  (`X-CSRF-Token`) and a JSON body; only this host scripts choco.
  `/api/fpga/gain-data` is login-only even from loopback.  FPGA and PDB
  controls stay login + CSRF because their audit line names a person or a
  trusted-host label.
- `server.trusted_hosts` (default empty) logs a listed peer in as a synthetic
  `User(trusted=True)` named by its label.  CSRF stays on; the user never
  enters `_users`, so its cookie is worthless from any other address; a real
  login is never replaced.  It is safe only because `remote_addr` is always
  the real TCP peer: nothing reads `X-Forwarded-For` (no `ProxyFix`).
- `server.proxy_auth` (default unset) is how the site proxy is recognised:
  its address **and** a shared secret (`X-Choco-Proxy-Secret`, stripped by
  `auth.ProxyMiddleware`).  Only such a request is served under the prefix
  (`SCRIPT_NAME`) and signed in as the proxy's `Remote-User`, a real user in
  `_users`, replaced when the header names someone else.  The peer may not
  be loopback nor in `trusted_hosts`; logout behind the proxy continues to
  its `logout_url`.
- Roles exist only through the proxy: with `proxy_auth.write_groups` set, a
  proxied user in none of those `Remote-Groups` is `read_only`, recomputed
  every request.  `auth._refuse_read_only_writes` refuses every
  POST/PUT/PATCH/DELETE for them in one place; never gate a write route by
  route.  The UI keeps write controls visible but inert under `.read-only`
  (`choco.css`): anything that writes from a script carries `.write-control`.
  Trusted hosts and loopback callers are never read-only.
- choco has no login of its own: `/login` (GET only) points to
  `proxy_auth.public_url` and, for emergencies, an ssh tunnel to loopback.
  An `ldap:` block is refused; do not bring back a password form.
- Every URL a page builds starts at the app root, so choco works under the
  proxy's prefix: `url_for`, or `{{ request.script_root }}` before a path
  in a template (`request.script_root ~ '/x'` in a Jinja expression), and
  in a script the page's `<meta name="choco-root">`.  Python modules without
  Flask (`waterfalls.py`) return root-relative paths for the template to
  prefix.  `tests/test_proxy_auth.py` renders every route behind the proxy
  and checks the sources.
- Keep the startup guardrails: a placeholder or short `server.secret_key` is
  refused; `server.dev_auth` requires a loopback `server.host`; the
  session cookie (`choco_session`, never Flask's shared default name) is Secure (when `server.ssl`), HttpOnly, SameSite=Lax;
  `_next_target` rejects `//`, backslashes and control characters.
- Dev mode turns login **and** CSRF off together (the token lives in the
  session cookie) and re-establishes the synthetic login per request.

**UI palette** ([ui.md](docs/design/ui.md))
- Status colour is a tone, never a hex value: templates call the `*_tone`
  macros and `tag()` in `_service_macros.html`, which emit
  `tag-ok|bad|warn|info|maint|off` painted by the tokens in
  `static/choco.css` (loaded by `base.html` and the standalone pipeline and
  plot pages).  No hex literal for a state in a template, no brown, and the
  CHOCO mark (`.brand`) stays grey.  The strip's tags are `quiet`: no tag
  carries a word (it lives in the tooltip and `aria-label`), so the strip
  never shifts with the state; ok, info and off show label and dot on
  neutral ground, warn and bad keep their tint.
- One page chrome: `.page-head` (title left, actions right), `.note` for
  prose, `h3` for sections, `.btn-sm secondary outline` for row-level
  actions with Pico's filled primary kept for the page's one main action,
  `.notice` for static warnings (`.flash` is transient only).  Controls are
  not status: switches are tinted only when on, group buttons colour their
  glyph text on the tone's soft tint, never a solid fill.
- Type is IBM Plex Sans / Plex Mono, vendored in `static/fonts/` (keep
  `static/fonts/*` in pyproject's package-data); no page loads anything from
  an outside host.  600 is the site's bold, never 700.  The base size is
  pinned to 125% of the browser default (20 px) from 576 px up, 100% below: Pico's fluid scale stays off, the grids
  are sized to it.  Titles are "Page — CHOCO"; deep pages carry a crumb.
- Below 1024 px nothing scrolls the page sideways: a wide table is a
  `.stack` (one card per row: `.stack-lead` / `.stack-end` share the first
  line, `data-label` captions the inline cells, `.stack-full` takes a line)
  or sits in a `.scroll-x`.  A new wide table picks one.

**Process hygiene**
- The web process never imports numpy, h5py, astropy or matplotlib (nor
  `choco.n2file` / `choco.healpix`; `tests/test_n2file.py` checks).  HDF5 is
  read by `choco.h5read` as a subprocess; the scientific stack lives in the
  `[jobs]` extra.
- Blocking filesystem work on NFS (`datafiles.py`, `waterfalls.py`) runs in
  gevent's threadpool with a timeout; a wedged mount must never block the hub.
- Host tools over Python dependencies: `openssl` for the self-signed cert,
  `dot` for graphs, `systemctl`/`journalctl` for job status, all run through
  `hosttool.run_tool`; every HTTP request the web process makes goes through
  `http.fetch`.  Before adding a
  dependency, check for a stdlib or existing-dep answer
  ([dependencies.md](docs/design/dependencies.md)).  Production installs are
  pinned and hash-locked; run `./choco.sh lock` after touching
  `pyproject.toml` and commit the diff.

**Jobs** ([jobs.md](docs/design/jobs.md))
- A job is a separate process (never an in-process greenlet) that pushes
  through the localhost JSON API, keeps state under `/var/lib/choco/<name>/`
  (`StateDirectory=choco/<name>`), has its own `/etc/choco/<name>.yaml`
  seeded by install, and ships `choco-<name>.service` + `.timer` in
  `jobs/<name>/`.
- State paths are a convention, never a config key.  A job resolves its
  directory with `choco.jobclient.job_state_dir` (`--state-dir`, else
  `$STATE_DIRECTORY`, else `/var/lib/choco/<name>`); choco reads
  `<state_dir>/<job>/<file>` by name.  Both sides refuse the retired path
  keys (`state_file`, `state.path`, `run_path`, `archive_dir`, `lock_file`,
  `output`, `image_file`) rather than read them.
- Exit codes: **0** ok or nothing to do, **2** degraded (a dependency or
  input was unavailable; retries self-heal; badge yellow), **1** failed
  (config error or bug; badge red).  Inside `main`: `OSError` and
  `Degraded` → 2, `ValueError`/`yaml.YAMLError` → 1 (`jobclient.failure`).
- A new badge costs one entry in `web._JOBS` plus, optionally, a summariser
  in `jobdetail.JOB_DETAILS` with its `_detail_<name>.html`.  A job shown on another page
  sets the entry's `page` instead (the waterfall and acqnotes on `/files`,
  folded into the DATA tag; the skymap on `/service/mapmaker`, folded into
  MAP): no badge or landing row of its own and `/service/<name>`
  redirects, but `/api/status` and `/metrics` still report it by name.
- Jobs reach choco through `choco.jobclient` and derive feed labels through
  `choco.dishlabels`; both are stdlib-only so the jobs' venv is the only
  requirement.  Do not copy either into a job.  The run plumbing is
  jobclient's too (`Degraded`, `failure`, `setup_logging`, `single_run`,
  `refuse_retired`, `atomic_output`, `write_run`, `push_updatable`): a job
  uses it rather than writing its own, and every unit runs its script
  through `jobs/run-job.sh`.  bffs also shares `choco.pdbmap` (power_db's
  wire conventions) and `choco.manualflags` (the override file) with the
  web process; neither imports anything the jobs' venv lacks.
- A bffs source that cannot measure a feed leaves it good and reports
  `degraded`; "no data" is never turned into "bad" (power-outlier abstains
  below `min_coverage`, rfi skips down nodes and stale gauges).  Absence from
  the master PDB table is inventory, not missing data: the power source
  flags it (`not in PDB table`) when the table is trusted — an operator CSV,
  or choco's table with no row kotekan does not know — and abstains
  otherwise.  Run facts go to `run.json` every run; `state.json` changes
  only with the bad list.  eigencal follows the same split: `run.json`
  every exit with the reason, `state.json` per solution, the archives
  `gain_*.h5` beside them; its page reads all three.
- N² reads are priced per `vis` chunk (16 channels × every product ×
  every bin, 42 MB at 128 elements; a chunk decompresses whole, and the
  link delivers 36–59 MB/s against a 12 GB file every 400 s).  Every reader
  opens whole aligned chunks, as few as it needs, never a channel or
  product slice across the band: bffs `blocks`, acqnotes
  `extract.chunk_blocks`, the mapmaker's `subband_read_channels`, the
  waterfall's `freq_stride`, `file_stride` and `products` (a selection
  fixed per acquisition in its index; its overview cubes come from the
  same pass and the web renders their slices with the stdlib, never
  numpy) ([jobs.md](docs/design/jobs.md),
  [waterfall.md](docs/design/waterfall.md)).  bffs also takes validity
  and weight from `frames_added`, not `vis_weight`, and caches the
  power-outlier verdict per file (`power_outlier.json`: path, mtime,
  size, parameters) because a landed file never changes; spectral-shape's
  history of recent files (`spectral_shape.json`) is its cache the same way.
- bffs joins the file to the flag axis by label, never by position: a
  file label the config does not know sidelines the file like a stale
  one (file sources skipped), it does not fail the run.  The config's
  `dish_inputs` `type` is
  the authority on what is a feed: `Missing` elements are always bad
  (`dish-type` source) and `RFIDish` elements are never power-outlier
  flagged nor part of its median (`exclude_types`).
- `jobs/eop/eop_utils.py` is vendored from kotekan: do not modify, update
  from upstream.  EOP merging is append-only and never overwrites a stored
  entry ([jobs.md](docs/design/jobs.md)).

**kotekan compatibility**
- Only the 2026-08 per-dish `dish_inputs` layout is accepted (each dish named
  once, per-element labels derived as label + X/Y).  Pre-2026-08 per-element
  tables are refused everywhere because their element ordering was wrong.
- N² files are labelled per element since kotekan chord.2021.10+988
  (2026-09-11): `index_map/label` has one entry per element of the file's
  own axis, compact `DishInputs` frames included, plus `index_map/pol`.  Two
  spellings are on disk — `B4p1`/`B4p2` (+988 to PR #1695) and `A01X`/`A01Y`
  (PR #1695, 2026-09-22; in the data from 2026-10-01) — and both are read.
  Every reader goes through `n2file.element_labels`, which applies
  `dishlabels.file_element_labels` (`p1`→X,
  `p2`→Y, count and `pol` cross-checked; the X/Y spelling only when
  `index_map/pol` is present, since by text alone it is the pre-2026-08
  layout) and refuses any other file layout: no expansion of per-dish
  tables, no `input_list` mapping, no positional guess — bffs and eigencal
  exit degraded, the waterfall stores no labels
  ([waterfall.md](docs/design/waterfall.md)).
- The fleet runs mixed kotekan versions: both pipeline palettes are styled
  (guarded by `tests/test_pipeline_palette.py`), and both N² subset wire
  forms (sparse `product_list`, compact `input_list`) are decoded, with the
  computed layout cross-checked against kotekan's `frame_size`.  A compact
  `DishInputs` descriptor from kotekan develop carries no list: the peek
  route fills it in from the pushed `dish_inputs` table
  (`dishlabels.connected_elements`), count-checked, never guessed.
- Peeks speak only `GET /buffer_frame?name=&len=`; 402/404/500 from kotekan
  are meaningful replies, not outages.
- Imaging conventions, fixed on a Cyg A transit ([mapmaker.md](docs/design/mapmaker.md)):
  hour angle is the file's `bin_ERA_deg` + `itrs_lon_deg` − CIRS RA; a
  baseline is `feed_positions_m[input_b] − feed_positions_m[input_a]`
  (x east, y north); a source at ENU `u` phases as `−k (b_x u_E + b_y u_N)`.
  N2Accumulate's fringestop (a config flag; the 40 ms frames of a 10 s bin
  to the pointing at the bin's ERA, never across bins) is already the
  frozen-sky-per-dump model; never undo it.  Gains are applied per
  channel before any frequency or redundancy average; colour bins average
  maps, never visibilities, and one stretch curve serves every bin.  The
  exposure is `Σ W B⁴` along the pointing row (not `Σ W B²`, which
  flattens the two-row Dec grating lobes, nor the pixel's own `Σ W B⁴`,
  which amplifies them), and the pointing declination is a config value
  (kotekan's `dish_coelev_deg` was wrong until 2026-10), checked against
  the files' `dish_coelev_deg` every run and warned on disagreement.  The deconvolution's dirty
  beam is computed from the same kernels and the per-baseline weights the
  scatter accumulated (`sky_blw`), never fitted, so the beam keys are part
  of the grid signature and the daily map is not deconvolved.

**Retired config keys are refused, not read.** `load_config` raises on
`sync.num_workers`, a `psu:` block, `eop.fpga_master_*`, the eop job's own
settings (now `/etc/choco/eop.yaml`), `waterfall.images_dir` (choco reads
the tree from the job's `state.json`), the `ldap:` block and every
per-job state path (`*.state_file`, `bffs.run_file`/`manual_file`,
`skymap.image_file`/`night_image_file`), naming the replacement.  Do not add
a silent fallback for a renamed key; extend `_RETIRED_KEYS` instead.
`choco.sh install` loads the config under the new code before restarting
anything, so a retired key stops the install, not the service.

**Who each document is for.** `README.md` is for people who run and use
choco: technical, professional English, close to ASD-STE100 without being
bound by it.  Be concise and explain (one idea per sentence, the reason
when it helps the reader act), and use the terms the README or choco
already use (`base_config`, `desired_config`, the job, route and key
names) rather than coin new ones.  This file holds the rules for agents;
`docs/design/` holds the reasoning and the history.

## Design docs

- [sync.md](docs/design/sync.md) — config model, config library and includes, workers, maintenance, one-offs, drift, load resilience
- [cli.md](docs/design/cli.md) — the `choco` command
- [auth.md](docs/design/auth.md) — config.yaml, the site proxy and roles, trusted hosts, dev mode
- [ui.md](docs/design/ui.md) — URL layout, service strip, service pages, `/api/status`, `/metrics`
- [fpga-pdb.md](docs/design/fpga-pdb.md) — gain archive, FPGA/PDB controls, master PDB map
- [instrument.md](docs/design/instrument.md) — the signal-path page: Prometheus as the only source, the switch wiring table and how it was first inferred
- [pipeline.md](docs/design/pipeline.md) — pipeline page, frame peeks, `bufferplot.js`
- [datafiles.md](docs/design/datafiles.md) — `/files` and the DATA badge
- [waterfall.md](docs/design/waterfall.md) — the append-only PNG renderer and its read side
- [skymap.md](docs/design/skymap.md) — the sky-map job
- [mapmaker.md](docs/design/mapmaker.md) — the drift-scan map job: incremental scatter-add, Cyg A self-calibration, the two-row Dec grating lobes
- [jobs.md](docs/design/jobs.md) — the jobs pattern and the EOP merge policy
- [dependencies.md](docs/design/dependencies.md) — the 2026-09 dependency audit
