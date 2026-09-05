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
  WebSockets), LDAP auth, JSON API, served by gevent's WSGIServer.
- **Per-node sync workers** — one greenlet owns each node, drains its FIFO
  change queue, and reconciles the node against its desired config.
- **Config directory** — YAML on disk is the source of truth for desired
  state; runtime state is ephemeral and rebuilt from polling.

Kotekan is deployed by Ansible; choco only monitors and pushes config through
kotekan's REST API (port 12048: `/status`, `/config`, `/start`, `/kill`,
updatable endpoints).  choco also fronts **fpga_master** (F-engine,
chive:54321) and **power_db** (PDB power boards, a Pi on port 5000), and hosts
a family of timer-driven **jobs**.

## Build & Run

`choco.sh` wraps every operation.  **Keep it in step** with any change to
install steps, dependencies or run commands.

```bash
./choco.sh install   # venv, hash-locked deps, config seeding, systemd units (root)
./choco.sh run       # run against ./config.yaml (root, for the iptables redirects)
./choco.sh develop   # loopback-only dev instance: no auth, no TLS, dev/ configs
./choco.sh test      # main suite + the four job suites (args forwarded to pytest)
./choco.sh lock      # regenerate requirements.lock after editing pyproject.toml
./choco.sh audit     # check pins against PyPI and OSV (read-only)
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
├── state.py        # Node (config state, queue, kotekan REST client), Registry
├── sync.py         # ChangeItem, NodeWorker, Orchestrator
├── services.py     # FpgaMonitor, GainArchive, PdbMonitor, job/systemd helpers, dot + SVG sanitizer
├── pdbmap.py       # Master dish-input <-> PDB channel CSV + kotekan cross-check
├── datafiles.py    # /files scan and the DATA badge probe (threadpooled NFS access)
├── waterfalls.py   # Read side of the waterfall image tree (hub-safe, cached)
├── h5read.py       # h5py subprocess for the gain archive (never imported for its deps)
├── auth.py         # Flask-Login + direct ldap3 bind, localhost bypass decorator
├── templates/      # Jinja2; _*.html are htmx partials; pipeline/plot are standalone pages
└── static/         # pico.css, htmx, idiomorph, Sortable (vendored); bufferplot.js, pipeline.js
jobs/               # One dir per job: systemd units, wrapper .sh, code, tests
├── choco.service   # Main service (Type=notify)
├── eop/            # EOP table broadcast (on choco start + daily); eop_utils.py is vendored
├── bffs/           # Bad-feed flagging (30 s timer); sources/ = one module per signal
├── eigencal/       # Point-source gain calibration (10 min timer, self-gating)
├── waterfall/      # Append-only visibility waterfall PNGs (2 min timer)
└── skymap/         # Current-sky Mollweide plot (5 min timer)
configs/            # nodes.yaml, vars.yaml, pdb_map.csv, <group>/<node>.yaml|.j2
tests/              # pytest; test_<module>.py per module, test_web.py for routes
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
- Workers are never force-killed; `stop()` is a flag plus a wake.
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
  `services.PIPELINE_LAYOUTS`; buffer names must match `_BUFFER_NAME_RE`;
  gain dataset names are checked against the manifest; waterfall path parts
  against `NAME_RE` / `SHARD_RE` / `IMAGE_RE`.  Extend the allowlist, never
  bypass it.
- kotekan-supplied markup reaches the DOM only through
  `services.sanitize_pipeline_svg` (whitelist reconstruction; unknown
  elements are unwrapped, never copied).  Plot panel DOM is built with
  `createElement`/`textContent`, never `innerHTML`.

**Auth and exposure** ([auth.md](docs/design/auth.md), [ui.md](docs/design/ui.md))
- Every route requires login except `/metrics` and `/skymap.png`, which are
  cross-host and must stay aggregate-only: no node names, hosts or configs.
- `/update/*`, `/oneshot/*` and `/api/*` bypass login for loopback callers
  (`localhost_or_login_required`); the jobs and the CLI use only these.  FPGA
  and PDB controls stay login + CSRF because their audit line names a person.
- Keep the startup guardrails: a placeholder or short `server.secret_key` is
  refused; `server.dev_auth` requires a loopback `server.host`; LDAPS
  verifies the server certificate (`Tls(CERT_REQUIRED)`, passed
  unconditionally); a missing `ldap.ca_cert` file is a startup error; the
  session cookie is Secure (when `server.ssl`), HttpOnly, SameSite=Lax;
  `_next_target` rejects `//`, backslashes and control characters.
- Dev mode turns login **and** CSRF off together (the token lives in the
  session cookie) and re-establishes the synthetic login per request.

**Process hygiene**
- The web process never imports numpy, h5py, astropy or matplotlib.  HDF5 is
  read by `choco.h5read` as a subprocess; the scientific stack lives in the
  `[jobs]` extra.
- Blocking filesystem work on NFS (`datafiles.py`, `waterfalls.py`) runs in
  gevent's threadpool with a timeout; a wedged mount must never block the hub.
- Host tools over Python dependencies: `openssl` for the self-signed cert,
  `dot` for graphs, `systemctl`/`journalctl` for job status.  Before adding a
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
- Exit codes: **0** ok or nothing to do, **2** degraded (a dependency or
  input was unavailable; retries self-heal; badge yellow), **1** failed
  (config error or bug; badge red).  Inside `main`: `OSError` → 2,
  `ValueError`/`yaml.YAMLError` → 1.
- A new badge costs one entry in `web._service_registry()` plus, optionally,
  a summary branch in `web._service_detail`.
- `jobs/eop/eop_utils.py` is vendored from kotekan: do not modify, update
  from upstream.  EOP merging is append-only and never overwrites a stored
  entry ([jobs.md](docs/design/jobs.md)).

**kotekan compatibility**
- Only the 2026-08 per-dish `dish_inputs` layout is accepted (each dish named
  once, per-element labels derived as label + X/Y).  Pre-2026-08 per-element
  tables are refused everywhere because their element ordering was wrong.
- The fleet runs mixed kotekan versions: both pipeline palettes are styled
  (guarded by `tests/test_pipeline_palette.py`), and both N² subset wire
  forms (sparse `product_list`, compact `input_list`) are decoded, with the
  computed layout cross-checked against kotekan's `frame_size`.
- Peeks speak only `GET /buffer_frame?name=&len=`; 402/404/500 from kotekan
  are meaningful replies, not outages.

**Compatibility shims still in place** (remove once the deployed
`config.yaml` is migrated): `sync.num_workers`, `eop.fpga_master_host/port`,
the `psu:` block and `/service/psu`, a relative `eop.state_file`, the
`/var/lib/<job>` migration in `choco.sh`, and the ignored flask-ldap3-login
keys.

## Design docs

- [sync.md](docs/design/sync.md) — config model, workers, maintenance, one-offs, drift, load resilience
- [cli.md](docs/design/cli.md) — the `choco` command
- [auth.md](docs/design/auth.md) — config.yaml, LDAP direct bind, dev mode
- [ui.md](docs/design/ui.md) — landing page, service strip, service pages, `/api/status`, `/metrics`
- [fpga-pdb.md](docs/design/fpga-pdb.md) — gain archive, FPGA/PDB controls, master PDB map
- [pipeline.md](docs/design/pipeline.md) — pipeline page, frame peeks, `bufferplot.js`
- [datafiles.md](docs/design/datafiles.md) — `/files` and the DATA badge
- [waterfall.md](docs/design/waterfall.md) — the append-only PNG renderer and its read side
- [skymap.md](docs/design/skymap.md) — the sky-map job
- [jobs.md](docs/design/jobs.md) — the jobs pattern and the EOP merge policy
- [dependencies.md](docs/design/dependencies.md) — the 2026-09 dependency audit
