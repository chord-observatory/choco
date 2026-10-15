"""Node registry and runtime state tracking."""

import copy
import json
import logging
import re
import time
from collections import deque
from enum import Enum
from pathlib import Path

import jinja2
import jinja2.loaders
from jinja2 import meta as _jinja_meta
import requests
import yaml

from .http import fetch
from .jobclient import write_json_atomic, write_text_atomic

logger = logging.getLogger(__name__)

# Config file extensions (order matters: later wins if both exist for same key)
CONFIG_SUFFIXES = (".yaml", ".yml", ".j2")
#: Where the updatable overrides live: ``<configs_dir>/.updatable/<group>/<node>.json``.
UPDATABLE_DIR = ".updatable"
#: kotekan's REST port, the one nodes.yaml may leave out.
DEFAULT_PORT = 12048

_UPDATABLE_MARKER = "kotekan_update_endpoint"

# PyYAML defaults to its pure-Python parser, which dominates config load
# cost (18.6 ms of a 21.3 ms render for a 13.6 KB config).  libyaml's C
# parser handles the same safe subset ~7.6x faster and raises the same
# yaml.YAMLError subclasses.  Fall back if PyYAML was built without it.
try:
    from yaml import CSafeLoader as _YamlLoader
except ImportError:  # pragma: no cover - depends on the PyYAML build
    from yaml import SafeLoader as _YamlLoader


def _yaml_load(stream):
    """``yaml.safe_load`` via libyaml's parser when it is available."""
    return yaml.load(stream, Loader=_YamlLoader)


# One component of a config path: a plain file or directory name.  No
# leading dot, so neither ``..`` nor anything hidden (the ``.updatable/``
# store) can be named.
PATH_PART_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.\-]*")


def resolve_config_path(configs_dir: Path, rel: str) -> Path:
    """The absolute path of config file *rel* under *configs_dir*.

    *rel* is operator-supplied text (a ``config:`` value in nodes.yaml,
    the path in a config-library route), so it is checked before it is
    joined to anything: relative, ``/``-separated, every component a
    plain name (no ``..``, nothing hidden, no backslashes or control
    characters), one of the config suffixes, not ``nodes.yaml`` (the
    registry, which has its own editor), and the result inside
    *configs_dir*.  Raises ``ValueError`` naming the problem.
    """
    if not isinstance(rel, str) or not rel:
        raise ValueError("config path must be a non-empty string")
    if "\\" in rel or any(ord(c) < 32 for c in rel):
        raise ValueError(
            f"config path {rel!r} contains a backslash or control character")
    if rel.startswith("/"):
        raise ValueError(
            f"config path {rel!r} must be relative to the configs directory")
    parts = rel.split("/")
    if any(not PATH_PART_RE.fullmatch(part) for part in parts):
        raise ValueError(
            f"config path {rel!r} has an empty, hidden or '..' component")
    if not rel.endswith(CONFIG_SUFFIXES):
        raise ValueError(
            f"config path {rel!r} must end in {' / '.join(CONFIG_SUFFIXES)}")
    if parts == ["nodes.yaml"]:
        raise ValueError("nodes.yaml is the node registry, not a config")
    root = Path(configs_dir).resolve()
    path = (root / rel).resolve()
    if root not in path.parents:
        raise ValueError(f"config path {rel!r} escapes the configs directory")
    return path


def list_config_files(configs_dir: Path) -> list[str]:
    """Relative (POSIX) paths of every config file under *configs_dir*.

    The library the web UI edits and nodes.yaml's ``config:`` selects
    from: ``.yaml`` / ``.yml`` / ``.j2`` files anywhere below the root,
    except hidden entries (so not the ``.updatable/`` store) and
    nodes.yaml itself.  Sorted, so listings are stable.
    """
    root = Path(configs_dir)
    if not root.is_dir():
        return []
    out = []
    for path in root.rglob("*"):
        if path.suffix not in CONFIG_SUFFIXES or not path.is_file():
            continue
        rel = path.relative_to(root)
        if any(part.startswith(".") for part in rel.parts):
            continue
        if rel.as_posix() == "nodes.yaml":
            continue
        out.append(rel.as_posix())
    return sorted(out)


class _OverlayLoader(jinja2.BaseLoader):
    """Serve unsaved text for the files in *overlay* (keyed by the
    absolute path the FileSystemLoader beside it would open), resolving
    template names against the same search *dirs*.  Lets a config-
    library edit be test-rendered through every node that includes the
    file before anything is written.  Names not in the overlay raise
    ``TemplateNotFound`` so the ``ChoiceLoader`` falls through to disk.
    """

    def __init__(self, dirs: list[Path], overlay: dict[Path, str]):
        self.dirs = dirs
        self.overlay = overlay

    def get_source(self, environment, template):
        pieces = jinja2.loaders.split_template_path(template)  # rejects '..'
        for d in self.dirs:
            candidate = d.joinpath(*pieces)
            if candidate in self.overlay:
                return self.overlay[candidate], str(candidate), lambda: False
        raise jinja2.TemplateNotFound(template)


def strip_updatable_values(config: dict) -> dict:
    """Return a deep copy of *config* with updatable config values removed.

    Any sub-dict (at any depth) that contains the key
    ``kotekan_update_endpoint`` is replaced with just that marker key,
    dropping the mutable value keys that kotekan may change at runtime.
    This lets two configs that differ only in updatable values compare as
    equal.
    """
    out = {}
    if not config:
        return out
    for key, value in config.items():
        if isinstance(value, dict):
            if _UPDATABLE_MARKER in value:
                out[key] = {_UPDATABLE_MARKER: value[_UPDATABLE_MARKER]}
            else:
                out[key] = strip_updatable_values(value)
        else:
            out[key] = value
    return out


def find_updatable_blocks(config: dict, _prefix: str = "") -> dict[str, dict]:
    """Find all updatable config blocks and return their endpoint paths + values.

    Walks *config* recursively.  Any sub-dict containing the
    ``kotekan_update_endpoint`` key is collected; its path (joined with ``/``)
    becomes the key and the values (without the marker) become the value.

    For example, this might return something like::

        {"updatable_config/flagging": {"start_time": …, …},
         "updatable_config/gains":    {"start_time": …, …},
         "updatable_config/26m_gated": {"enabled": False}}
    """
    blocks: dict[str, dict] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            path = f"{_prefix}/{key}" if _prefix else key
            if _UPDATABLE_MARKER in value:
                blocks[path] = {
                    k: v for k, v in value.items() if k != _UPDATABLE_MARKER
                }
            else:
                blocks.update(find_updatable_blocks(value, path))
    return blocks


class NodeStatus(Enum):
    UNKNOWN = "unknown"
    DOWN = "down"       # Unreachable
    IDLE = "idle"       # Reachable but kotekan not running (ready for /start)
    STARTED = "started" # kotekan running (whatever config; drift is checked separately)
    SYNCING = "syncing" # Push in progress (kill -> wait -> start with new config)


class Node:
    """A kotekan instance on the cluster.

    Each node owns its identity (name, group, host, port), its config
    state (base config file on disk, rendered config, updatable overrides),
    a FIFO change queue (drained by its ``sync.NodeWorker``), and an HTTP
    client for the kotekan REST API.

    Config lifecycle:
        - **base_content** — the on-disk file text (YAML or Jinja2)
        - **rendered_config** — base rendered through Jinja2 and parsed
        - **updatable_config** — runtime-mutable overrides stored in JSON
        - **desired_config** — rendered + updatable merged; what gets pushed

    The file rendered is nodes.yaml's ``config:`` for the node when set
    (a path under the configs directory, typically a library file that
    several nodes share), else the legacy per-node
    ``<group>/<name>.{yaml,yml,j2}``.  Either may ``{% include %}``
    other files; names resolve against the file's own directory first
    and the configs root second, which is how kotekan's own loader
    resolves them, so the same files render in both trees.  The
    include closure is recorded in ``dependencies`` so the sync loop
    knows which nodes a shared file's change re-renders.

    REST methods return ``None`` / ``False`` on connection failure rather
    than raising, so callers can treat unreachable nodes as a normal state.

    The *configs_dir* and *template_vars* parameters are optional so that
    the REST client can be used standalone in tests without a config
    directory.
    """

    def __init__(self, name: str, group: str, host: str,
                 port: int = DEFAULT_PORT, timeout: int = 10, *,
                 started: bool = False,
                 maintenance: bool = False,
                 configs_dir: Path | None = None,
                 template_vars: dict | None = None,
                 config: str | None = None):
        # Identity
        self.name = name
        self.group = group
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started = started
        # Maintenance mode: when True, push_updatable(), start() and kill()
        # are no-ops.  Ephemeral, never persisted to nodes.yaml.  The
        # ``Registry`` always constructs nodes with ``maintenance=True``
        # for production so a freshly-started choco never pushes before
        # the operator has reviewed the cluster state; the default here
        # is ``False`` so direct ``Node()`` construction in tests stays
        # in "normal mode" unless explicitly set.
        self.maintenance = maintenance
        self._base_url = f"http://{host}:{port}"

        # Config state (loaded from disk by load_config / load_updatable)
        self.configs_dir = configs_dir
        self.template_vars: dict = template_vars or {}
        self.base_content: str | None = None
        self.rendered_config: dict | None = None
        self._file_suffix: str = ".yaml"
        self.updatable_config: dict[str, dict] | None = None
        # nodes.yaml's ``config:``, validated here so a bad value shows
        # as this node's load error and is never joined to a path; None
        # means the legacy per-node file.
        self._config_file: str | None = config
        self._config_error: str | None = None
        if config is not None and configs_dir is not None:
            try:
                resolve_config_path(configs_dir, config)
            except ValueError as e:
                self._config_error = f"Bad config path: {e}"
        # Files the base config includes (absolute paths), from the last
        # load; see referenced_files.
        self.dependencies: set[Path] = set()
        self._env = self._make_env()

        # Runtime state (ephemeral, rebuilt from polling)
        self.status: NodeStatus = NodeStatus.UNKNOWN
        self.last_seen: float | None = None
        self.error: str | None = None
        self.version_info: dict | None = None

        # Per-file config-load errors; combined into `load_error` for
        # display.  Each method clears its own slot on a successful
        # reload so fixing one file doesn't mask a problem with another.
        self._base_load_error: str | None = None
        self._updatable_load_error: str | None = None

        # Change queue (drained by the node's sync worker)
        self._queue: deque = deque()

    @property
    def key(self) -> str:
        return f"{self.group}/{self.name}"

    @property
    def version(self) -> str | None:
        """kotekan's version string, from the last ``/version`` probe."""
        return (self.version_info or {}).get("kotekan_version")

    @property
    def config_problem(self) -> str | None:
        """Why there is no desired config to push, or None when there is."""
        if self.desired_config is not None:
            return None
        return self.load_error or f"No config file ({self.config_filename})"

    @property
    def updatable_path(self) -> Path | None:
        """This node's updatable store on disk, or None without a configs dir."""
        if self.configs_dir is None:
            return None
        return self.configs_dir / UPDATABLE_DIR / self.group / f"{self.name}.json"

    @property
    def last_seen_ago(self) -> str | None:
        """Human-readable time since last seen."""
        if self.last_seen is None:
            return None
        delta = time.time() - self.last_seen
        if delta < 60:
            return f"{int(delta)}s ago"
        if delta < 3600:
            return f"{int(delta / 60)}m ago"
        return f"{int(delta / 3600)}h ago"

    def __repr__(self) -> str:
        return f"Node({self.key}, {self.host}:{self.port}, {self.status.value})"

    # --- Change queue ---

    def queue_put(self, item):
        """Append a ChangeItem to this node's queue."""
        self._queue.append(item)

    def queue_pop(self):
        """Pop the next ChangeItem, or None if empty."""
        try:
            return self._queue.popleft()
        except IndexError:
            return None

    @property
    def queue_empty(self) -> bool:
        return len(self._queue) == 0

    @property
    def queue_depth(self) -> int:
        return len(self._queue)

    # --- Config state ---

    @property
    def config_filename(self) -> str:
        """Relative path of this node's base config file."""
        if self._config_file is not None:
            return self._config_file
        return f"{self.group}/{self.name}{self._file_suffix}"

    @property
    def explicit_config(self) -> str | None:
        """nodes.yaml's ``config:`` for this node, or None for the legacy
        per-node file."""
        return self._config_file

    @property
    def config_abspath(self) -> Path | None:
        """Absolute path of the base config file; None without a configs
        directory or with a ``config:`` value that failed validation."""
        if self.configs_dir is None or self._config_error:
            return None
        return self.configs_dir / self.config_filename

    def _search_dirs(self) -> list[Path]:
        """Where this node's includes resolve: the config file's own
        directory, then the configs root (kotekan's loader uses the
        first; the second lets a file name another directory)."""
        if self.configs_dir is None or self._config_error:
            return []
        own = self.configs_dir / Path(self.config_filename).parent
        return [own, self.configs_dir] if own != self.configs_dir else [own]

    def _make_env(self, overlay: dict[Path, str] | None = None
                  ) -> jinja2.Environment:
        """The Jinja2 environment this node renders with.

        Autoescape stays off and undefined variables render empty, as
        the bare ``jinja2.Template`` this replaced did (and as kotekan's
        loader does for ``.j2`` names).  With *overlay*, unsaved text
        for those absolute paths is served ahead of the files on disk.
        """
        dirs = self._search_dirs()
        loader = None
        if dirs:
            loader = jinja2.FileSystemLoader([str(d) for d in dirs])
            if overlay:
                loader = jinja2.ChoiceLoader(
                    [_OverlayLoader(dirs, overlay), loader])
        return jinja2.Environment(loader=loader, autoescape=False)

    def referenced_files(self, base_content: str) -> set[Path]:
        """Every file *base_content* includes, directly or through
        another include, as absolute paths: the files whose change must
        re-render this node.

        Static analysis of the include names (``jinja2.meta``), so a
        computed name is not followed.  A name that resolves to no file
        is recorded where the loader would look first, so creating it
        later triggers the reload that makes the config load.
        """
        env = self._env
        found: set[Path] = set()
        dirs = self._search_dirs()
        if env.loader is None or not dirs:
            return found
        pending = [base_content]
        seen: set[str] = set()
        while pending:
            try:
                ast = env.parse(pending.pop())
            except jinja2.TemplateSyntaxError:
                continue
            for name in _jinja_meta.find_referenced_templates(ast):
                if name is None or name in seen:
                    continue
                seen.add(name)
                try:
                    source, filename, _ = env.loader.get_source(env, name)
                except jinja2.TemplateNotFound:
                    try:
                        pieces = jinja2.loaders.split_template_path(name)
                    except jinja2.TemplateNotFound:
                        continue
                    found.add(dirs[0].joinpath(*pieces))
                    continue
                found.add(Path(filename))
                pending.append(source)
        return found

    @property
    def desired_config(self) -> dict | None:
        """Rendered config with updatable overrides applied.

        Computed from ``rendered_config`` and ``updatable_config`` on
        every access — no separate cache.  Returns a fresh deep copy
        safe to mutate, or None if no base config exists.
        """
        if self.rendered_config is None:
            return None
        desired = copy.deepcopy(self.rendered_config)
        if self.updatable_config:
            blocks = find_updatable_blocks(desired)
            for endpoint, values in self.updatable_config.items():
                if endpoint in blocks:
                    target = desired
                    for part in endpoint.split("/"):
                        target = target[part]
                    target.update(values)
        return desired

    @property
    def load_error(self) -> str | None:
        """Combined message for any config-load errors on this node."""
        parts = [e for e in (self._base_load_error,
                             self._updatable_load_error) if e]
        return "; ".join(parts) if parts else None

    def load_config(self):
        """Load (or reload) the base config from disk and render it.

        Errors reading or rendering the file are logged (with the file
        path) and recorded as ``load_error``; ``rendered_config`` is
        left as ``None`` so the sync loop can surface the issue on the
        dashboard rather than crashing service startup.
        """
        self._base_load_error = None
        self.dependencies = set()
        if self.configs_dir is None:
            return
        if self._config_error:
            self.base_content = None
            self.rendered_config = None
            self._base_load_error = self._config_error
            return
        if self._config_file is not None:
            candidates = [self.configs_dir / self._config_file]
        else:
            candidates = [self.configs_dir / self.group / f"{self.name}{s}"
                          for s in CONFIG_SUFFIXES]
        for path in candidates:
            if path.exists():
                if self._config_file is None:
                    self._file_suffix = path.suffix
                try:
                    self.base_content = path.read_text()
                    self.dependencies = self.referenced_files(self.base_content)
                    self.rendered_config = self.render(self.base_content)
                except Exception as e:
                    logger.error(
                        f"Failed to load base config for {self.key} "
                        f"from {path}: {e}"
                    )
                    self.base_content = None
                    self.rendered_config = None
                    self._base_load_error = (
                        f"Bad base config ({path.name}): {e}"
                    )
                return
        self.base_content = None
        self.rendered_config = None

    def load_updatable(self):
        """Load updatable overrides from the JSON store on disk.

        A corrupt file is logged (with the path) and skipped — the
        node falls back to no updatable overrides so the rest of the
        service can keep running.
        """
        self._updatable_load_error = None
        path = self.updatable_path
        if path is None or not path.exists():
            self.updatable_config = None
            return
        try:
            with open(path) as f:
                self.updatable_config = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.error(
                f"Failed to load updatable config for {self.key} "
                f"from {path}: {e}"
            )
            self.updatable_config = None
            self._updatable_load_error = (
                f"Bad updatable JSON ({path.name}): {e}"
            )

    def save_base(self, base_content: str):
        """Validate, write base config to disk, and update caches.

        A successful save also clears any previous base-config load
        error — the file on disk is now valid by construction.
        """
        rendered = self.render(base_content)
        path = self.config_abspath
        if path is None:
            raise ValueError(self._config_error or "node has no configs directory")
        write_text_atomic(path, base_content)
        self.base_content = base_content
        self.dependencies = self.referenced_files(base_content)
        self.rendered_config = rendered
        self._base_load_error = None

    def save_updatable(self, endpoint: str, values: dict):
        """Save updatable values for one endpoint to memory and disk.

        Writes the merged store as well-formed JSON, replacing whatever
        was there.  If the existing file was unreadable, those bytes
        are overwritten — any endpoints we couldn't parse are not
        recoverable afterwards.  This is intentional: the web UI shows
        the load error to the operator on the edit page, so submitting
        through it is a deliberate overwrite.  We log a WARNING with
        the path on this branch so the journal records what was lost.
        """
        path = self.updatable_path
        if self._updatable_load_error and path is not None:
            logger.warning(
                f"Overwriting previously-unreadable updatable file {path} "
                f"on save for {self.key}: prior contents are not recoverable"
            )
        if self.updatable_config is None:
            self.updatable_config = {}
        self.updatable_config[endpoint] = values
        if path is not None:
            # torn, this file would be a load error and stop all pushes
            write_json_atomic(path, self.updatable_config)
        self._updatable_load_error = None

    def render(self, base_content: str, *,
               overlay: dict[Path, str] | None = None) -> dict:
        """Render base config text through Jinja2 and parse as YAML.

        Also serves as validation — raises on invalid content.  Includes
        resolve from disk, except that *overlay* (absolute path -> text)
        stands in for files about to be written, so an edit to a shared
        include can be checked through every node that uses it first.
        """
        env = self._make_env(overlay) if overlay else self._env
        rendered = env.from_string(base_content).render(self.template_vars)
        config = _yaml_load(rendered)
        if not isinstance(config, dict):
            raise ValueError("Config must render to a YAML mapping")
        return config

    # --- Kotekan REST API ---

    def _request(
        self, method: str, path: str, accept_statuses: tuple[int, ...] = (),
        retries: int = 0, **kwargs,
    ) -> requests.Response | None:
        """One kotekan REST call.  Returns None on any transport or HTTP
        error, except that statuses in ``accept_statuses`` are returned to
        the caller (for endpoints where an error status is a meaningful
        reply, e.g. the frame peek's 402 "no full frame").

        ``retries`` extra attempts follow a failure.  The status probe and
        the buffer reads use one: a single dropped request would otherwise
        read as an outage for a whole poll interval (the same rule as the
        service monitors).  A genuinely down node fails every attempt, and
        connection-refused is instant, so a retry only costs time in the
        blackhole case.
        """
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            return fetch(method, url, timeout=self.timeout, retries=retries,
                         retry_delay=0, accept_statuses=accept_statuses, **kwargs)
        except requests.HTTPError as e:
            logger.warning(f"HTTP error from {url}: {e}")
        except OSError as e:
            # Every transport failure, the rarer ones included, degrades
            # like a down node instead of bubbling a 500 out of a route.
            logger.debug(f"{url}: {type(e).__name__}: {e}")
        return None

    @staticmethod
    def _json_dict(resp) -> dict | None:
        """A reply's JSON body when it is a mapping, else None."""
        try:
            data = resp.json()
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    def get_status(self) -> NodeStatus:
        """Probe kotekan: returns DOWN, IDLE, STARTED, or UNKNOWN."""
        resp = self._request("GET", "/status", retries=1)
        if resp is None:
            return NodeStatus.DOWN
        try:
            data = resp.json()
            return NodeStatus.STARTED if data.get("running", False) else NodeStatus.IDLE
        except Exception:
            return NodeStatus.UNKNOWN

    def get_config(self) -> dict | None:
        """Get the live config from kotekan.  Returns None if unreachable."""
        resp = self._request("GET", "/config")
        data = self._json_dict(resp) if resp is not None else None
        if resp is not None and data is None:
            logger.warning(f"Failed to parse config JSON from {self._base_url}")
        return data

    def push_updatable(self, path: str, values: dict) -> bool:
        """Push values to an updatable config endpoint on kotekan.

        A no-op (returns ``False``) when the node is in maintenance mode.
        """
        if self.maintenance:
            logger.info(
                f"Maintenance: skipping push_updatable to {self.key}{path}"
            )
            return False
        return self._request("POST", path, json=values) is not None

    def start(self, desired_config: dict, *,
              override_maintenance: bool = False) -> bool:
        """Start kotekan with the desired config via POST /start.

        A no-op (returns ``False``) when the node is in maintenance mode,
        unless *override_maintenance* is set.  The override is for an
        operator's explicit one-off start (``web._run_oneshot``), never
        for the sync loop — maintenance constrains choco's automation,
        not the operator acting through it.
        """
        if self.maintenance and not override_maintenance:
            logger.info(f"Maintenance: skipping /start of {self.key}")
            return False
        return self._request("POST", "/start", json=desired_config) is not None

    def kill(self) -> bool:
        """Kill the kotekan process. The daemon restarts it into an idle state.

        This is the reliable way to stop a running config — the ``/stop``
        endpoint is unreliable, so we always use ``/kill`` instead.

        A no-op (returns ``False``) when the node is in maintenance mode.
        """
        if self.maintenance:
            logger.info(f"Maintenance: skipping /kill of {self.key}")
            return False
        return self._request("GET", "/kill") is not None

    def get_version_info(self) -> dict | None:
        """Get the full kotekan version info dict.

        Returns the parsed JSON from ``GET /version``: ``kotekan_version``,
        ``branch``, ``git_commit_hash``, ``cmake_build_settings`` (dict),
        ``available_stages`` (list). Older kotekan builds may only return
        a subset of these fields.
        """
        resp = self._request("GET", "/version")
        return self._json_dict(resp) if resp is not None else None

    def get_pipeline_dot(self) -> str | None:
        """Get the pipeline graph as graphviz dot text.

        The labels carry live fullness, measured rates, per-stage CPU and
        array layouts, so a re-fetch is a fresh snapshot, not a static
        picture.  Returns None if the node is unreachable.

        ``urls=0`` drops the ``/buffer_frame?name=…`` link kotekan puts on
        every frame buffer.  Those paths are relative to the *node*, so they
        resolve against choco and 404; graphviz renders them as an ``<a>``
        wrapping the node's shape, which would fight the inline view's own
        click-to-plot handler.  Older kotekan ignores the argument.

        The reply is decoded as UTF-8 whatever the node says: layout lines
        hold ``×`` and ``·``, and kotekan builds before the charset fix
        label the body ``text/vnd.graphviz`` with no charset — which HTTP
        defines as ISO-8859-1, and ``requests`` believes it.
        """
        resp = self._request("GET", "/pipeline_dot", params={"urls": 0})
        if resp is None:
            return None
        resp.encoding = "utf-8"
        return resp.text

    def get_buffers(self) -> dict | None:
        """Get kotekan's buffer table (``GET /buffers``).

        One entry per buffer.  Frame buffers carry ``num_full_frame``,
        ``frames``, ``frame_size``, ``last_frame_arrival_time`` and (on
        new enough kotekan) ``peek_hold``; ring buffers only the shared
        producer/consumer bookkeeping.  Returns ``{}`` when kotekan
        answers but has no buffer table (an idle kotekan registers
        ``/buffers`` only once a pipeline is running — the process
        being up is not the same as buffers existing); None if the
        node is unreachable or the reply is malformed.
        """
        resp = self._request("GET", "/buffers", accept_statuses=(404,),
                             retries=1)
        if resp is None:
            return None
        if resp.status_code == 404:
            return {}
        return self._json_dict(resp)

    def get_buffer_frame(self, name: str, length: int | None = None) -> dict | None:
        """Peek the newest full frame of a buffer (``GET /buffer_frame?name=``).

        ``length`` bounds the data bytes copied out of the frame
        (``0`` = metadata and frame descriptor only); None copies the
        whole frame.  Returns the parsed JSON reply; ``{"error": ...}``
        when kotekan has no full frame to serve (HTTP 402 — expected on
        fast-draining buffers without ``peek_hold``) or when kotekan
        doesn't know the buffer (HTTP 404 — idle kotekan with no
        pipeline running, a stale buffer name, or a kotekan from before
        the per-buffer ``/buffer/<name>/frame`` endpoints were folded
        into ``/buffer_frame``, the only form spoken here; without
        this, a 404 would masquerade as "unreachable"); or when kotekan
        itself fails to serialise the frame (HTTP 500 — seen on
        dpdk-produced buffers whose metadata object is attached but
        never populated, so ``chordMetadata::to_json`` reads
        uninitialised dims: a reply about *that* frame, not an outage);
        or None if the node is unreachable or the reply is malformed.
        """
        params: dict = {"name": name}
        if length is not None:
            params["len"] = length
        accept = (402, 404, 500)
        resp = self._request("GET", "/buffer_frame", accept_statuses=accept,
                             retries=1, params=params)
        if resp is None:
            return None
        if resp.status_code == 402:
            return {"error": "no full frame currently in buffer"}
        if resp.status_code == 404:
            return {"error": f"kotekan has no buffer named '{name}' "
                             "(pipeline not running, stale buffer name, or a "
                             "kotekan predating the /buffer_frame endpoint)"}
        if resp.status_code == 500:
            return {"error": f"kotekan could not serialise a frame of '{name}' "
                             "(internal error — often uninitialised frame metadata)"}
        return self._json_dict(resp)


class Registry:
    """Node registry: loads node definitions from nodes.yaml and provides lookup.

    Each :class:`Node` owns its own config state (base config file,
    rendered config, updatable overrides).  The registry creates them
    and loads shared Jinja2 template variables from ``vars.yaml``.
    """

    def __init__(self, configs_dir: Path, kotekan_timeout: int = 10):
        self.configs_dir = Path(configs_dir)
        self.kotekan_timeout = kotekan_timeout
        self.nodes: dict[str, Node] = {}
        self.reload()

    def load_vars(self) -> dict:
        vars_file = self.configs_dir / "vars.yaml"
        if not vars_file.exists():
            return {}
        try:
            with open(vars_file) as f:
                return _yaml_load(f) or {}
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"Failed to load {vars_file}: {e}; using empty vars")
            return {}

    def reload(self):
        """Rebuild ``self.nodes`` from ``nodes.yaml`` on disk.

        Clears and repopulates the registry; all existing :class:`Node`
        objects are discarded along with any pending queue items or
        runtime state.  Callers that need to synchronise with the node
        workers should hold the orchestrator's submit lock around this
        call.

        If ``nodes.yaml`` is missing or unparseable the registry is left
        empty and the error is logged — the service comes up so it can
        be reconfigured via the UI rather than crash-looping.
        """
        nodes_file = self.configs_dir / "nodes.yaml"
        if not nodes_file.exists():
            logger.warning(f"No nodes.yaml found at {nodes_file}")
            self.nodes.clear()
            return

        try:
            with open(nodes_file) as f:
                data = _yaml_load(f) or {}
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"Failed to parse {nodes_file}: {e}; registry empty")
            self.nodes.clear()
            return

        groups = data.get("groups") if isinstance(data, dict) else data
        if not isinstance(groups or {}, dict):
            logger.error(f"{nodes_file}: 'groups' is not a mapping; registry empty")
            self.nodes.clear()
            return
        template_vars = self.load_vars()

        # A hand edit can leave any level the wrong shape; such an entry
        # is skipped and logged, never allowed to abort the rebuild (the
        # workers are already stopped when this runs).  A bare ``node:``
        # is a node with every default.
        self.nodes.clear()
        for group_name, members in (groups or {}).items():
            if not isinstance(members or {}, dict):
                logger.error(f"{nodes_file}: group {group_name!r} is not a mapping; skipped")
                continue
            for node_name, node_info in (members or {}).items():
                key = f"{group_name}/{node_name}"
                node_info = {} if node_info is None else node_info
                if not isinstance(node_info, dict):
                    logger.error(f"{nodes_file}: node {key!r} is not a mapping; skipped")
                    continue
                host = node_info.get("host", node_name)
                port = node_info.get("port", DEFAULT_PORT)
                started = node_info.get("started", False)
                config = node_info.get("config")
                node = Node(
                    node_name, group_name, host, port,
                    timeout=self.kotekan_timeout,
                    started=started,
                    # Always start in maintenance mode at the registry
                    # level — choco should observe before pushing.
                    maintenance=True,
                    configs_dir=self.configs_dir,
                    template_vars=template_vars,
                    config=None if config is None else str(config),
                )
                node.load_config()
                node.load_updatable()
                self.nodes[key] = node

        logger.info(f"Loaded {len(self.nodes)} nodes")

    def save_nodes_yaml(self, data: dict):
        """Write *data* to ``nodes.yaml`` atomically (temp file + rename)."""
        write_text_atomic(self.configs_dir / "nodes.yaml",
                          yaml.safe_dump(data, default_flow_style=False,
                                         sort_keys=False))

    def get_node(self, key: str) -> Node | None:
        return self.nodes.get(key)

    def config_files(self) -> list[str]:
        """The config library: every config file under the configs
        directory, as relative paths (see ``list_config_files``)."""
        return list_config_files(self.configs_dir)

    def users_of(self, rel: str) -> tuple[list[Node], list[Node]]:
        """``(direct, includers)`` for config file *rel*: the nodes whose
        base config it is, and the nodes whose base config includes it
        (directly or through another include).  Registry order."""
        path = self.configs_dir / rel
        direct = [n for n in self.nodes.values() if n.config_abspath == path]
        includers = [n for n in self.nodes.values()
                     if path in n.dependencies and n.config_abspath != path]
        return direct, includers

    def affected_by(self, rel: str) -> list[Node]:
        """The nodes a change to library file *rel* re-renders: every node
        for vars.yaml (the shared template context), else the file's
        direct users and includers (:meth:`users_of`)."""
        if rel == "vars.yaml":
            return list(self.nodes.values())
        direct, includers = self.users_of(rel)
        return direct + includers

    def in_group(self, group: str) -> list[Node]:
        """The nodes of *group* in registry order; empty for an unknown group."""
        return [n for n in self.nodes.values() if n.group == group]


def validate_nodes_payload(payload, configs_dir: Path) -> dict:
    """The nodes.yaml data a registry edit asks for, checked.

    *payload* is the editor's JSON: ``{"groups": {name: [{"name", "host",
    "port"?, "config"?}, ...]}}``.  Returns the mapping to write
    (``{"groups": {group: {node: {"host", "port", "config"?}}}}``) or
    raises ``ValueError`` naming the first problem.  A node's optional
    ``config`` is validated as a path (so a bad value is refused here,
    not joined to anything later); its existence is the node's own "No
    config file" error if it is missing.
    """
    groups_in = payload.get("groups") if isinstance(payload, dict) else None
    if not isinstance(groups_in, dict):
        raise ValueError("'groups' must be an object")
    new_data: dict = {"groups": {}}
    seen_keys: set[str] = set()
    for group_name, items in groups_in.items():
        if not isinstance(group_name, str):
            raise ValueError("Group name must be a string")
        group_name = group_name.strip()
        if not group_name or "/" in group_name or group_name.startswith("."):
            raise ValueError(f"Invalid group name {group_name!r}")
        if not isinstance(items, list):
            raise ValueError(f"Group {group_name!r} must be a list")
        if group_name in new_data["groups"]:
            raise ValueError(f"Duplicate group {group_name!r}")
        members: dict = {}
        for item in items:
            if not isinstance(item, dict):
                raise ValueError(f"Entry in {group_name!r} must be an object")
            name = str(item.get("name", "")).strip()
            host = str(item.get("host", "")).strip()
            if not name or "/" in name or name.startswith("."):
                raise ValueError(f"Invalid node name {name!r} in {group_name!r}")
            if not host:
                raise ValueError(f"Node {group_name}/{name} missing host")
            try:
                port = int(item.get("port", DEFAULT_PORT))
            except (TypeError, ValueError):
                raise ValueError(f"Node {group_name}/{name} invalid port") from None
            key = f"{group_name}/{name}"
            if key in seen_keys:
                raise ValueError(f"Duplicate node {key!r}")
            seen_keys.add(key)
            members[name] = {"host": host, "port": port}
            config = str(item.get("config") or "").strip()
            if config:
                try:
                    resolve_config_path(configs_dir, config)
                except ValueError as e:
                    raise ValueError(f"Node {key}: {e}") from None
                members[name]["config"] = config
        new_data["groups"][group_name] = members
    return new_data
