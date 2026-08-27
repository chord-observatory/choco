"""Node registry and runtime state tracking."""

import copy
import json
import logging
import time
from collections import deque
from enum import Enum
from pathlib import Path

import jinja2
import requests
import yaml

logger = logging.getLogger(__name__)

# Config file extensions (order matters: later wins if both exist for same key)
_CONFIG_SUFFIXES = (".yaml", ".yml", ".j2")

_UPDATABLE_MARKER = "kotekan_update_endpoint"


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
    STARTED = "started" # Running with correct config
    SYNCING = "syncing" # Push in progress (kill -> wait -> start with new config)


class Node:
    """A kotekan instance on the cluster.

    Each node owns its identity (name, group, host, port), its config
    state (base config file on disk, rendered config, updatable overrides),
    a FIFO change queue (used by the sync worker pool), and an HTTP
    client for the kotekan REST API.

    Config lifecycle:
        - **base_content** — the on-disk file text (YAML or Jinja2)
        - **rendered_config** — base rendered through Jinja2 and parsed
        - **updatable_config** — runtime-mutable overrides stored in JSON
        - **desired_config** — rendered + updatable merged; what gets pushed

    REST methods return ``None`` / ``False`` on connection failure rather
    than raising, so callers can treat unreachable nodes as a normal state.

    The *configs_dir* and *template_vars* parameters are optional so that
    the REST client can be used standalone in tests without a config
    directory.
    """

    def __init__(self, name: str, group: str, host: str,
                 port: int = 12048, timeout: int = 10, *,
                 started: bool = False,
                 maintenance: bool = False,
                 configs_dir: Path | None = None,
                 template_vars: dict | None = None):
        # Identity
        self.name = name
        self.group = group
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started = started
        # Maintenance mode: when True, push_updatable() and start() are
        # no-ops.  Ephemeral, never persisted to nodes.yaml.  The
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

        # Runtime state (ephemeral, rebuilt from polling)
        self.status: NodeStatus = NodeStatus.UNKNOWN
        self.last_seen: float | None = None
        self.error: str | None = None
        self.version: str | None = None
        self.version_info: dict | None = None

        # Per-file config-load errors; combined into `load_error` for
        # display.  Each method clears its own slot on a successful
        # reload so fixing one file doesn't mask a problem with another.
        self._base_load_error: str | None = None
        self._updatable_load_error: str | None = None

        # Change queue (used by the sync worker pool)
        self._queue: deque = deque()
        self._queue_lock: object | None = None  # set by Orchestrator (gevent semaphore)

    @property
    def key(self) -> str:
        return f"{self.group}/{self.name}"

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

    def queue_try_lock(self) -> bool:
        """Try to acquire exclusive access to this node's queue."""
        if self._queue_lock is None:
            return False
        return self._queue_lock.acquire(blocking=False)

    def queue_unlock(self):
        """Release exclusive access to this node's queue."""
        if self._queue_lock is not None:
            self._queue_lock.release()

    @property
    def queue_empty(self) -> bool:
        return len(self._queue) == 0

    # --- Config state ---

    @property
    def config_filename(self) -> str:
        """Relative path of this node's base config file."""
        return f"{self.group}/{self.name}{self._file_suffix}"

    @property
    def desired_config(self) -> dict | None:
        """Rendered config with updatable overrides applied.

        Computed from the current ``_rendered_config`` and ``_updatable``
        on every access — no separate cache.  Returns a fresh deep copy
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
        if self.configs_dir is None:
            return
        for suffix in _CONFIG_SUFFIXES:
            path = self.configs_dir / self.group / f"{self.name}{suffix}"
            if path.exists():
                self._file_suffix = suffix
                try:
                    self.base_content = path.read_text()
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
        if self.configs_dir is None:
            self.updatable_config = None
            return
        path = self.configs_dir / ".updatable" / self.group / f"{self.name}.json"
        if not path.exists():
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
        path = self.configs_dir / self.group / f"{self.name}{self._file_suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(base_content)
        self.base_content = base_content
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
        path = (self.configs_dir / ".updatable" / self.group
                / f"{self.name}.json") if self.configs_dir else None
        if self._updatable_load_error and path is not None:
            logger.warning(
                f"Overwriting previously-unreadable updatable file {path} "
                f"on save for {self.key}: prior contents are not recoverable"
            )
        if self.updatable_config is None:
            self.updatable_config = {}
        self.updatable_config[endpoint] = values
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(self.updatable_config, f, indent=2)
        self._updatable_load_error = None

    def render(self, base_content: str) -> dict:
        """Render base config text through Jinja2 and parse as YAML.

        Also serves as validation — raises on invalid content.
        """
        rendered = jinja2.Template(base_content).render(self.template_vars)
        config = yaml.safe_load(rendered)
        if not isinstance(config, dict):
            raise ValueError("Config must render to a YAML mapping")
        return config

    # --- Kotekan REST API ---

    def _request(
        self, method: str, path: str, accept_statuses: tuple[int, ...] = (), **kwargs
    ) -> requests.Response | None:
        """One kotekan REST call.  Returns None on any transport or HTTP
        error, except that statuses in ``accept_statuses`` are returned to
        the caller (for endpoints where an error status is a meaningful
        reply, e.g. the frame peek's 402 "no full frame")."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            resp = requests.request(method, url, timeout=self.timeout, **kwargs)
            if resp.status_code in accept_statuses:
                return resp
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, ConnectionError):
            logger.debug(f"Connection failed: {url}")
        except requests.Timeout:
            logger.debug(f"Timeout: {url}")
        except requests.HTTPError as e:
            logger.warning(f"HTTP error from {url}: {e}")
        except requests.RequestException as e:
            # Catch-all for the rarer transport failures (chunked-encoding
            # errors, protocol errors on a mid-body disconnect, ...) so
            # they degrade like any other failed request instead of
            # bubbling a 500 out of whatever route made the call.
            logger.warning(f"Request failed: {url}: {e}")
        return None

    def get_status(self) -> NodeStatus:
        """Probe kotekan: returns DOWN, IDLE, STARTED, or UNKNOWN."""
        resp = self._request("GET", "/status")
        if resp is None:
            # One quick retry before declaring DOWN: a single dropped
            # request otherwise flips the node red for a whole poll
            # cycle (and skips its sync tick).  A genuinely down node
            # fails both attempts; connection-refused is instant, so
            # the retry only costs time in the blackhole case.
            resp = self._request("GET", "/status")
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
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
            logger.warning(f"Failed to parse config JSON from {self._base_url}")
            return None

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

    def start(self, desired_config: dict) -> bool:
        """Start kotekan with the desired config via POST /start.

        A no-op (returns ``False``) when the node is in maintenance mode.
        """
        if self.maintenance:
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

    def get_version(self) -> str | None:
        """Get the kotekan version string."""
        info = self.get_version_info()
        return info.get("kotekan_version") if info else None

    def get_version_info(self) -> dict | None:
        """Get the full kotekan version info dict.

        Returns the parsed JSON from ``GET /version``: ``kotekan_version``,
        ``branch``, ``git_commit_hash``, ``cmake_build_settings`` (dict),
        ``available_stages`` (list). Older kotekan builds may only return
        a subset of these fields.
        """
        resp = self._request("GET", "/version")
        if resp is None:
            return None
        try:
            data = resp.json()
        except Exception:
            return None
        return data if isinstance(data, dict) else None

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
        resp = self._request("GET", "/buffers", accept_statuses=(404,))
        if resp is None:
            # One quick retry — the service-monitor rule: a single
            # dropped request shouldn't read as an outage for a whole
            # poll interval.
            resp = self._request("GET", "/buffers", accept_statuses=(404,))
        if resp is None:
            return None
        if resp.status_code == 404:
            return {}
        try:
            data = resp.json()
        except Exception:
            return None
        return data if isinstance(data, dict) else None

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
        resp = self._request(
            "GET", "/buffer_frame", accept_statuses=accept, params=params
        )
        if resp is None:
            # One quick retry, same rule as get_buffers.
            resp = self._request(
                "GET", "/buffer_frame", accept_statuses=accept, params=params
            )
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
        try:
            data = resp.json()
        except Exception:
            return None
        return data if isinstance(data, dict) else None


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

    def _load_vars(self) -> dict:
        vars_file = self.configs_dir / "vars.yaml"
        if not vars_file.exists():
            return {}
        try:
            with open(vars_file) as f:
                return yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"Failed to load {vars_file}: {e}; using empty vars")
            return {}

    def reload(self):
        """Rebuild ``self.nodes`` from ``nodes.yaml`` on disk.

        Clears and repopulates the registry; all existing :class:`Node`
        objects are discarded along with any pending queue items or
        runtime state.  Callers that need to synchronise with the sync
        worker pool should hold the orchestrator's submit lock around
        this call.

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
                data = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"Failed to parse {nodes_file}: {e}; registry empty")
            self.nodes.clear()
            return

        template_vars = self._load_vars()

        self.nodes.clear()
        for group_name, members in (data.get("groups") or {}).items():
            for node_name, node_info in (members or {}).items():
                key = f"{group_name}/{node_name}"
                host = node_info.get("host", node_name)
                port = node_info.get("port", 12048)
                started = node_info.get("started", False)
                node = Node(
                    node_name, group_name, host, port,
                    timeout=self.kotekan_timeout,
                    started=started,
                    # Always start in maintenance mode at the registry
                    # level — choco should observe before pushing.
                    maintenance=True,
                    configs_dir=self.configs_dir,
                    template_vars=template_vars,
                )
                node.load_config()
                node.load_updatable()
                self.nodes[key] = node

        logger.info(f"Loaded {len(self.nodes)} nodes")

    def save_nodes_yaml(self, data: dict):
        """Write *data* to ``nodes.yaml`` atomically (temp file + rename)."""
        nodes_file = self.configs_dir / "nodes.yaml"
        nodes_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = nodes_file.with_name(nodes_file.name + ".tmp")
        with open(tmp, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
        tmp.replace(nodes_file)

    def get_node(self, key: str) -> Node | None:
        return self.nodes.get(key)
