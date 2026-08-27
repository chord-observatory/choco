"""choco Flask application."""

from gevent import monkey
monkey.patch_all()

import logging
import ssl
import stat
import sys
from pathlib import Path

import gevent
import yaml
from flask import Flask

from .auth import init_auth
from .datafiles import DataFileScan
from .waterfalls import WaterfallStore
from .pdbmap import DEFAULT_MAP_FILENAME, PdbMapFile
from .services import FpgaMonitor, GainArchive, PdbMonitor
from .state import Registry
from .sync import Orchestrator

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 5000,
        "secret_key": "dev-key-change-me",
        "log_level": "INFO",
        "ssl": True,
        "dev_auth": None,
    },
    "configs_dir": "configs",
    "kotekan": {
        "timeout": 10,
    },
    "sync": {
        "poll_interval": 5,
        "restart_timeout": 10,
        "num_workers": 4,
    },
    "fpga_master": {},
    "pdb": {},
    "eop": {},
    "bffs": {},
    "eigencal": {},
    "waterfall": {},
    "skymap": {},
    "vis_files": {},
    "ldap": {},
}


def load_config(path: str | Path) -> dict:
    """Load configuration from a YAML file, filling in defaults."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            f"Copy config.yaml.template to config.yaml and edit it."
        )
    # Warn if config is world-readable (contains secrets)
    mode = path.stat().st_mode
    if mode & stat.S_IROTH:
        logger.warning(
            f"Config file {path} is world-readable. It contains secrets. "
            f"Fix with: chmod 600 {path}"
        )

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    config = dict(_DEFAULT_CONFIG)
    config["server"] = {**_DEFAULT_CONFIG["server"], **(raw.get("server") or {})}
    config["server"]["port"] = int(config["server"]["port"])
    # Dev mode disables login *and* CSRF, so the only thing standing
    # between it and an unauthenticated cluster control plane is the
    # bind address.  Refuse to start rather than warn: a warning in a
    # scrollback is not a security boundary, and the failure this
    # prevents (a dev instance answering on 0.0.0.0) is silent.
    if config["server"].get("dev_auth"):
        host = str(config["server"].get("host") or "")
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise ValueError(
                f"server.dev_auth is set but server.host is {host!r}. "
                f"Dev mode has no authentication, so it may only bind "
                f"loopback (127.0.0.1). Reach it over an ssh tunnel."
            )
    config["configs_dir"] = raw.get("configs_dir", "configs")
    config["kotekan"] = {**_DEFAULT_CONFIG["kotekan"], **(raw.get("kotekan") or {})}
    config["sync"] = {**_DEFAULT_CONFIG["sync"], **(raw.get("sync") or {})}
    config["fpga_master"] = raw.get("fpga_master") or {}
    config["eop"] = raw.get("eop") or {}
    # Backwards-compat: fpga_master_host/port used to live under eop:.
    # If the new top-level block is missing them but the old keys are
    # present, fold them in and warn so the operator can migrate.
    eop_block = config["eop"]
    legacy_host = eop_block.get("fpga_master_host")
    legacy_port = eop_block.get("fpga_master_port")
    if legacy_host and not config["fpga_master"].get("host"):
        config["fpga_master"]["host"] = legacy_host
        logger.warning("Config: eop.fpga_master_host is deprecated; "
                       "move it to a top-level fpga_master.host block.")
    if legacy_port and not config["fpga_master"].get("port"):
        config["fpga_master"]["port"] = legacy_port
    # The power controller block was called psu: before it was renamed
    # PDB (power distribution boards) to disambiguate it from the
    # supplies themselves.  Keep reading the old key so a deployed
    # config.yaml keeps working, and say so once at startup.
    config["pdb"] = raw.get("pdb") or {}
    legacy_pdb = raw.get("psu") or {}
    if legacy_pdb and not config["pdb"]:
        config["pdb"] = legacy_pdb
        logger.warning("Config: the psu: block is deprecated; "
                       "rename it to pdb:.")
    config["bffs"] = raw.get("bffs") or {}
    config["eigencal"] = raw.get("eigencal") or {}
    config["waterfall"] = raw.get("waterfall") or {}
    config["skymap"] = raw.get("skymap") or {}
    config["vis_files"] = raw.get("vis_files") or {}
    config["ldap"] = raw.get("ldap") or {}
    return config


def create_app(
    configs_dir: str | Path | None = None,
    config: dict | None = None,
) -> Flask:
    """Create and configure the Flask application.

    Args:
        configs_dir: Override configs directory (convenience for tests).
        config: Full config dict (for tests). If not provided, not loaded here —
                main() handles loading from file for production.
    """
    app = Flask(__name__)

    if config is None:
        config = _DEFAULT_CONFIG

    app.config["SECRET_KEY"] = config["server"]["secret_key"]

    if configs_dir is None:
        configs_dir = config.get("configs_dir", "configs")
    configs_dir = Path(configs_dir).resolve()

    # Initialize registry and sync loop
    kotekan_timeout = int(config["kotekan"]["timeout"])
    registry = Registry(configs_dir, kotekan_timeout=kotekan_timeout)

    sync_cfg = config["sync"]
    orchestrator = Orchestrator(
        registry,
        poll_interval=int(sync_cfg["poll_interval"]),
        restart_timeout=int(sync_cfg["restart_timeout"]),
        num_workers=int(sync_cfg["num_workers"]),
    )

    # Hardware service monitors: separate concern from kotekan polling,
    # used by the service-status strip in the page header.  Instantiated
    # unconditionally so the UI is uniform; unconfigured ones don't poll.
    fpga_cfg = config.get("fpga_master") or {}
    fpga_monitor = FpgaMonitor(
        host=fpga_cfg.get("host") or None,
        port=fpga_cfg.get("port") or None,
        timeout=float(fpga_cfg.get("timeout") or 5.0),
    )
    # The digital-gain archive is fetched on demand (and cached), not
    # polled: it changes when someone recalibrates, and nothing in the
    # header depends on it.
    gain_archive = GainArchive(
        fpga_monitor.base_url,
        ttl_s=float(fpga_cfg.get("gain_ttl") or GainArchive.DEFAULT_TTL_S),
    )
    pdb_cfg = config.get("pdb") or {}
    pdb_monitor = PdbMonitor(
        host=pdb_cfg.get("host") or None,
        port=pdb_cfg.get("port") or None,
        timeout=float(pdb_cfg.get("timeout") or 5.0),
    )

    # Data-file roots for /files.  Scanned on demand (and cached), never
    # polled: nothing in the header depends on it, and the roots are NFS
    # mounts we would rather not touch on a timer.
    vis_cfg = config.get("vis_files") or {}
    datafile_scan = DataFileScan(
        vis_cfg.get("roots") or (),
        ttl_s=float(vis_cfg.get("ttl") or DataFileScan.DEFAULT_TTL_S),
    )

    # The master dish-input <-> power-channel table.  Lives beside
    # nodes.yaml by default, so it version-controls and deploys with the
    # rest of the desired state; re-read on mtime change, never at import.
    map_file = Path(pdb_cfg.get("map_file") or DEFAULT_MAP_FILENAME)
    pdb_map_path = (map_file if map_file.is_absolute()
                    else configs_dir / map_file)

    # Store on app for access in routes
    app.config["registry"] = registry
    app.config["orchestrator"] = orchestrator
    app.config["fpga_monitor"] = fpga_monitor
    app.config["fpga_cfg"] = fpga_cfg
    app.config["gain_archive"] = gain_archive
    app.config["pdb_monitor"] = pdb_monitor
    app.config["pdb_cfg"] = pdb_cfg
    app.config["pdb_map"] = PdbMapFile(pdb_map_path)
    app.config["eop_cfg"] = config.get("eop") or {}
    app.config["bffs_cfg"] = config.get("bffs") or {}
    app.config["eigencal_cfg"] = config.get("eigencal") or {}
    app.config["skymap_cfg"] = config.get("skymap") or {}
    waterfall_cfg = config.get("waterfall") or {}
    app.config["waterfall_cfg"] = waterfall_cfg
    # Read-only view of the tree jobs/waterfall writes.  No greenlet: the
    # images are read on demand from the /files page, not polled.
    app.config["waterfall_store"] = WaterfallStore(
        waterfall_cfg.get("images_dir"),
        ttl_s=float(waterfall_cfg.get("ttl") or WaterfallStore.DEFAULT_TTL_S),
    )
    app.config["datafile_scan"] = datafile_scan
    app.config["configs_dir"] = configs_dir
    # Initialize authentication
    init_auth(app, config)

    # Register routes
    from .web import bp
    app.register_blueprint(bp)

    # Start background sync loop immediately (not deferred to first request)
    gevent.spawn(orchestrator.run)
    if fpga_monitor.configured:
        gevent.spawn(fpga_monitor.run)
    if pdb_monitor.configured:
        gevent.spawn(pdb_monitor.run)
    if datafile_scan.configured:
        gevent.spawn(datafile_scan.run)

    return app


def _start_http_redirect(host: str, http_port: int, https_port: int):
    """Start a background HTTP server that redirects all requests to HTTPS."""
    from flask import redirect, request
    from gevent.pywsgi import WSGIServer

    redirect_app = Flask("choco-redirect")

    @redirect_app.route("/", defaults={"path": ""})
    @redirect_app.route("/<path:path>")
    def _redirect(path):
        url = request.url.replace("http://", "https://", 1)
        # Strip the internal HTTP port — the public HTTPS port (443) is the default
        url = url.replace(f":{http_port}", "", 1)
        return redirect(url, code=301)

    server = WSGIServer((host, http_port), redirect_app, log=None)
    gevent.spawn(server.serve_forever)
    logger.info(f"HTTP redirect: :{http_port} -> :{https_port}")


def _make_ssl_context(server_config: dict) -> ssl.SSLContext | None:
    """Build an SSL context from config, auto-generating a self-signed cert if needed.

    ``ssl: false`` opts out entirely (plain HTTP).  That is a dev-mode
    convenience — a loopback-bound instance reached through an ssh
    tunnel is already encrypted on the wire, and dropping TLS drops the
    cert warning and the self-signed cert's interaction with the
    browser's per-host (port-blind) cookie jar along with it.
    """
    if not server_config.get("ssl", True):
        logger.warning("server.ssl is false. Serving plain HTTP, no TLS")
        return None

    cert = server_config.get("ssl_cert")
    key = server_config.get("ssl_key")

    if cert and key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        return ctx

    # Auto-generate a self-signed certificate, persisted to disk so it
    # survives restarts (avoids new browser cert warnings each time).
    # Generated by the openssl CLI — present on any Linux host we deploy
    # to — rather than the `cryptography` package, whose entire (large,
    # Rust-built) dependency existed for these few lines.
    import subprocess

    cert_path = Path(__file__).parent.parent / ".ssl" / "cert.pem"
    key_path = Path(__file__).parent.parent / ".ssl" / "key.pem"

    if cert_path.exists() and key_path.exists():
        logger.info(f"Using existing self-signed certificate from {cert_path.parent}")
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(cert_path), str(key_path))
        return ctx

    logger.info(f"Generating self-signed SSL certificate in {cert_path.parent}")
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    # Pre-create the key file 0600 so it is never world-readable, even
    # briefly — openssl truncates it in place, preserving the mode.
    key_path.touch(mode=0o600)
    try:
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", str(key_path), "-out", str(cert_path),
             "-days", "365", "-subj", "/CN=choco"],
            check=True, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError(
            "openssl not found! Install it, or set server.ssl_cert / "
            "server.ssl_key to an existing certificate")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"openssl failed to generate a certificate: "
                           f"{e.stderr.strip()}")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert_path), str(key_path))
    return ctx


def _sd_notify_ready():
    """Send READY=1 to systemd if running under Type=notify. No-op otherwise."""
    import os
    import socket as _socket
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr[0] == "@":
        addr = "\0" + addr[1:]
    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
    try:
        sock.sendto(b"READY=1", addr)
    finally:
        sock.close()
    logger.info("Notified systemd: READY=1")


def main():
    """Entry point for the choco command."""
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)

    logging.basicConfig(
        level=config["server"]["log_level"],
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Deduplicate repeated /partials/ polling requests in access logs.
    # Logs the first request per path, then suppresses repeats for 60s.
    import re
    import time

    class _PartialsDedup(logging.Filter):
        _partials_re = re.compile(r'"GET /partials/(\S+)')
        _cooldown = 60

        def __init__(self):
            super().__init__()
            self._last_logged: dict[str, float] = {}

        def filter(self, record: logging.LogRecord) -> bool:
            m = self._partials_re.search(record.getMessage())
            if not m:
                return True
            path = m.group(1)
            now = time.monotonic()
            if now - self._last_logged.get(path, 0) >= self._cooldown:
                self._last_logged[path] = now
                return True
            return False

    access_logger = logging.getLogger("choco.access")
    access_logger.addFilter(_PartialsDedup())

    app = create_app(config=config)
    host = config["server"]["host"]
    port = config["server"]["port"]

    ssl_context = _make_ssl_context(config["server"])

    # Start HTTP->HTTPS redirect server if SSL is enabled
    if ssl_context:
        http_port = config["server"].get("http_redirect_port")
        if http_port:
            _start_http_redirect(host, int(http_port), port)

    # Suppress noisy SSL handshake tracebacks (e.g. clients rejecting self-signed certs)
    if ssl_context:
        hub = gevent.get_hub()
        hub.NOT_ERROR = hub.NOT_ERROR + (ssl.SSLError,)

    scheme = "https" if ssl_context else "http"
    import socket
    display_host = socket.getfqdn() if host in ("0.0.0.0", "::") else host
    logger.info(f"Listening on {host}:{port} - access at {scheme}://{display_host}")
    _sd_notify_ready()

    from gevent.pywsgi import WSGIServer, LoggingLogAdapter
    server_kwargs = {"log": LoggingLogAdapter(access_logger)}
    if ssl_context is not None:
        server_kwargs["ssl_context"] = ssl_context
    server = WSGIServer((host, port), app, **server_kwargs)
    server.serve_forever()


if __name__ == "__main__":
    main()
