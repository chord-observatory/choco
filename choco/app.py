"""choco Flask application."""

from gevent import monkey
monkey.patch_all()

import logging
import ssl
import stat
import sys
from pathlib import Path

import yaml
from flask import Flask
from flask_socketio import SocketIO

from .auth import init_auth
from .state import Registry
from .sync import Orchestrator

logger = logging.getLogger(__name__)

socketio = SocketIO()

_DEFAULT_CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 5000,
        "secret_key": "dev-key-change-me",
        "log_level": "INFO",
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
    config["configs_dir"] = raw.get("configs_dir", "configs")
    config["kotekan"] = {**_DEFAULT_CONFIG["kotekan"], **(raw.get("kotekan") or {})}
    config["sync"] = {**_DEFAULT_CONFIG["sync"], **(raw.get("sync") or {})}
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
        registry, socketio=socketio,
        poll_interval=int(sync_cfg["poll_interval"]),
        restart_timeout=int(sync_cfg["restart_timeout"]),
        num_workers=int(sync_cfg["num_workers"]),
    )

    # Store on app for access in routes
    app.config["registry"] = registry
    app.config["orchestrator"] = orchestrator
    # Initialize authentication
    init_auth(app, config)

    # Register routes
    from .web import bp
    app.register_blueprint(bp)

    # Initialize SocketIO
    socketio.init_app(app)

    # Start background sync loop immediately (not deferred to first request)
    socketio.start_background_task(orchestrator.run)

    return app


def _start_http_redirect(host: str, http_port: int, https_port: int):
    """Start a background HTTP server that redirects all requests to HTTPS."""
    from flask import Flask as _Flask, redirect, request
    import gevent
    from gevent.pywsgi import WSGIServer

    redirect_app = _Flask("choco-redirect")

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
    """Build an SSL context from config, auto-generating a self-signed cert if needed."""
    cert = server_config.get("ssl_cert")
    key = server_config.get("ssl_key")

    if cert and key:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        return ctx

    # Auto-generate a self-signed certificate, persisted to disk so it
    # survives restarts (avoids new browser cert warnings each time).
    import datetime
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    cert_path = Path(__file__).parent.parent / ".ssl" / "cert.pem"
    key_path = Path(__file__).parent.parent / ".ssl" / "key.pem"

    if cert_path.exists() and key_path.exists():
        logger.info(f"Using existing self-signed certificate from {cert_path.parent}")
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(cert_path), str(key_path))
        return ctx

    logger.info(f"Generating self-signed SSL certificate in {cert_path.parent}")
    cert_path.parent.mkdir(parents=True, exist_ok=True)

    key_obj = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "choco")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert_obj = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key_obj.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key_obj, hashes.SHA256())
    )

    key_path.write_bytes(
        key_obj.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    cert_path.write_bytes(cert_obj.public_bytes(serialization.Encoding.PEM))

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

    logging.getLogger("geventwebsocket.handler").addFilter(_PartialsDedup())

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
        import gevent
        hub = gevent.get_hub()
        hub.NOT_ERROR = hub.NOT_ERROR + (ssl.SSLError,)

    scheme = "https" if ssl_context else "http"
    import socket
    display_host = socket.getfqdn() if host in ("0.0.0.0", "::") else host
    logger.info(f"Listening on {host}:{port} — access at {scheme}://{display_host}")
    _sd_notify_ready()
    socketio.run(app, host=host, port=port, debug=False, ssl_context=ssl_context)


if __name__ == "__main__":
    main()
