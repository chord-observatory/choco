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
from .sync import SyncLoop

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
    registry = Registry(configs_dir)
    sync_loop = SyncLoop(registry, socketio=socketio)

    # Store on app for access in routes
    app.config["registry"] = registry
    app.config["sync_loop"] = sync_loop
    # Initialize authentication
    init_auth(app, config)

    # Register routes
    from .web import bp
    app.register_blueprint(bp)

    # Initialize SocketIO
    socketio.init_app(app)

    # Start background sync loop immediately (not deferred to first request)
    socketio.start_background_task(sync_loop.run)

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


def main():
    """Entry point for the choco command."""
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    config = load_config(config_path)

    logging.basicConfig(
        level=config["server"]["log_level"],
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

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

    logger.info(f"Starting choco on {host}:{port} ({'https' if ssl_context else 'http'})")
    socketio.run(app, host=host, port=port, debug=False, ssl_context=ssl_context)


if __name__ == "__main__":
    main()
