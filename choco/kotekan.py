"""Kotekan REST API client."""

import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5  # seconds


@dataclass
class KotekanStatus:
    """Status of a kotekan instance."""

    reachable: bool
    running: bool | None = None

    @property
    def ok(self) -> bool:
        return self.reachable and self.running is True


class KotekanClient:
    """HTTP client for a single kotekan instance."""

    def __init__(self, host: str, port: int = 12048, timeout: float = DEFAULT_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._base_url = f"http://{host}:{port}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response | None:
        url = f"{self._base_url}/{path.lstrip('/')}"
        try:
            resp = requests.request(method, url, timeout=self.timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, ConnectionError):
            logger.debug(f"Connection failed: {url}")
        except requests.Timeout:
            logger.debug(f"Timeout: {url}")
        except requests.HTTPError as e:
            logger.warning(f"HTTP error from {url}: {e}")
        return None

    def get_status(self) -> KotekanStatus:
        """Check if kotekan is reachable and running."""
        resp = self._request("GET", "/status")
        if resp is None:
            return KotekanStatus(reachable=False)
        try:
            data = resp.json()
            return KotekanStatus(reachable=True, running=data.get("running", False))
        except Exception:
            return KotekanStatus(reachable=True, running=None)

    def get_config(self) -> dict | None:
        """Get the full current config. Returns None if unreachable."""
        resp = self._request("GET", "/config")
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
            logger.warning(f"Failed to parse config JSON from {self._base_url}")
            return None

    def update_config(self, path: str, values: dict) -> bool:
        """Push a config update to an updatable config block."""
        return self._request("POST", path, json=values) is not None

    def start(self, config: dict) -> bool:
        """Start kotekan with the given config via POST /start."""
        return self._request("POST", "/start", json=config) is not None

    def stop(self) -> bool:
        """Stop the running kotekan config."""
        return self._request("GET", "/stop") is not None

    def kill(self) -> bool:
        """Kill the kotekan process. The daemon will restart it."""
        return self._request("GET", "/kill") is not None

    def get_version(self) -> str | None:
        """Get the kotekan version string."""
        resp = self._request("GET", "/version")
        if resp is None:
            return None
        try:
            return resp.json().get("kotekan_version")
        except Exception:
            return None

    def __repr__(self) -> str:
        return f"KotekanClient({self.host}:{self.port})"
