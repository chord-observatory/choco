"""SSH client for managing kotekan on remote nodes.

Authentication uses Kerberos/GSSAPI via FreeIPA. The SSH user's password is
stored in config.yaml and used to obtain a Kerberos ticket (kinit) automatically.

Kotekan is managed as a systemd service. The choco user has limited sudo
(configured via FreeIPA) for: make install, systemctl start/stop/restart/daemon-reload.
"""

import logging
import shlex
import subprocess
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Default timeout for SSH commands (seconds)
DEFAULT_TIMEOUT = 30
BUILD_TIMEOUT = 600  # 10 minutes for builds

# Default paths
DEFAULT_KOTEKAN_DIR = "/kotekan"
DEFAULT_BUILD_DIR = "/kotekan/build"
DEFAULT_SSH_USER = "choco"
DEFAULT_REPO_URL = "https://github.com/kotekan/kotekan.git"

# Systemd service name and installed binary path (set by make install)
KOTEKAN_SERVICE = "kotekan"
KOTEKAN_BIN = "/usr/local/bin/kotekan"


@dataclass
class SSHConfig:
    """SSH and kotekan deployment settings, loaded from config.yaml."""

    kotekan_dir: str = DEFAULT_KOTEKAN_DIR
    build_dir: str = DEFAULT_BUILD_DIR
    ssh_user: str = DEFAULT_SSH_USER
    ssh_password: str = ""
    repo_url: str = DEFAULT_REPO_URL
    cmake_args: str = ""

    @classmethod
    def from_config(cls, config: dict) -> "SSHConfig":
        ssh = config.get("ssh") or {}
        fields = {k: v for k, v in ssh.items()
                  if k in cls.__dataclass_fields__ and v is not None}
        return cls(**fields)


_kinit_checked: float = 0  # timestamp of last successful klist check


def ensure_kerberos_ticket(cfg: SSHConfig) -> bool:
    """Obtain or refresh a Kerberos ticket for the SSH user.

    Uses kinit with the password from config. The ticket is cached in the
    default credential cache and reused by subsequent SSH connections.
    Checks are rate-limited to at most once per 60 seconds.

    Returns:
        True if a valid ticket exists or was obtained.
    """
    global _kinit_checked

    # Skip check if we verified recently
    now = time.time()
    if now - _kinit_checked < 60:
        return True

    # Check if we already have a valid ticket
    result = subprocess.run(
        ["klist", "-s"], capture_output=True, timeout=10,
    )
    if result.returncode == 0:
        _kinit_checked = now
        return True

    if not cfg.ssh_password:
        logger.error(
            "No Kerberos ticket and no ssh.ssh_password configured. "
            "Either run kinit manually or set ssh.ssh_password in config.yaml."
        )
        return False

    # Obtain a ticket using the configured password
    result = subprocess.run(
        ["kinit", cfg.ssh_user],
        input=cfg.ssh_password + "\n",
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode == 0:
        logger.info(f"Obtained Kerberos ticket for {cfg.ssh_user}")
        _kinit_checked = now
        return True

    logger.error(f"kinit failed for {cfg.ssh_user}: {result.stderr.strip()}")
    return False


def _ssh(host: str, command: str, user: str = DEFAULT_SSH_USER,
         timeout: int = DEFAULT_TIMEOUT) -> subprocess.CompletedProcess:
    """Run a command on a remote host via SSH using Kerberos/GSSAPI."""
    ssh_cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "GSSAPIAuthentication=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
        "-l", user,
        host,
        command,
    ]
    logger.debug(f"SSH {user}@{host}: {command}")
    result = subprocess.run(
        ssh_cmd, capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0 and result.stderr:
        logger.warning(f"SSH {user}@{host} stderr: {result.stderr.strip()}")
    return result


def check_connection(host: str, cfg: SSHConfig | None = None) -> bool:
    """Test SSH connectivity to a host (verifies Kerberos ticket works)."""
    cfg = cfg or SSHConfig()
    ensure_kerberos_ticket(cfg)
    try:
        result = _ssh(host, "echo ok", user=cfg.ssh_user, timeout=15)
        return result.returncode == 0 and "ok" in result.stdout
    except (subprocess.TimeoutExpired, OSError) as e:
        logger.warning(f"SSH connection to {host} failed: {e}")
        return False


def clone_kotekan(host: str, ref: str, cfg: SSHConfig | None = None) -> bool:
    """Clone or update a kotekan repository on a remote node.

    The kotekan directory must already exist and be owned by the SSH user
    (set up by ansible).

    Args:
        host: Remote hostname.
        ref: Branch name, tag, or commit hash to checkout.
        cfg: SSH/deployment config.

    Returns:
        True if successful.
    """
    cfg = cfg or SSHConfig()
    dest = cfg.kotekan_dir

    # Clone if not present, otherwise fetch and checkout
    git_cmd = (
        f"if [ -d {dest}/.git ]; then "
        f"  cd {dest} && git fetch --all && git checkout {shlex.quote(ref)} "
        f"  && git pull --ff-only 2>/dev/null; "
        f"else "
        f"  git clone {shlex.quote(cfg.repo_url)} {dest} "
        f"  && cd {dest} && git checkout {shlex.quote(ref)}; "
        f"fi"
    )
    try:
        result = _ssh(host, git_cmd, user=cfg.ssh_user, timeout=120)
        if result.returncode == 0:
            logger.info(f"Cloned/updated kotekan on {host} to {ref}")
            return True
        logger.error(f"Failed to clone kotekan on {host}: {result.stderr}")
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout cloning kotekan on {host}")
        return False


def build_kotekan(host: str, cfg: SSHConfig | None = None) -> bool:
    """Build kotekan on a remote node using CMake.

    Returns:
        True if build succeeded.
    """
    cfg = cfg or SSHConfig()

    nproc_cmd = "nproc 2>/dev/null || echo 4"
    cmd = (
        f"mkdir -p {cfg.build_dir} && cd {cfg.build_dir} "
        f"&& cmake {cfg.kotekan_dir} {cfg.cmake_args} "
        f"&& make -j$({nproc_cmd})"
    )
    try:
        result = _ssh(host, cmd, user=cfg.ssh_user, timeout=BUILD_TIMEOUT)
        if result.returncode == 0:
            logger.info(f"Built kotekan on {host}")
            return True
        logger.error(f"Build failed on {host}: {result.stderr[-500:]}")
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"Build timeout on {host}")
        return False


def _systemctl(action: str, host: str, cfg: SSHConfig) -> bool:
    """Run a systemctl action on the kotekan service via sudo."""
    try:
        result = _ssh(host, f"sudo systemctl {action} {KOTEKAN_SERVICE}",
                       user=cfg.ssh_user)
        if result.returncode == 0:
            logger.info(f"Kotekan {action} on {host}")
            return True
        logger.error(f"Failed to {action} kotekan on {host}: {result.stderr}")
        return False
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout: systemctl {action} on {host}")
        return False


def start_kotekan(host: str, cfg: SSHConfig | None = None) -> bool:
    """Start the kotekan systemd service on a remote node."""
    return _systemctl("start", host, cfg or SSHConfig())


def stop_kotekan(host: str, cfg: SSHConfig | None = None) -> bool:
    """Stop the kotekan systemd service on a remote node."""
    return _systemctl("stop", host, cfg or SSHConfig())


def restart_kotekan(host: str, cfg: SSHConfig | None = None) -> bool:
    """Restart the kotekan systemd service on a remote node."""
    return _systemctl("restart", host, cfg or SSHConfig())


def install_kotekan(
    host: str,
    ref: str,
    cfg: SSHConfig | None = None,
) -> bool:
    """Clone, build, install, and (re)start kotekan on a remote node.

    Runs: git clone/fetch → cmake + make → sudo make install →
          sudo systemctl daemon-reload → sudo systemctl restart kotekan.

    Returns:
        True if all steps succeeded.
    """
    cfg = cfg or SSHConfig()
    if not ensure_kerberos_ticket(cfg):
        return False
    if not clone_kotekan(host, ref, cfg):
        return False
    if not build_kotekan(host, cfg):
        return False

    # Install binary and service file, reload systemd unit files
    install_cmd = (
        f"cd {cfg.build_dir} && sudo make install "
        f"&& sudo systemctl daemon-reload"
    )
    try:
        result = _ssh(host, install_cmd, user=cfg.ssh_user, timeout=60)
        if result.returncode != 0:
            logger.error(f"make install failed on {host}: {result.stderr[-500:]}")
            return False
        logger.info(f"Installed kotekan on {host}")
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout installing kotekan on {host}")
        return False

    return restart_kotekan(host, cfg)


def get_kotekan_pid(host: str, cfg: SSHConfig | None = None) -> int | None:
    """Get the PID of kotekan on a remote node, or None if not running."""
    cfg = cfg or SSHConfig()
    try:
        result = _ssh(
            host,
            f"systemctl show {KOTEKAN_SERVICE} --property=MainPID --value",
            user=cfg.ssh_user,
        )
        if result.returncode == 0:
            pid = result.stdout.strip()
            if pid and pid != "0":
                return int(pid)
        return None
    except (subprocess.TimeoutExpired, ValueError):
        return None


def get_kotekan_version(host: str, cfg: SSHConfig | None = None) -> str | None:
    """Get the kotekan version string from a remote node."""
    cfg = cfg or SSHConfig()
    try:
        result = _ssh(host, f"{KOTEKAN_BIN} --version 2>/dev/null",
                       user=cfg.ssh_user)
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except subprocess.TimeoutExpired:
        return None


def run_command(host: str, command: str, timeout: int = DEFAULT_TIMEOUT) -> tuple[int, str, str]:
    """Run an arbitrary command on a remote host.

    Returns:
        Tuple of (return_code, stdout, stderr).
    """
    try:
        result = _ssh(host, command, timeout=timeout)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
