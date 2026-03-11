"""Tests for SSH client (mocked subprocess calls)."""

import subprocess
from unittest.mock import patch, MagicMock

import pytest

import choco.ssh
from choco.ssh import (
    SSHConfig,
    _ssh,
    ensure_kerberos_ticket,
    check_connection,
    clone_kotekan,
    build_kotekan,
    install_kotekan,
    start_kotekan,
    stop_kotekan,
    restart_kotekan,
    get_kotekan_pid,
    get_kotekan_version,
    run_command,
)

HOST = "node1.example.com"


@pytest.fixture(autouse=True)
def reset_kinit_cache():
    """Reset the kinit check cache between tests."""
    choco.ssh._kinit_checked = 0


def _mock_result(stdout="", stderr="", returncode=0):
    result = MagicMock(spec=subprocess.CompletedProcess)
    result.stdout = stdout
    result.stderr = stderr
    result.returncode = returncode
    return result


class TestSSHConfig:
    def test_defaults(self):
        cfg = SSHConfig()
        assert cfg.kotekan_dir == "/kotekan"
        assert cfg.build_dir == "/kotekan/build"
        assert cfg.ssh_user == "choco"

    def test_from_config(self):
        config = {"ssh": {"kotekan_dir": "/opt/kotekan", "ssh_user": "myuser"}}
        cfg = SSHConfig.from_config(config)
        assert cfg.kotekan_dir == "/opt/kotekan"
        assert cfg.ssh_user == "myuser"
        assert cfg.repo_url == "https://github.com/kotekan/kotekan.git"

    def test_from_empty_config(self):
        cfg = SSHConfig.from_config({})
        assert cfg.kotekan_dir == "/kotekan"


class TestSSHCommand:
    @patch("choco.ssh.subprocess.run")
    def test_ssh_uses_gssapi(self, mock_run):
        mock_run.return_value = _mock_result("ok\n")
        _ssh(HOST, "echo ok")
        args = mock_run.call_args[0][0]
        assert "GSSAPIAuthentication=yes" in " ".join(args)

    @patch("choco.ssh.subprocess.run")
    def test_ssh_batch_mode(self, mock_run):
        mock_run.return_value = _mock_result("ok\n")
        _ssh(HOST, "echo ok")
        args = mock_run.call_args[0][0]
        assert "BatchMode=yes" in " ".join(args)

    @patch("choco.ssh.subprocess.run")
    def test_ssh_user(self, mock_run):
        mock_run.return_value = _mock_result("ok\n")
        _ssh(HOST, "echo ok", user="myuser")
        args = mock_run.call_args[0][0]
        assert "-l" in args
        assert args[args.index("-l") + 1] == "myuser"


class TestCheckConnection:
    @patch("choco.ssh.subprocess.run")
    def test_success(self, mock_run):
        mock_run.side_effect = [
            _mock_result(""),       # klist -s (ticket valid)
            _mock_result("ok\n"),   # ssh echo ok
        ]
        assert check_connection(HOST) is True

    @patch("choco.ssh.subprocess.run")
    def test_failure(self, mock_run):
        mock_run.side_effect = [
            _mock_result(""),                              # klist -s
            _mock_result("", "Connection refused", 255),   # ssh
        ]
        assert check_connection(HOST) is False

    @patch("choco.ssh.subprocess.run")
    def test_timeout(self, mock_run):
        mock_run.side_effect = [
            _mock_result(""),                                    # klist -s
            subprocess.TimeoutExpired("ssh", 15),                # ssh
        ]
        assert check_connection(HOST) is False


class TestEnsureKerberosTicket:
    @patch("choco.ssh.subprocess.run")
    def test_existing_ticket(self, mock_run):
        mock_run.return_value = _mock_result("")  # klist -s succeeds
        cfg = SSHConfig()
        assert ensure_kerberos_ticket(cfg) is True
        assert mock_run.call_count == 1

    @patch("choco.ssh.subprocess.run")
    def test_kinit_with_password(self, mock_run):
        mock_run.side_effect = [
            _mock_result("", "", 1),  # klist -s fails (no ticket)
            _mock_result(""),         # kinit succeeds
        ]
        cfg = SSHConfig(ssh_password="secret")
        assert ensure_kerberos_ticket(cfg) is True

    @patch("choco.ssh.subprocess.run")
    def test_no_ticket_no_password(self, mock_run):
        mock_run.return_value = _mock_result("", "", 1)  # klist -s fails
        cfg = SSHConfig()
        assert ensure_kerberos_ticket(cfg) is False


class TestCloneKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_clone_success(self, mock_run):
        mock_run.return_value = _mock_result("")
        cfg = SSHConfig()
        assert clone_kotekan(HOST, "main", cfg) is True
        assert mock_run.call_count == 1

    @patch("choco.ssh.subprocess.run")
    def test_clone_failure(self, mock_run):
        mock_run.return_value = _mock_result("", "permission denied", 1)
        assert clone_kotekan(HOST, "main") is False


class TestBuildKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_build_success(self, mock_run):
        mock_run.return_value = _mock_result("")
        assert build_kotekan(HOST) is True

    @patch("choco.ssh.subprocess.run")
    def test_build_failure(self, mock_run):
        mock_run.return_value = _mock_result("", "error: cmake failed", 2)
        assert build_kotekan(HOST) is False

    @patch("choco.ssh.subprocess.run", side_effect=subprocess.TimeoutExpired("ssh", 600))
    def test_build_timeout(self, mock_run):
        assert build_kotekan(HOST) is False


class TestStartKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_start(self, mock_run):
        mock_run.return_value = _mock_result("")
        assert start_kotekan(HOST) is True
        # Verify it ran sudo systemctl start
        ssh_command = mock_run.call_args[0][0][-1]
        assert "sudo systemctl start kotekan" == ssh_command

    @patch("choco.ssh.subprocess.run")
    def test_start_failure(self, mock_run):
        mock_run.return_value = _mock_result("", "Failed to start", 1)
        assert start_kotekan(HOST) is False

    @patch("choco.ssh.subprocess.run")
    def test_start_uses_ssh_user(self, mock_run):
        mock_run.return_value = _mock_result("")
        cfg = SSHConfig(ssh_user="myuser")
        start_kotekan(HOST, cfg=cfg)
        cmd_str = " ".join(mock_run.call_args[0][0])
        assert "-l myuser" in cmd_str


class TestStopKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_stop_success(self, mock_run):
        mock_run.return_value = _mock_result("")
        assert stop_kotekan(HOST) is True
        ssh_command = mock_run.call_args[0][0][-1]
        assert "sudo systemctl stop kotekan" == ssh_command

    @patch("choco.ssh.subprocess.run")
    def test_stop_uses_ssh_user(self, mock_run):
        mock_run.return_value = _mock_result("")
        cfg = SSHConfig(ssh_user="myuser")
        stop_kotekan(HOST, cfg=cfg)
        cmd_str = " ".join(mock_run.call_args[0][0])
        assert "-l myuser" in cmd_str


class TestRestartKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_restart(self, mock_run):
        mock_run.return_value = _mock_result("")
        assert restart_kotekan(HOST) is True
        ssh_command = mock_run.call_args[0][0][-1]
        assert "sudo systemctl restart kotekan" == ssh_command


class TestInstallKotekan:
    @patch("choco.ssh.subprocess.run")
    def test_full_install(self, mock_run):
        mock_run.side_effect = [
            _mock_result(""),  # klist -s
            _mock_result(""),  # git clone/fetch
            _mock_result(""),  # cmake + make
            _mock_result(""),  # sudo make install + daemon-reload
            _mock_result(""),  # sudo systemctl restart
        ]
        assert install_kotekan(HOST, "main") is True
        assert mock_run.call_count == 5
        # Verify the install step
        install_cmd = mock_run.call_args_list[3][0][0][-1]
        assert "sudo make install" in install_cmd
        assert "sudo systemctl daemon-reload" in install_cmd

    @patch("choco.ssh.subprocess.run")
    def test_install_fails_on_make_install(self, mock_run):
        mock_run.side_effect = [
            _mock_result(""),                          # klist -s
            _mock_result(""),                          # git clone
            _mock_result(""),                          # cmake + make
            _mock_result("", "permission denied", 1),  # sudo make install fails
        ]
        assert install_kotekan(HOST, "main") is False


class TestGetKotekanPid:
    @patch("choco.ssh.subprocess.run")
    def test_running(self, mock_run):
        mock_run.return_value = _mock_result("12345\n")
        assert get_kotekan_pid(HOST) == 12345

    @patch("choco.ssh.subprocess.run")
    def test_not_running(self, mock_run):
        mock_run.return_value = _mock_result("0\n")
        assert get_kotekan_pid(HOST) is None

    @patch("choco.ssh.subprocess.run")
    def test_service_not_found(self, mock_run):
        mock_run.return_value = _mock_result("", "", 1)
        assert get_kotekan_pid(HOST) is None


class TestGetKotekanVersion:
    @patch("choco.ssh.subprocess.run")
    def test_version(self, mock_run):
        mock_run.return_value = _mock_result("kotekan 2.0.0\n")
        assert get_kotekan_version(HOST) == "kotekan 2.0.0"

    @patch("choco.ssh.subprocess.run")
    def test_not_installed(self, mock_run):
        mock_run.return_value = _mock_result("", "not found", 127)
        assert get_kotekan_version(HOST) is None


class TestRunCommand:
    @patch("choco.ssh.subprocess.run")
    def test_success(self, mock_run):
        mock_run.return_value = _mock_result("output\n", "", 0)
        rc, stdout, stderr = run_command(HOST, "ls")
        assert rc == 0
        assert stdout == "output\n"

    @patch("choco.ssh.subprocess.run", side_effect=subprocess.TimeoutExpired("ssh", 30))
    def test_timeout(self, mock_run):
        rc, stdout, stderr = run_command(HOST, "sleep 999")
        assert rc == -1
        assert "timed out" in stderr
