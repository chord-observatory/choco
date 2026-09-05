"""Tests for the ``choco`` command-line client (choco/cli.py).

The transport is patched to Flask's test client, whose requests come
from 127.0.0.1, so every command exercises the real routes through the
same localhost bypass a shell on the choco host gets -- no mocked
server, and no network.
"""

import io
import json
import urllib.error
from unittest.mock import patch

import pytest
import yaml

from choco import cli
from choco.app import create_app
from choco.sync import ChangeType


@pytest.fixture
def configs_dir(tmp_path):
    nodes = {
        "groups": {
            "cx": {
                "cx1": {"host": "cx1.example", "port": 12048},
                "cx2": {"host": "cx2.example", "port": 12048},
            },
            "recv": {
                "recv1": {"host": "recv1.example", "port": 12048},
            },
        }
    }
    (tmp_path / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "cx").mkdir()
    (tmp_path / "cx" / "cx1.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "cx" / "cx2.yaml").write_text("num_elements: 2048\n")
    (tmp_path / "recv").mkdir()
    (tmp_path / "recv" / "recv1.yaml").write_text("buffer_depth: 12\n")
    return tmp_path


@pytest.fixture
def app(configs_dir):
    app = create_app(configs_dir=configs_dir)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def urls():
    """Every URL the CLI asked for, for the tests about where it points."""
    return []


@pytest.fixture
def run(app, urls, monkeypatch, capsys):
    """``run(*argv) -> (exit_code, stdout, stderr)`` with HTTP routed to
    the Flask test client."""
    client = app.test_client()

    def fake_http(method, url, body=None, timeout=60.0):
        urls.append(url)
        base, _, path = url.partition("//")[2].partition("/")
        kwargs = {"method": method}
        if body is not None:
            kwargs["json"] = body
        resp = client.open("/" + path, **kwargs)
        return resp.status_code, resp.get_data(as_text=True)

    monkeypatch.setattr(cli, "_http", fake_http)

    def _run(*argv):
        try:
            code = cli.main(list(argv))
        except SystemExit as e:  # argparse usage errors
            code = e.code
        out, err = capsys.readouterr()
        return code, out, err

    return _run


def _node(app, key):
    return app.config["registry"].get_node(key)


# --- reads ---------------------------------------------------------------

def test_status_is_pretty_json(run):
    job_ok = {"health": "ok", "state_mtime": None, "result": "success",
              "systemd": True, "active_state": None, "sub_state": None,
              "exit_status": None, "state_file": None, "unit": "test.service"}
    with patch("choco.web.job_status", return_value=job_ok):
        code, out, err = run("status")
    assert code == 0 and err == ""
    data = json.loads(out)
    assert data["up"] is True
    assert data["nodes"]["total"] == 3
    assert "\n  " in out  # indented, not a single line


def test_nodes_table_and_raw_json(run):
    code, out, err = run("nodes")
    assert code == 0 and err == ""
    header, *rows = out.splitlines()
    assert header.split() == ["KEY", "STATUS", "STARTED", "MAINT",
                              "VERSION", "WORKER", "ERROR"]
    keys = [r.split()[0] for r in rows if r.startswith(("cx/", "recv/"))]
    assert keys == ["cx/cx1", "cx/cx2", "recv/recv1"]
    # Fresh registries are in maintenance; the table says so.
    assert all(r.split()[3] == "yes" for r in rows if r.startswith("cx/"))
    assert "total 3" in out

    code, out, err = run("nodes", "-j")
    assert code == 0
    assert json.loads(out)["summary"]["total"] == 3


def test_get_any_endpoint(run):
    code, out, err = run("get", "/api/nodes")
    assert code == 0
    assert set(json.loads(out)["groups"]) == {"cx", "recv"}

    code, out, err = run("get", "/api/config/cx")
    assert code == 0
    assert json.loads(out) == {"num_elements": 2048}

    code, out, err = run("get", "api/nodes")
    assert code == 1
    assert "must start with '/'" in err


# --- desired state -------------------------------------------------------

def test_stop_and_start_take_several_targets(run, app):
    for key in ("cx/cx1", "cx/cx2", "recv/recv1"):
        _node(app, key).started = True

    code, out, err = run("stop", "cx", "recv/recv1")
    assert code == 0 and err == ""
    assert all(not _node(app, k).started
               for k in ("cx/cx1", "cx/cx2", "recv/recv1"))
    # One reply per target, both printed.
    assert out.count('"status": "ok"') == 2
    # Each node's worker was woken with a POLL, as the dashboard does.
    for key in ("cx/cx1", "cx/cx2", "recv/recv1"):
        assert [i.type for i in _node(app, key)._queue] == [ChangeType.POLL]

    code, out, err = run("start", "cx/cx2")
    assert code == 0
    assert _node(app, "cx/cx2").started is True
    assert _node(app, "cx/cx1").started is False


def test_maint_on_off(run, app):
    code, out, err = run("maint", "off", "cx/cx1")
    assert code == 0
    assert _node(app, "cx/cx1").maintenance is False
    assert _node(app, "cx/cx2").maintenance is True

    code, out, err = run("maint", "on", "cx")
    assert code == 0
    assert _node(app, "cx/cx1").maintenance is True

    code, out, err = run("maint", "sideways", "cx")
    assert code == 1
    assert "invalid choice" in err


def test_bad_target_is_rejected_before_anything_is_sent(run, app, urls):
    _node(app, "cx/cx1").started = True
    code, out, err = run("stop", "cx", "cx/cx1/extra")
    assert code == 1
    assert "target must be <group> or <group>/<node>" in err
    assert urls == []
    assert _node(app, "cx/cx1").started is True


def test_unknown_group_is_exit_1_with_the_servers_error(run):
    code, out, err = run("start", "nope")
    assert code == 1
    assert "HTTP 404" in err and "Group 'nope' not found" in err
    assert json.loads(out)["error"].startswith("Group 'nope'")


# --- config pushes -------------------------------------------------------

def test_push_from_file_and_stdin(run, app, tmp_path, monkeypatch):
    submitted = []
    orch = app.config["orchestrator"]
    orch.submit_node = submitted.append
    orch.submit_group = lambda group, factory: submitted.extend(
        factory(n.key) for n in app.config["registry"].nodes.values()
        if n.group == group)

    text = "num_elements: 4096\nlog_level: debug\n"
    path = tmp_path / "new.yaml"
    path.write_text(text)
    code, out, err = run("push", "cx/cx1", str(path))
    assert code == 0 and err == ""
    assert json.loads(out)["status"] == "queued"
    assert [(i.type, i.node_key, i.config_content) for i in submitted] == \
        [(ChangeType.BASE_CONFIG, "cx/cx1", text)]

    submitted.clear()
    monkeypatch.setattr("sys.stdin", io.StringIO(text))
    code, out, err = run("push", "cx", "-")
    assert code == 0
    assert [(i.node_key, i.config_content) for i in submitted] == \
        [("cx/cx1", text), ("cx/cx2", text)]

    submitted.clear()
    path.write_text("not: [valid")
    code, out, err = run("push", "cx/cx1", str(path))
    assert code == 1
    assert "HTTP 400" in err and "Invalid config" in err
    assert submitted == []

    code, out, err = run("push", "cx/cx1", str(tmp_path / "missing.yaml"))
    assert code == 1 and "missing.yaml" in err


def test_set_takes_a_literal_a_file_or_stdin(run, app, tmp_path, monkeypatch):
    submitted = []
    app.config["orchestrator"].submit_node = submitted.append
    values = {"bad_inputs": [1, 2], "update_id": "x"}

    code, out, err = run("set", "cx/cx1", "updatable_config/bad_inputs",
                         json.dumps(values))
    assert code == 0 and err == ""
    item = submitted.pop()
    assert (item.type, item.endpoint, item.values) == \
        (ChangeType.UPDATABLE_CONFIG, "updatable_config/bad_inputs", values)

    path = tmp_path / "values.json"
    path.write_text(json.dumps(values))
    code, out, err = run("set", "cx/cx1", "updatable_config/bad_inputs",
                         "@" + str(path))
    assert code == 0
    assert submitted.pop().values == values

    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(values)))
    code, out, err = run("set", "cx/cx1", "updatable_config/bad_inputs", "-")
    assert code == 0
    assert submitted.pop().values == values

    code, out, err = run("set", "cx/cx1", "updatable_config/bad_inputs",
                         "{not json")
    assert code == 1 and "error:" in err and submitted == []

    code, out, err = run("set", "cx/cx1", "updatable_config/bad_inputs",
                         "values.json")
    assert code == 1 and "write @values.json" in err and submitted == []


# --- one-off configs -----------------------------------------------------

@pytest.fixture
def quiet_cluster(app):
    """No REST write leaves the process; probes answer per ``statuses``."""
    from choco.state import Node, NodeStatus
    statuses = {}
    calls = []

    def get_status(node):
        return statuses.get(node.key, NodeStatus.IDLE)

    def start(node, config, *, override_maintenance=False):
        if node.key in app.config["registry"].nodes:
            calls.append((node.key, config))
        return True

    with patch.object(Node, "get_status", get_status), \
         patch.object(Node, "start", start), \
         patch.object(Node, "kill", lambda self: True), \
         patch.object(Node, "get_version_info", lambda self: None):
        yield statuses, calls


def test_oneshot_reports_started_and_skipped(run, app, tmp_path, quiet_cluster):
    from choco.state import NodeStatus
    statuses, calls = quiet_cluster
    statuses["cx/cx2"] = NodeStatus.STARTED
    path = tmp_path / "once.yaml"
    path.write_text("num_elements: 8\n")

    code, out, err = run("oneshot", "cx", str(path))
    assert code == 0 and err == ""
    body = json.loads(out)
    assert body["started"] == ["cx/cx1"]
    assert body["skipped"] == {"cx/cx2": "running"}
    assert calls == [("cx/cx1", {"num_elements": 8})]


def test_oneshot_with_nothing_started_is_exit_1(run, app, tmp_path, quiet_cluster):
    from choco.state import NodeStatus
    statuses, calls = quiet_cluster
    statuses["cx/cx1"] = NodeStatus.STARTED
    path = tmp_path / "once.yaml"
    path.write_text("num_elements: 8\n")

    code, out, err = run("oneshot", "cx/cx1", str(path))
    assert code == 1
    assert "HTTP 409" in err
    assert json.loads(out)["skipped"] == {"cx/cx1": "running"}
    assert calls == []


# --- where it points, and when it cannot ---------------------------------

def test_url_flag_and_environment(run, urls, monkeypatch):
    run("status")
    assert urls[-1] == "https://localhost:5000/api/status"

    run("--url", "http://127.0.0.1:5050/", "status")
    assert urls[-1] == "http://127.0.0.1:5050/api/status"

    monkeypatch.setenv("CHOCO_URL", "http://dev:1")
    run("status")
    assert urls[-1] == "http://dev:1/api/status"


def test_unreachable_is_exit_2(monkeypatch, capsys):
    def refused(method, url, body=None, timeout=60.0):
        raise urllib.error.URLError("connection refused")
    monkeypatch.setattr(cli, "_http", refused)
    code = cli.main(["status"])
    out, err = capsys.readouterr()
    assert code == 2
    assert "cannot reach choco at https://localhost:5000" in err
    assert "connection refused" in err
    assert out == ""


def test_help_command_and_bare_invocation(run):
    for argv in ((), ("help",)):
        code, out, err = run(*argv)
        assert code == 0 and err == ""
        assert out.startswith("usage: choco ")
        assert "oneshot" in out and "help" in out

    code, out, err = run("help", "push")
    assert code == 0
    assert out.startswith("usage: choco push ")
    assert "base config" in out

    code, out, err = run("help", "nope")
    assert code == 1
    assert "unknown command 'nope'" in err


def test_url_is_accepted_after_the_command_too(run, urls):
    code, out, err = run("nodes", "--url", "http://x:1")
    assert code == 0
    assert urls[-1] == "http://x:1/api/nodes/status"


def test_target_path_quotes_and_validates():
    assert cli.target_path("cx") == "/cx"
    assert cli.target_path("cx/cx19") == "/cx/cx19"
    assert cli.target_path("/cx/a b/") == "/cx/a%20b"
    for bad in ("", "/", "cx//cx1", "cx/cx1/x"):
        with pytest.raises(ValueError):
            cli.target_path(bad)


def test_login_redirect_is_an_error_not_followed(monkeypatch, capsys):
    """Off the choco host the API answers with a redirect to /login;
    a CLI that followed it would print the login form and exit 0."""
    html = '<!doctype html><title>Redirecting...</title>'
    monkeypatch.setattr(cli, "_http", lambda *a, **k: (302, html))
    for argv in (["status"], ["nodes"], ["stop", "cx"]):
        code = cli.main(argv)
        out, err = capsys.readouterr()
        assert code == 1, argv
        assert out == ""  # the HTML is noise
        assert "HTTP 302" in err and "login" in err and "--url" in err


def test_broken_pipe_is_not_an_error(monkeypatch, capsys):
    """`choco nodes | head`: the reader closing the pipe must not be
    reported as choco being unreachable (BrokenPipeError is an OSError)."""
    def boom(*a, **k):
        raise BrokenPipeError(32, "Broken pipe")
    monkeypatch.setattr(cli, "_emit", boom)
    monkeypatch.setattr(cli, "_http", lambda *a, **k: (200, "{}"))
    code = cli.main(["status"])
    out, err = capsys.readouterr()
    assert code == 0
    assert err == ""


def test_unreadable_file_is_exit_1(run, tmp_path):
    code, out, err = run("push", "cx/cx1", str(tmp_path))  # a directory
    assert code == 1
    assert "error:" in err and "cannot reach" not in err


def test_usage_lines_read_as_placeholders(run):
    code, out, err = run("help", "oneshot")
    assert out.splitlines()[0] == \
        "usage: choco oneshot [-h] [--url URL] <target> <file>"
    code, out, err = run("help", "maint")
    assert out.splitlines()[0] == \
        "usage: choco maint [-h] [--url URL] on|off <target> [<target> ...]"
    code, out, err = run("help", "set")
    assert "<target> <endpoint> <json|@file|->" in out.splitlines()[0]


def test_usage_error_is_exit_1(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["frobnicate"])
    assert e.value.code == 1
    assert "invalid choice" in capsys.readouterr().err
