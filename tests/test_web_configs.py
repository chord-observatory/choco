"""The config library: editing, per-node file selection, the JSON API and
the upstream pull."""

import json
import logging
from unittest.mock import patch

import pytest
import responses
import yaml

from choco.state import Node, NodeStatus
from choco.sync import ChangeType

from tests.helpers import csrf, login


# --- Config library ------------------------------------------------------

class TestConfigLibrary:
    @pytest.fixture
    def library(self, configs_dir, app):
        """A shared chord/ library with cx1 rendering chord/pathfinder.j2,
        which includes chord/telescope.j2; cx2 and recv1 stay on their
        per-node files."""
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "telescope.j2" %}\n')
        (chord / "telescope.j2").write_text("telescope: {name: a}\n")
        data = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/pathfinder.j2"
        (configs_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        app.config["registry"].reload()
        return configs_dir

    def test_page_lists_files_and_users(self, client, library):
        login(client)
        body = client.get("/configs").get_data(as_text=True)
        assert "chord/pathfinder.j2" in body and "chord/telescope.j2" in body
        assert "cx/cx1.yaml" in body and "recv/recv1.yaml" in body
        assert 'href="/configs/edit/.updatable' not in body
        assert 'href="/configs/edit/nodes.yaml"' not in body
        row = body[body.index("chord/telescope.j2"):]
        row = row[:row.index("</tr>")]
        assert "cx/cx1" in row

    @pytest.mark.parametrize("path", [
        "chord/nope.j2", "nodes.yaml", ".updatable/cx/cx1.json",
        "chord/telescope.txt", "chord/.hidden.j2",
    ])
    def test_bad_or_missing_path_is_404(self, client, library, path):
        login(client)
        assert client.get("/configs/edit/" + path).status_code == 404

    def test_editor_shows_file_and_users(self, client, library):
        login(client)
        body = client.get("/configs/edit/chord/telescope.j2").get_data(as_text=True)
        assert "telescope: {name: a}" in body
        assert "Included by: cx/cx1" in body
        body = client.get("/configs/edit/chord/pathfinder.j2").get_data(as_text=True)
        assert "Rendered by: cx/cx1" in body

    def test_save_rerenders_every_user_now(self, client, app, library):
        login(client)
        token = csrf(client)
        orch = app.config["orchestrator"]
        orch._file_mtimes = orch._config_file_mtimes()  # scan baseline
        polled = []
        orch.submit_node = lambda key, item: polled.append((key, item.type))
        resp = client.post("/configs/edit/chord/telescope.j2",
                           data={"_csrf_token": token,
                                 "content": "telescope: {name: b}\r\n"})
        assert resp.status_code == 302
        # CRLF normalised, written atomically, no temp file left behind.
        assert (library / "chord" / "telescope.j2").read_text() == \
            "telescope: {name: b}\n"
        assert not list((library / "chord").glob("*.tmp"))
        node = app.config["registry"].get_node("cx/cx1")
        assert node.rendered_config["telescope"] == {"name": "b"}
        assert polled == [("cx/cx1", ChangeType.POLL)]
        # The scan does not take choco's own write for an external edit.
        polled.clear()
        orch.check_config_files()
        assert polled == []

    def test_save_refused_when_a_user_would_not_render(self, client, app, library):
        login(client)
        token = csrf(client)
        before = (library / "chord" / "telescope.j2").read_text()
        resp = client.post("/configs/edit/chord/telescope.j2",
                           data={"_csrf_token": token,
                                 "content": '{% include "absent.j2" %}\n'})
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Not saved" in body and "cx/cx1" in body
        assert (library / "chord" / "telescope.j2").read_text() == before
        assert app.config["registry"].get_node("cx/cx1").rendered_config[
            "telescope"] == {"name": "a"}

    def test_unused_file_only_needs_template_syntax(self, client, library):
        login(client)
        token = csrf(client)
        (library / "chord" / "spare.j2").write_text("x: 1\n")
        resp = client.post("/configs/edit/chord/spare.j2",
                           data={"_csrf_token": token, "content": "{% if %}"})
        assert resp.status_code == 200 and "Not saved" in resp.get_data(as_text=True)
        resp = client.post("/configs/edit/chord/spare.j2",
                           data={"_csrf_token": token, "content": "not: [a mapping\n"})
        assert resp.status_code == 302  # YAML is not checked for a fragment

    def test_vars_yaml_must_be_a_mapping(self, client, app, library):
        login(client)
        token = csrf(client)
        (library / "vars.yaml").write_text("n: 1\n")
        app.config["registry"].reload()
        resp = client.post("/configs/edit/vars.yaml",
                           data={"_csrf_token": token, "content": "- a\n"})
        assert resp.status_code == 200 and "mapping" in resp.get_data(as_text=True)
        body = client.get("/configs").get_data(as_text=True)
        assert "every node" in body

    def test_new_file(self, client, library):
        login(client)
        token = csrf(client)
        resp = client.post("/configs/new",
                           data={"_csrf_token": token, "name": "chord/extra.yaml"})
        assert resp.status_code == 302
        assert resp.headers["Location"].endswith("/configs/edit/chord/extra.yaml")
        assert (library / "chord" / "extra.yaml").read_text() == ""
        resp = client.post("/configs/new",
                           data={"_csrf_token": token, "name": "../x.yaml"},
                           follow_redirects=True)
        assert "Not created" in resp.get_data(as_text=True)
        assert not (library / "x.yaml").exists()


class TestNodeConfigSelection:
    @pytest.fixture
    def library(self, configs_dir, app):
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text("num_elements: 128\n")
        return configs_dir

    def test_node_page_offers_the_library(self, client, library):
        login(client)
        body = client.get("/nodes/edit/cx/cx1").get_data(as_text=True)
        assert 'name="action" value="set_config"' in body
        assert '<option value="chord/pathfinder.j2"' in body
        assert "shared with" not in body
        # No text editor on the node page: the file is edited in the library.
        assert "<textarea" not in body and 'value="save_config"' not in body
        assert 'href="/configs/edit/cx/cx1.yaml"' in body
        assert 'title="Edit cx/cx1.yaml in the config library"' in body
        assert 'value="push_config"' in body

    def test_dashboard_buttons(self, client, library):
        login(client)
        body = client.get("/nodes").get_data(as_text=True)
        assert 'href="/configs"' in body and ">Edit configs<" in body
        assert 'href="/nodes/edit"' in body and ">Edit nodes<" in body
        # The table names each node's file but carries no per-row editor
        # button: the library is one click away in the header.
        assert "<code>cx/cx1.yaml</code>" in body
        assert "/configs/edit/" not in body
        assert "edit-group" not in body
        assert client.get("/nodes/edit-group/cx").status_code == 404

    def test_library_page_has_edit_buttons(self, client, library):
        login(client)
        body = client.get("/configs").get_data(as_text=True)
        assert 'href="/configs/edit/chord/pathfinder.j2" role="button"' in body
        assert "no node uses it yet" in body

    def test_use_rewrites_nodes_yaml_and_rebuilds(self, client, app, library):
        login(client)
        token = csrf(client)
        registry = app.config["registry"]
        for n in registry.nodes.values():
            n.maintenance = False
        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/nodes/edit/cx/cx1",
                               data={"_csrf_token": token, "action": "set_config",
                                     "config": "chord/pathfinder.j2"},
                               follow_redirects=True)
        assert resp.status_code == 200
        assert "now renders chord/pathfinder.j2" in resp.get_data(as_text=True)
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert on_disk["groups"]["cx"]["cx1"]["config"] == "chord/pathfinder.j2"
        assert "config" not in on_disk["groups"]["cx"]["cx2"]
        node = registry.get_node("cx/cx1")
        assert node.config_filename == "chord/pathfinder.j2"
        assert node.rendered_config == {"num_elements": 128}
        assert all(n.maintenance for n in registry.nodes.values())

        # Both nodes on the file: the page says so.
        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            client.post("/nodes/edit/cx/cx2",
                        data={"_csrf_token": token, "action": "set_config",
                              "config": "chord/pathfinder.j2"})
        body = client.get("/nodes/edit/cx/cx1").get_data(as_text=True)
        assert "shared with cx/cx2" in body

        # Back to the per-node file.
        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            client.post("/nodes/edit/cx/cx1",
                        data={"_csrf_token": token, "action": "set_config",
                              "config": ""})
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert "config" not in on_disk["groups"]["cx"]["cx1"]
        assert registry.get_node("cx/cx1").config_filename == "cx/cx1.yaml"

    def test_missing_or_bad_file_refused(self, client, app, library):
        login(client)
        token = csrf(client)
        before = (library / "nodes.yaml").read_text()
        for bad in ("chord/absent.j2", "../x.j2", "nodes.yaml"):
            resp = client.post("/nodes/edit/cx/cx1",
                               data={"_csrf_token": token, "action": "set_config",
                                     "config": bad}, follow_redirects=True)
            assert resp.status_code == 200
        assert (library / "nodes.yaml").read_text() == before
        assert app.config["registry"].get_node("cx/cx1").explicit_config is None

    def test_nodes_editor_round_trips_config(self, client, app, library):
        login(client)
        token = csrf(client)
        payload = {"groups": {"cx": [
            {"name": "cx1", "host": "cx1.example", "port": 12048,
             "config": "chord/pathfinder.j2"},
            {"name": "cx2", "host": "cx2.example", "port": 12048, "config": ""},
        ]}}
        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/nodes/edit", data=json.dumps(payload),
                               content_type="application/json",
                               headers={"X-CSRF-Token": token})
        assert resp.status_code == 200
        on_disk = yaml.safe_load((library / "nodes.yaml").read_text())
        assert on_disk["groups"]["cx"] == {
            "cx1": {"host": "cx1.example", "port": 12048,
                    "config": "chord/pathfinder.j2"},
            "cx2": {"host": "cx2.example", "port": 12048},
        }
        body = client.get("/nodes/edit").get_data(as_text=True)
        assert 'value="chord/pathfinder.j2"' in body
        assert '<datalist id="config-files">' in body

        payload["groups"]["cx"][0]["config"] = "../../etc/x.yaml"
        resp = client.post("/nodes/edit", data=json.dumps(payload),
                           content_type="application/json",
                           headers={"X-CSRF-Token": token})
        assert resp.status_code == 400
        assert "cx/cx1" in resp.get_json()["error"]


class TestConfigLibraryApi:
    """The loopback JSON API the CLI and a deploy script use."""

    @pytest.fixture
    def library(self, configs_dir, app):
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "telescope.j2" %}\n')
        (chord / "telescope.j2").write_text("telescope: {name: a}\n")
        data = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/pathfinder.j2"
        (configs_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        app.config["registry"].reload()
        return configs_dir

    def test_list_and_get(self, client, library):
        files = client.get("/api/configs").get_json()["files"]
        by_path = {f["path"]: f for f in files}
        assert by_path["chord/pathfinder.j2"]["used_by"] == ["cx/cx1"]
        assert by_path["chord/telescope.j2"]["included_by"] == ["cx/cx1"]
        assert "nodes.yaml" not in by_path
        got = client.get("/api/configs/chord/telescope.j2").get_json()
        assert got["content"] == "telescope: {name: a}\n"
        assert got["included_by"] == ["cx/cx1"]
        assert client.get("/api/configs/chord/absent.j2").status_code == 404
        assert client.get("/api/configs/../x.yaml").status_code in (400, 404)
        assert client.get("/api/configs/nodes.yaml").status_code == 400

    def test_modified_is_utc(self, client, library):
        import os
        os.utime(library / "chord" / "telescope.j2", (0, 1791126511))
        got = client.get("/api/configs/chord/telescope.j2").get_json()
        assert got["modified"] == "2026-10-04 15:08"       # UTC, whatever TZ says

    def test_put_creates_validates_and_reloads(self, client, app, library):
        orch = app.config["orchestrator"]
        polled = []
        orch.submit_node = lambda key, item: polled.append(key)
        resp = client.put("/api/configs/chord/common.j2",
                          json={"content": "log_level: WARN\n"})
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "saved", "path": "chord/common.j2",
                                   "created": True, "reloaded": []}
        assert (library / "chord" / "common.j2").read_text() == "log_level: WARN\n"

        resp = client.put("/api/configs/chord/telescope.j2",
                          json={"content": "telescope: {name: api}\n"})
        assert resp.get_json()["reloaded"] == ["cx/cx1"]
        assert resp.get_json()["created"] is False
        assert app.config["registry"].get_node("cx/cx1").rendered_config[
            "telescope"] == {"name": "api"}
        assert polled == ["cx/cx1"]

        resp = client.put("/api/configs/chord/telescope.j2",
                          json={"content": "{% include 'absent.j2' %}"})
        assert resp.status_code == 400 and "cx/cx1" in resp.get_json()["error"]
        assert (library / "chord" / "telescope.j2").read_text() == \
            "telescope: {name: api}\n"

        assert client.put("/api/configs/chord/x.j2", json={}).status_code == 400
        assert client.put("/api/configs/../x.j2",
                          json={"content": "a: 1"}).status_code in (400, 404)
        assert client.put("/api/configs/.updatable/cx/cx1.json",
                          json={"content": "{}"}).status_code == 400

    def test_set_config_through_update(self, client, app, library):
        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/update/cx/cx2",
                               json={"action": "set_config",
                                     "config": "chord/pathfinder.j2"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "reloaded" and body["node"] == "cx/cx2"
        assert body["maintenance"] is True
        registry = app.config["registry"]
        assert registry.get_node("cx/cx2").config_filename == "chord/pathfinder.j2"
        assert all(n.maintenance for n in registry.nodes.values())

        with patch.object(Node, "get_status", return_value=NodeStatus.IDLE):
            resp = client.post("/update/cx", json={"action": "set_config",
                                                   "config": None})
        assert resp.status_code == 200 and resp.get_json()["group"] == "cx"
        assert registry.get_node("cx/cx1").explicit_config is None
        assert registry.get_node("cx/cx2").explicit_config is None

        resp = client.post("/update/cx/cx1", json={"action": "set_config",
                                                   "config": 42})
        assert resp.status_code == 400
        resp = client.post("/update/cx/cx1", json={"action": "set_config",
                                                   "config": "chord/absent.j2"})
        assert resp.status_code == 404

    def test_node_config_endpoint(self, client, app, library):
        assert client.get("/api/config/cx/cx1").get_json() == {
            "num_elements": 128, "telescope": {"name": "a"}}
        assert client.get("/api/config/cx/cx2").get_json() == {"num_elements": 2048}
        assert client.get("/api/config/cx/nope").status_code == 404
        (library / "chord" / "telescope.j2").unlink()
        app.config["registry"].get_node("cx/cx1").load_config()
        resp = client.get("/api/config/cx/cx1")
        assert resp.status_code == 503 and "telescope.j2" in resp.get_json()["error"]

    def test_nodes_api_carries_config(self, client, library):
        nodes = client.get("/api/nodes").get_json()["groups"]["cx"]
        assert {n["name"]: n["config"] for n in nodes} == {
            "cx1": "chord/pathfinder.j2", "cx2": "cx/cx2.yaml"}
        status = client.get("/api/nodes/status").get_json()["nodes"]
        assert any(n["config"] == "chord/pathfinder.j2" for n in status)


# --- the config library's upstream pull ----------------------------------

class TestConfigPull:
    """``Pull chord from GitHub`` on /configs and ``POST /api/configs/pull``:
    GitHub is answered by ``responses`` (no network)."""

    API = "https://api.github.com"
    RAW = "https://raw.githubusercontent.com"
    COMMIT = "b29e72d4e" + "f" * 31

    @pytest.fixture
    def library(self, configs_dir, app):
        """cx1 renders chord/pathfinder.j2 (includes telescope.j2), cx2
        renders chord/inuse.j2; chord/old.j2 is used by nobody."""
        chord = configs_dir / "chord"
        chord.mkdir()
        (chord / "pathfinder.j2").write_text(
            'num_elements: 128\n{% include "telescope.j2" %}\n')
        (chord / "telescope.j2").write_text("telescope: {name: a}\n")
        (chord / "inuse.j2").write_text("inuse: 1\n")
        (chord / "old.j2").write_text("old: 1\n")
        data = yaml.safe_load((configs_dir / "nodes.yaml").read_text())
        data["groups"]["cx"]["cx1"]["config"] = "chord/pathfinder.j2"
        data["groups"]["cx"]["cx2"]["config"] = "chord/inuse.j2"
        (configs_dir / "nodes.yaml").write_text(yaml.safe_dump(data))
        app.config["registry"].reload()
        for node in app.config["registry"].nodes.values():
            node.maintenance = False     # running fleet: the pull must pause
        orch = app.config["orchestrator"]
        orch._file_mtimes = orch._config_file_mtimes()  # scan baseline
        return configs_dir

    def _github(self, files: dict, status=200, message=None):
        """Register GitHub's replies for a listing of *files* (name ->
        bytes) at COMMIT, every file fetchable at the raw URL."""
        from choco.upstream import git_blob_sha
        if status != 200:
            responses.get(f"{self.API}/repos/kotekan/kotekan/commits/chord",
                          status=status, json={"message": message})
            return
        responses.get(f"{self.API}/repos/kotekan/kotekan/commits/chord",
                      json={"sha": self.COMMIT})
        listing = [{"name": n, "type": "file", "sha": git_blob_sha(d),
                    "size": len(d), "download_url": f"https://never.example/{n}"}
                   for n, d in files.items()]
        responses.get(f"{self.API}/repos/kotekan/kotekan/contents/config/chord"
                      f"?ref={self.COMMIT}", json=listing)
        for n, d in files.items():
            responses.get(f"{self.RAW}/kotekan/kotekan/{self.COMMIT}"
                          f"/config/chord/{n}", body=d)

    UPSTREAM = {
        # Changed: pathfinder.j2 now includes common.j2, which is new --
        # a set that only renders as a whole.
        "pathfinder.j2": b'num_elements: 128\n{% include "telescope.j2" %}\n'
                         b'{% include "common.j2" %}\n',
        "telescope.j2": b"telescope: {name: a}\n",         # unchanged
        "common.j2": b"num_dishes: 16\n",                   # added
        "README.md": b"# chord\n",                          # skipped
        # old.j2 and inuse.j2 are gone upstream.
    }

    def test_button_follows_the_upstream_block(self, client, app, library):
        login(client)
        body = client.get("/configs").get_data(as_text=True)
        assert 'action="/configs/pull"' in body
        assert ">Pull chord from GitHub</button>" in body
        assert "kotekan/kotekan" in body and "config/chord" in body
        app.config["upstream"] = None
        body = client.get("/configs").get_data(as_text=True)
        assert "/configs/pull" not in body and "Pull" not in body

    @responses.activate
    def test_pull_applies_the_set_and_reports_it(self, client, app, library, caplog):
        self._github(self.UPSTREAM)
        login(client)
        token = csrf(client)
        orch = app.config["orchestrator"]
        polled = []
        orch.submit_node = lambda key, item: polled.append((key, item.type))
        with caplog.at_level(logging.WARNING, logger="choco.web"):
            resp = client.post("/configs/pull", data={"_csrf_token": token},
                               follow_redirects=True)
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        assert "Pulled kotekan/kotekan@chord:config/chord @ b29e72d4ef into chord/" in body
        assert "added common.j2; changed pathfinder.j2; removed old.j2" in body
        assert ("re-rendered and paused cx/cx1; lift maintenance to restart "
                "them onto it") in body
        assert "kept chord/inuse.j2 (removed upstream, used by cx/cx2)" in body
        assert "skipped README.md (not a config file)" in body
        chord = library / "chord"
        assert (chord / "common.j2").read_bytes() == b"num_dishes: 16\n"
        assert (chord / "pathfinder.j2").read_bytes() == self.UPSTREAM["pathfinder.j2"]
        assert not (chord / "old.j2").exists()
        assert (chord / "inuse.j2").read_text() == "inuse: 1\n"
        assert not list(chord.glob("*.tmp"))
        # The sync loop heard of it: cx1 re-rendered onto the new set and
        # was woken; cx2's file did not change.
        cx1 = app.config["registry"].get_node("cx/cx1")
        assert cx1.rendered_config == {"num_elements": 128,
                                       "telescope": {"name": "a"}, "num_dishes": 16}
        assert ("cx/cx1", ChangeType.POLL) in polled
        assert all(key == "cx/cx1" for key, _ in polled)
        # Paused, so the sync loop pushes the new set only when the
        # operator lifts maintenance; cx2's file did not change.
        assert cx1.maintenance is True
        assert app.config["registry"].get_node("cx/cx2").maintenance is False
        assert app.config["registry"].get_node("recv/recv1").maintenance is False
        # Re-baselined, so the scan does not report the pull as an edit.
        assert orch._file_mtimes == orch._config_file_mtimes()
        audit = [r.getMessage() for r in caplog.records if "pulled" in r.getMessage()]
        assert len(audit) == 1 and "by tester" in audit[0]
        assert "chord/common.j2" in audit[0] and "chord/old.j2" in audit[0]
        assert "paused ['cx/cx1']" in audit[0]
        # The listing's download_url was never followed.
        assert not any("never.example" in c.request.url for c in responses.calls)

    @responses.activate
    def test_up_to_date_writes_nothing(self, client, app, library):
        chord = library / "chord"
        self._github({"pathfinder.j2": (chord / "pathfinder.j2").read_bytes(),
                      "telescope.j2": (chord / "telescope.j2").read_bytes(),
                      "inuse.j2": b"inuse: 1\n", "old.j2": b"old: 1\n"})
        login(client)
        before = {p: p.stat().st_mtime_ns for p in chord.iterdir()}
        resp = client.post("/configs/pull", data={"_csrf_token": csrf(client)},
                           follow_redirects=True)
        body = resp.get_data(as_text=True)
        assert "chord/ is up to date with kotekan/kotekan@chord:config/chord @ b29e72d4ef (4 files)" in body
        assert {p: p.stat().st_mtime_ns for p in chord.iterdir()} == before
        # Only the listing was needed: no file was downloaded.
        assert len(responses.calls) == 2

    @responses.activate
    def test_refused_when_a_node_would_not_render_and_nothing_is_written(
            self, client, app, library):
        files = dict(self.UPSTREAM)
        files["pathfinder.j2"] = b'num_elements: 128\n{% include "absent.j2" %}\n'
        self._github(files)
        login(client)
        chord = library / "chord"
        before = {p.name: p.read_bytes() for p in chord.iterdir()}
        resp = client.post("/configs/pull", data={"_csrf_token": csrf(client)},
                           follow_redirects=True)
        body = resp.get_data(as_text=True)
        assert "Not pulled: cx/cx1 would not render" in body
        assert "absent.j2" in body
        assert {p.name: p.read_bytes() for p in chord.iterdir()} == before

    @responses.activate
    def test_github_error_is_reported_not_raised(self, client, library):
        self._github({}, status=403, message="API rate limit exceeded for 1.2.3.4.")
        login(client)
        resp = client.post("/configs/pull", data={"_csrf_token": csrf(client)},
                           follow_redirects=True)
        body = resp.get_data(as_text=True)
        assert "Not pulled: GitHub: HTTP 403" in body
        assert "API rate limit exceeded" in body

    @responses.activate
    def test_api_dry_run_then_pull(self, client, app, library):
        self._github(self.UPSTREAM)
        chord = library / "chord"
        resp = client.post("/api/configs/pull", json={"dry_run": True})
        assert resp.status_code == 200
        plan = resp.get_json()
        assert plan["status"] == "planned" and plan["dry_run"] is True
        assert plan["commit"] == self.COMMIT
        assert plan["added"] == ["chord/common.j2"]
        assert plan["changed"] == ["chord/pathfinder.j2"]
        assert plan["removed"] == ["chord/old.j2"]
        assert plan["kept"] == [{"path": "chord/inuse.j2", "used_by": ["cx/cx2"]}]
        assert plan["skipped"] == [{"name": "README.md", "reason": "not a config file"}]
        assert plan["unchanged"] == 1 and plan["reloaded"] == ["cx/cx1"]
        assert plan["paused"] == ["cx/cx1"]
        assert plan["summary"].startswith("Would pull ")
        assert "would re-render and pause cx/cx1" in plan["summary"]
        assert "lift maintenance" not in plan["summary"]
        assert not (chord / "common.j2").exists() and (chord / "old.j2").exists()
        cx1 = app.config["registry"].get_node("cx/cx1")
        assert cx1.maintenance is False      # a dry run pauses nothing

        resp = client.post("/api/configs/pull")
        assert resp.status_code == 200
        done = resp.get_json()
        assert done["status"] == "pulled" and done["dry_run"] is False
        assert (chord / "common.j2").exists() and not (chord / "old.j2").exists()
        assert done["summary"].startswith("Pulled ")
        assert done["paused"] == ["cx/cx1"] and cx1.maintenance is True

    @responses.activate
    def test_nodes_already_paused_stay_so_and_are_said_to_be(self, client, app, library):
        self._github(self.UPSTREAM)
        cx1 = app.config["registry"].get_node("cx/cx1")
        cx1.maintenance = True
        done = client.post("/api/configs/pull").get_json()
        assert done["status"] == "pulled"
        assert done["reloaded"] == ["cx/cx1"] and done["paused"] == []
        assert ("re-rendered cx/cx1 (already in maintenance); lift maintenance "
                "to restart them onto it") in done["summary"]
        assert cx1.maintenance is True

    @responses.activate
    def test_api_refusal_is_409_with_the_nodes_error(self, client, library):
        files = dict(self.UPSTREAM)
        files["common.j2"] = b"{% if %}\n"
        self._github(files)
        resp = client.post("/api/configs/pull")
        assert resp.status_code == 409
        # The new include is parsed on its own before cx1 renders the set.
        error = resp.get_json()["error"]
        assert error.startswith("chord/common.j2: Template syntax:")
        assert not (library / "chord" / "common.j2").exists()

    def test_api_without_an_upstream_is_404(self, client, app, library):
        app.config["upstream"] = None
        resp = client.post("/api/configs/pull")
        assert resp.status_code == 404
        assert "upstream" in resp.get_json()["error"]

    @responses.activate
    def test_api_github_error_is_502(self, client, library):
        self._github({}, status=500, message="boom")
        resp = client.post("/api/configs/pull")
        assert resp.status_code == 502
        assert resp.get_json()["error"].startswith("GitHub: HTTP 500")
