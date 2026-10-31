"""Tests for the archive check: the verify-socket client and the /files partial."""

import re

import pytest
import yaml
from gevent.server import StreamServer

from choco.app import create_app
from choco.archive import ArchiveCheck, parse_reply
from tests.helpers import login

ACQ_A = "acq_20260414_213532_313167493"
ACQ_B = "acq_20260911_232919_986789057"
ACQ_C = "acq_20261030_211235_770745271"


@pytest.fixture
def verify_socket():
    """A fake kotekan-vis-verify socket: records each request line and
    answers with whatever ``reply`` holds at the time."""
    state = {"reply": "", "seen": []}

    def handle(sock, _addr):
        state["seen"].append(sock.makefile("rb").readline().decode())
        sock.sendall(state["reply"].encode())
        sock.close()

    server = StreamServer(("127.0.0.1", 0), handle)
    server.start()
    state["port"] = server.server_port
    yield state
    server.stop()


class TestParseReply:
    def test_lines(self):
        dirs = parse_reply(f"{ACQ_A} 5 0\n{ACQ_B} 5 2\n\n")
        assert dirs == {ACQ_A: {"files": 5, "missing": 0},
                        ACQ_B: {"files": 5, "missing": 2}}

    def test_error_line(self):
        with pytest.raises(ValueError, match="transfer host says: rsync exited 255"):
            parse_reply("error rsync exited 255: no route\n")

    @pytest.mark.parametrize("text", ["", "\n", f"{ACQ_A} 5\n", f"{ACQ_A} x 0\n",
                                      "nope 1 2\n"])
    def test_bad_shapes(self, text):
        with pytest.raises(ValueError):
            parse_reply(text)


class TestArchiveCheck:
    def test_unconfigured_without_every_part(self):
        assert not ArchiveCheck().configured
        assert not ArchiveCheck(host="h", port=1).configured
        assert ArchiveCheck(host="h", port=1, root="/r").configured

    def test_covers_ignores_trailing_slash(self):
        check = ArchiveCheck(host="h", port=1, root="/data/subset/")
        assert check.covers("/data/subset")
        assert not check.covers("/data/full")

    def test_query_asks_only_acquisition_names(self, verify_socket):
        verify_socket["reply"] = f"{ACQ_A} 5 0\n{ACQ_B} 5 2\n"
        check = ArchiveCheck("127.0.0.1", verify_socket["port"], "/r", timeout_s=5)
        result = check.query([ACQ_B, "timeline.yaml", "../etc", ACQ_A])
        assert verify_socket["seen"] == [f"{ACQ_B} {ACQ_A}\n"]
        assert result["ok"] and result["error"] is None
        assert result["dirs"][ACQ_B] == {"files": 5, "missing": 2}
        assert result["checked_at"] > 0

    def test_query_nothing_to_ask(self, verify_socket):
        check = ArchiveCheck("127.0.0.1", verify_socket["port"], "/r", timeout_s=5)
        result = check.query(["notes", "old"])
        assert result == {"ok": True, "error": None, "dirs": {},
                          "checked_at": result["checked_at"]}
        assert verify_socket["seen"] == []          # no connection made

    def test_query_error_line(self, verify_socket):
        verify_socket["reply"] = "error not an acquisition directory under /x: acq_1\n"
        check = ArchiveCheck("127.0.0.1", verify_socket["port"], "/r", timeout_s=5)
        result = check.query([ACQ_A])
        assert not result["ok"]
        assert "transfer host says" in result["error"]

    def test_query_refused(self, verify_socket):
        verify_socket["reply"] = f"{ACQ_A} 5 0\n"
        port = verify_socket["port"]
        check = ArchiveCheck("127.0.0.1", port, "/r", timeout_s=5)
        assert check.query([ACQ_A])["ok"]
        # The socket is gone: the failure names where we asked.
        dead = ArchiveCheck("127.0.0.1", 1, "/r", timeout_s=5)
        result = dead.query([ACQ_A])
        assert not result["ok"]
        assert result["error"].startswith("127.0.0.1:1:")


# --- the /files partials ---

@pytest.fixture
def roots(tmp_path):
    full = tmp_path / "full"
    subset = tmp_path / "subset"
    for d, n in ((full / ACQ_A, 3), (subset / ACQ_A, 1), (subset / ACQ_B, 2),
                 (subset / ACQ_C, 0)):
        d.mkdir(parents=True)
        for i in range(n):
            (d / f"vis_{i}.h5").write_bytes(b"x" * 100)
    (subset / "timeline.yaml").write_text("[]\n")
    return [full, subset]


@pytest.fixture
def configs_dir(tmp_path):
    nodes = {"groups": {"cx": {"cx1": {"host": "cx1.example", "port": 12048}}}}
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "configs" / "cx").mkdir()
    (tmp_path / "configs" / "cx" / "cx1.yaml").write_text("num_elements: 2\n")
    return tmp_path / "configs"


def _app(configs_dir, roots, archive=None):
    from choco.app import _DEFAULT_CONFIG
    config = dict(_DEFAULT_CONFIG)
    config["vis_files"] = {"roots": [str(r) for r in roots], "ttl": 30}
    if archive:
        config["vis_files"]["archive"] = archive
    app = create_app(configs_dir=configs_dir, config=config)
    app.config["TESTING"] = True
    app.config["state_dir"] = configs_dir.parent / "state"
    return app


def _archive(verify_socket, roots):
    return {"host": "127.0.0.1", "port": verify_socket["port"],
            "root": str(roots[1]) + "/", "timeout": 5}


class TestFilesPartials:
    def test_column_only_for_the_archived_root(self, configs_dir, roots, verify_socket):
        client = _app(configs_dir, roots, _archive(verify_socket, roots)).test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert body.count("<th") and body.count(">Archive</th>") == 1
        assert body.count("Check archive") == 1
        assert f'hx-get="/partials/files/archive/1"' in body
        assert f'id="archive-1-{ACQ_A}"' in body
        assert f'id="archive-0-{ACQ_A}"' not in body

    def test_no_column_without_archive(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert ">Archive</th>" not in body
        client.get("/partials/files")
        assert client.get("/partials/files/archive/1").status_code == 404

    def test_answer_fills_summary_and_cells(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = f"{ACQ_A} 3 0\n{ACQ_B} 4 1\n{ACQ_C} 2 2\n"
        client = _app(configs_dir, roots, _archive(verify_socket, roots)).test_client()
        login(client)
        client.get("/partials/files")                 # the scan the route allowlists from
        r = client.get("/partials/files/archive/1")
        assert r.status_code == 200
        body = r.get_data(as_text=True)
        # Asked about the scan's acquisition rows, not timeline.yaml.
        assert len(verify_socket["seen"]) == 1
        assert sorted(verify_socket["seen"][0].split()) == sorted([ACQ_A, ACQ_B, ACQ_C])
        assert "holds 1 of 3 acquisitions in full, 1 in part, 1 not at all" in body
        assert re.search(rf'id="archive-1-{ACQ_A}" hx-swap-oob="true">\s*<small class="t-ok"[^>]*>all 3<', body)
        assert re.search(rf'id="archive-1-{ACQ_B}" hx-swap-oob="true">\s*<small class="t-warn"[^>]*>3 of 4<', body)
        assert re.search(rf'id="archive-1-{ACQ_C}" hx-swap-oob="true">\s*<small class="t-bad"[^>]*>none of 2<', body)

    def test_failure_is_shown_not_raised(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = "error rsync exited 255: ssh: connect timed out\n"
        client = _app(configs_dir, roots, _archive(verify_socket, roots)).test_client()
        login(client)
        client.get("/partials/files")
        body = client.get("/partials/files/archive/1").get_data(as_text=True)
        assert "archive check failed: the transfer host says: rsync exited 255" in body
        assert body.count('hx-swap-oob="true"') == 3
        assert body.count(">?</small>") == 3

    def test_other_root_and_bad_index_are_404(self, configs_dir, roots, verify_socket):
        client = _app(configs_dir, roots, _archive(verify_socket, roots)).test_client()
        login(client)
        client.get("/partials/files")
        assert client.get("/partials/files/archive/0").status_code == 404
        assert client.get("/partials/files/archive/7").status_code == 404
        assert verify_socket["seen"] == []
