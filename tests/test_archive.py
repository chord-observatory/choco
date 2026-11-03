"""Tests for the archive monitor: the listing poll, its join with the
scan, and the /files table that shows both."""

import os
import re

import pytest
import yaml
from gevent.server import StreamServer

from choco.app import create_app
from choco.archive import (ACQ_NAME_RE, MERGED_NAME_RE, ArchiveMonitor, compare_dir,
                           join_root, parse_listing)
from tests.helpers import login

ACQ_A = "acq_20260414_213532_313167493"
ACQ_B = "acq_20260911_232919_986789057"
ACQ_C = "acq_20261030_211235_770745271"
ACQ_GONE = "acq_20260417_024809_098082229"       # on the archive only
MERGED = "20261029T170000Z_merged"
MERGED_GONE = "20261030T105920Z_merged"


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


def listing(*entries):
    """A ``list -l`` answer from ``(dir, path, bytes, mtime)`` tuples."""
    return "".join(f"{d}/{p} {b} {m}\n" for d, p, b, m in entries)


class TestParseListing:
    def test_lines(self):
        dirs = parse_listing(listing((ACQ_A, "vis_0.h5", 100, 1000),
                                     (ACQ_A, ".partial/baseband_gains.h5", 5, 1),
                                     (MERGED, "merge.log", 7, 2)) + "\n")
        assert dirs == {ACQ_A: {"vis_0.h5": (100, 1000), ".partial/baseband_gains.h5": (5, 1)},
                        MERGED: {"merge.log": (7, 2)}}

    def test_empty_is_an_empty_archive(self):
        assert parse_listing("") == {} and parse_listing("\n") == {}

    def test_error_line(self):
        with pytest.raises(ValueError, match="transfer host says: ls on fir exited 255"):
            parse_listing("error ls on fir exited 255: no route\n")

    @pytest.mark.parametrize("text", [f"{ACQ_A} 5 0\n", f"{ACQ_A}/x y 2\n", f"{ACQ_A}/ 1 2\n",
                                      "nope/x 1 2\n", f"{ACQ_A}/x 1\n"])
    def test_bad_shapes(self, text):
        with pytest.raises(ValueError, match="unexpected answer line"):
            parse_listing(text)


class TestCompareAndJoin:
    def test_compare_counts_top_level_h5_and_the_files_held(self):
        there = {"vis_0.h5": (100, 1000), "vis_1.h5": (101, 2000), "README.md": (9, 3000),
                 ".partial/baseband_gains.h5": (5, 1), ".rsync-partial/vis_2.h5": (50, 4000)}
        here = {"vis_0.h5": (100, 1000), "vis_1.h5": (999, 2000), "vis_2.h5": (102, 2500)}
        a = compare_dir(there, here)
        assert (a["files"], a["bytes"], a["oldest"], a["newest"]) == (2, 201, 1000, 2000)
        assert a["held"] == 1                    # vis_1 differs in size, vis_2 is not there
        assert compare_dir(None, here)["held"] == 0
        assert compare_dir(there, None)["held"] == 0

    def test_join_rows_from_both_sides(self):
        root = {"dirs": [
            {"name": ACQ_B, "path": "/r/b", "files": 1, "bytes": 1, "oldest": 5000, "newest": 5000,
             "notes": False, "error": None, "h5": {"vis_0.h5": (1, 5000)}},
            {"name": ACQ_A, "path": "/r/a", "files": 2, "bytes": 2, "oldest": 1000, "newest": 2000,
             "notes": True, "error": None, "h5": {"vis_0.h5": (1, 1000), "vis_1.h5": (1, 2000)}},
            {"name": ACQ_C, "path": "/r/c", "files": 0, "bytes": 0, "oldest": None, "newest": None,
             "notes": False, "error": None, "h5": {}},
        ]}
        dirs = {ACQ_A: {"vis_0.h5": (1, 1000), "vis_1.h5": (1, 2000)},
                ACQ_GONE: {"vis_0.h5": (10, 3000), "vis_1.h5": (10, 3500), "README.md": (1, 1)},
                MERGED: {"baseband_subset.00000000.h5": (3, 9000)}}
        rows, summary = join_root(root, dirs, ACQ_NAME_RE)
        assert [r["name"] for r in rows] == [ACQ_B, ACQ_GONE, ACQ_A, ACQ_C]   # newest first
        assert summary == {"full": 1, "part": 0, "none": 1, "archive_only": 1}
        gone = rows[1]
        assert gone["on_site"] is False and gone["path"] is None
        assert (gone["files"], gone["bytes"], gone["oldest"], gone["newest"]) == (2, 20, 3000, 3500)
        assert rows[2]["on_site"] is True and rows[2]["archive"]["held"] == 2
        assert rows[0]["archive"]["held"] == 0 and rows[0]["notes"] is False
        # The scan's own rows are not touched.
        assert "archive" not in root["dirs"][0]
        # The merged dump belongs to the baseband table.
        rows, summary = join_root({"dirs": []}, dirs, MERGED_NAME_RE)
        assert [r["name"] for r in rows] == [MERGED] and summary["archive_only"] == 1


class TestArchiveMonitor:
    def test_unconfigured_without_every_part(self):
        assert not ArchiveMonitor().configured
        assert not ArchiveMonitor(host="h", port=1).configured
        assert ArchiveMonitor(host="h", port=1, root="/r").configured
        assert ArchiveMonitor(host="h", port=1, root="/r", interval_s=60).INTERVAL_S == 60

    def test_covers_ignores_trailing_slash(self):
        monitor = ArchiveMonitor(host="h", port=1, root="/data/subset/")
        assert monitor.covers("/data/subset")
        assert not monitor.covers("/data/full")

    def test_poll_asks_for_the_listing(self, verify_socket):
        verify_socket["reply"] = listing((ACQ_A, "vis_0.h5", 100, 1000))
        monitor = ArchiveMonitor("127.0.0.1", verify_socket["port"], "/r", timeout_s=5)
        monitor.poll_once()
        assert verify_socket["seen"] == ["list -l\n"]
        assert monitor.health == "ok" and monitor.error is None
        assert monitor.dirs == {ACQ_A: {"vis_0.h5": (100, 1000)}}
        assert monitor.checked_at == monitor.last_polled > 0
        assert monitor.to_dict()["dirs"] == 1

    def test_failure_keeps_the_last_listing(self, verify_socket):
        verify_socket["reply"] = listing((ACQ_A, "vis_0.h5", 100, 1000))
        monitor = ArchiveMonitor("127.0.0.1", verify_socket["port"], "/r", timeout_s=5)
        monitor.poll_once()
        taken = monitor.checked_at
        verify_socket["reply"] = "error ls on fir exited 255: ssh: connect timed out\n"
        monitor.poll_once()
        assert monitor.health == "down"
        assert "transfer host says" in monitor.error
        assert monitor.dirs == {ACQ_A: {"vis_0.h5": (100, 1000)}}
        assert monitor.checked_at == taken < monitor.last_polled

    def test_refused_connection(self):
        monitor = ArchiveMonitor("127.0.0.1", 1, "/r", timeout_s=5)
        monitor.poll_once()
        assert monitor.health == "down" and monitor.error.startswith("127.0.0.1:1:")
        assert monitor.checked_at is None


# --- the /files table ---

@pytest.fixture
def roots(tmp_path):
    full = tmp_path / "full"
    subset = tmp_path / "subset"
    for d, n in ((full / ACQ_A, 3), (subset / ACQ_A, 1), (subset / ACQ_B, 2),
                 (subset / ACQ_C, 0)):
        d.mkdir(parents=True)
        for i in range(n):
            f = d / f"vis_{i}.h5"
            f.write_bytes(b"x" * 100)
            os.utime(f, (1000 + i, 1000 + i))
    (subset / "timeline.yaml").write_text("[]\n")
    return [full, subset]


@pytest.fixture
def baseband(tmp_path):
    root = tmp_path / "baseband"
    merged = root / MERGED
    merged.mkdir(parents=True)
    f = merged / "baseband_subset.00000000.h5"
    f.write_bytes(b"b" * 200)
    os.utime(f, (9000, 9000))
    (merged / "merge.log").write_text(
        "=== 2026-10-29T23:57:44Z start on corrcal\nscanning 1536 files\n"
        "   50.0%  60 GiB written\nmerged: 15728640 samples x 6144 channels\n"
        "=== 2026-10-30T08:18:24Z all parts written\n")
    return root


@pytest.fixture
def configs_dir(tmp_path):
    nodes = {"groups": {"cx": {"cx1": {"host": "cx1.example", "port": 12048}}}}
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "configs" / "cx").mkdir()
    (tmp_path / "configs" / "cx" / "cx1.yaml").write_text("num_elements: 2\n")
    return tmp_path / "configs"


def _app(configs_dir, roots, archive=None, baseband=None):
    from choco.app import _DEFAULT_CONFIG
    config = dict(_DEFAULT_CONFIG)
    config["vis_files"] = {"roots": [str(r) for r in roots], "ttl": 30}
    if archive:
        config["vis_files"]["archive"] = archive
    if baseband:
        config["vis_files"]["baseband"] = str(baseband)
    app = create_app(configs_dir=configs_dir, config=config)
    app.config["TESTING"] = True
    app.config["state_dir"] = configs_dir.parent / "state"
    return app


def _archive(verify_socket, roots):
    return {"host": "127.0.0.1", "port": verify_socket["port"],
            "root": str(roots[1]) + "/", "timeout": 5, "interval": 3600}


def _polled(app):
    """The listing as it stands: one explicit poll (the background
    greenlet asks the same question whenever it gets its turn)."""
    monitor = app.config["archive_monitor"]
    monitor.poll_once()
    return monitor


SUBSET_LISTING = listing(
    (ACQ_A, "vis_0.h5", 100, 1000), (ACQ_A, "README.md", 9, 1),
    (ACQ_B, "vis_0.h5", 100, 1000), (ACQ_B, "vis_1.h5", 100, 9999),       # vis_1 differs in mtime
    (ACQ_GONE, "vis_0.h5", 100, 3000), (ACQ_GONE, "vis_1.h5", 100, 3500),
    (MERGED, "baseband_subset.00000000.h5", 200, 9000), (MERGED, "merge.log", 1, 1),
    (MERGED_GONE, "baseband_subset.00000000.h5", 300, 9500),
)


class TestFilesTable:
    def test_column_only_for_the_archived_root(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = SUBSET_LISTING
        app = _app(configs_dir, roots, _archive(verify_socket, roots))
        _polled(app)
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert body.count(">Archive</th>") == 1
        assert all(line == "list -l\n" for line in verify_socket["seen"])
        assert "Check archive" not in body

    def test_no_column_without_archive(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert ">Archive</th>" not in body
        assert "the archive holds" not in body and "archive only" not in body

    def test_cells_from_the_listing(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = SUBSET_LISTING
        app = _app(configs_dir, roots, _archive(verify_socket, roots))
        _polled(app)
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert "the archive holds 1 of 2 in full, 1 in part; 1 only there" in body
        assert re.search(rf"{ACQ_A}.*?<small class=\"t-ok\"[^>]*>all 1<", body, re.S)
        assert re.search(rf"{ACQ_B}.*?<small class=\"t-warn\"[^>]*>1 of 2<", body, re.S)
        # The acquisition only the archive has: a row with its count and span there.
        m = re.search(rf"{ACQ_GONE}\s*<br><small class=\"muted\">archive only</small>.*?</tr>", body, re.S)
        assert m and 'data-label="files">2<' in m.group(0)
        assert "1970-01-01 00:50 &rarr; 01-01 00:58" in m.group(0)       # mtimes 3000..3500
        assert "archive only</small></td>" in m.group(0)                   # the Archive cell
        # The empty acquisition has nothing to hold; the other root no column.
        assert re.search(rf"{ACQ_C}.*?data-label=\"archive\"><small class=\"muted\">&mdash;<", body, re.S)
        assert body.count("archive only") == 2                              # name cell + Archive cell, once

    def test_none_held(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = listing((ACQ_B, "vis_0.h5", 1, 1))        # another size
        app = _app(configs_dir, roots, _archive(verify_socket, roots))
        _polled(app)
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert "the archive holds 0 of 2 in full, 2 not at all" in body
        assert re.search(rf"{ACQ_B}.*?<small class=\"t-bad\"[^>]*>none of 2<", body, re.S)

    def test_before_the_first_listing(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = "error ls on fir exited 255: no route\n"
        app = _app(configs_dir, roots, _archive(verify_socket, roots))
        _polled(app)
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert body.count("archive not listed yet") == 2       # the note and the root header
        assert body.count(">?</small>") == 3                    # every row of the archived root
        assert "transfer host says: ls on fir exited 255" in body

    def test_failed_poll_shows_the_last_listing(self, configs_dir, roots, verify_socket):
        verify_socket["reply"] = SUBSET_LISTING
        app = _app(configs_dir, roots, _archive(verify_socket, roots))
        monitor = _polled(app)
        verify_socket["reply"] = "error ls on fir exited 255: no route\n"
        monitor.poll_once()
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert "(the latest poll failed)" in body
        assert re.search(rf"{ACQ_A}.*?>all 1<", body, re.S)

    def test_old_archive_route_is_gone(self, configs_dir, roots, verify_socket):
        client = _app(configs_dir, roots, _archive(verify_socket, roots)).test_client()
        login(client)
        assert client.get("/partials/files/archive/1").status_code == 404


class TestBasebandTable:
    def test_dumps_at_the_end_with_the_archive(self, configs_dir, roots, baseband, verify_socket):
        verify_socket["reply"] = SUBSET_LISTING
        app = _app(configs_dir, roots, _archive(verify_socket, roots), baseband)
        _polled(app)
        client = app.test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        head = body.index(str(baseband))
        assert head > body.index(str(roots[1]))                 # after the acquisition roots
        tail = body[head:]
        assert re.search(r"1 merged dump ·\s+1 file ·\s+200 B", tail)
        assert "the archive holds 1 of 1 in full; 1 only there" in tail
        assert ">Dump</th>" in tail and ">Info</th>" in tail and ">Waterfalls</th>" not in tail
        assert re.search(rf"{MERGED}.*?<small class=\"t-ok\"[^>]*>all 1<", tail, re.S)
        assert re.search(rf"{MERGED_GONE}\s*<br><small class=\"muted\">archive only</small>", tail)
        assert f'hx-get="/files/baseband/{MERGED}"' in tail
        assert f'hx-get="/files/baseband/{MERGED_GONE}"' not in tail

    def test_dumps_without_an_archive(self, configs_dir, roots, baseband):
        client = _app(configs_dir, roots, baseband=baseband).test_client()
        login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert MERGED in body and ">Archive</th>" not in body

    def test_info_reads_the_log_and_the_part(self, configs_dir, roots, baseband):
        h5py = pytest.importorskip("h5py")
        np = pytest.importorskip("numpy")
        part = baseband / MERGED / "baseband_subset.00000000.h5"
        with h5py.File(part, "w") as f:
            d = f.create_dataset("baseband_subset", data=np.zeros((4, 3, 2, 2), np.uint8))
            d.attrs["coarse_freq"] = np.arange(1536, 1539, dtype=np.int32)
            d.attrs["fpga_seq_time_nsec"] = np.int64(1793293199959241010)
            d.attrs["seq_length_nsec"] = np.uint64(5120)
            d.attrs["num_dishes"] = np.uint64(2)
            d.attrs["num_polarizations"] = np.uint64(2)
            d.attrs["type"] = "int4x2_swapped_withoffset"
            d.attrs["dim_names"] = np.array(["T", "F", "P", "D"], dtype=object)
            d.attrs["merge_num_sources"] = np.int64(16)
            d.attrs["merge_tool"] = "merge_baseband_freq.py"
            f.attrs["digital_gains_source_file"] = "/scratch/x/.partial/baseband_gains_0.h5"
        client = _app(configs_dir, roots, baseband=baseband).test_client()
        login(client)
        client.get("/partials/files")
        r = client.get(f"/files/baseband/{MERGED}")
        assert r.status_code == 200
        body = r.get_data(as_text=True)
        assert "2026-10-29 16:59:59" in body                      # fpga_seq_time_nsec, 41 ms before the trigger
        assert "channels 1536–1538 (3)" in body and "300.0–300.6 MHz" in body
        assert "1 part of 0.000 s (4 samples of 5120 ns)" in body
        assert "2 dishes × 2 polarizations" in body
        assert "int4x2_swapped_withoffset; 4 × 3 × 2 × 2 (T, F, P, D), 48 B a part" in body
        assert "16 X-engine streams by merge_baseband_freq.py" in body
        assert "baseband_gains_0.h5" in body
        assert "=== 2026-10-29T23:57:44Z start on corrcal" in body
        assert "merged: 15728640 samples" in body and "60 GiB written" not in body

    def test_info_only_for_a_listed_dump(self, configs_dir, roots, baseband):
        client = _app(configs_dir, roots, baseband=baseband).test_client()
        login(client)
        client.get("/partials/files")
        assert client.get("/files/baseband/20261030T105920Z_merged").status_code == 404
        assert client.get("/files/baseband/../etc").status_code == 404

    def test_info_without_a_part_file(self, configs_dir, roots, baseband):
        os.remove(baseband / MERGED / "baseband_subset.00000000.h5")
        client = _app(configs_dir, roots, baseband=baseband).test_client()
        login(client)
        client.get("/partials/files")
        body = client.get(f"/files/baseband/{MERGED}").get_data(as_text=True)
        assert "No part file to read" in body and "start on corrcal" in body
