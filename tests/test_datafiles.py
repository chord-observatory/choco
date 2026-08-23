"""Tests for the data-file scan and the /files routes."""

import json
import os
import time

import pytest
import yaml

from choco.app import create_app, load_config
from choco.auth import save_user, _users
from choco.datafiles import (
    DataFileScan, human_bytes, probe_root, scan_dir, scan_root,
)


@pytest.fixture(autouse=True)
def clear_users():
    _users.clear()
    yield
    _users.clear()


@pytest.fixture
def roots(tmp_path):
    """Two roots shaped like the real ones: acq dirs holding .h5 files.

    ``full/acq_a`` also carries a ``.partial`` staging dir, which the
    one-level scan must not count.
    """
    full = tmp_path / "full"
    subset = tmp_path / "subset"
    for d, files in (
        (full / "acq_a", ["vis_0.h5", "vis_1.h5", "vis_2.h5"]),
        (full / "acq_b", []),
        (subset / "acq_a", ["vis_0.h5"]),
    ):
        d.mkdir(parents=True)
        for i, name in enumerate(files):
            (d / name).write_bytes(b"x" * (100 + i))
    partial = full / "acq_a" / ".partial"
    partial.mkdir()
    (partial / "vis_9.h5").write_bytes(b"y" * 4096)
    # A stray non-h5 file must not be counted either.
    (full / "acq_a" / "notes.txt").write_text("hello")
    return [full, subset]


@pytest.fixture
def configs_dir(tmp_path):
    nodes = {"groups": {"cx": {"cx1": {"host": "cx1.example", "port": 12048}}}}
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "nodes.yaml").write_text(yaml.safe_dump(nodes))
    (tmp_path / "configs" / "cx").mkdir()
    (tmp_path / "configs" / "cx" / "cx1.yaml").write_text("num_elements: 2\n")
    return tmp_path / "configs"


def _app(configs_dir, roots):
    from choco.app import _DEFAULT_CONFIG
    config = dict(_DEFAULT_CONFIG)
    config["vis_files"] = {"roots": [str(r) for r in roots], "ttl": 30}
    app = create_app(configs_dir=configs_dir, config=config)
    app.config["TESTING"] = True
    return app


def _login(client):
    user = save_user("cn=tester,dc=example", "tester")
    with client.session_transaction() as sess:
        sess["_user_id"] = user.get_id()


# --- the scan itself ---

class TestScan:
    def test_counts_only_h5_one_level_deep(self, roots):
        row = scan_dir(str(roots[0] / "acq_a"))
        # 3 .h5 directly inside; the .partial file and notes.txt excluded.
        assert row["files"] == 3
        assert row["bytes"] == 100 + 101 + 102
        assert row["error"] is None
        assert row["newest"] is not None

    def test_empty_dir(self, roots):
        row = scan_dir(str(roots[0] / "acq_b"))
        assert row == {"files": 0, "bytes": 0, "newest": None, "error": None}

    def test_missing_dir_reports_error(self, tmp_path):
        row = scan_dir(str(tmp_path / "nope"))
        assert row["files"] == 0
        assert "FileNotFoundError" in row["error"]

    def test_root_lists_subdirs(self, roots):
        root = scan_root(str(roots[0]))
        assert root["error"] is None
        assert {d["name"] for d in root["dirs"]} == {"acq_a", "acq_b"}
        assert root["files"] == 3
        assert root["bytes"] == 303

    def test_root_sorts_newest_first(self, roots):
        old = roots[1] / "acq_old"
        old.mkdir()
        (old / "vis.h5").write_bytes(b"z")
        os.utime(old / "vis.h5", (1000, 1000))
        root = scan_root(str(roots[1]))
        # acq_a's file was written now, acq_old's in 1970.
        assert [d["name"] for d in root["dirs"]] == ["acq_a", "acq_old"]

    def test_missing_root_is_not_fatal(self, tmp_path):
        root = scan_root(str(tmp_path / "gone"))
        assert root["dirs"] == []
        assert "FileNotFoundError" in root["error"]


class TestDataFileScan:
    def test_unconfigured(self):
        scan = DataFileScan([])
        assert not scan.configured
        assert scan.get()["configured"] is False

    def test_totals_across_roots(self, roots):
        result = DataFileScan(roots).get()
        assert result["configured"] is True
        assert result["dirs"] == 3          # acq_a, acq_b, acq_a
        assert result["files"] == 4
        assert result["bytes"] == 303 + 100

    def test_cached_until_stale(self, roots):
        scan = DataFileScan(roots, ttl_s=1000)
        first = scan.get()
        (roots[1] / "acq_a" / "vis_1.h5").write_bytes(b"x" * 50)
        assert scan.get()["files"] == first["files"]      # served from cache
        assert scan.get(force=True)["files"] == first["files"] + 1

    def test_stale_cache_refreshes(self, roots):
        scan = DataFileScan(roots, ttl_s=0)
        first = scan.get()
        (roots[1] / "acq_a" / "vis_1.h5").write_bytes(b"x" * 50)
        assert scan.get()["files"] == first["files"] + 1


def test_human_bytes():
    assert human_bytes(0) == "0 B"
    assert human_bytes(None) == "0 B"
    assert human_bytes(512) == "512 B"
    assert human_bytes(1536) == "1.5 KiB"
    assert human_bytes(3 * 1024 ** 4) == "3.0 TiB"


# --- routes ---

class TestFilesRoutes:
    def test_page_requires_login(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        resp = client.get("/files")
        assert resp.status_code == 302
        assert "/login" in resp.headers["Location"]

    def test_page_renders_lazily(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        _login(client)
        resp = client.get("/files")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # The page itself does not scan; it defers to the partial.
        assert "/partials/files" in body
        assert "acq_a" not in body

    def test_partial_lists_dirs_and_counts(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        _login(client)
        body = client.get("/partials/files").get_data(as_text=True)
        assert "acq_a" in body and "acq_b" in body
        assert "4 files" in body
        assert str(roots[0]) in body

    def test_unconfigured_page(self, configs_dir):
        client = _app(configs_dir, []).test_client()
        _login(client)
        body = client.get("/files").get_data(as_text=True)
        assert "No data roots configured" in body
        assert "/partials/files" not in body

    def test_api_returns_raw_numbers(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        _login(client)
        data = json.loads(client.get("/api/files").get_data())
        assert data["configured"] is True
        assert data["files"] == 4
        assert data["bytes"] == 403
        names = {d["name"] for r in data["roots"] for d in r["dirs"]}
        assert names == {"acq_a", "acq_b"}

    def test_api_unconfigured(self, configs_dir):
        client = _app(configs_dir, []).test_client()
        _login(client)
        data = json.loads(client.get("/api/files").get_data())
        assert data == {"configured": False, "roots": []}

    def test_api_refresh_bypasses_cache(self, configs_dir, roots):
        app = _app(configs_dir, roots)
        client = app.test_client()
        _login(client)
        first = json.loads(client.get("/api/files").get_data())["files"]
        (roots[1] / "acq_a" / "vis_1.h5").write_bytes(b"x")
        assert json.loads(client.get("/api/files").get_data())["files"] == first
        fresh = json.loads(client.get("/api/files?refresh=1").get_data())
        assert fresh["files"] == first + 1


def test_load_config_reads_vis_files(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.safe_dump({
        "vis_files": {"roots": ["/data/full", "/data/subset"], "ttl": 5},
    }))
    config = load_config(cfg)
    assert config["vis_files"]["roots"] == ["/data/full", "/data/subset"]
    assert config["vis_files"]["ttl"] == 5


def test_load_config_defaults_vis_files_empty(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("configs_dir: configs\n")
    assert load_config(cfg)["vis_files"] == {}


# --- the DATA badge ---

class TestProbeRoot:
    def test_live_root(self, roots):
        assert probe_root(str(roots[0])) == {
            "path": str(roots[0]), "ok": True, "error": None}

    def test_missing_root(self, tmp_path):
        r = probe_root(str(tmp_path / "gone"))
        assert r["ok"] is False and "FileNotFoundError" in r["error"]

    def test_file_is_not_a_root(self, tmp_path):
        f = tmp_path / "afile"
        f.write_text("x")
        r = probe_root(str(f))
        assert r["ok"] is False and r["error"] == "not a directory"

    def test_empty_dir_is_healthy(self, tmp_path):
        # No acquisitions yet is not the same as no filesystem.
        d = tmp_path / "empty"
        d.mkdir()
        assert probe_root(str(d))["ok"] is True


class TestHealth:
    def test_unconfigured(self):
        scan = DataFileScan([])
        assert scan.health == "unconfigured"
        scan.check_once()
        assert scan.health == "unconfigured"
        assert scan.to_dict()["health"] == "unconfigured"

    def test_all_roots_up_is_ok(self, roots):
        scan = DataFileScan(roots)
        scan.check_once()
        d = scan.to_dict()
        assert d["health"] == "ok"
        assert (d["n_ok"], d["n_roots"]) == (2, 2)
        assert d["error"] is None and d["last_seen"]

    def test_one_root_missing_is_degraded(self, roots, tmp_path):
        scan = DataFileScan([roots[0], tmp_path / "gone"])
        scan.check_once()
        assert scan.health == "degraded"
        assert "gone" in scan.error

    def test_all_roots_missing_is_down(self, tmp_path):
        scan = DataFileScan([tmp_path / "a", tmp_path / "b"])
        scan.check_once()
        assert scan.health == "down"
        assert scan.to_dict()["n_ok"] == 0

    def test_recovers(self, tmp_path):
        root = tmp_path / "later"
        scan = DataFileScan([root])
        scan.check_once()
        assert scan.health == "down"
        root.mkdir()
        scan.check_once()
        assert scan.health == "ok"

    def test_stuck_probe_reads_as_down(self, roots, monkeypatch):
        """A wedged mount must flip the badge, not hang the strip."""
        import choco.datafiles as df
        scan = DataFileScan(roots)
        scan.CHECK_TIMEOUT_S = 0.2
        monkeypatch.setattr(df, "probe_roots",
                            lambda paths: (time.sleep(2), [])[1])
        t = time.time()
        scan.check_once()
        assert scan.health == "down"
        assert "not responding" in scan.error
        assert time.time() - t < 1.5      # gave up, did not wait it out

    def test_stuck_probe_not_joined_twice(self, roots, monkeypatch):
        import choco.datafiles as df
        scan = DataFileScan(roots)
        scan.CHECK_TIMEOUT_S = 0.2
        monkeypatch.setattr(df, "probe_roots",
                            lambda paths: (time.sleep(2), [])[1])
        scan.check_once()
        t = time.time()
        scan.check_once()             # the thread is still stuck
        assert scan.health == "down"
        assert time.time() - t < 0.1  # answered immediately, no new wait

    def test_check_if_stale_skips_fresh(self, roots):
        scan = DataFileScan(roots)
        scan.check_once()
        first = scan.last_checked
        scan.check_if_stale(max_age_s=1000)
        assert scan.last_checked == first


class TestDataBadge:
    def test_pill_rendered_green_when_up(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        _login(client)
        body = client.get("/partials/services").get_data(as_text=True)
        assert "DATA" in body
        assert 'href="/files"' in body
        assert "#008000" in body        # monitor_color('ok')

    def test_pill_red_when_roots_gone(self, configs_dir, tmp_path):
        app = _app(configs_dir, [tmp_path / "gone"])
        client = app.test_client()
        _login(client)
        app.config["datafile_scan"].check_once()
        body = client.get("/partials/services").get_data(as_text=True)
        assert "DATA" in body and "#ff4136" in body    # monitor_color('down')
        assert "down" in body

    def test_unconfigured_pill_is_grey(self, configs_dir):
        client = _app(configs_dir, []).test_client()
        _login(client)
        body = client.get("/partials/services").get_data(as_text=True)
        assert "DATA" in body and "not configured" in body

    def test_dashboard_has_no_data_button(self, configs_dir, roots):
        client = _app(configs_dir, roots).test_client()
        _login(client)
        body = client.get("/").get_data(as_text=True)
        assert "Data files" not in body

    def test_api_status_carries_data_health(self, configs_dir, roots):
        app = _app(configs_dir, roots)
        client = app.test_client()
        _login(client)
        app.config["datafile_scan"].check_once()
        data = json.loads(client.get("/api/status").get_data())
        assert data["services"]["data"] == "ok"

    def test_metrics_exposes_data_states(self, configs_dir, roots):
        app = _app(configs_dir, roots)
        client = app.test_client()
        app.config["datafile_scan"].check_once()
        body = client.get("/metrics").get_data(as_text=True)
        assert 'choco_service_state{service="data",state="ok"} 1' in body
        assert 'choco_service_state{service="data",state="down"} 0' in body
