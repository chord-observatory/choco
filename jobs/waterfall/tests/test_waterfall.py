"""Tests for the job: what it picks up, in what order, and how it exits."""

import json
import os

import h5py
import numpy as np
import pytest
import yaml

import store as S
import waterfall as W
import wfpng
from test_reduce import make_file


def add_source(acq_dir, idx, **kw):
    """A completed source file, named the way kotekan names them."""
    acq_dir.mkdir(parents=True, exist_ok=True)
    p = acq_dir / f"vis_{idx:010d}_20260723T_000000_000000000.h5"
    make_file(p, **kw)
    with h5py.File(p, "r+") as f:
        f.attrs["abs_file_idx"] = idx
    return p


@pytest.fixture
def tree(tmp_path):
    """Two acquisitions, plus a .partial holding an in-progress file."""
    root = tmp_path / "subset"
    old = root / "acq_20260101_000000_000000000"
    new = root / "acq_20260202_000000_000000000"
    add_source(old, 100)
    add_source(old, 101)
    add_source(new, 200)
    partial = new / ".partial"
    partial.mkdir()
    make_file(partial / "vis_201.h5")
    return tmp_path, root


def write_cfg(tmp_path, root, **over):
    cfg = {
        "roots": [{"name": "subset", "path": str(root)}],
        "waterfalls_dir": str(tmp_path / "wf"),
        "state_file": str(tmp_path / "state.json"),
        "max_files_per_run": 40,
    }
    cfg.update(over)
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


# --- config --------------------------------------------------------------

def test_load_config_applies_defaults(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"roots": ["/data/subset"]}))
    cfg = W.load_config(p)
    assert cfg["level"] == wfpng.DEFAULT_LEVEL
    assert cfg["max_files_per_run"] == W.DEFAULTS["max_files_per_run"]
    assert cfg["roots"] == [{"path": "/data/subset", "name": "subset"}]


def test_load_config_accepts_named_roots(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"roots": [{"path": "/d/x", "name": "sub"}]}))
    assert W.load_config(p)["roots"] == [{"path": "/d/x", "name": "sub"}]


def test_load_config_requires_a_root(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"roots": []}))
    with pytest.raises(ValueError, match="no roots"):
        W.load_config(p)


def test_load_config_rejects_a_root_without_a_path(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"roots": [{"name": "x"}]}))
    with pytest.raises(ValueError, match="no path"):
        W.load_config(p)


def test_load_config_rejects_a_non_mapping(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(ValueError, match="must be a mapping"):
        W.load_config(p)


# --- discovery -----------------------------------------------------------

def test_acquisitions_are_listed_newest_first(tree):
    _, root = tree
    acqs = W.list_acquisitions(str(root))
    assert acqs[0].startswith("acq_20260202")
    assert acqs[1].startswith("acq_20260101")


def test_non_acquisition_directories_are_ignored(tree, tmp_path):
    _, root = tree
    (root / "scratch").mkdir()
    (root / "notes.txt").write_text("x")
    assert all(a.startswith("acq_") for a in W.list_acquisitions(str(root)))


def test_missing_root_is_degraded(tmp_path):
    with pytest.raises(W.Degraded):
        W.list_acquisitions(str(tmp_path / "nope"))


def test_partial_files_are_not_sources(tree):
    _, root = tree
    new = root / "acq_20260202_000000_000000000"
    files = W.source_files(str(new))
    assert len(files) == 1
    assert ".partial" not in files[0]


# --- running -------------------------------------------------------------

def test_run_renders_every_pending_file(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    rep = W.run(cfg)
    assert rep["files_rendered"] == 3
    assert rep["acquisitions_touched"] == 2
    assert not rep["degraded"] and not rep["errors"]

    wf = tmp_path / "wf" / "subset" / "acq_20260101_000000_000000000"
    img, _ = wfpng.read(wf / "e0000" / "wf_e0000xe0000.png")
    assert img.shape[0] == 12                 # two files x 6 time samples
    assert (wf / "e0000" / "th_e0000xe0000.png").exists()


def test_a_second_run_does_nothing(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    W.run(cfg)
    rep = W.run(cfg)
    assert rep["files_rendered"] == 0 and rep["backlog"] == 0


def test_a_new_file_is_appended_to_the_existing_images(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    W.run(cfg)
    add_source(root / "acq_20260202_000000_000000000", 201, seed=3)
    rep = W.run(cfg)
    assert rep["files_rendered"] == 1

    wf = tmp_path / "wf" / "subset" / "acq_20260202_000000_000000000"
    img, _ = wfpng.read(wf / "e0000" / "wf_e0000xe0000.png")
    assert img.shape[0] == 12


def test_the_newest_acquisition_gets_the_budget_first(tree, tmp_path):
    """Live data must never be starved by a backfill behind it."""
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root, max_files_per_run=1))
    rep = W.run(cfg)
    assert rep["files_rendered"] == 1
    assert rep["last_acquisition"].startswith("acq_20260202")
    assert (tmp_path / "wf" / "subset" / "acq_20260202_000000_000000000").exists()
    assert not (tmp_path / "wf" / "subset" / "acq_20260101_000000_000000000").exists()


def test_backlog_is_reported(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root, max_files_per_run=1))
    rep = W.run(cfg)
    assert rep["backlog"] == 2                # the two not yet done


def test_only_acq_limits_the_run(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    rep = W.run(cfg, only_acq="acq_20260101_000000_000000000")
    assert rep["files_rendered"] == 2
    assert rep["acquisitions_touched"] == 1


def test_a_shape_change_is_reported_not_fatal(tree, tmp_path):
    """A file that cannot join its acquisition costs that file only."""
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    W.run(cfg)
    add_source(root / "acq_20260202_000000_000000000", 202, n_freq=32)
    rep = W.run(cfg)
    assert rep["files_rendered"] == 0
    assert any("cannot be widened" in e for e in rep["errors"])


def test_a_file_without_an_index_is_skipped(tree, tmp_path):
    _, root = tree
    p = add_source(root / "acq_20260202_000000_000000000", 203)
    with h5py.File(p, "r+") as f:
        del f.attrs["abs_file_idx"]
    cfg = W.load_config(write_cfg(tmp_path, root))
    rep = W.run(cfg)
    assert any("no abs_file_idx" in e for e in rep["errors"])
    assert rep["files_rendered"] == 3         # the good ones still went


def test_an_unreadable_root_degrades_the_run(tmp_path):
    cfg = W.load_config(write_cfg(tmp_path, tmp_path / "gone"))
    rep = W.run(cfg)
    assert rep["degraded"] and rep["files_rendered"] == 0


def test_an_empty_acquisition_is_skipped(tmp_path):
    root = tmp_path / "subset"
    (root / "acq_20260101_000000_000000000").mkdir(parents=True)
    cfg = W.load_config(write_cfg(tmp_path, root))
    rep = W.run(cfg)
    assert rep["files_rendered"] == 0 and not rep["degraded"]


def test_a_broken_index_costs_one_acquisition(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root))
    W.run(cfg)
    bad = tmp_path / "wf" / "subset" / "acq_20260202_000000_000000000" / "index.json"
    bad.write_text("{oops")
    add_source(root / "acq_20260202_000000_000000000", 204)
    rep = W.run(cfg)
    assert any("unreadable index" in e for e in rep["errors"])
    assert not rep["degraded"]


# --- state and exit codes ------------------------------------------------

def test_state_file_records_the_run(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["files_rendered"] == 3
    assert state["roots"] == ["subset"]
    assert state["last_acquisition"].startswith("acq_")
    assert "updated" in state and "run_seconds" in state


def test_main_exits_1_on_a_bad_config(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"roots": []}))
    assert W.main(["-c", str(p)]) == 1


def test_main_exits_1_on_a_missing_config(tmp_path):
    assert W.main(["-c", str(tmp_path / "nope.yaml")]) == 1


def test_main_exits_2_when_a_root_is_unavailable(tmp_path):
    cfg_path = write_cfg(tmp_path, tmp_path / "gone")
    assert W.main(["-c", str(cfg_path)]) == 2


def test_main_exits_0_with_nothing_to_do(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    assert W.main(["-c", str(cfg_path)]) == 0


def test_dry_run_writes_nothing(tree, tmp_path, capsys):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path), "-n"]) == 0
    out = capsys.readouterr().out
    assert "3 files pending" in out
    assert not (tmp_path / "wf").exists()


def test_repalette_recolours_finished_acquisitions(tree, tmp_path):
    """A colormap change reaches images the timer will never touch again."""
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    state_before = (tmp_path / "state.json").read_text()

    images = sorted((tmp_path / "wf").rglob("wf_*.png"))
    assert images
    pixels_before = wfpng.read(images[0])[0]
    wfpng.set_palette(images[0], np.zeros((256, 3), np.uint8))

    assert W.main(["-c", str(cfg_path), "--repalette"]) == 0
    got, pal = wfpng.read(images[0])
    assert np.array_equal(got, pixels_before)          # pixels untouched
    assert pal.any()                                   # palette re-derived
    # a maintenance pass: the timer's state file is not overwritten
    assert (tmp_path / "state.json").read_text() == state_before


def test_cli_overrides_config(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root, max_files_per_run=40, level=9)
    assert W.main(["-c", str(cfg_path), "--max-files", "1", "--level", "1"]) == 0
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["files_rendered"] == 1


def test_state_file_failure_does_not_fail_the_run(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root,
                         state_file=str(tmp_path / "vis_a.h5" / "state.json"))
    (tmp_path / "vis_a.h5").write_text("not a directory")
    assert W.main(["-c", str(cfg_path)]) == 0


# --- not reopening what is already done ----------------------------------

class TestSkipsWork:
    @pytest.mark.parametrize("name,want", [
        ("vis_0004202414_20260723T_x.h5", 4202414),
        ("vis_7_x.h5", 7),
        ("vis_4214175.h5", None),          # no trailing separator
        ("other.h5", None),
        ("", None),
    ])
    def test_index_from_name(self, name, want):
        assert W.index_from_name(name) == want

    def test_settled_files_are_not_opened_again(self, tree, tmp_path, monkeypatch):
        """The archive is ~16k files and the timer runs every 2 minutes."""
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root))
        W.run(cfg)

        opened = []
        real = W.R.read_axes
        monkeypatch.setattr(W.R, "read_axes",
                            lambda p: (opened.append(p), real(p))[1])
        rep = W.run(cfg)
        assert rep["files_rendered"] == 0
        assert opened == []                # nothing reopened

    def test_a_new_file_is_still_opened(self, tree, tmp_path, monkeypatch):
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root))
        W.run(cfg)
        add_source(root / "acq_20260202_000000_000000000", 201)

        opened = []
        real = W.R.read_axes
        monkeypatch.setattr(W.R, "read_axes",
                            lambda p: (opened.append(p), real(p))[1])
        assert W.run(cfg)["files_rendered"] == 1
        assert len(opened) == 1


# --- permanently unusable files ------------------------------------------

class TestPermanentSkips:
    def _no_index(self, root):
        p = add_source(root / "acq_20260202_000000_000000000", 203)
        with h5py.File(p, "r+") as f:
            del f.attrs["abs_file_idx"]
        return p

    def test_recorded_once_and_not_repeated(self, tree, tmp_path):
        _, root = tree
        self._no_index(root)
        cfg = W.load_config(write_cfg(tmp_path, root))

        first = W.run(cfg)
        assert any("no abs_file_idx" in e for e in first["errors"])
        assert first["skipped"] == 1

        second = W.run(cfg)
        assert second["errors"] == []          # remembered, not re-reported
        assert second["skipped"] == 0
        assert second["backlog"] == 0          # and it stops inflating the backlog

    def test_a_permanent_skip_is_not_degraded(self, tree, tmp_path):
        """exit 2 means retries self-heal; these never will."""
        _, root = tree
        self._no_index(root)
        cfg_path = write_cfg(tmp_path, root)
        assert W.main(["-c", str(cfg_path)]) == 0
        assert W.main(["-c", str(cfg_path)]) == 0

    def test_a_shape_mismatch_is_remembered(self, tree, tmp_path):
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root))
        W.run(cfg)
        add_source(root / "acq_20260202_000000_000000000", 202, n_freq=32)
        assert any("cannot be widened" in e for e in W.run(cfg)["errors"])
        assert W.run(cfg)["errors"] == []

    def test_an_empty_frame_is_skipped_not_fatal(self, tree, tmp_path):
        """A zero-length axis used to reach the encoder and exit 1."""
        _, root = tree
        p = add_source(root / "acq_20260202_000000_000000000", 205)
        with h5py.File(p, "r+") as f:
            del f["vis"]
            f.create_dataset("vis", data=np.zeros((0, 10, 6), np.complex64))
        cfg_path = write_cfg(tmp_path, root)
        assert W.main(["-c", str(cfg_path)]) == 0
        state = json.loads((tmp_path / "state.json").read_text())
        assert any("empty frame" in e for e in state["errors"])


# --- degraded runs keep their accounting ---------------------------------

class TestDegradedAccounting:
    def test_a_mid_run_failure_still_spends_budget(self, tree, tmp_path, monkeypatch):
        """A flaky mount must not let one run render past the cap."""
        _, root = tree
        add_source(root / "acq_20260101_000000_000000000", 102)
        cfg = W.load_config(write_cfg(tmp_path, root, max_files_per_run=2))

        calls = {"n": 0}
        real = W.store_add if hasattr(W, "store_add") else None
        from store import AcquisitionStore
        real_add = AcquisitionStore.add_file

        def flaky(self, axes, indices, level=6):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("mount went away")
            return real_add(self, axes, indices, level=level)

        monkeypatch.setattr(AcquisitionStore, "add_file", flaky)
        rep = W.run(cfg)
        assert rep["degraded"]
        assert rep["files_rendered"] == 1          # the one that committed
        assert calls["n"] == 2                     # and it stopped there

    def test_degraded_exits_2(self, tmp_path):
        cfg_path = write_cfg(tmp_path, tmp_path / "gone")
        assert W.main(["-c", str(cfg_path)]) == 2


# --- one run at a time ---------------------------------------------------

class TestRunLock:
    def test_a_second_run_backs_off(self, tree, tmp_path):
        """Two processes appending to one image would corrupt it."""
        _, root = tree
        cfg_path = write_cfg(tmp_path, root)
        lock = tmp_path / "waterfall.lock"
        with W.single_run(lock) as held:
            assert held
            assert W.main(["-c", str(cfg_path)]) == 0
            assert not (tmp_path / "wf").exists()   # rendered nothing
        assert W.main(["-c", str(cfg_path)]) == 0
        assert (tmp_path / "wf").exists()

    def test_the_lock_is_released(self, tmp_path):
        lock = tmp_path / "x.lock"
        with W.single_run(lock) as a:
            assert a
        with W.single_run(lock) as b:
            assert b

    def test_an_unopenable_lock_does_not_stop_the_job(self, tmp_path):
        (tmp_path / "afile").write_text("x")
        with W.single_run(tmp_path / "afile" / "x.lock") as held:
            assert held


class TestBacklogIsCheap:
    def test_backlog_is_sized_without_opening_files(self, tree, tmp_path, monkeypatch):
        """Sizing the backlog by opening every pending file cost 454 s
        against the real 10,864-file archive; the filenames already say."""
        _, root = tree
        for i in range(6):
            add_source(root / "acq_20260202_000000_000000000", 300 + i)
        cfg = W.load_config(write_cfg(tmp_path, root, max_files_per_run=1))

        opened = []
        real = W.R.read_axes
        monkeypatch.setattr(W.R, "read_axes",
                            lambda p: (opened.append(p), real(p))[1])
        rep = W.run(cfg)
        assert rep["backlog"] == 8              # 9 pending, less the one rendered
        assert rep["files_rendered"] == 1
        assert len(opened) == 1                 # only the one it rendered

    def test_an_unnamed_file_is_still_picked_up(self, tree, tmp_path):
        """A file the regex cannot read must be opened, not ignored."""
        _, root = tree
        acq = root / "acq_20260202_000000_000000000"
        odd = acq / "strange_name.h5"
        make_file(odd)
        with h5py.File(odd, "r+") as f:
            f.attrs["abs_file_idx"] = 999
        cfg = W.load_config(write_cfg(tmp_path, root))
        rep = W.run(cfg)
        assert rep["files_rendered"] == 4       # the three named plus this one

    def test_a_done_file_the_name_hides_does_not_spend_budget(self, tree, tmp_path):
        _, root = tree
        acq = root / "acq_20260202_000000_000000000"
        odd = acq / "strange_name.h5"
        make_file(odd)
        with h5py.File(odd, "r+") as f:
            f.attrs["abs_file_idx"] = 999
        cfg = W.load_config(write_cfg(tmp_path, root))
        W.run(cfg)
        rep = W.run(cfg)
        assert rep["files_rendered"] == 0 and rep["backlog"] == 0
