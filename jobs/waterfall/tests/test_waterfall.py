"""Tests for the job: what it picks up, in what order, and how it exits."""

import json
import os

import h5py
import numpy as np
import pytest
import yaml

import waterfall as W
import wfpng
from test_reduce import add_gains, make_file


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


def test_same_pol_autos_share_one_scale_and_palette(tree, tmp_path):
    _, root = tree
    cfg = W.load_config(write_cfg(tmp_path, root, products={"autos": True}))
    W.run(cfg)
    wf = tmp_path / "wf" / "subset" / "acq_20260101_000000_000000000"
    index = json.loads((wf / "index.json").read_text())
    names = {p["name"]: p for p in index["products"]}
    assert [[index["products"][p]["name"] for p in g] for g in index["scale_groups"]] == \
        [["e0000xe0000", "e0001xe0001"], ["e0002xe0002", "e0003xe0003"]]
    x0, x1, y0 = names["e0000xe0000"], names["e0001xe0001"], names["e0002xe0002"]
    assert (x0["lo"], x0["hi"]) == (x1["lo"], x1["hi"])
    for kind in ("wf", "th"):
        _, pal0 = wfpng.read(wf / "e0000" / f"{kind}_e0000xe0000.png")
        _, pal1 = wfpng.read(wf / "e0001" / f"{kind}_e0001xe0001.png")
        assert (pal0 == pal1).all()


def test_thumbnails_keep_up_file_by_file(tree, tmp_path, monkeypatch):
    _, root = tree
    calls = []
    orig = W.AcquisitionStore.update_thumbnails
    monkeypatch.setattr(W.AcquisitionStore, "update_thumbnails",
                        lambda self, **kw: calls.append(self.n_rows) or orig(self, **kw))
    W.run(W.load_config(write_cfg(tmp_path, root)))
    # a call after each of the three files, not one per acquisition at the end
    assert len(calls) == 3


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


def test_an_acquisition_with_gains_divides_them_out(tmp_path):
    root = tmp_path / "subset"
    acq = root / "acq_20260303_000000_000000000"
    gain = np.tile([2.0, 3.0, 5.0, 7.0], (512, 1))
    add_gains(add_source(acq, 300), gain)
    cfg = W.load_config(write_cfg(tmp_path, root))
    rep = W.run(cfg)
    assert rep["files_rendered"] == 1 and not rep["errors"]
    index = json.loads((tmp_path / "wf" / "subset" / acq.name / "index.json").read_text())
    assert index["gains"] == "divided"
    # the A0X auto (element 0, gain input 2) on the divided scale
    with h5py.File(acq / "vis_0000000300_20260723T_000000_000000000.h5") as f:
        auto = np.abs(f["vis"][:, 0, :])
    assert index["products"][0]["hi"] < auto.max() / 25 * 10
    # a later file without gains would sit on another scale: skipped
    add_source(acq, 301)
    rep = W.run(cfg)
    assert rep["files_rendered"] == 0
    assert any("divides the gains out; the file carries no digital gains" in e for e in rep["errors"])


def test_divide_gains_off_keeps_the_data_as_written(tmp_path):
    root = tmp_path / "subset"
    acq = root / "acq_20260303_000000_000000000"
    add_gains(add_source(acq, 300), np.tile([2.0, 3.0, 5.0, 7.0], (512, 1)))
    rep = W.run(W.load_config(write_cfg(tmp_path, root, divide_gains=False)))
    assert rep["files_rendered"] == 1
    assert "gains" not in json.loads((tmp_path / "wf" / "subset" / acq.name / "index.json").read_text())


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


def test_relabel_rewrites_labels_from_the_source(tree, tmp_path):
    """Labels are written once, at start; a better reading of the source
    reaches a finished acquisition only through --relabel, which touches
    nothing else."""
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    state_before = (tmp_path / "state.json").read_text()

    acq = "acq_20260202_000000_000000000"
    index_path = tmp_path / "wf" / "subset" / acq / "index.json"
    index = json.loads(index_path.read_text())
    good = index["labels"]
    assert good == ["A0X", "A1X", "A0Y", "A1Y"]
    # the pre-fix mislabelling: a table wider than the element axis,
    # indexed by position
    index["labels"] = ["A0X", "A1X", "D0X", "D1X", "A0Y", "A1Y", "D0Y", "D1Y"]
    index_path.write_text(json.dumps(index))
    images = sorted((tmp_path / "wf" / "subset" / acq).rglob("wf_*.png"))
    pixels_before = wfpng.read(images[0])[0]

    assert W.main(["-c", str(cfg_path), "--relabel", "--acq", acq]) == 0
    after = json.loads(index_path.read_text())
    assert after["labels"] == good
    assert after["files"] == index["files"]
    assert np.array_equal(wfpng.read(images[0])[0], pixels_before)
    # the other acquisition, and the timer's state file, were left alone
    other = tmp_path / "wf" / "subset" / "acq_20260101_000000_000000000" / "index.json"
    assert json.loads(other.read_text())["labels"] == good
    assert (tmp_path / "state.json").read_text() == state_before


def test_relabel_clears_inconsistent_labels_when_the_source_is_gone(tree, tmp_path):
    """No source to re-read and more labels than elements: the labels are
    the mislabelling itself, so indices are shown instead.  Consistent
    labels without a source stay."""
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    old, new = "acq_20260101_000000_000000000", "acq_20260202_000000_000000000"
    for acq in (old, new):
        for f in (root / acq).glob("*.h5"):
            f.unlink()
    old_index = tmp_path / "wf" / "subset" / old / "index.json"
    index = json.loads(old_index.read_text())
    index["labels"] = ["A0X", "A1X", "D0X", "D1X", "A0Y", "A1Y", "D0Y", "D1Y"]
    old_index.write_text(json.dumps(index))

    assert W.main(["-c", str(cfg_path), "--relabel"]) == 0
    assert json.loads(old_index.read_text())["labels"] == []
    new_index = tmp_path / "wf" / "subset" / new / "index.json"
    assert json.loads(new_index.read_text())["labels"] == ["A0X", "A1X", "A0Y", "A1Y"]


def test_relabel_keeps_consistent_labels_when_the_source_resolves_none(tree, tmp_path):
    """An acquisition labelled under an earlier file layout keeps its
    names: a re-read that yields no labels is not a better reading."""
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path)]) == 0
    acq = "acq_20260101_000000_000000000"
    for f in (root / acq).glob("*.h5"):
        with h5py.File(f, "r+") as h:           # the 2026-08 per-dish table
            del h["index_map/label"]
            del h["index_map/pol"]
            h["index_map"].create_dataset("label", data=np.array([b"A0", b"A1"]))
    index_path = tmp_path / "wf" / "subset" / acq / "index.json"
    assert W.main(["-c", str(cfg_path), "--relabel", "--acq", acq]) == 0
    assert json.loads(index_path.read_text())["labels"] == ["A0X", "A1X", "A0Y", "A1Y"]


def test_cli_overrides_config(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root, max_files_per_run=40, level=9)
    assert W.main(["-c", str(cfg_path), "--max-files", "1", "--level", "1"]) == 0
    state = json.loads((tmp_path / "state.json").read_text())
    assert state["files_rendered"] == 1


def test_state_file_failure_does_not_fail_the_run(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    (tmp_path / "vis_a.h5").write_text("not a directory")
    # --state-dir pointed at a file: no lock, no state.json, still renders
    assert W.main(["-c", str(cfg_path), "--state-dir", str(tmp_path / "vis_a.h5")]) == 0


@pytest.mark.parametrize("key", ["state_file", "lock_file"])
def test_retired_state_paths_are_refused(tree, tmp_path, key):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root, **{key: str(tmp_path / "x")})
    with pytest.raises(ValueError, match="retired"):
        W.load_config(cfg_path)
    assert W.main(["-c", str(cfg_path)]) == 1


def test_state_dir_override(tree, tmp_path):
    _, root = tree
    cfg_path = write_cfg(tmp_path, root)
    assert W.main(["-c", str(cfg_path), "--state-dir", str(tmp_path / "elsewhere")]) == 0
    assert (tmp_path / "elsewhere" / "state.json").exists()
    assert (tmp_path / "elsewhere" / "waterfall.lock").exists()
    assert not (tmp_path / "state.json").exists()


# --- not reopening what is already done ----------------------------------

class TestSkipsWork:
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


# --- a new acquisition whose first files are empty ----------------------

def zero_vis(p):
    """Turn a source file into one with no usable data (all visibilities 0)."""
    with h5py.File(p, "r+") as f:
        f["vis"][...] = 0


class TestEmptyFirstFiles:
    ACQ = "acq_20260303_000000_000000000"

    def test_the_scale_comes_from_the_first_usable_file(self, tmp_path):
        root = tmp_path / "subset"
        zero_vis(add_source(root / self.ACQ, 300))
        add_source(root / self.ACQ, 301)
        add_source(root / self.ACQ, 302)
        cfg = W.load_config(write_cfg(tmp_path, root))

        first = W.run(cfg)
        assert first["files_rendered"] == 2
        assert any("no product has usable data" in e for e in first["errors"])
        index = json.loads((tmp_path / "wf" / "subset" / self.ACQ / "index.json").read_text())
        assert index["files"] == [301, 302]
        assert list(index["skipped"]) == ["vis_0000000300_20260723T_000000_000000000.h5"]
        assert not (tmp_path / "wf" / "subset" / self.ACQ / "skipped.json").exists()

        second = W.run(cfg)
        assert second["errors"] == [] and second["backlog"] == 0

    def test_an_empty_acquisition_is_not_reread_forever(self, tmp_path, monkeypatch, capsys):
        root = tmp_path / "subset"
        for i in range(5):
            zero_vis(add_source(root / self.ACQ, 400 + i))
        cfg = W.load_config(write_cfg(tmp_path, root))
        sampled = []
        real = W.R.sample_magnitudes
        monkeypatch.setattr(W.R, "sample_magnitudes",
                            lambda p, *a, **k: sampled.append(os.path.basename(p)) or real(p, *a, **k))

        first = W.run(cfg)
        assert first["files_rendered"] == 0 and first["skipped"] == W.SCALE_TRIES
        out = tmp_path / "wf" / "subset" / self.ACQ
        assert not (out / "index.json").exists()      # nothing for choco to show
        assert len(json.loads((out / "skipped.json").read_text())) == W.SCALE_TRIES

        second = W.run(cfg)
        assert second["skipped"] == 5 - W.SCALE_TRIES  # the rest, not the same ones
        assert len(sampled) == 5 and len(set(sampled)) == 5
        assert W.run(cfg)["errors"] == []
        # the dry run agrees: skipped files are not pending
        assert W.main(["-c", str(write_cfg(tmp_path, root)), "-n"]) == 0
        assert "0 files pending" in capsys.readouterr().out

        # data arrives: the acquisition starts and adopts the earlier skips
        add_source(root / self.ACQ, 410)
        rep = W.run(cfg)
        assert rep["files_rendered"] == 1
        index = json.loads((out / "index.json").read_text())
        assert len(index["skipped"]) == 5 and index["files"] == [410]
        assert not (out / "skipped.json").exists()


# --- degraded runs keep their accounting ---------------------------------

class TestDegradedAccounting:
    def test_a_mid_run_failure_still_spends_budget(self, tree, tmp_path, monkeypatch):
        """A flaky mount must not let one run render past the cap."""
        _, root = tree
        add_source(root / "acq_20260101_000000_000000000", 102)
        cfg = W.load_config(write_cfg(tmp_path, root, max_files_per_run=2))

        calls = {"n": 0}
        from store import AcquisitionStore
        real_add = AcquisitionStore.add_file

        def flaky(self, axes, indices, level=6, overview=None, diag=None):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("mount went away")
            return real_add(self, axes, indices, level=level, overview=overview, diag=diag)

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


# --- what of a file is rendered ------------------------------------------

class TestSelection:
    """The products and channels an acquisition renders are fixed when it
    starts and recorded in its index; the config shapes only new ones."""

    def test_config_rule_shapes_a_new_acquisition(self, tree, tmp_path):
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root,
                                      products={"autos": True, "reference": ["A0X"]}, freq_stride=2))
        rep = W.run(cfg)
        assert rep["files_rendered"] == 3 and rep["errors"] == []
        for acq in ("acq_20260101_000000_000000000", "acq_20260202_000000_000000000"):
            out = tmp_path / "wf" / "subset" / acq
            idx = json.loads((out / "index.json").read_text())
            assert idx["n_prod"] == 5 and idx["n_freq"] == 32
            assert idx["n_prod_file"] == 10 and idx["n_freq_file"] == 64 and len(idx["freq_idx"]) == 32
            assert sorted(p.name for p in out.glob("e*/wf_*.png")) == [
                "wf_e0000xe0000.png", "wf_e0000xe0001.png", "wf_e0001xe0001.png",
                "wf_e0002xe0002.png", "wf_e0003xe0003.png"]
            assert len(np.load(out / "freq.npy")) == 32

    def test_a_started_acquisition_keeps_its_set(self, tree, tmp_path):
        _, root = tree
        W.run(W.load_config(write_cfg(tmp_path, root)))                  # every product, every channel
        cfg = W.load_config(write_cfg(tmp_path, root, products={"autos": True}, freq_stride=2))
        add_source(root / "acq_20260202_000000_000000000", 201)
        rep = W.run(cfg)
        assert rep["files_rendered"] == 1 and rep["errors"] == []
        idx = json.loads((tmp_path / "wf" / "subset" / "acq_20260202_000000_000000000" / "index.json").read_text())
        assert idx["n_prod"] == 10 and idx["n_freq"] == 64 and "freq_idx" not in idx
        assert idx["files"] == [200, 201]

    def test_an_uncommitted_index_restarts_under_the_current_rule(self, tree, tmp_path):
        """A run that died between start() and the first append left an
        index no pixel depends on; the next run may change its mind."""
        import reduce as R
        import store as S
        _, root = tree
        acq = root / "acq_20260202_000000_000000000"
        src = sorted(acq.glob("*.h5"))[0]
        axes = R.read_axes(src)
        lo, hi = R.scale_from_sample(R.sample_magnitudes(src))
        st = S.AcquisitionStore(tmp_path / "wf" / "subset" / acq.name, acq.name, "subset")
        st.start(axes, lo, hi)
        assert st.started and not st.committed and st.index["n_prod"] == 10
        cfg = W.load_config(write_cfg(tmp_path, root, products={"autos": True}))
        assert W.run(cfg)["files_rendered"] == 3
        idx = json.loads((tmp_path / "wf" / "subset" / acq.name / "index.json").read_text())
        assert idx["n_prod"] == 4 and idx["files"] == [200]

    def test_unknown_labels_are_reported_not_guessed(self, tree, tmp_path, caplog):
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root, products={"autos": False, "reference": ["Z9X", "A1Y"]}))
        rep = W.run(cfg)
        assert rep["files_rendered"] == 3
        assert "Z9X: not an element" in caplog.text
        idx = json.loads((tmp_path / "wf" / "subset" / "acq_20260202_000000_000000000" / "index.json").read_text())
        assert [(p["a"], p["b"]) for p in idx["products"]] == [(2, 3)]      # A0Y x A1Y

    def test_product_rule(self, tmp_path):
        assert W.product_rule(None) is None and W.product_rule("all") is None
        assert W.product_rule({"reference": ["A0X"]}) == {"autos": True, "reference": ["A0X"], "pairs": []}
        for bad in ("some", {"autos": False}, {"pairs": [["A0X"]]}, {"stride": 2}):
            with pytest.raises(ValueError):
                W.product_rule(bad)
        with pytest.raises(ValueError, match="freq_stride"):
            W.load_config(write_cfg(tmp_path, tmp_path / "subset", freq_stride=0))
        cfg = W.load_config(write_cfg(tmp_path, tmp_path / "subset"))
        assert cfg["products"] is None and cfg["freq_stride"] == 1


# --- the overview ----------------------------------------------------------

class TestOverview:
    ACQ = "acq_20260202_000000_000000000"

    def test_run_writes_the_cubes(self, tree, tmp_path):
        _, root = tree
        cfg = W.load_config(write_cfg(tmp_path, root, products={"autos": True}))
        rep = W.run(cfg)
        assert rep["files_rendered"] == 3 and rep["errors"] == []
        out = tmp_path / "wf" / "subset" / self.ACQ
        idx = json.loads((out / "index.json").read_text())
        # a 16-channel chunk in two columns, a file in two rows (overview_split)
        assert idx["overview"]["chunks"] == 8 and idx["overview"]["n_el"] == 4
        assert idx["overview"]["rows_per_file"] == 2 and idx["overview"]["array_full"]
        assert idx["overview"]["prod_order"] == "triangle" and len(idx["overview"]["edges"]) == 9
        assert idx["coarse_rows"] == 1
        assert (out / "coarse.dat").stat().st_size == 2 * 10 * 8 == idx["coarse_bytes"]
        assert (out / "e0003" / "co_e0003.png").exists() and idx["elements"][3]["rows"] == 2
        assert idx["array"]["rows"] == 2 and idx["array_full"]["rows"] == 6
        assert idx["array_full"]["width"] == 64 and (out / "co_array_full.png").exists()
        assert (out / "matrix.dat").stat().st_size == 8 * 16 == idx["matrix_bytes"]
        assert (out / "coarse_times.bin").stat().st_size == 16 and (out / "palette.bin").stat().st_size == 768
        assert len(np.load(out / "coarse_freq.npy")) == 8 and np.load(out / "coarse_scale.npy").shape == (10, 2)
        # a second file grows every cube by its rows, the matrix by a frame
        add_source(root / self.ACQ, 201)
        assert W.run(cfg)["files_rendered"] == 1
        idx = json.loads((out / "index.json").read_text())
        assert idx["coarse_rows"] == 2 and (out / "coarse.dat").stat().st_size == 320
        assert idx["elements"][3]["rows"] == 4 and idx["array_full"]["rows"] == 12
        assert (out / "matrix.dat").stat().st_size == 2 * 8 * 16

    def test_overview_split_one_keeps_a_cell_per_chunk_and_file(self, tree, tmp_path):
        _, root = tree
        W.run(W.load_config(write_cfg(tmp_path, root, products={"autos": True}, overview_split=1)))
        idx = json.loads((tmp_path / "wf" / "subset" / self.ACQ / "index.json").read_text())
        assert idx["overview"]["chunks"] == 4 and idx["overview"]["rows_per_file"] == 1
        assert idx["elements"][3]["rows"] == 1 and idx["coarse_bytes"] == 10 * 4

    def test_overview_off_keeps_none(self, tree, tmp_path):
        _, root = tree
        W.run(W.load_config(write_cfg(tmp_path, root, overview=False)))
        out = tmp_path / "wf" / "subset" / self.ACQ
        idx = json.loads((out / "index.json").read_text())
        assert "overview" not in idx and not (out / "coarse.dat").exists()
        # turning it on later does not add one to a started acquisition
        add_source(root / self.ACQ, 201)
        assert W.run(W.load_config(write_cfg(tmp_path, root)))["files_rendered"] == 1
        assert "overview" not in json.loads((out / "index.json").read_text())
        assert not (out / "coarse.dat").exists()

    def test_file_stride_folds_every_nth_index(self, tree, tmp_path):
        _, root = tree
        for i in (201, 202, 203, 204):
            add_source(root / self.ACQ, i)
        cfg = W.load_config(write_cfg(tmp_path, root, file_stride=2))
        rep = W.run(cfg)
        assert rep["files_rendered"] == 4 and rep["backlog"] == 0
        new = json.loads((tmp_path / "wf" / "subset" / self.ACQ / "index.json").read_text())
        old = json.loads((tmp_path / "wf" / "subset" / "acq_20260101_000000_000000000" / "index.json").read_text())
        assert new["files"] == [200, 202, 204] and old["files"] == [100]
        assert W.run(cfg)["files_rendered"] == 0
        with pytest.raises(ValueError, match="file_stride"):
            W.load_config(write_cfg(tmp_path, root, file_stride=0))


class TestStreamNodes:
    METRICS = ('kotekan_N2accumulate_frac_flagged_fpga_ticks_pl{stage_name="/n2_accumulate/accum_0",'
               'freq_id="1537"} 0 1\n'
               'kotekan_N2accumulate_frac_flagged_fpga_ticks_pl{stage_name="/n2_accumulate/accum_1",'
               'freq_id="1545"} 0 1\n'
               'kotekan_other{freq_id="1540"} 0 1\n')

    def test_without_a_choco_block_no_names(self):
        assert W.stream_nodes({"choco": None}) == {}

    def test_names_come_from_each_started_node(self, monkeypatch):
        import io
        nodes = {"groups": {"cx": [
            {"name": "cx52", "host": "cx52", "port": 12048, "started": True, "status": "started"},
            {"name": "cx42", "host": "cx42", "started": True, "status": "down"},
            {"name": "cx19", "host": "cx19", "started": False}]}}
        monkeypatch.setattr(W, "get_json", lambda url, path: nodes)
        asked = []

        def urlopen(url, timeout):
            asked.append(url)
            return io.BytesIO(self.METRICS.encode())
        monkeypatch.setattr(W.urllib.request, "urlopen", urlopen)
        out = W.stream_nodes({"choco": {"url": "https://localhost:5000", "group": "cx"}})
        assert out == {"1": "cx52", "9": "cx52"} and asked == ["http://cx52:12048/metrics"]

    def test_an_unreachable_choco_names_nothing(self, monkeypatch):
        def boom(url, path):
            raise OSError("refused")
        monkeypatch.setattr(W, "get_json", boom)
        assert W.stream_nodes({"choco": {"url": "https://localhost:5000", "group": "cx"}}) == {}

    def test_only_a_live_file_is_named(self):
        now = 2_000_000_000.0
        ns = int((now - 60 - W._J2000_UNIX_S) * 1e9)
        assert W._live({"times": [ns - 10**10, ns]}, now)
        assert not W._live({"times": [ns - 4000 * 10**9]}, now) and not W._live({"times": [0]}, now)

    def test_a_choco_block_must_be_a_mapping(self, tmp_path):
        with pytest.raises(ValueError, match="choco"):
            W.load_config(write_cfg(tmp_path, tmp_path, choco="localhost"))
