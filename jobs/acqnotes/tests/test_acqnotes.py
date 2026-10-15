"""The acqnotes driver: what is due, what is written, and the run record.

The measurement steps read real kotekan files; here they are replaced by
stubs so the tests cover the scheduling and write-out, not the physics.
"""
import json

import pytest

import acqnotes
import render


def make_acq(root, name, files):
    d = root / name
    d.mkdir(parents=True)
    for i, stamp in enumerate(files):
        (d / f"vis_{i:010d}_{stamp}_000000000.h5").write_bytes(b"")
    return d


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "subset"
    r.mkdir()
    return r


@pytest.fixture
def stub_steps(monkeypatch):
    calls = []

    def step(name):
        def fn(d, prev):
            calls.append((name, d.rsplit("/", 1)[-1], prev))
            return {"step": name}
        return fn

    monkeypatch.setattr(acqnotes, "STEPS", {s: step(s) for s in acqnotes.STEPS})
    return calls


@pytest.fixture
def stub_render(monkeypatch):
    seen = {}

    def render_all(names, root, work, newest=None, gen_time=None):
        seen["names"], seen["newest"] = list(names), newest
        return [(n, f"# {n}\nThe choco acqnotes job wrote these notes on 2026-10-12.\n",
                 f"acquisition: {n}\n") for n in names]

    monkeypatch.setattr(render, "render_all", render_all)
    monkeypatch.setattr(render, "timeline_text", lambda: "entries: []\n")
    return seen


def cfg(root, **kw):
    c = dict(acqnotes.DEFAULTS, root=str(root))
    c.update(kw)
    return c


class TestScan:
    def test_lists_acquisitions_and_their_newest_file(self, root):
        make_acq(root, "acq_20261010_015818_013401699", ["20261010T_015900", "20261010T_020539"])
        make_acq(root, "acq_20261011_005653_461515121", [])
        (root / "waterfalls").mkdir()
        (root / "timeline.yaml").write_text("")
        got = acqnotes.scan(root)
        assert got == {
            "acq_20261010_015818_013401699": {
                "n_files": 2, "newest": "vis_0000000001_20261010T_020539_000000000.h5"},
            "acq_20261011_005653_461515121": {"n_files": 0, "newest": None},
        }

    def test_partial_and_stray_files_are_not_counted(self, root):
        d = make_acq(root, "acq_20261010_015818_013401699", ["20261010T_015900"])
        (d / ".partial").mkdir()
        (d / "README.md").write_text("")
        (d / "vis_x.h5.tmp").write_text("")
        assert acqnotes.scan(root)["acq_20261010_015818_013401699"]["n_files"] == 1

    def test_missing_root_is_an_oserror(self, tmp_path):
        with pytest.raises(OSError):
            acqnotes.scan(tmp_path / "gone")


class TestDue:
    A, B = "acq_20261010_015818_013401699", "acq_20261011_005653_461515121"

    def test_new_acquisitions_newest_first_and_empty_ones_wait(self):
        acqs = {self.A: {"n_files": 3, "newest": "a3"}, self.B: {"n_files": 1, "newest": "b1"},
                "acq_20261012_000000_000000000": {"n_files": 0, "newest": None}}
        assert acqnotes.due(acqs, {}, 1000.0, 3600) == [self.B, self.A]

    def test_unchanged_acquisitions_are_not_remeasured(self):
        acqs = {self.A: {"n_files": 3, "newest": "a3"}}
        index = {self.A: {"n_files": 3, "newest": "a3", "measured": 0.0}}
        assert acqnotes.due(acqs, index, 1e9, 3600) == []

    def test_newest_acquisition_waits_for_remeasure_interval(self):
        acqs = {self.A: {"n_files": 3, "newest": "a3"}, self.B: {"n_files": 5, "newest": "b5"}}
        index = {self.A: {"n_files": 3, "newest": "a3", "measured": 0.0},
                 self.B: {"n_files": 4, "newest": "b4", "measured": 1000.0}}
        slack = acqnotes.TICK_SLACK_S
        assert acqnotes.due(acqs, index, 1000.0 + 3600 - slack - 1, 3600) == []
        # a tick that lands a little early still counts
        assert acqnotes.due(acqs, index, 1000.0 + 3600 - slack, 3600) == [self.B]

    def test_a_finished_acquisition_that_changed_is_remeasured_at_once(self):
        acqs = {self.A: {"n_files": 4, "newest": "a4"}, self.B: {"n_files": 1, "newest": "b1"}}
        index = {self.A: {"n_files": 3, "newest": "a3", "measured": 999.0},
                 self.B: {"n_files": 1, "newest": "b1", "measured": 999.0}}
        assert acqnotes.due(acqs, index, 1000.0, 3600) == [self.A]


class TestRun:
    def test_measures_up_to_the_cap_newest_first_and_writes_notes(self, root, tmp_path,
                                                                  stub_steps, stub_render):
        names = ["acq_20261010_015818_013401699", "acq_20261011_005653_461515121",
                 "acq_20261012_005242_957492261"]
        for n in names:
            make_acq(root, n, ["20261010T_015900"])
        work = tmp_path / "state"
        report = acqnotes.run(cfg(root), work, max_acqs=2)

        assert report["measured"] == names[:0:-1]
        assert report["backlog"] == 1
        assert {c[1] for c in stub_steps} == set(names[1:])
        assert {c[0] for c in stub_steps} == set(acqnotes.STEPS)
        assert json.loads((work / "meta" / f"{names[2]}.json").read_text()) == {"step": "meta"}
        # only measured acquisitions are rendered; the newest is flagged
        assert stub_render["names"] == names[1:]
        assert stub_render["newest"] == names[2]
        assert (root / names[2] / "README.md").read_text().startswith(f"# {names[2]}")
        assert not (root / names[0] / "README.md").exists()
        assert (root / "timeline.yaml").read_text() == "entries: []\n"
        assert set(report["written"]) == {names[1], names[2], "timeline.yaml"}

        # the next run takes the backlog and rewrites nothing else
        report = acqnotes.run(cfg(root), work, max_acqs=2)
        assert report["measured"] == [names[0]]
        assert report["written"] == [names[0]]

    def test_previous_result_is_passed_back_to_the_step(self, root, tmp_path, stub_steps, stub_render):
        n = "acq_20261010_015818_013401699"
        d = make_acq(root, n, ["20261010T_015900"])
        work = tmp_path / "state"
        acqnotes.run(cfg(root, remeasure_hours=0), work, max_acqs=1)
        (d / "vis_0000000009_20261010T_020539_000000000.h5").write_bytes(b"")
        stub_steps.clear()
        acqnotes.run(cfg(root, remeasure_hours=0), work, max_acqs=1)
        assert ("verify", n, {"step": "verify"}) in stub_steps

    def test_one_failing_acquisition_does_not_stop_the_others(self, root, tmp_path, monkeypatch,
                                                             stub_render):
        bad, good = "acq_20261010_015818_013401699", "acq_20261011_005653_461515121"
        make_acq(root, bad, ["20261010T_015900"])
        make_acq(root, good, ["20261011T_005900"])

        def meta(d, prev):
            if d.endswith(bad):
                raise OSError("stale file handle")
            return {}
        monkeypatch.setattr(acqnotes, "STEPS", {"meta": meta})
        report = acqnotes.run(cfg(root), tmp_path / "state", max_acqs=5)
        assert report["measured"] == [good]
        assert report["degraded"] == [f"{bad}: stale file handle"]
        # not indexed, so the next run retries it
        index = json.loads((tmp_path / "state" / acqnotes.INDEX_FILE).read_text())
        assert set(index) == {good}


class TestWriteIfChanged:
    def test_a_new_date_alone_does_not_rewrite_a_readme(self, tmp_path):
        p = tmp_path / "README.md"
        old = "x\nThe choco acqnotes job (`jobs/acqnotes/`) wrote these notes on 2026-10-09.  y\n"
        p.write_text(old)
        new = old.replace("2026-10-09", "2026-10-12")
        assert not acqnotes.write_if_changed(p, new, readme=True)
        assert p.read_text() == old
        assert acqnotes.write_if_changed(p, new + "more\n", readme=True)
        assert p.read_text() == new + "more\n"

    def test_other_files_compare_whole(self, tmp_path):
        p = tmp_path / "acq_info.yaml"
        assert acqnotes.write_if_changed(p, "a: 1\n")
        assert not acqnotes.write_if_changed(p, "a: 1\n")
        assert acqnotes.write_if_changed(p, "a: 2\n")


class TestConfig:
    def test_defaults_without_a_file(self, tmp_path):
        assert acqnotes.load_config(tmp_path / "absent.yaml") == acqnotes.DEFAULTS

    def test_unknown_key_is_refused(self, tmp_path):
        p = tmp_path / "acqnotes.yaml"
        p.write_text("root: /x\nwork_dir: /y\n")
        with pytest.raises(ValueError, match="work_dir"):
            acqnotes.load_config(p)

    def test_cap_must_be_positive(self, tmp_path):
        p = tmp_path / "acqnotes.yaml"
        p.write_text("max_acqs_per_run: 0\n")
        with pytest.raises(ValueError):
            acqnotes.load_config(p)


class TestMain:
    def test_exit_codes_and_run_record(self, root, tmp_path, monkeypatch, stub_steps, stub_render):
        make_acq(root, "acq_20261010_015818_013401699", ["20261010T_015900"])
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text(f"root: {root}\n")
        assert acqnotes.main(["--config", str(conf)]) == 0
        run = json.loads((tmp_path / "run.json").read_text())
        assert run["status"] == "ok" and run["measured"] == ["acq_20261010_015818_013401699"]

    def test_unreadable_root_is_degraded(self, tmp_path):
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text(f"root: {tmp_path / 'gone'}\n")
        assert acqnotes.main(["--config", str(conf)]) == 2
        assert json.loads((tmp_path / "run.json").read_text())["status"] == "degraded"

    def test_bad_config_fails(self, tmp_path):
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text("bogus: 1\n")
        assert acqnotes.main(["--config", str(conf)]) == 1

    def test_remeasure_forgets_named_acquisitions(self, root, tmp_path, stub_steps, stub_render):
        n = "acq_20261010_015818_013401699"
        make_acq(root, n, ["20261010T_015900"])
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text(f"root: {root}\n")
        acqnotes.main(["--config", str(conf)])
        stub_steps.clear()
        acqnotes.main(["--config", str(conf)])
        assert stub_steps == []
        acqnotes.main(["--config", str(conf), "--remeasure", n])
        assert {c[1] for c in stub_steps} == {n}
        with pytest.raises(ValueError):
            acqnotes.forget(tmp_path, ["../etc"])


class TestRender:
    def test_file_bins_from_the_vis_shape(self):
        assert render.file_bins([{"datasets": [["vis", [6145, 1176, 40], "complex64"]]}]) == 40
        assert render.file_bins([{"error": "x"}, {}]) == 20

    def test_date_pattern_matches_the_header(self):
        assert render.GEN_DATE_RE.search(
            "The choco acqnotes job (`jobs/acqnotes/`) wrote these notes on 2026-10-12.")
