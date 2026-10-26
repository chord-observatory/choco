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

    def test_a_request_waits_for_the_lock(self, root, tmp_path, stub_steps, stub_render):
        old, new = "acq_20261016_000000_1", "acq_20261018_000000_2"
        make_acq(root, old, ["20261016T_000100"])
        make_acq(root, new, ["20261018T_000100"])
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text(f"root: {root}\n")
        acqnotes.main(["--config", str(conf)])
        stub_steps.clear()
        # queued while the timer's run holds the lock: nothing runs now
        with acqnotes.single_run(tmp_path / acqnotes.LOCK_FILE):
            assert acqnotes.main(["--config", str(conf), "--remeasure-from", "acq_20261017"]) == 0
        assert stub_steps == [] and len(list((tmp_path / "remeasure.d").iterdir())) == 1
        # the next run takes it
        acqnotes.main(["--config", str(conf)])
        assert {c[1] for c in stub_steps} == {new}
        assert json.loads((tmp_path / "run.json").read_text())["requested"] == [new]
        assert list((tmp_path / "remeasure.d").iterdir()) == []

    def test_queue_only_queues(self, root, tmp_path, stub_steps, stub_render):
        conf = tmp_path / "acqnotes.yaml"
        conf.write_text(f"root: {root}\n")
        assert acqnotes.main(["--config", str(conf), "--queue", "--remeasure", "acq_20261016_000000_1"]) == 0
        assert not (tmp_path / "run.json").exists()
        with pytest.raises(SystemExit):
            acqnotes.main(["--config", str(conf), "--queue"])
        with pytest.raises(SystemExit):
            acqnotes.main(["--config", str(conf), "--remeasure-from", "../etc"])


class TestRender:
    def test_file_bins_from_the_vis_shape(self):
        assert render.file_bins([{"datasets": [["vis", [6145, 1176, 40], "complex64"]]}]) == 40
        assert render.file_bins([{"error": "x"}, {}]) == 20

    def test_date_pattern_matches_the_header(self):
        assert render.GEN_DATE_RE.search(
            "The choco acqnotes job (`jobs/acqnotes/`) wrote these notes on 2026-10-12.")

    def test_dish_ranges(self):
        assert render.dish_ranges(["B05", "A02", "A01", "A03", "B03", "B06"]) == "A01-A03, B03, B05-B06"


class TestPolChecks:
    """Mixed pointing (2026-10-18): each transit is in the beam of one group of dishes."""

    DISHES = ["A01", "A02", "A03", "A04", "C01", "C02", "C03", "C04", "D01"]

    def axis(self):
        return [{"file_label": d + p, "dish": d, "pol": p, "phys": d, "note": None}
                for p in "XY" for d in self.DISHES]

    def result(self, source, group, swapped=()):
        """Fringes on *group* at 20x the off-source level (cross-pol 10 % of co-pol); the other
        dishes fringe 20x weaker against them, through their sidelobes; D01 has no signal."""
        ax = self.axis()
        r = {"source": source, "transit_unix": 1792375659.0, "coverage": 1.0,
             "prod_a": [], "prod_b": [], "amp_on": [], "amp_off": []}
        for i, a in enumerate(ax):
            for j in range(i, len(ax)):
                b = ax[j]
                pa = ("Y" if a["pol"] == "X" else "X") if a["phys"] in swapped else a["pol"]
                pb = ("Y" if b["pol"] == "X" else "X") if b["phys"] in swapped else b["pol"]
                n = (a["phys"] in group) + (b["phys"] in group)
                amp = {2: 1.0, 1: 0.05, 0: 0.05}[n] * (1.0 if pa == pb else 0.1)
                if "D01" in (a["phys"], b["phys"]):
                    amp = 0.0
                r["prod_a"].append(i)
                r["prod_b"].append(j)
                r["amp_on"].append(amp if n else 0.05)
                r["amp_off"].append(0.05)
        return r

    def test_each_group_is_checked_at_its_own_transit(self):
        cyg = self.result("CygA", {"A01", "A02", "A03", "A04"})
        tau = self.result("TauA", {"C01", "C02", "C03", "C04"}, swapped={"C03"})
        checks = render.pol_checks({"result": cyg, "results": [cyg, tau]}, self.axis(), None)
        assert [c["conclusive"] for c in checks] == [True, True]
        assert checks[0]["covered"] == ["A01", "A02", "A03", "A04"]
        assert checks[1]["covered"] == ["C01", "C02", "C03", "C04"]
        assert [d for d in checks[1]["covered"] if checks[1]["dishes"][d]["swapped"]] == ["C03"]
        assert "D01" in checks[0]["others"]
        a = {"pols": checks, "timeline": []}
        assert render.swapped_dishes(a) == ["C03"]

    def test_a_group_alone_is_not_diluted_by_the_rest_of_the_array(self):
        cyg = self.result("CygA", {"A01", "A02", "A03", "A04"})
        (c,) = render.pol_checks({"result": cyg}, self.axis(), None)  # a measurement from before 2026-10
        assert c["conclusive"] and c["covered"] == ["A01", "A02", "A03", "A04"]
        assert c["contrast"] >= 2

    def test_the_history_holds_for_dishes_no_check_covers(self):
        cyg = self.result("CygA", {"A01", "A02", "A03", "A04"})
        checks = render.pol_checks({"result": cyg, "results": [cyg]}, self.axis(), None)
        a = {"pols": checks, "timeline": [{"topic": "polarization", "swapped": ["A02", "C02"]}]}
        assert render.swapped_dishes(a) == ["C02"]

    def test_fringes_place_the_dishes_when_the_autos_cannot(self):
        # Tau A raises no autocorrelation; Cyg A's 18 % is a sidelobe (2026-10-26)
        tau = self.result("TauA", {"A01", "A02", "A03", "A04", "C01", "C02", "C03", "C04"})
        checks = render.pol_checks({"result": tau, "results": [tau]}, self.axis(), None)
        a = {"pols": checks, "axis": self.axis(), "attrs": {"dish_coelev_deg": -27.3},
             "sources": [{"source": "CygA", "dec": 40.7, "bands": [{"median_rise": 0.18}]},
                         {"source": "TauA", "dec": 22.0, "bands": [{"median_rise": None}]}]}
        p = render.pointing_summary(a)
        assert p["short"].startswith("8 of ") and "Tau A transit (Dec +22)" in p["short"]
        assert "near the config value +22" in p["short"] and p["agrees"]


class TestChunkBlocks:
    """Reads are priced per chunk: a chunk of ``vis`` holds every product."""

    def test_blocks_are_chunk_aligned_and_distinct(self, tmp_path):
        import h5py
        import numpy as np
        import extract
        with h5py.File(tmp_path / "v.h5", "w") as f:
            d = f.create_dataset("vis", shape=(100, 3, 2), dtype="c8", chunks=(16, 3, 2))
            assert extract.chunk_rows(d) == 16
            assert extract.chunk_blocks(d, (0.25, 0.5, 0.75)) == [(16, 32), (48, 64), (64, 80)]
            # a fraction past the last whole chunk still lands inside the axis
            assert extract.chunk_blocks(d, (0.99,)) == [(84, 100)]
            # the same chunk twice is read once
            assert extract.chunk_blocks(d, (0.5, 0.51)) == [(48, 64)]
            c = f.create_dataset("flat", data=np.zeros((20, 2, 2), "c8"))
            assert extract.chunk_rows(c) == 16 and extract.chunk_blocks(c, (0.5,)) == [(0, 16)]
