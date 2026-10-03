"""Tests for the bffs core (config, combine, state, CLI) — run with `pytest`."""

import json

import numpy as np
import pytest

import bffs
from testhelpers import write_chord_n2, write_manual, write_normalized


# -- config ---------------------------------------------------------------


def test_load_config(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(n2),
        "choco": {"url": "https://choco.local:5000", "group": "cx"},
        "sources": [{"kind": "power-outlier"}],
    }))
    cfg = bffs.load_config(cfg_file)
    assert cfg.kotekan_file == str(n2)
    assert cfg.url == "https://choco.local:5000"
    assert cfg.group == "cx"
    assert cfg.endpoint == "updatable_config/bad_inputs"


def test_load_config_requires_kotekan_file(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"sources": []}))
    try:
        bffs.load_config(cfg_file)
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing kotekan_file")


def test_load_config_requires_group_with_url(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": "n2.h5", "choco": {"url": "https://choco.local:5000"},
    }))
    try:
        bffs.load_config(cfg_file)
    except ValueError:
        return
    raise AssertionError("expected ValueError for choco.url without choco.group")


def test_unknown_source_kind_raises(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), sources=[{"kind": "nope"}])
    try:
        bffs.combine_sources(cfg)
    except ValueError:
        return
    raise AssertionError("expected ValueError for unknown source kind")


# -- end to end (dispatch through the source registry) --------------------


def test_flag_end_to_end(tmp_path):
    # feed 1 is a bright power outlier; feed 2 is an operator override.
    n2 = tmp_path / "n2.h5"
    auto = np.ones((2, 4, 4), "f4") * 10.0
    auto[..., 1] = 900.0
    write_normalized(n2, ["f0", "f1", "f2", "f3"], np.linspace(400, 800, 4), auto)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f2X"])

    cfg = bffs.Config(
        kotekan_file=str(n2), sync_delay=5.0,
        sources=[
            {"kind": "manual", "path": str(manualf)},
            {"kind": "power-outlier", "nsigma": 5.0},
        ],
    )
    payload, _, _ = bffs.run(cfg, now=1_700_000_000.0)
    assert payload["bad_inputs"] == [1, 2]
    assert payload["start_time"] == 1_700_000_005.0
    assert set(payload) == {"update_id", "start_time", "bad_inputs"}
    assert isinstance(payload["update_id"], str)


# -- state / change history -----------------------------------------------


def _state_config(n2, statef, manualf, **kw):
    return bffs.Config(
        kotekan_file=str(n2), sync_delay=5.0, state_path=str(statef),
        sources=[{"kind": "manual", "path": str(manualf)}], **kw,
    )


def test_state_records_change_history(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1", "f2"], [400.0], np.ones((1, 1, 3), "f4"))
    cfg = _state_config(n2, statef, manualf)
    write_manual(manualf, [])  # all good

    # run 1: all good -> first run sends and records a baseline entry.
    _, send, _ = bffs.run(cfg, now=1000.0)
    assert send is True
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == [] and len(st["history"]) == 1

    # run 2: nothing changed -> no send, no new history.
    _, send, _ = bffs.run(cfg, now=1001.0)
    assert send is False
    assert len(json.loads(statef.read_text())["history"]) == 1

    # feed 1 goes bad -> send, a new history entry naming the transition.
    write_manual(manualf, ["f1X"])
    payload, send, _ = bffs.run(cfg, now=1002.0)
    assert send is True and payload["bad_inputs"] == [1]
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == ["f1X"]
    assert len(st["history"]) == 2
    assert st["history"][-1]["became_bad"] == ["f1X"]
    assert st["history"][-1]["became_good"] == []

    # feed 1 recovers -> send, recorded as became_good.
    write_manual(manualf, [])
    _, send, _ = bffs.run(cfg, now=1003.0)
    assert send is True
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == []
    assert st["history"][-1]["became_good"] == ["f1X"]


def test_state_records_the_element_axis(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1", "f2"], [400.0], np.ones((1, 1, 3), "f4"))
    bffs.run(_state_config(n2, statef, manualf), now=1000.0)
    assert json.loads(statef.read_text())["labels"] == ["f0X", "f1X", "f2X"]


def test_pre_axis_state_file_gains_labels_without_a_transition(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    write_manual(manualf, ["f1X"])
    cfg = _state_config(n2, statef, manualf)
    bffs.run(cfg, now=1000.0)
    st = json.loads(statef.read_text())
    del st["labels"]                       # written before the axis was recorded
    statef.write_text(json.dumps(st))

    _, send, _ = bffs.run(cfg, now=1001.0)
    assert send is False                   # nothing changed: no send ...
    st = json.loads(statef.read_text())
    assert st["labels"] == ["f0X", "f1X"]  # ... but the axis is now on record
    assert len(st["history"]) == 1         # and no transition was invented


def test_axis_change_with_the_same_bad_labels_sends(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    write_manual(manualf, ["f1X"])
    cfg = _state_config(n2, statef, manualf)
    payload, _, _ = bffs.run(cfg, now=1000.0)
    assert payload["bad_inputs"] == [1]

    # the axis grows: f1X keeps its name but is element 2 now
    write_normalized(n2, ["f0", "f9", "f1"], [400.0], np.ones((1, 1, 3), "f4"))
    payload, send, _ = bffs.run(cfg, now=1001.0)
    assert send is True and payload["bad_inputs"] == [2]
    st = json.loads(statef.read_text())
    assert st["labels"] == ["f0X", "f9X", "f1X"]
    assert st["bad_inputs"] == ["f1X"]
    assert st["history"][-1]["became_bad"] == [] and st["history"][-1]["became_good"] == []


def test_force_sends_when_unchanged(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    write_manual(manualf, ["f1X"])
    cfg = _state_config(n2, statef, manualf)

    bffs.run(cfg, now=1000.0)                       # establish state
    _, send, _ = bffs.run(cfg, now=1001.0, force=True)  # unchanged, but forced
    assert send is True
    assert len(json.loads(statef.read_text())["history"]) == 1  # force adds no entry


def test_dry_run_does_not_write_state(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    bffs.run(_state_config(n2, statef, manualf), now=1000.0, write=False)  # manual file absent -> all good
    assert not statef.exists()


def test_max_history_truncates(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    cfg = _state_config(n2, statef, manualf, max_history=2)

    for i, bad in enumerate([[], ["f1X"], []]):  # three changes
        write_manual(manualf, bad)
        bffs.run(cfg, now=1000.0 + i)
    hist = json.loads(statef.read_text())["history"]
    assert len(hist) == 2  # capped at the last two


def test_corrupt_state_is_treated_as_first_run(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    statef.write_text("{ not json")  # corrupt -> recover, don't crash
    _, send, _ = bffs.run(_state_config(n2, statef, manualf), now=1000.0)
    assert send is True
    st = json.loads(statef.read_text())  # rewritten as valid JSON
    assert st["bad_inputs"] == [] and len(st["history"]) == 1


def test_main_dry_run_prints_payload(tmp_path, capsys):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(n2),
        "sources": [{"kind": "manual", "path": str(manualf)}],
    }))
    rc = bffs.main(["--config", str(cfg_file), "--dry-run"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bad_inputs"] == [1]


def test_glob_kotekan_file_reads_newest(tmp_path):
    import os
    old, new = tmp_path / "n2_old.h5", tmp_path / "n2_new.h5"
    write_normalized(old, ["old0"], [400.0], np.ones((1, 1, 1), "f4"))
    write_normalized(new, ["new0", "new1"], [400.0], np.ones((1, 1, 2), "f4"))
    os.utime(old, (1_000_000, 1_000_000))
    os.utime(new, (2_000_000, 2_000_000))
    labels, good, _, _ = bffs.combine_sources(bffs.Config(kotekan_file=str(tmp_path / "n2_*.h5"), max_age=0))
    assert list(labels) == ["new0X", "new1X"]


def test_glob_no_match_and_no_choco_raises(tmp_path):
    # No file and no choco context -> nothing to index flags against.
    try:
        bffs.combine_sources(bffs.Config(kotekan_file=str(tmp_path / "nope_*.h5")))
    except OSError as e:
        assert "no feed labels" in str(e)
        return
    raise AssertionError("expected OSError with no labels source")


def test_failed_send_leaves_state_unwritten(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    statef = tmp_path / "state.json"
    cfg = bffs.Config(
        kotekan_file=str(n2), state_path=str(statef),
        sources=[{"kind": "manual", "path": str(manualf)}],
    )

    def failing_sender(payload):
        raise OSError("choco unreachable")

    try:
        bffs.run(cfg, now=1000.0, sender=failing_sender)
    except OSError:
        pass
    assert not statef.exists()  # nothing recorded -> the next run retries

    sent = []
    _, send, _ = bffs.run(cfg, now=1001.0, sender=sent.append)
    assert send is True and sent[0]["bad_inputs"] == [1]
    assert json.loads(statef.read_text())["bad_inputs"] == ["f1X"]


def test_send_to_choco_posts_group_update(monkeypatch):
    import urllib.request
    captured = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b""

    def fake_urlopen(req, timeout=None, context=None):
        captured["url"] = req.full_url
        captured["body"] = json.loads(req.data)
        captured["context"] = context
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    cfg = bffs.Config(kotekan_file="n2.h5", url="https://localhost:5000", group="cx")
    payload = {"update_id": "bffs-1", "start_time": 5.0, "bad_inputs": [3, 7]}
    bffs.send_to_choco(cfg, payload)
    assert captured["url"] == "https://localhost:5000/update/cx"
    assert captured["body"] == {
        "action": "updatable_config",
        "endpoint": "updatable_config/bad_inputs",
        "values": payload,
    }
    assert captured["context"] is not None  # self-signed TLS goes unverified


def test_main_missing_kotekan_file_exits_degraded(tmp_path, caplog):
    """An environmental failure exits 2 (degraded) with one log line:
    the job is fine, its input wasn't — retries self-heal."""
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(tmp_path / "nope_*.h5"),
        "sources": [],
    }))
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 2
    assert "no kotekan file matches" in caplog.text


def test_main_config_error_exits_failed(tmp_path, caplog):
    """A config problem (unknown source kind) exits 1 — needs a human."""
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(n2),
        "sources": [{"kind": "nope"}],
    }))
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 1
    assert "unknown source kind" in caplog.text


def test_main_partial_skip_exits_degraded(tmp_path, monkeypatch, caplog):
    """File-based sources skipped but others still flagging: the run
    completes (flags computed) yet exits 2 so the badge shows degraded."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["A3X"])
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(tmp_path / "nope_*.h5"),
        "choco": {"url": "https://localhost:5000", "group": "cx"},
        "sources": [{"kind": "power-outlier"},
                    {"kind": "manual", "path": str(manualf)}],
    }))
    rc = bffs.main(["--config", str(cfg_file), "--dry-run"])
    assert rc == 2
    assert "degraded run" in caplog.text
    assert "skipped: power-outlier" in caplog.text


def test_main_bad_config_fails_cleanly(tmp_path, caplog):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"sources": []}))  # no kotekan_file
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 1
    assert "bad config" in caplog.text


def test_glob_across_acq_dirs_reads_newest(tmp_path):
    """Wildcards may span directories (acq_*/*.h5 layouts): the most
    recently written file wins across all acquisition dirs."""
    import os
    old_acq = tmp_path / "acq_20260101T000000"
    new_acq = tmp_path / "acq_20260716T000000"
    old_acq.mkdir()
    new_acq.mkdir()
    old = old_acq / "n2_000.h5"
    mid = new_acq / "n2_000.h5"
    new = new_acq / "n2_001.h5"
    write_normalized(old, ["old0"], [400.0], np.ones((1, 1, 1), "f4"))
    write_normalized(mid, ["mid0"], [400.0], np.ones((1, 1, 1), "f4"))
    write_normalized(new, ["new0", "new1"], [400.0], np.ones((1, 1, 2), "f4"))
    os.utime(old, (1_000_000, 1_000_000))
    os.utime(mid, (2_000_000, 2_000_000))
    os.utime(new, (3_000_000, 3_000_000))
    os.utime(old_acq, (1_000_000, 1_000_000))
    os.utime(new_acq, (3_000_000, 3_000_000))
    labels, good, _, _ = bffs.combine_sources(
        bffs.Config(kotekan_file=str(tmp_path / "acq_*" / "*.h5"), max_age=0))
    assert list(labels) == ["new0X", "new1X"]


def test_newest_file_compares_directories_first(tmp_path):
    """Only the newest directory's files are stat'ed, so the lookup does
    not grow with the archive: a newer file in an older directory is not
    seen until that directory's mtime moves."""
    import os
    a, b = tmp_path / "acq_a", tmp_path / "acq_b"
    a.mkdir(), b.mkdir()
    (a / "x.h5").write_bytes(b""), (a / "y.h5").write_bytes(b"")
    (b / "z.h5").write_bytes(b"")
    os.utime(a / "x.h5", (1_000, 1_000))
    os.utime(a / "y.h5", (9_000, 9_000))     # newest file overall ...
    os.utime(b / "z.h5", (5_000, 5_000))
    os.utime(a, (1_000, 1_000))
    os.utime(b, (5_000, 5_000))              # ... but b is the live directory
    assert bffs.newest_file(str(tmp_path / "acq_*" / "*.h5")) == str(b / "z.h5")
    os.utime(a, (9_000, 9_000))
    assert bffs.newest_file(str(tmp_path / "acq_*" / "*.h5")) == str(a / "y.h5")
    assert bffs.newest_file(str(tmp_path / "nothing" / "*.h5")) is None
    # a single directory: plain newest-by-mtime
    assert bffs.newest_file(str(a / "*.h5")) == str(a / "y.h5")


def test_choco_context_injected_into_sources(tmp_path, monkeypatch):
    """combine_sources merges choco url/group into each source's config."""
    from sources import rfi
    seen = {}
    node = {"name": "cx1", "host": "cx1.example", "port": 12048, "started": True}
    monkeypatch.setattr(rfi, "choco_group_nodes",
                        lambda url, group: seen.update(url=url, group=group) or [node])
    monkeypatch.setattr(rfi, "read_sk", lambda url: {})
    monkeypatch.setattr(rfi, "read_sk_freshness", lambda base: {})  # no network
    monkeypatch.setattr(bffs, "choco_group_config", lambda url, group: {})
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx", sources=[{"kind": "rfi"}])
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert seen == {"url": "https://localhost:5000", "group": "cx"}
    assert list(good) == [True]


def test_dish_types_injected_into_sources(tmp_path, monkeypatch):
    """Each source also gets ``dish_types`` — the config's per-element
    kotekan dish type by label — and the run record the counts."""
    import sources
    seen = {}

    class Probe:
        @staticmethod
        def mask(src, labels, path):
            seen.update(src)
            return np.ones(len(labels), dtype=bool)

    monkeypatch.setattr(sources, "get", lambda kind: Probe if kind == "probe" else None)
    cfg_dict = {
        "num_dishes": 3, "num_polarizations": 2,
        "telescope": {"dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A01"},
            {"dish_idx": 1, "type": "Missing", "label": "E01"},
            {"dish_idx": 2, "type": "RFIDish", "label": "RFIA1"},
        ]},
    }
    monkeypatch.setattr(bffs, "choco_group_config", lambda url, group: cfg_dict)
    run = {}
    bffs.combine_sources(bffs.Config(kotekan_file=str(tmp_path / "none.h5"),
                                     url="https://localhost:5000", group="cx",
                                     sources=[{"kind": "probe"}]), run)
    assert seen["dish_types"] == {
        "A01X": "ArrayDish", "E01X": "Missing", "RFIA1X": "RFIDish",
        "A01Y": "ArrayDish", "E01Y": "Missing", "RFIA1Y": "RFIDish"}
    assert run["n_by_type"] == {"ArrayDish": 2, "Missing": 2, "RFIDish": 2}
    # the file's own axis carries no types
    seen.clear()
    monkeypatch.setattr(bffs, "choco_group_config", lambda url, group: {})
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    run = {}
    bffs.combine_sources(bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                                     group="cx", sources=[{"kind": "probe"}]), run)
    assert seen["dish_types"] is None and "n_by_type" not in run


def test_stale_file_with_no_other_labels_fails(tmp_path):
    """A stale file is unusable; with no choco labels either, the run fails."""
    import os
    import time
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    old = time.time() - 7200
    os.utime(n2, (old, old))
    try:
        bffs.combine_sources(bffs.Config(kotekan_file=str(n2), max_age=3600))
    except OSError as e:
        assert "no feed labels" in str(e)
        return
    raise AssertionError("expected OSError for stale data and no labels")


def test_max_age_zero_disables_staleness(tmp_path):
    import os
    import time
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    old = time.time() - 7200
    os.utime(n2, (old, old))
    labels, good, _, _ = bffs.combine_sources(
        bffs.Config(kotekan_file=str(n2), max_age=0))
    assert list(labels) == ["f0X"]


# -- labels from the kotekan config (dish_inputs) ---------------------------


_DISH_CONFIG = {
    "telescope": {
        "dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A1X"},
            {"dish_idx": 2, "type": "Fake", "label": "A3X"},
        ],
    },
}


def test_per_element_config_table_is_refused():
    """Pre-2026-08 tables carried a wrong element ordering; flagging
    against one would flag the wrong feeds.  OSError -> exit 2: the
    badge reads degraded and heals when the config is migrated."""
    try:
        bffs.element_labels_from_config(_DISH_CONFIG)
    except OSError as e:
        assert "migrated" in str(e)
        return
    raise AssertionError("expected OSError for a per-element table")


def test_uniquify_labels_suffixes_duplicates():
    out = list(bffs.uniquify_labels(["A1X", "Fake", "Fake", "B2Y"]))
    assert out == ["A1X", "Fake[1]", "Fake[2]", "B2Y"]


# -- labels from a 2026-08 per-dish dish_inputs table ------------------------


_PER_DISH_CONFIG = {
    "num_dishes": 3,
    "num_polarizations": 2,
    "telescope": {
        "dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A1"},
            {"dish_idx": 2, "type": "ArrayDish", "label": "A3"},
        ],
    },
}

_PER_DISH_LABELS = ["A1X", "MissingX", "A3X", "A1Y", "MissingY", "A3Y"]


def test_element_labels_per_dish_expands_pol_blocks():
    # element = dish_idx + pol * num_dishes: the X block first, then Y,
    # placeholder dishes included in both.
    assert bffs.element_labels_from_config(_PER_DISH_CONFIG) == _PER_DISH_LABELS


def test_file_axis_identical_to_config_matches():
    axis = bffs.uniquify_labels(_PER_DISH_LABELS)
    assert bffs.file_axis_mismatch(axis, _PER_DISH_LABELS) is None


def test_file_axis_may_be_a_subset_in_any_order():
    """A compact subset/ file carries the wired elements only, in its own
    order; the file sources project by label, so that is fine."""
    axis = bffs.uniquify_labels(_PER_DISH_LABELS)
    assert bffs.file_axis_mismatch(axis, ["A3Y", "A1X"]) is None


def test_file_axis_with_an_unknown_label_mismatches():
    # A file naming an element the config does not know was written
    # under another dish_inputs table — none of it can be trusted.
    axis = bffs.uniquify_labels(_PER_DISH_LABELS)
    why = bffs.file_axis_mismatch(axis, ["A1X", "Fake", "A3X"])
    assert why and "Fake" in why and "1 element" in why


def test_file_axis_duplicate_labels_mismatch_on_a_subset():
    # Placeholders are uniquified by element index on both sides, so a
    # subset file's duplicates have no unique name to project by.
    axis = bffs.uniquify_labels(_PER_DISH_LABELS)
    why = bffs.file_axis_mismatch(axis, ["MissingX", "MissingX", "A1X"])
    assert why and "2 element" in why and "MissingX[0]" in why
    # one placeholder, unique on both sides, projects by name like any label
    assert bffs.file_axis_mismatch(axis, ["MissingX", "A1X"]) is None


def test_element_types_follow_the_label_axis():
    # a dish's type on both of its polarizations; a slot the table skips
    # is Missing, like its label; an entry without a type too
    assert bffs.element_types_from_config(_PER_DISH_CONFIG) == [
        "ArrayDish", "Missing", "ArrayDish", "ArrayDish", "Missing", "ArrayDish"]
    cfg = {"num_dishes": 2, "telescope": {"dish_inputs": [
        {"dish_idx": 0, "label": "A01"}, {"dish_idx": 1, "type": "RFIDish", "label": "R1"}]}}
    assert bffs.element_types_from_config(cfg) == ["Missing", "RFIDish", "Missing", "RFIDish"]
    assert bffs.element_types_from_config({}) is None


def test_element_labels_per_dish_idx_beyond_num_dishes_refuses():
    cfg = {"num_dishes": 2, "telescope": _PER_DISH_CONFIG["telescope"]}
    try:
        bffs.element_labels_from_config(cfg)
    except ValueError as e:
        assert "ambiguous" in str(e)
        return
    raise AssertionError("expected ValueError for dish_idx >= num_dishes")


def test_element_labels_per_dish_num_dishes_fallback():
    # No num_dishes in the config: the table's own extent sizes the axis.
    cfg = {"telescope": _PER_DISH_CONFIG["telescope"]}
    assert bffs.element_labels_from_config(cfg) == _PER_DISH_LABELS


def test_element_labels_expression_num_dishes_refuses():
    # kotekan evaluates expressions in config values; bffs must not guess.
    cfg = {"num_dishes": "num_polarizations * 32",
           "telescope": _PER_DISH_CONFIG["telescope"]}
    try:
        bffs.element_labels_from_config(cfg)
    except ValueError as e:
        assert "plain integer" in str(e)
        return
    raise AssertionError("expected ValueError for an expression num_dishes")


def test_per_dish_config_and_file_end_to_end(tmp_path, monkeypatch):
    """A per-dish config with a matching per-dish file resolves to the
    derived [P][D] element axis."""
    cfg_dict = {
        "num_dishes": 2, "num_polarizations": 2,
        "telescope": {"dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A1"},
            {"dish_idx": 1, "type": "ArrayDish", "label": "B1"},
        ]},
    }
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: cfg_dict)
    n2 = tmp_path / "n2.h5"
    write_chord_n2(n2, ["A1", "B1"], [400.0], np.ones((1, 1, 4), "f4"),
                   num_elements=4)
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert list(labels) == ["A1X", "B1X", "A1Y", "B1Y"]
    assert good.shape == (4,)


def test_per_element_choco_config_refuses_end_to_end(tmp_path, monkeypatch):
    """The refusal propagates out of combine_sources: an unmigrated
    kotekan config must stop the run (degraded), not fall back to
    file labels as if choco had no table."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _DISH_CONFIG)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["x0", "x1", "x2", "x3"], [400.0],
                     np.ones((1, 1, 4), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    try:
        bffs.combine_sources(cfg)
    except OSError as e:
        assert "migrated" in str(e)
        return
    raise AssertionError("expected OSError for a per-element table")


def test_config_file_element_mismatch_skips_the_file(tmp_path, monkeypatch):
    """A file naming elements the per-dish config does not know predates
    the running config: the file sources are skipped (degraded) with the
    reason in the run record, the axis is still the config's, and the
    other sources still flag."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["x0", "x1"], [400.0], np.ones((1, 1, 2), "f4"))
    manual = tmp_path / "manual.json"
    write_manual(manual, ["A3Y"])
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx", max_age=0,
                      sources=[{"kind": "power-outlier"},
                               {"kind": "manual", "path": str(manual)}])
    run = {}
    labels, good, _, degraded = bffs.combine_sources(cfg, run)
    assert list(labels) == _PER_DISH_LABELS
    assert list(good) == [True, True, True, True, True, False]
    assert run["kotekan_file"] == str(n2)
    assert "x0X" in run["kotekan_file_reason"]
    assert run["sources"][0]["status"] == "skipped"
    assert "x0X" in run["sources"][0]["reason"]
    assert degraded and "skipped: power-outlier" in degraded[0]


def test_subset_file_projects_onto_the_config_axis(tmp_path, monkeypatch):
    """The production shape: the config's axis has 128 elements, the
    compact subset/ file 48 of them in its own order.  A dead element in
    the file lands on the axis by label; the elements the file does not
    carry stay good and are counted in the report."""
    cfg_dict = {
        "num_dishes": 4, "num_polarizations": 2,
        "telescope": {"dish_inputs": [
            {"dish_idx": i, "type": "ArrayDish", "label": lbl}
            for i, lbl in enumerate(["A01", "A02", "C01", "RFIA1"])]},
    }
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: cfg_dict)
    # file axis: A01, RFIA1 (both pols) — dishes 0 and 3 of 4; RFIA1Y dead
    power = np.ones((2, 8, 4), "f4") * 10.0
    power[..., 3] = 0.0
    n2 = tmp_path / "n2.h5"
    write_chord_n2(n2, ["A01", "RFIA1"], np.linspace(400, 800, 8), power,
                   num_elements=4)
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx", max_age=0,
                      sources=[{"kind": "power-outlier"}])
    run = {}
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg, run)
    assert list(labels) == ["A01X", "A02X", "C01X", "RFIA1X",
                            "A01Y", "A02Y", "C01Y", "RFIA1Y"]
    assert run["kotekan_file_reason"] is None
    assert list(np.nonzero(~good)[0]) == [7]           # RFIA1Y, by label
    assert flagged_by == {"RFIA1Y": ["power-outlier"]}
    assert degraded == []
    rep = run["sources"][0]
    assert rep["status"] == "ok" and rep["n_measured"] == 3
    assert rep["detail"]["n_file_elements"] == 4
    assert rep["detail"]["n_in_file"] == 4
    assert rep["detail"]["n_not_in_file"] == 4


def test_missing_dishes_bad_and_rfi_never_power_flagged_end_to_end(tmp_path, monkeypatch):
    """The production shape with the config's dish types: the dish-type
    source flags every element of a Missing dish (the subset file never
    carries them), and power-outlier leaves the RFI antennas alone even
    when one reads dead."""
    cfg_dict = {
        "num_dishes": 4, "num_polarizations": 2,
        "telescope": {"dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A01"},
            {"dish_idx": 1, "type": "Missing", "label": "E01"},
            {"dish_idx": 2, "type": "Missing", "label": "H04"},
            {"dish_idx": 3, "type": "RFIDish", "label": "RFIA1"}]},
    }
    monkeypatch.setattr(bffs, "choco_group_config", lambda url, group: cfg_dict)
    power = np.ones((2, 8, 4), "f4") * 10.0
    power[..., 3] = 0.0                                  # RFIA1Y reads dead
    n2 = tmp_path / "n2.h5"
    write_chord_n2(n2, ["A01", "RFIA1"], np.linspace(400, 800, 8), power,
                   num_elements=4)
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx", max_age=0,
                      sources=[{"kind": "dish-type"}, {"kind": "power-outlier"}])
    run = {}
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg, run)
    assert list(labels) == ["A01X", "E01X", "H04X", "RFIA1X",
                            "A01Y", "E01Y", "H04Y", "RFIA1Y"]
    assert list(np.nonzero(~good)[0]) == [1, 2, 5, 6]
    assert flagged_by == {lbl: ["dish-type"] for lbl in ("E01X", "H04X", "E01Y", "H04Y")}
    assert run["flag_reasons"]["E01X"] == {"dish-type": "type Missing"}
    assert degraded == []
    by_kind = {r["kind"]: r for r in run["sources"]}
    assert by_kind["dish-type"]["n_flagged"] == 4
    assert by_kind["power-outlier"]["n_measured"] == 2       # A01X, A01Y
    assert by_kind["power-outlier"]["detail"]["n_excluded"] == 2
    assert run["n_by_type"] == {"ArrayDish": 2, "Missing": 4, "RFIDish": 2}


def test_choco_config_fetch_failure_falls_back_to_file(tmp_path, monkeypatch):
    def boom(url, group):
        raise OSError("choco down")
    monkeypatch.setattr(bffs, "choco_group_config", boom)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert list(labels) == ["f0X", "f1X"]


# -- file-optional operation ------------------------------------------------


def test_missing_file_skips_file_sources_but_still_flags(tmp_path, monkeypatch):
    """No usable N² file: power-outlier is skipped, manual still flags,
    labels come from the kotekan config via choco."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["A3X"])
    cfg = bffs.Config(
        kotekan_file=str(tmp_path / "nope_*.h5"),
        url="https://localhost:5000", group="cx",
        sources=[{"kind": "power-outlier"},
                 {"kind": "manual", "path": str(manualf)}],
    )
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg)
    assert list(labels) == ["A1X", "MissingX", "A3X", "A1Y", "MissingY", "A3Y"]
    assert list(good) == [True, True, False, True, True, True]
    assert flagged_by == {"A3X": ["manual"]}


def test_all_sources_skipped_fails(tmp_path, monkeypatch):
    """Only file-based sources configured and no usable file: red badge."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    cfg = bffs.Config(
        kotekan_file=str(tmp_path / "nope_*.h5"),
        url="https://localhost:5000", group="cx",
        sources=[{"kind": "power-outlier"}],
    )
    try:
        bffs.combine_sources(cfg)
    except OSError as e:
        assert "nothing to measure" in str(e)
        return
    raise AssertionError("expected OSError when every source is skipped")


# -- attribution --------------------------------------------------------------


def test_flagged_by_names_every_flagging_source(tmp_path):
    n2 = tmp_path / "n2.h5"
    # f1 is dead in the data (power-outlier) and also manually flagged.
    auto = np.ones((4, 1, 2), "f4")
    auto[:, :, 1] = 0.0
    write_normalized(n2, ["f0", "f1"], [400.0], auto)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    cfg = bffs.Config(
        kotekan_file=str(n2),
        sources=[{"kind": "power-outlier"},
                 {"kind": "manual", "path": str(manualf)}],
    )
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg)
    assert list(good) == [True, False]
    assert flagged_by == {"f1X": ["power-outlier", "manual"]}


def test_state_records_flagged_by_and_payload_is_unchanged(tmp_path):
    """Attribution lands in the state file only — the payload keeps the
    exact {update_id, start_time, bad_inputs} shape kotekan validates."""
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    statef = tmp_path / "state.json"
    cfg = bffs.Config(
        kotekan_file=str(n2), state_path=str(statef),
        sources=[{"kind": "manual", "path": str(manualf)}],
    )
    payload, send, _ = bffs.run(cfg, now=1000.0)
    assert set(payload) == {"update_id", "start_time", "bad_inputs"}
    assert payload["bad_inputs"] == [1]
    assert all(isinstance(i, int) for i in payload["bad_inputs"])
    state = json.loads(statef.read_text())
    assert state["flagged_by"] == {"f1X": ["manual"]}


# -- per-source reports and the run file --------------------------------------


def _bare_source(monkeypatch, mask_values):
    """Register a source kind whose mask() returns a bare array (the
    pre-report protocol) — it must still be accepted as an ok report."""
    import types
    import sources
    mod = types.SimpleNamespace(mask=lambda src, labels, path: np.array(mask_values))
    monkeypatch.setattr(sources, "get",
                        lambda kind: mod if kind == "bare" else sources_get(kind))


sources_get = __import__("sources").get


def test_run_report_collects_one_entry_per_source(tmp_path, monkeypatch):
    n2 = tmp_path / "n2.h5"
    auto = np.ones((2, 4, 3), "f4") * 10.0
    auto[..., 1] = 900.0                                   # f1 is a power outlier
    write_normalized(n2, ["f0", "f1", "f2"], np.linspace(400, 800, 4), auto)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f2X"])
    _bare_source(monkeypatch, [True, True, True])
    cfg = bffs.Config(kotekan_file=str(n2), sources=[
        {"kind": "power-outlier", "nsigma": 5.0},
        {"kind": "manual", "path": str(manualf)},
        {"kind": "bare"},
    ])
    run = {}
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg, run)
    assert list(good) == [True, False, False]
    assert degraded == []
    assert run["kotekan_file"] == str(n2) and run["kotekan_file_reason"] is None
    assert run["kotekan_file_age_s"] >= 0 and run["n_elements"] == 3
    kinds = [r["kind"] for r in run["sources"]]
    assert kinds == ["power-outlier", "manual", "bare"]
    po, man, bare = run["sources"]
    assert po["status"] == "ok" and po["n_flagged"] == 1 and po["n_measured"] == 3
    assert po["detail"]["band_coverage"] == 1.0
    assert man["status"] == "ok" and man["n_flagged"] == 1 and man["n_measured"] == 3
    assert bare == {"kind": "bare", "status": "ok", "reason": None,
                    "n_measured": None, "n_flagged": 0, "detail": {}}


def test_skipped_source_is_in_the_run_report(tmp_path, monkeypatch):
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["A3X"])
    cfg = bffs.Config(
        kotekan_file=str(tmp_path / "nope_*.h5"),
        url="https://localhost:5000", group="cx",
        sources=[{"kind": "power-outlier"},
                 {"kind": "manual", "path": str(manualf)}],
    )
    run = {}
    _, _, _, degraded = bffs.combine_sources(cfg, run)
    assert run["kotekan_file"] is None
    assert "no file matches" in run["kotekan_file_reason"]
    po = run["sources"][0]
    assert po["status"] == "skipped" and po["n_measured"] == 0
    assert "no usable kotekan file" in po["reason"]
    assert degraded == ["no usable kotekan file — skipped: power-outlier"]
    assert run["degraded"] is degraded                      # the same list, live


def test_degraded_source_report_degrades_the_run(tmp_path, monkeypatch):
    """A source that ran but abstained (rfi with every endpoint down)
    makes the run degraded with its reason, while the other sources'
    flags still go out."""
    from sources import rfi
    monkeypatch.setattr(rfi, "read_sk", lambda url: (_ for _ in ()).throw(OSError("refused")))
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    cfg = bffs.Config(kotekan_file=str(n2), sources=[
        {"kind": "rfi", "urls": ["u0", "u1"], "max_stale_s": 0},
        {"kind": "manual", "path": str(manualf)},
    ])
    run = {}
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg, run)
    assert list(good) == [True, False]
    assert degraded == ["rfi: 2 of 2 /sk endpoints unreachable"]
    assert run["sources"][0]["status"] == "degraded"


def test_every_source_measuring_nothing_fails_the_run(tmp_path, monkeypatch):
    """Skipped for lack of a file plus abstained for lack of endpoints:
    nothing was measured anywhere, which must not pass as all-good."""
    from sources import rfi
    monkeypatch.setattr(rfi, "read_sk", lambda url: (_ for _ in ()).throw(OSError("refused")))
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    cfg = bffs.Config(
        kotekan_file=str(tmp_path / "nope_*.h5"),
        url="https://localhost:5000", group="cx",
        sources=[{"kind": "power-outlier"},
                 {"kind": "rfi", "urls": ["u0"], "max_stale_s": 0}],
    )
    run = {}
    try:
        bffs.combine_sources(cfg, run)
    except OSError as e:
        assert "nothing to measure" in str(e)
        assert [r["status"] for r in run["sources"]] == ["skipped", "degraded"]
        return
    raise AssertionError("expected OSError when no source measured anything")


def test_feed_reasons_reach_the_state_file(tmp_path, monkeypatch):
    """A source may say why it flagged each feed (the power source: off,
    or not in the PDB table); the reason rides into the run record and
    the state file beside flagged_by, never into the kotekan payload."""
    import types
    import sources
    from sources.common import report
    mod = types.SimpleNamespace(mask=lambda src, labels, path: (
        np.array([True, False, False]),
        report("ok", n_measured=3, feed_reasons={"f1X": "off", "f2X": "not in PDB table",
                                                 "f0X": "ignored: not flagged"})))
    monkeypatch.setattr(sources, "get", lambda kind: mod if kind == "pwr" else sources_get(kind))
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1", "f2"], [400.0], np.ones((1, 1, 3), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f2X"])
    statef = tmp_path / "state.json"
    cfg = bffs.Config(kotekan_file=str(n2), state_path=str(statef), sources=[
        {"kind": "pwr"}, {"kind": "manual", "path": str(manualf)}])
    run = {}
    payload, _, _ = bffs.run(cfg, now=1000.0, report=run)
    assert set(payload) == {"update_id", "start_time", "bad_inputs"}
    assert run["flag_reasons"] == {"f1X": {"pwr": "off"}, "f2X": {"pwr": "not in PDB table"}}
    state = json.loads(statef.read_text())
    assert state["flagged_by"] == {"f1X": ["pwr"], "f2X": ["pwr", "manual"]}
    assert state["flag_reasons"] == {"f1X": {"pwr": "off"}, "f2X": {"pwr": "not in PDB table"}}


def test_bad_source_report_is_a_bug(tmp_path, monkeypatch):
    import types
    import sources
    mod = types.SimpleNamespace(
        mask=lambda src, labels, path: (np.ones(1, bool), {"status": "weird"}))
    monkeypatch.setattr(sources, "get", lambda kind: mod)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    try:
        bffs.combine_sources(bffs.Config(kotekan_file=str(n2), sources=[{"kind": "x"}]))
    except ValueError as e:
        assert "weird" in str(e)
        return
    raise AssertionError("expected ValueError for an unknown report status")


def _run_file_config(tmp_path, n2, sources, state=True):
    # the state directory is not in the config: the tests' conftest puts
    # systemd's $STATE_DIRECTORY at tmp_path/state
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"kotekan_file": str(n2), "sources": sources}))
    return cfg_file


def test_state_dir_is_a_convention_not_a_setting(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"kotekan_file": "n2.h5"}))
    # systemd's StateDirectory (set by the conftest) ...
    cfg = bffs.load_config(cfg_file)
    assert cfg.state_dir == str(tmp_path / "state")
    assert cfg.state_path == str(tmp_path / "state" / "state.json")
    assert cfg.run_path == str(tmp_path / "state" / "run.json")
    # ... unless --state-dir says otherwise ...
    cfg = bffs.load_config(cfg_file, state_dir=tmp_path / "elsewhere")
    assert cfg.run_path == str(tmp_path / "elsewhere" / "run.json")
    # ... and without either it is /var/lib/choco/bffs
    monkeypatch.delenv("STATE_DIRECTORY")
    assert bffs.load_config(cfg_file).state_dir == "/var/lib/choco/bffs"


@pytest.mark.parametrize("state", [
    {"path": "/var/lib/choco/bffs/state.json"},
    {"run_path": "/tmp/run.json"},
    {"path": "/x/state.json", "run_path": "/x/run.json", "max_history": 5},
])
def test_retired_state_paths_are_refused(tmp_path, state):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"kotekan_file": "n2.h5", "state": state}))
    with pytest.raises(ValueError, match="retired"):
        bffs.load_config(cfg_file)


def test_max_history_still_configurable(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"kotekan_file": "n2.h5",
                                    "state": {"max_history": 7}}))
    assert bffs.load_config(cfg_file).max_history == 7


def test_manual_source_defaults_into_the_state_dir(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    sdir = tmp_path / "state"
    sdir.mkdir()
    (sdir / "manual_overrides.yaml").write_text("bad_inputs: [f1X]\n")
    cfg_file = _run_file_config(tmp_path, n2, [{"kind": "manual"}])
    assert bffs.main(["--config", str(cfg_file)]) == 0
    run = json.loads((sdir / "run.json").read_text())
    assert run["n_bad"] == 1
    assert run["sources"][0]["detail"]["path"] == str(sdir / "manual_overrides.yaml")


def test_main_writes_the_run_file_on_an_ok_run(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1X"])
    cfg_file = _run_file_config(tmp_path, n2, [{"kind": "manual", "path": str(manualf)}])
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 0
    run = json.loads((tmp_path / "state" / "run.json").read_text())
    assert run["status"] == "ok" and run["exit_code"] == 0 and run["error"] is None
    assert run["degraded"] == [] and run["n_bad"] == 1 and run["n_elements"] == 2
    assert run["sent"] is False                     # no choco url configured
    assert run["dry_run"] is False
    assert run["update_id"].startswith("bffs-")
    assert run["kotekan_file"] == str(n2)
    assert [s["kind"] for s in run["sources"]] == ["manual"]
    # the state file keeps its on-change semantics: written once, same run
    assert (tmp_path / "state" / "state.json").exists()


def test_main_writes_the_run_file_on_a_degraded_run(tmp_path, monkeypatch):
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _PER_DISH_CONFIG)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["A3X"])
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(tmp_path / "nope_*.h5"),
        "choco": {"url": "https://localhost:5000", "group": "cx"},
        "sources": [{"kind": "power-outlier"},
                    {"kind": "manual", "path": str(manualf)}],
    }))
    sent = []
    monkeypatch.setattr(bffs, "send_to_choco", lambda cfg, payload: sent.append(payload))
    rc = bffs.main(["--config", str(cfg_file), "--state-dir", str(tmp_path)])
    assert rc == 2
    run = json.loads((tmp_path / "run.json").read_text())
    assert run["status"] == "degraded" and run["exit_code"] == 2
    assert run["degraded"] == ["no usable kotekan file — skipped: power-outlier"]
    assert run["sent"] is True and len(sent) == 1
    assert run["kotekan_file"] is None and "no file matches" in run["kotekan_file_reason"]
    assert [s["status"] for s in run["sources"]] == ["skipped", "ok"]


def test_main_writes_the_run_file_on_a_failed_run(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg_file = _run_file_config(tmp_path, n2, [{"kind": "nope"}])
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 1
    run = json.loads((tmp_path / "state" / "run.json").read_text())
    assert run["status"] == "failed" and run["exit_code"] == 1
    assert "unknown source kind" in run["error"]
    assert run["sources"] == [] and run["n_elements"] == 1   # partial record kept
    assert not (tmp_path / "state" / "state.json").exists()


def test_dry_run_writes_no_run_file(tmp_path):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg_file = _run_file_config(tmp_path, n2, [])
    assert bffs.main(["--config", str(cfg_file), "--dry-run"]) == 0
    assert not (tmp_path / "state" / "run.json").exists()


def test_unwritable_run_file_does_not_change_the_exit_code(tmp_path, caplog):
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg_file = _run_file_config(tmp_path, n2, [])
    (tmp_path / "state" / "run.json").mkdir(parents=True)   # a directory cannot be replaced
    assert bffs.main(["--config", str(cfg_file)]) == 0
    assert "could not write run file" in caplog.text
    assert (tmp_path / "state" / "state.json").exists()
