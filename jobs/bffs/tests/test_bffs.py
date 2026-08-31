"""Tests for the bffs core (config, combine, state, CLI) — run with `pytest`."""

import json

import numpy as np

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
    write_manual(manualf, ["f2"])

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
    write_manual(manualf, ["f1"])
    payload, send, _ = bffs.run(cfg, now=1002.0)
    assert send is True and payload["bad_inputs"] == [1]
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == ["f1"]
    assert len(st["history"]) == 2
    assert st["history"][-1]["became_bad"] == ["f1"]
    assert st["history"][-1]["became_good"] == []

    # feed 1 recovers -> send, recorded as became_good.
    write_manual(manualf, [])
    _, send, _ = bffs.run(cfg, now=1003.0)
    assert send is True
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == []
    assert st["history"][-1]["became_good"] == ["f1"]


def test_force_sends_when_unchanged(tmp_path):
    n2, statef, manualf = tmp_path / "n2.h5", tmp_path / "state.json", tmp_path / "manual.yaml"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    write_manual(manualf, ["f1"])
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

    for i, bad in enumerate([[], ["f1"], []]):  # three changes
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
    write_manual(manualf, ["f1"])
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
    assert list(labels) == ["new0", "new1"]


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
    write_manual(manualf, ["f1"])
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
    assert json.loads(statef.read_text())["bad_inputs"] == ["f1"]


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
                        lambda url, group: _DISH_CONFIG)
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
    labels, good, _, _ = bffs.combine_sources(
        bffs.Config(kotekan_file=str(tmp_path / "acq_*" / "*.h5"), max_age=0))
    assert list(labels) == ["new0", "new1"]


def test_choco_context_injected_into_sources(tmp_path, monkeypatch):
    """combine_sources merges choco url/group into each source's config."""
    from sources import rfi
    seen = {}
    node = {"name": "cx1", "host": "cx1.example", "port": 12048, "started": True}
    monkeypatch.setattr(rfi, "choco_group_nodes",
                        lambda url, group: seen.update(url=url, group=group) or [node])
    monkeypatch.setattr(rfi, "read_sk", lambda url: {})
    monkeypatch.setattr(bffs, "choco_group_config", lambda url, group: {})
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0"], [400.0], np.ones((1, 1, 1), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx", sources=[{"kind": "rfi"}])
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert seen == {"url": "https://localhost:5000", "group": "cx"}
    assert list(good) == [True]


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
    assert list(labels) == ["f0"]


# -- labels from the kotekan config (dish_inputs) ---------------------------


_DISH_CONFIG = {
    "telescope": {
        "dish_inputs": [
            {"dish_idx": 0, "type": "ArrayDish", "label": "A1X"},
            {"dish_idx": 2, "type": "Fake", "label": "A3X"},
        ],
    },
}


def test_dish_input_labels_builds_element_table():
    # Slots without a dish_inputs entry are implicit Fake dishes.
    assert bffs.dish_input_labels(_DISH_CONFIG) == ["A1X", "Fake", "A3X"]
    assert bffs.dish_input_labels(_DISH_CONFIG, n_elements=5) == [
        "A1X", "Fake", "A3X", "Fake", "Fake"]


def test_dish_input_labels_out_of_range_raises():
    try:
        bffs.dish_input_labels(_DISH_CONFIG, n_elements=2)
    except ValueError as e:
        assert "ambiguous" in str(e)
        return
    raise AssertionError("expected ValueError for dish_idx beyond the axis")


def test_dish_input_labels_absent_is_none():
    assert bffs.dish_input_labels({"num_elements": 8}) is None


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

_PER_DISH_LABELS = ["A1X", "FakeX", "A3X", "A1Y", "FakeY", "A3Y"]


def test_element_labels_per_dish_expands_pol_blocks():
    # element = dish_idx + pol * num_dishes: the X block first, then Y,
    # placeholder dishes included in both.
    assert bffs.element_labels_from_config(_PER_DISH_CONFIG) == _PER_DISH_LABELS


def test_element_labels_per_element_dispatches_to_old_path():
    # Pol-suffixed labels mark the pre-2026-08 layout: positions are
    # element indices, no expansion.
    assert bffs.element_labels_from_config(_DISH_CONFIG) == \
        ["A1X", "Fake", "A3X"]


def test_element_labels_per_dish_file_agreement():
    got = bffs.element_labels_from_config(
        _PER_DISH_CONFIG, file_labels=_PER_DISH_LABELS)
    assert got == _PER_DISH_LABELS


def test_element_labels_per_dish_stale_file_refuses():
    # An old per-element file (unexpanded axis) against a per-dish
    # config means the file predates the cutover — refuse.
    try:
        bffs.element_labels_from_config(
            _PER_DISH_CONFIG, file_labels=["A1X", "Fake", "A3X"])
    except ValueError as e:
        assert "ambiguous" in str(e)
        return
    raise AssertionError("expected ValueError on a pre-cutover file")


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


def test_labels_from_choco_config_win(tmp_path, monkeypatch):
    """With choco available, dish_inputs names the elements; the file
    fixes the axis length (implicit Fake dishes beyond the entries)."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _DISH_CONFIG)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["x0", "x1", "x2", "x3"], [400.0],
                     np.ones((1, 1, 4), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert list(labels) == ["A1X", "Fake[1]", "A3X", "Fake[3]"]


def test_config_file_element_mismatch_refuses(tmp_path, monkeypatch):
    """A file shorter than the config's dish_idx range means the file
    predates the running config — refuse rather than send wrong indices."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _DISH_CONFIG)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["x0", "x1"], [400.0], np.ones((1, 1, 2), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    try:
        bffs.combine_sources(cfg)
    except ValueError as e:
        assert "ambiguous" in str(e)
        return
    raise AssertionError("expected ValueError on element-count mismatch")


def test_choco_config_fetch_failure_falls_back_to_file(tmp_path, monkeypatch):
    def boom(url, group):
        raise OSError("choco down")
    monkeypatch.setattr(bffs, "choco_group_config", boom)
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    cfg = bffs.Config(kotekan_file=str(n2), url="https://localhost:5000",
                      group="cx")
    labels, good, _, _ = bffs.combine_sources(cfg)
    assert list(labels) == ["f0", "f1"]


# -- file-optional operation ------------------------------------------------


def test_missing_file_skips_file_sources_but_still_flags(tmp_path, monkeypatch):
    """No usable N² file: power-outlier is skipped, manual still flags,
    labels come from the kotekan config via choco."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _DISH_CONFIG)
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["A3X"])
    cfg = bffs.Config(
        kotekan_file=str(tmp_path / "nope_*.h5"),
        url="https://localhost:5000", group="cx",
        sources=[{"kind": "power-outlier"},
                 {"kind": "manual", "path": str(manualf)}],
    )
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg)
    assert list(labels) == ["A1X", "Fake", "A3X"]
    assert list(good) == [True, True, False]
    assert flagged_by == {"A3X": ["manual"]}


def test_all_sources_skipped_fails(tmp_path, monkeypatch):
    """Only file-based sources configured and no usable file: red badge."""
    monkeypatch.setattr(bffs, "choco_group_config",
                        lambda url, group: _DISH_CONFIG)
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
    write_manual(manualf, ["f1"])
    cfg = bffs.Config(
        kotekan_file=str(n2),
        sources=[{"kind": "power-outlier"},
                 {"kind": "manual", "path": str(manualf)}],
    )
    labels, good, flagged_by, degraded = bffs.combine_sources(cfg)
    assert list(good) == [True, False]
    assert flagged_by == {"f1": ["power-outlier", "manual"]}


def test_state_records_flagged_by_and_payload_is_unchanged(tmp_path):
    """Attribution lands in the state file only — the payload keeps the
    exact {update_id, start_time, bad_inputs} shape kotekan validates."""
    n2 = tmp_path / "n2.h5"
    write_normalized(n2, ["f0", "f1"], [400.0], np.ones((1, 1, 2), "f4"))
    manualf = tmp_path / "manual.yaml"
    write_manual(manualf, ["f1"])
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
    assert state["flagged_by"] == {"f1": ["manual"]}
