"""Tests for the bffs core (config, combine, state, CLI) — run with `pytest`."""

import json

import numpy as np

import bffs
from testhelpers import write_manual, write_normalized


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
    payload, _ = bffs.run(cfg, now=1_700_000_000.0)
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
    _, send = bffs.run(cfg, now=1000.0)
    assert send is True
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == [] and len(st["history"]) == 1

    # run 2: nothing changed -> no send, no new history.
    _, send = bffs.run(cfg, now=1001.0)
    assert send is False
    assert len(json.loads(statef.read_text())["history"]) == 1

    # feed 1 goes bad -> send, a new history entry naming the transition.
    write_manual(manualf, ["f1"])
    payload, send = bffs.run(cfg, now=1002.0)
    assert send is True and payload["bad_inputs"] == [1]
    st = json.loads(statef.read_text())
    assert st["bad_inputs"] == ["f1"]
    assert len(st["history"]) == 2
    assert st["history"][-1]["became_bad"] == ["f1"]
    assert st["history"][-1]["became_good"] == []

    # feed 1 recovers -> send, recorded as became_good.
    write_manual(manualf, [])
    _, send = bffs.run(cfg, now=1003.0)
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
    _, send = bffs.run(cfg, now=1001.0, force=True)  # unchanged, but forced
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
    _, send = bffs.run(_state_config(n2, statef, manualf), now=1000.0)
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
    labels, good = bffs.combine_sources(bffs.Config(kotekan_file=str(tmp_path / "n2_*.h5")))
    assert list(labels) == ["new0", "new1"]


def test_glob_kotekan_file_no_match_raises(tmp_path):
    try:
        bffs.combine_sources(bffs.Config(kotekan_file=str(tmp_path / "nope_*.h5")))
    except FileNotFoundError:
        return
    raise AssertionError("expected FileNotFoundError for unmatched glob")


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
    _, send = bffs.run(cfg, now=1001.0, sender=sent.append)
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


def test_main_missing_kotekan_file_fails_cleanly(tmp_path, caplog):
    """An expected environmental failure exits 1 with one log line, no traceback."""
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({
        "kotekan_file": str(tmp_path / "nope_*.h5"),
        "sources": [],
    }))
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 1
    assert "no kotekan file matches" in caplog.text


def test_main_bad_config_fails_cleanly(tmp_path, caplog):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(json.dumps({"sources": []}))  # no kotekan_file
    rc = bffs.main(["--config", str(cfg_file)])
    assert rc == 1
    assert "bad config" in caplog.text
