"""Tests for the pointing job: the table's validation, the array rule,
and a run against a stubbed choco."""

import json
from unittest.mock import patch

import pytest
import yaml

import pointing
from choco.jobclient import Degraded


def table_text(values=None, extra_rows=(), header=None):
    """The site's CSV for the 64 pathfinder foundations, with its quirk:
    the signed field carries a line break after the sign."""
    values = dict(values or {})
    lines = [",".join(header or pointing.HEADER)]
    for n, f in enumerate(pointing.FOUNDATIONS):
        v = values.get(f, -27.3)
        mag = f"{abs(v):.4f}"
        if v < 0:
            lines.append(f'{f},{mag},S,"-\n{mag}",{n:03d}')
        elif v > 0:   # north: the site leaves the side blank
            lines.append(f"{f},{mag},,{mag},{n:03d}")
        else:
            lines.append(f"{f},0.0000,,0.0000,{n:03d}")
    lines.extend(extra_rows)
    return "\n".join(lines) + "\n"


class TestParse:
    def test_site_format(self):
        table = pointing.parse(table_text({"E1": 0.0, "A9": 30.0}))
        assert list(table) == pointing.FOUNDATIONS
        assert table["A1"] == -27.3 and table["E1"] == 0.0

    def test_rows_beyond_the_pathfinder_are_dropped(self):
        text = table_text(extra_rows=['A9,30.0000,S,"-\n30.0000",075',
                                      "J10,0.0000,,0.0000,107"])
        assert "A9" not in pointing.parse(text)

    def test_header_must_match(self):
        with pytest.raises(Degraded, match="header"):
            pointing.parse(table_text(header=["Foundation", "Angle"]))
        with pytest.raises(Degraded, match="header"):
            pointing.parse("")

    def test_field_patterns(self):
        bad = ["A1x,27.3000,S,-27.3000,006",      # foundation
               "A1,27.3,S,-27.3,006",              # four decimals
               "A1,27.3000,W,-27.3000,006",        # side
               "A1,27.3000,S,-27.3000,6",          # id
               "A1,27.3000,S,-27.3000,006,extra"]  # field count
        for row in bad:
            text = table_text().replace('A1,27.3000,S,"-\n27.3000",000', row)
            with pytest.raises(Degraded, match="line 2"):
                pointing.parse(text)

    def test_sign_and_magnitude_must_agree(self):
        for row in ["A1,27.3000,S,27.3000,006", "A1,27.3000,N,-27.3000,006",
                    "A1,27.3000,,-27.3000,006", "A1,27.3000,S,-28.3000,006"]:
            text = table_text().replace('A1,27.3000,S,"-\n27.3000",000', row)
            with pytest.raises(Degraded, match="does not agree"):
                pointing.parse(text)
        # North and zero both come with a blank side; N is accepted too.
        for row in ["A1,27.3000,,27.3000,006", "A1,27.3000,N,27.3000,006",
                    "A1,0.0000,,0.0000,006"]:
            text = table_text().replace('A1,27.3000,S,"-\n27.3000",000', row)
            assert pointing.parse(text)["A1"] == float(row.split(",")[3])

    def test_duplicate_and_missing(self):
        with pytest.raises(Degraded, match="listed twice"):
            pointing.parse(table_text(extra_rows=["A1,0.0000,,0.0000,006"]))
        text = table_text().replace('A1,27.3000,S,"-\n27.3000",000\n', "")
        with pytest.raises(Degraded, match="missing: A1"):
            pointing.parse(text)

    def test_angle_range(self):
        text = table_text().replace('A1,27.3000,S,"-\n27.3000",000',
                                    'A1,91.0000,S,"-\n91.0000",000')
        with pytest.raises(Degraded, match="does not agree"):
            pointing.parse(text)


class TestFetch:
    def test_bounded_and_ascii(self, monkeypatch):
        class Resp:
            def __init__(self, raw):
                self.raw = raw

            def read(self, n):
                return self.raw[:n]

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(pointing.urllib.request, "urlopen",
                            lambda req, timeout: Resp(b"x" * 11))
        with pytest.raises(Degraded, match="longer than 10"):
            pointing.fetch("http://x", 1, 10)
        monkeypatch.setattr(pointing.urllib.request, "urlopen",
                            lambda req, timeout: Resp("é".encode()))
        with pytest.raises(Degraded, match="not ASCII"):
            pointing.fetch("http://x", 1, 10)
        monkeypatch.setattr(pointing.urllib.request, "urlopen",
                            lambda req, timeout: Resp(b"ok"))
        assert pointing.fetch("http://x", 1, 10) == "ok"


class TestArrayValue:
    def test_all_agree(self):
        table = pointing.parse(table_text())
        assert pointing.array_value(table, 56) == (-27.3, [])

    def test_outliers_are_named_not_applied(self):
        table = pointing.parse(table_text({"E1": 0.0, "E7": 30.0}))
        assert pointing.array_value(table, 56) == (-27.3, ["E1", "E7"])

    def test_too_few_agree(self):
        moving = {f: -30.0 for f in pointing.FOUNDATIONS[:20]}
        table = pointing.parse(table_text(moving))
        value, outliers = pointing.array_value(table, 56)
        assert value is None and len(outliers) == 64


class TestUpdatedText:
    def test_first_write_has_the_header(self):
        text = pointing.updated_text("", -27.3)
        assert text.startswith("# vars.yaml: the Jinja context")
        assert "Do not edit it by hand" in text
        assert text.endswith("\ndish_coelev_deg: -27.3\n")
        assert yaml.safe_load(text) == {"dish_coelev_deg": -27.3}

    def test_hand_edits_and_comments_survive(self):
        text = ("# vars.yaml: the Jinja context every node's config renders with.\n"
                "# mine\n"
                "other: 1   # a hand-set value\n"
                "dish_coelev_deg: -27.3\n"
                "later: [a, b]\n")
        new = pointing.updated_text(text, -8.6)
        assert new == text.replace("dish_coelev_deg: -27.3", "dish_coelev_deg: -8.6")
        assert new.count("# vars.yaml:") == 1

    def test_key_appended_and_header_added_once(self):
        new = pointing.updated_text("other: 1\n", 0.0)
        assert new.startswith("# vars.yaml:") and new.endswith("other: 1\ndish_coelev_deg: 0.0\n")
        again = pointing.updated_text(new, -27.3)
        assert again.count("# vars.yaml:") == 1 and again.endswith("dish_coelev_deg: -27.3\n")
        assert pointing.updated_text("other: 1", 0.0).endswith("other: 1\ndish_coelev_deg: 0.0\n")

    def test_block_under_the_key_is_refused(self):
        with pytest.raises(ValueError):
            pointing.updated_text("dish_coelev_deg:\n  a: 1\n", -27.3)


class FakeChoco:
    """``get_json`` / ``put_json`` against an in-memory vars.yaml."""

    def __init__(self, vars_text=None):
        self.vars_text = vars_text
        self.puts = []

    def get_json(self, url, path, timeout=10.0):
        assert path == "/api/configs/vars.yaml"
        if self.vars_text is None:
            import urllib.error
            raise urllib.error.HTTPError(url, 404, "no", {}, None)
        return {"content": self.vars_text}

    def put_json(self, url, path, body, timeout=10.0):
        assert path == "/api/configs/vars.yaml"
        self.puts.append(body)
        self.vars_text = body["content"]
        return {"status": "saved", "reloaded": ["cx/cx1", "recv/recv1"],
                "paused": ["cx/cx1", "recv/recv1"] if body["pause"] else []}


@pytest.fixture
def choco():
    fake = FakeChoco()
    with patch.object(pointing, "get_json", fake.get_json), \
            patch.object(pointing, "put_json", fake.put_json):
        yield fake


@pytest.fixture
def site(monkeypatch):
    """The fetched text, settable per test."""
    holder = {"text": table_text()}
    monkeypatch.setattr(pointing, "fetch", lambda url, t, m: holder["text"])
    return holder


class TestRun:
    def test_first_write_creates_vars(self, choco, site, scratch_state_dir):
        rc = pointing.main(["--state-dir", str(scratch_state_dir)])
        assert rc == 0
        assert yaml.safe_load(choco.vars_text) == {"dish_coelev_deg": -27.3}
        assert choco.puts[0]["pause"] is True
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert run["status"] == "ok" and run["changed"] is True
        assert run["previous"] is None and run["paused"] == ["cx/cx1", "recv/recv1"]
        hist = [json.loads(l) for l in
                (scratch_state_dir / "history.jsonl").read_text().splitlines()]
        assert len(hist) == 1 and hist[0]["applied"] is True
        assert hist[0]["table"]["H8"] == -27.3

    def test_state_file_changes_with_the_table(self, choco, site, scratch_state_dir):
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 0
        state_file = scratch_state_dir / "state.json"
        state = json.loads(state_file.read_text())
        assert state["array_value"] == -27.3 and state["outliers"] == []
        assert state["table"]["A1"] == -27.3 and len(state["table"]) == 64
        first = state_file.stat().st_mtime_ns
        # The same table again: history grows, state.json is left alone.
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 0
        assert state_file.stat().st_mtime_ns == first
        assert len((scratch_state_dir / "history.jsonl").read_text().splitlines()) == 2
        # One dish moves: the table changed, state.json is rewritten.
        site["text"] = table_text({"E1": 0.0})
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 0
        state = json.loads(state_file.read_text())
        assert state["outliers"] == ["E1"] and state["table"]["E1"] == 0.0

    def test_unchanged_writes_nothing(self, choco, site, scratch_state_dir):
        choco.vars_text = "dish_coelev_deg: -27.3\nother: 1\n"
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 0
        assert choco.puts == []
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert run["changed"] is False and run["previous"] == -27.3

    def test_change_keeps_other_vars(self, choco, site, scratch_state_dir, tmp_path):
        choco.vars_text = "dish_coelev_deg: -27.3\nother: 1\n"
        site["text"] = table_text({f: -8.6 for f in pointing.FOUNDATIONS})
        cfg = tmp_path / "pointing.yaml"
        cfg.write_text("pause: false\n")
        assert pointing.main(["--config", str(cfg),
                              "--state-dir", str(scratch_state_dir)]) == 0
        assert yaml.safe_load(choco.vars_text) == {"dish_coelev_deg": -8.6, "other": 1}
        assert choco.vars_text.startswith("# vars.yaml:") and "other: 1\n" in choco.vars_text
        assert choco.puts[0]["pause"] is False

    def test_dry_run_writes_nothing(self, choco, site, scratch_state_dir):
        assert pointing.main(["-n", "--state-dir", str(scratch_state_dir)]) == 0
        assert choco.puts == [] and choco.vars_text is None
        assert not (scratch_state_dir / "run.json").exists()
        assert not (scratch_state_dir / "history.jsonl").exists()

    def test_invalid_table_is_degraded_and_changes_nothing(self, choco, site,
                                                           scratch_state_dir):
        site["text"] = "nonsense\n"
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 2
        assert choco.puts == []
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert run["status"] == "degraded" and "header" in run["reason"]
        assert not (scratch_state_dir / "history.jsonl").exists()

    def test_site_down_is_degraded(self, choco, scratch_state_dir, monkeypatch):
        def down(url, t, m):
            raise OSError("no route")
        monkeypatch.setattr(pointing, "fetch", down)
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 2
        assert choco.puts == []

    def test_dishes_moving_is_degraded(self, choco, site, scratch_state_dir):
        site["text"] = table_text({f: -30.0 for f in pointing.FOUNDATIONS[:20]})
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 2
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert "shared by 56" in run["reason"] and choco.puts == []

    def test_outliers_are_reported(self, choco, site, scratch_state_dir):
        site["text"] = table_text({"E1": 0.0})
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 0
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert run["outliers"] == ["E1"] and run["array_value"] == -27.3

    def test_bad_vars_is_failed(self, choco, site, scratch_state_dir):
        choco.vars_text = "- a list\n"
        assert pointing.main(["--state-dir", str(scratch_state_dir)]) == 1

    def test_config_errors(self, choco, site, scratch_state_dir, tmp_path):
        cfg = tmp_path / "pointing.yaml"
        cfg.write_text("min_agree: 65\n")
        assert pointing.main(["--config", str(cfg),
                              "--state-dir", str(scratch_state_dir)]) == 1
        run = json.loads((scratch_state_dir / "run.json").read_text())
        assert run["status"] == "failed" and "min_agree" in run["reason"]
        cfg.write_text("- not a mapping\n")
        assert pointing.main(["--config", str(cfg),
                              "--state-dir", str(scratch_state_dir)]) == 1
