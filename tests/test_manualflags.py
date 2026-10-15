"""choco.manualflags: the bffs manual override file."""


class TestManualFlags:
    """services.read_manual_flags / toggle_manual_flag: the bffs manual
    override file as the job's manual source reads it."""

    def test_missing_file_is_none(self, tmp_path):
        from choco.manualflags import read_manual_flags
        assert read_manual_flags(tmp_path / "nope.yaml") is None
        assert read_manual_flags(None) is None

    def test_reads_both_shapes(self, tmp_path):
        from choco.manualflags import read_manual_flags
        f = tmp_path / "m.yaml"
        f.write_text("bad_inputs: [A1X, B2Y]\n")
        assert read_manual_flags(f) == {"A1X", "B2Y"}
        f.write_text("[A1X]\n")
        assert read_manual_flags(f) == {"A1X"}
        f.write_text('{"bad_inputs": ["C3X"]}')              # JSON is YAML
        assert read_manual_flags(f) == {"C3X"}
        f.write_text("")
        assert read_manual_flags(f) == set()

    def test_garbage_raises(self, tmp_path):
        import pytest
        from choco.manualflags import read_manual_flags
        f = tmp_path / "m.yaml"
        f.write_text("bad_inputs: [A1X\n")
        with pytest.raises(ValueError):
            read_manual_flags(f)
        f.write_text("just a string\n")
        with pytest.raises(ValueError, match="expected a bad_inputs list"):
            read_manual_flags(f)

    def test_toggle_round_trip_is_atomic_and_keeps_keys(self, tmp_path):
        from choco.manualflags import read_manual_flags, toggle_manual_flag
        f = tmp_path / "deep" / "m.yaml"
        assert toggle_manual_flag(f, "A1X") is True            # created, parent too
        assert read_manual_flags(f) == {"A1X"}
        f.write_text("owner: ops\nbad_inputs: [A1X]\n")
        assert toggle_manual_flag(f, "B1X") is True
        assert toggle_manual_flag(f, "A1X") is False
        import yaml
        assert yaml.safe_load(f.read_text()) == {"owner": "ops", "bad_inputs": ["B1X"]}
        assert not list(f.parent.glob("*.tmp"))                # no temp file left behind
        assert (f.stat().st_mode & 0o777) == 0o644

    def test_toggle_refuses_an_unparseable_file(self, tmp_path):
        import pytest
        from choco.manualflags import toggle_manual_flag
        f = tmp_path / "m.yaml"
        f.write_text("bad_inputs: [A1X\n")
        with pytest.raises(ValueError):
            toggle_manual_flag(f, "B1X")
        assert f.read_text() == "bad_inputs: [A1X\n"
