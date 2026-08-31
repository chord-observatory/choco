"""Tests for the master PDB channel map: parsing and the kotekan cross-check."""

import pytest

from choco.pdbmap import (
    PdbMap, PdbMapFile, cross_check, kotekan_dish_labels, load_pdb_map,
)


HEADER = "spi_bus,board,chip,channel,dish_input,amplifier,notes"


def write_map(tmp_path, body, name="pdb_map.csv"):
    path = tmp_path / name
    path.write_text(body)
    return path


class TestLoad:
    def test_parses_rows(self, tmp_path):
        path = write_map(tmp_path, f"{HEADER}\n0,0,A,0,A1X,AMP-1,first\n"
                                   f"0,15,B,7,B8Y,AMP-2,\n")
        m = load_pdb_map(path)
        assert m.errors == []
        assert m.n_entries == 2
        assert m.label(0, 0, "A", 0) == "A1X"
        assert m.label(0, 15, "B", 7) == "B8Y"
        entry = m.entry(0, 0, "A", 0)
        assert entry.amplifier == "AMP-1"
        assert entry.notes == "first"

    def test_unmapped_channel_is_none(self, tmp_path):
        path = write_map(tmp_path, f"{HEADER}\n0,0,A,0,A1X,,\n")
        m = load_pdb_map(path)
        assert m.label(0, 0, "A", 1) is None
        assert m.label(1, 0, "A", 0) is None

    def test_label_tolerates_junk_address(self, tmp_path):
        """Called once per grid cell — it must never raise."""
        path = write_map(tmp_path, f"{HEADER}\n0,0,A,0,A1X,,\n")
        m = load_pdb_map(path)
        assert m.label("x", 0, "A", 0) is None
        assert m.label(None, None, None, None) is None

    def test_comments_and_blank_lines_ignored(self, tmp_path):
        path = write_map(tmp_path,
                         f"# a heading\n\n{HEADER}\n"
                         f"# bus 0\n0,0,A,0,A1X,,\n\n")
        m = load_pdb_map(path)
        assert m.errors == []
        assert m.n_entries == 1

    @pytest.mark.parametrize("alias", ["correlator_input", "label"])
    def test_legacy_label_columns(self, tmp_path, alias):
        """bffs's vendored table calls the column correlator_input."""
        path = write_map(tmp_path,
                         f"spi_bus,board,chip,channel,{alias}\n0,0,A,0,A1X\n")
        m = load_pdb_map(path)
        assert m.errors == []
        assert m.label(0, 0, "A", 0) == "A1X"

    def test_missing_file_is_an_error_not_a_raise(self, tmp_path):
        m = load_pdb_map(tmp_path / "nope.csv")
        assert m.n_entries == 0
        assert m.errors and "FileNotFoundError" in m.errors[0]

    def test_bad_header_reported(self, tmp_path):
        path = write_map(tmp_path, "board,chip\n0,A\n")
        m = load_pdb_map(path)
        assert m.n_entries == 0
        assert "header must have columns" in m.errors[0]

    def test_empty_file_reported(self, tmp_path):
        path = write_map(tmp_path, "\n# nothing but a comment\n")
        m = load_pdb_map(path)
        assert m.errors == ["file is empty"]

    @pytest.mark.parametrize("row,fragment", [
        ("0,0,C,0,A1X,,", "out of range"),
        ("0,0,A,8,A1X,,", "out of range"),
        ("0,-1,A,0,A1X,,", "out of range"),
        ("0,0,A,x,A1X,,", "unparseable"),
        ("0,0,A,0,,,", "no dish_input value"),
    ])
    def test_bad_rows_are_skipped_not_fatal(self, tmp_path, row, fragment):
        path = write_map(tmp_path, f"{HEADER}\n{row}\n0,1,A,0,B1X,,\n")
        m = load_pdb_map(path)
        assert len(m.errors) == 1
        assert fragment in m.errors[0]
        # the good row still loaded
        assert m.label(0, 1, "A", 0) == "B1X"

    def test_error_line_numbers_match_the_file(self, tmp_path):
        path = write_map(tmp_path,
                         f"# comment\n{HEADER}\n0,0,A,0,A1X,,\n0,0,C,0,B1X,,\n")
        m = load_pdb_map(path)
        assert "line 4:" in m.errors[0]

    def test_duplicate_address_keeps_the_first(self, tmp_path):
        path = write_map(tmp_path,
                         f"{HEADER}\n0,0,A,0,A1X,,\n0,0,A,0,B1X,,\n")
        m = load_pdb_map(path)
        assert m.label(0, 0, "A", 0) == "A1X"
        assert "duplicate address" in m.errors[0]

    def test_to_list_emits_both_label_column_names(self, tmp_path):
        path = write_map(tmp_path, f"{HEADER}\n0,0,A,0,A1X,,\n")
        row = load_pdb_map(path).to_list()[0]
        assert row["dish_input"] == "A1X"
        assert row["correlator_input"] == "A1X"


class TestPdbMapFile:
    def test_reloads_when_the_file_changes(self, tmp_path):
        path = write_map(tmp_path, f"{HEADER}\n0,0,A,0,A1X,,\n")
        loader = PdbMapFile(path)
        assert loader.get().label(0, 0, "A", 0) == "A1X"
        # A rewrite big enough to change size; mtime resolution on some
        # filesystems is coarse, so the size is what makes this reliable.
        path.write_text(f"{HEADER}\n0,0,A,0,RENAMED_INPUT,,\n")
        assert loader.get().label(0, 0, "A", 0) == "RENAMED_INPUT"

    def test_missing_file_does_not_raise(self, tmp_path):
        loader = PdbMapFile(tmp_path / "absent.csv")
        assert loader.get().errors
        assert loader.get().n_entries == 0


class TestKotekanDishLabels:
    def test_per_dish_table_expands_to_element_labels(self):
        config = {"telescope": {"dish_inputs": [
            {"dish_idx": 0, "label": "B4", "type": "ArrayDish"},
            {"dish_idx": 1, "label": "C1", "type": "Fake"},
        ]}}
        sets = kotekan_dish_labels(config)
        # live = connected dishes only; known = every real position.
        assert sets["live"] == {"B4X", "B4Y"}
        assert sets["known"] == {"B4X", "B4Y", "C1X", "C1Y"}

    def test_placeholder_labels_dropped(self):
        config = {"dish_inputs": [
            {"dish_idx": 0, "label": "A1", "type": "ArrayDish"},
            {"dish_idx": 1, "label": "Fake"},
        ]}
        sets = kotekan_dish_labels(config)
        assert sets["known"] == {"A1X", "A1Y"}

    def test_entries_without_a_dish_idx_still_count(self):
        """Only the names matter; the element-axis position is bffs's problem."""
        config = {"dish_inputs": [{"label": "A1", "type": "ArrayDish"}]}
        assert kotekan_dish_labels(config)["live"] == {"A1X", "A1Y"}

    def test_no_table_is_none(self):
        assert kotekan_dish_labels({"a": 1}) is None
        assert kotekan_dish_labels(None) is None

    def test_garbage_entries_tolerated(self):
        config = {"dish_inputs": [{"dish_idx": "x", "label": "A1"}, "junk",
                                  {"no_label": 1}]}
        assert kotekan_dish_labels(config)["known"] == {"A1X", "A1Y"}

    def test_per_element_table_is_refused(self):
        """Pre-2026-08 tables carried a wrong element ordering.

        The check must not run against them: it raises, and the page
        reports a migrate-this-config reason instead of a verdict.
        """
        config = {"dish_inputs": [
            {"dish_idx": 0, "label": "A1X"},
            {"dish_idx": 1, "label": "A1Y"},
        ]}
        with pytest.raises(ValueError, match="per-element"):
            kotekan_dish_labels(config)


def _sets(live=(), known=()):
    """cross_check input: known always includes the live set."""
    return {"live": set(live), "known": set(known) | set(live)}


class TestCrossCheck:
    def _map(self, tmp_path, *rows):
        return load_pdb_map(write_map(
            tmp_path, HEADER + "\n" + "\n".join(rows) + "\n"))

    def test_agreement(self, tmp_path):
        m = self._map(tmp_path, "0,0,A,0,A1X,,", "0,0,A,1,A1Y,,")
        result = cross_check(m, _sets(live=["A1X", "A1Y"]))
        assert result["ok"] is True
        assert result["n_matched"] == 2
        assert result["missing_in_map"] == []
        assert result["unknown_to_kotekan"] == []

    def test_missing_in_map_counts_live_feeds_only(self):
        """A connected feed with no breaker is a problem; an unbuilt
        dish is not."""
        m = PdbMap()
        result = cross_check(m, _sets(live=["A1X"], known=["C1X"]))
        assert result["missing_in_map"] == ["A1X"]

    def test_unknown_to_kotekan_checks_all_known_positions(self, tmp_path):
        """Wiring for an existing-but-unconnected dish is legitimate;
        only a position kotekan has never heard of is stale."""
        m = self._map(tmp_path, "0,0,A,0,A1X,,", "0,0,A,1,C1X,,",
                      "0,0,A,2,GHOST,,")
        result = cross_check(m, _sets(live=["A1X"], known=["C1X"]))
        assert result["ok"] is False
        assert result["unknown_to_kotekan"] == ["GHOST"]

    def test_duplicate_labels(self, tmp_path):
        m = self._map(tmp_path, "0,0,A,0,A1X,,", "0,1,B,3,A1X,,")
        result = cross_check(m, _sets(live=["A1X"]))
        assert result["ok"] is False
        assert list(result["duplicate_labels"]) == ["A1X"]
        assert result["duplicate_labels"]["A1X"] == [
            "bus 0 board 0 chip A ch0", "bus 0 board 1 chip B ch3"]

    def test_no_kotekan_labels_is_not_ok(self, tmp_path):
        """Nothing to check against is not the same as agreement."""
        m = self._map(tmp_path, "0,0,A,0,A1X,,")
        assert cross_check(m, _sets())["ok"] is False

    def test_empty_map(self):
        result = cross_check(PdbMap(), _sets(live=["A1X"]))
        assert result["n_mapped"] == 0
        assert result["missing_in_map"] == ["A1X"]
