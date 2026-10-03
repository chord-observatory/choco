"""choco.dishlabels: the one copy of the per-element label layout."""

import pytest

from choco import dishlabels as dl


class TestFindKey:
    def test_top_level_wins_over_nested(self):
        assert dl.find_key({"n": 1, "a": {"n": 2}}, "n") == 1

    def test_walks_dicts_and_lists(self):
        cfg = {"stages": [{"a": 1}, {"inner": {"dish_coelev_deg": -10.0}}]}
        assert dl.find_key(cfg, "dish_coelev_deg") == -10.0

    def test_falsy_values_are_found(self):
        assert dl.find_key({"x": 0.0}, "x") == 0.0
        assert dl.find_key({"a": {"b": 1}}, "x") is None


class TestFindDishInputs:
    def test_skips_empty_lists(self):
        cfg = {"dish_inputs": [], "telescope": {"dish_inputs": [{"label": "A1"}]}}
        assert dl.find_dish_inputs(cfg) == [{"label": "A1"}]

    def test_none_when_absent(self):
        assert dl.find_dish_inputs({"a": {"b": []}}) is None


class TestConfigTable:
    def test_per_element_conventions(self):
        assert dl.labels_are_per_element(["A1X", "Fake", "RFI01"])
        assert dl.labels_are_per_element(["d0_pA", "d0_pB"])
        assert not dl.labels_are_per_element(["A1", "D8", "Fake", "RFI01"])
        assert not dl.labels_are_per_element(["CHORD-A01", "CHORD-H08"])

    def test_expand_is_pol_major(self):
        assert dl.expand_dish_labels(["A1", "Fake", "A3"]) == \
            ["A1X", "FakeX", "A3X", "A1Y", "FakeY", "A3Y"]

    def test_expand_one_pol_is_identity(self):
        assert dl.expand_dish_labels(["A1", "A2"], 1) == ["A1", "A2"]

    def test_expand_beyond_xy_numbers_the_suffix(self):
        assert dl.expand_dish_labels(["A"], 3) == ["AX", "AY", "AP2"]
        assert [dl.pol_suffix(p) for p in range(3)] == ["X", "Y", "P2"]


class TestFileElementLabels:
    """kotekan's per-element ``index_map/label`` (chord.2021.10+988)."""

    # the 48-element subset file scaled down: two dishes, both pols, in
    # the file's own order
    LABELS = ["B4p1", "RFIA1p1", "B4p2", "RFIA1p2"]

    def test_p_suffix_becomes_xy_position_for_position(self):
        assert dl.file_element_labels(self.LABELS, 4, [0, 0, 1, 1]) == \
            ["B4X", "RFIA1X", "B4Y", "RFIA1Y"]

    def test_count_and_pol_index_are_optional(self):
        assert dl.file_element_labels(["Fakep1", "Fakep2"]) == ["FakeX", "FakeY"]

    def test_last_p_group_is_the_suffix_and_pols_beyond_two_are_numbered(self):
        assert dl.file_element_labels(["Ap1p2", "A1p3"]) == ["Ap1Y", "A1P2"]

    def test_count_mismatch_is_refused(self):
        # a per-dish table: half as many labels as elements
        with pytest.raises(ValueError, match="2 entries for 4 elements"):
            dl.file_element_labels(["A1", "B1"], 4)
        # the whole telescope table on a compact axis
        with pytest.raises(ValueError, match="4 entries for 2 elements"):
            dl.file_element_labels(self.LABELS, 2)

    def test_xy_suffix_is_read_on_the_word_of_the_pol_index(self):
        # kotekan PR #1695 (2026-09-22; in the data from 2026-10-01): the
        # polarization name is the suffix, so the file's text is already
        # choco's name — position for position, nothing re-derived.
        labels = ["A01X", "RFIB4X", "A01Y", "RFIB4Y"]
        assert dl.file_element_labels(labels, 4, [0, 0, 1, 1]) == labels

    def test_xy_suffix_without_a_pol_index_is_refused(self):
        # By text alone this is the pre-2026-08 per-element layout, whose
        # element order was wrong; only index_map/pol tells them apart.
        with pytest.raises(ValueError, match="no index_map/pol to vouch"):
            dl.file_element_labels(["A1X", "B1X", "A1Y", "B1Y"], 4)
        with pytest.raises(ValueError, match="no index_map/pol to vouch"):
            dl.file_element_labels(["A01X", "A01Y"])

    def test_labels_without_either_suffix_are_refused(self):
        with pytest.raises(ValueError, match="no polarization suffix"):
            dl.file_element_labels(["d0_pA", "d0_pB"])
        with pytest.raises(ValueError, match="'A1p0'"):
            dl.file_element_labels(["A1p0"])       # polarizations count from 1
        with pytest.raises(ValueError, match="'X'"):
            dl.file_element_labels(["X"], 1, [0])  # a suffix needs a dish

    def test_pol_index_disagreeing_with_the_suffix_is_refused(self):
        with pytest.raises(ValueError, match="index_map/pol says 1"):
            dl.file_element_labels(["A1p1", "A1p2"], 2, [1, 0])
        with pytest.raises(ValueError, match="index_map/pol says 1"):
            dl.file_element_labels(["A01X", "A01Y"], 2, [1, 0])
        with pytest.raises(ValueError, match="index_map/pol has 1 entries"):
            dl.file_element_labels(["A1p1", "A1p2"], 2, [0])

    def test_empty_table(self):
        assert dl.file_element_labels([], 0) == []


class TestConnectedElements:
    TABLE = [{"label": "B4", "type": "ArrayDish"},
             {"label": "Missing", "type": "Missing"},
             {"label": "A1", "type": "ArrayDish"},
             {"label": "RFIA1", "type": "RFIDish"}]

    def test_skips_missing_rows_in_pol_major_order(self):
        # Compact element c is fiducial element result[c]: X block first,
        # then the Y block at + num_dishes, Missing rows dropped from both.
        assert dl.connected_elements(self.TABLE) == [0, 2, 3, 4, 6, 7]

    def test_only_an_explicit_missing_type_is_dropped(self):
        table = [{"label": "A1"}, "A2", {"label": "A3", "type": "Missing"}]
        assert dl.connected_elements(table, 1) == [0, 1]

    def test_polarization_count(self):
        assert dl.connected_elements(self.TABLE, 1) == [0, 2, 3]
        assert dl.connected_elements(self.TABLE, 3) == \
            [0, 2, 3, 4, 6, 7, 8, 10, 11]
        with pytest.raises(ValueError):
            dl.connected_elements(self.TABLE, 0)

    def test_live_pathfinder_table(self):
        # cx52, 2026-10: 16 ArrayDish + 8 RFIDish rows among 64, both
        # pols — the 48-element compact axis kotekan reports.
        table = [{"type": "ArrayDish"}] * 16 + [{"type": "Missing"}] * 40 \
            + [{"type": "RFIDish"}] * 8
        out = dl.connected_elements(table)
        assert len(out) == 48
        assert out == list(range(16)) + list(range(56, 64)) \
            + list(range(64, 80)) + list(range(120, 128))

