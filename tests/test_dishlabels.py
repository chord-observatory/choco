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


def _config(rows, **top):
    return {"telescope": {"dish_inputs": rows, **top}}


def _row(idx, label, kind="ArrayDish"):
    return {"dish_idx": idx, "label": label, "type": kind}


class TestDishTable:
    def test_rows_go_to_their_dish_idx_and_gaps_are_missing(self):
        """Placed as CHORDTelescope places them: out of order and sparse
        must not shift the axis."""
        cfg = _config([_row(2, "A3"), _row(0, "R1", "RFIDish")], num_dishes=4)
        assert dl.dish_table(cfg) == (["R1", "Missing", "A3", "Missing"],
                                      ["RFIDish", "Missing", "ArrayDish", "Missing"], 2)

    def test_without_num_dishes_the_table_sizes_itself(self):
        cfg = _config([_row(2, "A3")], num_polarizations=1)
        assert dl.dish_table(cfg) == (["Missing", "Missing", "A3"],
                                      ["Missing", "Missing", "ArrayDish"], 1)

    def test_no_table_is_none(self):
        assert dl.dish_table({"a": 1}) is None and dl.dish_table(None) is None

    @pytest.mark.parametrize("rows,top,match", [
        (["A1"], {}, "not a mapping"),
        ([{"dish_idx": 0, "label": "A1"}], {}, "lacks"),           # kotekan requires type
        ([{"label": "A1", "type": "ArrayDish"}], {}, "lacks"),     # ... and dish_idx
        ([_row(1, "A1"), _row(1, "A2")], {}, "duplicated"),
        ([_row(-1, "A1")], {}, "negative"),
        ([_row(4, "A1")], {"num_dishes": 4}, "past num_dishes"),
        ([_row(0, "A1")], {"num_dishes": "2 * 32"}, "plain integer"),
        ([_row(0, "A1X"), _row(1, "A1Y")], {}, "per-element"),
    ])
    def test_a_table_kotekan_would_refuse_raises(self, rows, top, match):
        with pytest.raises(ValueError, match=match):
            dl.dish_table(_config(rows, **top))


class TestConnectedElements:
    TABLE = [_row(0, "B4"), _row(1, "Missing", "Missing"), _row(2, "A1"),
             _row(3, "RFIA1", "RFIDish")]

    def test_skips_missing_slots_in_pol_major_order(self):
        # Compact element c is fiducial element result[c]: X block first,
        # then the Y block at + num_dishes, Missing slots dropped from both.
        assert dl.connected_elements(_config(self.TABLE)) == [0, 2, 3, 4, 6, 7]

    def test_polarization_count(self):
        assert dl.connected_elements(_config(self.TABLE, num_polarizations=1)) == [0, 2, 3]
        assert dl.connected_elements(_config(self.TABLE, num_polarizations=3)) == \
            [0, 2, 3, 4, 6, 7, 8, 10, 11]

    def test_no_table_is_none(self):
        assert dl.connected_elements({"a": 1}) is None

    def test_live_pathfinder_table(self):
        # cx52, 2026-10: 16 ArrayDish and 8 RFIDish rows in 64 slots, both
        # pols — the 48-element compact axis kotekan reports.
        rows = [_row(d, f"A{d}") for d in range(16)] + \
            [_row(d, f"R{d}", "RFIDish") for d in range(56, 64)]
        out = dl.connected_elements(_config(rows, num_dishes=64))
        assert len(out) == 48
        assert out == list(range(16)) + list(range(56, 64)) \
            + list(range(64, 80)) + list(range(120, 128))


def test_file_labels_may_be_bytes():
    """h5py yields kotekan's variable-length strings as bytes."""
    assert dl.file_element_labels([b"B4p1", b"B4p2"], 2, [0, 1]) == ["B4X", "B4Y"]
