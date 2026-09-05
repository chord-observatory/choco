"""choco.dishlabels: the one copy of the per-element label layout."""

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


class TestLayout:
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

    def test_num_polarizations(self):
        assert dl.num_polarizations(4, 8) == 2
        assert dl.num_polarizations(4, 4) == 1
        assert dl.num_polarizations(4, 12) == 3
        assert dl.num_polarizations(4, 0) == 2      # attribute missing
        assert dl.num_polarizations(4, 6) == 2      # not a multiple
        assert dl.num_polarizations(0, 8) == 2
