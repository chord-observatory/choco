"""Tests for the dish-type source — run with `pytest`."""

import numpy as np

from sources import dish_type

_TYPES = {"A01X": "ArrayDish", "E01X": "Missing", "RFIA1X": "RFIDish",
          "A01Y": "ArrayDish", "E01Y": "Missing", "RFIA1Y": "RFIDish"}
_AXIS = np.array(list(_TYPES))


def test_missing_dishes_are_bad_by_construction():
    good, rep = dish_type.mask({"kind": "dish-type", "dish_types": _TYPES}, _AXIS, None)
    assert list(good) == [True, False, True, True, False, True]
    assert rep["status"] == "ok" and rep["n_measured"] == 6
    d = rep["detail"]
    assert d["feed_reasons"] == {"E01X": "type Missing", "E01Y": "type Missing"}
    assert d["type_counts"] == {"ArrayDish": 2, "Missing": 2, "RFIDish": 2}
    assert d["bad_types"] == ["Missing"]


def test_rfi_antennas_are_not_flagged_by_default():
    good, _ = dish_type.mask({"kind": "dish-type", "dish_types": _TYPES}, _AXIS, None)
    assert good[list(_AXIS).index("RFIA1X")] and good[list(_AXIS).index("RFIA1Y")]


def test_bad_types_are_configurable():
    good, rep = dish_type.mask(
        {"kind": "dish-type", "dish_types": _TYPES, "bad_types": ["Missing", "RFIDish"]},
        _AXIS, None)
    assert list(good) == [True, False, False, True, False, False]
    assert rep["detail"]["feed_reasons"]["RFIA1X"] == "type RFIDish"


def test_no_types_abstains():
    """No kotekan config (choco down, the file's own axis): nothing to
    judge — every element left good, degraded, n_measured 0."""
    good, rep = dish_type.mask({"kind": "dish-type", "dish_types": None}, _AXIS, None)
    assert good.all()
    assert rep["status"] == "degraded" and rep["n_measured"] == 0
    assert "kotekan config" in rep["reason"]


def test_untyped_labels_are_left_good_and_reported():
    types = {k: v for k, v in _TYPES.items() if k != "E01Y"}
    good, rep = dish_type.mask({"kind": "dish-type", "dish_types": types}, _AXIS, None)
    assert list(good) == [True, False, True, True, True, True]
    assert rep["status"] == "degraded" and rep["n_measured"] == 5
    assert rep["detail"]["n_untyped"] == 1 and rep["detail"]["untyped"] == ["E01Y"]
