"""Tests for the EOP merge policy in jobs/eop/eop_update.py.

The job script lives outside the ``choco`` package, so we put it on
``sys.path`` before importing.  Only the pure-policy parts are exercised
here: the astropy/numpy machinery that talks to IERS and the fpga_master
TCP socket is left to integration runs.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "jobs" / "eop"))

from astropy.time import Time  # noqa: E402

import eop_update  # noqa: E402


def _entry(t, tag):
    """Minimal EOP-like dict — only the fields the merge policy looks at."""
    return {"t_inst_ns": t, "tag": tag}


# Synthetic instrument timestamps.  Values are integers; the merge does
# not care about real time, only ordering and equality.
DAY = 86_400 * 10**9  # one day in ns
T_M5 = -5 * DAY
T_M4 = -4 * DAY
T_M3 = -3 * DAY
T_M2 = -2 * DAY
T_M1 = -1 * DAY
T_0  = 0
T_P1 = 1 * DAY
T_P2 = 2 * DAY
T_P3 = 3 * DAY
T_P5 = 5 * DAY


# "Now" sits at zero in all tests below; pick lower_inst_ns explicitly per case.
NOW = T_0


class TestMergeTables:
    def test_truncation_applied_when_kept_brackets_now(self):
        """Old past entry dropped; surviving stored still has entries on both sides of now."""
        stored = [_entry(T_M3, "s"), _entry(T_M2, "s"),
                  _entry(T_M1, "s"), _entry(T_P1, "s")]
        fresh = []
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        assert [e["t_inst_ns"] for e in out] == [T_M2, T_M1, T_P1]

    def test_truncation_skipped_when_kept_has_no_before_now(self):
        """If truncation would leave stored with only future entries, keep all stored."""
        stored = [_entry(T_M5, "s"), _entry(T_M4, "s"), _entry(T_P1, "s")]
        fresh = []
        # lower=T_M2 would drop M5 and M4, leaving only [P1] -> no entry <= now.
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        # Truncation skipped -> all of stored kept.
        assert [e["t_inst_ns"] for e in out] == [T_M5, T_M4, T_P1]

    def test_truncation_skipped_when_kept_has_no_after_now(self):
        """If truncation would leave stored with only past entries, keep all stored."""
        stored = [_entry(T_M3, "s"), _entry(T_M2, "s"), _entry(T_M1, "s")]
        fresh = []
        # lower=T_M2 keeps [M2, M1], both <= now -> no entry >= now -> skip.
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        assert [e["t_inst_ns"] for e in out] == [T_M3, T_M2, T_M1]

    def test_preserves_stored_past_fresh_window(self):
        """Stored entries beyond the fresh upper bound are kept untouched."""
        stored = [_entry(T_M1, "s"), _entry(T_P3, "s"), _entry(T_P5, "s")]
        fresh = [_entry(T_M2, "f"), _entry(T_0, "f"), _entry(T_P2, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        # Truncation OK (M1 <= now, P3 >= now).  Nothing fresh is later than P5.
        assert [e["t_inst_ns"] for e in out] == [T_M1, T_P3, T_P5]
        assert all(e["tag"] == "s" for e in out)

    def test_does_not_fill_gaps_in_stored(self):
        """A gap in stored is preserved; fresh entries inside that gap are dropped."""
        stored = [_entry(T_M2, "s"), _entry(T_P2, "s")]  # gap at -1, 0, +1
        fresh = [_entry(T_M1, "f"), _entry(T_0, "f"), _entry(T_P1, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        assert [e["t_inst_ns"] for e in out] == [T_M2, T_P2]
        assert all(e["tag"] == "s" for e in out)

    def test_appends_only_after_last_stored(self):
        """Fresh entries strictly after the latest stored timestamp are appended."""
        stored = [_entry(T_M2, "s"), _entry(T_M1, "s"), _entry(T_0, "s")]
        fresh = [_entry(T_M2, "f"), _entry(T_M1, "f"), _entry(T_0, "f"),
                 _entry(T_P1, "f"), _entry(T_P2, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        ts = [e["t_inst_ns"] for e in out]
        assert ts == [T_M2, T_M1, T_0, T_P1, T_P2]
        tag_by_t = {e["t_inst_ns"]: e["tag"] for e in out}
        for t in (T_M2, T_M1, T_0):
            assert tag_by_t[t] == "s"
        for t in (T_P1, T_P2):
            assert tag_by_t[t] == "f"

    def test_never_overwrites_on_boundary_duplicate(self):
        """A fresh entry at the same timestamp as last stored is not appended."""
        stored = [_entry(T_M1, "s"), _entry(T_0, "s")]
        fresh = [_entry(T_0, "f"), _entry(T_P1, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        ts = [e["t_inst_ns"] for e in out]
        assert ts == [T_M1, T_0, T_P1]
        tag_by_t = {e["t_inst_ns"]: e["tag"] for e in out}
        assert tag_by_t[T_0] == "s"  # not overwritten by fresh
        assert tag_by_t[T_P1] == "f"

    def test_result_is_sorted_with_unsorted_inputs(self):
        stored = [_entry(T_0, "s"), _entry(T_M1, "s")]
        fresh = [_entry(T_P2, "f"), _entry(T_P1, "f"), _entry(T_M2, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        ts = [e["t_inst_ns"] for e in out]
        assert ts == sorted(ts)
        assert ts == [T_M1, T_0, T_P1, T_P2]

    def test_full_truncation_keeps_all_stored(self):
        """When every stored entry is older than the cutoff, truncation is skipped."""
        stored = [_entry(T_M3, "s")]
        fresh = [_entry(T_M2, "f"), _entry(T_M1, "f"), _entry(T_0, "f")]
        out = eop_update.merge_tables(stored, fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        # M3 is preserved; fresh is appended only strictly after M3.
        assert [e["t_inst_ns"] for e in out] == [T_M3, T_M2, T_M1, T_0]
        tag_by_t = {e["t_inst_ns"]: e["tag"] for e in out}
        assert tag_by_t[T_M3] == "s"
        for t in (T_M2, T_M1, T_0):
            assert tag_by_t[t] == "f"

    def test_empty_stored_falls_through_to_fresh(self):
        """An empty stored table -> just use fresh (caller's responsibility to scope it)."""
        fresh = [_entry(T_M1, "f"), _entry(T_0, "f"), _entry(T_P1, "f")]
        out = eop_update.merge_tables([], fresh,
                                      lower_inst_ns=T_M2, now_inst_ns=NOW)
        assert [e["t_inst_ns"] for e in out] == [T_M1, T_0, T_P1]


class TestComputeNowInstNs:
    def test_now_inst_ns_matches_frame0_offset(self):
        """Identity check: t_now == t_frame0 gives exactly frame0_ns back."""
        frame0_ns = 1_700_000_000_000_000_000
        t_frame0 = eop_update.eop_utils.calc_astropy_time_from_unix_ns(frame0_ns)
        assert eop_update.compute_now_inst_ns(frame0_ns, t_frame0) == frame0_ns

    def test_now_inst_ns_advances_with_t_now(self):
        frame0_ns = 1_700_000_000_000_000_000
        t1 = Time("2026-05-19T00:00:00", scale="utc", precision=9)
        t2 = Time("2026-05-19T00:00:01", scale="utc", precision=9)
        delta = (eop_update.compute_now_inst_ns(frame0_ns, t2)
                 - eop_update.compute_now_inst_ns(frame0_ns, t1))
        # One TAI second between the two; no leap second on this date.
        assert delta == 10**9


class TestComputeLowerCutoffNs:
    def test_cutoff_is_n_before_days_earlier_than_today(self):
        """At UTC midnight today, cutoff = frame0_ns when n_before=0; -1d for n=1."""
        # Use a fixed reference time at UTC midnight so the math is exact.
        t_now = Time("2026-05-19T00:00:00", scale="utc", precision=9)
        # frame0_ns chosen arbitrarily; integer values are what matter.
        frame0_ns = 1_700_000_000_000_000_000

        cutoff_0 = eop_update.compute_lower_cutoff_ns(frame0_ns, 0, t_now)
        cutoff_2 = eop_update.compute_lower_cutoff_ns(frame0_ns, 2, t_now)

        # n_before=2 should give a cutoff exactly 2 days earlier (in TAI ns).
        delta = cutoff_0 - cutoff_2
        assert delta == 2 * 86_400 * 10**9

    def test_cutoff_floors_to_utc_midnight(self):
        """Sub-day offsets in t_now shouldn't shift the cutoff away from midnight."""
        t_morning = Time("2026-05-19T08:30:00", scale="utc", precision=9)
        t_evening = Time("2026-05-19T23:59:59", scale="utc", precision=9)
        frame0_ns = 1_700_000_000_000_000_000
        c_morning = eop_update.compute_lower_cutoff_ns(frame0_ns, 2, t_morning)
        c_evening = eop_update.compute_lower_cutoff_ns(frame0_ns, 2, t_evening)
        assert c_morning == c_evening
