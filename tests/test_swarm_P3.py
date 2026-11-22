"""Offline regression tests for T-P3 (revised Patch 3)."""

from datalake.scrapers.universe_actions import parse_action_purpose


def test_multi_clause_dividend_is_summed():
    res = parse_action_purpose("Dividend Final Rs 2.80 And Special Rs 1.65 Per Share")
    assert res["ActionType"] == "DIVIDEND"
    assert res["DividendPerShareRs"] == 4.45


def test_bare_rs_special_after_dividend_context_is_summed():
    res = parse_action_purpose("Dividend Final Rs 3 And Special INR 1 Per Share")
    assert res["ActionType"] == "DIVIDEND"
    assert res["DividendPerShareRs"] == 4.0


def test_per_unit_payout_does_not_get_bare_rs_dividend():
    # "per unit" (e.g. mutual funds) must not trigger the new bare-Rs inference.
    res = parse_action_purpose("Dividend Final Rs 2.80 And Special Rs 1.65 Per Unit")
    assert res["DividendPerShareRs"] == 2.8
    assert res["ActionType"] == "DIVIDEND"


def test_bogus_bonus_denominator_is_capped():
    res = parse_action_purpose("Bonus 1:100000")
    assert res["BonusNew"] is None
    assert res["BonusHeld"] is None
    assert res["ShareMultiplier"] == 1.0
    assert res["ActionType"] == "OTHER"


def test_normal_bonus_still_works():
    res = parse_action_purpose("Bonus 1:1")
    assert res["ActionType"] == "BONUS"
    assert res["BonusNew"] == 1.0
    assert res["BonusHeld"] == 1.0
    assert res["ShareMultiplier"] == 2.0


def test_bonus_denominator_at_cap_boundary():
    res = parse_action_purpose("Bonus 1:100")
    assert res["ActionType"] == "BONUS"
    assert res["BonusHeld"] == 100.0
    assert res["ShareMultiplier"] == 1.01


def test_reverse_split_still_classified_as_consolidation():
    res = parse_action_purpose("Face Value Split (Consolidation) - From Rs 1/- Per Share To Rs 10/- Per Share")
    assert res["ActionType"] == "CONSOLIDATION"
    assert res["SplitFromFV"] == 1.0
    assert res["SplitToFV"] == 10.0
    assert res["ShareMultiplier"] == 0.1


def test_forward_split_still_classified_as_split():
    res = parse_action_purpose("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share")
    assert res["ActionType"] == "SPLIT"
    assert res["ShareMultiplier"] == 10.0
