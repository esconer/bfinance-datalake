"""Offline regression tests for T-P2A (consolidation phrasing in split regex).

NSE/BSE notices phrase a reverse split both as "Face Value Split ..." and as
"Face Value Consolidation ...". Both must parse to CONSOLIDATION with a
0.1 share multiplier (10 old shares -> 1 new share at 10x face value).
"""

from datalake.scrapers.universe_actions import parse_action_purpose


EXPECTED_CONSOLIDATION = {
    "ActionType": "CONSOLIDATION",
    "SplitFromFV": 1.0,
    "SplitToFV": 10.0,
    "ShareMultiplier": 0.1,
    "DividendPerShareRs": None,
}


def _assert_consolidation(text: str) -> None:
    result = parse_action_purpose(text)
    for key, expected in EXPECTED_CONSOLIDATION.items():
        assert result[key] == expected, f"{text!r}: {key}={result[key]!r}, expected {expected!r}"


def test_split_phrasing_consolidation_still_works():
    # Pre-existing behaviour must remain intact.
    _assert_consolidation("Face Value Split From Rs 1/- To Rs 10/-")


def test_consolidation_noun_phrasing():
    _assert_consolidation("Face Value Consolidation From Rs 1/- To Rs 10/-")


def test_consolidation_uppercase_phrasing():
    _assert_consolidation("FACE VALUE CONSOLIDATION FROM RS 1/- TO RS 10/-")


def test_consolidation_mixed_case_phrasing():
    _assert_consolidation("Face Value Consolidation from Rs 1/- to Rs 10/-")


def test_consolidate_verb_phrasing():
    _assert_consolidation("Company To Consolidate Face Value From Rs 1/- To Rs 10/-")


def test_subdivision_split_phrasing_still_works():
    # Forward split path must remain intact (10 -> 1, 10x multiplier).
    result = parse_action_purpose("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share")
    assert result["ActionType"] == "SPLIT"
    assert result["ShareMultiplier"] == 10.0
    assert result["SplitFromFV"] == 10.0
    assert result["SplitToFV"] == 1.0


def test_dividend_plus_consolidation_compound_notice():
    # The consolidation clause must be skipped by dividend extraction, so the
    # Rs 1 / Rs 10 face values are never mistaken for a payout.
    result = parse_action_purpose(
        "Interim Dividend - Rs 5 Per Share And Face Value Consolidation From Rs 1/- To Rs 10/-"
    )
    assert result["DividendPerShareRs"] == 5.0
    assert result["SplitFromFV"] == 1.0
    assert result["SplitToFV"] == 10.0
    assert result["ShareMultiplier"] == 0.1


def test_consolidated_results_adjective_does_not_swallow_dividend():
    # Rejection counterexample A: "consolidated" is common reporting boilerplate
    # and must not trigger the face-value-consolidation clause skip.
    for text, expected_div in (
        ("Dividend Rs 5 Per Share (Consolidated Results)", 5.0),
        ("Interim Dividend Rs 3 - consolidated declaration", 3.0),
        ("Dividend Rs 5 Per Share (Non-Consolidated)", 5.0),
        ("Dividend Rs 5 Per Share (Unconsolidated)", 5.0),
    ):
        result = parse_action_purpose(text)
        assert result["ActionType"] == "DIVIDEND", f"{text!r}: {result!r}"
        assert result["DividendPerShareRs"] == expected_div, f"{text!r}: {result!r}"
        assert result["ShareMultiplier"] == 1.0, f"{text!r}: {result!r}"


def test_consolidated_financials_are_not_share_consolidations():
    # Rejection counterexample B: no face-value context => never a multiplier change.
    for text, expected_type in (
        ("Consolidated results: revenue from 100 to 200", "OTHER"),
        ("Consolidated financial results: EPS from Rs 10 to Rs 20", "OTHER"),
        ("Consolidated Results For The Quarter Ended: Revenue From 100 To 200", "OTHER"),
        ("Consolidated statement of profit and loss: margin from 10 to 20", "OTHER"),
        ("Scheme of consolidation: authorized capital from Rs 100 to Rs 200", "DEMERGER"),
    ):
        result = parse_action_purpose(text)
        assert result["ActionType"] == expected_type, f"{text!r}: {result!r}"
        assert result["SplitFromFV"] is None, f"{text!r}: {result!r}"
        assert result["SplitToFV"] is None, f"{text!r}: {result!r}"
        assert result["ShareMultiplier"] == 1.0, f"{text!r}: {result!r}"


def test_dividend_with_consolidated_adjective_plus_face_value_consolidation():
    # Both signals must survive in a compound notice.
    result = parse_action_purpose(
        "Dividend Rs 5 Per Share (Consolidated Results) And Face Value Consolidation From Rs 1/- To Rs 10/-"
    )
    assert result["ActionType"] == "CONSOLIDATION"
    assert result["DividendPerShareRs"] == 5.0
    assert result["SplitFromFV"] == 1.0
    assert result["SplitToFV"] == 10.0
    assert result["ShareMultiplier"] == 0.1


def test_face_value_trigger_variants():
    for text in (
        "FV Consolidation From Rs 1/- To Rs 10/-",
        "F.V. Consolidation From Rs 1/- To Rs 10/-",
        "Face Value Of Shares Consolidation From Rs 1/- To Rs 10/-",
        "Consolidation Of Face Value From Rs 1/- To Rs 10/-",
    ):
        _assert_consolidation(text)


def test_forward_split_and_bonus_regressions_unchanged():
    result = parse_action_purpose("Face Value Split From Rs 10/- To Rs 5/-")
    assert result["ActionType"] == "SPLIT"
    assert result["ShareMultiplier"] == 2.0

    bonus = parse_action_purpose("Bonus 1:1")
    assert bonus["ActionType"] == "BONUS"
    assert bonus["ShareMultiplier"] == 2.0
