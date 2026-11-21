"""Offline regression tests for swarm bug T-D2.

D2-A: Upstox historical closes are already split-adjusted while NSE dividends are
      quoted in unadjusted pre-split rupees, so their share basis must be matched.
D2-B: Reverse splits / share consolidations (old FV < new FV) must be parsed.
D2-C: Callers can request split-adjusted-only series via adjust_dividends=False.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.scrapers.universe_actions import parse_action_purpose


def _upstox_frame() -> pd.DataFrame:
    """2008-2015-style Upstox rows (PrevClose/TurnoverValue NaN, prices already split-adjusted).

    A 1 -> 10 split happens later in the window (2016-01-01), so the unmodified
    cumulative split factor on the early rows is 1/10.
    """
    return pd.DataFrame([
        {"Date": pd.Timestamp("2010-05-31"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0,
         "PrevClose": None, "TurnoverValue": None, "Volume": 100},
        {"Date": pd.Timestamp("2010-06-01"), "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0,
         "PrevClose": None, "TurnoverValue": None, "Volume": 100},
        {"Date": pd.Timestamp("2010-06-02"), "Open": 101.0, "High": 101.0, "Low": 101.0, "Close": 101.0,
         "PrevClose": None, "TurnoverValue": None, "Volume": 100},
        {"Date": pd.Timestamp("2016-01-04"), "Open": 105.0, "High": 105.0, "Low": 105.0, "Close": 105.0,
         "PrevClose": 100.0, "TurnoverValue": 100000.0, "Volume": 1000},
    ])


def _adjust(dividend: float, **kwargs) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "actions").mkdir(parents=True, exist_ok=True)
        pd.DataFrame([
            {"symbol": "TEST", "ExDate": pd.Timestamp("2010-06-01"), "DividendPerShareRs": dividend,
             "ShareMultiplier": 1.0, "ActionType": "DIVIDEND"},
            {"symbol": "TEST", "ExDate": pd.Timestamp("2016-01-01"), "DividendPerShareRs": 0.0,
             "ShareMultiplier": 10.0, "ActionType": "SPLIT"},
        ]).to_parquet(root / "actions" / "nse_corporate_actions.parquet", index=False)
        return OHLCVAdjuster(root).apply_adjustments(_upstox_frame(), "TEST", exchange="NSE", **kwargs)


def test_D2A_upstox_dividend_matches_split_basis():
    # Pre-split dividend Rs 50 on a Rs 100 (post-split) close: effective dividend is Rs 5.
    # Before the fix, 50 / 100 was used, giving a 50% (10x inflated) adjustment.
    adj = _adjust(50.0)
    assert adj.loc[1, "Dividends"] == 50.0  # raw NSE value is preserved
    assert abs(adj.loc[0, "Adj Close"] - 95.0) < 1e-4
    # Backward adjustment: the ex-date row itself keeps its raw close.
    assert abs(adj.loc[1, "Adj Close"] - 100.0) < 1e-4


def test_D2A2_large_pre_split_dividend_is_not_dropped():
    # Rs 120 >= split-adjusted prev close 100 used to fail `ex_divs < prev_close`
    # and be silently dropped. On the matched basis it is Rs 12 / 100 = 12%.
    adj = _adjust(120.0)
    assert abs(adj.loc[0, "Adj Close"] - 88.0) < 1e-4
    assert abs(adj.loc[1, "Adj Close"] - 100.0) < 1e-4


def test_D2C_adjust_dividends_false_yields_split_adjusted_only_series():
    total_return = _adjust(50.0, adjust_dividends=True)
    price_return = _adjust(50.0, adjust_dividends=False)
    # Split basis is still honoured (Upstox rows keep their already-adjusted prices);
    # only the dividend drag is removed.
    assert abs(price_return.loc[0, "Adj Close"] - 100.0) < 1e-4
    assert abs(price_return.loc[1, "Adj Close"] - 100.0) < 1e-4
    assert price_return.loc[0, "Adj Close"] > total_return.loc[0, "Adj Close"]
    assert price_return.loc[1, "Dividends"] == 50.0


def test_D2B_reverse_split_consolidation_is_parsed():
    res = parse_action_purpose("Face Value Split (Consolidation) - From Rs 1/- Per Share To Rs 10/- Per Share")
    assert res["ActionType"] == "CONSOLIDATION"
    assert res["SplitFromFV"] == 1.0
    assert res["SplitToFV"] == 10.0
    assert abs(res["ShareMultiplier"] - 0.1) < 1e-12

    # Ordinary split direction must remain unchanged.
    fwd = parse_action_purpose("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share")
    assert fwd["ActionType"] == "SPLIT"
    assert fwd["ShareMultiplier"] == 10.0
