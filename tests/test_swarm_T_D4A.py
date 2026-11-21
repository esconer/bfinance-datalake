"""Regression test for T-D4A: all-NaN quarterly columns must never fabricate 0.0 TTM totals."""
import math

import pandas as pd

from datalake.finengine_bridge import compute_forensic_ratios

NAN = float("nan")


def _quarters(vals):
    """Build 4 quarterly rows from a dict of column -> list of 4 values."""
    rows = []
    for i in range(4):
        row = {"Symbol": "TEST", "PeriodType": "Quarterly", "PeriodEnd": f"2025-0{i+1}-30"}
        for col, series in vals.items():
            row[col] = series[i]
        rows.append(row)
    return pd.DataFrame(rows)


def test_all_nan_quarters_yield_none_not_zero():
    """All-NaN headline columns over 4 quarters must produce None, never fabricated 0.0."""
    df = _quarters({
        "Sales_Cr": [NAN] * 4,
        "NetProfit_Cr": [NAN] * 4,
        "PBT_Cr": [NAN] * 4,
        "BasicEPS_Rs": [NAN] * 4,
        "Interest_Cr": [NAN] * 4,
        "Depreciation_Cr": [NAN] * 4,
        "OtherIncome_Cr": [NAN] * 4,
    })
    r = compute_forensic_ratios(df, current_price=100.0, market_cap_cr=1000.0, total_shares=100_000_000.0)

    for key in ("eps_ttm", "sales_ttm_cr", "net_profit_ttm_cr", "ebitda_ttm_cr"):
        assert r[key] is None, f"{key} must be None for all-NaN quarters, got {r[key]!r}"
        assert not (isinstance(r[key], float) and math.isfinite(r[key])), f"{key} leaked a finite value"


def test_partial_quarter_headline_column_yields_none():
    """A headline column missing any of the 4 quarters must not be summed to a partial total."""
    df = _quarters({
        "Sales_Cr": [10.0, 20.0, 30.0, NAN],
        "NetProfit_Cr": [1.0, 2.0, 3.0, NAN],
        "PBT_Cr": [2.0, 3.0, 4.0, NAN],
        "BasicEPS_Rs": [0.5, 0.6, 0.7, NAN],
    })
    r = compute_forensic_ratios(df)
    for key in ("eps_ttm", "sales_ttm_cr", "net_profit_ttm_cr"):
        assert r[key] is None, f"{key} must be None when one of 4 quarters is missing, got {r[key]!r}"


def test_complete_four_quarters_compute_correct_ttm():
    """A complete 4-quarter frame still computes correct TTM sums and EBITDA."""
    df = _quarters({
        "Sales_Cr": [100.0, 110.0, 120.0, 130.0],
        "NetProfit_Cr": [10.0, 11.0, 12.0, 13.0],
        "PBT_Cr": [15.0, 16.0, 17.0, 18.0],
        "BasicEPS_Rs": [1.0, 1.1, 1.2, 1.3],
        "Interest_Cr": [2.0, 2.0, 2.0, 2.0],
        "Depreciation_Cr": [3.0, 3.0, 3.0, 3.0],
        "OtherIncome_Cr": [1.0, 1.0, 1.0, 1.0],
    })
    r = compute_forensic_ratios(df)

    assert r["sales_ttm_cr"] == 460.0
    assert r["net_profit_ttm_cr"] == 46.0
    assert r["eps_ttm"] == 4.6
    # EBITDA = PBT(66) + Interest(8) + Depreciation(12) - OtherIncome(4) = 82
    assert r["ebitda_ttm_cr"] == 82.0
