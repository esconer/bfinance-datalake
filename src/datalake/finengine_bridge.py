"""
bfinance & FinEngine Integration Bridge for bfinance-datalake.

Provides native compatibility with:
1. https://github.com/esconer/bfinance (`Ticker`, `screens`, `ratios`, `flows`, `ownership`, `CompanyProfile`)
2. https://github.com/esconer/finengine (`EquityResearchService`, `IndiaDataService`, `ai_context_india`, `ScreenerService`)

Adheres strictly to the `bfinance` & `finengine` honesty contract:
- Never fabricates `0.0` or `NaN` for unmeasured legs (e.g. `peg_ratio`, `ev_to_ebitda` when cash/debt/mcap is missing,
  `interest_coverage` when interest is `<= 0`, or `amihud_illiquidity` when rupee volume is unavailable).
- Returns `None` for unmeasured fields and records explicit omission reasons in `bfinance_omitted_metrics`.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

# Exact schema constants from FinEngine's `india_data_service.py`
BHAV_REQUIRED_FIELDS: tuple[str, ...] = (
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "prev_close",
    "avg_price",
    "ttl_trd_qnty",
    "turnover_lacs",
    "no_of_trades",
)

LIQUIDITY_ADV_LOOKBACK_SESSIONS: int = 30

# Pre-built institutional screen definitions matching `bfinance.screens` and `finengine`'s `ScreenerService`
INSTITUTIONAL_SCREENS: dict[str, dict[str, str]] = {
    "coffee_can": {
        "name": "Coffee Can Portfolio",
        "description": "Consistent compounders with ROCE >= 15%, ROE >= 15%, Low Debt/Equity <= 0.5, and Market Cap >= 5000 Cr.",
        "sql_filter": "COALESCE(ROCE_Pct, ROE_Pct) >= 15.0 AND ROE_Pct >= 15.0 AND COALESCE(DebtToEquity, 0.0) <= 0.5 AND COALESCE(MarketCap_Cr, 10000.0) >= 5000.0",
        "order_by": "ROE_Pct DESC NULLS LAST",
    },
    "magic_formula": {
        "name": "Greenblatt Magic Formula",
        "description": "High Return on Capital (ROCE/ROE >= 20%) combined with reasonable Earnings Yield (PE > 0 and PE <= 35).",
        "sql_filter": "COALESCE(ROCE_Pct, ROE_Pct) >= 20.0 AND PE > 0 AND PE <= 35.0",
        "order_by": "PE ASC NULLS LAST, ROE_Pct DESC NULLS LAST",
    },
    "debt_free": {
        "name": "Debt-Free Quality Leaders",
        "description": "Virtually debt-free companies (DebtToEquity <= 0.1) with positive net profit and ROE >= 12%.",
        "sql_filter": "COALESCE(DebtToEquity, 0.0) <= 0.1 AND NetProfit_Cr > 0 AND ROE_Pct >= 12.0",
        "order_by": "MarketCap_Cr DESC NULLS LAST",
    },
    "undervalued_growth": {
        "name": "Undervalued Growth (GARP)",
        "description": "Growth at a Reasonable Price: PE between 0 and 25, ROE >= 15%, trading above 200-DMA.",
        "sql_filter": "PE > 0 AND PE <= 25.0 AND ROE_Pct >= 15.0 AND Close >= COALESCE(SMA_200, 0.0)",
        "order_by": "ROE_Pct DESC NULLS LAST",
    },
    "institutional_accumulation": {
        "name": "Institutional Conviction (FII + DII)",
        "description": "Strong institutional ownership (FPI + Mutual Funds >= 20%) with zero promoter pledge.",
        "sql_filter": "(COALESCE(FPI_Pct, 0.0) + COALESCE(MutualFundsPct, 0.0)) >= 20.0 AND COALESCE(PromoterPledgedPct, 0.0) <= 1.0",
        "order_by": "MarketCap_Cr DESC NULLS LAST",
    },
}


def _finite_float(val: Any) -> float | None:
    """Convert value to a finite Python float, returning `None` for None/NaN/Inf/non-numeric."""
    if val is None or isinstance(val, bool):
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _first_finite(*vals: Any) -> float | None:
    for v in vals:
        f = _finite_float(v)
        if f is not None:
            return f
    return None


def compute_amihud_illiquidity(returns: pd.Series, rupee_volume: pd.Series) -> float | None:
    """
    Return measured Amihud (2002) illiquidity (`mean(|r_t| / rupee_volume_t * 1e6)`),
    or `None` without valid history. Matches `finengine`'s `india_data_service.compute_amihud_illiquidity`.
    """
    if returns is None or rupee_volume is None or returns.empty or rupee_volume.empty:
        return None
    aligned = pd.concat([returns, rupee_volume], axis=1).dropna()
    if aligned.empty:
        return None
    aligned.columns = ["return", "rupee_volume"]
    valid = aligned[(aligned["rupee_volume"] > 0) & np.isfinite(aligned["return"])]
    if valid.empty:
        return None
    ratio = valid["return"].abs() / valid["rupee_volume"] * 1e6
    ratio = ratio[np.isfinite(ratio)]
    return float(ratio.mean()) if not ratio.empty else None


def compute_days_to_liquidate(
    position_value: float, adv_value: float | None, participation_rate: float = 0.10
) -> float | None:
    """
    Return measured liquidation days at `participation_rate` of ADV, or `None` when ADV is unavailable.
    Matches `finengine`'s `india_data_service.compute_days_to_liquidate`.
    """
    if position_value < 0 or adv_value is None or adv_value <= 0 or participation_rate <= 0:
        return None
    if position_value == 0:
        return 0.0
    result = float(position_value / (participation_rate * adv_value))
    return result if math.isfinite(result) else None


def calculate_graham_number(eps: float | None, book_value_per_share: float | None) -> float | None:
    """
    Compute Benjamin Graham intrinsic value `sqrt(22.5 * EPS * BVPS)`.
    Returns `None` when either EPS or BVPS is missing or `<= 0`.
    """
    e = _finite_float(eps)
    bv = _finite_float(book_value_per_share)
    if e is None or bv is None or e <= 0 or bv <= 0:
        return None
    return round(math.sqrt(22.5 * e * bv), 2)


def compute_piotroski_score(fin_df: pd.DataFrame) -> tuple[int | None, dict[str, bool | None]]:
    """
    Compute the 9-point Piotroski F-Score from annual or quarterly financial rows.
    Returns `(score, breakdown_dict)`. If insufficient data exists, returns `(None, breakdown)`.
    """
    breakdown: dict[str, bool | None] = {
        "F1_Positive_ROA": None,
        "F2_Positive_CFO": None,
        "F3_Increasing_ROA": None,
        "F4_Accrual_CFO_gt_PAT": None,
        "F5_Decreasing_Leverage": None,
        "F6_Increasing_CurrentRatio": None,
        "F7_No_Dilution": None,
        "F8_Increasing_OperatingMargin": None,
        "F9_Increasing_AssetTurnover": None,
    }
    if fin_df is None or fin_df.empty:
        return None, breakdown

    df = fin_df.copy()
    if "PeriodEnd" in df.columns:
        df["_dt"] = pd.to_datetime(df["PeriodEnd"], format="mixed", dayfirst=True, errors="coerce")
        df = df.sort_values("_dt").reset_index(drop=True)

    ann = df[df["PeriodType"].astype(str).str.lower() == "annual"] if "PeriodType" in df.columns else pd.DataFrame()
    work = ann if len(ann) >= 2 else df
    if work.empty:
        return None, breakdown

    curr = work.iloc[-1]
    prev = work.iloc[-2] if len(work) >= 2 else None

    pat_c = _finite_float(curr.get("NetProfit_Cr"))
    cfo_c = _first_finite(curr.get("CFO_Cr"), curr.get("OperatingCashFlow_Cr"))
    sales_c = _finite_float(curr.get("Sales_Cr"))
    eq_c = (_finite_float(curr.get("EquityCapital_Cr")) or 0.0) + (_finite_float(curr.get("Reserves_Cr")) or 0.0)
    debt_c = _finite_float(curr.get("TotalBorrowings_Cr"))
    assets_c = _finite_float(curr.get("TotalAssets_Cr"))
    if assets_c is None or assets_c <= 0:
        cap_c = eq_c + (debt_c or 0.0)
        assets_c = cap_c if cap_c > 0 else None

    roa_c = (pat_c / assets_c) if (pat_c is not None and assets_c and assets_c > 0) else None
    if pat_c is not None:
        breakdown["F1_Positive_ROA"] = pat_c > 0
    if cfo_c is not None:
        breakdown["F2_Positive_CFO"] = cfo_c > 0
    if cfo_c is not None and pat_c is not None:
        breakdown["F4_Accrual_CFO_gt_PAT"] = cfo_c > pat_c

    if prev is not None:
        pat_p = _finite_float(prev.get("NetProfit_Cr"))
        sales_p = _finite_float(prev.get("Sales_Cr"))
        eq_p = (_finite_float(prev.get("EquityCapital_Cr")) or 0.0) + (_finite_float(prev.get("Reserves_Cr")) or 0.0)
        debt_p = _finite_float(prev.get("TotalBorrowings_Cr"))
        assets_p = _finite_float(prev.get("TotalAssets_Cr"))
        if assets_p is None or assets_p <= 0:
            cap_p = eq_p + (debt_p or 0.0)
            assets_p = cap_p if cap_p > 0 else None

        roa_p = (pat_p / assets_p) if (pat_p is not None and assets_p and assets_p > 0) else None
        if roa_c is not None and roa_p is not None:
            breakdown["F3_Increasing_ROA"] = roa_c > roa_p
        elif pat_c is not None and pat_p is not None:
            breakdown["F3_Increasing_ROA"] = pat_c > pat_p

        if debt_c is not None and debt_p is not None:
            lev_c = debt_c / assets_c if (assets_c and assets_c > 0) else debt_c
            lev_p = debt_p / assets_p if (assets_p and assets_p > 0) else debt_p
            breakdown["F5_Decreasing_Leverage"] = lev_c <= lev_p

        ca_c = _finite_float(curr.get("CurrentAssets_Cr"))
        cl_c = _finite_float(curr.get("CurrentLiabilities_Cr"))
        ca_p = _finite_float(prev.get("CurrentAssets_Cr"))
        cl_p = _finite_float(prev.get("CurrentLiabilities_Cr"))
        if ca_c and cl_c and cl_c > 0 and ca_p and cl_p and cl_p > 0:
            breakdown["F6_Increasing_CurrentRatio"] = (ca_c / cl_c) >= (ca_p / cl_p)
        else:
            cash_c = _finite_float(curr.get("TotalCashAndBank_Cr"))
            cash_p = _finite_float(prev.get("TotalCashAndBank_Cr"))
            if cash_c is not None and cash_p is not None:
                breakdown["F6_Increasing_CurrentRatio"] = cash_c >= cash_p

        sh_cap_c = _finite_float(curr.get("EquityCapital_Cr"))
        sh_cap_p = _finite_float(prev.get("EquityCapital_Cr"))
        if sh_cap_c is not None and sh_cap_p is not None and sh_cap_p > 0:
            breakdown["F7_No_Dilution"] = sh_cap_c <= sh_cap_p * 1.005

        if sales_c and sales_c > 0 and sales_p and sales_p > 0:
            pbt_c = _first_finite(curr.get("PBT_Cr"), pat_c)
            pbt_p = _first_finite(prev.get("PBT_Cr"), pat_p)
            if pbt_c is not None and pbt_p is not None:
                breakdown["F8_Increasing_OperatingMargin"] = (pbt_c / sales_c) >= (pbt_p / sales_p)
            if assets_c and assets_c > 0 and assets_p and assets_p > 0:
                breakdown["F9_Increasing_AssetTurnover"] = (sales_c / assets_c) >= (sales_p / assets_p)

    measured = [v for v in breakdown.values() if v is not None]
    if not measured:
        return None, breakdown
    score = int(sum(1 for v in measured if v))
    return score, breakdown


def compute_forensic_ratios(
    fin_df: pd.DataFrame,
    current_price: float | None = None,
    market_cap_cr: float | None = None,
    total_shares: float | None = None,
) -> dict[str, Any]:
    """
    Compute `bfinance` & `finengine` `custom_ratios` with full honesty metadata:
    - `piotroski_score`, `piotroski_breakdown`
    - `graham_number`, `graham_upside_pct`
    - `enterprise_value_cr`, `ev_to_ebitda`
    - `interest_coverage`, `cfo_to_pat_ratio`, `debt_to_equity`, `roe_pct`, `roce_pct`
    - `peg_ratio`: always `None` with explicit reason in `bfinance_omitted_metrics`
    """
    omitted: dict[str, str] = {
        "peg_ratio": "Omitted: requires multi-year forward consensus growth rate not verifiable from single-period filings."
    }

    p_score, p_breakdown = compute_piotroski_score(fin_df)
    if p_score is None:
        omitted["piotroski_score"] = "Insufficient multi-period financial history to evaluate Piotroski F-Score."

    price = _finite_float(current_price)
    mcap = _finite_float(market_cap_cr)
    shares = _finite_float(total_shares)

    eps_ttm: float | None = None
    sales_ttm: float | None = None
    pat_ttm: float | None = None
    pbt_ttm: float | None = None
    ebitda_ttm: float | None = None
    ebit_ttm: float | None = None
    interest_ttm: float | None = None
    book_value_cr: float | None = None
    borrowings_cr: float | None = None
    cash_cr: float | None = None
    cfo_cr: float | None = None
    cur_assets_cr: float | None = None
    cur_liab_cr: float | None = None
    inventories_cr: float | None = None
    receivables_cr: float | None = None
    payables_cr: float | None = None
    gross_npa_pct: float | None = None
    net_npa_pct: float | None = None
    car_pct: float | None = None
    deposits_cr: float | None = None
    advances_cr: float | None = None

    if fin_df is not None and not fin_df.empty:
        df = fin_df.copy()
        if "PeriodEnd" in df.columns:
            df["_dt"] = pd.to_datetime(df["PeriodEnd"], format="mixed", dayfirst=True, errors="coerce")
            df = df.sort_values("_dt").reset_index(drop=True)

        qdf = df[df["PeriodType"].astype(str).str.lower() == "quarterly"] if "PeriodType" in df.columns else pd.DataFrame()
        adf = df[df["PeriodType"].astype(str).str.lower() == "annual"] if "PeriodType" in df.columns else pd.DataFrame()

        if len(qdf) >= 4:
            last4 = qdf.tail(4)
            sales_ttm = _finite_float(last4["Sales_Cr"].sum()) if "Sales_Cr" in last4.columns else None
            pat_ttm = _finite_float(last4["NetProfit_Cr"].sum()) if "NetProfit_Cr" in last4.columns else None
            pbt_ttm = _finite_float(last4["PBT_Cr"].sum()) if "PBT_Cr" in last4.columns else None
            eps_ttm = _finite_float(last4["BasicEPS_Rs"].sum()) if "BasicEPS_Rs" in last4.columns else None
            interest_ttm = _finite_float(last4["Interest_Cr"].sum()) if "Interest_Cr" in last4.columns else None
            dep_ttm = _finite_float(last4["Depreciation_Cr"].sum()) if "Depreciation_Cr" in last4.columns else 0.0
            oi_ttm = _finite_float(last4["OtherIncome_Cr"].sum()) if "OtherIncome_Cr" in last4.columns else 0.0
            if pbt_ttm is not None:
                ebit_ttm = pbt_ttm + (interest_ttm or 0.0)
                ebitda_ttm = ebit_ttm + (dep_ttm or 0.0) - (oi_ttm or 0.0)
        elif not adf.empty:
            latest_a = adf.iloc[-1]
            sales_ttm = _finite_float(latest_a.get("Sales_Cr"))
            pat_ttm = _finite_float(latest_a.get("NetProfit_Cr"))
            pbt_ttm = _finite_float(latest_a.get("PBT_Cr"))
            eps_ttm = _finite_float(latest_a.get("BasicEPS_Rs"))
            interest_ttm = _finite_float(latest_a.get("Interest_Cr"))
            dep_a = _finite_float(latest_a.get("Depreciation_Cr")) or 0.0
            oi_a = _finite_float(latest_a.get("OtherIncome_Cr")) or 0.0
            if pbt_ttm is not None:
                ebit_ttm = pbt_ttm + (interest_ttm or 0.0)
                ebitda_ttm = ebit_ttm + dep_a - oi_a
        else:
            latest_r = df.iloc[-1]
            sales_ttm = _finite_float(latest_r.get("Sales_Cr"))
            pat_ttm = _finite_float(latest_r.get("NetProfit_Cr"))
            pbt_ttm = _finite_float(latest_r.get("PBT_Cr"))
            eps_ttm = _finite_float(latest_r.get("BasicEPS_Rs"))
            interest_ttm = _finite_float(latest_r.get("Interest_Cr"))
            dep_r = _finite_float(latest_r.get("Depreciation_Cr")) or 0.0
            oi_r = _finite_float(latest_r.get("OtherIncome_Cr")) or 0.0
            if pbt_ttm is not None:
                ebit_ttm = pbt_ttm + (interest_ttm or 0.0)
                ebitda_ttm = ebit_ttm + dep_r - oi_r

        for idx_r in range(len(df) - 1, -1, -1):
            r = df.iloc[idx_r]
            eq_cap = _finite_float(r.get("EquityCapital_Cr"))
            res = _finite_float(r.get("Reserves_Cr"))
            if book_value_cr is None and (eq_cap is not None or res is not None):
                book_value_cr = (eq_cap or 0.0) + (res or 0.0)
            if borrowings_cr is None and _finite_float(r.get("TotalBorrowings_Cr")) is not None:
                borrowings_cr = _finite_float(r.get("TotalBorrowings_Cr"))
            if cash_cr is None and _finite_float(r.get("TotalCashAndBank_Cr")) is not None:
                cash_cr = _finite_float(r.get("TotalCashAndBank_Cr"))
            cfo_cand = _first_finite(r.get("CFO_Cr"), r.get("OperatingCashFlow_Cr"))
            if cfo_cr is None and cfo_cand is not None:
                cfo_cr = cfo_cand
            if cur_assets_cr is None and _finite_float(r.get("CurrentAssets_Cr")) is not None:
                cur_assets_cr = _finite_float(r.get("CurrentAssets_Cr"))
            if cur_liab_cr is None and _finite_float(r.get("CurrentLiabilities_Cr")) is not None:
                cur_liab_cr = _finite_float(r.get("CurrentLiabilities_Cr"))
            if inventories_cr is None and _finite_float(r.get("Inventories_Cr")) is not None:
                inventories_cr = _finite_float(r.get("Inventories_Cr"))
            if receivables_cr is None and _finite_float(r.get("TradeReceivables_Cr")) is not None:
                receivables_cr = _finite_float(r.get("TradeReceivables_Cr"))
            if payables_cr is None and _finite_float(r.get("TradePayables_Cr")) is not None:
                payables_cr = _finite_float(r.get("TradePayables_Cr"))
            if gross_npa_pct is None and _finite_float(r.get("GrossNPA_Pct")) is not None:
                gross_npa_pct = _finite_float(r.get("GrossNPA_Pct"))
            if net_npa_pct is None and _finite_float(r.get("NetNPA_Pct")) is not None:
                net_npa_pct = _finite_float(r.get("NetNPA_Pct"))
            if car_pct is None and _finite_float(r.get("CapitalAdequacyRatio_Pct")) is not None:
                car_pct = _finite_float(r.get("CapitalAdequacyRatio_Pct"))
            if deposits_cr is None and _finite_float(r.get("Deposits_Cr")) is not None:
                deposits_cr = _finite_float(r.get("Deposits_Cr"))
            if advances_cr is None and _finite_float(r.get("Advances_Cr")) is not None:
                advances_cr = _finite_float(r.get("Advances_Cr"))

    if mcap is None and price is not None and shares is not None and shares > 0:
        mcap = round((price * shares) / 10000000.0, 2)

    bvps: float | None = None
    if book_value_cr is not None and book_value_cr > 0:
        if shares is not None and shares > 0:
            bvps = round((book_value_cr * 10000000.0) / shares, 2)
        elif mcap is not None and mcap > 0 and price is not None and price > 0:
            implied_shares = (mcap * 10000000.0) / price
            if implied_shares > 0:
                bvps = round((book_value_cr * 10000000.0) / implied_shares, 2)

    graham = calculate_graham_number(eps_ttm, bvps)
    graham_upside: float | None = None
    if graham is None:
        omitted["graham_number"] = "Requires positive TTM EPS and positive Book Value Per Share."
    elif price is not None and price > 0:
        graham_upside = round(((graham - price) / price) * 100.0, 2)

    ev_cr: float | None = None
    ev_to_ebitda: float | None = None
    if mcap is not None and borrowings_cr is not None and cash_cr is not None:
        ev_cr = round(mcap + borrowings_cr - cash_cr, 2)
        if ebitda_ttm is not None and ebitda_ttm > 0:
            ev_to_ebitda = round(ev_cr / ebitda_ttm, 2)
        else:
            omitted["ev_to_ebitda"] = "EBITDA is non-positive or unmeasured."
    else:
        missing_legs = [
            name
            for name, v in (("market_cap_cr", mcap), ("total_borrowings_cr", borrowings_cr), ("cash_and_bank_cr", cash_cr))
            if v is None
        ]
        omitted["enterprise_value_cr"] = f"Strict 3-leg EV gate: missing {', '.join(missing_legs)}."
        omitted["ev_to_ebitda"] = omitted["enterprise_value_cr"]

    interest_cov: float | None = None
    if ebit_ttm is not None and interest_ttm is not None and interest_ttm > 0:
        interest_cov = round(ebit_ttm / interest_ttm, 2)
    else:
        omitted["interest_coverage"] = "Interest expense is zero or unmeasured."

    cfo_to_pat: float | None = None
    if cfo_cr is not None and pat_ttm is not None and pat_ttm > 0:
        cfo_to_pat = round(cfo_cr / pat_ttm, 2)
    else:
        omitted["cfo_to_pat_ratio"] = "Requires measured Operating Cash Flow (CFO_Cr) and positive Net Profit."

    roe_pct: float | None = None
    roce_pct: float | None = None
    debt_to_eq: float | None = None
    if book_value_cr is not None and book_value_cr > 0:
        if pat_ttm is not None:
            roe_pct = round((pat_ttm / book_value_cr) * 100.0, 2)
        if borrowings_cr is not None:
            debt_to_eq = round(borrowings_cr / book_value_cr, 2)
        cap_employed = book_value_cr + (borrowings_cr or 0.0)
        if ebit_ttm is not None and cap_employed > 0:
            roce_pct = round((ebit_ttm / cap_employed) * 100.0, 2)

    current_ratio = round(cur_assets_cr / cur_liab_cr, 2) if (cur_assets_cr and cur_liab_cr and cur_liab_cr > 0) else None
    working_cap_cr = round(cur_assets_cr - cur_liab_cr, 2) if (cur_assets_cr is not None and cur_liab_cr is not None) else None
    rec_days = round((receivables_cr / sales_ttm) * 365.0, 1) if (receivables_cr is not None and sales_ttm and sales_ttm > 0) else None
    inv_days = round((inventories_cr / sales_ttm) * 365.0, 1) if (inventories_cr is not None and sales_ttm and sales_ttm > 0) else None
    pay_days = round((payables_cr / sales_ttm) * 365.0, 1) if (payables_cr is not None and sales_ttm and sales_ttm > 0) else None
    credit_deposit_pct = round((advances_cr / deposits_cr) * 100.0, 2) if (advances_cr and deposits_cr and deposits_cr > 0) else None

    return {
        "piotroski_score": p_score,
        "piotroski_breakdown": p_breakdown,
        "graham_number": graham,
        "graham_upside_pct": graham_upside,
        "book_value_per_share": bvps,
        "book_value": bvps,
        "eps_ttm": round(eps_ttm, 2) if eps_ttm is not None else None,
        "enterprise_value_cr": ev_cr,
        "ev_to_ebitda": ev_to_ebitda,
        "interest_coverage": interest_cov,
        "cfo_to_pat_ratio": cfo_to_pat,
        "cfo_to_pat": cfo_to_pat,
        "debt_to_equity": debt_to_eq,
        "roe_pct": roe_pct,
        "roe": roe_pct,
        "roce_pct": roce_pct,
        "roce": roce_pct,
        "sales_ttm_cr": round(sales_ttm, 2) if sales_ttm is not None else None,
        "net_profit_ttm_cr": round(pat_ttm, 2) if pat_ttm is not None else None,
        "ebitda_ttm_cr": round(ebitda_ttm, 2) if ebitda_ttm is not None else None,
        "current_ratio": current_ratio,
        "working_capital_cr": working_cap_cr,
        "receivable_days": rec_days,
        "inventory_days": inv_days,
        "payable_days": pay_days,
        "gross_npa_pct": gross_npa_pct,
        "net_npa_pct": net_npa_pct,
        "capital_adequacy_pct": car_pct,
        "deposits_cr": deposits_cr,
        "advances_cr": advances_cr,
        "credit_deposit_ratio_pct": credit_deposit_pct,
        "peg_ratio": None,
        "bfinance_omitted_metrics": omitted,
    }


def transpose_financials_to_statement(
    fin_df: pd.DataFrame, statement_type: str = "income_stmt"
) -> pd.DataFrame:
    """
    Convert `bfinance-datalake` period-row financials DataFrame into `bfinance` / `yfinance`
    transposed financial statements where line items are index rows and period end dates are columns.
    """
    if fin_df is None or fin_df.empty:
        return pd.DataFrame()

    df = fin_df.copy()
    if "PeriodEnd" in df.columns:
        df["_dt"] = pd.to_datetime(df["PeriodEnd"], format="mixed", dayfirst=True, errors="coerce")
        df = df.sort_values("_dt", ascending=False).drop_duplicates(subset=["PeriodEnd"], keep="first")
        col_labels = [
            dt.strftime("%Y-%m-%d") if pd.notna(dt) else str(pe)
            for dt, pe in zip(df["_dt"], df["PeriodEnd"])
        ]
    else:
        col_labels = [str(i) for i in range(len(df))]

    st = statement_type.strip().lower()
    if st in ("income_stmt", "income", "pnl"):
        if "RawMaterials_Cr" not in df.columns and (
            "MaterialCost_Cr" in df.columns or "StockInTradePurchases_Cr" in df.columns
        ):
            mat_s = pd.to_numeric(df.get("MaterialCost_Cr", pd.Series(dtype=float)), errors="coerce")
            sit_s = pd.to_numeric(df.get("StockInTradePurchases_Cr", pd.Series(dtype=float)), errors="coerce")
            df["RawMaterials_Cr"] = (mat_s.fillna(0.0) + sit_s.fillna(0.0)).where(mat_s.notna() | sit_s.notna())
        if "TotalExpenses_Cr" not in df.columns and "TotalIncome_Cr" in df.columns and "PBT_Cr" in df.columns:
            ti_s = pd.to_numeric(df["TotalIncome_Cr"], errors="coerce")
            pbt_s_tmp = pd.to_numeric(df["PBT_Cr"], errors="coerce")
            df["TotalExpenses_Cr"] = (ti_s - pbt_s_tmp).round(2).where(ti_s.notna() & pbt_s_tmp.notna())
        spec = [
            ("Total Revenue", "Sales_Cr"),
            ("Interest Earned", "InterestEarned_Cr"),
            ("Other Income", "OtherIncome_Cr"),
            ("Other Banking Income", "OtherBankingIncome_Cr"),
            ("Total Income", "TotalIncome_Cr"),
            ("Cost Of Revenue", "RawMaterials_Cr"),
            ("Cost Of Materials Consumed", "MaterialCost_Cr"),
            ("Purchases Of Stock In Trade", "StockInTradePurchases_Cr"),
            ("Employee Benefit Expense", "EmployeeCost_Cr"),
            ("Interest Expended", "InterestExpended_Cr"),
            ("Other Expenses", "OtherExpenses_Cr"),
            ("Operating Expense", "TotalExpenses_Cr"),
            ("Operating Profit Pre-Provision", "OperatingProfitPreProvision_Cr"),
            ("Provisions And Contingencies", "ProvisionsAndContingencies_Cr"),
            ("EBITDA", "_EBITDA_Cr"),
            ("Depreciation And Amortization", "Depreciation_Cr"),
            ("EBIT", "_EBIT_Cr"),
            ("Interest Expense", "Interest_Cr"),
            ("Pretax Income", "PBT_Cr"),
            ("Tax Provision", "Tax_Cr"),
            ("Net Income", "NetProfit_Cr"),
            ("Basic EPS", "BasicEPS_Rs"),
            ("Diluted EPS", "DilutedEPS_Rs"),
            ("Gross NPA %", "GrossNPA_Pct"),
            ("Net NPA %", "NetNPA_Pct"),
            ("Capital Adequacy Ratio %", "CapitalAdequacyRatio_Pct"),
        ]
        pbt_s = pd.to_numeric(df.get("PBT_Cr", pd.Series(dtype=float)), errors="coerce")
        int_s = pd.to_numeric(df.get("Interest_Cr", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        dep_s = pd.to_numeric(df.get("Depreciation_Cr", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        oi_s = pd.to_numeric(df.get("OtherIncome_Cr", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        df["_EBIT_Cr"] = (pbt_s + int_s).round(2)
        df["_EBITDA_Cr"] = (pbt_s + int_s + dep_s - oi_s).round(2)
    elif st in ("balance_sheet", "bs"):
        eq_s = pd.to_numeric(df.get("EquityCapital_Cr", pd.Series(dtype=float)), errors="coerce")
        res_s = pd.to_numeric(df.get("Reserves_Cr", pd.Series(dtype=float)), errors="coerce")
        debt_s = pd.to_numeric(df.get("TotalBorrowings_Cr", pd.Series(dtype=float)), errors="coerce")
        cash_s = pd.to_numeric(df.get("TotalCashAndBank_Cr", pd.Series(dtype=float)), errors="coerce")
        df["_NetWorth_Cr"] = (eq_s.fillna(0.0) + res_s.fillna(0.0)).where(eq_s.notna() | res_s.notna())
        df["_NetDebt_Cr"] = (debt_s - cash_s).where(debt_s.notna() & cash_s.notna())
        if "TotalLiabilities_Cr" not in df.columns and "TotalAssets_Cr" in df.columns:
            ta_s = pd.to_numeric(df["TotalAssets_Cr"], errors="coerce")
            df["TotalLiabilities_Cr"] = (ta_s - df["_NetWorth_Cr"]).round(2).where(ta_s.notna() & df["_NetWorth_Cr"].notna())
        spec = [
            ("Share Capital", "EquityCapital_Cr"),
            ("Reserves And Surplus", "Reserves_Cr"),
            ("Stockholders Equity", "_NetWorth_Cr"),
            ("Long Term Borrowings", "LongTermBorrowings_Cr"),
            ("Short Term Borrowings", "ShortTermBorrowings_Cr"),
            ("Total Debt", "TotalBorrowings_Cr"),
            ("Lease Liabilities Non-Current", "LeaseLiabilitiesNonCurrent_Cr"),
            ("Lease Liabilities Current", "LeaseLiabilitiesCurrent_Cr"),
            ("Deposits", "Deposits_Cr"),
            ("Advances", "Advances_Cr"),
            ("Cash And Cash Equivalents", "TotalCashAndBank_Cr"),
            ("Net Debt", "_NetDebt_Cr"),
            ("Property Plant And Equipment", "FixedAssets_Cr"),
            ("Capital Work In Progress", "CWIP_Cr"),
            ("Goodwill", "Goodwill_Cr"),
            ("Other Intangible Assets", "IntangibleAssets_Cr"),
            ("Non-Current Investments", "NonCurrentInvestments_Cr"),
            ("Current Investments", "CurrentInvestments_Cr"),
            ("Inventories", "Inventories_Cr"),
            ("Trade Receivables", "TradeReceivables_Cr"),
            ("Trade Payables", "TradePayables_Cr"),
            ("Current Assets", "CurrentAssets_Cr"),
            ("Current Liabilities", "CurrentLiabilities_Cr"),
            ("Non-Current Assets", "NonCurrentAssets_Cr"),
            ("Non-Current Liabilities", "NonCurrentLiabilities_Cr"),
            ("Total Assets", "TotalAssets_Cr"),
            ("Total Liabilities Net Minority Interest", "TotalLiabilities_Cr"),
            ("Gross NPA (Cr)", "GrossNPA_Cr"),
            ("Net NPA (Cr)", "NetNPA_Cr"),
        ]
    else:
        spec = [
            ("Operating Cash Flow", "CFO_Cr"),
            ("Investing Cash Flow", "CFI_Cr"),
            ("Financing Cash Flow", "CFF_Cr"),
            ("Net Cash Flow", "NetCashFlow_Cr"),
            ("Capital Expenditure", "Capex_Cr"),
            ("Free Cash Flow", "_FCF_Cr"),
        ]
        if "CFO_Cr" not in df.columns and "OperatingCashFlow_Cr" in df.columns:
            df["CFO_Cr"] = df["OperatingCashFlow_Cr"]
        if "CFI_Cr" not in df.columns and "InvestingCashFlow_Cr" in df.columns:
            df["CFI_Cr"] = df["InvestingCashFlow_Cr"]
        if "CFF_Cr" not in df.columns and "FinancingCashFlow_Cr" in df.columns:
            df["CFF_Cr"] = df["FinancingCashFlow_Cr"]
        if "NetCashFlow_Cr" not in df.columns and any(c in df.columns for c in ("CFO_Cr", "CFI_Cr", "CFF_Cr")):
            cfo_n = pd.to_numeric(df.get("CFO_Cr", pd.Series(dtype=float)), errors="coerce")
            cfi_n = pd.to_numeric(df.get("CFI_Cr", pd.Series(dtype=float)), errors="coerce")
            cff_n = pd.to_numeric(df.get("CFF_Cr", pd.Series(dtype=float)), errors="coerce")
            df["NetCashFlow_Cr"] = (cfo_n.fillna(0.0) + cfi_n.fillna(0.0) + cff_n.fillna(0.0)).round(2).where(
                cfo_n.notna() | cfi_n.notna() | cff_n.notna()
            )
        cfo_s = pd.to_numeric(df.get("CFO_Cr", pd.Series(dtype=float)), errors="coerce")
        capex_s = pd.to_numeric(df.get("Capex_Cr", pd.Series(dtype=float)), errors="coerce")
        df["_FCF_Cr"] = (cfo_s - capex_s.abs()).where(cfo_s.notna() & capex_s.notna())

    rows_dict: dict[str, list[float | None]] = {}
    for label, col in spec:
        if col in df.columns:
            vals = [_finite_float(v) for v in df[col].tolist()]
            if any(v is not None for v in vals):
                rows_dict[label] = vals

    if not rows_dict:
        return pd.DataFrame()
    out = pd.DataFrame.from_dict(rows_dict, orient="index", columns=col_labels)
    out.index.name = "Item"
    return out


def format_shareholding_for_finengine(shp_df: pd.DataFrame, symbol: str = "") -> dict[str, Any]:
    """
    Format unified NSE + BSE shareholding history into `finengine`'s `EquityResearchService.get_shareholding`
    response contract (`quarterly` and `yearly` blocks with `periods`, `rows`, and `chart_series`).
    """
    empty_block = {"periods": [], "rows": [], "chart_series": []}
    if shp_df is None or shp_df.empty:
        return {"symbol": symbol, "quarterly": empty_block, "yearly": empty_block}

    df = shp_df.copy()
    q_col = "QuarterEnd" if "QuarterEnd" in df.columns else ("Quarter" if "Quarter" in df.columns else None)
    if q_col:
        df["_dt"] = pd.to_datetime(df[q_col], format="mixed", dayfirst=True, errors="coerce")
        df = df.sort_values("_dt").drop_duplicates(subset=[q_col], keep="last").reset_index(drop=True)
    else:
        df["_dt"] = pd.NaT

    def _build_block(sub: pd.DataFrame) -> dict[str, Any]:
        if sub.empty:
            return empty_block
        periods: list[str] = []
        chart_series: list[dict[str, Any]] = []
        prom_vals: list[float | None] = []
        fii_vals: list[float | None] = []
        dii_vals: list[float | None] = []
        pub_vals: list[float | None] = []
        pledge_vals: list[float | None] = []

        for _, r in sub.iterrows():
            dt = r.get("_dt")
            p_label = dt.strftime("%b %Y") if pd.notna(dt) else str(r.get(q_col or "QuarterEnd", ""))
            periods.append(p_label)

            prom = _finite_float(r.get("PromoterPct"))
            fii = _finite_float(r.get("FPI_Pct"))
            mf = _finite_float(r.get("MutualFundsPct"))
            ins = _finite_float(r.get("InsurancePct"))
            dii = round((mf or 0.0) + (ins or 0.0), 2) if (mf is not None or ins is not None) else _finite_float(r.get("DII_Pct"))
            pub = _finite_float(r.get("RetailPct")) or _finite_float(r.get("PublicPct"))
            if pub is None and prom is not None:
                pub = round(max(0.0, 100.0 - prom - (fii or 0.0) - (dii or 0.0)), 2)
            pledge = _finite_float(r.get("PromoterPledgedPct"))

            prom_vals.append(prom)
            fii_vals.append(fii)
            dii_vals.append(dii)
            pub_vals.append(pub)
            pledge_vals.append(pledge)

            chart_series.append({
                "period": p_label,
                "promoters": prom,
                "fii": fii,
                "dii": dii,
                "public": pub,
                "pledged": pledge,
            })

        rows = [
            {"category": "Promoters", "values": prom_vals},
            {"category": "FII / FPI", "values": fii_vals},
            {"category": "DII / Mutual Funds", "values": dii_vals},
            {"category": "Public & Retail", "values": pub_vals},
            {"category": "Promoter Pledged %", "values": pledge_vals},
        ]
        return {"periods": periods, "rows": rows, "chart_series": chart_series}

    q_sub = df.tail(12)
    if df["_dt"].notna().any():
        df["_yr"] = df["_dt"].dt.year
        y_sub = df.groupby("_yr", as_index=False).last().tail(11)
    else:
        y_sub = df.iloc[::4].tail(11)

    return {
        "symbol": symbol,
        "total_quarters_available": int(len(df)),
        "quarterly": _build_block(q_sub),
        "yearly": _build_block(y_sub),
    }


def format_bhavcopy_records(eq_df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert an equity EOD DataFrame into `finengine`'s `IndiaDataService` bhavcopy records
    satisfying `BHAV_REQUIRED_FIELDS`.
    """
    if eq_df is None or eq_df.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, r in eq_df.iterrows():
        sym = str(r.get("Symbol") or r.get("symbol") or "").strip().upper()
        if not sym:
            continue
        open_p = _finite_float(r.get("Open") if "Open" in r else r.get("open"))
        high_p = _finite_float(r.get("High") if "High" in r else r.get("high"))
        low_p = _finite_float(r.get("Low") if "Low" in r else r.get("low"))
        close_p = _finite_float(r.get("Close") if "Close" in r else r.get("close"))
        prev_p = _finite_float(r.get("PrevClose") if "PrevClose" in r else r.get("prev_close")) or close_p
        vwap_p = _finite_float(r.get("VWAP") if "VWAP" in r else r.get("avg_price")) or close_p
        vol = _finite_float(r.get("Volume") if "Volume" in r else r.get("ttl_trd_qnty"))
        turnover_val = _finite_float(r.get("TurnoverValue"))
        turnover_lacs = _finite_float(r.get("turnover_lacs"))
        if turnover_lacs is None:
            if turnover_val is not None and turnover_val > 0:
                turnover_lacs = round(turnover_val / 100000.0, 4)
            elif close_p is not None and vol is not None:
                turnover_lacs = round((close_p * vol) / 100000.0, 4)
        trades = _finite_float(r.get("Trades") if "Trades" in r else r.get("no_of_trades")) or 0.0
        deliv_q = _finite_float(r.get("DeliverableQty") if "DeliverableQty" in r else r.get("deliv_qty"))
        deliv_pct = _finite_float(r.get("DeliveryPct") if "DeliveryPct" in r else r.get("deliv_per"))

        if any(v is None or v <= 0 for v in (open_p, high_p, low_p, close_p, prev_p, vwap_p)) or vol is None or vol < 0:
            continue

        dt_raw = r.get("Date") if "Date" in r else r.get("date")
        dt_str = pd.Timestamp(dt_raw).strftime("%Y-%m-%d") if pd.notna(dt_raw) else None
        records.append({
            "symbol": sym,
            "series": str(r.get("Series") or r.get("series") or "EQ").strip().upper(),
            "date": dt_str,
            "open": round(float(open_p), 4),
            "high": round(float(high_p), 4),
            "low": round(float(low_p), 4),
            "close": round(float(close_p), 4),
            "prev_close": round(float(prev_p), 4),
            "avg_price": round(float(vwap_p), 4),
            "ttl_trd_qnty": int(round(float(vol))),
            "turnover_lacs": round(float(turnover_lacs or 0.0), 4),
            "no_of_trades": int(round(float(trades))),
            "deliv_qty": int(round(float(deliv_q))) if deliv_q is not None else None,
            "deliv_per": round(float(deliv_pct), 2) if deliv_pct is not None else None,
        })
    return records


def compute_delivery_anomalies_from_frames(
    price_frames: Mapping[str, pd.DataFrame],
    lookback_sessions: int = LIQUIDITY_ADV_LOOKBACK_SESSIONS,
    z_threshold: float = 1.5,
) -> dict[str, Any]:
    """
    Compute symbol-scoped delivery-percentage z-score anomalies across a dict of `{symbol: ohlcv_df}`,
    matching `finengine`'s `ai_context_india` delivery anomalies contract.
    """
    anomalies: list[dict[str, Any]] = []
    evaluated: list[dict[str, Any]] = []
    missing_symbols: list[str] = []

    for raw_sym, df in price_frames.items():
        sym = str(raw_sym).strip().upper()
        if df is None or df.empty:
            missing_symbols.append(sym)
            continue
        w = df.sort_values("Date").tail(lookback_sessions).copy() if "Date" in df.columns else df.tail(lookback_sessions).copy()
        deliv_s: pd.Series | None = None
        if "DeliveryPct" in w.columns and w["DeliveryPct"].notna().any():
            deliv_s = pd.to_numeric(w["DeliveryPct"], errors="coerce")
        elif "DeliverableQty" in w.columns and "Volume" in w.columns:
            vol_s = pd.to_numeric(w["Volume"], errors="coerce").replace(0, np.nan)
            dq_s = pd.to_numeric(w["DeliverableQty"], errors="coerce")
            deliv_s = (dq_s / vol_s) * 100.0

        if deliv_s is None or deliv_s.dropna().shape[0] < 5:
            missing_symbols.append(sym)
            continue

        clean_d = deliv_s.dropna()
        latest_val = float(clean_d.iloc[-1])
        mean_val = float(clean_d.mean())
        std_val = float(clean_d.std(ddof=0))
        z_score = round((latest_val - mean_val) / std_val, 3) if std_val > 1e-6 else 0.0
        latest_dt = pd.Timestamp(w["Date"].iloc[-1]).strftime("%Y-%m-%d") if "Date" in w.columns else None

        entry = {
            "symbol": sym,
            "as_of_date": latest_dt,
            "latest_delivery_pct": round(latest_val, 2),
            "mean_delivery_pct_30d": round(mean_val, 2),
            "delivery_z_score": z_score,
            "is_anomaly": bool(abs(z_score) >= z_threshold),
            "direction": "high_delivery_accumulation" if z_score >= z_threshold else ("low_delivery_speculative" if z_score <= -z_threshold else "normal"),
            "sessions_observed": int(clean_d.shape[0]),
        }
        evaluated.append(entry)
        if abs(z_score) >= z_threshold:
            anomalies.append(entry)

    anomalies.sort(key=lambda x: abs(x["delivery_z_score"]), reverse=True)
    status = "complete" if evaluated and not missing_symbols else ("partial" if evaluated else "unavailable")
    return {
        "scope": "symbol_scoped",
        "lookback_sessions": lookback_sessions,
        "z_threshold": z_threshold,
        "coverage_status": status,
        "data_status": "available" if status == "complete" else status,
        "anomaly_count": len(anomalies),
        "anomalies": anomalies,
        "evaluated_symbols": evaluated,
        "missing_symbols": missing_symbols,
    }


def compute_portfolio_liquidity_limits(
    positions: Sequence[Mapping[str, Any]],
    price_frames: Mapping[str, pd.DataFrame],
    lookback_sessions: int = LIQUIDITY_ADV_LOOKBACK_SESSIONS,
    base_currency: str = "INR",
) -> dict[str, Any]:
    """
    Compute `finengine` `IndiaDataService` portfolio position liquidity limits:
    - 30-day ADV in shares and rupees
    - `days_to_liquidate_10pct_adv` and `days_to_liquidate_20pct_adv`
    - `amihud_illiquidity` (`mean(|r_t| / rupee_volume_t * 1e6)`)
    - `max_sane_position_value` (5% of 30d ADV) and `liquidity_tier`
    """
    pos_results: list[dict[str, Any]] = []
    missing_symbols: list[str] = []

    for pos in positions:
        raw_sym = str(pos.get("symbol") or pos.get("ticker") or "").strip().upper()
        sym = raw_sym.split(".")[0] if "." in raw_sym else raw_sym
        qty = _finite_float(pos.get("quantity") or pos.get("shares"))
        pos_val = _finite_float(pos.get("market_value") or pos.get("position_value"))

        df = price_frames.get(sym) if sym in price_frames else price_frames.get(raw_sym)
        if df is None or df.empty:
            missing_symbols.append(sym)
            pos_results.append({
                "symbol": sym,
                "ticker": raw_sym or sym,
                "market_value": pos_val,
                "position_value": pos_val,
                "adv_30d_shares": None,
                "adv_30d_rupees": None,
                "days_to_liquidate_10pct_adv": None,
                "days_to_liquidate_20pct_adv": None,
                "amihud_illiquidity": None,
                "max_sane_position_value": None,
                "liquidity_tier": "unmeasured",
                "coverage_status": "unavailable",
            })
            continue

        w = df.sort_values("Date").tail(lookback_sessions + 1).copy() if "Date" in df.columns else df.tail(lookback_sessions + 1).copy()
        close_s = pd.to_numeric(w["Close"], errors="coerce")
        vol_s = pd.to_numeric(w["Volume"], errors="coerce")
        if "TurnoverValue" in w.columns and w["TurnoverValue"].notna().any():
            rupee_vol_s = pd.to_numeric(w["TurnoverValue"], errors="coerce")
        else:
            rupee_vol_s = close_s * vol_s

        latest_price = _finite_float(close_s.dropna().iloc[-1]) if not close_s.dropna().empty else None
        if pos_val is None and qty is not None and latest_price is not None:
            pos_val = round(qty * latest_price, 2)

        recent_vol = vol_s.tail(lookback_sessions).dropna()
        recent_rvol = rupee_vol_s.tail(lookback_sessions).dropna()
        adv_shares = round(float(recent_vol.mean()), 2) if not recent_vol.empty else None
        adv_rupees = round(float(recent_rvol.mean()), 2) if not recent_rvol.empty else None

        ret_s = close_s.pct_change().tail(lookback_sessions)
        amihud = compute_amihud_illiquidity(ret_s, rupee_vol_s.tail(lookback_sessions))
        if amihud is not None:
            amihud = round(amihud, 6)

        dtl_10 = compute_days_to_liquidate(pos_val, adv_rupees, 0.10) if pos_val is not None else None
        dtl_20 = compute_days_to_liquidate(pos_val, adv_rupees, 0.20) if pos_val is not None else None
        if dtl_10 is not None:
            dtl_10 = round(dtl_10, 4)
        if dtl_20 is not None:
            dtl_20 = round(dtl_20, 4)

        max_sane = round(0.05 * adv_rupees, 2) if adv_rupees is not None and adv_rupees > 0 else None
        tier = "unmeasured"
        if dtl_10 is not None:
            if dtl_10 <= 1.0:
                tier = "liquid"
            elif dtl_10 <= 5.0:
                tier = "moderate"
            else:
                tier = "illiquid"

        pos_results.append({
            "symbol": sym,
            "ticker": raw_sym or f"{sym}.NS",
            "currency": base_currency,
            "market_value": pos_val,
            "position_value": pos_val,
            "adv_30d_shares": adv_shares,
            "adv_30d_rupees": adv_rupees,
            "days_to_liquidate_10pct_adv": dtl_10,
            "days_to_liquidate_20pct_adv": dtl_20,
            "amihud_illiquidity": amihud,
            "max_sane_position_value": max_sane,
            "liquidity_tier": tier,
            "coverage_status": "complete" if (adv_rupees is not None and amihud is not None) else "partial",
        })

    measured_count = sum(1 for p in pos_results if p["coverage_status"] == "complete")
    status = "complete" if (pos_results and measured_count == len(pos_results)) else ("partial" if measured_count > 0 else "unavailable")
    return {
        "scope": "portfolio_positions",
        "lookback_sessions": lookback_sessions,
        "base_currency": base_currency,
        "coverage_status": status,
        "data_status": "available" if status == "complete" else status,
        "positions": pos_results,
        "missing_symbols": missing_symbols,
    }


def build_ai_context_india(
    flows_df: pd.DataFrame,
    delivery_payload: dict[str, Any],
    liquidity_payload: dict[str, Any],
    participant_oi_df: pd.DataFrame | None = None,
    fo_ban_symbols: list[str] | None = None,
    short_selling_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Assemble the 3-component `finengine` `ai_context_india` payload:
    1. `institutional_flows` (`scope="market_wide"`)
    2. `delivery_anomalies` (`scope="symbol_scoped"`)
    3. `liquidity_limits` (`scope="portfolio_positions"`)
    """
    flows_block: dict[str, Any] = {
        "scope": "market_wide",
        "coverage_status": "unavailable",
        "latest_date": None,
        "fii_fpi": None,
        "dii": None,
        "recent_sessions": [],
    }
    if flows_df is not None and not flows_df.empty and "Date" in flows_df.columns:
        df = flows_df.copy()
        df["_dt"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["_dt"]).sort_values("_dt")
        if not df.empty:
            max_dt = df["_dt"].max()
            latest_rows = df[df["_dt"] == max_dt]
            fii_dict = None
            dii_dict = None
            for _, r in latest_rows.iterrows():
                cat = str(r.get("category") or r.get("Category") or "").upper()
                item = {
                    "date": max_dt.strftime("%Y-%m-%d"),
                    "category": str(r.get("category") or r.get("Category") or ""),
                    "buy_cr": _first_finite(r.get("buy_cr"), r.get("buyValue")),
                    "sell_cr": _first_finite(r.get("sell_cr"), r.get("sellValue")),
                    "net_cr": _first_finite(r.get("net_cr"), r.get("netValue")),
                }
                if "FII" in cat or "FPI" in cat:
                    fii_dict = item
                elif "DII" in cat:
                    dii_dict = item

            recent_list = []
            for _, r in df.tail(10).iterrows():
                recent_list.append({
                    "date": pd.Timestamp(r["_dt"]).strftime("%Y-%m-%d"),
                    "category": str(r.get("category") or r.get("Category") or ""),
                    "buy_cr": _first_finite(r.get("buy_cr"), r.get("buyValue")),
                    "sell_cr": _first_finite(r.get("sell_cr"), r.get("sellValue")),
                    "net_cr": _first_finite(r.get("net_cr"), r.get("netValue")),
                })
            flows_block = {
                "scope": "market_wide",
                "coverage_status": "complete" if (fii_dict and dii_dict) else "partial",
                "latest_date": max_dt.strftime("%Y-%m-%d"),
                "fii_fpi": fii_dict,
                "dii": dii_dict,
                "recent_sessions": recent_list,
            }

    oi_sentiment: dict[str, Any] | None = None
    if participant_oi_df is not None and not participant_oi_df.empty and "Date" in participant_oi_df.columns:
        odf = participant_oi_df.copy()
        odf["_dt"] = pd.to_datetime(odf["Date"], errors="coerce")
        odf = odf.dropna(subset=["_dt"]).sort_values("_dt")
        if not odf.empty:
            max_odt = odf["_dt"].max()
            latest_oi = odf[odf["_dt"] == max_odt]
            by_client: dict[str, Any] = {}
            for _, r in latest_oi.iterrows():
                ctype = str(r.get("Client Type") or "").strip()
                f_long = _finite_float(r.get("Future Index Long"))
                f_short = _finite_float(r.get("Future Index Short"))
                ls_ratio = round(f_long / f_short, 3) if (f_long is not None and f_short and f_short > 0) else None
                by_client[ctype] = {
                    "future_index_long": f_long,
                    "future_index_short": f_short,
                    "future_index_net": round((f_long or 0.0) - (f_short or 0.0), 0) if (f_long is not None or f_short is not None) else None,
                    "future_index_ls_ratio": ls_ratio,
                    "total_long_contracts": _finite_float(r.get("Total Long Contracts")),
                    "total_short_contracts": _finite_float(r.get("Total Short Contracts")),
                }
            oi_sentiment = {"latest_date": max_odt.strftime("%Y-%m-%d"), "participants": by_client}

    statuses = [
        flows_block.get("coverage_status", "unavailable"),
        delivery_payload.get("coverage_status", "unavailable"),
        liquidity_payload.get("coverage_status", "unavailable"),
    ]
    if all(s == "complete" for s in statuses):
        composite_status = "complete"
    elif all(s == "unavailable" for s in statuses):
        composite_status = "unavailable"
    else:
        composite_status = "partial"

    warnings: list[str] = []
    if flows_block["coverage_status"] != "complete":
        warnings.append("Institutional FII/DII cash flow series is partial or unavailable.")
    if delivery_payload.get("coverage_status") != "complete":
        warnings.append("Delivery anomaly coverage is partial across requested symbols.")
    if liquidity_payload.get("coverage_status") != "complete":
        warnings.append("Portfolio liquidity limits could not be measured for all positions.")

    return {
        "contract": "finengine.ai_context_india.v1",
        "composite_coverage_status": composite_status,
        "warnings": warnings,
        "institutional_flows": flows_block,
        "delivery_anomalies": delivery_payload,
        "liquidity_limits": liquidity_payload,
        "derivatives_participant_oi": oi_sentiment,
        "fo_ban_symbols": fo_ban_symbols or [],
    }


def build_research_profile(
    symbol: str,
    info_dict: Mapping[str, Any] | None = None,
    fin_df: pd.DataFrame | None = None,
    shp_df: pd.DataFrame | None = None,
    concalls_df: pd.DataFrame | None = None,
    *,
    company_name: str | None = None,
    bse_code: str | None = None,
    isin: str | None = None,
    industry: str | None = None,
    face_value: float | None = None,
    screener_row: Mapping[str, Any] | None = None,
    named_holders_df: pd.DataFrame | None = None,
    annual_reports_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Build the 360-degree `finengine` `EquityResearchService.get_full_profile` and `bfinance.CompanyProfile`
    payload from `bfinance-datalake` tables.
    """
    sym = symbol.strip().upper().replace(".NS", "").replace(".BO", "")
    scr = dict(screener_row or info_dict or {})
    close_p = _finite_float(scr.get("Close") or scr.get("regularMarketPrice") or scr.get("currentPrice"))
    mcap_cr = _finite_float(scr.get("MarketCap_Cr") or scr.get("marketCapInCr"))
    high_52w = _finite_float(scr.get("High_52W") or scr.get("fiftyTwoWeekHigh"))
    low_52w = _finite_float(scr.get("Low_52W") or scr.get("fiftyTwoWeekLow"))
    pe = _finite_float(scr.get("PE") or scr.get("trailingPE"))
    pb = _finite_float(scr.get("PB") or scr.get("priceToBook"))
    tot_shares = _finite_float(scr.get("TotalShares"))

    if tot_shares is None and shp_df is not None and not shp_df.empty and "TotalShares" in shp_df.columns:
        valid_sh = shp_df["TotalShares"].dropna()
        if not valid_sh.empty:
            tot_shares = _finite_float(valid_sh.iloc[-1])

    ratios = compute_forensic_ratios(
        fin_df if fin_df is not None else pd.DataFrame(),
        current_price=close_p,
        market_cap_cr=mcap_cr,
        total_shares=tot_shares,
    )

    if pe is None and close_p is not None and ratios.get("eps_ttm") and ratios["eps_ttm"] > 0:
        pe = round(close_p / ratios["eps_ttm"], 2)
    if mcap_cr is None and close_p is not None and tot_shares is not None and tot_shares > 0:
        mcap_cr = round((close_p * tot_shares) / 10000000.0, 2)

    prom_pct = _finite_float(scr.get("PromoterPct") or scr.get("heldPercentPromoters"))
    pledge_pct = _finite_float(scr.get("PromoterPledgedPct") or scr.get("promoterPledgedPct"))
    fii_pct = _finite_float(scr.get("FPI_Pct") or scr.get("heldPercentFPI"))
    mf_pct = _finite_float(scr.get("MutualFundsPct") or scr.get("heldPercentMutualFunds"))
    if shp_df is not None and not shp_df.empty:
        last_shp = shp_df.iloc[-1]
        if prom_pct is None:
            prom_pct = _finite_float(last_shp.get("PromoterPct"))
        if pledge_pct is None:
            pledge_pct = _finite_float(last_shp.get("PromoterPledgedPct"))
        if fii_pct is None:
            fii_pct = _finite_float(last_shp.get("FPI_Pct"))
        if mf_pct is None:
            mf_pct = _finite_float(last_shp.get("MutualFundsPct"))

    pros: list[str] = []
    cons: list[str] = []
    de_val = ratios.get("debt_to_equity") if ratios.get("debt_to_equity") is not None else _finite_float(scr.get("DebtToEquity"))
    roe_val = ratios.get("roe_pct") if ratios.get("roe_pct") is not None else _finite_float(scr.get("ROE_Pct"))
    roce_val = ratios.get("roce_pct") if ratios.get("roce_pct") is not None else _finite_float(scr.get("ROCE_Pct"))

    if de_val is not None and de_val <= 0.1:
        pros.append("Company is virtually debt-free (Debt/Equity <= 0.10).")
    elif de_val is not None and de_val > 1.5:
        cons.append(f"Elevated financial leverage (Debt/Equity = {de_val:.2f}).")

    if roe_val is not None and roe_val >= 15.0:
        pros.append(f"Strong Return on Equity track record (ROE = {roe_val:.1f}%).")
    elif roe_val is not None and roe_val < 8.0:
        cons.append(f"Low Return on Equity (ROE = {roe_val:.1f}%).")

    if ratios.get("piotroski_score") is not None and ratios["piotroski_score"] >= 7:
        pros.append(f"Robust Piotroski F-Score ({ratios['piotroski_score']}/9) signaling strong accounting health.")
    if ratios.get("cfo_to_pat_ratio") is not None and ratios["cfo_to_pat_ratio"] >= 0.9:
        pros.append(f"High cash conversion (Operating Cash Flow / Net Profit = {ratios['cfo_to_pat_ratio']:.2f}x).")
    if pledge_pct is not None and pledge_pct > 10.0:
        cons.append(f"High promoter share pledge ({pledge_pct:.1f}%).")
    if prom_pct is not None and prom_pct >= 50.0 and (pledge_pct or 0.0) == 0.0:
        pros.append(f"High unpledged promoter skin-in-the-game ({prom_pct:.1f}%).")
    if ratios.get("net_npa_pct") is not None:
        if ratios["net_npa_pct"] <= 1.0:
            pros.append(f"Strong banking asset quality (Net NPA = {ratios['net_npa_pct']:.2f}%).")
        elif ratios["net_npa_pct"] > 2.5:
            cons.append(f"Elevated Net NPA ratio ({ratios['net_npa_pct']:.2f}%).")
    if ratios.get("capital_adequacy_pct") is not None and ratios["capital_adequacy_pct"] >= 15.0:
        pros.append(f"Well-capitalized balance sheet (CRAR = {ratios['capital_adequacy_pct']:.2f}%).")

    concall_items: list[dict[str, Any]] = []
    if concalls_df is not None and not concalls_df.empty:
        for _, r in concalls_df.head(12).iterrows():
            doc_type = str(r.get("DocumentType") or r.get("SubCategory") or "").lower()
            pdf_url = r.get("PDFUrl") or r.get("AttachmentUrl") or r.get("TranscriptUrl") or r.get("URL")
            ppt_url = r.get("PresentationUrl") or (pdf_url if "presentation" in doc_type else None)
            aud_url = r.get("AudioUrl") or (pdf_url if "audio" in doc_type or "video" in doc_type else None)
            tr_url = r.get("AttachmentUrl") or r.get("TranscriptUrl") or (
                pdf_url if ("presentation" not in doc_type and "audio" not in doc_type) else None
            )
            concall_items.append({
                "date": str(r.get("FilingTimestamp") or r.get("Date") or "")[:10],
                "quarter": str(r.get("Quarter") or r.get("SubCategory") or r.get("Category") or "Filing"),
                "title": str(r.get("Headline") or r.get("Subject") or "Investor Filing"),
                "transcript_url": tr_url,
                "audio_url": aud_url,
                "presentation_url": ppt_url,
            })

    top_holders: list[dict[str, Any]] = []
    if named_holders_df is not None and not named_holders_df.empty:
        for _, r in named_holders_df.head(15).iterrows():
            top_holders.append({
                "holder_name": str(r.get("HolderName") or ""),
                "category": str(r.get("CategoryMember") or ""),
                "pct_of_total": _finite_float(r.get("PctOfTotal")),
                "shares": _finite_float(r.get("Shares")),
                "quarter_end": str(r.get("QuarterEnd") or ""),
            })

    annual_report_items: list[dict[str, Any]] = []
    if annual_reports_df is not None and not annual_reports_df.empty:
        for _, r in annual_reports_df.head(10).iterrows():
            annual_report_items.append({
                "year": str(r.get("Year") or ""),
                "file_name": str(r.get("FileName") or ""),
                "pdf_url": r.get("PDFUrl"),
            })

    return {
        "symbol": sym,
        "ticker": f"{sym}.NS",
        "name": company_name or scr.get("longName") or sym,
        "bse_code": bse_code or scr.get("bseCode"),
        "nse_symbol": sym,
        "isin": isin or scr.get("isin"),
        "sector": industry or scr.get("industry") or "Equities",
        "industry": industry or scr.get("industry") or "Equities",
        "current_price": close_p,
        "market_cap_cr": mcap_cr,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "stock_pe": pe,
        "price_to_book": pb,
        "book_value": ratios.get("book_value_per_share"),
        "roce": roce_val,
        "roe": roe_val,
        "face_value": _finite_float(face_value or scr.get("faceValue")),
        "debt_to_equity": de_val,
        "eps_ttm": ratios.get("eps_ttm"),
        "promoter_holding": prom_pct,
        "promoter_pledged": pledge_pct,
        "fii_holding": fii_pct,
        "dii_holding": mf_pct,
        "analysis": {"pros": pros, "cons": cons},
        "custom_ratios": ratios,
        "concalls": concall_items,
        "top_holders": top_holders,
        "annual_reports": annual_report_items,
    }
