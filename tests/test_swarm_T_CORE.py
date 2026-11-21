"""Offline regression tests for swarm task T-CORE (D3, D4, D5, D6).

Every test is fully local: temp Parquet files, in-memory DuckDB, and monkeypatched
network/HTTP helpers only. No network access is performed.
"""
import gc
import weakref
from unittest.mock import patch

import pandas as pd
import pytest

import datalake.engine as engmod
from datalake.engine import UnifiedLakeEngine
from datalake.remote import RemoteLakeClient

FIN_ROW = {
    "NatureOfReport": "Consolidated", "PeriodType": "Quarterly",
    "Sales_Cr": 100.0, "PBT_Cr": 20.0, "NetProfit_Cr": 10.0,
    "BasicEPS_Rs": 5.0, "Interest_Cr": 2.0, "Depreciation_Cr": 3.0,
    "OtherIncome_Cr": 1.0, "EquityCapital_Cr": 10.0,
}
SHP_ROW = {
    "QuarterEnd": "2025-06-30", "PromoterPct": 50.0, "PromoterPledgedPct": 0.0,
    "MutualFundsPct": 10.0, "FPI_Pct": 12.0, "RetailPct": 6.0,
}


def _mini_lake(tmp_path):
    """Latest quarter has no balance sheet; the prior quarter has the real one."""
    p = tmp_path
    for d in ("universe", "equities", "fundamentals", "shareholding"):
        (p / d).mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"SYMBOL": "TESTCO", "NAME OF COMPANY": "Test Co", "SERIES": "EQ",
                   "ISIN NUMBER": "INE000000001"}]).to_parquet(
        p / "universe" / "nse_equity_master.parquet", index=False)
    pd.DataFrame([
        {"Date": pd.Timestamp("2026-01-02"), "Symbol": "TESTCO", "Series": "EQ",
         "Close": 100.0, "Volume": 1000},
        {"Date": pd.Timestamp("2026-01-02"), "Symbol": "NODEBT", "Series": "EQ",
         "Close": 100.0, "Volume": 500},
    ]).to_parquet(p / "equities" / "nse_cm_2026.parquet", index=False)
    pd.DataFrame([
        {**FIN_ROW, "Symbol": "TESTCO", "PeriodEnd": "2025-06-30",
         "Reserves_Cr": None, "TotalBorrowings_Cr": None, "TotalCashAndBank_Cr": None},
        {**FIN_ROW, "Symbol": "TESTCO", "PeriodEnd": "2025-03-31",
         "Reserves_Cr": 900.0, "TotalBorrowings_Cr": 100.0, "TotalCashAndBank_Cr": 50.0},
        {**FIN_ROW, "Symbol": "NODEBT", "PeriodEnd": "2025-06-30",
         "Reserves_Cr": 500.0, "TotalBorrowings_Cr": None, "TotalCashAndBank_Cr": None},
    ]).to_parquet(p / "fundamentals" / "xbrl_financials.parquet", index=False)
    pd.DataFrame([
        {**SHP_ROW, "Symbol": "TESTCO"}, {**SHP_ROW, "Symbol": "NODEBT"},
    ]).to_parquet(p / "shareholding" / "nse_shareholding_summary.parquet", index=False)
    pd.DataFrame([
        {"Symbol": "TESTCO", "QuarterId": "99", "TotalShares": 1000.0},
        {"Symbol": "TESTCO", "QuarterId": "128", "TotalShares": 999999.0},
        {"Symbol": "NODEBT", "QuarterId": "1", "TotalShares": 100000.0},
    ]).to_parquet(p / "shareholding" / "bse_shareholding_history.parquet", index=False)
    return p


# --------------------------------------------------------------------------- D3
def test_d3_honest_valuation_sql_and_numeric_quarterid(tmp_path):
    scr = UnifiedLakeEngine(_mini_lake(tmp_path)).screener()
    row = scr[scr["Symbol"] == "TESTCO"].iloc[0]
    # Balance-sheet values come from the latest filing that actually HAS reserves.
    assert float(row["BookValue_Cr"]) == 910.0
    assert float(row["ROE_Pct"]) == pytest.approx(4.4, abs=0.01)
    # Real debt (not COALESCE(NULL, 0)) is used -> no fake "debt-free" verdict.
    assert float(row["DebtToEquity"]) > 0.0
    assert float(row["EV_EBITDA"]) > 0.0
    # String QuarterId '99' must not beat '128'.
    assert float(row["TotalShares"]) == 999999.0

    nd = scr[scr["Symbol"] == "NODEBT"].iloc[0]
    assert pd.isna(nd["DebtToEquity"])
    assert pd.isna(nd["ROCE_Pct"])
    assert pd.isna(nd["EV_EBITDA"])


def test_d3_bse_shareholder_cte_uses_try_cast(engine_source):
    assert "ORDER BY TRY_CAST(QuarterId AS BIGINT) DESC" in engine_source


# --------------------------------------------------------------------------- D4
def test_d4_research_profile_uses_meta_company_name_and_face_value():
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
    idx = {"aliases": {}, "tickers": {"TCS": {"meta": {
        "company_name": "Tata Consultancy Services Limited", "face_value": 1.0,
        "bse_code": "500325", "isin": "INE467B01029", "industry": "IT",
    }}}}
    c._load_ticker_index = lambda exch="NSE": idx
    c.dossier = lambda canon: {"financials": pd.DataFrame(), "concalls": pd.DataFrame(),
                               "named_holders": pd.DataFrame(), "annual_reports": pd.DataFrame()}
    c.shareholding = lambda canon: pd.DataFrame()
    c._screener_df_cache = pd.DataFrame([{"Symbol": "TCS", "Close": 3900.0}])
    prof = c.research_profile("TCS")
    assert prof["name"] == "Tata Consultancy Services Limited"
    assert prof["face_value"] == 1.0


def test_d4_bse_shareholding_orders_numeric_quarterid():
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
    c._ticker_index_loaded_bse = True
    c._ticker_index_bse = {}
    seen = {}

    def cap(query, params=None):
        seen["q"], seen["p"] = query, params
        return pd.DataFrame()

    c._safe_query = cap
    c.shareholding("500325", exchange="BSE", use_pack=False)
    assert "ORDER BY TRY_CAST(QuarterId AS BIGINT) ASC" in seen["q"]


def test_d4_calendar_uses_parameterized_symbol():
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
    seen = {}

    def cap(q, p=None):
        seen["q"], seen["p"] = q, p
        return pd.DataFrame()

    c._safe_query = cap
    crafted = "X' OR 1=1 --"
    c.calendar(crafted)
    assert "WHERE Symbol = ?" in seen["q"]
    assert seen["p"] == [crafted.strip().upper()]
    assert "'" not in seen["q"].split("WHERE")[-1]


def test_d4_screener_symbol_literal_is_escaped():
    crafted = "X' OR 1=1 --"
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
    c._screener_df_cache = pd.DataFrame([
        {"Symbol": "RELIANCE", "Close": 2800.0}, {"Symbol": "TCS", "Close": 3900.0},
    ])
    filters = []
    real = c.screener

    def spy(sql_filter=None):
        filters.append(sql_filter)
        return real(sql_filter)

    c.screener = spy
    assert c.info(crafted).get("regularMarketPrice") is None
    assert all("''" in f for f in filters if f)


def test_d4_engine_screener_symbol_literal_is_escaped(tmp_path):
    p = tmp_path
    (p / "snapshots").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([
        {"Symbol": "RELIANCE", "Close": 2800.0}, {"Symbol": "TCS", "Close": 3900.0},
    ]).to_parquet(p / "snapshots" / "market_screener_latest.parquet", index=False)
    engineered = UnifiedLakeEngine(p)
    info = engineered.info("X' OR 1=1 --")
    assert info.get("regularMarketPrice") is None


# --------------------------------------------------------------------------- D5
def test_d5_httpfs_history_honours_auto_adjust():
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False, use_pack=False)
    c._ensure_httpfs = lambda: None
    c._equity_urls_for_range = lambda exch, s, e: ["dummy-url"]
    raw = pd.DataFrame({
        "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "Open": [100.0] * 3, "High": [105.0] * 3, "Low": [95.0] * 3,
        "Close": [100.0] * 3, "Volume": [1000] * 3,
    })
    c._safe_query = lambda query, params=None: raw.copy()
    actions = pd.DataFrame([{"ExDate": pd.Timestamp("2024-01-02"),
                             "ActionType": "DIVIDEND", "DividendPerShareRs": 10.0,
                             "ShareMultiplier": 1.0}])
    calls = []

    def fake_actions(symbol, exchange=None, use_pack=None):
        calls.append((symbol, exchange, use_pack))
        return actions.copy()

    c.corporate_actions = fake_actions
    out = c._httpfs_history("TEST", 2024, 2024, "NSE", columns=None, auto_adjust=True)
    assert calls == [("TEST", "NSE", False)]
    assert out["Close"].tolist() == [90.0, 100.0, 100.0]
    assert "Adj Close" in out.columns and "Dividends" in out.columns


def test_d5_httpfs_history_no_actions_still_returns_raw_frame():
    c = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False, use_pack=False)
    c._ensure_httpfs = lambda: None
    c._equity_urls_for_range = lambda exch, s, e: ["dummy-url"]
    raw = pd.DataFrame({"Date": pd.to_datetime(["2024-01-01"]),
                        "Close": [100.0], "Volume": [1000]})
    c._safe_query = lambda query, params=None: raw.copy()
    c.corporate_actions = lambda *a, **k: pd.DataFrame()
    out = c._httpfs_history("TEST", 2024, 2024, "NSE", auto_adjust=True)
    assert out["Close"].tolist() == [100.0]


# --------------------------------------------------------------------------- D6
def test_d6_instance_cache_does_not_pin_engine_and_is_invalidated_by_reload(tmp_path):
    eq = tmp_path / "equities"
    eq.mkdir(parents=True, exist_ok=True)
    pq = eq / "nse_cm_2024.parquet"

    def write(close):
        pd.DataFrame([{"Date": pd.Timestamp("2024-01-02"), "Symbol": "FAKE",
                       "Series": "EQ", "Close": close}]).to_parquet(pq, index=False)

    write(100.0)
    eng = UnifiedLakeEngine(tmp_path)
    assert float(eng._cached_raw_equity("FAKE", "NSE", 2024, 2024)["Close"].iloc[0]) == 100.0
    ref = weakref.ref(eng)
    # Even while an entry is cached, dropping the engine must release it.
    eng2 = None
    del eng
    gc.collect()
    assert ref() is None

    eng2 = UnifiedLakeEngine(tmp_path)
    assert float(eng2._cached_raw_equity("FAKE", "NSE", 2024, 2024)["Close"].iloc[0]) == 100.0
    write(200.0)
    # Same instance keeps serving the cached frame until explicitly reloaded.
    assert float(eng2._cached_raw_equity("FAKE", "NSE", 2024, 2024)["Close"].iloc[0]) == 100.0
    eng2.sql("SELECT 1 AS x")
    old_duck = eng2._duck
    assert old_duck is not None
    old_adjuster = eng2.adjuster
    eng2.reload()
    assert eng2._duck is None
    with pytest.raises(Exception):
        old_duck.execute("SELECT 1")
    assert eng2.adjuster is not old_adjuster
    assert eng2.resolver is eng2.adjuster.resolver
    fresh = eng2._cached_raw_equity("FAKE", "NSE", 2024, 2024)
    assert float(fresh["Close"].iloc[0]) == 200.0
    # Callers never share the cached DataFrame object.
    fresh.loc[0, "Close"] = -1.0
    assert float(eng2._cached_raw_equity("FAKE", "NSE", 2024, 2024)["Close"].iloc[0]) == 200.0


def test_d6_optimize_layout_calls_reload(tmp_path):
    eng = UnifiedLakeEngine(tmp_path)
    calls = []
    eng.reload = lambda: calls.append(True)
    eng.optimize_layout()
    assert calls == [True]


def test_d6_screener_scratch_duckdb_connection_is_closed(tmp_path):
    class FakeCon:
        exited = False

        def register(self, name, df):
            self._df = df

        def execute(self, query):
            outer = self

            class Result:
                def df(self):
                    return outer._df.copy()
            return Result()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            FakeCon.exited = True

    fake = FakeCon()
    eng = UnifiedLakeEngine(tmp_path)
    seed = pd.DataFrame([{"Symbol": "TCS", "Close": 3900.0}])
    with patch.object(eng, "_compute_live_screener_df", return_value=seed), \
         patch.object(engmod.duckdb, "connect", return_value=fake):
        assert not eng.screener("Symbol = 'TCS'").empty
    assert FakeCon.exited is True


@pytest.fixture(scope="module")
def engine_source():
    from pathlib import Path
    return (Path(__file__).resolve().parents[1] / "src" / "datalake" / "engine.py").read_text(
        encoding="utf-8")
