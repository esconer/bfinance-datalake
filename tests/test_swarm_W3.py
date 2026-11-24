"""Deterministic offline regression tests for Workstream W3 (T-W3).

No network: all tests build a tiny synthetic Parquet lake in pytest's ``tmp_path``.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import datalake.engine as engine_mod
from datalake.engine import HAS_DUCKDB, UnifiedLakeEngine
from datalake.remote import RemoteLakeClient

INDEX_DATES = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
WEEKEND_DATES = {"2026-01-03", "2026-01-04"}


def _write_lake(root: Path, *, unified: bool = True, fo: bool = True) -> None:
    for sub in ("indices", "mutual_funds", "universe", "derivatives", "flows"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    pd.DataFrame({
        "IndexName": ["NIFTY 50"] * len(INDEX_DATES),
        "Date": pd.to_datetime(INDEX_DATES),
        "Close": [24000.0 + i * 100 for i in range(len(INDEX_DATES))],
    }).to_parquet(root / "indices" / "nifty_30y_ohlc_tri_pe.parquet", index=False)

    pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-02", "2026-01-05"]),
        "SchemeCode": [100122, 100122],
        "SchemeName": ["HDFC Test Fund", "HDFC Test Fund"],
        "NAV": [150.5, 151.0],
        "Category": ["Equity", "Equity"],
        "AMC": ["HDFC", "HDFC"],
        "ISINGrowth": ["INF200K01HI7", "INF200K01HI7"],
    }).to_parquet(root / "mutual_funds" / "amfi_nav_2026.parquet", index=False)

    if unified:
        pd.DataFrame([{
            "symbol": "ZZTESTCO",
            "bse_code": "500999",
            "isin": "INE999A01018",
            "company_name": "ZZ Test Company Ltd",
            "aliases_json": json.dumps(["ZZTESTCO", "OLDZZTESTCO"]),
            "face_value": 10.0,
            "industry": "Testing",
            "listing_date": "2001-04-02",
            "primary_exchange": "NSE",
        }]).to_parquet(root / "universe" / "security_master_unified.parquet", index=False)

    if fo:
        pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-02", "2026-01-02", "2026-01-05", "2026-01-05"]),
            "ExpiryDate": pd.to_datetime(["2026-01-29", "2026-02-26", "2026-01-29", "2026-02-26"]),
            "Symbol": ["NIFTY", "NIFTY", "NIFTY", "NIFTY"],
            "OptionType": ["XX", "XX", "XX", "XX"],
            "Close": [24000.0, 24100.0, 24400.0, 24500.0],
        }).to_parquet(root / "derivatives" / "nse_fo_2026.parquet", index=False)

    pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-02", "2026-01-02"]),
        "Client Type": ["Client", "FII"],
        "Turnover": [1.0, 2.0],
    }).to_parquet(root / "flows" / "fao_participant_vol_2026.parquet", index=False)


def _dates(idx: pd.DatetimeIndex) -> set[str]:
    return {pd.Timestamp(d).strftime("%Y-%m-%d") for d in idx}


# ---------------------------------------------------------------------------
# W3A-4 / W3B: trading_days(include_weekends=False)
# ---------------------------------------------------------------------------

def test_engine_trading_days_weekend_filter_both_ways(tmp_path):
    _write_lake(tmp_path)
    eng = UnifiedLakeEngine(tmp_path)
    start, end = "2026-01-01", "2026-01-05"

    default = eng.trading_days(start=start, end=end)
    assert _dates(default) == set(INDEX_DATES) - WEEKEND_DATES
    assert all(d.weekday() < 5 for d in default)

    with_weekends = eng.trading_days(start=start, end=end, include_weekends=True)
    assert _dates(with_weekends) == set(INDEX_DATES)
    assert WEEKEND_DATES <= _dates(with_weekends)


def test_remote_trading_days_weekend_filter_both_ways(tmp_path):
    _write_lake(tmp_path)
    idx_df = UnifiedLakeEngine(tmp_path).index_history("NIFTY 50")
    cl = RemoteLakeClient(repo="user/bfinance-datalake", load_httpfs=False, use_pack=False)
    try:
        cl.index_history = lambda name="NIFTY 50": idx_df  # type: ignore[method-assign]
        start, end = "2026-01-01", "2026-01-05"

        default = cl.trading_days(start=start, end=end)
        assert _dates(default) == set(INDEX_DATES) - WEEKEND_DATES
        with_weekends = cl.trading_days(start=start, end=end, include_weekends=True)
        assert _dates(with_weekends) == set(INDEX_DATES)
    finally:
        cl.close()


# ---------------------------------------------------------------------------
# W3A-1: unified-master crosswalk fast path (raw masters absent)
# ---------------------------------------------------------------------------

def test_unified_master_crosswalk_fast_path_with_raw_masters_absent(tmp_path):
    _write_lake(tmp_path)
    assert not (tmp_path / "universe" / "nse_equity_master.parquet").exists()
    assert not (tmp_path / "universe" / "bse_scrip_master.parquet").exists()

    eng = UnifiedLakeEngine(tmp_path)
    orig_read = engine_mod.read_table_safe
    reads: list[str] = []

    def counting_read(path):
        reads.append(Path(path).name)
        return orig_read(path)

    with patch.object(engine_mod, "read_table_safe", side_effect=counting_read):
        by_symbol = eng.resolve("ZZTESTCO")
        by_alias = eng.resolve("OLDZZTESTCO")
        by_bse = eng.resolve("500999")
        by_isin = eng.resolve("INE999A01018")

    assert reads == ["security_master_unified.parquet"]  # one read, no raw master reads
    for ident in (by_symbol, by_alias, by_bse, by_isin):
        assert ident.symbol == "ZZTESTCO"
        assert ident.bse_code == "500999"
        assert ident.isin == "INE999A01018"
        assert ident.company_name == "ZZ Test Company Ltd"
        assert ident.aliases == ("ZZTESTCO", "OLDZZTESTCO")
        assert ident.face_value == 10.0
        assert ident.industry == "Testing"
        assert ident.listing_date == "2001-04-02"
        assert ident.primary_exchange == "NSE"


def test_crosswalk_falls_back_when_unified_cache_disabled_or_broken(tmp_path):
    _write_lake(tmp_path)
    raw_master = pd.DataFrame([{
        "SYMBOL": "RAWMSTR",
        "NAME OF COMPANY": "Raw Master Co",
        "ISIN NUMBER": "INE888A01018",
    }])
    raw_master.to_parquet(tmp_path / "universe" / "nse_equity_master.parquet", index=False)

    eng = UnifiedLakeEngine(tmp_path)
    eng._ensure_crosswalk(use_unified_cache=False)
    assert eng.resolve("RAWMSTR").company_name == "Raw Master Co"
    assert "ZZTESTCO" not in eng._by_symbol

    eng2 = UnifiedLakeEngine(tmp_path)
    (tmp_path / "universe" / "security_master_unified.parquet").write_bytes(b"not a parquet")
    ident = eng2.resolve("RAWMSTR")  # exception in fast path -> existing raw-master path
    assert ident.company_name == "Raw Master Co"


# ---------------------------------------------------------------------------
# W3A-2: mutual_funds() cache + reload() refresh + copy-on-return
# ---------------------------------------------------------------------------

def test_mutual_funds_cache_count_and_reload_refresh(tmp_path):
    _write_lake(tmp_path)
    eng = UnifiedLakeEngine(tmp_path)
    orig_read = engine_mod.read_table_safe
    reads: list[str] = []

    def counting_read(path):
        reads.append(Path(path).name)
        return orig_read(path)

    with patch.object(engine_mod, "read_table_safe", side_effect=counting_read):
        first = eng.mutual_funds()
        second = eng.mutual_funds()
    assert reads == ["amfi_nav_2026.parquet"]
    assert len(first) == len(second) == 1
    assert float(first.iloc[0]["NAV"]) == 151.0

    # returned frame is a copy: mutating it cannot poison the cache
    first.loc[0, "NAV"] = -1.0
    assert float(eng.mutual_funds().iloc[0]["NAV"]) == 151.0

    # new file on disk only visible after reload()
    pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-06"]),
        "SchemeCode": [100122],
        "SchemeName": ["HDFC Test Fund"],
        "NAV": [999.0],
        "Category": ["Equity"],
        "AMC": ["HDFC"],
        "ISINGrowth": ["INF200K01HI7"],
    }).to_parquet(tmp_path / "mutual_funds" / "amfi_nav_2026.parquet", index=False)
    assert float(eng.mutual_funds().iloc[0]["NAV"]) == 151.0
    eng.reload()
    with patch.object(engine_mod, "read_table_safe", side_effect=counting_read):
        after_reload = eng.mutual_funds()
    assert reads == ["amfi_nav_2026.parquet", "amfi_nav_2026.parquet"]
    assert float(after_reload.iloc[0]["NAV"]) == 999.0


# ---------------------------------------------------------------------------
# W3A-5: participant_vol() query + SQL view
# ---------------------------------------------------------------------------

def test_participant_vol_query_and_sql_view(tmp_path):
    _write_lake(tmp_path)
    eng = UnifiedLakeEngine(tmp_path)

    all_rows = eng.participant_vol(start="2026-01-01", end="2026-01-31")
    assert len(all_rows) == 2
    assert list(all_rows["Client Type"]) == ["Client", "FII"]
    assert list(all_rows.columns).count("Date") == 1

    one = eng.participant_vol(start="2026-01-01", end="2026-01-31", client_type="FII")
    assert len(one) == 1
    assert one.iloc[0]["Client Type"] == "FII"

    sql_df = eng.sql("SELECT COUNT(*) AS n FROM participant_vol")
    assert int(sql_df.iloc[0]["n"]) == 2
    sql_filtered = eng.sql("SELECT \"Client Type\" AS ct FROM participant_vol WHERE \"Client Type\" = 'Client'")
    assert sql_filtered.iloc[0]["ct"] == "Client"


def test_participant_vol_empty_without_flows_dir(tmp_path):
    _write_lake(tmp_path)
    import shutil
    shutil.rmtree(tmp_path / "flows")
    assert UnifiedLakeEngine(tmp_path).participant_vol().empty


# ---------------------------------------------------------------------------
# W3A-3: continuous_futures() DuckDB pushdown + intact Python-loop fallback
# ---------------------------------------------------------------------------

def test_continuous_futures_duckdb_pushdown(tmp_path):
    if not HAS_DUCKDB:
        pytest.skip("duckdb not installed")
    _write_lake(tmp_path)
    eng = UnifiedLakeEngine(tmp_path)

    loop_calls: list[str] = []
    sql_calls: list[str] = []
    orig_filtered = eng._read_filtered_parquet
    orig_sql = eng.sql

    def counting_filtered(path, filters=None):
        loop_calls.append(Path(path).name)
        return orig_filtered(path, filters=filters)

    def counting_sql(query):
        sql_calls.append(query)
        return orig_sql(query)

    with patch.object(eng, "_read_filtered_parquet", side_effect=counting_filtered), \
         patch.object(eng, "sql", side_effect=counting_sql):
        out = eng.continuous_futures("NIFTY", contract=1, start="2026-01-01", end="2026-01-31")

    assert len(sql_calls) == 1
    assert "ROW_NUMBER() OVER" in sql_calls[0] and "FROM fo_nse" in sql_calls[0]
    assert loop_calls == []
    assert len(out) == 2
    assert set(out["Symbol"]) == {"NIFTY"}
    assert "_rk" not in out.columns


def test_continuous_futures_python_loop_fallback(tmp_path):
    _write_lake(tmp_path)
    eng = UnifiedLakeEngine(tmp_path)
    sql_calls: list[str] = []
    orig_sql = eng.sql

    def counting_sql(query):
        sql_calls.append(query)
        return orig_sql(query)

    with patch.object(engine_mod, "HAS_DUCKDB", False), \
         patch.object(eng, "sql", side_effect=counting_sql):
        out = eng.continuous_futures("NIFTY", contract=1, start="2026-01-01", end="2026-01-31")

    assert sql_calls == []
    assert len(out) == 2
    assert set(out["Symbol"]) == {"NIFTY"}
    assert "_ContractRank" not in out.columns


def test_continuous_futures_empty_when_no_parquet(tmp_path):
    _write_lake(tmp_path, fo=False)
    eng = UnifiedLakeEngine(tmp_path)
    assert eng.continuous_futures("NIFTY", start="2026-01-01", end="2026-01-31").empty
