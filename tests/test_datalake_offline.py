from __future__ import annotations

import asyncio
import io
import json
import tempfile
import zipfile
from datetime import date
from pathlib import Path
import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.config import LakeConfig
from datalake.doctor import LakeDoctor
from datalake.http_client import SimpleHTTPResponse
from datalake.scrapers.deals_docs import DealsAndDocumentsScraper, _normalize_deals_frame
from datalake.scrapers.equity_eod import EquityEODScraper, filter_bse_equity_only, repair_ohlc
from datalake.scrapers.fo_eod import DerivativesEODScraper
from datalake.scrapers.funds_flows import FundsAndFlowsScraper
from datalake.scrapers.indices import IndicesScraper
from datalake.scrapers.universe_actions import UniverseAndActionsScraper, parse_action_purpose
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe, sanitize_for_parquet, write_parquet_atomic
from datalake.symbols import SymbolRenameResolver


def _make_zip(filename: str, text_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, text_content.encode("utf-8"))
    return buf.getvalue()


SAMPLE_Q2_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:in-bse-fin="http://www.bseindia.com/xbrl/fin/2020-03-31/in-bse-fin">
  <xbrli:context id="OneD">
    <xbrli:entity><xbrli:identifier scheme="http://www.nseindia.com/NSESymbol">RELIANCE</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2026-07-01</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="TwoD">
    <xbrli:entity><xbrli:identifier scheme="http://www.nseindia.com/NSESymbol">RELIANCE</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2026-04-01</xbrli:startDate><xbrli:endDate>2026-09-30</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="OneI">
    <xbrli:entity><xbrli:identifier scheme="http://www.nseindia.com/NSESymbol">RELIANCE</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2026-09-30</xbrli:instant></xbrli:period>
  </xbrli:context>
  <in-bse-fin:NatureOfReportStandaloneConsolidated contextRef="OneD">Consolidated</in-bse-fin:NatureOfReportStandaloneConsolidated>
  <in-bse-fin:RevenueFromOperations contextRef="OneD" unitRef="INR" decimals="-7">2350000000000</in-bse-fin:RevenueFromOperations>
  <in-bse-fin:RevenueFromOperations contextRef="TwoD" unitRef="INR" decimals="-7">4650000000000</in-bse-fin:RevenueFromOperations>
  <in-bse-fin:CashFlowsFromUsedInOperatingActivities contextRef="TwoD" unitRef="INR" decimals="-7">610000000000</in-bse-fin:CashFlowsFromUsedInOperatingActivities>
  <in-bse-fin:CashAndCashEquivalents contextRef="OneI" unitRef="INR" decimals="-7">854000000000</in-bse-fin:CashAndCashEquivalents>
</xbrli:xbrl>
"""

SAMPLE_SHP_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
            xmlns:in-bse-shp="http://www.bseindia.com/xbrl/shp/2018-03-31/in-bse-shp">
  <xbrli:context id="c_promo">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.bseindia.com/ScripCode">500325</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="in-bse-shp:ShareholdingPatternAxis">in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2026-09-30</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="c_lic_typed">
    <xbrli:entity>
      <xbrli:identifier scheme="http://www.bseindia.com/ScripCode">500325</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="in-bse-shp:ShareholdingPatternAxis">in-bse-shp:InsuranceCompaniesMember</xbrldi:explicitMember>
        <xbrldi:typedMember dimension="in-bse-shp:DetailsOfSharesHeldByInstitutionsAxis">
          <in-bse-shp:DetailsOfSharesHeldByInstitutionsDomain>Inst_1</in-bse-shp:DetailsOfSharesHeldByInstitutionsDomain>
        </xbrldi:typedMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2026-09-30</xbrli:instant></xbrli:period>
  </xbrli:context>
  <in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares contextRef="c_promo">50.31</in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares>
  <in-bse-shp:NameOfTheShareHolders contextRef="c_lic_typed">Life Insurance Corporation of India</in-bse-shp:NameOfTheShareHolders>
  <in-bse-shp:NumberOfShares contextRef="c_lic_typed">432100000</in-bse-shp:NumberOfShares>
  <in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares contextRef="c_lic_typed">6.38</in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares>
</xbrli:xbrl>
"""


def test_corporate_action_regex_parser():
    r1 = parse_action_purpose("Face Value Split (Sub-Division) - From Rs 10/- Per Share To Re 1/- Per Share")
    assert r1["ActionType"] == "SPLIT"
    assert r1["ShareMultiplier"] == 10.0
    assert r1["DividendPerShareRs"] is None

    r2 = parse_action_purpose("Final Dividend - Rs. - 12.5000")
    assert r2["ActionType"] == "DIVIDEND"
    assert r2["DividendPerShareRs"] == 12.5

    r3 = parse_action_purpose("Buyback Of Equity Shares At Rs 500 Per Share / Dividend Rs 5 Per Share")
    assert r3["ActionType"] == "DIVIDEND"
    assert r3["DividendPerShareRs"] == 5.0

    r4 = parse_action_purpose("Bonus issue 1:1 on Non-Convertible Debentures (NCD)")
    assert r4["ActionType"] == "SKIP_NON_EQUITY"
    assert r4["ShareMultiplier"] == 1.0


def test_repair_ohlc_and_bse_debt_filter():
    corrupt = pd.DataFrame([
        {"Symbol": "A", "Series": "A", "ISIN": "INE002A01018", "Open": 0.0, "High": 95.0, "Low": 105.0, "Close": 102.0, "PrevClose": 100.0},
        {"Symbol": "GOI", "Series": "G", "ISIN": "IN0020110048", "Open": 100.0, "High": 100.0, "Low": 100.0, "Close": 100.0, "PrevClose": 100.0},
        {"Symbol": "BOND", "Series": "F", "ISIN": "INE134E07588", "Open": 1000.0, "High": 1000.0, "Low": 1000.0, "Close": 1000.0, "PrevClose": 1000.0},
        {"Symbol": "ETF", "Series": "F", "ISIN": "INF204KB10R1", "Open": 150.0, "High": 152.0, "Low": 149.0, "Close": 151.0, "PrevClose": 150.0},
    ])
    filtered = filter_bse_equity_only(corrupt)
    assert list(filtered["Symbol"]) == ["A", "ETF"]

    repaired = repair_ohlc(filtered)
    assert repaired.iloc[0]["Open"] == 100.0
    assert repaired.iloc[0]["High"] == 105.0
    assert repaired.iloc[0]["Low"] == 95.0


def test_xbrl_parsers():
    fin_rows = XBRLScraper.parse_indas_xbrl(SAMPLE_Q2_XBRL, "RELIANCE", "https://example.com/INDAS.xml")
    assert len(fin_rows) == 2
    by_type = {r["PeriodType"]: r for r in fin_rows}
    assert by_type["Quarterly"]["Sales_Cr"] == 235000.0
    assert by_type["HalfYearly"]["Sales_Cr"] == 465000.0
    assert by_type["Quarterly"]["CashAndEquivalents_Cr"] == 85400.0

    shp = XBRLScraper.parse_full_shp_xbrl(SAMPLE_SHP_XBRL)
    assert shp["summary"]["PromoterPct"] == 50.31
    assert shp["named_holders"][0]["HolderName"] == "Life Insurance Corporation of India"
    assert shp["named_holders"][0]["PctOfTotal"] == 6.38


def test_doctor_self_healing_and_adjuster():
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)

        (cfg.root_dir / "equities" / "nse_cm_2026.parquet.tmp").write_bytes(b"orphan tmp")
        (cfg.root_dir / "equities" / "nse_cm_2025.parquet").write_bytes(b"corrupt bytes")

        df_good = pd.DataFrame({"Date": [pd.Timestamp("2024-10-29")] * 250, "Symbol": [f"S{i}" for i in range(250)], "Close": [100.0] * 250})
        df_trunc = pd.DataFrame({"Date": [pd.Timestamp("2024-10-30")] * 3, "Symbol": ["A", "B", "C"], "Close": [10.0, 20.0, 30.0]})
        write_parquet_atomic(pd.concat([df_good, df_trunc], ignore_index=True), cfg.root_dir / "equities" / "nse_cm_2024.parquet")

        state.record("equity_eod_nse", "2024-10-30", "OK", row_count=3)
        state.record("equity_eod_nse", "2024-10-31", "OK", row_count=1500)

        report = LakeDoctor(cfg).run_full_check_and_repair()
        assert report["tmp_files_cleaned"] == 1
        assert report["corrupt_parquets_removed"] == 1
        assert report["truncated_days_purged"] == 1
        assert report["stale_watermarks_reset"] == 2
        assert report["watermarks_rebuilt_from_disk"] == 1
        assert state.is_completed("equity_eod_nse", "2024-10-29") is True
        assert state.is_completed("equity_eod_nse", "2024-10-30") is False

        # SymbolRenameResolver + OHLCVAdjuster test
        sc_df = pd.DataFrame([{"CompanyName": "Infosys", "OldSymbol": "INFOSYSTCH", "NewSymbol": "INFY", "ChangeDate": pd.Timestamp("2011-06-29")}])
        write_parquet_atomic(sc_df, cfg.root_dir / "universe" / "nse_symbol_changes.parquet")

        act_df = pd.DataFrame([
            {"symbol": "INFY", "ExDate": pd.Timestamp("2026-10-25"), "DividendPerShareRs": 20.0, "ShareMultiplier": 1.0, "ActionType": "DIVIDEND"},
            {"symbol": "INFY", "ExDate": pd.Timestamp("2026-10-28"), "DividendPerShareRs": 0.0, "ShareMultiplier": 2.0, "ActionType": "BONUS"},
        ])
        write_parquet_atomic(act_df, cfg.root_dir / "actions" / "nse_corporate_actions.parquet")

        raw_ohlc = pd.DataFrame([
            {"Date": pd.Timestamp("2026-10-23"), "Open": 1000.0, "High": 1000.0, "Low": 1000.0, "Close": 1000.0, "Volume": 100, "DeliverableQty": 50},
            {"Date": pd.Timestamp("2026-10-26"), "Open": 980.0,  "High": 980.0,  "Low": 980.0,  "Close": 980.0,  "Volume": 100, "DeliverableQty": 50},
            {"Date": pd.Timestamp("2026-10-27"), "Open": 1000.0, "High": 1000.0, "Low": 1000.0, "Close": 1000.0, "Volume": 100, "DeliverableQty": 50},
            {"Date": pd.Timestamp("2026-10-28"), "Open": 500.0,  "High": 500.0,  "Low": 500.0,  "Close": 500.0,  "Volume": 200, "DeliverableQty": 100},
        ])
        adj = OHLCVAdjuster(cfg.root_dir).apply_adjustments(raw_ohlc, "INFOSYSTCH", exchange="NSE", auto_adjust=False)
        assert adj.loc[0, "Adj Close"] == 490.0
        assert adj.loc[1, "Adj Close"] == 490.0


def test_sanitize_for_parquet_mixed_types():
    from datalake.storage import upsert_parquet_partition

    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.DataFrame({
            "MixedCol": [100, "200", 300, None, b"400", float("nan")],
            "StrCol": ["A", "B", "C", "D", "E", "F"],
            "IntCol": [1, 2, 3, 4, 5, 6],
            "SplitFromFV": [b"10.0", "5.0", 2.0, None, float("nan"), pd.NA],
            "SplitToFV": [None, None, 1.0, None, None, None],
        })
        target = Path(tmpdir) / "test_mixed.parquet"
        write_parquet_atomic(df, target)
        read_df = read_table_safe(target)
        assert read_df["MixedCol"].iloc[0] == "100"
        assert read_df["MixedCol"].iloc[1] == "200"
        assert pd.isna(read_df["MixedCol"].iloc[3])
        assert read_df["MixedCol"].iloc[4] == "400"
        assert pd.isna(read_df["MixedCol"].iloc[5])
        assert pd.api.types.is_float_dtype(read_df["SplitFromFV"])
        assert read_df["SplitFromFV"].iloc[0] == 10.0
        assert read_df["SplitFromFV"].iloc[2] == 2.0
        assert pd.isna(read_df["SplitFromFV"].iloc[3])

        # Verify upserting an all-None object batch followed by a float batch never raises ArrowTypeError
        act_target = Path(tmpdir) / "nse_corporate_actions.parquet"
        df_sme = pd.DataFrame({
            "symbol": ["SME1", "SME2"],
            "series": ["SM", "SM"],
            "exDate": ["01-Jan-2026", "02-Jan-2026"],
            "subject": ["Dividend Rs 2", "AGM"],
            "ExDate": [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")],
            "SplitFromFV": [None, None],
            "SplitToFV": [None, None],
            "ShareMultiplier": [1.0, 1.0],
        })
        upsert_parquet_partition(df_sme, act_target, dedup_cols=["symbol", "series", "exDate", "subject"])

        df_eq = pd.DataFrame({
            "symbol": ["EQ1", "EQ2"],
            "series": ["EQ", "EQ"],
            "exDate": ["03-Jan-2026", "04-Jan-2026"],
            "subject": ["Split 10 to 2", "Bonus 1:1"],
            "ExDate": [pd.Timestamp("2026-01-03"), pd.Timestamp("2026-01-04")],
            "SplitFromFV": [10.0, float("nan")],
            "SplitToFV": [2.0, float("nan")],
            "ShareMultiplier": [5.0, 2.0],
        })
        total = upsert_parquet_partition(df_eq, act_target, dedup_cols=["symbol", "series", "exDate", "subject"])
        assert total == 4
        merged_act = read_table_safe(act_target)
        assert pd.api.types.is_float_dtype(merged_act["SplitFromFV"])
        assert pd.api.types.is_float_dtype(merged_act["SplitToFV"])


def test_normalize_deals_frame():
    raw_hist = pd.DataFrame({
        "BD_DT_DATE": ["05-JAN-2022", "NO RECORDS FOUND"],
        "BD_SYMBOL": ["RELIANCE", ""],
        "BD_SCRIP_NAME": ["Reliance Industries", ""],
        "BD_CLIENT_NAME": ["ACE INVESTMENTS", ""],
        "BD_BUY_SELL": ["BUY", ""],
        "BD_QTY_TRD": ["1,500,000", "0"],
        "BD_TP_WATP": ["2,450.50", "0"],
        "BD_REMARKS": ["DEAL 1", ""],
    })
    res = _normalize_deals_frame(raw_hist, deal_type="BULK", exchange="NSE")
    assert len(res) == 1
    assert res.iloc[0]["Symbol"] == "RELIANCE"
    assert res.iloc[0]["Quantity Traded"] == 1500000.0
    assert res.iloc[0]["Trade Price / Wght. Avg. Price"] == 2450.50
    assert res.iloc[0]["DealType"] == "BULK"
    assert res.iloc[0]["Exchange"] == "NSE"


if __name__ == "__main__":
    test_corporate_action_regex_parser()
    test_repair_ohlc_and_bse_debt_filter()
    test_xbrl_parsers()
    test_doctor_self_healing_and_adjuster()
    test_sanitize_for_parquet_mixed_types()
    test_normalize_deals_frame()
    print("ALL OFFLINE UNIT & INTEGRATION TESTS PASSED!")
