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


def test_fo_2000_int64_ohlc_and_doctor_preservation():
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)

        # Simulate 11-Jul-2000 NSE F&O bhavcopy where OPEN/LOW are int64 with 0s and CLOSE is float64
        csv_2000 = (
            "INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP\n"
            "FUTIDX,S&P CNX NIFTY,27-Jul-2000,0,XX,1517,1525,1508,1518.95,1518.95,1886,5712.92,402800,21400,11-JUL-2000\n"
            "FUTIDX,S&P CNX NIFTY,31-Aug-2000,0,XX,0,1525,0,1515.05,1515.05,94,286.67,72000,1800,11-JUL-2000\n"
            "FUTIDX,S&P CNX NIFTY,28-Sep-2000,0,XX,0,1538,0,1538.00,1538.00,26,79.88,28400,400,11-JUL-2000\n"
        )
        zip_bytes = _make_zip("fo11JUL2000bhav.csv", csv_2000)

        class _DummyFOHttp:
            async def get(self, url, **kwargs):
                return SimpleHTTPResponse(status_code=200, content=zip_bytes)

        fo_scraper = DerivativesEODScraper(_DummyFOHttp(), state, cfg)
        df_fo, _, _ = asyncio.run(fo_scraper.fetch_nse_fo_day(date(2000, 7, 11)))
        assert df_fo is not None and len(df_fo) == 3
        assert df_fo.iloc[1]["Open"] == 1515.05
        assert df_fo.iloc[1]["Low"] == 1515.05

        write_parquet_atomic(df_fo, cfg.root_dir / "derivatives" / "nse_fo_2000_07.parquet")
        state.record("equity_eod_bse", "1995-01-02", "HOLIDAY")
        state.record("fo_eod_bse", "2001-01-02", "HOLIDAY")

        report = LakeDoctor(cfg).run_full_check_and_repair()
        assert report["truncated_days_purged"] == 0
        assert (cfg.root_dir / "derivatives" / "nse_fo_2000.parquet").exists()
        assert not (cfg.root_dir / "derivatives" / "nse_fo_2000_07.parquet").exists()
        assert state.is_completed("fo_eod_nse", "2000-07-11") is True
        assert state.is_completed("equity_eod_bse", "1995-01-02") is False
        assert state.is_completed("fo_eod_bse", "2001-01-02") is False


def test_bse_concalls_chunked_windows():
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)
        seen_ranges: list[tuple[str, str]] = []

        class _DummyBSEHttp:
            async def get(self, url, params=None, **kwargs):
                if params:
                    seen_ranges.append((params["strPrevDate"], params["strToDate"]))
                payload = json.dumps({
                    "Table": [{
                        "NEWSID": f"id-{len(seen_ranges)}",
                        "SCRIP_CD": "500325",
                        "SLONGNAME": "Reliance Industries",
                        "NEWSSUB": "Earnings Call Transcript",
                        "NEWS_DT": "2026-10-15T18:00:00",
                        "ATTACHMENTNAME": "rel_q2.pdf",
                        "OLD": 0,
                    }]
                }).encode("utf-8")
                return SimpleHTTPResponse(status_code=200, content=payload)

        scraper = DealsAndDocumentsScraper(_DummyBSEHttp(), state, cfg)
        asyncio.run(scraper.sync_concalls_and_presentations(date(2026, 8, 1), date(2026, 10, 30)))
        assert (cfg.root_dir / "documents" / "concalls_and_filings.parquet").exists()
        for prev_dt, to_dt in seen_ranges:
            d1 = pd.to_datetime(prev_dt, format="%Y%m%d")
            d2 = pd.to_datetime(to_dt, format="%Y%m%d")
            assert (d2 - d1).days <= 27


def test_bug_fix_2_audit_suite():
    # 1. Verify BUG-01: _safe_float
    from datalake.scrapers.xbrl import _safe_float
    assert pd.isna(_safe_float(None))
    assert pd.isna(_safe_float("-"))
    assert _safe_float("1,23,45,678") == 12345678.0
    assert _safe_float("50.31") == 50.31

    # 2. Verify BUG-04: UTF-8 BOM
    raw_bom_csv = b"\xef\xbb\xbf\"Date \",\"Symbol \",\"Security Name \",\"Client Name \",\"Buy / Sell \",\"Quantity Traded \",\"Trade Price / Wght. Avg. Price \",\"Remarks \"\n\"30-OCT-2026\",\"RELIANCE\",\"Reliance Ind\",\"FUND A\",\"BUY\",\"1,00,000\",\"1350.50\",\"-\"\n"
    resp = SimpleHTTPResponse(status_code=200, content=raw_bom_csv)
    assert not resp.text.startswith("\ufeff")
    raw_df = pd.read_csv(io.StringIO(resp.text))
    norm_df = _normalize_deals_frame(raw_df, deal_type="BULK", exchange="NSE")
    assert len(norm_df) == 1
    assert norm_df.iloc[0]["Symbol"] == "RELIANCE"

    # 3. Verify BUG-05: Annual reports dedup
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)

        class _MockARHttp:
            async def get(self, url, **kwargs):
                if "nseindia.com" in url:
                    sym = url.split("symbol=")[-1]
                    return SimpleHTTPResponse(
                        status_code=200,
                        content=f'{{"data":[{{"fromYr":"2025","toYr":"2026","fileName":"https://nsearchives.nseindia.com/ar/{sym}_2026.pdf"}}]}}'.encode(),
                    )
                scrip = url.split("scripcode=")[-1].split("&")[0]
                return SimpleHTTPResponse(
                    status_code=200,
                    content=f'{{"Table":[{{"year":"2026","file_name":"report_2026.pdf"}}]}}'.encode(),
                )

        dd = DealsAndDocumentsScraper(_MockARHttp(), state, cfg)
        asyncio.run(dd.sync_annual_reports_bse(["500325", "532540"], symbols=["RELIANCE", "TCS"]))
        df = read_table_safe(cfg.root_dir / "documents" / "annual_reports.parquet")
        assert len(df) == 4, f"Expected 4 annual reports, got {len(df)}"

    # 4. Verify BUG-07: Cyclic symbol renames
    sc_df = pd.DataFrame([
        {"CompanyName": "Test Corp", "OldSymbol": "SYMA", "NewSymbol": "SYMB", "ChangeDate": pd.Timestamp("2005-01-01")},
        {"CompanyName": "Test Corp", "OldSymbol": "SYMB", "NewSymbol": "SYMA", "ChangeDate": pd.Timestamp("2012-01-01")},
    ])
    resolver = SymbolRenameResolver(sc_df)
    assert resolver.traded_as_on("SYMA", date(2015, 1, 1)) == "SYMA"
    assert resolver.traded_as_on("SYMA", date(2008, 1, 1)) == "SYMB"
    assert resolver.traded_as_on("SYMA", date(2003, 1, 1)) == "SYMA"

    # 5. Verify BUG-09: Compound corporate action parser case-insensitive
    res = parse_action_purpose("Interim Dividend - Rs 5 Per Share And Face Value Split From Rs 10 To Rs 2")
    assert res["DividendPerShareRs"] == 5.0
    assert res["ShareMultiplier"] == 5.0

    # 6. Verify BUG-12: Empty XML and repair_ohlc without PrevClose
    df_no_prev = pd.DataFrame([{"Open": 0.0, "High": 100.0, "Low": 90.0, "Close": 95.0}])
    rep = repair_ohlc(df_no_prev)
    assert rep.iloc[0]["Open"] == 95.0

    assert XBRLScraper.parse_indas_xbrl(b"", "RELIANCE", "http://test") == []
    assert XBRLScraper.parse_full_shp_xbrl(b"<!-- empty comment -->") == {"summary": {}, "named_holders": []}


def test_pass_1_audit_suite():
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "actions").mkdir(parents=True)
        (root / "universe").mkdir(parents=True)

        # 1. Verify P1-07: StateStore.max_ok_date ignores non-ISO keys
        st = StateStore(root / "state.db")
        st.record("test_ds", "2026-10-30", "OK", 100)
        st.record("test_ds", "RELIANCE:2026", "OK", 10)
        assert st.max_ok_date("test_ds") == date(2026, 10, 30), "max_ok_date failed with non-ISO key!"

        # 2. Verify P1-01 & P1-02: OHLCVAdjuster applies future splits when querying historical window
        act_df = pd.DataFrame([{
            "symbol": "RELIANCE",
            "ExDate": pd.Timestamp("2024-10-28"),
            "DividendPerShareRs": 0.0,
            "ShareMultiplier": 2.0,
            "ActionType": "BONUS",
        }])
        write_parquet_atomic(act_df, root / "actions" / "nse_corporate_actions.parquet")

        df_2023 = pd.DataFrame([
            {"Date": pd.Timestamp("2023-12-28"), "Open": 2600.0, "High": 2600.0, "Low": 2600.0, "Close": 2600.0, "Volume": 100},
            {"Date": pd.Timestamp("2023-12-29"), "Open": 2580.0, "High": 2580.0, "Low": 2580.0, "Close": 2580.0, "Volume": 100},
        ])
        adj = OHLCVAdjuster(root).apply_adjustments(df_2023, "RELIANCE", exchange="NSE", include_future_splits=True)
        assert adj.loc[0, "Adj Close"] == 1300.0
        assert adj.loc[1, "Adj Close"] == 1290.0


def test_pass_2_audit_suite():
    # 1. Verify P2-01: NSE UDIFF sets InstrumentId = Symbol ('RELIANCE'), while BSE sets FinInstrmId ('500325')
    udiff_sample = pd.DataFrame([{
        "Sgmt": "CM",
        "FinInstrmId": 2885,
        "TckrSymb": "RELIANCE",
        "SctySrs": "EQ",
        "ISIN": "INE002A01018",
        "OpnPric": 1300.0,
        "HghPric": 1320.0,
        "LwPric": 1290.0,
        "ClsPric": 1310.0,
        "LastPric": 1310.0,
        "PrvsClsgPric": 1295.0,
        "TtlTradgVol": 100000,
        "TtlTrfVal": 131000000.0,
        "TtlNbOfTxsExctd": 5000,
    }])
    nse_norm = EquityEODScraper._normalize_udiff(udiff_sample.copy(), date(2026, 10, 30), "NSE")
    assert nse_norm.iloc[0]["InstrumentId"] == "RELIANCE", f"Expected RELIANCE, got {nse_norm.iloc[0]['InstrumentId']}"

    bse_sample = udiff_sample.copy()
    bse_sample["FinInstrmId"] = 500325.0
    bse_norm = EquityEODScraper._normalize_udiff(bse_sample, date(2026, 10, 30), "BSE")
    assert bse_norm.iloc[0]["InstrumentId"] == "500325", f"Expected 500325, got {bse_norm.iloc[0]['InstrumentId']}"

    # 2. Verify P2-03: FO zero close repair
    fo_sample = pd.DataFrame([{
        "FinInstrmTp": "OPTSTK",
        "TckrSymb": "RELIANCE",
        "XpryDt": "2026-11-26",
        "StrkPric": 1300.0,
        "OptnTp": "CE",
        "OpnPric": 0.0,
        "HghPric": 0.0,
        "LwPric": 0.0,
        "ClsPric": 0.0,
        "SttlmPric": 25.5,
        "UndrlygPric": 1310.0,
        "TtlTradgVol": 0.0,
        "TtlTrfVal": 0.0,
        "OpnIntrst": 50000.0,
        "ChngInOpnIntrst": 1000.0,
    }])
    fo_norm = DerivativesEODScraper._normalize_fo_udiff(fo_sample, date(2026, 10, 30), "NSE")
    assert fo_norm.iloc[0]["Close"] == 25.5
    assert fo_norm.iloc[0]["Open"] == 25.5

    # 3. Verify P2-04: AMFI parser enforces integer SchemeCode across 6-col, NAVAll 8-col, and DownloadNAVHistoryReport 8-col
    amfi_txt = (
        "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date\n"
        "Open Ended Schemes (Equity)\n"
        "HDFC Mutual Fund\n"
        "100122;INF179K01608;-;HDFC Balanced Fund;150.25;30-Oct-2026\n"
        "139618;Taurus Investor Education Pool;;Growth;;;10.0000;01-Oct-2024\n"
        "INVALID_CODE;-;-;Bad Row;10.0;30-Oct-2026\n"
    )
    df_amfi = FundsAndFlowsScraper._parse_amfi_text(amfi_txt)
    assert len(df_amfi) == 2
    assert pd.api.types.is_integer_dtype(df_amfi["SchemeCode"])
    assert df_amfi.iloc[0]["SchemeCode"] == 100122
    assert df_amfi.iloc[1]["SchemeCode"] == 139618
    assert df_amfi.iloc[1]["NAV"] == 10.0


def test_pass_3_audit_suite():
    # 1. Verify P3-01: iXBRL HTML with nested <span> tags inside ix:nonNumeric / ix:nonFraction
    ixbrl_html = b"""<?xml version="1.0" encoding="UTF-8"?>
    <html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
          xmlns:xbrli="http://www.xbrl.org/2003/instance"
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
      <xbrli:context id="c_holder">
        <xbrli:entity>
          <xbrli:identifier scheme="http://www.bseindia.com/ScripCode">500325</xbrli:identifier>
          <xbrli:segment>
            <xbrldi:explicitMember dimension="in-bse-shp:ShareholdingPatternAxis">in-bse-shp:InsuranceCompaniesMember</xbrldi:explicitMember>
            <xbrldi:typedMember dimension="in-bse-shp:DetailsOfSharesHeldByInstitutionsAxis">
              <in-bse-shp:DetailsOfSharesHeldByInstitutionsDomain><span>Inst_1</span></in-bse-shp:DetailsOfSharesHeldByInstitutionsDomain>
            </xbrldi:typedMember>
          </xbrli:segment>
        </xbrli:entity>
        <xbrli:period><xbrli:instant>2026-09-30</xbrli:instant></xbrli:period>
      </xbrli:context>
      <ix:nonFraction name="in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares" contextRef="c_promo"><span>50.31</span></ix:nonFraction>
      <ix:nonNumeric name="in-bse-shp:NameOfTheShareHolders" contextRef="c_holder"><span>LIC OF INDIA</span></ix:nonNumeric>
      <ix:nonFraction name="in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares" contextRef="c_holder"><span>6.38</span></ix:nonFraction>
    </html>
    """
    parsed = XBRLScraper.parse_full_shp_xbrl(ixbrl_html)
    assert parsed["summary"]["PromoterPct"] == 50.31
    assert len(parsed["named_holders"]) == 1
    assert parsed["named_holders"][0]["HolderName"] == "LIC OF INDIA"

    # 2. Verify P3-02: LakeDoctor restores exact row_count (250, not 1) into state.db
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        df_good = pd.DataFrame({
            "Date": [pd.Timestamp("2024-10-29")] * 250,
            "Symbol": [f"S{i}" for i in range(250)],
            "Close": [100.0] * 250,
        })
        write_parquet_atomic(df_good, cfg.root_dir / "equities" / "nse_cm_2024.parquet")
        LakeDoctor(cfg).run_full_check_and_repair()
        summary = {r["dataset"]: r for r in StateStore(cfg.state_db_path).summary()}
        assert summary["equity_eod_nse"]["total_rows"] == 250, f"Expected 250 rows, got {summary['equity_eod_nse']['total_rows']}"

        # 3. Verify empty XBRL filings record HOLIDAY in state.db so xbrl-all-market never gets stuck
        class _EmptyXBRLHttp:
            async def get(self, url, **kwargs):
                if "bseindia.com" in url:
                    return SimpleHTTPResponse(status_code=200, content=b'{"Table": []}')
                return SimpleHTTPResponse(status_code=200, content=b"[]")

        st = StateStore(cfg.state_db_path)
        xbrl = XBRLScraper(_EmptyXBRLHttp(), st, cfg)
        asyncio.run(xbrl.sync_symbol_financials_nse("EMPTYCO"))
        asyncio.run(xbrl.sync_bse_shareholding("599999"))
        assert st.is_completed("xbrl_financials_symbol", "EMPTYCO") is True
        assert st.is_completed("bse_shareholding", "599999") is True


def test_after_fix_2_audit_suite():
    # 1. Verify LakeDoctor OHLC & Shareholding domain invariants
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        # Invalid OHLC: row 0 has Close <= 0 (should be dropped), row 1 has High < Low (should be swapped)
        df_ohlc = pd.DataFrame({
            "Date": [pd.Timestamp("2000-01-03"), pd.Timestamp("2000-01-04")],
            "Symbol": ["A", "B"],
            "Open": [10.0, 10.0],
            "High": [12.0, 5.0],   # High < Low
            "Low": [8.0, 15.0],
            "Close": [0.0, 12.0],  # Close <= 0
        })
        write_parquet_atomic(df_ohlc, cfg.root_dir / "equities" / "nse_cm_2000.parquet")

        # Invalid Shareholding: row 0 valid, row 1 invalid (>100.5%)
        df_shp = pd.DataFrame({
            "Symbol": ["A", "B"],
            "QuarterEnd": ["2024-03-31", "2024-03-31"],
            "PromoterPct": [50.0, 150.0],
            "PublicPct": [45.0, 20.0],
        })
        write_parquet_atomic(df_shp, cfg.root_dir / "shareholding" / "nse_shareholding_summary.parquet")

        doctor = LakeDoctor(cfg)
        doctor.run_full_check_and_repair()

        repaired_ohlc = read_table_safe(cfg.root_dir / "equities" / "nse_cm_2000.parquet")
        assert len(repaired_ohlc) == 1, f"Expected 1 row after dropping Close<=0, got {len(repaired_ohlc)}"
        assert repaired_ohlc.iloc[0]["High"] >= repaired_ohlc.iloc[0]["Low"], "High < Low was not repaired"

        repaired_shp = read_table_safe(cfg.root_dir / "shareholding" / "nse_shareholding_summary.parquet")
        assert len(repaired_shp) == 1, f"Expected 1 valid shareholding row, got {len(repaired_shp)}"
        assert repaired_shp.iloc[0]["Symbol"] == "A"

    # 2. Verify build_manifest produces latest_dates watermarks
    from datalake.sync import build_manifest
    with tempfile.TemporaryDirectory() as tmpdir:
        t_path = Path(tmpdir)
        eq_dir = t_path / "equities"
        eq_dir.mkdir(parents=True)
        df_m = pd.DataFrame({"Date": [pd.Timestamp("2026-02-15")], "Symbol": ["TEST"], "Close": [100.0]})
        write_parquet_atomic(df_m, eq_dir / "nse_cm_2026.parquet")
        manifest = build_manifest(t_path)
        assert "latest_dates" in manifest
        assert manifest["latest_dates"]["equities/nse_cm_2026.parquet"] == "2026-02-15"


def test_nsearchives_resilience_and_uncaught_guards():
    from unittest.mock import AsyncMock, MagicMock
    from datalake.http_client import ResilientHTTPClient

    # 1. Verify 404 responses from nsearchives reset fail streak to 0 and do not trip circuit breaker
    client = ResilientHTTPClient()
    mock_404 = MagicMock(status_code=404, content=b"Not Found", headers={})
    if client.curl_session is not None:
        client.curl_session.get = AsyncMock(return_value=mock_404)
    if client.client is not None:
        client.client.get = AsyncMock(return_value=mock_404)

    url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_05112026.csv"
    for _ in range(7):
        resp = asyncio.run(client.get(url))
        assert resp.status_code == 404
        assert client._nsearchives_fail_streak == 0, f"Expected streak 0 on 404, got {client._nsearchives_fail_streak}"

    # Verify that fail streak increments on server error / network failure and trips at >= 8
    mock_410 = MagicMock(status_code=410, content=b"Gone", headers={})
    if client.curl_session is not None:
        client.curl_session.get = AsyncMock(return_value=mock_410)
    if client.client is not None:
        client.client.get = AsyncMock(return_value=mock_410)

    # Trigger failures until streak hits 8
    client._nsearchives_fail_streak = 7
    # One more non-200/404 response
    asyncio.run(client.get(url))
    assert client._nsearchives_fail_streak >= 8

    # Next call must raise RuntimeError circuit breaker
    tripped = False
    try:
        asyncio.run(client.get(url))
    except RuntimeError as exc:
        tripped = True
        assert "unreachable from this runner IP" in str(exc)
    assert tripped, "Circuit breaker should trip when _nsearchives_fail_streak >= 8"

    # 2. Verify DealsAndDocumentsScraper.sync_bulk_and_block_deals survives unreachable archive host
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LakeConfig(root_dir=Path(tmpdir))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)

        failing_http = MagicMock()
        failing_http.get = AsyncMock(side_effect=RuntimeError("nsearchives.nseindia.com is unreachable from this runner IP"))

        dd = DealsAndDocumentsScraper(failing_http, state, cfg)
        # Must not raise RuntimeError
        asyncio.run(dd.sync_bulk_and_block_deals())

        # 3. Verify UniverseAndActionsScraper.sync_security_masters survives unreachable archive host & HTML error responses
        univ = UniverseAndActionsScraper(failing_http, state, cfg)
        # Must not raise RuntimeError
        asyncio.run(univ.sync_security_masters())

        # Also test with HTML error page response (e.g. 200 with HTML)
        html_http = MagicMock()
        html_http.get = AsyncMock(return_value=SimpleHTTPResponse(
            status_code=200,
            content=b"<!DOCTYPE html><html><body>Access Denied</body></html>",
            headers={"Content-Type": "text/html"}
        ))
        univ_html = UniverseAndActionsScraper(html_http, state, cfg)
        asyncio.run(univ_html.sync_security_masters())


if __name__ == "__main__":
    test_corporate_action_regex_parser()
    test_repair_ohlc_and_bse_debt_filter()
    test_xbrl_parsers()
    test_doctor_self_healing_and_adjuster()
    test_sanitize_for_parquet_mixed_types()
    test_normalize_deals_frame()
    test_fo_2000_int64_ohlc_and_doctor_preservation()
    test_bse_concalls_chunked_windows()
    test_bug_fix_2_audit_suite()
    test_pass_1_audit_suite()
    test_pass_2_audit_suite()
    test_pass_3_audit_suite()
    test_after_fix_2_audit_suite()
    test_nsearchives_resilience_and_uncaught_guards()
    print("ALL OFFLINE UNIT & INTEGRATION TESTS PASSED!")
