import shutil
import tempfile
import unittest
from pathlib import Path
import pandas as pd

from datalake.engine import LakeTicker, SecurityIdentity, UnifiedLakeEngine


class TestUnifiedLakeEngine(unittest.TestCase):
    _tmp_dir = None

    @classmethod
    def setUpClass(cls):
        smoke = Path("smoke_data")
        if (smoke / "universe" / "nse_equity_master.parquet").exists():
            cls.data_path = smoke
        else:
            cls._tmp_dir = tempfile.TemporaryDirectory()
            cls.data_path = Path(cls._tmp_dir.name)
            for sub in ("universe", "equities", "derivatives", "indices", "mutual_funds", "actions"):
                (cls.data_path / sub).mkdir(parents=True, exist_ok=True)
            pd.DataFrame([{"SYMBOL": "RELIANCE", "NAME OF COMPANY": "Reliance Industries", "SERIES": "EQ", "ISIN NUMBER": "INE002A01018"}]).to_parquet(cls.data_path / "universe" / "nse_equity_master.parquet")
            pd.DataFrame([{"SCRIP_CD": "500325", "Scrip_Name": "Reliance Industries", "ISIN_NUMBER": "INE002A01018", "scrip_id": "RELIANCE"}]).to_parquet(cls.data_path / "universe" / "bse_scrip_master.parquet")
            pd.DataFrame([{"Date": pd.Timestamp("2026-01-02"), "Exchange": "NSE", "InstrumentId": "RELIANCE", "Symbol": "RELIANCE", "Series": "EQ", "Open": 1300.0, "High": 1310.0, "Low": 1290.0, "Close": 1305.0, "Volume": 1000}]).to_parquet(cls.data_path / "equities" / "nse_cm_2026.parquet")
            pd.DataFrame([{"Date": pd.Timestamp("2026-01-02"), "Exchange": "NSE", "InstrumentType": "OPTIDX", "Symbol": "NIFTY", "ExpiryDate": pd.Timestamp("2026-01-29"), "StrikePrice": 24000.0, "OptionType": "CE", "Open": 200.0, "High": 220.0, "Low": 190.0, "Close": 210.0, "SettlePrice": 210.0, "Contracts": 100.0, "OpenInterest": 5000.0, "ChangeInOI": 100.0}]).to_parquet(cls.data_path / "derivatives" / "nse_fo_2026.parquet")
            pd.DataFrame([{"Date": pd.Timestamp("2026-01-02"), "IndexName": "NIFTY 50", "Open": 24000.0, "High": 24100.0, "Low": 23950.0, "Close": 24050.0}]).to_parquet(cls.data_path / "indices" / "nifty_30y_ohlc_tri_pe.parquet")
            pd.DataFrame([{"Date": pd.Timestamp("2026-01-02"), "SchemeCode": 100122, "SchemeName": "HDFC Fund", "NAV": 150.5}]).to_parquet(cls.data_path / "mutual_funds" / "amfi_nav_2026.parquet")

    @classmethod
    def tearDownClass(cls):
        if cls._tmp_dir is not None:
            cls._tmp_dir.cleanup()

    def setUp(self):
        self.lake = UnifiedLakeEngine(self.data_path)

    def test_crosswalk_resolution(self):
        # Resolve by NSE symbol
        id_nse = self.lake.resolve("RELIANCE")
        self.assertEqual(id_nse.symbol, "RELIANCE")
        self.assertEqual(id_nse.bse_code, "500325")
        self.assertEqual(id_nse.isin, "INE002A01018")

        # Resolve by BSE code
        id_bse = self.lake.resolve("500325")
        self.assertEqual(id_bse.symbol, "RELIANCE")

        # Resolve by ISIN
        id_isin = self.lake.resolve("INE002A01018")
        self.assertEqual(id_isin.symbol, "RELIANCE")

        # Resolve with .NS or .BO suffix
        id_suffixed = self.lake.resolve("RELIANCE.NS")
        self.assertEqual(id_suffixed.symbol, "RELIANCE")

    def test_ticker_facade(self):
        ticker = self.lake.ticker("500325")
        self.assertIsInstance(ticker, LakeTicker)
        self.assertEqual(ticker.symbol, "RELIANCE")
        self.assertEqual(ticker.isin, "INE002A01018")
        self.assertEqual(ticker.bse_code, "500325")

        hist = ticker.history(start="2026-01-01")
        self.assertFalse(hist.empty)
        self.assertIn("Adj Close", hist.columns)
        self.assertIn("Dividends", hist.columns)

    def test_market_methods(self):
        opt = self.lake.option_chain("NIFTY")
        self.assertFalse(opt.empty)
        self.assertIn("StrikePrice", opt.columns)

        idx = self.lake.index_history("NIFTY 50")
        self.assertFalse(idx.empty)

        mf = self.lake.mutual_fund(100122)
        self.assertFalse(mf.empty)

    def test_duckdb_sql(self):
        df = self.lake.sql("SELECT COUNT(*) AS total FROM equities_nse")
        self.assertFalse(df.empty)
        self.assertGreater(int(df.iloc[0]["total"]), 0)

    def test_optimize_layout(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            eq_dir = tmp_path / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame({
                "Date": pd.to_datetime(["2026-01-02", "2026-01-01", "2026-01-02", "2026-01-01"]),
                "Symbol": ["TCS", "TCS", "INFY", "INFY"],
                "Close": [3000.0, 3010.0, 1500.0, 1510.0],
            })
            pq_file = eq_dir / "nse_cm_2026.parquet"
            df.to_parquet(pq_file)

            eng = UnifiedLakeEngine(tmp_path)
            res = eng.optimize_layout()
            self.assertEqual(res["files_clustered_by_symbol_date"], 1)

            opt_df = pd.read_parquet(pq_file)
            symbols = opt_df["Symbol"].tolist()
            self.assertEqual(symbols, ["INFY", "INFY", "TCS", "TCS"])

    def test_vectorized_download(self):
        dl_map = self.lake.download(["RELIANCE"])
        self.assertIn("RELIANCE", dl_map)
        df = dl_map["RELIANCE"]
        self.assertFalse(df.empty)
        self.assertIn("Close", df.columns)

    def test_concalls_and_insider_trades(self):
        cc = self.lake.concalls("RELIANCE")
        self.assertIsInstance(cc, pd.DataFrame)
        ins = self.lake.insider_trades("RELIANCE")
        self.assertIsInstance(ins, pd.DataFrame)

    def test_security_identity_fields(self):
        ident = self.lake.resolve("RELIANCE")
        self.assertTrue(hasattr(ident, "face_value"))
        self.assertTrue(hasattr(ident, "industry"))
        self.assertTrue(hasattr(ident, "listing_date"))

    def test_dossier_and_screener(self):
        dossier = self.lake.dossier("RELIANCE")
        self.assertIsInstance(dossier, dict)
        scr = self.lake.screener()
        self.assertIsInstance(scr, pd.DataFrame)

    def test_panel_and_continuous_futures(self):
        pan = self.lake.panel(["RELIANCE"])
        self.assertIsInstance(pan, pd.DataFrame)
        cf = self.lake.continuous_futures("NIFTY")
        self.assertIsInstance(cf, pd.DataFrame)

    def test_ttm_financials(self):
        t = self.lake.ticker("RELIANCE")
        ttm = t.ttm_financials
        self.assertIsInstance(ttm, pd.DataFrame)

    def test_demerger_prevclose_adjustment(self):
        from datalake.adjuster import OHLCVAdjuster
        adj = OHLCVAdjuster(self.lake.root)
        df = pd.DataFrame([
            {"Date": pd.Timestamp("2023-07-19"), "Close": 2841.85, "PrevClose": 2800.0, "Open": 2800.0, "High": 2850.0, "Low": 2790.0, "Volume": 100},
            {"Date": pd.Timestamp("2023-07-20"), "Close": 2600.00, "PrevClose": 2580.0, "Open": 2580.0, "High": 2610.0, "Low": 2570.0, "Volume": 100},
        ])
        res = adj.apply_adjustments(df, "RELIANCE", exchange="NSE")
        self.assertAlmostEqual(res.loc[0, "Adj Close"], 2580.0, places=1)
        self.assertAlmostEqual(res.loc[1, "Adj Close"], 2600.0, places=1)

    def test_fundamentals_consolidated_standalone_fallback(self):
        from unittest.mock import patch
        df = pd.DataFrame([
            {"Symbol": "TESTCO", "PeriodEnd": "2020-03-31", "PeriodType": "Quarterly", "NatureOfReport": "Standalone", "Sales_Cr": 100.0},
            {"Symbol": "TESTCO", "PeriodEnd": "2021-03-31", "PeriodType": "Quarterly", "NatureOfReport": "Standalone", "Sales_Cr": 120.0},
            {"Symbol": "TESTCO", "PeriodEnd": "2021-03-31", "PeriodType": "Quarterly", "NatureOfReport": "Consolidated", "Sales_Cr": 150.0},
        ])
        with patch.object(self.lake, "_read_filtered_parquet", return_value=df):
            res = self.lake.fundamentals("TESTCO", consolidated=True)
            self.assertEqual(len(res), 2)
            row_2020 = res[res["PeriodEnd"] == "2020-03-31"].iloc[0]
            self.assertEqual(row_2020["Sales_Cr"], 100.0)
            self.assertEqual(row_2020["NatureOfReport"], "Standalone")
            row_2021 = res[res["PeriodEnd"] == "2021-03-31"].iloc[0]
            self.assertEqual(row_2021["Sales_Cr"], 150.0)
            self.assertEqual(row_2021["NatureOfReport"], "Consolidated")

    def test_equity_series_dedup_and_priority(self):
        df = pd.DataFrame([
            {"Date": pd.Timestamp("2024-01-01"), "Series": "BE", "Close": 100.0},
            {"Date": pd.Timestamp("2024-01-02"), "Series": "BE", "Close": 102.0},
            {"Date": pd.Timestamp("2024-01-02"), "Series": "EQ", "Close": 103.0},
            {"Date": pd.Timestamp("2024-01-03"), "Series": "SM", "Close": 50.0},
        ])
        out = UnifiedLakeEngine._dedup_nse_equity_series(df)
        self.assertEqual(len(out), 3)
        # 2024-01-01 has only BE -> preserved
        self.assertEqual(out.loc[0, "Series"], "BE")
        # 2024-01-02 has EQ and BE -> EQ prioritized
        self.assertEqual(out.loc[1, "Series"], "EQ")
        self.assertEqual(out.loc[1, "Close"], 103.0)
        # 2024-01-03 has SM -> preserved
        self.assertEqual(out.loc[2, "Series"], "SM")

    def test_parse_action_purpose_face_value_noise(self):
        from datalake.scrapers.universe_actions import parse_action_purpose
        p1 = parse_action_purpose("Interim Dividend 50% on Face Value of Rs 2")
        self.assertEqual(p1["DividendPerShareRs"], 1.0)
        self.assertEqual(p1["ActionType"], "DIVIDEND")

        p2 = parse_action_purpose("Dividend @ 150% (Face Value Rs. 10/-)")
        self.assertEqual(p2["DividendPerShareRs"], 15.0)
        self.assertEqual(p2["ActionType"], "DIVIDEND")

        p3 = parse_action_purpose("Final Dividend - Rs 5 Per Share (Face Value Rs 10)")
        self.assertEqual(p3["DividendPerShareRs"], 5.0)
        self.assertEqual(p3["ActionType"], "DIVIDEND")

        p4 = parse_action_purpose("Dividend 50%")
        self.assertIsNone(p4["DividendPerShareRs"])
        self.assertEqual(p4["ActionType"], "OTHER")

        # ETF split with "Per Unit"
        p_etf = parse_action_purpose("Face Value Split (Sub-Division) - From Rs 10/- Per Unit To Re 1/- Per Unit")
        self.assertEqual(p_etf["ActionType"], "SPLIT")
        self.assertEqual(p_etf["SplitFromFV"], 10.0)
        self.assertEqual(p_etf["SplitToFV"], 1.0)
        self.assertEqual(p_etf["ShareMultiplier"], 10.0)

        # ETF / Security split with "Per Security"
        p_sec = parse_action_purpose("Split from Rs 10 Per Security into Re 1 Per Security")
        self.assertEqual(p_sec["ActionType"], "SPLIT")
        self.assertEqual(p_sec["SplitFromFV"], 10.0)
        self.assertEqual(p_sec["SplitToFV"], 1.0)
        self.assertEqual(p_sec["ShareMultiplier"], 10.0)

    def test_auto_adjust_dividend_scaling(self):
        from datalake.adjuster import OHLCVAdjuster
        adj = OHLCVAdjuster(self.lake.root)
        df = pd.DataFrame([
            {"Date": pd.Timestamp("2020-01-01"), "Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 100.0, "Volume": 1000},
            {"Date": pd.Timestamp("2020-01-02"), "Open": 50.0, "High": 55.0, "Low": 45.0, "Close": 50.0, "Volume": 2000},
        ])
        actions = pd.DataFrame([
            {"ExDate": pd.Timestamp("2020-01-01"), "ActionType": "DIVIDEND", "DividendPerShareRs": 10.0, "ShareMultiplier": 1.0},
            {"ExDate": pd.Timestamp("2020-01-02"), "ActionType": "SPLIT", "DividendPerShareRs": 0.0, "ShareMultiplier": 2.0},
        ])
        from unittest.mock import patch
        with patch.object(adj, "get_symbol_actions", return_value=actions):
            out_raw = adj.apply_adjustments(df.copy(), "TEST", auto_adjust=False)
            self.assertEqual(out_raw.loc[0, "Dividends"], 10.0)

            out_adj = adj.apply_adjustments(df.copy(), "TEST", auto_adjust=True)
            # cum_split_factor on day 0 is 0.5 (due to 2:1 split on day 1), so Dividend is scaled to 5.0
            self.assertEqual(out_adj.loc[0, "Dividends"], 5.0)

    def test_crosswalk_delisted_and_sme_resolution(self):
        sme_p = self.lake.root / "universe" / "nse_sme_master.parquet"
        sme_df = pd.DataFrame([
            {"SYMBOL": "SMETEST", "ISIN": "INE123SME456", "NAME": "SME Test Co", "FACE VALUE": 10}
        ])
        sme_p.parent.mkdir(parents=True, exist_ok=True)
        sme_df.to_parquet(sme_p)
        self.lake._crosswalk_loaded = False
        ident = self.lake.resolve("SMETEST")
        self.assertEqual(ident.symbol, "SMETEST")
        self.assertEqual(ident.company_name, "SME Test Co")

    def test_shareholding_date_normalization_dedup(self):
        import datetime
        from unittest.mock import patch
        nse_df = pd.DataFrame([
            {"Symbol": "DEMO", "QuarterEnd": "30-SEP-2026", "PromoterPct": 50.0, "MutualFundsPct": 15.0}
        ])
        bse_df = pd.DataFrame([
            {"ScripCode": "500001", "QuarterEnd": "September 2026", "PromoterPct": 50.0, "MutualFundsPct": None}
        ])
        def fake_read(p, filters=None):
            if "nse_shareholding" in str(p):
                return nse_df
            elif "bse_shareholding" in str(p):
                return bse_df
            return pd.DataFrame()

        with patch.object(self.lake, "_read_filtered_parquet", side_effect=fake_read):
            res = self.lake.shareholding("DEMO")
            self.assertEqual(len(res), 1)
            self.assertEqual(res.iloc[0]["QuarterEnd"], datetime.date(2026, 9, 30))
            self.assertEqual(res.iloc[0]["MutualFundsPct"], 15.0)

    def test_period_resolution_and_ticker(self):
        end = pd.Timestamp("2026-06-30")
        s_1mo = UnifiedLakeEngine._resolve_period_start("1mo", end)
        self.assertEqual(s_1mo, pd.Timestamp("2026-05-30"))

        s_1y = UnifiedLakeEngine._resolve_period_start("1y", end)
        self.assertEqual(s_1y, pd.Timestamp("2025-06-30"))

        s_ytd = UnifiedLakeEngine._resolve_period_start("ytd", end)
        self.assertEqual(s_ytd, pd.Timestamp("2026-01-01"))

        s_max = UnifiedLakeEngine._resolve_period_start("max", end)
        self.assertEqual(s_max, pd.Timestamp("1994-01-01"))

        ticker = self.lake.ticker("RELIANCE")
        h = ticker.history(period="1mo")
        self.assertFalse(h.empty)

        # Test ticker.dossier
        d = ticker.dossier
        self.assertIsInstance(d, dict)

    def test_fii_dii_flows_and_participant_oi(self):
        flows_dir = self.lake.root / "flows"
        flows_dir.mkdir(parents=True, exist_ok=True)
        f_df = pd.DataFrame([
            {"Date": pd.Timestamp("2026-01-02"), "category": "FII/FPI", "buy_cr": 1000.0, "sell_cr": 800.0, "net_cr": 200.0},
            {"Date": pd.Timestamp("2026-01-02"), "category": "DII", "buy_cr": 800.0, "sell_cr": 900.0, "net_cr": -100.0},
        ])
        f_df.to_parquet(flows_dir / "cash_fii_dii_flows.parquet")

        oi_df = pd.DataFrame([
            {"Date": pd.Timestamp("2026-01-02"), "Client Type": "Client", "Future Index Long": 10000},
            {"Date": pd.Timestamp("2026-01-02"), "Client Type": "FII", "Future Index Long": 20000},
        ])
        oi_df.to_parquet(flows_dir / "fao_participant_oi_2026.parquet")

        flows = self.lake.fii_dii_flows(start="2026-01-01")
        self.assertEqual(len(flows), 2)

        oi = self.lake.participant_oi(start="2026-01-01", client_type="FII")
        self.assertEqual(len(oi), 1)
        self.assertEqual(oi.iloc[0]["Client Type"], "FII")

    def test_hardened_demerger_guard(self):
        from datalake.adjuster import OHLCVAdjuster
        adj = OHLCVAdjuster(self.lake.root)

        # Case 1: Genuine demerger on consecutive trading days, same series, ratio ~ 0.85
        df_demerger = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Series": ["EQ", "EQ"],
            "Close": [1000.0, 850.0],
            "PrevClose": [1000.0, 850.0],  # Exchange lowered PrevClose from 1000 to 850 (ratio 0.85)
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 0.0],
        })
        out1 = adj.apply_adjustments(df_demerger.copy(), "TEST_DEMERGER", auto_adjust=False)
        # Prior day Adj Close adjusted by 0.85 -> 850.0
        self.assertAlmostEqual(out1.loc[0, "Adj Close"], 850.0, places=1)

        # Case 2: Series change BE -> EQ with huge gap should NOT trigger demerger factor
        df_series_switch = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Series": ["BE", "EQ"],
            "Close": [1000.0, 850.0],
            "PrevClose": [1000.0, 850.0],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 0.0],
        })
        out2 = adj.apply_adjustments(df_series_switch.copy(), "TEST_SERIES", auto_adjust=False)
        # Different series -> demerger adjustment suppressed -> Adj Close is 1000.0
        self.assertEqual(out2.loc[0, "Adj Close"], 1000.0)

        # Case 3: Date gap > 7 days (e.g. multi-month suspension) should NOT trigger demerger
        df_date_gap = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2024-02-15"]),
            "Series": ["EQ", "EQ"],
            "Close": [1000.0, 850.0],
            "PrevClose": [1000.0, 850.0],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 0.0],
        })
        out3 = adj.apply_adjustments(df_date_gap.copy(), "TEST_GAP", auto_adjust=False)
        self.assertEqual(out3.loc[0, "Adj Close"], 1000.0)

    def test_auto_exchange_detection_and_ticker_preference(self):
        # 1. Test strip_exchange_tag helper
        q, exch = self.lake._strip_exchange_tag("RELIANCE.NS")
        self.assertEqual((q, exch), ("RELIANCE", "NSE"))

        q, exch = self.lake._strip_exchange_tag("500325.BO")
        self.assertEqual((q, exch), ("500325", "BSE"))

        q, exch = self.lake._strip_exchange_tag("BSE:TCS")
        self.assertEqual((q, exch), ("TCS", "BSE"))

        q, exch = self.lake._strip_exchange_tag("SENSEX")
        self.assertEqual((q, exch), ("SENSEX", "BSE"))

        q, exch = self.lake._strip_exchange_tag("^NSEI")
        self.assertEqual((q, exch), ("NIFTY 50", "NSE"))

        # 2. Test infer_exchange helper
        ident, exch = self.lake._infer_exchange("RELIANCE.BO")
        self.assertEqual(exch, "BSE")

        ident, exch = self.lake._infer_exchange("512345")
        self.assertEqual(exch, "BSE")

        # 3. Test LakeTicker preference
        t_bse = self.lake.ticker("500325.BO")
        self.assertEqual(t_bse.default_exchange, "BSE")

        t_nse = self.lake.ticker("RELIANCE.NS")
        self.assertEqual(t_nse.default_exchange, "NSE")

    def test_etfs_catalog_method(self):
        etf_df = pd.DataFrame([
            {"Symbol": "NIFTYBEES", "Underlying": "NIFTY 50", "SecurityName": "Nippon India ETF Nifty 50 BeES"},
            {"Symbol": "GOLDBEES", "Underlying": "Gold", "SecurityName": "Nippon India ETF Gold BeES"},
        ])
        etf_path = self.lake.root / "universe" / "nse_etf_master.parquet"
        etf_path.parent.mkdir(parents=True, exist_ok=True)
        etf_df.to_parquet(etf_path, index=False)

        res = self.lake.etfs()
        self.assertEqual(len(res), 2)
        self.assertIn("NIFTYBEES", res["Symbol"].values)

    def test_nifty_constituent_urls_coverage(self):
        from datalake.scrapers.universe_actions import NIFTY_CONSTITUENT_URLS
        required = [
            "NIFTY 50", "NIFTY NEXT 50", "NIFTY 100", "NIFTY 200", "NIFTY 500", "NIFTY MIDCAP 150",
            "NIFTY SMALLCAP 250", "NIFTY BANK", "NIFTY IT", "NIFTY PHARMA", "NIFTY AUTO",
            "NIFTY FMCG", "NIFTY METAL", "NIFTY ENERGY", "NIFTY REALTY"
        ]
        for idx in required:
            self.assertIn(idx, NIFTY_CONSTITUENT_URLS)

    def test_index_history_merges_recent_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            p = Path(tmp_dir)
            idx_dir = p / "indices"
            idx_dir.mkdir(parents=True, exist_ok=True)
            # Older bulk file with bar on 2026-01-02
            pd.DataFrame([{
                "Date": pd.Timestamp("2026-01-02"),
                "IndexName": "NIFTY 50",
                "Open": 24000.0, "High": 24100.0, "Low": 23900.0, "Close": 24050.0
            }]).to_parquet(idx_dir / "nifty_30y_ohlc_tri_pe.parquet", index=False)
            # Newer daily snapshot file with bar on 2026-01-05
            pd.DataFrame([{
                "Date": pd.Timestamp("2026-01-05"),
                "IndexName": "NIFTY 50",
                "Open": 24100.0, "High": 24200.0, "Low": 24000.0, "Close": 24150.0
            }]).to_parquet(idx_dir / "ind_close_all_2026.parquet", index=False)

            engine = UnifiedLakeEngine(p)
            df = engine.index_history("NIFTY 50")
            self.assertEqual(len(df), 2)
            dates = [d.strftime("%Y-%m-%d") for d in df["Date"]]
            self.assertIn("2026-01-02", dates)
            self.assertIn("2026-01-05", dates)

    def test_screener_valuation_ratios_and_fast_info(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            p = Path(tmp_dir)
            for d in ("universe", "equities", "fundamentals", "shareholding"):
                (p / d).mkdir(parents=True, exist_ok=True)
            pd.DataFrame([{"SYMBOL": "TCS", "NAME OF COMPANY": "Tata Consultancy Services", "SERIES": "EQ", "ISIN NUMBER": "INE467B01029"}]).to_parquet(p / "universe" / "nse_equity_master.parquet", index=False)
            pd.DataFrame([{
                "Date": pd.Timestamp("2026-01-02"),
                "Symbol": "TCS",
                "Series": "EQ",
                "Close": 4000.0,
                "Volume": 500000,
            }]).to_parquet(p / "equities" / "nse_cm_2026.parquet", index=False)
            pd.DataFrame([{
                "Symbol": "TCS",
                "NatureOfReport": "Consolidated",
                "PeriodType": "Quarterly",
                "PeriodStart": "2025-07-01",
                "PeriodEnd": "2025-09-30",
                "Sales_Cr": 60000.0,
                "PBT_Cr": 16000.0,
                "NetProfit_Cr": 12000.0,
                "BasicEPS_Rs": 100.0,
                "Interest_Cr": 200.0,
                "Depreciation_Cr": 1000.0,
                "OtherIncome_Cr": 500.0,
                "EquityCapital_Cr": 360.0,
                "Reserves_Cr": 90000.0,
                "TotalBorrowings_Cr": 5000.0,
                "TotalCashAndBank_Cr": 15000.0,
            }]).to_parquet(p / "fundamentals" / "xbrl_financials.parquet", index=False)
            pd.DataFrame([{
                "Symbol": "TCS",
                "QuarterEnd": "2025-09-30",
                "PromoterPct": 72.0,
                "PromoterPledgedPct": 0.0,
                "MutualFundsPct": 10.0,
                "FPI_Pct": 12.0,
                "RetailPct": 6.0,
                "TotalShares": 3600000000.0,
            }]).to_parquet(p / "shareholding" / "nse_shareholding_summary.parquet", index=False)

            engine = UnifiedLakeEngine(p)
            scr = engine.screener()
            self.assertFalse(scr.empty)
            row = scr[scr["Symbol"] == "TCS"].iloc[0]
            self.assertIn("PE", scr.columns)
            self.assertIn("MarketCap_Cr", scr.columns)
            self.assertIn("PB", scr.columns)
            self.assertIn("EV_EBITDA", scr.columns)
            self.assertIn("ROE_Pct", scr.columns)
            self.assertIn("DebtToEquity", scr.columns)
            self.assertEqual(row["PE"], 40.0)
            self.assertEqual(row["MarketCap_Cr"], 1440000.0)

            t = engine.ticker("TCS")
            fi = t.fast_info
            self.assertEqual(fi.market_cap, 1440000.0)
            self.assertEqual(fi.pe_ratio, 40.0)
            self.assertEqual(fi.last_price, 4000.0)

    def test_shp_xbrl_total_shares(self):
        from datalake.scrapers.xbrl import XBRLScraper
        xml = b"""<?xml version="1.0" encoding="utf-8"?>
        <xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:in-bse-shp="http://www.bseindia.com/xbrl/shp/2015-12-31/in-bse-shp">
            <xbrli:context id="OneD">
                <xbrli:entity><xbrli:identifier scheme="http://www.bseindia.com/scrip">500325</xbrli:identifier></xbrli:entity>
                <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
            </xbrli:context>
            <xbrli:context id="Promo">
                <xbrli:entity><xbrli:segment><xbrli:explicitMember dimension="in-bse-shp:ShareholdingAxis">in-bse-shp:PromoterAndPromoterGroupMember</xbrli:explicitMember></xbrli:segment></xbrli:entity>
                <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
            </xbrli:context>
            <xbrli:context id="Pub">
                <xbrli:entity><xbrli:segment><xbrli:explicitMember dimension="in-bse-shp:ShareholdingAxis">in-bse-shp:PublicShareholdingMember</xbrli:explicitMember></xbrli:segment></xbrli:entity>
                <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
            </xbrli:context>
            <in-bse-shp:NumberOfShares contextRef="Promo">5000000</in-bse-shp:NumberOfShares>
            <in-bse-shp:PercentageOfTotalNumberOfShares contextRef="Promo">50.0</in-bse-shp:PercentageOfTotalNumberOfShares>
            <in-bse-shp:NumberOfShares contextRef="Pub">5000000</in-bse-shp:NumberOfShares>
            <in-bse-shp:PercentageOfTotalNumberOfShares contextRef="Pub">50.0</in-bse-shp:PercentageOfTotalNumberOfShares>
        </xbrli:xbrl>"""
        res = XBRLScraper.parse_full_shp_xbrl(xml)
        self.assertIsNotNone(res)
        summary = res.get("summary", {})
        self.assertEqual(summary.get("TotalShares"), 10000000.0)

    def test_get_symbol_actions_preserves_action_type(self):
        from datalake.adjuster import OHLCVAdjuster
        with tempfile.TemporaryDirectory() as tmp_dir:
            p = Path(tmp_dir)
            act_dir = p / "actions"
            act_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame([
                {
                    "symbol": "RELIANCE",
                    "ExDate": "2024-10-28",
                    "ActionType": "BONUS",
                    "DividendPerShareRs": 0.0,
                    "ShareMultiplier": 2.0,
                },
                {
                    "symbol": "RELIANCE",
                    "ExDate": "2024-08-19",
                    "ActionType": "DIVIDEND",
                    "DividendPerShareRs": 10.0,
                    "ShareMultiplier": 1.0,
                },
            ]).to_parquet(act_dir / "nse_corporate_actions.parquet", index=False)
            adj = OHLCVAdjuster(p)
            acts = adj.get_symbol_actions("RELIANCE", exchange="NSE")
            self.assertIn("ActionType", acts.columns)
            bonus_row = acts[acts["ExDate"] == pd.Timestamp("2024-10-28")].iloc[0]
            self.assertEqual(bonus_row["ActionType"], "BONUS")
            self.assertEqual(bonus_row["ShareMultiplier"], 2.0)

    def test_fo_and_indices_legacy_symbol_normalization(self):
        from datalake.scrapers.fo_eod import _FO_SYMBOL_MAP
        from datalake.scrapers.indices import _LEGACY_INDEX_MAP

        self.assertEqual(_FO_SYMBOL_MAP["S&P CNX NIFTY"], "NIFTY")
        self.assertEqual(_FO_SYMBOL_MAP["CNX BANK"], "BANKNIFTY")
        self.assertEqual(_LEGACY_INDEX_MAP["CNX NIFTY"], "NIFTY 50")
        self.assertEqual(_LEGACY_INDEX_MAP["CNX 100"], "NIFTY 100")

        # Test continuous_futures stitches pre-2016 CNX NIFTY data
        with tempfile.TemporaryDirectory() as tmp_dir:
            p = Path(tmp_dir)
            d_dir = p / "derivatives"
            d_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame([{
                "Date": pd.Timestamp("2014-06-12"),
                "Exchange": "NSE",
                "InstrumentType": "FUTIDX",
                "Symbol": "CNX NIFTY",
                "ExpiryDate": pd.Timestamp("2014-06-26"),
                "StrikePrice": 0.0,
                "OptionType": "XX",
                "Open": 7500.0, "High": 7550.0, "Low": 7480.0, "Close": 7520.0,
                "SettlePrice": 7520.0, "Contracts": 100.0, "OpenInterest": 5000.0, "ChangeInOI": 50.0
            }]).to_parquet(d_dir / "nse_fo_2014.parquet", index=False)

            eng = UnifiedLakeEngine(p)
            cf = eng.continuous_futures("NIFTY", start="2014-01-01", end="2014-12-31")
            self.assertFalse(cf.empty)
            self.assertEqual(cf.iloc[0]["Close"], 7520.0)
            self.assertEqual(cf.iloc[0]["Symbol"], "NIFTY")

    def test_trading_days_and_factor_returns(self):
        tdays = self.lake.trading_days(start="2026-01-01", end="2026-01-05")
        self.assertIsInstance(tdays, pd.DatetimeIndex)

        # Factor returns
        factors = self.lake.factor_returns(start="2026-01-01")
        self.assertIsInstance(factors, pd.DataFrame)

    def test_calendar_and_fo_ban_list(self):
        cal = self.lake.calendar("RELIANCE")
        self.assertIsInstance(cal, pd.DataFrame)
        bans = self.lake.fo_ban_list()
        self.assertIsInstance(bans, pd.DataFrame)

    def test_info_dictionary(self):
        inf = self.lake.info("RELIANCE")
        self.assertIsInstance(inf, dict)
        self.assertEqual(inf.get("symbol"), "RELIANCE")
        t = self.lake.ticker("RELIANCE")
        self.assertEqual(t.info.get("symbol"), "RELIANCE")

    def test_auto_adjust_scales_last_and_prevclose(self):
        from datalake.adjuster import OHLCVAdjuster
        with tempfile.TemporaryDirectory() as tmp_dir:
            p = Path(tmp_dir)
            act_dir = p / "actions"
            act_dir.mkdir(parents=True, exist_ok=True)
            # 1:1 bonus (multiplier 2.0)
            pd.DataFrame([{
                "symbol": "TCS",
                "ExDate": "2026-01-05",
                "ActionType": "BONUS",
                "DividendPerShareRs": 0.0,
                "ShareMultiplier": 2.0,
            }]).to_parquet(act_dir / "nse_corporate_actions.parquet", index=False)
            adj = OHLCVAdjuster(p)
            df = pd.DataFrame([
                {
                    "Date": pd.Timestamp("2026-01-02"),
                    "Open": 4000.0, "High": 4100.0, "Low": 3950.0, "Close": 4000.0,
                    "Last": 4000.0, "PrevClose": 3980.0, "Volume": 1000,
                },
                {
                    "Date": pd.Timestamp("2026-01-05"),
                    "Open": 2000.0, "High": 2050.0, "Low": 1980.0, "Close": 2000.0,
                    "Last": 2000.0, "PrevClose": 2000.0, "Volume": 2000,
                },
            ])
            res = adj.apply_adjustments(df, "TCS", exchange="NSE", auto_adjust=True)
            # Pre-bonus day should be halved across all 6 price columns
            row0 = res.iloc[0]
            self.assertEqual(row0["Close"], 2000.0)
            self.assertEqual(row0["Last"], 2000.0)
            self.assertEqual(row0["PrevClose"], 1990.0)
            self.assertEqual(row0["Volume"], 2000)


if __name__ == "__main__":
    unittest.main()
