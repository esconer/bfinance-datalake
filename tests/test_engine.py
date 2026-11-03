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


if __name__ == "__main__":
    unittest.main()
