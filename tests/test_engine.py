import shutil
import tempfile
import unittest
from pathlib import Path
import pandas as pd

from datalake.engine import LakeTicker, SecurityIdentity, UnifiedLakeEngine


class TestUnifiedLakeEngine(unittest.TestCase):
    def setUp(self):
        self.lake = UnifiedLakeEngine("smoke_data")

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


if __name__ == "__main__":
    unittest.main()
