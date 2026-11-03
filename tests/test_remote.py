import io
import tempfile
import unittest
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

from datalake.remote import RemoteLakeClient
from datalake.storage import upsert_parquet_partition, write_parquet_atomic


class TestRemoteAndStorageLayout(unittest.TestCase):
    def test_parquet_row_group_size(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "test_row_groups.parquet"
            # Create 25,000 rows
            n = 25000
            df = pd.DataFrame({
                "Symbol": ["RELIANCE"] * (n // 2) + ["TCS"] * (n - (n // 2)),
                "Date": pd.date_range("2020-01-01", periods=n, freq="min"),
                "Close": [100.0 + i for i in range(n)],
            })

            write_parquet_atomic(df, target, row_group_size=10_000)

            # Read via BytesIO so PyArrow does not hold an open file lock on Windows tempfile
            buf = io.BytesIO(target.read_bytes())
            pf = pq.ParquetFile(buf)
            # 25,000 rows with 10,000 row_group_size -> 3 row groups (10k, 10k, 5k)
            self.assertEqual(pf.num_row_groups, 3)
            self.assertEqual(pf.metadata.row_group(0).num_rows, 10000)
            self.assertEqual(pf.metadata.row_group(1).num_rows, 10000)
            self.assertEqual(pf.metadata.row_group(2).num_rows, 5000)

    def test_upsert_auto_symbol_date_sorting(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "test_upsert.parquet"
            df = pd.DataFrame({
                "Date": pd.to_datetime(["2026-01-02", "2026-01-01", "2026-01-02", "2026-01-01"]),
                "Symbol": ["TCS", "TCS", "INFY", "INFY"],
                "Close": [3000.0, 3010.0, 1500.0, 1510.0],
            })

            upsert_parquet_partition(
                df,
                target,
                dedup_cols=["Date", "Symbol"],
                # sort_cols is omitted -> should auto-sort by ["Symbol", "Date"]
            )

            res = pd.read_parquet(target)
            self.assertEqual(res["Symbol"].tolist(), ["INFY", "INFY", "TCS", "TCS"])
            self.assertEqual(
                res["Date"].dt.strftime("%Y-%m-%d").tolist(),
                ["2026-01-01", "2026-01-02", "2026-01-01", "2026-01-02"],
            )

    def test_remote_client_asset_url(self):
        client = RemoteLakeClient("user/bfinance-datalake", tag="datalake-latest", load_httpfs=False)
        url = client._asset_url("equities", "nse_cm_2026.parquet")
        self.assertEqual(
            url,
            "https://github.com/user/bfinance-datalake/releases/download/datalake-latest/equities__nse_cm_2026.parquet",
        )

    def test_remote_client_sql(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        res = client.sql("SELECT 42 AS answer")
        self.assertEqual(int(res.iloc[0]["answer"]), 42)

    def test_remote_client_clean_sym(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        self.assertEqual(client._clean_sym("RELIANCE.NS"), "RELIANCE")
        self.assertEqual(client._clean_sym("500325.BO"), "500325")
        self.assertEqual(client._clean_sym("TCS"), "TCS")

    def test_remote_client_two_tier_compacted_routing(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._compacted_bounds_cache = {"compacted__nse_cm_hist.parquet": (1995, 2025)}

        urls = client._equity_urls_for_range("NSE", 2004, 2026)
        # Instead of 23 yearly URLs, it resolves to just 2 URLs: compacted hist + 2026!
        self.assertEqual(len(urls), 2)
        self.assertTrue(urls[0].endswith("compacted__nse_cm_hist.parquet"))
        self.assertTrue(urls[1].endswith("equities__nse_cm_2026.parquet"))

    def test_remote_client_pack_slice_history(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._ticker_index_loaded = True
        client._ticker_index = {
            "version": 1,
            "current_year": 2026,
            "aliases": {},
            "tickers": {
                "TCS": {
                    "hist": [0, 100],
                    "curr": [100, 50],
                }
            },
        }

        df_hist = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2025-05-01"]), "Close": [3800.0]})
        df_curr = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-02"]), "Close": [3900.0]})

        def mock_fetch(pack_fn, offset, length):
            if "hist" in pack_fn:
                return df_hist
            return df_curr

        client._fetch_pack_slice = mock_fetch

        res = client.history("TCS", start_year=2024, end_year=2026, exchange="NSE")
        self.assertEqual(len(res), 2)
        self.assertEqual(res["Close"].tolist(), [3800.0, 3900.0])

        res_filtered = client.history("TCS", start_year=2026, end_year=2026, exchange="NSE", columns=["Date", "Close"])
        self.assertEqual(len(res_filtered), 1)
        self.assertEqual(list(res_filtered.columns), ["Date", "Close"])
        self.assertEqual(res_filtered["Close"].iloc[0], 3900.0)

    def test_remote_client_dossier(self):
        import json
        import zlib
        from unittest.mock import patch
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._ticker_index_loaded = True
        client._ticker_index = {
            "version": 1,
            "current_year": 2026,
            "aliases": {},
            "tickers": {
                "RELIANCE": {
                    "dossier": [0, 50],
                }
            },
        }
        sample_dossier = {"financials": [{"Symbol": "RELIANCE", "Sales_Cr": 200000.0}]}
        raw_compressed = zlib.compress(json.dumps(sample_dossier).encode("utf-8"))

        class MockResp:
            content = raw_compressed
            def read(self):
                return raw_compressed
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass

        client._http_pool = None
        with patch("urllib.request.urlopen", return_value=MockResp()):
            res = client.dossier("RELIANCE")
            self.assertIn("financials", res)
            self.assertEqual(res["financials"].iloc[0]["Sales_Cr"], 200000.0)


if __name__ == "__main__":
    unittest.main()
