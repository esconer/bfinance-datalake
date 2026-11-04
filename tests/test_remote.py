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

    def test_remote_symbol_crosswalk_resolution(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._ticker_index_loaded = True
        client._ticker_index = {
            "version": 1,
            "aliases": {"INFOSYSTCH": "INFY"},
            "bse_to_symbol": {"500325": "RELIANCE"},
            "isin_to_symbol": {"INE002A01018": "RELIANCE"},
            "tickers": {"RELIANCE": {}, "INFY": {}},
        }
        self.assertEqual(client._resolve_remote_symbol("500325"), "RELIANCE")
        self.assertEqual(client._resolve_remote_symbol("INE002A01018"), "RELIANCE")
        self.assertEqual(client._resolve_remote_symbol("INFOSYSTCH"), "INFY")
        self.assertEqual(client._resolve_remote_symbol("TCS"), "TCS")

    def test_remote_apply_pack_actions(self):
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "Open": [100.0, 50.0, 52.0],
            "High": [105.0, 55.0, 54.0],
            "Low": [95.0, 48.0, 50.0],
            "Close": [100.0, 50.0, 52.0],
            "Volume": [1000.0, 2000.0, 1500.0],
        })
        # Split 2:1 on 2024-01-02, Div 5.0 on 2024-01-03
        actions = [
            ["2024-01-02", 0.0, 2.0],
            ["2024-01-03", 5.0, 1.0],
        ]
        # Unadjusted: Adj Close has split and div factor applied, but Open/High/Low/Close untouched
        out_raw = RemoteLakeClient._apply_pack_actions(df.copy(), actions, auto_adjust=False)
        self.assertEqual(out_raw.loc[0, "Close"], 100.0)
        self.assertAlmostEqual(out_raw.loc[0, "Adj Close"], 45.0, places=2)
        self.assertEqual(out_raw.loc[1, "Stock Splits"], 2.0)
        self.assertEqual(out_raw.loc[2, "Dividends"], 5.0)

        # auto_adjust=True: Open/High/Low/Close replaced by adjusted prices
        out_adj = RemoteLakeClient._apply_pack_actions(df.copy(), actions, auto_adjust=True)
        self.assertAlmostEqual(out_adj.loc[0, "Close"], 45.0, places=2)
        # Volume on day 0 adjusted for 2:1 split (1000 / 0.5 = 2000)
        self.assertEqual(out_adj.loc[0, "Volume"], 2000.0)

    def test_remote_panel(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        mock_dl = pd.DataFrame([
            {"Date": "2024-01-01", "Symbol": "RELIANCE", "Adj Close": 1300.0, "Close": 1300.0},
            {"Date": "2024-01-02", "Symbol": "RELIANCE", "Adj Close": 1310.0, "Close": 1310.0},
            {"Date": "2024-01-01", "Symbol": "TCS", "Adj Close": 3800.0, "Close": 3800.0},
            {"Date": "2024-01-02", "Symbol": "TCS", "Adj Close": 3850.0, "Close": 3850.0},
        ])
        client.download = lambda *args, **kwargs: mock_dl
        p_df = client.panel(["RELIANCE", "TCS"])
        self.assertEqual(list(p_df.columns), ["RELIANCE", "TCS"])
        self.assertEqual(len(p_df), 2)
        self.assertEqual(p_df.loc["2024-01-01", "RELIANCE"], 1300.0)
        self.assertEqual(p_df.loc["2024-01-02", "TCS"], 3850.0)

    def test_remote_dossier_routing_and_cache(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False, use_pack=True)
        client._dossier_cache["WIPRO"] = {
            "financials": pd.DataFrame([{"Symbol": "WIPRO", "PeriodType": "Quarterly", "Sales_Cr": 22000.0}]),
            "shareholding": pd.DataFrame([{"Symbol": "WIPRO", "QuarterEnd": "2024-03-31", "PromoterPct": 73.0}]),
            "shareholding_bse": pd.DataFrame([{"Symbol": "WIPRO", "ScripCode": "507685", "QuarterId": "2024-03-31"}]),
            "named_holders": pd.DataFrame([{"Symbol": "WIPRO", "HolderName": "LIC", "PctOfTotal": 4.5}]),
            "actions": pd.DataFrame([{"symbol": "WIPRO", "ExDate": "2024-01-10", "purpose": "Bonus 1:1"}]),
            "actions_bse": pd.DataFrame([{"scrip_code": "507685", "ExDate": "2024-01-10", "purpose": "Bonus 1:1"}]),
            "deals": pd.DataFrame([{"Symbol": "WIPRO", "Date": "2024-02-01", "ClientName": "Big Fund"}]),
            "insider_trades": pd.DataFrame([{"symbol": "WIPRO", "date": "2024-02-02", "name": "Insider"}]),
            "annual_reports": pd.DataFrame([{"Symbol": "WIPRO", "Year": 2024, "Url": "http://wipro.pdf"}]),
            "concalls": pd.DataFrame([{"Symbol": "WIPRO", "FilingTimestamp": "2024-02-03", "TranscriptUrl": "http://wipro_cc.pdf"}]),
        }

        # Verify all non-price methods route through dossier cache without network calls
        fin = client.financials("WIPRO", period_type="Quarterly")
        self.assertEqual(fin["Sales_Cr"].iloc[0], 22000.0)

        shp = client.shareholding("WIPRO", exchange="NSE")
        self.assertEqual(shp["PromoterPct"].iloc[0], 73.0)

        shp_bse = client.shareholding("WIPRO", exchange="BSE")
        self.assertEqual(shp_bse["ScripCode"].iloc[0], "507685")

        nh = client.named_holders("WIPRO")
        self.assertEqual(nh["HolderName"].iloc[0], "LIC")

        ca = client.corporate_actions("WIPRO", exchange="NSE")
        self.assertEqual(ca["purpose"].iloc[0], "Bonus 1:1")

        ca_bse = client.corporate_actions("WIPRO", exchange="BSE")
        self.assertEqual(ca_bse["purpose"].iloc[0], "Bonus 1:1")

        deals = client.deals("WIPRO")
        self.assertEqual(deals["ClientName"].iloc[0], "Big Fund")

        pit = client.insider_trades("WIPRO")
        self.assertEqual(pit["name"].iloc[0], "Insider")

        ar = client.annual_reports("WIPRO")
        self.assertEqual(ar["Year"].iloc[0], 2024)

        cc = client.concalls("WIPRO")
        self.assertEqual(cc["TranscriptUrl"].iloc[0], "http://wipro_cc.pdf")

    def test_remote_exchange_detection_and_parsing(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._ticker_index_loaded = True
        client._ticker_index_loaded_nse = True
        client._ticker_index_nse = {}
        client._ticker_index_loaded_bse = True
        client._ticker_index_bse = {}
        self.assertEqual(client._detect_exchange("RELIANCE"), "NSE")
        self.assertEqual(client._detect_exchange("RELIANCE.NS"), "NSE")
        self.assertEqual(client._detect_exchange("NSE:RELIANCE"), "NSE")
        self.assertEqual(client._detect_exchange("RELIANCE.BO"), "BSE")
        self.assertEqual(client._detect_exchange("BSE:RELIANCE"), "BSE")
        self.assertEqual(client._detect_exchange("500325.BO"), "BSE")
        self.assertEqual(client._detect_exchange("SENSEX"), "BSE")
        self.assertEqual(client._detect_exchange("BANKEX"), "BSE")

        # Parse symbol & exchange
        sym, exch, is_exp = client._parse_symbol_and_exchange("RELIANCE")
        self.assertEqual((sym, exch, is_exp), ("RELIANCE", "NSE", False))

        sym, exch, is_exp = client._parse_symbol_and_exchange("RELIANCE.BO")
        self.assertEqual((sym, exch, is_exp), ("RELIANCE", "BSE", True))

        sym, exch, is_exp = client._parse_symbol_and_exchange("500325.BO")
        self.assertEqual((sym, exch, is_exp), ("500325", "BSE", True))

        sym, exch, is_exp = client._parse_symbol_and_exchange("512345")
        self.assertEqual((sym, exch, is_exp), ("512345", "BSE", False))

    def test_remote_http_pool_auth_isolation(self):
        client = RemoteLakeClient("user/bfinance-datalake", token="ghp_dummySecretToken123", load_httpfs=False)
        # Verify _http_pool does NOT contain Authorization in its default headers (protecting Azure SAS URLs)
        if client._http_pool is not None:
            self.assertNotIn("authorization", {k.lower() for k in client._http_pool.headers.keys()})

    def test_remote_etfs_method(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        client._safe_query = lambda query, params=None: pd.DataFrame([
            {"Symbol": "NIFTYBEES", "Underlying": "NIFTY 50"},
            {"Symbol": "GOLDBEES", "Underlying": "Gold"},
        ])
        res = client.etfs()
        self.assertEqual(len(res), 2)
        self.assertEqual(res.iloc[0]["Symbol"], "NIFTYBEES")

    def test_remote_screener_cached_get_and_sql_filter(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        # Pre-seed cache as if downloaded in single GET
        df_mock = pd.DataFrame({
            "Symbol": ["RELIANCE", "TCS", "INFY"],
            "Close": [2800.0, 3900.0, 1500.0],
            "PE": [24.5, 29.1, 22.0],
            "SMA_200": [2600.0, 3700.0, 1600.0],
        })
        client._screener_df_cache = df_mock

        # Test full screener fetch (0 SQL filter)
        all_df = client.screener()
        self.assertEqual(len(all_df), 3)

        # Test SQL filter in-memory DuckDB query
        filtered = client.screener("Close > SMA_200 AND PE < 26")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]["Symbol"], "RELIANCE")

    def test_remote_option_chain_single_pass_date_resolution(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        # Pre-populate max date cache or manifest watermark
        client._manifest_cache = {
            "latest_dates": {
                "derivatives/nse_fo_2026.parquet": "2026-11-04",
            }
        }
        resolved_date = client._get_latest_fo_date("nse", 2026, "http://dummy/nse_fo_2026.parquet")
        self.assertEqual(resolved_date, "2026-11-04")

        # 1. Test fast snapshot path
        snapshot_queries = []
        def mock_query_snap(q, params=None):
            snapshot_queries.append((q, params))
            return pd.DataFrame([{"Symbol": "WIPRO", "StrikePrice": 500.0, "OptionType": "CE"}])

        client._safe_query = mock_query_snap
        client._resolve_cdn_url = lambda u: u

        res_snap = client.option_chain("WIPRO", year=2026, exchange="NSE")
        self.assertEqual(len(res_snap), 1)
        self.assertIn("snapshots__fo_option_chain_latest.parquet", snapshot_queries[0][0])

        # 2. Test fallback to single-pass timestamp interval pushdown
        yearly_queries = []
        def mock_query_yearly(q, params=None):
            yearly_queries.append((q, params))
            if "snapshots__" in q:
                return pd.DataFrame()  # simulate snapshot missing/empty
            return pd.DataFrame([{"Symbol": "WIPRO", "StrikePrice": 500.0, "OptionType": "CE"}])

        client._safe_query = mock_query_yearly
        res_yearly = client.option_chain("WIPRO", year=2026, exchange="NSE")
        self.assertEqual(len(res_yearly), 1)
        last_sql, last_params = yearly_queries[-1]
        self.assertIn("Date >= CAST(? AS TIMESTAMP)", last_sql)
        self.assertEqual(last_params, ["WIPRO", "2026-11-04", "2026-11-04"])

    def test_remote_ticker_index_local_disk_cache(self):
        import json
        with tempfile.TemporaryDirectory() as tmp_dir:
            client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False, cache_dir=tmp_dir)
            index_payload = {
                "version": 1,
                "current_year": 2026,
                "aliases": {"INFOSYSTCH": "INFY"},
                "tickers": {"INFY": {"hist": [0, 500]}},
            }
            # Pre-populate disk cache file
            cache_file = Path(tmp_dir) / "user__bfinance-datalake_datalake-latest_by_ticker__nse_index.json"
            cache_file.write_text(json.dumps(index_payload), encoding="utf-8")

            # Load index - should read directly from disk cache without network
            data = client._load_ticker_index("NSE")
            self.assertIsNotNone(data)
            self.assertEqual(data["tickers"]["INFY"]["hist"], [0, 500])
            self.assertEqual(data["aliases"]["INFOSYSTCH"], "INFY")

    def test_remote_slice_mem_cache(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        network_calls = 0
        test_df = pd.DataFrame({"Symbol": ["RELIANCE"], "Close": [2800.0]})

        # Seed in-memory slice cache directly
        mem_key = ("by_ticker__nse_hist.pack", 1000, 200)
        client._slice_mem_cache[mem_key] = test_df

        # Fetch slice - should return copy from memory without any network or disk activity
        df1 = client._fetch_pack_slice("by_ticker__nse_hist.pack", 1000, 200)
        df2 = client._fetch_pack_slice("by_ticker__nse_hist.pack", 1000, 200)
        self.assertEqual(len(df1), 1)
        self.assertEqual(df1["Close"].iloc[0], 2800.0)
        self.assertEqual(len(df2), 1)
        self.assertEqual(df2["Close"].iloc[0], 2800.0)

    def test_remote_option_chain_dossier_fast_path(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        # Pre-seed dossier with option_chain table
        client._dossier_cache["RELIANCE"] = {
            "option_chain": pd.DataFrame([
                {"Symbol": "RELIANCE", "ExpiryDate": "2026-01-29", "StrikePrice": 2800.0, "OptionType": "CE", "Close": 55.0}
            ])
        }

        # Query option chain without trade_date - should immediately return dossier table
        opt_df = client.option_chain("RELIANCE")
        self.assertEqual(len(opt_df), 1)
        self.assertEqual(opt_df.iloc[0]["StrikePrice"], 2800.0)
        self.assertEqual(opt_df.iloc[0]["Close"], 55.0)

    def test_remote_resolve_cdn_url_uses_get_range(self):
        from unittest.mock import MagicMock
        client = RemoteLakeClient("esconer/bfinance-datalake", load_httpfs=False)
        mock_resp = MagicMock()
        mock_resp.status_code = 302
        mock_resp.headers = {"Location": "https://objects.githubusercontent.com/signed-asset"}
        mock_pool = MagicMock()
        mock_pool.get.return_value = mock_resp
        client._http_pool = mock_pool

        cdn = client._resolve_cdn_url("https://github.com/esconer/bfinance-datalake/releases/download/latest/test.pack")
        self.assertEqual(cdn, "https://objects.githubusercontent.com/signed-asset")
        # Verify get() was called with Range: bytes=0-0 and follow_redirects=False (signing for GET, not HEAD)
        mock_pool.get.assert_called_once()
        args, kwargs = mock_pool.get.call_args
        self.assertEqual(kwargs.get("headers"), {"Range": "bytes=0-0"})
        self.assertEqual(kwargs.get("follow_redirects"), False)

    def test_remote_transport_ipv4_and_timeout(self):
        client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False)
        if client._http_pool is not None:
            # Verify IPv4 binding and connect timeout
            self.assertEqual(client._http_pool.timeout.connect, 3.0)
            transport = getattr(client._http_pool, "_transport", None)
            if transport is not None:
                pool = getattr(transport, "_pool", None)
                if pool is not None and hasattr(pool, "_local_address"):
                    self.assertEqual(pool._local_address, "0.0.0.0")


if __name__ == "__main__":
    unittest.main()
