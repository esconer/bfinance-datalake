import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd

from datalake.engine import UnifiedLakeEngine
from datalake.sync import (
    build_manifest,
    compute_sha256,
    prepare_release_staging,
    prune_retired_release_assets,
    pull_from_github_release,
)


class TestSyncEngine(unittest.TestCase):
    def test_compute_sha256(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello BFinance Data Lake")
            f_path = Path(f.name)
        try:
            h = compute_sha256(f_path)
            # sha256 of "Hello BFinance Data Lake"
            self.assertEqual(len(h), 64)
        finally:
            f_path.unlink(missing_ok=True)

    def test_build_manifest_and_staging(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)

            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [3500.0]})
            pq_file = eq_dir / "nse_cm_2026.parquet"
            df.to_parquet(pq_file)

            (src / "state.db").write_text("sqlite-mock")

            manifest = build_manifest(src)
            self.assertEqual(manifest["version"], 1)
            self.assertEqual(manifest["file_count"], 1)
            rel_key = "equities/nse_cm_2026.parquet"
            self.assertIn(rel_key, manifest["files"])
            self.assertEqual(manifest["files"][rel_key]["asset_name"], "equities__nse_cm_2026.parquet")

            staged = prepare_release_staging(src, stg)
            self.assertIn("manifest.json", [p.name for p in stg.glob("*")])
            self.assertIn("state.db", [p.name for p in stg.glob("*")])
            self.assertIn("equities__nse_cm_2026.parquet", [p.name for p in stg.glob("*")])

    def test_prune_retired_release_assets(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [3500.0]})
            df.to_parquet(eq_dir / "nse_cm_2026.parquet")
            build_manifest(src)
            (stg / "datalake_store.tar.zst").write_bytes(b"archive")

            fake_gh_assets = json.dumps({
                "assets": [
                    {"name": "manifest.json"},
                    {"name": "state.db"},
                    {"name": "datalake_store.tar.zst"},
                    {"name": "datalake_store.tar.zst.part-00"},
                    {"name": "equities__nse_cm_2026.parquet"},
                    {"name": "retired__old_file.parquet"},
                ]
            })
            with patch("subprocess.check_output", return_value=fake_gh_assets), patch("subprocess.run") as mock_run:
                deleted = prune_retired_release_assets(src, tag="datalake-latest", staging_dir=stg)
                self.assertEqual(set(deleted), {"datalake_store.tar.zst.part-00", "retired__old_file.parquet"})
                self.assertEqual(mock_run.call_count, 2)

                # Also verify CLI entry point executes prune-release and stage-release without NameError
                from datalake.cli import main as cli_main
                with patch(
                    "sys.argv",
                    ["datalake", "--data-dir", str(src), "prune-release", "--tag", "datalake-latest", "--staging-dir", str(stg)],
                ):
                    cli_main()

                prev_m = src / "prev_manifest.json"
                prev_m.write_text((src / "manifest.json").read_text(encoding="utf-8"), encoding="utf-8")
                with patch(
                    "sys.argv",
                    ["datalake", "--data-dir", str(src), "stage-release", "--staging-dir", str(stg), "--prev-manifest", str(prev_m)],
                ):
                    cli_main()

    def test_pull_delta_skipping(self):
        with tempfile.TemporaryDirectory() as target_dir:
            target = Path(target_dir)
            eq_dir = target / "equities"
            eq_dir.mkdir(parents=True)

            df = pd.DataFrame({"Symbol": ["INFY"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [1800.0]})
            local_pq = eq_dir / "nse_cm_2026.parquet"
            df.to_parquet(local_pq)

            sha = compute_sha256(local_pq)
            sz = local_pq.stat().st_size

            mock_manifest = {
                "version": 1,
                "file_count": 1,
                "files": {
                    "equities/nse_cm_2026.parquet": {
                        "asset_name": "equities__nse_cm_2026.parquet",
                        "size_bytes": sz,
                        "sha256": sha,
                    }
                },
            }

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = mock_manifest

            with patch("datalake.sync.ResilientHTTPClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.get.return_value = mock_resp
                mock_client_cls.return_value.__aenter__.return_value = mock_client

                stats = asyncio.run(pull_from_github_release("user/repo", data_dir=target))
                self.assertEqual(stats["up_to_date_skipped"], 1)
                self.assertEqual(stats["downloaded"], 0)

    def test_engine_from_github_factory(self):
        with tempfile.TemporaryDirectory() as target_dir:
            eng = UnifiedLakeEngine.from_github("user/repo", lake_dir=target_dir, auto_sync=False)
            self.assertIsInstance(eng, UnifiedLakeEngine)
            self.assertEqual(str(eng.root), str(Path(target_dir).resolve()))

    def test_compacted_equity_bundle_generation(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)

            for yr in (2024, 2025, 2026):
                df = pd.DataFrame({
                    "Symbol": ["TCS", "INFY"],
                    "Date": pd.to_datetime([f"{yr}-01-02", f"{yr}-01-02"]),
                    "Close": [3500.0 + yr, 1500.0 + yr],
                })
                df.to_parquet(eq_dir / f"nse_cm_{yr}.parquet")

            manifest = prepare_release_staging(src, stg)
            self.assertIn("compacted_files", manifest)
            self.assertIn("compacted__nse_cm_hist.parquet", manifest["compacted_files"])
            self.assertTrue((stg / "compacted__nse_cm_hist.parquet").exists())

            # Verify compacted bundle only contains < current_year (2024 and 2025 = 4 rows)
            comp_df = pd.read_parquet(stg / "compacted__nse_cm_hist.parquet")
            self.assertEqual(len(comp_df), 4)
            self.assertEqual(comp_df["Symbol"].tolist(), ["INFY", "INFY", "TCS", "TCS"])

    def test_by_ticker_packs_generation(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)

            for yr in (2025, 2026):
                df = pd.DataFrame({
                    "Symbol": ["TCS", "INFY"],
                    "Date": pd.to_datetime([f"{yr}-01-02", f"{yr}-01-02"]),
                    "Close": [3500.0 + yr, 1500.0 + yr],
                })
                df.to_parquet(eq_dir / f"nse_cm_{yr}.parquet")

                bse_df = pd.DataFrame({
                    "InstrumentId": ["500325"],
                    "Date": pd.to_datetime([f"{yr}-01-02"]),
                    "Close": [1300.0 + yr],
                })
                bse_df.to_parquet(eq_dir / f"bse_cm_{yr}.parquet")

            manifest = prepare_release_staging(src, stg)
            self.assertIn("by_ticker_packs", manifest)
            self.assertIn("by_ticker__nse_hist.pack", manifest["by_ticker_packs"])
            self.assertIn("by_ticker__nse_curr.pack", manifest["by_ticker_packs"])
            self.assertIn("by_ticker__nse_index.json", manifest["by_ticker_packs"])
            self.assertIn("by_ticker__bse_hist.pack", manifest["by_ticker_packs"])
            self.assertIn("by_ticker__bse_curr.pack", manifest["by_ticker_packs"])
            self.assertIn("by_ticker__bse_index.json", manifest["by_ticker_packs"])

            self.assertTrue((stg / "by_ticker__nse_hist.pack").exists())
            self.assertTrue((stg / "by_ticker__nse_curr.pack").exists())
            self.assertTrue((stg / "by_ticker__nse_index.json").exists())
            self.assertTrue((stg / "by_ticker__bse_hist.pack").exists())
            self.assertTrue((stg / "by_ticker__bse_curr.pack").exists())
            self.assertTrue((stg / "by_ticker__bse_index.json").exists())

            idx = json.loads((stg / "by_ticker__nse_index.json").read_text(encoding="utf-8"))
            self.assertIn("TCS", idx["tickers"])
            self.assertIn("INFY", idx["tickers"])
            self.assertIn("hist", idx["tickers"]["TCS"])
            self.assertIn("curr", idx["tickers"]["TCS"])

            bse_idx = json.loads((stg / "by_ticker__bse_index.json").read_text(encoding="utf-8"))
            self.assertIn("500325", bse_idx["tickers"])
            self.assertIn("hist", bse_idx["tickers"]["500325"])
            self.assertIn("curr", bse_idx["tickers"]["500325"])

    def test_screener_snapshot_in_manifest_files(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame([
                {"Symbol": "TCS", "Date": pd.to_datetime("2026-01-01"), "Close": 3500.0, "Volume": 1000, "Series": "EQ"}
            ])
            df.to_parquet(eq_dir / "nse_cm_2026.parquet")

            prepare_release_staging(src, stg)
            m_path = src / "manifest.json"
            self.assertTrue(m_path.exists())
            manifest = json.loads(m_path.read_text(encoding="utf-8"))
            self.assertIn("snapshots/market_screener_latest.parquet", manifest["files"])
            self.assertIn("snapshots__market_screener_latest.parquet", [p.name for p in stg.glob("*")])

    def test_fo_option_chain_snapshot_in_manifest(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            fo_dir = src / "derivatives"
            fo_dir.mkdir(parents=True)
            df = pd.DataFrame([
                {"Symbol": "NIFTY", "Date": pd.to_datetime("2026-01-01"), "ExpiryDate": pd.to_datetime("2026-01-29"), "StrikePrice": 24000.0, "OptionType": "CE", "Close": 100.0},
                {"Symbol": "NIFTY", "Date": pd.to_datetime("2026-01-02"), "ExpiryDate": pd.to_datetime("2026-01-29"), "StrikePrice": 24000.0, "OptionType": "CE", "Close": 110.0},
            ])
            df.to_parquet(fo_dir / "nse_fo_2026.parquet")

            prepare_release_staging(src, stg)
            m_path = src / "manifest.json"
            self.assertTrue(m_path.exists())
            manifest = json.loads(m_path.read_text(encoding="utf-8"))
            self.assertIn("snapshots/fo_option_chain_latest.parquet", manifest["files"])
            self.assertIn("snapshots__fo_option_chain_latest.parquet", [p.name for p in stg.glob("*")])
            snap_df = pd.read_parquet(stg / "snapshots__fo_option_chain_latest.parquet")
            self.assertEqual(len(snap_df), 1)
            self.assertEqual(snap_df.iloc[0]["Close"], 110.0)

    def test_dossier_pack_includes_option_chain(self):
        import zlib
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            fo_dir = src / "derivatives"
            fo_dir.mkdir(parents=True)
            df = pd.DataFrame([
                {"Symbol": "RELIANCE", "Date": pd.to_datetime("2026-01-02"), "ExpiryDate": "2026-01-29", "StrikePrice": 2800.0, "OptionType": "CE", "Close": 55.0},
            ])
            df.to_parquet(fo_dir / "nse_fo_2026.parquet")

            from datalake.sync import build_company_dossier_pack
            offsets = build_company_dossier_pack(src, stg, alias_to_canon={"RELIANCE": "RELIANCE"})
            self.assertIn("RELIANCE", offsets)

            pack_file = stg / "by_ticker__nse_dossier.pack"
            self.assertTrue(pack_file.exists())
            off, length = offsets["RELIANCE"]
            with pack_file.open("rb") as f:
                f.seek(off)
                blob = f.read(length)
            payload = json.loads(zlib.decompress(blob).decode("utf-8"))
            self.assertIn("option_chain", payload)
            self.assertEqual(len(payload["option_chain"]), 1)
            self.assertEqual(payload["option_chain"][0]["StrikePrice"], 2800.0)

    def test_dossier_pack_scrip_code_always_overrides_symbol_for_tcs(self):
        import zlib
        from datalake.remote import RemoteLakeClient
        from datalake.sync import build_company_dossier_pack

        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            shp_dir = src / "shareholding"
            shp_dir.mkdir(parents=True)

            # Simulate BSE shareholding history where Symbol was set to numeric "532540"
            bse_shp = pd.DataFrame([
                {
                    "Symbol": "532540",
                    "ScripCode": "532540",
                    "QuarterId": f"Q{i}",
                    "QuarterStr": f"Q{i} FY26",
                    "PromoterPct": 71.77,
                    "PublicPct": 28.23,
                }
                for i in range(1, 108)
            ])
            bse_shp.to_parquet(shp_dir / "bse_shareholding_history.parquet")

            offsets = build_company_dossier_pack(
                src,
                stg,
                alias_to_canon={"TCS": "TCS"},
                scrip_to_canon={"532540": "TCS"},
            )
            self.assertIn("TCS", offsets)
            self.assertNotIn("532540", offsets)

            pack_file = stg / "by_ticker__nse_dossier.pack"
            off, length = offsets["TCS"]
            with pack_file.open("rb") as f:
                f.seek(off)
                payload = json.loads(zlib.decompress(f.read(length)).decode("utf-8"))
            self.assertIn("shareholding_bse", payload)
            self.assertEqual(len(payload["shareholding_bse"]), 107)

            # Verify RemoteLakeClient.shareholding("TCS") returns all 107 quarters
            client = RemoteLakeClient("user/bfinance-datalake", load_httpfs=False, use_pack=True)
            client._dossier_cache["TCS"] = {
                "shareholding_bse": pd.DataFrame(payload["shareholding_bse"]),
            }
            shp_df = client.shareholding("TCS")
            self.assertEqual(len(shp_df), 107)

    def test_bse_financials_fallback_and_shp_ixbrl_typed_member(self):
        from datalake.config import LakeConfig
        from datalake.scrapers.xbrl import XBRLScraper
        from datalake.state import StateStore

        sample_ixbrl = b"""<?xml version="1.0" encoding="utf-8"?>
        <html xmlns:xbrli="http://www.xbrl.org/2003/instance"
              xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
              xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
              xmlns:in-bse-shp="http://www.bseindia.com/xbrl/shp/2022-09-30/in-bse-shp">
          <head>
            <xbrli:context id="OneD_Promo">
              <xbrli:entity><xbrli:identifier scheme="http://www.bseindia.com">500325</xbrli:identifier>
                <xbrli:segment>
                  <xbrldi:explicitMember dimension="in-bse-shp:ShareholdingPatternAxis">in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember</xbrldi:explicitMember>
                </xbrli:segment>
              </xbrli:entity>
            </xbrli:context>
            <xbrli:context id="TypedOnly_Ctx1">
              <xbrli:entity><xbrli:identifier scheme="http://www.bseindia.com">500325</xbrli:identifier>
                <xbrli:segment>
                  <xbrldi:typedMember dimension="in-bse-shp:DetailsOfSharesHeldByIndividualsOrHUFAxis">
                    <in-bse-shp:DetailsOfSharesHeldByIndividualsOrHUFDomain>IndividualsOrHUF1</in-bse-shp:DetailsOfSharesHeldByIndividualsOrHUFDomain>
                  </xbrldi:typedMember>
                </xbrli:segment>
              </xbrli:entity>
            </xbrli:context>
          </head>
          <body>
            <ix:nonFraction contextRef="OneD_Promo" name="in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares">50.01</ix:nonFraction>
            <ix:nonNumeric contextRef="TypedOnly_Ctx1" name="in-bse-shp:NameOfTheShareholder">Life Insurance Corporation of India</ix:nonNumeric>
            <ix:nonFraction contextRef="TypedOnly_Ctx1" name="in-bse-shp:ShareholdingAsAPercentageOfTotalNumberOfShares">6.45</ix:nonFraction>
          </body>
        </html>
        """
        parsed = XBRLScraper.parse_full_shp_xbrl(sample_ixbrl)
        self.assertEqual(parsed["summary"]["PromoterPct"], 50.01)
        self.assertEqual(len(parsed["named_holders"]), 1)
        self.assertEqual(parsed["named_holders"][0]["HolderName"], "Life Insurance Corporation of India")
        self.assertEqual(parsed["named_holders"][0]["PctOfTotal"], 6.45)

        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = LakeConfig(root_dir=Path(tmp_dir))
            state = StateStore(cfg.state_db_path)
            mock_http = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.text = json.dumps({
                "QtlyinCr": (
                    "<table><tr><td>Particulars</td><td>Sep-26</td><td>Jun-26</td></tr>"
                    "<tr><td>Revenue</td><td>65,000.00</td><td>64,000.00</td></tr>"
                    "<tr><td>Net Profit</td><td>12,500.00</td><td>12,100.00</td></tr>"
                    "<tr><td>EPS</td><td>34.50</td><td>33.40</td></tr></table>"
                ),
                "AnninCr": (
                    "<table><tr><td>Particulars</td><td>2026</td><td>2025</td></tr>"
                    "<tr><td>Revenue</td><td>255,000.00</td><td>240,000.00</td></tr>"
                    "<tr><td>Net Profit</td><td>48,500.00</td><td>45,900.00</td></tr>"
                    "<tr><td>EPS</td><td>134.00</td><td>126.80</td></tr></table>"
                ),
            })
            mock_resp.json.return_value = json.loads(mock_resp.text)
            mock_http.get.return_value = mock_resp

            scraper = XBRLScraper(mock_http, state, cfg)
            count = asyncio.run(scraper.sync_symbol_financials_bse("532540", "TCS"))
            self.assertEqual(count, 4)
            fin_df = pd.read_parquet(cfg.root_dir / "fundamentals" / "xbrl_financials.parquet")
            self.assertEqual(len(fin_df), 4)
            self.assertEqual(set(fin_df["Symbol"]), {"TCS"})
            self.assertEqual(set(fin_df["PeriodType"]), {"Quarterly", "Annual"})


if __name__ == "__main__":
    unittest.main()

