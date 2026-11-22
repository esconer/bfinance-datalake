from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.config import LakeConfig
from datalake.engine import HAS_DUCKDB, UnifiedLakeEngine
from datalake.http_client import SimpleHTTPResponse
from datalake.scrapers.universe_actions import parse_action_purpose
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe, write_parquet_atomic
from datalake.sync import (
    CONTRACT_VERSION,
    prepare_release_staging,
    pull_from_github_release,
)


class _FakeHTTP:
    """Offline async HTTP stub for XBRL scraper and GitHub Release sync tests."""

    def __init__(self, responses: dict[str, SimpleHTTPResponse]):
        self._responses = responses
        self.calls: list[str] = []
        self.client = None  # Force pull_from_github_release to route through http.get()

    async def __aenter__(self) -> _FakeHTTP:
        return self

    async def __aexit__(self, *args) -> None:
        return None

    async def get(self, url: str, **kwargs) -> SimpleHTTPResponse:
        self.calls.append(url)
        for key, resp in self._responses.items():
            if key in url:
                return resp
        return SimpleHTTPResponse(status_code=404, content=b"not found", headers={})


class TestStabilizationRegressions(unittest.TestCase):
    def test_1_sparse_bse_fallback_never_overwrites_existing_xbrl_bs_cfo(self) -> None:
        """Patch 1: Sparse BSE P&L rows must not erase existing XBRL Reserves, Assets, or CFO."""
        with tempfile.TemporaryDirectory() as td:
            cfg = LakeConfig(root_dir=Path(td))
            cfg.ensure_dirs()
            state = StateStore(cfg.state_db_path)

            fin_path = cfg.root_dir / "fundamentals" / "xbrl_financials.parquet"
            rich_xbrl = pd.DataFrame([
                {
                    "Symbol": "RELIANCE",
                    "NatureOfReport": "Consolidated",
                    "PeriodType": "Quarterly",
                    "PeriodStart": "2025-01-01",
                    "PeriodEnd": "2025-03-31",
                    "FilingDate": "2025-04-25",
                    "Sales_Cr": 240000.0,
                    "NetProfit_Cr": 19500.0,
                    "Reserves_Cr": 780000.0,
                    "TotalAssets_Cr": 1750000.0,
                    "TotalBorrowings_Cr": 310000.0,
                    "TotalCashAndBank_Cr": 190000.0,
                    "CFO_Cr": 42000.0,
                }
            ])
            write_parquet_atomic(rich_xbrl, fin_path)

            # BSE returns sparse P&L for BOTH existing Mar-25 and missing Jun-25
            bse_html = (
                "<tr><td>(In Cr.)</td><td>Jun-25</td><td>Mar-25</td></tr>"
                "<tr><td>Revenue</td><td>250000</td><td>239999</td></tr>"
                "<tr><td>Net Profit</td><td>20100</td><td>19499</td></tr>"
            )
            payload = json.dumps({"QtlyinCr": bse_html, "AnninCr": ""}).encode("utf-8")
            fake_http = _FakeHTTP({
                "GetReportNewFor_Result/w": SimpleHTTPResponse(200, payload),
            })

            scraper = XBRLScraper(fake_http, state, cfg)  # type: ignore[arg-type]
            added = asyncio.run(scraper.sync_symbol_financials_bse("500325", "RELIANCE"))
            self.assertGreater(added, 0)

            stored = read_table_safe(fin_path)
            mar_row = stored[stored["PeriodEnd"] == "2025-03-31"].iloc[0]
            jun_rows = stored[stored["PeriodEnd"] == "2025-06-30"]

            # Existing Mar-25 XBRL Balance Sheet & Cash Flow values remain untouched
            self.assertAlmostEqual(float(mar_row["Sales_Cr"]), 240000.0)
            self.assertAlmostEqual(float(mar_row["Reserves_Cr"]), 780000.0)
            self.assertAlmostEqual(float(mar_row["TotalAssets_Cr"]), 1750000.0)
            self.assertAlmostEqual(float(mar_row["CFO_Cr"]), 42000.0)
            # Missing Jun-25 quarter is still appended
            self.assertEqual(len(jun_rows), 1)
            self.assertAlmostEqual(float(jun_rows.iloc[0]["Sales_Cr"]), 250000.0)

    def test_2_upstox_dividend_scaling_and_reverse_split_consolidation(self) -> None:
        """Patch 2 & 3: Upstox rows scale raw dividends by raw_cum_split_factor; old_fv < new_fv -> CONSOLIDATION."""
        with tempfile.TemporaryDirectory() as td:
            lake = Path(td)
            (lake / "actions").mkdir(parents=True)

            actions_df = pd.DataFrame([
                {
                    "symbol": "TESTCO",
                    "ExDate": pd.Timestamp("2015-06-10"),
                    "ActionType": "DIVIDEND",
                    "DividendPerShareRs": 20.0,  # Raw pre-split NSE dividend
                    "ShareMultiplier": 1.0,
                    "subject": "Dividend Rs 20 Per Share",
                },
                {
                    "symbol": "TESTCO",
                    "ExDate": pd.Timestamp("2018-06-10"),
                    "ActionType": "SPLIT",
                    "DividendPerShareRs": 0.0,
                    "ShareMultiplier": 2.0,  # 2-for-1 split in 2018
                    "subject": "Face Value Split From Rs 10 to Rs 5",
                },
            ])
            write_parquet_atomic(actions_df, lake / "actions" / "nse_corporate_actions.parquet")

            ohlcv = pd.DataFrame([
                # Upstox pre-2016 rows (PrevClose & TurnoverValue are NaN; prices already split-adjusted 1000 -> 500)
                {
                    "Date": pd.Timestamp("2015-06-09"), "Symbol": "TESTCO", "Series": "EQ",
                    "Open": 500.0, "High": 505.0, "Low": 495.0, "Close": 500.0,
                    "PrevClose": np.nan, "TurnoverValue": np.nan, "Volume": 1000.0,
                },
                {
                    "Date": pd.Timestamp("2015-06-10"), "Symbol": "TESTCO", "Series": "EQ",
                    "Open": 490.0, "High": 495.0, "Low": 485.0, "Close": 490.0,
                    "PrevClose": np.nan, "TurnoverValue": np.nan, "Volume": 1000.0,
                },
                # Native NSE raw rows (2018): 1200 -> 600 on 2:1 split
                {
                    "Date": pd.Timestamp("2018-06-08"), "Symbol": "TESTCO", "Series": "EQ",
                    "Open": 1200.0, "High": 1210.0, "Low": 1190.0, "Close": 1200.0,
                    "PrevClose": 1195.0, "TurnoverValue": 1e7, "Volume": 1000.0,
                },
                {
                    "Date": pd.Timestamp("2018-06-10"), "Symbol": "TESTCO", "Series": "EQ",
                    "Open": 600.0, "High": 605.0, "Low": 595.0, "Close": 600.0,
                    "PrevClose": 600.0, "TurnoverValue": 1e7, "Volume": 2000.0,
                },
            ])

            adj = OHLCVAdjuster(lake)
            res_tr = adj.apply_adjustments(ohlcv, "TESTCO", exchange="NSE", auto_adjust=False, adjust_dividends=True)
            res_sp = adj.apply_adjustments(ohlcv, "TESTCO", exchange="NSE", auto_adjust=False, adjust_dividends=False)

            # Raw div Rs 20 * raw_cum_split_factor 0.5 = Rs 10 on split-adj prev_close 500
            # => div_step = 1 - 10/500 = 0.98 => Adj Close = 500 * 0.98 = 490.0
            self.assertAlmostEqual(float(res_tr.loc[0, "Adj Close"]), 490.0, places=4)
            # Split-only mode skips dividend step
            self.assertAlmostEqual(float(res_sp.loc[0, "Adj Close"]), 500.0, places=4)
            # Native NSE pre-split row adjusts 1200 -> 600
            self.assertAlmostEqual(float(res_tr.loc[2, "Adj Close"]), 600.0, places=4)

        # Reverse split / consolidation: old_fv < new_fv -> CONSOLIDATION with ShareMultiplier = old_fv / new_fv
        parsed_rev = parse_action_purpose("Face Value Split From Rs 1/- Per Share To Rs 10/- Per Share")
        self.assertEqual(parsed_rev["ActionType"], "CONSOLIDATION")
        self.assertAlmostEqual(parsed_rev["ShareMultiplier"], 0.1, places=6)

        # After applying Patch A, phrasing with "Consolidation" also parses identically:
        parsed_cons = parse_action_purpose("Face Value Consolidation From Rs 1/- To Rs 10/-")
        self.assertEqual(parsed_cons["ActionType"], "CONSOLIDATION")
        self.assertAlmostEqual(parsed_cons["ShareMultiplier"], 0.1, places=6)

    @unittest.skipUnless(HAS_DUCKDB, "duckdb is required for screener() SQL tests")
    def test_3_screener_carries_forward_bs_enforces_ev_gate_and_numeric_quarter_id(self) -> None:
        """Patch 4: Q1/Q3 carries forward Q4 BS, EV_EBITDA=NULL if cash/debt missing, QuarterId numeric sort."""
        with tempfile.TemporaryDirectory() as td:
            cfg = LakeConfig(root_dir=Path(td))
            cfg.ensure_dirs()

            # Equities snapshot
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "Date": pd.Timestamp("2025-07-01"), "Exchange": "NSE",
                        "InstrumentId": "ALPHA", "Symbol": "ALPHA", "Series": "EQ",
                        "Open": 100.0, "High": 105.0, "Low": 98.0, "Close": 100.0,
                        "PrevClose": 99.0, "Volume": 500000.0, "TurnoverValue": 5e7,
                    },
                    {
                        "Date": pd.Timestamp("2025-07-01"), "Exchange": "NSE",
                        "InstrumentId": "NOCASH", "Symbol": "NOCASH", "Series": "EQ",
                        "Open": 50.0, "High": 52.0, "Low": 49.0, "Close": 50.0,
                        "PrevClose": 49.5, "Volume": 100000.0, "TurnoverValue": 5e6,
                    },
                ]),
                cfg.root_dir / "equities" / "nse_cm_2025.parquet",
            )

            # Financials: ALPHA has Q4 BS + Q1 NULL BS; NOCASH has missing TotalCashAndBank_Cr
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "Symbol": "ALPHA", "NatureOfReport": "Consolidated", "PeriodType": "Quarterly",
                        "PeriodStart": "2025-01-01", "PeriodEnd": "2025-03-31", "FilingDate": "2025-04-20",
                        "Sales_Cr": 100.0, "PBT_Cr": 20.0, "NetProfit_Cr": 15.0, "BasicEPS_Rs": 5.0,
                        "Interest_Cr": 2.0, "Depreciation_Cr": 3.0, "OtherIncome_Cr": 1.0,
                        "EquityCapital_Cr": 10.0, "Reserves_Cr": 90.0,
                        "TotalBorrowings_Cr": 50.0, "TotalCashAndBank_Cr": 20.0,
                    },
                    {
                        "Symbol": "ALPHA", "NatureOfReport": "Consolidated", "PeriodType": "Quarterly",
                        "PeriodStart": "2025-04-01", "PeriodEnd": "2025-06-30", "FilingDate": "2025-07-20",
                        "Sales_Cr": 110.0, "PBT_Cr": 22.0, "NetProfit_Cr": 16.0, "BasicEPS_Rs": 5.5,
                        "Interest_Cr": 2.0, "Depreciation_Cr": 3.0, "OtherIncome_Cr": 1.0,
                        "EquityCapital_Cr": 10.0, "Reserves_Cr": np.nan,
                        "TotalBorrowings_Cr": np.nan, "TotalCashAndBank_Cr": np.nan,
                    },
                    {
                        "Symbol": "NOCASH", "NatureOfReport": "Consolidated", "PeriodType": "Quarterly",
                        "PeriodStart": "2025-01-01", "PeriodEnd": "2025-03-31", "FilingDate": "2025-04-20",
                        "Sales_Cr": 80.0, "PBT_Cr": 12.0, "NetProfit_Cr": 9.0, "BasicEPS_Rs": 3.0,
                        "Interest_Cr": 1.0, "Depreciation_Cr": 2.0, "OtherIncome_Cr": 0.5,
                        "EquityCapital_Cr": 10.0, "Reserves_Cr": 90.0,
                        "TotalBorrowings_Cr": 40.0, "TotalCashAndBank_Cr": np.nan,
                    },
                ]),
                cfg.root_dir / "fundamentals" / "xbrl_financials.parquet",
            )

            # NSE shareholding omits TotalShares so screener joins latest_bse_shares
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "Symbol": "ALPHA", "QuarterEnd": "2025-06-30",
                        "PromoterPct": 55.0, "PromoterPledgedPct": 0.0,
                        "MutualFundsPct": 12.0, "FPI_Pct": 18.0, "RetailPct": 15.0,
                    },
                    {
                        "Symbol": "NOCASH", "QuarterEnd": "2025-06-30",
                        "PromoterPct": 60.0, "PromoterPledgedPct": 0.0,
                        "MutualFundsPct": 10.0, "FPI_Pct": 10.0, "RetailPct": 20.0,
                    },
                ]),
                cfg.root_dir / "shareholding" / "nse_shareholding_summary.parquet",
            )

            # BSE shareholding with string QuarterIds "99" vs "128" (numeric 128 > 99, lex "99" > "128")
            write_parquet_atomic(
                pd.DataFrame([
                    {"ScripCode": "500001", "Symbol": "ALPHA", "QuarterId": "99", "TotalShares": 10_000_000.0},
                    {"ScripCode": "500001", "Symbol": "ALPHA", "QuarterId": "128", "TotalShares": 20_000_000.0},
                    {"ScripCode": "500002", "Symbol": "NOCASH", "QuarterId": "128", "TotalShares": 10_000_000.0},
                ]),
                cfg.root_dir / "shareholding" / "bse_shareholding_history.parquet",
            )

            eng = UnifiedLakeEngine(cfg.root_dir)
            scr = eng.screener()
            alpha = scr[scr["Symbol"] == "ALPHA"].iloc[0]
            nocash = scr[scr["Symbol"] == "NOCASH"].iloc[0]

            # Q4 BS carried forward into Q1 instead of COALESCE(..., 0)
            self.assertAlmostEqual(float(alpha["BookValue_Cr"]), 100.0, places=2)
            self.assertAlmostEqual(float(alpha["TotalBorrowings_Cr"]), 50.0, places=2)
            self.assertAlmostEqual(float(alpha["TotalCashAndBank_Cr"]), 20.0, places=2)
            self.assertTrue(pd.notna(alpha["EV_EBITDA"]))

            # Numeric QuarterId sort picks "128" (20M shares) over "99" (10M shares)
            self.assertAlmostEqual(float(alpha["TotalShares"]), 20_000_000.0, places=0)
            self.assertAlmostEqual(float(alpha["MarketCap_Cr"]), 200.0, places=2)

            # Missing TotalCashAndBank_Cr trips strict 3-leg EV gate -> EV_EBITDA must be NULL
            self.assertTrue(pd.isna(nocash["EV_EBITDA"]))

    def test_4_multicomponent_dividend_and_non_adjusting_demerger(self) -> None:
        """Patch 2 & 3: Multi-component dividend sums to 4.45; include_non_adjusting returns DEMERGER."""
        parsed = parse_action_purpose("Dividend Final Rs 2.80 And Special Rs 1.65 Per Share")
        self.assertEqual(parsed["ActionType"], "DIVIDEND")
        self.assertAlmostEqual(parsed["DividendPerShareRs"], 4.45, places=4)

        with tempfile.TemporaryDirectory() as td:
            lake = Path(td)
            (lake / "actions").mkdir(parents=True)
            actions_df = pd.DataFrame([
                {
                    "symbol": "ITC",
                    "ExDate": pd.Timestamp("2025-01-06"),
                    "ActionType": "DEMERGER",
                    "DividendPerShareRs": 0.0,
                    "ShareMultiplier": 1.0,
                    "subject": "Demerger of Hotel Business",
                },
                {
                    "symbol": "ITC",
                    "ExDate": pd.Timestamp("2025-02-12"),
                    "ActionType": "DIVIDEND",
                    "DividendPerShareRs": 6.50,
                    "ShareMultiplier": 1.0,
                    "subject": "Interim Dividend - Rs 6.50 Per Share",
                },
            ])
            write_parquet_atomic(actions_df, lake / "actions" / "nse_corporate_actions.parquet")

            eng = UnifiedLakeEngine(lake)
            default_acts = eng.corporate_actions("ITC")
            all_acts = eng.corporate_actions("ITC", include_non_adjusting=True)

            self.assertNotIn("DEMERGER", default_acts["ActionType"].tolist())
            self.assertIn("DEMERGER", all_acts["ActionType"].tolist())
            self.assertIn("DIVIDEND", all_acts["ActionType"].tolist())

    def test_5_release_contract_v1_unified_master_and_profile_pull(self) -> None:
        """Patch 5: contract_version=='1.0.0', unified master, profile tags, and core_longterm pull filter."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_dir = root / "data"
            staging_dir = root / "staging"
            pull_dir = root / "pulled"
            cfg = LakeConfig(root_dir=data_dir)
            cfg.ensure_dirs()

            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "SYMBOL": "RELIANCE",
                        "NAME OF COMPANY": "Reliance Industries Limited",
                        "ISIN NUMBER": "INE002A01018",
                        "FACE VALUE": 10.0,
                        "DATE OF LISTING": "29-NOV-1995",
                    }
                ]),
                data_dir / "universe" / "nse_equity_master.parquet",
            )
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "SCRIP_CD": "500325",
                        "Scrip_Name": "Reliance Industries Ltd",
                        "ISIN_NUMBER": "INE002A01018",
                        "INDUSTRY": "Refineries",
                        "FACE_VALUE": 10.0,
                        "scrip_id": "RELIANCE",
                    }
                ]),
                data_dir / "universe" / "bse_scrip_master.parquet",
            )
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "Date": pd.Timestamp("2026-11-20"), "Exchange": "NSE",
                        "InstrumentId": "RELIANCE", "Symbol": "RELIANCE", "Series": "EQ",
                        "Open": 1300.0, "High": 1310.0, "Low": 1295.0, "Close": 1305.0,
                        "PrevClose": 1300.0, "Volume": 100000.0, "TurnoverValue": 1.3e8,
                    }
                ]),
                data_dir / "equities" / "nse_cm_2026.parquet",
            )
            write_parquet_atomic(
                pd.DataFrame([
                    {
                        "Date": pd.Timestamp("2022-05-10"), "Exchange": "NSE",
                        "InstrumentType": "FUTIDX", "Symbol": "NIFTY",
                        "ExpiryDate": pd.Timestamp("2022-05-26"),
                        "StrikePrice": 0.0, "OptionType": "XX",
                        "Open": 16200.0, "High": 16300.0, "Low": 16100.0, "Close": 16250.0,
                        "SettlePrice": 16250.0, "Contracts": 1000.0, "OpenInterest": 50000.0,
                    }
                ]),
                data_dir / "derivatives" / "nse_fo_2022.parquet",
            )

            manifest = prepare_release_staging(data_dir, staging_dir)

            self.assertEqual(manifest.get("contract_version"), CONTRACT_VERSION)
            self.assertEqual(CONTRACT_VERSION, "1.0.0")

            unified_path = data_dir / "universe" / "security_master_unified.parquet"
            self.assertTrue(unified_path.exists())
            unified_df = read_table_safe(unified_path)
            expected_cols = {
                "symbol", "bse_code", "isin", "company_name", "industry",
                "face_value", "listing_date", "primary_exchange", "aliases_json",
            }
            self.assertTrue(expected_cols.issubset(set(unified_df.columns)))
            self.assertEqual(unified_df.iloc[0]["symbol"], "RELIANCE")
            self.assertEqual(unified_df.iloc[0]["bse_code"], "500325")
            self.assertEqual(unified_df.iloc[0]["isin"], "INE002A01018")

            self.assertEqual(manifest["files"]["equities/nse_cm_2026.parquet"]["profile"], "core_longterm")
            self.assertEqual(manifest["files"]["derivatives/nse_fo_2022.parquet"]["profile"], "derivatives_archive")

            # Verify core_longterm pull skips historical F&O archive
            fake_responses: dict[str, SimpleHTTPResponse] = {
                "/manifest.json": SimpleHTTPResponse(200, (staging_dir / "manifest.json").read_bytes()),
            }
            for meta in manifest["files"].values():
                asset_name = meta["asset_name"]
                asset_path = staging_dir / asset_name
                if asset_path.exists():
                    fake_responses[f"/{asset_name}"] = SimpleHTTPResponse(200, asset_path.read_bytes())

            fake_http = _FakeHTTP(fake_responses)
            with patch("datalake.sync.ResilientHTTPClient", return_value=fake_http):
                stats = asyncio.run(
                    pull_from_github_release(
                        repo="esconer/bfinance-datalake",
                        data_dir=pull_dir,
                        profile="core_longterm",
                    )
                )

            self.assertGreater(stats["downloaded"], 0)
            self.assertTrue((pull_dir / "equities" / "nse_cm_2026.parquet").exists())
            self.assertTrue((pull_dir / "universe" / "security_master_unified.parquet").exists())
            self.assertFalse((pull_dir / "derivatives" / "nse_fo_2022.parquet").exists())


if __name__ == "__main__":
    unittest.main()
