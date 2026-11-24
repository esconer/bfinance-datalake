"""Offline verification suite for p3.txt Section 3 ("Verification Spec").

Consolidates the six named regression checks from ``p3.txt`` lines 921-944 into a
standalone ``unittest.TestCase`` module. Everything is deterministic and offline:
HTTP is mocked with ``SimpleHTTPResponse`` and every lake lives in a temp dir.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

import datalake.engine as engine_mod
import datalake.sync as sync
from datalake.config import LakeConfig
from datalake.doctor import measure_turnover_unit, unexplained_price_jumps
from datalake.engine import UnifiedLakeEngine
from datalake.http_client import SimpleHTTPResponse
from datalake.scrapers.equity_eod import is_debt_instrument
from datalake.scrapers.funds_flows import FundsAndFlowsScraper
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe
from datalake.symbols import SymbolRenameResolver


def _rupee_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Low": [100.0, 200.0],
            "High": [110.0, 220.0],
            "Volume": [1000.0, 500.0],
            "TurnoverValue": [105000.0, 105000.0],
        }
    )


def _jump_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Symbol": ["AAA", "AAA", "AAA", "BBB", "BBB"],
            "Date": [
                "2026-01-05", "2026-01-06", "2026-01-07",
                "2026-01-01", "2026-01-20",
            ],
            "Close": [100.0, 50.0, 48.0, 100.0, 50.0],
            # Exchange-reported PrevClose steps down overnight for AAA's drop.
            "PrevClose": [float("nan"), 50.0, 48.0, float("nan"), float("nan")],
        }
    )


def _dates(idx: pd.DatetimeIndex) -> set[str]:
    return {pd.Timestamp(d).strftime("%Y-%m-%d") for d in idx}


class _ParticipantHttp:
    """Minimal async HTTP stub returning the same CSV for every archive URL."""

    def __init__(self, payload: bytes):
        self.payload = payload
        self.urls: list[str] = []

    async def get(self, url, **kwargs):
        self.urls.append(url)
        return SimpleHTTPResponse(status_code=200, content=self.payload)


_PARTICIPANT_CSV = (
    "NSE Participant Volume Data (fao_participant_vol)\n"
    "Client Type,Long Clients %,Long Clients (Contracts),Short Clients %,Short Clients (Contracts)\n"
    "Client,10,100,20,200\n"
    "FII,30,300,40,400\n"
    "DII,5,50,6,60\n"
    "Pro,7,70,8,80\n"
    "Other,1,1,1,1\n"
).encode()

_INDEX_DATES = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
_WEEKEND_DATES = {"2026-01-03", "2026-01-04"}


class TestTask2AndAuditFixes(unittest.TestCase):
    """Six p3.txt verification checks (W1-W5 fixes), fully offline."""

    def _tmpdir(self) -> Path:
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        return Path(td.name)

    # 1 ----------------------------------------------------------------------
    def test_vwap_turnover_unit_detects_rupees_lakhs_and_corrupt_rows(self):
        # Valid rupee rows are healthy.
        healthy = measure_turnover_unit(_rupee_df())
        self.assertEqual(healthy["detected_unit"], "rupees")
        self.assertIs(healthy["is_healthy"], True)
        self.assertEqual(healthy["rows_checked"], 2)
        self.assertEqual(healthy["bad_rows"], 0)

        # Small-volume 2-decimal TURNOVER_LACS rounding must NOT be flagged.
        small_volume = pd.DataFrame(
            {
                "Low": [100.0],
                "High": [100.0],
                "Volume": [10.0],
                "TurnoverValue": [1000.05],  # +/-Rs 500 lacs rounding -> VWAP 100.005
            }
        )
        tolerant = measure_turnover_unit(small_volume, allow_lacs_rounding=True)
        self.assertEqual(tolerant["detected_unit"], "rupees")
        self.assertEqual(tolerant["bad_rows"], 0)
        self.assertIs(tolerant["is_healthy"], True)
        # The very same row is corrupt without the rounding tolerance.
        strict = measure_turnover_unit(small_volume, allow_lacs_rounding=False)
        self.assertEqual(strict["bad_rows"], 1)
        self.assertIs(strict["is_healthy"], False)

        # Lakhs-scaled turnover is an unhealthy unit and corrupts every row.
        lakhs = measure_turnover_unit(
            _rupee_df().assign(TurnoverValue=lambda d: d["TurnoverValue"] / 1e5)
        )
        self.assertEqual(lakhs["detected_unit"], "lakhs")
        self.assertIs(lakhs["is_healthy"], False)
        self.assertEqual(lakhs["bad_rows"], 2)
        self.assertGreater(lakhs["violation_rate"], 0.0)

        # Crores-scaled turnover is likewise unhealthy.
        crores = measure_turnover_unit(
            _rupee_df().assign(TurnoverValue=lambda d: d["TurnoverValue"] / 1e7)
        )
        self.assertEqual(crores["detected_unit"], "crores")
        self.assertIs(crores["is_healthy"], False)
        self.assertEqual(crores["bad_rows"], 2)
        self.assertGreater(crores["violation_rate"], 0.0)

    # 2 ----------------------------------------------------------------------
    def test_unexplained_price_jumps_flags_missing_action_and_ignores_recorded_split(self):
        # No action + exchange PrevClose step-down -> flagged, LikelyMissingAction True.
        jumps = unexplained_price_jumps(_jump_df())
        self.assertEqual(len(jumps), 1)
        hit = jumps.iloc[0]
        self.assertEqual(hit["Symbol"], "AAA")
        self.assertEqual(hit["PctDrop"], -50.0)
        self.assertIs(bool(hit["LikelyMissingAction"]), True)
        self.assertEqual(hit["ExchangePrevClose"], 50.0)

        # A matching SPLIT within +/-3d suppresses the jump entirely.
        actions = pd.DataFrame(
            {
                "symbol": ["AAA"],
                "ExDate": ["2026-01-06"],
                "ActionType": ["SPLIT"],
            }
        )
        self.assertTrue(unexplained_price_jumps(_jump_df(), actions).empty)

        # The recorded SPLIT may belong to a pre-rename alias of the symbol.
        resolver = SymbolRenameResolver(
            pd.DataFrame(
                {
                    "OldSymbol": ["OLD"],
                    "NewSymbol": ["AAA"],
                    "ChangeDate": ["2025-01-01"],
                }
            )
        )
        alias_actions = pd.DataFrame(
            {
                "symbol": ["OLD"],
                "ExDate": ["2026-01-06"],
                "ActionType": ["SPLIT"],
            }
        )
        self.assertTrue(
            unexplained_price_jumps(_jump_df(), alias_actions, resolver=resolver).empty
        )

        # A drop across a >10-day suspension gap is ignored.
        gap_df = pd.DataFrame(
            {
                "Symbol": ["BBB", "BBB"],
                "Date": ["2026-01-01", "2026-01-20"],
                "Close": [100.0, 50.0],
            }
        )
        self.assertTrue(unexplained_price_jumps(gap_df).empty)

    # 3 ----------------------------------------------------------------------
    def test_is_debt_instrument_zero_false_positives_on_equities_and_etfs(self):
        equities_and_etfs = [
            ("RELIANCE", "EQ", "INE002A01018", "RELIANCE INDUSTRIES LTD", "NSE"),
            ("BONDADA", "EQ", "INE0Q8P01011", "BONDADA", "NSE"),
            ("BONDADA", "SM", "INE0Q8P01011", "BONDADA", "NSE"),
            ("GOLDBEES", "EQ", "INF204KB17I5", "GOLDBEES", "NSE"),
            ("LIQUIDBEES", "EQ", "INF732E01037", "LIQUIDBEES", "NSE"),
            ("GSFC", "EQ", "INE026A01025", "GSFC", "NSE"),
        ]
        for symbol, series, isin, name, exchange in equities_and_etfs:
            with self.subTest(symbol=symbol, series=series):
                self.assertIs(
                    is_debt_instrument(
                        symbol=symbol, series=series, isin=isin, name=name, exchange=exchange
                    ),
                    False,
                )

        debt_instruments = [
            ("726GS2033", "GS", "IN0020220151", None, "NSE"),
            ("364D150126", "TB", "IN002024Z441", None, "NSE"),
            ("SGBFEB32IV", "GB", "IN0020230184", None, "NSE"),
            ("NCD", "N1", "INE001A07RU9", None, "NSE"),
            ("GOVT", "G", None, None, "BSE"),
            ("CORP", "F", "INE001A07RU9", None, "BSE"),
            (None, "B", None, "8.50% NHAI TAX FREE BOND 2029", "NSE"),
        ]
        for symbol, series, isin, name, exchange in debt_instruments:
            with self.subTest(symbol=symbol, series=series, name=name):
                self.assertIs(
                    is_debt_instrument(
                        symbol=symbol, series=series, isin=isin, name=name, exchange=exchange
                    ),
                    True,
                )

    # 4 ----------------------------------------------------------------------
    def test_participant_vol_sync_and_engine_query(self):
        root = self._tmpdir()
        day = date(2026, 11, 20)  # fao_participant_vol_20112026.csv
        http = _ParticipantHttp(_PARTICIPANT_CSV)
        cfg = LakeConfig(root_dir=root, polite_delay_sec=0.0)
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)
        scraper = FundsAndFlowsScraper(http, state, cfg)

        asyncio.run(scraper.sync_participant_vol(day, day))

        # Mocked the exact archive CSV named in the spec.
        self.assertTrue(http.urls)
        self.assertTrue(any(u.endswith("fao_participant_vol_20112026.csv") for u in http.urls))

        target = root / "flows" / "fao_participant_vol_2026.parquet"
        self.assertTrue(target.exists())
        out = read_table_safe(target)
        self.assertEqual(set(out["Client Type"]), {"Client", "FII", "DII", "Pro"})
        self.assertNotIn("Other", set(out["Client Type"]))

        # nse_participant_vol watermark was recorded.
        self.assertIs(state.is_completed("nse_participant_vol", day.isoformat()), True)

        # Engine query API filters by client type.
        eng = UnifiedLakeEngine(root)
        fii = eng.participant_vol(client_type="FII")
        self.assertEqual(len(fii), 1)
        self.assertEqual(fii.iloc[0]["Client Type"], "FII")

        # SQL view is queryable.
        sql_df = eng.sql("SELECT * FROM participant_vol")
        self.assertEqual(len(sql_df), 4)
        self.assertIn("Client Type", sql_df.columns)

    # 5 ----------------------------------------------------------------------
    def test_trading_days_weekend_filter_and_manifest_multi_col_watermarks(self):
        root = self._tmpdir()
        (root / "indices").mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "IndexName": ["NIFTY 50"] * len(_INDEX_DATES),
                "Date": pd.to_datetime(_INDEX_DATES),
                "Close": [24000.0 + i * 100 for i in range(len(_INDEX_DATES))],
            }
        ).to_parquet(root / "indices" / "nifty_30y_ohlc_tri_pe.parquet", index=False)

        eng = UnifiedLakeEngine(root)
        start, end = "2026-01-01", "2026-01-05"
        default = eng.trading_days(start=start, end=end)
        self.assertEqual(_dates(default), set(_INDEX_DATES) - _WEEKEND_DATES)
        self.assertTrue(all(d.weekday() < 5 for d in default))

        weekends = eng.trading_days(start=start, end=end, include_weekends=True)
        self.assertEqual(_dates(weekends), set(_INDEX_DATES))
        self.assertTrue(_WEEKEND_DATES <= _dates(weekends))

        cases = {
            "fundamentals/xbrl_financials.parquet": ("PeriodEnd", "2026-03-31"),
            "actions/nse_corporate_actions.parquet": ("ExDate", "2026-04-10"),
            "documents/upcoming_board_meetings.parquet": ("MeetingDate", "2026-05-12"),
            "actions/nse_symbol_changes.parquet": ("ChangeDate", "2026-06-01"),
        }
        for rel, (col, val) in cases.items():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({col: [val]}).to_parquet(p, index=False)

        man = sync.build_manifest(root)
        for rel, (_, val) in cases.items():
            with self.subTest(path=rel):
                self.assertEqual(man["latest_dates"][rel], val)
                self.assertEqual(man["files"][rel]["latest_date"], val)

    # 6 ----------------------------------------------------------------------
    def test_fast_crosswalk_unified_cache_and_mf_cache_and_sparse_xbrl_upgrade(self):
        root = self._tmpdir()
        (root / "universe").mkdir(parents=True, exist_ok=True)
        (root / "mutual_funds").mkdir(parents=True, exist_ok=True)

        # (a) Unified-master crosswalk fast path with raw masters absent.
        pd.DataFrame(
            [
                {
                    "symbol": "ZZTESTCO",
                    "bse_code": "500999",
                    "isin": "INE999A01018",
                    "company_name": "ZZ Test Company Ltd",
                    "aliases_json": json.dumps(["ZZTESTCO", "OLDZZTESTCO"]),
                    "face_value": 10.0,
                    "industry": "Testing",
                    "listing_date": "2001-04-02",
                    "primary_exchange": "NSE",
                }
            ]
        ).to_parquet(root / "universe" / "security_master_unified.parquet", index=False)
        self.assertFalse((root / "universe" / "nse_equity_master.parquet").exists())
        self.assertFalse((root / "universe" / "bse_scrip_master.parquet").exists())

        eng = UnifiedLakeEngine(root)
        orig_read = engine_mod.read_table_safe
        reads: list[str] = []

        def counting_read(path):
            reads.append(Path(path).name)
            return orig_read(path)

        with patch.object(engine_mod, "read_table_safe", side_effect=counting_read):
            identities = [
                eng.resolve("ZZTESTCO"),
                eng.resolve("OLDZZTESTCO"),
                eng.resolve("500999"),
                eng.resolve("INE999A01018"),
            ]
        self.assertEqual(reads, ["security_master_unified.parquet"])
        for ident in identities:
            self.assertEqual(ident.symbol, "ZZTESTCO")
            self.assertEqual(ident.bse_code, "500999")
            self.assertEqual(ident.isin, "INE999A01018")
            self.assertEqual(ident.aliases, ("ZZTESTCO", "OLDZZTESTCO"))

        # build_unified_security_master() must bypass the unified cache.
        mock_eng = MagicMock()
        mock_eng._by_symbol = {}
        mock_eng._by_bse = {}
        with patch.object(sync, "UnifiedLakeEngine", return_value=mock_eng):
            self.assertIsNone(sync.build_unified_security_master(root))
        mock_eng._ensure_crosswalk.assert_called_once()
        self.assertIs(
            mock_eng._ensure_crosswalk.call_args.kwargs["use_unified_cache"], False
        )

        # (b) mutual_funds() cached on second call; refreshed by reload().
        mf_path = root / "mutual_funds" / "amfi_nav_2026.parquet"

        def _mf_frame(nav: float) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "Date": pd.to_datetime(["2026-01-02"]),
                    "SchemeCode": [100122],
                    "SchemeName": ["HDFC Test Fund"],
                    "NAV": [nav],
                    "Category": ["Equity"],
                    "AMC": ["HDFC"],
                    "ISINGrowth": ["INF200K01HI7"],
                }
            )

        _mf_frame(151.0).to_parquet(mf_path, index=False)
        mf_reads: list[str] = []

        def counting_mf_read(path):
            mf_reads.append(Path(path).name)
            return orig_read(path)

        with patch.object(engine_mod, "read_table_safe", side_effect=counting_mf_read):
            first = eng.mutual_funds()
            second = eng.mutual_funds()
        self.assertEqual(mf_reads, ["amfi_nav_2026.parquet"])
        self.assertEqual(len(first), len(second))
        self.assertEqual(float(first.iloc[0]["NAV"]), 151.0)

        # On-disk change is invisible until reload().
        _mf_frame(999.0).to_parquet(mf_path, index=False)
        self.assertEqual(float(eng.mutual_funds().iloc[0]["NAV"]), 151.0)
        eng.reload()
        with patch.object(engine_mod, "read_table_safe", side_effect=counting_mf_read):
            refreshed = eng.mutual_funds()
        self.assertEqual(mf_reads, ["amfi_nav_2026.parquet", "amfi_nav_2026.parquet"])
        self.assertEqual(float(refreshed.iloc[0]["NAV"]), 999.0)

        # (c) Sparse XBRL symbols: all-NaN Reserves_Cr only.
        cfg = LakeConfig(root_dir=root, polite_delay_sec=0.0)
        cfg.ensure_dirs()
        scraper = XBRLScraper(http=None, state=StateStore(cfg.state_db_path), cfg=cfg)
        (root / "fundamentals").mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {"Symbol": "SPARSE", "Reserves_Cr": float("nan"), "Sales_Cr": 10.0},
                {"Symbol": "RICH", "Reserves_Cr": 123.4, "Sales_Cr": 20.0},
                {"Symbol": "MIXED", "Reserves_Cr": float("nan"), "Sales_Cr": 1.0},
                {"Symbol": "MIXED", "Reserves_Cr": 5.0, "Sales_Cr": 2.0},
            ]
        ).to_parquet(root / "fundamentals" / "xbrl_financials.parquet", index=False)
        self.assertEqual(scraper._sparse_financial_symbols(), {"SPARSE"})


if __name__ == "__main__":
    unittest.main()
