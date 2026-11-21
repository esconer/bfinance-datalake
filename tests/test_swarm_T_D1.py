"""Offline regression tests for Bug D1 (sparse BSE fallback erasing rich NSE XBRL rows)."""
from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd

from datalake.config import LakeConfig
from datalake.http_client import SimpleHTTPResponse
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe, write_parquet_atomic

_RICH_COLS = (
    "Reserves_Cr",
    "TotalBorrowings_Cr",
    "TotalCashAndBank_Cr",
    "TotalAssets_Cr",
    "OperatingCashFlow_Cr",
)

_RICH_ROW = {
    "Symbol": "RELIANCE",
    "NatureOfReport": "Consolidated",
    "PeriodType": "Quarterly",
    "PeriodStart": "2026-04-01",
    "PeriodEnd": "2026-06-30",
    "FilingDate": "2026-07-15",
    "SourceURL": "https://nsearchives.nseindia.com/rich.xml",
    "Sales_Cr": 235000.0,
    "NetProfit_Cr": 18000.0,
    "Reserves_Cr": 900000.0,
    "TotalBorrowings_Cr": 120000.0,
    "TotalCashAndBank_Cr": 85400.0,
    "TotalAssets_Cr": 1500000.0,
    "OperatingCashFlow_Cr": 61000.0,
}

# BSE returns P&L only: no Reserves_Cr / TotalBorrowings_Cr / TotalCashAndBank_Cr /
# TotalAssets_Cr / OperatingCashFlow_Cr.
_BSE_SPARSE_HTML = (
    "<table>"
    "<tr><td>(In Cr)</td><td>Jun-26</td></tr>"
    "<tr><td>Revenue</td><td>1000</td></tr>"
    "<tr><td>Net Profit</td><td>100</td></tr>"
    "</table>"
)


def _seed_lake(tmpdir: str):
    cfg = LakeConfig(root_dir=Path(tmpdir))
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    target = cfg.root_dir / "fundamentals" / "xbrl_financials.parquet"
    write_parquet_atomic(pd.DataFrame([_RICH_ROW]), target)
    return cfg, state, target


def _http(side_effect) -> MagicMock:
    http = MagicMock()
    http.get = AsyncMock(side_effect=side_effect)
    return http


def test_d1_sparse_bse_does_not_erase_rich_nse_xbrl_columns():
    with tempfile.TemporaryDirectory() as tmp:
        cfg, state, target = _seed_lake(tmp)
        bse_calls: list[str] = []

        async def bse_get(url, **kwargs):
            bse_calls.append(url)
            if "GetReportNewFor_Result" in url:
                return SimpleHTTPResponse(
                    200, json.dumps({"QtlyinCr": _BSE_SPARSE_HTML, "AnninCr": ""}).encode()
                )
            return SimpleHTTPResponse(200, b'{"Table": []}')

        scraper = XBRLScraper(_http(bse_get), state, cfg)
        written = asyncio.run(scraper.sync_symbol_financials_bse("500325", "RELIANCE"))
        assert written == 1
        assert bse_calls, "BSE endpoint must actually be exercised by this test"

        after = read_table_safe(target)
        assert len(after) == 1
        row = after.iloc[0]
        for col in _RICH_COLS:
            assert col in after.columns, f"rich column {col} disappeared from parquet schema"
            assert row[col] == _RICH_ROW[col], f"sparse BSE erased {col}: {row[col]}"
        assert row["Sales_Cr"] == 235000.0, "rich P&L row was replaced by the sparse BSE row"
        assert row["SourceURL"] == _RICH_ROW["SourceURL"]


def test_d1_sparse_bse_still_inserts_new_periods():
    with tempfile.TemporaryDirectory() as tmp:
        cfg, state, target = _seed_lake(tmp)

        async def bse_get(url, **kwargs):
            if "GetReportNewFor_Result" in url:
                # Mar-26 quarter -> key not present in the seeded rich row.
                html = (
                    "<table>"
                    "<tr><td>(In Cr)</td><td>Mar-26</td></tr>"
                    "<tr><td>Revenue</td><td>2000</td></tr>"
                    "<tr><td>Net Profit</td><td>200</td></tr>"
                    "</table>"
                )
                return SimpleHTTPResponse(200, json.dumps({"QtlyinCr": html, "AnninCr": ""}).encode())
            return SimpleHTTPResponse(200, b'{"Table": []}')

        scraper = XBRLScraper(_http(bse_get), state, cfg)
        assert asyncio.run(scraper.sync_symbol_financials_bse("500325", "RELIANCE")) == 1

        after = read_table_safe(target)
        assert len(after) == 2, "genuinely new BSE periods must still be ingested"
        assert set(after["PeriodEnd"]) == {"2026-06-30", "2026-03-31"}
        rich = after[after["PeriodEnd"] == "2026-06-30"].iloc[0]
        for col in _RICH_COLS:
            assert rich[col] == _RICH_ROW[col]


def test_d1_bse_not_called_when_nse_filings_already_completed():
    with tempfile.TemporaryDirectory() as tmp:
        cfg, state, target = _seed_lake(tmp)
        state.record("xbrl_financials", "RELIANCE:INDAS_Q2.xml", "OK", 1)
        bse_calls: list[str] = []

        async def nse_get(url, **kwargs):
            if "api.bseindia.com" in url:
                bse_calls.append(url)
                return SimpleHTTPResponse(200, b'{"QtlyinCr":"","AnninCr":"","Table":[]}')
            if "corporates-financial-results" in url:
                if "period=Quarterly" in url:
                    filings = [{
                        "symbol": "RELIANCE",
                        "xbrl": "https://nsearchives.nseindia.com/INDAS_Q2.xml",
                        "filingDate": "2026-07-15",
                    }]
                    return SimpleHTTPResponse(200, json.dumps(filings).encode())
                return SimpleHTTPResponse(200, b"[]")
            return SimpleHTTPResponse(403, b"blocked")

        scraper = XBRLScraper(_http(nse_get), state, cfg)
        asyncio.run(scraper.sync_symbol_financials_nse("RELIANCE"))

        assert bse_calls == [], f"BSE must be skipped when NSE XML is already completed: {bse_calls}"
        assert state.is_completed("xbrl_financials_symbol", "RELIANCE")
        after = read_table_safe(target)
        row = after.iloc[0]
        for col in _RICH_COLS:
            assert row[col] == _RICH_ROW[col], f"rich {col} was lost during NSE no-op sync"


def test_d1_bse_fallback_still_runs_when_nse_blocked():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = LakeConfig(root_dir=Path(tmp))
        cfg.ensure_dirs()
        state = StateStore(cfg.state_db_path)
        bse_calls: list[str] = []

        async def nse_get(url, **kwargs):
            if "api.bseindia.com" in url:
                bse_calls.append(url)
                if "GetReportNewFor_Result" in url:
                    return SimpleHTTPResponse(
                        200, json.dumps({"QtlyinCr": _BSE_SPARSE_HTML, "AnninCr": ""}).encode()
                    )
                return SimpleHTTPResponse(200, b'{"Table": []}')
            return SimpleHTTPResponse(403, b"blocked")

        scraper = XBRLScraper(_http(nse_get), state, cfg)
        asyncio.run(scraper.sync_symbol_financials_nse("RELIANCE"))

        assert bse_calls, "BSE fallback must still run when NSE is blocked and nothing is completed"
        target = cfg.root_dir / "fundamentals" / "xbrl_financials.parquet"
        assert target.exists()
        after = read_table_safe(target)
        assert len(after) == 1
        assert after.iloc[0]["Sales_Cr"] == 1000.0
        assert state.is_completed("xbrl_financials_symbol", "RELIANCE")
