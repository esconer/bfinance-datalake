"""Deterministic offline regression tests for W4 sparse-XBRL upgrade path."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from datalake.config import LakeConfig
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore


def _scraper(root: Path) -> XBRLScraper:
    cfg = LakeConfig(root_dir=root, polite_delay_sec=0.0)
    cfg.ensure_dirs()
    return XBRLScraper(http=None, state=StateStore(cfg.state_db_path), cfg=cfg)


def _write_financials(root: Path, df: pd.DataFrame) -> None:
    df.to_parquet(root / "fundamentals" / "xbrl_financials.parquet", index=False)


def test_sparse_financial_symbols_flags_only_all_nan_symbols(tmp_path):
    scraper = _scraper(tmp_path)
    _write_financials(
        tmp_path,
        pd.DataFrame(
            [
                {"Symbol": "SPARSE", "Reserves_Cr": float("nan"), "Sales_Cr": 10.0},
                {"Symbol": "RICH", "Reserves_Cr": 123.4, "Sales_Cr": 20.0},
                # Mixed rows for one symbol: any populated Reserves_Cr means not sparse.
                {"Symbol": "MIXED", "Reserves_Cr": float("nan"), "Sales_Cr": 1.0},
                {"Symbol": "MIXED", "Reserves_Cr": 5.0, "Sales_Cr": 2.0},
            ]
        ),
    )
    assert scraper._sparse_financial_symbols() == {"SPARSE"}


def test_sparse_financial_symbols_when_reserves_column_absent(tmp_path):
    scraper = _scraper(tmp_path)
    _write_financials(
        tmp_path,
        pd.DataFrame(
            [
                {"Symbol": "alpha", "Sales_Cr": 10.0},
                {"Symbol": "BETA", "Sales_Cr": 20.0},
                {"Symbol": "alpha", "Sales_Cr": 11.0},
                {"Symbol": None, "Sales_Cr": 30.0},
            ]
        ),
    )
    assert scraper._sparse_financial_symbols() == {"ALPHA", "BETA"}


def test_sparse_financial_symbols_missing_file(tmp_path):
    scraper = _scraper(tmp_path)
    assert not (tmp_path / "fundamentals" / "xbrl_financials.parquet").exists()
    assert scraper._sparse_financial_symbols() == set()


def test_sparse_financial_symbols_unreadable_or_schemaless_file(tmp_path):
    scraper = _scraper(tmp_path)
    fin_p = tmp_path / "fundamentals" / "xbrl_financials.parquet"
    fin_p.write_bytes(b"definitely not a parquet file")
    assert scraper._sparse_financial_symbols() == set()
    _write_financials(tmp_path, pd.DataFrame({"Sales_Cr": [1.0, 2.0]}))
    assert scraper._sparse_financial_symbols() == set()


def test_sync_all_market_upgrade_sparse_requeues_completed_sparse_symbol(tmp_path):
    scraper = _scraper(tmp_path)
    _write_financials(
        tmp_path,
        pd.DataFrame(
            [
                {"Symbol": "SPARSE", "Reserves_Cr": float("nan"), "Sales_Cr": 10.0},
                {"Symbol": "RICH", "Reserves_Cr": 123.4, "Sales_Cr": 20.0},
            ]
        ),
    )
    pd.DataFrame({"SYMBOL": ["SPARSE", "RICH"]}).to_parquet(
        tmp_path / "universe" / "nse_equity_master.parquet", index=False
    )
    for sym in ("SPARSE", "RICH"):
        scraper.state.record("xbrl_financials_symbol", sym, "OK", 1)
        scraper.state.record("nse_shareholding_symbol", sym, "OK", 1)

    calls: list[tuple[str, str]] = []

    async def fake_fin(sym: str) -> None:
        calls.append(("fin", sym))

    async def fake_shp(sym: str) -> None:
        calls.append(("shp", sym))

    scraper.sync_symbol_financials_nse = fake_fin  # type: ignore[method-assign]
    scraper.sync_symbol_shareholding_nse = fake_shp  # type: ignore[method-assign]

    normal_syms, normal_scrips = asyncio.run(scraper.sync_all_market())
    assert normal_syms == []
    assert normal_scrips == []
    assert calls == []

    upgrade_syms, upgrade_scrips = asyncio.run(scraper.sync_all_market(upgrade_sparse=True))
    assert upgrade_syms == ["SPARSE"]
    assert upgrade_scrips == []
    assert ("fin", "SPARSE") in calls
    assert ("fin", "RICH") not in calls

    # done_fin is recomputed per call: the upgrade must not permanently mutate state.
    assert asyncio.run(scraper.sync_all_market())[0] == []
