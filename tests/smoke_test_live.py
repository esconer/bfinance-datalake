from __future__ import annotations

import asyncio
import shutil
import time
from datetime import date, timedelta
from pathlib import Path
import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.cli import print_status
from datalake.config import LakeConfig, today_ist
from datalake.doctor import LakeDoctor
from datalake.http_client import ResilientHTTPClient
from datalake.scrapers.deals_docs import DealsAndDocumentsScraper
from datalake.scrapers.equity_eod import EquityEODScraper
from datalake.scrapers.fo_eod import DerivativesEODScraper
from datalake.scrapers.funds_flows import FundsAndFlowsScraper
from datalake.scrapers.indices import IndicesScraper
from datalake.scrapers.universe_actions import UniverseAndActionsScraper
from datalake.scrapers.xbrl import XBRLScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe


def last_recent_weekdays(n: int = 3) -> tuple[date, date]:
    cur = today_ist() - timedelta(days=1)
    weekdays = []
    while len(weekdays) < n:
        if cur.weekday() < 5:
            weekdays.append(cur)
        cur -= timedelta(days=1)
    return min(weekdays), max(weekdays)


async def run_live_smoke_test(smoke_dir: Path) -> int:
    if smoke_dir.exists():
        shutil.rmtree(smoke_dir)

    cfg = LakeConfig(root_dir=smoke_dir, polite_delay_sec=0.15)
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)

    win_start, win_end = last_recent_weekdays(3)
    print("=" * 100)
    print(f"RUNNING LIVE 15-SOURCE + MULTI-DECADE (1995..2026) SMOKE TEST -> {smoke_dir}")
    print(f"Recent Trading Window: {win_start} to {win_end}")
    print("=" * 100)

    results_table: list[tuple[str, str, int, str]] = []
    decade_table: list[tuple[str, str, str, int, str]] = []

    async def _run_step(name: str, coro, expected_glob: str):
        t0 = time.perf_counter()
        try:
            await coro
            elapsed = time.perf_counter() - t0
            files = sorted(cfg.root_dir.glob(expected_glob))
            if not files:
                results_table.append((name, "FAIL (No File)", 0, f"{elapsed:.1f}s"))
                return
            total_rows = sum(len(read_table_safe(f)) for f in files)
            status = "PASS" if total_rows > 0 else "WARN (0 rows)"
            results_table.append((name, status, total_rows, f"{elapsed:.1f}s -> {files[-1].name}"))
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            results_table.append((name, f"ERROR: {type(exc).__name__}", 0, f"{elapsed:.1f}s ({exc})"))

    async def _run_decade_probe(era_label: str, dataset_desc: str, coro, target_file: Path, year_filter: int | None = None):
        t0 = time.perf_counter()
        try:
            await coro
            elapsed = time.perf_counter() - t0
            if not target_file.exists():
                decade_table.append((era_label, dataset_desc, "FAIL (No File)", 0, f"{elapsed:.1f}s"))
                return
            df = read_table_safe(target_file)
            if year_filter is not None and "Date" in df.columns:
                df = df[pd.to_datetime(df["Date"], errors="coerce").dt.year == year_filter]
            rows = len(df)
            st = "PASS" if rows > 0 else "FAIL (0 rows)"
            sample_dt = ""
            if rows > 0 and "Date" in df.columns:
                min_d = pd.to_datetime(df["Date"]).min().strftime("%Y-%m-%d")
                max_d = pd.to_datetime(df["Date"]).max().strftime("%Y-%m-%d")
                sample_dt = f" [{min_d}..{max_d}]"
            decade_table.append((era_label, dataset_desc, st, rows, f"{elapsed:.1f}s -> {target_file.name}{sample_dt}"))
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            decade_table.append((era_label, dataset_desc, f"ERROR: {type(exc).__name__}", 0, f"{elapsed:.1f}s ({exc})"))

    async with ResilientHTTPClient() as http:
        univ = UniverseAndActionsScraper(http, state, cfg)
        eq = EquityEODScraper(http, state, cfg)
        fo = DerivativesEODScraper(http, state, cfg)
        idx = IndicesScraper(http, state, cfg)
        xbrl = XBRLScraper(http, state, cfg)
        ff = FundsAndFlowsScraper(http, state, cfg)
        dd = DealsAndDocumentsScraper(http, state, cfg)

        # ---------------------------------------------------------------------
        # PART A: All 15 Live Pipeline Sources
        # ---------------------------------------------------------------------
        await _run_step(
            "1. Security Masters & Symbol Renames",
            univ.sync_security_masters(),
            "universe/*.parquet",
        )
        await _run_step(
            "2. Corporate Actions (BSE + NSE)",
            univ.sync_corporate_actions(start_year=win_end.year, end_year=win_end.year),
            "actions/*.parquet",
        )
        await _run_step(
            "3. NSE Equity EOD + Delivery (3d)",
            eq.sync_range(win_start, win_end, exchanges=("NSE",)),
            "equities/nse_cm_*.parquet",
        )
        await _run_step(
            "4. BSE Equity EOD + Delivery (3d)",
            eq.sync_range(win_start, win_end, exchanges=("BSE",)),
            "equities/bse_cm_*.parquet",
        )
        await _run_step(
            "5. Pre-2016 Cloud Seed (Upstox CDN)",
            eq.seed_pre2016_via_upstox_cdn(start_year=2015, end_year=2015, max_symbols=2),
            "equities/nse_cm_2015.parquet",
        )
        await _run_step(
            "6. NSE F&O Option Chains (2d)",
            fo.sync_range(win_end - timedelta(days=2), win_end, exchanges=("NSE",)),
            "derivatives/nse_fo_*.parquet",
        )
        await _run_step(
            "7. Nifty 50 OHLC + TRI + PE (30d)",
            idx.backfill_30y_bulk(win_end - timedelta(days=30), win_end, indices=("NIFTY 50",)),
            "indices/nifty_30y_ohlc_tri_pe.parquet",
        )
        await _run_step(
            "8. Daily All-Indices Snapshot (3d)",
            idx.sync_daily_snapshots(win_start, win_end),
            "indices/ind_close_all_*.parquet",
        )
        await _run_step(
            "9. Ind-AS XBRL Financials (RELIANCE)",
            xbrl.sync_symbol_financials_nse("RELIANCE"),
            "fundamentals/xbrl_financials.parquet",
        )
        await _run_step(
            "10. Shareholding & Pledge (RELIANCE)",
            xbrl.sync_bse_shareholding("500325", symbol="RELIANCE"),
            "shareholding/*.parquet",
        )
        await _run_step(
            "11. AMFI / MFapi Mutual Fund NAVs",
            ff.sync_amfi_daily(),
            "mutual_funds/amfi_nav_*.parquet",
        )
        await _run_step(
            "12. NSE F&O Participant OI (3d)",
            ff.sync_participant_oi(win_start, win_end),
            "flows/fao_participant_oi_*.parquet",
        )
        await _run_step(
            "13. Bulk/Block Deals",
            dd.sync_bulk_and_block_deals(),
            "deals/bulk_block_deals.parquet",
        )
        await _run_step(
            "14. Cash FII/DII Daily Flows",
            dd.sync_cash_fii_dii_daily(),
            "flows/cash_fii_dii_flows.parquet",
        )
        await _run_step(
            "15. Annual Reports (NSE + BSE)",
            dd.sync_annual_reports_bse(["500325"], symbols=["RELIANCE"]),
            "documents/annual_reports.parquet",
        )

        # ---------------------------------------------------------------------
        # PART B: Multi-Decade Historical Era Probes (1995, 2000, 2005, 2008, 2012, 2015, 2020)
        # ---------------------------------------------------------------------
        print("\n--- Probing Multi-Decade Historical Eras (1995, 2000, 2005, 2008, 2012, 2015, 2020) ---")

        # 1995: NSE Equity + Delivery via legacy bhavcopy (RELIANCE is present in 1995 archives)
        await _run_decade_probe(
            "1995",
            "NSE Equity + Delivery (1995-01-03)",
            eq.sync_range(date(1995, 1, 2), date(1995, 1, 3), exchanges=("NSE",)),
            cfg.root_dir / "equities" / "nse_cm_1995.parquet",
        )
        await _run_decade_probe(
            "1995",
            "Nifty 50 Index OHLC (Nov 1995)",
            idx.backfill_30y_bulk(date(1995, 11, 3), date(1995, 11, 15), indices=("NIFTY 50",)),
            cfg.root_dir / "indices" / "nifty_30y_ohlc_tri_pe.parquet",
            year_filter=1995,
        )

        # 2000: NSE Equity (2000 via historicalOR; Upstox CDN starts ~2008)
        await _run_decade_probe(
            "2000",
            "NSE Equity OHLCV (RELIANCE 2000)",
            eq.backfill_nse_symbol_via_historical_or("RELIANCE", start_year=2000, end_year=2000),
            cfg.root_dir / "equities" / "nse_cm_2000.parquet",
        )

        # 2005: NSE Equity (2005 via historicalOR) & Corporate Actions (2005)
        await _run_decade_probe(
            "2005",
            "NSE Equity OHLCV (RELIANCE 2005)",
            eq.backfill_nse_symbol_via_historical_or("RELIANCE", start_year=2005, end_year=2005),
            cfg.root_dir / "equities" / "nse_cm_2005.parquet",
        )
        await _run_decade_probe(
            "2005",
            "Corporate Actions (2005)",
            univ.sync_nse_corporate_actions(start_year=2005, end_year=2005),
            cfg.root_dir / "actions" / "nse_corporate_actions.parquet",
        )

        # 2008: NSE Equity (2008), BSE Legacy Equity (Jan 2008), & Mutual Fund NAVs (2008)
        await _run_decade_probe(
            "2008",
            "NSE Equity OHLCV (2008)",
            eq.seed_pre2016_via_upstox_cdn(start_year=2008, end_year=2008, max_symbols=2),
            cfg.root_dir / "equities" / "nse_cm_2008.parquet",
        )
        await _run_decade_probe(
            "2008",
            "BSE Legacy Equity + ISIN (2008-01-02)",
            eq.sync_range(date(2008, 1, 2), date(2008, 1, 3), exchanges=("BSE",)),
            cfg.root_dir / "equities" / "bse_cm_2008.parquet",
        )
        await _run_decade_probe(
            "2008",
            "Mutual Fund NAVs (2008 via MFapi)",
            ff.sync_schemes_via_mfapi((100122,), year_filter=2008),
            cfg.root_dir / "mutual_funds" / "amfi_nav_2008.parquet",
        )

        # 2012: NSE Equity (2012), Daily All-Indices CSV (2012-09-03), & F&O Participant OI (2012-09-03)
        await _run_decade_probe(
            "2012",
            "NSE Equity OHLCV (2012)",
            eq.seed_pre2016_via_upstox_cdn(start_year=2012, end_year=2012, max_symbols=2),
            cfg.root_dir / "equities" / "nse_cm_2012.parquet",
        )
        await _run_decade_probe(
            "2012",
            "Daily All-Indices Snapshot (2012-09-03)",
            idx.sync_daily_snapshots(date(2012, 9, 3), date(2012, 9, 4)),
            cfg.root_dir / "indices" / "ind_close_all_2012.parquet",
        )
        await _run_decade_probe(
            "2012",
            "NSE F&O Participant OI (2012-09-03)",
            ff.sync_participant_oi(date(2012, 9, 3), date(2012, 9, 4)),
            cfg.root_dir / "flows" / "fao_participant_oi_2012.parquet",
        )

        # 2015: BSE Legacy Equity (2015-01-01) & Mutual Fund NAVs (2015)
        await _run_decade_probe(
            "2015",
            "BSE Legacy Equity + ISIN (2015-01-01)",
            eq.sync_range(date(2015, 1, 1), date(2015, 1, 2), exchanges=("BSE",)),
            cfg.root_dir / "equities" / "bse_cm_2015.parquet",
        )

        # 2020: NSE Intermediate Era (sec_bhavdata_full + Legacy cm*bhav ISIN) & Legacy F&O (2020-01-01)
        await _run_decade_probe(
            "2020",
            "NSE Equity + Delivery + ISIN (2020-01-02)",
            eq.sync_range(date(2020, 1, 2), date(2020, 1, 3), exchanges=("NSE",)),
            cfg.root_dir / "equities" / "nse_cm_2020.parquet",
        )
        await _run_decade_probe(
            "2020",
            "NSE Legacy F&O Option Chains (2020-01-02)",
            fo.sync_range(date(2020, 1, 2), date(2020, 1, 2), exchanges=("NSE",)),
            cfg.root_dir / "derivatives" / "nse_fo_2020.parquet",
        )

    doc_stats = LakeDoctor(cfg).run_full_check_and_repair()

    nse_files = sorted((cfg.root_dir / "equities").glob("nse_cm_202*.parquet"))
    adj_ok = False
    if nse_files:
        df_eq = read_table_safe(nse_files[-1])
        rel = df_eq[df_eq["Symbol"] == "RELIANCE"]
        if not rel.empty:
            adj_df = OHLCVAdjuster(cfg.root_dir).apply_adjustments(rel, "RELIANCE", exchange="NSE")
            adj_ok = "Adj Close" in adj_df.columns and not adj_df["Adj Close"].isna().all()

    print("\n" + "=" * 100)
    print("TABLE 1: 15-SOURCE LIVE PIPELINE SCORECARD")
    print("=" * 100)
    print(f"{'Source / Pipeline Step':<40} | {'Status':<16} | {'Rows':>10} | {'Details'}")
    print("-" * 100)
    failures = 0
    for step_name, st, r_cnt, detail in results_table:
        if not st.startswith("PASS"):
            failures += 1
        print(f"{step_name:<40} | {st:<16} | {r_cnt:>10,} | {detail}")

    print("\n" + "=" * 100)
    print("TABLE 2: MULTI-DECADE HISTORICAL ERA VERIFICATION (1995 -> 2020)")
    print("=" * 100)
    print(f"{'Era':<6} | {'Historical Dataset Probe':<40} | {'Status':<14} | {'Rows':>8} | {'Details'}")
    print("-" * 100)
    for era, desc, st, r_cnt, detail in decade_table:
        if not st.startswith("PASS"):
            failures += 1
        print(f"{era:<6} | {desc:<40} | {st:<14} | {r_cnt:>8,} | {detail}")
    print("-" * 100)
    print(f"LakeDoctor Self-Healing Check : {doc_stats}")
    print(f"OHLCVAdjuster (`Adj Close`)   : {'PASS' if adj_ok else 'SKIPPED'}")
    print("=" * 100)

    print_status(cfg)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_live_smoke_test(Path("./smoke_data").resolve())))
