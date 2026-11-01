from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, timedelta
from pathlib import Path

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("datalake.cli")


async def run_backfill(
    cfg: LakeConfig,
    start: date,
    end: date,
    include_fo: bool,
    include_amfi_history: bool,
) -> None:
    cfg.ensure_dirs()
    LakeDoctor(cfg).run_full_check_and_repair()
    state = StateStore(cfg.state_db_path)

    async with ResilientHTTPClient() as http:
        univ = UniverseAndActionsScraper(http, state, cfg)
        indices = IndicesScraper(http, state, cfg)
        eq = EquityEODScraper(http, state, cfg)
        fo = DerivativesEODScraper(http, state, cfg)
        ff = FundsAndFlowsScraper(http, state, cfg)
        dd = DealsAndDocumentsScraper(http, state, cfg)

        logger.info("=== Step 1/6: Security Masters, Symbol Renames, PIT Nifty Constituents & 30Y Corporate Actions ===")
        await univ.sync_security_masters()
        await univ.sync_corporate_actions(start_year=max(1995, start.year), end_year=end.year)

        logger.info("=== Step 2/6: 30-Year Nifty Indices OHLC + TRI + P/E Bulk Backfill ===")
        await indices.backfill_30y_bulk(start_date=start, end_date=end)
        await indices.sync_daily_snapshots(start_date=max(date(2012, 1, 2), start), end_date=end)

        logger.info("=== Step 3/6: NSE & BSE Daily Equity OHLCV + Delivery (%s -> %s) ===", start, end)
        await eq.sync_range(start_date=start, end_date=end, exchanges=("NSE", "BSE"))

        if include_fo:
            logger.info("=== Step 4/6: NSE & BSE F&O Futures + Option Chains (%s -> %s) ===", start, end)
            await fo.sync_range(start_date=start, end_date=end, exchanges=("NSE", "BSE"))

        logger.info("=== Step 5/6: F&O Participant OI, Cash FII/DII, Bulk/Block Deals, Insider PIT & Concalls ===")
        await ff.sync_participant_oi(start_date=max(date(2012, 1, 2), start), end_date=end)
        await dd.sync_cash_fii_dii_daily()
        await dd.sync_bulk_and_block_deals()
        await dd.backfill_historical_bulk_block_deals(start_year=max(2005, start.year), end_year=end.year)
        await dd.sync_insider_trading_pit(start_date=max(date(2018, 1, 1), start), end_date=end)
        await dd.sync_concalls_and_presentations(start_date=max(end - timedelta(days=90), start), end_date=end)

        logger.info("=== Step 6/6: AMFI Mutual Fund NAVs ===")
        await ff.sync_amfi_daily()
        if include_amfi_history:
            await ff.backfill_amfi_history(start_date=max(date(2006, 4, 1), start), end_date=end)

    LakeDoctor(cfg).run_full_check_and_repair()


async def run_daily_update(cfg: LakeConfig, lookback_days: int = 7, include_fo: bool = True) -> None:
    cfg.ensure_dirs()
    LakeDoctor(cfg).run_full_check_and_repair()
    state = StateStore(cfg.state_db_path)
    today = today_ist()
    default_start = today - timedelta(days=lookback_days)

    async with ResilientHTTPClient() as http:
        univ = UniverseAndActionsScraper(http, state, cfg)
        indices = IndicesScraper(http, state, cfg)
        eq = EquityEODScraper(http, state, cfg)
        fo = DerivativesEODScraper(http, state, cfg)
        ff = FundsAndFlowsScraper(http, state, cfg)
        dd = DealsAndDocumentsScraper(http, state, cfg)

        logger.info("=== Daily Update (%s): Masters, PIT Constituents & Parsed Corporate Actions ===", today)
        await univ.sync_security_masters()
        await univ.sync_corporate_actions(start_year=today.year, end_year=today.year)

        nse_start = state.max_ok_date("equity_eod_nse") or default_start
        bse_start = state.max_ok_date("equity_eod_bse") or default_start
        idx_start = state.max_ok_date("indices_daily_all") or default_start
        oi_start = state.max_ok_date("nse_participant_oi") or default_start

        await eq.sync_range(nse_start, today, exchanges=("NSE",))
        await eq.sync_range(bse_start, today, exchanges=("BSE",))

        if include_fo:
            fo_nse_start = state.max_ok_date("fo_eod_nse") or default_start
            fo_bse_start = state.max_ok_date("fo_eod_bse") or default_start
            await fo.sync_range(fo_nse_start, today, exchanges=("NSE",))
            await fo.sync_range(fo_bse_start, today, exchanges=("BSE",))

        await indices.sync_daily_snapshots(idx_start, today)
        await ff.sync_participant_oi(oi_start, today)
        await dd.sync_cash_fii_dii_daily()
        await dd.sync_bulk_and_block_deals()
        await dd.sync_insider_trading_pit(default_start, today)
        await dd.sync_concalls_and_presentations(default_start, today)
        await ff.sync_amfi_daily()

    LakeDoctor(cfg).run_full_check_and_repair()


async def run_xbrl_sync(
    cfg: LakeConfig, symbols: list[str], bse_scrips: list[str], all_market: bool, limit: int | None
) -> None:
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    async with ResilientHTTPClient() as http:
        xbrl = XBRLScraper(http, state, cfg)
        dd = DealsAndDocumentsScraper(http, state, cfg)
        if all_market:
            await xbrl.sync_all_market(max_companies=limit)
            return
        for sym in symbols:
            await xbrl.sync_symbol_financials_nse(sym)
            await xbrl.sync_symbol_shareholding_nse(sym)
        if bse_scrips:
            for scrip in bse_scrips:
                await xbrl.sync_bse_shareholding(scrip)
            await dd.sync_annual_reports_bse(bse_scrips)


async def run_pre2016_nse(cfg: LakeConfig, symbols: list[str], start_year: int, end_year: int) -> None:
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    async with ResilientHTTPClient() as http:
        eq = EquityEODScraper(http, state, cfg)
        for sym in symbols:
            await eq.backfill_nse_symbol_via_historical_or(sym, start_year=start_year, end_year=end_year)


async def run_seed_cloud(cfg: LakeConfig, start_year: int, end_year: int, limit: int | None) -> None:
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    async with ResilientHTTPClient() as http:
        univ = UniverseAndActionsScraper(http, state, cfg)
        eq = EquityEODScraper(http, state, cfg)
        await univ.sync_security_masters()
        await eq.seed_pre2016_via_upstox_cdn(start_year=start_year, end_year=end_year, max_symbols=limit)


def print_status(cfg: LakeConfig) -> None:
    state = StateStore(cfg.state_db_path)
    rows = state.summary()
    if not rows:
        print(f"No datasets recorded yet in {cfg.state_db_path}")
        return
    print(f"\nData Lake Root: {cfg.root_dir}")
    print("-" * 105)
    print(f"{'Dataset':<28} | {'OK Partitions':>13} | {'Holidays':>8} | {'Total Rows':>12} | {'Earliest':<12} | {'Latest':<12}")
    print("-" * 105)
    for r in rows:
        print(
            f"{r['dataset']:<28} | {r['ok_partitions']:>13,} | {r['holidays']:>8,} | "
            f"{r['total_rows']:>12,} | {str(r['min_key'])[:12]:<12} | {str(r['max_key'])[:12]:<12}"
        )
    print("-" * 105)


def main() -> None:
    parser = argparse.ArgumentParser(prog="datalake", description="Complete Automated Indian Financial Data Lake")
    parser.add_argument("--data-dir", default="./data", help="Root directory for Parquet & SQLite state")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # 1. backfill
    p_bf = sub.add_parser("backfill", help="Download & stitch full historical datasets (resumable)")
    p_bf.add_argument("--start", default="2016-01-01", help="Start date YYYY-MM-DD")
    p_bf.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: today IST)")
    p_bf.add_argument("--include-fo", action="store_true", help="Also backfill daily NSE/BSE F&O Option Chains & Futures")
    p_bf.add_argument("--amfi-history", action="store_true", help="Also backfill 90-day AMFI NAV windows since 2006")

    # 2. daily
    p_dy = sub.add_parser("daily", help="Incremental daily update from last watermark to today IST")
    p_dy.add_argument("--lookback-days", type=int, default=7, help="Fallback lookback days if state.db is empty")
    p_dy.add_argument("--no-fo", action="store_true", help="Skip daily F&O Option Chain sync")

    # 3. xbrl
    p_xb = sub.add_parser("xbrl", help="Sync Ind-AS XBRL financials, shareholding/pledge, >1%% holders & Annual Reports")
    p_xb.add_argument("--symbols", default="RELIANCE,TCS,HDFCBANK,INFY,ITC", help="Comma-separated NSE symbols")
    p_xb.add_argument("--bse-scrips", default="500325,532540,500180,500209,500875", help="Comma-separated BSE codes")
    p_xb.add_argument("--all-market", action="store_true", help="Automatically loop through all NSE & BSE companies")
    p_xb.add_argument("--limit", type=int, default=None, help="Optional cap when running --all-market")

    # 4. pre2016-nse
    p_p16 = sub.add_parser("pre2016-nse", help="Backfill 1995-2015 NSE Equity + Delivery via historicalOR endpoint")
    p_p16.add_argument("--symbols", default="RELIANCE,TCS,INFY,HDFCBANK,SBIN", help="Comma-separated NSE symbols")
    p_p16.add_argument("--start-year", type=int, default=1995)
    p_p16.add_argument("--end-year", type=int, default=2015)

    # 5. seed-cloud
    p_sc = sub.add_parser("seed-cloud", help="Cloud-native 2000-2015 NSE backfill via Upstox public V2 CDN")
    p_sc.add_argument("--start-year", type=int, default=2000)
    p_sc.add_argument("--end-year", type=int, default=2015)
    p_sc.add_argument("--limit", type=int, default=None)

    # 6. doctor
    sub.add_parser("doctor", help="Verify Parquet integrity, purge truncated days, and reconcile state.db")

    # 7. status
    sub.add_parser("status", help="Show watermark & row counts across all datasets")

    args = parser.parse_args()
    cfg = LakeConfig(root_dir=Path(args.data_dir).resolve())

    if args.cmd == "backfill":
        start_dt = date.fromisoformat(args.start)
        end_dt = date.fromisoformat(args.end) if args.end else today_ist()
        asyncio.run(run_backfill(cfg, start_dt, end_dt, args.include_fo, args.amfi_history))
        print_status(cfg)
    elif args.cmd == "daily":
        asyncio.run(run_daily_update(cfg, args.lookback_days, include_fo=not args.no_fo))
        print_status(cfg)
    elif args.cmd == "xbrl":
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
        scrips = [s.strip() for s in args.bse_scrips.split(",") if s.strip()]
        asyncio.run(run_xbrl_sync(cfg, syms, scrips, args.all_market, args.limit))
        print_status(cfg)
    elif args.cmd == "pre2016-nse":
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
        asyncio.run(run_pre2016_nse(cfg, syms, args.start_year, args.end_year))
        print_status(cfg)
    elif args.cmd == "seed-cloud":
        asyncio.run(run_seed_cloud(cfg, args.start_year, args.end_year, args.limit))
        print_status(cfg)
    elif args.cmd == "doctor":
        report = LakeDoctor(cfg).run_full_check_and_repair()
        print("Doctor Repair Summary:", report)
        print_status(cfg)
    elif args.cmd == "status":
        print_status(cfg)


if __name__ == "__main__":
    main()
