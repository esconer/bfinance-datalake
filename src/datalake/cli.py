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
from datalake.sync import (
    prepare_release_staging,
    prune_retired_release_assets,
    pull_from_github_release,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("datalake.cli")


async def run_backfill(
    cfg: LakeConfig,
    start: date,
    end: date,
    include_fo: bool,
    include_amfi_history: bool,
    force: bool = False,
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
        await univ.sync_corporate_actions(start_year=max(1995, start.year), end_year=end.year, force=force)

        logger.info("=== Step 2/6: 30-Year Nifty Indices OHLC + TRI + P/E Bulk Backfill ===")
        await indices.backfill_30y_bulk(start_date=start, end_date=end, force=force)
        await indices.sync_daily_snapshots(start_date=max(date(2012, 1, 2), start), end_date=end)

        logger.info("=== Step 3/6: NSE & BSE Daily Equity OHLCV + Delivery (%s -> %s) ===", start, end)
        await eq.sync_range(start_date=start, end_date=end, exchanges=("NSE", "BSE"), force=force)

        if include_fo:
            logger.info("=== Step 4/6: NSE & BSE F&O Futures + Option Chains (%s -> %s) ===", start, end)
            await fo.sync_range(start_date=start, end_date=end, exchanges=("NSE", "BSE"), force=force)

        logger.info("=== Step 5/6: F&O Participant OI, Cash FII/DII, Bulk/Block Deals, Insider PIT & Concalls ===")
        await ff.sync_participant_oi(start_date=max(date(2012, 1, 2), start), end_date=end, force=force)
        await dd.sync_cash_fii_dii_daily()
        await dd.sync_bulk_and_block_deals()
        await dd.backfill_historical_bulk_block_deals(start_year=max(2005, start.year), end_year=end.year, force=force)
        await dd.sync_insider_trading_pit(start_date=max(date(2018, 1, 1), start), end_date=end, force=force)
        await dd.sync_concalls_and_presentations(start_date=max(end - timedelta(days=365), start), end_date=end)
        xbrl = XBRLScraper(http, state, cfg)
        await xbrl.sync_recent_market_financials_nse()

        logger.info("=== Step 6/6: AMFI Mutual Fund NAVs ===")
        await ff.sync_amfi_daily()
        if include_amfi_history:
            await ff.backfill_amfi_history(start_date=max(date(2006, 4, 1), start), end_date=end, force=force)

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

        try:
            m_resp = await http.get("https://www.nseindia.com/api/merged-daily-reports?key=CM", prime_nse=True, timeout=10.0)
            if m_resp.status_code == 200:
                actl_name = m_resp.json().get("CM-UDIFF-BHAVCOPY-CSV", {}).get("fileActlName", "")
                if today.strftime("%Y%m%d") not in actl_name and today.strftime("%d%m%Y") not in actl_name:
                    logger.info("NSE EOD Bhavcopy for %s is not published yet on exchange (typical release is ~18:30 IST).", today)
        except Exception as exc:
            logger.debug("Publication check skipped: %s", exc)

        await eq.sync_range(nse_start, today, exchanges=("NSE",))
        await eq.sync_range(bse_start, today, exchanges=("BSE",))

        if include_fo:
            fo_nse_start = state.max_ok_date("fo_eod_nse") or default_start
            fo_bse_start = state.max_ok_date("fo_eod_bse") or default_start
            await fo.sync_range(fo_nse_start, today, exchanges=("NSE",))
            await fo.sync_range(fo_bse_start, today, exchanges=("BSE",))

        await indices.sync_daily_snapshots(idx_start, today)
        await indices.backfill_30y_bulk(start_date=default_start, end_date=today)
        indices.sync_vix_and_1d_rate_from_snapshots()
        await ff.sync_participant_oi(oi_start, today)
        await dd.sync_cash_fii_dii_daily()
        await dd.sync_bulk_and_block_deals()
        await dd.sync_insider_trading_pit(default_start, today)
        await dd.sync_concalls_and_presentations(default_start, today)
        await XBRLScraper(http, state, cfg).sync_recent_market_financials_nse()
        await ff.sync_amfi_daily()

    LakeDoctor(cfg).run_full_check_and_repair()


async def run_xbrl_sync(
    cfg: LakeConfig,
    symbols: list[str],
    bse_scrips: list[str],
    all_market: bool,
    limit: int | None,
    force: bool = False,
) -> None:
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    async with ResilientHTTPClient() as http:
        xbrl = XBRLScraper(http, state, cfg)
        dd = DealsAndDocumentsScraper(http, state, cfg)
        if all_market:
            synced_syms, synced_scrips = await xbrl.sync_all_market(max_companies=limit, force=force)
            if synced_scrips or synced_syms:
                await dd.sync_annual_reports_bse(synced_scrips[:200], symbols=synced_syms[:200])
            return
        for sym in symbols:
            try:
                await xbrl.sync_symbol_financials_nse(sym)
                await xbrl.sync_symbol_shareholding_nse(sym)
            except Exception as exc:
                logger.warning("[XBRL CLI] Failed symbol %s: %s", sym, exc)
        if bse_scrips:
            for idx_s, scrip in enumerate(bse_scrips):
                matched_sym = symbols[idx_s] if idx_s < len(symbols) else None
                try:
                    await xbrl.sync_bse_shareholding(scrip, symbol=matched_sym)
                except Exception as exc:
                    logger.warning("[XBRL CLI] Failed BSE scrip %s: %s", scrip, exc)
            await dd.sync_annual_reports_bse(bse_scrips, symbols=symbols)


async def run_pre2016_nse(cfg: LakeConfig, symbols: list[str], start_year: int, end_year: int) -> None:
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    async with ResilientHTTPClient() as http:
        eq = EquityEODScraper(http, state, cfg)
        for sym in symbols:
            await eq.backfill_nse_symbol_via_historical_or(sym, start_year=start_year, end_year=end_year)


async def run_seed_cloud(cfg: LakeConfig, start_year: int = 2008, end_year: int = 2015, limit: int | None = None) -> None:
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
    p_bf.add_argument("--amfi-history", action="store_true", help="Also backfill 30-day AMFI NAV windows since 2006")
    p_bf.add_argument("--force", action="store_true", help="Force redownload even if partitions/indices are already complete on disk")

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
    p_xb.add_argument("--force", action="store_true", help="Force re-sync even if symbols already completed in state.db")

    # 4. pre2016-nse
    p_p16 = sub.add_parser("pre2016-nse", help="Backfill 1995-2015 NSE Equity + Delivery via historicalOR endpoint")
    p_p16.add_argument("--symbols", default="RELIANCE,TCS,INFY,HDFCBANK,SBIN", help="Comma-separated NSE symbols")
    p_p16.add_argument("--start-year", type=int, default=1995)
    p_p16.add_argument("--end-year", type=int, default=2015)

    # 5. seed-cloud
    p_sc = sub.add_parser("seed-cloud", help="Cloud-native 2008-2015 NSE backfill via Upstox public V2 CDN")
    p_sc.add_argument("--start-year", type=int, default=2008)
    p_sc.add_argument("--end-year", type=int, default=2015)
    p_sc.add_argument("--limit", type=int, default=None)

    # 6. doctor
    sub.add_parser("doctor", help="Verify Parquet integrity, purge truncated days, and reconcile state.db")

    # 7. status
    sub.add_parser("status", help="Show watermark & row counts across all datasets")

    # 8. pull (Delta sync from GitHub Release to local PC)
    p_pull = sub.add_parser("pull", help="Fast SHA-256 delta sync from GitHub Release to local disk")
    p_pull.add_argument("--repo", required=True, help="GitHub repo 'username/repo-name'")
    p_pull.add_argument("--tag", default="datalake-latest", help="Release tag (default: datalake-latest)")
    p_pull.add_argument("--token", default=None, help="Optional GitHub PAT token for private repositories")
    p_pull.add_argument("--pattern", default=None, help="Optional substring filter to sync matching partitions only (e.g. '2026')")

    # 9. stage-release (Used by GitHub Actions before gh release upload)
    p_stg = sub.add_parser("stage-release", help="Optimize Parquet layout, build manifest.json, and stage assets")
    p_stg.add_argument("--staging-dir", default="/tmp/release_assets", help="Output folder for flat release assets")
    p_stg.add_argument("--prev-manifest", default=None, help="Optional previous manifest.json to stage only changed files")

    # 10. prune-release (Used by GitHub Actions after gh release upload)
    p_prn = sub.add_parser("prune-release", help="Prune retired Parquet assets and stale archive parts from GitHub Release")
    p_prn.add_argument("--tag", default="datalake-latest", help="Release tag (default: datalake-latest)")
    p_prn.add_argument("--staging-dir", default=None, help="Optional staging folder to inspect whether archive is split")

    args = parser.parse_args()
    cfg = LakeConfig(root_dir=Path(args.data_dir).resolve())

    if args.cmd == "backfill":
        start_dt = date.fromisoformat(args.start)
        end_dt = date.fromisoformat(args.end) if args.end else today_ist()
        asyncio.run(run_backfill(cfg, start_dt, end_dt, args.include_fo, args.amfi_history, force=args.force))
        print_status(cfg)
    elif args.cmd == "daily":
        asyncio.run(run_daily_update(cfg, args.lookback_days, include_fo=not args.no_fo))
        print_status(cfg)
    elif args.cmd == "xbrl":
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
        scrips = [s.strip() for s in args.bse_scrips.split(",") if s.strip()]
        asyncio.run(run_xbrl_sync(cfg, syms, scrips, args.all_market, args.limit, force=args.force))
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
    elif args.cmd == "pull":
        stats = asyncio.run(pull_from_github_release(args.repo, cfg.root_dir, tag=args.tag, token=args.token, pattern=args.pattern))
        print("Sync Summary:", stats)
    elif args.cmd == "stage-release":
        prev_m = Path(args.prev_manifest).resolve() if args.prev_manifest else None
        prepare_release_staging(cfg.root_dir, Path(args.staging_dir).resolve(), previous_manifest_path=prev_m)
    elif args.cmd == "prune-release":
        stg_d = Path(args.staging_dir).resolve() if args.staging_dir else None
        prune_retired_release_assets(cfg.root_dir, tag=args.tag, staging_dir=stg_d)


if __name__ == "__main__":
    main()
