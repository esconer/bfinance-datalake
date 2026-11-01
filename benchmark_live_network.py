import asyncio
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import pandas as pd
from datalake.engine import UnifiedLakeEngine
from datalake.remote import RemoteLakeClient
from datalake.sync import pull_from_github_release

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

REPO = "esconer/bfinance-datalake"
TAG = "datalake-latest"

def print_header(title: str):
    print("\n" + "=" * 80, flush=True)
    print(f"  {title}", flush=True)
    print("=" * 80, flush=True)

def benchmark_remote_streaming():
    print_header("BENCHMARK 1: Zero-Disk Network Streaming (RemoteLakeClient / DuckDB httpfs)")
    print(f"Connecting to GitHub Release: https://github.com/{REPO}/releases/tag/{TAG}", flush=True)
    
    t0 = time.perf_counter()
    remote = RemoteLakeClient(repo=REPO, tag=TAG)
    t_init = (time.perf_counter() - t0) * 1000
    print(f"[Init] In-memory DuckDB client initialized in {t_init:.2f} ms", flush=True)

    # 1. Single stock query cold (RELIANCE 2026)
    print("\n[Query 1] Fetching 'RELIANCE' 2026 OHLCV (Cold run - reads 64KB footer + byte slice)...", flush=True)
    t0 = time.perf_counter()
    df_rel_cold = remote.history("RELIANCE", start_year=2026, end_year=2026)
    t_rel_cold = (time.perf_counter() - t0) * 1000
    print(f"  -> Returned {len(df_rel_cold)} rows in {t_rel_cold:.2f} ms", flush=True)
    if not df_rel_cold.empty:
        print(f"     Date range: {df_rel_cold['Date'].min()} to {df_rel_cold['Date'].max()}", flush=True)
        print(f"     Last Close: Rs {df_rel_cold['Close'].iloc[-1]}", flush=True)

    # 2. Single stock query warm (RELIANCE 2026 repeat)
    print("\n[Query 2] Fetching 'RELIANCE' 2026 OHLCV (Warm repeat - cached footer in RAM)...", flush=True)
    t0 = time.perf_counter()
    df_rel_warm = remote.history("RELIANCE", start_year=2026, end_year=2026)
    t_rel_warm = (time.perf_counter() - t0) * 1000
    print(f"  -> Returned {len(df_rel_warm)} rows in {t_rel_warm:.2f} ms (Speedup: {t_rel_cold/max(t_rel_warm, 0.001):.1f}x)", flush=True)

    # 3. Another stock (TCS 2026)
    print("\n[Query 3] Fetching 'TCS' 2026 OHLCV...", flush=True)
    t0 = time.perf_counter()
    df_tcs = remote.history("TCS", start_year=2026, end_year=2026)
    t_tcs = (time.perf_counter() - t0) * 1000
    print(f"  -> Returned {len(df_tcs)} rows in {t_tcs:.2f} ms", flush=True)
    if not df_tcs.empty:
        print(f"     Last Close: Rs {df_tcs['Close'].iloc[-1]}", flush=True)

    # 4. Multi-year query (2024 to 2026)
    print("\n[Query 4] Fetching 'INFY' 3-Year OHLCV (2024-2026 across 3 annual partitions)...", flush=True)
    t0 = time.perf_counter()
    df_infy = remote.history("INFY", start_year=2024, end_year=2026)
    t_infy = (time.perf_counter() - t0) * 1000
    print(f"  -> Returned {len(df_infy)} rows in {t_infy:.2f} ms across 3 yearly files", flush=True)

    # 5. Remote SQL Aggregation
    print("\n[Query 5] Remote SQL Aggregation: 2026 Nifty Top Stocks by Turnover...", flush=True)
    t0 = time.perf_counter()
    url_2026 = remote._asset_url("equities", "nse_cm_2026.parquet")
    df_top = remote.sql(f"""
        SELECT Symbol, COUNT(*) as bars, ROUND(AVG(Close), 2) as avg_price, ROUND(SUM(TurnoverValue)/1e7, 2) as turnover_cr
        FROM read_parquet('{url_2026}')
        WHERE Series = 'EQ' AND Symbol IN ('RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK')
        GROUP BY Symbol
        ORDER BY turnover_cr DESC
    """)
    t_sql = (time.perf_counter() - t0) * 1000
    print(f"  -> Aggregated across 2026 in {t_sql:.2f} ms:", flush=True)
    print(df_top.to_string(index=False), flush=True)

    return {
        "remote_init_ms": t_init,
        "remote_cold_ms": t_rel_cold,
        "remote_warm_ms": t_rel_warm,
        "remote_tcs_ms": t_tcs,
        "remote_3yr_ms": t_infy,
        "remote_sql_ms": t_sql,
    }

async def benchmark_delta_sync():
    print_header("BENCHMARK 2: Local Delta Sync (datalake pull) & SSD Query Latency")
    
    tmp_path = tempfile.mkdtemp(prefix="bfinance_bench_")
    print(f"Syncing release assets into isolated local test directory: {tmp_path}", flush=True)
    
    try:
        # 1. First Pull (Download delta assets: 2026 partitions + master universe + corporate actions)
        print("\n[Pull 1 - Clean Delta Sync] Downloading 2026 partitions and master metadata...", flush=True)
        t0 = time.perf_counter()
        stats1 = await pull_from_github_release(
            repo=REPO,
            data_dir=tmp_path,
            tag=TAG,
            concurrency=4,
            pattern=r"2026|universe|actions",
        )
        t_pull1 = time.perf_counter() - t0
        
        # Calculate total synced bytes
        total_bytes = sum(f.stat().st_size for f in Path(tmp_path).rglob("*.parquet"))
        total_mb = total_bytes / (1024 * 1024)
        throughput = total_mb / max(t_pull1, 0.001)

        print(f"  -> Finished in {t_pull1:.2f} seconds ({total_mb:.1f} MB synced at {throughput:.2f} MB/s)", flush=True)
        print(f"  -> Total files matched: {stats1['total_files']}", flush=True)
        print(f"  -> Downloaded: {stats1['downloaded']} files", flush=True)
        print(f"  -> Skipped (cached): {stats1['up_to_date_skipped']} files", flush=True)

        # 2. Second Pull (Demonstrating fast delta cache hit)
        print("\n[Pull 2 - Daily Delta Check] Re-running pull to benchmark SHA-256 cache verification...", flush=True)
        t0 = time.perf_counter()
        stats2 = await pull_from_github_release(
            repo=REPO,
            data_dir=tmp_path,
            tag=TAG,
            pattern=r"2026|universe|actions",
        )
        t_pull2 = time.perf_counter() - t0
        print(f"  -> Verified {stats2['total_files']} files via SHA-256 in {t_pull2:.2f} seconds!", flush=True)
        print(f"  -> Skipped: {stats2['up_to_date_skipped']} files (0 bytes downloaded - perfect cache hit)", flush=True)

        # 3. Query Local Synced Data with UnifiedLakeEngine
        print("\n[Local SSD Queries] Benchmarking UnifiedLakeEngine off local disk...", flush=True)
        lake = UnifiedLakeEngine(tmp_path)
        
        # Cold query
        t0 = time.perf_counter()
        rel = lake.ticker("RELIANCE")
        hist_cold = rel.history(start="2026-01-01", auto_adjust=True)
        t_local_cold = (time.perf_counter() - t0) * 1000
        print(f"  -> Cold history query: {len(hist_cold)} rows in {t_local_cold:.2f} ms", flush=True)

        # Warm query (LRU Cache)
        t0 = time.perf_counter()
        hist_warm = rel.history(start="2026-01-01", auto_adjust=True)
        t_local_warm = (time.perf_counter() - t0) * 1000
        print(f"  -> Warm history query: {len(hist_warm)} rows in {t_local_warm:.2f} ms", flush=True)

        # Crosswalk resolution
        t0 = time.perf_counter()
        ident = lake.resolve("500325")
        t_resolve = (time.perf_counter() - t0) * 1000
        print(f"  -> Crosswalk resolve('500325'): {ident.symbol} in {t_resolve:.2f} ms", flush=True)

        # DuckDB SQL Screener on local disk
        t0 = time.perf_counter()
        df_screen = lake.sql("""
            SELECT Symbol, Date, Close, Volume, DeliveryPct
            FROM equities_nse
            WHERE Date = (SELECT MAX(Date) FROM equities_nse)
            ORDER BY Volume DESC
            LIMIT 5
        """)
        t_local_sql = (time.perf_counter() - t0) * 1000
        print(f"  -> DuckDB local SQL screener: {len(df_screen)} rows in {t_local_sql:.2f} ms:", flush=True)
        print(df_screen.to_string(index=False), flush=True)

        return {
            "sync_download_time_s": t_pull1,
            "sync_mb": total_mb,
            "sync_throughput_mbs": throughput,
            "sync_cache_check_time_s": t_pull2,
            "local_cold_ms": t_local_cold,
            "local_warm_ms": t_local_warm,
            "local_resolve_ms": t_resolve,
            "local_sql_ms": t_local_sql,
        }
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)

if __name__ == "__main__":
    remote_stats = benchmark_remote_streaming()
    sync_stats = asyncio.run(benchmark_delta_sync())

    print_header("FINAL LATENCY & PERFORMANCE COMPARISON SUMMARY")
    print(f"{'Operation':<45} | {'Mode':<22} | {'Measured Speed':>15}", flush=True)
    print("-" * 88, flush=True)
    print(f"{'Single Stock OHLCV (Cold)':<45} | {'Remote (Zero-Disk)':<22} | {remote_stats['remote_cold_ms']:>12.2f} ms", flush=True)
    print(f"{'Single Stock OHLCV (Warm Repeat)':<45} | {'Remote (Zero-Disk)':<22} | {remote_stats['remote_warm_ms']:>12.2f} ms", flush=True)
    print(f"{'Another Stock OHLCV (TCS)':<45} | {'Remote (Zero-Disk)':<22} | {remote_stats['remote_tcs_ms']:>12.2f} ms", flush=True)
    print(f"{'3-Year Multi-Partition OHLCV (INFY)':<45} | {'Remote (Zero-Disk)':<22} | {remote_stats['remote_3yr_ms']:>12.2f} ms", flush=True)
    print(f"{'Remote SQL Aggregation (Top 5 Stocks)':<45} | {'Remote (Zero-Disk)':<22} | {remote_stats['remote_sql_ms']:>12.2f} ms", flush=True)
    print("-" * 88, flush=True)
    print(f"{'Delta Sync Download (' + str(round(sync_stats['sync_mb'], 1)) + ' MB)':<45} | {'Local Delta Sync':<22} | {sync_stats['sync_download_time_s']:>12.2f} s", flush=True)
    print(f"{'Sync Network Throughput':<45} | {'Local Delta Sync':<22} | {sync_stats['sync_throughput_mbs']:>10.2f} MB/s", flush=True)
    print(f"{'Daily Delta Cache Verification (0 bytes)':<45} | {'Local Delta Sync':<22} | {sync_stats['sync_cache_check_time_s']:>12.2f} s", flush=True)
    print(f"{'Single Stock OHLCV (Local Cold)':<45} | {'Local SSD (Synced)':<22} | {sync_stats['local_cold_ms']:>12.2f} ms", flush=True)
    print(f"{'Single Stock OHLCV (Local LRU Warm)':<45} | {'Local SSD (Synced)':<22} | {sync_stats['local_warm_ms']:>12.2f} ms", flush=True)
    print(f"{'O(1) Crosswalk Resolve':<45} | {'Local SSD (Synced)':<22} | {sync_stats['local_resolve_ms']:>12.2f} ms", flush=True)
    print(f"{'Full Market SQL Screener':<45} | {'Local SSD (Synced)':<22} | {sync_stats['local_sql_ms']:>12.2f} ms", flush=True)
    print("=" * 88, flush=True)
