# T-04 — All-data-type benchmarks (stocks and ETFs)

**Worker:** T-04 (fresh context, SOLO Wave 2) · **Date:** 2026-11-23 · **Owned file:** `swarm_reports/T04_benchmarks.md` only.
**Worktree:** `/run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w2-bench` (`data` is a read-only symlink to the main repo snapshot; never written).

## 1. Environment and measurement protocol

| item | measured value |
|---|---|
| kernel | `Linux fedora 7.3.4-300.fc45.x86_64 #1 SMP PREEMPT_DYNAMIC Wed Nov 11 00:15:32 UTC 2026 x86_64 GNU/Linux` |
| CPU | AMD Ryzen 5 5600G with Radeon Graphics; 6 cores / 12 threads (2 HW threads per core), 1 socket; scaling 86% |
| RAM | 15 GiB total, ~6.7 GiB available at run time; 8 GiB swap (1.5 GiB used) |
| disk | `/dev/sda8` ext4-ish mount on `/run/media/esco/ed`: 494 G size, 478 G used, 16 G free, **97% used** |
| interpreter | `/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python` (Python 3.13.8) |
| `PYTHONPATH` | `src` (engine loaded from worktree `src/datalake/engine.py`) |
| libraries | pandas 3.1.0, pyarrow 26.0.0, duckdb 2.0.0 |
| lake handle | `UnifiedLakeEngine("./data")` → resolves to main-repo snapshot; manifest `generated_at=2026-11-23T00:42:11.568816+05:30`, 161 Parquet files, 2137.31 MB, contract 1.0.0 |
| harness | `/tmp/opencode/t04_bench.py` (outside repo); raw output `/tmp/opencode/t04_cold.jsonl`, `/tmp/opencode/t04_warm.jsonl` |
| timing | `time.perf_counter()`; peak RSS `resource.getrusage(RUSAGE_SELF).ru_maxrss / 1024` (MiB) |

**Bench lock (mandatory, serialised timing).** Window A (all cold processes): acquired `2026-11-22T20:07:20Z`, released `2026-11-22T20:11:03Z` (cold run `2026-11-22T20:07:20Z`→`2026-11-22T20:11:03Z`). Window B (warm process): acquired `2026-11-22T20:11:10Z`, released `2026-11-22T20:12:07Z` (warm run `2026-11-22T20:11:10Z`→`2026-11-22T20:12:07Z`). Both windows used `bench_lock.sh acquire "$LOCK" 300 600` with a shell `trap` that always releases; no stale lock remains. Two windows were used so each stayed under the 300 s lock TTL.

**Definitions.**
- **cold_ms** = elapsed time of the *first* invocation of a given data type in a **fresh process** (one process per cold measurement, 149 processes). The `UnifiedLakeEngine` is constructed in that process immediately before the timed call; construction itself is untimed here and measured separately in §8, but all *lazy* work triggered by the call (crosswalk load, DuckDB view registration, Parquet page loads) is included. Page cache: OS `buff/cache` was 6.9 GiB against a 2.14 GiB lake, and wave-1 audits plus verifiers had already read the snapshot, so **the filesystem cache was warm**. Dropping caches is not permitted on this shared host, so these are *process-cold / FS-warm* numbers, not true disk-cold numbers.
- **warm_best_ms / warm_median_ms** = best and median of 5 in-process repeats after one untimed priming call, reusing a single engine (its raw-equity LRU, crosswalk dicts and DuckDB connection persist). All 5 repeats are shown in the raw JSONL.
- **rows** = `len()` for DataFrames/DictIndices, number of keys for dict payloads (documented per type in T-01), 1 for identity objects; for `download()` it is the sum of rows across returned symbols. Cold and warm row counts were cross-checked and **matched on all 139 master specs** (0 mismatches).
- **rows_per_sec** = `rows / (warm_best_ms / 1000)` (steady-state throughput).
- **peak_rss_mb** (master table) comes from the **cold, single-measurement process**, so it is attributable to that one data type. The warm process reports one process-wide cumulative peak of **2,059.05 MiB** (a single `resource.getrusage` high-water mark cannot be attributed per call).

**Measurement caveat (external swarm).** A separate, unrelated swarm (worktrees `r2p2-a/b/c/t`) is running concurrently in the same repository and is landing external commits on main. The bench lock serialises benchmarks only **within this run**; it cannot prevent that swarm's CPU/IO activity or main-branch commits from influencing wall-clock numbers. Row counts come from the read-only data symlink, which is not modified by those commits. Treat sub-10% timing differences as noise.

**Subjects.** 3 stocks (RELIANCE, TCS, INFY) and 3 ETFs (NIFTYBEES, GOLDBEES, BANKBEES) — all drawn from T-02/T-03. Market-wide paths use subject `MARKET`. 30 T-01 categories are covered by 139 data-type×subject master rows plus 10 auxiliary rows.

## 2. Falsification / BEFORE baseline

Hypothesis under test: *"all query paths complete in under 100 ms warm and under 2 s cold."*
BEFORE: only a single-symbol RELIANCE history benchmark existed (~17 ms warm, ~1.7 s end-to-end CSV).
Result: **FALSE on both bounds.** Measured worst warm path is 790.509 ms (`mutual_funds()`) and worst cold path is 3974.2 ms (`continuous_futures("RELIANCE")`); 10/139 paths exceed 100 ms warm and 3/139 exceed 2 s cold (15/139 exceed 1 s cold). Full evidence in §3, §9 and §10.

## 3. Master table — every T-01 data type × subject

| data_type | subject | cold_ms | warm_best_ms | warm_median_ms | rows | rows_per_sec | peak_rss_mb |
|---|---|---:|---:|---:|---:|---:|---:|
| 01 equity: history NSE | RELIANCE | 722.13 | 17.739 | 18.002 | 7,885 | 444,501 | 243.04 |
| 01 equity: history NSE | TCS | 1,177.67 | 23.339 | 23.416 | 5,491 | 235,271 | 232.36 |
| 01 equity: history NSE | INFY | 1,164.23 | 20.472 | 20.856 | 7,845 | 383,206 | 235.38 |
| 01 equity: history NSE | NIFTYBEES | 920.51 | 6.674 | 6.929 | 6,150 | 921,486 | 241.86 |
| 01 equity: history NSE | GOLDBEES | 868.72 | 6.852 | 8.986 | 4,855 | 708,552 | 237.64 |
| 01 equity: history NSE | BANKBEES | 892.25 | 7.304 | 7.858 | 5,202 | 712,212 | 239.87 |
| 02 equity: history BSE | RELIANCE | 1,525.51 | 19.834 | 20.080 | 5,089 | 256,580 | 484.09 |
| 02 equity: history BSE | TCS | 2,120.97 | 31.323 | 35.683 | 5,089 | 162,468 | 459.46 |
| 02 equity: history BSE | INFY | 1,538.44 | 22.623 | 23.097 | 5,089 | 224,948 | 410.22 |
| 02 equity: history BSE | NIFTYBEES | 1,318.74 | 11.145 | 11.454 | 4,224 | 379,004 | 436.43 |
| 02 equity: history BSE | GOLDBEES | 790.35 | 10.889 | 11.193 | 4,241 | 389,476 | 219.17 |
| 02 equity: history BSE | BANKBEES | 1,251.11 | 11.158 | 11.307 | 3,887 | 348,360 | 466.23 |
| 05 derivatives: option_chain | RELIANCE | 466.22 | 43.503 | 45.572 | 234 | 5,379 | 267.89 |
| 05 derivatives: option_chain | TCS | 400.45 | 49.650 | 71.011 | 196 | 3,948 | 263.92 |
| 05 derivatives: option_chain | INFY | 505.63 | 46.710 | 58.118 | 185 | 3,961 | 269.92 |
| 05 derivatives: option_chain | NIFTYBEES | 372.89 | 20.457 | 20.706 | 0 | 0.0 | 259.86 |
| 05 derivatives: option_chain | GOLDBEES | 371.38 | 17.633 | 20.376 | 0 | 0.0 | 253.84 |
| 05 derivatives: option_chain | BANKBEES | 421.95 | 16.618 | 17.471 | 0 | 0.0 | 253.70 |
| 06 derivatives: continuous_futures | RELIANCE | 3,974.23 | 388.967 | 424.584 | 6,144 | 15,796 | 308.12 |
| 06 derivatives: continuous_futures | TCS | 1,747.97 | 404.752 | 421.804 | 5,453 | 13,472 | 296.76 |
| 06 derivatives: continuous_futures | INFY | 2,580.67 | 369.485 | 393.409 | 6,144 | 16,629 | 292.02 |
| 06 derivatives: continuous_futures | NIFTYBEES | 1,014.93 | 355.302 | 357.662 | 0 | 0.0 | 299.88 |
| 06 derivatives: continuous_futures | GOLDBEES | 1,261.73 | 327.734 | 331.061 | 0 | 0.0 | 286.26 |
| 06 derivatives: continuous_futures | BANKBEES | 1,813.57 | 308.973 | 324.906 | 0 | 0.0 | 276.32 |
| 11 fundamentals: XBRL | RELIANCE | 414.21 | 9.468 | 10.585 | 10 | 1,056 | 261.66 |
| 11 fundamentals: XBRL | TCS | 418.29 | 8.556 | 9.311 | 10 | 1,169 | 263.76 |
| 11 fundamentals: XBRL | INFY | 360.15 | 8.898 | 9.708 | 10 | 1,124 | 259.78 |
| 11 fundamentals: XBRL | NIFTYBEES | 350.97 | 5.075 | 5.657 | 0 | 0.0 | 265.18 |
| 11 fundamentals: XBRL | GOLDBEES | 349.39 | 4.572 | 4.947 | 0 | 0.0 | 265.02 |
| 11 fundamentals: XBRL | BANKBEES | 347.17 | 5.347 | 6.150 | 0 | 0.0 | 275.48 |
| 12 shareholding: summary | RELIANCE | 367.63 | 14.121 | 15.979 | 103 | 7,294 | 250.54 |
| 12 shareholding: summary | TCS | 367.47 | 14.097 | 14.432 | 89 | 6,313 | 254.86 |
| 12 shareholding: summary | INFY | 371.68 | 15.138 | 17.521 | 103 | 6,804 | 264.67 |
| 12 shareholding: summary | NIFTYBEES | 432.45 | 16.856 | 17.768 | 0 | 0.0 | 281.73 |
| 12 shareholding: summary | GOLDBEES | 379.65 | 16.464 | 17.209 | 0 | 0.0 | 277.46 |
| 12 shareholding: summary | BANKBEES | 468.25 | 17.778 | 18.682 | 0 | 0.0 | 279.76 |
| 13 shareholding: named_holders | RELIANCE | 384.61 | 4.646 | 4.728 | 734 | 157,985 | 256.91 |
| 13 shareholding: named_holders | TCS | 364.39 | 4.943 | 5.350 | 3,137 | 634,635 | 254.68 |
| 13 shareholding: named_holders | INFY | 391.27 | 4.666 | 5.939 | 488 | 104,586 | 250.82 |
| 13 shareholding: named_holders | NIFTYBEES | 368.99 | 4.527 | 4.897 | 0 | 0.0 | 257.04 |
| 13 shareholding: named_holders | GOLDBEES | 384.55 | 5.305 | 7.030 | 0 | 0.0 | 252.93 |
| 13 shareholding: named_holders | BANKBEES | 397.37 | 5.169 | 5.303 | 0 | 0.0 | 254.73 |
| 14 shareholding: trends | RELIANCE | 392.02 | 19.775 | 20.491 | 4 | 202.3 | 261.05 |
| 14 shareholding: trends | TCS | 421.07 | 19.290 | 19.748 | 4 | 207.4 | 257.08 |
| 14 shareholding: trends | INFY | 440.78 | 20.471 | 21.159 | 4 | 195.4 | 261.32 |
| 14 shareholding: trends | NIFTYBEES | 380.06 | 16.403 | 16.757 | 3 | 182.9 | 287.77 |
| 14 shareholding: trends | GOLDBEES | 376.19 | 14.831 | 15.224 | 3 | 202.3 | 297.69 |
| 14 shareholding: trends | BANKBEES | 380.60 | 16.788 | 17.099 | 3 | 178.7 | 301.79 |
| 15 actions: corporate_actions | RELIANCE | 444.68 | 7.160 | 7.382 | 25 | 3,492 | 296.68 |
| 15 actions: corporate_actions | TCS | 412.63 | 7.238 | 7.334 | 81 | 11,191 | 284.75 |
| 15 actions: corporate_actions | INFY | 410.88 | 7.078 | 7.211 | 42 | 5,934 | 286.27 |
| 15 actions: corporate_actions | NIFTYBEES | 626.17 | 11.454 | 11.467 | 0 | 0.0 | 278.62 |
| 15 actions: corporate_actions | GOLDBEES | 514.23 | 3.344 | 3.484 | 0 | 0.0 | 246.29 |
| 15 actions: corporate_actions | BANKBEES | 477.66 | 11.181 | 11.373 | 0 | 0.0 | 238.95 |
| 16 deals: bulk/block (symbol) | RELIANCE | 411.94 | 3.627 | 4.146 | 341 | 94,017 | 211.77 |
| 16 deals: bulk/block (symbol) | TCS | 461.57 | 4.362 | 4.514 | 54 | 12,380 | 202.84 |
| 16 deals: bulk/block (symbol) | INFY | 411.65 | 4.079 | 4.529 | 57 | 13,974 | 203.35 |
| 16 deals: bulk/block (symbol) | NIFTYBEES | 461.41 | 3.542 | 4.396 | 33 | 9,317 | 213.49 |
| 16 deals: bulk/block (symbol) | GOLDBEES | 504.83 | 4.002 | 4.389 | 4 | 999.5 | 217.58 |
| 16 deals: bulk/block (symbol) | BANKBEES | 415.98 | 3.650 | 4.331 | 0 | 0.0 | 256.73 |
| 17 deals: insider_trades (symbol) | RELIANCE | 356.80 | 8.133 | 9.098 | 1,162 | 142,875 | 279.61 |
| 17 deals: insider_trades (symbol) | TCS | 359.41 | 9.044 | 9.292 | 130 | 14,374 | 277.44 |
| 17 deals: insider_trades (symbol) | INFY | 347.43 | 7.292 | 7.432 | 3,132 | 429,512 | 273.44 |
| 17 deals: insider_trades (symbol) | NIFTYBEES | 362.42 | 7.168 | 7.609 | 0 | 0.0 | 269.81 |
| 17 deals: insider_trades (symbol) | GOLDBEES | 347.52 | 6.386 | 6.750 | 0 | 0.0 | 267.34 |
| 17 deals: insider_trades (symbol) | BANKBEES | 350.50 | 6.448 | 6.950 | 0 | 0.0 | 275.09 |
| 19 documents: annual_reports | RELIANCE | 359.71 | 4.079 | 4.548 | 34 | 8,335 | 261.55 |
| 19 documents: annual_reports | TCS | 348.92 | 4.537 | 4.731 | 35 | 7,714 | 269.83 |
| 19 documents: annual_reports | INFY | 352.30 | 4.075 | 4.258 | 35 | 8,589 | 265.70 |
| 19 documents: annual_reports | NIFTYBEES | 353.36 | 3.881 | 4.222 | 0 | 0.0 | 264.32 |
| 19 documents: annual_reports | GOLDBEES | 370.83 | 3.200 | 3.795 | 0 | 0.0 | 269.78 |
| 19 documents: annual_reports | BANKBEES | 362.25 | 4.077 | 4.944 | 0 | 0.0 | 263.52 |
| 20 documents: concalls | RELIANCE | 351.51 | 3.957 | 4.387 | 21 | 5,307 | 272.46 |
| 20 documents: concalls | TCS | 357.83 | 4.156 | 4.379 | 11 | 2,647 | 270.46 |
| 20 documents: concalls | INFY | 369.38 | 4.202 | 4.554 | 3 | 713.9 | 262.50 |
| 20 documents: concalls | NIFTYBEES | 362.82 | 1.895 | 2.004 | 0 | 0.0 | 256.18 |
| 20 documents: concalls | GOLDBEES | 393.64 | 8.832 | 9.244 | 0 | 0.0 | 306.62 |
| 20 documents: concalls | BANKBEES | 359.96 | 2.235 | 2.294 | 0 | 0.0 | 254.18 |
| 25 resolution: resolve | RELIANCE | 356.43 | 0.001 | 0.001 | 1 | 1,000,000 | 253.42 |
| 25 resolution: resolve | TCS | 342.63 | 0.001 | 0.001 | 1 | 1,000,000 | 243.22 |
| 25 resolution: resolve | INFY | 335.01 | 0.001 | 0.001 | 1 | 1,000,000 | 255.64 |
| 25 resolution: resolve | NIFTYBEES | 347.85 | 0.001 | 0.001 | 1 | 1,000,000 | 251.68 |
| 25 resolution: resolve | GOLDBEES | 371.26 | 0.001 | 0.001 | 1 | 1,000,000 | 249.84 |
| 25 resolution: resolve | BANKBEES | 341.44 | 0.001 | 0.001 | 1 | 1,000,000 | 251.81 |
| 26 screener: filtered row | RELIANCE | 203.95 | 6.231 | 8.732 | 1 | 160.5 | 264.29 |
| 26 screener: filtered row | TCS | 200.34 | 6.295 | 6.757 | 1 | 158.9 | 263.19 |
| 26 screener: filtered row | INFY | 194.63 | 5.653 | 5.784 | 1 | 176.9 | 264.07 |
| 26 screener: filtered row | NIFTYBEES | 197.99 | 6.888 | 7.600 | 1 | 145.2 | 264.93 |
| 26 screener: filtered row | GOLDBEES | 196.09 | 7.443 | 8.646 | 1 | 134.4 | 264.88 |
| 26 screener: filtered row | BANKBEES | 212.59 | 7.572 | 7.592 | 1 | 132.1 | 263.51 |
| 27 derived: custom_ratios | RELIANCE | 699.50 | 20.236 | 20.855 | 33 | 1,631 | 381.40 |
| 27 derived: custom_ratios | TCS | 663.05 | 20.968 | 21.331 | 33 | 1,574 | 377.80 |
| 27 derived: custom_ratios | INFY | 637.18 | 21.248 | 22.564 | 33 | 1,553 | 385.85 |
| 27 derived: custom_ratios | NIFTYBEES | 595.45 | 13.075 | 13.359 | 33 | 2,524 | 380.60 |
| 27 derived: custom_ratios | GOLDBEES | 601.86 | 13.217 | 15.263 | 33 | 2,497 | 373.15 |
| 27 derived: custom_ratios | BANKBEES | 696.19 | 13.395 | 14.047 | 33 | 2,464 | 368.47 |
| 27 derived: research_profile | RELIANCE | 608.50 | 52.837 | 64.020 | 29 | 548.9 | 406.45 |
| 27 derived: research_profile | TCS | 606.11 | 55.281 | 63.272 | 29 | 524.6 | 408.55 |
| 27 derived: research_profile | INFY | 612.02 | 54.583 | 55.548 | 29 | 531.3 | 409.63 |
| 27 derived: research_profile | NIFTYBEES | 575.04 | 41.514 | 47.873 | 29 | 698.6 | 431.75 |
| 27 derived: research_profile | GOLDBEES | 622.31 | 46.915 | 49.025 | 29 | 618.1 | 460.72 |
| 27 derived: research_profile | BANKBEES | 584.80 | 43.528 | 49.677 | 29 | 666.2 | 425.16 |
| 27 derived: dossier | RELIANCE | 437.70 | 59.327 | 68.365 | 8 | 134.8 | 342.03 |
| 27 derived: dossier | TCS | 471.47 | 55.607 | 62.761 | 8 | 143.9 | 351.80 |
| 27 derived: dossier | INFY | 478.76 | 60.645 | 62.006 | 8 | 131.9 | 350.79 |
| 27 derived: dossier | NIFTYBEES | 531.59 | 61.192 | 67.487 | 1 | 16.3 | 349.56 |
| 27 derived: dossier | GOLDBEES | 521.28 | 57.582 | 58.742 | 1 | 17.4 | 355.46 |
| 27 derived: dossier | BANKBEES | 513.79 | 57.235 | 63.190 | 0 | 0.0 | 349.40 |
| 28 calendar: board meetings (symbol) | RELIANCE | 387.24 | 2.848 | 3.134 | 0 | 0.0 | 248.85 |
| 28 calendar: board meetings (symbol) | TCS | 380.00 | 2.606 | 3.524 | 0 | 0.0 | 246.42 |
| 28 calendar: board meetings (symbol) | INFY | 391.97 | 2.787 | 3.264 | 0 | 0.0 | 256.53 |
| 28 calendar: board meetings (symbol) | NIFTYBEES | 362.79 | 3.158 | 3.326 | 0 | 0.0 | 254.81 |
| 28 calendar: board meetings (symbol) | GOLDBEES | 348.19 | 2.836 | 3.303 | 0 | 0.0 | 255.12 |
| 28 calendar: board meetings (symbol) | BANKBEES | 363.40 | 2.690 | 2.976 | 0 | 0.0 | 256.92 |
| 03 equity: download(3) | MARKET | 939.03 | 118.452 | 122.713 | 5,106 | 43,106 | 306.62 |
| 04 equity: panel(2) | MARKET | 1,462.82 | 256.128 | 267.893 | 7,885 | 30,785 | 329.49 |
| 07 derivatives: fo_ban_list | MARKET | 1.68 | 1.019 | 1.180 | 1 | 981.4 | 145.25 |
| 08 indices: index_history | MARKET | 97.84 | 13.112 | 14.172 | 7,921 | 604,103 | 230.88 |
| 09 indices: all_indices | MARKET | 21.67 | 3.196 | 3.443 | 139 | 43,492 | 162.96 |
| 10 indices: index_constituents | MARKET | 62.06 | 24.248 | 29.193 | 50 | 2,062 | 287.52 |
| 18 deals: short_selling | MARKET | 0.39 | 0.163 | 0.167 | 0 | 0.0 | 147.45 |
| 21 mutual_funds: catalog | MARKET | 1,873.59 | 790.509 | 1,260.994 | 9,157 | 11,584 | 1,393.77 |
| 21 mutual_funds: single NAV | MARKET | 274.88 | 116.217 | 124.568 | 3,215 | 27,664 | 248.61 |
| 22 flows: fii_dii_flows | MARKET | 4.34 | 1.969 | 2.095 | 30 | 15,236 | 150.71 |
| 23 flows: participant_oi | MARKET | 127.84 | 39.382 | 39.670 | 14,656 | 372,150 | 187.91 |
| 24 universe: etfs | MARKET | 2.95 | 1.344 | 1.432 | 361 | 268,601 | 152.00 |
| 24 universe: market_universe | MARKET | 3.04 | 1.322 | 1.457 | 2,186 | 1,653,555 | 156.04 |
| 24 universe: sme_stocks | MARKET | 2.24 | 1.210 | 1.269 | 2 | 1,653 | 147.58 |
| 24 universe: delisted_stocks | MARKET | 2.92 | 1.342 | 1.468 | 328 | 244,411 | 151.55 |
| 24 universe: symbol_changes | MARKET | 1.97 | 1.533 | 1.664 | 1,082 | 705,806 | 149.82 |
| 26 screener: full snapshot | MARKET | 210.94 | 14.136 | 14.749 | 9,323 | 659,522 | 269.50 |
| 26 screener: run_screen | MARKET | 263.95 | 29.076 | 30.308 | 68 | 2,339 | 265.98 |
| 27 derived: factor_returns | MARKET | 156.83 | 36.506 | 42.528 | 5,428 | 148,688 | 267.10 |
| 27 derived: liquidity_limits | MARKET | 463.91 | 13.853 | 13.888 | 7 | 505.3 | 299.65 |
| 27 derived: delivery_anomalies | MARKET | 475.02 | 12.306 | 12.477 | 9 | 731.4 | 286.03 |
| 27 derived: ai_context_india | MARKET | 542.97 | 81.189 | 84.682 | 8 | 98.5 | 293.80 |
| 28 calendar: trading_days | MARKET | 28.76 | 13.588 | 15.164 | 7,724 | 568,443 | 231.32 |
| 29 health: health | MARKET | 39.24 | 38.947 | 39.378 | 7 | 179.7 | 152.19 |
| 30 sql: count equities_nse | MARKET | 206.99 | 31.108 | 32.063 | 1 | 32.1 | 262.97 |

## 4. Multi-symbol download scaling (`lake.download([...], start="2020-01-01")`)

| symbols | cold_ms | warm_best_ms | warm_median_ms | rows | symbols_returned | rows_per_sec | peak_rss_mb (cold) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3 | 502.61 | 115.550 | 118.433 | 5,106 | 3 | 44,189 | 308.55 |
| 10 | 1,506.96 | 278.235 | 287.408 | 17,020 | 10 | 61,171 | 349.56 |
| 30 | 6,440.23 | 730.756 | 765.077 | 51,060 | 30 | 69,873 | 382.59 |

Scaling is measured with a fixed date window so only the symbol count varies. `download()` reads each yearly NSE partition once with a single `Symbol IN (...)` predicate, then adjusts each group; the 3→30 symbol increase moves warm time from 115.550 ms to 730.756 ms (6.32×) for a 10.00× row increase, and cold time from 502.61 ms to 6,440.23 ms (12.81×).

## 5. SQL (`lake.sql`)

| query | cold_ms | warm_best_ms | warm_median_ms | rows | result value | peak_rss_mb (cold) |
|---|---:|---:|---:|---:|---|---:|
| `SELECT count(*) AS n FROM equities_nse` (T-01 #30) | 206.99 | 31.108 | 32.063 | 1 | 1 result row containing scalar n = 12,026,057 | 262.97 |
| `SELECT count(*) AS n FROM equities_nse (dedicated)` | 216.69 | 31.367 | 31.585 | 1 | n = 12,026,057 | 263.10 |
| `SELECT Symbol, COUNT(*) n FROM equities_nse WHERE Symbol IN (3) GROUP BY Symbol` | 249.49 | 35.148 | 39.516 | 3 | {'INFY': 3813, 'RELIANCE': 10786, 'TCS': 5507} | 269.84 |

DuckDB view registration (30 `CREATE VIEW ... read_parquet` statements, one per entry of the `views` dict in `engine.py::sql`) happens once per engine on the first `sql()` call; that is why cold ≫ warm even though the full scan itself is 31.367 ms warm.

## 6. Dossier (`lake.dossier`)

| subject | cold_ms | warm_best_ms | warm_median_ms | non-empty tables | peak_rss_mb (cold) |
|---|---:|---:|---:|---:|---:|
| RELIANCE | 437.70 | 59.327 | 68.365 | 8 | 342.03 |
| TCS | 471.47 | 55.607 | 62.761 | 8 | 351.80 |
| INFY | 478.76 | 60.645 | 62.006 | 8 | 350.79 |
| NIFTYBEES | 531.59 | 61.192 | 67.487 | 1 | 349.56 |
| GOLDBEES | 521.28 | 57.582 | 58.742 | 1 | 355.46 |
| BANKBEES | 513.79 | 57.235 | 63.190 | 0 | 349.40 |
| RELIANCE (dedicated) | 425.02 | 55.982 | 56.986 | 8 | 349.88 |
| NIFTYBEES (dedicated) | 443.70 | 55.023 | 56.089 | 1 | 368.64 |

The ETF dossier returns fewer keys because 8 of the 9 non-price feeds have zero ETF rows (T-03 §4); it is not faster because the same source Parquet files are still opened and filtered.

## 7. Full-history `DataFrame.to_csv()` (RELIANCE NSE, auto_adjust=False)

| operation | cold_ms | warm_best_ms | warm_median_ms | rows | bytes | peak_rss_mb (cold) |
|---|---:|---:|---:|---:|---:|---:|
| `history(...).to_csv(io.StringIO(), index=False)` | 706.56 | 79.918 | 81.975 | 7,885 | 1,110,052 | 308.55 |

Warm serialisation throughput = 13.25 MiB/s of CSV text; the cold number includes reading all 32 yearly NSE partitions and applying corporate-action adjustments before serialisation. BEFORE baseline was ~1.7 s end-to-end CSV; measured cold is 706.56 ms (2.41× faster than that rough baseline).

## 8. Engine construction, resolve and health

| operation | cold_ms | warm_best_ms | warm_median_ms | rows/result | peak_rss_mb (cold) |
|---|---:|---:|---:|---|---:|
| `UnifiedLakeEngine('./data')` construction | 32.64 | 31.856 | 32.353 | — | 149.99 |
| `lake.resolve('RELIANCE')` (dedicated) | 340.20 | 0.001 | 0.001 | 1 identity | 247.61 |
| `25 resolution: resolve` [RELIANCE] | 356.43 | 0.001 | 0.001 | 1 identity | 253.42 |
| `25 resolution: resolve` [TCS] | 342.63 | 0.001 | 0.001 | 1 identity | 243.22 |
| `25 resolution: resolve` [INFY] | 335.01 | 0.001 | 0.001 | 1 identity | 255.64 |
| `25 resolution: resolve` [NIFTYBEES] | 347.85 | 0.001 | 0.001 | 1 identity | 251.68 |
| `25 resolution: resolve` [GOLDBEES] | 371.26 | 0.001 | 0.001 | 1 identity | 249.84 |
| `25 resolution: resolve` [BANKBEES] | 341.44 | 0.001 | 0.001 | 1 identity | 251.81 |
| `29 health: health` [MARKET] | 39.24 | 38.947 | 39.378 | 7 keys (ready=True) | 152.19 |

Construction (`engine.py` lines 54–64) creates `OHLCVAdjuster(root)` and then eagerly evaluates `self.adjuster.resolver`; that property (`adjuster.py` lines 23–27) calls `SymbolRenameResolver.from_lake()`, which reads `universe/nse_symbol_changes.parquet` (1,082 rows) — the ~32 ms construction cost. Corporate-action Parquet files are **not** loaded at construction; they load on first adjustment. The first `resolve()` then lazily runs `_ensure_crosswalk()`, reading six universe files (four NSE masters incl. `nse_etf_master`, `bse_scrip_master`, `nifty_constituents_pit`), which is why cold resolve (~0.3 s) is orders of magnitude above warm dict lookup (~0.001 ms).

## 9. Fastest / slowest paths and evidenced root causes

**Fastest warm (best of 5):**

| data type | subject | warm_best_ms | cold_ms |
|---|---|---:|---:|
| 25 resolution: resolve | INFY | 0.001 | 335.01 |
| 25 resolution: resolve | BANKBEES | 0.001 | 341.44 |
| 25 resolution: resolve | TCS | 0.001 | 342.63 |
| 25 resolution: resolve | NIFTYBEES | 0.001 | 347.85 |
| 25 resolution: resolve | RELIANCE | 0.001 | 356.43 |
| 25 resolution: resolve | GOLDBEES | 0.001 | 371.26 |

**Slowest warm (best of 5):**

| data type | subject | warm_best_ms | cold_ms | rows |
|---|---|---:|---:|---:|
| 21 mutual_funds: catalog | MARKET | 790.509 | 1,873.59 | 9,157 |
| 06 derivatives: continuous_futures | TCS | 404.752 | 1,747.97 | 5,453 |
| 06 derivatives: continuous_futures | RELIANCE | 388.967 | 3,974.23 | 6,144 |
| 06 derivatives: continuous_futures | INFY | 369.485 | 2,580.67 | 6,144 |
| 06 derivatives: continuous_futures | NIFTYBEES | 355.302 | 1,014.93 | 0 |
| 06 derivatives: continuous_futures | GOLDBEES | 327.734 | 1,261.73 | 0 |
| 06 derivatives: continuous_futures | BANKBEES | 308.973 | 1,813.57 | 0 |
| 04 equity: panel(2) | MARKET | 256.128 | 1,462.82 | 7,885 |

**Slowest cold (fresh process):**

| data type | subject | cold_ms | warm_best_ms |
|---|---|---:|---:|
| 06 derivatives: continuous_futures | RELIANCE | 3,974.23 | 388.967 |
| 06 derivatives: continuous_futures | INFY | 2,580.67 | 369.485 |
| 02 equity: history BSE | TCS | 2,120.97 | 31.323 |
| 21 mutual_funds: catalog | MARKET | 1,873.59 | 790.509 |
| 06 derivatives: continuous_futures | BANKBEES | 1,813.57 | 308.973 |
| 06 derivatives: continuous_futures | TCS | 1,747.97 | 404.752 |
| 02 equity: history BSE | INFY | 1,538.44 | 22.623 |
| 02 equity: history BSE | RELIANCE | 1,525.51 | 19.834 |

Aggregates over 139 master rows: warm_best median **9.468 ms**, mean 39.021 ms, range 0.001–790.509 ms; cold median **387.24 ms**, mean 531.01 ms, range 0.39–3,974.23 ms.

**Evidenced root causes of slowness (from engine code + the numbers above, not estimates):**
- `continuous_futures()` (slowest cold, 3974.2 ms RELIANCE; 2580.7 ms INFY; warm 309–405 ms) loops `for yr in range(2000, current_year+1)` and issues a filtered Parquet read on **27 yearly `nse_fo_*.parquet` files** (45.4 M F&O rows across them per T-03 §5), then concatenates and stitches contracts. ETF calls are slower-than-empty (309–355 ms) because they still scan the same 27 files and filter them down to 0 rows.
- `mutual_funds()` (slowest warm, 790.509 ms; cold 1873.6 ms) reads the whole latest `amfi_nav_YYYY.parquet` and runs `sort_values('Date').drop_duplicates('SchemeCode')` across all historical dated rows before returning the 9,157-scheme snapshot — a full-file sort, not the 161-file lake scan.
- `history_bse()` (cold 1251–2121 ms) reads 21 yearly `bse_cm_*` partitions (2006→2026) with an `InstrumentId` filter; each cold read pulls byte ranges that were not in the engine's LRU yet. `panel(2)` (cold 1462.8 ms) is `download()` over two symbols and inherits the same 32-partition scan.
- `sql()` cold (216.7–258.7 ms) is dominated by registering 30 DuckDB views; warm SQL full scan of 12,026,057 rows is only ~31 ms thanks to DuckDB's Parquet pushdown.
- `resolve()` cold (~335–371 ms) loads the crosswalk masters on first use; warm is a dict hit (~0.001 ms). `csv_full` cold (706.6 ms) is the same 32-partition history read + CSV text encoding.

## 10. Verdict against the 100 ms warm / 2 s cold hypothesis

| bound | threshold | measured worst | paths over bound | verdict |
|---|---:|---:|---:|---|
| warm (best of 5) | 100 ms | 790.509 ms (`mutual_funds()`) | 10 / 139 | **FAIL** |
| cold (fresh process, FS-warm) | 2000 ms | 3,974.23 ms (`continuous_futures('RELIANCE')`) | 3 / 139 | **FAIL** |

The hypothesis is rejected on both counts. Most paths are fast (median warm 9.468 ms), and cold/warm row counts matched on all 139 specs, but three cold paths (RELIANCE/INFY continuous futures, TCS BSE history) and exactly ten warm paths — all 6 `continuous_futures` calls, plus `mutual_funds` (790.5 ms), `panel(2)` (256.1 ms), `download(3)` (118.5 ms) and `mutual_fund` (116.2 ms) — breach the stated bounds. `ai_context_india` (81.2 ms) and the NIFTYBEES dossier (61.2 ms) are close but under 100 ms.

## 11. Reproduction

```bash
cd /run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w2-bench
export PYTHONPATH=src
PY=/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python
LOCK=/run/media/esco/ed/dev/code/bfinance-datalake/.opencode/swarm-runs/20261123-alltypes-audit/.bench.lock
BL=/home/esco/.config/opencode/skills/swarm-orchestrator/scripts/bench_lock.sh
bash $BL acquire "$LOCK" 300 600   # trap-release on EXIT
for i in $(seq 0 148); do $PY /tmp/opencode/t04_bench.py cold $i >> /tmp/opencode/t04_cold.jsonl; done
$PY /tmp/opencode/t04_bench.py warm > /tmp/opencode/t04_warm.jsonl
bash $BL release "$LOCK"
```

One-line spot checks (no harness):
```bash
$PY -c 'from datalake import UnifiedLakeEngine as L; l=L("./data"); d=l.history("RELIANCE",start="1995-01-01",exchange="NSE",auto_adjust=False); print(len(d))'  # 7885
$PY -c 'from datalake import UnifiedLakeEngine as L; l=L("./data"); print(l.sql("SELECT count(*) AS n FROM equities_nse"))'  # 12026057
$PY -c 'from datalake import UnifiedLakeEngine as L; l=L("./data"); print(len(l.download(["RELIANCE","TCS","INFY"],start="2020-01-01")))'  # 3 symbols
```

## 12. Method limitations

- READ-ONLY respected: no file under `data/` or `src/` was written; harness lived in `/tmp/opencode/`, and the only repo file created is this report.
- Filesystem page cache was warm (not dropped; not permitted on shared host), so *cold* means process-cold, not disk-cold. A truly cold FS would be slower for the Parquet-heavy paths.
- Peak RSS in the master table is from the single-shot cold process; the warm process's 2,059.05 MiB is a cumulative high-water mark across all 149 warm measurements and cannot be split per data type with `getrusage`.
- `warm_best_ms` for the cached `resolve()` path hits the ~1 µs timer floor; its `rows_per_sec` should be read as "timer-resolution limited", not a precise rate.
- Concurrent external swarm commits/CPU activity (see §1 caveat) add uncontrolled noise; numbers are single-run measurements, not confidence intervals. No values were estimated or interpolated.
- **VERIFY_CMD (run from the main repo root, per task rule 8, because external commits moved the test baseline):**
  `cd /run/media/esco/ed/dev/code/bfinance-datalake && PYTHONPATH=src .venv/bin/python -m pytest -q tests/ --disable-warnings`
  → **181 passed in 15.33 s** (wave-start baseline was 176 passed; the increase is from external commits landing on main during the run, not from this report-only task). No T-04 code or data was touched.

