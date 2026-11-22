# T-03 — ETF coverage and missing-data audit

Wave 1 (parallel), WORKTREE `.worktrees/w1-etf`. READ-ONLY audit of `data/` (symlink never written).
Interpreter for every probe: `PYTHONPATH=src /run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python`
Engine under test: `UnifiedLakeEngine("data")` from `src/datalake/engine.py`.
Data watermark (all history calls): **2026-11-20**. Audit date: 2026-11-23.

## 0. Baseline / falsification result

- BEFORE: no ETF coverage map existed. `engine.py` has `etfs()` (line 1422) returning the catalog, and the crosswalk loader includes `nse_etf_master.parquet` (line 172), but there is no ETF-specific coverage audit.
- Falsification target: *"ETFs have the same data coverage as ordinary stocks in this lake."*
- **FALSIFIED.** ETFs have full price/screener/price-derived coverage but **0 rows** for corporate actions, shareholding, named holders, XBRL fundamentals, insider trades, annual reports, concalls, option chain and continuous futures. Identical code paths return large, non-empty frames for RELIANCE and TCS, so this is a dataset-coverage boundary, not API misuse.

## 1. Mandatory full-file reads (Step 1)

1. `TASK_T03.md` — lines 1–54 (full).
2. `src/datalake/engine.py` — lines 1–2049 (full). Relevant: `resolve()` 278, `history()` 432, `_load_raw_equity()` 371, `fundamentals()` 552, `shareholding()` 603, `named_holders()` 642, `corporate_actions()` 648, `option_chain()` 680, `index_constituents()` 818, `dossier()` 987, `etfs()` 1422, `deals()` 1647, `short_selling()` 1657, `annual_reports()` 1667, `concalls()` 1681, `insider_trades()` 1696, `custom_ratios()` 1223, `research_profile()` 1267, `factor_returns()` 908, `sql()` 1773.
3. `data/universe/nse_etf_master.parquet` via pyarrow/pandas — **361 rows, 1 row group, 9 columns**: `Symbol, Underlying Asset, SecurityName, DateofListing, MarketLot, ISINNumber, FaceValue, ETF Underlying, Underlying Key`. All string columns except `MarketLot` (int64) and `FaceValue` (float64).
4. `readme.md` — lines 1–204 (full). It documents 15 datasets but no ETF-specific dataset; ETFs only appear indirectly via the catalog.

`lake.etfs()` returned `(361, 9)` with exactly the same columns as the Parquet file — catalog method is a thin pass-through.

## 2. Symbol selection and validation (Step 3.1)

Five real ETFs chosen from the 361 catalog rows; `INDNIFTY` was requested by the task spec but is **ABSENT** from the master, so `JUNIORBEES` was substituted. Validation = exact uppercase match of `Symbol` in `nse_etf_master.parquet` plus row retrieval from that file.

| ETF | ISIN (master) | DateofListing | FV | `ETF Underlying` class | Resolved bse_code | `resolve()` company_name |
|---|---|---|---|---|---|---|
| NIFTYBEES | INF204KB14I2 | 08-Jan-02 | 1.0 | EQUITY | 590103 | NIPINDETFNIFTYBEES |
| GOLDBEES | INF204KB17I5 | 19-Mar-07 | 1.0 | COMMODITY | None | NIPINDETFGOLDBEES |
| BANKBEES | INF204KB15I9 | 04-Jun-04 | 1.0 | EQUITY | 590106 | NIPINDETFBANKBEES |
| CPSEETF | INF457M01133 | 04-Apr-14 | 10.0 | EQUITY | 538057 | CPSEETF |
| JUNIORBEES | INF732E01045 | 06-Mar-03 | 1.25 | EQUITY | 590104 | NIP IND ETF JUNIOR BEES |

Stock controls (same code path, no special casing): **RELIANCE** (`500325`, `INE002A01018`, FV 10) and **TCS** (`532540`, `INE467B01029`, FV 1).

Crosswalk note: `engine.py` line 297 accepts 12-char ISINs starting `INE`/`INF`, so all ETF ISINs resolve. All 361 ETF ISINs appear in `universe/security_master_unified.parquet` (3,611 ISIN matches in the 3,611-row master crosswalk; 204 ETF ISINs also appear in `bse_scrip_master.parquet`). The five ETFs each have exactly one row in `security_master_unified.parquet` (6,989 rows) with `isin`, `face_value`, `bse_code`, `company_name` populated. **Instrument type is not a dedicated column** in any identity store: the only type-like field is `ETF Underlying` in the ETF master (`EQUITY` ×4, `COMMODITY` ×1 for GOLDBEES). `SecurityIdentity` has no instrument-type attribute.

## 3. Price / history coverage (Step 3.3 first bullet, 3.5, 3.6)

All calls: `lake.history(q, start="1990-01-01", exchange=<E>, auto_adjust=<bool>)`. `null_pct` = null cells / all cells; `Close_null_pct` = nulls in `Close`. All statuses `OK`, no error text. Raw partition counts cross-check the engine counts exactly for all five ETFs (series = `EQ` only, so `_dedup_nse_equity_series()` drops nothing).

| Symbol | NSE rows (aa=True / aa=False) | NSE first → last | `Close`/`Adj Close` null % | NSE overall null % | BSE rows (aa=True) | BSE engine first → last | status |
|---|---|---|---|---|---|---|---|
| NIFTYBEES | 6150 / 6150 | 2002-01-08 → 2026-11-20 | 0.0 / 0.0 | 3.85 | 4224 | 2009-10-07 → 2026-11-20 | OK |
| GOLDBEES | 4855 / 4855 | 2007-03-19 → 2026-11-20 | 0.0 / 0.0 | 2.17 | 4241* | 2009-09-04 → 2026-11-20 | OK |
| BANKBEES | 5202 / 5202 | 2004-06-04 → 2026-11-20 | 0.0 / 0.0 | 2.69 | 3887 | 2009-10-30 → 2026-11-20 | OK |
| CPSEETF | 3111 / 3111 | 2014-04-04 → 2026-11-20 | 0.0 / 0.0 | 0.00 | 3112 | 2014-04-04 → 2026-11-20 | OK |
| JUNIORBEES | 5669 / 5669 | 2003-03-06 → 2026-11-20 | 0.0 / 0.0 | 3.31 | 3999 | 2009-10-12 → 2026-11-20 | OK |
| RELIANCE (control) | 7885 / 7885 | 1995-01-02 → 2026-11-20 | 0.0 / 0.0 | 7.40 | 5089 | 2006-03-01 → 2026-11-20 | OK |
| TCS (control) | 5491 / 5491 | 2004-08-25 → 2026-11-20 | 0.0 / 0.0 | 3.08 | 5089 | 2006-03-01 → 2026-11-20 | OK |

\* GOLDBEES has `bse_code=None`; `history(exchange="BSE")` falls back to a `Symbol`-based filter (engine lines 387–390) and still returns 4,241 BSE rows (series `E`,`F`). The other four resolve BSE by `InstrumentId` (bse code). Raw NSE yearly-partition scan reproduces the exact NSE counts (6150/4855/5202/3111/5669); raw BSE-by-InstrumentId reproduces 4224/3887/3112; GOLDBEES BSE-by-Symbol reproduces 4241. ETF first-price dates equal the master `DateofListing` dates exactly (DD-Mon-YY parsed).

**All 361 master ETFs have NSE price rows: 361/361 with price rows, 0 with zero price rows.** No ETF in the master needs flagging for absent EOD prices. `compacted__nse_cm_hist.parquet` does not exist; coverage is in yearly partitions `data/equities/nse_cm_1995..2026.parquet` (32 files).

## 4. ETF × data-type matrix with stock controls (Step 3.3)

Cells = `rows` with `max_date`; `null_pct` shown in parentheses where non-empty. Every call returned `status="OK"` with empty error text — **no API errors anywhere**; all empties are genuine zero-row results.

| Data type (`engine` call) | NIFTYBEES | GOLDBEES | BANKBEES | CPSEETF | JUNIORBEES | RELIANCE | TCS |
|---|---|---|---|---|---|---|---|
| corporate_actions | 0 | 0 | 0 | 0 | 0 | 25 (2026-06-05, 0%) | 81 (2026-10-14, 0%) |
| shareholding | 0 | 0 | 0 | 0 | 0 | 103 (2026-09-30, 66.5%) | 89 (2026-09-30, 65.41%) |
| named_holders | 0 | 0 | 0 | 0 | 0 | 734 (2026-09-30, 28.47%) | 3137 (2026-09-30, 22.65%) |
| fundamentals / financials | 0 | 0 | 0 | 0 | 0 | 10 (2026-09-30, 66%) | 10 (2026-09-30, 66%) |
| deals (bulk/block) | 33 (2005-02-17, 10%) | 4 (2007-03-19, 10%) | 0 | 0 | 67 (2007-01-24, 10%) | 341 (2026-10-08, 10%) | 54 (2026-06-24, 10%) |
| insider_trades | 0 | 0 | 0 | 0 | 0 | 1162 (no date col, 6.75%) | 130 (no date col, 6.66%) |
| short_selling | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| annual_reports | 0 | 0 | 0 | 0 | 0 | 34 (no date col, 0%) | 35 (no date col, 0%) |
| concalls | 0 | 0 | 0 | 0 | 0 | 21 (2026-11-04, 0%) | 11 (2026-10-14, 0%) |
| option_chain | 0 | 0 | 0 | 0 | 0 | 234 (2026-11-20, 0%) | 196 (2026-11-20, 0%) |
| continuous_futures | 0 | 0 | 0 | 0 | 0 | 6144 (2026-11-20, 5.32%) | 5453 (2026-11-20, 5.25%) |
| calendar (board meetings) | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| index-constituent membership | 0 | 0 | 0 | 0 | 0 | present (stock index) | present (stock index) |
| screener row | 1 (2026-11-20, 71.88%) | 1 (2026-11-20, 71.88%) | 1 (2026-11-20, 71.88%) | 1 (2026-11-20, 71.88%) | 1 (2026-11-20, 71.88%) | 1 (2026-11-20, 28.12%) | 1 (2026-11-20, 28.12%) |

Derived/aggregate APIs:

| API | All 5 ETFs | Controls |
|---|---|---|
| `research_profile()` | `OK`, 29 keys; price fields populated (e.g. NIFTYBEES `current_price=294.22`, `high_52w=297.55`), but `market_cap_cr`, `stock_pe`, `price_to_book` and all fundamental-derived fields `None` | 29 keys; RELIANCE `market_cap_cr=1830860.71`, `stock_pe=41.03` |
| `custom_ratios()` | `OK`, 33 keys; every forensic value `None` (`piotroski_score`, `graham_number`, `book_value_per_share`, `eps_ttm`, `roe_pct`, …) | RELIANCE `piotroski_score=4`, `graham_number=87.69`, `eps_ttm=33.57`, `roe_pct=335.8` |
| `dossier()` | `OK`; keys `['deals']` for NIFTYBEES/GOLDBEES/JUNIORBEES, `[]` for BANKBEES/CPSEETF | 8 keys each: actions, annual_reports, concalls, deals, financials, insider_trades, named_holders, shareholding |
| `factor_returns()` (market-wide, not symbol-scoped) | `OK`, 5428 rows × 13 cols, `null_pct=4.11%`; returns MKT/RF/SMB/sector/VIX regardless of symbol | identical (same market-wide result) |

## 5. Direct Parquet evidence for every EMPTY (Step 3.4)

All scanned files below are `data/...` Parquet. "ETF hits" = exact uppercase match of any of the 5 symbols in **any** column. Full 361-symbol scan was also run; coincidence caveat below.

| Empty data type | Underlying Parquet file(s) | File rows | Exact ETF-symbol hits | Controls in same file | Classification |
|---|---|---|---|---|---|
| Corporate actions | `actions/nse_corporate_actions.parquet`; `actions/bse_corporate_actions.parquet` | 45,573; 38,882 | 0 for chosen 5. Full-master scan: 0 genuine ETF rows (`DIVIDEND` hit is an action-type column value, not a symbol) | RELIANCE 25, TCS 81 via API | **STRUCTURAL_N_A** — feeds cover equities only |
| Shareholding | `shareholding/nse_shareholding_summary.parquet`; `shareholding/bse_shareholding_history.parquet` | 24,912; 245,418 | 0 (chosen 5 and all 361) | 103 / 89 | **STRUCTURAL_N_A** — ETFs have no promoter/shareholding filings |
| Named holders | `shareholding/named_shareholders.parquet` | 499,267 | 0 (all 361) | 734 / 3137 | **STRUCTURAL_N_A** |
| Fundamentals | `fundamentals/xbrl_financials.parquet` | 16,928 | 0 (all 361) | 10 / 10 | **STRUCTURAL_N_A** — XBRL issuers are companies, not ETFs |
| Insider trades | `deals/insider_trading_pit.parquet` | 231,136 | 0 (all 361) | 1162 / 130 | **STRUCTURAL_N_A** — SEBI PIT insiders are company insiders |
| Short selling | `deals/short_selling.parquet` | **FILE NOT FOUND** (directory `data/deals/` contains only bulk_block_deals + insider_trading_pit) | n/a | 0 / 0 | **STRUCTURAL_N_A** (lake-wide dataset absent; affects controls identically — global hole, not ETF-specific) |
| Annual reports | `documents/annual_reports.parquet` | 12,521 | 0 (all 361) | 34 / 35 | **STRUCTURAL_N_A** — ETFs are not companies issuing annual reports |
| Concalls | `documents/concalls_and_filings.parquet` | 23,191 | 0 (all 361) | 21 / 11 | **STRUCTURAL_N_A** |
| Option chain | `derivatives/nse_fo_*.parquet` (27 files) | 45,401,493 combined | 0 (all 361) | 234 / 196 | **STRUCTURAL_N_A** — no ETF F&O contracts in lake |
| Continuous futures | same 27 F&O files | 45,401,493 | 0 (all 361) | 6144 / 5453 | **STRUCTURAL_N_A** |
| Calendar | `actions/upcoming_board_meetings.parquet` | 230 | 0 for chosen 5; `DIVIDEND` false positive in full-master scan (action-type value) | 0 / 0 | **STRUCTURAL_N_A** (empty for controls too; not ETF-diagnostic) |
| Index membership | `indices/index_constituents_live.parquet`; `universe/nifty_constituents_pit.parquet` | 111,940; 24,768 | 0 genuine ETF rows (chosen 5 and all 361; `HEALTHCARE` in PIT is an `IndexName`, not a constituent) | stocks present | **STRUCTURAL_N_A** — indices track stocks, not ETFs |
| Deals (BANKBEES, CPSEETF only) | `deals/bulk_block_deals.parquet` | 215,335 | 0 for BANKBEES/CPSEETF; **4 ETFs present overall**: NIFTYBEES×33, JUNIORBEES×67, GOLDBEES×4, LIQUIDBEES (in master) | 341 / 54 | **GENUINE_MISSING** (no rows in lake; same file/engine path carries 104 rows for 3 other ETFs, so not engine failure). May legitimately reflect no reported bulk deals for these two. |
| research_profile / custom_ratios fundamental fields | derived from `fundamentals` (16,928 rows), `shareholding` (24,912), screener snapshot (9,323) | see rows above | 0 in source fundamentals/shareholding for ETFs | populated | **STRUCTURAL_N_A** (downstream consequence) |

Parquet evidence confirms the engine is **not** failing to resolve ETFs: each ETF resolves to a `SecurityIdentity` with correct symbol/ISIN/FV, each appears once in `security_master_unified.parquet` and once in `snapshots/market_screener_latest.parquet` (9,323 rows), and the empties coincide with zero matching rows in the source files.

Deal sample proof they are real ETF deals: `bulk_block_deals.parquet` rows for `NIFTYBEES` include `Security Name='NIFTYBEES JUNIOR BENCHMARK ETF'`, 2005-01-05, BULK, NSE; `GOLDBEES` includes `Security Name='Benchmark Mutual Fund - G'`, 2007-03-19; `JUNIORBEES` includes `'JUNIOR BENCHMARK ETF'`, 2005-01-03.

**Caveat on false positives:** `DIVIDEND` (master row 298, Mirae Asset BSE 500 Dividend Leaders ETF) and `HEALTHCARE` (master row 306, Mirae Asset Nifty 500 Healthcare ETF) are genuine ETF symbols in the master, but their "matches" in actions/board-meetings and PIT constituents respectively are coincidental column values (action type `DIVIDEND`, `IndexName='HEALTHCARE'`). They were excluded from all ETF hit counts; the five audit ETFs have zero hits regardless.

## 6. Ranked gaps (exact counts/dates)

1. **No fundamental/ownership/issuer-document data for any of 361 ETFs** — 0 rows in `xbrl_financials` (16,928 rows), both shareholding files (24,912 + 245,418), `named_shareholders` (499,267), `insider_trading_pit` (231,136), `annual_reports` (12,521), `concalls_and_filings` (23,191). Controls are non-empty in every one of these (RELIANCE 10/103/734/1162/34/21; TCS 10/89/3137/130/35/11). Largest coverage asymmetry.
2. **No corporate actions for ETFs** — 0 rows in 45,573 + 38,882 action rows; controls 25/81. Note ETF distributions may be unreported upstream; this is a candidate genuine hole for dividend-bearing ETFs, but the action feeds contain zero ETF rows lake-wide.
3. **No ETF derivatives** — 0 rows across 45.4 M F&O rows; controls have 234/196 option-chain and 6144/5453 continuous-futures rows.
4. **Deals partial** — 3/5 audited ETFs have only legacy bulk deals (NIFTYBEES 33 through 2005-02-17, JUNIORBEES 67 through 2007-01-24, GOLDBEES 4 through 2007-03-19); BANKBEES and CPSEETF have 0. Nothing after 2007 for any ETF, while controls run to 2026-10-08.
5. **`deals/short_selling.parquet` absent entirely** — 0 for ETFs and controls alike; global dataset hole.
6. **No ETF index-constituent membership** — 0 across 111,940 live + 24,768 PIT constituent rows; structurally expected.
7. **Price-derived payloads are partial** — `research_profile()` 29 keys but valuation/fundamental fields `None`; `custom_ratios()` 33 keys but every forensic value `None`; `dossier()` only `['deals']` (or empty). Screener row exists but 71.88% of its cells are null vs 28.12% for controls.

**Not gaps:** NSE/BSE EOD history (all 361 ETFs priced; chosen 5 have 3,111–6,150 NSE bars, zero `Close` nulls, watermark 2026-11-20), ISIN/BSE-code/face-value identity resolution, and market-wide `factor_returns()` are fully covered.

## 7. Reproduction commands

```bash
cd /run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-etf
export PYTHONPATH=src
PY=/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python

# Catalog validation + identity
$PY -c 'from datalake import UnifiedLakeEngine as L; l=L("data"); e=l.etfs(); print(e.shape); print(e[e.Symbol.isin(["NIFTYBEES","GOLDBEES","BANKBEES","CPSEETF","JUNIORBEES"])].to_string()); print([l.resolve(s) for s in ["NIFTYBEES","RELIANCE"]])'

# Price + full data-type probe (rows/max_date/null_pct/status/error for 5 ETFs + 2 controls)
$PY /tmp/opencode/etf_probe.py          # -> /tmp/opencode/etf_probe_results.json

# Direct Parquet evidence + 361-ETF price coverage + ISIN presence
$PY /tmp/opencode/etf_parquet_evidence.py  # -> /tmp/opencode/etf_parquet_evidence.json

# All-361 ETF non-price coverage + security-master instrument fields + BSE-by-symbol + deal samples
$PY /tmp/opencode/etf_gapfill.py        # -> /tmp/opencode/etf_gapfill.json

# Derived payload spot check
$PY -c 'from datalake import UnifiedLakeEngine as L; l=L("data"); print(l.research_profile("NIFTYBEES")); print(l.custom_ratios("NIFTYBEES")); print(l.dossier("NIFTYBEES").keys())'
```

`read_table_safe()` (used by `engine.etfs()`) reads the Parquet files directly; no data file was modified or created by this audit.
