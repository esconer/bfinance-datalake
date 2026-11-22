# T-02 — Multi-stock missing-data audit (STOCKS)

**Worker:** T-02 (fresh context) · **Date:** 2026-11-23 · **Scope:** READ-ONLY audit of the local lake.
**Worktree:** `/run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-stocks` (`data -> /run/media/esco/ed/dev/code/bfinance-datalake/data`, never written to).

## 0. Hypothesis / BEFORE baseline

**Falsify target:** "for the chosen stocks, all per-symbol data types are complete and internally consistent."
**BEFORE:** no multi-stock gap map existed; only per-dataset health/manifest watermarks.
**AFTER:** the map below yields **9 findings (3 HIGH, 1 MED-HIGH, 2 MED, 3 LOW)**, ranked in §5:
a 4–5 year shortfall in XBRL fundamentals, a 100%-null promoter column in all pre-2021
shareholding rows, a polluted trading-day reference, empty short-selling coverage, stale insider
filing watermarks for 3 of 5 names (TCS, SBIN, HDFCBANK), null derived ratios in the screener
snapshot, BSE **null-volume** days (not zero-volume days), an INFY duplicate ExDate, and a
manifest watermark blind spot. The hypothesis is **falsified**.

## 1. Exact commands and subjects

Environment / interpreter (per task rule 4):

```bash
cd /run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-stocks
export PYTHONPATH=src
PY=/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python
```

Harness scripts were placed **outside the repo** at `/tmp/opencode/t02_audit.py`,
`/tmp/opencode/t02_probe.py`, `/tmp/opencode/t02_probe2.py` (no repo file other than this
report was created/edited). Run as:

```bash
$PY /tmp/opencode/t02_audit.py     # full matrix + 5 explicit tests -> /tmp/opencode/t02_results.json
$PY /tmp/opencode/t02_probe.py     # anomaly deep-dives              -> /tmp/opencode/t02_probe.json
$PY /tmp/opencode/t02_probe2.py    # refined gap classification      -> /tmp/opencode/t02_probe2.json
```

Lake handle: `lake = UnifiedLakeEngine("./data")` (resolves to the read-only symlink).
Runtime: Python 3.13.8, pandas 3.1.0, duckdb 2.0.0. `lake.health()`:
contract_version `1.0.0`, generated_at `2026-11-23T00:42:11+05:30`, 161 parquet files,
2137.31 MB, ready=True.

**Symbols actually used (all 5 resolved successfully; no substitution needed):**

| Query | canonical | BSE code | ISIN | aliases | listing_date |
|---|---|---|---|---|---|
| RELIANCE | RELIANCE | 500325 | INE002A01018 | RELIANCE | 29-NOV-1995 |
| TCS | TCS | 532540 | INE467B01029 | TCS | 25-AUG-2004 |
| INFY | INFY | 500209 | INE009A01021 | **INFY, INFOSYSTCH** | 08-FEB-1995 |
| HDFCBANK | HDFCBANK | 500180 | INE040A01034 | HDFCBANK | 08-NOV-1995 |
| SBIN | SBIN | 500112 | INE062A01020 | SBIN | 01-MAR-1995 |

Key engine calls per cell: `lake.history(sym, start="1995-01-01", exchange="NSE"|"BSE",
auto_adjust=False|True)`, `lake.fundamentals(sym, period_type="Annual"|"Quarterly")`,
`lake.shareholding`, `lake.named_holders`, `lake.corporate_actions(include_non_adjusting=True)`,
`lake.deals`, `lake.insider_trades`, `lake.short_selling`, `lake.annual_reports`,
`lake.concalls`, `lake.dossier`, `lake.factor_returns`, `lake.custom_ratios`,
`lake.research_profile`, `lake.screener("Symbol = '<SYM>'")`, `lake.option_chain`,
`lake.continuous_futures(contract=1)`, `lake.index_constituents("NIFTY 50")`.

Metric definitions: `null_pct` = % NaN in the key column (OHLCV → Close; fundamentals → Sales_Cr;
shareholding → PromoterPct); `dup_dates` = duplicated normalized date values; `non_monotonic`
= rows where the normalized date decreases vs the previous row. Volume is reported as two
**separate** counts: `null_volume_days` = `Volume.isna()` and `zero_volume_days` =
`Volume<=0` on the numeric-coerced series (they are not interchangeable — see §2.1);
`max_date`/`min_date` from the dataset's date column. `N/A` means the dataset has no meaningful
per-row date or no applicable volume/key column; it is **not** a data defect.

## 2. Master matrix — stock × data type

Cell = `rows | max_date | status`. Statuses limited to **OK / EMPTY / ERROR / N_A**;
caveats are in the detail tables and §5. `*` = OK but with a coverage caveat documented below.

| Data type | RELIANCE | TCS | INFY | HDFCBANK | SBIN |
|---|---|---|---|---|---|
| history NSE (1995→) | 7885 \| 2026-11-20 \| OK | 5491 \| 2026-11-20 \| OK | 7845 \| 2026-11-20 \| OK | 7776 \| 2026-11-20 \| OK | 7885 \| 2026-11-20 \| OK |
| history BSE (2006→) | 5089 \| 2026-11-20 \| OK* | 5089 \| 2026-11-20 \| OK | 5089 \| 2026-11-20 \| OK* | 5089 \| 2026-11-20 \| OK* | 5089 \| 2026-11-20 \| OK* |
| fundamentals — annual | 5 \| 2026-03-31 \| OK* | 5 \| 2026-03-31 \| OK* | 5 \| 2026-03-31 \| OK* | 5 \| 2026-03-31 \| OK* | 5 \| 2026-03-31 \| OK* |
| fundamentals — quarterly | 5 \| 2026-09-30 \| OK* | 5 \| 2026-09-30 \| OK* | 5 \| 2026-09-30 \| OK* | 5 \| 2026-09-30 \| OK* | 6 \| 2026-09-30 \| OK* |
| shareholding | 103 \| 2026-09-30 \| OK* | 89 \| 2026-09-30 \| OK* | 103 \| 2026-09-30 \| OK* | 103 \| 2026-09-30 \| OK* | 103 \| 2026-09-30 \| OK* |
| named holders | 734 \| 2026-09-30 \| OK | 3137 \| 2026-09-30 \| OK | 488 \| 2026-09-30 \| OK | 188 \| 2026-09-30 \| OK | 134 \| 2026-09-30 \| OK |
| corporate actions (all) | 27 \| 2026-06-05 \| OK | 86 \| 2026-10-14 \| OK | 44 \| 2026-11-03 \| OK* | 25 \| 2026-06-19 \| OK | 23 \| 2026-05-15 \| OK |
| deals (bulk/block) | 341 \| 2026-10-08 \| OK | 54 \| 2026-06-24 \| OK | 57 \| 2026-06-29 \| OK | 61 \| 2026-06-24 \| OK | 37 \| 2025-11-18 \| OK* |
| insider trades | 1162 \| 2026-02-18 \| OK* | 130 \| 2022-05-25 \| OK* | 3132 \| 2026-04-06 \| OK | 14109 \| 2025-03-26 \| OK* | 10 \| 2024-09-05 \| OK* |
| short selling | 0 \| — \| EMPTY | 0 \| — \| EMPTY | 0 \| — \| EMPTY | 0 \| — \| EMPTY | 0 \| — \| EMPTY |
| annual reports | 34 \| FY2025-26 \| OK | 35 \| FY2025-26 \| OK | 35 \| FY2025-26 \| OK | 36 \| FY2025-26 \| OK | 34 \| FY2025-26 \| OK |
| concalls / filings | 21 \| 2026-11-04 \| OK | 11 \| 2026-10-14 \| OK | 3 \| 2026-10-28 \| OK* | 6 \| 2026-08-20 \| OK | 12 \| 2026-11-03 \| OK |
| dossier (keys / non-empty) | 8/8 | 8/8 | 8/8 | 8/8 | 8/8 |
| factor returns | 5428 \| 2026-11-20 \| N_A (market-wide) | N_A | N_A | N_A | N_A |
| custom ratios | 33 keys, 16 non-null \| — \| OK* | 33/16 \| OK* | 33/16 \| OK* | 33/15 \| OK* | 33/15 \| OK* |
| research profile | 29 keys \| — \| OK | 29 keys \| OK | 29 keys \| OK | 29 keys \| OK | 29 keys \| OK |
| screener row | 1 \| snapshot 2026-11-20 \| OK* | 1 \| OK* | 1 \| OK* | 1 \| OK* | 1 \| OK* |
| option chain (latest) | 234 \| 2026-11-20 \| OK | 196 \| 2026-11-20 \| OK | 185 \| 2026-11-20 \| OK | 159 \| 2026-11-20 \| OK | 197 \| 2026-11-20 \| OK |
| continuous futures (contract 1) | 6144 \| 2026-11-20 \| OK | 5453 \| 2026-11-20 \| OK | 6144 \| 2026-11-20 \| OK | 5695 \| 2026-11-20 \| OK | 6144 \| 2026-11-20 \| OK |
| index membership (NIFTY 50 live) | yes (1 row, 2026-11-21) | yes | yes | yes | yes |

### 2.1 Per-cell quality metrics (exact measured numbers)

**NSE daily OHLCV** (`auto_adjust=False`): Open/High/Low/Close/Volume null% = **0.0** for all five;
`dup_dates=0`, `non_monotonic=0`, `null_volume_days=0`, `zero_volume_days=0` for all five. Min dates: RELIANCE/INFY/SBIN
1995-01-02, HDFCBANK 1995-06-14, TCS 2004-08-25 (= listing, structural).

**BSE daily OHLCV** (`auto_adjust=False`): all five 5089 rows, 2006-03-01 → 2026-11-20,
`dup_dates=0`, `non_monotonic=0`, OHLC null% = 0.0. Volume anomalies — these are **NaN-volume
cells, not zero-volume days**: measured `Volume.isna()` vs `Volume==0` vs `Volume<=0` counts
are shown explicitly (all rows 5089 per symbol):

| Symbol | Volume null % | null-volume days (`isna`) | `Volume==0` | `Volume<0` | `Volume<=0` (true zero-volume days) |
|---|---|---|---|---|---|
| RELIANCE | 0.57 | 29 | 0 | 0 | **0** |
| TCS | 0.00 | 0 | 0 | 0 | **0** |
| INFY | 0.20 | 10 | 0 | 0 | **0** |
| HDFCBANK | 0.06 | 3 | 0 | 0 | **0** |
| SBIN | 0.20 | 10 | 0 | 0 | **0** |

There are **zero** actual zero/negative-volume days for all five symbols; the 29/10/3/10
counts equal the NaN counts exactly.

**Fundamentals / shareholding / holders / actions / deals / concalls**

| Dataset | RELIANCE | TCS | INFY | HDFCBANK | SBIN |
|---|---|---|---|---|---|
| Annual (rows, range) | 5, 2022-03-31→2026-03-31 | same | same | same | same |
| Quarterly (rows, range) | 5, 2025-09-30→2026-09-30 | same | same | same | 6, 2024-12-31→2026-09-30 |
| Fundamentals key null% (Sales_Cr) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| Shareholding rows / range | 103 / 2001-03-31→2026-09-30 | 89 / 2004-09-30→2026-09-30 | 103 / 2001-03-31→2026-09-30 | 103 / 2001-03-31→2026-09-30 | 103 / 2001-03-31→2026-09-30 |
| Shareholding `PromoterPct` null% | **79.61** | **76.40** | **79.61** | **79.61** | **79.61** |
| Shareholding dup/non-monotonic | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Named holders rows | 734 | 3137 | 488 | 188 | 134 |
| Named holders dup dates | 722 | 3121 | 476 | 176 | 122 |
| Named holders non-monotonic | 4 | 5 | 4 | 4 | 4 |
| Corporate actions rows (dup dates) | 27 (0) | 86 (0) | 44 (**1**) | 25 (0) | 23 (0) |
| Deals rows (dup dates) | 341 (290) | 54 (28) | 57 (34) | 61 (37) | 37 (21) |
| Concalls rows (dup dates) | 21 (3) | 11 (0) | 3 (0) | 6 (0) | 12 (2) |
| Annual-report rows / FY span | 34 / 2009-10→2025-26 | 35 / same | 35 / same | 36 / same | 34 / same |

All fundamentals dup/non-monotonic counts = 0; all deals/concalls non-monotonic = 0.
Dossier keys for every symbol: `financials, shareholding, named_holders, actions, deals,
insider_trades, annual_reports, concalls` (all 8 non-empty). Dossier `financials` rows =
10 for four names, 11 for SBIN (= annual + quarterly counts above). Dossier `actions` rows
excludes non-adjusting events: 25 / 81 / 42 / 25 / 22.

**Insider trades** (date column is lowercase `date`; captured with `pd.to_datetime(d["date"])`):

| Symbol | rows | min date | max date | lag vs lake 2026-11-20 |
|---|---|---|---|---|
| RELIANCE | 1162 | 2018-01-03 | 2026-02-18 | 275 d |
| TCS | 130 | 2018-01-24 | **2022-05-25** | **1640 d** |
| INFY | 3132 | 2018-01-02 | 2026-04-06 | 228 d |
| HDFCBANK | 14109 | 2018-01-04 | 2025-03-26 | 604 d |
| SBIN | 10 | 2018-06-22 | **2024-09-05** | **806 d** |

**Derived / snapshot cells** — `factor_returns`: 5428 rows, 2005-01-04→2026-11-20,
13 columns (`MKT, RF, MKT_RF, SMB, SEC_BANK…SEC_REALTY, INDIA_VIX`), market-wide → N_A per stock.
`custom_ratios`: 33 keys each; non-null 16/16/16/15/15 (RELIANCE/TCS/INFY/HDFCBANK/SBIN);
Piotroski 4/4/5/2/4; Graham number 87.69/57.12/95.53/36.00/45.71.
`research_profile`: 29 keys each. `screener`: exactly 1 row per symbol, 32 columns; **9 columns
are null for all five**: `BookValue_Cr, DebtToEquity, EV_EBITDA, PB, PromoterPledgedPct,
ROCE_Pct, ROE_Pct, TotalBorrowings_Cr, TotalCashAndBank_Cr`.
Option-chain duplicate dates = rows−1 (234/196/185/159/197) — expected: many strikes × expiries
on one trade date. Continuous-futures min dates 2001-11-09 (RELIANCE/INFY/SBIN), 2004-08-25
(TCS), 2003-08-29 (HDFCBANK); dup 0.

## 3. The 5 explicit missing-data tests

### Test 1 — Trading-day gaps vs `lake.trading_days()`
**Method.** `td = lake.trading_days()` (NIFTY-50-derived sessions, n=7724, 1995-11-03→2026-11-20).
For each symbol, restrict `td` to that symbol's own `[min,max]` history window, set-diff the
symbol's NSE date index, then re-classify every "missing" date by day-of-week.

| Symbol | Ref sessions | Symbol dates | Raw missing | **of which weekend dates in ref** | **true non-weekend missing** | Extra symbol dates | Extra after index start |
|---|---|---|---|---|---|---|---|
| RELIANCE | 7724 | 7885 | 45 | 42 | **3** | 206 | 1 (1996-07-17) |
| TCS | 5519 | 5491 | 28 | 27 | **1** | 0 | 0 |
| INFY | 7724 | 7845 | 50 | 42 | **8** | 171 | 0 |
| HDFCBANK | 7724 | 7776 | 44 | 42 | **2** | 96 | 1 (1996-07-17) |
| SBIN | 7724 | 7885 | 45 | 42 | **3** | 206 | 1 (1996-07-17) |

Non-weekend missing dates: RELIANCE `[1996-10-30, 1997-06-02, 2021-11-04]`;
TCS `[2021-11-04]`; INFY `[1995-11-06, 1995-11-09, 1995-11-30, 1995-12-01, 1995-12-13,
1996-04-01, 1996-10-30, 2021-11-04]`; HDFCBANK `[1996-10-30, 2021-11-04]`;
SBIN `[1996-10-30, 1997-06-02, 2021-11-04]`.
**Clustering:** 42 of 45 "missing" sessions are **weekend bars in the reference itself** —
`trading_days()` contains **42 Saturday/Sunday dates** (33 Sat + 9 Sun; peak years 1999=5,
2012=4, 1998/2003/2006/2024=3 each), and `index_history("NIFTY 50")` has 43 weekend rows.
Raw missing counts cluster in 1999/2012/2024 exactly because those are the weekend-bar years.
The residual real gap is tiny and concentrated in **2021-11-04 (all 5 stocks)** — a Diwali
session present in the index but absent from every equity frame — plus a handful of 1995–97
dates (mostly INFY's rename-era pre-listing window).
The 171–206 "extra" dates are **all before 1995-11-03**, the index start date: the equity
archive begins 1995-01-02 while the trading-day reference begins 1995-11-03 — a reference
coverage artifact, not missing equity data.

### Test 2 — Watermark staleness
**Method.** Compare dataset `max_date` with `manifest.json → latest_dates` and `lake.health()`.
Equity/BSE/FO/index watermark = **2026-11-20**; lake-wide max manifest date = **2026-11-22**
(mutual-fund NAVs). All five NSE price series end exactly on 2026-11-20 → lag **0 days**.
BSE series and FO/index/screener snapshots also end 2026-11-20; NIFTY-50 live constituents
2026-11-21; F&O ban list 2026-11-21.
**Manifest observability gap (exact set difference):** `manifest.json → files` has **161**
keys, `latest_dates` has **142** keys, and `files − latest_dates` = **19 files**:

- 9 non-universe data datasets: `fundamentals/xbrl_financials.parquet`,
  `shareholding/nse_shareholding_summary.parquet`, `shareholding/bse_shareholding_history.parquet`,
  `shareholding/named_shareholders.parquet`, `actions/nse_corporate_actions.parquet`,
  `actions/bse_corporate_actions.parquet`, `deals/insider_trading_pit.parquet`,
  `documents/annual_reports.parquet`, `documents/concalls_and_filings.parquet`;
- 1 further non-universe file: `actions/upcoming_board_meetings.parquet`;
- 9 `universe/*` masters: `bse_scrip_master.parquet`, `nifty_constituents_pit.parquet`,
  `nse_delisted_master.parquet`, `nse_equity_master.parquet`, `nse_etf_master.parquet`,
  `nse_market_universe.parquet`, `nse_sme_master.parquet`, `nse_symbol_changes.parquet`,
  `security_master_unified.parquet`.

(`deals/short_selling.parquet` is **not** in `files` at all — the file does not exist; it is not
part of this 19-file difference. `deals/bulk_block_deals.parquet` and
`snapshots/market_screener_latest.parquet` *are* present at 2026-11-20.)
So staleness cannot be asserted from the manifest for the 19 files; it was measured directly
from rows here → see insider-trade lag table in §2.1.

### Test 3 — Per-symbol presence in yearly partitions (holes)
**Method.** For every `equities/nse_cm_YYYY.parquet` (1995–2026) and
`equities/bse_cm_YYYY.parquet` (2006–2026), read the key column with a PyArrow `in` filter over
`lake.resolve(sym).aliases` (BSE: `InstrumentId` = BSE code + aliases).

| Symbol | NSE years present | NSE holes | BSE years present | BSE holes |
|---|---|---|---|---|
| RELIANCE | 1995–2026 (32 files) | none | 2006–2026 (21) | none |
| TCS | **2004–2026 (23)** | none | 2006–2026 (21) | none |
| INFY | 1995–2026 (32) | none | 2006–2026 (21) | none |
| HDFCBANK | 1995–2026 (32) | none | 2006–2026 (21) | none |
| SBIN | 1995–2026 (32) | none | 2006–2026 (21) | none |

No mid-range holes. TCS's later start matches its 2004-08-25 listing; the common 2006 BSE
start matches BSE archive inception — both structurally correct, not gaps.

### Test 4 — Null/NaN hotspots in the OHLCV frame
**Method.** `lake.history("RELIANCE", exchange="NSE", auto_adjust=False)` (7885 rows, 20 cols),
% NaN per column, sorted descending.

| Rank | Column | Null % | Assessment |
|---|---|---|---|
| 1 | Exchange | 100.0 | engine artifact (see note) |
| 2 | Symbol | 100.0 | engine artifact |
| 3 | Series | 100.0 | engine artifact |
| 4 | ISIN | 100.0 | engine artifact |
| 5 | InstrumentId | 91.16 | engine artifact |

Genuine coverage hotspots (excluding metadata): `Trades` **51.79%**,
`DeliveryPct` **22.44%**, `DeliverableQty` **22.05%**. By era (RELIANCE):
pre-2002 (1729 rows) → Trades/DeliverableQty/DeliveryPct all 100% null;
2002–2009 (1987 rows) → Trades 100% null, DeliverableQty 0.5%, DeliveryPct 2.0%;
2010–2015 (1483 rows) → Trades 24.8% null, others 0%; 2016+ (2686 rows) → all 0%.
**Artifact proof:** raw on-disk `nse_cm_2026.parquet` has Exchange/Symbol/ISIN/InstrumentId
**0.0%** null (Series 0.2%); the 100%/91% nulls appear only in the engine's adjusted frame —
i.e., added by `OHLCVAdjuster.apply_adjustments`, not a source-data gap.

### Test 5 — Adj Close cross-check
**Method.** For each symbol, `lake.history(sym, start="1995-01-01", exchange="NSE",
auto_adjust=True)`; count `Adj Close` null or ≤0 while `Close` > 0 (and also run on raw
`auto_adjust=False`).

| Symbol | rows | Adj Close null | Adj Close = 0 | bad where Close>0 |
|---|---|---|---|---|
| RELIANCE | 7885 | 0 | 0 | **0** |
| TCS | 5491 | 0 | 0 | **0** |
| INFY | 7845 | 0 | 0 | **0** |
| HDFCBANK | 7776 | 0 | 0 | **0** |
| SBIN | 7885 | 0 | 0 | **0** |

Raw-frame cross-check also returned 0 for all five. **No missing adjustment values.**

## 4. Genuinely missing vs structurally not applicable

**Genuinely missing / defective (evidence-backed):**
1. XBRL fundamentals history — only 5 annual periods (FY2022–FY2026) and 5–6 quarters
   (2024-12/2025-09→2026-09) per symbol; raw file total 16,928 rows / 1,972 symbols and
   SQL min/max for the five names = 2022-03-31→2026-09-30 (52 rows). Docs advertise ~25 years.
2. `PromoterPct` in shareholding — 100% null for the 82 BSE-sourced rows (2001–2020,
   `SourceURL` null) and 0% null for the 21 NSE-sourced rows (2021+); 79.61% null overall
   (TCS 76.40% with 89 quarters). Real column-level hole in the long-history source.
3. Trading-day reference pollution — 42 weekend sessions in `trading_days()` / 43 in the
   NIFTY index frame; makes naive gap counts 45/28/50/44/44 instead of 3/1/8/2/3.
4. Short selling — `deals/short_selling.parquet` does not exist; all five symbol queries return
   0 rows and market-wide `lake.short_selling()` also returns 0 rows. Entire dataset un-ingested.
5. Insider-trade staleness — TCS last filing 2022-05-25 (1640 d before lake watermark),
   SBIN 2024-09-05 (806 d), HDFCBANK 2025-03-26 (604 d); SBIN has only 10 rows market-wide
   symbol `SBIN` out of 231,136 insider rows (symbol fragmentation is plausible; note
   `HDFC`=16,081 and `HDFCBANK`=14,109 co-exist) — cause suggested but **not proven**.
6. Screener derived fields — 9/32 columns null for all five (`ROE_Pct`, `ROCE_Pct`, `PB`,
   `EV_EBITDA`, `DebtToEquity`, `BookValue_Cr`, `TotalBorrowings_Cr`, `TotalCashAndBank_Cr`,
   `PromoterPledgedPct`); consistent with absent book-value/borrowings inputs in XBRL.
7. BSE **null-volume days** (not zero-volume) — `Volume.isna()` counts 29 (RELIANCE),
   10 (INFY), 3 (HDFCBANK), 10 (SBIN); 0 for TCS. `Volume==0` and `Volume<=0` counts are
   **0 for all five**, so no genuine zero-volume day exists in this sample.
8. INFY corporate actions — one duplicated ExDate 2017-10-31: BUYBACK and
   "Interim Dividend - Rs 13/- (Purpose Revised)" — two genuinely distinct events sharing an
   ex-date (benign but will double-count if keyed on ExDate alone).
9. Manifest watermark blind spot — **19 files** have no `latest_dates` entry (161 files in
   `manifest.json → files` minus 142 keys in `latest_dates`): 9 non-universe data datasets
   (`fundamentals/xbrl_financials.parquet`, `shareholding/nse_shareholding_summary.parquet`,
   `shareholding/bse_shareholding_history.parquet`, `shareholding/named_shareholders.parquet`,
   `actions/nse_corporate_actions.parquet`, `actions/bse_corporate_actions.parquet`,
   `deals/insider_trading_pit.parquet`, `documents/annual_reports.parquet`,
   `documents/concalls_and_filings.parquet`), plus `actions/upcoming_board_meetings.parquet`
   and 9 `universe/*` masters. Full enumeration in §3 Test 2.

**Structurally not applicable / NOT defects:**
- Option-chain `dup_dates` = rows−1 (multiple strikes/expiries per trade date).
- Named-holder and deals `dup_dates` (multiple records per quarter/day) and their small
  non-monotonic counts from NSE+BSE frame concatenation.
- Missing pre-1995-11-03 "trading days": equity archive starts 1995-01-02, index reference
  starts 1995-11-03; TCS pre-2004 and all five pre-2006 BSE rows are pre-listing/pre-archive.
- `factor_returns` (market-wide), annual-report `Year` values such as `2009-2010` (string FY
  labels, no holes 2009→2026), `ScripCode` parsing as a date (false positive, ignored),
  and `N_A` cells above.
- NSE history metadata columns null in the *engine output* (raw parquet is populated).

## 5. Ranked findings (severity · symbol · dataset · evidence · likely cause)

| # | Sev | Symbol(s) | Dataset | Evidence (exact) | Likely cause (evidence-supported only) |
|---|---|---|---|---|---|
| 1 | HIGH | all 5 | fundamentals / xbrl_financials | 5 annual rows FY2022–26, 5–6 quarterly; raw SQL 52 rows, min 2022-03-31, max 2026-09-30 for 5 names | Only recent XBRL filings were ever ingested; long-history backfill absent (whole file only 16,928 rows/1,972 symbols ≈ 8.6 rows/symbol) |
| 2 | HIGH | all 5 (TCS most) | shareholding → PromoterPct | 79.61% null overall; 100% null in 82 BSE rows 2001–2020 vs 0% in 21 NSE rows 2021+ | BSE long-history schema lacks promoter columns; NSE source added only from 2021 |
| 3 | HIGH | all 5 | trading_days / NIFTY index | 42 weekend sessions in reference (33 Sat/9 Sun), 43 weekend rows in index; naive gaps 45/28/50/44/44 vs true 3/1/8/2/3 | Weekend-dated bars in index source propagate into the authoritative trading-day calendar |
| 4 | MED-HIGH | all 5 | short_selling | file `deals/short_selling.parquet` absent; 0 rows symbol-level and market-wide | Dataset never ingested into this lake snapshot |
| 5 | MED | TCS, SBIN, HDFCBANK | insider_trades | last `date` 2022-05-25 / 2024-09-05 / 2025-03-26; normalized-date lags 1640/806/604 d; SBIN only 10 rows of 231,136 | TCS/SBIN/HDFCBANK ingestion stall; SBIN additionally likely symbol-key fragmentation (hypothesis, not proven) |
| 6 | MED | all 5 | screener snapshot | 9 of 32 columns null in every row (ROE_Pct, ROCE_Pct, PB, EV_EBITDA, DebtToEquity, BookValue_Cr, TotalBorrowings_Cr, TotalCashAndBank_Cr, PromoterPledgedPct) | Missing balance-sheet inputs in the 5-year XBRL set (finding 1) propagate into derived ratios |
| 7 | LOW | RELIANCE, INFY, HDFCBANK, SBIN | BSE OHLCV | **null-volume days** (`Volume.isna()`) 29 / 10 / 3 / 10 (null% 0.57/0.20/0.06/0.20); `Volume==0` and `Volume<=0` counts = **0 for all five** | Missing-volume cells in BSE source rows; not zero-volume trading days |
| 8 | LOW | INFY | corporate actions | duplicate ExDate 2017-10-31 (BUYBACK + revised dividend) | Two distinct legitimate events; dedup on ExDate alone is unsafe |
| 9 | LOW | lake-wide | manifest.json | 161 `files` − 142 `latest_dates` = **19** files with no watermark (9 data datasets + `actions/upcoming_board_meetings.parquet` + 9 `universe/*`); full list in §3 Test 2 | Watermark bookkeeping is populated only for time-partitioned datasets, not snapshot/master files |

**Non-findings worth recording:** NSE OHLCV is clean (0 null, 0 dups, 0 null-volume,
0 zero-volume, 0 bad Adj Close); no yearly-partition holes; no per-symbol watermark lag;
index membership present for all five.

## 6. Reproduction commands (top 3 findings)

```bash
cd /run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-stocks
PY=/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python
export PYTHONPATH=src

# F1 — XBRL depth: expect only 5 annual rows / 5-6 quarters, min 2022-03-31
$PY - <<'PY'
from datalake import UnifiedLakeEngine
import pandas as pd
lake = UnifiedLakeEngine("./data")
for s in ["RELIANCE","TCS","INFY","HDFCBANK","SBIN"]:
    a = lake.fundamentals(s, period_type="Annual")
    q = lake.fundamentals(s, period_type="Quarterly")
    print(s, "annual", len(a), a["PeriodEnd"].min(), a["PeriodEnd"].max(),
          "quarterly", len(q), q["PeriodEnd"].min(), q["PeriodEnd"].max())
print(lake.sql("SELECT MIN(PeriodEnd) mn, MAX(PeriodEnd) mx, COUNT(*) n FROM financials "
               "WHERE Symbol IN ('RELIANCE','TCS','INFY','HDFCBANK','SBIN')"))
PY

# F2 — shareholding PromoterPct null by source era
$PY - <<'PY'
from datalake import UnifiedLakeEngine
import pandas as pd
lake = UnifiedLakeEngine("./data")
d = lake.shareholding("RELIANCE")
d["yr"] = pd.to_datetime(d["QuarterEnd"]).dt.year
print(d.groupby(d["yr"] <= 2020)["PromoterPct"].agg(["size", lambda x: x.isna().mean()*100]))
print("SourceURL null rows:", int(d["SourceURL"].isna().sum()), "of", len(d))
PY

# F3 — polluted trading-day reference
$PY - <<'PY'
from datalake import UnifiedLakeEngine
import pandas as pd
lake = UnifiedLakeEngine("./data")
td = lake.trading_days()
print("sessions", len(td), "weekend sessions", int((td.dayofweek >= 5).sum()))
print("weekend examples", [str(d.date()) for d in td[td.dayofweek >= 5][:8]])
idx = lake.index_history("NIFTY 50")
print("index weekend rows", int((pd.to_datetime(idx["Date"]).dt.dayofweek >= 5).sum()))
for s in ["RELIANCE","TCS","INFY","HDFCBANK","SBIN"]:
    dates = set(pd.to_datetime(lake.history(s, exchange="NSE", auto_adjust=False)["Date"]).dt.normalize())
    print(s, "missing 2021-11-04:", pd.Timestamp("2021-11-04") not in dates)
PY
```

## 7. Method limitations

- READ-ONLY mandate respected: no file under `data/` or `src/` was modified; all probe scripts
  lived in `/tmp/opencode/`.
- `null_pct` for named-holders/dossier/insider/annual-reports uses whatever key column exists;
  where none matched it is recorded `N/A` rather than fabricated.
- `non_monotonic`/`dup_dates` on event-style datasets (deals, holders, actions) are descriptive
  counts, not automatically errors; interpretation is in §4.
- Findings are based solely on the local lake snapshot generated 2026-11-23; no network calls.

**VERIFY_CMD (amendment cycle 2, exact command from this worktree):**
`cd .worktrees/w1-stocks && PYTHONPATH=src /run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python -m pytest -q /run/media/esco/ed/dev/code/bfinance-datalake/tests/ --disable-warnings`
→ **161 passed, 3 failed**. The 3 failures are all in `tests/test_swarm_P2C.py`, a *different
task's* regression file that landed in the shared main-repo tests directory after this worktree
was branched: it requires `from typing import Any` in `src/datalake/sync.py`, a fix committed to
main at `7912334` (T-P2C) but absent from this pre-P2C worktree's `src`. Running the identical
command from the main repo root (which has that fix) gives **164 passed**. No T-02 code or data
was touched by this amendment (report-only), so there is no T-02 regression; src edits were
prohibited by the task scope and were not attempted.
