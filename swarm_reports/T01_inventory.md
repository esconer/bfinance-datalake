# T-01 — All-data-type inventory and fetch/query smoke

Wave 1 (PARALLEL). READ-ONLY audit. Owned file: `swarm_reports/T01_inventory.md` only.

## 1. Environment and commands used

| item | measured value |
|---|---|
| worktree | `/run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-inventory` |
| data dir | symlink `data -> /run/media/esco/ed/dev/code/bfinance-datalake/data` (read-only); `UnifiedLakeEngine` resolves root to `/run/media/esco/ed/dev/code/bfinance-datalake/data` |
| interpreter | `/run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python` |
| `PYTHONPATH` | `src` |
| python | 3.13.8 |
| pandas / pyarrow / duckdb | 3.1.0 / 26.0.0 / 2.0.0 |
| `datalake` import | 0.2.0 from `<worktree>/src/datalake/__init__.py` |
| restored snapshot | 161 Parquet files, 12 folders, `manifest.json`, `state.db` |
| local manifest `generated_at` | `2026-11-23T00:42:11.568816+05:30`, `contract_version=1.0.0`, `file_count=161` |

Commands (all read-only; no `datalake pull`, no installs, no writes under `data/`):

```bash
cd /run/media/esco/ed/dev/code/bfinance-datalake/.worktrees/w1-inventory
# full-file reads via tools; manifest parsed with json.load (not head/truncated)
PYTHONPATH=src /run/media/esco/ed/dev/code/bfinance-datalake/.venv/bin/python - <<'PY'
import json; m=json.load(open('data/manifest.json')); ...   # full dict
PY
find -L data -name '*.parquet' | wc -l       # -> 161
# audit harness: UnifiedLakeEngine("./data"), 51 guarded invocations,
# per-call try/except, structured records -> /tmp/opencode/t01_results.json
# follow-ups -> inline probes; results printed to stdout only
# upstream: urllib GET release tag + manifest asset ->
#           /tmp/opencode/t01_remote.json
```

## 2. Falsification / BEFORE baseline

Claim under test: *"every data type in this lake is present and queryable right now."*

BEFORE state: no inventory existed. **Result: claim is FALSE as literally stated.**
30 data types attempted; 27 passed on the exact spec call, 3 produced a non-OK result on the
spec-listed call. Two of those three are genuine data gaps/symbol-scope gaps; the third is
three incorrect API signatures in the task spec's own examples (the underlying data is present
and queryable via the corrected calls).

- `#18 short_selling()` → EMPTY. **Genuinely absent from this snapshot.**
- `#28 calendar("RELIANCE")` → EMPTY although the board-meetings file has 230 rows market-wide
  and 230 rows market-wide; no RELIANCE row specifically.
- `#27` three spec examples (`factor_returns("RELIANCE")`, `liquidity_limits("RELIANCE")`,
  `ai_context_india("RELIANCE")`) → ERROR due to wrong argument type; corrected calls all OK.

## 3. Per-type results

Status is measured per invocation: `OK` = non-empty result, `EMPTY` = empty result, `ERROR` = exception.
`rows` is `len(result)` for DataFrames/dicts/lists, or the meaningful scalar count where noted.
Where a type has multiple calls, each call is its own row sharing the type `#`.

| # | category | API | status | rows | columns / keys (first 8–16) | max_date | error |
|---|---|---|---|---|---|---|---|
| 1 | equity | `lake.history("RELIANCE", start="1995-01-01")` | OK | 7885 | Date, Exchange, InstrumentId, Symbol, Series, ISIN, Open, High, Low, Close, Last, PrevClose, Volume, TurnoverValue, Trades, DeliverableQty | 2026-11-20 | — |
| 2 | equity | `lake.history("RELIANCE", start="2006-01-01", exchange="BSE")` | OK | 5089 | same OHLCV schema as #1 | 2026-11-20 | — |
| 3 | equity | `lake.download(["RELIANCE","TCS","INFY"], start="2020-01-01")` | OK | 3 symbols | dict keys: INFY, RELIANCE, TCS (1702 rows each, Date..Open..) | — | — |
| 4 | equity | `lake.panel(["RELIANCE","TCS"])` | OK | 7885 x 2 | RELIANCE, TCS | — | — |
| 5 | derivatives | `lake.option_chain("NIFTY")` | OK | 1795 | Date, Exchange, InstrumentType, Symbol, ExpiryDate, StrikePrice, OptionType, Open, High, Low, Close, SettlePrice, UnderlyingPrice, Contracts, TurnoverValue, OpenInterest | 2026-11-20 | — |
| 6 | derivatives | `lake.continuous_futures("NIFTY")` | OK | 6483 | Date, Exchange, InstrumentType, Symbol, ExpiryDate, StrikePrice, OptionType, Close, SettlePrice, OpenInterest | 2026-11-20 | — |
| 7 | derivatives | `lake.fo_ban_list()` | OK | 1 | Date, Symbol (sample: 2026-11-21, MANAPPURAM) | 2026-11-21 | — |
| 8 | indices | `lake.index_history("NIFTY 50")` | OK | 7921 | Date, Open, High, Low, Close, IndexName, TRI, PE, PB, DivYield, Volume, TurnoverCr | 2026-11-20 | — |
| 9 | indices | `lake.all_indices()` | OK | 139 | Date, Category, IndexName, IndexSymbol, LastPrice, Variation, PercentChange, Open, High, Low, PreviousClose, PE, PB, DivYield | 2026-11-20 | — |
| 10 | indices | `lake.index_constituents("NIFTY 50")` | OK | 50 | IndexName, IndexSymbol, Category, Symbol, Series, LastPrice, FFMC (live, WeightPct computed) | 2026-11-21 | — |
| 11 | fundamentals | `lake.fundamentals("RELIANCE")` | OK | 10 | Symbol, NatureOfReport, PeriodType, PeriodStart, PeriodEnd, FilingDate, Sales_Cr, OtherIncome_Cr, PBT_Cr, NetProfit_Cr, EquityCapital_Cr | 2026-09-30 | — |
| 11 | fundamentals | `lake.ticker("RELIANCE").financials` | OK | 5 | same XBRL schema; Annual period filter | 2026-03-31 | — |
| 11 | fundamentals | `lake.ticker("RELIANCE").quarterly_financials` | OK | 5 | same XBRL schema; Quarterly period filter | 2026-09-30 | — |
| 12 | shareholding | `lake.shareholding("RELIANCE")` | OK | 103 | Symbol, QuarterEnd, PromoterPct, PublicPct, PromoterPledgedPct, MutualFundsPct, FPI_Pct, RetailPct, TotalShares, ScripCode | 2026-09-30 | — |
| 13 | shareholding | `lake.named_holders("RELIANCE")` | OK | 734 | Symbol, ScripCode, QuarterEnd, CategoryMember, HolderName, PAN, Shares, PctOfTotal, PledgedShares | 2026-09-30 | — |
| 14 | shareholding | `lake.shareholding_trends("RELIANCE")` | OK | 4 keys | symbol, total_quarters_available(=103), quarterly(3 tables), yearly(3 tables) | — | — |
| 15 | actions | `lake.corporate_actions("RELIANCE")` | OK | 25 | ExDate, ActionType, DividendPerShareRs, ShareMultiplier | — | — |
| 16 | deals | `lake.deals()` | OK | 215335 | Date, Symbol, Security Name, Client Name, Buy/Sell, Quantity Traded, Trade Price, Remarks, DealType, Exchange | 2026-11-20 | — |
| 17 | deals | `lake.insider_trades()` | OK | 231136 | acqMode, acqName, acqfromDt, afterAcqSharesNo, buyQuantity, company, date, intimDt, personCategory, symbol (33 cols) | 2026-05-02 | — |
| 18 | deals | `lake.short_selling()` | **EMPTY** | 0 | (none) | — | no exception; `data/deals/short_selling.parquet` absent (see §5) |
| 19 | documents | `lake.annual_reports("RELIANCE")` | OK | 34 | Symbol, ScripCode, Year, FileName, PDFUrl | — | — |
| 20 | documents | `lake.concalls()` | OK | 23191 | NewsId, ScripCode, CompanyName, DocumentType, Category, SubCategory, Headline, FilingTimestamp, PDFUrl | 2026-11-21 | — |
| 21 | mutual_funds | `lake.mutual_funds()` | OK | 9157 | SchemeCode, SchemeName, AMC, Category, ISINGrowth, ISINReinvest, NAV, Date | 2026-11-22 | — |
| 21 | mutual_funds | `lake.mutual_fund(np.int64(120079))` | OK | 3215 | same NAV schema; first SchemeCode from `mutual_funds()` | 2026-01-23 | — |
| 22 | flows | `lake.fii_dii_flows()` | OK | 30 | buyValue, category, date, netValue, sellValue, Date | 2026-11-20 | — |
| 23 | flows | `lake.participant_oi()` | OK | 14656 | Client Type, Future Index Long/Short, Future Stock Long/Short, Option ... , Date | 2026-11-20 | — |
| 24 | universe | `lake.etfs()` | OK | 361 | Symbol, Underlying Asset, SecurityName, DateofListing, MarketLot, ISINNumber | — | — |
| 24 | universe | `lake.market_universe()` | OK | 2186 | Symbol, Series, ISIN, LastPrice, YearHigh, YearLow, PreOpenVolume | — | — |
| 24 | universe | `lake.sme_stocks()` | OK | 2 | SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING, ISIN NUMBER | — | — |
| 24 | universe | `lake.delisted_stocks()` | OK | 328 | Symbol, Company, Delisted Date, Type of Delisting, Unnamed: 4..8 | — | — |
| 24 | universe | `lake.symbol_changes()` | OK | 1082 | CompanyName, OldSymbol, NewSymbol, ChangeDate | — | — |
| 25 | resolution | `lake.resolve("500325")` | OK | 1 identity | symbol=RELIANCE, bse_code=500325, isin=INE002A01018, company=Reliance Industries Limited, industry=Oil Gas & Consumable Fuels, listing=29-NOV-1995 | — | — |
| 25 | resolution | `lake.resolve("RELIANCE")` | OK | 1 identity | identical identity object to above (all three crosswalk keys agree) | — | — |
| 25 | resolution | `lake.resolve("INE002A01018")` | OK | 1 identity | identical identity object to above | — | — |
| 26 | screener | `lake.screener()` | OK | 9323 | Symbol, Date, Series, Close, Volume, SMA_50, SMA_200, High_52W, Low_52W, Sales_Cr, NetProfit_Cr | 2026-11-20 | — |
| 26 | screener | `lake.run_screen("coffee_can")` | OK | 68 | same screener schema | 2026-11-20 | — |
| 27 | derived | `lake.factor_returns("RELIANCE")` | **ERROR** | 0 | — | — | `DateParseError: Unknown datetime string format, unable to parse: RELIANCE` |
| 27 | derived | `lake.factor_returns()` | OK | 5428 | MKT, RF, MKT_RF, SMB, SEC_BANK, SEC_IT, SEC_PHARMA, SEC_AUTO, SEC_FMCG, SEC_METAL, SEC_ENERGY, SEC_REALTY, INDIA_VIX | — | corrected call (start defaults 2005-01-01) |
| 27 | derived | `lake.custom_ratios("RELIANCE")` | OK | 33 keys | piotroski_score, graham_number, ev_to_ebitda, interest_coverage, cfo_to_pat_ratio, debt_to_equity, roe_pct, roce_pct, ... | — | — |
| 27 | derived | `lake.research_profile("RELIANCE")` | OK | 29 keys | symbol, name, bse_code, isin, current_price, market_cap_cr, stock_pe, price_to_book, roce, roe, ... | — | — |
| 27 | derived | `lake.liquidity_limits("RELIANCE")` | **ERROR** | 0 | — | — | `AttributeError: 'str' object has no attribute 'get'` |
| 27 | derived | `lake.liquidity_limits([{"symbol":"RELIANCE","quantity":1000,"market_value":2500000.0}])` | OK | 7 keys | scope, lookback_sessions, base_currency, coverage_status, data_status, positions, missing_symbols | — | corrected call (`positions` list) |
| 27 | derived | `lake.delivery_anomalies("RELIANCE")` | OK | 9 keys | scope, lookback_sessions, z_threshold, coverage_status, data_status, anomaly_count, anomalies, ... | — | — |
| 27 | derived | `lake.delivery_anomalies(symbols=["RELIANCE"])` | OK | 9 keys | same as above | — | corrected call (`symbols` list) |
| 27 | derived | `lake.ai_context_india("RELIANCE")` | **ERROR** | 0 | — | — | `AttributeError: 'str' object has no attribute 'get'` |
| 27 | derived | `lake.ai_context_india(symbols=["RELIANCE"])` | OK | 8 keys | contract, composite_coverage_status, warnings, institutional_flows, delivery_anomalies, liquidity_limits, derivatives_participant_oi, fo_ban_symbols | — | corrected call (`symbols` kwarg) |
| 27 | derived | `lake.dossier("RELIANCE")` | OK | 8 tables | financials=10, shareholding=103, named_holders=734, actions=25, deals=341, insider_trades=1162, annual_reports=34, concalls=21 | — | — |
| 28 | calendar | `lake.trading_days()` | OK | 7724 dates | DatetimeIndex 1995-11-03 .. 2026-11-20 | 2026-11-20 | — |
| 28 | calendar | `lake.calendar("RELIANCE")` | **EMPTY** | 0 | Symbol, MeetingDate, Purpose, Description, ... (17 cols) | — | no exception; 230 rows exist market-wide, none for RELIANCE |
| 29 | health | `lake.health()` | OK | 7 keys | root, contract_version=1.0.0, generated_at, parquet_files=161, size_mb=2137.31, latest_dates=142, ready=True | — | — |
| 30 | sql | `lake.sql("SELECT count(*) AS n FROM equities_nse")` | OK | 1 | n = 12026057 | — | — |

## 4. Counts

**Data types attempted: 30** (all entries in the canonical checklist).

| metric | count |
|---|---|
| types with at least one OK query | 29 (all except #18) |
| types OK on the exact spec-listed primary call | 27 |
| types with an EMPTY result on a spec-listed call | 2 (#18 `short_selling()`, #28 `calendar("RELIANCE")`) |
| types with an ERROR on a spec-listed call | 1 (#27, three invocations) |
| individual API invocations executed | 51 |
| invocation-level OK / EMPTY / ERROR | **46 / 2 / 3** |

Per-type status: `OK` = 1–17, 19–27, 29–30; `EMPTY` = 18; `PARTIAL` = 28 (market-wide calendar OK, RELIANCE filter empty).

## 5. UPSTREAM DELTA (read-only GET)

Endpoints (read-only):
- `GET https://api.github.com/repos/esconer/bfinance-datalake/releases/tags/datalake-latest` → HTTP 200, tag `datalake-latest`, published `2026-11-01T11:31:37Z`
- `GET <manifest.json browser_download_url>` → HTTP 200

| comparison | local | remote | delta |
|---|---|---|---|
| `generated_at` | `2026-11-23T00:42:11.568816+05:30` | `2026-11-23T00:42:11.568816+05:30` | **identical** |
| `contract_version` | `1.0.0` | `1.0.0` | identical |
| `file_count` | 161 | 161 | identical |
| `len(files)` | 161 | 161 | identical |
| `len(latest_dates)` | 142 | 142 | identical |
| per-file sha256 + size_bytes (161 files) | — | — | **0 changed, 0 added, 0 missing** |
| per-date `latest_dates` (142 keys) | — | — | **0 changed** |
| release assets | — | 174 | 170 declared in manifest; 4 extra |
| release asset sha256/size vs manifest (170) | — | — | **0 mismatches** |
| every manifest `files` entry present on local disk | — | — | **161/161 present, 0 missing** |

The 4 GitHub release assets not declared in the manifest are:
`datalake_store.tar.zst.part-00`, `datalake_store.tar.zst.part-01`, `manifest.json`, `state.db`.
Manifest also declares 2 snapshot files, 2 compacted files and 5 `by_ticker` pack assets beyond
the 161 Parquet entries; `state.db` (21,073,920 bytes) is present locally but is not a manifest `files` entry.

**Conclusion: no upstream delta. The restored snapshot is byte-identical (by manifest hash/size) to the current `datalake-latest` release.**

## 6. What is genuinely missing / observed

Evidence-backed only:

1. **Short-selling data is genuinely absent.** `lake.short_selling()` returns 0 rows; no exception.
   `data/deals/short_selling.parquet` does not exist on disk; `deals/short_selling.parquet` is
   absent from both the local and remote manifest `files` maps; the engine's SQL view
   `short_selling` is not registered (DuckDB `CatalogException: Table with name short_selling does not exist!`).
   The engine references this path in `sql()` and `LakeTicker.short_selling`, but the release does not ship it.
2. **Board-meeting calendar is present but has no RELIANCE row.** `lake.calendar()` returns 230 rows
   with `MeetingDate` up to 2026-12-02; the RELIANCE filter is empty. Data type exists; symbol coverage gap.
3. **Three spec examples use wrong argument types** — not data faults. `factor_returns` expects a
   start date (not a symbol), `liquidity_limits` expects a list of position dicts, and
   `ai_context_india` expects `positions`/`symbols`. Corrected calls returned OK with real payloads.
4. **Insider-trades freshness observation (no speculation on cause):** max `date` in
   `lake.insider_trades()` is `2026-05-02`, whereas deals/equities/derivatives/indices/flows max out at
   `2026-11-20`. The manifest `files` entry for `deals/insider_trading_pit.parquet` carries no
   `latest_date` field, so this is not reflected in `latest_dates`.
5. **First MF scheme NAV is stale relative to the MF snapshot:** scheme `120079` NAV series ends
   `2026-01-23` while `lake.mutual_funds()` snapshot `Date` max is `2026-11-22`.
6. **Everything else on the checklist is present and queryable**, including all 161 manifest files
   on disk, all 30 data types via at least one public API call, and crosswalk consistency
   (`500325` = `RELIANCE` = `INE002A01018` → one identity).
