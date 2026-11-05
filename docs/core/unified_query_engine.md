# Unified Query Engine & Ticker Facade (`UnifiedLakeEngine`)

The **Unified Query Engine** (`datalake.engine`) provides an ultra-low-latency (1–7 ms) unified querying interface over all 15 Indian financial datasets stored in the local Parquet data lake.

It eliminates the complexity of querying disparate partitions, different ticker conventions, corporate action adjustments, and historical symbol renames by providing an O(1) in-memory crosswalk index and a unified, yfinance-like API.

---

## Architecture Overview

```mermaid
flowchart LR
    Q["Client Query:<br/>lake.ticker('RELIANCE')<br/>or lake.ticker('500325')<br/>or lake.ticker('INE002A01018')"]
    
    subgraph Engine["UnifiedLakeEngine (datalake.engine)"]
        CW["O(1) Crosswalk + Rename Resolver<br/>(NSE &lt;-&gt; BSE &lt;-&gt; ISIN &lt;-&gt; Aliases)"]
        LRU["In-Memory LRU Cache +<br/>OHLCVAdjuster (Adj Close)"]
        PP["PyArrow Predicate Pushdown<br/>& DuckDB Zero-Copy SQL Views"]
    end

    subgraph Disk["Local Parquet Data Lake (data/)"]
        P1[("equities/<br/>nse_cm_*.parquet<br/>bse_cm_*.parquet")]
        P2[("fundamentals/<br/>xbrl_financials.parquet")]
        P3[("shareholding/<br/>nse/bse + named_holders")]
        P4[("derivatives / indices /<br/>actions / mf / deals / docs")]
    end

    Q --> CW --> LRU --> PP
    PP -->|Row-group byte skipping (1-7ms)| P1 & P2 & P3 & P4
```

### Three Core Pillars

1. **O(1) Crosswalk Index (`SecurityIdentity`):**
   Seamlessly maps any identifier — NSE Symbol (`"RELIANCE"`), BSE 6-digit Scrip Code (`"500325"`), ISIN (`"INE002A01018"`), or historical rename aliases (`"INFOSYSTCH"` $\to$ `"INFY"`, `"ETERNAL"` $\to$ `"ZOMATO"`) — to the canonical entity in microseconds.

2. **Predicate Pushdown & Symbol Clustering:**
   Leverages PyArrow's C++ predicate pushdown (`filters=[("Symbol", "in", aliases)]`). When parquet files are clustered by `["Symbol", "Date"]` via `engine.optimize_layout()`, row-group metadata in the Parquet footer allows the engine to skip up to 99% of file bytes on disk.

3. **Zero-Copy Analytical SQL Views (`engine.sql(...)`):**
   Auto-registers all Parquet tables as virtual SQL views in DuckDB without loading tables into RAM, allowing multi-table joins and market-wide 2,500-stock screener queries in ~10–25 milliseconds.

---

## Quickstart

```python
from datalake.engine import UnifiedLakeEngine

# 1. Initialize engine pointing to your local Parquet data lake
lake = UnifiedLakeEngine("./data")

# 2. Query by NSE symbol, BSE Scrip Code, or ISIN - all resolve to the same security
ticker = lake.ticker("500325")  # Reliance Industries
print(ticker.symbol)   # "RELIANCE"
print(ticker.isin)     # "INE002A01018"
print(ticker.bse_code) # "500325"

# 3. Fetch 30-year daily history with splits, dividends, and backward Adj Close
df_price = ticker.history(start="1995-01-01", auto_adjust=False)

# 4. Access XBRL financials, shareholding, corporate actions, deals, and annual reports
df_fin = ticker.quarterly_financials
df_shp = ticker.shareholding
df_act = ticker.actions
df_deals = ticker.deals
df_ar = ticker.annual_reports
```

---

## API Reference

### `UnifiedLakeEngine(lake_dir="./data")`

The primary engine instance that caches crosswalk mappings and serves queries across all 15 datasets.

#### Methods

- **`resolve(query: str | int) -> SecurityIdentity`**
  Resolves any NSE symbol, BSE code, or ISIN to a `SecurityIdentity(symbol, bse_code, isin, company_name, aliases, face_value, industry, listing_date)`. Strips suffixes like `.NS` or `.BO` automatically.

- **`ticker(query: str | int) -> LakeTicker`**
  Returns a `LakeTicker` facade for the resolved security.

- **`history(query, start=None, end=None, exchange="NSE", auto_adjust=False) -> pd.DataFrame`**
  Retrieves daily OHLCV + Delivery data. Backward-computes `Dividends`, `Stock Splits`, and `Adj Close` using corporate action records. When `auto_adjust=True`, open/high/low/close and volume are scaled by split/dividend factors.

- **`download(queries, start=None, end=None, exchange="NSE", auto_adjust=False) -> dict[str, pd.DataFrame]`**
  Vectorized multi-symbol OHLCV download that reads each annual Parquet partition only once using `Symbol IN (...)` pushdown, returning a dictionary mapping each canonical symbol to its adjusted DataFrame (25x–50x faster than sequential history calls).

- **`fundamentals(query, period_type=None, consolidated=True) -> pd.DataFrame`**
  Queries Ind-AS XBRL filings (P&L, Balance Sheet, Cash Flow). Filter by `period_type="Quarterly"` or `period_type="Annual"`.

- **`shareholding(query) -> pd.DataFrame`**
  Returns unified NSE and BSE quarterly promoter and public shareholding history, including promoter pledge percentages (2001–Present).

- **`named_holders(query) -> pd.DataFrame`**
  Returns detailed institutional holdings (FIIs, Mutual Funds, Insurance) and individual shareholders holding >1% equity.

- **`corporate_actions(query) -> pd.DataFrame`**
  Returns parsed dividends, splits, and bonus issues for the resolved security across both exchanges.

- **`option_chain(symbol="NIFTY", trade_date=None, expiry=None, exchange="NSE") -> pd.DataFrame`**
  Fetches EOD derivatives contracts (CE/PE, strike, settlement price, OI, change in OI) from yearly `nse_fo_YYYY.parquet` partitions. Defaults to the latest available trade date.

- **`index_history(index_name="NIFTY 50", start=None, end=None) -> pd.DataFrame`**
  Fetches index spot OHLC, Total Return Index (TRI), and valuation metrics (P/E, P/B, Dividend Yield).

- **`mutual_fund(scheme_code, start=None, end=None) -> pd.DataFrame`**
  Fetches historical Net Asset Value (NAV) time-series by AMFI Scheme Code.

- **`deals(query=None) -> pd.DataFrame`**
  Returns Bulk and Block deals. If `query` is specified, filters to that security.

- **`insider_trades(query=None) -> pd.DataFrame`**
  Returns SEBI PIT Regulation 7(2) insider trading and promoter pledge disclosures.

- **`concalls(query=None) -> pd.DataFrame`**
  Returns BSE earnings call transcripts, investor presentations, audio links, and credit ratings.

- **`annual_reports(query) -> pd.DataFrame`**
  Returns direct PDF and ZIP download links for 20+ years of NSE and BSE annual reports.

- **`optimize_layout() -> dict[str, int]`**
  Re-clusters all equity Parquet files on disk by `["Symbol", "Date"]` and derivatives partitions by `["Symbol", "Date", "ExpiryDate", "StrikePrice", "OptionType"]` with `row_group_size = 10_000` so PyArrow/DuckDB reads only contiguous single-stock row groups.

- **`sql(query: str) -> pd.DataFrame`**
  Executes high-performance SQL queries over DuckDB virtual views.

---

### `LakeTicker`

Convenience wrapper returned by `lake.ticker(...)`:

| Property / Method | Returns | Description |
| :--- | :--- | :--- |
| `.symbol` | `str` | Primary NSE trading symbol |
| `.isin` | `str \| None` | 12-character ISIN code |
| `.bse_code` | `str \| None` | BSE 6-digit Scrip Code |
| `.history(start, end, exchange, auto_adjust)` | `pd.DataFrame` | Daily adjusted OHLCV price series |
| `.financials` | `pd.DataFrame` | Annual Ind-AS XBRL financial statements |
| `.quarterly_financials` | `pd.DataFrame` | Quarterly Ind-AS XBRL financial statements |
| `.shareholding` | `pd.DataFrame` | Quarterly promoter & institutional shareholding |
| `.institutional_holders` | `pd.DataFrame` | Named >1% institutional and public holders |
| `.actions` | `pd.DataFrame` | Parsed corporate actions (splits, dividends) |
| `.deals` | `pd.DataFrame` | Bulk and block deals |
| `.insider_trades` | `pd.DataFrame` | SEBI PIT insider trading disclosures |
| `.concalls` | `pd.DataFrame` | Concall transcripts, presentations, audio links |
| `.annual_reports` | `pd.DataFrame` | Annual report PDF/ZIP URLs |

---

## Zero-Copy SQL Screener Example

Using DuckDB, you can query across datasets without manual merging:

```python
# Screen for high-delivery, high-promoter companies with strong cash balances
df_screen = lake.sql("""
    SELECT
        e.Symbol,
        e.Date,
        e.Close,
        e.DeliveryPct,
        f.Sales_Cr,
        f.NetProfit_Cr,
        f.TotalCashAndBank_Cr
    FROM equities_nse e
    LEFT JOIN financials f
      ON e.Symbol = f.Symbol AND f.PeriodType = 'Quarterly'
    WHERE e.Date = (SELECT MAX(Date) FROM equities_nse)
      AND e.Series = 'EQ'
      AND e.DeliveryPct > 55.0
    ORDER BY e.TurnoverValue DESC
    LIMIT 25;
""")
```

### Auto-Registered SQL Views

- `equities_nse`: All NSE cash market partitions (`nse_cm_*.parquet`)
- `equities_bse`: All BSE cash market partitions (`bse_cm_*.parquet`)
- `fo_nse`: NSE F&O daily contract bars (`nse_fo_*.parquet`)
- `indices`: Nifty 30-year OHLC, TRI, P/E, P/B (`nifty_30y_ohlc_tri_pe.parquet`)
- `indices_daily`: Daily index close files (`ind_close_all_*.parquet`)
- `financials`: XBRL financial line items (`xbrl_financials.parquet`)
- `shareholding`: NSE shareholding summaries
- `shareholding_bse`: BSE shareholding history
- `named_holders`: Individual and institutional holders (>1%)
- `actions_nse`: NSE corporate actions
- `actions_bse`: BSE corporate actions
- `mutual_funds`: AMFI NAV partitions (`amfi_nav_*.parquet`)
- `deals`: Bulk and block deals
- `insider_pit`: SEBI PIT Regulation 7(2) insider trades (`deals/insider_trading_pit.parquet`)
- `concalls`: BSE earnings call transcripts & investor filings (`documents/concalls_and_filings.parquet`)
- `participant_oi`: Participant Open Interest by client category
- `fii_dii`: Daily Cash FII/DII net flows
- `annual_reports`: Annual report document URLs

---

## Performance Benchmarks

| Operation | Cold Run | Warm Run (LRU Cache) |
| :--- | :--- | :--- |
| Crosswalk Resolution (`resolve`) | 120 µs | < 5 µs |
| 1-Year Stock OHLCV (`history`) | 4.2 ms | 0.8 ms |
| 30-Year Complete History (1995–2026) | 18 ms | 1.1 ms |
| XBRL Balance Sheet & P&L | 3.5 ms | 0.9 ms |
| Market-Wide Option Chain Lookup | 5.1 ms | 1.2 ms |
| DuckDB Cross-Dataset 2,500-Stock Screener | 18 ms | 12 ms |
