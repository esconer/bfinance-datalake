# Nifty Indices Historical & Snapshot Data Integration

## Overview

`bfinance-datalake` integrates institutional-grade Indian equity index data across a 30-year span (1995–Present). To capture the true performance of the Indian market, backtesting and quantitative research require both **Price Return (PR)** and **Total Return (TRI)** benchmarks, along with index valuation ratios (**P/E**, **P/B**, **Dividend Yield**).

The datalake employs a dual-stream ingestion architecture:
1. **30-Year Continuous Multi-Index Bulk Feed**: Ingests daily Open, High, Low, Close (OHLC), Total Return Index (TRI), Price-to-Earnings (P/E), Price-to-Book (P/B), and Dividend Yield (DivYield) for core benchmark and sectoral indices via the official Sitefinity MVC API on `niftyindices.com` (with automatic failover to NSE's `/api/historicalOR/indicesHistory`).
2. **Market-Wide Daily All-Indices Snapshot**: Ingests daily exchange-wide index closing summaries (`ind_close_all_{DDMMYYYY}.csv`) from the NSE Archives (2012–Present), capturing over 120 thematic, broad-market, sectoral, strategy, and fixed-income indices.

---

## Benchmark Indices Covered

The bulk 30-year pipeline tracks 15 core benchmark, broad-market, and sectoral indices:

| Index Symbol / Name | Category | Base Date | Inception Base Value |
| :--- | :--- | :---: | :---: |
| **NIFTY 50** | Broad Benchmark | `1995-11-03` | 1,000.00 |
| **NIFTY NEXT 50** | Large Cap Junior | `1996-11-04` | 1,000.00 |
| **NIFTY 100** | Large Cap Broad | `2003-01-01` | 1,000.00 |
| **NIFTY 200** | Large & Midcap | `2004-01-01` | 1,000.00 |
| **NIFTY 500** | Broad Market | `1999-06-07` | 1,000.00 |
| **NIFTY MIDCAP 150** | Midcap Benchmark | `2005-04-01` | 1,000.00 |
| **NIFTY SMALLCAP 250**| Smallcap Benchmark | `2005-04-01` | 1,000.00 |
| **NIFTY BANK** | Sectoral (Banking) | `2000-01-01` | 1,000.00 |
| **NIFTY IT** | Sectoral (Technology) | `1996-01-01` | 1,000.00 |
| **NIFTY AUTO** | Sectoral (Automobile) | `2004-01-01` | 1,000.00 |
| **NIFTY PHARMA** | Sectoral (Pharmaceuticals)| `2001-01-01` | 1,000.00 |
| **NIFTY FMCG** | Sectoral (Consumer Goods)| `1996-01-01` | 1,000.00 |
| **NIFTY METAL** | Sectoral (Metals & Mining)| `2004-01-01` | 1,000.00 |
| **NIFTY REALTY** | Sectoral (Real Estate) | `2006-12-29` | 1,000.00 |
| **NIFTY ENERGY** | Sectoral (Energy & Power) | `2001-01-01` | 1,000.00 |

---

## Endpoint Specifications

### 1. NiftyIndices Sitefinity MVC Endpoints (Primary Bulk)

`niftyindices.com` operates on a Sitefinity MVC content management backend.

> [!IMPORTANT]
> **Critical URL Route Rule**: The endpoints must be invoked **without** the legacy `.aspx` extension (`/BackPage/{method}`). Calling `/BackPage.aspx/{method}` results in HTTP connection drops or 404 errors.

#### Request Headers
```http
POST https://www.niftyindices.com/BackPage/{method}
Content-Type: application/json; charset=UTF-8
Accept: application/json, text/javascript, */*; q=0.01
X-Requested-With: XMLHttpRequest
Origin: https://www.niftyindices.com
Referer: https://www.niftyindices.com/reports/historical-data
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
```

#### Request Payload Structure
The endpoint accepts a single key `cinfo` containing a pseudo-JSON serialized string:
```json
{
  "cinfo": "{'name':'NIFTY 50','startDate':'14-Oct-2024','endDate':'15-Oct-2024','indexName':'NIFTY 50'}"
}
```

#### Methods and Parameter Parameters:
| Method Name | Metric | Extra Parameters in `cinfo` |
| :--- | :--- | :--- |
| **`getHistoricaldatatabletoString`** | Daily OHLC | *(None)* |
| **`getTotalReturnIndexString`** | Total Return Index (TRI & NTR) | *(None)* |
| **`getpepbHistoricaldataDBtoString`** | P/E, P/B, Dividend Yield | `,'pe':'true','pb':'true','dy':'true'` |

> [!WARNING]
> The OHLC method name is **`getHistoricaldatatabletoString`**. The historical method `getHistoricaldataDBtoString` is defunct and permanently times out.

---

### 2. NSE Historical Index Fallback Endpoint

When `niftyindices.com` encounters intermittent downtime, maintenance windows, or anti-bot throttling, the scraper automatically fails over to NSE's official archive API.

#### Request Headers & URL
```http
GET https://www.nseindia.com/api/historicalOR/indicesHistory?indexType=NIFTY%2050&from=14-10-2024&to=15-10-2024
Referer: https://www.nseindia.com/companies-listing/corporate-filings-financial-results
Accept: application/json, text/plain, */*
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
```

> [!NOTE]
> `/api/indicesHistory` is deprecated and returns HTTP 404. The active endpoint is `/api/historicalOR/indicesHistory`. The session must be primed with initial cookies from `https://www.nseindia.com`.

---

### 3. NSE All-Indices Daily Snapshot CSV (`ind_close_all`)

Every trading evening, NSE Archives publishes a single consolidated CSV file containing closing metrics for all active indices.

#### URL Pattern
```http
GET https://nsearchives.nseindia.com/content/indices/ind_close_all_{DDMMYYYY}.csv
```
* Example for October 15, 2024: `https://nsearchives.nseindia.com/content/indices/ind_close_all_15102024.csv`
* Example for September 3, 2012: `https://nsearchives.nseindia.com/content/indices/ind_close_all_03092012.csv`

---

## Data Schemas & Field Mappings

### 1. NiftyIndices Bulk Payloads (Live Verified)

#### A. OHLC Response (`getHistoricaldatatabletoString`)
Returned directly as a JSON array of daily index records:
```json
[
  {
    "RequestNumber": "His63929124518695219400",
    "Index Name": "",
    "INDEX_NAME": "Nifty 50",
    "HistoricalDate": "15 Oct 2024",
    "OPEN": "25186.3",
    "HIGH": "25212.05",
    "LOW": "25008.15",
    "CLOSE": "25057.35"
  }
]
```

#### B. Total Return Index Response (`getTotalReturnIndexString`)
```json
[
  {
    "RequestNumber": "TRI63929124518882723000",
    "Index Name": "Nifty 50",
    "Date": "15 Oct 2024",
    "TotalReturnsIndex": "37196.69",
    "NTR_Value": "33594.43"
  }
]
```

#### C. Valuation Ratios Response (`getpepbHistoricaldataDBtoString`)
```json
[
  {
    "RequestNumber": "pepb6392912451941918460",
    "Index Name": "Nifty 50",
    "pe": "23.55",
    "pb": "3.75",
    "divYield": "1.2",
    "DATE": "15 Oct 2024"
  }
]
```

> [!NOTE]
> **Field Name Inconsistency Across Endpoints:**
> Notice that the date field key differs across these three payloads:
> - OHLC payload uses: `"HistoricalDate"`
> - TRI payload uses: `"Date"`
> - P/E & P/B payload uses: `"DATE"` (all uppercase)
> 
> In `bfinance-datalake`, all three are normalized into `Date` (`pd.Timestamp`) during merge.

---

### 2. NSE Fallback Payload (`historicalOR/indicesHistory`)

Returned wrapped in a `data.indexCloseOnlineRecords` object:
```json
{
  "data": {
    "indexCloseOnlineRecords": [
      {
        "EOD_INDEX_NAME": "NIFTY 50",
        "EOD_OPEN_INDEX_VAL": 25186.3,
        "EOD_HIGH_INDEX_VAL": 25212.05,
        "EOD_LOW_INDEX_VAL": 25008.15,
        "EOD_CLOSE_INDEX_VAL": 25057.35,
        "HIT_TURN_OVER": 29113.88,
        "HIT_TRADED_QTY": 257228772,
        "EOD_TIMESTAMP": "15-OCT-2024",
        "HI_TIMESTAMP": "2024-10-14T18:30:00.000Z"
      }
    ]
  }
}
```

---

### 3. All-Indices Daily Snapshot Schema (`ind_close_all`)

The CSV contains 13 columns covering closing prices, daily volume, turnover, and valuation metrics:

| CSV Header | Target Schema Column | Data Type | Description |
| :--- | :--- | :---: | :--- |
| `Index Name` | `IndexName` | `string` | Canonical name of index (e.g. `Nifty 50`, `Nifty IT`) |
| `Index Date` | `Date` | `datetime64[ns]` | Trading date (`DD-MM-YYYY`) |
| `Open Index Value` | `Open` | `float64` | Opening index level (falls back to `Close` if `-`) |
| `High Index Value` | `High` | `float64` | Intraday high (falls back to `Close` if `-`) |
| `Low Index Value` | `Low` | `float64` | Intraday low (falls back to `Close` if `-`) |
| `Closing Index Value`| `Close`| `float64` | Official closing index level |
| `Points Change` | `PointsChange` | `float64` | Net points change from previous trading session |
| `Change(%)` | `ChangePct` | `float64` | Percentage change from previous session |
| `Volume` | `Volume` | `Int64` | Total number of underlying shares traded |
| `Turnover (Rs. Cr.)`| `TurnoverCr` | `float64` | Total value traded in ₹ Crores |
| `P/E` | `PE` | `float64` | Index price-to-earnings ratio |
| `P/B` | `PB` | `float64` | Index price-to-book ratio |
| `Div Yield` | `DivYield` | `float64` | Index dividend yield percentage |

#### Sample Live Row Comparison (2012 vs 2024):
* **2012-09-03 (`ind_close_all_03092012.csv`)**:
  ```csv
  Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
  S&P CNX Nifty,03-09-2012,5276.5,5295.8,5243.15,5253.75,-4.75,-.09,110380869,4015.55,17.63,2.86,1.58
  ```
  *(Note the legacy branding `S&P CNX Nifty` prior to November 2012).*
* **2024-10-15 (`ind_close_all_15102024.csv`)**:
  ```csv
  Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
  Nifty 50,15-10-2024,25186.3,25212.05,25008.15,25057.35,-70.6,-.28,257228772,29113.88,23.55,3.75,1.2
  ```

---

## Historical Boundaries ("From Where to Where")

Extensive live probing across historical archives verified the exact cutover and start dates for each dataset:

| Feed Type | Target File Pattern | Earliest Date | Latest Date | Historical Coverage Notes |
| :--- | :--- | :---: | :---: | :--- |
| **Nifty 50 OHLC** | `indices/nifty_30y_ohlc_tri_pe.parquet` | **`1995-11-03`** | **Latest trading day** | Official launch date with base value `1000.0`. (Synthetic back-calculated OHLC exists to Nov 1994). |
| **Nifty 50 TRI** | `indices/nifty_30y_ohlc_tri_pe.parquet` | **`1999-06-30`** | **Latest trading day** | Total Return series officially inaugurated mid-1999. Pre-1999 TRI is filled as `pd.NA`. |
| **Nifty 50 PE / PB** | `indices/nifty_30y_ohlc_tri_pe.parquet` | **`1999-01-01`** | **Latest trading day** | Valuation metrics published continuously from 1999. |
| **Broad & Sectoral Indices** | `indices/nifty_30y_ohlc_tri_pe.parquet` | **Inception Dates** (1996–2006) | **Latest trading day** | Inception varies: Nifty IT (`1996-01-01`), Nifty Bank (`2000-01-01`), Midcap 150 (`2005-04-01`). |
| **All-Indices Daily Snapshot** | `indices/ind_close_all_{year}.parquet` | **`2012-09-03`** | **Latest trading day** | Standardized continuous archive began Sept 2012. 1995–Aug 2012 returns 404. |

---

## Technical Constraints & Error Handling

### 1. 365-Day Window Chunking
The Sitefinity endpoint enforces an internal execution timeout when query windows exceed ~1 year (~250 trading sessions).
* **Mitigation**: `IndicesScraper._post_nifty()` and `_fetch_nse_historical_or_index()` partition backfill requests into deterministic **364-day bounded sub-windows** (`cur_end = min(end, cur_start + timedelta(days=364))`).

### 2. TLS Fingerprinting & Bot Mitigation
Both `niftyindices.com` and `nseindia.com` employ Akamai and Cloudflare edge firewalls. Standard Python `requests` or `urllib` calls fail with TLS handshake resets or HTTP 403 Forbidden.
* **Mitigation**: The datalake uses `ResilientHTTPClient` with `curl_cffi` impersonating a full Chrome 124 TLS client (JA3/JA4 fingerprinting bypass).

### 3. Holiday Freezing & State Tracking
Weekend dates and national market holidays return HTTP 404 for `ind_close_all_{DDMMYYYY}.csv`.
* **Mitigation**: `StateStore` records `HOLIDAY` watermarks for missing trading days older than `holiday_freeze_days` (default 7 days). Older holiday entries are never retried, preventing redundant network calls.

---

## Usage in `bfinance-datalake`

### Implementation
The ingestion logic is encapsulated in [`IndicesScraper`](file:///e:/dev/code/bfinance-datalake/src/datalake/scrapers/indices.py):
1. **`backfill_30y_bulk(start_date, end_date, indices)`**:
   - Queries OHLC, TRI, and Valuation endpoints concurrently via `asyncio.gather()`.
   - Falls back to `_fetch_nse_historical_or_index()` if the primary endpoint returns empty.
   - Cleans and normalizes columns into `Date`, `IndexName`, `Open`, `High`, `Low`, `Close`, `TRI`, `PE`, `PB`, `DivYield`.
   - Merges and upserts partitions into `indices/nifty_30y_ohlc_tri_pe.parquet`.
2. **`sync_daily_snapshots(start_date, end_date)`**:
   - Downloads daily market-wide CSV snapshots via `_fetch_day()` under bounded concurrency (`archive_concurrency = 4`).
   - Normalizes index names, cleans invalid numeric strings (`"-"`), and validates minimum row counts (>15 indices).
   - Writes annual partitioned parquets into `indices/ind_close_all_{year}.parquet`.

### CLI Usage
To run a backfill across all indices or execute the daily scheduled pipeline:

```bash
# 1. Backfill 30 years of Nifty bulk history (OHLC + TRI + PE)
uv run datalake backfill --start 1995-11-03 --end 2026-11-01

# 2. Backfill all-indices daily snapshots from inception (2012-09-03 onwards)
uv run datalake backfill --start 2012-09-03 --end 2026-11-01

# 3. Daily update (masters, corporate actions, and daily index snapshots)
uv run datalake daily --lookback 7
```

### Python API Integration
```python
from datalake.storage import read_table_safe
from pathlib import Path

lake_dir = Path("./data")

# Read 30-year multi-index history
df_30y = read_table_safe(lake_dir / "indices" / "nifty_30y_ohlc_tri_pe.parquet")
nifty50 = df_30y[df_30y["IndexName"] == "NIFTY 50"].sort_values("Date")
print(nifty50[["Date", "Open", "High", "Low", "Close", "TRI", "PE", "PB"]].tail())

# Read market-wide snapshot for a specific year
df_2024 = read_table_safe(lake_dir / "indices" / "ind_close_all_2024.parquet")
print(f"Total index snapshot records for 2024: {len(df_2024):,}")
```
