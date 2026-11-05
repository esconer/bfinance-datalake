# Upstox V2 Historical Candle CDN Integration

## Overview

`bfinance-datalake` leverages Upstox's public, unauthenticated V2 Historical Candle API to provide ultra-fast, cloud-compatible seeding of historical equity price data for **2000–2015**. 

Because this CDN is public and requires no API keys, OAuth logins, or broker credentials, GitHub Actions runners and cloud environments can download multi-year daily bars across the entire market without being blocked by exchange anti-bot mechanisms.

---

## Endpoint Specification

### URL Structure
```http
GET https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}
```

> **Note on Parameter Ordering:**  
> In Upstox's V2 API, **`to_date` precedes `from_date`** in the path. Passing them in reverse will result in empty payloads or invalid date range errors.

### Request Headers
```http
Accept: application/json
```
*(No Authorization or Bearer token is required for this public CDN endpoint).*

### Instrument Key Formats

Instrument keys must be URL-encoded (replacing `|` with `%7C` and spaces with `%20`):

| Asset Class | Key Syntax | URL-Encoded Example |
| :--- | :--- | :--- |
| **NSE Cash Equities** | `NSE_EQ\|{ISIN}` | `NSE_EQ%7CINE002A01018` *(RELIANCE)* |
| **BSE Cash Equities** | `BSE_EQ\|{ISIN}` | `BSE_EQ%7CINE002A01018` *(RELIANCE)* |
| **NSE Major Indices** | `NSE_INDEX\|{Index Name}` | `NSE_INDEX%7CNifty%2050` *(NIFTY 50)* |
| **NSE Sectoral Indices** | `NSE_INDEX\|{Index Name}` | `NSE_INDEX%7CNifty%20Bank` *(NIFTY BANK)* |
| **Volatility Index** | `NSE_INDEX\|India VIX` | `NSE_INDEX%7CIndia%20VIX` *(INDIA VIX)* |

---

## Data Schema & Fields

Each candle is returned as a 7-element JSON array in chronological descending order (newest first):

```json
[
  "2026-10-30T00:00:00+05:30",
  1170.4,
  1174.6,
  1152.5,
  1164.9,
  14753650,
  0
]
```

### Field Definitions

| Index | Field | Data Type | Description |
| :---: | :--- | :--- | :--- |
| **0** | `Timestamp` | ISO 8601 String | Date and time in Indian Standard Time (`+05:30`). |
| **1** | `Open` | Float | Opening price in INR. |
| **2** | `High` | Float | Highest traded price in INR. |
| **3** | `Low` | Float | Lowest traded price in INR. |
| **4** | `Close` | Float | Closing price in INR. |
| **5** | `Volume` | Integer | Total quantity of shares traded on that exchange for the bar. |
| **6** | `OI` | Integer | Open Interest (`0` for Cash Equities; non-zero for F&O contracts). |

### Corporate Action Adjustment Status

* **Prices are Split & Bonus Adjusted**: Historical prices on Upstox are retroactively adjusted for stock splits and bonus issues.
* **Live Proof**: Reliance Industries had a 1:1 bonus issue on `2024-10-28`. Raw unadjusted NSE closing prices prior to this date were `~2,660`. Upstox's feed serves retroactively adjusted prices (`~1,330`) continuously across the event:
  ```text
  2024-10-25 (Pre-Bonus):  Open: 1343.50 | Close: 1327.85 | Volume: 18,597,496
  2024-10-28 (Bonus Day):  Open: 1337.00 | Close: 1334.35 | Volume: 10,824,350
  ```

### Differences vs. Exchange Bhavcopies
Upstox candles provide fast OHLCV, but **do not** include exchange-reported delivery metrics:
* Deliverable Quantity (`DeliverableQty`): N/A (set to `pd.NA`).
* Delivery Percentage (`DeliveryPct`): N/A (set to `pd.NA`).
* Number of Trades (`Trades`): N/A (set to `pd.NA`).
* Turnover Value (`TurnoverValue`): N/A (set to `pd.NA`).

*(These delivery fields are collected by our datalake's primary NSE and BSE bhavcopy scrapers for 2016–Present).*

---

## Historical Boundaries ("From Where to Where")

Live probes across four decades (1990–2026) established the exact temporal limits of Upstox's database:

| Feed Type | Interval | Earliest Available Date | Latest Available Date | Coverage Notes |
| :--- | :---: | :---: | :---: | :--- |
| **NSE Equities** | `day` | **`2000-01-03`** | **Latest trading day** | Stocks listed before 2000 start on `2000-01-03`. Stocks listed later start on their IPO date (e.g. TCS starts `2004-08-25`). Pre-2000 returns `0` candles. |
| **BSE Equities** | `day` | **`2008-01-01`** | **Latest trading day** | Pre-2008 returns `0` candles on BSE. |
| **NSE Indices** | `day` | **`2000-01-03`** | **Latest trading day** | Full history for `Nifty 50`, `Nifty Bank`, and `India VIX`. Volume & OI are `0`. |
| **Intraday 1-Min** | `1minute` | **~3 Years Lookback** | **Real-time / Today** | 5-year lookbacks return `0` candles. |
| **Intraday 30-Min**| `30minute`| **~3 Years Lookback** | **Real-time / Today** | 5-year lookbacks return `0` candles. |
| **Higher Frames** | `week`, `month` | **`2000-01-03`** | **Latest trading day** | Aggregated from daily bars. |

---

## Technical Constraints & Error Handling

### 1. Maximum Query Window (`UDAPI1148`)
The API enforces a maximum window length per HTTP request of approximately **8 years** (~2,500 trading days). Requesting ranges exceeding 8–10 years fails with:
```json
{
  "status": "error",
  "errors": [
    {
      "errorCode": "UDAPI1148",
      "message": "Invalid date range"
    }
  ]
}
```
**Datalake Mitigation**: Our scraper batches requests by year or bounded sub-ranges (e.g. 1–5 years per request) to prevent triggering this constraint.

### 2. Rate Limits & Concurrency
* The CDN is backed by Cloudflare/AWS CloudFront caches.
* Our scraper uses an `asyncio.Semaphore` (concurrency limit: 4–8) with a polite `0.15s` sleep between requests.

---

## Usage in `bfinance-datalake`

### Implementation
The scraper logic is located in [`EquityEODScraper.seed_pre2016_via_upstox_cdn`](file:///e:/dev/code/bfinance-datalake/src/datalake/scrapers/equity_eod.py):
1. Reads active ISINs from `universe/nse_equity_master.parquet`.
2. Queries Upstox V2 CDN in bounded batches for the requested start and end years.
3. Normalizes columns to the unified datalake schema (`Date`, `Exchange`, `InstrumentId`, `Symbol`, `Series`, `ISIN`, `Open`, `High`, `Low`, `Close`, `Volume`).
4. Runs `repair_ohlc()` to guarantee data integrity (`High >= max(Open, Close)` and `Low <= min(Open, Close)`).
5. Writes partitions atomically into `equities/nse_cm_{year}.parquet`.
6. Records ingestion checkpoints in `state.db` under the `upstox_cloud_seed` dataset.

### CLI Usage
To seed historical 2000–2015 equity bars for the entire market:
```bash
# Seed 2000-2015 for top 100 liquid stocks
uv run datalake seed-cloud --start-year 2000 --end-year 2015 --limit 100

# Seed 2000-2015 for all listed NSE stocks
uv run datalake seed-cloud --start-year 2000 --end-year 2015
```

### Pre-2000 Fallback Architecture
For market dates between **1995 and 1999**, the datalake does not use Upstox. Instead, it routes through:
* **`eq.sync_range(date(1995, 1, 1), date(1999, 12, 31))`**: Uses official NSE legacy monthly bhavcopies (`cm{dd}{mon}{yyyy}bhav.csv.zip`).
* **`eq.backfill_nse_symbol_via_historical_or`**: Uses NSE's official `/api/historicalOR` endpoint to retrieve exact 1995–1999 raw exchange trades and delivery figures.
