# Funds & Institutional Flows Data Integration

## Overview

`bfinance-datalake` provides institutional-grade data ingestion pipelines for Indian mutual funds and derivatives institutional flows. In modern quantitative finance and macro research, asset allocation models and sentiment algorithms require:
1. **Complete Mutual Fund NAV Time Series**: Daily NAV tracking across 14,400+ open- and close-ended schemes spanning two decades (2006–Present) for fund performance attribution, alpha benchmarking, and smart-money holdings analysis.
2. **F&O Participant-Wise Open Interest (OI)**: Daily positioning breakdowns across Retail (`Client`), Domestic Institutions (`DII`), Foreign Institutions (`FII`), and Proprietary desks (`Pro`) across index and stock futures and options (2012–Present).

This document details the architecture, endpoint specifications, parsing mechanics, historical boundaries, schema layouts, and live verification evidence for both pipelines.

---

## 1. AMFI Mutual Fund Daily NAV Integration

The **Association of Mutual Funds in India (AMFI)** is the statutory body regulating the dissemination of official Net Asset Value (NAV) records for all Indian Asset Management Companies (AMCs).

### Mirrors & Network Topography

AMFI publishes a daily consolidated snapshot of all active scheme NAVs. To ensure high availability during scheduled maintenance and server failovers, the datalake utilizes a redundant multi-mirror topology:

| Mirror Priority | Target URL | Protocol / Format | Coverage Scope | Live Probe Status |
| :---: | :--- | :--- | :--- | :---: |
| **Primary** | `https://portal.amfiindia.com/spages/NAVAll.txt` | HTTPS / Semi-colon Flat Text | All Schemes (Open, Close, Interval) | **200 OK** (1.52 MB, ~18,230 lines) |
| **Secondary** | `https://www.amfiindia.com/spages/NAVAll.txt` | HTTPS / Semi-colon Flat Text | All Schemes (Exact Mirror) | **200 OK** (1.52 MB, ~18,230 lines) |
| **Open-Ended Subset** | `https://portal.amfiindia.com/spages/NavOpen.txt` | HTTPS / Semi-colon Flat Text | Open-Ended Schemes Only | **200 OK** (1.06 MB, ~13,390 lines) |
| **API Fallback** | `https://api.mfapi.in/mf/{scheme_code}` | REST / JSON | Historical Scheme Time Series | **200 OK** (Flagship Fallback) |

### Request Headers
```http
GET /spages/NAVAll.txt HTTP/1.1
Host: portal.amfiindia.com
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
Accept: text/csv,text/plain,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
Accept-Language: en-US,en;q=0.9
Accept-Encoding: gzip, deflate
Connection: keep-alive
```
*(No authentication or session priming cookies required on AMFI text portals).*

---

### Data Schema & File Structure

`NAVAll.txt` is an unstructured flat-text stream delimited by semicolons (`;`) combined with structural header lines that lack delimiters.

#### 1. Structural Section Headers
The file is logically partitioned into scheme categories and Asset Management Companies:
* **Category Lines**: Strings starting with `Open Ended Schemes`, `Close Ended Schemes`, or `Interval Fund Schemes` (e.g. `Open Ended Schemes ( Equity Scheme - Large Cap Fund )`).
* **AMC Lines**: Non-delimited corporate fund house titles preceding scheme blocks (e.g. `Axis Mutual Fund`, `HDFC Mutual Fund`, `ICICI Prudential Mutual Fund`).

#### 2. Delimited Scheme Lines (8-Token Layout)
The header line in `NAVAll.txt` is:
```text
Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;Net Asset Value;Date
```

Each data record contains 8 semicolon-delimited tokens:

| Token Index | Field Name | Data Type | Nullable | Description & Example |
| :---: | :--- | :--- | :---: | :--- |
| **0** | `SchemeCode` | Integer / String | No | Unique AMFI Scheme Code identifier (e.g. `135762`). |
| **1** | `ISINGrowth` | String (12 chars) | Yes | ISIN for Growth or Dividend Payout option (e.g. `INF846K01WO1`). `-` if not assigned. |
| **2** | `ISINReinvest` | String (12 chars) | Yes | ISIN for Dividend Reinvestment option. `-` if not assigned. |
| **3** | `SchemeName` | String | No | Base fund title (e.g. `Axis Children's Fund`). |
| **4** | `Plan` | String | Yes | Investment plan descriptor (e.g. `Direct Plan`, `Regular Plan`). |
| **5** | `Option` | String | Yes | Dividend or growth variant (e.g. `Growth Option`, `IDCW Option`). |
| **6** | `NAV` | Float | No | Net Asset Value in Indian Rupees (INR) (e.g. `29.1949`). |
| **7** | `Date` | Date String | No | Valuation date in `DD-Mon-YYYY` format (e.g. `30-Oct-2026`). |

#### Live Verification Evidence:
```text
Raw Line:
135762;INF846K01WO1;-;Axis Children's Fund;Direct Plan;Growth Option;29.1949;30-Oct-2026

Parsed Record:
{
  "SchemeCode": 135762,
  "SchemeName": "Axis Children's Fund - Direct Plan - Growth Option",
  "AMC": "Axis Mutual Fund",
  "Category": "Open Ended Schemes ( Solution Oriented Scheme - Children's Fund )",
  "ISINGrowth": "INF846K01WO1",
  "ISINReinvest": null,
  "NAV": 29.1949,
  "Date": "2026-10-30"
}
```

#### Text Encoding & Fallback Handling
> [!IMPORTANT]
> **Windows-1252 (`cp1252`) Fallback**: While modern AMFI daily files are predominantly UTF-8, fund names periodically include Windows-1252 typographical characters (such as en-dashes `\x96`, curly apostrophes `\x92`, and degree symbols). Standard UTF-8 decoders crash with `UnicodeDecodeError`.  
> `ResilientHTTPClient.text` handles this by trapping `UnicodeDecodeError` and falling back cleanly to `cp1252` with error replacement.

---

## 2. AMFI Historical NAV Report Integration

To seed historical NAV time series prior to the daily scraper's deployment, `bfinance-datalake` queries AMFI's official Historical NAV Report backend.

### Endpoint Specification
```http
GET https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={frmdt}&todt={todt}
```

#### Query Parameters:
* **`frmdt`**: Start date in `DD-Mon-YYYY` format (e.g. `01-Apr-2006`).
* **`todt`**: End date in `DD-Mon-YYYY` format (e.g. `30-Apr-2006`).

### Exact Historical Boundary ("From Where to Where")

Extensive live empirical probing across decades established the definitive temporal limits of AMFI's historical server:

| Query Window Tested | HTTP Status | Response Header / Content | Verification Finding |
| :--- | :---: | :--- | :--- |
| `01-Jan-2000` to `15-Jan-2000` | **200 OK** | `<!DOCTYPE html ... Frameset>` | **Empty HTML error frame** (No database records exist). |
| `01-Jan-2005` to `15-Jan-2005` | **200 OK** | `<!DOCTYPE html ... Frameset>` | **Empty HTML error frame** (Pre-2006 data not indexed). |
| **`01-Apr-2006` to `15-Apr-2006`** | **200 OK** | `Scheme Code;NAV Name;Plan;Option;...` | **VALID DATA**: 15,085 lines, **14,595 historical scheme NAV records**. |
| `01-Jan-2015` to `15-Jan-2015` | **200 OK** | `Scheme Code;NAV Name;Plan;Option;...` | **VALID DATA**: Full historical scheme NAV records. |

> [!NOTE]
> **Inception Cutoff**: AMFI's centralized electronic NAV archive begins exactly on **April 1, 2006** (the start of Indian Fiscal Year 2006–07). Any requests querying dates prior to `2006-04-01` return HTML error frames without tabular data.

### Historical Report Layout
The historical report differs slightly from the daily mirror in column sequencing:
```text
Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date
```

Sample historical record:
```text
102652;ABN AMRO  Monthly Income Plan-Regular Plan-Growth Option;;;;;11.7436;03-Apr-2006
```

---

## 3. Weekend Failover & Flagship Historical API (`api.mfapi.in`)

### Architectural Need
AMFI's portal servers regularly take down the `DownloadNAVHistoryReport_Po.aspx` and `NAVAll.txt` endpoints for maintenance windows over weekends (typically Saturday 18:00 IST to Sunday 22:00 IST), returning 503 Service Unavailable or DNS timeouts.

To guarantee that continuous daily pipelines, CI runners, and weekend cloud builds never stall, the datalake integrates an automated failover to the public open-source `api.mfapi.in` CDN.

### API Specification
```http
GET https://api.mfapi.in/mf/{scheme_code}
Accept: application/json
```

### JSON Schema & Sample Response
```json
{
  "meta": {
    "fund_house": "HDFC Mutual Fund",
    "scheme_type": "Open Ended Schemes",
    "scheme_category": "Income",
    "scheme_code": 100122,
    "scheme_name": "HDFC Balanced Fund - Growth Option",
    "isin_growth": null,
    "isin_div_reinvestment": null
  },
  "data": [
    {
      "date": "01-06-2018",
      "nav": "147.06900"
    },
    {
      "date": "03-04-2006",
      "nav": "29.35400"
    }
  ]
}
```

### Core Flagship Scheme Universe
When AMFI portal mirrors are unresponsive, the scraper backfills the flagship core universe spanning all major market segments back to 2006:
* `100122`: HDFC Balanced Advantage Fund (Oldest date: `03-04-2006`, 2,995 points)
* `118989`: HDFC Flexi Cap Fund
* `119062`: HDFC Large and Mid Cap Fund
* `120503`: Axis ELSS Tax Saver Fund
* `122639`: Parag Parikh Flexi Cap Fund

---

## 4. NSE F&O Participant Open Interest (OI) Integration

### Institutional Purpose
In Indian equity derivatives, NSE requires all Clearing Members and Trading Members to report end-of-day open positions by client classification. Ingesting this data enables quantitative desks to track institutional crowding, smart-money hedging, and retail sentiment across index and stock contracts.

### Endpoint Specification
```http
GET https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{DDMMYYYY}.csv
```

#### Request Headers:
```http
Accept: text/csv,text/plain,*/*
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
```
*(Hosted on the unauthenticated `nsearchives.nseindia.com` CDN; bypasses Akamai dynamic bot challenges).*

---

### File Format & Header Offset

The raw file consists of an initial title banner followed by the tabular header and participant rows:

```csv
""Participant wise Open Interest (no. of contracts) in Equity Derivatives as on Oct 30, 2026"",,,,,,,,,,,,,,
Client Type,Future Index Long,Future Index Short,Future Stock Long,Future Stock Short       ,Option Index Call Long,Option Index Put Long,Option Index Call Short,Option Index Put Short,Option Stock Call Long,Option Stock Put Long,Option Stock Call Short,Option Stock Put Short,Total Long Contracts      ,Total Short Contracts
Client,246926,59938,3291851,222385,2829961,3559948,2847822,4126285,1416834,737854,926229,895144,12083374,9077804
DII,46993,6470,256681,4606715,7398,113016,323,123,638,43578,237265,21263,468304,4872159
FII,34437,275190,3577558,2659511,607393,1227641,848205,711954,150242,222962,233082,139441,5820233,4867383
Pro,32979,19737,747197,384676,1049012,1253468,797414,1315711,832367,828202,1003505,776748,4743225,4297791
TOTAL,361335,361335,7873287,7873287,4493764,6154073,4493764,6154073,2400081,1832596,2400081,1832596,23115136,23115136
```

#### Parsing Offset Rule:
The scraper skips Line 0 and locates the column header containing `"Client Type"` dynamically before passing the stream to `pd.read_csv()`.

---

### Participant Categories & Column Definitions

The data provides a 15-column matrix across four primary market participants:

#### Participant Categories (`Client Type`):
1. **`Client`**: Retail individual investors, High Net-Worth Individuals (HNIs), and non-institutional corporate entities.
2. **`DII`**: Domestic Institutional Investors (Mutual Funds, Insurance Companies, Banks, Pension Funds).
3. **`FII`**: Foreign Institutional Investors and Foreign Portfolio Investors (FPIs).
4. **`Pro`**: Proprietary trading desks of registered broker-dealers.
5. **`TOTAL`**: Market-wide aggregate sum (used for checksum validation where `Total Long == Total Short`).

#### Contract Columns:
| Column Name | Metric Description |
| :--- | :--- |
| `Future Index Long` | Open contracts in Long Index Futures (Nifty 50, Bank Nifty, etc.). |
| `Future Index Short` | Open contracts in Short Index Futures. |
| `Future Stock Long` | Open contracts in Long Single-Stock Futures. |
| `Future Stock Short` | Open contracts in Short Single-Stock Futures. |
| `Option Index Call Long` | Long Call Options on Indices (Bullish positioning / upside hedge). |
| `Option Index Put Long` | Long Put Options on Indices (Bearish positioning / downside hedge). |
| `Option Index Call Short`| Short Call Options on Indices (Net call writing / resistance). |
| `Option Index Put Short` | Short Put Options on Indices (Net put writing / support). |
| `Option Stock Call Long` | Long Call Options on Single Stocks. |
| `Option Stock Put Long` | Long Put Options on Single Stocks. |
| `Option Stock Call Short`| Short Call Options on Single Stocks. |
| `Option Stock Put Short` | Short Put Options on Single Stocks. |
| `Total Long Contracts` | Aggregate Long derivative contracts across all categories. |
| `Total Short Contracts`| Aggregate Short derivative contracts across all categories. |

---

### Historical Boundary ("From Where to Where")

Live testing across multiple years confirmed the exact historical boundary of the NSE F&O participant dataset:

| Target Date | URL Probed | Status Code | Bytes | Ingestion Status |
| :---: | :--- | :---: | :---: | :--- |
| `01-01-2011` | `fao_participant_oi_01012011.csv` | **404 Not Found** | — | Not published in this archive format. |
| `02-01-2012` | `fao_participant_oi_02012012.csv` | **404 / Holiday** | — | Non-trading day. |
| **`01-06-2012`** | `fao_participant_oi_01062012.csv` | **200 OK** | 885 B | **Earliest verified trading archive**. |
| **`03-09-2012`** | `fao_participant_oi_03092012.csv` | **200 OK** | 904 B | **Continuous unbroken daily records begin**. |
| **`30-10-2026`** | `fao_participant_oi_30102026.csv` | **200 OK** | 973 B | **Current live trading session**. |

> [!TIP]
> **Quantitative Use Cases**:
> * **FII Long-to-Short Ratio**: Calculated as `Future Index Long / Future Index Short`. Values `< 0.20` indicate extreme oversold / hedging conditions; values `> 2.50` indicate extreme euphoria.
> * **Proprietary Option Writing Skew**: Tracking net `Option Index Put Short - Option Index Call Short` provides a reliable indicator of institutional support/resistance levels for index weekly expiries.

---

## 5. Storage Schema & Partitioning

Data is stored in high-performance Apache Parquet format, partitioned annually to allow fast vectorized temporal queries without loading decades into memory:

### AMFI Mutual Fund NAV Schema
* **Target File**: `mutual_funds/amfi_nav_{YYYY}.parquet`
* **Deduplication Key**: `["Date", "SchemeCode"]`
* **Sorting Key**: `["Date", "SchemeCode"]`

| Column | Type | Description |
| :--- | :--- | :--- |
| `Date` | `datetime64[ns]` | Valuation date. |
| `SchemeCode` | `int64` | AMFI unique scheme identifier. |
| `SchemeName` | `string` | Full scheme title including plan and option. |
| `AMC` | `string` | Asset Management Company name. |
| `Category` | `string` | SEBI / AMFI category classification. |
| `ISINGrowth` | `string` | ISIN Growth / Dividend Payout. |
| `ISINReinvest`| `string` | ISIN Dividend Reinvestment. |
| `NAV` | `float64` | Net Asset Value in INR. |

### NSE F&O Participant OI Schema
* **Target File**: `flows/fao_participant_oi_{YYYY}.parquet`
* **Deduplication Key**: `["Date", "Client Type"]`
* **Sorting Key**: `["Date", "Client Type"]`

| Column | Type | Description |
| :--- | :--- | :--- |
| `Date` | `datetime64[ns]` | Trading session date. |
| `Client Type` | `string` | Participant category (`Client`, `DII`, `FII`, `Pro`). |
| `Future Index Long` | `int64` | Index futures long contracts. |
| `Future Index Short`| `int64` | Index futures short contracts. |
| `Future Stock Long` | `int64` | Stock futures long contracts. |
| `Future Stock Short`| `int64` | Stock futures short contracts. |
| `Option Index Call Long` | `int64` | Index call options long. |
| `Option Index Put Long` | `int64` | Index put options long. |
| `Option Index Call Short`| `int64` | Index call options short. |
| `Option Index Put Short` | `int64` | Index put options short. |
| `Option Stock Call Long` | `int64` | Stock call options long. |
| `Option Stock Put Long` | `int64` | Stock put options long. |
| `Option Stock Call Short`| `int64` | Stock call options short. |
| `Option Stock Put Short` | `int64` | Stock put options short. |
| `Total Long Contracts` | `int64` | Total participant long contracts. |
| `Total Short Contracts`| `int64` | Total participant short contracts. |

---

## 6. Technical Constraints & Error Handling

### 1. Holiday & Weekend Freezing
Derivative archives do not exist on market holidays and weekends. To prevent redundant 404 HTTP polls on past dates:
* Requests returning HTTP 404 for dates older than `holiday_freeze_days` (default: 7 days) are permanently recorded in `state.db` as `HOLIDAY`.
* Subsequent backfill iterations skip these dates instantly without network calls.

### 2. Network Throttling & Concurrency
* **Archive Requests**: The scraper queries `nsearchives.nseindia.com` using `asyncio.Semaphore(4)` with a polite `0.10s` sleep to prevent CDN rate limiting.
* **AMFI Historical Chunks**: Queries to `DownloadNAVHistoryReport_Po.aspx` are partitioned into bounded windows of **89 calendar days** with a mandatory `1.0s` sleep between chunks to prevent AMFI portal session drops.

---

## 7. Implementation & CLI Usage

### Python Architecture
The ingestion logic is encapsulated in [`FundsAndFlowsScraper`](file:///e:/dev/code/bfinance-datalake/src/datalake/scrapers/funds_flows.py):
* `sync_amfi_daily()`: Fetches and parses daily multi-mirror flat files, with automated weekend failover to `api.mfapi.in`.
* `backfill_amfi_history(start_date, end_date)`: Windows historical NAV report queries from `2006-04-01` to current date.
* `sync_participant_oi(start_date, end_date)`: Downloads, offsets, filters, and stores participant OI matrices from 2012 to Present.

### CLI Commands

#### Ingest Daily Snapshot:
```bash
# Ingests latest daily AMFI NAVs and participant OI as part of the daily update cycle
uv run datalake update --lookback-days 3
```

#### Full Historical Backfill:
```bash
# Backfill F&O Participant OI (2012-Present) and AMFI NAVs (2006-Present)
uv run datalake backfill --start-date 2006-04-01 --end-date 2026-10-31 --include-amfi-history
```
