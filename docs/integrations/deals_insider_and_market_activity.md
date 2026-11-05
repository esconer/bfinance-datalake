# Deals, Insider Trading & Market Activity Data Integration

## Overview

Tracking institutional capital allocation and internal corporate transactions is critical for understanding market microstructure, discovering high-conviction momentum, and assessing governance dynamics. `bfinance-datalake` provides production-grade ingestion pipelines for three foundational market activity datasets:

1. **Exchange Bulk & Block Deals (2005–Present)**: Large-scale institutional, promoter, and marquee investor transactions executed in the cash equity segment across NSE and BSE.
2. **Cash Market Provisional FII / DII Daily Flows (Rolling / Daily)**: Exchange-wide daily tally of aggregate buying, selling, and net capital deployment by Foreign and Domestic Institutional Investors.
3. **SEBI PIT Insider Trading Disclosures (2015/2018–Present)**: Mandatory regulatory disclosures under SEBI (Prohibition of Insider Trading) Regulations, 2015 (Regulation 7(2)) detailing acquisitions, disposals, and pledges by Promoters, Directors, and Key Managerial Personnel (KMP).

This document details the regulatory context, endpoint specifications, network bypass architecture, data schemas, live empirical verification evidence, and storage design for these datasets.

---

## 1. Bulk & Block Deals Integration

### Regulatory Framework & Definitions

The Securities and Exchange Board of India (SEBI) defines strict criteria for large transactions:

* **Bulk Deal**: A trade where the total quantity of shares bought or sold exceeds **0.5% of the total equity shares** of the listed company. Bulk deals are executed during normal market trading hours through standard order matching and are disclosed by exchanges at the end of the trading day.
* **Block Deal**: A single transaction executed through a dedicated 15-minute trading window with a minimum order size of **₹10 Crore**. Block deals occur in two specialized windows:
  * *Morning Block Window*: 08:45 AM – 09:00 AM IST.
  * *Afternoon Block Window*: 02:05 PM – 02:20 PM IST.
  * Execution must be within $\pm 1\%$ of the prevailing market price or previous close.

---

### Endpoints Specification

`bfinance-datalake` integrates two distinct ingestion paths: daily static CSVs from NSE Archives and multi-year historical CSV streams from NSE's historical order repository backend:

| Feed Type | Target URL / Pattern | Method | Auth / Priming | Coverage Scope |
| :--- | :--- | :---: | :---: | :--- |
| **Daily Bulk CSV** | `https://nsearchives.nseindia.com/content/equities/bulk.csv` | GET | Unauthenticated | Latest trading day Bulk deals |
| **Daily Block CSV** | `https://nsearchives.nseindia.com/content/equities/block.csv` | GET | Unauthenticated | Latest trading day Block deals |
| **Modern Snapshot API** | `https://www.nseindia.com/api/snapshot-capital-market-bulk-deals` | GET | Cookie Primed | Real-time / Daily session deals |
| **Historical OR API** | `https://www.nseindia.com/api/historicalOR/bulk-block-short-deals` | GET | Cookie Primed | Multi-year annual archives (2004–Present) |

#### Historical OR Query Parameters:
```http
GET https://www.nseindia.com/api/historicalOR/bulk-block-short-deals?optionType={optionType}&from=01-01-{year}&to=31-12-{year}&csv=true
```
* **`optionType`**: `bulk_deals` or `block_deals`.
* **`from`**: Date in `DD-MM-YYYY` format (e.g. `01-01-2024`).
* **`to`**: Date in `DD-MM-YYYY` format (e.g. `31-12-2024`).
* **`csv`**: Set to `true` to return the complete exchange CSV export.

---

### Data Schema & Fields

The raw CSV returned by both daily and historical archives shares an 8-column layout:

```csv
"Date ","Symbol ","Security Name ","Client Name ","Buy / Sell ","Quantity Traded ","Trade Price / Wght. Avg. Price ","Remarks "
"30-OCT-2026","AEROFLEX","Aeroflex Industries Ltd","QE SECURITIES LLP","BUY","6,70,611","540.12","-"
"01-JAN-2024","3IINFOLTD","3i Infotech Limited","GRAVITON RESEARCH CAPITAL LLP","SELL","14,17,288","51.63","-"
"05-JAN-2024","INFIBEAM","Infibeam Avenues Limited","ABHISHEK MAYUR DESAI","SELL","45,00,000","22.80","-"
```

#### Field Specifications:

| Column Name | Normalized Field | Data Type | Description |
| :--- | :--- | :--- | :--- |
| `Date ` | `Date` | `datetime64[ns]` | Transaction execution date (parsed from `DD-Mon-YYYY`). |
| `Symbol ` | `Symbol` | `string` | Listed equity ticker on NSE (e.g. `AEROFLEX`, `RELIANCE`). |
| `Security Name ` | `SecurityName` | `string` | Full corporate name of the company. |
| `Client Name ` | `ClientName` | `string` | Name of the executing fund, institution, or individual. |
| `Buy / Sell ` | `Buy/Sell` | `string` | Direction of trade: `BUY` or `SELL`. |
| `Quantity Traded ` | `QuantityTraded` | `int64` | Number of shares transacted (stripped of commas). |
| `Trade Price / Wght. Avg. Price `| `TradePrice` | `float64` | Volume-weighted execution price in INR. |
| `Remarks ` | `Remarks` | `string` | Exchange remarks or `-` if none. |
| *(Datalake Metadata)* | `DealType` | `string` | `BULK` or `BLOCK`. |
| *(Datalake Metadata)* | `Exchange` | `string` | `NSE` (or `BSE`). |

#### Encoding & Byte Order Mark (BOM)
> [!IMPORTANT]
> **UTF-8 BOM (`\ufeff`) Handling**: NSE's `/api/historicalOR` endpoints prepend a UTF-8 Byte Order Mark (`\ufeff`) to the first line. If parsed with a standard ASCII or Latin-1 parser, the first column name becomes `\ufeff"Date "`, causing column lookup failures. The datalake uses `resp.text` decoding and strips the BOM before initializing `pd.read_csv()`.

---

### Historical Boundary ("From Where to Where")

Live testing across two decades verified the exact temporal availability of bulk and block deals:

| Year Tested | Endpoint Option | HTTP Status | Lines Returned | Status / Verification Details |
| :---: | :---: | :---: | :---: | :--- |
| **2003** | `bulk_deals` | **200 OK** | 1 Line | **Empty**: Header only, 0 deals recorded by exchange. |
| **2004** | `bulk_deals` | **200 OK** | 1,154 Lines | **Inception Era**: Bulk deals reporting initiated mid-2004. |
| **2005** | `bulk_deals` | **200 OK** | 6,919 Lines | **Full Annual History**: Continuous unbroken coverage starts. |
| **2006** | `bulk_deals` | **200 OK** | 8,516 Lines | Full continuous history. |
| **2010** | `bulk_deals` | **200 OK** | 16,203 Lines | Full continuous history. |
| **2024** | `bulk_deals` | **200 OK** | 26,845 Lines | Modern high-volume institutional liquidity. |
| **2024** | `block_deals` | **200 OK** | 1,557 Lines | High-value block window executions. |

> [!NOTE]
> **Definitive Cutoff**: Bulk and block deal tracking in `bfinance-datalake` begins on **`2005-01-01`**. Full historical backfills cover 2005 through the present day.

---

## 2. Cash Market FII / DII Daily Flows

### Institutional Context

The Cash Market FII/DII dataset represents the official provisional daily turnover of institutional trading in the equity cash segment. Published by NSE every trading evening at approximately **17:30 IST**, this feed is widely monitored as an indicator of foreign capital flows, currency pressure, and domestic institutional support.

### Endpoint Specification
```http
GET https://www.nseindia.com/api/fiidiiTradeReact
Referer: https://www.nseindia.com/
Accept: application/json, text/plain, */*
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
```
*(Requires session priming on `https://www.nseindia.com` to obtain Akamai bot cookies).*

---

### Data Schema & Live Verification

The endpoint returns a JSON array containing two records representing Domestic Institutional Investors (`DII`) and Foreign Institutional Investors (`FII/FPI`):

#### Live Verified Payload:
```json
[
  {
    "category": "DII",
    "date": "30-Oct-2026",
    "buyValue": "16643.55",
    "sellValue": "17461.06",
    "netValue": "-817.51"
  },
  {
    "category": "FII/FPI",
    "date": "30-Oct-2026",
    "buyValue": "20564.02",
    "sellValue": "16965.48",
    "netValue": "3598.54"
  }
]
```

#### Field Definitions:

| Field Name | Normalized Field | Data Type | Units | Description |
| :--- | :--- | :--- | :---: | :--- |
| `date` | `Date` | `datetime64[ns]` | — | Date of trading session (parsed from `DD-Mon-YYYY`). |
| `category` | `category` | `string` | — | Institutional category: `DII` or `FII/FPI`. |
| `buyValue` | `buyValue` | `float64` | **₹ Crores** | Gross equity purchase turnover across the session. |
| `sellValue` | `sellValue` | `float64` | **₹ Crores** | Gross equity sale turnover across the session. |
| `netValue` | `netValue` | `float64` | **₹ Crores** | Net capital flow: `buyValue - sellValue`. |

> [!TIP]
> **Units & Interpretation**:
> All values are denominated in **₹ Crores** ($1 \text{ Crore} = 10^7 \text{ INR} \approx \$120,000\text{ USD}$).  
> A positive `netValue` indicates net capital inflows (accumulation), while a negative `netValue` indicates net capital outflows (distribution).

---

## 3. SEBI PIT Insider Trading Disclosures

### Regulatory Framework

Under Regulation 7(2) of the **SEBI (Prohibition of Insider Trading) Regulations, 2015**, every promoter, member of the promoter group, designated person, and director of every listed company must disclose to the company any transaction that exceeds **₹10 Lakh in value** within two trading days of the transaction. The listed company must in turn notify the stock exchanges within two trading days.

Tracking insider trading disclosures allows quant models to monitor:
1. **Promoter Buying**: Substantial open-market purchases by promoters often indicate significant insider undervaluation conviction.
2. **Key Executive Disposals**: Large sales by Founders, Directors, or KMP ahead of quarterly results or macroeconomic headwinds.
3. **Pledge Activity**: Creation or revocation of share pledges by promoters securing corporate loans.

---

### Endpoint Specification
```http
GET https://www.nseindia.com/api/corporates-pit?index=equities&from_date={dd-mm-yyyy}&to_date={dd-mm-yyyy}
```

#### Query Parameters:
* **`index`**: Always set to `equities`.
* **`from_date`**: Start date in `DD-MM-YYYY` format (e.g. `01-10-2024`).
* **`to_date`**: End date in `DD-MM-YYYY` format (e.g. `20-10-2024`).

---

### Data Schema & Critical Fields

The endpoint returns a structured JSON payload with a `data` array containing detailed regulatory filings.

#### Live Verified Record:
```json
{
  "symbol": "MOS",
  "company": "Mos Utility Limited",
  "acqName": "Kurjibhai Premjibhai Ruparelia",
  "personCategory": "Promoters",
  "secType": "Equity Shares",
  "secAcq": "480480",
  "secVal": "99459360",
  "tdpTransactionType": "Sell",
  "acqMode": "Off Market",
  "befAcqSharesNo": "7745830",
  "befAcqSharesPer": "31.06",
  "afterAcqSharesNo": "7265350",
  "afterAcqSharesPer": "29.14",
  "anex": "7(2)",
  "acqfromDt": "16-Oct-2024",
  "acqtoDt": "16-Oct-2024",
  "intimDt": "18-Oct-2024",
  "date": "19-Oct-2024 12:36",
  "exchange": "NA",
  "xbrl": "https://nsearchives.nseindia.com/corporate/xbrl/IT_1172587_1279620_19102024123603_WEB.xml"
}
```

#### Schema Field Definitions:

| JSON Field | Data Type | Description | Key Categorical Values |
| :--- | :--- | :--- | :--- |
| `symbol` | `string` | NSE equity symbol. | e.g. `RELIANCE`, `TCS`, `MOS`. |
| `company` | `string` | Full corporate name. | e.g. `Mos Utility Limited`. |
| `acqName` | `string` | Transacting entity or individual. | Insider / Institution name. |
| `personCategory`| `string` | Insider regulatory classification. | `Promoters`, `Promoter Group`, `Directors`, `KMP`, `Immediate Relative`. |
| `secType` | `string` | Security type. | `Equity Shares`, `Warrants`, `Convertible Debentures`. |
| `secAcq` | `int64` | Number of securities transacted. | Quantity of shares traded. |
| `secVal` | `float64` | Total transaction value in INR. | Raw trade value (e.g. `99,459,360`). |
| `tdpTransactionType`| `string` | Nature of transaction. | `Buy`, `Sell`, `Pledge Creation`, `Pledge Revocation`, `Pledge Invocation`. |
| `acqMode` | `string` | Transaction execution channel. | `Market Purchase`, `Market Sale`, `Off Market`, `ESOP`, `Inter-se Transfer`, `Preferential Offer`, `Rights Issue`. |
| `befAcqSharesNo`| `int64` | Holding count prior to trade. | Pre-trade share count. |
| `befAcqSharesPer`| `float64` | Holding percentage prior to trade. | Pre-trade percentage of capital. |
| `afterAcqSharesNo`| `int64` | Holding count following trade. | Post-trade share count. |
| `afterAcqSharesPer`| `float64`| Holding percentage following trade.| Post-trade percentage of capital. |
| `acqfromDt` | `string` | Start date of trade execution. | `DD-Mon-YYYY`. |
| `acqtoDt` | `string` | End date of trade execution. | `DD-Mon-YYYY`. |
| `intimDt` | `string` | Date company was notified. | `DD-Mon-YYYY`. |
| `date` | `string` | Exchange filing timestamp. | `DD-Mon-YYYY HH:MM`. |
| `xbrl` | `string` | Direct link to XML/XBRL filing. | Full URL on `nsearchives.nseindia.com`. |

---

### Historical Boundary ("From Where to Where")

Live testing confirmed the availability of electronic PIT disclosures on NSE:

| Query Window | Status Code | Items Returned | Regulatory & Ingestion Context |
| :---: | :---: | :---: | :--- |
| **`01-01-2015`** to `15-01-2015` | **200 OK** | 0 Items | Prior to SEBI PIT Regulations, 2015 coming into force (May 15, 2015). |
| **`01-06-2016`** to `15-06-2016` | **200 OK** | **2,227 Items** | **Early Electronic Disclosures Available**. |
| **`01-06-2017`** to `15-06-2017` | **200 OK** | **2,500 Items** | Continuous electronic reporting active. |
| **`01-10-2024`** to `20-10-2024` | **200 OK** | **612 Items** | Current active system-driven disclosures. |

> [!NOTE]
> **Production Boundary**:
> * `bfinance-datalake` defaults historical backfilling of PIT disclosures from **`2018-01-01` to Present** (capturing modern XBRL-standardized system-driven disclosures), with support for backfilling back to **`2016-01-01`**.

---

## 4. Network Architecture, Anti-Bot & Windows DNS / DoH

### The Challenge: Akamai Bot Manager & Windows c-ares DNS
Endpoints hosted on `www.nseindia.com` (such as `/api/fiidiiTradeReact`, `/api/corporates-pit`, and `/api/historicalOR`) are protected by **Akamai Enterprise Bot Manager**. Standard HTTP libraries (`curl`, `urllib`, standard `requests`, `httpx`) are immediately blocked with **HTTP 403 Forbidden** or drop TCP connections during TLS handshakes.

Furthermore, on Windows environments, libcurl's bundled `c-ares` DNS resolver frequently encounters UDP socket timeouts or fails IPv6 AAAA queries, triggering `curl: (28) Resolving timed out after 10000 milliseconds`.

### The Solution: Chrome 124 TLS Impersonation + DNS-over-HTTPS (DoH)

`bfinance-datalake` solves both hurdles in [`ResilientHTTPClient`](file:///e:/dev/code/bfinance-datalake/src/datalake/http_client.py):
1. **TLS / JA3 Impersonation**: Uses `curl_cffi` to replicate the exact TLS Client Hello, cipher suites, ALPN tokens, and TCP window fingerprints of Google Chrome 124 on Windows.
2. **Cookie Priming**: The client transparently initializes a session against `https://www.nseindia.com`, capturing necessary Akamai tracking tokens (`_abck`, `ak_bmsc`, `bm_sz`).
3. **DNS-over-HTTPS (`DoH`)**: Bypasses local Windows UDP DNS failures by routing domain resolution over encrypted HTTPS queries (`https://dns.google/dns-query` or `https://cloudflare-dns.com/dns-query`).

#### Verified Live Probe Architecture:
```python
from curl_cffi import requests

session = requests.Session(impersonate="chrome124")
doh_endpoint = "https://dns.google/dns-query"

# Step 1: Prime session cookies
session.get("https://www.nseindia.com", doh_url=doh_endpoint, timeout=10)

# Step 2: Query API with primed session
response = session.get(
    "https://www.nseindia.com/api/fiidiiTradeReact",
    headers={"Referer": "https://www.nseindia.com/"},
    doh_url=doh_endpoint,
    timeout=10
)
# Returns HTTP 200 OK with complete JSON payload
```

---

## 5. Storage Schemas & Partitioning

All datasets are stored in compressed Apache Parquet format with deterministic deduplication rules:

### 1. Bulk & Block Deals Parquet
* **File Path**: `deals/bulk_block_deals.parquet`
* **Deduplication Columns**: `["Date", "Exchange", "DealType", "Symbol", "Client Name", "Buy/Sell", "Quantity Traded"]`
* **Sort Columns**: `["Date", "Symbol"]`

### 2. Cash Market FII / DII Daily Flows Parquet
* **File Path**: `flows/cash_fii_dii_flows.parquet`
* **Deduplication Columns**: `["Date", "category"]`
* **Sort Columns**: `["Date", "category"]`

### 3. Insider Trading PIT Parquet
* **File Path**: `deals/insider_trading_pit.parquet`
* **Deduplication Columns**: `["symbol", "acqName", "date", "secVal", "tdpTransactionType"]`
* **Sort Columns**: `["symbol", "date"]`

---

## 6. Implementation & CLI Usage

### Python Scraper Architecture
The scraping pipeline is implemented in [`DealsAndDocumentsScraper`](file:///e:/dev/code/bfinance-datalake/src/datalake/scrapers/deals_docs.py):
* `sync_bulk_and_block_deals()`: Captures daily static CSVs from `nsearchives.nseindia.com`.
* `backfill_historical_bulk_block_deals(start_year, end_year)`: Iterates annually from `2005` to present, querying `/api/historicalOR/bulk-block-short-deals` and appending to Parquet.
* `sync_cash_fii_dii_daily()`: Fetches `/api/fiidiiTradeReact` and upserts daily institutional turnover into `flows/cash_fii_dii_flows.parquet`.
* `sync_insider_trading_pit(start_date, end_date)`: Batches 89-day windows against `/api/corporates-pit` and merges disclosures into `deals/insider_trading_pit.parquet`.

### CLI Commands

#### Daily Incremental Sync:
```bash
# Ingests latest bulk/block deals, cash FII/DII flows, and PIT disclosures
uv run datalake update --lookback-days 3
```

#### Historical Backfill:
```bash
# Backfill bulk and block deals from 2005 to today, along with insider trading PIT disclosures from 2018
uv run datalake backfill --start-date 2005-01-01 --end-date 2026-10-31
```
