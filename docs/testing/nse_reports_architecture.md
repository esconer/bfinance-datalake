# NSE Report Portals Architecture & Source Investigation Audit

This document details the reverse-engineering, empirical network probing, and architectural audit of the National Stock Exchange of India (NSE) reporting infrastructure, specifically covering the official report directories:

- **All Reports (Equities & Capital Market)**: `https://www.nseindia.com/all-reports`
- **Security-wise Historical Archive**: `https://www.nseindia.com/report-detail/eq_security`
- **Derivatives Reports (Equity, Currency, Commodity, IRF)**: `https://www.nseindia.com/all-reports-derivatives`
- **Debt & Corporate Bonds Reports**: `https://www.nseindia.com/all-reports-debt`
- **Electronic Gold Receipts (EGR)**: `https://www.nseindia.com/all-reports-egr`

---

## 1. Executive Summary & Research Context

Many automated data lakes and CI runners experience intermittent or total failures when requesting data from `nsearchives.nseindia.com` (e.g. `HTTP 403 Forbidden`, TCP resets, or timeouts). To address this, we conducted an empirical investigation into the architecture of `www.nseindia.com`'s frontend report portals to answer three core questions:

1. **How does the official web portal discover and serve reports?**
2. **What underlying APIs power these pages, and can they be scraped directly?**
3. **What alternative, high-reliability channels exist to source data when `nsearchives` is unreachable?**

### Key Findings
1. **Dynamic Single-Page Application (SPA) Architecture**:  
   The front-facing pages are not static HTML directories. They are client-side web applications powered by **hidden JSON manifest APIs** hosted directly on `www.nseindia.com`.
2. **Three Master Scraping Channels Discovered**:
   - **Daily Manifest API (`/api/daily-reports?key={SEGMENT}`)**: Covers 11 distinct market segments and publishes a real-time index of ~100 daily files with exact file sizes, dates, and direct download links.
   - **Monthly Manifest API (`/api/monthly-reports?key={SEGMENT}`)**: Indexes monthly position limits, settlement statistics, mode of trading, and impact cost files across 10 segments.
   - **Security-Wise Historical API (`/api/historicalOR/generateSecurityWiseHistoricalData`)**: Directly streams up to **5 continuous years** of daily OHLCV + Delivery + Trades per stock directly from `www.nseindia.com`, with **zero reliance on `nsearchives`**.
3. **Root Cause of `nsearchives` Access Failures**:
   - `nsearchives.nseindia.com` is hosted on a separate Akamai CDN edge that enforces geographic and IP-reputation rate limits independently of `www.nseindia.com`.
   - Local client libraries often trip on an aggressive timeout limit (e.g., 6 seconds) for multi-megabyte ZIP/DAT archives, triggering false-positive circuit breakers.

---

## 2. Reverse-Engineering Methodology & Runtime Architecture

We downloaded and analyzed the JavaScript bundles loaded by these pages, specifically:
- [`all-reports.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/all-reports.js) (77.4 KB, version `30102026`)
- [`security-wise-archives.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/security-wise-archives.js) (14.8 KB, version `30102026`)
- [`common.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/common.js) (280.3 KB, version `30102026`)

### System Architecture Flow
```
                           +----------------------------+
                           |     www.nseindia.com       |
                           +--------------+-------------+
                                          |
                +-------------------------+-------------------------+
                |                                                   |
      [JSON Manifest APIs]                                 [Dynamic Data APIs]
                |                                                   |
   /api/daily-reports?key={SEGMENT}           /api/historicalOR/generateSecurityWiseHistoricalData
   /api/monthly-reports?key={SEGMENT}                               |
                |                                          (Direct CSV stream up to 5 years,
    (Provides real-time file catalog:                       zero reliance on nsearchives)
     CurrentDay, PreviousDay, FutureDay)
                |
                v
   +----------------------------+
   |  nsearchives.nseindia.com  |
   +----------------------------+
     (Underlying static asset
      storage: CSV, ZIP, DAT)
```

### Decompiled JavaScript Logic: Report Discovery Loop
In `all-reports.js` (lines 40–85), when a user loads `/all-reports` or `/all-reports-derivatives`, the page initializes market statuses and dispatches concurrent GET calls:

```javascript
var symbol = ["CM", "INDEX", "SLBS", "SME", "FO", "COM", "CD", "NBF", "WDM", "CBM", "TRI-PARTY", "EGR"];
symbol.forEach(function (name) {
    B.get('/api/daily-reports?key=' + name).then(function (resp) {
        var respJSON = JSON.parse(resp);
        // Binds CurrentDay, PreviousDay, FutureDay to the DOM
        dailyReports = ReportCard({ 
            data: respJSON.CurrentDay || [], 
            info: { name: "capital-market", section: name.toLowerCase(), type: "daily-reports" } 
        });
        B.render(dailyReports, B.findOne('#cr_' + name.toLowerCase() + '_daily_Current'));
    });
});
```

---

## 3. Channel 1: The Master Daily Reports Manifest API

### Endpoint Specification
```http
GET https://www.nseindia.com/api/daily-reports?key={MARKET_KEY} HTTP/1.1
Host: www.nseindia.com
Accept: application/json
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
Referer: https://www.nseindia.com/all-reports
```

### Complete Segment Catalog & Itemized Files
The API accepts 11 segment keys. Each call returns an explicit JSON object partitioned into `CurrentDay`, `PreviousDay`, and `FutureDay`.

```json
{
  "currentDate": "05-Nov-2026",
  "previousDate": "04-Nov-2026",
  "futureDate": "06-Nov-2026",
  "CurrentDay": [
    {
      "fileHashKey": "CM-FULL-BHAVCOPY-SECURITY-DELIVERABLE-DATA-2",
      "fileKey": "CM-FULL-BHAVCOPY-SECURITY-DELIVERABLE-DATA",
      "fileSegment": "CM",
      "displayName": "Full Bhavcopy and Security Deliverable data",
      "fileActlName": "sec_bhavdata_full_05112026.csv",
      "filePath": "https://nsearchives.nseindia.com/products/content/",
      "tradingDate": "05-Nov-2026",
      "filePosition": 24,
      "fileSize": "395.68 KB"
    }
  ]
}
```

### Itemized Breakdown Across All 11 Segments

#### 1. Capital Market Equities (`key=CM`) — 37 Daily Files
- **Full Bhavcopy + Delivery**: `sec_bhavdata_full_DDMMYYYY.csv` (~400 KB) — Complete OHLCV, weighted average price, total trades, deliverable quantity, and delivery percentage for all listed equities.
- **CM UDiFF Common Bhavcopy Final**: `BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip` (~210 KB) — SEBI-mandated standard format.
- **Security-wise Delivery Positions**: `MTO_DDMMYYYY.DAT` (~130 KB) — Gross delivery quantity and client turnover statistics.
- **Daily Scrip Volatility**: `CMVOLT_DDMMYYYY.CSV` (~295 KB) — Annualized and daily volatility computed per equity symbol.
- **Bulk & Block Deals**: `bulk.csv`, `block.csv` — Static snapshots of all transactions exceeding SEBI disclosure thresholds.
- **Institutional Short Selling**: `shortselling_DDMMYYYY.csv` (~10 KB) — Scrip-wise short positions by institutional and non-institutional participants.
- **52-Week High / Low**: `CM_52_wk_High_low_DDMMYYYY.csv` (~320 KB) — Historical price extremes.
- **Price Bands & Circuit Filters**: `sec_list_DDMMYYYY.csv` (~175 KB), `eq_band_changes_DDMMYYYY.csv` — Full tick sizes, upper/lower circuit limits, and dynamic band revisions.
- **Intra-Day VaR Margin Profiles**: `C_VAR1_DDMMYYYY_1.DAT` to `_6.DAT` (~1.06 MB each) — Begin-of-day through end-of-day Value-at-Risk margins.
- **Market Activity Report**: `MA_DDMMYY.csv` (~235 KB) — Consolidated exchange turnover statistics.

#### 2. Futures & Options Derivatives (`key=FO`) — 32 Daily Files
- **F&O UDiFF Common Bhavcopy**: `BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip` (~1.2 MB) — All active Futures and Options contracts, settlement prices, open interest, and turnover.
- **Participant-wise Open Interest**: `fao_participant_oi_DDMMYYYY.csv` (~1 KB) — Long/short contract breakdowns for `Client`, `DII`, `FII`, and `Pro` across Index Futures, Stock Futures, Index Options, and Stock Options.
- **Participant-wise Trading Volumes**: `fao_participant_vol_DDMMYYYY.csv` (~1 KB) — Gross traded volume by participant type.
- **FII Derivatives Statistics**: `fii_stats_DD-Mon-YYYY.xls` (~9 KB) — Buy/sell contracts and value for Foreign Portfolio Investors.
- **Security Ban Period**: `fo_secban.csv` — Securities currently in F&O ban period (exceeding 95% MWPL).
- **Daily Settlement Prices**: `FOSett_prce_DDMMYYYY.csv` (~70 KB) — Official Mark-to-Market settlement prices.
- **Daily Volatility**: `FOVOLT_DDMMYYYY.csv` (~40 KB) — Contract-level volatility metrics.
- **Market-wide Position Limits & Combined OI**: `combineoi_DDMMYYYY.zip`, `combineoi_deleq_DDMMYYYY.csv`.
- **Contract Master Files**: `NSE_FO_contract_DDMMYYYY.csv.gz` (~1.84 MB) — Full specification of all traded derivatives contracts.

#### 3. Indices (`key=INDEX`) — 2 Daily Files
- **Consolidated Indices Daily Snapshot**: `ind_close_all_DDMMYYYY.csv` (~16 KB) — Single file providing Open, High, Low, Close, P/E, P/B, Dividend Yield, and Turnover for **all 120+ Nifty indices** (NIFTY 50, Next 50, Midcap 150, Bank, IT, Sectorals, Thematic, and Strategy indices).
- **NIFTY 50 Top 10 Holdings**: `top10nifty50_DDMMYY.csv` (~0.4 KB) — Weights and market caps of the top 10 index constituents.

#### 4. Securities Lending & Borrowing (`key=SLBS`) — 5 Daily Files
- **SLBS Bhavcopy**: `SLBM_BC_DDMMYYYY.DAT` (~42 KB) — Daily lending fees, tenors, and traded volumes.
- **List of Eligible Securities**: `SLB_ELG_SEC_DDMMYYYY.csv` (~775 KB) — Complete universe of stocks permitted for lending/borrowing.
- **Daily Open Positions**: `slb_openpos_DDMMYYYY.csv` (~36 KB).
- **Foreclosure Report**: `Forclosure_SLB_YYYYMMDD.CSV`.

#### 5. SME Platform (`key=SME`) — 2 Daily Files
- **SME Bhavcopy**: `smeDDMMYYYY.csv` (~81 KB) — Daily OHLCV for SME Emerge-listed securities.
- **SME Price Bands**: `sme_bands_complete_DDMMYYYY.csv` (~26 KB).

#### 6. Corporate Bond Market (`key=CBM`) — 3 Daily Files
- **Corporate Bond Daily Trades**: `cbm_trdYYYYMMDD.csv` (~55 KB) — Secondary market executed trades on corporate debentures.
- **Corporate Bond Security Master**: `Corporate_bond_report_DD-Mon-YYYY.csv` (~615 KB) — Master catalog of all active corporate debt ISINs, coupon rates, maturity dates, and credit ratings.
- **G-Sec Settlement Order Report**: `GSEC_SettlementOrderReport_DDMMYYYY.csv`.

#### 7. Wholesale Debt Market (`key=WDM`) — 8 Daily Files
- **Securities Available for Trading**: `wdmlist_DDMMYYYY.csv` (~1.18 MB) — Exhaustive universe of government bonds, treasury bills, and state development loans.
- **Daily WDM Summary**: `dlyDDMMYYYY.zip` (~105 KB).
- **WDM SPAN Margins**: `nsccl_d.YYYYMMDD.s.zip`.

#### 8. Tri-Party Repo (`key=TRI-PARTY`) — 1 Daily File
- **Tri-Party Repo Bhavcopy**: `TRM_BCDDMMYYYY.csv` (~10 KB) — Sovereign repo borrowing rates, collateral haircut schedules, and traded volumes.

#### 9. Electronic Gold Receipts (`key=EGR`) — 7 Daily Files
- **EGR VaR Margin Profiles**: `E_VAR1_DDMMYYYY_1.DAT` to `_7.DAT` — Daily margin schedules for dematerialized vault-backed bullion receipts.

#### 10. Currency Derivatives (`key=CD`) — 23 Daily Files
- Full intra-day SPAN margins and volatility reports across USDINR, EURINR, GBPINR, and JPYINR.

#### 11. Commodity Derivatives (`key=COM`) — 11 Daily Files
- SPAN margins and volatility profiles across Crude Oil, Natural Gas, Gold, and Silver futures and options.

---

## 4. Channel 2: The Monthly Reports Manifest API

### Endpoint Specification
```http
GET https://www.nseindia.com/api/monthly-reports?key={MARKET_KEY} HTTP/1.1
Host: www.nseindia.com
Accept: application/json
```
Available for `CM`, `FO`, `INDICES`, `SLBS`, `CD`, `COM`, `IRD`, `WDM`, `CBM`, and `EGR`.

### High-Value Monthly Datasets
- **Trading Member-wise Overall Position Limits**: `tmopl_MonYYYY.csv`
- **Market-wide Position Limits (MWPL)**: `mpl_MonYYYY.csv`
- **FII / Mutual Fund Aggregate Exposure Limits**: `fopl_MonYYYY.csv`
- **Stock Categorization & Impact Cost Benchmarks**: `C_CATG_MonYYYY.T01` (~450 KB)
- **Monthly SLBS Transaction Register**: `SLB_Transaction_Data_MonYYYY.csv` (~20 KB)

---

## 5. Channel 3: Security-Wise 5-Year Deep History API (`/report-detail/eq_security`)

In [`security-wise-archives.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/security-wise-archives.js), lines 150–185 reveal the exact mechanism used to generate security-wise historical reports:

```javascript
downloadFile("/api/historicalOR/generateSecurityWiseHistoricalData" + params + "&csv=true");
```

### Endpoint Specification
```http
GET https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from={DD-MM-YYYY}&to={DD-MM-YYYY}&symbol={SYMBOL}&type={TYPE}&series={SERIES}&csv=true HTTP/1.1
Host: www.nseindia.com
Accept: text/csv,application/json
Referer: https://www.nseindia.com/report-detail/eq_security
```

### Parameter Matrix
| Parameter | Permitted Values | Notes |
| :--- | :--- | :--- |
| `from` | Date formatted as `DD-MM-YYYY` | Maximum range between `from` and `to` is **1,826 days (5 full years)**. |
| `to` | Date formatted as `DD-MM-YYYY` | Must be $\ge$ `from`. |
| `symbol` | Uppercase NSE Symbol (e.g. `TCS`, `RELIANCE`) | Mandatory. |
| `type` | `priceVolumeDeliverable`, `priceVolume`, `deliverable` | `priceVolumeDeliverable` supplies all metrics in a single pass. |
| `series` | `ALL`, `EQ`, `BE`, `SM`, etc. | Valid series are dynamically resolved via `/api/historicalOR/SASeries`. |
| `csv` | `true` or `false` | When `true`, returns standard CSV directly from `www.nseindia.com`. |

### Returned Schema
```csv
Date,Series,Open Price,High Price,Low Price,Close Price,Last Price,Prev Close,Total Traded Quantity,Turnover ₹,No. of Trades,Deliverable Qty,% Dly Qt to Traded Qty
05-Nov-2026,EQ,3950.00,3985.00,3925.50,3960.25,3961.00,3945.10,1250430,4945823100.00,45210,782150,62.55
```

### Why This Endpoint is Critical for Reliability
1. **100% Bypass of `nsearchives`**:  
   Requests terminate on `www.nseindia.com` (which readily responds to standard TLS-impersonated clients).
2. **5-Year Continuous Ingestion**:  
   Rather than unzipping 1,250 daily Bhavcopies, 5 years of daily OHLCV and delivery data for a security can be acquired in **one single HTTP request**.
3. **No Unzipping Overhead**:  
   Returns clean, pre-parsed CSV text.

---

## 6. Root Cause Analysis: `nsearchives` Unreachability & Mitigation

Our empirical network probes revealed two distinct reasons why scrapers fail against `nsearchives.nseindia.com`:

### 1. Akamai CDN Edge Geo-Fencing
- `www.nseindia.com` and `nsearchives.nseindia.com` utilize different Akamai edge security configurations.
- `www.nseindia.com` allows traffic once primed with standard session cookies (`_abck`, `bm_sz`, `ak_bmsc`).
- `nsearchives.nseindia.com` enforces strict IP-reputation checks and occasional non-Indian geo-blocking, returning `HTTP 403 Forbidden` or TCP resets on unrecognized cloud IPs.

### 2. Aggressive Timeout Traps on Multi-Megabyte Archives
- In `http_client.py`, requests to `nsearchives` were constrained by:
  ```python
  if is_nsearchives:
      timeout = min(timeout, 6.0)
  ```
- While small CSV files (~30 KB) download in under 500 ms, multi-megabyte files (such as F&O Bhavcopy ZIPs at 1.2 MB or intra-day DAT files at 10 MB) frequently require 7–12 seconds over international connections.
- The 6.0-second cap caused legitimate downloads to abort with `curl: (28) Operation timed out after 6004 milliseconds`, falsely incrementing `self._nsearchives_fail_streak` and prematurely tripping the host circuit breaker.

---

## 7. Concrete Integration Architecture for `bfinance-datalake`

### Blueprint 1: The "Publication Oracle" Pattern
To eliminate false-positive `HTTP 404` errors before evening publication (~18:30 IST), scrapers should query the daily manifest before requesting files:

```python
async def is_eod_published_today(http: ResilientHTTPClient, target_date: date) -> bool:
    """Check www.nseindia.com manifest to confirm if today's Bhavcopy is published."""
    url = "https://www.nseindia.com/api/daily-reports?key=CM"
    resp = await http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
    if resp.status_code != 200:
        return False
    manifest = resp.json()
    expected_str = target_date.strftime("%d-%b-%Y")
    return manifest.get("currentDate") == expected_str
```

### Blueprint 2: Sourcing Newly Discovered High-Value Feeds
The discovery of `daily-reports` provides access to several datasets not yet ingested into `bfinance-datalake`:

| Dataset | Proposed Storage Path | Source File in Manifest | Strategic Value |
| :--- | :--- | :--- | :--- |
| **Corporate Bond Trades** | `data/debt/cbm_trades_YYYY.parquet` | `cbm_trdYYYYMMDD.csv` | Secondary market corporate debt pricing and liquidity metrics. |
| **Corporate Bond Master** | `data/debt/cbm_security_master.parquet` | `Corporate_bond_report_...csv` | Complete directory of active debt ISINs, coupon rates, and ratings. |
| **Short Selling Disclosures** | `data/deals/short_selling.parquet` | `shortselling_DDMMYYYY.csv` | Institutional and retail short positioning intelligence. |
| **Daily Volatility Matrix** | `data/analytics/equity_volatility.parquet` | `CMVOLT_DDMMYYYY.CSV` | Official exchange annualized & daily volatility for risk modeling. |
| **Tri-Party Repo Rates** | `data/fixed_income/treps_repo.parquet` | `TRM_BCDDMMYYYY.csv` | Daily money market short-term lending rates and collateral volumes. |
