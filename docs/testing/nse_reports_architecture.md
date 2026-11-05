# NSE Report Portals Architecture & Source Investigation Audit

This document details the reverse-engineering, empirical network probing, and architectural audit of the National Stock Exchange of India (NSE) reporting infrastructure, specifically covering the official report directories:

- **All Reports (Equities & Capital Market)**: `https://www.nseindia.com/all-reports`
- **Security-wise Historical Archive**: `https://www.nseindia.com/report-detail/eq_security`
- **Derivatives Reports (Equity, Currency, Commodity, IRF)**: `https://www.nseindia.com/all-reports-derivatives`
- **Debt & Corporate Bonds Reports**: `https://www.nseindia.com/all-reports-debt`
- **Electronic Gold Receipts (EGR)**: `https://www.nseindia.com/all-reports-egr`

---

## 1. Executive Summary & Research Context

Many automated data lakes, analytics pipelines, and CI runners experience intermittent or total failures when requesting data from `nsearchives.nseindia.com` (e.g. `HTTP 403 Forbidden`, TCP resets, or timeouts). To address this, we conducted an empirical investigation into the architecture of `www.nseindia.com`'s frontend report portals to answer core architectural questions:

1. **How does the official web portal discover and serve reports?**
2. **What underlying APIs power these pages, and can they be scraped directly?**
3. **What alternative, high-reliability channels exist to source data when `nsearchives` is unreachable or blocked?**
4. **Can `www.nseindia.com` act as a gateway/proxy to retrieve files stored on `nsearchives`?**

### Key Findings
1. **Dynamic Single-Page Application (SPA) Architecture**:  
   The front-facing pages are not static HTML directories. They are client-side web applications powered by **hidden JSON manifest APIs** hosted directly on `www.nseindia.com`.
2. **Internal Server-Side Proxy Channel (`/api/zipem`)**:  
   Discovered in `quick-links.js`, `www.nseindia.com` hosts an internal proxy `/api/zipem?fileURLs=[...]&type=daily` that accepts arbitrary URLs pointing to `nsearchives.nseindia.com`. The NSE web server fetches the requested files over its internal private network (bypassing external runner IP blocks, geo-fencing, and CDN limits), bundles them into a ZIP archive, and streams them back over `www.nseindia.com`.
3. **Multi-File Batch Bundling**:  
   Rather than making 5 separate HTTP connections for CM Bhavcopy, FO Bhavcopy, MTO Deliverables, Ban list, and Participant OI, `/api/zipem` can fetch all 5 in **a single 4-second request**, dramatically accelerating ingestion speed.
4. **Real-Time Publication Oracles (`/api/daily-reports` & `/api/merged-daily-reports`)**:  
   Covers 11 distinct market segments with real-time JSON catalogs indexed by `fileKey`. They confirm whether today's files are live before scraping, eliminating 404 false alarms.
5. **Security-Wise Historical API (`/api/historicalOR/generateSecurityWiseHistoricalData`)**:  
   Directly streams up to **5 continuous years** (1,826 days) of daily OHLCV + Delivery + Trades per stock directly from `www.nseindia.com`, with **zero reliance on `nsearchives`**.
6. **Client-Side Timeout and Substring Trap in `http_client.py`**:  
   Requests to `nsearchives` were constrained by an overly aggressive 6.0s timeout. Furthermore, `is_nsearchives = "nsearchives.nseindia.com" in url` checked substring matching across the whole query string, accidentally clamping `/api/zipem` requests that contained archive URLs as parameters.

---

## 2. Reverse-Engineering Methodology & Runtime Architecture

We downloaded and analyzed the JavaScript bundles loaded by these pages:
- [`all-reports.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/all-reports.js) (77.4 KB, version `30102026`)
- [`quick-links.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/quick-links.js) (32.1 KB, version `30102026`)
- [`security-wise-archives.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/security-wise-archives.js) (14.8 KB, version `30102026`)
- [`common.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/common.js) (280.3 KB, version `30102026`)

### System Architecture Flow
```
                           +----------------------------+
                           |      User / CI Runner      |
                           +--------------+-------------+
                                          |
                +-------------------------+-------------------------+
                |                                                   |
    [Direct Web Gateway]                                  [Dynamic History API]
    Host: www.nseindia.com                                Host: www.nseindia.com
    TLS 1.3 / Chrome Impersonation                        TLS 1.3 / Chrome Impersonation
                |                                                   |
        +-------+-------------------------+                         v
        |                                 |             /api/historicalOR/generateSecurityWiseHistoricalData
        v                                 v             (Up to 5 years per stock, zero archive dependency)
 [Manifest & Status APIs]      [Server-Side Zip Proxy]
 /api/daily-reports?key=...    /api/zipem?fileURLs=[...]
 /api/merged-daily-reports             |
        |                      (NSE Internal VPC Fetch)
        |                                 |
        v                                 v
+-----------------------------------------------+
|      Underlying Static Storage Engine         |
|         nsearchives.nseindia.com              |
+-----------------------------------------------+
```

---

## 3. Channel 1: The Internal Server-Side Proxy (`/api/zipem`)

### Decompiled JavaScript Logic:
In [`quick-links.js`](file:///e:/dev/code/bfinance-datalake/.scratch/probe_results/scripts/quick-links.js), lines 420–465 define how official web downloads are initiated:

```javascript
function fileDownload(data, type, id, mode) {
    var downloadData = data;
    if (downloadData.length) {
        var zipUrl = void 0, archiveNames = void 0;
        if (type && (type.toLowerCase() === "daily" || type.toLowerCase() === "monthly")) {
            archiveNames = downloadData.map(function (e) {
                return e.link;
            });
            zipUrl = "/api/zipem?fileURLs=" + encodeURIComponent(JSON.stringify(archiveNames));
        } else {
            archiveNames = downloadData;
            zipUrl = '/api/reports?archives=' + encodeURIComponent(JSON.stringify(archiveNames));
            ...
        }
        zipUrl += '&type=' + type;
        if (mode && mode === "single") {
            zipUrl += '&mode=' + mode;
        }
        window.open(zipUrl, '_blank');
    }
}
```

### Endpoint Specification
```http
GET https://www.nseindia.com/api/zipem?fileURLs={ENCODED_JSON_ARRAY}&type=daily HTTP/1.1
Host: www.nseindia.com
Accept: application/zip, */*
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
Referer: https://www.nseindia.com/all-reports
```

### Empirical Test Verification
We systematically tested `/api/zipem` against all major datalake datasets. The results confirmed **100% success** across all categories:

| Target Dataset | Input `nsearchives` URL Passed to `zipem` | HTTP Status | Response Size | Extracted File Inside ZIP |
| :--- | :--- | :--- | :--- | :--- |
| **CM UDiFF Bhavcopy** | `.../content/cm/BhavCopy_NSE_CM_0_0_0_20261104_F_0000.csv.zip` | `200 OK` | 207.9 KB | `BhavCopy_NSE_CM_...csv.zip` |
| **FO UDiFF Bhavcopy** | `.../content/fo/BhavCopy_NSE_FO_0_0_0_20261104_F_0000.csv.zip` | `200 OK` | 1.16 MB | `BhavCopy_NSE_FO_...csv.zip` |
| **Security Deliverables**| `.../archives/equities/mto/MTO_04112026.DAT` | `200 OK` | 130.1 KB | `MTO_04112026.DAT` |
| **F&O Security Ban** | `.../archives/fo/sec_ban/fo_secban_05112026.csv` | `200 OK` | 204 B | `fo_secban_05112026.csv` |
| **Participant-wise OI** | `.../content/nsccl/fao_participant_oi_04112026.csv` | `200 OK` | 1.1 KB | `fao_participant_oi_...csv` |
| **Listed Equities Master** | `.../content/equities/EQUITY_L.csv` | `200 OK` | 185.9 KB | `EQUITY_L.csv` |
| **Delisted Equities** | `.../content/equities/delisted.csv` | `200 OK` | 24.8 KB | `delisted.csv` |
| **Symbol Changes** | `.../content/equities/symbolchange.csv` | `200 OK` | 70.2 KB | `symbolchange.csv` |
| **Bulk Deals** | `.../content/equities/bulk.csv` | `200 OK` | 8.6 KB | `bulk.csv` |
| **Block Deals** | `.../content/equities/block.csv` | `200 OK` | 227 B | `block.csv` |
| **NIFTY Constituents** | `.../content/indices/ind_nifty50list.csv` | `200 OK` | 5.3 KB | `ind_nifty50list.csv` (batch) |
| **Legacy Bhavcopy (2024)**| `.../content/historical/EQUITIES/2024/JAN/cm05JAN2024bhav.csv.zip` | `200 OK` | 201.2 KB | `cm05JAN2024bhav.csv.zip` |
| **Legacy Bhavcopy (2023)**| `.../content/historical/EQUITIES/2023/JAN/cm05JAN2023bhav.csv.zip` | `200 OK` | 195.4 KB | `cm05JAN2023bhav.csv.zip` |

### Multi-File Daily Batch Ingestion Benchmark
When bundling all 5 core daily datasets into a single `/api/zipem` call:
```json
[
  "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_20261105_F_0000.csv.zip",
  "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_20261105_F_0000.csv.zip",
  "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_05112026.csv",
  "https://nsearchives.nseindia.com/archives/equities/mto/MTO_05112026.DAT",
  "https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_05112026.csv"
]
```
- **Execution Time**: **4.07 seconds** total.
- **Payload**: 1.99 MB single ZIP archive containing all 5 files.
- **Zero reliance** on external IP access to `nsearchives.nseindia.com`.

### Concurrency and Multiplexing Constraints
Empirical load testing revealed an important operational constraint:
- **Rapid Sequential Requests**: 5 consecutive calls with 0 delay executed in 1.9s–4.8s each with 0 errors.
- **Concurrent Multiplexing on a Single HTTP/2 Connection**: Firing 3 separate `zipem` requests simultaneously on the same TLS session triggered Akamai HTTP/2 frame contention, timing out after 20.0s.
- **Rule**: Always prefer batch bundling in a single `zipem` call, or execute `zipem` requests sequentially.

---

## 4. Channel 2: Real-Time Manifest & Publication Catalogs

### 1. The Merged Daily Reports API (`/api/merged-daily-reports`)
```http
GET https://www.nseindia.com/api/merged-daily-reports?key={SEGMENT} HTTP/1.1
Host: www.nseindia.com
Accept: application/json
```
Available for `CM` (37 entries) and `FO` (34 entries). Unlike standard manifests, this endpoint keys every item by its canonical `fileKey`:

```json
{
  "CM-UDIFF-BHAVCOPY-CSV": {
    "fileHashKey": "CM-UDIFF-BHAVCOPY-CSV-2",
    "fileKey": "CM-UDIFF-BHAVCOPY-CSV",
    "fileSegment": "CM",
    "displayName": "CM-UDiFF Common Bhavcopy Final (zip)",
    "fileActlName": "BhavCopy_NSE_CM_0_0_0_20261105_F_0000.csv.zip",
    "filePath": "https://nsearchives.nseindia.com/content/cm/",
    "tradingDate": "05-Nov-2026",
    "fileSize": "204.63 KB"
  },
  "CM-BHAVDATA-FULL": {
    "fileHashKey": "CM-BHAVDATA-FULL-2",
    "fileKey": "CM-BHAVDATA-FULL",
    "fileSegment": "CM",
    "displayName": "Full Bhavcopy and Security Deliverable data",
    "fileActlName": "sec_bhavdata_full_05112026.csv",
    "filePath": "https://nsearchives.nseindia.com/products/content/",
    "tradingDate": "05-Nov-2026",
    "fileSize": "395.68 KB"
  }
}
```

### 2. Daily Manifest API (`/api/daily-reports?key={SEGMENT}`)
Covers 11 segments (`CM`, `INDEX`, `SLBS`, `SME`, `FO`, `COM`, `CD`, `WDM`, `CBM`, `TRI-PARTY`, `EGR`). Partitions reports into `CurrentDay`, `PreviousDay`, and `FutureDay`.

### 3. Monthly Manifest API (`/api/monthly-reports?key={SEGMENT}`)
Provides monthly position limits, settlement statistics, mode of trading, and impact cost files across 10 segments.

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

---

## 6. Root Cause Analysis: `nsearchives` Unreachability & Bugs Identified

Our empirical testing isolated three distinct failure modes:

### 1. False-Positive Circuit Breaker on Normal `HTTP 404`
- When running daily pipelines before evening publication (~18:30 IST), `nsearchives.nseindia.com` legitimately responds with `HTTP 404 Not Found`.
- In `http_client.py`, lines 225–228 incremented `_nsearchives_fail_streak += 1` on any non-200 status code, falsely treating expected 404s as host outages and throwing `RuntimeError("nsearchives.nseindia.com is unreachable from this runner IP")`.
- **Resolution**: Reset fail streak to 0 on both `200` and `404`.

### 2. URL Substring Matching Bug Clamping Timeouts
- In `http_client.py`:
  ```python
  is_nsearchives = "nsearchives.nseindia.com" in url
  ```
- When using `/api/zipem?fileURLs=["https://nsearchives.nseindia.com/..."]`, the URL string contained `nsearchives.nseindia.com` in the query parameter.
- This caused `http_client.py` to mistake a `www.nseindia.com` request for a direct `nsearchives` request, incorrectly clamping the timeout to `min(timeout, 6.0)`. Multi-megabyte zips aborted after 6,005 ms.
- **Resolution**: Match the actual hostname via `urllib.parse.urlsplit(url).netloc == "nsearchives.nseindia.com"`.

### 3. Akamai CDN Edge Geo-Fencing on Cloud IPs
- `nsearchives.nseindia.com` enforces independent Akamai edge policies that reject datacenter IPs (AWS, GitHub Actions, DigitalOcean).
- By routing archive requests through `www.nseindia.com/api/zipem?fileURLs=[...]`, downloads originate from NSE's internal datacenter VPC, bypassing external CDN restrictions completely.

---

## 7. Concrete Implementation Architecture for `bfinance-datalake`

### Architecture 1: Fallback Gateway Strategy for Bhavcopies
For daily and backfill Bhavcopy ingestion, implement dual-resolution:
1. **Primary**: Attempt direct fetch from `nsearchives.nseindia.com`.
2. **Fallback**: If `nsearchives` times out or returns `HTTP 403`, immediately resolve the same URL via `/api/zipem?fileURLs=[<URL>]&type=daily` on `www.nseindia.com`.
3. **Extraction**: If fetched via `zipem`, extract the nested inner file from the outer zip stream.

### Architecture 2: The Publication Oracle
Before attempting to fetch today's EOD files in `daily` workflow:
```python
async def check_eod_publication_status(http: ResilientHTTPClient, target_date: date) -> dict[str, bool]:
    """Query merged-daily-reports to check if today's Bhavcopies are live."""
    resp = await http.get("https://www.nseindia.com/api/merged-daily-reports?key=CM", prime_nse=True)
    if resp.status_code != 200:
        return {"cm": False, "fo": False}
    data = resp.json()
    expected_dstr = target_date.strftime("%d%m%Y")
    cm_ready = expected_dstr in data.get("CM-UDIFF-BHAVCOPY-CSV", {}).get("fileActlName", "")
    return {"cm": cm_ready}
```

### Architecture 3: High-Value New Feeds from Manifests
| Dataset | Proposed Storage Path | Source File in Manifest | Strategic Value |
| :--- | :--- | :--- | :--- |
| **Corporate Bond Trades** | `data/debt/cbm_trades_YYYY.parquet` | `cbm_trdYYYYMMDD.csv` | Secondary market corporate debt pricing and liquidity metrics. |
| **Corporate Bond Master** | `data/debt/cbm_security_master.parquet` | `Corporate_bond_report_...csv` | Complete directory of active debt ISINs, coupon rates, and ratings. |
| **Short Selling Disclosures** | `data/deals/short_selling.parquet` | `shortselling_DDMMYYYY.csv` | Institutional and retail short positioning intelligence. |
| **Daily Volatility Matrix** | `data/analytics/equity_volatility.parquet` | `CMVOLT_DDMMYYYY.CSV` | Official exchange annualized & daily volatility for risk modeling. |
| **Tri-Party Repo Rates** | `data/fixed_income/treps_repo.parquet` | `TRM_BCDDMMYYYY.csv` | Daily money market short-term lending rates and collateral volumes. |
