# NSE & BSE Corporate Annual Reports Integration

## Overview

`bfinance-datalake` provides an automated, dual-exchange catalog and retrieval pipeline for **Corporate Annual Reports** across all Indian equities listed on the National Stock Exchange of India (**NSE**) and the Bombay Stock Exchange (**BSE**).

Annual Reports constitute the definitive statutory disclosure for Indian public corporations, containing:
* Comprehensive Ind-AS Audited Standalone & Consolidated Financial Statements.
* Management Discussion & Analysis (**MD&A**).
* Auditor's Report, Notes to Accounts, and Contingent Liabilities.
* Related Party Transactions (**RPT**).
* Business Responsibility and Sustainability Reporting (**BRSR** / ESG).
* Director's Report and Corporate Governance Disclosures.

The datalake indexes direct HTTPS PDF download links spanning **FY 2009–10 through FY 2025–26** (~17–18 continuous fiscal years per company), storing the metadata in columnar Parquet format for downstream NLP, LLM financial extraction, and fundamental equity research.

---

## 1. Endpoint Specifications

### 1.1 NSE Annual Reports API

NSE exposes a structured JSON API that returns the historical catalog of Annual Report PDF filings for any active equity symbol.

#### URL Structure
```http
GET https://www.nseindia.com/api/annual-reports?index=equities&symbol={symbol}
```

* **Query Parameters**:
  * `index=equities`: Required filter designating the cash equity market segment.
  * `symbol`: The NSE trading ticker in uppercase (e.g., `RELIANCE`, `TCS`, `INFY`, `HDFCBANK`).

#### Request Headers & Session Priming
```http
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
Accept: application/json, text/plain, */*
Accept-Language: en-US,en;q=0.9
Accept-Encoding: gzip, deflate
Referer: https://www.nseindia.com/companies-listing/corporate-filings-annual-reports
```
*(Direct queries without session cookies are challenged by Akamai WAF; the datalake primes the session via `curl_cffi` against `https://www.nseindia.com` before issuing the API call).*

#### Response Schema (NSE)
The response is a JSON object with a `data` list:

```json
{
  "data": [
    {
      "companyName": "Reliance Industries Limited",
      "fromYr": "2025",
      "toYr": "2026",
      "submission_type": "New",
      "broadcast_dttm": "28-MAY-2026 13:39:47",
      "disseminationDateTime": "28-MAY-2026 13:39:48",
      "timeTaken": "00:00:01",
      "fileName": "https://nsearchives.nseindia.com/annual_reports/AR_29285_RELIANCE_2025_2026_A_11007429_28052026133947.pdf",
      "attFileSize": "10.50 MB"
    },
    {
      "companyName": "Reliance Industries Limited",
      "fromYr": "2024",
      "toYr": "2025",
      "submission_type": "New",
      "broadcast_dttm": "29-MAY-2025 18:22:10",
      "disseminationDateTime": "29-MAY-2025 18:22:11",
      "timeTaken": "00:00:01",
      "fileName": "https://nsearchives.nseindia.com/annual_reports/AR_25114_RELIANCE_2024_2025_A_9812401_29052025182210.pdf",
      "attFileSize": "14.20 MB"
    }
  ]
}
```

#### Field Definitions (NSE)

| Field | Data Type | Description |
| :--- | :--- | :--- |
| `companyName` | String | Registered corporate name of the entity |
| `fromYr` | String | Beginning calendar year of the financial year (e.g., `"2025"`) |
| `toYr` | String | Ending calendar year of the financial year (e.g., `"2026"`) |
| `submission_type` | String | Filing status (`"New"` for initial filing; `"Revised"` for amendments) |
| `broadcast_dttm` | String | Timestamp when the exchange broadcasted the filing (IST) |
| `disseminationDateTime` | String | Timestamp when the document was published to the public portal |
| `timeTaken` | String | Exchange processing lag (e.g., `"00:00:01"`) |
| `fileName` | String | Direct, full HTTPS download URL on `nsearchives.nseindia.com` |
| `attFileSize` | String | File size of the PDF attachment (e.g., `"10.50 MB"`) |

---

### 1.2 BSE Annual Reports API

BSE provides an unauthenticated REST endpoint returning historical Annual Reports by numerical security code.

#### URL Structure
```http
GET https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?scripcode={scripcode}&type=C
```

> **Critical Parameter Requirement (`scripcode` vs `scrip_code`):**  
> The BSE API requires the query parameter name **`scripcode`** (without an underscore).  
> Passing `scrip_code={scrip}` returns an empty `Table: []` payload with zero results.

* **Query Parameters**:
  * `scripcode`: BSE 6-digit Scrip Code (e.g., `500325` for Reliance, `532540` for TCS, `500209` for Infosys, `500180` for HDFC Bank).
  * `type=C`: Corporate / Equity filings filter.

#### Request Headers
```http
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36
Accept: application/json, text/plain, */*
Accept-Language: en-US,en;q=0.9
Accept-Encoding: gzip, deflate
Referer: https://www.bseindia.com/
```

#### Response Schema (BSE)
The response is a JSON object with a `Table` array:

```json
{
  "Table": [
    {
      "year": "2026",
      "file_name": "cc08294f-ec68-4f15-aaa4-c14a3c4bdfb2.pdf.pdf",
      "dt_tm": "2026-05-28T13:37:02.723"
    },
    {
      "year": "2025",
      "file_name": "500325_2025.pdf",
      "dt_tm": "2025-05-29T18:19:45.000"
    },
    {
      "year": "2010",
      "file_name": "5003250310.pdf",
      "dt_tm": "2012-03-26T00:00:00"
    }
  ]
}
```

#### BSE PDF Download URL Construction
Unlike NSE (which returns a fully-qualified URL), BSE returns a relative `file_name`. The full PDF download URL is constructed as:

```http
https://www.bseindia.com/AnnualReports/{scripcode}/{file_name}
```

* **Example (Reliance FY26)**:  
  `https://www.bseindia.com/AnnualReports/500325/cc08294f-ec68-4f15-aaa4-c14a3c4bdfb2.pdf.pdf`
* **Example (Reliance FY10)**:  
  `https://www.bseindia.com/AnnualReports/500325/5003250310.pdf`

---

## 2. Historical Boundaries ("From Where to Where")

Empirical live verification across blue-chip equities established the exact temporal limits for digital Annual Reports across both exchanges:

| Exchange | Ticker / Scrip | Earliest Available FY | Latest Available FY | Total Fiscal Years | File Types |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **NSE** | `RELIANCE` | **2009–2010** | **2025–2026** | **17 Years** | PDF (10–35 MB) |
| **NSE** | `TCS` | **2009–2010** | **2025–2026** | **17 Years** | PDF (12–25 MB) |
| **NSE** | `INFY` | **2009–2010** | **2025–2026** | **17 Years** | PDF (8–20 MB) |
| **NSE** | `HDFCBANK` | **2008–2009** | **2025–2026** | **18 Years** | PDF (12–40 MB) |
| **BSE** | `500325` (RELIANCE) | **2010** | **2026** | **17 Years** | PDF |
| **BSE** | `532540` (TCS) | **2010** | **2026** | **17 Years** | PDF |
| **BSE** | `500209` (INFY) | **2010** | **2026** | **17 Years** | PDF |
| **BSE** | `500180` (HDFCBANK) | **2010** | **2026** | **17 Years** | PDF |

### Coverage Characteristics
1. **Digital Era (FY2010–Present)**: 100% of listed companies have direct digital PDF copies published by both exchanges.
2. **Pre-2010 Filings**: Annual reports prior to FY 2009–10 were submitted in physical paper format to the Ministry of Corporate Affairs (MCA) and exchange registries. Selected pre-2010 reports are hosted in legacy exchange archival bundles or available on corporate investor relations portals.

---

## 3. Data Schema & Parquet Storage

Catalog records are unified and written to `documents/annual_reports.parquet`:

```python
{
    "Symbol": "RELIANCE",
    "ScripCode": "500325",
    "Year": "2025-2026",
    "FileName": "AR_29285_RELIANCE_2025_2026_A_11007429_28052026133947.pdf",
    "PDFUrl": "https://nsearchives.nseindia.com/annual_reports/AR_29285_RELIANCE_2025_2026_A_11007429_28052026133947.pdf"
}
```

### Table Schema

| Column Name | Type | Description | Sample Value |
| :--- | :--- | :--- | :--- |
| `Symbol` | `string` | NSE Trading Symbol (empty if BSE-only scrip) | `"RELIANCE"` |
| `ScripCode` | `string` | BSE Numerical Scrip Code (empty if NSE-only) | `"500325"` |
| `Year` | `string` | Financial year span or single calendar year | `"2025-2026"` |
| `FileName` | `string` | Base filename of the PDF | `"AR_29285_RELIANCE_2025_2026_A_11007429_28052026133947.pdf"` |
| `PDFUrl` | `string` | Full downloadable HTTPS URL | `https://nsearchives.nseindia.com/annual_reports/...` |

### Partitioning & Deduplication
* **Target Path**: `data/documents/annual_reports.parquet`
* **Deduplication Key**: `["Symbol", "Year", "FileName"]` (for NSE filings) and `["ScripCode", "Year", "FileName"]` (for BSE filings).
* **Sorting Key**: `["Symbol", "Year"]` descending.

---

## 4. Technical Constraints & Handling

### 1. Large Attachment Handling
Annual Report PDF attachments frequently exceed 20 MB (reaching 50+ MB for large banking conglomerates like HDFC Bank and SBI).
* **Datalake Strategy**: The ingestion pipeline catalogs all document metadata, verification timestamps, and URLs first into Parquet without downloading multi-gigabyte payloads in memory.
* **On-Demand Streaming**: When downstream document processors (e.g. OCR, PyMuPDF, Unstructured, LlamaParse) require document text, files are downloaded via chunked streams directly to disk.

### 2. Akamai Anti-Bot Protection
* Downloads from `nsearchives.nseindia.com` require standard browser headers (`ARCHIVE_HEADERS`) with an authentic Chrome User-Agent.
* Direct scraping without headers results in HTTP 403 Forbidden.

### 3. Rate Limiting & Concurrency
* Scraper queries are throttled with polite delays (`0.15s` per company) using an `asyncio.Semaphore` pool to prevent IP blacklisting.

---

## 5. Usage in `bfinance-datalake`

### Implementation
The logic is implemented in [`DealsAndDocumentsScraper`](file:///e:/dev/code/bfinance-datalake/src/datalake/scrapers/deals_docs.py):
* `sync_annual_reports_nse(symbols: list[str])`: Iterates through equities, querying `/api/annual-reports`, parsing fiscal years and PDF links, and persisting to Parquet.
* `sync_annual_reports_bse(scrip_codes: list[str], symbols: list[str] | None = None)`: Queries `api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?scripcode={scrip}&type=C`, formats direct URLs, and merges records.

### CLI Invocation

```bash
# Sync Annual Reports for specified equities
uv run datalake xbrl --symbols RELIANCE,TCS,HDFCBANK,INFY --bse-scrips 500325,532540,500180,500209

# Query cataloged Annual Reports via Python / DuckDB
uv run python -c "
import duckdb
con = duckdb.connect()
df = con.execute('''
    SELECT Symbol, Year, FileName, PDFUrl
    FROM \"data/documents/annual_reports.parquet\"
    WHERE Symbol = 'RELIANCE'
    ORDER BY Year DESC
''').df()
print(df)
"
```
