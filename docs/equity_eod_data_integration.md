# Equity EOD Data Integration (NSE & BSE Cash Equities)

## Overview

`bfinance-datalake` provides production-grade, 30+ year point-in-time (PIT) ingestion of daily cash equity OHLCV, ISIN identifiers, trade counts, turnover value, and deliverable volume for both the **National Stock Exchange of India (NSE)** and the **Bombay Stock Exchange (BSE)**.

Because Indian exchanges have undergone multiple structural and regulatory migrations between **1995 and the present day**, no single API or file format spans the full history. The datalake orchestrates a resilient multi-tier ingestion architecture that transparently bridges regulatory cutovers, file format deprecations, and exchange data anomalies.

---

## Historical Boundaries ("From Where to Where")

Live probes across four decades (1995–2026) verified the exact temporal availability for every exchange feed:

| Segment | Historical Era | Valid Date Range | Primary Source / Archive Format | Delivery & Volume Metrics |
| :--- | :--- | :--- | :--- | :--- |
| **NSE Equities** | **Modern UDIFF Era** | **`2024-07-08` to Present** | SEBI UDIFF Zip (`BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip`) | Merged with `sec_bhavdata_full_{DDMMYYYY}.csv` for Delivery Qty & % |
| **NSE Equities** | **Full Bhavdata Era** | **`2020-01-01` to `2024-07-05`** | `sec_bhavdata_full_{DDMMYYYY}.csv` | Built-in Deliverable Qty, Delivery %, Trades, Turnover (₹ Lakhs) |
| **NSE Equities** | **Legacy Bhavcopy + MTO** | **`2002-01-07` to `2019-12-31`** | Legacy Bhavcopy (`cm{DD}{MON}{YYYY}bhav.csv.zip`) + MTO Delivery (`MTO_{DDMMYYYY}.DAT`) | Deliverable Qty & Delivery % joined from MTO file on `[Symbol, Series]` |
| **NSE Equities** | **Early Demat / Legacy** | **`1995-01-03` to `2001-12-31`** | Legacy Bhavcopy (`cm{DD}{MON}{YYYY}bhav.csv.zip`) | OHLCV only (Pre-dates compulsory rolling settlement; MTO = 404) |
| **NSE Equities** | **Pre-2016 Symbol Backfill** | **`1995-01-01` to `2015-12-31`** | NSE Official Historical API (`/api/historicalOR/generateSecurityWiseHistoricalData`) | Raw individual stock trades, turnover, and delivery |
| **BSE Equities** | **Modern UDIFF Era** | **`2024-07-08` to Present** | SEBI UDIFF CSV (`BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV`) | Uncompressed CSV; Delivery merged from Gross Delivery (`SCBSEALL{DDMM}.zip`) |
| **BSE Equities** | **Legacy Zip Era** | **`2007-01-02` to `2024-07-05`** | Legacy Zip (`EQ{DDMMYY}_CSV.ZIP`) + ISIN Zip (`EQ_ISINCODE_{DDMMYY}.zip`) | Delivery joined from Gross Delivery (`SCBSEALL{DDMM}.zip`) on `InstrumentId` |
| **BSE Equities** | **Pre-2007 Era** | **`2000-01-01` to `2006-12-31`** | Legacy Bhavcopy Archives | Basic OHLCV via BSE archive repository |

---

## NSE Cash Equities Architecture

### 1. Modern UDIFF Era (`2024-07-08` to Present)

On July 8, 2024, the Securities and Exchange Board of India (SEBI) mandated the **Unified Data Interchange Format (UDIFF)** across all market infrastructure institutions (MIIs).

* **Download URL**:
  ```http
  GET https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{YYYYMMDD}_F_0000.csv.zip
  ```
* **Payload Structure**: Compressed ZIP containing a single `.csv` file with 34 standardized columns.
* **Segment Filtering**: Filtered strictly on `Sgmt == 'CM'` (Cash Market).
* **Delivery Enrichment**: The NSE CM UDIFF file reports trade volume and turnover, but omits gross deliverable quantity and percentage. The datalake concurrently downloads `sec_bhavdata_full_{DDMMYYYY}.csv` and merges `DeliverableQty` and `DeliveryPct` on `['Symbol', 'Series']`.

### 2. Full Bhavdata Era (`2020-01-01` to `2024-07-05`)

From January 1, 2020, through July 5, 2024, NSE published a unified full security bhavdata file:

* **Download URL**:
  ```http
  GET https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{DDMMYYYY}.csv
  ```
* **Payload Structure**: Uncompressed CSV containing OHLC, trades, turnover in lakhs, and deliverable quantity/percentage in a single file.
* **Date Validation**: The scraper validates the internal header field `DATE1` against the requested calendar day to prevent stale file ingestion.
* **ISIN Resolution**: For dates in this window, ISINs are mapped by cross-referencing the concurrent legacy CM bhavcopy or universe master.

### 3. Legacy Bhavcopy & MTO Delivery (`1995-01-03` to `2019-12-31`)

Before 2020, trade execution and security delivery were recorded in distinct exchange reports:

* **Price & Volume Archive**:
  ```http
  GET https://nsearchives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MON}/cm{DD}{MON}{YYYY}bhav.csv.zip
  ```
  *(Where `{MON}` is the uppercase 3-letter month abbreviation, e.g., `JAN`, `JUL`).*
  
  > **SEBI Cutover Sunset Proof**:  
  > This legacy URL operated continuously from **January 3, 1995**, until **July 5, 2024**. On July 8, 2024, NSE permanently turned off legacy generation, returning `HTTP 404 Not Found`.

* **MTO Gross Delivery Archive (`2002-01-07` to `2019-12-31`)**:
  ```http
  GET https://nsearchives.nseindia.com/archives/equities/mto/MTO_{DDMMYYYY}.DAT
  ```
  - Text-based fixed-structure report generated at settlement EOD.
  - Record structure: Lines beginning with `20,` contain record details:
    ```text
    Record Type,Sr No,Name of Security,Quantity Traded,Deliverable Quantity(gross across client level),% of Deliverable Quantity to Traded Quantity
    20,1,20MICRONS,EQ,258669,116118,44.89
    ```
  - Joined to the daily OHLCV dataframe on `['Symbol', 'Series']`.
  - Prior to January 2002 (account-period settlement era), MTO endpoints return `HTTP 404`.

### 4. NSE `/api/historicalOR` Individual Stock Fallback

To bypass bulk archive access restrictions or to seed high-fidelity single-ticker histories for 1995–2015:

* **URL Structure**:
  ```http
  GET https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData?from=01-01-{year}&to=31-12-{year}&symbol={SYMBOL}&type=priceVolumeDeliverable&series=ALL&csv=true
  ```
* **TLS Fingerprint & Session Priming**: Requires browser TLS impersonation (Chrome 124 JA3/ALPN via `curl_cffi`) and initial cookie harvesting (`prime_nse=True`) from `nseindia.com`.
* **Output**: CSV table returning unadjusted OHLCV, Average Price, Number of Trades, and Deliverable Quantity across all historical trading sessions in that calendar year.

---

## BSE Cash Equities Architecture

### 1. Modern UDIFF Era (`2024-07-08` to Present)

* **Download URL**:
  ```http
  GET https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{YYYYMMDD}_F_0000.CSV
  ```
* **Payload Structure**: Plain, uncompressed CSV (~750 KB to ~780 KB) containing all listed BSE instruments.
* **Key Difference vs NSE**: BSE serves raw uncompressed `.CSV`, whereas NSE serves `.csv.zip`.
* **Equity Filtering**: Filtered by `Sgmt == 'CM'`. Furthermore, debt instruments are purged via `filter_bse_equity_only()`.

### 2. Legacy Bhavcopy & ISIN Archive (`2007-01-02` to `2024-07-05`)

Prior to UDIFF, BSE published trade data across two linked files:

* **Daily Bhavcopy ZIP**:
  ```http
  GET https://www.bseindia.com/download/BhavCopy/Equity/EQ{DDMMYY}_CSV.ZIP
  ```
  *(Note two-digit year `{YY}`, e.g., `EQ050724_CSV.ZIP`).*
  
  > **BSE Cutover Proof**:  
  > On and after July 8, 2024, requesting `EQ{DDMMYY}_CSV.ZIP` returns `HTTP 200` with an HTML redirect/error page (`<!DOCTYPE html>...`, size ~16.9 KB) instead of a ZIP binary (`PK` header). The scraper checks `resp.content.startswith(b"PK")` to guarantee file integrity.

* **Daily ISIN Code Mapping ZIP**:
  ```http
  GET https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{DDMMYY}.zip
  ```
  Maps `SC_CODE` (BSE 6-digit scrip code) to the instrument's official 12-character alphanumeric `ISIN_CODE`.

### 3. BSE Gross Delivery Archives (`2007` to Present)

* **Download URL**:
  ```http
  GET https://www.bseindia.com/BSEDATA/gross/{YYYY}/SCBSEALL{DDMM}.zip
  ```
* **Payload Format**: Pipe-delimited (`|`) text file compressed inside a ZIP:
  ```text
  DATE|SCRIP CODE|DELIVERY QTY|DELIVERY VAL|DAY'S VOLUME|DAY'S TURNOVER|DELV. PER.
  05/07/2024|500325|481923|1529145610|1248921|3964120300|38.59
  ```
* **Join Key**: Joined to the normalized BSE dataframe on `InstrumentId` (`SCRIP CODE`).

---

## Technical Quirks & Data Cleaning Engine

### 1. The BSE Glued Comma Bug (`_BSE_GLUED_LINE_RE`)
* **Problem**: In BSE legacy bhavcopy generation, buffer flushing flaws periodically omit the trailing newline between records. The last column of line $N$ (e.g. turnover or closing index) is directly concatenated to the 6-digit `SC_CODE` of line $N+1$.
* **Live Verification Evidence (2024-07-05)**:
  ```text
  Glued line occurrences found: 3
  Example glued match: ('23.96', '120853,1')
  ```
* **Regex Mitigation**:
  ```python
  _BSE_GLUED_LINE_RE = re.compile(r"(\d+\.\d{2}),(\d{6},[A-Z0-9])")
  repaired_csv = _BSE_GLUED_LINE_RE.sub(r"\1,\n\2", raw_csv)
  ```

### 2. BSE Security Type Filtering (`SC_TYPE == 'Q'`)
Legacy BSE bhavcopies contain equities, debt debentures, preference shares, and institutional bonds in a single file:
* `SC_TYPE == 'Q'`: Ordinary Equity Shares (e.g., 4,063 issues on 2024-07-05).
* `SC_TYPE == 'D'`: Debentures (199 issues).
* `SC_TYPE == 'B'`: Corporate Bonds (110 issues).
* `SC_TYPE == 'P'`: Preference Shares (1 issue).
* **Action**: Scraper strictly enforces `df[df["SC_TYPE"] == "Q"]` to prevent contaminating equity models with debt yields.

### 3. BSE Corporate & Government Debt Removal (`filter_bse_equity_only`)
Even in modern UDIFF files, certain debt series are categorized under cash equities:
* Government Securities (Group `G`).
* Corporate Bonds in Group `F` carrying debt ISIN prefixes (`INE...` with debenture identifiers).
* **Action**: `filter_bse_equity_only(df)` strips these series before committing to parquet.

### 4. Legacy OHLC Repair Algorithm (`repair_ohlc`)
In 1995–2003 NSE bhavcopies, illiquid stocks frequently contain zero open prices (`Open = 0`), inverted high/low values (`High < Low`), or traded prices outside the reported bounds:
* **Zero Open Price**: Replaced with `PrevClose` (or `Close` if `PrevClose <= 0`).
* **Inverted High/Low**: `High` and `Low` columns are swapped if `High < Low`.
* **Range Enclosure**: `High` is clamped to `max(High, Open, Close)` and `Low` is clamped to `min(Low, Open, Close)`.

---

## Data Schema & Field Mappings

Both NSE and BSE cash feeds are normalized into the unified datalake parquet schema:

| Unified Column | Type | NSE UDIFF Source | NSE Full Bhav Source | BSE UDIFF Source | BSE Legacy Source | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`Date`** | Timestamp | `TradDt` | `DATE1` | `TradDt` | URL / File Date | Session trade date (midnight IST) |
| **`Exchange`** | String | `"NSE"` | `"NSE"` | `"BSE"` | `"BSE"` | Market identifier (`NSE` or `BSE`) |
| **`InstrumentId`**| String | `FinInstrmId` | `SYMBOL` | `FinInstrmId` | `SC_CODE` | Exchange internal identifier |
| **`Symbol`** | String | `TckrSymb` | `SYMBOL` | `TckrSymb` | `SC_NAME` | Trading ticker symbol |
| **`Series`** | String | `SctySrs` | `SERIES` | `SctySrs` | `SC_GROUP` | Market segment (`EQ`, `BE`, `A`, `B`, etc.) |
| **`ISIN`** | String | `ISIN` | Legacy / Master Map | `ISIN` | `EQ_ISINCODE` map | 12-character International Securities ID |
| **`Open`** | Float | `OpnPric` | `OPEN_PRICE` | `OpnPric` | `OPEN` | Day's opening price in INR |
| **`High`** | Float | `HghPric` | `HIGH_PRICE` | `HghPric` | `HIGH` | Day's highest traded price in INR |
| **`Low`** | Float | `LwPric` | `LOW_PRICE` | `LwPric` | `LOW` | Day's lowest traded price in INR |
| **`Close`** | Float | `ClsPric` | `CLOSE_PRICE` | `ClsPric` | `CLOSE` | Session closing settlement price in INR |
| **`Last`** | Float | `LastPric` | `LAST_PRICE` | `LastPric` | `LAST` | Last traded price (LTP) in INR |
| **`PrevClose`** | Float | `PrvsClsgPric`| `PREV_CLOSE` | `PrvsClsgPric`| `PREVCLOSE` | Previous trading session's close |
| **`Volume`** | Float | `TtlTradgVol` | `TTL_TRD_QNTY` | `TtlTradgVol` | `NO_OF_SHRS` | Total number of shares traded |
| **`TurnoverValue`**| Float | `TtlTrfVal` | `TURNOVER_LACS`*1e5 | `TtlTrfVal` | `NET_TURNOV` | Total traded value in INR |
| **`Trades`** | Float | `TtlNbOfTxsExctd`| `NO_OF_TRADES`| `TtlNbOfTxsExctd`| `NO_TRADES`| Total execution tickets count |
| **`DeliverableQty`**| Float | Full Bhav merge | `DELIV_QTY` | Gross Deliv merge | `DELIVERY QTY` | Gross quantity marked for delivery |
| **`DeliveryPct`** | Float | Full Bhav merge | `DELIV_PER` | Gross Deliv merge | `DELV. PER.` | Deliverable quantity as % of volume |

---

## Live Probe Verification Evidence

The table below catalogs real network responses collected during live validation:

```text
====================================================================================================================
FEED / DATE        HTTP STATUS   PAYLOAD SIZE   RECORDS   VERIFIED EVIDENCE / HIGHLIGHTS
====================================================================================================================
NSE UDIFF
  2024-07-08       200 OK        166,236 bytes  2,815     Official SEBI Go-Live Day. Sgmt='CM', FinInstrmTp='STK'
  2024-07-05       200 OK        164,296 bytes  2,775     Parallel test feed available pre-cutover
  2024-01-02       200 OK        151,130 bytes  2,658     Earliest UDIFF beta rollout on NSE archives
  2024-10-25       200 OK        170,963 bytes  2,902     Production modern feed stability verified
--------------------------------------------------------------------------------------------------------------------
NSE FULL BHAVDATA
  2024-07-08       200 OK        303,411 bytes  2,637     Delivers [' DELIV_QTY', ' DELIV_PER'] for UDIFF merge
  2021-01-01       200 OK        222,351 bytes  1,993     Full coverage verified
  2020-01-01       200 OK        208,521 bytes  1,901     Earliest availability of full bhavdata format
  2019-01-01       404 Not Found 3,925 bytes    0         Format did not exist prior to Jan 1, 2020
--------------------------------------------------------------------------------------------------------------------
NSE LEGACY CM BHAVCOPY
  2024-07-08       404 Not Found 3,815 bytes    0         PROVED DISCONTINUED after 2024-07-05 cutover
  2024-07-05       200 OK        109,203 bytes  2,775     Final trading day of 30-year legacy cm bhavcopy
  2016-01-04       200 OK        59,858 bytes   1,623     Historical coverage confirmed
  2000-01-03       200 OK        28,424 bytes   1,188     Millennium session verified
  1995-01-03       200 OK        5,251 bytes    258       Earliest electronic trading archive on NSE
--------------------------------------------------------------------------------------------------------------------
NSE MTO GROSS DELIVERY
  2024-07-05       200 OK        94,511 bytes   2,497     Header '10,MTO', Records '20,{SrNo},{Sym},{Series}...'
  2016-01-04       200 OK        58,110 bytes   1,561     Rolling settlement deliverable quantities verified
  2005-01-03       200 OK        28,639 bytes   752       Early T+2 settlement records confirmed
  2002-01-07       200 OK        21,440 bytes   512       Earliest available MTO archive on record
  2000-01-05       404 Not Found 3,925 bytes    0         MTO did not exist prior to rolling settlement
--------------------------------------------------------------------------------------------------------------------
BSE UDIFF
  2024-07-08       200 OK        775,348 bytes  4,517     Uncompressed CSV, Reliance: Opn=3179.9, Cls=3202.1
  2024-07-05       200 OK        752,306 bytes  4,373     Parallel run verified
  2024-10-25       200 OK        751,297 bytes  4,374     Reliance post-bonus: Opn=2687.7, Cls=2655.45
--------------------------------------------------------------------------------------------------------------------
BSE LEGACY BHAVCOPY & ISIN
  2024-07-08       200 OK (HTML) 16,901 bytes   0         PROVED DISCONTINUED (Non-zip HTML error page served)
  2024-07-05       200 OK (ZIP)  142,761 bytes  4,376     Final legacy zip. SC_TYPE: Q=4063, D=199, B=110, P=1
  2008-01-01       200 OK (ZIP)  98,982 bytes   2,906     Historical coverage confirmed. SC_TYPE: Q=2889
  2007-01-02       200 OK (ZIP)  88,354 bytes   2,655     Historical coverage confirmed. SC_TYPE: Q=2644
--------------------------------------------------------------------------------------------------------------------
BSE GROSS DELIVERY (SCBSEALL)
  2024-07-08       200 OK        92,208 bytes   4,517     Deliv Qty, Turnover, Delv Pct verified
  2008-01-01       200 OK        68,432 bytes   2,902     Deliv Qty, Turnover, Delv Pct verified
  2007-01-02       200 OK        61,904 bytes   2,649     Earliest verified pipe-delimited gross delivery
--------------------------------------------------------------------------------------------------------------------
NSE HISTORICAL OR API (/api/historicalOR)
  RELIANCE 2015    200 OK        39,077 bytes   248 days  Full trades, volume, delivery qty & %
  RELIANCE 2005    200 OK        39,116 bytes   251 days  Full trades, volume, delivery qty & %
  RELIANCE 2000    200 OK        53,873 bytes   250 days  Full trades & turnover (Delivery '-' pre-2002)
====================================================================================================================
```

---

## Storage & Partitioning Architecture

1. **Partition Path**:
   - `equities/nse_cm_{year}.parquet`
   - `equities/bse_cm_{year}.parquet`
2. **Deduplication Key**: `["Date", "Exchange", "InstrumentId", "Series"]`
3. **Sort Order**: `["Date", "Symbol"]`
4. **Compression**: Snappy compression via Apache Arrow.
5. **State Watermarking**: Ingestion checkpoints are committed to SQLite (`state.db`) in dataset partitions `equity_eod_nse` and `equity_eod_bse` only after file writes succeed.

---

## CLI & Pipeline Invocation

### Full Backfill Range
```bash
# Ingest full historical Cash EOD data for both NSE and BSE from 2016 to 2026
uv run datalake backfill --start 2016-01-01 --end 2026-10-31

# Ingest specific 30-year multi-decade range
uv run datalake backfill --start 1995-01-01 --end 2005-12-31
```

### Daily Incremental Update
```bash
# Fetch latest 7 days across NSE and BSE equities with delivery reconciliation
uv run datalake update --lookback-days 7
```

### Fast Cloud Seeding (2000–2015)
```bash
# Seed 15 years of daily bars for all listed stocks via unauthenticated Upstox CDN
uv run datalake seed-cloud --start-year 2000 --end-year 2015
```
