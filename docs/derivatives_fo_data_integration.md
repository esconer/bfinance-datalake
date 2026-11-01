# Derivatives F&O Data Integration (NSE & BSE Equity Derivatives)

## Overview

`bfinance-datalake` provides historical and daily point-in-time (PIT) ingestion of equity futures and options contracts for both the **National Stock Exchange of India (NSE)** and the **Bombay Stock Exchange (BSE)**.

The dataset captures the full 26-year evolution of the Indian derivatives market—from the inception of cash-settled index futures on **June 12, 2000**, to the present multi-expiry options environment handling over 35,000+ active contracts daily.

Ingested metrics include:
* **Contract Definitions**: Underlying Symbol, Expiry Date, Strike Price, Option Type (`CE`, `PE`, or `XX` for futures).
* **Pricing & Valuation**: Open, High, Low, Close, Daily Settlement Price (`SettlePrice`), and Underlying Spot Price (`UnderlyingPrice`).
* **Liquidity & Open Interest**: Traded Contracts Volume, Turnover Value in INR, Total Open Interest (`OpenInterest`), and Change in Open Interest (`ChangeInOI`).

---

## Historical Boundaries ("From Where to Where")

Live probes across historical trading sessions established the precise temporal boundaries for Indian exchange derivatives:

| Segment | Historical Era | Valid Date Range | Primary Source / Archive Format | Contract Coverage |
| :--- | :--- | :--- | :--- | :--- |
| **NSE F&O** | **Modern UDIFF Era** | **`2024-07-08` to Present** | SEBI UDIFF Zip (`BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip`) | All Index & Stock Futures & Options (`IDF`, `STF`, `IDO`, `STO`) |
| **NSE F&O** | **Legacy Bhavcopy Era** | **`2000-06-12` to `2024-07-05`** | Legacy Bhavcopy Zip (`fo{DD}{MON}{YYYY}bhav.csv.zip`) | Full history of `FUTIDX`, `FUTSTK`, `OPTIDX`, `OPTSTK` |
| **NSE F&O** | **Pre-Derivatives Era** | **Prior to `2000-06-12`** | None (Returns `HTTP 404`) | Equity derivatives did not exist in India prior to June 12, 2000 |
| **BSE F&O** | **Modern UDIFF Era** | **`2024-07-08` to Present** | SEBI UDIFF CSV (`BhavCopy_BSE_FO_0_0_0_{YYYYMMDD}_F_0000.CSV`) | SENSEX & BANKEX Index Futures & Options + Liquid Stock F&O |
| **BSE F&O** | **Pre-2024 Relaunch Era**| **Prior to `2024-07-08`** | Low/Zero Liquidity Archives | Disregarded (Derivatives volume was nominal prior to the 2023–2024 relaunch) |

### Key Milestones in the Indian Derivatives Timeline

1. **2000-06-12 (Index Futures Launch)**: NSE launched trading in Nifty 50 Index Futures (`FUTIDX`). Only 3 near-month contracts existed on the first day.
2. **2001-06-04 (Index Options Launch)**: Trading commenced in Nifty 50 European-style Index Options (`OPTIDX`).
3. **2001-07-02 (Stock Options Launch)**: Trading commenced in American-style Individual Stock Options (`OPTSTK`).
4. **2001-11-09 (Stock Futures Launch)**: Trading commenced in Individual Stock Futures (`FUTSTK`).
5. **2019-10-01 (Physical Settlement Mandate)**: SEBI transitioned all stock derivatives from cash settlement to compulsory physical delivery of underlying shares at expiry.
6. **2024-07-08 (UDIFF Harmonization)**: SEBI phased out exchange-proprietary bhavcopy formats in favor of the Unified Data Interchange Format.

---

## Architecture & Endpoints

### 1. NSE Modern UDIFF F&O (`2024-07-08` to Present)

* **Download URL**:
  ```http
  GET https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip
  ```
* **Payload Structure**: Compressed ZIP file containing a single standardized CSV file (~1.1 MB to ~1.4 MB zipped, ~12 MB uncompressed).
* **Daily Volume**: ~34,000 to ~40,000 active contract records per day.
* **Unified Instrument Codes**:
  - `IDF`: Index Futures (corresponds to legacy `FUTIDX`)
  - `STF`: Stock Futures (corresponds to legacy `FUTSTK`)
  - `IDO`: Index Options (corresponds to legacy `OPTIDX`)
  - `STO`: Stock Options (corresponds to legacy `OPTSTK`)
* **Underlying Spot Price**: Unlike legacy bhavcopies, UDIFF natively populates `UndrlygPric` (underlying cash market index or stock reference price at market close).

### 2. NSE Legacy F&O Bhavcopy (`2000-06-12` to `2024-07-05`)

* **Download URL**:
  ```http
  GET https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{YYYY}/{MON}/fo{DD}{MON}{YYYY}bhav.csv.zip
  ```
  *(Where `{MON}` is the uppercase 3-letter month abbreviation, e.g., `JUN`, `DEC`).*
* **Payload Structure**: Compressed ZIP containing a single CSV (`fo{DD}{MON}{YYYY}bhav.csv`).
* **Sunset Verification**:
  - On **2024-07-05**, the endpoint successfully returned a valid ZIP payload with 33,930 contracts.
  - On **2024-07-08**, requesting this URL returned `HTTP 404 Not Found`, confirming complete retirement by NSE.

### 3. BSE Modern UDIFF F&O (`2024-07-08` to Present)

* **Download URL**:
  ```http
  GET https://www.bseindia.com/download/BhavCopy/Derivative/BhavCopy_BSE_FO_0_0_0_{YYYYMMDD}_F_0000.CSV
  ```
* **Payload Structure**: Raw uncompressed `.CSV` file (~68 KB to ~125 KB).
* **Key Contracts**: Captures weekly and monthly contracts for `SENSEX` (Index Options `IDO`, Index Futures `IDF`), `BANKEX`, and selected stock derivatives.
* **Daily Volume**: ~350 to ~650 active traded contracts per session.

---

## Technical Quirks & Normalization Pipeline

### 1. Mid-File Repeated Headers
In legacy NSE F&O zip archives, occasional transmission glitches or archive mergers produce duplicate header rows embedded mid-way through the file. If parsed directly by `pd.read_csv`, these trigger parsing exceptions or cast errors.
* **Mitigation**:
  ```python
  lines = [
      ln for i, ln in enumerate(raw_csv.splitlines())
      if i == 0 or not ln.strip().upper().startswith("INSTRUMENT,")
  ]
  df = pd.read_csv(io.StringIO("\n".join(lines)), on_bad_lines="skip")
  ```

### 2. Column Header Inconsistencies Across Decades
* In earlier legacy files (e.g., 2005), the option type column was labeled `OPTIONTYPE`, whereas later files use `OPTION_TYP`.
* Trailing commas frequently create a phantom column `Unnamed: 15`.
* **Mitigation**: The parser dynamically resolves `opt_col = "OPTION_TYP" if "OPTION_TYP" in df.columns else "OPTIONTYPE"` and trims all column names.

### 3. Illiquid Strike Price Repair
In deep out-of-the-money (OTM) options or far-month contracts, no trades may execute during a session, leaving `Open`, `High`, and `Low` reported as `0.0` or `NaN`, while the exchange publishes an official marked-to-market closing price (`Close` / `SettlePrice`).
* **Mitigation**:
  ```python
  for col in ("Open", "High", "Low"):
      zero_mask = (out[col].isna()) | (out[col] <= 0)
      out.loc[zero_mask, col] = out.loc[zero_mask, "Close"]
  ```

### 4. Zero-Activity Pruning
A complete option chain across all strikes and expiries contains tens of thousands of dormant theoretical strikes. To keep storage efficient and focus on tradeable liquidity, the datalake filters out ghost contracts that have neither traded nor maintain open positions:
```python
out = out[(out["Contracts"] > 0) | (out["OpenInterest"] > 0)]
```

---

## Data Schema & Field Mappings

Both legacy and modern UDIFF files from NSE and BSE are normalized into the unified F&O schema:

| Unified Column | Type | NSE UDIFF Source | NSE Legacy Source | BSE UDIFF Source | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`Date`** | Timestamp | `TradDt` | `TIMESTAMP` / URL | `TradDt` | Session trade date (midnight IST) |
| **`Exchange`** | String | `"NSE"` | `"NSE"` | `"BSE"` | Derivatives exchange (`NSE` or `BSE`) |
| **`InstrumentType`**| String | `FinInstrmTp` | `INSTRUMENT` | `FinInstrmTp` | Contract type (`FUTIDX`, `FUTSTK`, `OPTIDX`, `OPTSTK`) |
| **`Symbol`** | String | `TckrSymb` | `SYMBOL` | `TckrSymb` | Underlying trading symbol (`NIFTY`, `RELIANCE`, `SENSEX`) |
| **`ExpiryDate`** | Timestamp | `XpryDt` | `EXPIRY_DT` | `XpryDt` | Contract expiration date |
| **`StrikePrice`** | Float | `StrkPric` | `STRIKE_PR` | `StrkPric` | Strike price in INR (`0.0` for futures) |
| **`OptionType`** | String | `OptnTp` | `OPTION_TYP` | `OptnTp` | Option call/put (`CE`, `PE`, or `XX` for futures) |
| **`Open`** | Float | `OpnPric` | `OPEN` | `OpnPric` | Opening traded price in INR |
| **`High`** | Float | `HghPric` | `HIGH` | `HghPric` | Highest traded price in INR |
| **`Low`** | Float | `LwPric` | `LOW` | `LwPric` | Lowest traded price in INR |
| **`Close`** | Float | `ClsPric` | `CLOSE` | `ClsPric` | Last closing traded price in INR |
| **`SettlePrice`** | Float | `SttlmPric` | `SETTLE_PR` | `SttlmPric` | Official exchange daily settlement price |
| **`UnderlyingPrice`**| Float | `UndrlygPric`| `pd.NA` | `UndrlygPric`| Underlying cash spot closing price |
| **`Contracts`** | Float | `TtlTradgVol` | `CONTRACTS` | `TtlTradgVol` | Number of contracts traded during session |
| **`TurnoverValue`**| Float | `TtlTrfVal` | `VAL_INLAKH`*1e5 | `TtlTrfVal` | Total contract turnover value in INR |
| **`OpenInterest`** | Float | `OpnIntrst` | `OPEN_INT` | `OpnIntrst` | Total outstanding open interest in shares/units |
| **`ChangeInOI`** | Float | `ChngInOpnIntrst`| `CHG_IN_OI` | `ChngInOpnIntrst`| Daily net addition or unwinding of OI |

---

## Live Probe Verification Evidence

The table below catalogs real live probe outputs across 26 years of Indian derivatives trading:

```text
========================================================================================================================
SESSION DATE   FEED TYPE         HTTP STATUS   PAYLOAD SIZE   RECORDS    VERIFIED LIVE EVIDENCE & SAMPLE CONTRACTS
========================================================================================================================
2024-10-25     NSE FO UDIFF      200 OK        1,338,180 b    38,310     STO: 31,380 | IDO: 6,380 | STF: 535 | IDF: 15
                                                                         Full option chain with Strike, Expiry, OI, ChgInOI
------------------------------------------------------------------------------------------------------------------------
2024-07-08     NSE FO UDIFF      200 OK        1,147,960 b    33,930     SEBI UDIFF Mandate Day. Modern pipeline active.
2024-07-08     NSE FO LEGACY     404 Not Found 3,815 b        0          PROVED DISCONTINUED after 2024-07-05
------------------------------------------------------------------------------------------------------------------------
2024-07-05     NSE FO LEGACY     200 OK        539,677 b      33,930     Final day of legacy format.
                                                                         OPTSTK: 26,065 | OPTIDX: 7,308 | FUTSTK: 542 | FUTIDX: 15
                                                                         Sample NIFTY FUTIDX (25-Jul-2024):
                                                                         Open: 24,306.90 | Close: 24,379.40 | OI: 14,171,325
------------------------------------------------------------------------------------------------------------------------
2020-01-02     NSE FO LEGACY     200 OK        366,460 b      31,835     OPTSTK: 26,258 | OPTIDX: 5,144 | FUTSTK: 424 | FUTIDX: 9
                                                                         Sample NIFTY FUTIDX (30-Jan-2020):
                                                                         Open: 12,252.10 | Close: 12,339.30 | OI: 12,446,775
------------------------------------------------------------------------------------------------------------------------
2016-01-04     NSE FO LEGACY     200 OK        333,711 b      31,259     OPTSTK: 26,722 | OPTIDX: 4,000 | FUTSTK: 498 | FUTIDX: 36
                                                                         Sample NIFTY FUTIDX (28-Jan-2016):
                                                                         Open: 7,934.90  | Close: 7,809.80  | OI: 20,242,200
------------------------------------------------------------------------------------------------------------------------
2005-01-03     NSE FO LEGACY     200 OK        49,082 b       4,361      OPTSTK: 3,868 | OPTIDX: 310 | FUTSTK: 156 | FUTIDX: 6
                                                                         Early options era. Uses header 'OPTIONTYPE'.
                                                                         Sample NIFTY FUTIDX (27-Jan-2005):
                                                                         Open: 2,092.00  | Close: 2,118.70  | OI: 11,977,400
------------------------------------------------------------------------------------------------------------------------
2000-06-12     NSE FO LEGACY     200 OK        386 b          3          HISTORIC INAUGURAL DAY OF DERIVATIVES IN INDIA.
                                                                         Contains exactly 3 near-month Nifty Futures:
                                                                         NIFTY Jun-2000: Open=1461.0, Settle=1451.15, OI=4,000
                                                                         NIFTY Jul-2000: Open=1460.0, Settle=1455.00, OI=0
                                                                         NIFTY Aug-2000: Open=1460.0, Settle=1456.00, OI=0
------------------------------------------------------------------------------------------------------------------------
2000-01-03     NSE FO LEGACY     404 Not Found 3,815 b        0          PRE-DERIVATIVES ERA. Confirmed non-existent.
------------------------------------------------------------------------------------------------------------------------
2024-10-25     BSE FO UDIFF      200 OK        121,059 b      609        IDO: 482 | STO: 106 | STF: 14 | IDF: 7
                                                                         SENSEX active options & futures captured.
2024-07-08     BSE FO UDIFF      200 OK        68,409 b       341        IDO: 294 | STF: 21 | STO: 16 | IDF: 10
                                                                         Sample SENSEX 80200 PE: Strk=80200, OptnTp=PE, OI=2170
========================================================================================================================
```

---

## Storage & Partitioning Architecture

Because active options chains generate ~35,000 records per daily session (~750,000 to ~850,000 records per month), storing derivatives in single annual files would result in multi-gigabyte partitions that impede query performance.

1. **Partition Path**:
   - `derivatives/nse_fo_{year}_{month:02d}.parquet`
   - `derivatives/bse_fo_{year}_{month:02d}.parquet`
   *(e.g., `derivatives/nse_fo_2024_07.parquet`).*
2. **Deduplication Key**:
   `["Date", "Exchange", "InstrumentType", "Symbol", "ExpiryDate", "StrikePrice", "OptionType"]`
3. **Sort Order**:
   `["Date", "Symbol", "ExpiryDate", "StrikePrice", "OptionType"]`
4. **Compression**: Snappy compression via Apache Arrow.
5. **State Tracking**: Completed trading sessions are watermarked in SQLite (`state.db`) under dataset `fo_eod_nse` and `fo_eod_bse` only after Parquet files are safely synced to disk.

---

## CLI & Pipeline Invocation

### Backfill Historical Derivatives
```bash
# Ingest historical F&O data across NSE and BSE from 2020 through present
uv run datalake backfill --start 2020-01-01 --end 2026-10-31 --include-fo

# Backfill the historic inception era (2000–2005)
uv run datalake backfill --start 2000-06-01 --end 2005-12-31 --include-fo
```

### Daily Derivatives Update
```bash
# Ingest latest daily F&O settlement files along with cash equities and indices
uv run datalake update --lookback-days 7 --include-fo
```
