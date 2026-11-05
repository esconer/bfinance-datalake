# bfinance-datalake Documentation Master Index

Welcome to the comprehensive technical documentation for **`bfinance-datalake`** — an open-source, point-in-time (PIT) historical and daily data lake engineered for Indian capital markets across equities, derivatives, indices, fundamentals, corporate actions, mutual funds, and institutional flows.

Every feed in this repository has been rigorously verified via empirical live network probes against official exchange endpoints, public CDNs, and statutory regulators.

---

## 1. Documentation Catalog

### A. Core Architecture & Engineering Specifications
| Document | Focus Area | Contents & Purpose |
| :--- | :--- | :--- |
| [**Architecture & System Design**](core/architecture_and_design.md) | System Architecture & Storage | 3-tier batch architecture, storage invariants, atomic replacement loops, network resilience, self-healing doctor engine, and two-tier packfile remote query protocol. |
| [**Core Codebase Reference**](core/codebase_reference.md) | Core Python Implementation | Line-by-line, module-by-module reference for all modules in `src/datalake/` (`config`, `http_client`, `storage`, `state`, `symbols`, `adjuster`, `doctor`, `sync`, `remote`, `engine`, `cli`). |
| [**Scrapers Implementation Reference**](core/scrapers_reference.md) | Ingestion & Parser Pipelines | Complete technical guide to all 7 scraper modules in `src/datalake/scrapers/` (`equity_eod`, `fo_eod`, `indices`, `universe_actions`, `deals_docs`, `funds_flows`, `xbrl`). |
| [**Unified Query Engine**](core/unified_query_engine.md) | Query Engine | `UnifiedLakeEngine`: universal crosswalk resolution, metadata attributes, vectorized `download()`, `concalls()`, `insider_trades()`, PyArrow row-group pushdown, DuckDB zero-copy SQL. |
| [**Zero-Disk Remote Querying**](core/zero_disk_remote_querying.md) | Remote Querying | `RemoteLakeClient`: direct HTTPS queries over GitHub Releases via DuckDB `httpfs` byte-range requests with 0 MB downloaded to disk. |

### B. Operations, Cloud & Testing
| Document | Focus Area | Contents & Purpose |
| :--- | :--- | :--- |
| [**CLI & Operational Runbook**](operations/cli_and_operations.md) | Operations & CI/CD | Comprehensive CLI guide across all 10 subcommands, GitHub Actions automated cloud sync, Cloudflare WARP bypass, and disaster recovery runbook. |
| [**Smart Cloud Delta Sync & Releases**](operations/cloud_sync_and_releases.md) | Cloud Sync | 3-stage architecture: GitHub Actions daily delta builds, SHA-256 `manifest.json` indexing, and ~1.5s local incremental pull via `datalake pull`. |
| [**Developer & Testing Guide**](testing/developer_and_testing_guide.md) | Testing & QA | Testing framework, offline unit suites (`app.py`), invariant assertions, live smoke test harness, latency benchmarks, and contributing standards. |
| [**Empirical Data Testing & Verification Audit**](testing/data_testing.md) | Data Quality Audit | Quality audits, invariant checks, SHA-256 manifest validation, crosswalk verification & benchmarks over release files. |
| [**NSE Report Portals Architecture & Source Investigation Audit**](testing/nse_reports_architecture.md) | Source Auditing | Reverse-engineering and architectural audit of NSE report directories, manifest APIs, and security-wise historical archives. |

### C. Upstream Feed & Domain Integration Specifications
| Document | Primary Datasets Covered | Source Authorities | Protocol / Format | Temporal Boundary | Update Frequency |
| :--- | :--- | :--- | :--- | :--- | :---: |
| [**Upstox V2 Historical Candle CDN**](integrations/upstox_data_integration.md) | NSE & BSE Cash Equities (Pre-2016 Cloud Seeding), Sectoral & Major Indices | Upstox Public CDN | Unauthenticated HTTP JSON | **`2000-01-03` to Present** | On-demand Historical Seeding |
| [**Equity EOD Integration**](integrations/equity_eod_data_integration.md) | NSE & BSE Cash Equities: Modern UDIFF, Full Bhavdata, Legacy Bhavcopies, MTO Gross Delivery, Pre-2016 Symbol Fallback | NSE Archives, BSE Downloads, NSE HistoricalOR | HTTPS Zip/CSV, Fixed Width DAT | **`1995-01-03` to Present** | Daily EOD (~18:30 IST) |
| [**Derivatives F&O Integration**](integrations/derivatives_fo_data_integration.md) | NSE & BSE Equity Derivatives: Index/Stock Futures (`FUTIDX`, `FUTSTK`), Index/Stock Options Chains (`OPTIDX`, `OPTSTK`), Strikes, Expiries, Settle Price, OI, Change in OI (Yearly Parquet `nse_fo_YYYY.parquet`) | NSE Archives, BSE Downloads | HTTPS Zip/CSV (UDIFF + Legacy) | **`2000-06-12` to Present** | Daily EOD (~19:00 IST) |
| [**Nifty Indices Integration**](integrations/indices_data_integration.md) | 30Y Continuous OHLC, Total Return Index (TRI), Valuation Ratios (P/E, P/B, Div Yield), Daily All-Indices Snapshot (120+ Indices) | NiftyIndices (Sitefinity MVC API), NSE Archives, NSE HistoricalOR | HTTPS POST JSON, CSV | **`1995-11-03` to Present** | Daily EOD (~19:30 IST) |
| [**Universe & Corporate Actions**](integrations/universe_and_actions_data_integration.md) | NSE & BSE Security Masters, 1,078 Symbol Rename DAG, PIT Nifty 50 Constituents, 30Y Regex Action Parser, Multiplicative Backward Adjustment | NSE Archives, BSE India, Upstox | Parquet, CSV, Regex Engine | **`1995-01-01` to Present** | Weekly / Daily EOD |
| [**XBRL Fundamentals & Shareholding**](integrations/xbrl_fundamentals_and_shareholding_data_integration.md) | Ind-AS XBRL Balance Sheet, P&L, Cash Flow, Banking Metrics, 25-Year BSE Multi-Decade Shareholding, Promoter Pledge, >1% Institutional Holders | NSE Corporates API, BSE Corp Shareholding NG | XML (in-bse-fin, in-bse-bank, in-bse-shp), JSON fallback | **`2001-06-30` to Present** | Quarterly & Annual Disclosures |
| [**Corporate Annual Reports**](integrations/annual_reports_integration.md) | 17–18 Continuous Fiscal Years of Official Annual Reports (PDF / ZIP Direct Catalog) | NSE Annual Reports API, BSE AnnualReport/w | HTTPS JSON to Raw PDF Links | **`FY 2009–10` to Present** | Annual / Real-time Filings |
| [**Funds & Institutional Flows**](integrations/funds_and_flows_data_integration.md) | AMFI Mutual Fund NAVs (14,400+ Active Schemes, Multi-Mirror Failover, mfapi.in Fallback), NSE F&O Participant Open Interest (`Client`, `DII`, `FII`, `Pro`) | AMFI Portal, MFapi.in, NSE Clearing (NSCCL) | Semicolon Flat Text (Windows-1252), CSV | **`2006-04-01` (AMFI) / `2012-09-03` (Participant OI)** | Daily NAVs (~21:30 IST) / Daily EOD (~20:00 IST) |
| [**Deals, Insider Trading & Market Activity**](integrations/deals_insider_and_market_activity.md) | NSE & BSE Bulk & Block Deals, Cash Market Provisional FII / DII Daily Flows (₹ Crores), SEBI PIT Insider Trading Disclosures (Reg 7(2)) | NSE Archives, NSE Corporates API | HTTPS CSV (UTF-8 BOM), JSON | **`2004-01-01` (Deals) / `2016-01-01` (PIT)** | Daily EOD (~18:00 IST) / Real-time Disclosures |

---

## 2. Multi-Decade Historical Boundaries Summary

Indian financial markets have transitioned across multiple technological and regulatory eras over the last 30 years. The table below outlines the exact temporal boundaries and regulatory cutovers verified by our empirical probes:

```mermaid
timeline
    title 30-Year Evolution of Indian Capital Market Data
    1995 : NSE Electronic Screen Trading Launch (Jan 1995) : NIFTY 50 Base Date (Nov 3, 1995) : Legacy Bhavcopies Begin
    2000 : Nifty 50 Index Futures Inception (Jun 12, 2000) : Upstox Historical CDN Coverage Begins (Jan 3, 2000)
    2001 : Stock Options (Jul 2001) & Stock Futures (Nov 2001) : BSE Multi-Decade Shareholding Patterns Begin
    2002 : Compulsory Rolling Settlement T+3 : NSE MTO Gross Delivery Files Begin (Jan 2002)
    2004 : SEBI Electronic Bulk & Block Deal Disclosures Inception (Mid-2004)
    2006 : AMFI Historical NAV Report Inception (Apr 1, 2006)
    2010 : NSE & BSE Electronic Corporate Annual Reports Catalog Inception (FY 2009–10)
    2012 : NSE Clearing Participant-Wise F&O Open Interest Archives Begin (Sep 2012)
    2016 : SEBI Ind-AS XBRL Financial Taxonomy Mandate : SEBI PIT Insider Trading Electronic Disclosures
    2020 : NSE Full Security Bhavdata (sec_bhavdata_full) Native Delivery Inception (Jan 2020)
    2024 : SEBI Unified Data Interchange Format (UDIFF) Cutover (Jul 8, 2024) : Exchange Legacy Formats Permanently Sunsat
```

---

## 3. Resilience, Network & Anti-Bot Infrastructure

To download multi-decade historical archives and real-time feeds reliably without being blocked by exchange firewalls or hitting OS-level socket limitations, `bfinance-datalake` implements:

1. **Universal TLS Fingerprint Impersonation**:
   - Powered by `curl_cffi` impersonating **Chrome 124**.
   - Bypasses Akamai and Cloudflare JA3/JA4 fingerprinting rules on `www.nseindia.com`, `bseindia.com`, `niftyindices.com`, and `amfiindia.com`.
2. **Automated Host Priming**:
   - Automatically visits exchange homepages (`_prime_host`) to acquire required session cookies (`_abck`, `ak_bmsc`, `AKA_A2`, `bm_sz`) before dispatching API calls.
3. **DNS-over-HTTPS (DoH)**:
   - Uses `doh_url="https://dns.google/dns-query"` to circumvent Windows `c-ares` UDP socket timeouts and IPv6 DNS latency.
4. **Encoding Fallbacks**:
   - Dual UTF-8 and Windows-1252 (`cp1252`) decoding to handle typographical symbols and accents in AMFI flat files and legacy BSE records.
5. **Atomic Storage & File Lock Protection**:
   - Unique temporary file generation (`.{uuid}.tmp`) with exponential retry loops and explicit garbage collection (`gc.collect()`) to prevent Windows OS `PermissionError` file-locking deadlocks during Parquet partition upserts.

---

## 4. License & Statutory Notice

`bfinance-datalake` is licensed under the **GNU Affero General Public License v3.0 or later** (`AGPL-3.0-or-later`).  
See [`LICENSE`](../LICENSE) and [`NOTICE`](../NOTICE) for full terms.

Pursuant to Section 7(b) of the GNU AGPLv3, all redistributions must preserve the 4-point **Legal Disclaimer, Data Ownership & Provenance, and SEBI Compliance Notice** intact.
