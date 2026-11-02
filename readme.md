# bfinance-datalake

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

Automated point-in-time (PIT) historical backfill & daily EOD data lake for Indian Equities, Derivatives (F&O), Nifty Indices, Ind-AS XBRL Fundamentals, Shareholding Patterns, Corporate Actions, Deals, and Mutual Funds.

---

## Technical Documentation Suite

Comprehensive technical specifications, schema definitions, live empirical probe verifications, and multi-decade temporal boundaries are documented in [`docs/`](docs/README.md):

* [**Documentation Master Index & Historical Timeline**](docs/README.md) - High-level architectural overview, 30-year market evolution timeline, and network resilience.
* [**Upstox Historical Candle CDN Integration**](docs/upstox_data_integration.md) - Public unauthenticated CDN endpoints, OHLCV schemas, split/bonus adjustment proof, and 2000–2015 cloud seeding.
* [**Equity EOD Integration (NSE & BSE Cash Equities)**](docs/equity_eod_data_integration.md) - Modern UDIFF cutover (2024), Full Bhavdata (2020–2024), Legacy Bhavcopies (1995–2024), MTO Gross Delivery (2002–2024), and NSE HistoricalOR fallback.
* [**Derivatives F&O Integration (NSE & BSE)**](docs/derivatives_fo_data_integration.md) - Inception of Index Futures (June 12, 2000), Stock Futures/Options, modern UDIFF format, settlement prices, strike grids, open interest (OI), and yearly Parquet partitioning (`nse_fo_YYYY.parquet`).
* [**Nifty Indices Historical & Snapshot Data Integration**](docs/indices_data_integration.md) - 30Y continuous OHLC, Total Return Index (TRI), Valuation Ratios (P/E, P/B, Div Yield) via Sitefinity MVC `/BackPage/*`, and market-wide daily snapshots (120+ indices).
* [**Corporate Universe, Security Masters & Actions**](docs/universe_and_actions_data_integration.md) - NSE & BSE security masters, 1,078 symbol rename DAG, Point-in-Time (PIT) Nifty 50 constituents, 30Y regex corporate actions parser, and multiplicative backward adjustment math.
* [**XBRL Fundamentals & Shareholding Patterns**](docs/xbrl_fundamentals_and_shareholding_data_integration.md) - Ind-AS XBRL taxonomies (`in-bse-fin` and `in-bse-bank`), quarterly/annual financial statements, 25-year BSE multi-decade shareholding patterns (107 quarters for Reliance back to 2001), promoter pledge, and >1% institutional holders.
* [**Corporate Annual Reports Catalog**](docs/annual_reports_integration.md) - 17–18 continuous fiscal years of official PDF annual reports via dual NSE `/api/annual-reports` and BSE `/api/AnnualReport/w` APIs (FY 2009–10 to FY 2025–26).
* [**Funds & Institutional Flows Integration**](docs/funds_and_flows_data_integration.md) - AMFI Mutual Fund NAVs across 14,400+ schemes with multi-mirror failover and `mfapi.in` backup, plus NSE Clearing F&O Participant Open Interest (`Client`, `DII`, `FII`, `Pro`) back to 2012.
* [**Deals, Insider Trading & Market Activity**](docs/deals_insider_and_market_activity.md) - NSE & BSE Bulk and Block Deals (2004–Present), Cash Market Provisional FII/DII daily net flows (₹ Crores), and SEBI PIT Insider Trading disclosures (Regulation 7(2)).
* [**Unified Query Engine**](docs/unified_query_engine.md) - `UnifiedLakeEngine` Python API with universal crosswalk resolution (`RELIANCE` == `500325` == `INE002A01018`), PyArrow row-group pushdown, and DuckDB zero-copy SQL querying.
* [**Smart Cloud Delta Sync & Releases**](docs/cloud_sync_and_releases.md) - 3-stage architecture: GitHub Actions daily delta builds, SHA-256 `manifest.json` indexing, and ~1.5s local incremental pull via `datalake pull`.
* [**Zero-Disk Remote Querying**](docs/zero_disk_remote_querying.md) - `RemoteLakeClient` direct HTTPS queries over GitHub Releases via DuckDB `httpfs` byte-range requests with 0 MB downloaded to disk.

---

## Unified Query Engine (`UnifiedLakeEngine`)

Query any stock across all 15 datasets simultaneously using its NSE Symbol, BSE Scrip Code, or ISIN:

```python
from datalake import UnifiedLakeEngine
from datalake.remote import RemoteLakeClient

# Option A: Point to local Parquet data lake (or auto-sync via UnifiedLakeEngine.from_github)
lake = UnifiedLakeEngine("./data")

# 1. Universal Crosswalk Resolution with Metadata
rel = lake.ticker("500325")  # Also resolves by "RELIANCE" or "INE002A01018"
print(rel.identity)
# SecurityIdentity(symbol='RELIANCE', bse_code='500325', isin='INE002A01018',
#                  face_value=10.0, industry='Refineries', listing_date='1995-11-29', ...)

# 2. Daily OHLCV Price Bars with backward split & dividend Adj Close
df_price = rel.history(start="1995-01-01", auto_adjust=True)

# 3. Vectorized Multi-Symbol Download (Single pass over yearly partitions)
stocks = lake.download(["RELIANCE", "TCS", "INFY"], start="2020-01-01", auto_adjust=True)

# 4. Ind-AS XBRL Balance Sheet, P&L & Cash Flow Statements
df_qtr = rel.quarterly_financials
df_ann = rel.financials

# 5. Multi-Decade Shareholding & Promoter Pledge
df_shp = rel.shareholding
df_inst = rel.institutional_holders

# 6. Corporate Actions, Deals, Concalls & Annual Reports
df_act = rel.actions
df_deals = rel.deals
df_insider = rel.insider_trades
df_concalls = rel.concalls
df_ar = rel.annual_reports

# 7. Zero-Copy DuckDB SQL Screeners Across All Parquet Datasets
df_screen = lake.sql("""
    SELECT e.Symbol, e.Date, e.Close, e.DeliveryPct, f.Sales_Cr, f.NetProfit_Cr
    FROM equities_nse e
    LEFT JOIN financials f ON e.Symbol = f.Symbol AND f.PeriodType = 'Quarterly'
    WHERE e.Date = (SELECT MAX(Date) FROM equities_nse)
      AND e.DeliveryPct > 55.0
    LIMIT 25;
""")

# Option B: Zero-Disk Remote Querying (Streams byte-ranges via DuckDB httpfs, 0 MB disk)
remote = RemoteLakeClient(repo="esconer/bfinance-datalake")
df_rem = remote.history("RELIANCE", start_year=2024, end_year=2026)
df_opt = remote.option_chain("NIFTY", year=2026)
```

---

## Command Reference

```bash
# 1. Install dependencies
uv sync

# 2. Fast SHA-256 Delta Sync from GitHub Release to local machine (~1.5s)
uv run datalake pull --repo esconer/bfinance-datalake

# 3. Full Historical Backfill (2016–Present via static archives + 1995–Present Indices & Actions)
uv run datalake backfill --start 2016-01-01 --include-fo --amfi-history

# 4. Pre-2016 NSE Equity + Delivery Backfill (1995–2015 via /api/historicalOR)
uv run datalake pre2016-nse --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN --start-year 1995 --end-year 2015

# 5. Seed Pre-2016 Equities Ultra-Fast via Public CDN (Cloud & GitHub Actions Compatible)
uv run datalake seed-cloud --start-year 2000 --end-year 2015

# 6. Sync Ind-AS XBRL Financials, Shareholding, Pledge, >1% Holders & Annual Reports
uv run datalake xbrl --all-market

# 7. Daily Incremental EOD Sync (runs in ~20s every evening)
uv run datalake daily

# 8. View Data Lake Watermarks & Row Counts
uv run datalake status

# 9. Run Self-Healing Integrity Doctor & Watermark Verification
uv run datalake doctor

# 10. Stage Release Assets & Generate manifest.json (Used by GitHub Actions)
uv run datalake stage-release --staging-dir /tmp/release_assets --prev-manifest /tmp/prev_manifest.json
```

---

## Architecture with `bfinance` (Batch + Speed Layers)

`bfinance-datalake` is engineered to act as the foundational **Historical Batch Layer** for [`esconer/bfinance`](https://github.com/esconer/bfinance):

* **Batch Layer (`bfinance-datalake`)**: Houses 1995 $\to$ Yesterday's market close across all 15 datasets in Zstandard Parquet. Offers 1–5 ms local NVMe querying or ~350 ms remote zero-disk streaming via DuckDB `httpfs`.
* **Speed Layer (`bfinance`)**: Captures live market hours (9:15 AM $\to$ 3:30 PM IST), intraday candles (1m, 5m, 15m, 1h), and live CMP.
* **Live Tail Stitcher**: Transparently appends today's unclosed EOD quote to 30 years of adjusted Parquet bars, eliminating 95% of upstream exchange rate limits while providing up-to-the-second data.

---

## Verification & Testing

```bash
# 1. Run instant offline unit & self-healing test suite:
uv run python -m unittest discover -s tests -p "test_*.py"

# 2. Run the live 15-source mini-slice smoke test (~30 seconds, writes to ./smoke_data/):
uv run python tests/smoke_test_live.py

# 3. Inspect smoke data status or run doctor on it:
uv run datalake --data-dir ./smoke_data status
uv run datalake --data-dir ./smoke_data doctor
```

---

## License & Legal Notices

This project is licensed under the **GNU Affero General Public License v3.0 or later** (`AGPL-3.0-or-later`).  
See the [`LICENSE`](LICENSE) file for the full license terms.

Pursuant to **Section 7(b)** of the GNU Affero General Public License v3.0, all copies, forks, modifications, or redistributions of this software must retain the following **Legal Disclaimer, Data Provenance, and SEBI Compliance Notices** intact:

```
========================================================================
LEGAL DISCLAIMER, DATA PROVENANCE & SEBI NOTICE
========================================================================

1. NO AFFILIATION WITH EXCHANGES OR REGULATORS
`bfinance` and `bfinance-datalake` are independent, open-source software
tools designed for educational, academic, and quantitative research
purposes. This project is NOT affiliated with, endorsed by, sponsored by,
or vetted by the National Stock Exchange of India Ltd. (NSE), BSE Ltd.
(formerly Bombay Stock Exchange), NSE Indices Ltd., the Association of
Mutual Funds in India (AMFI), or the Securities and Exchange Board of
India (SEBI).

2. DATA OWNERSHIP & PERMITTED USE
All financial data, end-of-day (EOD) bhavcopies, XBRL filings, indices,
corporate actions, and mutual fund NAVs retrieved or cached by this
software are sourced from publicly accessible statutory filings and
archives and remain the intellectual property of their respective
exchanges, issuers, and regulatory bodies.
- Any pre-built Parquet caches or release artifacts in this repository
  are provided strictly as a technical mirror for personal, educational,
  and non-commercial research use.
- Users are solely responsible for complying with the Terms of Use of
  upstream data providers (NSE, BSE, AMFI) and must not redistribute
  raw exchange data commercially without obtaining appropriate licenses
  from the respective exchanges.

3. NOT INVESTMENT ADVICE (SEBI COMPLIANCE)
Nothing contained in this software, its outputs, screeners, ratios, or
documentation constitutes financial, investment, legal, or tax advice,
nor does it constitute a recommendation under the SEBI (Research
Analysts) Regulations, 2014 or SEBI (Investment Advisers) Regulations,
2013.

4. NO WARRANTY & LIMITATION OF LIABILITY
Market data may be delayed, incomplete, unadjusted, or subject to
upstream formatting changes or errors. In no event shall the authors,
maintainers, or contributors be liable for any financial losses, trading
losses, damages, or claims arising from the use of this software or the
data retrieved by it. Always verify critical figures against official
exchange filings before making any financial decisions.
```
