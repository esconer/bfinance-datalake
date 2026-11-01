# bfinance-datalake

Automated point-in-time (PIT) historical backfill & daily EOD data lake for Indian Equities, Derivatives (F&O), Nifty Indices, Ind-AS XBRL Fundamentals, Shareholding Patterns, Corporate Actions, Deals, and Mutual Funds.

---

## Technical Documentation Suite

Comprehensive technical specifications, schema definitions, live empirical probe verifications, and multi-decade temporal boundaries are documented in [`docs/`](docs/README.md):

* [**Documentation Master Index & Historical Timeline**](docs/README.md) - High-level architectural overview, 30-year market evolution timeline, and network resilience.
* [**Upstox Historical Candle CDN Integration**](docs/upstox_data_integration.md) - Public unauthenticated CDN endpoints, OHLCV schemas, split/bonus adjustment proof, and 2000–2015 cloud seeding.
* [**Equity EOD Integration (NSE & BSE Cash Equities)**](docs/equity_eod_data_integration.md) - Modern UDIFF cutover (2024), Full Bhavdata (2020–2024), Legacy Bhavcopies (1995–2024), MTO Gross Delivery (2002–2024), and NSE HistoricalOR fallback.
* [**Derivatives F&O Integration (NSE & BSE)**](docs/derivatives_fo_data_integration.md) - Inception of Index Futures (June 12, 2000), Stock Futures/Options, modern UDIFF format, settlement prices, strike grids, open interest (OI), and monthly Parquet partitioning.
* [**Nifty Indices Historical & Snapshot Data Integration**](docs/indices_data_integration.md) - 30Y continuous OHLC, Total Return Index (TRI), Valuation Ratios (P/E, P/B, Div Yield) via Sitefinity MVC `/BackPage/*`, and market-wide daily snapshots (120+ indices).
* [**Corporate Universe, Security Masters & Actions**](docs/universe_and_actions_data_integration.md) - NSE & BSE security masters, 1,078 symbol rename DAG, Point-in-Time (PIT) Nifty 50 constituents, 30Y regex corporate actions parser, and multiplicative backward adjustment math.
* [**XBRL Fundamentals & Shareholding Patterns**](docs/xbrl_fundamentals_and_shareholding_data_integration.md) - Ind-AS XBRL taxonomies (`in-bse-fin` and `in-bse-bank`), quarterly/annual financial statements, 25-year BSE multi-decade shareholding patterns (107 quarters for Reliance back to 2001), promoter pledge, and >1% institutional holders.
* [**Corporate Annual Reports Catalog**](docs/annual_reports_integration.md) - 17–18 continuous fiscal years of official PDF annual reports via dual NSE `/api/annual-reports` and BSE `/api/AnnualReport/w` APIs (FY 2009–10 to FY 2025–26).
* [**Funds & Institutional Flows Integration**](docs/funds_and_flows_data_integration.md) - AMFI Mutual Fund NAVs across 14,400+ schemes with multi-mirror failover and `mfapi.in` backup, plus NSE Clearing F&O Participant Open Interest (`Client`, `DII`, `FII`, `Pro`) back to 2012.
* [**Deals, Insider Trading & Market Activity**](docs/deals_insider_and_market_activity.md) - NSE & BSE Bulk and Block Deals (2004–Present), Cash Market Provisional FII/DII daily net flows (₹ Crores), and SEBI PIT Insider Trading disclosures (Regulation 7(2)).
* [**Unified Query Engine**](docs/unified_query_engine.md) - `UnifiedLakeEngine` Python API with universal crosswalk resolution, PyArrow row-group pushdown, and DuckDB zero-copy SQL querying.

---

## Unified Query Engine (`UnifiedLakeEngine`)

Query any stock across all 15 datasets simultaneously using its NSE Symbol, BSE Scrip Code, or ISIN:

```python
from datalake import UnifiedLakeEngine

engine = UnifiedLakeEngine("./data")

# 1. Universal Crosswalk Resolution
tcs = engine.ticker("TCS")         # Also resolves by "532540" or "INE467B01029"
print(tcs.identity)
# SecurityIdentity(symbol='TCS', bse_code='532540', isin='INE467B01029', company_name='Tata Consultancy Services Limited', aliases=('TCS',))

# 2. Adjusted Daily OHLCV Price Bars (with delivery volume)
df_daily = tcs.daily(start="2015-01-01", end="2024-12-31", adjusted=True)

# 3. Quarterly Fundamentals & Financial Statements (Ind-AS XBRL)
fin_df = tcs.financials(period="Quarterly")

# 4. Multi-Decade Shareholding Pattern & Institutional Ownership
shp_df = tcs.shareholding()

# 5. Direct PDF Links to 15+ Years of Annual Reports
reports = tcs.annual_reports()

# 6. Zero-Copy DuckDB SQL Across All Parquet Files
res = engine.sql("""
    SELECT Date, Symbol, Close, Volume, DeliverableQty, DeliveryPct
    FROM 'data/equities/nse_cm_*.parquet'
    WHERE Symbol = 'RELIANCE' AND Date >= '2024-01-01'
    ORDER BY Date DESC
    LIMIT 10
""").df()
```

---

## Command Reference

```bash
# 1. Install dependencies
uv sync

# 2. Full Historical Backfill (2016–Present via static archives + 1995–Present Indices & Actions)
uv run datalake backfill --start 2016-01-01 --include-fo --amfi-history

# 3. Pre-2016 NSE Equity + Delivery Backfill (1995–2015 via /api/historicalOR)
uv run datalake pre2016-nse --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN --start-year 1995 --end-year 2015

# 4. Seed Pre-2016 Equities Ultra-Fast via Public CDN (Cloud & GitHub Actions Compatible)
uv run datalake seed-cloud --start-year 2000 --end-year 2015

# 5. Sync Ind-AS XBRL Financials (Industrial + Banking), Shareholding, Pledge, >1% Holders & Annual Reports
uv run datalake xbrl --all-market

# 6. Daily Incremental EOD Sync (runs in ~20s every evening)
uv run datalake daily

# 7. View Data Lake Watermarks & Row Counts
uv run datalake status

# 8. Run Self-Healing Integrity Doctor & Watermark Verification
uv run datalake doctor
```

---

## Verification & Testing

```bash
# 1. Run instant offline unit & self-healing test suite (< 3 seconds):
uv run python -m unittest discover -s tests -p "test_*.py"

# 2. Run the live 15-source mini-slice smoke test (~30 seconds, writes to ./smoke_data/):
uv run python tests/smoke_test_live.py

# 3. Inspect smoke data status or run doctor on it:
uv run datalake --data-dir ./smoke_data status
uv run datalake --data-dir ./smoke_data doctor
```
