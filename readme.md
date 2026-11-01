# bfinance-datalake

Automated historical backfill & daily EOD scraper for Indian Equities, F&O, Indices, XBRL Fundamentals, Shareholding, Deals, and Mutual Funds.

## Documentation
- [Upstox Historical Candle CDN Integration](docs/upstox_data_integration.md) - Details on public CDN endpoints, schemas, corporate action adjustments, and multi-decade coverage boundaries.

## Command Reference

```bash
# 1. Install dependencies
uv sync

# 2. Full Historical Backfill (2016–Present via static archives + 1995–Present Indices & Actions)
uv run datalake backfill --start 2016-01-01 --include-fo --amfi-history

# 3. Pre-2016 NSE Equity + Delivery Backfill (1995–2015 via /api/historicalOR)
uv run datalake pre2016-nse --symbols RELIANCE,TCS,INFY,HDFCBANK,SBIN --start-year 1995 --end-year 2015

# 4. Sync Ind-AS XBRL Financials (Industrial + Banking), Shareholding, Pledge, >1% Holders & Annual Reports
uv run datalake xbrl --all-market

# 5. Daily Incremental EOD Sync (runs in ~20s every evening)
uv run datalake daily

# 6. View Data Lake Watermarks & Row Counts
uv run datalake status
```

# 1. Run the instant offline unit & self-healing test suite (< 2 seconds):
uv run python tests/test_datalake_offline.py

# 2. Run the live 15-source mini-slice smoke test (~30 seconds, writes to ./smoke_data/):
uv run python tests/smoke_test_live.py

# 3. Inspect the smoke data status or run doctor on it:
uv run datalake --data-dir ./smoke_data status
uv run datalake --data-dir ./smoke_data doctor
