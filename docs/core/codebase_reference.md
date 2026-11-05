# bfinance-datalake: Core Codebase Reference

This document provides a line-by-line, module-by-module technical breakdown of all core modules in `src/datalake/`.

---

## 1. Module Index

| Module | Primary Responsibilities | Core Classes & Functions |
| :--- | :--- | :--- |
| [`datalake.__init__`](#2-module-datalake__init__) | Top-level exports and version definition. | `UnifiedLakeEngine`, `LakeTicker`, `SecurityIdentity`, `RemoteLakeClient`, `RemoteLakeEngine`, `__version__` |
| [`datalake.config`](#3-module-datalakeconfig) | Timezone helpers and path/concurrency configuration. | `IST`, `today_ist()`, `now_ist()`, `LakeConfig` |
| [`datalake.http_client`](#4-module-datalakehttp_client) | Anti-bot bypass, TLS fingerprint impersonation, host priming, session management. | `SimpleHTTPResponse`, `ResilientHTTPClient`, headers, profiles |
| [`datalake.storage`](#5-module-datalakestorage) | Parquet serialization, numeric coercion, atomic file replacement, in-memory safe reading. | `NUMERIC_FLOAT_COLS`, `sanitize_for_parquet()`, `read_table_safe()`, `write_parquet_atomic()`, `upsert_parquet_partition()` |
| [`datalake.state`](#6-module-datalakestate) | SQLite WAL watermark store, provenance, idempotency tracking. | `StateStore` (`is_completed`, `completed_keys`, `max_ok_date`, `record`, `summary`) |
| [`datalake.symbols`](#7-module-datalakesymbols) | Symbol rename DAG resolution across historical mergers and renames. | `SymbolRenameResolver` (`traded_as_on`, `all_aliases`, `from_lake`) |
| [`datalake.adjuster`](#8-module-datalakeadjuster) | Multiplicative backward price adjustment, split factor accumulation, dividend step-downs, demerger discovery. | `OHLCVAdjuster` (`get_symbol_actions`, `apply_adjustments`) |
| [`datalake.doctor`](#9-module-datalakedoctor) | Self-healing auditor, crash recovery, watermark reconciliation, domain invariant repair. | `LakeDoctor` (`run_full_check_and_repair`) |
| [`datalake.sync`](#10-module-datalakesync) | Release manifest generation, compaction bundles, two-tier packfiles, smart delta sync. | `build_manifest()`, `build_compacted_equity_bundles()`, `build_by_ticker_packs()`, `build_company_dossier_pack()`, `build_market_screener_snapshot()`, `pull_from_github_release()` |
| [`datalake.remote`](#11-module-datalakeremote) | Zero-disk HTTPS streaming client over GitHub Releases via DuckDB `httpfs` and pack slices. | `RemoteLakeClient`, `RemoteLakeEngine` (`history`, `download`, `dossier`, `financials`, `shareholding`, `option_chain`, `screener`, `sql`) |
| [`datalake.engine`](#12-module-datalakeengine) | Local in-memory unified query engine, crosswalk resolution, PyArrow predicate pushdown, DuckDB views. | `SecurityIdentity`, `UnifiedLakeEngine`, `LakeTicker` (`history`, `download`, `fundamentals`, `shareholding`, `option_chain`, `sql`, `screener`, `panel`) |
| [`datalake.cli`](#13-module-datalakecli) | CLI argument parsing, subcommands execution, workflow lifecycle management. | `main()`, `run_backfill()`, `run_daily_update()`, `run_xbrl_sync()`, `run_seed_cloud()` |

---

## 2. Module: `datalake.__init__`

**File:** `src/datalake/__init__.py` (14 lines)

### Purpose
Exposes public API symbols for library consumers and specifies package version metadata.

### Exports
- `UnifiedLakeEngine`: Primary entry point for querying local Parquet partitions.
- `LakeTicker`: Ticker facade wrapping a specific security identity.
- `SecurityIdentity`: Immutable dataclass representing a resolved crosswalk identity.
- `RemoteLakeClient`: Client for zero-disk remote queries over GitHub Releases.
- `RemoteLakeEngine`: Backward-compatible alias for `RemoteLakeClient`.
- `__version__ = "0.2.0"`

---

## 3. Module: `datalake.config`

**File:** `src/datalake/config.py` (51 lines)

### Purpose
Centralizes timezone configurations (`Asia/Kolkata`), directory scaffolding, and concurrency settings.

### Constants & Functions
- **`IST = ZoneInfo("Asia/Kolkata")`**: Authoritative timezone for Indian market trading sessions.
- **`today_ist() -> date`**: Returns current date in IST (`datetime.now(IST).date()`). Avoids UTC date mismatch bugs when running evening scrapers.
- **`now_ist() -> datetime`**: Returns current timezone-aware timestamp in IST.

### Class: `LakeConfig`
Frozen dataclass (`@dataclass(frozen=True)`) containing runtime configuration:

```python
@dataclass(frozen=True)
class LakeConfig:
    root_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("BFINANCE_LAKE_DIR", "./data")).resolve()
    )
    archive_concurrency: int = 4
    api_concurrency: int = 1
    polite_delay_sec: float = 0.25
    holiday_freeze_days: int = 5
```

- **`root_dir`**: Defaults to `./data`, overridden by environment variable `BFINANCE_LAKE_DIR`.
- **`archive_concurrency`**: Number of concurrent workers for static archive downloads (default: `4`).
- **`api_concurrency`**: Number of concurrent workers for dynamic exchange REST APIs (default: `1`).
- **`polite_delay_sec`**: Rate-limit sleep duration between requests (default: `0.25`s).
- **`holiday_freeze_days`**: Lookback threshold beyond which missing days are permanently flagged as `HOLIDAY` in `state.db` (default: `5` days).
- **`state_db_path -> Path`**: Property returning `self.root_dir / "state.db"`.
- **`ensure_dirs() -> None`**: Idempotently creates the 12 functional subdirectories: `universe`, `equities`, `derivatives`, `indices`, `actions`, `fundamentals`, `shareholding`, `mutual_funds`, `flows`, `deals`, `documents`, `snapshots`.

---

## 4. Module: `datalake.http_client`

**File:** `src/datalake/http_client.py` (341 lines)

### Purpose
Implements an asynchronous, resilient HTTP client equipped with browser TLS impersonation (`curl_cffi`), automatic session priming, cookie persistence, and intelligent decoding.

### Key Headers & Profiles
- **`CHROME_UA`**: Modern Chrome User-Agent header string.
- **`ARCHIVE_HEADERS`**: Headers for static CSV/ZIP downloads from `nsearchives.nseindia.com` and `bseindia.com`.
- **`BSE_API_HEADERS`**: Headers including `Referer: https://www.bseindia.com/` for BSE REST endpoints.
- **`NIFTY_INDICES_HEADERS`**: Specialized AJAX headers (`X-Requested-With`, `Origin`, `Content-Type: application/json; charset=UTF-8`) for NiftyIndices Sitefinity MVC endpoints.
- **`NSE_API_HEADERS`**: Headers including `Referer` to internal corporate filings pages on NSE.
- **`_IMPERSONATE_PROFILES`**: Tuple of rotating profiles: `("chrome124", "chrome120", "chrome116")`.

### Class: `SimpleHTTPResponse`
Normalized response object returned by `ResilientHTTPClient`:
- **`status_code: int`**: HTTP response code.
- **`content: bytes`**: Raw binary payload.
- **`headers: dict[str, str]`**: Response headers dictionary.
- **`text -> str`**: Property decoding `content` via `utf-8-sig`. If a `UnicodeDecodeError` occurs, automatically decodes using `cp1252` (Windows-1252) with error replacement, resolving AMFI and legacy BSE encoding incompatibilities.
- **`json() -> Any`**: Parses JSON payload from `self.text`.
- **`raise_for_status() -> None`**: Raises `RuntimeError` if `status_code >= 400`.

### Class: `ResilientHTTPClient`
Asynchronous HTTP client managing connection pooling and anti-bot defense.

- **Initialization (`__init__`)**:
  - Sets up `httpx.AsyncClient` with keep-alive limits (max 32 connections, 180s read timeout).
  - Initializes `curl_cffi.requests.AsyncSession` with Chrome impersonation profile (`chrome124`) and 45s timeout.
  - Maintains session priming flags (`_nse_primed`, `_bse_primed`, `_nifty_primed`) and an `asyncio.Lock` (`_prime_lock`).
- **`_active_ua() -> str`**: Returns the User-Agent string corresponding to the currently active impersonation profile index.
- **`_align_headers(headers: dict) -> dict`**: Overwrites `User-Agent` in the header dictionary with the active profile User-Agent.
- **`get(url, headers, params, retries=3, prime_nse=False, prime_bse=False, timeout=60.0)`**:
  - Checks if the target URL is an exchange domain (`nseindia.com`, `bseindia.com`, `niftyindices.com`).
  - Automatically invokes `_prime_host()` if host priming is requested or required.
  - Routes exchange requests through `curl_session` for TLS impersonation; non-exchange calls utilize `httpx.AsyncClient`.
  - **401/403 Handling**: If an exchange returns 401 or 403, resets the primed flag, sleeps with exponential backoff, rotates the impersonation profile, re-primes the host, and retries.
  - **Truncation Guard**: Compares received bytes against `Content-Length`. If incomplete, throws an error to trigger an immediate retry.
- **`post(url, json_body, headers, retries=3, prime_nifty=False, timeout=60.0)`**:
  - Dispatches POST requests via `curl_session` with `allow_redirects=False` (critical for Sitefinity MVC).
  - Handles 301/302 redirects from `niftyindices.com` as session expiration signals, triggering re-priming.
- **`_prime_host(target: str, reset_session: bool = False)`**:
  - Thread/coroutine-safe via `async with self._prime_lock`.
  - For `target="nse"`: GETs `https://www.nseindia.com`, then GETs `/market-data/live-equity-market` to acquire cookies (`_abck`, `ak_bmsc`, `bm_sz`).
  - For `target="bse"`: GETs `https://www.bseindia.com/` to establish session cookies.
  - For `target="nifty"`: GETs `https://www.niftyindices.com/reports/historical-data`.

---

## 5. Module: `datalake.storage`

**File:** `src/datalake/storage.py` (231 lines)

### Purpose
Low-level Parquet file I/O, column-level data sanitization, atomic filesystem replacements, and deduplication logic.

### Constants
- **`NUMERIC_FLOAT_COLS: frozenset[str]`**: Exhaustive set of numeric columns across all 15 datasets that must be strictly stored as `float64`. Includes OHLCV values, corporate action metrics (`DividendPerShareRs`, `BonusNew`, `BonusHeld`, `SplitFromFV`, `SplitToFV`, `ShareMultiplier`), valuation ratios (`TRI`, `PE`, `PB`, `DivYield`), shareholding percentages, and deal values.
- **`_NUMERIC_SUFFIXES: tuple[str, ...]`**: Suffixes (`_Cr`, `_Rs`, `_Pct`) automatically coerced to numeric floats.

### Functions
- **`_is_numeric_col(col_name: str) -> bool`**: Returns `True` if `col_name` is in `NUMERIC_FLOAT_COLS` or ends with one of `_NUMERIC_SUFFIXES`.
- **`_clean_object_value(v: object) -> str | None`**: Converts string/object cells to clean UTF-8 strings or `None` if null/NaN. Safely handles byte sequences.
- **`_coerce_numeric_series(s: pd.Series, default_val: float | None = None) -> pd.Series`**: Strips commas and whitespace from strings, converts to numeric floats via `pd.to_numeric(s, errors="coerce")`, and casts to `float64`. Fills nulls with `default_val` if specified.
- **`sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame`**:
  - Drops duplicate column names.
  - Applies `_coerce_numeric_series` to all identified numeric columns (`default_val=1.0` for `ShareMultiplier`).
  - Applies `_clean_object_value` to all object columns, eliminating mixed-type serialization errors.
- **`read_table_safe(path: Path) -> pd.DataFrame`**:
  - Reads `path.read_bytes()` into an `io.BytesIO` buffer in RAM before invoking `pd.read_parquet(buf)`.
  - Prevents lingering OS file handle locks on Windows caused by memory mapping.
- **`write_parquet_atomic(df, target_path, row_group_size=10_000, already_sanitized=False)`**:
  - Creates parent directories if missing.
  - Generates a unique temporary filename: `{target_path.stem}.{uuid.uuid4().hex[:8]}.tmp`.
  - Writes Parquet using Zstandard compression (`compression="zstd"`), page indexes (`write_page_index=True`), 64 KB page size, and specified `row_group_size`.
  - Replaces target atomically via `os.replace` with an 8-attempt retry loop backed by `gc.collect()` to resolve Windows `PermissionError`.
  - In a `finally` block, deletes the temporary file if replacement fails.
- **`upsert_parquet_partition(new_df, target_path, dedup_cols, sort_cols=None, row_group_size=10_000) -> int`**:
  - Reads existing Parquet file if present via `read_table_safe()`.
  - Concatenates existing and new dataframes, sanitizing via `sanitize_for_parquet()`.
  - Deduplicates rows on `dedup_cols`, retaining the newest records (`keep="last"`).
  - Sorts by `sort_cols` (defaults to `["Symbol", "Date"]`).
  - Atomically writes back to disk and returns total row count.

---

## 6. Module: `datalake.state`

**File:** `src/datalake/state.py` (144 lines)

### Purpose
SQLite-based provenance, watermark tracking, and idempotency store (`state.db`).

### Class: `StateStore`
- **`__init__(db_path: Path)`**: Initializes database path, creates parent directory, and runs `_init_db()`.
- **`_connect() -> sqlite3.Connection`**: Opens SQLite connection with 30s timeout and enables `PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;`.
- **`_session() -> Iterator[sqlite3.Connection]`**: Context manager yielding a transactional connection that is explicitly closed upon exit, preventing Windows lock contention.
- **`_init_db() -> None`**: Creates `ingestion_log` table and index on `(dataset, status)` if not present.
- **`is_completed(dataset: str, partition_key: str) -> bool`**: Returns `True` if partition exists with status `OK` or `HOLIDAY`.
- **`completed_keys(dataset: str) -> set[str]`**: Returns set of all partition keys marked `OK` or `HOLIDAY` for a dataset.
- **`max_ok_date(dataset: str) -> date | None`**: Finds the maximum ISO-formatted date string (`YYYY-MM-DD`) marked `OK` for a dataset, used for incremental daily synchronization.
- **`record(dataset, partition_key, status, row_count=0, raw_bytes=None, source_url=None) -> None`**:
  - Computes SHA-256 hash of `raw_bytes` if provided.
  - Inserts or updates the record using `ON CONFLICT(dataset, partition_key) DO UPDATE SET ...`, updating timestamps in IST.
- **`summary() -> list[dict]`**: Executes aggregate query returning dataset status, counts of `OK` partitions, counts of `HOLIDAY` days, total rows, minimum/maximum partition keys, and last updated timestamps.

---

## 7. Module: `datalake.symbols`

**File:** `src/datalake/symbols.py` (85 lines)

### Purpose
Builds an in-memory directed acyclic graph (DAG) of historical NSE ticker renames from `nse_symbol_changes.parquet` (`symbolchange.csv`).

### Class: `SymbolRenameResolver`
- **`__init__(df: pd.DataFrame | None = None)`**:
  - Builds two adjacency lists: `_back_edges` (mapping `NewSymbol -> [(ChangeDate, OldSymbol)]`) and `_fwd_edges` (mapping `OldSymbol -> [(ChangeDate, NewSymbol)]`).
  - Sorts backward edges in descending chronological order and forward edges in ascending chronological order.
- **`from_lake(lake_dir="./data") -> SymbolRenameResolver`**: Factory classmethod reading `universe/nse_symbol_changes.parquet`.
- **`traded_as_on(current_symbol: str, target_date: date) -> str`**:
  - Traverses backward edges to determine the exact ticker symbol a company was listed under on a specific historical date.
  - Example: `traded_as_on("ZOMATO", date(2022, 1, 1))` returns `"ZOMATO"`; for a multi-step rename, it traverses backward up to 32 steps until `target_date >= ChangeDate`.
- **`all_aliases(symbol: str) -> list[str]`**:
  - Returns a list beginning with the modern canonical symbol followed by all historical predecessor tickers: `[current_symbol, ...historical_predecessors]`.
  - Walks forward edges to find the modern canonical ticker, then walks backward edges breadth-first to collect all historical aliases.
  - Example: for `MINDTREE` or `LTIM`, returns `['LTIM', 'MINDTREE']`.

---

## 8. Module: `datalake.adjuster`

**File:** `src/datalake/adjuster.py` (194 lines)

### Purpose
Calculates backward-adjusted prices (`Adj Close`), cash dividends, and stock split multipliers across multi-decade corporate actions and symbol rename chains.

### Class: `OHLCVAdjuster`
- **`__init__(lake_dir="./data")`**: Initializes paths and lazy-loaded action dataframes.
- **`resolver -> SymbolRenameResolver`**: Property lazily instantiating `SymbolRenameResolver`.
- **`_load_actions(exchange: str) -> pd.DataFrame`**: Loads and caches `actions/nse_corporate_actions.parquet` or `actions/bse_corporate_actions.parquet`.
- **`get_symbol_actions(symbol_or_scrip: str, exchange="NSE") -> pd.DataFrame`**:
  - Filters actions matching the symbol across all aliases (for NSE) or by 6-digit scrip code (for BSE).
  - Normalizes `ExDate` to timestamps.
  - Aggregates duplicate actions on the same `ExDate` (`sum` for dividends, `prod` for share multipliers).
  - Returns a clean DataFrame of `ExDate`, `ActionType`, `DividendPerShareRs`, and `ShareMultiplier`.
- **`apply_adjustments(df, symbol_or_scrip, exchange="NSE", auto_adjust=False, include_future_splits=False) -> pd.DataFrame`**:
  - **Temporal Alignment**: Uses `np.searchsorted` to map corporate action `ExDate` entries to trading session rows in `df["Date"]`.
  - **Split Factor Accumulation**: Computes reverse cumulative product (`np.cumprod(split_mult[::-1])[::-1]`) of split multipliers.
  - **Future Split Factor**: If `include_future_splits=True`, scales historical slices by splits that occurred after the window's end date.
  - **Upstox Seed Protection**: Checks if `PrevClose` and `TurnoverValue` are NaN (indicating Upstox-seeded data that is already split-adjusted at source) and locks `cum_split_factor = 1.0` for those rows.
  - **Dividend Adjustment Factor**: Calculates proportional price adjustments:
    $$\text{step} = 1.0 - \left(\frac{\text{Dividend}}{\text{Prior Close}}\right)$$
    and accumulates backward via reverse cumulative product.
  - **Demerger / Discovery Detection**: Detects unannounced demergers or pre-open discovery sessions where the exchange adjusted `PrevClose` downward without an explicit dividend filing:
    ```python
    ratio = prev_close / prior_close
    unexplained_gap = (
        (split_mult == 1.0) & (dividend == 0.0) & (days_diff <= 7)
        & (ratio > 0.10) & (ratio < 0.985)
    )
    ```
    Multiplies the adjustment step by `ratio` to prevent artificial chart gaps.
  - **Adj Close Computation**: `out["Adj Close"] = np.round(out["Close"] * total_price_factor, 4)`.
  - **`auto_adjust=True` Mode**: Directly adjusts `Open`, `High`, `Low`, `Close`, `Last`, `PrevClose` by `total_price_factor`, and scales `Volume` and `DeliverableQty` inversely by `cum_split_factor`.

---

## 9. Module: `datalake.doctor`

**File:** `src/datalake/doctor.py` (276 lines)

### Purpose
Automated integrity validation, domain invariant repair, temporary file cleanup, and state reconciliation engine.

### Class: `LakeDoctor`
- **`__init__(cfg: LakeConfig)`**: Initializes with target lake configuration.
- **`run_full_check_and_repair() -> dict[str, int]`**:
  Executes a comprehensive 5-phase audit and repair workflow:
  1. **Temporary File Cleanup**: Scans and unlinks all `*.tmp`, `*.dl_tmp`, and `*.dl.tmp` files.
  2. **SQLite Integrity Check**: Connects to `state.db` and runs `PRAGMA integrity_check;`. If corrupt, creates a backup `state.db.corrupt` and initializes a fresh database.
  3. **Legacy F&O Migration**: Locates `derivatives/*_fo_*_*.parquet` monthly files and merges them into annual files (`*_fo_YYYY.parquet`).
  4. **Parquet Domain Invariant & Truncation Audit**:
     - Tests Parquet readability via `read_table_safe()`. Deletes unreadable/corrupted files.
     - **OHLC Geometry**: Ensures `Close > 0` and executes `repair_ohlc()`.
     - **Shareholding Bounds**: Drops records where `PromoterPct` or `PublicPct` falls outside `[0.0, 100.5]` or total exceeds `101.0%`.
     - **Schema Healing**: Checks that all `NUMERIC_FLOAT_COLS` are float dtypes and deduplicates column headers.
     - **Truncation Detection**: Groups rows by ISO date. If a trading day has fewer rows than the dataset threshold (`nse_cm`: 200, `bse_cm`: 200, `nse_fo`: 50, `indices`: 15, `participant_oi`: 2), purges those dates from the partition and unlinks empty files.
  5. **State Store Reconciliation**:
     - Purges invalid pre-launch `HOLIDAY` records from `state.db`.
     - Deletes `OK` watermarks from `state.db` that no longer exist on disk.
     - Inserts missing `OK` records for dates present on disk.
     - Scans `fundamentals/xbrl_financials.parquet`, `shareholding/nse_shareholding_summary.parquet`, and `shareholding/bse_shareholding_history.parquet` to reconcile completed symbol watermarks.
  - Returns a statistics dictionary:
    ```python
    {
        "tmp_files_cleaned": int,
        "corrupt_parquets_removed": int,
        "truncated_days_purged": int,
        "stale_watermarks_reset": int,
        "watermarks_rebuilt_from_disk": int,
    }
    ```

---

## 10. Module: `datalake.sync`

**File:** `src/datalake/sync.py` (845 lines)

### Purpose
Packaging, release asset generation, delta synchronization, and client-side update retrieval.

### Functions
- **`compute_sha256(path: Path) -> str`**: Reads file in 1 MB chunks and computes hex digest.
- **`build_manifest(data_dir: Path) -> dict`**:
  - Scans all `.parquet` files, mapping relative paths to asset names (`folder__file.parquet`), sizes, SHA-256 hashes, and latest date watermarks.
  - Writes `data/manifest.json`.
- **`build_compacted_equity_bundles(data_dir, staging_dir, manifest, old_manifest=None, remote_sizes=None) -> dict`**:
  - Bundles completed historical equity years ($< \text{current\_year}$) into `compacted__nse_cm_hist.parquet` and `compacted__bse_cm_hist.parquet`.
  - Re-sorts by `(Symbol, Date)` and sets `row_group_size=5_000`.
  - Reuses existing bundles if underlying yearly files have matching SHA-256 hashes.
- **`_serialize_ticker_parquet(df: pd.DataFrame) -> bytes`**:
  - Serializes a single stock's DataFrame to Zstd-compressed Parquet bytes with `write_statistics=False` to eliminate redundant footers.
- **`_pack_frames_by_symbol(frames, alias_to_canon, target_pack_path, exchange="NSE", adjuster=None) -> dict[str, list[int]]`**:
  - Maps tickers to canonical symbols, applies adjustments, serializes each ticker in parallel using `ThreadPoolExecutor`, and concatenates into `target_pack_path`.
  - Returns byte offset dictionary: `{symbol: [byte_offset, byte_length]}`.
- **`build_company_dossier_pack(data_dir, staging_dir, alias_to_canon, scrip_to_canon=None) -> dict[str, list[int]]`**:
  - Consolidates 8 non-price tables (Financials, Shareholding, Actions, Deals, Insider, Concalls, Annual Reports) per symbol.
  - Compresses JSON payloads with `zlib` (level 6) and writes `by_ticker__nse_dossier.pack`.
- **`build_market_screener_snapshot(data_dir, staging_dir) -> dict | None`**:
  - Calls `UnifiedLakeEngine._compute_live_screener_df()` to generate a ~400 KB pre-computed market screener saved to `snapshots/market_screener_latest.parquet`.
- **`_compute_curr_year_carry_factors(curr_frames, alias_to_canon, exchange, adjuster) -> dict[str, tuple[float, float]]`**:
  - Computes `(price_factor, split_factor)` carry values from current-year corporate actions so historical packs remain valid without complete rebuilds.
- **`build_by_ticker_packs(data_dir, staging_dir, manifest, old_manifest=None, remote_sizes=None) -> dict[str, dict]`**:
  - Builds two-tier packs (`hist.pack`, `curr.pack`, `index.json`) for both NSE and BSE.
- **`prepare_release_staging(data_dir, staging_dir, previous_manifest_path=None, tag="datalake-latest") -> dict`**:
  - Clusters equity Parquets by `(Symbol, Date)`.
  - Builds screener snapshot, compacted bundles, packfiles, and `manifest.json`.
  - Copies changed assets into `staging_dir`.
- **`prune_retired_release_assets(data_dir, tag="datalake-latest", staging_dir=None) -> list[str]`**:
  - Compares assets on GitHub Release against active manifest entries and unlinks retired assets via `gh release delete-asset`.
- **`pull_from_github_release(repo, data_dir="./data", tag="datalake-latest", token=None, concurrency=4, pattern=None) -> dict[str, int]`**:
  - Fetches remote `manifest.json`.
  - Compares local file sizes and SHA-256 hashes against remote entries.
  - Downloads only changed/new Parquets concurrently via temporary files (`.dl.tmp`) and verifies SHA-256 digests before atomic replacement.
  - Unlinks obsolete local partitions not present in the remote manifest.

---

## 11. Module: `datalake.remote`

**File:** `src/datalake/remote.py` (1149 lines)

### Purpose
Zero-disk, in-memory query engine querying remote GitHub Release Parquets via DuckDB `httpfs` HTTP Range requests and packfile byte slicing.

### Class: `RemoteLakeClient`
- **`__init__(repo, tag="datalake-latest", token=None, load_httpfs=True, cache_dir=None, use_pack=True)`**:
  - Initializes DuckDB in-memory database (`duckdb.connect(":memory:")`).
  - Sets up `httpx.Client` connection pool without Authorization headers on default calls (preventing Azure CDN 403 errors).
  - Caches CDN redirect locations (`_cdn_url_cache`) for 10 minutes.
- **`_ensure_httpfs()`**:
  - Executes `LOAD httpfs;`, sets `enable_http_metadata_cache=true`, `parquet_metadata_cache=true`, `http_keep_alive=true`, `http_retries=3`.
  - Injects GitHub Authorization token if provided using DuckDB `CREATE OR REPLACE SECRET`.
- **`_resolve_cdn_url(gh_url: str) -> str`**:
  - Dispatches a `HEAD` request to resolve the 302 redirect from `github.com/.../releases/download/...` to `objects.githubusercontent.com` (Azure CDN SAS URL).
  - Caches the resolved CDN URL for 600 seconds.
- **`_parse_symbol_and_exchange(symbol: str, exchange: str | None = None) -> tuple[str, str, bool]`**:
  - Parses tickers with prefixes or suffixes (e.g., `RELIANCE.BO` $\to$ `('RELIANCE', 'BSE', True)`, `500325` $\to$ `('500325', 'BSE', False)`).
- **`_load_ticker_index(exchange="NSE") -> dict | None`**:
  - Downloads and caches `by_ticker__{exch}_index.json` once per session.
- **`_fetch_pack_slice(pack_filename: str, offset: int, length: int) -> pd.DataFrame`**:
  - Sends HTTP request with `Range: bytes={offset}-{offset + length - 1}` to the CDN URL.
  - Reads returning byte slice directly via `pd.read_parquet(io.BytesIO(raw_bytes))`.
  - Caches slices locally if `cache_dir` is configured.
- **`history(symbol, start_year=2024, end_year=2026, exchange=None, columns=None, auto_adjust=True, use_pack=None) -> pd.DataFrame`**:
  - **Fast Path (`use_pack=True`)**: Queries `by_ticker__{exch}_hist.pack` and `curr.pack` via exact byte-ranges in ~120 ms. Applies corporate action carry factors and in-memory adjustments.
  - **Fallback Path (`use_pack=False`)**: Executes DuckDB `httpfs` query over compacted bundle or yearly Parquet files.
- **`download(symbols, start_year=2024, end_year=2026, exchange=None, columns=None, use_pack=None) -> pd.DataFrame`**:
  - For $\le 15$ symbols, downloads pack slices concurrently using `ThreadPoolExecutor`.
  - For larger symbol lists, executes a vectorized DuckDB `httpfs` query with `WHERE Symbol IN (...)`.
- **`dossier(symbol: str) -> dict[str, pd.DataFrame]`**:
  - Fetches the zlib-compressed company dossier from `by_ticker__nse_dossier.pack` in a single byte-range call, returning all 8 non-price tables.
- **`financials(symbol, period_type="Quarterly") -> pd.DataFrame`**: Streams Ind-AS XBRL financial statements.
- **`shareholding(symbol, exchange=None) -> pd.DataFrame`**: Streams quarterly shareholding pattern and promoter pledge history.
- **`named_holders(symbol) -> pd.DataFrame`**: Streams $>1\%$ institutional and individual shareholders.
- **`corporate_actions(symbol, exchange=None) -> pd.DataFrame`**: Streams parsed dividends, bonuses, and splits.
- **`index_history(index_name="NIFTY 50") -> pd.DataFrame`**: Queries 30-year index history from `nifty_30y_ohlc_tri_pe.parquet`.
- **`deals(symbol) -> pd.DataFrame`**: Streams Bulk and Block deals.
- **`insider_trades(symbol) -> pd.DataFrame`**: Streams SEBI PIT insider trading disclosures.
- **`annual_reports(symbol) -> pd.DataFrame`**: Streams official annual report PDF download links.
- **`concalls(scrip_code_or_symbol) -> pd.DataFrame`**: Streams earnings call transcripts, presentations, and audio links.
- **`fii_dii_flows() -> pd.DataFrame`**: Streams cash market provisional institutional flows.
- **`participant_oi(year=None) -> pd.DataFrame`**: Streams NSCCL participant open interest breakdown.
- **`screener(sql_filter=None) -> pd.DataFrame`**: Queries `snapshots__market_screener_latest.parquet` in ~120 ms.
- **`option_chain(symbol, year=None, month=None, trade_date=None, exchange=None) -> pd.DataFrame`**: Streams EOD option chains with strike grids and open interest.
- **`panel(symbols, field="Adj Close", start_year=2024, end_year=2026) -> pd.DataFrame`**: Generates a date-aligned 2D pivot matrix (`Date x Symbol`).
- **`continuous_futures(symbol="NIFTY", contract=1, start_year=2024, end_year=2026) -> pd.DataFrame`**: Stitches front-month or next-month futures contracts over HTTPS.
- **`sql(query: str, params=None) -> pd.DataFrame`**: Executes arbitrary SQL over remote Parquet datasets.

---

## 12. Module: `datalake.engine`

**File:** `src/datalake/engine.py` (1465 lines)

### Purpose
Local high-performance analytical query engine providing universal crosswalk resolution, PyArrow row-group pushdown, DuckDB SQL views, and a `yfinance`-style `LakeTicker` facade.

### Class: `SecurityIdentity`
Immutable dataclass representing a resolved security:
```python
@dataclass(frozen=True)
class SecurityIdentity:
    symbol: str
    bse_code: str | None
    isin: str | None
    company_name: str | None
    aliases: tuple[str, ...]
    face_value: float | None = None
    industry: str | None = None
    listing_date: str | None = None
    primary_exchange: str = "NSE"
```

### Class: `UnifiedLakeEngine`
Core local query engine.

- **Initialization (`__init__(lake_dir="./data")`)**:
  - Initializes root path, `OHLCVAdjuster`, and `SymbolRenameResolver`.
  - Sets up internal crosswalk lookup dictionaries: `_by_symbol`, `_by_bse`, and `_by_isin`.
- **`from_github(repo, lake_dir="./data", auto_sync=True, token=None) -> UnifiedLakeEngine`**:
  - Factory method that automatically runs `pull_from_github_release()` before initializing and returning the engine.
- **`_ensure_crosswalk()`**:
  - Scans `universe/nse_equity_master.parquet`, `bse_scrip_master.parquet`, `nifty_constituents_pit.parquet`, and security masters.
  - Builds universal crosswalk resolving any identifier (`RELIANCE` $\leftrightarrow$ `500325` $\leftrightarrow$ `INE002A01018`).
- **`resolve(query: str | int) -> SecurityIdentity`**:
  - Resolves any NSE symbol, historical rename, BSE scrip code, or ISIN to its canonical `SecurityIdentity`.
- **`ticker(query: str | int) -> LakeTicker`**:
  - Returns a `LakeTicker` instance for the resolved identity.
- **`_read_filtered_parquet(path: Path, filters: list[tuple] | None = None) -> pd.DataFrame`**:
  - Utilizes PyArrow row-group pushdown (`pd.read_parquet(path, filters=filters)`).
  - Skips reading irrelevant row groups from disk, reducing I/O by up to 99%.
- **`_dedup_nse_equity_series(df: pd.DataFrame) -> pd.DataFrame`**:
  - Resolves duplicate price records for stocks trading across multiple NSE series on the same day (`EQ` > `BE` > `BZ` > `SM` > `ST` > `BL`).
- **`_cached_raw_equity(query_key, exchange, start_yr, end_yr) -> pd.DataFrame`**:
  - LRU-cached (`@lru_cache(maxsize=256)`) raw equity reader providing sub-millisecond retrieval on repeat queries.
- **`history(query, start=None, end=None, period=None, exchange=None, auto_adjust=True) -> pd.DataFrame`**:
  - Reads raw equity bars, applies corporate action adjustments, and stitches historical ticker renames.
- **`download(queries, start=None, end=None, period=None, exchange=None, auto_adjust=True) -> dict[str, pd.DataFrame]`**:
  - Vectorized multi-symbol downloader that scans each yearly Parquet file only once for all requested tickers.
- **`fundamentals(query, period_type=None, consolidated=True) -> pd.DataFrame`**:
  - Reads Ind-AS XBRL financials, deduplicating Standalone vs Consolidated reports.
- **`shareholding(query) -> pd.DataFrame`**:
  - Merges NSE and BSE quarterly shareholding patterns (2001 to Present).
- **`option_chain(symbol="NIFTY", trade_date=None, expiry=None, exchange=None) -> pd.DataFrame`**:
  - Queries derivatives partitions with strike price, settlement price, and open interest.
  - Automatically backfills missing `UnderlyingPrice` fields from spot index or equity close prices.
- **`index_history(index_name="NIFTY 50", start=None, end=None) -> pd.DataFrame`**:
  - Queries 30-year continuous index OHLC, TRI, P/E, P/B, and DivYield.
- **`factor_returns(start="2005-01-01", end=None) -> pd.DataFrame`**:
  - Computes institutional risk factor returns: `MKT` (NIFTY 500 TRI), `RF` (NIFTY 1D Rate Index), `MKT_RF`, `SMB` (Size factor), and sector excess returns (`BANK`, `IT`, `PHARMA`, etc.).
- **`trading_days(start="1995-11-03", end=None) -> pd.DatetimeIndex`**:
  - Returns authoritative NSE trading session calendar derived from NIFTY 50 history.
- **`optimize_layout() -> dict[str, int]`**:
  - Re-clusters and sorts Parquet files by `(Symbol, Date)` with page indexes, enabling optimal pushdown performance.
- **`sql(query: str) -> pd.DataFrame`**:
  - Registers all 15 Parquet datasets as DuckDB SQL views (`equities_nse`, `equities_bse`, `fo_nse`, `indices`, `financials`, `shareholding`, `deals`, etc.) and executes zero-copy SQL queries.

### Class: `LakeTicker`
Facade providing a `yfinance`-compatible object-oriented interface:
- **Properties**: `symbol`, `isin`, `bse_code`, `info`, `fast_info`, `dossier`, `financials`, `quarterly_financials`, `ttm_financials`, `shareholding`, `institutional_holders`, `calendar`, `actions`, `deals`, `annual_reports`, `concalls`, `insider_trades`.
- **Methods**: `history(start=None, end=None, period=None, exchange=None, auto_adjust=True)`.

---

## 13. Module: `datalake.cli`

**File:** `src/datalake/cli.py` (306 lines)

### Purpose
Command-line interface powering batch backfills, daily updates, XBRL extraction, health checks, delta synchronization, and release staging.

### Subcommands & Handlers
1. **`backfill` (`run_backfill`)**: Executes full historical ingestion (Steps 1–6) across security masters, corporate actions, 30Y indices, equities (1995+), F&O (2000+), flows, deals, and AMFI NAVs (2006+).
2. **`daily` (`run_daily_update`)**: Incremental synchronization from last recorded watermarks in `state.db` up to today IST.
3. **`xbrl` (`run_xbrl_sync`)**: Synchronizes Ind-AS XBRL financials, shareholding patterns, and annual reports for specified symbols or market-wide (`--all-market`).
4. **`pre2016-nse` (`run_pre2016_nse`)**: Backfills 1995–2015 NSE cash equities via the `/api/historicalOR/generateSecurityWiseHistoricalData` endpoint.
5. **`seed-cloud` (`run_seed_cloud`)**: Cloud-native seeding of 2008–2015 NSE equities via Upstox public V2 candle CDN.
6. **`doctor` (`LakeDoctor.run_full_check_and_repair`)**: Audits filesystem integrity, repairs schemas, and reconciles watermarks.
7. **`status` (`print_status`)**: Displays tabular summary of dataset watermarks, partition counts, holidays, and total rows from `state.db`.
8. **`pull` (`pull_from_github_release`)**: Client-side delta sync downloading changed Parquets from GitHub Releases via SHA-256 manifest comparison.
9. **`stage-release` (`prepare_release_staging`)**: CI/CD staging task optimizing layouts, generating `manifest.json`, building compacted bundles, and staging assets.
10. **`prune-release` (`prune_retired_release_assets`)**: Deletes retired or obsolete assets from GitHub Releases.
