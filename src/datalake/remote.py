from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import io
import json
import logging
import os
from pathlib import Path
import time
from typing import Any
import urllib.request
import zlib
import pandas as pd

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    duckdb = None  # type: ignore
    HAS_DUCKDB = False

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None  # type: ignore
    HAS_HTTPX = False

logger = logging.getLogger("datalake.remote")


class RemoteLakeClient:
    """
    Zero-disk (or hybrid-cached) network client that queries GitHub Release Parquet files
    directly over HTTPS using DuckDB `httpfs` HTTP Byte-Range requests.
    """

    def __init__(
        self,
        repo: str,
        tag: str = "datalake-latest",
        token: str | None = None,
        load_httpfs: bool = True,
        cache_dir: Path | str | None = None,
    ):
        if not HAS_DUCKDB:
            raise RuntimeError("Install duckdb (`uv add duckdb`) for zero-disk HTTP Range Parquet queries.")
        self.repo = repo.strip("/")
        self.tag = tag
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.base_url = f"https://github.com/{self.repo}/releases/download/{tag}"
        self.con = duckdb.connect(":memory:")
        self.cache_dir = Path(cache_dir).expanduser().resolve() if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._httpfs_loaded = False
        self._cdn_url_cache: dict[str, tuple[float, str]] = {}
        self._compacted_bounds_cache: dict[str, tuple[int, int] | None] | None = None

        headers = {"User-Agent": "bfinance-datalake/0.2.0"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self._http_pool = (
            httpx.Client(
                headers=headers,
                follow_redirects=False,
                timeout=5.0,
                limits=httpx.Limits(max_keepalive_connections=16, max_connections=24),
            )
            if HAS_HTTPX
            else None
        )

        if load_httpfs:
            self._ensure_httpfs()

    def close(self) -> None:
        """Close pooled HTTP connections and DuckDB in-memory handle."""
        if self._http_pool is not None:
            try:
                self._http_pool.close()
            except Exception:
                pass
            self._http_pool = None
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass

    def __enter__(self) -> RemoteLakeClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _ensure_httpfs(self) -> None:
        if self._httpfs_loaded:
            return
        self._httpfs_loaded = True
        try:
            self.con.execute("LOAD httpfs;")
        except Exception:
            try:
                self.con.execute("INSTALL httpfs; LOAD httpfs;")
            except Exception as exc:
                logger.warning("Could not load DuckDB httpfs extension: %s", exc)

        for stmt in (
            "SET enable_http_metadata_cache=true;",
            "SET parquet_metadata_cache=true;",
            "SET http_keep_alive=true;",
            "SET http_retries=3;",
        ):
            try:
                self.con.execute(stmt)
            except Exception:
                pass

        if self.token:
            try:
                self.con.execute(
                    "CREATE OR REPLACE SECRET gh_token (TYPE HTTP, EXTRA_HTTP_HEADERS MAP {'Authorization': ?});",
                    [f"Bearer {self.token}"],
                )
            except Exception:
                pass

    def _asset_url(self, folder: str, filename: str) -> str:
        return f"{self.base_url}/{folder}__{filename}"

    def _resolve_cdn_url(self, gh_url: str) -> str:
        """Resolve GitHub Release 302 redirect to direct CDN URL using a pooled connection (cached 10 min)."""
        now = time.monotonic()
        cached = self._cdn_url_cache.get(gh_url)
        if cached and (now - cached[0]) < 600.0:
            return cached[1]

        if self._http_pool is not None:
            try:
                resp = self._http_pool.head(gh_url)
                if resp.status_code in (301, 302, 303, 307, 308):
                    cdn_url = resp.headers.get("Location") or resp.headers.get("location")
                    if cdn_url:
                        self._cdn_url_cache[gh_url] = (now, cdn_url)
                        return cdn_url
            except Exception:
                pass
            return gh_url

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        req = urllib.request.Request(gh_url, method="HEAD")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        opener = urllib.request.build_opener(_NoRedirect)
        try:
            opener.open(req, timeout=5.0)
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 303, 307, 308):
                cdn_url = e.headers.get("Location")
                if cdn_url:
                    self._cdn_url_cache[gh_url] = (now, cdn_url)
                    return cdn_url
        except Exception:
            pass
        return gh_url

    def _resolve_cdn_urls_parallel(self, gh_urls: list[str]) -> list[str]:
        if len(gh_urls) <= 1:
            return [self._resolve_cdn_url(u) for u in gh_urls]
        with ThreadPoolExecutor(max_workers=min(12, len(gh_urls))) as pool:
            return list(pool.map(self._resolve_cdn_url, gh_urls))

    def _get_compacted_bounds(self, exchange: str) -> tuple[int, int] | None:
        """Check manifest.json once per session to see if compacted__{exch}_cm_hist.parquet is published."""
        if self._compacted_bounds_cache is None:
            self._compacted_bounds_cache = {}
            manifest_url = f"{self.base_url}/manifest.json"
            try:
                cdn_m_url = self._resolve_cdn_url(manifest_url)
                if self._http_pool is not None:
                    r = self._http_pool.get(cdn_m_url, follow_redirects=True)
                    if r.status_code == 200:
                        comp = r.json().get("compacted_files", {})
                        for k, meta in comp.items():
                            self._compacted_bounds_cache[k] = (int(meta["min_year"]), int(meta["max_year"]))
                else:
                    req = urllib.request.Request(cdn_m_url)
                    if self.token:
                        req.add_header("Authorization", f"Bearer {self.token}")
                    with urllib.request.urlopen(req, timeout=5.0) as resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode("utf-8"))
                            comp = data.get("compacted_files", {})
                            for k, meta in comp.items():
                                self._compacted_bounds_cache[k] = (int(meta["min_year"]), int(meta["max_year"]))
            except Exception:
                pass
        asset_name = f"compacted__{exchange.lower()}_cm_hist.parquet"
        return self._compacted_bounds_cache.get(asset_name)

    def _equity_urls_for_range(self, exchange: str, start_year: int, end_year: int) -> list[str]:
        """
        Return minimal set of Parquet URLs for [start_year..end_year].
        If compacted history bundle exists and >1 historical year is requested, uses
        `compacted__{exch}_cm_hist.parquet` + `equities__{exch}_cm_{current_year}.parquet` (2 files instead of 32).
        """
        exch = exchange.lower()
        prefix = f"{exch}_cm_"
        bounds = self._get_compacted_bounds(exch)
        if bounds is not None:
            c_min, c_max = bounds
            hist_years_requested = min(end_year, c_max) - max(start_year, c_min) + 1
            if hist_years_requested >= 2:
                raw_urls = [
                    self._asset_url("equities", f"{prefix}{yr}.parquet")
                    for yr in range(start_year, min(end_year + 1, c_min))
                ]
                raw_urls.append(self._asset_url("compacted", f"{prefix}hist.parquet"))
                for yr in range(max(start_year, c_max + 1), end_year + 1):
                    raw_urls.append(self._asset_url("equities", f"{prefix}{yr}.parquet"))
                return self._resolve_cdn_urls_parallel(raw_urls)

        raw_urls = [self._asset_url("equities", f"{prefix}{yr}.parquet") for yr in range(start_year, end_year + 1)]
        return self._resolve_cdn_urls_parallel(raw_urls)

    @staticmethod
    def _clean_sym(symbol: str) -> str:
        s = str(symbol).strip().upper()
        return s[:-3] if s.endswith((".NS", ".BO")) else s

    @staticmethod
    def _select_clause(columns: list[str] | None) -> str:
        if not columns:
            return "*"
        safe_cols = [f'"{c.replace(chr(34), "")}"' for c in columns]
        return ", ".join(safe_cols)

    def _load_ticker_index(self) -> dict | None:
        """Fetch and cache `by_ticker__nse_index.json` once per client session."""
        if getattr(self, "_ticker_index_loaded", False):
            return self._ticker_index
        self._ticker_index_loaded = True
        self._ticker_index: dict | None = None

        idx_url = self._resolve_cdn_url(f"{self.base_url}/by_ticker__nse_index.json")
        try:
            if self._http_pool is not None:
                resp = self._http_pool.get(idx_url, follow_redirects=True)
                if resp.status_code == 200:
                    self._ticker_index = resp.json()
            else:
                headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
                req = urllib.request.Request(idx_url, headers=headers)
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status == 200:
                        self._ticker_index = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.debug("Per-ticker index not available, falling back to DuckDB httpfs: %s", exc)
        return self._ticker_index

    def _fetch_pack_slice(self, pack_filename: str, offset: int, length: int) -> pd.DataFrame:
        """Fetch a single ticker's exact byte range (`Range: bytes=start-end`) and parse as Parquet."""
        if self.cache_dir:
            cache_file = self.cache_dir / f"{pack_filename}_{offset}_{length}.parquet"
            if cache_file.exists():
                try:
                    return pd.read_parquet(cache_file)
                except Exception:
                    pass

        cdn_url = self._resolve_cdn_url(f"{self.base_url}/{pack_filename}")
        end_byte = offset + length - 1
        headers = {"Range": f"bytes={offset}-{end_byte}"}
        if self._http_pool is not None:
            resp = self._http_pool.get(cdn_url, headers=headers, follow_redirects=True)
            raw_bytes = resp.content
        else:
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            req = urllib.request.Request(cdn_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15.0) as resp:
                raw_bytes = resp.read()

        df = pd.read_parquet(io.BytesIO(raw_bytes))
        if self.cache_dir and not df.empty:
            try:
                cache_file = self.cache_dir / f"{pack_filename}_{offset}_{length}.parquet"
                df.to_parquet(cache_file, compression="zstd", index=False)
            except Exception:
                pass
        return df

    def history(
        self,
        symbol: str,
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str = "NSE",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Stream a single stock's OHLCV + Delivery over HTTPS.
        - Fast Path (~120 ms): Uses `by_ticker__nse_index.json` + direct HTTP Range slice from packfile.
        - Fallback Path: Uses two-tier compacted or yearly partitions via DuckDB `httpfs`.
        """
        sym = self._clean_sym(symbol)
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        exch = exchange.upper()
        if exch == "NSE":
            idx_data = self._load_ticker_index()
            if idx_data is not None:
                aliases = idx_data.get("aliases", {})
                canon_sym = aliases.get(sym, sym)
                entry = idx_data.get("tickers", {}).get(canon_sym)
                if entry is not None:
                    cur_yr = int(idx_data.get("current_year", pd.Timestamp.now().year))
                    tasks: list[tuple[str, int, int]] = []
                    if start_year < cur_yr and "hist" in entry:
                        tasks.append(("by_ticker__nse_hist.pack", entry["hist"][0], entry["hist"][1]))
                    if end_year >= cur_yr and "curr" in entry:
                        tasks.append(("by_ticker__nse_curr.pack", entry["curr"][0], entry["curr"][1]))

                    if tasks:
                        if len(tasks) == 1:
                            frames = [self._fetch_pack_slice(*tasks[0])]
                        else:
                            with ThreadPoolExecutor(max_workers=2) as pool:
                                frames = list(pool.map(lambda t: self._fetch_pack_slice(*t), tasks))
                        df = pd.concat(frames, ignore_index=True)
                        years = pd.to_datetime(df["Date"]).dt.year
                        out = (
                            df[(years >= start_year) & (years <= end_year)]
                            .sort_values("Date")
                            .reset_index(drop=True)
                        )
                        if columns:
                            valid_cols = [c for c in columns if c in out.columns]
                            if valid_cols:
                                out = out[valid_cols]
                        return out
                    return pd.DataFrame()

        self._ensure_httpfs()
        sym = self._clean_sym(symbol)
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        exch = exchange.upper()
        sel = self._select_clause(columns)
        current_yr = pd.Timestamp.now().year

        # Optional local immutable slice cache for historical years (< current_yr)
        if self.cache_dir and start_year < current_yr and columns is None:
            cache_file = self.cache_dir / f"{exch}_{sym}_{start_year}_{min(end_year, current_yr - 1)}.parquet"
            if cache_file.exists():
                hist_df = pd.read_parquet(cache_file)
                if end_year >= current_yr:
                    cur_urls = self._equity_urls_for_range(exch, current_yr, end_year)
                    cur_sql = ", ".join(f"'{u}'" for u in cur_urls)
                    cur_df = self.con.cursor().execute(
                        f"SELECT {sel} FROM read_parquet([{cur_sql}]) WHERE Symbol = ? ORDER BY Date ASC",
                        [sym],
                    ).df()
                    return pd.concat([hist_df, cur_df], ignore_index=True).sort_values("Date").reset_index(drop=True)
                return hist_df

        urls = self._equity_urls_for_range(exch, start_year, end_year)
        url_list_sql = ", ".join(f"'{u}'" for u in urls)

        id_col = "InstrumentId" if exch == "BSE" and sym.isdigit() else "Symbol"
        query = f"""
            SELECT {sel}
            FROM read_parquet([{url_list_sql}])
            WHERE {id_col} = ?
              AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
            ORDER BY Date ASC
        """
        df = self.con.cursor().execute(query, [sym, start_year, end_year]).df()

        if self.cache_dir and not df.empty and start_year < current_yr and columns is None:
            hist_end = min(end_year, current_yr - 1)
            cache_file = self.cache_dir / f"{exch}_{sym}_{start_year}_{hist_end}.parquet"
            if not cache_file.exists():
                hist_slice = df[pd.to_datetime(df["Date"]).dt.year <= hist_end]
                if not hist_slice.empty:
                    hist_slice.to_parquet(cache_file, compression="zstd", index=False)

        return df

    def download(
        self,
        symbols: list[str],
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str = "NSE",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Stream multiple symbols in a single vectorized HTTP Range query."""
        self._ensure_httpfs()
        if not symbols:
            return pd.DataFrame()
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        clean_syms = [self._clean_sym(s) for s in symbols]
        urls = self._equity_urls_for_range(exchange.upper(), start_year, end_year)
        url_list_sql = ", ".join(f"'{u}'" for u in urls)
        placeholders = ", ".join(["?"] * len(clean_syms))
        sel = self._select_clause(columns)
        return self.con.cursor().execute(
            f"""
            SELECT {sel}
            FROM read_parquet([{url_list_sql}])
            WHERE Symbol IN ({placeholders})
              AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
            ORDER BY Symbol, Date ASC
            """,
            [*clean_syms, start_year, end_year],
        ).df()

    def financials(self, symbol: str, period_type: str = "Quarterly") -> pd.DataFrame:
        """Stream a company's Ind-AS XBRL Financials over HTTPS in ~250ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("fundamentals", "xbrl_financials.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? AND PeriodType = ? ORDER BY PeriodEnd ASC",
            [self._clean_sym(symbol), period_type],
        ).df()

    def shareholding(self, symbol: str) -> pd.DataFrame:
        """Stream a company's Shareholding & Promoter Pledge history over HTTPS in ~250ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("shareholding", "nse_shareholding_summary.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd ASC",
            [self._clean_sym(symbol)],
        ).df()

    def named_holders(self, symbol: str) -> pd.DataFrame:
        """Stream >1% institutional and named shareholders over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("shareholding", "named_shareholders.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd DESC, PctOfTotal DESC",
            [self._clean_sym(symbol)],
        ).df()

    def corporate_actions(self, symbol: str) -> pd.DataFrame:
        """Stream parsed corporate actions (splits, bonuses, dividends) over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("actions", "nse_corporate_actions.parquet"))
        df = self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE symbol = ? ORDER BY exDate ASC",
            [self._clean_sym(symbol)],
        ).df()
        if "ExDate_1" in df.columns and "ExDate" not in df.columns:
            df["ExDate"] = pd.to_datetime(df["ExDate_1"], errors="coerce")
        elif "ExDate" not in df.columns and "exDate" in df.columns:
            df["ExDate"] = pd.to_datetime(df["exDate"], errors="coerce")
        return df

    def index_history(self, index_name: str = "NIFTY 50") -> pd.DataFrame:
        """Stream 30-year Nifty index OHLC, TRI, P/E, P/B, and DivYield over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("indices", "nifty_30y_ohlc_tri_pe.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(IndexName) = ? ORDER BY Date ASC",
            [index_name.strip().upper()],
        ).df()

    def deals(self, symbol: str) -> pd.DataFrame:
        """Stream Bulk & Block deals for a symbol over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "bulk_block_deals.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Date DESC",
            [self._clean_sym(symbol)],
        ).df()

    def insider_trades(self, symbol: str) -> pd.DataFrame:
        """Stream SEBI PIT insider trading disclosures for a symbol over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "insider_trading_pit.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE symbol = ? ORDER BY date DESC",
            [self._clean_sym(symbol)],
        ).df()

    def annual_reports(self, symbol: str) -> pd.DataFrame:
        """Stream 20-year Annual Report PDF links over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "annual_reports.parquet"))
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Year DESC",
            [self._clean_sym(symbol)],
        ).df()

    def concalls(self, scrip_code_or_symbol: str) -> pd.DataFrame:
        """Stream Concall Transcripts, Investor Presentations & Credit Ratings over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "concalls_and_filings.parquet"))
        q = str(scrip_code_or_symbol).strip()
        if q.isdigit():
            return self.con.cursor().execute(
                f"SELECT * FROM read_parquet('{url}') WHERE ScripCode = ? ORDER BY FilingTimestamp DESC",
                [q],
            ).df()
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(CompanyName) LIKE ? ORDER BY FilingTimestamp DESC",
            [f"%{self._clean_sym(q)}%"],
        ).df()

    def dossier(self, symbol: str) -> dict[str, pd.DataFrame]:
        """Fetch Financials, Shareholding, Holders, Actions, Deals, Insider & Annual Reports in 1 HTTP Range call."""
        sym = self._clean_sym(symbol)
        idx_data = self._load_ticker_index() or {}
        canon = idx_data.get("aliases", {}).get(sym, sym)
        loc = idx_data.get("tickers", {}).get(canon, {}).get("dossier")
        if not loc:
            return {}
        offset, length = loc
        cdn_url = self._resolve_cdn_url(f"{self.base_url}/by_ticker__nse_dossier.pack")
        end_byte = offset + length - 1
        headers = {"Range": f"bytes={offset}-{end_byte}"}
        if self._http_pool is not None:
            resp = self._http_pool.get(cdn_url, headers=headers, follow_redirects=True)
            raw_bytes = resp.content
        else:
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            req = urllib.request.Request(cdn_url, headers=headers)
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                raw_bytes = resp.read()
        payload = json.loads(zlib.decompress(raw_bytes).decode("utf-8"))
        return {k: pd.DataFrame(v) for k, v in payload.items()}

    def screener(self, sql_filter: str | None = None) -> pd.DataFrame:
        """Query the latest cross-sectional market screener snapshot (~400 KB) in ~120 ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(f"{self.base_url}/snapshots__market_screener_latest.parquet")
        where = f"WHERE {sql_filter}" if sql_filter else ""
        return self.con.cursor().execute(f"SELECT * FROM read_parquet('{url}') {where}").df()

    def option_chain(
        self,
        symbol: str,
        year: int | None = None,
        month: int | None = None,
        trade_date: str | date | None = None,
    ) -> pd.DataFrame:
        """Stream an EOD Option Chain for a specific symbol over HTTPS in ~400ms."""
        self._ensure_httpfs()
        if trade_date and year is None:
            year = pd.Timestamp(trade_date).year
        yr = year or pd.Timestamp.now().year
        url = self._resolve_cdn_url(self._asset_url("derivatives", f"nse_fo_{yr}.parquet"))
        if trade_date:
            return self.con.cursor().execute(
                f"""SELECT * FROM read_parquet('{url}')
                WHERE Symbol = ?
                  AND Date >= CAST(? AS TIMESTAMP)
                  AND Date < CAST(? AS TIMESTAMP) + INTERVAL 1 DAY
                ORDER BY ExpiryDate, StrikePrice, OptionType""",
                [symbol.strip().upper(), str(trade_date), str(trade_date)],
            ).df()
        return self.con.cursor().execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Date, ExpiryDate, StrikePrice",
            [symbol.strip().upper()],
        ).df()

    def sql(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute arbitrary analytical SQL over remote Parquet datasets via DuckDB."""
        if params:
            return self.con.cursor().execute(query, params).df()
        return self.con.cursor().execute(query).df()
