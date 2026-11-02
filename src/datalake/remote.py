from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import os
import time
from datetime import date
from typing import Any
import urllib.request
import pandas as pd

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    duckdb = None  # type: ignore
    HAS_DUCKDB = False

logger = logging.getLogger("datalake.remote")


class RemoteLakeClient:
    """
    Zero-disk network client that queries your GitHub Release Parquet files directly over HTTPS
    using DuckDB `httpfs` HTTP Byte-Range requests (transfers ~250 KB per query in ~300-550ms).
    """

    def __init__(
        self,
        repo: str,
        tag: str = "datalake-latest",
        token: str | None = None,
        load_httpfs: bool = True,
    ):
        if not HAS_DUCKDB:
            raise RuntimeError("Install duckdb (`uv add duckdb`) for zero-disk HTTP Range Parquet queries.")
        self.repo = repo.strip("/")
        self.tag = tag
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.base_url = f"https://github.com/{self.repo}/releases/download/{tag}"
        self.con = duckdb.connect(":memory:")
        self._httpfs_loaded = False
        self._cdn_url_cache: dict[str, tuple[float, str]] = {}

        if load_httpfs:
            self._ensure_httpfs()

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

        try:
            # Cache Parquet footers and HTTP Range chunks in RAM so repeat queries take ~120ms
            self.con.execute("SET enable_http_metadata_cache=true;")
            self.con.execute("SET http_keep_alive=true;")
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
        """Resolve GitHub Release 302 redirect to direct CDN URL (cached for 10 minutes)."""
        now = time.monotonic()
        cached = self._cdn_url_cache.get(gh_url)
        if cached and (now - cached[0]) < 600.0:
            return cached[1]

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

    @staticmethod
    def _clean_sym(symbol: str) -> str:
        s = str(symbol).strip().upper()
        return s[:-3] if s.endswith((".NS", ".BO")) else s

    def history(
        self,
        symbol: str,
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Stream a single stock's OHLCV + Delivery directly over HTTPS via HTTP Range requests
        without downloading the full Parquet files to disk.
        """
        self._ensure_httpfs()
        sym = self._clean_sym(symbol)
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        prefix = "nse_cm_" if exchange.upper() == "NSE" else "bse_cm_"
        raw_urls = [self._asset_url("equities", f"{prefix}{yr}.parquet") for yr in range(start_year, end_year + 1)]
        urls = self._resolve_cdn_urls_parallel(raw_urls)
        url_list_sql = ", ".join(f"'{u}'" for u in urls)

        query = f"""
            SELECT *
            FROM read_parquet([{url_list_sql}])
            WHERE Symbol = ?
            ORDER BY Date ASC
        """
        return self.con.execute(query, [sym]).df()

    def download(
        self,
        symbols: list[str],
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """Stream multiple symbols in a single vectorized HTTP Range query."""
        self._ensure_httpfs()
        if not symbols:
            return pd.DataFrame()
        if start_year > end_year:
            start_year, end_year = end_year, start_year
        clean_syms = [self._clean_sym(s) for s in symbols]
        prefix = "nse_cm_" if exchange.upper() == "NSE" else "bse_cm_"
        raw_urls = [self._asset_url("equities", f"{prefix}{yr}.parquet") for yr in range(start_year, end_year + 1)]
        urls = self._resolve_cdn_urls_parallel(raw_urls)
        url_list_sql = ", ".join(f"'{u}'" for u in urls)
        placeholders = ", ".join(["?"] * len(clean_syms))
        return self.con.execute(
            f"SELECT * FROM read_parquet([{url_list_sql}]) WHERE Symbol IN ({placeholders}) ORDER BY Symbol, Date ASC",
            clean_syms,
        ).df()

    def financials(self, symbol: str, period_type: str = "Quarterly") -> pd.DataFrame:
        """Stream a company's Ind-AS XBRL Financials over HTTPS in ~250ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("fundamentals", "xbrl_financials.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? AND PeriodType = ? ORDER BY PeriodEnd ASC",
            [self._clean_sym(symbol), period_type],
        ).df()

    def shareholding(self, symbol: str) -> pd.DataFrame:
        """Stream a company's Shareholding & Promoter Pledge history over HTTPS in ~250ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("shareholding", "nse_shareholding_summary.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd ASC",
            [self._clean_sym(symbol)],
        ).df()

    def named_holders(self, symbol: str) -> pd.DataFrame:
        """Stream >1% institutional and named shareholders over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("shareholding", "named_shareholders.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd DESC, PctOfTotal DESC",
            [self._clean_sym(symbol)],
        ).df()

    def corporate_actions(self, symbol: str) -> pd.DataFrame:
        """Stream parsed corporate actions (splits, bonuses, dividends) over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("actions", "nse_corporate_actions.parquet"))
        df = self.con.execute(
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
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(IndexName) = ? ORDER BY Date ASC",
            [index_name.strip().upper()],
        ).df()

    def deals(self, symbol: str) -> pd.DataFrame:
        """Stream Bulk & Block deals for a symbol over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "bulk_block_deals.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Date DESC",
            [self._clean_sym(symbol)],
        ).df()

    def insider_trades(self, symbol: str) -> pd.DataFrame:
        """Stream SEBI PIT insider trading disclosures for a symbol over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "insider_trading_pit.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE symbol = ? ORDER BY date DESC",
            [self._clean_sym(symbol)],
        ).df()

    def annual_reports(self, symbol: str) -> pd.DataFrame:
        """Stream 20-year Annual Report PDF links over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "annual_reports.parquet"))
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Year DESC",
            [self._clean_sym(symbol)],
        ).df()

    def concalls(self, scrip_code_or_symbol: str) -> pd.DataFrame:
        """Stream Concall Transcripts, Investor Presentations & Credit Ratings over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "concalls_and_filings.parquet"))
        q = str(scrip_code_or_symbol).strip()
        if q.isdigit():
            return self.con.execute(
                f"SELECT * FROM read_parquet('{url}') WHERE ScripCode = ? ORDER BY FilingTimestamp DESC",
                [q],
            ).df()
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(CompanyName) LIKE ? ORDER BY FilingTimestamp DESC",
            [f"%{self._clean_sym(q)}%"],
        ).df()

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
            return self.con.execute(
                f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? AND CAST(Date AS DATE) = CAST(? AS DATE) ORDER BY ExpiryDate, StrikePrice, OptionType",
                [symbol.strip().upper(), str(trade_date)],
            ).df()
        return self.con.execute(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Date, ExpiryDate, StrikePrice",
            [symbol.strip().upper()],
        ).df()

    def sql(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute arbitrary analytical SQL over remote Parquet datasets via DuckDB."""
        if params:
            return self.con.execute(query, params).df()
        return self.con.execute(query).df()
