from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
import io
import json
import logging
import os
from pathlib import Path
import threading
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
        use_pack: bool = True,
    ):
        if not HAS_DUCKDB:
            raise RuntimeError("Install duckdb (`uv add duckdb`) for zero-disk HTTP Range Parquet queries.")
        self.repo = repo.strip("/")
        self.tag = tag
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        self.base_url = f"https://github.com/{self.repo}/releases/download/{tag}"
        self.con = duckdb.connect(":memory:")
        self._lock = threading.Lock()
        self.cache_dir = Path(cache_dir).expanduser().resolve() if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.use_pack = use_pack
        self._httpfs_loaded = False
        self._cdn_url_cache: dict[str, tuple[float, str]] = {}
        self._compacted_bounds_cache: dict[str, tuple[int, int] | None] | None = None
        self._dossier_cache: dict[str, dict[str, pd.DataFrame]] = {}

        # IMPORTANT: Do NOT set Authorization on _http_pool default headers, because
        # objects.githubusercontent.com (Azure CDN) returns 403 if Bearer auth is sent with ?sig= SAS URLs.
        self._http_pool = (
            httpx.Client(
                headers={"User-Agent": "bfinance-datalake/0.2.0"},
                follow_redirects=False,
                timeout=15.0,
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
        with self._lock:
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
                        "CREATE OR REPLACE SECRET gh_token (TYPE HTTP, SCOPE 'https://github.com', EXTRA_HTTP_HEADERS MAP {'Authorization': ?});",
                        [f"Bearer {self.token}"],
                    )
                except Exception:
                    pass

    def _asset_url(self, folder: str, filename: str) -> str:
        return f"{self.base_url}/{folder}__{filename}"

    def _resolve_cdn_url(self, gh_url: str) -> str:
        """Resolve GitHub Release 302 redirect to direct CDN URL using a pooled connection (cached 10 min)."""
        if self.repo == "user/bfinance-datalake":
            return gh_url
        now = time.monotonic()
        cached = self._cdn_url_cache.get(gh_url)
        if cached and (now - cached[0]) < 600.0:
            return cached[1]

        if self._http_pool is not None:
            try:
                gh_headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
                resp = self._http_pool.head(gh_url, headers=gh_headers)
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
            opener.open(req, timeout=15.0)
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
        for pfx in ("NSE:", "BSE:"):
            if s.startswith(pfx):
                s = s[len(pfx):].strip()
        for sfx in (".NS", ".BO", ".NSE", ".BSE"):
            if s.endswith(sfx):
                s = s[:-len(sfx)].strip()
        return s

    @staticmethod
    def _detect_exchange(symbol: str, default_exchange: str = "NSE") -> str:
        s = str(symbol).strip().upper()
        if s.endswith((".BO", ".BSE")) or s.startswith("BSE:"):
            return "BSE"
        if s.endswith((".NS", ".NSE")) or s.startswith("NSE:"):
            return "NSE"
        if s in ("SENSEX", "BANKEX", "SENSEX50", "^BSESN"):
            return "BSE"
        return default_exchange.upper()

    def _parse_symbol_and_exchange(self, symbol: str, exchange: str | None = None) -> tuple[str, str, bool]:
        """
        Returns `(clean_symbol, target_exchange, is_explicit_exchange)`.
        - 'RELIANCE'       -> ('RELIANCE', 'NSE', False)
        - 'RELIANCE.NS'    -> ('RELIANCE', 'NSE', True)
        - 'RELIANCE.BO'    -> ('RELIANCE', 'BSE', True)
        - '500325.BO'      -> ('500325',   'BSE', True)
        - 'BSE:500325'     -> ('500325',   'BSE', True)
        - '500325'         -> ('500325',   'BSE' if BSE-only or httpfs-only, else 'NSE', False)
        """
        raw = str(symbol).strip().upper()
        clean = self._clean_sym(raw)

        if exchange is not None:
            return clean, exchange.strip().upper(), True
        if raw.endswith((".BO", ".BSE")) or raw.startswith("BSE:"):
            return clean, "BSE", True
        if raw.endswith((".NS", ".NSE")) or raw.startswith("NSE:"):
            return clean, "NSE", True
        if clean in ("SENSEX", "BANKEX", "SENSEX50", "^BSESN"):
            return ("SENSEX" if clean == "^BSESN" else clean), "BSE", True

        if clean.isdigit() and len(clean) == 6:
            nse_idx = self._load_ticker_index("NSE") if self.use_pack else None
            if nse_idx and clean in nse_idx.get("bse_to_symbol", {}):
                return clean, "NSE", False
            return clean, "BSE", False

        if len(clean) == 12 and clean.startswith(("INE", "INF")):
            nse_idx = self._load_ticker_index("NSE") if self.use_pack else None
            if nse_idx and clean not in nse_idx.get("isin_to_symbol", {}):
                bse_idx = self._load_ticker_index("BSE") if self.use_pack else None
                if bse_idx and clean in bse_idx.get("isin_to_symbol", {}):
                    return clean, "BSE", False

        return clean, "NSE", False

    def _resolve_for_exchange(self, symbol: str, exchange: str) -> str:
        """
        Resolves any input (NSE Symbol, old alias, 6-digit BSE ScripCode, or ISIN)
        to the target key for `exchange` ('RELIANCE' for NSE, '500325' for BSE).
        """
        sym = self._clean_sym(symbol)
        exch = exchange.upper()
        idx_data = self._load_ticker_index(exch) or self._load_ticker_index("NSE")
        if not idx_data:
            return sym

        if exch == "NSE":
            return (
                idx_data.get("aliases", {}).get(sym)
                or idx_data.get("bse_to_symbol", {}).get(sym)
                or idx_data.get("isin_to_symbol", {}).get(sym)
                or sym
            )
        bse_idx = self._load_ticker_index("BSE") or {}
        if sym in bse_idx.get("tickers", {}):
            return sym
        if sym in bse_idx.get("aliases", {}):
            return bse_idx["aliases"][sym]
        if sym in bse_idx.get("isin_to_symbol", {}):
            return bse_idx["isin_to_symbol"][sym]
        nse_idx = self._load_ticker_index("NSE") or {}
        canon_nse = nse_idx.get("aliases", {}).get(sym, sym)
        meta = nse_idx.get("tickers", {}).get(canon_nse, {}).get("meta", {})
        if meta.get("bse_code"):
            return str(meta["bse_code"])
        return sym

    @staticmethod
    def _select_clause(columns: list[str] | None) -> str:
        if not columns:
            return "*"
        safe_cols = [f'"{c.replace(chr(34), "")}"' for c in columns]
        return ", ".join(safe_cols)

    def _load_ticker_index(self, exchange: str = "NSE") -> dict | None:
        """Fetch and cache `by_ticker__{exch}_index.json` once per client session."""
        with self._lock:
            exch = exchange.lower()
            if exch == "nse" and getattr(self, "_ticker_index_loaded", False):
                return getattr(self, "_ticker_index", None)
            cache_attr = f"_ticker_index_{exch}"
            loaded_attr = f"_ticker_index_loaded_{exch}"
            if getattr(self, loaded_attr, False):
                return getattr(self, cache_attr, None)

            setattr(self, loaded_attr, True)
            setattr(self, cache_attr, None)
            if exch == "nse":
                self._ticker_index_loaded = True
                self._ticker_index = None

            idx_url = self._resolve_cdn_url(f"{self.base_url}/by_ticker__{exch}_index.json")
            try:
                if self._http_pool is not None:
                    resp = self._http_pool.get(idx_url, follow_redirects=True)
                    if resp.status_code == 200:
                        data = resp.json()
                        setattr(self, cache_attr, data)
                        if exch == "nse":
                            self._ticker_index = data
                else:
                    headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
                    req = urllib.request.Request(idx_url, headers=headers)
                    with urllib.request.urlopen(req, timeout=5.0) as resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode("utf-8"))
                            setattr(self, cache_attr, data)
                            if exch == "nse":
                                self._ticker_index = data
            except Exception as exc:
                logger.debug("Per-ticker %s index not available, falling back to DuckDB httpfs: %s", exch, exc)
            return getattr(self, cache_attr, None)

    def _fetch_pack_slice(self, pack_filename: str, offset: int, length: int) -> pd.DataFrame:
        """Fetch a single ticker's exact byte range (`Range: bytes=start-end`) and parse as Parquet."""
        is_immutable_hist = "hist.pack" in pack_filename
        if self.cache_dir and is_immutable_hist:
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
        if self.cache_dir and is_immutable_hist and not df.empty:
            try:
                cache_file = self.cache_dir / f"{pack_filename}_{offset}_{length}.parquet"
                df.to_parquet(cache_file, compression="zstd", index=False)
            except Exception:
                pass
        return df

    def _resolve_remote_symbol(self, symbol_or_code: str, exchange: str = "NSE") -> str:
        """Resolve NSE symbol, old alias, BSE 6-digit code, or 12-char ISIN via `by_ticker__{exch}_index.json`."""
        s = self._clean_sym(symbol_or_code)
        idx_data = self._load_ticker_index(exchange)
        if not idx_data:
            return s
        if s in idx_data.get("aliases", {}):
            return idx_data["aliases"][s]
        if s.isdigit() and s in idx_data.get("bse_to_symbol", {}):
            return idx_data["bse_to_symbol"][s]
        if len(s) == 12 and s.startswith(("INE", "INF")) and s in idx_data.get("isin_to_symbol", {}):
            return idx_data["isin_to_symbol"][s]
        return s

    @staticmethod
    def _apply_pack_actions(df: pd.DataFrame, actions_list: list[list], auto_adjust: bool = False) -> pd.DataFrame:
        """Vectorized in-memory OHLCV adjustment using compact action tuples `[ex_date, div, split_mult]`."""
        import numpy as np
        if df.empty:
            return df
        out = df.sort_values("Date").reset_index(drop=True).copy()
        out["Date"] = pd.to_datetime(out["Date"]).dt.normalize()
        out["Dividends"] = 0.0
        out["Stock Splits"] = 0.0

        post_window_split = 1.0
        if actions_list:
            trading_dates = out["Date"].to_numpy()
            min_dt, max_dt = trading_dates[0], trading_dates[-1]
            for ex_str, div_val, split_val in actions_list:
                ex_dt = pd.Timestamp(ex_str).normalize().to_datetime64()
                if min_dt <= ex_dt <= max_dt:
                    idx = int(np.searchsorted(trading_dates, ex_dt, side="left"))
                    if 0 <= idx < len(out):
                        if div_val > 0:
                            out.loc[idx, "Dividends"] += float(div_val)
                        if split_val != 1.0:
                            cur_s = out.loc[idx, "Stock Splits"]
                            out.loc[idx, "Stock Splits"] = float(split_val) if cur_s == 0.0 else cur_s * float(split_val)
                elif ex_dt > max_dt and split_val != 1.0:
                    post_window_split /= float(split_val)

        split_mult = out["Stock Splits"].replace(0.0, 1.0).to_numpy(dtype=float)
        rev_cum_splits = np.cumprod(split_mult[::-1])[::-1]
        cum_split = np.ones(len(out), dtype=float)
        if len(out) > 1:
            cum_split[:-1] = 1.0 / rev_cum_splits[1:]
        cum_split *= post_window_split

        if "PrevClose" in out.columns and "TurnoverValue" in out.columns:
            is_already_adj = out["PrevClose"].isna() & out["TurnoverValue"].isna()
            cum_split = np.where(is_already_adj.to_numpy(), 1.0, cum_split)

        close_arr = out["Close"].to_numpy(dtype=float)
        div_arr = out["Dividends"].to_numpy(dtype=float)
        div_step = np.ones(len(out), dtype=float)
        if len(out) > 1:
            prev_close = close_arr[:-1]
            ex_divs = div_arr[1:]
            valid = (ex_divs > 0.0) & (prev_close > 0.0) & (ex_divs < prev_close)
            step = np.ones(len(out) - 1, dtype=float)
            step[valid] = 1.0 - (ex_divs[valid] / prev_close[valid])
            div_step[:-1] = np.cumprod(step[::-1])[::-1]

        total_factor = cum_split * div_step
        out["Adj Close"] = np.round(out["Close"] * total_factor, 4)
        if auto_adjust:
            for col in ("Open", "High", "Low", "Close"):
                if col in out.columns:
                    out[col] = np.round(out[col] * total_factor, 4)
            for vcol in ("Volume", "DeliverableQty"):
                if vcol in out.columns:
                    out[vcol] = (out[vcol] / cum_split).round()
            out["Dividends"] = np.round(out["Dividends"] * cum_split, 4)
        return out

    def _try_pack_history(
        self,
        sym: str,
        start_year: int,
        end_year: int,
        exchange: str,
        columns: list[str] | None,
        auto_adjust: bool,
    ) -> pd.DataFrame | None:
        exch = exchange.lower()
        idx_data = self._load_ticker_index(exch)
        if idx_data is None:
            return None
        canon_key = (
            idx_data.get("aliases", {}).get(sym)
            or idx_data.get("bse_to_symbol", {}).get(sym)
            or idx_data.get("isin_to_symbol", {}).get(sym)
            or sym
        )
        entry = idx_data.get("tickers", {}).get(canon_key)
        if entry is None:
            return None

        cur_yr = int(idx_data.get("current_year", pd.Timestamp.now().year))
        tasks: list[tuple[str, int, int]] = []
        if start_year < cur_yr and "hist" in entry:
            tasks.append((f"by_ticker__{exch}_hist.pack", entry["hist"][0], entry["hist"][1]))
        if end_year >= cur_yr and "curr" in entry:
            tasks.append((f"by_ticker__{exch}_curr.pack", entry["curr"][0], entry["curr"][1]))
        if not tasks:
            return pd.DataFrame()

        if len(tasks) == 1:
            frames = [self._fetch_pack_slice(*tasks[0])]
        else:
            with ThreadPoolExecutor(max_workers=2) as pool:
                frames = list(pool.map(lambda t: self._fetch_pack_slice(*t), tasks))

        # Apply current-year corporate action carry-back factor to hist.pack slice so 2025->2026 Adj Close is seamless
        if start_year < cur_yr and "hist" in entry and "hist_carry" in entry and frames:
            p_fac, s_fac = float(entry["hist_carry"][0]), float(entry["hist_carry"][1])
            h_df = frames[0].copy()
            if "Adj Close" in h_df.columns and p_fac != 1.0:
                h_df["Adj Close"] = (h_df["Adj Close"] * p_fac).round(4)
            if s_fac != 1.0:
                for vcol in ("Volume", "DeliverableQty"):
                    if vcol in h_df.columns:
                        h_df[vcol] = (h_df[vcol] / s_fac).round()
            frames[0] = h_df

        df = pd.concat(frames, ignore_index=True)
        if "actions" in entry and ("Adj Close" not in df.columns or df["Adj Close"].isna().all()):
            df = self._apply_pack_actions(df, entry.get("actions", []), auto_adjust=auto_adjust)
        elif auto_adjust and "Adj Close" in df.columns and "Close" in df.columns:
            ratio = (df["Adj Close"] / df["Close"].replace(0, float("nan"))).fillna(1.0)
            for c in ("Open", "High", "Low", "Close", "Last", "PrevClose"):
                if c in df.columns:
                    df[c] = (df[c] * ratio).round(4)
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

    def history(
        self,
        symbol: str,
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str | None = None,
        columns: list[str] | None = None,
        auto_adjust: bool = True,
        use_pack: bool | None = None,
    ) -> pd.DataFrame:
        """
        Stream a single stock's OHLCV + Delivery over HTTPS from NSE or BSE.
        - `use_pack=True` (default): Uses per-ticker `.pack` byte range (~120 ms), with auto-fallback to DuckDB `httpfs`.
        - `use_pack=False`: Queries compacted/yearly `.parquet` files directly via DuckDB `httpfs`.
        """
        sym, exch, is_explicit = self._parse_symbol_and_exchange(symbol, exchange)
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        should_use_pack = self.use_pack if use_pack is None else use_pack

        # 1. Fast Path: Per-Ticker Packfile (NSE or BSE)
        if should_use_pack:
            pack_df = self._try_pack_history(sym, start_year, end_year, exch, columns, auto_adjust)
            if pack_df is not None and not pack_df.empty:
                return pack_df
            # Auto-fallback to other exchange pack if not explicitly locked
            if not is_explicit:
                alt_exch = "BSE" if exch == "NSE" else "NSE"
                alt_df = self._try_pack_history(sym, start_year, end_year, alt_exch, columns, auto_adjust)
                if alt_df is not None and not alt_df.empty:
                    return alt_df
            if pack_df is not None:
                return pack_df

        # 2. DuckDB httpfs Path (Two-Tier Compacted or Yearly Parquet)
        df = self._httpfs_history(sym, start_year, end_year, exch, columns, auto_adjust)
        if df.empty and not is_explicit:
            alt_exch = "BSE" if exch == "NSE" else "NSE"
            df = self._httpfs_history(sym, start_year, end_year, alt_exch, columns, auto_adjust)
        return df

    def _httpfs_history(
        self,
        sym: str,
        start_year: int,
        end_year: int,
        exchange: str,
        columns: list[str] | None = None,
        auto_adjust: bool = False,
    ) -> pd.DataFrame:
        self._ensure_httpfs()
        exch = exchange.upper()
        target_key = self._resolve_for_exchange(sym, exch) if self.use_pack else sym
        sel = self._select_clause(columns if not auto_adjust else None)
        current_yr = pd.Timestamp.now().year

        if self.cache_dir and start_year < current_yr and columns is None and not auto_adjust:
            cache_file = self.cache_dir / f"{exch}_{target_key}_{start_year}_{min(end_year, current_yr - 1)}.parquet"
            if cache_file.exists():
                hist_df = pd.read_parquet(cache_file)
                if end_year >= current_yr:
                    cur_urls = self._equity_urls_for_range(exch, current_yr, end_year)
                    if cur_urls:
                        cur_sql = ", ".join(f"'{u}'" for u in cur_urls)
                        where_clause = "InstrumentId = ? OR Symbol = ?" if exch == "BSE" else "Symbol = ?"
                        params = [target_key, sym] if exch == "BSE" else [target_key]
                        cur_df = self._safe_query(
                            f"SELECT {sel} FROM read_parquet([{cur_sql}]) WHERE ({where_clause}) ORDER BY Date ASC",
                            params,
                        )
                        return pd.concat([hist_df, cur_df], ignore_index=True).sort_values("Date").reset_index(drop=True)
                return hist_df

        urls = self._equity_urls_for_range(exch, start_year, end_year)
        if not urls:
            return pd.DataFrame()
        url_list_sql = ", ".join(f"'{u}'" for u in urls)

        if exch == "BSE":
            query = f"""
                SELECT {sel}
                FROM read_parquet([{url_list_sql}])
                WHERE (InstrumentId = ? OR Symbol = ?)
                  AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
                ORDER BY Date ASC
            """
            df = self._safe_query(query, [target_key, sym, start_year, end_year])
        else:
            query = f"""
                SELECT {sel}
                FROM read_parquet([{url_list_sql}])
                WHERE Symbol = ?
                  AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
                ORDER BY Date ASC
            """
            df = self._safe_query(query, [target_key, start_year, end_year])
            # If empty on NSE and user passed a 6-digit BSE code, auto-fallback to BSE httpfs
            if df.empty and sym.isdigit():
                return self._httpfs_history(sym, start_year, end_year, "BSE", columns, auto_adjust)

        if self.cache_dir and not df.empty and start_year < current_yr and columns is None and not auto_adjust:
            hist_end = min(end_year, current_yr - 1)
            cache_file = self.cache_dir / f"{exch}_{target_key}_{start_year}_{hist_end}.parquet"
            if not cache_file.exists():
                hist_slice = df[pd.to_datetime(df["Date"]).dt.year <= hist_end]
                if not hist_slice.empty:
                    hist_slice.to_parquet(cache_file, compression="zstd", index=False)

        if columns and not df.empty:
            valid_cols = [c for c in columns if c in df.columns]
            if valid_cols:
                df = df[valid_cols]
        return df

    def download(
        self,
        symbols: list[str],
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str | None = None,
        columns: list[str] | None = None,
        use_pack: bool | None = None,
    ) -> pd.DataFrame:
        """Stream multiple symbols from NSE or BSE via Pack slices (for small lists) or vectorized DuckDB httpfs."""
        if not symbols:
            return pd.DataFrame()
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        if exchange is None:
            nse_syms = [s for s in symbols if self._parse_symbol_and_exchange(s, None)[1] == "NSE"]
            bse_syms = [s for s in symbols if self._parse_symbol_and_exchange(s, None)[1] == "BSE"]
            if nse_syms and bse_syms:
                f_nse = self.download(nse_syms, start_year, end_year, exchange="NSE", columns=columns, use_pack=use_pack)
                f_bse = self.download(bse_syms, start_year, end_year, exchange="BSE", columns=columns, use_pack=use_pack)
                return pd.concat([f for f in (f_nse, f_bse) if not f.empty], ignore_index=True)
            exch = "BSE" if bse_syms else "NSE"
        else:
            exch = exchange.strip().upper()

        should_use_pack = self.use_pack if use_pack is None else use_pack

        if should_use_pack and len(symbols) <= 15 and self._load_ticker_index(exch) is not None:
            with ThreadPoolExecutor(max_workers=min(8, len(symbols))) as pool:
                frames = list(
                    pool.map(
                        lambda s: self.history(s, start_year=start_year, end_year=end_year, exchange=exch, columns=columns, use_pack=True),
                        symbols,
                    )
                )
            non_empty = [f for f in frames if not f.empty]
            return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

        self._ensure_httpfs()
        clean_syms = [self._clean_sym(s) for s in symbols]
        urls = self._equity_urls_for_range(exch, start_year, end_year)
        if not urls:
            return pd.DataFrame()
        url_list_sql = ", ".join(f"'{u}'" for u in urls)
        placeholders = ", ".join(["?"] * len(clean_syms))
        sel = self._select_clause(columns)

        if exch == "BSE":
            return self._safe_query(
                f"""
                SELECT {sel}
                FROM read_parquet([{url_list_sql}])
                WHERE (InstrumentId IN ({placeholders}) OR Symbol IN ({placeholders}))
                  AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
                ORDER BY Symbol, Date ASC
                """,
                [*clean_syms, *clean_syms, start_year, end_year],
            )
        return self._safe_query(
            f"""
            SELECT {sel}
            FROM read_parquet([{url_list_sql}])
            WHERE Symbol IN ({placeholders})
              AND EXTRACT(YEAR FROM CAST(Date AS DATE)) BETWEEN ? AND ?
            ORDER BY Symbol, Date ASC
            """,
            [*clean_syms, start_year, end_year],
        )

    def _safe_query(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute a DuckDB SQL query, gracefully returning an empty DataFrame if remote asset is not found (HTTP 404)."""
        try:
            cursor = self.con.cursor()
            if params:
                return cursor.execute(query, params).df()
            return cursor.execute(query).df()
        except Exception as exc:
            err_msg = str(exc)
            if "404" in err_msg or "Not Found" in err_msg or "HTTP HEAD error" in err_msg:
                logger.debug("Remote asset not found on %s: %s", self.tag, exc)
                return pd.DataFrame()
            raise

    def dossier(self, symbol: str) -> dict[str, pd.DataFrame]:
        """Fetch Financials, Shareholding, Holders, Actions, Deals, Insider, Concalls & Annual Reports in 1 HTTP Range call."""
        sym = self._clean_sym(symbol)
        if sym in self._dossier_cache:
            return self._dossier_cache[sym]
        idx_data = self._load_ticker_index("NSE") or {}
        canon = (
            idx_data.get("aliases", {}).get(sym)
            or idx_data.get("bse_to_symbol", {}).get(sym)
            or idx_data.get("isin_to_symbol", {}).get(sym)
            or sym
        )
        if canon in self._dossier_cache:
            return self._dossier_cache[canon]

        loc = idx_data.get("tickers", {}).get(canon, {}).get("dossier")
        if not loc:
            bse_idx = self._load_ticker_index("BSE") or {}
            canon_bse = (
                bse_idx.get("aliases", {}).get(sym)
                or bse_idx.get("isin_to_symbol", {}).get(sym)
                or sym
            )
            loc = bse_idx.get("tickers", {}).get(canon_bse, {}).get("dossier")
            if not loc:
                return {}

        offset, length = loc
        cdn_url = self._resolve_cdn_url(f"{self.base_url}/by_ticker__nse_dossier.pack")
        if not cdn_url:
            return {}
        end_byte = offset + length - 1
        headers = {"Range": f"bytes={offset}-{end_byte}"}
        if self._http_pool is not None:
            resp = self._http_pool.get(cdn_url, headers=headers, follow_redirects=True)
            raw_bytes = resp.content
        else:
            req = urllib.request.Request(cdn_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15.0) as resp:
                raw_bytes = resp.read()
        payload = json.loads(zlib.decompress(raw_bytes).decode("utf-8"))
        res = {k: pd.DataFrame(v) for k, v in payload.items()}
        self._dossier_cache[canon] = res
        return res

    def financials(self, symbol: str, period_type: str = "Quarterly", use_pack: bool | None = None) -> pd.DataFrame:
        """Stream a company's Ind-AS XBRL Financials over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(symbol)
            if "financials" in dos:
                df = dos["financials"]
                if not df.empty and "PeriodType" in df.columns:
                    return df[df["PeriodType"].astype(str).str.lower() == period_type.lower()].reset_index(drop=True)
                return df
            if self._load_ticker_index() is not None:
                return pd.DataFrame()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("fundamentals", "xbrl_financials.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? AND PeriodType = ? ORDER BY PeriodEnd ASC",
            [self._clean_sym(symbol), period_type],
        )

    def shareholding(self, symbol: str, exchange: str | None = None, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream a company's Shareholding & Promoter Pledge history from NSE or BSE."""
        sym, exch, _ = self._parse_symbol_and_exchange(symbol, exchange)
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(sym)
            key = "shareholding_bse" if exch == "BSE" and "shareholding_bse" in dos else "shareholding"
            if key in dos:
                return dos[key].copy()
            if "shareholding_bse" in dos:
                return dos["shareholding_bse"].copy()
            if self._load_ticker_index() is not None:
                return pd.DataFrame()

        self._ensure_httpfs()
        if exch == "BSE":
            url = self._resolve_cdn_url(self._asset_url("shareholding", "bse_shareholding_history.parquet"))
            if not url:
                return pd.DataFrame()
            bse_key = self._resolve_for_exchange(sym, "BSE")
            return self._safe_query(
                f"SELECT * FROM read_parquet('{url}') WHERE (ScripCode = ? OR UPPER(Symbol) = ?) ORDER BY QuarterId ASC",
                [bse_key, sym],
            )
        url = self._resolve_cdn_url(self._asset_url("shareholding", "nse_shareholding_summary.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd ASC",
            [self._resolve_for_exchange(sym, "NSE")],
        )

    def named_holders(self, symbol: str, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream >1% institutional and named shareholders over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(symbol)
            if "named_holders" in dos:
                return dos["named_holders"].copy()
            if self._load_ticker_index() is not None:
                return pd.DataFrame()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("shareholding", "named_shareholders.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY QuarterEnd DESC, PctOfTotal DESC",
            [self._clean_sym(symbol)],
        )

    def corporate_actions(self, symbol: str, exchange: str | None = None, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream parsed corporate actions (splits, bonuses, dividends) from NSE or BSE."""
        sym, exch, _ = self._parse_symbol_and_exchange(symbol, exchange)
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(sym)
            key = "actions_bse" if exch == "BSE" and "actions_bse" in dos else "actions"
            if key in dos:
                df = dos[key].copy()
                if "ExDate" in df.columns:
                    df["ExDate"] = pd.to_datetime(df["ExDate"], errors="coerce")
                return df
            if "actions_bse" in dos:
                df = dos["actions_bse"].copy()
                if "ExDate" in df.columns:
                    df["ExDate"] = pd.to_datetime(df["ExDate"], errors="coerce")
                return df

        self._ensure_httpfs()
        if exch == "BSE":
            url = self._resolve_cdn_url(self._asset_url("actions", "bse_corporate_actions.parquet"))
            if not url:
                return pd.DataFrame()
            bse_key = self._resolve_for_exchange(sym, "BSE")
            return self._safe_query(
                f"SELECT * FROM read_parquet('{url}') WHERE (CAST(scrip_code AS VARCHAR) = ? OR UPPER(short_name) = ?) ORDER BY ExDate ASC",
                [bse_key, sym],
            )
        url = self._resolve_cdn_url(self._asset_url("actions", "nse_corporate_actions.parquet"))
        if not url:
            return pd.DataFrame()
        df = self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE symbol = ? ORDER BY exDate ASC",
            [self._resolve_for_exchange(sym, "NSE")],
        )
        if df.empty:
            return df
        if "ExDate_1" in df.columns and "ExDate" not in df.columns:
            df["ExDate"] = pd.to_datetime(df["ExDate_1"], errors="coerce")
        elif "ExDate" not in df.columns and "exDate" in df.columns:
            df["ExDate"] = pd.to_datetime(df["exDate"], errors="coerce")
        return df

    def index_history(self, index_name: str = "NIFTY 50") -> pd.DataFrame:
        """Stream 30-year Nifty index OHLC, TRI, P/E, P/B, and DivYield over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("indices", "nifty_30y_ohlc_tri_pe.parquet"))
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(IndexName) = ? ORDER BY Date ASC",
            [index_name.strip().upper()],
        )

    def deals(self, symbol: str, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream Bulk & Block deals for a symbol over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(symbol)
            if "deals" in dos:
                return dos["deals"].copy()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "bulk_block_deals.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Date DESC",
            [self._clean_sym(symbol)],
        )

    def insider_trades(self, symbol: str, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream SEBI PIT insider trading disclosures for a symbol over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(symbol)
            if "insider_trades" in dos:
                return dos["insider_trades"].copy()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("deals", "insider_trading_pit.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE symbol = ? ORDER BY date DESC",
            [self._clean_sym(symbol)],
        )

    def annual_reports(self, symbol: str, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream 20-year Annual Report PDF links over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(symbol)
            if "annual_reports" in dos:
                return dos["annual_reports"].copy()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "annual_reports.parquet"))
        if not url:
            return pd.DataFrame()
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE Symbol = ? ORDER BY Year DESC",
            [self._clean_sym(symbol)],
        )

    def concalls(self, scrip_code_or_symbol: str, use_pack: bool | None = None) -> pd.DataFrame:
        """Stream Concall Transcripts, Investor Presentations & Credit Ratings over HTTPS."""
        should_use_pack = self.use_pack if use_pack is None else use_pack
        if should_use_pack:
            dos = self.dossier(scrip_code_or_symbol)
            if "concalls" in dos:
                return dos["concalls"].copy()
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("documents", "concalls_and_filings.parquet"))
        if not url:
            return pd.DataFrame()
        q = str(scrip_code_or_symbol).strip()
        if q.isdigit():
            return self._safe_query(
                f"SELECT * FROM read_parquet('{url}') WHERE ScripCode = ? ORDER BY FilingTimestamp DESC",
                [q],
            )
        return self._safe_query(
            f"SELECT * FROM read_parquet('{url}') WHERE UPPER(CompanyName) LIKE ? ORDER BY FilingTimestamp DESC",
            [f"%{self._clean_sym(q)}%"],
        )

    def fii_dii_flows(self) -> pd.DataFrame:
        """Stream Cash Market FII/FPI and DII net inflows/outflows over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("flows", "cash_fii_dii_flows.parquet"))
        return self._safe_query(f"SELECT * FROM read_parquet('{url}') ORDER BY Date DESC")

    def participant_oi(self, year: int | None = None) -> pd.DataFrame:
        """Stream NSE F&O Participant Open Interest breakdown over HTTPS."""
        self._ensure_httpfs()
        yr = year or pd.Timestamp.now().year
        url = self._resolve_cdn_url(self._asset_url("flows", f"fao_participant_oi_{yr}.parquet"))
        return self._safe_query(f"SELECT * FROM read_parquet('{url}') ORDER BY Date DESC, \"Client Type\"")

    def screener(self, sql_filter: str | None = None) -> pd.DataFrame:
        """Query the latest cross-sectional market screener snapshot (~400 KB) in ~120 ms."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(f"{self.base_url}/snapshots__market_screener_latest.parquet")
        where = f"WHERE {sql_filter}" if sql_filter else ""
        return self._safe_query(f"SELECT * FROM read_parquet('{url}') {where}")

    def etfs(self) -> pd.DataFrame:
        """Stream the NSE ETF Master catalog (NIFTYBEES, GOLDBEES, LIQUIDBEES, etc.) over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("universe", "nse_etf_master.parquet"))
        return self._safe_query(f"SELECT * FROM read_parquet('{url}')")

    def option_chain(
        self,
        symbol: str,
        year: int | None = None,
        month: int | None = None,
        trade_date: str | date | None = None,
        exchange: str | None = None,
    ) -> pd.DataFrame:
        """Stream an EOD Option Chain from NSE or BSE (auto-selects BSE for SENSEX/BANKEX, defaults to latest trade date)."""
        self._ensure_httpfs()
        sym = self._clean_sym(symbol)
        exch = "bse" if (sym in ("SENSEX", "BANKEX") or (exchange and exchange.upper() == "BSE")) else "nse"
        if trade_date and year is None:
            year = pd.Timestamp(trade_date).year
        yr = year or pd.Timestamp.now().year
        url = self._resolve_cdn_url(self._asset_url("derivatives", f"{exch}_fo_{yr}.parquet"))
        if not url:
            return pd.DataFrame()
        if trade_date:
            return self._safe_query(
                f"""SELECT * FROM read_parquet('{url}')
                WHERE Symbol = ?
                  AND Date >= CAST(? AS TIMESTAMP)
                  AND Date < CAST(? AS TIMESTAMP) + INTERVAL 1 DAY
                ORDER BY ExpiryDate, StrikePrice, OptionType""",
                [sym, str(trade_date), str(trade_date)],
            )
        return self._safe_query(
            f"""
            WITH sym_fo AS (
                SELECT * FROM read_parquet('{url}') WHERE Symbol = ?
            )
            SELECT * FROM sym_fo
            WHERE Date = (SELECT MAX(Date) FROM sym_fo)
            ORDER BY ExpiryDate, StrikePrice, OptionType
            """,
            [sym],
        )

    def panel(
        self,
        symbols: list[str],
        field: str = "Adj Close",
        start_year: int = 2024,
        end_year: int = 2026,
        exchange: str | None = None,
    ) -> pd.DataFrame:
        """Stream a date-aligned 2D (Date x Symbol) matrix over HTTPS."""
        df = self.download(symbols, start_year=start_year, end_year=end_year, exchange=exchange)
        if df.empty:
            return pd.DataFrame()
        col = field if field in df.columns else "Close"
        df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
        return df.pivot_table(index="Date", columns="Symbol", values=col, aggfunc="last").sort_index()

    def continuous_futures(
        self,
        symbol: str = "NIFTY",
        contract: int = 1,
        start_year: int = 2024,
        end_year: int = 2026,
    ) -> pd.DataFrame:
        """Stream continuous front-month (1) or next-month (2) futures over HTTPS."""
        self._ensure_httpfs()
        raw_urls = [self._asset_url("derivatives", f"nse_fo_{yr}.parquet") for yr in range(start_year, end_year + 1)]
        urls = self._resolve_cdn_urls_parallel(raw_urls)
        url_sql = ", ".join(f"'{u}'" for u in urls)
        return self._safe_query(
            f"""
            WITH active_fut AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY CAST(Date AS DATE) ORDER BY CAST(ExpiryDate AS DATE) ASC) AS rk
                FROM read_parquet([{url_sql}])
                WHERE Symbol = ? AND OptionType = 'XX' AND CAST(ExpiryDate AS DATE) >= CAST(Date AS DATE)
            )
            SELECT * EXCLUDE (rk) FROM active_fut WHERE rk = ? ORDER BY Date ASC
            """,
            [symbol.strip().upper(), contract],
        )

    def sql(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute arbitrary analytical SQL over remote Parquet datasets via DuckDB."""
        return self._safe_query(query, params)

    def info(self, symbol: str) -> dict[str, Any]:
        """Return summary dict combining SecurityIdentity metadata, technicals, and valuation."""
        sym = symbol.strip().upper()
        out: dict[str, Any] = {"symbol": sym}
        try:
            scr = self.screener(f"Symbol = '{sym}'")
            if not scr.empty:
                row = scr.iloc[0]
                close_p = float(row["Close"]) if pd.notna(row.get("Close")) else None
                out.update({
                    "regularMarketPrice": close_p,
                    "fiftyDayAverage": float(row["SMA_50"]) if pd.notna(row.get("SMA_50")) else None,
                    "twoHundredDayAverage": float(row["SMA_200"]) if pd.notna(row.get("SMA_200")) else None,
                    "fiftyTwoWeekHigh": float(row["High_52W"]) if pd.notna(row.get("High_52W")) else None,
                    "fiftyTwoWeekLow": float(row["Low_52W"]) if pd.notna(row.get("Low_52W")) else None,
                    "volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                    "trailingPE": float(row["PE"]) if pd.notna(row.get("PE")) else None,
                    "trailingEps": float(row["BasicEPS_Rs"]) if pd.notna(row.get("BasicEPS_Rs")) else None,
                    "totalRevenueCr": float(row["Sales_Cr"]) if pd.notna(row.get("Sales_Cr")) else None,
                    "netIncomeCr": float(row["NetProfit_Cr"]) if pd.notna(row.get("NetProfit_Cr")) else None,
                    "returnOnEquityPct": float(row["ROE_Pct"]) if pd.notna(row.get("ROE_Pct")) else None,
                    "debtToEquity": float(row["DebtToEquity"]) if pd.notna(row.get("DebtToEquity")) else None,
                    "heldPercentPromoters": float(row["PromoterPct"]) if pd.notna(row.get("PromoterPct")) else None,
                    "promoterPledgedPct": float(row["PromoterPledgedPct"]) if pd.notna(row.get("PromoterPledgedPct")) else None,
                    "heldPercentFPI": float(row["FPI_Pct"]) if pd.notna(row.get("FPI_Pct")) else None,
                    "heldPercentMutualFunds": float(row["MutualFundsPct"]) if pd.notna(row.get("MutualFundsPct")) else None,
                })
        except Exception:
            pass
        return out

    def calendar(self, symbol: str | None = None) -> pd.DataFrame:
        """Return upcoming Board Meetings over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("actions", "upcoming_board_meetings.parquet"))
        where = f"WHERE Symbol = '{symbol.strip().upper()}'" if symbol else ""
        return self._safe_query(f"SELECT * FROM read_parquet('{url}') {where}")

    def fo_ban_list(self) -> pd.DataFrame:
        """Return current F&O security ban list over HTTPS."""
        self._ensure_httpfs()
        url = self._resolve_cdn_url(self._asset_url("derivatives", "fo_ban_list.parquet"))
        return self._safe_query(f"SELECT * FROM read_parquet('{url}')")
