from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Any
import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.storage import HAS_PARQUET, read_table_safe, write_parquet_atomic
from datalake.symbols import SymbolRenameResolver

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    duckdb = None  # type: ignore
    HAS_DUCKDB = False


@dataclass(frozen=True)
class SecurityIdentity:
    """Unified crosswalk identity linking NSE Symbol, BSE ScripCode, ISIN, and historical rename aliases."""
    symbol: str
    bse_code: str | None
    isin: str | None
    company_name: str | None
    aliases: tuple[str, ...]
    face_value: float | None = None
    industry: str | None = None
    listing_date: str | None = None
    primary_exchange: str = "NSE"


class UnifiedLakeEngine:
    """
    Ultra-low-latency unified query engine over all 15 bfinance-datalake Parquet datasets.
    Features:
      - Automatic crosswalk resolution: query by NSE Symbol ('RELIANCE'), Old Symbol ('INFOSYSTCH'),
        BSE Scrip Code ('500325'), or ISIN ('INE002A01018').
      - PyArrow row-group predicate pushdown (`filters=[...]`) so only matching byte ranges are read.
      - In-memory hot LRU cache for sub-millisecond repeated lookups.
      - Optional DuckDB zero-copy SQL engine (`engine.sql("SELECT ...")`) across all Parquet files.
    """

    def __init__(self, lake_dir: Path | str = "./data"):
        self.root = Path(lake_dir).resolve()
        self.adjuster = OHLCVAdjuster(self.root)
        self.resolver = self.adjuster.resolver
        self._by_symbol: dict[str, SecurityIdentity] = {}
        self._by_bse: dict[str, SecurityIdentity] = {}
        self._by_isin: dict[str, SecurityIdentity] = {}
        self._crosswalk_loaded = False
        self._duck: Any = None
        self._lock = threading.Lock()

    @classmethod
    def from_github(
        cls,
        repo: str,
        lake_dir: Path | str = "./data",
        auto_sync: bool = True,
        token: str | None = None,
    ) -> UnifiedLakeEngine:
        """
        Syncs only changed Parquet partitions from `github.com/{repo}/releases/download/datalake-latest`
        (takes ~1.5s for daily deltas, 0s if already synced) and returns a ready UnifiedLakeEngine.
        """
        if auto_sync:
            import asyncio
            from datalake.sync import pull_from_github_release

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    pool.submit(asyncio.run, pull_from_github_release(repo=repo, data_dir=lake_dir, token=token)).result()
            else:
                asyncio.run(pull_from_github_release(repo=repo, data_dir=lake_dir, token=token))
        return cls(lake_dir=lake_dir)

    def _ensure_crosswalk(self) -> None:
        if self._crosswalk_loaded:
            return
        self._crosswalk_loaded = True

        nse_path = self.root / "universe" / "nse_equity_master.parquet"
        bse_path = self.root / "universe" / "bse_scrip_master.parquet"
        pit_path = self.root / "universe" / "nifty_constituents_pit.parquet"

        sym_to_industry: dict[str, str] = {}
        if pit_path.exists():
            pdf = read_table_safe(pit_path)
            if "Symbol" in pdf.columns and "Industry" in pdf.columns:
                for r in pdf.dropna(subset=["Symbol", "Industry"]).to_dict("records"):
                    sym_to_industry[str(r["Symbol"]).strip().upper()] = str(r["Industry"]).strip()

        isin_to_bse: dict[str, tuple[str, str, str | None, float | None]] = {}
        bse_records: list[dict] = []
        if bse_path.exists():
            bdf = read_table_safe(bse_path)
            bse_records = bdf.to_dict("records")
            for r in bse_records:
                sc = str(r.get("SCRIP_CD") or "").strip()
                if sc.endswith(".0"):
                    sc = sc[:-2]
                isin = str(r.get("ISIN_NUMBER") or "").strip().upper()
                name = str(r.get("Scrip_Name") or r.get("scrip_id") or "").strip()
                ind = str(r.get("INDUSTRY") or "").strip() or None
                bse_fv = pd.to_numeric(r.get("FACE_VALUE") or r.get("FaceValue"), errors="coerce")
                if isin and isin not in ("", "NA", "NAN", "NONE") and sc:
                    isin_to_bse[isin] = (sc, name, ind, float(bse_fv) if pd.notna(bse_fv) else None)

        for nse_file in (
            "nse_equity_master.parquet",
            "nse_sme_master.parquet",
            "nse_etf_master.parquet",
            "nse_delisted_master.parquet",
        ):
            nse_p = self.root / "universe" / nse_file
            if not nse_p.exists():
                continue
            ndf = read_table_safe(nse_p)
            if ndf.empty:
                continue
            isin_col = next((c for c in ndf.columns if "ISIN" in c.upper()), None)
            name_col = next((c for c in ndf.columns if "NAME" in c.upper()), None)
            fv_col = next((c for c in ndf.columns if "FACE" in c.upper()), None)
            list_col = next(
                (c for c in ndf.columns if "LISTING" in c.upper() and "DELIST" not in c.upper()),
                None,
            )
            sym_col = next((c for c in ndf.columns if c.strip().upper() in ("SYMBOL", "SECURITY", "SECURITY SYMBOL")), "SYMBOL")
            for r in ndf.to_dict("records"):
                sym = str(r.get(sym_col) or "").strip().upper()
                if not sym:
                    continue
                isin = str(r.get(isin_col) or "").strip().upper() if isin_col else None
                if isin in ("", "NA", "NAN", "NONE"):
                    isin = None
                name = str(r.get(name_col) or sym).strip() if name_col else sym
                bse_info = isin_to_bse.get(isin, (None, None, None, None)) if isin else (None, None, None, None)
                fv_raw = pd.to_numeric(r.get(fv_col), errors="coerce") if fv_col else None
                aliases = tuple(self.resolver.all_aliases(sym))
                canon = aliases[0]
                ident = SecurityIdentity(
                    symbol=canon,
                    bse_code=bse_info[0],
                    isin=isin,
                    company_name=name,
                    aliases=aliases,
                    face_value=float(fv_raw) if pd.notna(fv_raw) else bse_info[3],
                    industry=sym_to_industry.get(canon) or bse_info[2],
                    listing_date=str(r.get(list_col) or "").strip() or None if list_col else None,
                )
                for al in aliases:
                    if al not in self._by_symbol:
                        self._by_symbol[al] = ident
                if bse_info[0] and bse_info[0] not in self._by_bse:
                    self._by_bse[bse_info[0]] = ident
                if isin and isin not in self._by_isin:
                    self._by_isin[isin] = ident

        for r in bse_records:
            sc = str(r.get("SCRIP_CD") or "").strip()
            if sc.endswith(".0"):
                sc = sc[:-2]
            if not sc or sc in self._by_bse:
                continue
            isin = str(r.get("ISIN_NUMBER") or "").strip().upper()
            if isin in ("", "NA", "NAN", "NONE"):
                isin = None
            sym = str(r.get("scrip_id") or r.get("Scrip_Name") or sc).strip().upper()
            name = str(r.get("Scrip_Name") or sym).strip()
            ind = str(r.get("INDUSTRY") or "").strip() or None
            bse_fv = pd.to_numeric(r.get("FACE_VALUE") or r.get("FaceValue"), errors="coerce")
            ident = SecurityIdentity(
                symbol=sym,
                bse_code=sc,
                isin=isin,
                company_name=name,
                aliases=(sym,),
                face_value=float(bse_fv) if pd.notna(bse_fv) else None,
                industry=ind,
                listing_date=None,
                primary_exchange="BSE",
            )
            self._by_bse[sc] = ident
            if sym not in self._by_symbol:
                self._by_symbol[sym] = ident
            if isin and isin not in self._by_isin:
                self._by_isin[isin] = ident

    @staticmethod
    def _strip_exchange_tag(query: str | int) -> tuple[str, str | None]:
        """Extract clean query and explicit exchange ('NSE', 'BSE', or None) from prefix/suffix."""
        q = str(query).strip().upper()
        if q.endswith(".BO") or q.endswith(".BSE"):
            return q.rsplit(".", 1)[0].strip(), "BSE"
        if q.endswith(".NS") or q.endswith(".NSE"):
            return q.rsplit(".", 1)[0].strip(), "NSE"
        if q.startswith("BSE:"):
            return q[4:].strip(), "BSE"
        if q.startswith("NSE:"):
            return q[4:].strip(), "NSE"
        if q in ("SENSEX", "BANKEX", "SENSEX50", "^BSESN"):
            return ("SENSEX" if q == "^BSESN" else q), "BSE"
        if q in ("^NSEI", "^NSEBANK", "^CNXIT"):
            idx_map = {"^NSEI": "NIFTY 50", "^NSEBANK": "NIFTY BANK", "^CNXIT": "NIFTY IT"}
            return idx_map[q], "NSE"
        return q, None

    def _infer_exchange(self, query: str | int, explicit_exchange: str | None = None) -> tuple[SecurityIdentity, str]:
        """Resolve identity and determine target exchange ('NSE' or 'BSE') automatically."""
        clean_q, tag_exch = self._strip_exchange_tag(query)
        ident = self.resolve(clean_q)
        if explicit_exchange:
            return ident, explicit_exchange.strip().upper()
        if tag_exch:
            return ident, tag_exch
        return ident, ident.primary_exchange

    def resolve(self, query: str | int) -> SecurityIdentity:
        """Resolve any NSE symbol, old renamed symbol, BSE 6-digit code, or 12-char ISIN into a unified identity."""
        self._ensure_crosswalk()
        q, tag_exch = self._strip_exchange_tag(query)

        if q in self._by_symbol:
            return self._by_symbol[q]
        if q in self._by_bse:
            return self._by_bse[q]
        if q in self._by_isin:
            return self._by_isin[q]

        aliases = tuple(self.resolver.all_aliases(q))
        if aliases[0] in self._by_symbol:
            return self._by_symbol[aliases[0]]
        is_bse_num = q.isdigit() and len(q) == 6
        return SecurityIdentity(
            symbol=aliases[0],
            bse_code=q if q.isdigit() else None,
            isin=q if len(q) == 12 and q.startswith(("INE", "INF")) else None,
            company_name=None,
            aliases=aliases,
            primary_exchange=tag_exch or ("BSE" if is_bse_num else "NSE"),
        )

    def ticker(self, query: str | int) -> LakeTicker:
        """Return a unified yfinance-style LakeTicker object (remembers .BO / .NS exchange preference)."""
        ident, pref_exch = self._infer_exchange(query)
        return LakeTicker(self, ident, default_exchange=pref_exch)

    def _read_filtered_parquet(self, path: Path, filters: list[tuple] | None = None) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        if HAS_PARQUET and filters:
            try:
                return pd.read_parquet(path, filters=filters)
            except Exception:
                # Retry with alternate int <-> str filter scalar if schema type differs
                try:
                    alt_filters = [
                        (c, op, int(v) if isinstance(v, str) and v.isdigit() else str(v))
                        if op == "==" else (c, op, v)
                        for c, op, v in filters
                    ]
                    return pd.read_parquet(path, filters=alt_filters)
                except Exception:
                    pass
        df = read_table_safe(path)
        if filters and not df.empty:
            for col, op, val in filters:
                if col not in df.columns:
                    continue
                if op == "==":
                    df = df[df[col].astype(str).str.strip() == str(val)]
                elif op == "in":
                    df = df[df[col].astype(str).str.strip().isin({str(v) for v in val})]
        return df

    _EQUITY_SERIES_PRIORITY = {"EQ": 0, "BE": 1, "BZ": 2, "SM": 3, "ST": 4, "BL": 5}

    @classmethod
    def _dedup_nse_equity_series(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "Series" not in df.columns:
            return df
        ser = df["Series"].astype(str).str.strip().str.upper()
        eq_mask = ser.isin(cls._EQUITY_SERIES_PRIORITY)
        out = df[eq_mask].copy() if eq_mask.any() else df.copy()
        out["_ser_prio"] = out["Series"].astype(str).str.strip().str.upper().map(
            lambda s: cls._EQUITY_SERIES_PRIORITY.get(s, 99)
        )
        return (
            out.sort_values(["Date", "_ser_prio"])
            .drop_duplicates(subset=["Date"], keep="first")
            .drop(columns=["_ser_prio"])
            .reset_index(drop=True)
        )

    @lru_cache(maxsize=256)
    def _cached_raw_equity(self, query_key: str, exchange: str, start_yr: int, end_yr: int) -> pd.DataFrame:
        ident = self.resolve(query_key)
        exch = exchange.upper()
        prefix = "nse_cm_" if exch == "NSE" else "bse_cm_"
        eq_dir = self.root / "equities"
        if not eq_dir.exists():
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for yr in range(start_yr, end_yr + 1):
            p = eq_dir / f"{prefix}{yr}.parquet"
            if not p.exists():
                continue
            if exch == "NSE":
                sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])
            else:
                if ident.bse_code:
                    sub = self._read_filtered_parquet(p, filters=[("InstrumentId", "==", ident.bse_code)])
                else:
                    sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])
            if not sub.empty:
                frames.append(sub)

        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        if exch == "NSE":
            out = self._dedup_nse_equity_series(out)
        return out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

    @staticmethod
    def _resolve_period_start(period: str | None, end_dt: pd.Timestamp) -> pd.Timestamp:
        if not period:
            return pd.Timestamp("1994-01-01")
        p = str(period).strip().lower()
        if p == "max":
            return pd.Timestamp("1994-01-01")
        if p == "ytd":
            return pd.Timestamp(year=end_dt.year, month=1, day=1)
        offsets = {
            "1d": pd.DateOffset(days=1),
            "5d": pd.DateOffset(days=7),
            "1mo": pd.DateOffset(months=1),
            "3mo": pd.DateOffset(months=3),
            "6mo": pd.DateOffset(months=6),
            "1y": pd.DateOffset(years=1),
            "2y": pd.DateOffset(years=2),
            "3y": pd.DateOffset(years=3),
            "5y": pd.DateOffset(years=5),
            "10y": pd.DateOffset(years=10),
        }
        if p in offsets:
            return (end_dt - offsets[p]).normalize()
        if p.endswith("y") and p[:-1].isdigit():
            return (end_dt - pd.DateOffset(years=int(p[:-1]))).normalize()
        if p.endswith("mo") and p[:-2].isdigit():
            return (end_dt - pd.DateOffset(months=int(p[:-2]))).normalize()
        if p.endswith("d") and p[:-1].isdigit():
            return (end_dt - pd.DateOffset(days=int(p[:-1]))).normalize()
        return pd.Timestamp("1994-01-01")

    def history(
        self,
        query: str | int,
        start: str | date | None = None,
        end: str | date | None = None,
        period: str | None = None,
        exchange: str | None = None,
        auto_adjust: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV + Delivery + Dividends + Stock Splits + Adj Close across 1995..2026
        in milliseconds (automatically stitching symbol renames like INFOSYSTCH -> INFY).
        """
        e_dt = pd.Timestamp(end).normalize() if end else pd.Timestamp.now().normalize()
        s_dt = pd.Timestamp(start).normalize() if start else self._resolve_period_start(period, e_dt)
        ident, exch = self._infer_exchange(query, exchange)
        load_end_yr = max(e_dt.year, pd.Timestamp.now().year)
        load_start_yr = s_dt.year if start else max(1990, s_dt.year - 1)

        lookup_key = (ident.bse_code or ident.symbol) if exch == "BSE" else ident.symbol
        raw = self._cached_raw_equity(lookup_key, exch, load_start_yr, load_end_yr)
        # Auto-fallback to the other exchange if no explicit suffix/exchange was forced
        if raw.empty and exchange is None and self._strip_exchange_tag(query)[1] is None:
            alt_exch = "BSE" if exch == "NSE" else "NSE"
            alt_key = (ident.bse_code or ident.symbol) if alt_exch == "BSE" else ident.symbol
            raw = self._cached_raw_equity(alt_key, alt_exch, load_start_yr, load_end_yr)
            if not raw.empty:
                exch = alt_exch

        if raw.empty:
            return pd.DataFrame()

        if end is None and start is None and period is not None and "Date" in raw.columns:
            max_raw_dt = pd.to_datetime(raw["Date"]).max()
            if pd.notna(max_raw_dt):
                e_dt = pd.Timestamp(max_raw_dt).normalize()
                s_dt = self._resolve_period_start(period, e_dt)

        key_for_actions = ident.symbol if exch == "NSE" else (ident.bse_code or ident.symbol)
        adj = self.adjuster.apply_adjustments(
            raw, key_for_actions, exchange=exch, auto_adjust=auto_adjust, include_future_splits=True
        )
        adj = adj[(adj["Date"] >= s_dt) & (adj["Date"] <= e_dt)].reset_index(drop=True)
        return adj

    def download(
        self,
        queries: list[str | int],
        start: str | date | None = None,
        end: str | date | None = None,
        period: str | None = None,
        exchange: str | None = None,
        auto_adjust: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Vectorized multi-symbol OHLCV download reading each annual Parquet file only once."""
        if not queries:
            return {}
        if exchange is None:
            nse_queries = [q for q in queries if self._infer_exchange(q, None)[1] == "NSE"]
            bse_queries = [q for q in queries if self._infer_exchange(q, None)[1] == "BSE"]
            if nse_queries and bse_queries:
                res_nse = self.download(nse_queries, start=start, end=end, period=period, exchange="NSE", auto_adjust=auto_adjust)
                res_bse = self.download(bse_queries, start=start, end=end, period=period, exchange="BSE", auto_adjust=auto_adjust)
                return {**res_nse, **res_bse}
            exch = "BSE" if bse_queries else "NSE"
        else:
            exch = exchange.strip().upper()

        e_dt = pd.Timestamp(end).normalize() if end else pd.Timestamp.now().normalize()
        s_dt = pd.Timestamp(start).normalize() if start else self._resolve_period_start(period, e_dt)
        load_end_yr = max(e_dt.year, pd.Timestamp.now().year)
        prefix = "nse_cm_" if exch == "NSE" else "bse_cm_"
        eq_dir = self.root / "equities"
        if not eq_dir.exists():
            return {}

        idents = [self.resolve(q) for q in queries]
        if exch == "BSE":
            alias_to_canon = {}
            for ident in idents:
                target_k = ident.bse_code or ident.symbol
                alias_to_canon[target_k] = target_k
                for al in ident.aliases:
                    alias_to_canon[al] = target_k
                if ident.bse_code:
                    alias_to_canon[ident.bse_code] = target_k
        else:
            alias_to_canon = {al: ident.symbol for ident in idents for al in ident.aliases}
        all_aliases = list(alias_to_canon.keys())

        frames = []
        for yr in range(s_dt.year, load_end_yr + 1):
            p = eq_dir / f"{prefix}{yr}.parquet"
            if p.exists():
                if exch == "BSE":
                    sub = self._read_filtered_parquet(p, filters=[("InstrumentId", "in", all_aliases)])
                    if sub.empty:
                        sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", all_aliases)])
                else:
                    sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", all_aliases)])
                if not sub.empty:
                    frames.append(sub)
        if not frames:
            return {}

        combined = pd.concat(frames, ignore_index=True)
        key_col = "InstrumentId" if (exch == "BSE" and "InstrumentId" in combined.columns) else "Symbol"
        combined["_CanonSymbol"] = combined[key_col].astype(str).str.strip().str.upper().map(alias_to_canon)
        out_map: dict[str, pd.DataFrame] = {}
        for canon_sym, grp in combined.groupby("_CanonSymbol"):
            grp = grp.drop(columns=["_CanonSymbol"])
            if exch == "NSE":
                grp = self._dedup_nse_equity_series(grp)
            grp = grp.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
            adj = self.adjuster.apply_adjustments(grp, str(canon_sym), exchange=exch, auto_adjust=auto_adjust, include_future_splits=True)
            out_map[str(canon_sym)] = adj[(adj["Date"] >= s_dt) & (adj["Date"] <= e_dt)].reset_index(drop=True)
        return out_map

    def fundamentals(
        self,
        query: str | int,
        period_type: str | None = None,
        consolidated: bool = True,
        as_of: str | date | None = None,
    ) -> pd.DataFrame:
        """Return Ind-AS XBRL Financial Statements (with optional Point-in-Time `as_of` filter to prevent look-ahead bias)."""
        ident = self.resolve(query)
        p = self.root / "fundamentals" / "xbrl_financials.parquet"
        df = self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])
        if df.empty:
            return df
        if period_type:
            df = df[df["PeriodType"].astype(str).str.lower() == period_type.lower()].copy()
        f_dt = (
            pd.to_datetime(df["FilingDate"], format="ISO8601", errors="coerce").fillna(
                pd.to_datetime(df["FilingDate"], format="mixed", dayfirst=True, errors="coerce")
            )
            if "FilingDate" in df.columns
            else pd.Series(pd.NaT, index=df.index)
        )
        if as_of is not None:
            as_of_dt = pd.Timestamp(as_of).normalize()
            pe_dt = pd.to_datetime(df["PeriodEnd"], format="ISO8601", errors="coerce").fillna(
                pd.to_datetime(df["PeriodEnd"], format="mixed", dayfirst=True, errors="coerce")
            )
            eff_filing_dt = f_dt.where(f_dt.notna(), pe_dt + pd.Timedelta(days=45))
            df = df[eff_filing_dt <= as_of_dt].copy()
            if df.empty:
                return df
            f_dt = f_dt.loc[df.index]
        df["_f_dt"] = f_dt.fillna(pd.Timestamp("1900-01-01"))
        if "NatureOfReport" in df.columns and not df.empty:
            is_cons = df["NatureOfReport"].astype(str).str.lower().str.contains("consolidated")
            df["_nat_prio"] = is_cons if consolidated else ~is_cons
            df = (
                df.sort_values(["PeriodEnd", "PeriodType", "_nat_prio", "_f_dt"])
                .drop_duplicates(subset=["PeriodType", "PeriodEnd"], keep="last")
                .drop(columns=["_nat_prio", "_f_dt"])
            )
        else:
            df = (
                df.sort_values(["PeriodEnd", "PeriodType", "_f_dt"])
                .drop_duplicates(subset=["PeriodType", "PeriodEnd"], keep="last")
                .drop(columns=["_f_dt"])
            )
        return df.sort_values("PeriodEnd").reset_index(drop=True)

    financials = fundamentals

    def shareholding(self, query: str | int) -> pd.DataFrame:
        """Return unified NSE + BSE quarterly Shareholding & Promoter Pledge history (2001–Present)."""
        ident = self.resolve(query)
        nse_p = self.root / "shareholding" / "nse_shareholding_summary.parquet"
        bse_p = self.root / "shareholding" / "bse_shareholding_history.parquet"

        nse_df = self._read_filtered_parquet(nse_p, filters=[("Symbol", "in", list(ident.aliases))])
        bse_df = (
            self._read_filtered_parquet(bse_p, filters=[("ScripCode", "==", ident.bse_code)])
            if ident.bse_code
            else pd.DataFrame()
        )
        if bse_df.empty:
            bse_df = self._read_filtered_parquet(bse_p, filters=[("Symbol", "in", list(ident.aliases))])
        frames = []
        if not nse_df.empty:
            ndf = nse_df.copy()
            ndf["_source_prio"] = 0
            frames.append(ndf)
        if not bse_df.empty:
            bdf = bse_df.copy()
            bdf["_source_prio"] = 1
            frames.append(bdf)

        if not frames:
            return pd.DataFrame()

        comb = pd.concat(frames, ignore_index=True)
        if "QuarterEnd" in comb.columns:
            norm_q = pd.to_datetime(comb["QuarterEnd"], format="mixed", errors="coerce").dt.to_period("Q").dt.end_time.dt.date
            comb["QuarterEnd"] = norm_q
            comb["_has_xbrl"] = comb["MutualFundsPct"].notna() if "MutualFundsPct" in comb.columns else False
            comb = comb.sort_values(["QuarterEnd", "_has_xbrl", "_source_prio"], ascending=[True, False, True])
            comb = comb.drop_duplicates(subset=["QuarterEnd"], keep="first")
            comb = comb.drop(columns=["_source_prio", "_has_xbrl"], errors="ignore")
            return comb.sort_values("QuarterEnd").reset_index(drop=True)

        return comb

    def named_holders(self, query: str | int) -> pd.DataFrame:
        """Return named institutional and >1% individual shareholders from XBRL filings."""
        ident = self.resolve(query)
        p = self.root / "shareholding" / "named_shareholders.parquet"
        return self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])

    def corporate_actions(self, query: str | int, exchange: str | None = None) -> pd.DataFrame:
        """Return parsed Dividends, Splits, and Bonuses for the security across NSE or BSE."""
        ident, exch = self._infer_exchange(query, exchange)
        if exch == "BSE" and ident.bse_code:
            bse_act = self.adjuster.get_symbol_actions(ident.bse_code, exchange="BSE")
            if not bse_act.empty:
                return bse_act
        nse_act = self.adjuster.get_symbol_actions(ident.symbol, exchange="NSE")
        if not nse_act.empty:
            return nse_act
        if ident.bse_code:
            return self.adjuster.get_symbol_actions(ident.bse_code, exchange="BSE")
        return pd.DataFrame()

    def option_chain(
        self,
        symbol: str = "NIFTY",
        trade_date: str | date | None = None,
        expiry: str | date | None = None,
        exchange: str | None = None,
    ) -> pd.DataFrame:
        """Return EOD Futures & Option Chain (CE/PE, Strike, SettlePrice, OI, ChangeInOI) in milliseconds."""
        sym, tag_exch = self._strip_exchange_tag(symbol)
        exch = (exchange or tag_exch or ("BSE" if sym in ("SENSEX", "BANKEX") else "NSE")).lower()
        fo_dir = self.root / "derivatives"
        if not fo_dir.exists():
            return pd.DataFrame()

        fo_aliases = [sym]
        if sym == "NIFTY":
            fo_aliases = ["NIFTY", "S&P CNX NIFTY", "CNX NIFTY"]
        elif sym == "BANKNIFTY":
            fo_aliases = ["BANKNIFTY", "CNX BANK", "BANK NIFTY"]
        elif sym in ("NIFTYIT", "CNXIT", "CNX IT"):
            fo_aliases = ["NIFTYIT", "CNX IT", "CNXIT"]
        elif sym in ("NIFTYMID50", "NFTYMCAP50"):
            fo_aliases = ["NIFTYMID50", "NFTYMCAP50"]
        else:
            try:
                fo_aliases = list(self.resolve(sym).aliases)
            except Exception:
                fo_aliases = [sym]

        if trade_date:
            dt = pd.Timestamp(trade_date).normalize()
            yearly = fo_dir / f"{exch}_fo_{dt.year}.parquet"
            monthly = fo_dir / f"{exch}_fo_{dt.year}_{dt.month:02d}.parquet"
            files = [yearly] if yearly.exists() else [monthly]
            filters = [("Symbol", "in", fo_aliases), ("Date", "==", dt)]
        else:
            files = sorted(fo_dir.glob(f"{exch}_fo_*.parquet"), key=lambda p: p.stem.split("_")[2:])[-1:]
            if not files or not files[0].exists():
                return pd.DataFrame()
            try:
                date_df = pd.read_parquet(files[0], columns=["Date"], filters=[("Symbol", "in", fo_aliases)])
                if date_df.empty:
                    return pd.DataFrame()
                max_dt = pd.to_datetime(date_df["Date"]).max()
                filters = [("Symbol", "in", fo_aliases), ("Date", "==", max_dt)]
            except Exception:
                filters = [("Symbol", "in", fo_aliases)]

        frames = [self._read_filtered_parquet(f, filters=filters) for f in files if f.exists()]
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        if df.empty:
            return df
        if "Date" in df.columns:
            dt_norm = pd.to_datetime(df["Date"]).dt.normalize()
            if trade_date:
                df = df[dt_norm == pd.Timestamp(trade_date).normalize()]
            else:
                df = df[dt_norm == dt_norm.max()]
        if expiry and "ExpiryDate" in df.columns:
            df = df[pd.to_datetime(df["ExpiryDate"]).dt.normalize() == pd.Timestamp(expiry).normalize()]

        if "UnderlyingPrice" in df.columns and df["UnderlyingPrice"].isna().any() and not df.empty:
            try:
                if sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
                    idx_name = "NIFTY 50" if sym == "NIFTY" else ("NIFTY BANK" if sym == "BANKNIFTY" else sym)
                    spot_df = self.index_history(idx_name)
                    if not spot_df.empty and "Close" in spot_df.columns:
                        spot_df["_dt"] = pd.to_datetime(spot_df["Date"]).dt.normalize()
                        df_dt = pd.to_datetime(df["Date"]).dt.normalize()
                        spot_map = dict(zip(spot_df["_dt"], spot_df["Close"]))
                        df["UnderlyingPrice"] = df["UnderlyingPrice"].fillna(df_dt.map(spot_map))
                else:
                    eq_df = self.history(sym, auto_adjust=False)
                    if not eq_df.empty and "Close" in eq_df.columns:
                        eq_df["_dt"] = pd.to_datetime(eq_df["Date"]).dt.normalize()
                        df_dt = pd.to_datetime(df["Date"]).dt.normalize()
                        eq_map = dict(zip(eq_df["_dt"], eq_df["Close"]))
                        df["UnderlyingPrice"] = df["UnderlyingPrice"].fillna(df_dt.map(eq_map))
            except Exception:
                pass

        return df.sort_values(["ExpiryDate", "StrikePrice", "OptionType"]).reset_index(drop=True)

    def index_history(
        self,
        index_name: str = "NIFTY 50",
        start: str | date | None = None,
        end: str | date | None = None,
    ) -> pd.DataFrame:
        """Return 30-year Nifty Index Spot OHLC + Total Return Index (TRI) + P/E, P/B, DivYield."""
        p_bulk = self.root / "indices" / "nifty_30y_ohlc_tri_pe.parquet"
        clean_idx = index_name.strip().upper()
        df = self._read_filtered_parquet(p_bulk, filters=[("IndexName", "==", clean_idx)])

        # Also check recent ind_close_all_*.parquet for either non-bulk indices or newer daily bars
        idx_dir = self.root / "indices"
        if idx_dir.exists():
            max_bulk_dt = pd.to_datetime(df["Date"]).max() if not df.empty else pd.Timestamp("1990-01-01")
            files = sorted(idx_dir.glob("ind_close_all_*.parquet"))
            if not df.empty:
                files = [f for f in files if f.stem.split("_")[-1].isdigit() and int(f.stem.split("_")[-1]) >= max_bulk_dt.year]
            snap_frames = []
            for f in files:
                idf = read_table_safe(f)
                if not idf.empty and "IndexName" in idf.columns:
                    sub = idf[idf["IndexName"].astype(str).str.strip().str.upper() == clean_idx].copy()
                    if not sub.empty:
                        sub["IndexName"] = clean_idx
                        snap_frames.append(sub)
            if snap_frames:
                snap_df = pd.concat(snap_frames, ignore_index=True)
                df = pd.concat([df, snap_df], ignore_index=True) if not df.empty else snap_df

        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values("Date").drop_duplicates(subset=["Date"], keep="first").reset_index(drop=True)

    def all_indices(self) -> pd.DataFrame:
        """
        Return the latest snapshot of all 139 NSE indices (`indices/all_indices_snapshot.parquet`)
        including Category, LastPrice, PE, PB, DivYield, Advances, Declines, and 30d/365d returns.
        """
        p = self.root / "indices" / "all_indices_snapshot.parquet"
        if not p.exists():
            return pd.DataFrame()
        df = read_table_safe(p)
        if df.empty or "Date" not in df.columns:
            return df
        max_dt = pd.to_datetime(df["Date"]).max()
        return df[pd.to_datetime(df["Date"]) == max_dt].reset_index(drop=True)

    def index_constituents(self, index_name: str = "NIFTY 50") -> pd.DataFrame:
        """
        Return live constituents and computed Free-Float Market Cap weights (`WeightPct`)
        for any of the 139 NSE indices from `indices/index_constituents_live.parquet`
        (falling back to `universe/nifty_constituents_pit.parquet`).
        """
        clean_idx = index_name.strip().upper()
        p_live = self.root / "indices" / "index_constituents_live.parquet"
        if p_live.exists():
            df = read_table_safe(p_live)
            if not df.empty and "IndexName" in df.columns:
                mask = (
                    (df["IndexName"].astype(str).str.strip().str.upper() == clean_idx)
                    | (df.get("IndexSymbol", pd.Series(dtype=str)).astype(str).str.strip().str.upper() == clean_idx)
                )
                sub = df[mask].copy()
                if not sub.empty:
                    if "Date" in sub.columns:
                        max_dt = pd.to_datetime(sub["Date"]).max()
                        sub = sub[pd.to_datetime(sub["Date"]) == max_dt].copy()
                    if "FFMC" in sub.columns:
                        ffmc = pd.to_numeric(sub["FFMC"], errors="coerce")
                        total_ffmc = ffmc.sum()
                        if total_ffmc and total_ffmc > 0:
                            sub["WeightPct"] = ((ffmc / total_ffmc) * 100.0).round(4)
                            sub = sub.sort_values("WeightPct", ascending=False)
                    return sub.reset_index(drop=True)
        p_pit = self.root / "universe" / "nifty_constituents_pit.parquet"
        if p_pit.exists():
            pdf = read_table_safe(p_pit)
            sub = pdf[pdf["IndexName"].astype(str).str.strip().str.upper() == clean_idx].copy()
            if not sub.empty and "SnapshotDate" in sub.columns:
                max_s = pd.to_datetime(sub["SnapshotDate"]).max()
                sub = sub[pd.to_datetime(sub["SnapshotDate"]) == max_s]
            return sub.reset_index(drop=True)
        return pd.DataFrame()

    def trading_days(
        self,
        start: str | date = "1995-11-03",
        end: str | date | None = None,
    ) -> pd.DatetimeIndex:
        """Return authoritative NSE trading session dates (derived from NIFTY 50 index history)."""
        idx_df = self.index_history("NIFTY 50", start=start, end=end)
        if not idx_df.empty and "Date" in idx_df.columns:
            return pd.DatetimeIndex(pd.to_datetime(idx_df["Date"]).dt.normalize().unique()).sort_values()
        return pd.DatetimeIndex([])

    def info(self, query: str | int) -> dict[str, Any]:
        """
        Return a yfinance-compatible summary dictionary combining SecurityIdentity metadata,
        latest price/52W technicals, TTM valuation multiples, and shareholding breakdown.
        """
        ident = self.resolve(query)
        out: dict[str, Any] = {
            "symbol": ident.symbol,
            "bseCode": ident.bse_code,
            "isin": ident.isin,
            "longName": ident.company_name or ident.symbol,
            "industry": ident.industry,
            "faceValue": ident.face_value,
            "listingDate": ident.listing_date,
            "exchange": ident.primary_exchange,
        }
        try:
            scr = self.screener(f"Symbol = '{ident.symbol}'")
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

    def factor_returns(
        self,
        start: str | date = "2005-01-01",
        end: str | date | None = None,
    ) -> pd.DataFrame:
        """
        Compute daily Indian institutional risk factor returns from the 30Y Indices table:
        - `MKT`: NIFTY 500 (or NIFTY 50) TRI daily return
        - `RF`: Daily risk-free return (from NIFTY 1D RATE INDEX, fallback 6.6% p.a.)
        - `MKT_RF`: Excess market return (MKT - RF)
        - `SMB`: Size factor (0.5 * (Midcap 150 + Smallcap 250) - Nifty 50)
        - Sector excess returns (`BANK`, `IT`, `PHARMA`, `AUTO`, `FMCG`, `METAL`, `ENERGY`, `REALTY`)
        - `INDIA_VIX`: Implied volatility level
        """
        p_bulk = self.root / "indices" / "nifty_30y_ohlc_tri_pe.parquet"
        if not p_bulk.exists():
            return pd.DataFrame()
        df = read_table_safe(p_bulk)
        if df.empty:
            return pd.DataFrame()

        df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]

        # Prefer TRI where available, falling back to Close
        df["_Val"] = df["TRI"].where(df["TRI"].notna(), df["Close"]) if "TRI" in df.columns else df["Close"]
        piv = df.pivot_table(index="Date", columns="IndexName", values="_Val", aggfunc="last").sort_index()
        rets = piv.pct_change(fill_method=None)

        out = pd.DataFrame(index=rets.index)
        mkt_col = "NIFTY 500" if "NIFTY 500" in rets.columns else "NIFTY 50"
        if mkt_col not in rets.columns:
            return pd.DataFrame()

        out["MKT"] = rets[mkt_col]
        if "NIFTY 1D RATE INDEX" in rets.columns:
            out["RF"] = rets["NIFTY 1D RATE INDEX"].fillna((1.0 + 0.066) ** (1.0 / 252.0) - 1.0)
        else:
            out["RF"] = (1.0 + 0.066) ** (1.0 / 252.0) - 1.0
        out["MKT_RF"] = out["MKT"] - out["RF"]

        if "NIFTY 50" in rets.columns and "NIFTY MIDCAP 150" in rets.columns:
            sm_leg = (
                0.5 * (rets["NIFTY MIDCAP 150"] + rets["NIFTY SMALLCAP 250"])
                if "NIFTY SMALLCAP 250" in rets.columns
                else rets["NIFTY MIDCAP 150"]
            )
            out["SMB"] = sm_leg - rets["NIFTY 50"]

        for sec in ("BANK", "IT", "PHARMA", "AUTO", "FMCG", "METAL", "ENERGY", "REALTY"):
            col = f"NIFTY {sec}"
            if col in rets.columns:
                out[f"SEC_{sec}"] = rets[col] - out["MKT"]

        if "INDIA VIX" in piv.columns:
            out["INDIA_VIX"] = piv["INDIA VIX"]

        return out.dropna(subset=["MKT"]).round(6)

    def calendar(self, query: str | int | None = None) -> pd.DataFrame:
        """Return upcoming Board Meetings (earnings/dividends) and future Ex-Dates."""
        p = self.root / "actions" / "upcoming_board_meetings.parquet"
        if not p.exists():
            return pd.DataFrame()
        if query is None:
            return read_table_safe(p)
        ident = self.resolve(query)
        return self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])

    def fo_ban_list(self) -> pd.DataFrame:
        """Return current F&O security ban list."""
        p = self.root / "derivatives" / "fo_ban_list.parquet"
        if not p.exists():
            return pd.DataFrame()
        return read_table_safe(p)

    def dossier(self, query: str | int) -> dict[str, pd.DataFrame]:
        """Return 360-degree company dossier combining all 8 non-price datasets for a symbol."""
        ident = self.resolve(query)
        sym = ident.symbol
        bse = ident.bse_code

        res: dict[str, pd.DataFrame] = {}
        for key, func, arg in (
            ("financials", self.financials, sym),
            ("shareholding", self.shareholding, sym),
            ("named_holders", self.named_holders, sym),
            ("actions", self.corporate_actions, sym),
            ("deals", self.deals, sym),
            ("short_selling", self.short_selling, sym),
            ("insider_trades", self.insider_trades, sym),
            ("annual_reports", self.annual_reports, sym),
            ("concalls", self.concalls, bse or sym),
        ):
            try:
                table = func(arg)
                if not table.empty:
                    res[key] = table
            except Exception:
                pass
        return res

    def _compute_live_screener_df(self) -> pd.DataFrame:
        eq_dir = self.root / "equities"
        if not eq_dir.exists() or not list(eq_dir.glob("nse_cm_*.parquet")):
            return pd.DataFrame()
        has_fin = (self.root / "fundamentals" / "xbrl_financials.parquet").exists()
        has_shp = (self.root / "shareholding" / "nse_shareholding_summary.parquet").exists()
        has_shp_bse = (self.root / "shareholding" / "bse_shareholding_history.parquet").exists()

        fin_cols_available = set()
        if has_fin:
            try:
                cols_df = self.sql("DESCRIBE financials")
                fin_cols_available = set(cols_df["column_name"].tolist())
            except Exception:
                pass

        def _fin_col(name: str) -> str:
            return name if name in fin_cols_available else f"CAST(NULL AS DOUBLE) AS {name}"

        fin_cte = f"""
            , dedup_fin AS (
                SELECT Symbol, PeriodEnd, Sales_Cr, PBT_Cr, NetProfit_Cr, BasicEPS_Rs,
                    {_fin_col("Interest_Cr")}, {_fin_col("Depreciation_Cr")}, {_fin_col("OtherIncome_Cr")},
                    {_fin_col("EquityCapital_Cr")}, {_fin_col("Reserves_Cr")}, {_fin_col("TotalBorrowings_Cr")}, {_fin_col("TotalCashAndBank_Cr")},
                    ROW_NUMBER() OVER (
                        PARTITION BY Symbol, PeriodEnd
                        ORDER BY CASE WHEN LOWER(CAST(NatureOfReport AS VARCHAR)) LIKE '%consolidated%' THEN 1 ELSE 0 END DESC
                    ) AS fn_rn
                FROM financials WHERE PeriodType = 'Quarterly'
            ),
            ttm_fin AS (
                SELECT Symbol, PeriodEnd AS FinQuarter,
                    COUNT(Sales_Cr) OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS q_cnt,
                    SUM(Sales_Cr) OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS Sales_Cr,
                    SUM(PBT_Cr) OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS PBT_Cr,
                    SUM(NetProfit_Cr) OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS NetProfit_Cr,
                    SUM(BasicEPS_Rs) OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS BasicEPS_Rs,
                    SUM( COALESCE(PBT_Cr, 0) + COALESCE(Interest_Cr, 0) + COALESCE(Depreciation_Cr, 0) - COALESCE(OtherIncome_Cr, 0) )
                        OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS EBITDA_Cr,
                    SUM( COALESCE(PBT_Cr, 0) + COALESCE(Interest_Cr, 0) )
                        OVER (PARTITION BY Symbol ORDER BY PeriodEnd ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS EBIT_Cr,
                    (COALESCE(EquityCapital_Cr, 0) + COALESCE(Reserves_Cr, 0)) AS BookValue_Cr,
                    COALESCE(TotalBorrowings_Cr, 0) AS TotalBorrowings_Cr,
                    COALESCE(TotalCashAndBank_Cr, 0) AS TotalCashAndBank_Cr,
                    ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY PeriodEnd DESC) AS rn
                FROM dedup_fin WHERE fn_rn = 1
            ),
            latest_fin AS (
                SELECT *,
                    CASE WHEN BookValue_Cr > 0 THEN ROUND(((NetProfit_Cr * (CASE WHEN q_cnt BETWEEN 1 AND 3 THEN 4.0 / q_cnt ELSE 1.0 END)) / BookValue_Cr) * 100.0, 2) ELSE NULL END AS ROE_Pct,
                    CASE WHEN (BookValue_Cr + TotalBorrowings_Cr) > 0 THEN ROUND(((EBIT_Cr * (CASE WHEN q_cnt BETWEEN 1 AND 3 THEN 4.0 / q_cnt ELSE 1.0 END)) / (BookValue_Cr + TotalBorrowings_Cr)) * 100.0, 2) ELSE NULL END AS ROCE_Pct,
                    CASE WHEN BookValue_Cr > 0 THEN ROUND(TotalBorrowings_Cr / BookValue_Cr, 2) ELSE NULL END AS DebtToEquity
                FROM ttm_fin WHERE rn = 1
            )
        """ if has_fin else ""

        shp_cols_available = set()
        if has_shp:
            try:
                cols_df = self.sql("DESCRIBE shareholding")
                shp_cols_available = set(cols_df["column_name"].tolist())
            except Exception:
                pass

        def _shp_col(name: str) -> str:
            return name if name in shp_cols_available else f"CAST(NULL AS DOUBLE) AS {name}"

        bse_shares_cte = """
            , latest_bse_shares AS (
                SELECT Symbol, TotalShares AS BSE_TotalShares
                FROM (
                    SELECT Symbol, TotalShares,
                           ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY QuarterId DESC) AS rn
                    FROM shareholding_bse
                    WHERE TotalShares IS NOT NULL AND TotalShares > 0
                ) WHERE rn = 1
            )
        """ if has_shp_bse else ""

        bse_sh_join = "LEFT JOIN latest_bse_shares bs USING (Symbol)" if has_shp_bse else ""
        if "TotalShares" in shp_cols_available:
            tot_sh_expr = "COALESCE(s.TotalShares, bs.BSE_TotalShares) AS TotalShares" if has_shp_bse else "s.TotalShares"
        else:
            tot_sh_expr = "bs.BSE_TotalShares AS TotalShares" if has_shp_bse else "CAST(NULL AS DOUBLE) AS TotalShares"

        shp_cte = f"""
            {bse_shares_cte}
            , latest_shp AS (
                SELECT s.Symbol, s.QuarterEnd AS ShpQuarter, s.PromoterPct, s.PromoterPledgedPct, s.MutualFundsPct, s.FPI_Pct, s.RetailPct,
                    {tot_sh_expr}
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY QuarterEnd DESC) AS rn
                    FROM shareholding
                ) s {bse_sh_join} WHERE s.rn = 1
            )
        """ if has_shp else ""

        fin_cols = """
            , f.FinQuarter, f.Sales_Cr, f.EBITDA_Cr, f.PBT_Cr, f.NetProfit_Cr, f.BasicEPS_Rs,
            f.BookValue_Cr, f.TotalBorrowings_Cr, f.TotalCashAndBank_Cr, f.ROE_Pct, f.ROCE_Pct, f.DebtToEquity,
            CASE WHEN f.BasicEPS_Rs > 0 THEN ROUND(e.Close / f.BasicEPS_Rs, 2) ELSE NULL END AS PE
        """ if has_fin else ""

        shp_cols = """
            , s.ShpQuarter, s.PromoterPct, s.PromoterPledgedPct, s.MutualFundsPct, s.FPI_Pct, s.RetailPct, s.TotalShares,
            CASE WHEN s.TotalShares > 0 THEN ROUND((e.Close * s.TotalShares) / 10000000.0, 2) ELSE NULL END AS MarketCap_Cr
        """ if has_shp else ""

        val_cols = """
            , CASE WHEN f.BookValue_Cr > 0 AND s.TotalShares > 0 THEN ROUND(((e.Close * s.TotalShares) / 10000000.0) / f.BookValue_Cr, 2) ELSE NULL END AS PB
            , CASE WHEN f.EBITDA_Cr > 0 AND s.TotalShares > 0 THEN ROUND((((e.Close * s.TotalShares) / 10000000.0) + COALESCE(f.TotalBorrowings_Cr, 0) - COALESCE(f.TotalCashAndBank_Cr, 0)) / f.EBITDA_Cr, 2) ELSE NULL END AS EV_EBITDA
        """ if (has_fin and has_shp) else ""

        fin_join = "LEFT JOIN latest_fin f USING (Symbol)" if has_fin else ""
        shp_join = "LEFT JOIN latest_shp s USING (Symbol)" if has_shp else ""

        return self.sql(f"""
            WITH dedup_eq AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Symbol, Date
                        ORDER BY CASE Series WHEN 'EQ' THEN 0 WHEN 'BE' THEN 1 WHEN 'BZ' THEN 2 WHEN 'SM' THEN 3 WHEN 'ST' THEN 4 ELSE 9 END
                    ) AS sr_rn
                FROM equities_nse
            ),
            recent_eq AS (
                SELECT
                    Symbol, Date, Series, Close, Volume,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS SMA_50,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS SMA_200,
                    MAX(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS High_52W,
                    MIN(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS Low_52W,
                    ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date DESC) AS rn
                FROM dedup_eq WHERE sr_rn = 1
            ),
            latest_eq AS (SELECT * EXCLUDE (rn) FROM recent_eq WHERE rn = 1)
            {fin_cte}
            {shp_cte}
            SELECT e.* {fin_cols} {shp_cols} {val_cols}
            FROM latest_eq e
            {fin_join}
            {shp_join}
            ORDER BY e.Volume DESC NULLS LAST
        """)

    def screener(self, sql_filter: str | None = None) -> pd.DataFrame:
        """Query latest market screener snapshot locally or compute cross-sectional view on the fly."""
        snap_path = self.root / "snapshots" / "market_screener_latest.parquet"
        where = f"WHERE {sql_filter}" if sql_filter else ""
        if snap_path.exists():
            return self.sql(f"SELECT * FROM read_parquet('{snap_path.as_posix()}') {where}")
        base_df = self._compute_live_screener_df()
        if base_df.empty or not sql_filter:
            return base_df
        if HAS_DUCKDB and duckdb is not None:
            con = duckdb.connect(":memory:")
            con.register("_snap", base_df)
            return con.execute(f"SELECT * FROM _snap {where}").df()
        return base_df

    def run_screen(self, strategy: str = "coffee_can") -> pd.DataFrame:
        """
        Execute a pre-built institutional screen (`coffee_can`, `magic_formula`, `debt_free`,
        `undervalued_growth`, `institutional_accumulation`) compatible with `bfinance.screens`
        and `finengine`'s `ScreenerService`.
        """
        from datalake.finengine_bridge import INSTITUTIONAL_SCREENS

        key = strategy.strip().lower()
        spec = INSTITUTIONAL_SCREENS.get(key)
        if not spec:
            raise ValueError(f"Unknown screen strategy '{strategy}'. Available: {list(INSTITUTIONAL_SCREENS.keys())}")
        base = self.screener(spec["sql_filter"])
        if base.empty:
            return base
        order_by = spec.get("order_by")
        if order_by and HAS_DUCKDB and duckdb is not None:
            con = duckdb.connect(":memory:")
            con.register("_scr_sub", base)
            return con.execute(f"SELECT * FROM _scr_sub ORDER BY {order_by}").df()
        return base

    def custom_ratios(self, query: str | int, as_of: str | date | None = None) -> dict[str, Any]:
        """
        Return `bfinance` & `finengine` forensic ratios (Piotroski F-Score, Graham Number,
        EV/EBITDA, Interest Coverage, CFO/PAT) adhering to the strict honesty contract.
        """
        from datalake.finengine_bridge import compute_forensic_ratios

        ident = self.resolve(query)
        fin_df = self.fundamentals(ident.symbol, consolidated=True, as_of=as_of)
        scr_row = None
        try:
            scr = self.screener(f"Symbol = '{ident.symbol}'")
            if not scr.empty:
                scr_row = scr.iloc[0].to_dict()
        except Exception:
            pass
        current_price = scr_row.get("Close") if scr_row else None
        market_cap_cr = scr_row.get("MarketCap_Cr") if scr_row else None
        total_shares = scr_row.get("TotalShares") if scr_row else None
        if current_price is None:
            hist = self.history(ident.symbol, period="1mo")
            if not hist.empty and "Close" in hist.columns:
                current_price = float(hist.iloc[-1]["Close"])
        if total_shares is None:
            shp = self.shareholding(ident.symbol)
            if not shp.empty and "TotalShares" in shp.columns:
                valid_sh = shp["TotalShares"].dropna()
                if not valid_sh.empty:
                    total_shares = float(valid_sh.iloc[-1])
        return compute_forensic_ratios(
            fin_df,
            current_price=current_price,
            market_cap_cr=market_cap_cr,
            total_shares=total_shares,
        )

    def shareholding_trends(self, query: str | int) -> dict[str, Any]:
        """Return quarterly and yearly shareholding tables + chart_series for FinEngine EquityResearchService."""
        from datalake.finengine_bridge import format_shareholding_for_finengine

        ident = self.resolve(query)
        shp_df = self.shareholding(ident.symbol)
        return format_shareholding_for_finengine(shp_df, ident.symbol)

    def research_profile(self, query: str | int) -> dict[str, Any]:
        """
        Return a complete 360-degree Equity Research payload matching `finengine`'s
        `EquityResearchService.get_full_profile(ticker)` and `bfinance`'s `CompanyProfile`.
        """
        from datalake.finengine_bridge import build_research_profile

        ident = self.resolve(query)
        fin_df = self.fundamentals(ident.symbol, consolidated=True)
        shp_df = self.shareholding(ident.symbol)
        concalls_df = self.concalls(ident.bse_code or ident.symbol)
        nh_df = self.named_holders(ident.symbol)
        ar_df = self.annual_reports(ident.symbol)
        scr_row = None
        try:
            scr = self.screener(f"Symbol = '{ident.symbol}'")
            if not scr.empty:
                scr_row = scr.iloc[0].to_dict()
        except Exception:
            pass
        if (not scr_row or scr_row.get("Close") is None):
            hist = self.history(ident.symbol, period="1y")
            if not hist.empty and "Close" in hist.columns:
                scr_row = dict(scr_row or {})
                scr_row["Close"] = float(hist.iloc[-1]["Close"])
                scr_row["High_52W"] = float(hist["Close"].max())
                scr_row["Low_52W"] = float(hist["Close"].min())
        return build_research_profile(
            symbol=ident.symbol,
            company_name=ident.company_name,
            bse_code=ident.bse_code,
            isin=ident.isin,
            industry=ident.industry,
            face_value=ident.face_value,
            screener_row=scr_row,
            fin_df=fin_df,
            shp_df=shp_df,
            concalls_df=concalls_df,
            named_holders_df=nh_df,
            annual_reports_df=ar_df,
        )

    def bhavcopy_records(self, trade_date: str | date | None = None, exchange: str = "NSE") -> list[dict[str, Any]]:
        """
        Return standardized `finengine` `IndiaDataService.get_bhavcopy` records for a session date
        (or latest session if `trade_date` is omitted).
        """
        from datalake.finengine_bridge import format_bhavcopy_records

        exch = exchange.strip().upper()
        prefix = "nse_cm_" if exch == "NSE" else "bse_cm_"
        eq_dir = self.root / "equities"
        if not eq_dir.exists():
            return []
        if trade_date:
            dt = pd.Timestamp(trade_date).normalize()
            pq = eq_dir / f"{prefix}{dt.year}.parquet"
            if not pq.exists():
                return []
            df = self._read_filtered_parquet(pq, filters=[("Date", "==", dt)])
        else:
            files = sorted(eq_dir.glob(f"{prefix}*.parquet"))
            if not files:
                return []
            df = read_table_safe(files[-1])
            if not df.empty and "Date" in df.columns:
                max_dt = pd.to_datetime(df["Date"]).max()
                df = df[pd.to_datetime(df["Date"]) == max_dt]
        if df.empty:
            return []
        if exch == "NSE":
            df = self._dedup_nse_equity_series(df)
        return format_bhavcopy_records(df)

    def delivery_anomalies(
        self,
        symbols: list[str] | None = None,
        lookback_sessions: int = 30,
        z_threshold: float = 1.5,
    ) -> dict[str, Any]:
        """
        Compute symbol-scoped delivery-percentage z-score anomalies across recent NSE sessions,
        matching `finengine`'s `ai_context_india` delivery anomalies component.
        """
        from datalake.finengine_bridge import compute_delivery_anomalies_from_frames

        if not symbols:
            scr = self.screener()
            symbols = scr["Symbol"].head(20).tolist() if not scr.empty and "Symbol" in scr.columns else ["RELIANCE", "TCS", "HDFCBANK", "INFY"]
        price_frames = {sym: self.history(sym, period="6mo", auto_adjust=False) for sym in symbols}
        return compute_delivery_anomalies_from_frames(
            price_frames, lookback_sessions=lookback_sessions, z_threshold=z_threshold
        )

    def liquidity_limits(
        self,
        positions: list[dict[str, Any]],
        lookback_sessions: int = 30,
        base_currency: str = "INR",
    ) -> dict[str, Any]:
        """
        Compute 30-day ADV, Amihud (2002) illiquidity, days-to-liquidate at 10%/20% ADV participation,
        and `max_sane_position_value` (5% ADV) for a portfolio of Indian equity positions.
        """
        from datalake.finengine_bridge import compute_portfolio_liquidity_limits

        syms = [
            str(p.get("symbol") or p.get("ticker") or "").strip().upper().split(".")[0]
            for p in positions
            if p.get("symbol") or p.get("ticker")
        ]
        price_frames = {s: self.history(s, period="6mo", auto_adjust=False) for s in syms if s}
        return compute_portfolio_liquidity_limits(
            positions=positions,
            price_frames=price_frames,
            lookback_sessions=lookback_sessions,
            base_currency=base_currency,
        )

    def ai_context_india(
        self,
        positions: list[dict[str, Any]] | None = None,
        symbols: list[str] | None = None,
        lookback_sessions: int = 30,
    ) -> dict[str, Any]:
        """
        Assemble the 3-component `finengine` `ai_context_india` payload:
        1. `institutional_flows` (FII/FPI & DII cash flows)
        2. `delivery_anomalies` (delivery-% z-scores)
        3. `liquidity_limits` (30d ADV, Amihud illiquidity, days-to-liquidate)
        """
        from datalake.finengine_bridge import build_ai_context_india

        pos_list = positions or [
            {"symbol": s, "quantity": 1000, "market_value": 2500000.0}
            for s in (symbols or ["RELIANCE", "TCS", "HDFCBANK", "INFY"])
        ]
        target_syms = symbols or [
            str(p.get("symbol") or p.get("ticker") or "").strip().upper().split(".")[0]
            for p in pos_list
        ]
        flows_df = self.fii_dii_flows()
        deliv = self.delivery_anomalies(symbols=target_syms, lookback_sessions=lookback_sessions)
        liq = self.liquidity_limits(positions=pos_list, lookback_sessions=lookback_sessions)
        poi_df = self.participant_oi()
        bans_df = self.fo_ban_list()
        ban_syms = bans_df["Symbol"].dropna().astype(str).tolist() if not bans_df.empty and "Symbol" in bans_df.columns else []
        return build_ai_context_india(
            flows_df=flows_df,
            delivery_payload=deliv,
            liquidity_payload=liq,
            participant_oi_df=poi_df,
            fo_ban_symbols=ban_syms,
        )

    def etfs(self) -> pd.DataFrame:
        """Return the NSE ETF Master catalog (Symbol, Underlying, SecurityName, ISIN, ListingDate)."""
        p = self.root / "universe" / "nse_etf_master.parquet"
        return read_table_safe(p) if p.exists() else pd.DataFrame()

    def market_universe(self) -> pd.DataFrame:
        """Return the NSE Pre-Open Market Universe (`universe/nse_market_universe.parquet`) with 52W High/Low."""
        p = self.root / "universe" / "nse_market_universe.parquet"
        return read_table_safe(p) if p.exists() else pd.DataFrame()

    def sme_stocks(self) -> pd.DataFrame:
        """Return the NSE SME Emerge Master catalog (`universe/nse_sme_master.parquet`)."""
        p = self.root / "universe" / "nse_sme_master.parquet"
        return read_table_safe(p) if p.exists() else pd.DataFrame()

    def delisted_stocks(self) -> pd.DataFrame:
        """Return the NSE Delisted Securities archive (`universe/nse_delisted_master.parquet`)."""
        p = self.root / "universe" / "nse_delisted_master.parquet"
        return read_table_safe(p) if p.exists() else pd.DataFrame()

    def symbol_changes(self) -> pd.DataFrame:
        """Return the historical NSE Symbol Rename table (`universe/nse_symbol_changes.parquet`)."""
        p = self.root / "universe" / "nse_symbol_changes.parquet"
        return read_table_safe(p) if p.exists() else pd.DataFrame()

    def fii_dii_flows(self, start: str | date | None = None, end: str | date | None = None) -> pd.DataFrame:
        """Return daily Cash Market FII/FPI and DII net inflows/outflows (in Rs Crores)."""
        p = self.root / "flows" / "cash_fii_dii_flows.parquet"
        if not p.exists():
            return pd.DataFrame()
        df = read_table_safe(p)
        if df.empty or "Date" not in df.columns:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values(["Date", "category"]).reset_index(drop=True)

    def participant_oi(
        self,
        start: str | date | None = None,
        end: str | date | None = None,
        client_type: str | None = None,
    ) -> pd.DataFrame:
        """Return NSE F&O Participant Open Interest breakdown (Client, DII, FII, Pro)."""
        fl_dir = self.root / "flows"
        if not fl_dir.exists():
            return pd.DataFrame()
        s_yr = pd.Timestamp(start).year if start else 2012
        e_yr = pd.Timestamp(end).year if end else pd.Timestamp.now().year
        frames = []
        for yr in range(s_yr, e_yr + 1):
            p = fl_dir / f"fao_participant_oi_{yr}.parquet"
            if p.exists():
                sub = (
                    self._read_filtered_parquet(p, filters=[("Client Type", "==", client_type)])
                    if client_type
                    else read_table_safe(p)
                )
                if not sub.empty:
                    frames.append(sub)
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values(["Date", "Client Type"]).reset_index(drop=True)

    def panel(
        self,
        queries_or_index: list[str | int] | str = "NIFTY 50",
        field: str = "Adj Close",
        start: str | date | None = None,
        end: str | date | None = None,
        auto_adjust: bool = True,
    ) -> pd.DataFrame:
        """
        Returns a clean, date-aligned 2D (Date x Symbol) wide DataFrame for a list of symbols
        or a named index ('NIFTY 50', 'NIFTY 100', 'NIFTY 500') in a single vectorized pass.
        """
        if isinstance(queries_or_index, str):
            const_df = self.index_constituents(queries_or_index)
            if not const_df.empty and "Symbol" in const_df.columns:
                symbols = [
                    s for s in const_df["Symbol"].dropna().astype(str).unique().tolist()
                    if not s.startswith(("DEBT_BASKET:", "UNDERLYING:")) and s != "INDIA VIX"
                ]
            else:
                symbols = [queries_or_index]
        else:
            symbols = list(queries_or_index)

        dl = self.download(symbols, start=start, end=end, auto_adjust=auto_adjust)
        if not dl:
            return pd.DataFrame()

        series_map = {}
        for sym, df in dl.items():
            col = field if field in df.columns else "Close"
            if not df.empty and col in df.columns:
                series_map[sym] = df.set_index("Date")[col]

        return pd.DataFrame(series_map).sort_index()

    def continuous_futures(
        self,
        symbol: str = "NIFTY",
        contract: int = 1,
        start: str | date | None = None,
        end: str | date | None = None,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Stitches active futures contracts (FUTIDX / FUTSTK / IDF / STF) into a continuous
        time-series (1=Near Month, 2=Next Month) ordered by nearest unexpired ExpiryDate.
        """
        sym = symbol.strip().upper()
        exch = exchange.lower()
        fo_dir = self.root / "derivatives"
        if not fo_dir.exists():
            return pd.DataFrame()

        fo_aliases = [sym]
        if sym == "NIFTY":
            fo_aliases = ["NIFTY", "S&P CNX NIFTY", "CNX NIFTY"]
        elif sym == "BANKNIFTY":
            fo_aliases = ["BANKNIFTY", "CNX BANK", "BANK NIFTY"]
        elif sym in ("NIFTYIT", "CNXIT", "CNX IT"):
            fo_aliases = ["NIFTYIT", "CNX IT", "CNXIT"]
        elif sym in ("NIFTYMID50", "NFTYMCAP50"):
            fo_aliases = ["NIFTYMID50", "NFTYMCAP50"]
        else:
            try:
                fo_aliases = list(self.resolve(sym).aliases)
            except Exception:
                fo_aliases = [sym]

        s_dt = pd.Timestamp(start) if start else pd.Timestamp("2000-06-12")
        e_dt = pd.Timestamp(end) if end else pd.Timestamp.now().normalize()

        frames = []
        for yr in range(s_dt.year, e_dt.year + 1):
            p = fo_dir / f"{exch}_fo_{yr}.parquet"
            if p.exists():
                sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", fo_aliases), ("OptionType", "==", "XX")])
                if not sub.empty:
                    sub["Symbol"] = sym
                    frames.append(sub)
        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
        df["ExpiryDate"] = pd.to_datetime(df["ExpiryDate"]).dt.normalize()
        df = df[(df["Date"] >= s_dt) & (df["Date"] <= e_dt) & (df["ExpiryDate"] >= df["Date"])].copy()
        if df.empty:
            return df

        df = df.sort_values(["Date", "ExpiryDate"])
        df["_ContractRank"] = df.groupby("Date").cumcount() + 1
        out = df[df["_ContractRank"] == contract].drop(columns=["_ContractRank"]).reset_index(drop=True)
        return out

    def mutual_fund(self, scheme_code: int | str, start: str | date | None = None, end: str | date | None = None) -> pd.DataFrame:
        """Return historical NAV series for a Mutual Fund scheme code across 2006..2026."""
        mf_dir = self.root / "mutual_funds"
        if not mf_dir.exists():
            return pd.DataFrame()
        s_yr = pd.Timestamp(start).year if start else 2006
        e_yr = pd.Timestamp(end).year if end else pd.Timestamp.now().year
        code_int = int(scheme_code) if str(scheme_code).isdigit() else scheme_code

        frames = []
        for yr in range(s_yr, e_yr + 1):
            p = mf_dir / f"amfi_nav_{yr}.parquet"
            if p.exists():
                sub = self._read_filtered_parquet(p, filters=[("SchemeCode", "==", code_int)])
                if not sub.empty:
                    frames.append(sub)
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values("Date").drop_duplicates(subset=["Date"]).reset_index(drop=True)

    def mutual_funds(
        self,
        search: str | None = None,
        category: str | None = None,
        amc: str | None = None,
    ) -> pd.DataFrame:
        """
        Search or list active AMFI Mutual Fund schemes (latest NAV, SchemeCode, SchemeName, AMC, Category, ISIN).
        """
        mf_dir = self.root / "mutual_funds"
        if not mf_dir.exists():
            return pd.DataFrame()
        files = sorted(mf_dir.glob("amfi_nav_*.parquet"))
        if not files:
            return pd.DataFrame()
        df = read_table_safe(files[-1])
        if df.empty:
            return df
        df = df.sort_values("Date").drop_duplicates(subset=["SchemeCode"], keep="last").reset_index(drop=True)
        if search:
            q = search.strip()
            mask = df["SchemeName"].astype(str).str.contains(q, case=False, na=False)
            if "ISINGrowth" in df.columns:
                mask = mask | (df["ISINGrowth"].astype(str).str.upper() == q.upper())
            df = df[mask]
        if category and "Category" in df.columns:
            df = df[df["Category"].astype(str).str.contains(category.strip(), case=False, na=False)]
        if amc and "AMC" in df.columns:
            df = df[df["AMC"].astype(str).str.contains(amc.strip(), case=False, na=False)]
        return df.reset_index(drop=True)

    def deals(self, query: str | int | None = None) -> pd.DataFrame:
        """Return Bulk & Block deals (filtered to `query` symbol if provided, or market-wide)."""
        p = self.root / "deals" / "bulk_block_deals.parquet"
        if not p.exists():
            return pd.DataFrame()
        if query is None:
            return read_table_safe(p)
        ident = self.resolve(query)
        return self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])

    def short_selling(self, query: str | int | None = None) -> pd.DataFrame:
        """Return NSE Short Selling disclosures (filtered to `query` symbol if provided, or market-wide)."""
        p = self.root / "deals" / "short_selling.parquet"
        if not p.exists():
            return pd.DataFrame()
        if query is None:
            return read_table_safe(p)
        ident = self.resolve(query)
        return self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])

    def annual_reports(self, query: str | int) -> pd.DataFrame:
        """Return 20-year Annual Report PDF/ZIP download links from NSE & BSE."""
        ident = self.resolve(query)
        p = self.root / "documents" / "annual_reports.parquet"
        if not p.exists():
            return pd.DataFrame()
        df = read_table_safe(p)
        if df.empty or "Symbol" not in df.columns:
            return pd.DataFrame()
        mask = df["Symbol"].astype(str).str.upper().isin(ident.aliases)
        if ident.bse_code and "ScripCode" in df.columns:
            mask = mask | (df["ScripCode"].astype(str).str.strip() == ident.bse_code)
        return df[mask].reset_index(drop=True)

    def concalls(self, query: str | int | None = None) -> pd.DataFrame:
        """Return BSE Earnings Call Transcripts, Investor Presentations, Audio & Credit Ratings."""
        p = self.root / "documents" / "concalls_and_filings.parquet"
        if not p.exists():
            return pd.DataFrame()
        if query is None:
            return read_table_safe(p)
        ident = self.resolve(query)
        if ident.bse_code:
            return self._read_filtered_parquet(p, filters=[("ScripCode", "==", ident.bse_code)])
        df = read_table_safe(p)
        if df.empty or "CompanyName" not in df.columns or not ident.company_name:
            return pd.DataFrame()
        return df[df["CompanyName"].astype(str).str.contains(ident.symbol, case=False, na=False)].reset_index(drop=True)

    def insider_trades(self, query: str | int | None = None) -> pd.DataFrame:
        """Return SEBI PIT Regulation 7(2) Insider Trading & Promoter Pledge disclosures."""
        p = self.root / "deals" / "insider_trading_pit.parquet"
        if not p.exists():
            return pd.DataFrame()
        if query is None:
            return read_table_safe(p)
        ident = self.resolve(query)
        return self._read_filtered_parquet(p, filters=[("symbol", "in", list(ident.aliases))])

    @staticmethod
    def _has_page_index(pq_path: Path) -> bool:
        try:
            import pyarrow.parquet as pq_mod
            pf = pq_mod.ParquetFile(pq_path)
            return pf.num_row_groups == 0 or pf.metadata.row_group(0).column(0).has_offset_index
        except Exception:
            return True

    def optimize_layout(self) -> dict[str, int]:
        """
        Re-clusters all Equity Parquet files by `(Symbol, Date)` instead of `(Date, Symbol)`
        so PyArrow/DuckDB row-group predicate pushdown skips 99% of disk blocks on single-stock queries!
        """
        reclustered = 0
        eq_dir = self.root / "equities"
        if eq_dir.exists():
            for pq in sorted(eq_dir.glob("*.parquet")):
                try:
                    df = read_table_safe(pq)
                except Exception:
                    continue
                if not df.empty and "Symbol" in df.columns and "Date" in df.columns:
                    sorted_df = df.sort_values(["Symbol", "Date"])
                    if not df.index.equals(sorted_df.index) or not self._has_page_index(pq):
                        write_parquet_atomic(sorted_df.reset_index(drop=True), pq)
                        reclustered += 1
        fo_dir = self.root / "derivatives"
        if fo_dir.exists():
            for pq in sorted(fo_dir.glob("*.parquet")):
                try:
                    df = read_table_safe(pq)
                except Exception:
                    continue
                sort_cols = [c for c in ("Symbol", "Date", "ExpiryDate", "StrikePrice", "OptionType") if c in df.columns]
                if not df.empty and len(sort_cols) >= 2:
                    sorted_df = df.sort_values(sort_cols)
                    if not df.index.equals(sorted_df.index) or not self._has_page_index(pq):
                        write_parquet_atomic(sorted_df.reset_index(drop=True), pq)
                        reclustered += 1
        cc_path = self.root / "documents" / "concalls_and_filings.parquet"
        if cc_path.exists():
            try:
                cc_df = read_table_safe(cc_path)
                if not cc_df.empty and "ScripCode" in cc_df.columns and "FilingTimestamp" in cc_df.columns:
                    sorted_cc = cc_df.sort_values(["ScripCode", "FilingTimestamp"])
                    if not cc_df.index.equals(sorted_cc.index) or not self._has_page_index(cc_path):
                        write_parquet_atomic(sorted_cc.reset_index(drop=True), cc_path)
                        reclustered += 1
            except Exception:
                pass
        act_dir = self.root / "actions"
        if act_dir.exists():
            for pq in sorted(act_dir.glob("*.parquet")):
                try:
                    df = read_table_safe(pq)
                except Exception:
                    continue
                key_col = "symbol" if "symbol" in df.columns else ("scrip_code" if "scrip_code" in df.columns else None)
                if not df.empty and key_col and "ExDate" in df.columns:
                    sorted_df = df.sort_values([key_col, "ExDate"])
                    if not df.index.equals(sorted_df.index) or not self._has_page_index(pq):
                        write_parquet_atomic(sorted_df.reset_index(drop=True), pq, row_group_size=2_000)
                        reclustered += 1
        self._cached_raw_equity.cache_clear()
        return {"files_clustered_by_symbol_date": reclustered}

    def sql(self, query: str) -> pd.DataFrame:
        """
        Run zero-copy analytical SQL directly across all Parquet datasets using DuckDB.
        Auto-registers views: `equities_nse`, `equities_bse`, `fo_nse`, `indices`, `financials`,
        `shareholding`, `named_holders`, `actions_nse`, `actions_bse`, `mutual_funds`, `deals`.
        """
        if not HAS_DUCKDB:
            raise RuntimeError("Install duckdb (`uv add duckdb`) to run zero-copy SQL queries over Parquet.")
        with self._lock:
            if self._duck is None:
                self._duck = duckdb.connect(":memory:")
                views = {
                    "equities_nse": "equities/nse_cm_*.parquet",
                    "equities_bse": "equities/bse_cm_*.parquet",
                    "fo_nse": "derivatives/nse_fo_*.parquet",
                    "fo_bse": "derivatives/bse_fo_*.parquet",
                    "fo_ban_list": "derivatives/fo_ban_list.parquet",
                    "indices": "indices/nifty_30y_ohlc_tri_pe.parquet",
                    "indices_daily": "indices/ind_close_all_*.parquet",
                    "all_indices": "indices/all_indices_snapshot.parquet",
                    "index_constituents_live": "indices/index_constituents_live.parquet",
                    "nifty_constituents_pit": "universe/nifty_constituents_pit.parquet",
                    "market_universe": "universe/nse_market_universe.parquet",
                    "etfs": "universe/nse_etf_master.parquet",
                    "sme_master": "universe/nse_sme_master.parquet",
                    "delisted_master": "universe/nse_delisted_master.parquet",
                    "symbol_changes": "universe/nse_symbol_changes.parquet",
                    "financials": "fundamentals/xbrl_financials.parquet",
                    "shareholding": "shareholding/nse_shareholding_summary.parquet",
                    "shareholding_bse": "shareholding/bse_shareholding_history.parquet",
                    "named_holders": "shareholding/named_shareholders.parquet",
                    "actions_nse": "actions/nse_corporate_actions.parquet",
                    "actions_bse": "actions/bse_corporate_actions.parquet",
                    "board_meetings": "actions/upcoming_board_meetings.parquet",
                    "mutual_funds": "mutual_funds/amfi_nav_*.parquet",
                    "deals": "deals/bulk_block_deals.parquet",
                    "short_selling": "deals/short_selling.parquet",
                    "insider_pit": "deals/insider_trading_pit.parquet",
                    "participant_oi": "flows/fao_participant_oi_*.parquet",
                    "fii_dii": "flows/cash_fii_dii_flows.parquet",
                    "annual_reports": "documents/annual_reports.parquet",
                    "concalls": "documents/concalls_and_filings.parquet",
                }
                for view_name, glob_pat in views.items():
                    matches = list(self.root.glob(glob_pat))
                    if matches:
                        full_glob = str((self.root / glob_pat).as_posix())
                        self._duck.execute(
                            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{full_glob}', union_by_name=true);"
                        )
        return self._duck.cursor().execute(query).df()


class LakeTicker:
    """Unified yfinance/bfinance-like Ticker facade backed by UnifiedLakeEngine (sub-millisecond cached access)."""

    def __init__(self, engine: UnifiedLakeEngine, identity: SecurityIdentity, default_exchange: str | None = None):
        self.engine = engine
        self.identity = identity
        self.default_exchange = default_exchange or getattr(identity, "primary_exchange", "NSE")

    @property
    def symbol(self) -> str:
        return self.identity.symbol

    @property
    def isin(self) -> str | None:
        return self.identity.isin

    @property
    def bse_code(self) -> str | None:
        return self.identity.bse_code

    def history(
        self,
        start: str | date | None = None,
        end: str | date | None = None,
        period: str | None = None,
        exchange: str | None = None,
        auto_adjust: bool = True,
    ) -> pd.DataFrame:
        exch = exchange or self.default_exchange
        return self.engine.history(
            self.identity.symbol, start=start, end=end, period=period, exchange=exch, auto_adjust=auto_adjust
        )

    @property
    def dossier(self) -> dict[str, pd.DataFrame]:
        return self.engine.dossier(self.identity.symbol)

    @property
    def financials(self) -> pd.DataFrame:
        return self.engine.fundamentals(self.identity.symbol, period_type="Annual")

    @property
    def quarterly_financials(self) -> pd.DataFrame:
        return self.engine.fundamentals(self.identity.symbol, period_type="Quarterly")

    @property
    def income_stmt(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Annual Income Statement (line items as index, period dates as columns)."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        adf = self.financials
        return transpose_financials_to_statement(adf if not adf.empty else self.quarterly_financials, "income_stmt")

    @property
    def quarterly_income_stmt(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Quarterly Income Statement."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        return transpose_financials_to_statement(self.quarterly_financials, "income_stmt")

    @property
    def balance_sheet(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Balance Sheet (in Rs Crores)."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        adf = self.financials
        return transpose_financials_to_statement(adf if not adf.empty else self.quarterly_financials, "balance_sheet")

    @property
    def quarterly_balance_sheet(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Quarterly Balance Sheet (in Rs Crores)."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        return transpose_financials_to_statement(self.quarterly_financials, "balance_sheet")

    @property
    def cashflow(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Cash Flow Statement (in Rs Crores)."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        adf = self.financials
        return transpose_financials_to_statement(adf if not adf.empty else self.quarterly_financials, "cashflow")

    @property
    def quarterly_cashflow(self) -> pd.DataFrame:
        """Return bfinance/yfinance transposed Quarterly Cash Flow Statement (in Rs Crores)."""
        from datalake.finengine_bridge import transpose_financials_to_statement
        return transpose_financials_to_statement(self.quarterly_financials, "cashflow")

    @property
    def custom_ratios(self) -> dict[str, Any]:
        """Return forensic ratios (Piotroski F-Score, Graham Number, EV/EBITDA, CFO/PAT) with honesty contract."""
        return self.engine.custom_ratios(self.identity.symbol)

    @property
    def piotroski_score(self) -> int | None:
        return self.custom_ratios.get("piotroski_score")

    @property
    def graham_number(self) -> float | None:
        return self.custom_ratios.get("graham_number")

    @property
    def research_profile(self) -> dict[str, Any]:
        """Return full FinEngine EquityResearchService 360-degree profile dict."""
        return self.engine.research_profile(self.identity.symbol)

    @property
    def shareholding_trends(self) -> dict[str, Any]:
        """Return FinEngine quarterly/yearly shareholding tables & Recharts series."""
        return self.engine.shareholding_trends(self.identity.symbol)

    @property
    def shareholding(self) -> pd.DataFrame:
        return self.engine.shareholding(self.identity.symbol)

    @property
    def institutional_holders(self) -> pd.DataFrame:
        return self.engine.named_holders(self.identity.symbol)

    @property
    def info(self) -> dict[str, Any]:
        return self.engine.info(self.identity.symbol)

    @property
    def calendar(self) -> pd.DataFrame:
        return self.engine.calendar(self.identity.symbol)

    @property
    def actions(self) -> pd.DataFrame:
        return self.engine.corporate_actions(self.identity.symbol, exchange=self.default_exchange)

    @property
    def deals(self) -> pd.DataFrame:
        return self.engine.deals(self.identity.symbol)

    @property
    def short_selling(self) -> pd.DataFrame:
        return self.engine.short_selling(self.identity.symbol)

    @property
    def options(self) -> tuple[str, ...]:
        """Return sorted tuple of available F&O expiry dates (`YYYY-MM-DD`) for this ticker."""
        chain = self.engine.option_chain(self.identity.symbol, exchange=self.default_exchange)
        if chain.empty or "ExpiryDate" not in chain.columns:
            return ()
        exps = pd.to_datetime(chain["ExpiryDate"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique()
        return tuple(sorted(exps.tolist()))

    def option_chain(self, trade_date: str | date | None = None, expiry: str | date | None = None) -> pd.DataFrame:
        return self.engine.option_chain(
            self.identity.symbol, trade_date=trade_date, expiry=expiry, exchange=self.default_exchange
        )

    @property
    def annual_reports(self) -> pd.DataFrame:
        return self.engine.annual_reports(self.identity.symbol)

    @property
    def concalls(self) -> pd.DataFrame:
        return self.engine.concalls(self.identity.symbol)

    @property
    def insider_trades(self) -> pd.DataFrame:
        return self.engine.insider_trades(self.identity.symbol)

    @property
    def ttm_financials(self) -> pd.DataFrame:
        """Compute Trailing 12-Month (4-quarter rolling sum) P&L metrics and margins."""
        qdf = self.quarterly_financials.copy()
        if qdf.empty:
            return qdf
        qdf["PeriodEnd_dt"] = pd.to_datetime(qdf["PeriodEnd"], format="mixed", dayfirst=True, errors="coerce")
        qdf = qdf.sort_values("PeriodEnd_dt").reset_index(drop=True)

        for c in ("PBT_Cr", "Interest_Cr", "Depreciation_Cr", "OtherIncome_Cr", "Sales_Cr", "NetProfit_Cr", "BasicEPS_Rs"):
            if c not in qdf.columns:
                qdf[c] = 0.0
        qdf["EBITDA_Cr"] = (
            qdf["PBT_Cr"].fillna(0.0)
            + qdf["Interest_Cr"].fillna(0.0)
            + qdf["Depreciation_Cr"].fillna(0.0)
            - qdf["OtherIncome_Cr"].fillna(0.0)
        ).round(4)

        for col in ("Sales_Cr", "EBITDA_Cr", "PBT_Cr", "NetProfit_Cr", "BasicEPS_Rs"):
            qdf[f"{col}_TTM"] = qdf[col].rolling(4, min_periods=4).sum().round(4)

        qdf["OPM_Pct"] = ((qdf["EBITDA_Cr"] / qdf["Sales_Cr"].replace(0, float("nan"))) * 100.0).round(2)
        qdf["NPM_Pct"] = ((qdf["NetProfit_Cr"] / qdf["Sales_Cr"].replace(0, float("nan"))) * 100.0).round(2)
        return qdf

    @property
    def fast_info(self) -> SimpleNamespace:
        """Quick summary of key valuation and technical metrics accessible via dot notation."""
        res: dict[str, Any] = {
            "last_price": None,
            "market_cap": None,
            "pe_ratio": None,
            "pb_ratio": None,
            "ev_ebitda": None,
            "roe_pct": None,
            "roce_pct": None,
            "debt_to_equity": None,
            "sma_50": None,
            "sma_200": None,
            "high_52w": None,
            "low_52w": None,
        }
        try:
            scr = self.engine.screener(f"Symbol = '{self.identity.symbol}'")
            if not scr.empty:
                row = scr.iloc[0].to_dict()
                res.update({
                    "last_price": row.get("Close"),
                    "market_cap": row.get("MarketCap_Cr"),
                    "pe_ratio": row.get("PE"),
                    "pb_ratio": row.get("PB"),
                    "ev_ebitda": row.get("EV_EBITDA"),
                    "roe_pct": row.get("ROE_Pct"),
                    "roce_pct": row.get("ROCE_Pct"),
                    "debt_to_equity": row.get("DebtToEquity"),
                    "sma_50": row.get("SMA_50"),
                    "sma_200": row.get("SMA_200"),
                    "high_52w": row.get("High_52W"),
                    "low_52w": row.get("Low_52W"),
                })
        except Exception:
            pass
        return SimpleNamespace(**res)
