from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
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

        isin_to_bse: dict[str, tuple[str, str, str | None]] = {}
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
                if isin and isin not in ("", "NA", "NAN", "NONE") and sc:
                    isin_to_bse[isin] = (sc, name, ind)

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
            list_col = next((c for c in ndf.columns if "LISTING" in c.upper() or "DATE" in c.upper()), None)
            sym_col = next((c for c in ndf.columns if c.strip().upper() in ("SYMBOL", "SECURITY", "SECURITY SYMBOL")), "SYMBOL")
            for r in ndf.to_dict("records"):
                sym = str(r.get(sym_col) or "").strip().upper()
                if not sym:
                    continue
                isin = str(r.get(isin_col) or "").strip().upper() if isin_col else None
                if isin in ("", "NA", "NAN", "NONE"):
                    isin = None
                name = str(r.get(name_col) or sym).strip() if name_col else sym
                bse_info = isin_to_bse.get(isin, (None, None, None)) if isin else (None, None, None)
                fv_raw = pd.to_numeric(r.get(fv_col), errors="coerce") if fv_col else None
                aliases = tuple(self.resolver.all_aliases(sym))
                canon = aliases[0]
                ident = SecurityIdentity(
                    symbol=canon,
                    bse_code=bse_info[0],
                    isin=isin,
                    company_name=name,
                    aliases=aliases,
                    face_value=float(fv_raw) if pd.notna(fv_raw) else None,
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
            ident = SecurityIdentity(
                symbol=sym,
                bse_code=sc,
                isin=isin,
                company_name=name,
                aliases=(sym,),
                face_value=None,
                industry=ind,
                listing_date=None,
            )
            self._by_bse[sc] = ident
            if sym not in self._by_symbol:
                self._by_symbol[sym] = ident
            if isin and isin not in self._by_isin:
                self._by_isin[isin] = ident

    def resolve(self, query: str | int) -> SecurityIdentity:
        """Resolve any NSE symbol, old renamed symbol, BSE 6-digit code, or 12-char ISIN into a unified identity."""
        self._ensure_crosswalk()
        q = str(query).strip().upper()
        if q.endswith(".NS") or q.endswith(".BO"):
            q = q[:-3]

        if q in self._by_symbol:
            return self._by_symbol[q]
        if q in self._by_bse:
            return self._by_bse[q]
        if q in self._by_isin:
            return self._by_isin[q]

        aliases = tuple(self.resolver.all_aliases(q))
        if aliases[0] in self._by_symbol:
            return self._by_symbol[aliases[0]]
        return SecurityIdentity(
            symbol=aliases[0],
            bse_code=q if q.isdigit() else None,
            isin=q if len(q) == 12 and q.startswith(("INE", "INF")) else None,
            company_name=None,
            aliases=aliases,
        )

    def ticker(self, query: str | int) -> LakeTicker:
        """Return a unified yfinance-style LakeTicker object for instant access to all 15 datasets."""
        return LakeTicker(self, self.resolve(query))

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
        exchange: str = "NSE",
        auto_adjust: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV + Delivery + Dividends + Stock Splits + Adj Close across 1995..2026
        in milliseconds (automatically stitching symbol renames like INFOSYSTCH -> INFY).
        """
        e_dt = pd.Timestamp(end).normalize() if end else pd.Timestamp.now().normalize()
        s_dt = pd.Timestamp(start).normalize() if start else self._resolve_period_start(period, e_dt)
        ident = self.resolve(query)
        load_end_yr = max(e_dt.year, pd.Timestamp.now().year)

        raw = self._cached_raw_equity(ident.symbol, exchange.upper(), s_dt.year, load_end_yr)
        if raw.empty and exchange.upper() == "NSE" and ident.bse_code:
            raw = self._cached_raw_equity(ident.bse_code, "BSE", s_dt.year, load_end_yr)
            exchange = "BSE"

        if raw.empty:
            return pd.DataFrame()

        key_for_actions = ident.symbol if exchange.upper() == "NSE" else (ident.bse_code or ident.symbol)
        adj = self.adjuster.apply_adjustments(
            raw, key_for_actions, exchange=exchange, auto_adjust=auto_adjust, include_future_splits=True
        )
        adj = adj[(adj["Date"] >= s_dt) & (adj["Date"] <= e_dt)].reset_index(drop=True)
        return adj

    def download(
        self,
        queries: list[str | int],
        start: str | date | None = None,
        end: str | date | None = None,
        period: str | None = None,
        exchange: str = "NSE",
        auto_adjust: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """Vectorized multi-symbol OHLCV download reading each annual Parquet file only once."""
        e_dt = pd.Timestamp(end).normalize() if end else pd.Timestamp.now().normalize()
        s_dt = pd.Timestamp(start).normalize() if start else self._resolve_period_start(period, e_dt)
        load_end_yr = max(e_dt.year, pd.Timestamp.now().year)
        exch = exchange.upper()
        prefix = "nse_cm_" if exch == "NSE" else "bse_cm_"
        eq_dir = self.root / "equities"
        if not eq_dir.exists() or not queries:
            return {}

        idents = [self.resolve(q) for q in queries]
        alias_to_canon = {al: ident.symbol for ident in idents for al in ident.aliases}
        all_aliases = list(alias_to_canon.keys())

        frames = []
        for yr in range(s_dt.year, load_end_yr + 1):
            p = eq_dir / f"{prefix}{yr}.parquet"
            if p.exists():
                sub = self._read_filtered_parquet(p, filters=[("Symbol", "in", all_aliases)])
                if not sub.empty:
                    frames.append(sub)
        if not frames:
            return {}

        combined = pd.concat(frames, ignore_index=True)
        combined["_CanonSymbol"] = combined["Symbol"].astype(str).str.strip().str.upper().map(alias_to_canon)
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
    ) -> pd.DataFrame:
        """Return Ind-AS XBRL Financial Statements (P&L, Balance Sheet with Cash & Debt, Cash Flow in Rs Cr)."""
        ident = self.resolve(query)
        p = self.root / "fundamentals" / "xbrl_financials.parquet"
        df = self._read_filtered_parquet(p, filters=[("Symbol", "in", list(ident.aliases))])
        if df.empty:
            return df
        if period_type:
            df = df[df["PeriodType"].astype(str).str.lower() == period_type.lower()].copy()
        if "NatureOfReport" in df.columns and not df.empty:
            is_cons = df["NatureOfReport"].astype(str).str.lower().str.contains("consolidated")
            df["_nat_prio"] = is_cons if consolidated else ~is_cons
            df = (
                df.sort_values(["PeriodEnd", "PeriodType", "_nat_prio"])
                .drop_duplicates(subset=["PeriodType", "PeriodEnd"], keep="last")
                .drop(columns=["_nat_prio"])
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

    def corporate_actions(self, query: str | int) -> pd.DataFrame:
        """Return parsed Dividends, Splits, and Bonuses for the security across NSE & BSE."""
        ident = self.resolve(query)
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
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """Return EOD Futures & Option Chain (CE/PE, Strike, SettlePrice, OI, ChangeInOI) in milliseconds."""
        sym = symbol.strip().upper()
        exch = exchange.lower()
        fo_dir = self.root / "derivatives"
        if not fo_dir.exists():
            return pd.DataFrame()

        if trade_date:
            dt = pd.Timestamp(trade_date).normalize()
            yearly = fo_dir / f"{exch}_fo_{dt.year}.parquet"
            monthly = fo_dir / f"{exch}_fo_{dt.year}_{dt.month:02d}.parquet"
            files = [yearly] if yearly.exists() else [monthly]
            filters = [("Symbol", "==", sym), ("Date", "==", dt)]
        else:
            files = sorted(fo_dir.glob(f"{exch}_fo_*.parquet"), key=lambda p: p.stem.split("_")[2:])[-1:]
            if not files or not files[0].exists():
                return pd.DataFrame()
            try:
                date_df = pd.read_parquet(files[0], columns=["Date"], filters=[("Symbol", "==", sym)])
                if date_df.empty:
                    return pd.DataFrame()
                max_dt = pd.to_datetime(date_df["Date"]).max()
                filters = [("Symbol", "==", sym), ("Date", "==", max_dt)]
            except Exception:
                filters = [("Symbol", "==", sym)]

        frames = [self._read_filtered_parquet(f, filters=filters) for f in files if f.exists()]
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        if df.empty:
            return df
        dt_norm = pd.to_datetime(df["Date"]).dt.normalize()
        if trade_date:
            df = df[dt_norm == pd.Timestamp(trade_date).normalize()]
        else:
            df = df[dt_norm == dt_norm.max()]
        if expiry:
            df = df[pd.to_datetime(df["ExpiryDate"]).dt.normalize() == pd.Timestamp(expiry).normalize()]

        if "UnderlyingPrice" in df.columns and df["UnderlyingPrice"].isna().all() and not df.empty:
            try:
                if sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"):
                    idx_name = "NIFTY 50" if sym == "NIFTY" else ("NIFTY BANK" if sym == "BANKNIFTY" else sym)
                    spot_df = self.index_history(idx_name)
                    if not spot_df.empty and "Close" in spot_df.columns:
                        spot_df["_dt"] = pd.to_datetime(spot_df["Date"]).dt.normalize()
                        df_dt = pd.to_datetime(df["Date"]).dt.normalize()
                        spot_map = dict(zip(spot_df["_dt"], spot_df["Close"]))
                        df["UnderlyingPrice"] = df_dt.map(spot_map)
                else:
                    eq_df = self.history(sym, auto_adjust=False)
                    if not eq_df.empty and "Close" in eq_df.columns:
                        eq_df["_dt"] = pd.to_datetime(eq_df["Date"]).dt.normalize()
                        df_dt = pd.to_datetime(df["Date"]).dt.normalize()
                        eq_map = dict(zip(eq_df["_dt"], eq_df["Close"]))
                        df["UnderlyingPrice"] = df_dt.map(eq_map)
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
        if df.empty:
            idx_dir = self.root / "indices"
            if idx_dir.exists():
                files = sorted(idx_dir.glob("ind_close_all_*.parquet"))
                frames = [
                    self._read_filtered_parquet(f, filters=[("IndexName", "==", clean_idx)])
                    for f in files
                    if f.exists()
                ]
                frames = [f for f in frames if not f.empty]
                if frames:
                    df = pd.concat(frames, ignore_index=True)
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

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

        fin_cte = """
            , latest_fin AS (
                SELECT Symbol, PeriodEnd AS FinQuarter, Sales_Cr, PBT_Cr, NetProfit_Cr, BasicEPS_Rs
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY PeriodEnd DESC) AS rn
                    FROM financials WHERE PeriodType = 'Quarterly'
                ) WHERE rn = 1
            )
        """ if has_fin else ""
        shp_cte = """
            , latest_shp AS (
                SELECT Symbol, QuarterEnd AS ShpQuarter, PromoterPct, PromoterPledgedPct, MutualFundsPct, FPI_Pct, RetailPct
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY QuarterEnd DESC) AS rn
                    FROM shareholding
                ) WHERE rn = 1
            )
        """ if has_shp else ""

        fin_cols = ", f.FinQuarter, f.Sales_Cr, f.PBT_Cr, f.NetProfit_Cr, f.BasicEPS_Rs" if has_fin else ""
        shp_cols = ", s.ShpQuarter, s.PromoterPct, s.PromoterPledgedPct, s.MutualFundsPct, s.FPI_Pct, s.RetailPct" if has_shp else ""
        fin_join = "LEFT JOIN latest_fin f USING (Symbol)" if has_fin else ""
        shp_join = "LEFT JOIN latest_shp s USING (Symbol)" if has_shp else ""

        return self.sql(f"""
            WITH recent_eq AS (
                SELECT
                    Symbol, Date, Close, Volume,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS SMA_50,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS SMA_200,
                    MAX(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS High_52W,
                    MIN(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS Low_52W,
                    ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date DESC) AS rn
                FROM equities_nse
            ),
            latest_eq AS (
                SELECT * EXCLUDE (rn) FROM recent_eq WHERE rn = 1
            )
            {fin_cte}
            {shp_cte}
            SELECT e.* {fin_cols} {shp_cols}
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
            pit_path = self.root / "universe" / "nifty_constituents_pit.parquet"
            if pit_path.exists():
                pdf = read_table_safe(pit_path)
                sub = pdf[pdf["IndexName"].astype(str).str.upper() == queries_or_index.strip().upper()]
                symbols = sub["Symbol"].dropna().unique().tolist() if not sub.empty else [queries_or_index]
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

        s_dt = pd.Timestamp(start) if start else pd.Timestamp("2000-06-12")
        e_dt = pd.Timestamp(end) if end else pd.Timestamp.now().normalize()

        frames = []
        for yr in range(s_dt.year, e_dt.year + 1):
            p = fo_dir / f"{exch}_fo_{yr}.parquet"
            if p.exists():
                sub = self._read_filtered_parquet(p, filters=[("Symbol", "==", sym), ("OptionType", "==", "XX")])
                if not sub.empty:
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

    def deals(self, query: str | int | None = None) -> pd.DataFrame:
        """Return Bulk & Block deals (filtered to `query` symbol if provided, or market-wide)."""
        p = self.root / "deals" / "bulk_block_deals.parquet"
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
                    if not df.index.equals(sorted_df.index):
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
                    if not df.index.equals(sorted_df.index):
                        write_parquet_atomic(sorted_df.reset_index(drop=True), pq)
                        reclustered += 1
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
                    if not df.index.equals(sorted_df.index):
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
        if self._duck is None:
            self._duck = duckdb.connect(":memory:")
            views = {
                "equities_nse": "equities/nse_cm_*.parquet",
                "equities_bse": "equities/bse_cm_*.parquet",
                "fo_nse": "derivatives/nse_fo_*.parquet",
                "indices": "indices/nifty_30y_ohlc_tri_pe.parquet",
                "indices_daily": "indices/ind_close_all_*.parquet",
                "financials": "fundamentals/xbrl_financials.parquet",
                "shareholding": "shareholding/nse_shareholding_summary.parquet",
                "shareholding_bse": "shareholding/bse_shareholding_history.parquet",
                "named_holders": "shareholding/named_shareholders.parquet",
                "actions_nse": "actions/nse_corporate_actions.parquet",
                "actions_bse": "actions/bse_corporate_actions.parquet",
                "mutual_funds": "mutual_funds/amfi_nav_*.parquet",
                "deals": "deals/bulk_block_deals.parquet",
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
    """Unified yfinance-like Ticker facade backed by UnifiedLakeEngine (sub-millisecond cached access)."""

    def __init__(self, engine: UnifiedLakeEngine, identity: SecurityIdentity):
        self.engine = engine
        self.identity = identity

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
        exchange: str = "NSE",
        auto_adjust: bool = False,
    ) -> pd.DataFrame:
        return self.engine.history(
            self.identity.symbol, start=start, end=end, period=period, exchange=exchange, auto_adjust=auto_adjust
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
    def shareholding(self) -> pd.DataFrame:
        return self.engine.shareholding(self.identity.symbol)

    @property
    def institutional_holders(self) -> pd.DataFrame:
        return self.engine.named_holders(self.identity.symbol)

    @property
    def actions(self) -> pd.DataFrame:
        return self.engine.corporate_actions(self.identity.symbol)

    @property
    def deals(self) -> pd.DataFrame:
        return self.engine.deals(self.identity.symbol)

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
