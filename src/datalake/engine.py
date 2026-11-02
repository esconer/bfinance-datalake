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

        isin_to_bse: dict[str, tuple[str, str]] = {}
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
                if isin and isin not in ("", "NA", "NAN", "NONE") and sc:
                    isin_to_bse[isin] = (sc, name)

        if nse_path.exists():
            ndf = read_table_safe(nse_path)
            isin_col = next((c for c in ndf.columns if "ISIN" in c.upper()), "ISIN NUMBER")
            name_col = next((c for c in ndf.columns if "NAME" in c.upper()), "NAME OF COMPANY")
            for r in ndf.to_dict("records"):
                sym = str(r.get("SYMBOL") or "").strip().upper()
                if not sym:
                    continue
                isin = str(r.get(isin_col) or "").strip().upper()
                if isin in ("", "NA", "NAN", "NONE"):
                    isin = None
                name = str(r.get(name_col) or sym).strip()
                bse_code = isin_to_bse.get(isin, (None, None))[0] if isin else None
                aliases = tuple(self.resolver.all_aliases(sym))
                ident = SecurityIdentity(
                    symbol=aliases[0],
                    bse_code=bse_code,
                    isin=isin,
                    company_name=name,
                    aliases=aliases,
                )
                for al in aliases:
                    self._by_symbol[al] = ident
                if bse_code:
                    self._by_bse[bse_code] = ident
                if isin:
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
            ident = SecurityIdentity(
                symbol=sym,
                bse_code=sc,
                isin=isin,
                company_name=name,
                aliases=(sym,),
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
        if exch == "NSE" and "Series" in out.columns and "EQ" in set(out["Series"].astype(str).str.strip()):
            out = out[out["Series"].astype(str).str.strip() == "EQ"].copy()
        return out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

    def history(
        self,
        query: str | int,
        start: str | date | None = None,
        end: str | date | None = None,
        exchange: str = "NSE",
        auto_adjust: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV + Delivery + Dividends + Stock Splits + Adj Close across 1995..2026
        in milliseconds (automatically stitching symbol renames like INFOSYSTCH -> INFY).
        """
        s_dt = pd.Timestamp(start) if start else pd.Timestamp("1994-01-01")
        e_dt = pd.Timestamp(end) if end else pd.Timestamp.now().normalize()
        ident = self.resolve(query)

        raw = self._cached_raw_equity(ident.symbol, exchange.upper(), s_dt.year, e_dt.year)
        if raw.empty and exchange.upper() == "NSE" and ident.bse_code:
            raw = self._cached_raw_equity(ident.bse_code, "BSE", s_dt.year, e_dt.year)
            exchange = "BSE"

        if raw.empty:
            return pd.DataFrame()

        key_for_actions = ident.symbol if exchange.upper() == "NSE" else (ident.bse_code or ident.symbol)
        adj = self.adjuster.apply_adjustments(
            raw, key_for_actions, exchange=exchange, auto_adjust=auto_adjust, include_future_splits=True
        )
        adj = adj[(adj["Date"] >= s_dt) & (adj["Date"] <= e_dt)].reset_index(drop=True)
        return adj

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
        if "NatureOfReport" in df.columns:
            target_nat = "consolidated" if consolidated else "standalone"
            sub = df[df["NatureOfReport"].astype(str).str.lower().str.contains(target_nat)]
            if not sub.empty:
                df = sub.copy()
        return df.sort_values("PeriodEnd").reset_index(drop=True)

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
        if not nse_df.empty and not bse_df.empty:
            return pd.concat([bse_df, nse_df], ignore_index=True)
        return nse_df if not nse_df.empty else bse_df

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
        else:
            files = sorted(fo_dir.glob(f"{exch}_fo_*.parquet"), key=lambda p: p.stem.split("_")[2:])[-1:]

        frames = [self._read_filtered_parquet(f, filters=[("Symbol", "==", sym)]) for f in files if f.exists()]
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
        return df.sort_values(["ExpiryDate", "StrikePrice", "OptionType"]).reset_index(drop=True)

    def index_history(
        self,
        index_name: str = "NIFTY 50",
        start: str | date | None = None,
        end: str | date | None = None,
    ) -> pd.DataFrame:
        """Return 30-year Nifty Index Spot OHLC + Total Return Index (TRI) + P/E, P/B, DivYield."""
        p_bulk = self.root / "indices" / "nifty_30y_ohlc_tri_pe.parquet"
        df = self._read_filtered_parquet(p_bulk, filters=[("IndexName", "==", index_name.strip().upper())])
        if df.empty:
            idx_dir = self.root / "indices"
            if idx_dir.exists():
                files = sorted(idx_dir.glob("ind_close_all_*.parquet"))
                frames = [read_table_safe(f) for f in files]
                if frames:
                    all_idx = pd.concat(frames, ignore_index=True)
                    if "IndexName" in all_idx.columns:
                        df = all_idx[all_idx["IndexName"].astype(str).str.upper() == index_name.strip().upper()].copy()
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        if start:
            df = df[df["Date"] >= pd.Timestamp(start)]
        if end:
            df = df[df["Date"] <= pd.Timestamp(end)]
        return df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

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
                "participant_oi": "flows/fao_participant_oi_*.parquet",
                "fii_dii": "flows/cash_fii_dii_flows.parquet",
                "annual_reports": "documents/annual_reports.parquet",
            }
            for view_name, glob_pat in views.items():
                matches = list(self.root.glob(glob_pat))
                if matches:
                    full_glob = str((self.root / glob_pat).as_posix())
                    self._duck.execute(
                        f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{full_glob}', union_by_name=true);"
                    )
        return self._duck.execute(query).df()


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
        exchange: str = "NSE",
        auto_adjust: bool = False,
    ) -> pd.DataFrame:
        return self.engine.history(self.identity.symbol, start=start, end=end, exchange=exchange, auto_adjust=auto_adjust)

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
