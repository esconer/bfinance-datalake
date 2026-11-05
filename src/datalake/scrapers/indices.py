from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import date, timedelta
from urllib.parse import quote
import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, NIFTY_INDICES_HEADERS, NSE_API_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import read_table_safe, upsert_parquet_partition

logger = logging.getLogger("datalake.indices")

CORE_NIFTY_INDICES = (
    "NIFTY 50",
    "NIFTY NEXT 50",
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 250",
    "NIFTY BANK",
    "NIFTY IT",
    "NIFTY AUTO",
    "NIFTY PHARMA",
    "NIFTY FMCG",
    "NIFTY METAL",
    "NIFTY REALTY",
    "NIFTY ENERGY",
    "INDIA VIX",
    "NIFTY 1D RATE INDEX",
)

_LEGACY_INDEX_MAP = {
    "S&P CNX NIFTY": "NIFTY 50",
    "CNX NIFTY": "NIFTY 50",
    "CNX NIFTY JUNIOR": "NIFTY NEXT 50",
    "CNX 100": "NIFTY 100",
    "CNX 200": "NIFTY 200",
    "CNX 500": "NIFTY 500",
    "S&P CNX 500": "NIFTY 500",
    "CNX BANK": "NIFTY BANK",
    "CNX IT": "NIFTY IT",
    "CNX AUTO": "NIFTY AUTO",
    "CNX PHARMA": "NIFTY PHARMA",
    "CNX FMCG": "NIFTY FMCG",
    "CNX METAL": "NIFTY METAL",
    "CNX REALTY": "NIFTY REALTY",
    "CNX ENERGY": "NIFTY ENERGY",
}


class IndicesScraper:
    """
    Scrapes 30-year Nifty index OHLC + TRI + Valuation via Sitefinity `/BackPage/*`
    (with automatic failover to NSE `/api/historicalOR/indicesHistory`) + daily `ind_close_all` CSV.
    """

    # Sitefinity MVC endpoint (no .aspx suffix!)
    BASE_POST_URL = "https://www.niftyindices.com/BackPage"

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def backfill_30y_bulk(
        self,
        start_date: date = date(1995, 1, 1),
        end_date: date | None = None,
        indices: tuple[str, ...] = CORE_NIFTY_INDICES,
        force: bool = False,
    ) -> None:
        end = end_date or today_ist()
        target = self.cfg.root_dir / "indices" / "nifty_30y_ohlc_tri_pe.parquet"

        existing_coverage: dict[str, tuple[date, date, int]] = {}
        if not force and target.exists():
            try:
                existing_df = read_table_safe(target)
                if not existing_df.empty and "IndexName" in existing_df.columns and "Date" in existing_df.columns:
                    for idx_n, grp in existing_df.groupby("IndexName"):
                        dts = pd.to_datetime(grp["Date"], errors="coerce").dropna()
                        if not dts.empty:
                            existing_coverage[str(idx_n).strip().upper()] = (
                                dts.min().date(),
                                dts.max().date(),
                                len(grp),
                            )
            except Exception as exc:
                logger.debug("[Index 30Y Bulk] Could not read existing coverage: %s", exc)

        for idx_name in indices:
            cur_start = start_date
            sym_key = str(idx_name).strip().upper()
            if not force and sym_key in existing_coverage:
                cov_min, cov_max, row_cnt = existing_coverage[sym_key]
                # If existing history has substantial records (> 500 trading days)
                if row_cnt >= 500:
                    delta_start = cov_max + timedelta(days=1)
                    has_pending_weekdays = any(
                        (delta_start + timedelta(days=i)).weekday() < 5
                        for i in range(max(0, (end - delta_start).days + 1))
                    )
                    if cov_max >= end or not has_pending_weekdays:
                        logger.info(
                            "[Index 30Y Bulk] %s already covered (%s -> %s, %d rows). Skipping redundant fetch.",
                            idx_name, cov_min, cov_max, row_cnt,
                        )
                        continue
                    logger.info(
                        "[Index 30Y Bulk] %s covered up to %s (%d rows). Fetching incremental delta (%s -> %s)...",
                        idx_name, cov_max, row_cnt, delta_start, end,
                    )
                    cur_start = delta_start

            logger.info("[Index 30Y Bulk] Fetching OHLC + TRI + PE for %s (%s -> %s)...", idx_name, cur_start, end)
            ohlc_df, tri_df, pe_df = await asyncio.gather(
                self._post_nifty(idx_name, cur_start, end, "getHistoricaldatatabletoString"),
                self._post_nifty(idx_name, cur_start, end, "getTotalReturnIndexString"),
                self._post_nifty(
                    idx_name,
                    cur_start,
                    end,
                    "getpepbHistoricaldataDBtoString",
                    extra=",'pe':'true','pb':'true','dy':'true'",
                ),
                return_exceptions=True,
            )
            if isinstance(ohlc_df, Exception):
                ohlc_df = pd.DataFrame()
            if isinstance(tri_df, Exception):
                tri_df = pd.DataFrame()
            if isinstance(pe_df, Exception):
                pe_df = pd.DataFrame()

            # Fallback to NSE /api/historicalOR/indicesHistory if niftyindices.com returned empty
            if ohlc_df.empty:
                ohlc_df = await self._fetch_nse_historical_or_index(idx_name, cur_start, end)

            if ohlc_df.empty:
                continue

            df = ohlc_df.rename(columns={
                "HistoricalDate": "Date",
                "EOD_TIMESTAMP": "Date",
                "OPEN": "Open",
                "EOD_OPEN_INDEX_VAL": "Open",
                "HIGH": "High",
                "EOD_HIGH_INDEX_VAL": "High",
                "LOW": "Low",
                "EOD_LOW_INDEX_VAL": "Low",
                "CLOSE": "Close",
                "EOD_CLOSE_INDEX_VAL": "Close",
            })
            df = df.loc[:, ~df.columns.duplicated()].copy()
            if "Date" not in df.columns or "Close" not in df.columns:
                continue
            keep_base = [c for c in ("Date", "Open", "High", "Low", "Close") if c in df.columns]
            df = df[keep_base].copy()
            df["Date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=True, errors="coerce")
            for c in ("Open", "High", "Low", "Close"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c].astype(str).str.strip().replace("-", pd.NA), errors="coerce")
            for c in ("Open", "High", "Low"):
                if c in df.columns:
                    df[c] = df[c].fillna(df["Close"])
            df["IndexName"] = idx_name

            if not tri_df.empty and "Date" in tri_df.columns and "TotalReturnsIndex" in tri_df.columns:
                t = tri_df[["Date", "TotalReturnsIndex"]].copy()
                t["Date"] = pd.to_datetime(t["Date"], format="mixed", dayfirst=True, errors="coerce")
                t["TRI"] = pd.to_numeric(
                    t["TotalReturnsIndex"].astype(str).str.strip().replace("-", pd.NA), errors="coerce"
                )
                df = df.merge(t[["Date", "TRI"]].drop_duplicates(subset=["Date"]), on="Date", how="left")

            if not pe_df.empty and ("Date" in pe_df.columns or "DATE" in pe_df.columns):
                p = pe_df.rename(columns={"DATE": "Date"}).copy()
                p = p.loc[:, ~p.columns.duplicated()].copy()
                p["Date"] = pd.to_datetime(p["Date"], format="mixed", dayfirst=True, errors="coerce")
                for src, dst in (
                    ("pe", "PE"), ("PE", "PE"),
                    ("pb", "PB"), ("PB", "PB"),
                    ("divYield", "DivYield"), ("DIVYIELD", "DivYield"),
                ):
                    if src in p.columns:
                        p[dst] = pd.to_numeric(p[src].astype(str).str.strip().replace("-", pd.NA), errors="coerce")
                keep_cols = [c for c in ("Date", "PE", "PB", "DivYield") if c in p.columns]
                df = df.merge(p[keep_cols].drop_duplicates(subset=["Date"]), on="Date", how="left")

            df = df.dropna(subset=["Date", "Close"])
            if not df.empty:
                upsert_parquet_partition(
                    df, target, dedup_cols=["Date", "IndexName"], sort_cols=["IndexName", "Date"]
                )
                self.state.record("indices_30y_bulk", f"{idx_name}:{cur_start}_{end}", "OK", row_count=len(df))
            await asyncio.sleep(self.cfg.polite_delay_sec)

    async def _post_nifty(
        self, index_name: str, start: date, end: date, method: str, extra: str = ""
    ) -> pd.DataFrame:
        """Chunks requests into 365-day windows against Sitefinity `/BackPage/{method}`."""
        frames: list[pd.DataFrame] = []
        cur_start = start
        consecutive_errors = 0
        while cur_start <= end:
            cur_end = min(end, cur_start + timedelta(days=364))
            cinfo = (
                f"{{'name':'{index_name}',"
                f"'startDate':'{cur_start:%d-%b-%Y}',"
                f"'endDate':'{cur_end:%d-%b-%Y}',"
                f"'indexName':'{index_name}'{extra}}}"
            )
            try:
                resp = await self.http.post(
                    f"{self.BASE_POST_URL}/{method}",
                    json_body={"cinfo": cinfo},
                    headers=NIFTY_INDICES_HEADERS,
                    prime_nifty=True,
                )
                consecutive_errors = 0
                if resp.status_code == 200 and resp.text.strip().startswith(("{", "[")):
                    payload = resp.json()
                    if isinstance(payload, list) and payload:
                        frames.append(pd.DataFrame(payload))
                    elif isinstance(payload, dict):
                        d_val = payload.get("d", "[]")
                        rows = json.loads(d_val) if isinstance(d_val, str) else d_val
                        if isinstance(rows, list) and rows:
                            frames.append(pd.DataFrame(rows))
            except Exception as exc:
                logger.debug("[NiftyIndices %s %s] %s", index_name, method, exc)
                consecutive_errors += 1
                if consecutive_errors >= 2:
                    break
            cur_start = cur_end + timedelta(days=1)
            await asyncio.sleep(self.cfg.polite_delay_sec)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    async def _fetch_nse_historical_or_index(self, index_name: str, start: date, end: date) -> pd.DataFrame:
        """Fallback to NSE's official `/api/historicalOR/indicesHistory` (365-day chunks)."""
        frames: list[pd.DataFrame] = []
        cur_start = start
        consecutive_errors = 0
        while cur_start <= end:
            cur_end = min(end, cur_start + timedelta(days=364))
            url = (
                f"https://www.nseindia.com/api/historicalOR/indicesHistory"
                f"?indexType={quote(index_name)}&from={cur_start:%d-%m-%Y}&to={cur_end:%d-%m-%Y}"
            )
            try:
                resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                consecutive_errors = 0
                if resp.status_code == 200 and resp.text.strip().startswith("{"):
                    data = resp.json().get("data", [])
                    if isinstance(data, dict):
                        data = data.get("indexCloseOnlineRecords", [])
                    if isinstance(data, list) and data:
                        frames.append(pd.DataFrame(data))
            except Exception as exc:
                logger.debug("[NSE Index Fallback %s] %s", index_name, exc)
                consecutive_errors += 1
                if consecutive_errors >= 2:
                    break
            cur_start = cur_end + timedelta(days=1)
            await asyncio.sleep(self.cfg.polite_delay_sec)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    async def sync_daily_snapshots(self, start_date: date, end_date: date) -> None:
        dataset = "indices_daily_all"
        done_keys = self.state.completed_keys(dataset)
        freeze_cutoff = today_ist() - timedelta(days=self.cfg.holiday_freeze_days)
        sem = asyncio.Semaphore(self.cfg.archive_concurrency)

        for year in range(max(2012, start_date.year), end_date.year + 1):
            yr_start = max(start_date, date(year, 1, 1))
            yr_end = min(end_date, date(year, 12, 31))
            pending = [
                yr_start + timedelta(days=i)
                for i in range((yr_end - yr_start).days + 1)
                if (yr_start + timedelta(days=i)).weekday() < 5
                and (yr_start + timedelta(days=i)).isoformat() not in done_keys
            ]
            if not pending:
                continue

            async def _fetch_day(d: date):
                url = f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{d:%d%m%Y}.csv"
                async with sem:
                    try:
                        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
                        await asyncio.sleep(self.cfg.polite_delay_sec)
                        if resp.status_code in (404, 403):
                            return d, None, None, url, None
                        head_txt = resp.text[:250].lstrip()
                        if head_txt.startswith("<") or "Index Name" not in head_txt:
                            return d, None, None, url, None
                        df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
                        df.columns = [str(c).lstrip("\ufeff").strip() for c in df.columns]
                        def _num_col(col_name: str) -> pd.Series:
                            if col_name not in df.columns:
                                return pd.Series(float("nan"), index=df.index, dtype="float64")
                            return pd.to_numeric(df[col_name].astype(str).str.strip().replace("-", pd.NA), errors="coerce")

                        out = pd.DataFrame({
                            "Date": pd.Timestamp(d),
                            "IndexName": df["Index Name"].astype(str).str.strip().map(lambda s: _LEGACY_INDEX_MAP.get(s.upper(), s)),
                            "Open": _num_col("Open Index Value"),
                            "High": _num_col("High Index Value"),
                            "Low": _num_col("Low Index Value"),
                            "Close": _num_col("Closing Index Value"),
                            "Volume": _num_col("Volume"),
                            "TurnoverCr": _num_col("Turnover (Rs. Cr.)"),
                            "PE": _num_col("P/E"),
                            "PB": _num_col("P/B"),
                            "DivYield": _num_col("Div Yield"),
                        })
                        for c in ("Open", "High", "Low"):
                            out[c] = out[c].fillna(out["Close"])
                        return d, out, resp.content, url, None
                    except Exception as exc:
                        return d, None, None, url, exc

            for chunk_start in range(0, len(pending), 22):
                batch = pending[chunk_start : chunk_start + 22]
                results = await asyncio.gather(*[_fetch_day(d) for d in batch])
                frames = []
                ok_records = []

                for d, df, raw_bytes, url, err in results:
                    key = d.isoformat()
                    if err:
                        self.state.record(dataset, key, "FAILED", source_url=url)
                    elif df is None or df.empty:
                        if d <= freeze_cutoff:
                            self.state.record(dataset, key, "HOLIDAY", source_url=url)
                    elif len(df) < 15:
                        self.state.record(dataset, key, "FAILED", row_count=len(df), source_url=url)
                    else:
                        frames.append(df)
                        ok_records.append((key, len(df), raw_bytes, url))

                if frames:
                    batch_df = pd.concat(frames, ignore_index=True)
                    target = self.cfg.root_dir / "indices" / f"ind_close_all_{year}.parquet"
                    upsert_parquet_partition(
                        batch_df,
                        target,
                        dedup_cols=["Date", "IndexName"],
                        sort_cols=["Date", "IndexName"],
                    )
                    # Also upsert India VIX into nifty_30y_ohlc_tri_pe.parquet for finengine regime/GARCH models
                    vix_rows = batch_df[batch_df["IndexName"].astype(str).str.upper() == "INDIA VIX"].copy()
                    if not vix_rows.empty:
                        vix_rows["IndexName"] = "INDIA VIX"
                        keep_vix = [c for c in ("Date", "IndexName", "Open", "High", "Low", "Close") if c in vix_rows.columns]
                        upsert_parquet_partition(
                            vix_rows[keep_vix],
                            self.cfg.root_dir / "indices" / "nifty_30y_ohlc_tri_pe.parquet",
                            dedup_cols=["Date", "IndexName"],
                            sort_cols=["IndexName", "Date"],
                        )
                    for key, r_cnt, raw_b, s_url in ok_records:
                        self.state.record(dataset, key, "OK", row_count=r_cnt, raw_bytes=raw_b, source_url=s_url)

        self.sync_vix_and_1d_rate_from_snapshots()

    def sync_vix_and_1d_rate_from_snapshots(self) -> int:
        """Extract INDIA VIX and NIFTY 1D RATE INDEX from ind_close_all_*.parquet into nifty_30y_ohlc_tri_pe.parquet."""
        idx_dir = self.cfg.root_dir / "indices"
        if not idx_dir.exists():
            return 0
        target_names = {"INDIA VIX": "INDIA VIX", "NIFTY 1D RATE INDEX": "NIFTY 1D RATE INDEX"}
        extracted: list[pd.DataFrame] = []
        for f in sorted(idx_dir.glob("ind_close_all_*.parquet")):
            df = read_table_safe(f)
            if df.empty or "IndexName" not in df.columns:
                continue
            upper_names = df["IndexName"].astype(str).str.strip().str.upper()
            sub = df[upper_names.isin(target_names)].copy()
            if not sub.empty:
                sub["IndexName"] = upper_names[upper_names.isin(target_names)].map(target_names)
                keep = [c for c in ("Date", "IndexName", "Open", "High", "Low", "Close", "PE", "PB", "DivYield") if c in sub.columns]
                extracted.append(sub[keep])
        if not extracted:
            return 0
        merged = pd.concat(extracted, ignore_index=True)
        return upsert_parquet_partition(
            merged,
            idx_dir / "nifty_30y_ohlc_tri_pe.parquet",
            dedup_cols=["Date", "IndexName"],
            sort_cols=["IndexName", "Date"],
        )

    async def sync_live_index_constituents(
        self, indices: tuple[str, ...] = ("NIFTY 50", "NIFTY NEXT 50", "NIFTY BANK", "NIFTY IT")
    ) -> None:
        """Fetches live constituent weights, valuation ratios, and pricing for key Nifty indices."""
        frames = []
        today = today_ist()
        for idx in indices:
            url = f"https://www.nseindia.com/api/equity-stock-indices?index={quote(idx)}"
            try:
                resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                if resp.status_code != 200 or not resp.text.lstrip().startswith("{"):
                    continue
                data = resp.json().get("data", [])
                if isinstance(data, list) and len(data) > 1:
                    for it in data[1:]:
                        sym = str(it.get("symbol") or "").strip().upper()
                        if sym:
                            frames.append({
                                "IndexName": idx,
                                "Symbol": sym,
                                "Series": str(it.get("series") or "EQ").strip().upper(),
                                "Open": pd.to_numeric(it.get("open"), errors="coerce"),
                                "High": pd.to_numeric(it.get("dayHigh"), errors="coerce"),
                                "Low": pd.to_numeric(it.get("dayLow"), errors="coerce"),
                                "LastPrice": pd.to_numeric(it.get("lastPrice"), errors="coerce"),
                                "TotalTradedVolume": pd.to_numeric(it.get("totalTradedVolume"), errors="coerce"),
                                "TotalTradedValue": pd.to_numeric(it.get("totalTradedValue"), errors="coerce"),
                                "NearWKH": pd.to_numeric(it.get("nearWKH"), errors="coerce"),
                                "NearWKL": pd.to_numeric(it.get("nearWKL"), errors="coerce"),
                                "Date": pd.Timestamp(today),
                            })
            except Exception as exc:
                logger.debug("[Live Index Constituents %s] Skipped: %s", idx, exc)

        if frames:
            df = pd.DataFrame(frames)
            target = self.cfg.root_dir / "indices" / "index_constituents_live.parquet"
            upsert_parquet_partition(
                df,
                target,
                dedup_cols=["Date", "IndexName", "Symbol"],
                sort_cols=["IndexName", "Symbol", "Date"],
            )
            self.state.record("live_index_constituents", today.isoformat(), "OK", len(df))
