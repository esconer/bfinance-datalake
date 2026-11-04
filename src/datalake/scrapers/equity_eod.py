from __future__ import annotations

import asyncio
import io
import logging
import re
import zipfile
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, NSE_API_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import read_table_safe, upsert_parquet_partition

logger = logging.getLogger("datalake.equity_eod")

_BSE_GLUED_LINE_RE = re.compile(r"(\d+\.\d{2}),(\d{6},[A-Z0-9])")


def repair_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """Repair 1995-2003 NSE bhavcopy rows where Open=0, High<Low, or Open/Close fall outside [Low, High]."""
    if df.empty:
        return df
    out = df.copy()
    for col in ("Open", "High", "Low", "Close", "PrevClose"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")

    open_zero = (out["Open"].isna()) | (out["Open"] <= 0)
    if open_zero.any():
        if "PrevClose" in out.columns:
            fallback = out["PrevClose"].where(out["PrevClose"] > 0, out["Close"])
        else:
            fallback = out["Close"]
        out.loc[open_zero, "Open"] = fallback[open_zero]

    oc_max = out[["Open", "Close"]].max(axis=1)
    oc_min = out[["Open", "Close"]].min(axis=1)
    out["High"] = out["High"].where(out["High"] > 0, oc_max)
    out["Low"] = out["Low"].where(out["Low"] > 0, oc_min)

    swapped = out["High"] < out["Low"]
    if swapped.any():
        out.loc[swapped, ["High", "Low"]] = out.loc[swapped, ["Low", "High"]].to_numpy()

    out["High"] = np.maximum(out["High"], oc_max)
    out["Low"] = np.minimum(out["Low"], oc_min)
    return out


def filter_bse_equity_only(df: pd.DataFrame) -> pd.DataFrame:
    """Remove Government Securities (Group 'G') and Corporate Bonds (Group 'F' with 'INE' ISIN) from BSE files."""
    if df.empty:
        return df
    series = df["Series"].astype(str).str.strip().str.upper()
    isin = df["ISIN"].fillna("").astype(str).str.strip().str.upper()
    is_gov_debt = series == "G"
    is_corp_bond = (series == "F") & isin.str.startswith("INE")
    return df[~(is_gov_debt | is_corp_bond)].reset_index(drop=True)


class EquityEODScraper:
    """Scrapes & updates NSE (1994+) and BSE (2006+) daily OHLCV, ISIN, Trades, and Delivery."""

    NSE_LEGACY_START = date(1994, 11, 3)
    BSE_LEGACY_START = date(2006, 3, 1)
    NSE_UDIFF_CUTOVER = date(2024, 1, 2)
    NSE_FULL_BHAV_CUTOVER = date(2020, 1, 1)
    BSE_UDIFF_CUTOVER = date(2024, 7, 8)

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_range(
        self,
        start_date: date,
        end_date: date,
        exchanges: tuple[str, ...] = ("NSE", "BSE"),
        force: bool = False,
    ) -> None:
        sem = asyncio.Semaphore(self.cfg.archive_concurrency)
        freeze_cutoff = today_ist() - timedelta(days=self.cfg.holiday_freeze_days)

        for exch in exchanges:
            dataset_name = f"equity_eod_{exch.lower()}"
            done_keys = set() if force else self.state.completed_keys(dataset_name)
            exch_min_start = self.BSE_LEGACY_START if exch.upper() == "BSE" else self.NSE_LEGACY_START
            eff_start = max(start_date, exch_min_start)
            if eff_start > end_date:
                continue

            for year in range(eff_start.year, end_date.year + 1):
                yr_start = max(eff_start, date(year, 1, 1))
                yr_end = min(end_date, date(year, 12, 31))

                pending_days = [
                    yr_start + timedelta(days=i)
                    for i in range((yr_end - yr_start).days + 1)
                    if (yr_start + timedelta(days=i)).weekday() < 5
                    and (yr_start + timedelta(days=i)).isoformat() not in done_keys
                ]
                if not pending_days:
                    continue

                logger.info("[%s %d] Fetching %d pending weekdays in 22-day micro-batches...", exch, year, len(pending_days))

                async def _worker(d: date):
                    async with sem:
                        try:
                            if exch == "NSE":
                                df, raw_bytes, url = await self.fetch_nse_day(d)
                            else:
                                df, raw_bytes, url = await self.fetch_bse_day(d)
                            await asyncio.sleep(self.cfg.polite_delay_sec)
                            return d, df, raw_bytes, url, None
                        except Exception as exc:
                            return d, None, None, None, exc

                for chunk_start in range(0, len(pending_days), 22):
                    batch_days = pending_days[chunk_start : chunk_start + 22]
                    results = await asyncio.gather(*[_worker(d) for d in batch_days])

                    batch_frames: list[pd.DataFrame] = []
                    ok_records: list[tuple[str, int, bytes | None, str | None]] = []

                    for d, df, raw_bytes, url, err in results:
                        key = d.isoformat()
                        if err is not None:
                            logger.warning("[%s %s] Failed: %s", exch, key, err)
                            self.state.record(dataset_name, key, "FAILED", source_url=url)
                        elif df is None or df.empty:
                            if d <= freeze_cutoff:
                                self.state.record(dataset_name, key, "HOLIDAY", source_url=url)
                        elif d.year >= 2016 and len(df) < 200:
                            logger.warning("[%s %s] Truncated payload (%d rows < 200). Will retry next run.", exch, key, len(df))
                            self.state.record(dataset_name, key, "FAILED", row_count=len(df), source_url=url)
                        else:
                            batch_frames.append(df)
                            ok_records.append((key, len(df), raw_bytes, url))

                    if batch_frames:
                        merged_batch = pd.concat(batch_frames, ignore_index=True)
                        target = self.cfg.root_dir / "equities" / f"{exch.lower()}_cm_{year}.parquet"
                        total_rows = upsert_parquet_partition(
                            merged_batch,
                            target,
                            dedup_cols=["Date", "Exchange", "InstrumentId", "Series"],
                            sort_cols=["Symbol", "Date"],
                        )
                        for key, r_cnt, raw_b, s_url in ok_records:
                            self.state.record(dataset_name, key, "OK", row_count=r_cnt, raw_bytes=raw_b, source_url=s_url)
                        logger.info("[%s %d] Committed micro-batch to %s (%d total rows)", exch, year, target.name, total_rows)

    async def fetch_nse_day(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        if day >= self.NSE_UDIFF_CUTOVER:
            (full_df, raw_full, url_full), (udiff_df, raw_udiff, url_udiff) = await asyncio.gather(
                self._fetch_nse_sec_bhavdata_full(day),
                self._fetch_nse_udiff(day),
            )
            if udiff_df is None and full_df is None:
                return None, None, url_udiff
            if udiff_df is not None and full_df is not None:
                deliv = full_df[["Symbol", "Series", "DeliverableQty", "DeliveryPct"]].drop_duplicates(
                    subset=["Symbol", "Series"]
                )
                merged = udiff_df.drop(columns=["DeliverableQty", "DeliveryPct"], errors="ignore").merge(
                    deliv, on=["Symbol", "Series"], how="left"
                )
                return repair_ohlc(merged), raw_udiff, url_udiff
            res_df = udiff_df if udiff_df is not None else full_df
            return repair_ohlc(res_df), (raw_udiff or raw_full), (url_udiff if udiff_df is not None else url_full)

        if day >= self.NSE_FULL_BHAV_CUTOVER:
            (full_df, raw_full, url_full), (leg_df, raw_leg, url_leg) = await asyncio.gather(
                self._fetch_nse_sec_bhavdata_full(day),
                self._fetch_nse_legacy_cm(day),
            )
            if full_df is not None:
                if leg_df is not None and "ISIN" in leg_df.columns:
                    isin_map = leg_df[["Symbol", "Series", "ISIN"]].drop_duplicates(subset=["Symbol", "Series"])
                    full_df = full_df.drop(columns=["ISIN"], errors="ignore").merge(
                        isin_map, on=["Symbol", "Series"], how="left"
                    )
                return repair_ohlc(full_df), raw_full, url_full
            # Fallback when sec_bhavdata_full is missing/HTML on a trading day (e.g., 2022-08-08)
            if leg_df is not None:
                mto_df = await self._fetch_nse_mto_delivery(day)
                if mto_df is not None:
                    leg_df = leg_df.drop(columns=["DeliverableQty", "DeliveryPct"], errors="ignore").merge(
                        mto_df, on=["Symbol", "Series"], how="left"
                    )
                return repair_ohlc(leg_df), raw_leg, url_leg
            return None, None, url_full

        leg_df, raw_leg, url_leg = await self._fetch_nse_legacy_cm(day)
        if leg_df is None:
            return None, None, url_leg
        mto_df = await self._fetch_nse_mto_delivery(day)
        if mto_df is not None:
            leg_df = leg_df.drop(columns=["DeliverableQty", "DeliveryPct"], errors="ignore").merge(
                mto_df, on=["Symbol", "Series"], how="left"
            )
        return repair_ohlc(leg_df), raw_leg, url_leg

    async def seed_pre2016_via_upstox_cdn(
        self, start_year: int = 2008, end_year: int = 2015, max_symbols: int | None = None
    ) -> None:
        """
        100% Cloud/GitHub-Actions-compatible backfill for 2008–2015 NSE Equities.
        Chunks requests into 4-year windows (to respect Upstox V2's max range limit)
        and filters to EQ-series stocks that were actually listed on or before the window.
        """
        master_path = self.cfg.root_dir / "universe" / "nse_equity_master.parquet"
        etf_path = self.cfg.root_dir / "universe" / "nse_etf_master.parquet"
        if not master_path.exists() and not etf_path.exists():
            logger.warning("Run sync_security_masters() first so ISINs are available.")
            return

        frames_m = []
        if master_path.exists():
            df_eq = read_table_safe(master_path).copy()
            df_eq.columns = [c.strip() for c in df_eq.columns]
            if "SERIES" in df_eq.columns:
                df_eq = df_eq[df_eq["SERIES"].astype(str).str.strip().str.upper() == "EQ"].copy()
            frames_m.append(df_eq)

        if etf_path.exists():
            df_etf = read_table_safe(etf_path).copy()
            df_etf.columns = [c.strip() for c in df_etf.columns]
            sym_c = next((c for c in df_etf.columns if "SYMBOL" in c.upper()), None)
            isin_c = next((c for c in df_etf.columns if "ISIN" in c.upper()), None)
            list_c = next((c for c in df_etf.columns if "LISTING" in c.upper() or "DATE" in c.upper()), None)
            if sym_c and isin_c:
                etf_norm = pd.DataFrame({
                    "SYMBOL": df_etf[sym_c].astype(str).str.strip(),
                    "SERIES": "EQ",
                    "ISIN NUMBER": df_etf[isin_c].astype(str).str.strip(),
                    "DATE OF LISTING": df_etf[list_c] if list_c else pd.NA,
                })
                frames_m.append(etf_norm)

        if not frames_m:
            return
        mdf = pd.concat(frames_m, ignore_index=True)
        isin_col = next((c for c in mdf.columns if "ISIN" in c.upper()), None)
        list_col = next((c for c in mdf.columns if "LISTING" in c.upper()), None)
        if not isin_col:
            return

        # Filter out IPOs that listed after end_year
        if list_col:
            mdf["_list_dt"] = pd.to_datetime(mdf[list_col], format="mixed", dayfirst=True, errors="coerce")
            mdf = mdf[mdf["_list_dt"].isna() | (mdf["_list_dt"].dt.year <= end_year)].copy()

        records = mdf[["SYMBOL", "SERIES", isin_col]].dropna().to_dict("records")
        if max_symbols:
            records = records[:max_symbols]

        # Upstox daily candles start in 2008; chunk into 4-year windows so range is always <= 4 years
        eff_start = max(2008, start_year)
        windows: list[tuple[int, int]] = []
        y = eff_start
        while y <= end_year:
            w_end = min(end_year, y + 3)
            windows.append((y, w_end))
            y = w_end + 1

        sem = asyncio.Semaphore(self.cfg.archive_concurrency)

        for w_start, w_end in windows:
            logger.info("[Upstox Cloud Seed %d..%d] Fetching %d pre-%d listed EQ stocks...", w_start, w_end, len(records), w_end + 1)

            async def _fetch_isin(rec: dict, ws: int = w_start, we: int = w_end):
                sym = str(rec["SYMBOL"]).strip()
                ser = str(rec.get("SERIES", "EQ")).strip()
                isin = str(rec[isin_col]).strip()
                key = f"upstox_nse:{sym}:{ws}_{we}"
                if self.state.is_completed("upstox_cloud_seed", key):
                    return key, None, None
                url = (
                    f"https://api.upstox.com/v2/historical-candle/NSE_EQ%7C{isin}"
                    f"/day/{we}-12-31/{ws}-01-01"
                )
                async with sem:
                    try:
                        resp = await self.http.get(url, headers={"Accept": "application/json"})
                        await asyncio.sleep(0.15)
                        if resp.status_code != 200:
                            if resp.status_code in (400, 404):
                                self.state.record("upstox_cloud_seed", key, "HOLIDAY", source_url=url)
                            return key, None, url
                        candles = resp.json().get("data", {}).get("candles", [])
                        if not candles:
                            self.state.record("upstox_cloud_seed", key, "HOLIDAY", source_url=url)
                            return key, None, url
                        cdf = pd.DataFrame(
                            candles, columns=["Date", "Open", "High", "Low", "Close", "Volume", "OI"]
                        )
                        out = pd.DataFrame({
                            "Date": pd.to_datetime(cdf["Date"]).dt.tz_localize(None).dt.normalize(),
                            "Exchange": "NSE",
                            "InstrumentId": sym,
                            "Symbol": sym,
                            "Series": ser,
                            "ISIN": isin,
                            "Open": pd.to_numeric(cdf["Open"], errors="coerce"),
                            "High": pd.to_numeric(cdf["High"], errors="coerce"),
                            "Low": pd.to_numeric(cdf["Low"], errors="coerce"),
                            "Close": pd.to_numeric(cdf["Close"], errors="coerce"),
                            "Last": pd.to_numeric(cdf["Close"], errors="coerce"),
                            "PrevClose": pd.NA,
                            "Volume": pd.to_numeric(cdf["Volume"], errors="coerce"),
                            "TurnoverValue": pd.NA,
                            "Trades": pd.NA,
                            "DeliverableQty": pd.NA,
                            "DeliveryPct": pd.NA,
                        })
                        return key, repair_ohlc(out), url
                    except Exception as exc:
                        logger.debug("[Upstox Cloud Seed %s] %s", sym, exc)
                        return key, None, url

            for chunk_start in range(0, len(records), 50):
                batch_recs = records[chunk_start : chunk_start + 50]
                res = await asyncio.gather(*[_fetch_isin(r) for r in batch_recs])
                frames = [r[1] for r in res if r[1] is not None and not r[1].empty]
                if frames:
                    combined = pd.concat(frames, ignore_index=True)
                    for yr, grp in combined.groupby(combined["Date"].dt.year):
                        target = self.cfg.root_dir / "equities" / f"nse_cm_{int(yr)}.parquet"
                        upsert_parquet_partition(
                            grp,
                            target,
                            dedup_cols=["Date", "Exchange", "InstrumentId", "Series"],
                            sort_cols=["Symbol", "Date"],
                        )
                    for key, df_out, s_url in res:
                        if df_out is not None and not df_out.empty:
                            self.state.record("upstox_cloud_seed", key, "OK", row_count=len(df_out), source_url=s_url)
                    logger.info("[Upstox Cloud Seed %d..%d] Committed batch (%d symbols)", w_start, w_end, len(frames))

    async def backfill_nse_symbol_via_historical_or(
        self, symbol: str, start_year: int = 1995, end_year: int = 2015
    ) -> None:
        sym = symbol.strip().upper()
        for year in range(start_year, end_year + 1):
            key = f"{sym}:{year}"
            if self.state.is_completed("nse_historical_or_symbol", key):
                continue
            url = (
                "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
                f"?from=01-01-{year}&to=31-12-{year}&symbol={sym}"
                "&type=priceVolumeDeliverable&series=ALL&csv=true"
            )
            resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
            if resp.status_code != 200 or not resp.text.strip() or resp.text.lstrip().startswith("<"):
                continue
            df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
            df.columns = [c.strip() for c in df.columns]
            if df.empty or "Date" not in df.columns:
                self.state.record("nse_historical_or_symbol", key, "HOLIDAY", source_url=url)
                continue
            out = pd.DataFrame({
                "Date": pd.to_datetime(df["Date"].astype(str).str.strip(), format="mixed", dayfirst=True, errors="coerce"),
                "Exchange": "NSE",
                "InstrumentId": df["Symbol"].astype(str).str.strip(),
                "Symbol": df["Symbol"].astype(str).str.strip(),
                "Series": df["Series"].astype(str).str.strip(),
                "ISIN": pd.NA,
                "Open": pd.to_numeric(df["Open Price"].astype(str).str.replace(",", ""), errors="coerce"),
                "High": pd.to_numeric(df["High Price"].astype(str).str.replace(",", ""), errors="coerce"),
                "Low": pd.to_numeric(df["Low Price"].astype(str).str.replace(",", ""), errors="coerce"),
                "Close": pd.to_numeric(df["Close Price"].astype(str).str.replace(",", ""), errors="coerce"),
                "Last": pd.to_numeric(df["Last Price"].astype(str).str.replace(",", ""), errors="coerce"),
                "PrevClose": pd.to_numeric(df["Prev Close"].astype(str).str.replace(",", ""), errors="coerce"),
                "Volume": pd.to_numeric(df["Total Traded Quantity"].astype(str).str.replace(",", ""), errors="coerce"),
                "TurnoverValue": pd.to_numeric(
                    df.get("Turnover ₹", df.get("Turnover", pd.Series(dtype=str))).astype(str).str.replace(",", ""),
                    errors="coerce",
                ),
                "Trades": pd.to_numeric(
                    df.get("No. of Trades", pd.Series(dtype=str)).astype(str).str.replace(",", "").replace("-", pd.NA),
                    errors="coerce",
                ),
                "DeliverableQty": pd.to_numeric(
                    df.get("Deliverable Qty", pd.Series(dtype=str)).astype(str).str.replace(",", "").replace("-", pd.NA),
                    errors="coerce",
                ),
                "DeliveryPct": pd.to_numeric(
                    df.get("% Dly Qt to Traded Qty", pd.Series(dtype=str)).astype(str).str.replace(",", "").replace("-", pd.NA),
                    errors="coerce",
                ),
            }).dropna(subset=["Date"])
            out = repair_ohlc(out)
            target = self.cfg.root_dir / "equities" / f"nse_cm_{year}.parquet"
            upsert_parquet_partition(
                out, target, dedup_cols=["Date", "Exchange", "InstrumentId", "Series"], sort_cols=["Symbol", "Date"]
            )
            self.state.record("nse_historical_or_symbol", key, "OK", len(out), resp.content, url)
            await asyncio.sleep(0.35)

    async def _fetch_nse_sec_bhavdata_full(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{day:%d%m%Y}.csv"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403):
            return None, None, url
        # Guard against HTML error pages served with HTTP 200 (e.g. 2022-08-08)
        head_txt = resp.text[:250].lstrip()
        if head_txt.startswith("<") or "SYMBOL" not in head_txt.upper():
            return None, None, url

        try:
            df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
        except Exception:
            return None, None, url

        df.columns = [c.strip() for c in df.columns]
        for col in ("SYMBOL", "SERIES", "DATE1", "DELIV_QTY", "DELIV_PER"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        if df.empty or "DATE1" not in df.columns:
            return None, None, url

        try:
            file_date = datetime.strptime(df["DATE1"].iloc[0], "%d-%b-%Y").date()
        except ValueError:
            return None, None, url

        if file_date != day:
            return None, None, url

        df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"].replace("-", pd.NA), errors="coerce")
        df["DELIV_PER"] = pd.to_numeric(df["DELIV_PER"].replace("-", pd.NA), errors="coerce")
        df["TURNOVER_LACS"] = pd.to_numeric(df["TURNOVER_LACS"], errors="coerce")

        out = pd.DataFrame({
            "Date": pd.Timestamp(day),
            "Exchange": "NSE",
            "InstrumentId": df["SYMBOL"],
            "Symbol": df["SYMBOL"],
            "Series": df["SERIES"],
            "ISIN": pd.NA,
            "Open": pd.to_numeric(df["OPEN_PRICE"], errors="coerce"),
            "High": pd.to_numeric(df["HIGH_PRICE"], errors="coerce"),
            "Low": pd.to_numeric(df["LOW_PRICE"], errors="coerce"),
            "Close": pd.to_numeric(df["CLOSE_PRICE"], errors="coerce"),
            "Last": pd.to_numeric(df["LAST_PRICE"], errors="coerce"),
            "PrevClose": pd.to_numeric(df["PREV_CLOSE"], errors="coerce"),
            "Volume": pd.to_numeric(df["TTL_TRD_QNTY"], errors="coerce"),
            "TurnoverValue": df["TURNOVER_LACS"] * 100_000.0,
            "Trades": pd.to_numeric(df["NO_OF_TRADES"], errors="coerce"),
            "DeliverableQty": df["DELIV_QTY"],
            "DeliveryPct": df["DELIV_PER"],
        })
        return out, resp.content, url

    async def _fetch_nse_udiff(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
            return None, None, url
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            df = pd.read_csv(zf.open(zf.namelist()[0]), on_bad_lines="skip")
        return self._normalize_udiff(df, day, "NSE"), resp.content, url

    async def _fetch_nse_legacy_cm(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        mon = day.strftime("%b").upper()
        url = f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{day:%Y}/{mon}/cm{day:%d}{mon}{day:%Y}bhav.csv.zip"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
            return None, None, url
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            df = pd.read_csv(zf.open(zf.namelist()[0]), on_bad_lines="skip")
        df.columns = [c.strip() for c in df.columns]
        out = pd.DataFrame({
            "Date": pd.Timestamp(day),
            "Exchange": "NSE",
            "InstrumentId": df["SYMBOL"].astype(str).str.strip(),
            "Symbol": df["SYMBOL"].astype(str).str.strip(),
            "Series": df["SERIES"].astype(str).str.strip(),
            "ISIN": df["ISIN"].astype(str).str.strip() if "ISIN" in df.columns else pd.NA,
            "Open": pd.to_numeric(df["OPEN"], errors="coerce"),
            "High": pd.to_numeric(df["HIGH"], errors="coerce"),
            "Low": pd.to_numeric(df["LOW"], errors="coerce"),
            "Close": pd.to_numeric(df["CLOSE"], errors="coerce"),
            "Last": pd.to_numeric(df["LAST"], errors="coerce"),
            "PrevClose": pd.to_numeric(df["PREVCLOSE"], errors="coerce"),
            "Volume": pd.to_numeric(df["TOTTRDQTY"], errors="coerce"),
            "TurnoverValue": pd.to_numeric(df["TOTTRDVAL"], errors="coerce"),
            "Trades": pd.to_numeric(df["TOTALTRADES"], errors="coerce") if "TOTALTRADES" in df.columns else pd.NA,
            "DeliverableQty": pd.NA,
            "DeliveryPct": pd.NA,
        })
        return out, resp.content, url

    async def _fetch_nse_mto_delivery(self, day: date) -> pd.DataFrame | None:
        url = f"https://nsearchives.nseindia.com/archives/equities/mto/MTO_{day:%d%m%Y}.DAT"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403):
            return None
        raw_text = resp.text
        if "10,MTO" not in raw_text and "Security Wise Delivery" not in raw_text:
            return None
        rows = []
        for line in raw_text.splitlines():
            if not line.startswith("20,"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 7:
                rows.append({
                    "Symbol": parts[2],
                    "Series": parts[3],
                    "DeliverableQty": pd.to_numeric(parts[5], errors="coerce"),
                    "DeliveryPct": pd.to_numeric(parts[6], errors="coerce"),
                })
            elif len(parts) == 4:
                rows.append({
                    "Symbol": parts[1],
                    "Series": parts[2],
                    "DeliverableQty": pd.to_numeric(parts[3], errors="coerce"),
                    "DeliveryPct": pd.NA,
                })
        return pd.DataFrame(rows).drop_duplicates(subset=["Symbol", "Series"]) if rows else None

    async def fetch_bse_day(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        if day >= self.BSE_UDIFF_CUTOVER:
            url = f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{day:%Y%m%d}_F_0000.CSV"
            resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
            if resp.status_code in (404, 403):
                return None, None, url
            head_txt = resp.text[:250].lstrip()
            if head_txt.startswith("<") or "TradDt" not in head_txt:
                return None, None, url
            df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
            out = filter_bse_equity_only(self._normalize_udiff(df, day, "BSE"))
            raw_content = resp.content
        else:
            url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{day:%d%m%y}_CSV.ZIP"
            isin_url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{day:%d%m%y}.zip"
            resp, isin_resp = await asyncio.gather(
                self.http.get(url, headers=ARCHIVE_HEADERS),
                self.http.get(isin_url, headers=ARCHIVE_HEADERS),
            )
            # On Indian market holidays, BSE returns HTTP 200 with an HTML page instead of a PK zip!
            if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
                return None, None, url
            raw_content = resp.content

            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                raw_csv = zf.open(zf.namelist()[0]).read().decode("utf-8", errors="replace")
            repaired_csv = _BSE_GLUED_LINE_RE.sub(r"\1,\n\2", raw_csv)
            df = pd.read_csv(io.StringIO(repaired_csv), on_bad_lines="skip")
            df.columns = [c.strip() for c in df.columns]
            if "SC_TYPE" in df.columns:
                df = df[df["SC_TYPE"].astype(str).str.strip() == "Q"].copy()

            out = pd.DataFrame({
                "Date": pd.Timestamp(day),
                "Exchange": "BSE",
                "InstrumentId": df["SC_CODE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
                "Symbol": df["SC_NAME"].astype(str).str.strip(),
                "Series": df["SC_GROUP"].astype(str).str.strip(),
                "ISIN": pd.NA,
                "Open": pd.to_numeric(df["OPEN"], errors="coerce"),
                "High": pd.to_numeric(df["HIGH"], errors="coerce"),
                "Low": pd.to_numeric(df["LOW"], errors="coerce"),
                "Close": pd.to_numeric(df["CLOSE"], errors="coerce"),
                "Last": pd.to_numeric(df["LAST"], errors="coerce"),
                "PrevClose": pd.to_numeric(df["PREVCLOSE"], errors="coerce"),
                "Volume": pd.to_numeric(df["NO_OF_SHRS"], errors="coerce"),
                "TurnoverValue": pd.to_numeric(df["NET_TURNOV"], errors="coerce"),
                "Trades": pd.to_numeric(df["NO_TRADES"], errors="coerce"),
                "DeliverableQty": pd.NA,
                "DeliveryPct": pd.NA,
            })

            if isin_resp.status_code == 200 and isin_resp.content.startswith(b"PK"):
                try:
                    with zipfile.ZipFile(io.BytesIO(isin_resp.content)) as izf:
                        idf = pd.read_csv(izf.open(izf.namelist()[0]), on_bad_lines="skip")
                    idf.columns = [c.strip() for c in idf.columns]
                    if "SC_CODE" in idf.columns and "ISIN_CODE" in idf.columns:
                        imap = pd.DataFrame({
                            "InstrumentId": idf["SC_CODE"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
                            "ISIN": idf["ISIN_CODE"].astype(str).str.strip(),
                        }).drop_duplicates(subset=["InstrumentId"])
                        out = out.drop(columns=["ISIN"], errors="ignore").merge(imap, on="InstrumentId", how="left")
                except Exception:
                    pass

            out = filter_bse_equity_only(out)

        deliv_df = await self._fetch_bse_delivery(day)
        if deliv_df is not None and not deliv_df.empty:
            out = out.drop(columns=["DeliverableQty", "DeliveryPct"], errors="ignore").merge(
                deliv_df, on="InstrumentId", how="left"
            )
        return repair_ohlc(out), raw_content, url

    async def _fetch_bse_delivery(self, day: date) -> pd.DataFrame | None:
        url = f"https://www.bseindia.com/BSEDATA/gross/{day:%Y}/SCBSEALL{day:%d%m}.zip"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
            return None
        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                raw_txt = zf.open(zf.namelist()[0]).read().decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(raw_txt), sep="|", on_bad_lines="skip")
            df.columns = [c.strip().upper() for c in df.columns]
            scrip_col = next((c for c in df.columns if "SCRIP" in c), None)
            qty_col = next((c for c in df.columns if "DELIVERY" in c and "QTY" in c), None)
            pct_col = next((c for c in df.columns if "DELV" in c and "PER" in c), None)
            if not scrip_col or not qty_col:
                return None
            return pd.DataFrame({
                "InstrumentId": df[scrip_col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
                "DeliverableQty": pd.to_numeric(df[qty_col], errors="coerce"),
                "DeliveryPct": pd.to_numeric(df[pct_col], errors="coerce") if pct_col else pd.NA,
            }).drop_duplicates(subset=["InstrumentId"])
        except Exception:
            return None

    @staticmethod
    def _normalize_udiff(df: pd.DataFrame, day: date, exchange: str) -> pd.DataFrame:
        df.columns = [c.strip() for c in df.columns]
        if "Sgmt" in df.columns:
            df = df[df["Sgmt"].astype(str).str.strip() == "CM"].copy()
        sym_s = df["TckrSymb"].astype(str).str.strip()
        inst_id = (
            sym_s
            if exchange.upper() == "NSE"
            else df["FinInstrmId"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        )
        return pd.DataFrame({
            "Date": pd.Timestamp(day),
            "Exchange": exchange,
            "InstrumentId": inst_id,
            "Symbol": sym_s,
            "Series": df["SctySrs"].astype(str).str.strip(),
            "ISIN": df["ISIN"].astype(str).str.strip(),
            "Open": pd.to_numeric(df["OpnPric"], errors="coerce"),
            "High": pd.to_numeric(df["HghPric"], errors="coerce"),
            "Low": pd.to_numeric(df["LwPric"], errors="coerce"),
            "Close": pd.to_numeric(df["ClsPric"], errors="coerce"),
            "Last": pd.to_numeric(df["LastPric"], errors="coerce"),
            "PrevClose": pd.to_numeric(df["PrvsClsgPric"], errors="coerce"),
            "Volume": pd.to_numeric(df["TtlTradgVol"], errors="coerce"),
            "TurnoverValue": pd.to_numeric(df["TtlTrfVal"], errors="coerce"),
            "Trades": pd.to_numeric(df["TtlNbOfTxsExctd"], errors="coerce"),
            "DeliverableQty": pd.NA,
            "DeliveryPct": pd.NA,
        })
