from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from datetime import date, timedelta
import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import upsert_parquet_partition

logger = logging.getLogger("datalake.fo_eod")


class DerivativesEODScraper:
    """
    Scrapes full historical NSE (2000–Present) and BSE (2024–Present) F&O Bhavcopies
    (Index/Stock Futures & Option Chains with Strike, Expiry, CE/PE, OHLC, SettlePrice, OI, ChangeInOI).
    """

    FO_UDIFF_CUTOVER = date(2024, 7, 8)
    NSE_FO_START = date(2000, 6, 12)
    BSE_FO_START = date(2024, 7, 8)

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_range(self, start_date: date, end_date: date, exchanges: tuple[str, ...] = ("NSE",)) -> None:
        sem = asyncio.Semaphore(self.cfg.archive_concurrency)
        freeze_cutoff = today_ist() - timedelta(days=self.cfg.holiday_freeze_days)

        for exch in exchanges:
            dataset = f"fo_eod_{exch.lower()}"
            done_keys = self.state.completed_keys(dataset)
            exch_min_date = self.BSE_FO_START if exch.upper() == "BSE" else self.NSE_FO_START
            eff_start = max(start_date, exch_min_date)
            if eff_start > end_date:
                continue

            for year in range(eff_start.year, end_date.year + 1):
                yr_start = max(eff_start, date(year, 1, 1))
                yr_end = min(end_date, date(year, 12, 31))
                pending = [
                    yr_start + timedelta(days=i)
                    for i in range((yr_end - yr_start).days + 1)
                    if (yr_start + timedelta(days=i)).weekday() < 5
                    and (yr_start + timedelta(days=i)).isoformat() not in done_keys
                ]
                if not pending:
                    continue

                logger.info("[F&O %s %d] Fetching %d pending weekdays in 22-day micro-batches...", exch, year, len(pending))

                async def _worker(d: date):
                    async with sem:
                        try:
                            if exch == "NSE":
                                df, raw_b, url = await self.fetch_nse_fo_day(d)
                            else:
                                df, raw_b, url = await self.fetch_bse_fo_day(d)
                            await asyncio.sleep(self.cfg.polite_delay_sec)
                            return d, df, raw_b, url, None
                        except Exception as exc:
                            return d, None, None, None, exc

                for chunk_start in range(0, len(pending), 22):
                    batch = pending[chunk_start : chunk_start + 22]
                    results = await asyncio.gather(*[_worker(d) for d in batch])
                    frames: list[pd.DataFrame] = []
                    ok_records: list[tuple[str, int, bytes | None, str | None]] = []

                    for d, df, raw_b, url, err in results:
                        key = d.isoformat()
                        if err:
                            logger.warning("[F&O %s %s] Failed: %s", exch, key, err)
                            self.state.record(dataset, key, "FAILED", source_url=url)
                        elif df is None or df.empty:
                            if d <= freeze_cutoff:
                                self.state.record(dataset, key, "HOLIDAY", source_url=url)
                        else:
                            frames.append(df)
                            ok_records.append((key, len(df), raw_b, url))

                    if frames:
                        combined = pd.concat(frames, ignore_index=True)
                        for (yr, mo), grp in combined.groupby([combined["Date"].dt.year, combined["Date"].dt.month]):
                            target = self.cfg.root_dir / "derivatives" / f"{exch.lower()}_fo_{int(yr)}_{int(mo):02d}.parquet"
                            upsert_parquet_partition(
                                grp,
                                target,
                                dedup_cols=["Date", "Exchange", "InstrumentType", "Symbol", "ExpiryDate", "StrikePrice", "OptionType"],
                                sort_cols=["Symbol", "Date", "ExpiryDate", "StrikePrice", "OptionType"],
                            )
                        for key, r_cnt, raw_b, s_url in ok_records:
                            self.state.record(dataset, key, "OK", r_cnt, raw_b, s_url)

    async def fetch_nse_fo_day(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        if day < self.NSE_FO_START:
            return None, None, ""
        if day >= self.FO_UDIFF_CUTOVER:
            url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{day:%Y%m%d}_F_0000.csv.zip"
            resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
            if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
                return None, None, url
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                df = pd.read_csv(zf.open(zf.namelist()[0]), on_bad_lines="skip")
            return self._normalize_fo_udiff(df, day, "NSE"), resp.content, url

        mon = day.strftime("%b").upper()
        url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{day:%Y}/{mon}/fo{day:%d}{mon}{day:%Y}bhav.csv.zip"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403) or not resp.content.startswith(b"PK"):
            return None, None, url
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            raw_csv = zf.open(zf.namelist()[0]).read().decode("utf-8", errors="replace")

        lines = [
            ln for i, ln in enumerate(raw_csv.splitlines())
            if i == 0 or not ln.strip().upper().startswith("INSTRUMENT,")
        ]
        df = pd.read_csv(io.StringIO("\n".join(lines)), on_bad_lines="skip")
        df.columns = [c.strip().upper() for c in df.columns]
        opt_col = "OPTION_TYP" if "OPTION_TYP" in df.columns else "OPTIONTYPE"

        out = pd.DataFrame({
            "Date": pd.Timestamp(day),
            "Exchange": "NSE",
            "InstrumentType": df["INSTRUMENT"].astype(str).str.strip(),
            "Symbol": df["SYMBOL"].astype(str).str.strip(),
            "ExpiryDate": pd.to_datetime(df["EXPIRY_DT"], format="%d-%b-%Y", errors="coerce"),
            "StrikePrice": pd.to_numeric(df["STRIKE_PR"], errors="coerce").fillna(0.0).astype("float64"),
            "OptionType": df[opt_col].fillna("XX").astype(str).str.strip(),
            "Open": pd.to_numeric(df["OPEN"], errors="coerce").astype("float64"),
            "High": pd.to_numeric(df["HIGH"], errors="coerce").astype("float64"),
            "Low": pd.to_numeric(df["LOW"], errors="coerce").astype("float64"),
            "Close": pd.to_numeric(df["CLOSE"], errors="coerce").astype("float64"),
            "SettlePrice": pd.to_numeric(df["SETTLE_PR"], errors="coerce").astype("float64"),
            "UnderlyingPrice": pd.Series(float("nan"), index=df.index, dtype="float64"),
            "Contracts": pd.to_numeric(df["CONTRACTS"], errors="coerce").astype("float64"),
            "TurnoverValue": (pd.to_numeric(df["VAL_INLAKH"], errors="coerce") * 100_000.0).astype("float64"),
            "OpenInterest": pd.to_numeric(df["OPEN_INT"], errors="coerce").astype("float64"),
            "ChangeInOI": pd.to_numeric(df["CHG_IN_OI"], errors="coerce").astype("float64"),
        })
        for col in ("Open", "High", "Low"):
            z = (out[col].isna()) | (out[col] <= 0)
            out.loc[z, col] = out.loc[z, "Close"]
        out = out[(out["Contracts"] > 0) | (out["OpenInterest"] > 0)]
        out = out.drop_duplicates(
            subset=["Date", "InstrumentType", "Symbol", "ExpiryDate", "StrikePrice", "OptionType"]
        ).reset_index(drop=True)
        return out, resp.content, url

    async def fetch_bse_fo_day(self, day: date) -> tuple[pd.DataFrame | None, bytes | None, str]:
        if day < self.FO_UDIFF_CUTOVER:
            return None, None, ""
        url = f"https://www.bseindia.com/download/BhavCopy/Derivative/BhavCopy_BSE_FO_0_0_0_{day:%Y%m%d}_F_0000.CSV"
        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
        if resp.status_code in (404, 403):
            return None, None, url
        head_txt = resp.text[:250].lstrip()
        if head_txt.startswith("<") or "TradDt" not in head_txt:
            return None, None, url
        df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
        return self._normalize_fo_udiff(df, day, "BSE"), resp.content, url

    @staticmethod
    def _normalize_fo_udiff(df: pd.DataFrame, day: date, exchange: str) -> pd.DataFrame:
        df.columns = [c.strip() for c in df.columns]
        out = pd.DataFrame({
            "Date": pd.Timestamp(day),
            "Exchange": exchange,
            "InstrumentType": df["FinInstrmTp"].astype(str).str.strip(),
            "Symbol": df["TckrSymb"].astype(str).str.strip(),
            "ExpiryDate": pd.to_datetime(df["XpryDt"], errors="coerce"),
            "StrikePrice": pd.to_numeric(df["StrkPric"], errors="coerce").fillna(0.0).astype("float64"),
            "OptionType": df["OptnTp"].fillna("XX").astype(str).str.strip(),
            "Open": pd.to_numeric(df["OpnPric"], errors="coerce").astype("float64"),
            "High": pd.to_numeric(df["HghPric"], errors="coerce").astype("float64"),
            "Low": pd.to_numeric(df["LwPric"], errors="coerce").astype("float64"),
            "Close": pd.to_numeric(df["ClsPric"], errors="coerce").astype("float64"),
            "SettlePrice": pd.to_numeric(df["SttlmPric"], errors="coerce").astype("float64"),
            "UnderlyingPrice": pd.to_numeric(df["UndrlygPric"], errors="coerce").astype("float64"),
            "Contracts": pd.to_numeric(df["TtlTradgVol"], errors="coerce").astype("float64"),
            "TurnoverValue": pd.to_numeric(df["TtlTrfVal"], errors="coerce").astype("float64"),
            "OpenInterest": pd.to_numeric(df["OpnIntrst"], errors="coerce").astype("float64"),
            "ChangeInOI": pd.to_numeric(df["ChngInOpnIntrst"], errors="coerce").astype("float64"),
        })
        for col in ("Open", "High", "Low"):
            z = (out[col].isna()) | (out[col] <= 0)
            out.loc[z, col] = out.loc[z, "Close"]
        return out[(out["Contracts"] > 0) | (out["OpenInterest"] > 0)].reset_index(drop=True)
