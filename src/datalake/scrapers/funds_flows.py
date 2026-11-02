from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import date, timedelta

import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import upsert_parquet_partition

logger = logging.getLogger("datalake.funds_flows")

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

AMFI_DAILY_MIRRORS = (
    "https://portal.amfiindia.com/spages/NAVAll.txt",
    "https://www.amfiindia.com/spages/NAVAll.txt",
    "https://portal.amfiindia.com/spages/NavOpen.txt",
)

# Benchmark flagship schemes (active since 2006) used when AMFI portal is under weekend maintenance
CORE_MFAPI_SCHEMES = (
    119062,  # HDFC Large and Mid Cap Fund
    120503,  # Axis ELSS Tax Saver Fund
    122639,  # Parag Parikh Flexi Cap Fund
    118989,  # HDFC Flexi Cap Fund (2006+ history)
    100122,  # HDFC Balanced Advantage Fund (2006+ history)
)


class FundsAndFlowsScraper:
    """Scrapes AMFI Mutual Fund NAVs (2006–Present, with MFapi.in failover) and NSE F&O Participant OI."""

    AMFI_HIST_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_amfi_daily(self) -> None:
        df = pd.DataFrame()
        used_url = AMFI_DAILY_MIRRORS[0]
        raw_content = None

        for url in AMFI_DAILY_MIRRORS:
            try:
                resp = await self.http.get(url, headers=ARCHIVE_HEADERS, timeout=90.0)
                if resp.status_code == 200 and ";" in resp.text:
                    parsed = self._parse_amfi_text(resp.text)
                    if len(parsed) >= 100:
                        df = parsed
                        used_url = url
                        raw_content = resp.content
                        break
            except Exception as exc:
                logger.debug("[AMFI Mirror %s] %s", url, exc)

        # Weekend maintenance failover: fetch full multi-decade NAV history from api.mfapi.in
        if df.empty:
            logger.info("[AMFI Daily] AMFI text portal unavailable; syncing flagship schemes via api.mfapi.in...")
            df = await self.sync_schemes_via_mfapi(CORE_MFAPI_SCHEMES)
            used_url = "https://api.mfapi.in/mf"

        if df.empty:
            return

        for year, grp in df.groupby(df["Date"].dt.year):
            target = self.cfg.root_dir / "mutual_funds" / f"amfi_nav_{int(year)}.parquet"
            upsert_parquet_partition(
                grp, target, dedup_cols=["Date", "SchemeCode"], sort_cols=["SchemeCode", "Date"]
            )
        max_dt = df["Date"].max().date().isoformat()
        self.state.record("amfi_nav_daily", max_dt, "OK", len(df), raw_content, used_url)
        logger.info("[AMFI NAVs] Ingested %d scheme NAV rows (latest date: %s)", len(df), max_dt)

    async def sync_schemes_via_mfapi(
        self,
        scheme_codes: tuple[int, ...] = CORE_MFAPI_SCHEMES,
        year_filter: int | None = None,
    ) -> pd.DataFrame:
        """Fetch historical NAVs from api.mfapi.in (optionally restricted to `year_filter`)."""
        frames: list[pd.DataFrame] = []
        for code in scheme_codes:
            url = f"https://api.mfapi.in/mf/{code}"
            try:
                resp = await self.http.get(url, headers={"Accept": "application/json"}, timeout=60.0)
                if resp.status_code != 200:
                    continue
                payload = resp.json()
                meta = payload.get("meta") or {}
                data = payload.get("data") or []
                if not data:
                    continue
                sdf = pd.DataFrame(data)
                sdf["Date"] = pd.to_datetime(sdf["date"], format="%d-%m-%Y", errors="coerce")
                sdf["NAV"] = pd.to_numeric(sdf["nav"], errors="coerce")
                sdf = sdf.dropna(subset=["Date", "NAV"])
                sdf = sdf[sdf["NAV"] > 0.0].copy()
                if year_filter is not None:
                    sdf = sdf[sdf["Date"].dt.year == year_filter].copy()
                if sdf.empty:
                    continue
                sdf["SchemeCode"] = meta.get("scheme_code", code)
                sdf["SchemeName"] = meta.get("scheme_name", str(code))
                sdf["AMC"] = meta.get("fund_house")
                sdf["Category"] = meta.get("scheme_category")
                sdf["ISINGrowth"] = meta.get("isin_growth")
                sdf["ISINReinvest"] = meta.get("isin_div_reinvestment")
                frames.append(
                    sdf[["SchemeCode", "SchemeName", "AMC", "Category", "ISINGrowth", "ISINReinvest", "NAV", "Date"]]
                )
            except Exception as exc:
                logger.debug("[MFapi %s] %s", code, exc)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        for year, grp in combined.groupby(combined["Date"].dt.year):
            target = self.cfg.root_dir / "mutual_funds" / f"amfi_nav_{int(year)}.parquet"
            upsert_parquet_partition(
                grp, target, dedup_cols=["Date", "SchemeCode"], sort_cols=["SchemeCode", "Date"]
            )
        return combined

    async def backfill_amfi_history(
        self, start_date: date = date(2006, 4, 1), end_date: date | None = None, force: bool = False
    ) -> None:
        """Walks AMFI historical NAVs in 30-day windows with bounded concurrency and batch Parquet writes."""
        end = end_date or today_ist()
        cur = start_date
        pending_windows: list[tuple[date, date, str]] = []
        while cur <= end:
            win_end = min(end, cur + timedelta(days=29))
            key = f"{cur.isoformat()}_{win_end.isoformat()}"
            if not force and self.state.is_completed("amfi_nav_window", key):
                cur = win_end + timedelta(days=1)
                continue
            pending_windows.append((cur, win_end, key))
            cur = win_end + timedelta(days=1)

        if not pending_windows:
            return

        sem = asyncio.Semaphore(3)

        async def _fetch_window(w_start: date, w_end: date, key: str):
            params = {"frmdt": w_start.strftime("%d-%b-%Y"), "todt": w_end.strftime("%d-%b-%Y")}
            async with sem:
                try:
                    resp = await self.http.get(
                        self.AMFI_HIST_URL, headers=ARCHIVE_HEADERS, params=params, timeout=120.0
                    )
                    resp.raise_for_status()
                    df = self._parse_amfi_text(resp.text)
                    await asyncio.sleep(0.3)
                    return key, df, resp.content, None
                except Exception as exc:
                    return key, pd.DataFrame(), None, exc

        for chunk_start in range(0, len(pending_windows), 6):
            batch = pending_windows[chunk_start : chunk_start + 6]
            results = await asyncio.gather(*[_fetch_window(ws, we, k) for ws, we, k in batch])
            frames: list[pd.DataFrame] = []
            ok_windows: list[tuple[str, int, bytes | None]] = []

            for key, df, raw_b, err in results:
                if err is not None:
                    logger.warning("[AMFI History %s] Failed: %s", key, err)
                elif not df.empty:
                    frames.append(df)
                    ok_windows.append((key, len(df), raw_b))

            if frames:
                combined = pd.concat(frames, ignore_index=True)
                for year, grp in combined.groupby(combined["Date"].dt.year):
                    target = self.cfg.root_dir / "mutual_funds" / f"amfi_nav_{int(year)}.parquet"
                    upsert_parquet_partition(
                        grp, target, dedup_cols=["Date", "SchemeCode"], sort_cols=["SchemeCode", "Date"]
                    )
                for key, r_cnt, raw_b in ok_windows:
                    self.state.record("amfi_nav_window", key, "OK", r_cnt, raw_b)
                    logger.info("[AMFI History %s] Saved %d rows", key, r_cnt)

    @staticmethod
    def _parse_amfi_text(raw_text: str) -> pd.DataFrame:
        amc = None
        cat = None
        rows = []
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("Scheme Code;"):
                continue
            if ";" not in line:
                if line.startswith(("Open Ended", "Close Ended", "Interval Fund")):
                    cat = line
                else:
                    amc = line
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) == 6:
                code, isin_g, isin_r, name, nav_s, dt_s = parts
            elif len(parts) >= 8:
                if parts[1] == "-" or ISIN_RE.match(parts[1]):
                    # NAVAll.txt: code;ISIN growth;ISIN reinvest;name;plan;option;NAV;date
                    code, isin_g, isin_r, name, plan, option, nav_s, dt_s = parts[:8]
                    name = " - ".join(p for p in (name, plan, option) if p)
                else:
                    # DownloadNAVHistoryReport_Po.aspx current format:
                    # Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date
                    # Or legacy 8-column format:
                    # Scheme Code;Scheme Name;ISIN Growth;ISIN Reinvest;Net Asset Value;Repurchase Price;Sale Price;Date
                    p6 = parts[6]
                    p4 = parts[4]
                    is_p6_float = False
                    if p6 and p6 != "-":
                        try:
                            float(p6)
                            is_p6_float = True
                        except ValueError:
                            pass
                    if is_p6_float or (not p4 or p4 == "-" or ISIN_RE.match(p4)):
                        code, name, plan, option, isin_g, isin_r, nav_s, dt_s = parts[:8]
                        if plan or option:
                            suffix = " - ".join(p for p in (plan, option) if p and p.lower() not in name.lower())
                            if suffix:
                                name = f"{name} - {suffix}"
                    else:
                        code, name, isin_g, isin_r, nav_s, _, _, dt_s = parts[:8]
            else:
                continue
            try:
                nav = float(nav_s)
            except ValueError:
                continue
            if nav <= 0.0 or not dt_s or not code.isdigit():
                continue
            rows.append({
                "SchemeCode": int(code),
                "SchemeName": name,
                "AMC": amc,
                "Category": cat,
                "ISINGrowth": None if isin_g in ("-", "") else isin_g,
                "ISINReinvest": None if isin_r in ("-", "") else isin_r,
                "NAV": nav,
                "Date": dt_s,
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            df["Date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=True, errors="coerce")
            df = df.dropna(subset=["Date"])
        return df

    async def sync_participant_oi(self, start_date: date, end_date: date, force: bool = False) -> None:
        dataset = "nse_participant_oi"
        done_keys = set() if force else self.state.completed_keys(dataset)
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

            async def _fetch(d: date):
                url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{d:%d%m%Y}.csv"
                async with sem:
                    try:
                        resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
                        await asyncio.sleep(self.cfg.polite_delay_sec)
                        if resp.status_code in (404, 403):
                            return d, None, None, url, None
                        head_txt = resp.text[:250].lstrip()
                        if head_txt.startswith("<") or "Client Type" not in head_txt:
                            return d, None, None, url, None
                        lines = [ln for ln in resp.text.splitlines() if ln.strip()]
                        header_idx = next((i for i, ln in enumerate(lines) if "Client Type" in ln), 0)
                        df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])), on_bad_lines="skip")
                        df.columns = [c.strip() for c in df.columns]
                        df = df[df["Client Type"].astype(str).str.strip().isin(["Client", "DII", "FII", "Pro"])].copy()
                        df["Client Type"] = df["Client Type"].astype(str).str.strip()
                        for col in df.columns:
                            if col not in ("Client Type", "Date"):
                                df[col] = pd.to_numeric(
                                    df[col].astype(str).str.replace(",", "").str.strip().replace("-", pd.NA),
                                    errors="coerce",
                                )
                        df["Date"] = pd.Timestamp(d)
                        return d, df, resp.content, url, None
                    except Exception as exc:
                        return d, None, None, url, exc

            for chunk_start in range(0, len(pending), 22):
                batch = pending[chunk_start : chunk_start + 22]
                results = await asyncio.gather(*[_fetch(d) for d in batch])
                frames = []
                ok_records = []

                for d, df, raw_b, url, err in results:
                    key = d.isoformat()
                    if err:
                        self.state.record(dataset, key, "FAILED", source_url=url)
                    elif df is None or df.empty:
                        if d <= freeze_cutoff:
                            self.state.record(dataset, key, "HOLIDAY", source_url=url)
                    else:
                        frames.append(df)
                        ok_records.append((key, len(df), raw_b, url))

                if frames:
                    target = self.cfg.root_dir / "flows" / f"fao_participant_oi_{year}.parquet"
                    upsert_parquet_partition(
                        pd.concat(frames, ignore_index=True),
                        target,
                        dedup_cols=["Date", "Client Type"],
                        sort_cols=["Date", "Client Type"],
                    )
                    for key, r_cnt, raw_b, s_url in ok_records:
                        self.state.record(dataset, key, "OK", r_cnt, raw_b, s_url)
