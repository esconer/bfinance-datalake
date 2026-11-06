from __future__ import annotations

import io
import logging
import re
from datetime import date
import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, BSE_API_HEADERS, NSE_API_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import upsert_parquet_partition, write_parquet_atomic

logger = logging.getLogger("datalake.universe_actions")

NIFTY_CONSTITUENT_URLS = {
    "NIFTY 50": "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "NIFTY NEXT 50": "https://nsearchives.nseindia.com/content/indices/ind_niftynext50list.csv",
    "NIFTY 100": "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv",
    "NIFTY 200": "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv",
    "NIFTY 500": "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "NIFTY MIDCAP 150": "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    "NIFTY SMALLCAP 250": "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    "NIFTY BANK": "https://nsearchives.nseindia.com/content/indices/ind_niftybanklist.csv",
    "NIFTY IT": "https://nsearchives.nseindia.com/content/indices/ind_niftyitlist.csv",
    "NIFTY PHARMA": "https://nsearchives.nseindia.com/content/indices/ind_niftypharmalist.csv",
    "NIFTY AUTO": "https://nsearchives.nseindia.com/content/indices/ind_niftyautolist.csv",
    "NIFTY FMCG": "https://nsearchives.nseindia.com/content/indices/ind_niftyfmcglist.csv",
    "NIFTY METAL": "https://nsearchives.nseindia.com/content/indices/ind_niftymetallist.csv",
    "NIFTY ENERGY": "https://nsearchives.nseindia.com/content/indices/ind_niftyenergylist.csv",
    "NIFTY REALTY": "https://nsearchives.nseindia.com/content/indices/ind_niftyrealtylist.csv",
}

_RS_DASH_GAP_RE = re.compile(r"((?:rs|re|inr|₹)\.?)[\s]*-[\s]*(?=\d)", re.I)
_NON_EQUITY_ISSUE_RE = re.compile(
    r"\b(?:debenture|ncd|ccd|ccps|preference|ncrps|dvr|warrant|bond)\b", re.I
)
_FV_EXTRACT_RE = re.compile(
    r"\b(?:face\s*value|fv|f\.v\.)\b\s*(?:of\s+)?(?:rs\.?|re\.?|₹|inr)?\s*[-:]?\s*(\d+(?:\.\d+)?)",
    re.I,
)
_FV_NOISE_RE = re.compile(
    r"\(?[,\s]*(?:\b(?:on|of)\s+)?\b(?:face\s*value|fv|f\.v\.)\b\s*(?:of\s+)?(?:rs\.?|re\.?|₹|inr)?\s*[-:]?\s*\d+(?:\.\d+)?(?:\s*/-)?(?:\s*(?:each|per\s+(?:share|unit|security)))?\s*\)?",
    re.I,
)
_PCT_DIV_RE = re.compile(
    r"\b(?:dividend|div)\b[^₹\d]*?(?:@\s*)?(\d+(?:\.\d+)?)\s*%",
    re.I,
)
_DIV_RE = re.compile(
    r"\b(?:dividend|div|interest|income\s+distribution|dist)\b.*?(?:rs\.?|re\.?|₹|inr)\s*[-:]?\s*(\d+(?:\.\d+)?)",
    re.I,
)
_BONUS_RE = re.compile(r"\bbonus\b.*?(\d+)\s*:\s*(\d+)", re.I)
_SPLIT_RE = re.compile(
    r"(?:split|sub[-\s]*division).*?(?:from\s+)?(?:rs\.?|re\.?|₹|inr)?\s*(\d+(?:\.\d+)?)\s*(?:/-)?(?:\s*(?:per\s+(?:share|unit|security)|each))?\s*(?:to|into)\s*(?:rs\.?|re\.?|₹|inr)?\s*(\d+(?:\.\d+)?)",
    re.I,
)
_RIGHTS_RE = re.compile(r"\brights?\b.*?(\d+)\s*:\s*(\d+)", re.I)


def parse_action_purpose(text: str, face_value: float | None = None) -> dict:
    """Parse raw NSE/BSE corporate action subject into structured numerical fields."""
    s = _RS_DASH_GAP_RE.sub(r"\1 ", str(text or "").strip())
    out = {
        "ActionType": "OTHER",
        "DividendPerShareRs": None,
        "BonusNew": None,
        "BonusHeld": None,
        "SplitFromFV": None,
        "SplitToFV": None,
        "ShareMultiplier": 1.0,
    }
    if _NON_EQUITY_ISSUE_RE.search(s) and re.search(r"\b(?:bonus|rights?)\b", s, re.I):
        out["ActionType"] = "SKIP_NON_EQUITY"
        return out

    fv_match = _FV_EXTRACT_RE.search(s)
    detected_fv = float(fv_match.group(1)) if fv_match else face_value

    clauses = re.split(r"[/;]|(?:\band\b)", s, flags=re.I)
    for clause in clauses:
        if re.search(r"buy\s*back|\brights?\b|split|sub[-\s]*division", clause, re.I):
            continue
        clean_clause = _FV_NOISE_RE.sub(" ", clause)
        found_rs_div = False
        for m_div in _DIV_RE.finditer(clean_clause):
            val = float(m_div.group(1))
            out["DividendPerShareRs"] = (out["DividendPerShareRs"] or 0.0) + val
            out["ActionType"] = "DIVIDEND"
            found_rs_div = True
        if not found_rs_div and detected_fv and detected_fv > 0:
            for m_pct in _PCT_DIV_RE.finditer(clean_clause):
                pct = float(m_pct.group(1))
                out["DividendPerShareRs"] = round((out["DividendPerShareRs"] or 0.0) + (pct / 100.0) * detected_fv, 4)
                out["ActionType"] = "DIVIDEND"

    m_bon = _BONUS_RE.search(s)
    if m_bon:
        new_sh, held_sh = float(m_bon.group(1)), float(m_bon.group(2))
        if held_sh > 0:
            out["BonusNew"] = new_sh
            out["BonusHeld"] = held_sh
            out["ShareMultiplier"] *= (new_sh + held_sh) / held_sh
            out["ActionType"] = "BONUS"

    m_spl = _SPLIT_RE.search(s)
    if m_spl:
        old_fv, new_fv = float(m_spl.group(1)), float(m_spl.group(2))
        if new_fv > 0 and old_fv > new_fv:
            out["SplitFromFV"] = old_fv
            out["SplitToFV"] = new_fv
            out["ShareMultiplier"] *= old_fv / new_fv
            out["ActionType"] = "SPLIT"

    if out["ActionType"] == "OTHER":
        if _RIGHTS_RE.search(s):
            out["ActionType"] = "RIGHTS"
        elif re.search(r"demerger|spin[-\s]*off|scheme", s, re.I):
            out["ActionType"] = "DEMERGER"
        elif re.search(r"buy\s*back", s, re.I):
            out["ActionType"] = "BUYBACK"

    return out


ACTION_NUMERIC_COLS: tuple[str, ...] = (
    "DividendPerShareRs",
    "BonusNew",
    "BonusHeld",
    "SplitFromFV",
    "SplitToFV",
    "ShareMultiplier",
)


def _normalize_action_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all parsed corporate action numeric columns have strict float64 dtype."""
    out = df.copy()
    for col in ACTION_NUMERIC_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
        else:
            out[col] = pd.Series(
                1.0 if col == "ShareMultiplier" else float("nan"),
                index=out.index,
                dtype="float64",
            )
    out["ShareMultiplier"] = out["ShareMultiplier"].fillna(1.0).astype("float64")
    if "ActionType" in out.columns:
        out["ActionType"] = out["ActionType"].fillna("OTHER").astype(str)
    return out


class UniverseAndActionsScraper:
    """Syncs NSE/BSE Security Masters, Symbol Rename History, PIT Nifty Constituents, and 30Y Corporate Actions."""

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_security_masters(self) -> None:
        for name, url in (
            ("nse_equity_master.parquet", "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"),
            ("nse_sme_master.parquet", "https://nsearchives.nseindia.com/content/equities/SME_EQUITY_L.csv"),
            ("nse_etf_master.parquet", "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv"),
            ("nse_delisted_master.parquet", "https://nsearchives.nseindia.com/content/equities/delisted.csv"),
        ):
            try:
                resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
                if resp.status_code == 200 and not resp.text.lstrip().startswith("<"):
                    df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
                    df.columns = [c.strip() for c in df.columns]
                    write_parquet_atomic(df, self.cfg.root_dir / "universe" / name)
                    self.state.record("security_master", name, "OK", len(df), resp.content, url)
            except Exception as exc:
                logger.warning("[Security Master %s] Skipped: %s", name, exc)

        sym_chg_url = "https://nsearchives.nseindia.com/content/equities/symbolchange.csv"
        try:
            sc_resp = await self.http.get(sym_chg_url, headers=ARCHIVE_HEADERS)
            if sc_resp.status_code == 200 and not sc_resp.text.lstrip().startswith("<"):
                sc_df = pd.read_csv(
                    io.StringIO(sc_resp.text),
                    names=["CompanyName", "OldSymbol", "NewSymbol", "ChangeDate"],
                    on_bad_lines="skip",
                )
                for c in ("OldSymbol", "NewSymbol"):
                    sc_df[c] = sc_df[c].astype(str).str.strip().str.upper()
                sc_df["ChangeDate"] = pd.to_datetime(sc_df["ChangeDate"], format="%d-%b-%Y", errors="coerce")
                sc_df = sc_df.dropna(subset=["ChangeDate"])
                write_parquet_atomic(sc_df, self.cfg.root_dir / "universe" / "nse_symbol_changes.parquet")
                self.state.record("security_master", "nse_symbol_changes", "OK", len(sc_df), sc_resp.content, sym_chg_url)
        except Exception as exc:
            logger.warning("[Symbol Changes] Skipped: %s", exc)

        # NSE-only: Enriched market universe with 52W high/low and pre-open volumes
        try:
            po_resp = await self.http.get(
                "https://www.nseindia.com/api/market-data-pre-open?key=ALL",
                headers=NSE_API_HEADERS,
                prime_nse=True,
            )
            if po_resp.status_code == 200 and po_resp.text.lstrip().startswith("{"):
                po_data = po_resp.json().get("data", [])
                if isinstance(po_data, list) and po_data:
                    po_rows = []
                    for it in po_data:
                        meta = it.get("metadata", {})
                        det = it.get("detail", {}).get("preOpenMarket", {})
                        sym = str(meta.get("symbol") or "").strip().upper()
                        if sym:
                            po_rows.append({
                                "Symbol": sym,
                                "Series": str(meta.get("series") or "EQ").strip().upper(),
                                "ISIN": str(meta.get("identifier") or "").strip().upper(),
                                "LastPrice": pd.to_numeric(meta.get("lastPrice"), errors="coerce"),
                                "YearHigh": pd.to_numeric(meta.get("yearHigh"), errors="coerce"),
                                "YearLow": pd.to_numeric(meta.get("yearLow"), errors="coerce"),
                                "PreOpenVolume": pd.to_numeric(det.get("totalTradedVolume"), errors="coerce"),
                            })
                    if po_rows:
                        po_df = pd.DataFrame(po_rows)
                        write_parquet_atomic(po_df, self.cfg.root_dir / "universe" / "nse_market_universe.parquet")
                        self.state.record("security_master", "nse_market_universe", "OK", len(po_df))
                        eq_master_path = self.cfg.root_dir / "universe" / "nse_equity_master.parquet"
                        if not eq_master_path.exists():
                            synth_eq = pd.DataFrame({
                                "SYMBOL": po_df["Symbol"],
                                "NAME OF COMPANY": po_df["Symbol"],
                                "SERIES": po_df["Series"],
                                "ISIN NUMBER": po_df["ISIN"],
                            })
                            write_parquet_atomic(synth_eq, eq_master_path)
                            self.state.record("security_master", "nse_equity_master.parquet", "OK", len(synth_eq))
        except Exception as exc:
            logger.warning("[NSE Market Universe] Skipped: %s", exc)

        bse_frames = []
        for status in ("Active", "Suspended"):
            bse_url = (
                "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
                f"?Group=&Scripcode=&industry=&segment=Equity&status={status}"
            )
            try:
                b_resp = await self.http.get(bse_url, headers=BSE_API_HEADERS)
                if b_resp.status_code == 200 and b_resp.text.lstrip().startswith("["):
                    payload = b_resp.json()
                    if isinstance(payload, list) and payload:
                        bse_frames.append(pd.DataFrame(payload).assign(Status=status))
            except Exception as exc:
                logger.warning("[BSE Scrip Master %s] Skipped: %s", status, exc)

        if bse_frames:
            bse_df = pd.concat(bse_frames, ignore_index=True)
            write_parquet_atomic(bse_df, self.cfg.root_dir / "universe" / "bse_scrip_master.parquet")
            self.state.record("security_master", "bse_equity", "OK", len(bse_df))

        today = today_ist()
        const_frames = []
        for idx_name, url in NIFTY_CONSTITUENT_URLS.items():
            try:
                c_resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
                if c_resp.status_code == 200 and not c_resp.text.lstrip().startswith("<"):
                    cdf = pd.read_csv(io.StringIO(c_resp.text))
                    cdf.columns = [c.strip() for c in cdf.columns]
                    cdf["IndexName"] = idx_name
                    cdf["SnapshotDate"] = pd.Timestamp(today)
                    const_frames.append(cdf)
            except Exception as exc:
                logger.warning("[Nifty Constituents %s] Skipped: %s", idx_name, exc)

        if const_frames:
            target = self.cfg.root_dir / "universe" / "nifty_constituents_pit.parquet"
            upsert_parquet_partition(
                pd.concat(const_frames, ignore_index=True),
                target,
                dedup_cols=["SnapshotDate", "IndexName", "Symbol"],
                sort_cols=["SnapshotDate", "IndexName", "Symbol"],
            )
            self.state.record("nifty_constituents_pit", today.isoformat(), "OK", sum(len(f) for f in const_frames))

        await self.sync_upcoming_calendar_and_fo_ban()

    async def sync_upcoming_calendar_and_fo_ban(self) -> None:
        """
        Syncs:
          1. Upcoming NSE Corporate Board Meetings (`/api/corporate-board-meetings?index=equities`)
             into `actions/upcoming_board_meetings.parquet` (powers `ticker.calendar` in bfinance).
          2. Daily NSE F&O Security Ban List (`fo_secban.csv`) into `derivatives/fo_ban_list.parquet`.
        """
        bm_url = "https://www.nseindia.com/api/corporate-board-meetings?index=equities"
        try:
            resp = await self.http.get(bm_url, headers=NSE_API_HEADERS, prime_nse=True)
            if resp.status_code == 200 and resp.text.lstrip().startswith("["):
                rows = resp.json()
                if isinstance(rows, list) and rows:
                    bm_df = pd.DataFrame(rows)
                    bm_df = bm_df.rename(columns={
                        "bm_symbol": "Symbol",
                        "bm_date": "MeetingDate",
                        "bm_purpose": "Purpose",
                        "bm_desc": "Description",
                        "sm_name": "CompanyName",
                    })
                    if "MeetingDate" in bm_df.columns:
                        bm_df["MeetingDate"] = pd.to_datetime(bm_df["MeetingDate"], format="mixed", dayfirst=True, errors="coerce")
                    upsert_parquet_partition(
                        bm_df,
                        self.cfg.root_dir / "actions" / "upcoming_board_meetings.parquet",
                        dedup_cols=["Symbol", "MeetingDate", "Purpose"],
                        sort_cols=["Symbol", "MeetingDate"],
                    )
        except Exception as exc:
            logger.debug("[Board Meetings] %s", exc)

        ban_url = "https://nsearchives.nseindia.com/content/fo/fo_secban.csv"
        try:
            b_resp = await self.http.get(ban_url, headers=ARCHIVE_HEADERS)
            if b_resp.status_code == 200 and not b_resp.text.lstrip().startswith("<"):
                lines = [ln.strip() for ln in b_resp.text.splitlines() if ln.strip()]
                syms = [
                    ln.split(",")[-1].strip().upper()
                    for ln in lines[1:]
                    if "," in ln or ln.strip().isalpha()
                ]
                ban_df = pd.DataFrame({
                    "Date": [pd.Timestamp(today_ist())] * len(syms),
                    "Symbol": syms,
                })
                write_parquet_atomic(ban_df, self.cfg.root_dir / "derivatives" / "fo_ban_list.parquet")
        except Exception as exc:
            logger.debug("[FO Ban List] %s", exc)

    async def sync_corporate_actions(
        self, start_year: int = 2000, end_year: int | None = None, force: bool = False
    ) -> None:
        cur_year = end_year or today_ist().year
        dataset = "corporate_actions_bse"
        target = self.cfg.root_dir / "actions" / "bse_corporate_actions.parquet"

        frames = []
        for year in range(start_year, cur_year + 1):
            key = str(year)
            if not force and year < cur_year and self.state.is_completed(dataset, key):
                continue

            url = (
                "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"
                f"?scripcode=&Fdate={year}0101&TDate={year}1231"
                "&Purposecode=&ddlcategorys=E&ddlindustrys=&segment=0&strSearch=S"
            )
            try:
                resp = await self.http.get(url, headers=BSE_API_HEADERS)
                resp.raise_for_status()
                if not resp.text.lstrip().startswith("["):
                    continue
                rows = resp.json()
                if isinstance(rows, list) and rows:
                    df = pd.DataFrame(rows)
                    df["ExDate"] = pd.to_datetime(df.get("exdate"), format="%Y%m%d", errors="coerce")
                    parsed_cols = _normalize_action_frame(
                        pd.DataFrame([parse_action_purpose(p) for p in df.get("Purpose", [])])
                    )
                    df = pd.concat([df.reset_index(drop=True), parsed_cols], axis=1)
                    frames.append(df)
                    self.state.record(dataset, key, "OK", len(df), resp.content, url)
                    logger.info("[BSE Corporate Actions %d] Fetched & parsed %d actions", year, len(df))
            except Exception as exc:
                logger.warning("[BSE Corporate Actions %d] Failed: %s", year, exc)

        if frames:
            upsert_parquet_partition(
                _normalize_action_frame(pd.concat(frames, ignore_index=True)),
                target,
                dedup_cols=["scrip_code", "exdate", "Purpose"],
                sort_cols=["scrip_code", "ExDate"],
                row_group_size=2_000,
            )

        await self.sync_nse_corporate_actions(start_year=start_year, end_year=cur_year, force=force)

    async def sync_nse_corporate_actions(
        self, start_year: int = 1995, end_year: int | None = None, force: bool = False
    ) -> None:
        """Walks NSE Corporate Actions year-by-year across both `equities` (1995+) and `sme` (2013+)."""
        cur_year = end_year or today_ist().year
        target = self.cfg.root_dir / "actions" / "nse_corporate_actions.parquet"

        for idx_type in ("equities", "sme"):
            min_yr = max(start_year, 2013 if idx_type == "sme" else 1995)
            dataset = f"corporate_actions_nse_{idx_type}"
            frames = []
            for year in range(min_yr, cur_year + 1):
                key = str(year)
                if not force and year < cur_year and self.state.is_completed(dataset, key):
                    continue
                url = (
                    f"https://www.nseindia.com/api/corporates-corporateActions?index={idx_type}"
                    f"&from_date=01-01-{year}&to_date=31-12-{year}"
                )
                try:
                    resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                    if resp.status_code != 200 or not isinstance(resp.json(), list):
                        continue
                    rows = resp.json()
                    if not rows:
                        self.state.record(dataset, key, "HOLIDAY", source_url=url)
                        continue
                    df = pd.DataFrame(rows)
                    df["ExDate"] = pd.to_datetime(df.get("exDate"), format="%d-%b-%Y", errors="coerce")
                    parsed_cols = _normalize_action_frame(
                        pd.DataFrame([parse_action_purpose(p) for p in df.get("subject", [])])
                    )
                    df = pd.concat([df.reset_index(drop=True), parsed_cols], axis=1)
                    frames.append(df)
                    self.state.record(dataset, key, "OK", len(df), resp.content, url)
                except Exception as exc:
                    logger.warning("[NSE Corporate Actions %s %d] Failed: %s", idx_type, year, exc)

            if frames:
                upsert_parquet_partition(
                    _normalize_action_frame(pd.concat(frames, ignore_index=True)),
                    target,
                    dedup_cols=["symbol", "series", "exDate", "subject"],
                    sort_cols=["symbol", "ExDate"],
                    row_group_size=2_000,
                )
