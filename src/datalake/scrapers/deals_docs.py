from __future__ import annotations

import io
import logging
from datetime import date, timedelta
import pandas as pd

from datalake.config import LakeConfig, today_ist
from datalake.http_client import ARCHIVE_HEADERS, BSE_API_HEADERS, NSE_API_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import upsert_parquet_partition

logger = logging.getLogger("datalake.deals_docs")

BSE_DOC_SUBCATEGORIES = (
    ("Concall_Transcript", "Company Update", "Earnings Call Transcript"),
    ("Investor_Presentation", "Company Update", "Investor Presentation"),
    ("Concall_Audio_Video", "Company Update", "Audio or Video recordings of post earnings or quarterly calls"),
    ("Credit_Rating", "Company Update", "Credit Rating"),
    ("Press_Release", "Company Update", "Press Release / Media Release"),
)


def _normalize_deals_frame(df: pd.DataFrame, deal_type: str, exchange: str = "NSE") -> pd.DataFrame:
    """Clean and normalize Bulk & Block deals from either static CSV or historicalOR CSV."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [c.strip() for c in out.columns]
    col_map = {
        "BD_DT_DATE": "Date",
        "BD_SYMBOL": "Symbol",
        "BD_SCRIP_NAME": "Security Name",
        "BD_CLIENT_NAME": "Client Name",
        "BD_BUY_SELL": "Buy/Sell",
        "BD_QTY_TRD": "Quantity Traded",
        "BD_TP_WATP": "Trade Price / Wght. Avg. Price",
        "BD_REMARKS": "Remarks",
    }
    out = out.rename(columns={k: v for k, v in col_map.items() if k in out.columns})
    if "Date" not in out.columns:
        return pd.DataFrame()

    out = out[~out["Date"].astype(str).str.strip().str.upper().str.contains("NO RECORDS")].copy()
    if "Symbol" in out.columns:
        out = out[out["Symbol"].notna() & (out["Symbol"].astype(str).str.strip() != "")].copy()
    if out.empty:
        return pd.DataFrame()

    out["Date"] = pd.to_datetime(out["Date"].astype(str).str.strip(), format="mixed", dayfirst=True, errors="coerce")
    out = out.dropna(subset=["Date"])
    if out.empty:
        return pd.DataFrame()

    for num_col in ("Quantity Traded", "Trade Price / Wght. Avg. Price"):
        if num_col in out.columns:
            out[num_col] = pd.to_numeric(
                out[num_col].astype(str).str.replace(",", "").str.strip(), errors="coerce"
            )

    for str_col in ("Symbol", "Security Name", "Client Name", "Buy/Sell", "Remarks"):
        if str_col in out.columns:
            out[str_col] = out[str_col].fillna("").astype(str).str.strip()

    out["DealType"] = deal_type
    out["Exchange"] = exchange
    keep = [
        c for c in (
            "Date", "Exchange", "DealType", "Symbol", "Security Name",
            "Client Name", "Buy/Sell", "Quantity Traded", "Trade Price / Wght. Avg. Price", "Remarks",
        )
        if c in out.columns
    ]
    return out[keep].reset_index(drop=True)


class DealsAndDocumentsScraper:
    """
    Scrapes:
      1. NSE & BSE Bulk and Block Deals (daily static CSV + historical `/api/historicalOR/bulk-block-short-deals`)
      2. SEBI PIT Insider Trading Disclosures (`/api/corporates-pit`)
      3. Cash-Market FII/DII Daily Provisional Net Flows (`/api/fiidiiTradeReact`)
      4. BSE Concall Transcripts (PDF), Investor Presentations (PPT), Concall Audio, & Credit Ratings
      5. 20-Year Annual Reports Catalog (Dual NSE `/api/annual-reports` + BSE `AnnualReport/w`)
    """

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_bulk_and_block_deals(self) -> None:
        for deal_type, url in (
            ("BULK", "https://nsearchives.nseindia.com/content/equities/bulk.csv"),
            ("BLOCK", "https://nsearchives.nseindia.com/content/equities/block.csv"),
        ):
            resp = await self.http.get(url, headers=ARCHIVE_HEADERS)
            if resp.status_code != 200 or resp.text.lstrip().startswith("<"):
                continue
            raw_df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
            df = _normalize_deals_frame(raw_df, deal_type=deal_type, exchange="NSE")
            if df.empty:
                continue
            target = self.cfg.root_dir / "deals" / "bulk_block_deals.parquet"
            upsert_parquet_partition(
                df,
                target,
                dedup_cols=["Date", "Exchange", "DealType", "Symbol", "Client Name", "Buy/Sell", "Quantity Traded"],
                sort_cols=["Symbol", "Date"],
            )
            self.state.record(f"nse_{deal_type.lower()}_deals", today_ist().isoformat(), "OK", len(df), resp.content, url)

    async def backfill_historical_bulk_block_deals(
        self, start_year: int = 2005, end_year: int | None = None, force: bool = False
    ) -> None:
        cur_year = end_year or today_ist().year
        target = self.cfg.root_dir / "deals" / "bulk_block_deals.parquet"

        for opt_type, label in (("bulk_deals", "BULK"), ("block_deals", "BLOCK")):
            for year in range(start_year, cur_year + 1):
                key = f"{label}:{year}"
                if not force and year < cur_year and self.state.is_completed("nse_historical_or_deals", key):
                    continue
                url = (
                    f"https://www.nseindia.com/api/historicalOR/bulk-block-short-deals"
                    f"?optionType={opt_type}&from=01-01-{year}&to=31-12-{year}&csv=true"
                )
                try:
                    resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                    if resp.status_code != 200 or not resp.text.strip() or resp.text.lstrip().startswith("<"):
                        continue
                    raw_df = pd.read_csv(io.StringIO(resp.text), on_bad_lines="skip")
                    df = _normalize_deals_frame(raw_df, deal_type=label, exchange="NSE")
                    if df.empty:
                        self.state.record("nse_historical_or_deals", key, "HOLIDAY", source_url=url)
                        continue
                    upsert_parquet_partition(
                        df,
                        target,
                        dedup_cols=["Date", "Exchange", "DealType", "Symbol", "Client Name", "Buy/Sell", "Quantity Traded"],
                        sort_cols=["Symbol", "Date"],
                    )
                    self.state.record("nse_historical_or_deals", key, "OK", len(df), resp.content, url)
                    logger.info("[Historical Deals %s] Saved %d deals", key, len(df))
                except Exception as exc:
                    logger.warning("[Historical Deals %s] Failed: %s", key, exc)

    async def sync_insider_trading_pit(
        self, start_date: date, end_date: date, force: bool = False
    ) -> None:
        cur = start_date
        target = self.cfg.root_dir / "deals" / "insider_trading_pit.parquet"

        while cur <= end_date:
            win_end = min(end_date, cur + timedelta(days=89))
            key = f"{cur:%Y-%m-%d}_{win_end:%Y-%m-%d}"
            if not force and win_end < today_ist() - timedelta(days=7) and self.state.is_completed("insider_pit", key):
                cur = win_end + timedelta(days=1)
                continue

            url = (
                "https://www.nseindia.com/api/corporates-pit?index=equities"
                f"&from_date={cur:%d-%m-%Y}&to_date={win_end:%d-%m-%Y}"
            )
            try:
                resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                if resp.status_code == 200 and resp.text.strip().startswith("{"):
                    data = resp.json().get("data", [])
                    if data:
                        df = pd.DataFrame(data)
                        upsert_parquet_partition(
                            df,
                            target,
                            dedup_cols=["symbol", "acqName", "date", "secVal", "tdpTransactionType"],
                            sort_cols=["symbol", "date"],
                        )
                        self.state.record("insider_pit", key, "OK", len(df), resp.content, url)
            except Exception as exc:
                logger.warning("[Insider Trading PIT %s] Failed: %s", key, exc)
            cur = win_end + timedelta(days=1)

    async def sync_cash_fii_dii_daily(self) -> None:
        url = "https://www.nseindia.com/api/fiidiiTradeReact"
        try:
            resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
            if resp.status_code != 200 or not isinstance(resp.json(), list):
                return
            df = pd.DataFrame(resp.json())
            if df.empty:
                return
            df["Date"] = pd.to_datetime(df["date"], format="%d-%b-%Y", errors="coerce")
            for col in ("buyValue", "sellValue", "netValue"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            target = self.cfg.root_dir / "flows" / "cash_fii_dii_flows.parquet"
            upsert_parquet_partition(df, target, dedup_cols=["Date", "category"], sort_cols=["Date", "category"])
            self.state.record("cash_fii_dii", today_ist().isoformat(), "OK", len(df), resp.content, url)
        except Exception as exc:
            logger.warning("[Cash FII/DII] Failed: %s", exc)

    async def sync_concalls_and_presentations(
        self, start_date: date, end_date: date, scrip_code: str = ""
    ) -> None:
        target = self.cfg.root_dir / "documents" / "concalls_and_filings.parquet"
        all_rows = []

        # BSE AnnSubCategoryGetData/w enforces: "Date range cannot exceed 1 month."
        cur = start_date
        windows: list[tuple[date, date]] = []
        while cur <= end_date:
            win_end = min(end_date, cur + timedelta(days=27))
            windows.append((cur, win_end))
            cur = win_end + timedelta(days=1)

        for w_start, w_end in windows:
            for doc_label, cat, subcat in BSE_DOC_SUBCATEGORIES:
                page = 1
                while page <= 25:
                    params = {
                        "pageno": str(page),
                        "strCat": cat,
                        "strPrevDate": w_start.strftime("%Y%m%d"),
                        "strScrip": scrip_code,
                        "strSearch": "P",
                        "strToDate": w_end.strftime("%Y%m%d"),
                        "strType": "C",
                        "subcategory": subcat,
                    }
                    url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
                    try:
                        resp = await self.http.get(url, headers=BSE_API_HEADERS, params=params, prime_bse=True)
                        if resp.status_code != 200 or not resp.text.strip().startswith("{"):
                            break
                        table = resp.json().get("Table", [])
                    except Exception:
                        break
                    if not table:
                        break

                    for item in table:
                        att = item.get("ATTACHMENTNAME")
                        folder = "AttachHis" if item.get("OLD") == 1 else "AttachLive"
                        pdf_url = f"https://www.bseindia.com/xml-data/corpfiling/{folder}/{att}" if att else None
                        all_rows.append({
                            "NewsId": str(item.get("NEWSID", "")),
                            "ScripCode": str(item.get("SCRIP_CD", "")),
                            "CompanyName": str(item.get("SLONGNAME", "")),
                            "DocumentType": doc_label,
                            "Category": cat,
                            "SubCategory": subcat,
                            "Headline": str(item.get("NEWSSUB", "")),
                            "FilingTimestamp": str(item.get("NEWS_DT", "")),
                            "PDFUrl": pdf_url,
                        })
                    if len(table) < 50:
                        break
                    page += 1

        if all_rows:
            df = pd.DataFrame(all_rows)
            upsert_parquet_partition(
                df, target, dedup_cols=["NewsId"], sort_cols=["ScripCode", "FilingTimestamp"]
            )
            self.state.record(
                "bse_concalls_filings", f"{start_date}_{end_date}", "OK", len(df)
            )

    async def sync_annual_reports_nse(self, symbols: list[str]) -> None:
        """Fetch 15–20+ years of direct Annual Report PDF/ZIP links from NSE `/api/annual-reports`."""
        target = self.cfg.root_dir / "documents" / "annual_reports.parquet"
        rows = []
        for sym in symbols:
            clean = sym.strip().upper()
            url = f"https://www.nseindia.com/api/annual-reports?index=equities&symbol={clean}"
            try:
                resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                if resp.status_code != 200 or not resp.text.strip().startswith("{"):
                    continue
                data = resp.json().get("data", [])
                for item in data:
                    fname = item.get("fileName")
                    if fname:
                        rows.append({
                            "Symbol": clean,
                            "ScripCode": "",
                            "Year": f"{item.get('fromYr', '')}-{item.get('toYr', '')}",
                            "FileName": str(fname).split("/")[-1],
                            "PDFUrl": fname,
                        })
            except Exception as exc:
                logger.debug("[NSE Annual Reports %s] %s", clean, exc)

        if rows:
            upsert_parquet_partition(
                pd.DataFrame(rows),
                target,
                dedup_cols=["Symbol", "ScripCode", "Year", "FileName"],
                sort_cols=["Symbol", "ScripCode", "Year"],
            )
            self.state.record("annual_reports_nse", f"batch_{len(symbols)}", "OK", len(rows))

    async def sync_annual_reports_bse(self, scrip_codes: list[str], symbols: list[str] | None = None) -> None:
        if symbols:
            await self.sync_annual_reports_nse(symbols)

        target = self.cfg.root_dir / "documents" / "annual_reports.parquet"
        rows = []
        for scrip in scrip_codes:
            url = f"https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?scripcode={scrip}&type=C"
            try:
                resp = await self.http.get(url, headers=BSE_API_HEADERS, prime_bse=True)
                if resp.status_code != 200 or not resp.text.strip().startswith("{"):
                    continue
                table = resp.json().get("Table", [])
            except Exception:
                continue
            for item in table:
                fname = item.get("file_name")
                if fname:
                    rows.append({
                        "Symbol": "",
                        "ScripCode": str(scrip),
                        "Year": str(item.get("year", "")),
                        "FileName": fname,
                        "PDFUrl": f"https://www.bseindia.com/AnnualReports/{scrip}/{fname}",
                    })
        if rows:
            upsert_parquet_partition(
                pd.DataFrame(rows),
                target,
                dedup_cols=["Symbol", "ScripCode", "Year", "FileName"],
                sort_cols=["Symbol", "ScripCode", "Year"],
            )
            self.state.record("annual_reports_bse", f"batch_{len(scrip_codes)}", "OK", len(rows))
