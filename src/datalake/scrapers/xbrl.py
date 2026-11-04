from __future__ import annotations

import asyncio
from datetime import date
import logging
from lxml import etree
import pandas as pd

from datalake.config import LakeConfig
from datalake.http_client import ARCHIVE_HEADERS, BSE_API_HEADERS, NSE_API_HEADERS, ResilientHTTPClient
from datalake.state import StateStore
from datalake.storage import read_table_safe, upsert_parquet_partition

logger = logging.getLogger("datalake.xbrl")

XBRL_FIN_TAGS = {
    "RevenueFromOperations": "Sales_Cr",
    "OtherIncome": "OtherIncome_Cr",
    "Income": "TotalIncome_Cr",
    "CostOfMaterialsConsumed": "MaterialCost_Cr",
    "PurchasesOfStockInTrade": "StockInTradePurchases_Cr",
    "EmployeeBenefitExpense": "EmployeeCost_Cr",
    "FinanceCosts": "Interest_Cr",
    "DepreciationDepletionAndAmortisationExpense": "Depreciation_Cr",
    "OtherExpenses": "OtherExpenses_Cr",
    "ProfitBeforeTax": "PBT_Cr",
    "TaxExpense": "Tax_Cr",
    "ProfitLossForPeriod": "NetProfit_Cr",
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "BasicEPS_Rs",
    "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "DilutedEPS_Rs",
    "InterestEarned": "InterestEarned_Cr",
    "InterestExpended": "InterestExpended_Cr",
    "OtherOperatingIncomeBanking": "OtherBankingIncome_Cr",
    "OperatingProfitBeforeProvisionsAndContingencies": "OperatingProfitPreProvision_Cr",
    "ProvisionsAndContingencies": "ProvisionsAndContingencies_Cr",
    "GrossNonPerformingAssets": "GrossNPA_Cr",
    "NetNonPerformingAssets": "NetNPA_Cr",
    "PercentageOfGrossNPA": "GrossNPA_Pct",
    "PercentageOfNetNPA": "NetNPA_Pct",
    "CapitalAdequacyRatio": "CapitalAdequacyRatio_Pct",
    "Deposits": "Deposits_Cr",
    "Advances": "Advances_Cr",
    "EquityShareCapital": "EquityCapital_Cr",
    "OtherEquity": "Reserves_Cr",
    "BorrowingsNoncurrent": "LongTermBorrowings_Cr",
    "BorrowingsCurrent": "ShortTermBorrowings_Cr",
    "LeaseLiabilitiesNoncurrent": "LeaseLiabilitiesNonCurrent_Cr",
    "LeaseLiabilitiesCurrent": "LeaseLiabilitiesCurrent_Cr",
    "CashAndCashEquivalents": "CashAndEquivalents_Cr",
    "BankBalanceOtherThanCashAndCashEquivalents": "OtherBankBalances_Cr",
    "Inventories": "Inventories_Cr",
    "TradeReceivablesCurrent": "TradeReceivables_Cr",
    "TradePayablesCurrent": "TradePayables_Cr",
    "CurrentAssets": "CurrentAssets_Cr",
    "CurrentLiabilities": "CurrentLiabilities_Cr",
    "NoncurrentAssets": "NonCurrentAssets_Cr",
    "NoncurrentLiabilities": "NonCurrentLiabilities_Cr",
    "PropertyPlantAndEquipment": "FixedAssets_Cr",
    "CapitalWorkInProgress": "CWIP_Cr",
    "Goodwill": "Goodwill_Cr",
    "OtherIntangibleAssets": "IntangibleAssets_Cr",
    "InvestmentsNoncurrent": "NonCurrentInvestments_Cr",
    "InvestmentsCurrent": "CurrentInvestments_Cr",
    "Assets": "TotalAssets_Cr",
    "EquityAndLiabilities": "TotalLiabilitiesAndEquity_Cr",
    "CashFlowsFromUsedInOperatingActivities": "OperatingCashFlow_Cr",
    "CashFlowsFromUsedInInvestingActivities": "InvestingCashFlow_Cr",
    "CashFlowsFromUsedInFinancingActivities": "FinancingCashFlow_Cr",
}

UNSCALED_TAGS = {
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations",
    "PercentageOfGrossNPA",
    "PercentageOfNetNPA",
    "CapitalAdequacyRatio",
}


def _is_valid_xbrl_url(url: str | None) -> bool:
    if not url:
        return False
    s = str(url).strip()
    if not s.startswith("http") or s.endswith("/-") or s.endswith("-"):
        return False
    return s.lower().endswith((".xml", ".html"))


def _safe_float(v: object) -> float:
    if v is None or v is pd.NA:
        return float("nan")
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return float("nan")
    try:
        return float(s)
    except (ValueError, TypeError):
        return float("nan")


class XBRLScraper:
    """
    Discovers and parses NSE/BSE Ind-AS Financial XBRL XMLs (with JSON fallback when XBRL link is '/-')
    and Shareholding XBRL/iXBRLs (Promoter Pledge, FPI/MF/Insurance %, and Named >1% Holders).
    """

    def __init__(self, http: ResilientHTTPClient, state: StateStore, cfg: LakeConfig):
        self.http = http
        self.state = state
        self.cfg = cfg

    async def sync_all_market(
        self, max_companies: int | None = None, force: bool = False
    ) -> tuple[list[str], list[str]]:
        nse_master = self.cfg.root_dir / "universe" / "nse_equity_master.parquet"
        bse_master = self.cfg.root_dir / "universe" / "bse_scrip_master.parquet"
        synced_syms: list[str] = []
        synced_scrips: list[str] = []

        if nse_master.exists():
            ndf = read_table_safe(nse_master)
            done_fin = self.state.completed_keys("xbrl_financials_symbol")
            done_shp = self.state.completed_keys("nse_shareholding_symbol")
            all_syms = ndf["SYMBOL"].dropna().astype(str).str.strip().unique().tolist()
            syms = all_syms if force else [s for s in all_syms if not (s in done_fin and s in done_shp)]
            if max_companies:
                syms = syms[:max_companies]
            synced_syms = syms
            if syms:
                logger.info("[XBRL All-Market] Syncing NSE Financials + Shareholding for %d symbols...", len(syms))
                sem = asyncio.Semaphore(4)
                async def _sync_one_sym(s: str) -> None:
                    async with sem:
                        if force or s not in done_fin:
                            try:
                                await self.sync_symbol_financials_nse(s)
                            except Exception as exc:
                                logger.warning("[XBRL All-Market] NSE Financials failed for %s: %s", s, exc)
                        if force or s not in done_shp:
                            try:
                                await self.sync_symbol_shareholding_nse(s)
                            except Exception as exc:
                                logger.warning("[XBRL All-Market] NSE Shareholding failed for %s: %s", s, exc)
                        await asyncio.sleep(self.cfg.polite_delay_sec)
                await asyncio.gather(*[_sync_one_sym(s) for s in syms])
            else:
                logger.info("[XBRL All-Market] All %d NSE symbols already completed in state.db.", len(all_syms))

        if bse_master.exists():
            bdf = read_table_safe(bse_master)
            bdf = bdf[bdf["Status"] == "Active"] if "Status" in bdf.columns else bdf
            done_bse = self.state.completed_keys("bse_shareholding")
            all_scrips = bdf["SCRIP_CD"].dropna().astype(str).str.strip().unique().tolist()
            scrips = all_scrips if force else [sc for sc in all_scrips if sc not in done_bse]
            if max_companies:
                scrips = scrips[:max_companies]
            synced_scrips = scrips
            if scrips:
                logger.info("[XBRL All-Market] Syncing BSE Shareholding for %d scrips...", len(scrips))
                sem_bse = asyncio.Semaphore(4)
                async def _sync_one_scrip(sc: str) -> None:
                    async with sem_bse:
                        try:
                            await self.sync_bse_shareholding(sc)
                        except Exception as exc:
                            logger.warning("[XBRL All-Market] BSE Shareholding failed for %s: %s", sc, exc)
                        await asyncio.sleep(self.cfg.polite_delay_sec)
                await asyncio.gather(*[_sync_one_scrip(sc) for sc in scrips])
            else:
                logger.info("[XBRL All-Market] All %d BSE scrips already completed in state.db.", len(all_scrips))

        return synced_syms, synced_scrips

    async def sync_recent_market_financials_nse(self, max_new_xmls: int = 100) -> int:
        """
        Fetches NSE's market-wide recent financial results manifest in 1 HTTP call and
        ingests any new quarterly/annual XBRL filings not yet in `state.db`.
        """
        rows: list[dict] = []
        sem = asyncio.Semaphore(4)
        for period in ("Quarterly", "Annual"):
            url = f"https://www.nseindia.com/api/corporates-financial-results?index=equities&period={period}"
            try:
                resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
                if resp.status_code != 200 or not resp.text.strip().startswith("["):
                    continue
                filings = resp.json()
            except Exception:
                continue
            if not isinstance(filings, list):
                continue

            pending = []
            for item in filings:
                sym = str(item.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                xbrl_url = item.get("xbrl")
                filing_dt = item.get("filingDate") or item.get("re_create_dt") or item.get("broadcastDate")
                if _is_valid_xbrl_url(xbrl_url):
                    f_key = f"{sym}:{xbrl_url.split('/')[-1]}"
                    if not self.state.is_completed("xbrl_financials", f_key):
                        pending.append((sym, period, item, xbrl_url, f_key, filing_dt))

            async def _dl_xml(entry):
                sym, per, item, x_url, f_key, f_dt = entry
                async with sem:
                    try:
                        xml_resp = await self.http.get(x_url, headers=ARCHIVE_HEADERS, prime_nse=True)
                        await asyncio.sleep(0.1)
                        if xml_resp.status_code == 200 and b"<" in xml_resp.content[:200]:
                            parsed = self.parse_indas_xbrl(xml_resp.content, default_symbol=sym, source_url=x_url, filing_date=f_dt)
                            if parsed:
                                self.state.record("xbrl_financials", f_key, "OK", len(parsed), xml_resp.content, x_url)
                                return parsed
                    except Exception:
                        pass
                fb = self._extract_json_financial_row(item, sym, per, filing_date=f_dt)
                return [fb] if fb else []

            if pending[:max_new_xmls]:
                batches = await asyncio.gather(*[_dl_xml(e) for e in pending[:max_new_xmls]])
                for b in batches:
                    rows.extend(b)

        if rows:
            target = self.cfg.root_dir / "fundamentals" / "xbrl_financials.parquet"
            upsert_parquet_partition(
                pd.DataFrame(rows),
                target,
                dedup_cols=["Symbol", "NatureOfReport", "PeriodType", "PeriodStart", "PeriodEnd"],
                sort_cols=["Symbol", "PeriodEnd", "PeriodType", "FilingDate"],
            )
        return len(rows)

    async def sync_symbol_financials_nse(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        rows: list[dict] = []
        any_200_ok = False
        already_completed_filings = 0

        for period in ("Quarterly", "Annual"):
            disc_url = (
                f"https://www.nseindia.com/api/corporates-financial-results"
                f"?index=equities&symbol={sym}&period={period}"
            )
            try:
                resp = await self.http.get(disc_url, headers=NSE_API_HEADERS, prime_nse=True)
                if resp.status_code != 200 or not resp.text.strip().startswith("["):
                    continue
                filings = resp.json()
                if isinstance(filings, list):
                    any_200_ok = True
                else:
                    continue
            except Exception as exc:
                logger.debug("[XBRL Financials %s %s] %s", sym, period, exc)
                continue

            for item in filings:
                filing_dt = (
                    item.get("filingDate")
                    or item.get("re_create_dt")
                    or item.get("broadcastDate")
                    or item.get("broadcast_dttm")
                )
                xbrl_url = item.get("xbrl")
                if _is_valid_xbrl_url(xbrl_url):
                    filing_key = f"{sym}:{xbrl_url.split('/')[-1]}"
                    if not self.state.is_completed("xbrl_financials", filing_key):
                        try:
                            xml_resp = await self.http.get(xbrl_url, headers=ARCHIVE_HEADERS, prime_nse=True)
                            if xml_resp.status_code == 200 and b"<" in xml_resp.content[:200]:
                                parsed_rows = self.parse_indas_xbrl(
                                    xml_resp.content, default_symbol=sym, source_url=xbrl_url, filing_date=filing_dt
                                )
                                if parsed_rows:
                                    rows.extend(parsed_rows)
                                    self.state.record(
                                        "xbrl_financials", filing_key, "OK", len(parsed_rows), xml_resp.content, xbrl_url
                                    )
                                    continue
                        except Exception:
                            pass
                    else:
                        already_completed_filings += 1
                        continue

                # JSON Fallback when `xbrl` is `.../corporate/xbrl/-` or XML download unavailable
                fb_row = self._extract_json_financial_row(item, sym, period, filing_date=filing_dt)
                if fb_row is not None:
                    rows.append(fb_row)

        if rows:
            target = self.cfg.root_dir / "fundamentals" / "xbrl_financials.parquet"
            upsert_parquet_partition(
                pd.DataFrame(rows),
                target,
                dedup_cols=["Symbol", "NatureOfReport", "PeriodType", "PeriodStart", "PeriodEnd"],
                sort_cols=["Symbol", "PeriodEnd", "PeriodType", "FilingDate"],
            )
            self.state.record("xbrl_financials_symbol", sym, "OK", len(rows))
        elif already_completed_filings > 0:
            self.state.record("xbrl_financials_symbol", sym, "OK", already_completed_filings)
        elif any_200_ok:
            self.state.record("xbrl_financials_symbol", sym, "HOLIDAY")

    @staticmethod
    def _extract_json_financial_row(
        item: dict,
        default_symbol: str,
        default_period: str,
        filing_date: str | None = None,
    ) -> dict | None:
        to_dt = item.get("toDate") or item.get("periodEnd")
        from_dt = item.get("fromDate") or item.get("periodStart")
        if not to_dt:
            return None

        def _num(val) -> float | None:
            if val is None or str(val).strip() in ("", "-", "NA", "null"):
                return None
            try:
                return float(str(val).replace(",", "").strip())
            except ValueError:
                return None

        inc_lakhs = _num(item.get("income") or item.get("re_net_sale") or item.get("totalIncome"))
        pat_lakhs = _num(item.get("netProfit") or item.get("re_net_profit") or item.get("proLossAftTax"))
        pbt_lakhs = _num(item.get("re_pro_loss_bef_tax") or item.get("profitBeforeTax"))
        eps = _num(item.get("eps") or item.get("re_basic_eps_for_cont_dic_opr"))
        if inc_lakhs is None and pat_lakhs is None:
            return None

        nature = item.get("consolidated") or "Standalone"
        return {
            "Symbol": item.get("symbol") or default_symbol,
            "NatureOfReport": str(nature).strip(),
            "PeriodType": default_period,
            "PeriodStart": str(from_dt).strip() if from_dt else None,
            "PeriodEnd": str(to_dt).strip(),
            "FilingDate": str(filing_date).strip() if filing_date else None,
            "SourceURL": "https://www.nseindia.com/api/corporates-financial-results",
            "Sales_Cr": round(inc_lakhs / 100.0, 4) if inc_lakhs is not None else None,
            "PBT_Cr": round(pbt_lakhs / 100.0, 4) if pbt_lakhs is not None else None,
            "NetProfit_Cr": round(pat_lakhs / 100.0, 4) if pat_lakhs is not None else None,
            "BasicEPS_Rs": eps,
        }

    @staticmethod
    def _classify_duration_days(start_str: str | None, end_str: str | None) -> str:
        if not start_str or not end_str:
            return "Instant"
        try:
            days = (date.fromisoformat(end_str) - date.fromisoformat(start_str)).days
            if 70 <= days <= 105:
                return "Quarterly"
            if 165 <= days <= 200:
                return "HalfYearly"
            if 255 <= days <= 290:
                return "NineMonths"
            if 345 <= days <= 380:
                return "Annual"
            return f"Duration_{days}d"
        except ValueError:
            return "Unknown"

    @classmethod
    def parse_indas_xbrl(
        cls,
        xml_bytes: bytes,
        default_symbol: str,
        source_url: str,
        filing_date: str | None = None,
    ) -> list[dict]:
        if not xml_bytes or not xml_bytes.strip():
            return []
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(xml_bytes, parser=parser)
        if root is None:
            return []

        board_mtg = root.xpath("//*[local-name()='DateOfBoardMeetingWhenFinancialResultsWereApproved']/text()")
        f_dt = str(filing_date).strip() if filing_date else (board_mtg[0].strip() if board_mtg else None)

        contexts: dict[str, dict] = {}
        for ctx in root.xpath("//*[local-name()='context']"):
            cid = ctx.get("id")
            if not cid or ctx.xpath(".//*[local-name()='explicitMember']"):
                continue
            start = ctx.xpath(".//*[local-name()='startDate']/text()")
            end = ctx.xpath(".//*[local-name()='endDate']/text()")
            inst = ctx.xpath(".//*[local-name()='instant']/text()")
            s_str = start[0].strip() if start else None
            e_str = (end[0] if end else (inst[0] if inst else "")).strip()
            contexts[cid] = {
                "start": s_str,
                "end": e_str,
                "period_type": cls._classify_duration_days(s_str, e_str),
            }

        nature_vals = root.xpath("//*[local-name()='NatureOfReportStandaloneConsolidated']/text()")
        nature = nature_vals[0].strip() if nature_vals else "Unknown"

        duration_records: dict[tuple[str, str], dict] = {}
        instant_bs_by_end: dict[str, dict] = {}

        for tag, col_name in XBRL_FIN_TAGS.items():
            xpath = (
                f"//*[local-name()='{tag}'] | "
                f"//*[local-name()='nonFraction'][contains(@name, ':{tag}')]"
            )
            for elem in root.xpath(xpath):
                cref = elem.get("contextRef")
                val_txt = "".join(elem.itertext()).strip()
                if cref not in contexts or not val_txt:
                    continue
                try:
                    val = float(val_txt.replace(",", ""))
                    if elem.get("sign") == "-":
                        val = -val
                    scale_attr = elem.get("scale")
                    if scale_attr is not None:
                        val *= 10 ** int(scale_attr)
                except ValueError:
                    continue
                scaled = val if tag in UNSCALED_TAGS else round(val / 1e7, 4)
                cinfo = contexts[cref]
                end_dt = cinfo["end"]
                start_dt = cinfo["start"]

                if start_dt is None:
                    instant_bs_by_end.setdefault(end_dt, {})[col_name] = scaled
                else:
                    rec = duration_records.setdefault(
                        (start_dt, end_dt),
                        {
                            "Symbol": default_symbol,
                            "NatureOfReport": nature,
                            "PeriodType": cinfo["period_type"],
                            "PeriodStart": start_dt,
                            "PeriodEnd": end_dt,
                            "FilingDate": f_dt,
                            "SourceURL": source_url,
                        },
                    )
                    rec[col_name] = scaled

        out_list: list[dict] = []
        for (_, end_dt), rec in duration_records.items():
            if end_dt in instant_bs_by_end:
                rec.update(instant_bs_by_end[end_dt])
            if "Sales_Cr" not in rec and "InterestEarned_Cr" in rec:
                rec["Sales_Cr"] = rec["InterestEarned_Cr"]
            if "CashAndEquivalents_Cr" in rec or "OtherBankBalances_Cr" in rec:
                rec["TotalCashAndBank_Cr"] = round(
                    rec.get("CashAndEquivalents_Cr", 0.0) + rec.get("OtherBankBalances_Cr", 0.0), 4
                )
            if "LongTermBorrowings_Cr" in rec or "ShortTermBorrowings_Cr" in rec:
                rec["TotalBorrowings_Cr"] = round(
                    rec.get("LongTermBorrowings_Cr", 0.0) + rec.get("ShortTermBorrowings_Cr", 0.0), 4
                )
            out_list.append(rec)

        return out_list

    async def sync_symbol_shareholding_nse(self, symbol: str) -> None:
        sym = symbol.strip().upper()
        url = f"https://www.nseindia.com/api/corporate-share-holdings-master?index=equities&symbol={sym}"
        try:
            resp = await self.http.get(url, headers=NSE_API_HEADERS, prime_nse=True)
            if resp.status_code != 200 or not resp.text.strip().startswith("["):
                return
            items = resp.json()
            if not isinstance(items, list):
                return
        except Exception as exc:
            logger.debug("[NSE Shareholding %s] %s", sym, exc)
            return

        nse_p = self.cfg.root_dir / "shareholding" / "nse_shareholding_summary.parquet"
        existing_qtrs = set()
        if nse_p.exists():
            try:
                ex_df = read_table_safe(nse_p)
                if not ex_df.empty and "Symbol" in ex_df.columns and "QuarterEnd" in ex_df.columns:
                    existing_qtrs = set(ex_df.loc[ex_df["Symbol"] == sym, "QuarterEnd"].astype(str))
            except Exception:
                pass

        summary_rows = []
        named_rows = []
        for item in items:
            if not isinstance(item, dict):
                continue
            xbrl_url = item.get("xbrl")
            qtr_date = str(item.get("date") or "").strip()
            if not qtr_date:
                continue
            base_row = {
                "Symbol": sym,
                "QuarterEnd": qtr_date,
                "PromoterPct": _safe_float(item.get("pr_and_prgrp")),
                "PublicPct": _safe_float(item.get("public_val")),
                "PromoterPledgedShares": float("nan"),
                "PromoterPledgedPct": float("nan"),
                "MutualFundsPct": float("nan"),
                "FPI_Pct": float("nan"),
                "InsurancePct": float("nan"),
                "RetailPct": float("nan"),
                "SourceURL": str(xbrl_url or url),
            }
            if _is_valid_xbrl_url(xbrl_url):
                key = f"{sym}:{qtr_date}"
                if not self.state.is_completed("nse_shareholding_xbrl", key):
                    try:
                        x_resp = await self.http.get(xbrl_url, headers=ARCHIVE_HEADERS, prime_nse=True)
                        if x_resp.status_code == 200 and b"<" in x_resp.content[:200]:
                            parsed = self.parse_full_shp_xbrl(x_resp.content)
                            for k, v in parsed["summary"].items():
                                if v is not None:
                                    base_row[k] = float(v)
                            for nh in parsed["named_holders"]:
                                named_rows.append({"Symbol": sym, "QuarterEnd": qtr_date, **nh})
                            self.state.record("nse_shareholding_xbrl", key, "OK", 1, x_resp.content, xbrl_url)
                    except Exception:
                        pass
                elif qtr_date in existing_qtrs:
                    continue
            summary_rows.append(base_row)

        if summary_rows:
            upsert_parquet_partition(
                pd.DataFrame(summary_rows),
                self.cfg.root_dir / "shareholding" / "nse_shareholding_summary.parquet",
                dedup_cols=["Symbol", "QuarterEnd"],
                sort_cols=["Symbol", "QuarterEnd"],
            )
            self.state.record("nse_shareholding_symbol", sym, "OK", len(summary_rows))
        else:
            self.state.record("nse_shareholding_symbol", sym, "HOLIDAY")

        if named_rows:
            upsert_parquet_partition(
                pd.DataFrame(named_rows),
                self.cfg.root_dir / "shareholding" / "named_shareholders.parquet",
                dedup_cols=["Symbol", "QuarterEnd", "CategoryMember", "HolderName"],
                sort_cols=["Symbol", "QuarterEnd", "PctOfTotal"],
            )

    async def sync_bse_shareholding(
        self,
        scrip_code: str | int,
        symbol: str | None = None,
        max_xbrl_quarters: int | None = None,
    ) -> None:
        # Also sync NSE shareholding if a symbol was provided so shareholding always populates
        if symbol:
            await self.sync_symbol_shareholding_nse(symbol)

        scrip_str = str(scrip_code).strip()
        if scrip_str.endswith(".0"):
            scrip_str = scrip_str[:-2]

        url = (
            f"https://api.bseindia.com/BseIndiaAPI/api/Corp_Shareholding_ng/w"
            f"?scripcode={scrip_str}&flag=0&indtype="
        )
        try:
            resp = await self.http.get(url, headers=BSE_API_HEADERS, prime_bse=True)
            if resp.status_code != 200 or not resp.text.strip().startswith("{"):
                return
            table = resp.json().get("Table", [])
        except Exception as exc:
            logger.debug("[BSE Shareholding %s] %s", scrip_code, exc)
            return

        if not table:
            self.state.record("bse_shareholding", scrip_str, "HOLIDAY", source_url=url)
            return

        bse_hist_p = self.cfg.root_dir / "shareholding" / "bse_shareholding_history.parquet"
        existing_qtrs = set()
        if bse_hist_p.exists():
            try:
                ex_df = read_table_safe(bse_hist_p)
                if not ex_df.empty and "ScripCode" in ex_df.columns and "QuarterId" in ex_df.columns:
                    sc_mask = ex_df["ScripCode"].astype(str) == scrip_str
                    existing_qtrs = set(ex_df.loc[sc_mask, "QuarterId"].astype(str))
            except Exception:
                pass

        pending_items = []
        for item in table:
            qtr_id = str(item.get("qtrid", item.get("nqtrid", "")))
            qtr_name = item.get("QtrName") or item.get("sQtrName")
            sym = symbol or item.get("scripname") or item.get("Company_NAme") or str(scrip_code)
            xbrl_rel = item.get("XBRLAttachment")
            row_data = {
                "ScripCode": str(scrip_code),
                "Symbol": sym,
                "QuarterId": qtr_id,
                "QuarterEnd": qtr_name,
                "PromoterPct": _safe_float(item.get("ese_p")),
                "PublicPct": _safe_float(item.get("ese_np")),
                "TotalShares": _safe_float(item.get(" tot_no_shs", item.get("tot_no_shs"))),
                "PromoterPledgedShares": float("nan"),
                "PromoterPledgedPct": float("nan"),
                "MutualFundsPct": float("nan"),
                "FPI_Pct": float("nan"),
                "InsurancePct": float("nan"),
                "RetailPct": float("nan"),
            }
            pending_items.append((row_data, xbrl_rel, qtr_id, qtr_name, sym))

        sem = asyncio.Semaphore(4)
        xbrl_fetches = 0

        async def _fetch_xbrl_entry(entry):
            nonlocal xbrl_fetches
            row_data, xbrl_rel, qtr_id, qtr_name, sym = entry
            local_named = []
            if not xbrl_rel or not str(xbrl_rel).startswith("/XBRLFILES/"):
                return row_data, local_named

            xbrl_key = f"{scrip_str}:{qtr_id}"
            if self.state.is_completed("bse_shareholding_xbrl", xbrl_key) and qtr_id in existing_qtrs:
                return None, local_named

            if max_xbrl_quarters is not None and xbrl_fetches >= max_xbrl_quarters:
                return row_data, local_named

            xbrl_fetches += 1
            xbrl_url = f"https://www.bseindia.com{xbrl_rel}"
            async with sem:
                try:
                    x_resp = await self.http.get(xbrl_url, headers=ARCHIVE_HEADERS, prime_bse=True)
                    if x_resp.status_code == 200 and b"<" in x_resp.content[:200]:
                        parsed = self.parse_full_shp_xbrl(x_resp.content)
                        for k, v in parsed["summary"].items():
                            if v is not None:
                                row_data[k] = v
                        for nh in parsed["named_holders"]:
                            local_named.append({
                                "Symbol": sym,
                                "ScripCode": str(scrip_code),
                                "QuarterEnd": qtr_name,
                                **nh,
                            })
                        self.state.record("bse_shareholding_xbrl", xbrl_key, "OK", 1, x_resp.content, xbrl_url)
                except Exception:
                    pass
            return row_data, local_named

        results = await asyncio.gather(*[_fetch_xbrl_entry(e) for e in pending_items])
        rows = [r[0] for r in results if r[0] is not None]
        named_rows = [nh for r in results for nh in r[1]]

        if rows:
            upsert_parquet_partition(
                pd.DataFrame(rows),
                self.cfg.root_dir / "shareholding" / "bse_shareholding_history.parquet",
                dedup_cols=["ScripCode", "QuarterId"],
                sort_cols=["ScripCode", "QuarterId"],
            )
            self.state.record("bse_shareholding", str(scrip_code), "OK", len(rows))
        if named_rows:
            upsert_parquet_partition(
                pd.DataFrame(named_rows),
                self.cfg.root_dir / "shareholding" / "named_shareholders.parquet",
                dedup_cols=["Symbol", "QuarterEnd", "CategoryMember", "HolderName"],
                sort_cols=["Symbol", "QuarterEnd", "PctOfTotal"],
            )

    @staticmethod
    def parse_full_shp_xbrl(raw_bytes: bytes) -> dict:
        if not raw_bytes or not raw_bytes.strip():
            return {"summary": {}, "named_holders": []}
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(raw_bytes, parser=parser)
        if root is None:
            return {"summary": {}, "named_holders": []}

        ctx_to_member: dict[str, tuple[str, bool]] = {}
        for ctx in root.xpath("//*[local-name()='context']"):
            cid = ctx.get("id")
            members = ctx.xpath(".//*[local-name()='explicitMember']/text()")
            typed = [t.strip() for t in ctx.xpath(".//*[local-name()='typedMember']//text()") if t.strip()]
            if cid and members:
                m_clean = members[-1].split(":")[-1].strip()
                if typed:
                    ctx_to_member[cid] = (f"{m_clean}:{typed[0].strip()}", True)
                else:
                    ctx_to_member[cid] = (m_clean, False)

        tags = {
            "NumberOfShares": "Shares",
            "ShareholdingAsAPercentageOfTotalNumberOfShares": "PctOfTotal",
            "NumberOfSharesPledgedOrOtherwiseEncumbered": "PledgedShares",
            "SharesPledgedOrOtherwiseEncumberedAsAPercentageOfTotalNumberOfShares": "PledgedPct",
            "NameOfTheShareHolders": "HolderName",
            "PermanentAccountNumberOfShareholder": "PAN",
        }

        by_member: dict[str, dict] = {}
        for tag, key in tags.items():
            xpath = (
                f"//*[local-name()='{tag}'] | "
                f"//*[local-name()='nonFraction' or local-name()='nonNumeric'][contains(@name, ':{tag}')]"
            )
            for elem in root.xpath(xpath):
                cref = elem.get("contextRef")
                val_txt = "".join(elem.itertext()).strip()
                if cref not in ctx_to_member or not val_txt:
                    continue
                m_key, is_typed = ctx_to_member[cref]
                entry = by_member.setdefault(m_key, {"_is_typed": is_typed, "CategoryMember": m_key.split(":")[0]})
                if key in ("HolderName", "PAN"):
                    entry[key] = val_txt
                else:
                    try:
                        fval = float(val_txt.replace(",", ""))
                        if elem.get("sign") == "-":
                            fval = -fval
                        entry[key] = fval
                    except ValueError:
                        pass

        def _get_pct(*member_names: str) -> float | None:
            for m in member_names:
                if m in by_member and "PctOfTotal" in by_member[m]:
                    return by_member[m]["PctOfTotal"]
            return None

        promo = (
            by_member.get("ShareholdingOfPromoterAndPromoterGroupMember")
            or by_member.get("PromoterAndPromoterGroupMember")
            or by_member.get("PromoterMember")
            or {}
        )
        fpi_pct = (
            _get_pct("ForeignPortfolioInvestorsCategoryIMember", "ForeignPortfolioInvestorsMember") or 0.0
        ) + (_get_pct("ForeignPortfolioInvestorsCategoryIIMember") or 0.0)

        pub_entry = by_member.get("PublicShareholdingMember", {})
        tot_sh = (promo.get("Shares") or 0.0) + (pub_entry.get("Shares") or 0.0)

        summary = {
            "PromoterPct": promo.get("PctOfTotal"),
            "PromoterPledgedShares": promo.get("PledgedShares"),
            "PromoterPledgedPct": promo.get("PledgedPct"),
            "PublicPct": _get_pct("PublicShareholdingMember"),
            "TotalShares": tot_sh if tot_sh > 0 else None,
            "MutualFundsPct": _get_pct("MutualFundsOrUTIMember", "MutualFundsMember"),
            "FPI_Pct": round(fpi_pct, 4) if fpi_pct > 0 else _get_pct("ForeignInstitutionsMember"),
            "InsurancePct": _get_pct("InsuranceCompaniesMember"),
            "RetailPct": _get_pct(
                "IndividualsShareholdersHoldingNominalShareCapitalUpToTwoLakhMember",
                "ResidentIndividualsHoldingNominalShareCapitalUpToTwoLakhsMember",
            ),
        }

        named_holders = [
            {
                "CategoryMember": v["CategoryMember"],
                "HolderName": v["HolderName"],
                "PAN": v.get("PAN"),
                "Shares": v.get("Shares"),
                "PctOfTotal": v.get("PctOfTotal"),
                "PledgedShares": v.get("PledgedShares"),
            }
            for v in by_member.values()
            if v.get("_is_typed") and v.get("HolderName")
        ]
        return {"summary": summary, "named_holders": named_holders}
