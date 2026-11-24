from __future__ import annotations

from datetime import datetime
import logging
import sqlite3
from typing import Any
import numpy as np
import pandas as pd

from datalake.config import IST, LakeConfig
from datalake.state import StateStore
from datalake.storage import NUMERIC_FLOAT_COLS, read_table_safe, upsert_parquet_partition, write_parquet_atomic
from datalake.symbols import SymbolRenameResolver

logger = logging.getLogger("datalake.doctor")


def measure_turnover_unit(
    df: pd.DataFrame,
    *,
    allow_lacs_rounding: bool = True,
) -> dict[str, Any]:
    """
    Audit cash-equity VWAP (`TurnoverValue / Volume`) against `[Low, High]` and infer
    the turnover scale (`rupees`, `thousands`, `lakhs`, `crores`, `unknown`).
    """
    required = ("Low", "High", "Volume", "TurnoverValue")
    if df is None or df.empty or not all(c in df.columns for c in required):
        return {
            "rows_checked": 0,
            "bad_rows": 0,
            "violation_rate": 0.0,
            "median_vwap_ratio": None,
            "detected_unit": "empty",
            "is_healthy": True,
        }

    low = pd.to_numeric(df["Low"], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(df["High"], errors="coerce").to_numpy(dtype=float)
    vol = pd.to_numeric(df["Volume"], errors="coerce").to_numpy(dtype=float)
    tval = pd.to_numeric(df["TurnoverValue"], errors="coerce").to_numpy(dtype=float)

    valid = (
        np.isfinite(low) & np.isfinite(high) & np.isfinite(vol) & np.isfinite(tval)
        & (low > 0.0) & (high >= low) & (vol > 0.0) & (tval > 0.0)
    )
    n_valid = int(valid.sum())
    if n_valid == 0:
        return {
            "rows_checked": 0,
            "bad_rows": 0,
            "violation_rate": 0.0,
            "median_vwap_ratio": None,
            "detected_unit": "empty",
            "is_healthy": True,
        }

    vwap = tval[valid] / vol[valid]
    lo_v, hi_v, vol_v = low[valid], high[valid], vol[valid]
    mid_v = 0.5 * (lo_v + hi_v)
    med_ratio = float(np.median(vwap / mid_v))

    if 0.5 <= med_ratio <= 2.0:
        unit = "rupees"
    elif 0.5e-3 <= med_ratio <= 2.0e-3:
        unit = "thousands"
    elif 0.5e-5 <= med_ratio <= 2.0e-5:
        unit = "lakhs"
    elif 0.5e-7 <= med_ratio <= 2.0e-7:
        unit = "crores"
    else:
        unit = "unknown"

    tol = np.maximum(1e-6, (500.0 / vol_v) + 0.01) if allow_lacs_rounding else 1e-6
    in_bounds = (vwap >= (lo_v - tol)) & (vwap <= (hi_v + tol))
    bad_rows = int((~in_bounds).sum())
    viol_rate = round(bad_rows / n_valid, 6)

    return {
        "rows_checked": n_valid,
        "bad_rows": bad_rows,
        "violation_rate": viol_rate,
        "median_vwap_ratio": round(med_ratio, 8),
        "detected_unit": unit,
        "is_healthy": (unit == "rupees") and (viol_rate <= 0.05),
    }


def unexplained_price_jumps(
    eq_df: pd.DataFrame,
    actions_df: pd.DataFrame | None = None,
    *,
    drop_threshold: float = 0.25,
    min_price: float = 5.0,
    max_gap_days: int = 10,
    action_window_days: int = 3,
    resolver: SymbolRenameResolver | None = None,
    exchange: str = "NSE",
) -> pd.DataFrame:
    """
    Identify single-session price drops exceeding `drop_threshold` (default >25%) on a
    symbol with no recorded corporate action within `+-action_window_days`.
    Tags `LikelyMissingAction=True` when the exchange also stepped down `PrevClose` overnight.
    """
    cols = [
        "Exchange", "Symbol", "Date", "PrevDate",
        "PrevCloseRaw", "Close", "PctDrop", "ExchangePrevClose", "LikelyMissingAction",
    ]
    if eq_df is None or eq_df.empty or not {"Symbol", "Date", "Close"}.issubset(eq_df.columns):
        return pd.DataFrame(columns=cols)

    work = eq_df.copy()
    work["Date"] = pd.to_datetime(work["Date"], errors="coerce").dt.normalize()
    work["Close"] = pd.to_numeric(work["Close"], errors="coerce")
    if "PrevClose" in work.columns:
        work["PrevClose"] = pd.to_numeric(work["PrevClose"], errors="coerce")
    work = work.dropna(subset=["Symbol", "Date", "Close"])
    work = work[work["Close"] > 0.0]
    if work.empty:
        return pd.DataFrame(columns=cols)

    key_col = "InstrumentId" if (exchange.upper() == "BSE" and "InstrumentId" in work.columns) else "Symbol"
    work["_Key"] = work[key_col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.upper()
    work = work.sort_values(["_Key", "Date"]).reset_index(drop=True)

    # Build lookup of recorded corporate action dates per symbol/alias
    act_dates: dict[str, np.ndarray] = {}
    if actions_df is not None and not actions_df.empty and "ExDate" in actions_df.columns:
        adf = actions_df.copy()
        if "ActionType" in adf.columns:
            adf = adf[adf["ActionType"].astype(str).str.upper() != "SKIP_NON_EQUITY"]
        adf["_ExDt"] = pd.to_datetime(adf["ExDate"], errors="coerce").dt.normalize()
        adf = adf.dropna(subset=["_ExDt"])
        sym_col = "symbol" if (exchange.upper() == "NSE" and "symbol" in adf.columns) else (
            "scrip_code" if "scrip_code" in adf.columns else None
        )
        if sym_col:
            adf["_ActKey"] = adf[sym_col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip().str.upper()
            for k, grp in adf.groupby("_ActKey"):
                dts = np.sort(grp["_ExDt"].unique())
                act_dates[str(k)] = dts
                if resolver is not None and exchange.upper() == "NSE":
                    for al in resolver.all_aliases(str(k)):
                        act_dates[al] = np.unique(np.concatenate([act_dates.get(al, np.array([], dtype="datetime64[ns]")), dts]))

    prev_key = work["_Key"].shift(1)
    prev_dt = work["Date"].shift(1)
    prev_close = work["Close"].shift(1)
    days_diff = (work["Date"] - prev_dt).dt.days
    same_series = (
        work["Series"].astype(str).str.strip().eq(work["Series"].astype(str).str.strip().shift(1))
        if "Series" in work.columns
        else pd.Series(True, index=work.index)
    )

    pct_drop = (work["Close"] / prev_close) - 1.0
    cand_mask = (
        work["_Key"].eq(prev_key)
        & same_series
        & (days_diff > 0)
        & (days_diff <= max_gap_days)
        & (prev_close >= min_price)
        & (pct_drop < -abs(drop_threshold))
    )
    if not cand_mask.any():
        return pd.DataFrame(columns=cols)

    win_ns = np.timedelta64(int(action_window_days), "D")
    hits: list[dict[str, Any]] = []
    for idx in work.index[cand_mask]:
        k = str(work.at[idx, "_Key"])
        dt = work.at[idx, "Date"].to_datetime64()
        known = act_dates.get(k)
        if known is not None and len(known) > 0:
            if np.any(np.abs(known - dt) <= win_ns):
                continue
        exch_prev = float(work.at[idx, "PrevClose"]) if "PrevClose" in work.columns and pd.notna(work.at[idx, "PrevClose"]) else float("nan")
        p_raw = float(prev_close.at[idx])
        likely_missing = bool(np.isfinite(exch_prev) and exch_prev > 0 and (exch_prev / p_raw) < (1.0 - abs(drop_threshold)))
        hits.append({
            "Exchange": exchange.upper(),
            "Symbol": str(work.at[idx, "Symbol"]),
            "Date": work.at[idx, "Date"],
            "PrevDate": prev_dt.at[idx],
            "PrevCloseRaw": round(p_raw, 4),
            "Close": round(float(work.at[idx, "Close"]), 4),
            "PctDrop": round(float(pct_drop.at[idx]) * 100.0, 2),
            "ExchangePrevClose": round(exch_prev, 4) if np.isfinite(exch_prev) else None,
            "LikelyMissingAction": likely_missing,
        })

    if not hits:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(hits, columns=cols).sort_values(["LikelyMissingAction", "PctDrop"], ascending=[False, True]).reset_index(drop=True)


class LakeDoctor:
    """
    Self-healing & crash-recovery engine for bfinance-datalake.
    Detects and repairs:
      1. Leftover `.parquet.tmp` / `.part` files from mid-write crashes.
      2. Corrupted or unreadable `.parquet` partitions.
      3. Truncated / half-downloaded days (below minimum row-count sanity thresholds).
      4. Desynchronization between `state.db` watermarks and actual rows on disk.
      5. Corrupted or missing `state.db` (rebuilds watermarks directly from existing Parquet files).
    """

    def __init__(self, cfg: LakeConfig):
        self.cfg = cfg

    def run_full_check_and_repair(self) -> dict[str, int]:
        self.cfg.ensure_dirs()
        stats = {
            "tmp_files_cleaned": 0,
            "corrupt_parquets_removed": 0,
            "truncated_days_purged": 0,
            "stale_watermarks_reset": 0,
            "watermarks_rebuilt_from_disk": 0,
        }

        for tmp_file in list(self.cfg.root_dir.rglob("*.tmp")) + list(self.cfg.root_dir.rglob("*.dl_tmp")) + list(self.cfg.root_dir.rglob("*.dl.tmp")):
            logger.warning("[Doctor] Removing leftover temp file: %s", tmp_file)
            tmp_file.unlink(missing_ok=True)
            stats["tmp_files_cleaned"] += 1

        if self.cfg.state_db_path.exists():
            conn = None
            try:
                conn = sqlite3.connect(self.cfg.state_db_path)
                res = conn.execute("PRAGMA integrity_check;").fetchone()
                if not res or res[0] != "ok":
                    raise sqlite3.DatabaseError("PRAGMA integrity_check failed")
            except Exception as exc:
                logger.error("[Doctor] state.db corrupted (%s). Rebuilding from Parquet files!", exc)
                if conn is not None:
                    conn.close()
                    conn = None
                corrupt_backup = self.cfg.state_db_path.with_suffix(".db.corrupt")
                self.cfg.state_db_path.replace(corrupt_backup)
            finally:
                if conn is not None:
                    conn.close()

        state = StateStore(self.cfg.state_db_path)
        valid_dates_on_disk: dict[str, dict[str, int]] = {
            "equity_eod_nse": {},
            "equity_eod_bse": {},
            "fo_eod_nse": {},
            "fo_eod_bse": {},
            "indices_daily_all": {},
            "nse_participant_oi": {},
            "nse_participant_vol": {},
        }

        # Auto-migrate legacy monthly F&O files (nse_fo_YYYY_MM.parquet -> nse_fo_YYYY.parquet)
        fo_dir = self.cfg.root_dir / "derivatives"
        if fo_dir.exists():
            for pq_mo in sorted(fo_dir.glob("*_fo_*_*.parquet")):
                parts = pq_mo.stem.split("_")
                if len(parts) == 4 and parts[2].isdigit() and parts[3].isdigit():
                    yearly_target = fo_dir / f"{parts[0]}_fo_{parts[2]}.parquet"
                    try:
                        df_mo = read_table_safe(pq_mo)
                        if not df_mo.empty:
                            upsert_parquet_partition(
                                df_mo,
                                yearly_target,
                                dedup_cols=["Date", "Exchange", "InstrumentType", "Symbol", "ExpiryDate", "StrikePrice", "OptionType"],
                                sort_cols=["Symbol", "Date", "ExpiryDate", "StrikePrice", "OptionType"],
                            )
                        pq_mo.unlink(missing_ok=True)
                    except Exception as exc:
                        logger.warning("[Doctor] Failed migrating %s: %s", pq_mo.name, exc)

        for pq_path in sorted(self.cfg.root_dir.rglob("*.parquet")):
            try:
                df = read_table_safe(pq_path)
                if not isinstance(df, pd.DataFrame):
                    raise ValueError("Not a DataFrame")
            except Exception as exc:
                logger.error("[Doctor] Corrupted Parquet file: %s (%s). Deleting for re-fetch.", pq_path, exc)
                pq_path.unlink(missing_ok=True)
                stats["corrupt_parquets_removed"] += 1
                continue

            if not df.empty:
                modified = False
                # 1. Domain Invariant: OHLC Geometry & Positive Close on Equity/F&O/Index files
                if all(c in df.columns for c in ("Open", "High", "Low", "Close")):
                    bad_close = pd.to_numeric(df["Close"], errors="coerce") <= 0
                    if bad_close.any():
                        df = df[~bad_close].copy()
                        modified = True
                    if not df.empty:
                        from datalake.scrapers.equity_eod import repair_ohlc
                        repaired = repair_ohlc(df)
                        if not repaired.equals(df):
                            df = repaired
                            modified = True

                # 2. Domain Invariant: Shareholding percentage bounds (0..100%)
                if "PromoterPct" in df.columns and "PublicPct" in df.columns:
                    p_pct = pd.to_numeric(df["PromoterPct"], errors="coerce").fillna(0.0)
                    pub_pct = pd.to_numeric(df["PublicPct"], errors="coerce").fillna(0.0)
                    invalid_shp = (p_pct < 0) | (p_pct > 100.5) | (pub_pct < 0) | ((p_pct + pub_pct) > 101.0)
                    if invalid_shp.any():
                        df = df[~invalid_shp].copy()
                        modified = True

                needs_schema_heal = (
                    modified
                    or any(
                        col in df.columns and not pd.api.types.is_float_dtype(df[col])
                        for col in NUMERIC_FLOAT_COLS
                    )
                    or df.columns.duplicated().any()
                )
                if needs_schema_heal:
                    logger.info("[Doctor] Healing schema/invariants in %s", pq_path.name)
                    write_parquet_atomic(df, pq_path)

            if df.empty or "Date" not in df.columns:
                continue

            fname = pq_path.name
            ds_name = None
            min_rows_for_full_day = 200
            purge_below = 1
            if fname.startswith("nse_cm_"):
                ds_name = "equity_eod_nse"
                yr_str = fname.split("_")[-1].split(".")[0]
                if yr_str.isdigit():
                    yr = int(yr_str)
                    min_rows_for_full_day = 200
                    purge_below = 200 if yr >= 2016 else 1
            elif fname.startswith("bse_cm_"):
                ds_name = "equity_eod_bse"
                yr_str = fname.split("_")[-1].split(".")[0]
                if yr_str.isdigit():
                    yr = int(yr_str)
                    min_rows_for_full_day = 200
                    purge_below = 200 if yr >= 2016 else 1
            elif fname.startswith("nse_fo_"):
                ds_name = "fo_eod_nse"
                yr_str = fname.split("_")[2].split(".")[0]
                if yr_str.isdigit():
                    yr = int(yr_str)
                    min_rows_for_full_day = 50 if yr >= 2002 else 1
                    purge_below = 50 if yr >= 2002 else 1
            elif fname.startswith("bse_fo_"):
                ds_name = "fo_eod_bse"
                min_rows_for_full_day = 1
                purge_below = 1
            elif fname.startswith("ind_close_all_"):
                ds_name = "indices_daily_all"
                min_rows_for_full_day = 15
                purge_below = 15
            elif fname.startswith("fao_participant_oi_"):
                ds_name = "nse_participant_oi"
                min_rows_for_full_day = 2
                purge_below = 2
            elif fname.startswith("fao_participant_vol_"):
                ds_name = "nse_participant_vol"
                min_rows_for_full_day = 2
                purge_below = 2

            if ds_name is not None:
                df["_iso_date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
                counts = df.groupby("_iso_date").size()
                bad_dates = set(counts[counts < purge_below].index)
                if bad_dates:
                    logger.warning(
                        "[Doctor] %s has %d truncated dates (< %d rows). Purging for re-download.",
                        fname, len(bad_dates), purge_below,
                    )
                    clean_df = df[~df["_iso_date"].isin(bad_dates)].drop(columns=["_iso_date"])
                    if clean_df.empty:
                        pq_path.unlink(missing_ok=True)
                    else:
                        write_parquet_atomic(clean_df, pq_path)
                    stats["truncated_days_purged"] += len(bad_dates)
                for dt_key, cnt in counts[counts >= min_rows_for_full_day].items():
                    valid_dates_on_disk[ds_name][str(dt_key)] = int(cnt)

        now_str = datetime.now(IST).isoformat()
        with state._session() as conn:
            # Purge pre-launch fake HOLIDAY entries recorded before exchange archive start dates
            for ds_bound, min_key in (
                ("equity_eod_bse", "2006-03-01"),
                ("fo_eod_bse", "2024-07-08"),
                ("fo_eod_nse", "2000-06-12"),
            ):
                conn.execute(
                    "DELETE FROM ingestion_log WHERE dataset = ? AND partition_key < ? AND status != 'OK'",
                    (ds_bound, min_key),
                )

            for ds_name, disk_date_counts in valid_dates_on_disk.items():
                disk_dates = set(disk_date_counts.keys())
                rows = conn.execute(
                    "SELECT partition_key FROM ingestion_log WHERE dataset = ? AND status = 'OK'",
                    (ds_name,),
                ).fetchall()
                db_ok_dates = {r[0] for r in rows}

                missing_on_disk = db_ok_dates - disk_dates
                if missing_on_disk:
                    logger.warning(
                        "[Doctor] %s had %d 'OK' dates missing on disk! Resetting watermarks.",
                        ds_name, len(missing_on_disk),
                    )
                    conn.executemany(
                        "DELETE FROM ingestion_log WHERE dataset = ? AND partition_key = ?",
                        [(ds_name, k) for k in missing_on_disk],
                    )
                    stats["stale_watermarks_reset"] += len(missing_on_disk)

                unrecorded_on_disk = disk_dates - db_ok_dates
                if unrecorded_on_disk:
                    conn.executemany(
                        """
                        INSERT INTO ingestion_log (dataset, partition_key, status, row_count, updated_at)
                        VALUES (?, ?, 'OK', ?, ?)
                        ON CONFLICT(dataset, partition_key) DO UPDATE SET
                            status = 'OK', row_count = excluded.row_count, updated_at = excluded.updated_at
                        """,
                        [(ds_name, k, disk_date_counts[k], now_str) for k in unrecorded_on_disk],
                    )
                    stats["watermarks_rebuilt_from_disk"] += len(unrecorded_on_disk)

            for pq_rel, ds_sym, key_col in (
                ("fundamentals/xbrl_financials.parquet", "xbrl_financials_symbol", "Symbol"),
                ("shareholding/nse_shareholding_summary.parquet", "nse_shareholding_symbol", "Symbol"),
                ("shareholding/bse_shareholding_history.parquet", "bse_shareholding", "ScripCode"),
            ):
                p_sym = self.cfg.root_dir / pq_rel
                if not p_sym.exists():
                    continue
                try:
                    sdf = read_table_safe(p_sym)
                except Exception:
                    continue
                if sdf.empty or key_col not in sdf.columns:
                    continue
                sym_counts = sdf[key_col].dropna().astype(str).str.strip().value_counts().to_dict()
                existing_rows = conn.execute(
                    "SELECT partition_key FROM ingestion_log WHERE dataset = ? AND status = 'OK'",
                    (ds_sym,),
                ).fetchall()
                existing_keys = {r[0] for r in existing_rows}
                missing_syms = [
                    (ds_sym, str(k), int(cnt), now_str)
                    for k, cnt in sym_counts.items()
                    if k and str(k) not in existing_keys
                ]
                if missing_syms:
                    conn.executemany(
                        """
                        INSERT INTO ingestion_log (dataset, partition_key, status, row_count, updated_at)
                        VALUES (?, ?, 'OK', ?, ?)
                        ON CONFLICT(dataset, partition_key) DO UPDATE SET
                            status = 'OK', row_count = excluded.row_count, updated_at = excluded.updated_at
                        """,
                        missing_syms,
                    )
                    stats["watermarks_rebuilt_from_disk"] += len(missing_syms)

            for pq_rel, ds_date in (
                ("indices/all_indices_snapshot.parquet", "all_indices_snapshot"),
                ("indices/index_constituents_live.parquet", "live_index_constituents"),
                ("deals/short_selling.parquet", "nse_short_selling"),
            ):
                p_dt = self.cfg.root_dir / pq_rel
                if p_dt.exists():
                    try:
                        ddf = read_table_safe(p_dt)
                        if not ddf.empty and "Date" in ddf.columns:
                            disk_dates = set(pd.to_datetime(ddf["Date"], errors="coerce").dt.strftime("%Y-%m-%d").dropna().unique())
                            dt_counts = pd.to_datetime(ddf["Date"], errors="coerce").dt.strftime("%Y-%m-%d").value_counts().to_dict()

                            rows = conn.execute(
                                "SELECT partition_key FROM ingestion_log WHERE dataset = ? AND status = 'OK'",
                                (ds_date,),
                            ).fetchall()
                            db_ok_dates = {r[0] for r in rows}

                            missing_on_disk = db_ok_dates - disk_dates
                            if missing_on_disk:
                                conn.executemany(
                                    "DELETE FROM ingestion_log WHERE dataset = ? AND partition_key = ?",
                                    [(ds_date, k) for k in missing_on_disk],
                                )
                                stats["stale_watermarks_reset"] += len(missing_on_disk)

                            unrecorded = disk_dates - db_ok_dates
                            for k_dt in unrecorded:
                                if k_dt:
                                    conn.execute(
                                        """
                                        INSERT INTO ingestion_log (dataset, partition_key, status, row_count, updated_at)
                                        VALUES (?, ?, 'OK', ?, ?)
                                        ON CONFLICT(dataset, partition_key) DO UPDATE SET
                                            status = 'OK', row_count = excluded.row_count, updated_at = excluded.updated_at
                                        """,
                                        (ds_date, str(k_dt), int(dt_counts[k_dt]), now_str),
                                    )
                                    stats["watermarks_rebuilt_from_disk"] += 1
                    except Exception:
                        pass

        logger.info("[Doctor] Self-healing check complete: %s", stats)
        return stats

    def rebuild_watermarks_from_disk(self) -> dict[str, int]:
        """Convenience alias for run_full_check_and_repair."""
        return self.run_full_check_and_repair()

    def audit_data_quality(
        self,
        *,
        drop_threshold: float = 0.25,
        max_jump_examples: int = 50,
    ) -> dict[str, Any]:
        """
        Non-fatal quality audit across cash equity partitions:
          1. VWAP turnover-unit invariant (`Low <= TurnoverValue/Volume <= High`).
          2. Unexplained >25% single-day drops without a recorded corporate action.
        """
        eq_dir = self.cfg.root_dir / "equities"
        vwap_anomalies: list[dict[str, Any]] = []
        jump_frames: list[pd.DataFrame] = []
        files_checked = 0

        if eq_dir.exists():
            resolver = SymbolRenameResolver.from_lake(self.cfg.root_dir)
            actions_cache: dict[str, pd.DataFrame] = {}
            for exch, act_name in (("NSE", "nse_corporate_actions.parquet"), ("BSE", "bse_corporate_actions.parquet")):
                ap = self.cfg.root_dir / "actions" / act_name
                actions_cache[exch] = read_table_safe(ap) if ap.exists() else pd.DataFrame()

            for exch in ("NSE", "BSE"):
                prefix = f"{exch.lower()}_cm_"
                for pq_path in sorted(eq_dir.glob(f"{prefix}*.parquet")):
                    yr_str = pq_path.stem.split("_")[-1]
                    if not yr_str.isdigit():
                        continue
                    try:
                        df = read_table_safe(pq_path)
                    except Exception:
                        continue
                    if df.empty:
                        continue
                    files_checked += 1
                    vwap_res = measure_turnover_unit(df)
                    if not vwap_res["is_healthy"]:
                        vwap_anomalies.append({"file": pq_path.name, **vwap_res})
                        logger.warning(
                            "[Doctor Quality] VWAP turnover anomaly in %s: unit=%s, bad=%d/%d (%.2f%%)",
                            pq_path.name, vwap_res["detected_unit"],
                            vwap_res["bad_rows"], vwap_res["rows_checked"],
                            vwap_res["violation_rate"] * 100.0,
                        )

                    jumps = unexplained_price_jumps(
                        df,
                        actions_cache[exch],
                        drop_threshold=drop_threshold,
                        resolver=resolver if exch == "NSE" else None,
                        exchange=exch,
                    )
                    if not jumps.empty:
                        jump_frames.append(jumps)

        all_jumps = pd.concat(jump_frames, ignore_index=True) if jump_frames else pd.DataFrame()
        likely_missing = int(all_jumps["LikelyMissingAction"].sum()) if not all_jumps.empty else 0
        if not all_jumps.empty:
            logger.warning(
                "[Doctor Quality] Detected %d unexplained >%.0f%% single-day drops (%d with exchange PrevClose step-down).",
                len(all_jumps), drop_threshold * 100.0, likely_missing,
            )
        return {
            "equity_files_checked": files_checked,
            "vwap_unit_anomalies": len(vwap_anomalies),
            "vwap_anomaly_details": vwap_anomalies,
            "unexplained_price_jumps": int(len(all_jumps)),
            "likely_missing_actions": likely_missing,
            "jump_examples": all_jumps.head(max_jump_examples).to_dict("records") if not all_jumps.empty else [],
        }
