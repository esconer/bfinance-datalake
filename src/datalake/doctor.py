from __future__ import annotations

from datetime import datetime
import logging
import sqlite3
import pandas as pd

from datalake.config import IST, LakeConfig
from datalake.state import StateStore
from datalake.storage import read_table_safe, write_parquet_atomic

logger = logging.getLogger("datalake.doctor")


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

        for tmp_file in self.cfg.root_dir.rglob("*.tmp"):
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
        valid_dates_on_disk: dict[str, set[str]] = {
            "equity_eod_nse": set(),
            "equity_eod_bse": set(),
            "fo_eod_nse": set(),
            "fo_eod_bse": set(),
            "indices_daily_all": set(),
            "nse_participant_oi": set(),
        }

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

            if df.empty or "Date" not in df.columns:
                continue

            fname = pq_path.name
            ds_name = None
            min_rows_for_full_day = 200
            purge_below = 1
            if fname.startswith("nse_cm_"):
                ds_name = "equity_eod_nse"
                yr = int(fname.split("_")[-1].split(".")[0])
                min_rows_for_full_day = 200
                purge_below = 200 if yr >= 2016 else 1
            elif fname.startswith("bse_cm_"):
                ds_name = "equity_eod_bse"
                yr = int(fname.split("_")[-1].split(".")[0])
                min_rows_for_full_day = 200
                purge_below = 200 if yr >= 2016 else 1
            elif fname.startswith("nse_fo_"):
                ds_name = "fo_eod_nse"
                min_rows_for_full_day = 50
                purge_below = 50
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
                good_dates = set(counts[counts >= min_rows_for_full_day].index)
                valid_dates_on_disk[ds_name].update(good_dates)

        now_str = datetime.now(IST).isoformat()
        with state._session() as conn:
            for ds_name, disk_dates in valid_dates_on_disk.items():
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
                        VALUES (?, ?, 'OK', 1, ?)
                        ON CONFLICT(dataset, partition_key) DO UPDATE SET
                            status = 'OK', updated_at = excluded.updated_at
                        """,
                        [(ds_name, k, now_str) for k in unrecorded_on_disk],
                    )
                    stats["watermarks_rebuilt_from_disk"] += len(unrecorded_on_disk)

        logger.info("[Doctor] Self-healing check complete: %s", stats)
        return stats
