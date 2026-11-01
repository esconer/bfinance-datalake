from __future__ import annotations

import gc
import io
import os
import time
import uuid
from pathlib import Path
import pandas as pd


def _has_parquet_engine() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return True
        except ImportError:
            return False


HAS_PARQUET = _has_parquet_engine()


def read_table_safe(path: Path) -> pd.DataFrame:
    """
    Read a Parquet partition via an in-memory BytesIO buffer so PyArrow never holds
    a lingering memory-map or OS file handle on Windows.
    """
    raw_bytes = path.read_bytes()
    buf = io.BytesIO(raw_bytes)
    if HAS_PARQUET:
        return pd.read_parquet(buf)
    return pd.read_pickle(buf)


def write_parquet_atomic(df: pd.DataFrame, target_path: Path) -> None:
    """Write a DataFrame to Zstandard-compressed Parquet atomically via a temporary file."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(f"{target_path.stem}_{uuid.uuid4().hex[:8]}.parquet.tmp")
    try:
        if HAS_PARQUET:
            df.to_parquet(tmp_path, compression="zstd", index=False)
        else:
            df.to_pickle(tmp_path)
        for attempt in range(10):
            try:
                os.replace(tmp_path, target_path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                gc.collect()
                time.sleep(0.05 * (attempt + 1))
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def upsert_parquet_partition(
    new_df: pd.DataFrame,
    target_path: Path,
    dedup_cols: list[str],
    sort_cols: list[str] | None = None,
) -> int:
    """
    Merge `new_df` into an existing Parquet file (if present), deduplicate by `dedup_cols`
    keeping the newest rows, sort, and write atomically. Returns total rows in partition.
    """
    if new_df is None or new_df.empty:
        return 0

    if target_path.exists():
        existing_df = read_table_safe(target_path)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        del existing_df
    else:
        combined = new_df.copy()

    valid_dedup = [c for c in dedup_cols if c in combined.columns]
    if valid_dedup:
        combined = combined.drop_duplicates(subset=valid_dedup, keep="last")

    valid_sort = [c for c in (sort_cols or valid_dedup) if c in combined.columns]
    if valid_sort:
        combined = combined.sort_values(valid_sort).reset_index(drop=True)

    write_parquet_atomic(combined, target_path)
    return len(combined)
