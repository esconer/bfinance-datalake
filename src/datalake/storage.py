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


NUMERIC_FLOAT_COLS: frozenset[str] = frozenset({
    # Corporate Action numeric columns
    "DividendPerShareRs",
    "BonusNew",
    "BonusHeld",
    "SplitFromFV",
    "SplitToFV",
    "ShareMultiplier",
    # Equity & Derivatives OHLCV + Delivery numeric columns
    "Open",
    "High",
    "Low",
    "Close",
    "Last",
    "PrevClose",
    "Volume",
    "TurnoverValue",
    "Trades",
    "DeliverableQty",
    "DeliveryPct",
    "StrikePrice",
    "SettlePrice",
    "UnderlyingPrice",
    "Contracts",
    "OpenInterest",
    "ChangeInOI",
    # Index & Mutual Fund numeric columns
    "TRI",
    "PE",
    "PB",
    "DivYield",
    "TurnoverCr",
    "NAV",
    # Shareholding numeric columns
    "PromoterPct",
    "PublicPct",
    "TotalShares",
    "PromoterPledgedShares",
    "PromoterPledgedPct",
    "MutualFundsPct",
    "FPI_Pct",
    "InsurancePct",
    "RetailPct",
    "Shares",
    "PctOfTotal",
    "PledgedShares",
    "PledgedPct",
    # Deals & Flows numeric columns
    "Quantity Traded",
    "Trade Price / Wght. Avg. Price",
    "buyValue",
    "sellValue",
    "netValue",
})

_NUMERIC_SUFFIXES: tuple[str, ...] = ("_Cr", "_Rs", "_Pct")


def _is_numeric_col(col_name: str) -> bool:
    return col_name in NUMERIC_FLOAT_COLS or col_name.endswith(_NUMERIC_SUFFIXES)


def _clean_object_value(v: object) -> str | None:
    if v is None or v is pd.NA:
        return None
    if isinstance(v, float) and (v != v):
        return None
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return str(v)


def _coerce_numeric_series(s: pd.Series, default_val: float | None = None) -> pd.Series:
    if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
        s = s.map(
            lambda v: v.decode("utf-8", errors="replace").replace(",", "").strip()
            if isinstance(v, (bytes, bytearray))
            else (
                None
                if (v is None or v is pd.NA or (isinstance(v, float) and v != v))
                else (v.replace(",", "").strip() if isinstance(v, str) else v)
            )
        )
    res = pd.to_numeric(s, errors="coerce").astype("float64")
    if default_val is not None:
        res = res.fillna(default_val).astype("float64")
    return res


def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure numeric action columns are float64 and object columns never mix bytes/numbers/strings when PyArrow serializes to Parquet."""
    out = df.copy()
    if out.columns.duplicated().any():
        out = out.loc[:, ~out.columns.duplicated()].copy()

    for col in out.columns:
        if _is_numeric_col(str(col)):
            default_v = 1.0 if col == "ShareMultiplier" else None
            out[col] = _coerce_numeric_series(out[col], default_val=default_v)
        elif pd.api.types.is_object_dtype(out[col]):
            out[col] = out[col].map(_clean_object_value)
    return out


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


def write_parquet_atomic(
    df: pd.DataFrame,
    target_path: Path,
    row_group_size: int = 10_000,
    already_sanitized: bool = False,
) -> None:
    """
    Write a DataFrame to Zstandard-compressed Parquet atomically via a temporary file with
    `row_group_size=10_000` so remote HTTP Byte-Range queries (DuckDB httpfs / PyArrow)
    only download ~250 KB per stock instead of the whole file.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df = df if already_sanitized else sanitize_for_parquet(df)
    tmp_path = target_path.with_name(f"{target_path.stem}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        if HAS_PARQUET:
            clean_df.to_parquet(
                tmp_path,
                compression="zstd",
                index=False,
                row_group_size=row_group_size,
            )
        else:
            clean_df.to_pickle(tmp_path)

        last_err: Exception | None = None
        for attempt in range(8):
            try:
                os.replace(tmp_path, target_path)
                return
            except PermissionError as exc:
                last_err = exc
                gc.collect()
                time.sleep(0.05 * (attempt + 1))
        if last_err is not None:
            raise last_err
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


def upsert_parquet_partition(
    new_df: pd.DataFrame,
    target_path: Path,
    dedup_cols: list[str],
    sort_cols: list[str] | None = None,
    row_group_size: int = 10_000,
) -> int:
    """
    Merge `new_df` into an existing Parquet file (if present), deduplicate by `dedup_cols`
    keeping the newest rows, sort, and write atomically. Returns total rows in partition.
    """
    if new_df is None or new_df.empty:
        return 0

    if target_path.exists():
        existing_df = read_table_safe(target_path)
        if existing_df.columns.duplicated().any():
            existing_df = existing_df.loc[:, ~existing_df.columns.duplicated()].copy()
        new_clean = new_df.loc[:, ~new_df.columns.duplicated()].copy() if new_df.columns.duplicated().any() else new_df
        combined = sanitize_for_parquet(pd.concat([existing_df, new_clean], ignore_index=True))
        del existing_df, new_clean
    else:
        combined = sanitize_for_parquet(new_df)

    valid_dedup = [c for c in dedup_cols if c in combined.columns]
    if valid_dedup:
        combined = combined.drop_duplicates(subset=valid_dedup, keep="last")

    if sort_cols is None and "Symbol" in combined.columns and "Date" in combined.columns:
        sort_cols = ["Symbol", "Date"]

    valid_sort = [c for c in (sort_cols or valid_dedup) if c in combined.columns]
    if valid_sort:
        combined = combined.sort_values(valid_sort).reset_index(drop=True)

    write_parquet_atomic(combined, target_path, row_group_size=row_group_size, already_sanitized=True)
    return len(combined)
