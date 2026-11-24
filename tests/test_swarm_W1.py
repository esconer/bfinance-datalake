"""Deterministic offline tests for Workstream W1 (LakeDoctor quality gates)."""

import sqlite3

import pandas as pd

from datalake.config import LakeConfig
from datalake.doctor import (
    LakeDoctor,
    measure_turnover_unit,
    unexplained_price_jumps,
)
from datalake.symbols import SymbolRenameResolver


def _rupee_df():
    return pd.DataFrame(
        {
            "Low": [100.0, 200.0],
            "High": [110.0, 220.0],
            "Volume": [1000.0, 500.0],
            "TurnoverValue": [105000.0, 105000.0],
        }
    )


def test_measure_turnover_unit_rupees_healthy_and_lakhs_crores_unhealthy():
    healthy = measure_turnover_unit(_rupee_df())
    assert healthy["detected_unit"] == "rupees"
    assert healthy["is_healthy"] is True
    assert healthy["rows_checked"] == 2
    assert healthy["bad_rows"] == 0
    assert healthy["violation_rate"] == 0.0

    lakhs = measure_turnover_unit(
        _rupee_df().assign(TurnoverValue=lambda d: d["TurnoverValue"] / 1e5)
    )
    assert lakhs["detected_unit"] == "lakhs"
    assert lakhs["is_healthy"] is False
    assert lakhs["bad_rows"] == 2

    crores = measure_turnover_unit(
        _rupee_df().assign(TurnoverValue=lambda d: d["TurnoverValue"] / 1e7)
    )
    assert crores["detected_unit"] == "crores"
    assert crores["is_healthy"] is False

    empty = measure_turnover_unit(pd.DataFrame(columns=["Low", "High", "Volume", "TurnoverValue"]))
    assert empty == {
        "rows_checked": 0,
        "bad_rows": 0,
        "violation_rate": 0.0,
        "median_vwap_ratio": None,
        "detected_unit": "empty",
        "is_healthy": True,
    }


def test_measure_turnover_unit_small_volume_2_decimal_rounding_tolerance():
    # 10 shares * Rs 100 = Rs 1000; a 2-decimal LACS rounding of +/-Rs 500
    # can shift turnover by Rs 0.05 => VWAP off by Rs 0.005 per share.
    df = pd.DataFrame(
        {
            "Low": [100.0],
            "High": [100.0],
            "Volume": [10.0],
            "TurnoverValue": [1000.05],
        }
    )
    tolerant = measure_turnover_unit(df, allow_lacs_rounding=True)
    assert tolerant["detected_unit"] == "rupees"
    assert tolerant["bad_rows"] == 0
    assert tolerant["is_healthy"] is True

    strict = measure_turnover_unit(df, allow_lacs_rounding=False)
    assert strict["bad_rows"] == 1
    assert strict["is_healthy"] is False


def _jump_df():
    return pd.DataFrame(
        {
            "Symbol": ["AAA", "AAA", "AAA", "BBB", "BBB"],
            "Date": [
                "2026-01-05", "2026-01-06", "2026-01-07",
                "2026-01-01", "2026-01-20",
            ],
            "Close": [100.0, 50.0, 48.0, 100.0, 50.0],
            # Exchange-reported PrevClose steps down overnight for AAA's drop.
            "PrevClose": [float("nan"), 50.0, 48.0, float("nan"), float("nan")],
        }
    )


def test_unexplained_price_jumps_flags_missing_action():
    jumps = unexplained_price_jumps(_jump_df())
    assert len(jumps) == 1
    hit = jumps.iloc[0]
    assert hit["Symbol"] == "AAA"
    assert hit["PctDrop"] == -50.0
    assert bool(hit["LikelyMissingAction"]) is True
    assert hit["ExchangePrevClose"] == 50.0


def test_unexplained_price_jumps_recorded_split_suppresses_jump():
    actions = pd.DataFrame(
        {
            "symbol": ["AAA"],
            "ExDate": ["2026-01-06"],
            "ActionType": ["SPLIT"],
        }
    )
    jumps = unexplained_price_jumps(_jump_df(), actions)
    assert jumps.empty


def test_unexplained_price_jumps_recorded_split_via_rename_alias_suppresses_jump():
    resolver = SymbolRenameResolver(
        pd.DataFrame(
            {
                "OldSymbol": ["OLD"],
                "NewSymbol": ["AAA"],
                "ChangeDate": ["2025-01-01"],
            }
        )
    )
    actions = pd.DataFrame(
        {
            "symbol": ["OLD"],
            "ExDate": ["2026-01-06"],
            "ActionType": ["SPLIT"],
        }
    )
    assert unexplained_price_jumps(_jump_df(), actions, resolver=resolver).empty


def test_unexplained_price_jumps_over_ten_day_gap_ignored():
    gap_df = pd.DataFrame(
        {
            "Symbol": ["BBB", "BBB"],
            "Date": ["2026-01-01", "2026-01-20"],
            "Close": [100.0, 50.0],
        }
    )
    assert unexplained_price_jumps(gap_df).empty


def _write_lake_equity_files(root):
    eq = root / "equities"
    eq.mkdir(parents=True, exist_ok=True)
    nse = pd.DataFrame(
        {
            "Symbol": ["AAA", "AAA", "AAA"],
            "Date": ["2026-01-05", "2026-01-06", "2026-01-07"],
            "Open": [100.0, 50.0, 48.0],
            "High": [110.0, 55.0, 52.0],
            "Low": [95.0, 45.0, 46.0],
            "Close": [100.0, 50.0, 48.0],
            "PrevClose": [float("nan"), 50.0, 48.0],
            "Volume": [1000.0, 1200.0, 900.0],
            # Lakhs-scaled turnover => VWAP unit anomaly.
            "TurnoverValue": [0.105, 0.06, 0.0432],
        }
    )
    nse.to_parquet(eq / "nse_cm_2026.parquet", index=False)
    bse = pd.DataFrame(
        {
            "Symbol": ["GOOD"] * 2,
            "Date": ["2026-01-05", "2026-01-06"],
            "Open": [100.0, 101.0],
            "High": [105.0, 106.0],
            "Low": [99.0, 100.0],
            "Close": [102.0, 103.0],
            "Volume": [1000.0, 1100.0],
            "TurnoverValue": [102000.0, 113300.0],
        }
    )
    bse.to_parquet(eq / "bse_cm_2026.parquet", index=False)


def test_audit_data_quality_returns_diagnostics_without_mutating(tmp_path):
    _write_lake_equity_files(tmp_path)
    before = {p.name: p.read_bytes() for p in (tmp_path / "equities").glob("*.parquet")}
    report = LakeDoctor(LakeConfig(root_dir=tmp_path)).audit_data_quality()

    assert set(report) == {
        "equity_files_checked",
        "vwap_unit_anomalies",
        "vwap_anomaly_details",
        "unexplained_price_jumps",
        "likely_missing_actions",
        "jump_examples",
    }
    assert report["equity_files_checked"] == 2
    assert report["vwap_unit_anomalies"] == 1
    assert report["vwap_anomaly_details"][0]["file"] == "nse_cm_2026.parquet"
    assert report["unexplained_price_jumps"] == 1
    assert report["likely_missing_actions"] == 1
    assert len(report["jump_examples"]) == 1
    assert bool(report["jump_examples"][0]["LikelyMissingAction"]) is True

    after = {p.name: p.read_bytes() for p in (tmp_path / "equities").glob("*.parquet")}
    assert after == before  # read-only diagnostic

    limited = LakeDoctor(LakeConfig(root_dir=tmp_path)).audit_data_quality(max_jump_examples=0)
    assert limited["jump_examples"] == []


def test_run_full_check_and_repair_keeps_five_key_contract(tmp_path):
    stats = LakeDoctor(LakeConfig(root_dir=tmp_path)).run_full_check_and_repair()
    assert set(stats) == {
        "tmp_files_cleaned",
        "corrupt_parquets_removed",
        "truncated_days_purged",
        "stale_watermarks_reset",
        "watermarks_rebuilt_from_disk",
    }


def test_participant_vol_partition_gets_watermark_and_truncation_guard(tmp_path):
    flows = tmp_path / "flows"
    flows.mkdir(parents=True)
    df = pd.DataFrame(
        {
            "Date": ["2026-01-05"] * 3 + ["2026-01-06"],
            "ClientType": ["FII", "FII", "DII", "FII"],
            "GrossBuy": [100.0, 200.0, 300.0, 50.0],
        }
    )
    pq = flows / "fao_participant_vol_2026.parquet"
    df.to_parquet(pq, index=False)

    cfg = LakeConfig(root_dir=tmp_path)
    stats = LakeDoctor(cfg).run_full_check_and_repair()
    assert stats["truncated_days_purged"] == 1

    conn = sqlite3.connect(cfg.state_db_path)
    rows = conn.execute(
        "SELECT partition_key, row_count FROM ingestion_log WHERE dataset = 'nse_participant_vol'"
    ).fetchall()
    conn.close()
    assert rows == [("2026-01-05", 3)]

    remaining = pd.read_parquet(pq)
    assert set(remaining["Date"].astype(str)) == {"2026-01-05"}
