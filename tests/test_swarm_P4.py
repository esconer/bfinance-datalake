"""Deterministic offline tests for revised Patch 4 (no network, no seeded lake download).

Covers ``UnifiedLakeEngine.health()``, the caller-controlled
``corporate_actions(include_non_adjusting=...)`` flag, and the
``history(adjust_dividends=...)`` passthrough to ``apply_adjustments``.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

from datalake.engine import SecurityIdentity, UnifiedLakeEngine


class _FakeResolver:
    def all_aliases(self, key):
        return {"RELIANCE"}


def _ident() -> SecurityIdentity:
    return SecurityIdentity(
        symbol="RELIANCE", bse_code=None, isin=None, company_name=None,
        aliases=("RELIANCE",), primary_exchange="NSE",
    )


def _seed_actions(root: Path) -> None:
    (root / "actions").mkdir(parents=True, exist_ok=True)
    rows = [
        {"symbol": "RELIANCE", "ExDate": pd.Timestamp("2024-01-10"), "ActionType": "DIVIDEND",
         "DividendPerShareRs": 5.0, "ShareMultiplier": 1.0, "subject": "x", "Purpose": "x"},
        {"symbol": "RELIANCE", "ExDate": pd.Timestamp("2024-02-10"), "ActionType": "SPLIT",
         "DividendPerShareRs": 0.0, "ShareMultiplier": 2.0, "subject": "x", "Purpose": "x"},
        {"symbol": "RELIANCE", "ExDate": pd.Timestamp("2024-03-12"), "ActionType": "DEMERGER",
         "DividendPerShareRs": 0.0, "ShareMultiplier": 1.0, "subject": "JioFin", "Purpose": "x"},
        {"symbol": "RELIANCE", "ExDate": pd.Timestamp("2024-04-12"), "ActionType": "RIGHTS",
         "DividendPerShareRs": 0.0, "ShareMultiplier": 1.0, "subject": "1:1", "Purpose": "x"},
        {"symbol": "RELIANCE", "ExDate": pd.Timestamp("2024-05-12"), "ActionType": "BUYBACK",
         "DividendPerShareRs": 0.0, "ShareMultiplier": 1.0, "subject": "buyback", "Purpose": "x"},
    ]
    pd.DataFrame(rows).to_parquet(root / "actions" / "nse_corporate_actions.parquet", index=False)


# --- health() ---

def test_P4_health_seeded_manifest_and_parquet_stats():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "equities").mkdir()
        (root / "actions").mkdir()
        (root / "manifest.json").write_text(json.dumps({
            "contract_version": "2.3.4",
            "generated_at": "2026-11-22T00:00:00Z",
            "latest_dates": {"equities": "2026-11-21", "actions": "2026-11-20"},
        }), encoding="utf-8")
        pd.DataFrame({"Close": [1.0, 2.0, 3.0]}).to_parquet(
            root / "equities" / "nse_cm_2024.parquet", index=False
        )
        pd.DataFrame({"ActionType": ["DIVIDEND"]}).to_parquet(
            root / "actions" / "nse_corporate_actions.parquet", index=False
        )
        expected_size = sum(p.stat().st_size for p in root.rglob("*.parquet"))
        expected_mb = round(expected_size / (1024 * 1024), 2)
        expected_root = str(root.resolve())

        h = UnifiedLakeEngine(root).health()

    assert h["root"] == expected_root
    assert h["contract_version"] == "2.3.4"
    assert h["generated_at"] == "2026-11-22T00:00:00Z"
    assert h["latest_dates"] == {"equities": "2026-11-21", "actions": "2026-11-20"}
    assert h["parquet_files"] == 2
    assert h["size_mb"] == expected_mb
    assert expected_size > 0
    assert h["ready"] is True


def test_P4_health_empty_lake_without_manifest():
    with tempfile.TemporaryDirectory() as tmp:
        h = UnifiedLakeEngine(Path(tmp)).health()

    assert h["ready"] is False
    assert h["parquet_files"] == 0
    assert h["size_mb"] == 0.0
    assert h["contract_version"] == "1.0.0"
    assert h["generated_at"] is None
    assert h["latest_dates"] == {}


def test_P4_health_corrupt_manifest_falls_back_to_defaults():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "manifest.json").write_text("{ not valid json", encoding="utf-8")
        (root / "equities").mkdir()
        pd.DataFrame({"Close": [1.0]}).to_parquet(root / "equities" / "nse_cm_2024.parquet", index=False)

        h = UnifiedLakeEngine(root).health()

    assert h["contract_version"] == "1.0.0"
    assert h["generated_at"] is None
    assert h["latest_dates"] == {}
    assert h["parquet_files"] == 1
    assert h["ready"] is True


def test_P4_health_non_dict_manifest_falls_back_to_defaults():
    """Valid JSON that is not an object must be tolerated like a corrupt manifest."""
    for label, payload in (
        ("list", b"[]"),
        ("null", b"null"),
        ("int", b"123"),
        ("str", b'"not-an-object"'),
    ):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "equities").mkdir()
            (root / "manifest.json").write_bytes(payload)
            pd.DataFrame({"Close": [1.0]}).to_parquet(root / "equities" / "nse_cm_2024.parquet", index=False)

            h = UnifiedLakeEngine(root).health()

        assert h["contract_version"] == "1.0.0", label
        assert h["generated_at"] is None, label
        assert h["latest_dates"] == {}, label
        assert h["parquet_files"] == 1, label
        assert h["ready"] is True, label


# --- history(adjust_dividends=...) ---

def _history_engine() -> tuple[UnifiedLakeEngine, Mock]:
    eng = UnifiedLakeEngine(Path("/tmp/opencode/p4-unused-lake"))
    eng.resolve = lambda q: _ident()  # type: ignore[method-assign]
    raw = pd.DataFrame({
        "Date": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")],
        "Close": [100.0, 101.0],
    })
    eng._cached_raw_equity = Mock(return_value=raw.copy())  # type: ignore[method-assign]
    applied = Mock(return_value=raw.copy())
    eng.adjuster = Mock()  # type: ignore[assignment]
    eng.adjuster.apply_adjustments = applied
    return eng, applied


def test_P4_history_adjust_dividends_false_forwarded_to_adjuster():
    eng, applied = _history_engine()
    eng.history("RELIANCE", adjust_dividends=False)

    assert applied.call_count == 1
    assert applied.call_args.args[1] == "RELIANCE"
    assert applied.call_args.kwargs["exchange"] == "NSE"
    assert applied.call_args.kwargs["auto_adjust"] is True
    assert applied.call_args.kwargs["include_future_splits"] is True
    assert applied.call_args.kwargs["adjust_dividends"] is False


def test_P4_history_default_adjust_dividends_true():
    eng, applied = _history_engine()
    eng.history("RELIANCE")

    assert applied.call_count == 1
    assert applied.call_args.kwargs["adjust_dividends"] is True


# --- corporate_actions default vs opt-in ---

def test_P4_corporate_actions_default_vs_opt_in():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed_actions(root)
        eng = UnifiedLakeEngine(root)
        eng.adjuster._resolver = _FakeResolver()  # type: ignore[assignment]
        eng.resolve = lambda q: _ident()  # type: ignore[method-assign]

        default_out = eng.corporate_actions("RELIANCE")
        opt_in_out = eng.corporate_actions("RELIANCE", include_non_adjusting=True)

    non_adj = {"DEMERGER", "RIGHTS", "BUYBACK"}
    assert set(default_out["ActionType"]) == {"DIVIDEND", "SPLIT"}
    assert non_adj.isdisjoint(set(default_out["ActionType"]))
    assert set(opt_in_out["ActionType"]) == {"DIVIDEND", "SPLIT", "DEMERGER", "RIGHTS", "BUYBACK"}
    assert len(opt_in_out[opt_in_out["ActionType"].isin(non_adj)]) == 3
