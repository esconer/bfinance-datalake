"""Deterministic offline regression tests for swarm task T-P5 (revised Patch 5).

Covers: contract version, profile-tier classification / filtering, the unified
security master, and atomic manifest writes. No network access.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from datalake.config import today_ist
from datalake.sync import (
    CONTRACT_VERSION,
    _classify_sync_profile,
    _matches_sync_profile,
    build_manifest,
    build_unified_security_master,
    prepare_release_staging,
)


def _one_parquet(directory, name):
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / name
    pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [1.0]}).to_parquet(p)
    return p


def _ident(symbol, bse_code, isin, exchange="NSE", aliases=("X",)):
    return SimpleNamespace(
        symbol=symbol,
        bse_code=bse_code,
        isin=isin,
        company_name=f"{symbol} Ltd",
        industry="IT",
        face_value=1.0,
        listing_date="2000-01-01",
        primary_exchange=exchange,
        aliases=tuple(aliases),
    )


class _StubEngine:
    def __init__(self, data_dir):
        tcs = _ident("TCS", "500425", "INE002A01018", "NSE", ("TCS", "TATA CONSULTANCY"))
        bse_only = _ident("BSEONLY", "999999", None, "BSE", ("BSEONLY",))
        self._by_symbol = {"TCS": tcs, "BSEONLY": bse_only}
        # TCS appears in both indexes with the same key -> must be deduped.
        self._by_bse = {"500425": tcs, "999999": bse_only}
        self.crosswalk_calls = 0

    def _ensure_crosswalk(self):
        self.crosswalk_calls += 1


# ---------------------------------------------------------------------------
# Section 3 assertion + contract version
# ---------------------------------------------------------------------------
def test_section3_contract_version_and_tier():
    assert CONTRACT_VERSION == "1.0.0"
    assert _matches_sync_profile("derivatives/nse_fo_2022.parquet", {}, "core_longterm") is False
    # Dynamic guard: a clearly historical F&O year is always excluded.
    old_year = today_ist().year - 10
    assert _matches_sync_profile(f"derivatives/nse_fo_{old_year}.parquet", {}, "core_longterm") is False


# ---------------------------------------------------------------------------
# (a) build_manifest contract_version + per-file profile
# ---------------------------------------------------------------------------
def test_build_manifest_contract_version_and_per_file_profile():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _one_parquet(d / "equities", "nse_cm_2026.parquet")
        _one_parquet(d / "derivatives", f"nse_fo_{today_ist().year}.parquet")
        _one_parquet(d / "derivatives", "nse_fo_2018.parquet")
        m = build_manifest(d)
        assert m["contract_version"] == "1.0.0"
        assert m["files"]["equities/nse_cm_2026.parquet"]["profile"] == "core_longterm"
        assert m["files"][f"derivatives/nse_fo_{today_ist().year}.parquet"]["profile"] == "derivatives_recent"
        assert m["files"]["derivatives/nse_fo_2018.parquet"]["profile"] == "derivatives_archive"
        # Persisted to disk too.
        assert json.loads((d / "manifest.json").read_text(encoding="utf-8"))["contract_version"] == "1.0.0"


def test_classify_sync_profile():
    cur = today_ist().year
    assert _classify_sync_profile("equities/nse_cm_2020.parquet") == "core_longterm"
    assert _classify_sync_profile("snapshots/foo.parquet") == "core_longterm"
    assert _classify_sync_profile(f"derivatives/nse_fo_{cur}.parquet") == "derivatives_recent"
    assert _classify_sync_profile("derivatives/nse_fo_2018.parquet") == "derivatives_archive"
    assert _classify_sync_profile("derivatives/nse_fo_2018_03.parquet") == "derivatives_archive"


# ---------------------------------------------------------------------------
# (c) / (d) revised profile tiers
# ---------------------------------------------------------------------------
def test_core_longterm_uses_manifest_profile_field():
    cur = today_ist().year
    # Metadata profile wins over path classification.
    assert _matches_sync_profile("some/odd/path.parquet", {"profile": "derivatives_recent"}, "core_longterm") is True
    assert _matches_sync_profile("some/odd/path.parquet", {"profile": "derivatives_archive"}, "core_longterm") is False
    assert _matches_sync_profile("derivatives/nse_fo_2018.parquet", {"profile": "core_longterm"}, "core_longterm") is True
    # Empty meta falls back to path classification.
    assert _matches_sync_profile(f"derivatives/nse_fo_{cur}.parquet", {}, "core_longterm") is True
    assert _matches_sync_profile("derivatives/nse_fo_2018.parquet", {}, "core_longterm") is False
    # full / None keep everything.
    assert _matches_sync_profile("anything.parquet", {}, "full") is True
    assert _matches_sync_profile("anything.parquet", {}, None) is True


def test_core_no_fo_profile():
    cur = today_ist().year
    # core_no_fo drops even the current-year F&O file (derivatives_recent).
    assert _matches_sync_profile("equities/nse_cm_2020.parquet", {}, "core_no_fo") is True
    assert _matches_sync_profile(f"derivatives/nse_fo_{cur}.parquet", {}, "core_no_fo") is False
    assert _matches_sync_profile("derivatives/nse_fo_2018.parquet", {}, "core_no_fo") is False
    assert _matches_sync_profile("x", {"profile": "derivatives_recent"}, "core_no_fo") is False
    with pytest.raises(ValueError, match="Unknown sync profile"):
        _matches_sync_profile("x", {}, "bogus")


# ---------------------------------------------------------------------------
# (b) unified security master
# ---------------------------------------------------------------------------
def test_build_unified_security_master():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        with patch("datalake.sync.UnifiedLakeEngine", _StubEngine):
            out = build_unified_security_master(d)
        assert out is not None
        assert out == d / "universe" / "security_master_unified.parquet"
        assert out.exists()
        df = pd.read_parquet(out)
        # TCS deduped across _by_symbol + _by_bse; BSE-only kept -> 2 rows.
        assert len(df) == 2, df
        assert list(df.columns) == [
            "symbol", "bse_code", "isin", "company_name", "industry",
            "face_value", "listing_date", "primary_exchange", "aliases_json",
        ]
        # Sorted by primary_exchange then symbol: BSE before NSE.
        assert list(df["primary_exchange"]) == ["BSE", "NSE"]
        assert list(df["symbol"]) == ["BSEONLY", "TCS"]
        tcs = df[df["symbol"] == "TCS"].iloc[0]
        assert tcs["bse_code"] == "500425"
        assert tcs["isin"] == "INE002A01018"
        assert json.loads(tcs["aliases_json"]) == ["TCS", "TATA CONSULTANCY"]


def test_build_unified_security_master_no_rows():
    class EmptyEngine:
        def __init__(self, data_dir):
            self._by_symbol = {}
            self._by_bse = {}

        def _ensure_crosswalk(self):
            pass

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        with patch("datalake.sync.UnifiedLakeEngine", EmptyEngine):
            assert build_unified_security_master(d) is None
        assert not (d / "universe" / "security_master_unified.parquet").exists()


# ---------------------------------------------------------------------------
# (e) atomic manifest writes (build_manifest + prepare_release_staging)
# ---------------------------------------------------------------------------
def test_build_manifest_write_is_atomic(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _one_parquet(d / "equities", "nse_cm_2026.parquet")
        real_replace = os.replace
        calls = []

        def rec(src, dst, *a, **k):
            calls.append((Path(src).name, Path(dst).name))
            return real_replace(src, dst, *a, **k)

        monkeypatch.setattr("datalake.sync.os.replace", rec)
        build_manifest(d)
        assert ("manifest.json.tmp", "manifest.json") in calls, calls
        assert not (d / "manifest.json.tmp").exists()


def test_prepare_release_staging_calls_master_and_writes_manifest_atomically(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        d, stg = root / "data", root / "stage"
        _one_parquet(d / "equities", "nse_cm_2026.parquet")
        (d / "state.db").write_text("sqlite-mock")

        master_calls = []
        monkeypatch.setattr(
            "datalake.sync.build_unified_security_master",
            lambda dd: (master_calls.append(dd), None)[1],
        )

        class StubEngine:
            def __init__(self, data_dir):
                pass

            def optimize_layout(self):
                pass

        monkeypatch.setattr("datalake.sync.UnifiedLakeEngine", StubEngine)
        monkeypatch.setattr("datalake.sync.build_market_screener_snapshot", lambda *a, **k: None)
        monkeypatch.setattr("datalake.sync.build_fo_option_chain_snapshot", lambda *a, **k: None)
        monkeypatch.setattr("datalake.sync.build_by_ticker_packs", lambda *a, **k: {})

        def fake_compacted(*a, **k):
            return {"compacted__nse_cm_hist.parquet": {"asset_name": "compacted__nse_cm_hist.parquet"}}

        monkeypatch.setattr("datalake.sync.build_compacted_equity_bundles", fake_compacted)

        real_replace = os.replace
        calls = []

        def rec(src, dst, *a, **k):
            calls.append((Path(src).name, Path(dst).name))
            return real_replace(src, dst, *a, **k)

        monkeypatch.setattr("datalake.sync.os.replace", rec)
        prepare_release_staging(d, stg)

        assert master_calls == [d.resolve()], master_calls
        # Both the initial build_manifest write and the post-bundle write are atomic.
        assert calls.count(("manifest.json.tmp", "manifest.json")) >= 2, calls
        assert (stg / "manifest.json").exists()
