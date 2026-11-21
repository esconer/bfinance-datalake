"""Deterministic offline regression tests for swarm task T-D7.

Covers: safe post-download retirement pruning, atomic manifest replace,
`profile` filtering (`core_longterm`), doctor's non-yearly Parquet guard and
the `face_value` field in per-ticker pack metadata. No network access.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from datalake.config import LakeConfig, today_ist
from datalake.doctor import LakeDoctor
from datalake.sync import (
    _matches_sync_profile,
    build_by_ticker_packs,
    compute_sha256,
    pull_from_github_release,
)


def _install_client(cls_patch, manifest: dict, *, fail_assets: bool = False,
                    asset_content: bytes = b"x"):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = manifest
    resp.content = asset_content

    def get(url: str, **kwargs):
        if url.endswith("/manifest.json"):
            return resp
        if fail_assets:
            resp.raise_for_status.side_effect = RuntimeError("simulated network drop")
        else:
            resp.raise_for_status.side_effect = None
        return resp

    client = MagicMock()
    client.client = None
    client.get = AsyncMock(side_effect=get)
    mock_cls = cls_patch.start()
    mock_cls.return_value.__aenter__.return_value = client
    return client


async def _no_sleep(*args, **kwargs):
    return None


def _one_local_parquet(directory: Path, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    p = directory / name
    pd.DataFrame({"Date": [pd.Timestamp("2024-01-01")], "Close": [1.0]}).to_parquet(p)
    return p


# ---------------------------------------------------------------------------
# (a) retired parquet must survive a failed / partial pull
# ---------------------------------------------------------------------------
def test_retired_parquet_survives_failed_download():
    with tempfile.TemporaryDirectory() as td:
        target = Path(td)
        retired = _one_local_parquet(target / "equities", "nse_cm_2020.parquet")
        _one_local_parquet(target / "equities", "nse_cm_2026.parquet")
        manifest = {"files": {
            "equities/nse_cm_2026.parquet": {
                "asset_name": "equities__nse_cm_2026.parquet",
                "size_bytes": 1, "sha256": "deadbeef",
            }
        }}
        cls_patch = patch("datalake.sync.ResilientHTTPClient")
        _install_client(cls_patch, manifest, fail_assets=True)
        try:
            with patch("datalake.sync.asyncio.sleep", new=_no_sleep):
                with pytest.raises(RuntimeError, match="simulated network drop"):
                    asyncio.run(pull_from_github_release("u/r", data_dir=target))
        finally:
            cls_patch.stop()
        assert retired.exists(), "retired parquet must NOT be deleted when downloads fail"


def test_no_prune_for_partial_pulls():
    """Pruning is gated on pattern is None AND profile == 'full'."""
    for kwargs in ({"pattern": "equities"}, {"profile": "core_longterm"}):
        with tempfile.TemporaryDirectory() as td:
            target = Path(td)
            retired = _one_local_parquet(target / "equities", "nse_cm_2020.parquet")
            local = _one_local_parquet(target / "equities", "nse_cm_2026.parquet")
            sha, sz = compute_sha256(local), local.stat().st_size
            manifest = {"files": {
                "equities/nse_cm_2026.parquet": {
                    "asset_name": "equities__nse_cm_2026.parquet",
                    "size_bytes": sz, "sha256": sha,
                }
            }}
            cls_patch = patch("datalake.sync.ResilientHTTPClient")
            _install_client(cls_patch, manifest)
            try:
                asyncio.run(pull_from_github_release("u/r", data_dir=target, **kwargs))
            finally:
                cls_patch.stop()
            assert retired.exists(), f"retired parquet pruned for partial pull {kwargs}"


def test_retired_parquet_pruned_after_successful_full_sync():
    with tempfile.TemporaryDirectory() as td:
        target = Path(td)
        retired = _one_local_parquet(target / "equities", "nse_cm_2020.parquet")
        local = _one_local_parquet(target / "equities", "nse_cm_2026.parquet")
        sha, sz = compute_sha256(local), local.stat().st_size
        manifest = {"files": {
            "equities/nse_cm_2026.parquet": {
                "asset_name": "equities__nse_cm_2026.parquet",
                "size_bytes": sz, "sha256": sha,
            }
        }}
        cls_patch = patch("datalake.sync.ResilientHTTPClient")
        _install_client(cls_patch, manifest)
        try:
            asyncio.run(pull_from_github_release("u/r", data_dir=target))
        finally:
            cls_patch.stop()
        assert not retired.exists(), "full successful sync should prune retired partitions"


# ---------------------------------------------------------------------------
# (b) profile support / historical F&O filtering
# ---------------------------------------------------------------------------
def test_matches_sync_profile():
    cur = str(today_ist().year)
    assert _matches_sync_profile("equities/nse_cm_2026.parquet", "full") is True
    assert _matches_sync_profile("equities/nse_cm_2026.parquet", None) is True
    assert _matches_sync_profile("derivatives/nse_fo_2018.parquet", "full") is True
    # core_longterm: non-F&O paths always kept
    assert _matches_sync_profile("equities/nse_cm_2020.parquet", "core_longterm") is True
    assert _matches_sync_profile("snapshots/market_screener_latest.parquet", "core_longterm") is True
    # historical F&O chains dropped, current-year yearly file kept
    assert _matches_sync_profile("derivatives/nse_fo_2018.parquet", "core_longterm") is False
    assert _matches_sync_profile(f"derivatives/nse_fo_{cur}.parquet", "core_longterm") is True
    assert _matches_sync_profile("derivatives/nse_fo_2020_03.parquet", "core_longterm") is False
    assert _matches_sync_profile("derivatives/bse_fo_2019.parquet", "core_longterm") is False
    with pytest.raises(ValueError, match="Unknown sync profile"):
        _matches_sync_profile("x", "unknown-profile")


def test_core_longterm_filters_historical_fo_during_pull():
    cur = today_ist().year
    content = b"deterministic-payload"
    sha = __import__("hashlib").sha256(content).hexdigest()
    paths = [
        f"derivatives/nse_fo_{cur}.parquet",
        "derivatives/nse_fo_2018.parquet",
        "equities/nse_cm_2020.parquet",
    ]
    manifest = {"files": {
        p: {
            "asset_name": p.replace("/", "__"),
            "size_bytes": len(content),
            "sha256": sha,
        }
        for p in paths
    }}
    with tempfile.TemporaryDirectory() as td:
        target = Path(td)
        cls_patch = patch("datalake.sync.ResilientHTTPClient")
        _install_client(cls_patch, manifest, asset_content=content)
        try:
            stats = asyncio.run(pull_from_github_release(
                "u/r", data_dir=target, profile="core_longterm"))
        finally:
            cls_patch.stop()
        assert stats["downloaded"] == 2, stats
        assert (target / f"derivatives/nse_fo_{cur}.parquet").exists()
        assert (target / "equities/nse_cm_2020.parquet").exists()
        assert not (target / "derivatives/nse_fo_2018.parquet").exists(), \
            "historical F&O chain must be skipped under core_longterm"


def test_unknown_profile_rejected():
    with tempfile.TemporaryDirectory() as td:
        cls_patch = patch("datalake.sync.ResilientHTTPClient")
        _install_client(cls_patch, {"files": {"a.parquet": {
            "asset_name": "a.parquet", "size_bytes": 1, "sha256": "x"}}})
        try:
            with pytest.raises(ValueError, match="Unknown sync profile"):
                asyncio.run(pull_from_github_release("u/r", data_dir=td, profile="bogus"))
        finally:
            cls_patch.stop()


# ---------------------------------------------------------------------------
# (c) atomic manifest replace
# ---------------------------------------------------------------------------
def test_manifest_written_via_tmp_and_os_replace(monkeypatch):
    with tempfile.TemporaryDirectory() as td:
        target = Path(td)
        local = _one_local_parquet(target / "equities", "nse_cm_2026.parquet")
        sha, sz = compute_sha256(local), local.stat().st_size
        manifest = {"files": {
            "equities/nse_cm_2026.parquet": {
                "asset_name": "equities__nse_cm_2026.parquet",
                "size_bytes": sz, "sha256": sha,
            }
        }}
        real_replace = os.replace
        calls = []

        def recording_replace(src, dst, *a, **k):
            calls.append((Path(src), Path(dst)))
            return real_replace(src, dst, *a, **k)

        monkeypatch.setattr("datalake.sync.os.replace", recording_replace)
        cls_patch = patch("datalake.sync.ResilientHTTPClient")
        _install_client(cls_patch, manifest)
        try:
            asyncio.run(pull_from_github_release("u/r", data_dir=target))
        finally:
            cls_patch.stop()
        expected_tmp = target / "manifest.json.tmp"
        expected_dst = target / "manifest.json"
        assert (expected_tmp, expected_dst) in calls, calls
        assert not expected_tmp.exists(), "tmp manifest must be consumed by os.replace"
        assert expected_dst.exists()
        assert json.loads(expected_dst.read_text(encoding="utf-8")) == manifest


# ---------------------------------------------------------------------------
# (d) doctor: non-yearly parquet names must not crash int(yr)
# ---------------------------------------------------------------------------
def test_doctor_handles_non_yearly_parquet_names():
    with tempfile.TemporaryDirectory() as td:
        cfg = LakeConfig(root_dir=Path(td))
        cfg.ensure_dirs()
        df = pd.DataFrame({
            "Date": [pd.Timestamp("2024-10-29")] * 250,
            "Symbol": [f"S{i}" for i in range(250)],
            "Close": [100.0] * 250,
        })
        df.to_parquet(cfg.root_dir / "equities" / "nse_cm_hist.parquet")
        df.to_parquet(cfg.root_dir / "equities" / "bse_cm_hist.parquet")
        df.to_parquet(cfg.root_dir / "derivatives" / "nse_fo_hist.parquet")
        df.to_parquet(cfg.root_dir / "equities" / "nse_cm_2024.parquet")
        report = LakeDoctor(cfg).run_full_check_and_repair()
        assert report["corrupt_parquets_removed"] == 0
        assert report["watermarks_rebuilt_from_disk"] >= 1


# ---------------------------------------------------------------------------
# (e) face_value in per-ticker pack meta
# ---------------------------------------------------------------------------
def test_face_value_written_in_by_ticker_meta():
    cur = today_ist().year

    class StubResolver:
        _back_edges: dict = {}
        _fwd_edges: dict = {}

        def all_aliases(self, sym):
            return [sym]

    class StubIdent:
        symbol = "TCS"
        bse_code = "500425"
        isin = "INE002A01018"
        company_name = "Tata Consultancy Services"
        face_value = 1.0
        industry = "IT"

    class StubEngine:
        def __init__(self, data_dir):
            self.resolver = StubResolver()
            ident = StubIdent()
            self._by_symbol = {"TCS": ident}
            self._by_bse = {}
            self._by_isin = {}
            self.adjuster = None

        def _ensure_crosswalk(self):
            pass

        @staticmethod
        def _dedup_nse_equity_series(df):
            return df

    with tempfile.TemporaryDirectory() as td:
        data_dir, staging_dir = Path(td) / "data", Path(td) / "stage"
        eq = data_dir / "equities"
        eq.mkdir(parents=True)
        staging_dir.mkdir(parents=True)
        pd.DataFrame({
            "Symbol": ["TCS"] * 3,
            "Date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
            "Open": [1.0] * 3, "High": [1.0] * 3, "Low": [1.0] * 3,
            "Close": [1.0] * 3, "Volume": [100] * 3,
        }).to_parquet(eq / f"nse_cm_{cur}.parquet")
        with patch("datalake.sync.UnifiedLakeEngine", StubEngine):
            packs_meta = build_by_ticker_packs(data_dir, staging_dir, {"files": {}})
        assert "by_ticker__nse_index.json" in packs_meta
        payload = json.loads((staging_dir / "by_ticker__nse_index.json").read_text(encoding="utf-8"))
        meta = payload["tickers"]["TCS"]["meta"]
        assert meta["face_value"] == 1.0
        assert meta["company_name"] == "Tata Consultancy Services"
