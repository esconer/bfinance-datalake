"""Deterministic offline regression tests for W5 (sync fixes + CLI wiring).

No network: all filesystem state lives in pytest's ``tmp_path`` and every scraper,
engine and HTTP client is mocked.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from datalake import cli, sync


class AsyncAll(MagicMock):
    """MagicMock whose child attributes are awaitable (mirrors async scrapers)."""

    def __getattr__(self, name):
        if name.startswith("_"):
            return super().__getattr__(name)
        child = AsyncMock(name=name)
        setattr(self, name, child)
        return child


def _write(root: Path, rel: str, df: pd.DataFrame) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False)


def test_manifest_watermarks_for_exdate_periodend_meetingdate_changedate(tmp_path):
    cases = {
        "fundamentals/xbrl_financials.parquet": ("PeriodEnd", "2026-03-31"),
        "actions/nse_corporate_actions.parquet": ("ExDate", "2026-04-10"),
        "documents/upcoming_board_meetings.parquet": ("MeetingDate", "2026-05-12"),
        "actions/nse_symbol_changes.parquet": ("ChangeDate", "2026-06-01"),
    }
    for rel, (col, val) in cases.items():
        _write(tmp_path, rel, pd.DataFrame({col: [val]}))
    # All-NaN/mixed-format dates must not produce a bogus watermark, and "Date"
    # keeps precedence when several candidate columns are present.
    _write(tmp_path, "misc/nat_dates.parquet", pd.DataFrame({"ChangeDate": [None, None]}))
    _write(tmp_path, "equities/nse_cm_2026.parquet", pd.DataFrame({
        "Date": ["2026-02-03"], "ExDate": ["2020-01-01"],
    }))

    man = sync.build_manifest(tmp_path)
    for rel, (_, val) in cases.items():
        assert man["latest_dates"][rel] == val
        assert man["files"][rel]["latest_date"] == val
    assert "misc/nat_dates.parquet" not in man["latest_dates"]
    assert man["latest_dates"]["equities/nse_cm_2026.parquet"] == "2026-02-03"


def test_unified_master_builder_bypasses_unified_cache(tmp_path):
    eng = MagicMock()
    eng._by_symbol = {}
    eng._by_bse = {}
    with patch.object(sync, "UnifiedLakeEngine", return_value=eng):
        assert sync.build_unified_security_master(tmp_path) is None
    assert eng._ensure_crosswalk.call_count == 1
    assert eng._ensure_crosswalk.call_args.kwargs["use_unified_cache"] is False


def test_release_staging_flags_vwap_anomalies_without_failing(tmp_path):
    audit = MagicMock(return_value={
        "vwap_unit_anomalies": 2,
        "vwap_anomaly_details": [{"file": "nse_cm_2026.parquet"}, {"file": "nse_cm_2025.parquet"}],
    })
    log = MagicMock()
    eng = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(patch.object(sync, "UnifiedLakeEngine", return_value=eng))
        stack.enter_context(patch("datalake.doctor.LakeDoctor.audit_data_quality", audit))
        stack.enter_context(patch.object(sync, "logger", log))
        manifest = sync.prepare_release_staging(tmp_path, tmp_path / "stage")
    assert manifest["file_count"] == 0
    assert audit.call_count == 1
    assert any(c.args[1] == 2 for c in log.warning.call_args_list)
    assert any("nse_cm_2026.parquet" in c.args[2] for c in log.warning.call_args_list)


def test_release_staging_swallows_quality_audit_errors(tmp_path):
    audit = MagicMock(side_effect=RuntimeError("audit exploded"))
    log = MagicMock()
    eng = MagicMock()
    with ExitStack() as stack:
        stack.enter_context(patch.object(sync, "UnifiedLakeEngine", return_value=eng))
        stack.enter_context(patch("datalake.doctor.LakeDoctor.audit_data_quality", audit))
        stack.enter_context(patch.object(sync, "logger", log))
        manifest = sync.prepare_release_staging(tmp_path, tmp_path / "stage")
    assert manifest["file_count"] == 0
    assert audit.call_count == 1
    assert any("Quality audit skipped" in str(c) for c in log.debug.call_args_list)
    assert not log.warning.call_args_list


def test_cli_upgrade_sparse_flag_forwards_to_run_xbrl_sync(tmp_path):
    runner = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch.object(sys, "argv", [
            "datalake", "--data-dir", str(tmp_path), "xbrl", "--upgrade-sparse",
        ]))
        stack.enter_context(patch.object(cli, "run_xbrl_sync", runner))
        stack.enter_context(patch.object(cli, "print_status", MagicMock()))
        cli.main()
    runner.assert_awaited_once()
    assert runner.await_args.kwargs["upgrade_sparse"] is True


def test_run_xbrl_sync_forwards_upgrade_sparse_to_sync_all_market(tmp_path):
    xbrl = AsyncAll()
    xbrl.sync_all_market.return_value = ([], [])
    doc = AsyncAll()
    http = AsyncAll()
    with ExitStack() as stack:
        stack.enter_context(patch.object(cli, "ResilientHTTPClient", MagicMock()))
        stack.enter_context(patch.object(cli, "StateStore", MagicMock(return_value=MagicMock())))
        stack.enter_context(patch.object(cli, "XBRLScraper", MagicMock(return_value=xbrl)))
        stack.enter_context(patch.object(cli, "DealsAndDocumentsScraper", MagicMock(return_value=doc)))
        asyncio.run(cli.run_xbrl_sync(
            MagicMock(), [], [], True, None, force=False, upgrade_sparse=True,
        ))
    xbrl.sync_all_market.assert_awaited_once()
    kwargs = xbrl.sync_all_market.await_args.kwargs
    assert kwargs["upgrade_sparse"] is True
    assert kwargs["force"] is False
    assert kwargs["max_companies"] is None


def _run_cli_pipeline(which: str):
    """Run backfill or daily with fully mocked scrapers; return captured fakes."""
    fakes = {name: AsyncAll() for name in (
        "UniverseAndActionsScraper", "IndicesScraper", "EquityEODScraper",
        "DerivativesEODScraper", "FundsAndFlowsScraper", "DealsAndDocumentsScraper",
        "XBRLScraper", "ResilientHTTPClient", "LakeDoctor",
    )}
    state = MagicMock()
    state.max_ok_date.side_effect = lambda key: (
        date(2026, 10, 1) if key == "nse_participant_vol" else None
    )
    with ExitStack() as stack:
        for name, fake in fakes.items():
            stack.enter_context(patch.object(cli, name, MagicMock(return_value=fake)))
        stack.enter_context(patch.object(cli, "StateStore", MagicMock(return_value=state)))
        stack.enter_context(patch.object(cli, "_resolve_published_eod_end",
                                         new=AsyncMock(return_value=date(2020, 1, 1))))
        if which == "backfill":
            asyncio.run(cli.run_backfill(
                MagicMock(), date(2010, 1, 1), date(2020, 1, 2), False, False, force=False,
            ))
        else:
            asyncio.run(cli.run_daily_update(MagicMock(), 7, include_fo=False))
    return fakes, state


def test_backfill_wires_participant_volume_sync():
    fakes, _ = _run_cli_pipeline("backfill")
    ff = fakes["FundsAndFlowsScraper"]
    ff.sync_participant_vol.assert_awaited_once()
    kwargs = ff.sync_participant_vol.await_args.kwargs
    assert kwargs["start_date"] == date(2012, 1, 2)
    assert kwargs["end_date"] == date(2020, 1, 1)
    assert kwargs["force"] is False


def test_daily_update_wires_participant_volume_sync_and_watermark():
    fakes, state = _run_cli_pipeline("daily")
    ff = fakes["FundsAndFlowsScraper"]
    state.max_ok_date.assert_any_call("nse_participant_vol")
    ff.sync_participant_vol.assert_awaited_once()
    assert ff.sync_participant_vol.await_args.args[0] == date(2026, 10, 1)
    assert ff.sync_participant_vol.await_args.args[1] == date(2020, 1, 1)


def test_daily_update_prefetches_participant_volume_archive():
    client = AsyncAll()
    with ExitStack() as stack:
        for name in (
            "UniverseAndActionsScraper", "IndicesScraper", "EquityEODScraper",
            "DerivativesEODScraper", "FundsAndFlowsScraper", "DealsAndDocumentsScraper",
            "XBRLScraper", "LakeDoctor",
        ):
            stack.enter_context(patch.object(cli, name, MagicMock(return_value=AsyncAll())))
        stack.enter_context(patch.object(cli, "StateStore", MagicMock(return_value=MagicMock(
            max_ok_date=MagicMock(return_value=None),
        ))))
        client_cm = MagicMock()
        client_cm.__aenter__ = AsyncMock(return_value=client)
        client_cm.__aexit__ = AsyncMock(return_value=False)
        stack.enter_context(patch.object(cli, "ResilientHTTPClient", MagicMock(return_value=client_cm)))
        stack.enter_context(patch.object(cli, "_resolve_published_eod_end",
                                         new=AsyncMock(return_value=date(2020, 1, 1))))
        asyncio.run(cli.run_daily_update(MagicMock(), 7, include_fo=False))
    client.prefetch_archives_via_proxy.assert_awaited_once()
    urls = client.prefetch_archives_via_proxy.await_args.args[0]
    assert any("fao_participant_vol_01012020.csv" in u for u in urls)


def test_doctor_command_prints_quality_summary_without_jump_examples(tmp_path, capsys):
    doc = MagicMock()
    doc.run_full_check_and_repair.return_value = {"repaired": 1}
    doc.audit_data_quality.return_value = {
        "equity_files_checked": 4,
        "vwap_unit_anomalies": 1,
        "vwap_anomaly_details": [{"file": "nse_cm_2026.parquet"}],
        "unexplained_price_jumps": 3,
        "likely_missing_actions": 2,
        "jump_examples": [{"Symbol": "VERYVERBOSE", "Pct": -0.4}] * 50,
    }
    with ExitStack() as stack:
        stack.enter_context(patch.object(sys, "argv", [
            "datalake", "--data-dir", str(tmp_path), "doctor",
        ]))
        stack.enter_context(patch.object(cli, "LakeDoctor", MagicMock(return_value=doc)))
        stack.enter_context(patch.object(cli, "print_status", MagicMock()))
        cli.main()
    doc.audit_data_quality.assert_called_once_with()
    out = capsys.readouterr().out
    assert "Doctor Repair Summary:" in out
    assert "Doctor Quality Audit:" in out
    assert "VERYVERBOSE" not in out
    assert "jump_examples" not in out
    assert "'unexplained_price_jumps': 3" in out
