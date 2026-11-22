"""Offline regression tests for swarm bug T-D8B (revised Patch 4 contract).

``UnifiedLakeEngine.corporate_actions()`` exposes a caller-controlled
``include_non_adjusting`` flag (default False). The query view forwards that
flag to all three ``get_symbol_actions()`` lookups: with the default the
price-adjusting view hides DEMERGER / RIGHTS / BUYBACK rows, while opting in
surfaces them. The price-adjustment path (``apply_adjustments``) keeps using
the adjuster default and is unaffected. No network / no on-disk lake required.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import Mock, call

import pandas as pd

from datalake.adjuster import OHLCVAdjuster
from datalake.engine import SecurityIdentity, UnifiedLakeEngine


class _FakeResolver:
    """Deterministic alias resolver: no dependency on on-disk crosswalk data."""

    def all_aliases(self, key):
        return {"RELIANCE"}


def _row(action, exdate, dividend=0.0, multiplier=1.0, subject="x", purpose="x"):
    return {
        "symbol": "RELIANCE",
        "ExDate": pd.Timestamp(exdate),
        "ActionType": action,
        "DividendPerShareRs": dividend,
        "ShareMultiplier": multiplier,
        "subject": subject,
        "Purpose": purpose,
    }


def _seed(root: Path, rows: list[dict]) -> None:
    (root / "actions").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(root / "actions" / "nse_corporate_actions.parquet", index=False)


def _fixture_rows() -> list[dict]:
    return [
        _row("DIVIDEND", "2024-01-10", dividend=5.0),
        _row("SPLIT", "2024-02-10", multiplier=2.0),
        _row("DEMERGER", "2024-03-12", subject="Scheme of demerger - JioFin"),
        _row("RIGHTS", "2024-04-12", subject="Rights issue 1:1"),
        _row("BUYBACK", "2024-05-12", subject="Buyback of equity shares"),
    ]


def _ident(bse_code=None, primary="NSE") -> SecurityIdentity:
    return SecurityIdentity(
        symbol="RELIANCE", bse_code=bse_code, isin=None, company_name=None,
        aliases=("RELIANCE",), primary_exchange=primary,
    )


def _seeded_engine(root: Path, ident: SecurityIdentity) -> UnifiedLakeEngine:
    eng = UnifiedLakeEngine(root)
    eng.adjuster._resolver = _FakeResolver()  # type: ignore[assignment]
    eng.resolve = lambda q: ident  # type: ignore[method-assign]
    return eng


def _empty_actions() -> pd.DataFrame:
    return pd.DataFrame(columns=["ExDate", "ActionType", "DividendPerShareRs", "ShareMultiplier"])


def _one_action(action, symbol="RELIANCE", exchange="NSE") -> pd.DataFrame:
    return pd.DataFrame([{
        "ExDate": pd.Timestamp("2024-03-12"),
        "ActionType": action,
        "DividendPerShareRs": 0.0,
        "ShareMultiplier": 1.0,
        "symbol": symbol,
        "exchange": exchange,
    }])


_NON_ADJUSTING = {"DEMERGER", "RIGHTS", "BUYBACK"}


# (a) + (d): real adjuster, seeded data.
def test_D8B_A_default_hides_opt_in_surfaces_non_adjusting_events():
    """Default view is price-adjusting only; opt-in surfaces DEMERGER/RIGHTS/BUYBACK."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, _fixture_rows())
        eng = _seeded_engine(root, _ident())
        default_out = eng.corporate_actions("RELIANCE")
        opt_in_out = eng.corporate_actions("RELIANCE", include_non_adjusting=True)

    assert set(default_out["ActionType"]) == {"DIVIDEND", "SPLIT"}
    assert _NON_ADJUSTING.isdisjoint(set(default_out["ActionType"]))
    assert _NON_ADJUSTING.issubset(set(opt_in_out["ActionType"]))
    assert len(opt_in_out[opt_in_out["ActionType"].isin(_NON_ADJUSTING)]) == 3


def test_D8B_D_existing_dividend_split_behavior_unchanged():
    """Price-adjusting rows must still be present with unchanged values."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, _fixture_rows())
        out = _seeded_engine(root, _ident()).corporate_actions("RELIANCE")

    div = out[out["ActionType"] == "DIVIDEND"].iloc[0]
    spl = out[out["ActionType"] == "SPLIT"].iloc[0]
    assert div["DividendPerShareRs"] == 5.0
    assert spl["ShareMultiplier"] == 2.0
    assert list(out["ExDate"]) == sorted(list(out["ExDate"]))


# (b): all three call sites forward the caller's flag (default False, opt-in True).
def _mock_adjuster(nse_df: pd.DataFrame, bse_df: pd.DataFrame) -> Mock:
    adj = Mock()

    def _gsa(symbol_or_scrip, exchange="NSE", include_non_adjusting=False):
        frame = nse_df if exchange.upper() == "NSE" else bse_df
        if not include_non_adjusting:
            frame = frame[~frame["ActionType"].isin(_NON_ADJUSTING)]
        return frame.copy()

    adj.get_symbol_actions.side_effect = _gsa
    return adj


def _engine_with_mock_adjuster(adj: Mock, ident: SecurityIdentity) -> UnifiedLakeEngine:
    eng = UnifiedLakeEngine(Path("/tmp/opencode/d8b-unused-lake"))
    eng.adjuster = adj  # type: ignore[assignment]
    eng.resolve = lambda q: ident  # type: ignore[method-assign]
    return eng


def _assert_call_flags(adj: Mock, expected_flag: bool) -> None:
    calls = adj.get_symbol_actions.call_args_list
    assert calls, "get_symbol_actions was never called"
    for c in calls:
        assert c.kwargs.get("include_non_adjusting") is expected_flag, c


def test_D8B_B1_bse_first_call_site_forwards_flag():
    # Opt-in: DEMERGER from the BSE-first lookup is surfaced and the flag is True.
    adj = _mock_adjuster(_empty_actions(), _one_action("DEMERGER", exchange="BSE"))
    eng = _engine_with_mock_adjuster(adj, _ident(bse_code="500325"))
    out = eng.corporate_actions("RELIANCE", exchange="BSE", include_non_adjusting=True)

    assert not out.empty
    assert call("500325", exchange="BSE", include_non_adjusting=True) in adj.get_symbol_actions.call_args_list
    _assert_call_flags(adj, True)

    # Default: flag is False and the non-adjusting row is hidden.
    adj_def = _mock_adjuster(_empty_actions(), _one_action("DEMERGER", exchange="BSE"))
    eng_def = _engine_with_mock_adjuster(adj_def, _ident(bse_code="500325"))
    out_def = eng_def.corporate_actions("RELIANCE", exchange="BSE")

    assert out_def.empty
    assert call("500325", exchange="BSE", include_non_adjusting=False) in adj_def.get_symbol_actions.call_args_list
    _assert_call_flags(adj_def, False)


def test_D8B_B2_nse_call_site_forwards_flag():
    adj = _mock_adjuster(_one_action("DEMERGER"), _empty_actions())
    eng = _engine_with_mock_adjuster(adj, _ident(bse_code=None))
    out = eng.corporate_actions("RELIANCE", include_non_adjusting=True)

    assert not out.empty
    assert call("RELIANCE", exchange="NSE", include_non_adjusting=True) in adj.get_symbol_actions.call_args_list
    _assert_call_flags(adj, True)

    adj_def = _mock_adjuster(_one_action("DEMERGER"), _empty_actions())
    eng_def = _engine_with_mock_adjuster(adj_def, _ident(bse_code=None))
    out_def = eng_def.corporate_actions("RELIANCE")

    assert out_def.empty
    assert call("RELIANCE", exchange="NSE", include_non_adjusting=False) in adj_def.get_symbol_actions.call_args_list
    _assert_call_flags(adj_def, False)


def test_D8B_B3_bse_fallback_call_site_forwards_flag():
    adj = _mock_adjuster(_empty_actions(), _one_action("RIGHTS", exchange="BSE"))
    eng = _engine_with_mock_adjuster(adj, _ident(bse_code="500325"))
    out = eng.corporate_actions("RELIANCE", include_non_adjusting=True)  # NSE empty -> BSE fallback

    assert not out.empty
    calls = adj.get_symbol_actions.call_args_list
    assert call("RELIANCE", exchange="NSE", include_non_adjusting=True) in calls
    assert call("500325", exchange="BSE", include_non_adjusting=True) in calls
    _assert_call_flags(adj, True)

    adj_def = _mock_adjuster(_empty_actions(), _one_action("RIGHTS", exchange="BSE"))
    eng_def = _engine_with_mock_adjuster(adj_def, _ident(bse_code="500325"))
    out_def = eng_def.corporate_actions("RELIANCE")

    assert out_def.empty
    calls_def = adj_def.get_symbol_actions.call_args_list
    assert call("RELIANCE", exchange="NSE", include_non_adjusting=False) in calls_def
    assert call("500325", exchange="BSE", include_non_adjusting=False) in calls_def
    _assert_call_flags(adj_def, False)


# (c): apply_adjustments (price-adjustment path) still uses the DEFAULT flag.
def test_D8B_C_apply_adjustments_still_uses_default_flag():
    """Read-only assertion: adjuster.py is untouched and keeps the default call."""
    adj = OHLCVAdjuster(Path("/tmp/opencode/d8b-unused-lake"))
    probe = Mock(return_value=_empty_actions())
    adj.get_symbol_actions = probe  # type: ignore[method-assign]

    df = pd.DataFrame({
        "Date": [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03")],
        "Close": [100.0, 101.0],
    })
    adj.apply_adjustments(df, "RELIANCE", exchange="NSE")

    probe.assert_called_once_with("RELIANCE", exchange="NSE")
    assert "include_non_adjusting" not in probe.call_args.kwargs
