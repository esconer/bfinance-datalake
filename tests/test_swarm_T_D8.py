"""Offline regression tests for swarm bug T-D8.

get_symbol_actions() used to drop every DEMERGER / RIGHTS / BUYBACK row because
those actions have DividendPerShareRs == 0 and ShareMultiplier == 1.0. They are
now available via include_non_adjusting=True while the default (price-adjustment)
path is unchanged.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

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


def _adjuster(root: Path) -> OHLCVAdjuster:
    adj = OHLCVAdjuster(root)
    adj._resolver = _FakeResolver()  # type: ignore[assignment]
    return adj


def _fixture_rows() -> list[dict]:
    # Deliberately NOT in ExDate order to verify sorting in the include path.
    return [
        _row("BUYBACK", "2024-05-12", subject="Buyback of equity shares"),
        _row("DIVIDEND", "2024-01-10", dividend=5.0),
        _row("DEMERGER", "2024-03-12", subject="Scheme of demerger - JioFin"),
        _row("SPLIT", "2024-02-10", multiplier=2.0),
        _row("RIGHTS", "2024-04-12", subject="Rights issue 1:1"),
    ]


def test_D8A_default_keeps_price_adjustment_behavior_unchanged():
    """Default call must still drop non-adjusting actions and keep the 4-col projection."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, _fixture_rows())
        out = _adjuster(root).get_symbol_actions("RELIANCE")

    assert list(out.columns) == ["ExDate", "ActionType", "DividendPerShareRs", "ShareMultiplier"]
    assert set(out["ActionType"]) == {"DIVIDEND", "SPLIT"}
    assert {"DEMERGER", "RIGHTS", "BUYBACK"}.isdisjoint(set(out["ActionType"]))
    assert list(out["ExDate"]) == sorted(list(out["ExDate"]))


def test_D8B_include_non_adjusting_returns_all_classified_actions_sorted_and_projected():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, _fixture_rows())
        out = _adjuster(root).get_symbol_actions("RELIANCE", include_non_adjusting=True)

    expected_cols = ["ExDate", "ActionType", "DividendPerShareRs", "ShareMultiplier", "subject", "Purpose"]
    assert list(out.columns) == expected_cols
    assert set(out["ActionType"]) == {"DIVIDEND", "SPLIT", "DEMERGER", "RIGHTS", "BUYBACK"}
    assert list(out["ExDate"]) == sorted(list(out["ExDate"]))
    non_adj = out[out["ActionType"].isin({"DEMERGER", "RIGHTS", "BUYBACK"})]
    assert len(non_adj) == 3
    assert (non_adj["DividendPerShareRs"] == 0.0).all()
    assert (non_adj["ShareMultiplier"] == 1.0).all()


def test_D8C_unknown_none_and_skipped_action_types_are_excluded():
    rows = _fixture_rows() + [
        _row("OTHER", "2024-06-12"),
        _row(None, "2024-07-12"),
        _row("SKIP_NON_EQUITY", "2024-08-12"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, rows)
        out = _adjuster(root).get_symbol_actions("RELIANCE", include_non_adjusting=True)

    types = set(out["ActionType"].dropna().astype(str))
    assert {"OTHER", "SKIP_NON_EQUITY", "None", "nan"}.isdisjoint(types)
    assert set(out["ActionType"].dropna()) == {"DIVIDEND", "SPLIT", "DEMERGER", "RIGHTS", "BUYBACK"}


def test_D8D_engine_default_path_still_uses_price_adjusting_view():
    """Engine.corporate_actions calls with the default, so demergers stay hidden there.

    NOTE: engine.py is out of scope for T-D8; it does not forward
    include_non_adjusting=True. This test locks in that the engine's default
    price-adjustment view is unchanged by the D8 patch (no regression).
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _seed(root, _fixture_rows())
        eng = UnifiedLakeEngine(root)
        eng.adjuster._resolver = _FakeResolver()  # type: ignore[assignment]
        ident = SecurityIdentity(
            symbol="RELIANCE", bse_code=None, isin=None, company_name=None,
            aliases=("RELIANCE",), primary_exchange="NSE",
        )
        eng.resolve = lambda q: ident  # type: ignore[method-assign]
        out = eng.corporate_actions("RELIANCE")

    assert set(out["ActionType"]) == {"DIVIDEND", "SPLIT"}
    assert not (out["ActionType"].astype(str) == "DEMERGER").any()
