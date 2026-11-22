"""T-P6 (revised): `--profile` on `datalake pull` is parsed and forwarded.

All tests are fully offline: the `pull_from_github_release` coroutine is
AsyncMocked at the cli module boundary, so no network / release manifest is
needed and the tests pass regardless of the tier implementation in sync.py.
"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, patch

import pytest

import datalake.cli as cli

VALID_TIERS = ("full", "core_longterm", "core_no_fo")


def _run_pull(*extra_args: str, mock_pull: AsyncMock) -> None:
    """Invoke cli.main() for `datalake pull` with the sync call mocked out."""
    argv = [
        "datalake",
        "--data-dir", "/tmp/opencode/bfdl-p6-test-data",
        "pull",
        "--repo", "u/r",
        *extra_args,
    ]
    with patch.object(sys, "argv", argv), patch.object(cli, "pull_from_github_release", mock_pull):
        cli.main()


def test_pull_default_profile_is_full() -> None:
    mock_pull = AsyncMock(return_value={"ok": 1})
    _run_pull(mock_pull=mock_pull)
    assert mock_pull.call_args.kwargs["profile"] == "full"


@pytest.mark.parametrize("tier", VALID_TIERS)
def test_pull_accepts_and_forwards_each_profile_choice(tier: str) -> None:
    mock_pull = AsyncMock(return_value={"ok": 1})
    _run_pull("--profile", tier, mock_pull=mock_pull)
    assert mock_pull.call_args.kwargs["profile"] == tier


def test_pull_rejects_unknown_profile_choice() -> None:
    mock_pull = AsyncMock(return_value={"ok": 1})
    with pytest.raises(SystemExit) as exc:
        _run_pull("--profile", "bogus", mock_pull=mock_pull)
    assert exc.value.code == 2
    mock_pull.assert_not_called()


def test_pull_profile_forwarded_without_network() -> None:
    """The chosen tier is forwarded as the profile= kwarg; sync is never real."""
    mock_pull = AsyncMock(return_value={})
    _run_pull("--profile", "core_no_fo", mock_pull=mock_pull)
    mock_pull.assert_awaited_once()
    assert mock_pull.call_args.kwargs["profile"] == "core_no_fo"
    assert mock_pull.call_args.kwargs["pattern"] is None
