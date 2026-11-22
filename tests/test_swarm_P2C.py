"""T-P2C regression tests: `typing.Any` must be importable from datalake.sync.

`sync.py` uses `Any` in annotations on `_pack_frames_by_symbol()` and
`_compute_curr_year_carry_factors()`. Because the module opts into deferred
annotations (`from __future__ import annotations`), the missing import is
invisible at runtime until someone calls `typing.get_type_hints()`, runs mypy,
or runs ruff. These tests pin the fix without changing any runtime behavior.

All tests are fully offline and deterministic: they only introspect annotations.
"""
from __future__ import annotations

import typing

import datalake.sync as sync


def test_Any_is_imported_from_typing() -> None:
    assert hasattr(sync, "Any"), "datalake.sync must expose Any"
    assert sync.Any is typing.Any


def test_get_type_hints_resolves_pack_frames_by_symbol() -> None:
    hints = typing.get_type_hints(sync._pack_frames_by_symbol)
    assert hints["adjuster"] is typing.Any
    assert hints["return"] == dict[str, list[int]]


def test_get_type_hints_resolves_curr_year_carry_factors() -> None:
    hints = typing.get_type_hints(sync._compute_curr_year_carry_factors)
    assert hints["adjuster"] is typing.Any
    assert hints["return"] == dict[str, tuple[float, float]]
