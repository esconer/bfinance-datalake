"""Offline regression tests for swarm task T-APPGUARD.

These tests are fully local: no network access and no mutation of any ``.venv``.

The integration test specifically catches the ABI-shadowing regression where a
repo-local ``.venv`` built for a different Python (e.g. a python3.13 numpy) is
unconditionally prepended to ``sys.path`` and shadows the active interpreter's
own C extensions (e.g. under python3.15), producing
``ModuleNotFoundError: No module named 'numpy._core._multiarray_umath'``.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
APP_PATH = ROOT_DIR / "app.py"


def _app_source() -> str:
    return APP_PATH.read_text(encoding="utf-8")


def test_app_guards_local_venv_injection_by_interpreter_abi():
    """app.py must gate the local-venv sys.path injection and use an exact ABI."""
    source = _app_source()

    # The venv injection must be gated by the "not inside a venv" check.
    gate = "sys.prefix == sys.base_prefix"
    assert gate in source, "app.py must gate local-venv injection with sys.prefix == sys.base_prefix"

    # The old unguarded, cross-ABI glob must be gone.
    assert 'glob("python*/site-packages")' not in source, (
        "app.py must not unconditionally glob python*/site-packages"
    )

    # An exact pythonX.Y ABI match must be used instead of the wildcard.
    assert "_venv_abi" in source and "sys.version_info.major" in source and "sys.version_info.minor" in source, (
        "app.py must derive an exact pythonX.Y ABI tag from sys.version_info"
    )
    assert 'glob(f"{_venv_abi}/site-packages")' in source, (
        "app.py must glob the exact pythonX.Y/site-packages directory"
    )

    # Ordering: the gate must appear before the venv glob (it actually guards it).
    assert source.index(gate) < source.index('glob(f"{_venv_abi}/site-packages")'), (
        "the sys.prefix == sys.base_prefix gate must precede the venv site-packages glob"
    )


def test_app_test_only_runs_successfully_offline():
    """`app.py --test-only` exits 0 with the success banner under PYTHONPATH=src:tests.

    Replaying this test against the unpatched app.py in a worktree that has a
    mismatched repo .venv (python3.13 numpy under a python3.15 interpreter)
    fails with the numpy C-extension ImportError, catching the regression.
    """
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT_DIR / "src"), str(ROOT_DIR / "tests")])

    proc = subprocess.run(
        [sys.executable, str(APP_PATH), "--test-only"],
        cwd=str(ROOT_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=600,
    )
    output = proc.stdout or ""
    assert proc.returncode == 0, f"app.py --test-only failed (rc={proc.returncode}):\n{output}"
    assert "ALL BFINANCE-DATALAKE & FINENGINE INTEGRATION TESTS PASSED" in output, (
        f"success banner missing from app.py --test-only output:\n{output}"
    )
