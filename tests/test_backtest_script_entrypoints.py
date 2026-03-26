from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "relative_path",
    [
        Path("backtests/kalshi_trade_tick/kalshi_breakout.py"),
        Path("backtests/polymarket_trade_tick/polymarket_vwap_reversion.py"),
    ],
)
def test_direct_script_entrypoints_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "NAME" in globals_dict
    assert "run" in globals_dict
