from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTESTS_ROOT = REPO_ROOT / "backtests"


PUBLIC_RUNNER_PATHS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("*.py")
    if path.name not in {"__init__.py", "_script_helpers.py", "sitecustomize.py"}
    and not path.name.startswith("_")
)

PMXT_SINGLE_MARKET_QUOTE_TICK_RUNNERS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("polymarket_quote_tick_pmxt_*.py")
    if "sports_" not in path.name
)


@pytest.mark.parametrize(
    "relative_path",
    [
        Path("backtests/kalshi_trade_tick_breakout.py"),
        Path("backtests/polymarket_trade_tick_deep_value_resolution_hold.py"),
        Path("backtests/polymarket_quote_tick_pmxt_ema_crossover.py"),
        Path("backtests/polymarket_trade_tick_simple_quoter.py"),
        Path("backtests/polymarket_trade_tick_vwap_reversion.py"),
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
    sys.modules.pop("sitecustomize", None)
    __import__("sitecustomize")

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "NAME" in globals_dict
    assert "run" in globals_dict


def test_backtests_tree_keeps_public_runners_flat() -> None:
    top_level_dirs = {
        path.name
        for path in BACKTESTS_ROOT.iterdir()
        if path.is_dir() and path.name != "__pycache__"
    }
    assert top_level_dirs <= {"_shared", "private"}

    unexpected_nested_runners = [
        path.relative_to(BACKTESTS_ROOT)
        for path in BACKTESTS_ROOT.rglob("*.py")
        if len(path.relative_to(BACKTESTS_ROOT).parts) > 1
        and path.relative_to(BACKTESTS_ROOT).parts[0]
        not in {"_shared", "private", "__pycache__"}
    ]
    assert unexpected_nested_runners == []


@pytest.mark.parametrize("relative_path", PUBLIC_RUNNER_PATHS)
def test_public_runner_modules_expose_metadata_contract(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert isinstance(globals_dict.get("NAME"), str) and globals_dict["NAME"]
    assert (
        isinstance(globals_dict.get("DESCRIPTION"), str) and globals_dict["DESCRIPTION"]
    )
    assert globals_dict.get("PLATFORM") in {"kalshi", "polymarket"}
    assert globals_dict.get("DATA_TYPE") in {"trade_tick", "quote_tick"}
    assert isinstance(globals_dict.get("VENDOR"), str) and globals_dict["VENDOR"]
    if "DATA" in globals_dict:
        data = globals_dict["DATA"]
        assert getattr(data, "platform", None) == globals_dict["PLATFORM"]
        assert getattr(data, "data_type", None) == globals_dict["DATA_TYPE"]
        assert getattr(data, "vendor", None) == globals_dict["VENDOR"]
        assert isinstance(getattr(data, "sources", ()), tuple)
    assert callable(globals_dict.get("run"))


@pytest.mark.parametrize("relative_path", PMXT_SINGLE_MARKET_QUOTE_TICK_RUNNERS)
def test_pmxt_single_market_quote_tick_runners_expose_explicit_experiment_constants(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    for key in (
        "MARKET_SLUG",
        "TOKEN_INDEX",
        "START_TIME",
        "END_TIME",
        "MIN_QUOTES",
        "MIN_PRICE_RANGE",
        "INITIAL_CASH",
    ):
        assert key in globals_dict
