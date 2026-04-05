from __future__ import annotations

import ast
from pathlib import Path

import pytest


BACKTESTS_ROOT = Path(__file__).resolve().parents[1] / "backtests"
KALSHI_SINGLE_MARKET_RUNNERS = sorted(BACKTESTS_ROOT.glob("kalshi_trade_tick_*.py"))
POLYMARKET_SINGLE_MARKET_RUNNERS = sorted(
    path
    for path in BACKTESTS_ROOT.glob("polymarket_trade_tick_*.py")
    if "sports_" not in path.name
)
POLYMARKET_SPORTS_RUNNERS = sorted(
    BACKTESTS_ROOT.glob("polymarket_trade_tick_sports_*.py")
)


def _find_assignment(module: ast.Module, name: str) -> ast.Assign:
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                return node
    raise AssertionError(f"missing top-level assignment for {name}")


def _constant_or_name(value: ast.AST) -> object:
    if isinstance(value, ast.Constant):
        return value.value
    if isinstance(value, ast.Name):
        return value.id
    raise AssertionError(
        f"expected ast.Constant or ast.Name, got {type(value).__name__}"
    )


def _call_keyword_values(call: ast.Call) -> dict[str, object]:
    return {keyword.arg: _constant_or_name(keyword.value) for keyword in call.keywords}


def _keyword_value(call: ast.Call, name: str) -> object:
    for keyword in call.keywords:
        if keyword.arg == name:
            return _constant_or_name(keyword.value)
    raise AssertionError(f"missing keyword argument {name}")


def _single_sim_keyword_values(module: ast.Module) -> dict[str, object]:
    sims_assign = _find_assignment(module, "SIMS")
    assert isinstance(sims_assign.value, ast.Tuple)
    assert len(sims_assign.value.elts) == 1
    sim_call = sims_assign.value.elts[0]
    assert isinstance(sim_call, ast.Call)
    assert isinstance(sim_call.func, ast.Name)
    assert sim_call.func.id == "MarketSimConfig"
    return _call_keyword_values(sim_call)


def _data_sources_value(module: ast.Module) -> object:
    data_assign = _find_assignment(module, "DATA")
    assert isinstance(data_assign.value, ast.Call)
    return next(
        _constant_or_name(keyword.value)
        for keyword in data_assign.value.keywords
        if keyword.arg == "sources"
    )


def _backtest_initial_cash_value(module: ast.Module) -> object:
    backtest_assign = _find_assignment(module, "BACKTEST")
    assert isinstance(backtest_assign.value, ast.Call)
    return next(
        _constant_or_name(keyword.value)
        for keyword in backtest_assign.value.keywords
        if keyword.arg == "initial_cash"
    )


@pytest.mark.parametrize(
    "runner_path", KALSHI_SINGLE_MARKET_RUNNERS, ids=lambda path: path.stem
)
def test_kalshi_trade_tick_runners_use_shared_defaults(runner_path: Path) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == "DEFAULT_KALSHI_NATIVE_DATA_SOURCES"
    assert _backtest_initial_cash_value(module) == "DEFAULT_INITIAL_CASH"

    sim_values = _single_sim_keyword_values(module)
    assert sim_values["market_ticker"] == "DEFAULT_KALSHI_MARKET_TICKER"
    assert (
        sim_values["lookback_days"] == "DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS"
    )


@pytest.mark.parametrize(
    "runner_path",
    POLYMARKET_SINGLE_MARKET_RUNNERS,
    ids=lambda path: path.stem,
)
def test_polymarket_trade_tick_single_market_runners_use_shared_defaults(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == "DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES"
    assert _backtest_initial_cash_value(module) == "DEFAULT_INITIAL_CASH"

    sim_values = _single_sim_keyword_values(module)
    assert sim_values["market_slug"] == "DEFAULT_POLYMARKET_MARKET_SLUG"
    assert (
        sim_values["lookback_days"] == "DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS"
    )


@pytest.mark.parametrize(
    "runner_path", POLYMARKET_SPORTS_RUNNERS, ids=lambda path: path.stem
)
def test_polymarket_trade_tick_sports_runners_use_shared_defaults(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == "DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES"
    assert _backtest_initial_cash_value(module) == "DEFAULT_INITIAL_CASH"

    fixed_lookback_assign = _find_assignment(module, "FIXED_LOOKBACK_DAYS")
    assert _constant_or_name(fixed_lookback_assign.value) == (
        "DEFAULT_FIXED_TRADE_TICK_SPORTS_LOOKBACK_DAYS"
    )

    sims_assign = _find_assignment(module, "SIMS")
    assert isinstance(sims_assign.value, ast.Tuple)
    assert len(sims_assign.value.elts) >= 2
    for sim_call in sims_assign.value.elts:
        assert isinstance(sim_call, ast.Call)
        assert isinstance(sim_call.func, ast.Name)
        assert sim_call.func.id == "MarketSimConfig"
        assert _keyword_value(sim_call, "lookback_days") == "FIXED_LOOKBACK_DAYS"
