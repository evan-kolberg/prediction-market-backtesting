from __future__ import annotations

import ast
from pathlib import Path

import pytest


EXPECTED_INITIAL_CASH = 100.0
EXPECTED_SINGLE_MARKET_LOOKBACK_DAYS = 30
EXPECTED_FIXED_SPORTS_LOOKBACK_DAYS = 7
EXPECTED_EMIT_HTML = True
EXPECTED_CHART_OUTPUT_PATH = "output"
EXPECTED_KALSHI_TRADE_SOURCES = ("rest:https://api.elections.kalshi.com/trade-api/v2",)
EXPECTED_KALSHI_MARKET_TICKER = "KXNEXTIRANLEADER-45JAN01-MKHA"
EXPECTED_POLYMARKET_TRADE_SOURCES = (
    "gamma:https://gamma-api.polymarket.com",
    "trades:https://data-api.polymarket.com",
    "clob:https://clob.polymarket.com",
)
EXPECTED_POLYMARKET_MARKET_SLUG = (
    "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
)

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


def _literal_value(value: ast.AST) -> object:
    try:
        return ast.literal_eval(value)
    except ValueError as exc:
        raise AssertionError(
            f"expected a literal-compatible AST node, got {type(value).__name__}"
        ) from exc


def _call_literal_keyword_values(call: ast.Call) -> dict[str, object]:
    return {keyword.arg: _literal_value(keyword.value) for keyword in call.keywords}


def _keyword_value(call: ast.Call, name: str) -> object:
    for keyword in call.keywords:
        if keyword.arg == name:
            return _constant_or_name(keyword.value)
    raise AssertionError(f"missing keyword argument {name}")


def _single_replay_keyword_values(
    module: ast.Module,
    *,
    constructor_name: str,
) -> dict[str, object]:
    replays_assign = _find_assignment(module, "REPLAYS")
    assert isinstance(replays_assign.value, ast.Tuple)
    assert len(replays_assign.value.elts) == 1
    replay_call = replays_assign.value.elts[0]
    assert isinstance(replay_call, ast.Call)
    assert isinstance(replay_call.func, ast.Name)
    assert replay_call.func.id == constructor_name
    return _call_literal_keyword_values(replay_call)


def _data_sources_value(module: ast.Module) -> object:
    data_assign = _find_assignment(module, "DATA")
    assert isinstance(data_assign.value, ast.Call)
    return next(
        _literal_value(keyword.value)
        for keyword in data_assign.value.keywords
        if keyword.arg == "sources"
    )


def _experiment_keyword_value(module: ast.Module, name: str) -> object:
    experiment_assign = _find_assignment(module, "EXPERIMENT")
    assert isinstance(experiment_assign.value, ast.Call)
    return _keyword_value(experiment_assign.value, name)


def _top_level_literal_value(module: ast.Module, name: str) -> object:
    return _literal_value(_find_assignment(module, name).value)


@pytest.mark.parametrize(
    "runner_path", KALSHI_SINGLE_MARKET_RUNNERS, ids=lambda path: path.stem
)
def test_kalshi_trade_tick_runners_use_typed_manifest_contract(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == EXPECTED_KALSHI_TRADE_SOURCES
    assert _top_level_literal_value(module, "EMIT_HTML") == EXPECTED_EMIT_HTML
    assert (
        _top_level_literal_value(module, "CHART_OUTPUT_PATH")
        == EXPECTED_CHART_OUTPUT_PATH
    )
    assert _experiment_keyword_value(module, "emit_html") == "EMIT_HTML"
    assert _experiment_keyword_value(module, "chart_output_path") == "CHART_OUTPUT_PATH"
    assert (
        _literal_value(
            next(
                keyword.value
                for keyword in _find_assignment(module, "EXPERIMENT").value.keywords
                if keyword.arg == "initial_cash"
            )
        )
        == EXPECTED_INITIAL_CASH
    )

    replay_values = _single_replay_keyword_values(
        module,
        constructor_name="KalshiTradeTickReplay",
    )
    assert replay_values["market_ticker"] == EXPECTED_KALSHI_MARKET_TICKER
    assert replay_values["lookback_days"] == EXPECTED_SINGLE_MARKET_LOOKBACK_DAYS


@pytest.mark.parametrize(
    "runner_path",
    POLYMARKET_SINGLE_MARKET_RUNNERS,
    ids=lambda path: path.stem,
)
def test_polymarket_trade_tick_single_market_runners_use_typed_manifest_contract(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == EXPECTED_POLYMARKET_TRADE_SOURCES
    assert _top_level_literal_value(module, "EMIT_HTML") == EXPECTED_EMIT_HTML
    assert (
        _top_level_literal_value(module, "CHART_OUTPUT_PATH")
        == EXPECTED_CHART_OUTPUT_PATH
    )
    assert _experiment_keyword_value(module, "emit_html") == "EMIT_HTML"
    assert _experiment_keyword_value(module, "chart_output_path") == "CHART_OUTPUT_PATH"

    replay_values = _single_replay_keyword_values(
        module,
        constructor_name="PolymarketTradeTickReplay",
    )
    assert replay_values["market_slug"] == EXPECTED_POLYMARKET_MARKET_SLUG
    assert replay_values["lookback_days"] == EXPECTED_SINGLE_MARKET_LOOKBACK_DAYS


@pytest.mark.parametrize(
    "runner_path", POLYMARKET_SPORTS_RUNNERS, ids=lambda path: path.stem
)
def test_polymarket_trade_tick_sports_runners_use_fixed_replay_windows(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    assert _data_sources_value(module) == EXPECTED_POLYMARKET_TRADE_SOURCES
    assert _top_level_literal_value(module, "EMIT_HTML") == EXPECTED_EMIT_HTML
    assert (
        _top_level_literal_value(module, "CHART_OUTPUT_PATH")
        == EXPECTED_CHART_OUTPUT_PATH
    )
    assert _experiment_keyword_value(module, "emit_html") == "EMIT_HTML"
    assert _experiment_keyword_value(module, "chart_output_path") == "CHART_OUTPUT_PATH"
    assert _experiment_keyword_value(module, "return_summary_series") is True
    assert "output/" in ast.unparse(
        _find_assignment(module, "SUMMARY_REPORT_PATH").value
    )
    assert "_multi_market.html" in ast.unparse(
        _find_assignment(module, "SUMMARY_REPORT_PATH").value
    )

    report_assign = _find_assignment(module, "REPORT")
    assert isinstance(report_assign.value, ast.Call)
    assert _keyword_value(report_assign.value, "summary_report") is True
    assert (
        _keyword_value(report_assign.value, "summary_report_path")
        == "SUMMARY_REPORT_PATH"
    )

    fixed_lookback_assign = _find_assignment(module, "FIXED_LOOKBACK_DAYS")
    assert (
        _literal_value(fixed_lookback_assign.value)
        == EXPECTED_FIXED_SPORTS_LOOKBACK_DAYS
    )

    replays_assign = _find_assignment(module, "REPLAYS")
    assert isinstance(replays_assign.value, ast.Tuple)
    assert len(replays_assign.value.elts) >= 2
    for replay_call in replays_assign.value.elts:
        assert isinstance(replay_call, ast.Call)
        assert isinstance(replay_call.func, ast.Name)
        assert replay_call.func.id == "PolymarketTradeTickReplay"
        assert _keyword_value(replay_call, "lookback_days") == "FIXED_LOOKBACK_DAYS"
