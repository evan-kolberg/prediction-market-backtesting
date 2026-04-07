from __future__ import annotations

import ast
from pathlib import Path

import pytest


EXPECTED_PMXT_SOURCES = (
    "local:/Volumes/LaCie/pmxt_raws",
    "archive:r2.pmxt.dev",
    "relay:209-209-10-83.sslip.io",
)
EXPECTED_PMXT_LATENCY = {
    "base_latency_ms": 75.0,
    "insert_latency_ms": 10.0,
    "update_latency_ms": 5.0,
    "cancel_latency_ms": 5.0,
}
EXPECTED_RUNNER_EMIT_HTML = True
EXPECTED_OPTIMIZER_EMIT_HTML = False
EXPECTED_CHART_OUTPUT_PATH = "output"
EXPECTED_DETAIL_PLOT_PANELS = (
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)
EXPECTED_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "equity",
    "periodic_pnl",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)
SUPPORTED_SUMMARY_PLOT_PANELS = frozenset(EXPECTED_SUMMARY_PLOT_PANELS)

RUNNER_FILES = sorted(
    Path(__file__)
    .resolve()
    .parents[1]
    .joinpath("backtests")
    .glob("polymarket_quote_tick_pmxt_*.py")
)
MULTI_SIM_RUNNER_FILES = sorted(
    path
    for path in RUNNER_FILES
    if "multi_sim_runner" in path.name or path.name.endswith("_sims_runner.py")
)
SINGLE_MARKET_RUNNER_FILES = sorted(
    path
    for path in RUNNER_FILES
    if "optimizer" not in path.name and path not in MULTI_SIM_RUNNER_FILES
)
OPTIMIZER_RUNNER_FILES = sorted(
    path for path in RUNNER_FILES if "optimizer" in path.name
)


def _find_assignment(module: ast.Module, name: str) -> ast.Assign:
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                return node
    raise AssertionError(f"missing top-level assignment for {name}")


def _literal_value(value: ast.AST) -> object:
    try:
        return ast.literal_eval(value)
    except ValueError as exc:
        raise AssertionError(
            f"expected a literal-compatible AST node, got {type(value).__name__}"
        ) from exc


def _constant_or_name(value: ast.AST) -> object:
    if isinstance(value, ast.Constant):
        return value.value
    if isinstance(value, ast.Name):
        return value.id
    raise AssertionError(
        f"expected ast.Constant or ast.Name, got {type(value).__name__}"
    )


def _call_keyword_value(call: ast.Call, name: str) -> object:
    for keyword in call.keywords:
        if keyword.arg == name:
            return _constant_or_name(keyword.value)
    raise AssertionError(f"missing keyword argument {name}")


@pytest.mark.parametrize(
    "runner_path", SINGLE_MARKET_RUNNER_FILES, ids=lambda path: path.stem
)
def test_quote_tick_runners_use_typed_manifest_contract(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())
    emit_html_assign = _find_assignment(module, "EMIT_HTML")
    assert _literal_value(emit_html_assign.value) == EXPECTED_RUNNER_EMIT_HTML
    chart_output_assign = _find_assignment(module, "CHART_OUTPUT_PATH")
    assert _literal_value(chart_output_assign.value) == EXPECTED_CHART_OUTPUT_PATH

    execution_assign = _find_assignment(module, "EXECUTION")
    assert isinstance(execution_assign.value, ast.Call)
    assert isinstance(execution_assign.value.func, ast.Name)
    assert execution_assign.value.func.id == "ExecutionModelConfig"

    execution_keywords = {keyword.arg for keyword in execution_assign.value.keywords}
    assert execution_keywords >= {"queue_position", "latency_model"}
    queue_keyword = next(
        keyword
        for keyword in execution_assign.value.keywords
        if keyword.arg == "queue_position"
    )
    assert isinstance(queue_keyword.value, ast.Constant)
    assert queue_keyword.value.value is True

    latency_keyword = next(
        keyword
        for keyword in execution_assign.value.keywords
        if keyword.arg == "latency_model"
    )
    assert isinstance(latency_keyword.value, ast.Call)
    assert isinstance(latency_keyword.value.func, ast.Name)
    assert latency_keyword.value.func.id == "StaticLatencyConfig"
    assert {
        keyword.arg: _literal_value(keyword.value)
        for keyword in latency_keyword.value.keywords
    } == EXPECTED_PMXT_LATENCY

    data_assign = _find_assignment(module, "DATA")
    assert isinstance(data_assign.value, ast.Call)
    sources_keyword = next(
        keyword for keyword in data_assign.value.keywords if keyword.arg == "sources"
    )
    assert isinstance(sources_keyword.value, ast.Tuple)
    assert _literal_value(sources_keyword.value) == EXPECTED_PMXT_SOURCES

    replays_assign = _find_assignment(module, "REPLAYS")
    assert isinstance(replays_assign.value, ast.Tuple)
    assert len(replays_assign.value.elts) >= 1
    for replay_call in replays_assign.value.elts:
        assert isinstance(replay_call, ast.Call)
        assert isinstance(replay_call.func, ast.Name)
        assert replay_call.func.id == "PolymarketPMXTQuoteReplay"

    experiment_assign = _find_assignment(module, "EXPERIMENT")
    assert isinstance(experiment_assign.value, ast.Call)
    assert isinstance(experiment_assign.value.func, ast.Name)
    assert experiment_assign.value.func.id == "build_replay_experiment"

    execution_keyword = next(
        keyword
        for keyword in experiment_assign.value.keywords
        if keyword.arg == "execution"
    )
    assert isinstance(execution_keyword.value, ast.Name)
    assert execution_keyword.value.id == "EXECUTION"
    assert _call_keyword_value(experiment_assign.value, "replays") == "REPLAYS"
    assert _call_keyword_value(experiment_assign.value, "emit_html") == "EMIT_HTML"
    assert (
        _call_keyword_value(experiment_assign.value, "chart_output_path")
        == "CHART_OUTPUT_PATH"
    )
    assert (
        _literal_value(_find_assignment(module, "DETAIL_PLOT_PANELS").value)
        == EXPECTED_DETAIL_PLOT_PANELS
    )
    assert (
        _call_keyword_value(experiment_assign.value, "detail_plot_panels")
        == "DETAIL_PLOT_PANELS"
    )


@pytest.mark.parametrize(
    "runner_path", MULTI_SIM_RUNNER_FILES, ids=lambda path: path.stem
)
def test_pmxt_multi_sim_runners_use_explicit_summary_plot_contract(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())

    name_assign = _find_assignment(module, "NAME")
    assert _literal_value(name_assign.value) == runner_path.stem

    emit_html_assign = _find_assignment(module, "EMIT_HTML")
    assert _literal_value(emit_html_assign.value) == EXPECTED_RUNNER_EMIT_HTML
    chart_output_assign = _find_assignment(module, "CHART_OUTPUT_PATH")
    assert _literal_value(chart_output_assign.value) == EXPECTED_CHART_OUTPUT_PATH

    replays_assign = _find_assignment(module, "REPLAYS")
    assert isinstance(replays_assign.value, ast.Tuple)
    assert len(replays_assign.value.elts) > 1

    assert (
        _literal_value(_find_assignment(module, "DETAIL_PLOT_PANELS").value)
        == EXPECTED_DETAIL_PLOT_PANELS
    )

    summary_panels = _literal_value(
        _find_assignment(module, "SUMMARY_PLOT_PANELS").value
    )
    assert isinstance(summary_panels, tuple)
    assert summary_panels
    assert set(summary_panels) <= SUPPORTED_SUMMARY_PLOT_PANELS

    report_assign = _find_assignment(module, "REPORT")
    assert isinstance(report_assign.value, ast.Call)
    assert _call_keyword_value(report_assign.value, "summary_report") is True
    assert (
        _call_keyword_value(report_assign.value, "summary_report_path")
        == "SUMMARY_REPORT_PATH"
    )
    assert (
        _call_keyword_value(report_assign.value, "summary_plot_panels")
        == "SUMMARY_PLOT_PANELS"
    )

    experiment_assign = _find_assignment(module, "EXPERIMENT")
    assert isinstance(experiment_assign.value, ast.Call)
    assert isinstance(experiment_assign.value.func, ast.Name)
    assert experiment_assign.value.func.id == "build_replay_experiment"
    assert _call_keyword_value(experiment_assign.value, "replays") == "REPLAYS"
    assert _call_keyword_value(experiment_assign.value, "emit_html") == "EMIT_HTML"
    assert (
        _call_keyword_value(experiment_assign.value, "chart_output_path")
        == "CHART_OUTPUT_PATH"
    )
    assert (
        _call_keyword_value(experiment_assign.value, "detail_plot_panels")
        == "DETAIL_PLOT_PANELS"
    )
    assert _call_keyword_value(experiment_assign.value, "return_summary_series") is True


@pytest.mark.parametrize(
    "runner_path", OPTIMIZER_RUNNER_FILES, ids=lambda path: path.stem
)
def test_quote_tick_optimizer_runners_inline_explicit_search_controls(
    runner_path: Path,
) -> None:
    module = ast.parse(runner_path.read_text())
    emit_html_assign = _find_assignment(module, "EMIT_HTML")
    assert _literal_value(emit_html_assign.value) == EXPECTED_OPTIMIZER_EMIT_HTML
    chart_output_assign = _find_assignment(module, "CHART_OUTPUT_PATH")
    assert _literal_value(chart_output_assign.value) == EXPECTED_CHART_OUTPUT_PATH

    data_assign = _find_assignment(module, "DATA")
    assert isinstance(data_assign.value, ast.Call)
    sources_keyword = next(
        keyword for keyword in data_assign.value.keywords if keyword.arg == "sources"
    )
    assert isinstance(sources_keyword.value, ast.Tuple)
    assert _literal_value(sources_keyword.value) == EXPECTED_PMXT_SOURCES

    execution_assign = _find_assignment(module, "EXECUTION")
    assert isinstance(execution_assign.value, ast.Call)
    assert isinstance(execution_assign.value.func, ast.Name)
    assert execution_assign.value.func.id == "ExecutionModelConfig"
    optimizer_execution_keywords = {
        keyword.arg: keyword.value for keyword in execution_assign.value.keywords
    }
    assert isinstance(optimizer_execution_keywords["queue_position"], ast.Constant)
    assert optimizer_execution_keywords["queue_position"].value is True
    assert isinstance(optimizer_execution_keywords["latency_model"], ast.Call)
    assert isinstance(optimizer_execution_keywords["latency_model"].func, ast.Name)
    assert (
        optimizer_execution_keywords["latency_model"].func.id == "StaticLatencyConfig"
    )
    assert {
        keyword.arg: _literal_value(keyword.value)
        for keyword in optimizer_execution_keywords["latency_model"].keywords
    } == EXPECTED_PMXT_LATENCY

    base_replay_assign = _find_assignment(module, "BASE_REPLAY")
    assert isinstance(base_replay_assign.value, ast.Call)
    assert isinstance(base_replay_assign.value.func, ast.Name)
    assert base_replay_assign.value.func.id == "PolymarketPMXTQuoteReplay"

    market_slug_keyword = next(
        keyword
        for keyword in base_replay_assign.value.keywords
        if keyword.arg == "market_slug"
    )
    token_index_keyword = next(
        keyword
        for keyword in base_replay_assign.value.keywords
        if keyword.arg == "token_index"
    )
    assert isinstance(market_slug_keyword.value, ast.Constant)
    assert (
        market_slug_keyword.value.value
        == "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
    )
    assert _literal_value(token_index_keyword.value) == 0

    parameter_grid_assign = _find_assignment(module, "PARAMETER_GRID")
    parameter_grid = _literal_value(parameter_grid_assign.value)
    assert parameter_grid == {
        "fast_period": (32, 64, 96),
        "slow_period": (128, 256, 384),
        "entry_buffer": (0.00025, 0.0005),
        "take_profit": (0.005, 0.01),
        "stop_loss": (0.005, 0.01),
    }

    train_windows_assign = _find_assignment(module, "TRAIN_WINDOWS")
    assert isinstance(train_windows_assign.value, ast.Tuple)
    assert len(train_windows_assign.value.elts) == 3
    for window in train_windows_assign.value.elts:
        assert isinstance(window, ast.Call)
        assert isinstance(window.func, ast.Name)
        assert window.func.id == "OptimizationWindow"

    holdout_windows_assign = _find_assignment(module, "HOLDOUT_WINDOWS")
    assert isinstance(holdout_windows_assign.value, ast.Tuple)
    assert len(holdout_windows_assign.value.elts) == 1
    holdout_window = holdout_windows_assign.value.elts[0]
    assert isinstance(holdout_window, ast.Call)
    assert isinstance(holdout_window.func, ast.Name)
    assert holdout_window.func.id == "OptimizationWindow"

    optimization_assign = _find_assignment(module, "OPTIMIZATION")
    assert isinstance(optimization_assign.value, ast.Call)
    assert isinstance(optimization_assign.value.func, ast.Name)
    assert optimization_assign.value.func.id == "OptimizationConfig"

    optimization_values = {
        keyword.arg: keyword.value.id
        for keyword in optimization_assign.value.keywords
        if isinstance(keyword.value, ast.Name)
    }
    assert optimization_values["data"] == "DATA"
    assert optimization_values["base_replay"] == "BASE_REPLAY"
    assert optimization_values["strategy_spec"] == "STRATEGY_SPEC"
    assert optimization_values["parameter_grid"] == "PARAMETER_GRID"
    assert optimization_values["train_windows"] == "TRAIN_WINDOWS"
    assert optimization_values["holdout_windows"] == "HOLDOUT_WINDOWS"
    assert optimization_values["execution"] == "EXECUTION"
    assert _call_keyword_value(optimization_assign.value, "emit_html") == "EMIT_HTML"
    assert (
        _call_keyword_value(optimization_assign.value, "chart_output_path")
        == "CHART_OUTPUT_PATH"
    )
