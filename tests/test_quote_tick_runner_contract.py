import ast
from pathlib import Path

import pytest


RUNNER_FILES = sorted(
    Path(__file__)
    .resolve()
    .parents[1]
    .joinpath("backtests")
    .glob("polymarket_quote_tick_pmxt_*.py")
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


@pytest.mark.parametrize("runner_path", RUNNER_FILES, ids=lambda path: path.stem)
def test_quote_tick_runners_expose_execution_controls(runner_path: Path) -> None:
    module = ast.parse(runner_path.read_text())

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
    assert _constant_or_name(queue_keyword.value) == "DEFAULT_PMXT_QUEUE_POSITION"

    latency_keyword = next(
        keyword
        for keyword in execution_assign.value.keywords
        if keyword.arg == "latency_model"
    )
    assert isinstance(latency_keyword.value, ast.Call)
    assert isinstance(latency_keyword.value.func, ast.Name)
    assert latency_keyword.value.func.id == "StaticLatencyConfig"

    latency_values = {
        keyword.arg: _constant_or_name(keyword.value)
        for keyword in latency_keyword.value.keywords
    }
    assert latency_values == {
        "base_latency_ms": "DEFAULT_PMXT_BASE_LATENCY_MS",
        "insert_latency_ms": "DEFAULT_PMXT_INSERT_LATENCY_MS",
        "update_latency_ms": "DEFAULT_PMXT_UPDATE_LATENCY_MS",
        "cancel_latency_ms": "DEFAULT_PMXT_CANCEL_LATENCY_MS",
    }

    data_assign = _find_assignment(module, "DATA")
    assert isinstance(data_assign.value, ast.Call)
    sources_keyword = next(
        keyword for keyword in data_assign.value.keywords if keyword.arg == "sources"
    )
    assert _constant_or_name(sources_keyword.value) == "DEFAULT_PMXT_DATA_SOURCES"

    backtest_assign = _find_assignment(module, "BACKTEST")
    assert isinstance(backtest_assign.value, ast.Call)
    assert isinstance(backtest_assign.value.func, ast.Name)
    assert backtest_assign.value.func.id == "PredictionMarketBacktest"

    execution_keyword = next(
        (
            keyword
            for keyword in backtest_assign.value.keywords
            if keyword.arg == "execution"
        ),
        None,
    )
    assert execution_keyword is not None
    assert isinstance(execution_keyword.value, ast.Name)
    assert execution_keyword.value.id == "EXECUTION"
