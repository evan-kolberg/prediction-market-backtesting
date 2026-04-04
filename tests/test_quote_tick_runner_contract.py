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

    latency_values = {
        keyword.arg: keyword.value.value
        for keyword in latency_keyword.value.keywords
        if isinstance(keyword.value, ast.Constant)
    }
    assert latency_values == {
        "base_latency_ms": 75.0,
        "insert_latency_ms": 10.0,
        "update_latency_ms": 5.0,
        "cancel_latency_ms": 5.0,
    }

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
