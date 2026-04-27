from __future__ import annotations

import csv
import json
import math
import warnings
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from prediction_market_extensions.backtesting import _optimizer as optimizer
from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
    StaticLatencyConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting.data_sources import PMXT, Book, Polymarket
from prediction_market_extensions.backtesting.optimizers import OPTIMIZER_TYPE_PARAMETER_SEARCH


def _window(name: str, start_time: str, end_time: str) -> optimizer.ParameterSearchWindow:
    return optimizer.ParameterSearchWindow(
        name=name,
        start_time=start_time,
        end_time=end_time,
    )


def _result_for_score(score: float) -> dict[str, object]:
    return {
        "pnl": score,
        "fills": 3,
        "requested_coverage_ratio": 1.0,
        "terminated_early": False,
        "equity_series": [(0, 100.0), (1, 100.0 + score)],
    }


def _strict_json_loads(text: str) -> object:
    def _reject_constant(value: str) -> None:
        raise ValueError(f"non-strict JSON constant {value}")

    return json.loads(text, parse_constant=_reject_constant)


def _make_config(
    tmp_path: Path,
    *,
    name: str = "optimizer_test",
    strategy_spec: dict[str, object] | None = None,
    parameter_grid: dict[str, tuple[object, ...]] | None = None,
    parameter_space: dict[str, dict[str, object]] | None = None,
    sampler: str = "random",
    train_windows: tuple[optimizer.ParameterSearchWindow, ...] | None = None,
    holdout_windows: tuple[optimizer.ParameterSearchWindow, ...] | None = None,
    max_trials: int = 3,
    random_seed: int = 7,
    holdout_top_k: int = 2,
    min_fills_per_window: int = 1,
) -> optimizer.ParameterSearchConfig:
    resolved_strategy_spec = (
        strategy_spec
        if strategy_spec is not None
        else {
            "strategy_path": "strategies:DemoStrategy",
            "config_path": "strategies:DemoConfig",
            "config": {"edge": "__SEARCH__:edge"},
        }
    )
    resolved_parameter_grid = parameter_grid if parameter_grid is not None else {"edge": (1, 2, 3)}
    resolved_parameter_space = parameter_space if parameter_space is not None else {}
    resolved_train_windows = (
        train_windows
        if train_windows is not None
        else (
            _window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),
            _window("train-b", "2026-01-02T00:00:00Z", "2026-01-02T02:00:00Z"),
        )
    )
    resolved_holdout_windows = (
        holdout_windows
        if holdout_windows is not None
        else (_window("holdout-a", "2026-01-03T00:00:00Z", "2026-01-03T02:00:00Z"),)
    )

    return optimizer.ParameterSearchConfig(
        name=name,
        data=MarketDataConfig(
            platform=Polymarket, data_type=Book, vendor=PMXT, sources=("local:/tmp/pmxt_raws",)
        ),
        base_replay=BookReplay(market_slug="demo-market", token_index=0),
        strategy_spec=resolved_strategy_spec,
        parameter_grid=resolved_parameter_grid,
        parameter_space=resolved_parameter_space,
        sampler=sampler,
        train_windows=resolved_train_windows,
        holdout_windows=resolved_holdout_windows,
        max_trials=max_trials,
        random_seed=random_seed,
        holdout_top_k=holdout_top_k,
        initial_cash=100.0,
        min_book_events=500,
        min_price_range=0.005,
        min_fills_per_window=min_fills_per_window,
        execution=ExecutionModelConfig(
            queue_position=True,
            latency_model=StaticLatencyConfig(
                base_latency_ms=75.0,
                insert_latency_ms=10.0,
                update_latency_ms=5.0,
                cancel_latency_ms=5.0,
            ),
        ),
        artifact_root=tmp_path,
    )


def test_parameter_search_config_carries_explicit_optimizer_type(tmp_path: Path) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = _make_config(tmp_path)

    assert config.optimizer_type == OPTIMIZER_TYPE_PARAMETER_SEARCH
    assert any("time-split validation" in str(warning.message) for warning in caught)


def test_sample_parameter_sets_is_deterministic_and_unique(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path, parameter_grid={"edge": (1, 1, 2, 3)}, max_trials=2, random_seed=11
    )

    first = optimizer._sample_parameter_sets(config)
    second = optimizer._sample_parameter_sets(config)
    candidates = optimizer._parameter_candidates(config.parameter_grid)

    assert first == second
    assert len(candidates) == 3
    assert len(first) == 2
    assert len({json.dumps(dict(params), sort_keys=True) for params in first}) == 2

    full_grid_config = replace(config, max_trials=10)
    assert optimizer._sample_parameter_sets(full_grid_config) == candidates


def test_parameter_candidates_preserve_typed_distinct_values(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (Path("models/calibration.json"), "models/calibration.json")},
        max_trials=10,
    )

    candidates = optimizer._parameter_candidates(config.parameter_grid)

    assert candidates == [
        (("edge", Path("models/calibration.json")),),
        (("edge", "models/calibration.json"),),
    ]


def test_discrete_search_rejects_string_values(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="parameter_grid"):
        _make_config(tmp_path, parameter_grid={"edge": "abc"})  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="parameter_space"):
        _make_config(
            tmp_path,
            parameter_grid={},
            parameter_space={"edge": {"type": "categorical", "choices": "abc"}},
            sampler="random",
        )


def test_replace_search_placeholders_binds_nested_payloads() -> None:
    payload = {
        "fast_period": "__SEARCH__:fast_period",
        "nested": [{"slow_period": "__SEARCH__:slow_period"}, ("keep", "__SEARCH__:stop_loss")],
    }

    replaced = optimizer._replace_search_placeholders(
        payload, {"fast_period": 32, "slow_period": 128, "stop_loss": 0.01}
    )

    assert replaced == {"fast_period": 32, "nested": [{"slow_period": 128}, ("keep", 0.01)]}


def test_score_result_penalizes_drawdown_termination_low_coverage_and_low_fills() -> None:
    baseline = optimizer._score_result(
        pnl=10.0,
        max_drawdown_currency=2.0,
        fills=3,
        requested_coverage_ratio=1.0,
        terminated_early=False,
        initial_cash=100.0,
        min_fills_per_window=1,
    )
    deeper_drawdown = optimizer._score_result(
        pnl=10.0,
        max_drawdown_currency=8.0,
        fills=3,
        requested_coverage_ratio=1.0,
        terminated_early=False,
        initial_cash=100.0,
        min_fills_per_window=1,
    )
    terminated = optimizer._score_result(
        pnl=10.0,
        max_drawdown_currency=2.0,
        fills=3,
        requested_coverage_ratio=1.0,
        terminated_early=True,
        initial_cash=100.0,
        min_fills_per_window=1,
    )
    low_coverage = optimizer._score_result(
        pnl=10.0,
        max_drawdown_currency=2.0,
        fills=3,
        requested_coverage_ratio=0.90,
        terminated_early=False,
        initial_cash=100.0,
        min_fills_per_window=1,
    )
    low_fill = optimizer._score_result(
        pnl=10.0,
        max_drawdown_currency=2.0,
        fills=0,
        requested_coverage_ratio=1.0,
        terminated_early=False,
        initial_cash=100.0,
        min_fills_per_window=1,
    )

    assert optimizer._max_drawdown_currency([(0, 100.0), (1, 110.0), (2, 103.0)]) == 7.0
    assert baseline > deeper_drawdown
    assert terminated == pytest.approx(baseline - 100.0)
    assert low_coverage == pytest.approx(baseline - 80.0)
    assert low_fill == pytest.approx(baseline - 2.0)


def test_optimizer_builds_repo_layer_backtest_with_summary_series_enabled(tmp_path: Path) -> None:
    config = replace(
        _make_config(tmp_path, parameter_grid={"edge": (5,)}, max_trials=1),
    )
    window = config.train_windows[0]

    backtest = optimizer._build_backtest(
        config=config, trial_id=7, window=window, params=(("edge", 5),)
    )

    assert isinstance(backtest, PredictionMarketBacktest)
    assert backtest.name == "optimizer_test:train-a:trial-007"
    assert backtest.data is config.data
    assert backtest.initial_cash == 100.0
    assert backtest.min_book_events == 500
    assert backtest.min_price_range == 0.005
    assert backtest.execution == config.execution
    assert backtest.return_summary_series is True
    assert len(backtest.replays) == 1
    assert backtest.replays[0].start_time == window.start_time
    assert backtest.replays[0].end_time == window.end_time
    assert backtest.replays[0].metadata == {"optimization_window": "train-a"}
    assert backtest.strategy_configs[0]["config"]["edge"] == 5


def test_build_optimization_window_backtest_supports_generic_holdout_replays(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    window = config.holdout_windows[0]

    backtest = optimizer.build_optimization_window_backtest(
        config=config,
        window=window,
        params={"edge": 2},
        trial_id=11,
        name="generic_optimizer_research",
        return_summary_series=False,
    )

    assert isinstance(backtest, PredictionMarketBacktest)
    assert backtest.name == "generic_optimizer_research"
    assert backtest.return_summary_series is False
    assert len(backtest.replays) == 1
    assert backtest.replays[0].start_time == window.start_time
    assert backtest.replays[0].end_time == window.end_time
    assert backtest.strategy_configs[0]["strategy_path"] == "strategies:DemoStrategy"
    assert backtest.strategy_configs[0]["config_path"] == "strategies:DemoConfig"
    assert backtest.strategy_configs[0]["config"]["edge"] == 2


def test_build_parameter_search_window_backtest_accepts_mapping_params_for_tpe_space(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        strategy_spec={
            "strategy_path": "strategies:DemoStrategy",
            "config_path": "strategies:DemoConfig",
            "config": {"edge": "__SEARCH__:edge", "lookback": "__SEARCH__:lookback"},
        },
        parameter_grid={},
        parameter_space={
            "edge": {"type": "float", "low": 0.001, "high": 0.01, "log": True},
            "lookback": {"type": "int", "low": 16, "high": 128},
        },
        sampler="tpe",
    )

    backtest = optimizer.build_parameter_search_window_backtest(
        config=config,
        window=config.train_windows[0],
        params={"edge": 0.003, "lookback": 64},
    )

    assert backtest.strategy_configs[0]["config"] == {"edge": 0.003, "lookback": 64}


def test_tpe_int_step_is_forwarded_to_optuna(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        strategy_spec={
            "strategy_path": "strategies:DemoStrategy",
            "config_path": "strategies:DemoConfig",
            "config": {"lookback": "__SEARCH__:lookback"},
        },
        parameter_grid={},
        parameter_space={"lookback": {"type": "int", "low": 16, "high": 128, "step": 8}},
        sampler="tpe",
    )

    class Trial:
        kwargs: dict[str, object] | None = None

        def suggest_int(self, name: str, low: int, high: int, **kwargs: object) -> int:
            self.kwargs = {"name": name, "low": low, "high": high, **kwargs}
            return 24

    trial = Trial()

    assert optimizer._suggest_params_from_trial(trial, config.parameter_space) == (
        ("lookback", 24),
    )
    assert trial.kwargs == {"name": "lookback", "low": 16, "high": 128, "step": 8}


def test_tpe_step_rejects_log_sampling(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="step is not supported with log sampling"):
        _make_config(
            tmp_path,
            strategy_spec={
                "strategy_path": "strategies:DemoStrategy",
                "config_path": "strategies:DemoConfig",
                "config": {"edge": "__SEARCH__:edge"},
            },
            parameter_grid={},
            parameter_space={
                "edge": {"type": "float", "low": 0.001, "high": 0.01, "log": True, "step": 0.001}
            },
            sampler="tpe",
        )


def test_tpe_int_space_rejects_silent_bound_truncation(tmp_path: Path) -> None:
    for spec, match in (
        ({"type": "int", "low": 1.5, "high": 8}, "int low must be an integer"),
        ({"type": "int", "low": 1, "high": 8.5}, "int high must be an integer"),
        ({"type": "int", "low": 1, "high": 10, "step": 4}, "divisible by step"),
    ):
        with pytest.raises(ValueError, match=match):
            _make_config(
                tmp_path,
                strategy_spec={
                    "strategy_path": "strategies:DemoStrategy",
                    "config_path": "strategies:DemoConfig",
                    "config": {"lookback": "__SEARCH__:lookback"},
                },
                parameter_grid={},
                parameter_space={"lookback": spec},
                sampler="tpe",
            )


def test_tpe_float_space_rejects_silent_bound_adjustment(tmp_path: Path) -> None:
    for spec, match in (
        ({"type": "float", "low": 0.0, "high": 1.0, "log": True}, "low > 0"),
        ({"type": "float", "low": 0.0, "high": 1.0, "step": 0.3}, "divisible by step"),
    ):
        with pytest.raises(ValueError, match=match):
            _make_config(
                tmp_path,
                strategy_spec={
                    "strategy_path": "strategies:DemoStrategy",
                    "config_path": "strategies:DemoConfig",
                    "config": {"edge": "__SEARCH__:edge"},
                },
                parameter_grid={},
                parameter_space={"edge": spec},
                sampler="tpe",
            )


def test_optimizer_reruns_only_top_k_train_candidates_on_holdout_and_selects_by_holdout(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path, parameter_grid={"edge": (1, 2, 3)}, max_trials=3, holdout_top_k=2
    )
    scores = {
        1: {"train-a": 10.0, "train-b": 10.0, "holdout-a": 2.0},
        2: {"train-a": 9.0, "train-b": 9.0, "holdout-a": 7.0},
        3: {"train-a": 8.0, "train-b": 8.0, "holdout-a": 20.0},
    }
    calls: list[tuple[int, str]] = []

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        calls.append((edge, window_name))
        return _result_for_score(scores[edge][window_name])

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert summary.optimizer_type == OPTIMIZER_TYPE_PARAMETER_SEARCH
    assert dict(summary.selected_params) == {"edge": 2}
    assert len(summary.leaderboard) == 3
    assert (3, "holdout-a") not in calls
    assert sorted(edge for edge, window_name in calls if window_name == "holdout-a") == [1, 2]
    assert summary.best_row.holdout_median_score == 7.0


def test_optimizer_breaks_holdout_ties_with_train_median_score(tmp_path: Path) -> None:
    config = _make_config(tmp_path, parameter_grid={"edge": (1, 2)}, max_trials=2, holdout_top_k=2)
    scores = {
        1: {"train-a": 10.0, "train-b": 10.0, "holdout-a": 5.0},
        2: {"train-a": 9.0, "train-b": 9.0, "holdout-a": 5.0},
    }

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        return _result_for_score(scores[edge][window_name])

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 1}
    assert summary.best_row.holdout_median_score == 5.0
    assert summary.best_row.train_median_score == 10.0


def test_optimizer_keeps_failed_trials_visible_on_leaderboard(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path, parameter_grid={"edge": (1, 2)}, max_trials=2, holdout_windows=()
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 2:
            raise RuntimeError("simulated failure")
        return _result_for_score(5.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert len(summary.leaderboard) == 2
    failed_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 2})
    assert failed_row.train_scores == (config.invalid_score, config.invalid_score)
    assert failed_row.train_median_score == config.invalid_score


def test_optimizer_makes_any_invalid_train_window_fatal(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(
            _window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),
            _window("train-b", "2026-01-02T00:00:00Z", "2026-01-02T02:00:00Z"),
            _window("train-c", "2026-01-03T00:00:00Z", "2026-01-03T02:00:00Z"),
        ),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        if edge == 1 and window_name == "train-b":
            raise RuntimeError("simulated missing train window")
        return _result_for_score(1_000.0 if edge == 1 else 10.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_median_score == config.invalid_score


def test_optimizer_does_not_rescue_invalid_train_candidate_on_holdout(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_top_k=2,
        train_windows=(
            _window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),
            _window("train-b", "2026-01-02T00:00:00Z", "2026-01-02T02:00:00Z"),
        ),
        holdout_windows=(_window("holdout-a", "2026-01-03T00:00:00Z", "2026-01-03T02:00:00Z"),),
    )
    calls: list[tuple[int, str]] = []

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        calls.append((edge, window_name))
        if edge == 1 and window_name == "train-b":
            raise RuntimeError("simulated missing train window")
        if window_name.startswith("holdout"):
            return _result_for_score(1_000.0 if edge == 1 else 10.0)
        return _result_for_score(100.0 if edge == 1 else 20.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_median_score == config.invalid_score
    assert invalid_row.holdout_scores == ()
    assert (1, "holdout-a") not in calls


def test_optimizer_makes_any_invalid_holdout_window_fatal(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_top_k=2,
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
        holdout_windows=(
            _window("holdout-a", "2026-01-04T00:00:00Z", "2026-01-04T02:00:00Z"),
            _window("holdout-b", "2026-01-05T00:00:00Z", "2026-01-05T02:00:00Z"),
            _window("holdout-c", "2026-01-06T00:00:00Z", "2026-01-06T02:00:00Z"),
        ),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        if window_name.startswith("train"):
            return _result_for_score(20.0 if edge == 1 else 19.0)
        if edge == 1 and window_name == "holdout-b":
            raise RuntimeError("simulated missing holdout window")
        return _result_for_score(1_000.0 if edge == 1 else 10.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.holdout_median_score == config.invalid_score


def test_optimizer_maps_nonfinite_metrics_to_invalid_score(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2, 3, 4)},
        max_trials=4,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return _result_for_score(float("nan"))
        if edge == 2:
            return _result_for_score(float("inf"))
        if edge == 3:
            result = _result_for_score(1_000.0)
            result["equity_series"] = [
                ("2026-01-01T00:00:00Z", float("nan")),
                ("2026-01-01T00:01:00Z", float("inf")),
            ]
            return result
        return _result_for_score(1.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 4}
    invalid_rows = [
        row
        for row in summary.leaderboard
        if dict(row.params) in ({"edge": 1}, {"edge": 2}, {"edge": 3})
    ]
    assert len(invalid_rows) == 3
    assert all(row.train_scores == (config.invalid_score,) for row in invalid_rows)


def test_optimizer_maps_empty_and_wrong_result_counts_to_invalid_score(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2, 3)},
        max_trials=3,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> object:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return []
        if edge == 2:
            return [_result_for_score(1.0), _result_for_score(2.0)]
        return _result_for_score(3.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 3}
    invalid_rows = [
        row for row in summary.leaderboard if dict(row.params) in ({"edge": 1}, {"edge": 2})
    ]
    assert len(invalid_rows) == 2
    assert all(row.train_scores == (config.invalid_score,) for row in invalid_rows)


def test_optimizer_requires_equity_series_for_scoring(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2, 3)},
        max_trials=3,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            result = _result_for_score(1_000.0)
            result.pop("equity_series")
            return result
        if edge == 2:
            result = _result_for_score(900.0)
            result["equity_series"] = []
            return result
        return _result_for_score(3.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 3}
    invalid_rows = [
        row for row in summary.leaderboard if dict(row.params) in ({"edge": 1}, {"edge": 2})
    ]
    assert len(invalid_rows) == 2
    assert all(row.train_scores == (config.invalid_score,) for row in invalid_rows)


def test_optimizer_rejects_duplicate_multi_replay_result_identifiers(tmp_path: Path) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )
    config = replace(config, base_replays=replays, base_replay=config.base_replay)

    def _market_result(score: float, slug: str) -> dict[str, object]:
        result = _result_for_score(score)
        result["slug"] = slug
        result["token_index"] = 0
        return result

    def _evaluator(backtest: PredictionMarketBacktest) -> list[dict[str, object]]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return [_market_result(100.0, "market-one"), _market_result(100.0, "market-one")]
        return [_market_result(10.0, "market-one"), _market_result(10.0, "market-two")]

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_scores == (config.invalid_score,)


def test_optimizer_rejects_missing_multi_replay_result_identifiers(
    tmp_path: Path,
) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )
    config = replace(config, base_replays=replays, base_replay=config.base_replay)

    def _market_result(score: float, slug: str | None) -> dict[str, object]:
        result = _result_for_score(score)
        if slug is not None:
            result["slug"] = slug
            result["token_index"] = 0
        return result

    def _evaluator(backtest: PredictionMarketBacktest) -> list[dict[str, object]]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return [_market_result(100.0, None), _market_result(100.0, None)]
        return [_market_result(10.0, "market-one"), _market_result(10.0, "market-two")]

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_scores == (config.invalid_score,)


def test_optimizer_treats_backtest_realism_invalid_as_invalid_score(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            result = _result_for_score(1_000.0)
            result["backtest_realism_invalid"] = True
            return result
        return _result_for_score(10.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_scores == (config.invalid_score,)
    assert invalid_row.train_median_pnl == 1_000.0


def test_random_sampler_accepts_all_categorical_parameter_space(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={},
        parameter_space={"edge": {"type": "categorical", "choices": (2, 1)}},
        sampler="random",
        max_trials=2,
        holdout_windows=(),
    )

    assert optimizer._parameter_candidates(config.parameter_grid) == [
        (("edge", 2),),
        (("edge", 1),),
    ]


def test_random_sampler_rejects_continuous_parameter_space(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sampler='random'"):
        _make_config(
            tmp_path,
            parameter_grid={},
            parameter_space={"edge": {"type": "float", "low": 0.001, "high": 0.01}},
            sampler="random",
        )


def test_run_parameter_optimization_writes_artifacts(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        name="optimizer_artifact_test",
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_top_k=1,
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.replays[0].metadata["optimization_window"]
        holdout_bonus = 1.0 if window_name == "holdout-a" else 0.0
        return _result_for_score(float(edge) + holdout_bonus)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    leaderboard_path = tmp_path / "optimizer_artifact_test_leaderboard.csv"
    summary_path = tmp_path / "optimizer_artifact_test_summary.json"
    assert leaderboard_path.exists()
    assert summary_path.exists()

    payload = json.loads(summary_path.read_text())
    assert payload["name"] == summary.name
    assert payload["optimizer_type"] == OPTIMIZER_TYPE_PARAMETER_SEARCH
    assert payload["evaluated_trials"] == 2
    assert payload["train_windows"] == [window.name for window in config.train_windows]
    assert payload["holdout_windows"] == [window.name for window in config.holdout_windows]
    assert set(payload["best_candidate"]["params"]) == {"edge"}


def test_run_parameter_optimization_serializes_json_safe_params(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        name="optimizer_json_safe_params",
        parameter_grid={
            "edge": (
                Path("models/calibration.json"),
                datetime(2026, 1, 1, 12, 30, tzinfo=UTC),
            )
        },
        max_trials=2,
        holdout_windows=(),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        value = backtest.strategy_configs[0]["config"]["edge"]
        return _result_for_score(2.0 if isinstance(value, datetime) else 1.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    payload = json.loads(Path(summary.summary_json_path).read_text(encoding="utf-8"))
    assert payload["selected_params"] == {"edge": "2026-01-01T12:30:00+00:00"}
    assert payload["best_candidate"]["params"] == {"edge": "2026-01-01T12:30:00+00:00"}
    csv_text = Path(summary.leaderboard_csv_path).read_text(encoding="utf-8")
    assert '"models/calibration.json"' in csv_text
    assert '"2026-01-01T12:30:00+00:00"' in csv_text


def test_run_parameter_optimization_serializes_nonfinite_params_as_strict_json(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        name="optimizer_nonfinite_json_safe_params",
        parameter_grid={
            "edge": (
                Path("models/calibration.json"),
                datetime(2026, 1, 1, 12, 30, tzinfo=UTC),
                float("nan"),
                float("inf"),
            )
        },
        max_trials=4,
        holdout_windows=(),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        value = backtest.strategy_configs[0]["config"]["edge"]
        if isinstance(value, float) and math.isnan(value):
            return _result_for_score(4.0)
        if isinstance(value, float) and math.isinf(value):
            return _result_for_score(3.0)
        if isinstance(value, datetime):
            return _result_for_score(2.0)
        return _result_for_score(1.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    summary_text = Path(summary.summary_json_path).read_text(encoding="utf-8")
    payload = _strict_json_loads(summary_text)
    assert payload["selected_params"] == {"edge": "nan"}  # type: ignore[index]
    assert "NaN" not in summary_text
    assert "Infinity" not in summary_text

    with Path(summary.leaderboard_csv_path).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    params = [_strict_json_loads(row["params_json"]) for row in rows]
    assert {"edge": "nan"} in params
    assert {"edge": "inf"} in params


def test_joint_portfolio_drawdown_captures_diversification() -> None:
    # Two anti-correlated equity curves: market A dips while B rises, and
    # vice versa. Joint portfolio drawdown should be much smaller than the
    # sum of per-market drawdowns (which is the naive conservative estimate).
    market_a = [
        ("2026-01-01T00:00:00Z", 100.0),
        ("2026-01-01T01:00:00Z", 90.0),
        ("2026-01-01T02:00:00Z", 110.0),
        ("2026-01-01T03:00:00Z", 100.0),
    ]
    market_b = [
        ("2026-01-01T00:00:00Z", 100.0),
        ("2026-01-01T01:00:00Z", 110.0),
        ("2026-01-01T02:00:00Z", 90.0),
        ("2026-01-01T03:00:00Z", 100.0),
    ]
    per_market_drawdowns = optimizer._max_drawdown_currency(
        market_a
    ) + optimizer._max_drawdown_currency(market_b)
    joint = optimizer._joint_portfolio_drawdown([market_a, market_b])
    assert per_market_drawdowns == pytest.approx(30.0)
    assert joint < per_market_drawdowns
    assert joint == pytest.approx(0.0, abs=1e-9)


def test_joint_portfolio_drawdown_rejects_nonfinite_values() -> None:
    for value in (float("nan"), float("inf"), -float("inf")):
        joint = optimizer._joint_portfolio_drawdown(
            [
                [
                    ("2026-01-01T00:00:00Z", 100.0),
                    ("2026-01-01T01:00:00Z", value),
                    ("2026-01-01T02:00:00Z", 90.0),
                ]
            ]
        )

        assert not math.isfinite(joint)


def test_optimizer_rejects_malformed_joint_equity_timestamps(tmp_path: Path) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1,)},
        max_trials=1,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )
    config = replace(config, base_replays=replays, base_replay=config.base_replay)

    def _market_result(score: float, slug: str, equity_series: object) -> dict[str, object]:
        result = _result_for_score(score)
        result["slug"] = slug
        result["token_index"] = 0
        result["equity_series"] = equity_series
        return result

    evaluation = optimizer._evaluate_window(
        config=config,
        evaluator=lambda _backtest: [
            _market_result(100.0, "market-one", [("not-a-timestamp", 100.0)]),
            _market_result(
                100.0,
                "market-two",
                [
                    ("2026-01-01T00:00:00Z", 100.0),
                    ("2026-01-01T01:00:00Z", 90.0),
                ],
            ),
        ],
        trial_id=1,
        params=(("edge", 1),),
        window=config.train_windows[0],
    )

    assert evaluation.status == "invalid_nonfinite_metric"
    assert evaluation.score == config.invalid_score
    assert evaluation.error == "max_drawdown_currency is non-finite: nan"


def test_optimizer_uses_returned_joint_portfolio_equity_series_for_drawdown(
    tmp_path: Path,
) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )
    config = replace(config, base_replays=replays, base_replay=config.base_replay)

    def _market_result(
        score: float,
        *,
        slug: str,
        equity_series: object,
        joint_portfolio_equity_series: object | None = None,
    ) -> dict[str, object]:
        result = _result_for_score(score)
        result["slug"] = slug
        result["token_index"] = 0
        result["equity_series"] = equity_series
        if joint_portfolio_equity_series is not None:
            result["joint_portfolio_equity_series"] = joint_portfolio_equity_series
        return result

    def _evaluator(backtest: PredictionMarketBacktest) -> list[dict[str, object]]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return [
                _market_result(
                    3.0,
                    slug="market-one",
                    equity_series=[
                        ("2026-01-01T00:00:00Z", 100.0),
                        ("2026-01-01T01:00:00Z", 70.0),
                        ("2026-01-01T02:00:00Z", 103.0),
                    ],
                    joint_portfolio_equity_series=[
                        ("2026-01-01T00:00:00Z", 200.0),
                        ("2026-01-01T01:00:00Z", 200.0),
                        ("2026-01-01T02:00:00Z", 206.0),
                    ],
                ),
                _market_result(
                    3.0,
                    slug="market-two",
                    equity_series=[
                        ("2026-01-01T00:00:00Z", 100.0),
                        ("2026-01-01T01:00:00Z", 70.0),
                        ("2026-01-01T02:00:00Z", 103.0),
                    ],
                ),
            ]
        return [
            _market_result(
                2.0,
                slug="market-one",
                equity_series=[("2026-01-01T00:00:00Z", 100.0)],
            ),
            _market_result(
                2.0,
                slug="market-two",
                equity_series=[("2026-01-01T00:00:00Z", 100.0)],
            ),
        ]

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 1}
    best_row = summary.best_row
    assert best_row.train_median_pnl == pytest.approx(6.0)
    assert best_row.train_median_drawdown == pytest.approx(0.0)
    assert best_row.train_scores == (6.0,)


def test_optimizer_rejects_nonfinite_returned_joint_portfolio_equity_series(
    tmp_path: Path,
) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
    )
    config = replace(config, base_replays=replays, base_replay=config.base_replay)

    def _market_result(
        score: float,
        *,
        slug: str,
        joint_portfolio_equity_series: object | None = None,
    ) -> dict[str, object]:
        result = _result_for_score(score)
        result["slug"] = slug
        result["token_index"] = 0
        if joint_portfolio_equity_series is not None:
            result["joint_portfolio_equity_series"] = joint_portfolio_equity_series
        return result

    def _evaluator(backtest: PredictionMarketBacktest) -> list[dict[str, object]]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 1:
            return [
                _market_result(
                    1_000.0,
                    slug="market-one",
                    joint_portfolio_equity_series=[
                        ("2026-01-01T00:00:00Z", 200.0),
                        ("2026-01-01T01:00:00Z", float("nan")),
                    ],
                ),
                _market_result(1_000.0, slug="market-two"),
            ]
        return [_market_result(1.0, slug="market-one"), _market_result(1.0, slug="market-two")]

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 2}
    invalid_row = next(row for row in summary.leaderboard if dict(row.params) == {"edge": 1})
    assert invalid_row.train_scores == (config.invalid_score,)


def test_joint_portfolio_drawdown_tracks_concurrent_losses() -> None:
    # Two correlated curves that drop at the same time should produce a
    # joint drawdown equal to the sum of individual drawdowns.
    series = [
        ("2026-01-01T00:00:00Z", 100.0),
        ("2026-01-01T01:00:00Z", 80.0),
        ("2026-01-01T02:00:00Z", 100.0),
    ]
    joint = optimizer._joint_portfolio_drawdown([series, series])
    assert joint == pytest.approx(40.0)


def test_joint_portfolio_drawdown_backfills_later_market_starts() -> None:
    market_a = [
        ("2026-01-01T00:00:00Z", 100.0),
        ("2026-01-01T01:00:00Z", 90.0),
        ("2026-01-01T02:00:00Z", 90.0),
    ]
    market_b = [
        ("2026-01-01T01:00:00Z", 100.0),
        ("2026-01-01T02:00:00Z", 100.0),
    ]

    joint = optimizer._joint_portfolio_drawdown([market_a, market_b])

    assert joint == pytest.approx(10.0)


def test_parameter_search_config_accepts_base_replays_for_multi_market(
    tmp_path: Path,
) -> None:
    replays = (
        BookReplay(market_slug="market-one", token_index=0),
        BookReplay(market_slug="market-two", token_index=0),
    )
    config = optimizer.ParameterSearchConfig(
        name="joint_test",
        data=MarketDataConfig(
            platform=Polymarket, data_type=Book, vendor=PMXT, sources=("local:/tmp",)
        ),
        base_replays=replays,
        strategy_spec={
            "strategy_path": "strategies:DemoStrategy",
            "config_path": "strategies:DemoConfig",
            "config": {"edge": "__SEARCH__:edge"},
        },
        parameter_grid={"edge": (1, 2)},
        train_windows=(_window("train-a", "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"),),
        holdout_windows=(),
        max_trials=1,
        artifact_root=tmp_path,
    )
    assert len(config.base_replays) == 2
    window = config.train_windows[0]
    kwargs = optimizer._build_backtest_kwargs(
        config=config, trial_id=1, window=window, params=(("edge", 1),)
    )
    assert len(kwargs["replays"]) == 2
    assert all(r.start_time == window.start_time for r in kwargs["replays"])
