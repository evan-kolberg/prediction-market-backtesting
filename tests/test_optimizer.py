from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from prediction_market_extensions.backtesting import _optimizer as optimizer
from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
)
from prediction_market_extensions.backtesting._execution_config import (
    StaticLatencyConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_runner import (
    MarketDataConfig,
)
from prediction_market_extensions.backtesting._replay_specs import (
    PolymarketPMXTQuoteReplay,
)
from prediction_market_extensions.backtesting.data_sources import (
    PMXT,
    Polymarket,
    QuoteTick,
)
from prediction_market_extensions.backtesting.optimizers import (
    OPTIMIZER_TYPE_PARAMETER_SEARCH,
)


def _window(
    name: str,
    start_time: str,
    end_time: str,
) -> optimizer.ParameterSearchWindow:
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


def _make_config(
    tmp_path: Path,
    *,
    name: str = "optimizer_test",
    strategy_spec: dict[str, object] | None = None,
    parameter_grid: dict[str, tuple[object, ...]] | None = None,
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
    resolved_parameter_grid = (
        parameter_grid if parameter_grid is not None else {"edge": (1, 2, 3)}
    )
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
            platform=Polymarket,
            data_type=QuoteTick,
            vendor=PMXT,
            sources=("local:/tmp/pmxt_raws",),
        ),
        base_replay=PolymarketPMXTQuoteReplay(
            market_slug="demo-market",
            token_index=0,
        ),
        strategy_spec=resolved_strategy_spec,
        parameter_grid=resolved_parameter_grid,
        train_windows=resolved_train_windows,
        holdout_windows=resolved_holdout_windows,
        max_trials=max_trials,
        random_seed=random_seed,
        holdout_top_k=holdout_top_k,
        initial_cash=100.0,
        min_quotes=500,
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


def test_parameter_search_config_carries_explicit_optimizer_type(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)

    assert config.optimizer_type == OPTIMIZER_TYPE_PARAMETER_SEARCH


def test_sample_parameter_sets_is_deterministic_and_unique(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 1, 2, 3)},
        max_trials=2,
        random_seed=11,
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


def test_replace_search_placeholders_binds_nested_payloads() -> None:
    payload = {
        "fast_period": "__SEARCH__:fast_period",
        "nested": [
            {"slow_period": "__SEARCH__:slow_period"},
            ("keep", "__SEARCH__:stop_loss"),
        ],
    }

    replaced = optimizer._replace_search_placeholders(
        payload,
        {
            "fast_period": 32,
            "slow_period": 128,
            "stop_loss": 0.01,
        },
    )

    assert replaced == {
        "fast_period": 32,
        "nested": [{"slow_period": 128}, ("keep", 0.01)],
    }


def test_score_result_penalizes_drawdown_termination_low_coverage_and_low_fills() -> (
    None
):
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


def test_optimizer_builds_repo_layer_backtest_with_summary_series_enabled(
    tmp_path: Path,
) -> None:
    config = replace(
        _make_config(tmp_path, parameter_grid={"edge": (5,)}, max_trials=1),
        chart_output_path="output/optimizer_trial_chart.html",
    )
    window = config.train_windows[0]

    backtest = optimizer._build_backtest(
        config=config,
        trial_id=7,
        window=window,
        params=(("edge", 5),),
    )

    assert isinstance(backtest, PredictionMarketBacktest)
    assert backtest.name == "optimizer_test:train-a:trial-007"
    assert backtest.data is config.data
    assert backtest.initial_cash == 100.0
    assert backtest.min_quotes == 500
    assert backtest.min_price_range == 0.005
    assert backtest.execution == config.execution
    assert backtest.emit_html is False
    assert backtest.chart_output_path == "output/optimizer_trial_chart.html"
    assert backtest.return_summary_series is True
    assert len(backtest.sims) == 1
    assert backtest.sims[0].start_time == window.start_time
    assert backtest.sims[0].end_time == window.end_time
    assert backtest.sims[0].metadata == {"optimization_window": "train-a"}
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
        emit_html=True,
        chart_output_path="output/generic_optimizer_research.html",
        return_summary_series=False,
    )

    assert isinstance(backtest, PredictionMarketBacktest)
    assert backtest.name == "generic_optimizer_research"
    assert backtest.emit_html is True
    assert backtest.chart_output_path == "output/generic_optimizer_research.html"
    assert backtest.return_summary_series is False
    assert len(backtest.sims) == 1
    assert backtest.sims[0].start_time == window.start_time
    assert backtest.sims[0].end_time == window.end_time
    assert backtest.strategy_configs[0]["strategy_path"] == "strategies:DemoStrategy"
    assert backtest.strategy_configs[0]["config_path"] == "strategies:DemoConfig"
    assert backtest.strategy_configs[0]["config"]["edge"] == 2


def test_optimizer_reruns_only_top_k_train_candidates_on_holdout_and_selects_by_holdout(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2, 3)},
        max_trials=3,
        holdout_top_k=2,
    )
    scores = {
        1: {"train-a": 10.0, "train-b": 10.0, "holdout-a": 2.0},
        2: {"train-a": 9.0, "train-b": 9.0, "holdout-a": 7.0},
        3: {"train-a": 8.0, "train-b": 8.0, "holdout-a": 20.0},
    }
    calls: list[tuple[int, str]] = []

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.sims[0].metadata["optimization_window"]
        calls.append((edge, window_name))
        return _result_for_score(scores[edge][window_name])

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert summary.optimizer_type == OPTIMIZER_TYPE_PARAMETER_SEARCH
    assert dict(summary.selected_params) == {"edge": 2}
    assert len(summary.leaderboard) == 3
    assert (3, "holdout-a") not in calls
    assert sorted(
        edge for edge, window_name in calls if window_name == "holdout-a"
    ) == [
        1,
        2,
    ]
    assert summary.best_row.holdout_median_score == 7.0


def test_optimizer_breaks_holdout_ties_with_train_median_score(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_top_k=2,
    )
    scores = {
        1: {"train-a": 10.0, "train-b": 10.0, "holdout-a": 5.0},
        2: {"train-a": 9.0, "train-b": 9.0, "holdout-a": 5.0},
    }

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.sims[0].metadata["optimization_window"]
        return _result_for_score(scores[edge][window_name])

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert dict(summary.selected_params) == {"edge": 1}
    assert summary.best_row.holdout_median_score == 5.0
    assert summary.best_row.train_median_score == 10.0


def test_optimizer_keeps_failed_trials_visible_on_leaderboard(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_windows=(),
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        if edge == 2:
            raise RuntimeError("simulated failure")
        return _result_for_score(5.0)

    summary = optimizer.run_parameter_optimization(config, evaluator=_evaluator)

    assert len(summary.leaderboard) == 2
    failed_row = next(
        row for row in summary.leaderboard if dict(row.params) == {"edge": 2}
    )
    assert failed_row.train_scores == (config.invalid_score, config.invalid_score)
    assert failed_row.train_median_score == config.invalid_score


def test_run_parameter_optimization_writes_artifacts(
    tmp_path: Path,
) -> None:
    config = _make_config(
        tmp_path,
        name="optimizer_artifact_test",
        parameter_grid={"edge": (1, 2)},
        max_trials=2,
        holdout_top_k=1,
    )

    def _evaluator(backtest: PredictionMarketBacktest) -> dict[str, object]:
        edge = backtest.strategy_configs[0]["config"]["edge"]
        window_name = backtest.sims[0].metadata["optimization_window"]
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
    assert payload["holdout_windows"] == [
        window.name for window in config.holdout_windows
    ]
    assert set(payload["best_candidate"]["params"]) == {"edge"}
