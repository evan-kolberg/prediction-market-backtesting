from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pandas as pd

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketReportConfig,
    PredictionMarketBacktest,
    finalize_market_results,
)
from prediction_market_extensions.backtesting._replay_specs import ReplaySpec
from prediction_market_extensions.backtesting._result_policies import ResultPolicy
from prediction_market_extensions.backtesting._strategy_configs import StrategyConfigSpec
from prediction_market_extensions.backtesting.optimizers import (
    ParameterSearchConfig,
    ParameterSearchSummary,
    run_parameter_search,
)

if TYPE_CHECKING:
    from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig


type MultiReplayMode = Literal["joint_portfolio", "independent"]


@dataclass(frozen=True)
class ReplayExperiment:
    name: str
    description: str
    data: MarketDataConfig
    replays: Sequence[ReplaySpec]
    strategy_configs: Sequence[StrategyConfigSpec] = ()
    strategy_factory: Callable[..., Any] | None = None
    initial_cash: float = 100.0
    probability_window: int = 30
    min_trades: int = 0
    min_quotes: int = 0
    min_price_range: float = 0.0
    default_lookback_days: int | None = None
    default_lookback_hours: float | None = None
    default_start_time: pd.Timestamp | datetime | str | None = None
    default_end_time: pd.Timestamp | datetime | str | None = None
    nautilus_log_level: str = "INFO"
    execution: ExecutionModelConfig | None = None
    chart_resample_rule: str | None = None
    emit_html: bool = True
    chart_output_path: str | Path | None = None
    return_chart_layout: bool = False
    return_summary_series: bool = False
    detail_plot_panels: Sequence[str] | None = None
    multi_replay_mode: MultiReplayMode = "joint_portfolio"
    report: MarketReportConfig | None = None
    empty_message: str | None = None
    partial_message: str | None = None
    result_policy: ResultPolicy | None = None


@dataclass(frozen=True)
class ParameterSearchExperiment:
    name: str
    description: str
    parameter_search: ParameterSearchConfig

    @property
    def optimization(self) -> ParameterSearchConfig:
        return self.parameter_search


type Experiment = ReplayExperiment | ParameterSearchExperiment


def build_backtest_for_experiment(experiment: ReplayExperiment) -> PredictionMarketBacktest:
    return PredictionMarketBacktest(
        name=experiment.name,
        data=experiment.data,
        replays=tuple(experiment.replays),
        strategy_configs=tuple(experiment.strategy_configs),
        strategy_factory=experiment.strategy_factory,
        initial_cash=experiment.initial_cash,
        probability_window=experiment.probability_window,
        min_trades=experiment.min_trades,
        min_quotes=experiment.min_quotes,
        min_price_range=experiment.min_price_range,
        default_lookback_days=experiment.default_lookback_days,
        default_lookback_hours=experiment.default_lookback_hours,
        default_start_time=experiment.default_start_time,
        default_end_time=experiment.default_end_time,
        nautilus_log_level=experiment.nautilus_log_level,
        execution=experiment.execution,
        chart_resample_rule=experiment.chart_resample_rule,
        emit_html=experiment.emit_html,
        chart_output_path=experiment.chart_output_path,
        return_chart_layout=experiment.return_chart_layout,
        return_summary_series=experiment.return_summary_series,
        detail_plot_panels=experiment.detail_plot_panels,
    )


def build_replay_experiment(
    *,
    name: str,
    description: str,
    data: MarketDataConfig,
    replays: Sequence[ReplaySpec],
    strategy_configs: Sequence[StrategyConfigSpec] = (),
    strategy_factory: Callable[..., Any] | None = None,
    initial_cash: float = 100.0,
    probability_window: int = 30,
    min_trades: int = 0,
    min_quotes: int = 0,
    min_price_range: float = 0.0,
    default_lookback_days: int | None = None,
    default_lookback_hours: float | None = None,
    default_start_time: pd.Timestamp | datetime | str | None = None,
    default_end_time: pd.Timestamp | datetime | str | None = None,
    nautilus_log_level: str = "INFO",
    execution: ExecutionModelConfig | None = None,
    chart_resample_rule: str | None = None,
    emit_html: bool = True,
    chart_output_path: str | Path | None = None,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    detail_plot_panels: Sequence[str] | None = None,
    multi_replay_mode: MultiReplayMode = "joint_portfolio",
    report: MarketReportConfig | None = None,
    empty_message: str | None = None,
    partial_message: str | None = None,
    result_policy: ResultPolicy | None = None,
) -> ReplayExperiment:
    return ReplayExperiment(
        name=name,
        description=description,
        data=data,
        replays=tuple(replays),
        strategy_configs=tuple(strategy_configs),
        strategy_factory=strategy_factory,
        initial_cash=initial_cash,
        probability_window=probability_window,
        min_trades=min_trades,
        min_quotes=min_quotes,
        min_price_range=min_price_range,
        default_lookback_days=default_lookback_days,
        default_lookback_hours=default_lookback_hours,
        default_start_time=default_start_time,
        default_end_time=default_end_time,
        nautilus_log_level=nautilus_log_level,
        execution=execution,
        chart_resample_rule=chart_resample_rule,
        emit_html=emit_html,
        chart_output_path=chart_output_path,
        return_chart_layout=return_chart_layout,
        return_summary_series=return_summary_series,
        detail_plot_panels=detail_plot_panels,
        multi_replay_mode=multi_replay_mode,
        report=report,
        empty_message=empty_message,
        partial_message=partial_message,
        result_policy=result_policy,
    )


def replay_experiment_from_backtest(
    *,
    backtest: PredictionMarketBacktest,
    description: str,
    report: MarketReportConfig | None = None,
    empty_message: str | None = None,
    partial_message: str | None = None,
    result_policy: ResultPolicy | None = None,
) -> ReplayExperiment:
    return ReplayExperiment(
        name=backtest.name,
        description=description,
        data=backtest.data,
        replays=backtest.replays,
        strategy_configs=backtest.strategy_configs,
        strategy_factory=backtest.strategy_factory,
        initial_cash=backtest.initial_cash,
        probability_window=backtest.probability_window,
        min_trades=backtest.min_trades,
        min_quotes=backtest.min_quotes,
        min_price_range=backtest.min_price_range,
        default_lookback_days=backtest.default_lookback_days,
        default_lookback_hours=backtest.default_lookback_hours,
        default_start_time=backtest.default_start_time,
        default_end_time=backtest.default_end_time,
        nautilus_log_level=backtest.nautilus_log_level,
        execution=backtest.execution,
        chart_resample_rule=backtest.chart_resample_rule,
        emit_html=backtest.emit_html,
        chart_output_path=backtest.chart_output_path,
        return_chart_layout=backtest.return_chart_layout,
        return_summary_series=backtest.return_summary_series,
        detail_plot_panels=backtest.detail_plot_panels,
        multi_replay_mode="joint_portfolio",
        report=report,
        empty_message=empty_message,
        partial_message=partial_message,
        result_policy=result_policy,
    )


async def run_replay_experiment_async(experiment: ReplayExperiment) -> list[dict[str, Any]]:
    backtest = build_backtest_for_experiment(experiment)
    results = await _run_replay_backtest_async(
        backtest=backtest, multi_replay_mode=experiment.multi_replay_mode
    )
    if not results:
        if experiment.empty_message:
            print(experiment.empty_message)
        return []

    if experiment.result_policy is not None:
        transformed = experiment.result_policy.apply(results)
        if transformed is not None:
            results = transformed

    if experiment.partial_message and len(results) < len(experiment.replays):
        print(
            experiment.partial_message.format(completed=len(results), total=len(experiment.replays))
        )

    if experiment.report is not None:
        finalize_market_results(
            name=experiment.name,
            results=results,
            report=experiment.report,
            multi_replay_mode=experiment.multi_replay_mode,
        )
    return results


def _dispatch_multi_replay_runner(backtest: PredictionMarketBacktest) -> list[dict[str, Any]]:
    from prediction_market_extensions.backtesting._independent_multi_replay_runner import (
        run_independent_multi_replay_backtest_async,
    )

    return asyncio.run(run_independent_multi_replay_backtest_async(backtest=backtest))


async def _dispatch_multi_replay_runner_async(
    backtest: PredictionMarketBacktest,
) -> list[dict[str, Any]]:
    from prediction_market_extensions.backtesting._independent_multi_replay_runner import (
        run_independent_multi_replay_backtest_async,
    )

    return await run_independent_multi_replay_backtest_async(backtest=backtest)


def _run_replay_backtest(
    backtest: PredictionMarketBacktest, *, multi_replay_mode: MultiReplayMode
) -> list[dict[str, Any]]:
    if multi_replay_mode == "independent" and len(backtest.replays) > 1:
        return _dispatch_multi_replay_runner(backtest)
    return backtest.run()


async def _run_replay_backtest_async(
    *, backtest: PredictionMarketBacktest, multi_replay_mode: MultiReplayMode
) -> list[dict[str, Any]]:
    if multi_replay_mode == "independent" and len(backtest.replays) > 1:
        return await _dispatch_multi_replay_runner_async(backtest)
    return await backtest.run_async()


def _finalize_replay_results(
    experiment: ReplayExperiment, results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Apply result policies, print status messages, and run report finalization."""
    if not results:
        if experiment.empty_message:
            print(experiment.empty_message)
        return []

    if experiment.partial_message and len(results) < len(experiment.replays):
        print(
            experiment.partial_message.format(completed=len(results), total=len(experiment.replays))
        )

    if experiment.result_policy is not None:
        transformed = experiment.result_policy.apply(results)
        if transformed is not None:
            results = transformed

    if experiment.report is not None:
        finalize_market_results(
            name=experiment.name,
            results=results,
            report=experiment.report,
            multi_replay_mode=experiment.multi_replay_mode,
        )

    return results


def run_experiment(experiment: Experiment) -> list[dict[str, Any]] | ParameterSearchSummary:
    if isinstance(experiment, ParameterSearchExperiment):
        return run_parameter_search(experiment.parameter_search)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "run_experiment() cannot be called inside an active event loop; use await run_experiment_async() instead."
        )

    backtest = build_backtest_for_experiment(experiment)
    results = _run_replay_backtest(backtest, multi_replay_mode=experiment.multi_replay_mode)
    return _finalize_replay_results(experiment, results)


async def run_experiment_async(
    experiment: Experiment,
) -> list[dict[str, Any]] | ParameterSearchSummary:
    if isinstance(experiment, ParameterSearchExperiment):
        return await asyncio.to_thread(run_parameter_search, experiment.parameter_search)

    backtest = build_backtest_for_experiment(experiment)
    results = await _run_replay_backtest_async(
        backtest=backtest, multi_replay_mode=experiment.multi_replay_mode
    )
    return _finalize_replay_results(experiment, results)


__all__ = [
    "Experiment",
    "MultiReplayMode",
    "ParameterSearchExperiment",
    "ReplayExperiment",
    "build_backtest_for_experiment",
    "build_replay_experiment",
    "replay_experiment_from_backtest",
    "run_experiment",
    "run_experiment_async",
    "run_replay_experiment_async",
]


OptimizationExperiment = ParameterSearchExperiment
