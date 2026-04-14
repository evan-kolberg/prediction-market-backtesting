from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._experiments import (
    ReplayExperiment,
    run_replay_experiment_async,
)
from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._result_policies import ResultPolicy
from prediction_market_extensions.backtesting._strategy_configs import StrategyConfigSpec
from prediction_market_extensions.backtesting.data_sources.registry import (
    build_single_market_replay,
    resolve_market_data_support,
)

type StrategyFactory = Callable[[InstrumentId], Strategy]


async def run_single_market_backtest(
    *,
    name: str,
    data: MarketDataConfig,
    probability_window: int,
    strategy_factory: StrategyFactory | None = None,
    strategy_configs: Sequence[StrategyConfigSpec] | None = None,
    market_slug: str | None = None,
    market_ticker: str | None = None,
    token_index: int = 0,
    lookback_days: int | None = None,
    lookback_hours: float | None = None,
    min_trades: int = 0,
    min_quotes: int = 0,
    min_price_range: float = 0.0,
    initial_cash: float = 100.0,
    nautilus_log_level: str = "INFO",
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    chart_output_path: str | Path | None = None,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    detail_plot_panels: Sequence[str] | None = None,
    report: MarketReportConfig | None = None,
    empty_message: str | None = None,
    partial_message: str | None = None,
    result_policy: ResultPolicy | None = None,
    start_time: pd.Timestamp | datetime | str | None = None,
    end_time: pd.Timestamp | datetime | str | None = None,
    execution: ExecutionModelConfig | None = None,
) -> dict[str, Any] | None:
    support = resolve_market_data_support(
        platform=data.platform, data_type=data.data_type, vendor=data.vendor
    )
    replay = build_single_market_replay(
        support=support,
        field_values={
            "market_slug": market_slug,
            "market_ticker": market_ticker,
            "token_index": token_index,
            "lookback_days": lookback_days,
            "lookback_hours": lookback_hours,
            "start_time": start_time,
            "end_time": end_time,
        },
    )
    results = await run_replay_experiment_async(
        ReplayExperiment(
            name=name,
            description=name,
            data=data,
            replays=(replay,),
            strategy_factory=strategy_factory,
            strategy_configs=tuple(strategy_configs or ()),
            initial_cash=initial_cash,
            probability_window=probability_window,
            min_trades=min_trades,
            min_quotes=min_quotes,
            min_price_range=min_price_range,
            nautilus_log_level=nautilus_log_level,
            execution=execution,
            chart_resample_rule=chart_resample_rule,
            emit_html=emit_html,
            chart_output_path=chart_output_path,
            return_chart_layout=return_chart_layout,
            return_summary_series=return_summary_series,
            detail_plot_panels=detail_plot_panels,
            report=report if emit_summary else None,
            empty_message=empty_message,
            partial_message=partial_message,
            result_policy=result_policy,
        )
    )
    return results[0] if results else None


__all__ = ["MarketDataConfig", "run_single_market_backtest"]
