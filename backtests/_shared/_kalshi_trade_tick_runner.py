# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Shared runner for single-market Kalshi trade-tick backtests.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Any

import pandas as pd

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._strategy_configs import resolve_strategy_factory
from backtests._shared._strategy_configs import StrategyConfigSpec


type StrategyFactory = Callable[[InstrumentId], Strategy]


async def run_single_market_trade_backtest(
    *,
    name: str,
    market_ticker: str,
    lookback_days: int,
    probability_window: int,
    strategy_factory: StrategyFactory | None = None,
    strategy_configs: Sequence[StrategyConfigSpec] | None = None,
    min_trades: int = 0,
    min_price_range: float = 0.0,
    initial_cash: float = 100.0,
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    end_time: pd.Timestamp | datetime | None = None,
    data_sources: tuple[str, ...] = (),
    execution: ExecutionModelConfig | None = None,
) -> dict[str, Any] | None:
    strategy_factory = resolve_strategy_factory(
        strategy_factory=strategy_factory,
        strategy_configs=strategy_configs,
    )
    end = pd.Timestamp(end_time if end_time is not None else datetime.now(UTC))
    if end.tzinfo is None:
        end = end.tz_localize(UTC)

    backtest = PredictionMarketBacktest(
        name=name,
        data=MarketDataConfig(
            platform="kalshi",
            data_type="trade_tick",
            vendor="native",
            sources=data_sources,
        ),
        sims=(
            MarketSimConfig(
                market_ticker=market_ticker,
                lookback_days=lookback_days,
                end_time=end,
            ),
        ),
        strategy_factory=strategy_factory,
        initial_cash=initial_cash,
        probability_window=probability_window,
        min_trades=min_trades,
        min_price_range=min_price_range,
        execution=execution,
        chart_resample_rule=chart_resample_rule,
        emit_html=emit_html,
        return_chart_layout=return_chart_layout,
    )
    report = MarketReportConfig(
        count_key="trades",
        count_label="Trades",
        pnl_label="PnL (USD)",
        market_key="ticker",
    )

    results = await backtest.run_async()
    if emit_summary and results:
        finalize_market_results(name=backtest.name, results=results, report=report)
    return results[0] if results else None
