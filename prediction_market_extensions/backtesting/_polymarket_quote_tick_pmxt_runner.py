# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-31, 2026-04-04, and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Shared runner for single-market Polymarket PMXT L2 backtests.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
)
from prediction_market_extensions.backtesting._experiments import ReplayExperiment
from prediction_market_extensions.backtesting._experiments import (
    run_replay_experiment_async,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketReportConfig,
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
from prediction_market_extensions.backtesting._strategy_configs import (
    resolve_strategy_factory,
)
from prediction_market_extensions.backtesting._strategy_configs import (
    StrategyConfigSpec,
)


type StrategyFactory = Callable[[InstrumentId], Strategy]


async def run_single_market_pmxt_backtest(
    *,
    name: str,
    market_slug: str,
    token_index: int = 0,
    lookback_hours: float | None = None,
    probability_window: int,
    strategy_factory: StrategyFactory | None = None,
    strategy_configs: Sequence[StrategyConfigSpec] | None = None,
    min_quotes: int = 0,
    min_price_range: float = 0.0,
    initial_cash: float = 100.0,
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    chart_output_path: str | Path | None = None,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    detail_plot_panels: Sequence[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    data_sources: Sequence[str] = (),
    execution: ExecutionModelConfig | None = None,
    nautilus_log_level: str = "INFO",
) -> dict[str, Any] | None:
    strategy_factory = resolve_strategy_factory(
        strategy_factory=strategy_factory,
        strategy_configs=strategy_configs,
    )
    report = MarketReportConfig(
        count_key="quotes",
        count_label="Quotes",
        pnl_label="PnL (USDC)",
        market_key="slug",
    )
    experiment = ReplayExperiment(
        name=name,
        description=name,
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
            sources=tuple(data_sources),
        ),
        replays=(
            PolymarketPMXTQuoteReplay(
                market_slug=market_slug,
                token_index=token_index,
                lookback_hours=lookback_hours,
                start_time=start_time,
                end_time=end_time,
            ),
        ),
        strategy_factory=strategy_factory,
        initial_cash=initial_cash,
        probability_window=probability_window,
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
        report=report,
    )
    if not emit_summary:
        experiment = replace(experiment, report=None)
    results = await run_replay_experiment_async(experiment)
    return results[0] if results else None


__all__ = ["PredictionMarketBacktest", "run_single_market_pmxt_backtest"]
