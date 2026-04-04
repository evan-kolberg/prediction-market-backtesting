from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources import MarketDataType
from backtests._shared.data_sources import MarketPlatform
from backtests._shared.data_sources import MarketDataVendor
from backtests._shared._kalshi_trade_tick_runner import (
    run_single_market_trade_backtest as run_single_market_kalshi_trade_backtest,
)
from backtests._shared._polymarket_quote_tick_pmxt_runner import (
    run_single_market_pmxt_backtest,
)
from backtests._shared._polymarket_trade_tick_runner import (
    run_single_market_trade_backtest as run_single_market_polymarket_trade_backtest,
)


type StrategyFactory = Callable[[InstrumentId], Strategy]


@dataclass(frozen=True)
class MarketDataConfig:
    platform: str | MarketPlatform
    data_type: str | MarketDataType
    vendor: str | MarketDataVendor
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "platform", _normalize_name(self.platform))
        object.__setattr__(self, "data_type", _normalize_name(self.data_type))
        object.__setattr__(self, "vendor", _normalize_name(self.vendor))
        object.__setattr__(
            self,
            "sources",
            tuple(source.strip() for source in self.sources if source.strip()),
        )


def _normalize_name(
    value: str | MarketPlatform | MarketDataType | MarketDataVendor,
) -> str:
    if isinstance(value, str):
        return value.strip().casefold()
    return value.name.strip().casefold()


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
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    start_time: pd.Timestamp | datetime | str | None = None,
    end_time: pd.Timestamp | datetime | str | None = None,
    execution: ExecutionModelConfig | None = None,
) -> dict[str, Any] | None:
    if (
        data.platform == "kalshi"
        and data.data_type == "trade_tick"
        and data.vendor == "native"
    ):
        if market_ticker is None:
            raise ValueError("market_ticker is required for Kalshi trade-tick runs.")
        if lookback_days is None:
            raise ValueError("lookback_days is required for Kalshi trade-tick runs.")
        return await run_single_market_kalshi_trade_backtest(
            name=name,
            market_ticker=market_ticker,
            lookback_days=lookback_days,
            strategy_factory=strategy_factory,
            strategy_configs=strategy_configs,
            probability_window=probability_window,
            min_trades=min_trades,
            min_price_range=min_price_range,
            initial_cash=initial_cash,
            chart_resample_rule=chart_resample_rule,
            emit_summary=emit_summary,
            emit_html=emit_html,
            return_chart_layout=return_chart_layout,
            end_time=end_time,
            data_sources=data.sources,
            execution=execution,
        )

    if (
        data.platform == "polymarket"
        and data.data_type == "trade_tick"
        and data.vendor == "native"
    ):
        if market_slug is None:
            raise ValueError("market_slug is required for Polymarket trade-tick runs.")
        if lookback_days is None:
            raise ValueError(
                "lookback_days is required for Polymarket trade-tick runs."
            )
        return await run_single_market_polymarket_trade_backtest(
            name=name,
            market_slug=market_slug,
            token_index=token_index,
            lookback_days=lookback_days,
            strategy_factory=strategy_factory,
            strategy_configs=strategy_configs,
            probability_window=probability_window,
            min_trades=min_trades,
            min_price_range=min_price_range,
            initial_cash=initial_cash,
            chart_resample_rule=chart_resample_rule,
            emit_summary=emit_summary,
            emit_html=emit_html,
            return_chart_layout=return_chart_layout,
            return_summary_series=return_summary_series,
            end_time=end_time,
            data_sources=data.sources,
            execution=execution,
        )

    if (
        data.platform == "polymarket"
        and data.data_type == "quote_tick"
        and data.vendor == "pmxt"
    ):
        if market_slug is None:
            raise ValueError("market_slug is required for Polymarket quote-tick runs.")
        return await run_single_market_pmxt_backtest(
            name=name,
            market_slug=market_slug,
            token_index=token_index,
            lookback_hours=lookback_hours,
            strategy_factory=strategy_factory,
            strategy_configs=strategy_configs,
            probability_window=probability_window,
            min_quotes=min_quotes,
            min_price_range=min_price_range,
            initial_cash=initial_cash,
            chart_resample_rule=chart_resample_rule,
            emit_summary=emit_summary,
            emit_html=emit_html,
            return_chart_layout=return_chart_layout,
            return_summary_series=return_summary_series,
            start_time=start_time,
            end_time=end_time,
            data_sources=data.sources,
            execution=execution,
        )

    raise NotImplementedError(
        "Unsupported backtest data selection: "
        f"platform={data.platform!r}, data_type={data.data_type!r}, vendor={data.vendor!r}."
    )
