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
from backtests._shared._market_data_support import (
    build_single_market_runner_kwargs,
)
from backtests._shared._market_data_support import load_single_market_runner
from backtests._shared._market_data_support import resolve_market_data_support
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources import MarketDataType
from backtests._shared.data_sources import MarketPlatform
from backtests._shared.data_sources import MarketDataVendor


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
    support = resolve_market_data_support(
        platform=data.platform,
        data_type=data.data_type,
        vendor=data.vendor,
    )
    runner_fn = load_single_market_runner(support.single_market_runner)
    runner_kwargs = build_single_market_runner_kwargs(
        spec=support.single_market_runner,
        name=name,
        probability_window=probability_window,
        strategy_factory=strategy_factory,
        strategy_configs=list(strategy_configs)
        if strategy_configs is not None
        else None,
        initial_cash=initial_cash,
        field_values={
            "market_slug": market_slug,
            "market_ticker": market_ticker,
            "token_index": token_index,
            "lookback_days": lookback_days,
            "lookback_hours": lookback_hours,
            "min_trades": min_trades,
            "min_quotes": min_quotes,
            "min_price_range": min_price_range,
            "chart_resample_rule": chart_resample_rule,
            "emit_summary": emit_summary,
            "emit_html": emit_html,
            "return_chart_layout": return_chart_layout,
            "return_summary_series": return_summary_series,
            "start_time": start_time,
            "end_time": end_time,
            "data_sources": data.sources,
            "execution": execution,
        },
    )
    return await runner_fn(**runner_kwargs)
