# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
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

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._trade_tick_ui import build_single_market_trade_summary_row
from backtests._shared._trade_tick_ui import print_single_market_trade_summary
from backtests._shared._strategy_configs import resolve_strategy_factory
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources.kalshi_native import (
    configured_kalshi_native_data_source,
)
from backtests._shared.data_sources.kalshi_native import (
    RunnerKalshiDataLoader as KalshiDataLoader,
)
from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.prediction_market.research import run_market_backtest
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.trading.strategy import Strategy


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
    start = end - pd.Timedelta(days=lookback_days)

    print(
        f"Loading Kalshi market {market_ticker} "
        f"(trades, lookback={lookback_days}d, window_end={end.isoformat()})..."
    )

    try:
        with configured_kalshi_native_data_source(sources=data_sources) as data_source:
            print(data_source.summary)
            loader = await KalshiDataLoader.from_market_ticker(market_ticker)
            trades = await loader.load_trades(start, end)
    except Exception as exc:
        print(f"Unable to load Kalshi market {market_ticker}: {exc}")
        return

    if len(trades) < min_trades:
        print(f"Skip {market_ticker}: {len(trades)} trades < {min_trades} required")
        return

    prices = [float(trade.price) for trade in trades]
    if prices:
        price_range = max(prices) - min(prices)
        if price_range < min_price_range:
            print(
                f"Skip {market_ticker}: price range {price_range:.3f} "
                f"< {min_price_range:.3f}"
            )
            return

    if not trades:
        print(f"No trades returned for {market_ticker}")
        return

    print(f"  running backtest for {market_ticker}...")
    result = run_market_backtest(
        market_id=market_ticker,
        instrument=loader.instrument,
        data=trades,
        strategy=strategy_factory(loader.instrument.id),
        strategy_name=f"{name}:{market_ticker}",
        output_prefix=name,
        platform="kalshi",
        venue=Venue("KALSHI"),
        base_currency=USD,
        fee_model=KalshiProportionalFeeModel(),
        initial_cash=initial_cash,
        probability_window=probability_window,
        price_attr="price",
        count_key="trades",
        chart_resample_rule=chart_resample_rule,
        market_key="ticker",
        emit_html=emit_html,
        return_chart_layout=return_chart_layout,
        queue_position=False if execution is None else execution.queue_position,
        latency_model=None if execution is None else execution.build_latency_model(),
    )

    if emit_summary:
        summary_row = build_single_market_trade_summary_row(
            market_label=market_ticker,
            count=int(result.get("trades", len(trades))),
            fills=int(result["fills"]),
            pnl=float(result["pnl"]),
            prices=prices,
        )
        print_single_market_trade_summary(
            rows=[summary_row],
            count_label="Trades",
            pnl_label="PnL (USD)",
        )
        if emit_html and result.get("chart_path"):
            print(f"\nLegacy chart saved to {result['chart_path']}")

    return result
