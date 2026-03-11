"""
Shared runner for single-market Kalshi trade-tick backtests.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any

import pandas as pd

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
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
    strategy_factory: StrategyFactory,
    probability_window: int,
    min_trades: int = 0,
    min_price_range: float = 0.0,
    initial_cash: float = 100.0,
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    end_time: pd.Timestamp | datetime | None = None,
) -> dict[str, Any] | None:
    end = pd.Timestamp(end_time if end_time is not None else datetime.now(UTC))
    if end.tzinfo is None:
        end = end.tz_localize(UTC)
    start = end - pd.Timedelta(days=lookback_days)

    print(
        f"Loading Kalshi market {market_ticker} "
        f"(trades, lookback={lookback_days}d, window_end={end.isoformat()})..."
    )

    try:
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
    )

    if emit_summary:
        print_backtest_summary(
            results=[result],
            market_key="ticker",
            count_key="trades",
            count_label="Trades",
            pnl_label="PnL (USD)",
        )
        if emit_html and result.get("chart_path"):
            print(f"\nLegacy chart saved to {result['chart_path']}")

    return result
