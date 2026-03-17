# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Panic-fade strategy on one Polymarket market.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

from nautilus_trader.examples.strategies.prediction_market import TradeTickPanicFadeConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickPanicFadeStrategy


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_runner import run_single_market_trade_backtest
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_runner import run_single_market_trade_backtest


NAME = "polymarket_panic_fade"
DESCRIPTION = "Panic selloff fade strategy on a single Polymarket market"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
MIN_TRADES = int(os.getenv("MIN_TRADES", "300"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))

DROP_WINDOW = int(os.getenv("DROP_WINDOW", "30"))
MIN_DROP = float(os.getenv("MIN_DROP", "0.002"))
PANIC_PRICE = float(os.getenv("PANIC_PRICE", "0.249"))
REBOUND_EXIT = float(os.getenv("REBOUND_EXIT", "0.251"))
MAX_HOLDING_PERIODS = int(os.getenv("MAX_HOLDING_PERIODS", "80"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.004"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.004"))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "100"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", str(DEFAULT_INITIAL_CASH)))


async def run() -> None:
    await run_single_market_trade_backtest(
        name=NAME,
        market_slug=MARKET_SLUG,
        lookback_days=LOOKBACK_DAYS,
        min_trades=MIN_TRADES,
        min_price_range=MIN_PRICE_RANGE,
        initial_cash=INITIAL_CASH,
        probability_window=DROP_WINDOW,
        strategy_factory=lambda instrument_id: TradeTickPanicFadeStrategy(
            config=TradeTickPanicFadeConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                drop_window=DROP_WINDOW,
                min_drop=MIN_DROP,
                panic_price=PANIC_PRICE,
                rebound_exit=REBOUND_EXIT,
                max_holding_periods=MAX_HOLDING_PERIODS,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
