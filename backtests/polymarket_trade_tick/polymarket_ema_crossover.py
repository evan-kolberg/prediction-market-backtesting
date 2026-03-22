# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
EMA-crossover momentum on one Polymarket market.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

from strategies import TradeTickEMACrossoverConfig
from strategies import TradeTickEMACrossoverStrategy


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


NAME = "polymarket_ema_crossover"
DESCRIPTION = "EMA crossover momentum on a single Polymarket market"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
MIN_TRADES = int(os.getenv("MIN_TRADES", "300"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))

FAST_PERIOD = int(os.getenv("FAST_PERIOD", "8"))
SLOW_PERIOD = int(os.getenv("SLOW_PERIOD", "24"))
ENTRY_BUFFER = float(os.getenv("ENTRY_BUFFER", "0.0003"))
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
        probability_window=SLOW_PERIOD,
        strategy_factory=lambda instrument_id: TradeTickEMACrossoverStrategy(
            config=TradeTickEMACrossoverConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                fast_period=FAST_PERIOD,
                slow_period=SLOW_PERIOD,
                entry_buffer=ENTRY_BUFFER,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
