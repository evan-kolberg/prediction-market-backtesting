"""
EMA crossover momentum on one Polymarket market using PMXT historical L2 data.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
for _path in (_REPO_ROOT, _THIS_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickEMACrossoverStrategy


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ModuleNotFoundError:
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest


NAME = "polymarket_pmxt_ema_crossover"
DESCRIPTION = "EMA crossover momentum on a single Polymarket market using PMXT L2 data"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME")

FAST_PERIOD = int(os.getenv("FAST_PERIOD", "64"))
SLOW_PERIOD = int(os.getenv("SLOW_PERIOD", "256"))
ENTRY_BUFFER = float(os.getenv("ENTRY_BUFFER", "0.0005"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.010"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.010"))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "100"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", str(DEFAULT_INITIAL_CASH)))


async def run() -> None:
    await run_single_market_pmxt_backtest(
        name=NAME,
        market_slug=MARKET_SLUG,
        lookback_hours=LOOKBACK_HOURS,
        min_quotes=MIN_QUOTES,
        min_price_range=MIN_PRICE_RANGE,
        initial_cash=INITIAL_CASH,
        probability_window=SLOW_PERIOD,
        end_time=None if not END_TIME else END_TIME,
        strategy_factory=lambda instrument_id: QuoteTickEMACrossoverStrategy(
            config=QuoteTickEMACrossoverConfig(
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
