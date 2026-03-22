# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

"""
EMA crossover momentum on one Polymarket market using PMXT historical L2 data.
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickEMACrossoverStrategy


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest


NAME = "polymarket_pmxt_relay_ema_crossover"
DESCRIPTION = "EMA crossover momentum on a single Polymarket market using PMXT L2 data"
DEFAULT_SAMPLE_MARKET_SLUG = "bitcoin-up-or-down-february-22-8am-et"
DEFAULT_SAMPLE_END_TIME = "2026-02-22T13:00:00Z"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_SAMPLE_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "4"))
TOKEN_INDEX = int(os.getenv("TOKEN_INDEX", "0"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME", DEFAULT_SAMPLE_END_TIME)

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
        token_index=TOKEN_INDEX,
        lookback_hours=LOOKBACK_HOURS,
        min_quotes=MIN_QUOTES,
        min_price_range=MIN_PRICE_RANGE,
        initial_cash=INITIAL_CASH,
        probability_window=SLOW_PERIOD,
        end_time=END_TIME,
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
