# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

"""
Panic-fade strategy on one Polymarket market using PMXT historical L2 data.
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

from strategies import QuoteTickPanicFadeConfig
from strategies import QuoteTickPanicFadeStrategy


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
    from _polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest


NAME = "polymarket_pmxt_relay_panic_fade"
DESCRIPTION = "Panic selloff fade on a single Polymarket market using PMXT L2 data"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
TOKEN_INDEX = int(os.getenv("TOKEN_INDEX", "0"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME")

DROP_WINDOW = int(os.getenv("DROP_WINDOW", "80"))
MIN_DROP = float(os.getenv("MIN_DROP", "0.06"))
PANIC_PRICE = float(os.getenv("PANIC_PRICE", "0.30"))
REBOUND_EXIT = float(os.getenv("REBOUND_EXIT", "0.42"))
MAX_HOLDING_PERIODS = int(os.getenv("MAX_HOLDING_PERIODS", "500"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.040"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.030"))

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
        probability_window=DROP_WINDOW,
        end_time=None if not END_TIME else END_TIME,
        strategy_factory=lambda instrument_id: QuoteTickPanicFadeStrategy(
            config=QuoteTickPanicFadeConfig(
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
