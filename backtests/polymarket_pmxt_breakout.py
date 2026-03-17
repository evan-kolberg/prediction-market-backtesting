# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

"""
Breakout strategy on one Polymarket market using PMXT historical L2 data.
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

from strategies import QuoteTickBreakoutConfig
from strategies import QuoteTickBreakoutStrategy


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


NAME = "polymarket_pmxt_breakout"
DESCRIPTION = "Volatility breakout on a single Polymarket market using PMXT L2 data"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME")

WINDOW = int(os.getenv("WINDOW", "120"))
BREAKOUT_STD = float(os.getenv("BREAKOUT_STD", "1.5"))
BREAKOUT_BUFFER = float(os.getenv("BREAKOUT_BUFFER", "0.001"))
MEAN_REVERSION_BUFFER = float(os.getenv("MEAN_REVERSION_BUFFER", "0.0005"))
MIN_HOLDING_PERIODS = int(os.getenv("MIN_HOLDING_PERIODS", "20"))
REENTRY_COOLDOWN = int(os.getenv("REENTRY_COOLDOWN", "80"))
MAX_ENTRY_PRICE = float(os.getenv("MAX_ENTRY_PRICE", "0.92"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.015"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.020"))

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
        probability_window=WINDOW,
        end_time=None if not END_TIME else END_TIME,
        strategy_factory=lambda instrument_id: QuoteTickBreakoutStrategy(
            config=QuoteTickBreakoutConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                window=WINDOW,
                breakout_std=BREAKOUT_STD,
                breakout_buffer=BREAKOUT_BUFFER,
                mean_reversion_buffer=MEAN_REVERSION_BUFFER,
                min_holding_periods=MIN_HOLDING_PERIODS,
                reentry_cooldown=REENTRY_COOLDOWN,
                max_entry_price=MAX_ENTRY_PRICE,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
