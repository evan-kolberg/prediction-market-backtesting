# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

"""
Final-period momentum on one Polymarket market using PMXT historical L2 data.
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

from strategies import QuoteTickFinalPeriodMomentumConfig
from strategies import QuoteTickFinalPeriodMomentumStrategy


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


NAME = "polymarket_pmxt_final_period_momentum"
DESCRIPTION = "Late breakout momentum on a single Polymarket market using PMXT L2 data"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME")

MARKET_CLOSE_TIME_NS = int(os.getenv("MARKET_CLOSE_TIME_NS", "0"))
FINAL_PERIOD_MINUTES = int(os.getenv("FINAL_PERIOD_MINUTES", "180"))
ENTRY_PRICE = float(os.getenv("ENTRY_PRICE", "0.80"))
TAKE_PROFIT_PRICE = float(os.getenv("TAKE_PROFIT_PRICE", "0.92"))
STOP_LOSS_PRICE = float(os.getenv("STOP_LOSS_PRICE", "0.50"))

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
        probability_window=max(FINAL_PERIOD_MINUTES, 10),
        end_time=None if not END_TIME else END_TIME,
        strategy_factory=lambda instrument_id: QuoteTickFinalPeriodMomentumStrategy(
            config=QuoteTickFinalPeriodMomentumConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                market_close_time_ns=MARKET_CLOSE_TIME_NS,
                final_period_minutes=FINAL_PERIOD_MINUTES,
                entry_price=ENTRY_PRICE,
                take_profit_price=TAKE_PROFIT_PRICE,
                stop_loss_price=STOP_LOSS_PRICE,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
