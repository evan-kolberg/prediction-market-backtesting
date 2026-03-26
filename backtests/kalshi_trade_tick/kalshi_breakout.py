# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Breakout strategy on one Kalshi market.

Defaults to KXNEXTIRANLEADER-45JAN01-MKHA
and uses a 30-day trade-tick lookback.
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

try:
    from ._script_helpers import ensure_repo_root
except ImportError:
    from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from strategies import TradeTickBreakoutConfig
from strategies import TradeTickBreakoutStrategy


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_KALSHI_MARKET_TICKER
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _kalshi_single_market_trade_runner import run_single_market_trade_backtest
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_KALSHI_MARKET_TICKER
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _kalshi_single_market_trade_runner import run_single_market_trade_backtest


NAME = "kalshi_breakout"
DESCRIPTION = "Volatility breakout strategy on a single Kalshi market using trade ticks"

MARKET_TICKER = os.getenv("MARKET_TICKER", DEFAULT_KALSHI_MARKET_TICKER).upper()
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
MIN_TRADES = int(os.getenv("MIN_TRADES", "1000"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.03"))

WINDOW = int(os.getenv("WINDOW", "60"))
BREAKOUT_STD = float(os.getenv("BREAKOUT_STD", "1.35"))
MAX_ENTRY_PRICE = float(os.getenv("MAX_ENTRY_PRICE", "0.90"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.025"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.020"))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "1"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", str(DEFAULT_INITIAL_CASH)))


async def run() -> None:
    await run_single_market_trade_backtest(
        name=NAME,
        market_ticker=MARKET_TICKER,
        lookback_days=LOOKBACK_DAYS,
        min_trades=MIN_TRADES,
        min_price_range=MIN_PRICE_RANGE,
        initial_cash=INITIAL_CASH,
        probability_window=WINDOW,
        strategy_factory=lambda instrument_id: TradeTickBreakoutStrategy(
            config=TradeTickBreakoutConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                window=WINDOW,
                breakout_std=BREAKOUT_STD,
                max_entry_price=MAX_ENTRY_PRICE,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
