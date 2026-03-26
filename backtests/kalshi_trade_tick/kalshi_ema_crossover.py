# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
EMA-crossover momentum strategy on one Kalshi market.

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

from strategies import TradeTickEMACrossoverConfig
from strategies import TradeTickEMACrossoverStrategy


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


NAME = "kalshi_ema_crossover"
DESCRIPTION = "EMA crossover momentum on a single Kalshi market using trade ticks"

MARKET_TICKER = os.getenv("MARKET_TICKER", DEFAULT_KALSHI_MARKET_TICKER).upper()
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
MIN_TRADES = int(os.getenv("MIN_TRADES", "1000"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.03"))

FAST_PERIOD = int(os.getenv("FAST_PERIOD", "24"))
SLOW_PERIOD = int(os.getenv("SLOW_PERIOD", "96"))
ENTRY_BUFFER = float(os.getenv("ENTRY_BUFFER", "0.002"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.030"))
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
