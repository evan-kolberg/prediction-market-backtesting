"""
Bar-based mean-reversion (spread capture) on one Kalshi market.

Defaults to KXNEXTIRANLEADER-45JAN01-MKHA
and uses a 30-day minute-bar lookback.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path

from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    BarMeanReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    BarMeanReversionStrategy as BarMeanReversion,
)


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_KALSHI_MARKET_TICKER
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _kalshi_single_market_runner import run_single_market_bar_backtest
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_KALSHI_MARKET_TICKER
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _kalshi_single_market_runner import run_single_market_bar_backtest


# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "kalshi_spread_capture"
DESCRIPTION = "Mean-reversion spread capture on a single Kalshi market"

# ── Configure here ────────────────────────────────────────────────────────────
MARKET_TICKER = os.getenv("MARKET_TICKER", DEFAULT_KALSHI_MARKET_TICKER).upper()
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
BAR_INTERVAL = os.getenv("BAR_INTERVAL", "Minutes1")
MIN_BARS = int(os.getenv("MIN_BARS", "1000"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.03"))

WINDOW = 20  # rolling average window
ENTRY_THRESHOLD = 0.01  # enter when close is 1¢ below rolling average (0-1 scale)
TAKE_PROFIT = 0.01  # exit when price recovers 1¢ above fill price
STOP_LOSS = 0.03  # stop out 3¢ below fill price
TRADE_SIZE = Decimal(1)
INITIAL_CASH = float(os.getenv("INITIAL_CASH", str(DEFAULT_INITIAL_CASH)))
# ─────────────────────────────────────────────────────────────────────────────


async def run() -> None:
    await run_single_market_bar_backtest(
        name=NAME,
        market_ticker=MARKET_TICKER,
        lookback_days=LOOKBACK_DAYS,
        interval=BAR_INTERVAL,
        min_bars=MIN_BARS,
        min_price_range=MIN_PRICE_RANGE,
        initial_cash=INITIAL_CASH,
        probability_window=WINDOW,
        strategy_factory=lambda instrument_id, bar_type: BarMeanReversion(
            config=BarMeanReversionConfig(
                instrument_id=instrument_id,
                bar_type=bar_type,
                trade_size=TRADE_SIZE,
                window=WINDOW,
                entry_threshold=ENTRY_THRESHOLD,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
