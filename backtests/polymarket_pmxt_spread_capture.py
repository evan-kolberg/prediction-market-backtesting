"""
Mean-reversion spread capture on one Polymarket market using PMXT historical L2 data.
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

from strategies import QuoteTickMeanReversionConfig
from strategies import QuoteTickMeanReversionStrategy


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


NAME = "polymarket_pmxt_spread_capture"
DESCRIPTION = (
    "Mean-reversion spread capture on a single Polymarket market using PMXT L2 data"
)

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
MIN_QUOTES = int(os.getenv("MIN_QUOTES", "500"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.005"))
END_TIME = os.getenv("END_TIME")

VWAP_WINDOW = int(os.getenv("VWAP_WINDOW", "20"))
ENTRY_THRESHOLD = float(os.getenv("ENTRY_THRESHOLD", "0.0015"))
TAKE_PROFIT = float(os.getenv("TAKE_PROFIT", "0.004"))
STOP_LOSS = float(os.getenv("STOP_LOSS", "0.004"))

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
        probability_window=VWAP_WINDOW,
        end_time=None if not END_TIME else END_TIME,
        strategy_factory=lambda instrument_id: QuoteTickMeanReversionStrategy(
            config=QuoteTickMeanReversionConfig(
                instrument_id=instrument_id,
                trade_size=TRADE_SIZE,
                window=VWAP_WINDOW,
                entry_threshold=ENTRY_THRESHOLD,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
