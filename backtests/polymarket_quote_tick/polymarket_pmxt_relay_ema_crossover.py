# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
EMA crossover momentum on one Polymarket market using PMXT historical L2 data.
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
from decimal import Decimal

try:
    from ._script_helpers import ensure_repo_root
except ImportError:
    from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickEMACrossoverStrategy


try:
    from ._polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ImportError:
    from backtests.polymarket_quote_tick._polymarket_single_market_pmxt_runner import (
        run_single_market_pmxt_backtest,
    )


NAME = "polymarket_pmxt_relay_ema_crossover"
DESCRIPTION = "EMA crossover momentum on a single Polymarket market using PMXT L2 data"


async def run() -> None:
    await run_single_market_pmxt_backtest(
        name=NAME,
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-19T07:35:57.277659Z",
        end_time="2026-03-24T07:35:57.277659Z",
        min_quotes=500,
        min_price_range=0.005,
        initial_cash=100.0,
        probability_window=256,
        strategy_factory=lambda instrument_id: QuoteTickEMACrossoverStrategy(
            config=QuoteTickEMACrossoverConfig(
                instrument_id=instrument_id,
                trade_size=Decimal("100"),
                fast_period=64,
                slow_period=256,
                entry_buffer=0.0005,
                take_profit=0.010,
                stop_loss=0.010,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
