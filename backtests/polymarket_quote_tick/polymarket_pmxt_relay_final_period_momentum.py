# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
Final-period momentum on one Polymarket market using PMXT historical L2 data.
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

from strategies import QuoteTickFinalPeriodMomentumConfig
from strategies import QuoteTickFinalPeriodMomentumStrategy


try:
    from ._polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ImportError:
    from backtests.polymarket_quote_tick._polymarket_single_market_pmxt_runner import (
        run_single_market_pmxt_backtest,
    )


NAME = "polymarket_pmxt_relay_final_period_momentum"
DESCRIPTION = "Late breakout momentum on a single Polymarket market using PMXT L2 data"


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
        probability_window=180,
        strategy_factory=lambda instrument_id: QuoteTickFinalPeriodMomentumStrategy(
            config=QuoteTickFinalPeriodMomentumConfig(
                instrument_id=instrument_id,
                trade_size=Decimal("100"),
                market_close_time_ns=1774337757277659000,
                final_period_minutes=180,
                entry_price=0.80,
                take_profit_price=0.92,
                stop_loss_price=0.50,
            ),
        ),
    )


if __name__ == "__main__":
    asyncio.run(run())
