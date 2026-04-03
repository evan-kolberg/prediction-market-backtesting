# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
Breakout strategy on one Polymarket market using PMXT historical L2 data.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import run_reported_backtest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT_VENDOR


NAME = "polymarket_quote_tick_pmxt_breakout"
DESCRIPTION = "Volatility breakout on a single Polymarket market using PMXT L2 data"
PLATFORM = "polymarket"
DATA_TYPE = "quote_tick"
VENDOR = PMXT_VENDOR.name

MARKET_SLUG = "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
TOKEN_INDEX = 0
START_TIME = "2026-02-21T16:00:00Z"
END_TIME = "2026-02-23T10:00:00Z"
MIN_QUOTES = 500
MIN_PRICE_RANGE = 0.005
INITIAL_CASH = 100.0
PROBABILITY_WINDOW = 120

TRADE_SIZE = Decimal("100")
WINDOW = 120
BREAKOUT_STD = 1.5
BREAKOUT_BUFFER = 0.001
MEAN_REVERSION_BUFFER = 0.0005
MIN_HOLDING_PERIODS = 20
REENTRY_COOLDOWN = 80
MAX_ENTRY_PRICE = 0.92
TAKE_PROFIT = 0.015
STOP_LOSS = 0.020
DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=PMXT_VENDOR,
    sources=("/Volumes/LaCie/pmxt_raws",),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickBreakoutStrategy",
        "config_path": "strategies:QuoteTickBreakoutConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "window": WINDOW,
            "breakout_std": BREAKOUT_STD,
            "breakout_buffer": BREAKOUT_BUFFER,
            "mean_reversion_buffer": MEAN_REVERSION_BUFFER,
            "min_holding_periods": MIN_HOLDING_PERIODS,
            "reentry_cooldown": REENTRY_COOLDOWN,
            "max_entry_price": MAX_ENTRY_PRICE,
            "take_profit": TAKE_PROFIT,
            "stop_loss": STOP_LOSS,
        },
    },
]
SIMS = (
    MarketSimConfig(
        market_slug=MARKET_SLUG,
        token_index=TOKEN_INDEX,
        start_time=START_TIME,
        end_time=END_TIME,
    ),
)
REPORT = MarketReportConfig(
    count_key="quotes", count_label="Quotes", pnl_label="PnL (USDC)"
)
BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=INITIAL_CASH,
    probability_window=PROBABILITY_WINDOW,
    min_quotes=MIN_QUOTES,
    min_price_range=MIN_PRICE_RANGE,
)


@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No PMXT breakout sims met the quote-tick requirements.",
    )


if __name__ == "__main__":
    run()
