# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-16, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP-reversion strategy on one Polymarket market using PMXT historical L2 data.
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


NAME = "polymarket_quote_tick_pmxt_vwap_reversion"
DESCRIPTION = (
    "VWAP dislocation mean-reversion on a single Polymarket market using PMXT L2 data"
)
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
PROBABILITY_WINDOW = 30

TRADE_SIZE = Decimal("100")
VWAP_WINDOW = 30
ENTRY_THRESHOLD = 0.0015
EXIT_THRESHOLD = 0.0003
MIN_TICK_SIZE = 0.0
TAKE_PROFIT = 0.004
STOP_LOSS = 0.004
DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=PMXT_VENDOR,
    sources=("/Volumes/LaCie/pmxt_raws",),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickVWAPReversionStrategy",
        "config_path": "strategies:QuoteTickVWAPReversionConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "vwap_window": VWAP_WINDOW,
            "entry_threshold": ENTRY_THRESHOLD,
            "exit_threshold": EXIT_THRESHOLD,
            "min_tick_size": MIN_TICK_SIZE,
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
        empty_message="No PMXT VWAP-reversion sims met the quote-tick requirements.",
    )


if __name__ == "__main__":
    run()
