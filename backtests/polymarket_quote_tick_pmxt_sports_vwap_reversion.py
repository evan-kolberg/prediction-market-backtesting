# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-29, 2026-03-31, and 2026-04-03.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP reversion on a fixed Polymarket sports basket using PMXT quote ticks.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT_VENDOR


NAME = "polymarket_quote_tick_pmxt_sports_vwap_reversion"
DESCRIPTION = "VWAP reversion on a fixed Polymarket sports basket using PMXT L2 data"
PLATFORM = "polymarket"
DATA_TYPE = "quote_tick"
VENDOR = PMXT_VENDOR.name

MIN_QUOTES = 500
MIN_PRICE_RANGE = 0.005
VWAP_WINDOW = 30
ENTRY_THRESHOLD = 0.0015
EXIT_THRESHOLD = 0.0003
MIN_TICK_SIZE = 0.0
TAKE_PROFIT = 0.004
STOP_LOSS = 0.004
TRADE_SIZE = Decimal("100")
INITIAL_CASH = 100.0
NAUTILUS_LOG_LEVEL = "INFO"

DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=PMXT_VENDOR,
    sources=("/Volumes/LaCie/pmxt_raws",),
)
SIMS = (
    MarketSimConfig(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        token_index=0,
        outcome="Yes",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
    ),
    MarketSimConfig(
        market_slug="will-man-city-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
    ),
    MarketSimConfig(
        market_slug="will-chelsea-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
    ),
    MarketSimConfig(
        market_slug="will-newcastle-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
    ),
    MarketSimConfig(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
    ),
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
REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
)
BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=INITIAL_CASH,
    probability_window=VWAP_WINDOW,
    min_quotes=MIN_QUOTES,
    min_price_range=MIN_PRICE_RANGE,
    nautilus_log_level=NAUTILUS_LOG_LEVEL,
)


@timing_harness
def run() -> None:
    results = BACKTEST.run()
    if not results:
        print("No fixed Polymarket PMXT sports sims met the quote-tick requirements.")
        return

    if len(results) < len(SIMS):
        print(f"Completed {len(results)} of {len(SIMS)} fixed sports sims.")

    finalize_market_results(name=NAME, results=results, report=REPORT)


if __name__ == "__main__":
    run()
