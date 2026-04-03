# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-03.
# See the repository NOTICE file for provenance and licensing scope.

"""
Final-period momentum on a fixed Polymarket sports basket.
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
from backtests._shared.data_sources import NATIVE_VENDOR


NAME = "polymarket_trade_tick_sports_final_period_momentum"
DESCRIPTION = "Late-breakout momentum on a fixed Polymarket sports basket"
PLATFORM = "polymarket"
DATA_TYPE = "trade_tick"
VENDOR = NATIVE_VENDOR.name

LOOKBACK_DAYS = 30
MIN_TRADES = 25
MIN_PRICE_RANGE = 0.05
INITIAL_CASH = 100.0
NAUTILUS_LOG_LEVEL = "INFO"

TRADE_SIZE = Decimal("25")
FINAL_PERIOD_MINUTES = 180
ENTRY_PRICE = 0.8
TAKE_PROFIT_PRICE = 0.92
STOP_LOSS_PRICE = 0.5

DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=NATIVE_VENDOR,
    sources=(),
)
SIMS = (
    MarketSimConfig(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        token_index=0,
        outcome="Yes",
        metadata={"market_close_time_ns": 1774569239000000000},
    ),
    MarketSimConfig(
        market_slug="will-man-city-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797297000000000},
    ),
    MarketSimConfig(
        market_slug="will-chelsea-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        metadata={"market_close_time_ns": 1773796929000000000},
    ),
    MarketSimConfig(
        market_slug="will-newcastle-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        metadata={"market_close_time_ns": 1773874561000000000},
    ),
    MarketSimConfig(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        token_index=0,
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797295000000000},
    ),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickFinalPeriodMomentumStrategy",
        "config_path": "strategies:TradeTickFinalPeriodMomentumConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
            "final_period_minutes": FINAL_PERIOD_MINUTES,
            "entry_price": ENTRY_PRICE,
            "take_profit_price": TAKE_PROFIT_PRICE,
            "stop_loss_price": STOP_LOSS_PRICE,
        },
    },
]
REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USDC)",
)
BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=INITIAL_CASH,
    probability_window=max(FINAL_PERIOD_MINUTES, 10),
    min_trades=MIN_TRADES,
    min_price_range=MIN_PRICE_RANGE,
    default_lookback_days=LOOKBACK_DAYS,
    nautilus_log_level=NAUTILUS_LOG_LEVEL,
)


@timing_harness
def run() -> None:
    results = BACKTEST.run()
    if not results:
        print("No fixed Polymarket sports sims met the final-period requirements.")
        return

    if len(results) < len(SIMS):
        print(f"Completed {len(results)} of {len(SIMS)} fixed sports sims.")

    finalize_market_results(name=NAME, results=results, report=REPORT)


if __name__ == "__main__":
    run()
