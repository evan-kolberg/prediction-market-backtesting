# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-04-03, and 2026-04-04.
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
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_sports_final_period_momentum"

DESCRIPTION = (
    "Late-breakout momentum on a fixed Polymarket sports basket pinned to market close"
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=TradeTick,
    vendor=Native,
    sources=(
        "gamma=https://gamma-api.polymarket.com",
        "trades=https://data-api.polymarket.com",
        "clob=https://clob.polymarket.com",
    ),
)

# Pin each replay window to the market close so the fixed basket stays
# reproducible and under the public trades API offset ceiling.
FIXED_LOOKBACK_DAYS = 7

SIMS = (
    MarketSimConfig(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-26T23:53:59Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1774569239000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-man-city-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:17Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797297000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-chelsea-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:22:09Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773796929000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-newcastle-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T22:56:01Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773874561000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:15Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797295000000000,
        },
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickFinalPeriodMomentumStrategy",
        "config_path": "strategies:TradeTickFinalPeriodMomentumConfig",
        "config": {
            "trade_size": Decimal("25"),
            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
            "final_period_minutes": 180,
            "entry_price": 0.8,
            "take_profit_price": 0.92,
            "stop_loss_price": 0.5,
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
    initial_cash=100.0,
    probability_window=180,
    min_trades=25,
    min_price_range=0.01,
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
