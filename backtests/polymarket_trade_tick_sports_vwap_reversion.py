# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-16, 2026-04-03, and 2026-04-04.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP reversion on a fixed Polymarket sports basket using native trade ticks.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._trade_tick_defaults import (
    DEFAULT_FIXED_TRADE_TICK_SPORTS_LOOKBACK_DAYS,
)
from backtests._shared._trade_tick_defaults import DEFAULT_INITIAL_CASH
from backtests._shared._trade_tick_defaults import (
    DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES,
)
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_sports_vwap_reversion"

DESCRIPTION = (
    "VWAP reversion on a fixed Polymarket sports basket pinned to market close"
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=TradeTick,
    vendor=Native,
    sources=DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES,
)

# Pin each replay window to the market close so the fixed basket stays
# reproducible and under the public trades API offset ceiling.
FIXED_LOOKBACK_DAYS = DEFAULT_FIXED_TRADE_TICK_SPORTS_LOOKBACK_DAYS

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
        "strategy_path": "strategies:TradeTickVWAPReversionStrategy",
        "config_path": "strategies:TradeTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
            "vwap_window": 80,
            "entry_threshold": 0.02,
            "exit_threshold": 0.004,
            "min_tick_size": 10.0,
            "take_profit": 0.03,
            "stop_loss": 0.02,
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
    initial_cash=DEFAULT_INITIAL_CASH,
    probability_window=80,
    min_trades=25,
    min_price_range=0.01,
)


@timing_harness
def run() -> None:
    results = BACKTEST.run()
    if not results:
        print("No fixed Polymarket sports sims met the trade-tick requirements.")
        return

    if len(results) < len(SIMS):
        print(f"Completed {len(results)} of {len(SIMS)} fixed sports sims.")

    finalize_market_results(name=NAME, results=results, report=REPORT)


if __name__ == "__main__":
    run()
