# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-03.
# See the repository NOTICE file for provenance and licensing scope.

"""
Late-favorite limit holds on a fixed Polymarket sports basket.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from nautilus_trader.adapters.prediction_market.backtest_utils import (
    compute_binary_settlement_pnl,
)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_sports_late_favorite_limit_hold"

DESCRIPTION = "Late-favorite limit holds on a fixed Polymarket sports basket"

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

SIMS = (
    MarketSimConfig(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1774569239000000000,
            "activation_start_time_ns": 1774558439000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-man-city-win-the-202526-champions-league",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797297000000000,
            "activation_start_time_ns": 1773786497000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-chelsea-win-the-202526-champions-league",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773796929000000000,
            "activation_start_time_ns": 1773786129000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-newcastle-win-the-202526-champions-league",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773874561000000000,
            "activation_start_time_ns": 1773863761000000000,
        },
    ),
    MarketSimConfig(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797295000000000,
            "activation_start_time_ns": 1773786495000000000,
        },
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickLateFavoriteLimitHoldStrategy",
        "config_path": "strategies:TradeTickLateFavoriteLimitHoldConfig",
        "config": {
            "trade_size": Decimal("25"),
            "activation_start_time_ns": "__SIM_METADATA__:activation_start_time_ns",
            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
            "entry_price": 0.9,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="Settlement PnL (USDC)",
)

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=180,
    min_trades=25,
    min_price_range=0.05,
    default_lookback_days=30,
    execution=EXECUTION,
)


@timing_harness
def run() -> None:
    results = BACKTEST.run()
    if not results:
        print("No fixed Polymarket sports sims met the late-favorite requirements.")
        return

    for result in results:
        settlement_pnl = compute_binary_settlement_pnl(
            result.get("fill_events", []),
            result.get("realized_outcome"),
        )
        if settlement_pnl is None:
            continue
        result["market_exit_pnl"] = float(result["pnl"])
        result["pnl"] = float(settlement_pnl)

    if len(results) < len(SIMS):
        print(f"Completed {len(results)} of {len(SIMS)} fixed sports sims.")

    finalize_market_results(name=NAME, results=results, report=REPORT)


if __name__ == "__main__":
    run()
