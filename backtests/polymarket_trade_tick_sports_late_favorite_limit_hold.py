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

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import NATIVE_VENDOR


NAME = "polymarket_trade_tick_sports_late_favorite_limit_hold"
DESCRIPTION = "Late-favorite limit holds on a fixed Polymarket sports basket"
PLATFORM = "polymarket"
DATA_TYPE = "trade_tick"
VENDOR = NATIVE_VENDOR.name

LOOKBACK_DAYS = 30
MIN_TRADES = 25
MIN_PRICE_RANGE = 0.05
INITIAL_CASH = 100.0
NAUTILUS_LOG_LEVEL = "INFO"

ACTIVE_WINDOW_HOURS = 3.0
ENTRY_PRICE = 0.9
TRADE_SIZE = Decimal("25")


def _sim_with_close(slug: str, close_time_ns: int) -> MarketSimConfig:
    active_window_ns = int(ACTIVE_WINDOW_HOURS * 3_600_000_000_000)
    return MarketSimConfig(
        market_slug=slug,
        token_index=0,
        outcome="Yes",
        metadata={
            "market_close_time_ns": close_time_ns,
            "activation_start_time_ns": close_time_ns - active_window_ns,
        },
    )


DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=NATIVE_VENDOR,
    sources=(),
)
SIMS = (
    _sim_with_close(
        "will-ukraine-qualify-for-the-2026-fifa-world-cup",
        1774569239000000000,
    ),
    _sim_with_close(
        "will-man-city-win-the-202526-champions-league",
        1773797297000000000,
    ),
    _sim_with_close(
        "will-chelsea-win-the-202526-champions-league",
        1773796929000000000,
    ),
    _sim_with_close(
        "will-newcastle-win-the-202526-champions-league",
        1773874561000000000,
    ),
    _sim_with_close(
        "will-leverkusen-win-the-202526-champions-league",
        1773797295000000000,
    ),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickLateFavoriteLimitHoldStrategy",
        "config_path": "strategies:TradeTickLateFavoriteLimitHoldConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "activation_start_time_ns": "__SIM_METADATA__:activation_start_time_ns",
            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
            "entry_price": ENTRY_PRICE,
        },
    },
]
REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="Settlement PnL (USDC)",
)
BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=INITIAL_CASH,
    probability_window=max(int(ACTIVE_WINDOW_HOURS * 60), 10),
    min_trades=MIN_TRADES,
    min_price_range=MIN_PRICE_RANGE,
    default_lookback_days=LOOKBACK_DAYS,
    nautilus_log_level=NAUTILUS_LOG_LEVEL,
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
