# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
RSI-reversion strategy on one Polymarket market.
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
from backtests._shared._prediction_market_backtest import run_reported_backtest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._trade_tick_defaults import DEFAULT_INITIAL_CASH
from backtests._shared._trade_tick_defaults import DEFAULT_POLYMARKET_MARKET_SLUG
from backtests._shared._trade_tick_defaults import (
    DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES,
)
from backtests._shared._trade_tick_defaults import (
    DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS,
)
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_rsi_reversion"

DESCRIPTION = "RSI pullback mean-reversion on a single Polymarket market"

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=TradeTick,
    vendor=Native,
    sources=DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES,
)

SIMS = (
    MarketSimConfig(
        market_slug=DEFAULT_POLYMARKET_MARKET_SLUG,
        lookback_days=DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS,
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickRSIReversionStrategy",
        "config_path": "strategies:TradeTickRSIReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
            "period": 20,
            "entry_rsi": 45.0,
            "exit_rsi": 53.0,
            "take_profit": 0.004,
            "stop_loss": 0.004,
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
    probability_window=20,
    min_trades=300,
    min_price_range=0.005,
)


@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No Polymarket RSI-reversion sims met the trade-tick requirements.",
    )


if __name__ == "__main__":
    run()
