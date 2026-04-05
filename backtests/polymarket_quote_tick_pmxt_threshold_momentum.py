# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
Threshold momentum on one Polymarket market using PMXT historical L2 data.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import run_reported_backtest
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_BASE_LATENCY_MS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_CANCEL_LATENCY_MS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_CLOSE_WINDOW_END_TIME,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_CLOSE_WINDOW_START_TIME,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_DATA_SOURCES,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_INSERT_LATENCY_MS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_MARKET_ACTIVATION_START_NS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_QUEUE_POSITION,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_UPDATE_LATENCY_MS,
)
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_threshold_momentum"

DESCRIPTION = (
    "Threshold breakout momentum on a single Polymarket market using PMXT L2 data"
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=DEFAULT_PMXT_DATA_SOURCES,
)

SIMS = (
    MarketSimConfig(
        market_slug=DEFAULT_POLYMARKET_MARKET_SLUG,
        token_index=0,
        start_time=DEFAULT_PMXT_CLOSE_WINDOW_START_TIME,
        end_time=DEFAULT_PMXT_CLOSE_WINDOW_END_TIME,
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickThresholdMomentumStrategy",
        "config_path": "strategies:QuoteTickThresholdMomentumConfig",
        "config": {
            "trade_size": Decimal("100"),
            "activation_start_time_ns": DEFAULT_PMXT_MARKET_ACTIVATION_START_NS,
            "market_close_time_ns": DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
            "entry_price": 0.8,
            "take_profit_price": 0.92,
            "stop_loss_price": 0.5,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
)

EXECUTION = ExecutionModelConfig(
    queue_position=DEFAULT_PMXT_QUEUE_POSITION,
    latency_model=StaticLatencyConfig(
        base_latency_ms=DEFAULT_PMXT_BASE_LATENCY_MS,
        insert_latency_ms=DEFAULT_PMXT_INSERT_LATENCY_MS,
        update_latency_ms=DEFAULT_PMXT_UPDATE_LATENCY_MS,
        cancel_latency_ms=DEFAULT_PMXT_CANCEL_LATENCY_MS,
    ),
)

BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=10,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
)


@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No PMXT threshold-momentum sims met the quote-tick requirements.",
    )


if __name__ == "__main__":
    run()
