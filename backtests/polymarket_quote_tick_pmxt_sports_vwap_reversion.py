# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-29, 2026-03-31, 2026-04-03, and 2026-04-04.
# See the repository NOTICE file for provenance and licensing scope.

"""
Example PMXT quote-tick runner using multiple fixed historical sims.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._polymarket_quote_tick_pmxt_multi_runner import (
    run_reported_multi_sim_pmxt_backtest,
)
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_sports_vwap_reversion"

DESCRIPTION = "Example PMXT quote-tick runner showing multiple fixed historical sims"

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "/Volumes/LaCie/pmxt_raws",
        "r2.pmxt.dev",
        "209-209-10-83.sslip.io",
    ),
)

SIMS = (
    MarketSimConfig(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        metadata={"sim_label": "sample-a-full-window"},
    ),
    MarketSimConfig(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-02-22T10:00:00Z",
        end_time="2026-02-22T22:00:00Z",
        metadata={"sim_label": "sample-b-2026-02-22-day"},
    ),
    MarketSimConfig(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-02-22T22:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        metadata={"sim_label": "sample-c-2026-02-22-late"},
    ),
    MarketSimConfig(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
        metadata={"sim_label": "sample-d-close-window"},
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickVWAPReversionStrategy",
        "config_path": "strategies:QuoteTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
            "vwap_window": 30,
            "entry_threshold": 0.0015,
            "exit_threshold": 0.0003,
            "min_tick_size": 0.0,
            "take_profit": 0.004,
            "stop_loss": 0.004,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
    market_key="sim_label",
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
    probability_window=30,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
)


@timing_harness
def run() -> None:
    run_reported_multi_sim_pmxt_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No PMXT multi-sim example windows met the quote-tick requirements.",
        partial_message="Completed {completed} of {total} fixed example sims.",
    )


if __name__ == "__main__":
    run()
