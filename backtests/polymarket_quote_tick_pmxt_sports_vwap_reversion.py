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

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_sports_vwap_reversion"

DESCRIPTION = "VWAP reversion on a fixed Polymarket sports basket using PMXT L2 data"

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
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        outcome="Yes",
    ),
    MarketSimConfig(
        market_slug="will-man-city-win-the-202526-champions-league",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        outcome="Yes",
    ),
    MarketSimConfig(
        market_slug="will-chelsea-win-the-202526-champions-league",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        outcome="Yes",
    ),
    MarketSimConfig(
        market_slug="will-newcastle-win-the-202526-champions-league",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        outcome="Yes",
    ),
    MarketSimConfig(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        token_index=0,
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-02-23T10:00:00Z",
        outcome="Yes",
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
    results = BACKTEST.run()
    if not results:
        print("No fixed Polymarket PMXT sports sims met the quote-tick requirements.")
        return

    if len(results) < len(SIMS):
        print(f"Completed {len(results)} of {len(SIMS)} fixed sports sims.")

    finalize_market_results(name=NAME, results=results, report=REPORT)


if __name__ == "__main__":
    run()
