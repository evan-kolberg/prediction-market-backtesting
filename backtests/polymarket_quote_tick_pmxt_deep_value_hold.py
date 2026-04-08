# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, 2026-03-31, and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Deep-value hold strategy on one Polymarket market using PMXT historical L2 data.
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
from backtests._shared._experiments import build_replay_experiment
from backtests._shared._experiments import run_experiment
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import PolymarketPMXTQuoteReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_deep_value_hold"

DESCRIPTION = "Buy below a threshold on a single Polymarket market using PMXT L2 data"

EMIT_HTML = True
CHART_OUTPUT_PATH = "output"
DETAIL_PLOT_PANELS = (
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:r2.pmxt.dev",
        "relay:209-209-10-83.sslip.io",
    ),
)

REPLAYS = (
    PolymarketPMXTQuoteReplay(
        market_slug="will-the-tennessee-titans-draft-a-quarterback-in-the-first-round-of-the-2026-nfl-draft",
        token_index=0,
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickDeepValueHoldStrategy",
        "config_path": "strategies:QuoteTickDeepValueHoldConfig",
        "config": {
            "trade_size": Decimal("5"),
            "entry_price_max": 0.247,
            "single_entry": True,
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

EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=10,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
    report=REPORT,
    empty_message="No PMXT deep-value sims met the quote-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
