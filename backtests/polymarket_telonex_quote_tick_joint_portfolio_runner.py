# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-19.
# See the repository NOTICE file for provenance and licensing scope.

"""Joint-portfolio Telonex quote-tick backtest using fixed historical replays."""

# ruff: noqa: E402

from __future__ import annotations

import os
from decimal import Decimal

from dotenv import load_dotenv

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)
load_dotenv()

TELONEX_API_KEY = os.environ["TELONEX_API_KEY"]

from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
    StaticLatencyConfig,
)
from prediction_market_extensions.backtesting._experiments import (
    build_replay_experiment,
    run_experiment,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import Polymarket, QuoteTick, Telonex

DETAIL_PLOT_PANELS = (
    "total_equity",
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "total_drawdown",
    "drawdown",
    "total_rolling_sharpe",
    "rolling_sharpe",
    "total_cash_equity",
    "cash_equity",
    "monthly_returns",
    "total_brier_advantage",
    "brier_advantage",
)
SUMMARY_REPORT_PATH = (
    "output/polymarket_telonex_quote_tick_joint_portfolio_runner_joint_portfolio.html"
)
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)
EMPTY_MESSAGE = "No Telonex joint-portfolio example windows met the quote-tick requirements."
PARTIAL_MESSAGE = "Completed {completed} of {total} joint-portfolio Telonex example replays."


DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=Telonex,
    sources=(
        "local:/Volumes/LaCie/telonex_data",
        f"api:{TELONEX_API_KEY}",
    ),
)


REPLAYS = (
    QuoteReplay(
        market_slug="will-tesla-release-optimus-by-june-30-2026",
        token_index=0,
        start_time="2025-10-11T00:00:00Z",
        end_time="2026-04-19T00:00:00Z",
        metadata={"sim_label": "tesla-optimus-june-2026"},
    ),
    QuoteReplay(
        market_slug="will-stripe-not-ipo-by-june-30-2026",
        token_index=0,
        start_time="2025-10-11T00:00:00Z",
        end_time="2026-04-19T00:00:00Z",
        metadata={"sim_label": "stripe-no-ipo-june-2026"},
    ),
    QuoteReplay(
        market_slug="will-trump-pardon-ghislaine-maxwell",
        token_index=0,
        start_time="2025-10-11T00:00:00Z",
        end_time="2026-04-19T00:00:00Z",
        metadata={"sim_label": "trump-pardon-maxwell"},
    ),
    QuoteReplay(
        market_slug="will-kylian-mbapp-win-the-2026-ballon-dor",
        token_index=0,
        start_time="2025-10-11T00:00:00Z",
        end_time="2026-04-19T00:00:00Z",
        metadata={"sim_label": "mbappe-ballon-dor-2026"},
    ),
    QuoteReplay(
        market_slug="will-databricks-market-cap-be-between-100b-and-125b-at-market-close-on-ipo-day",
        token_index=0,
        start_time="2025-10-11T00:00:00Z",
        end_time="2026-04-19T00:00:00Z",
        metadata={"sim_label": "databricks-ipo-100b-125b"},
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickDeepValueHoldStrategy",
        "config_path": "strategies:QuoteTickDeepValueHoldConfig",
        "config": {
            "trade_size": Decimal(5),
            "entry_price_max": 0.15,
            "single_entry": True,
        },
    }
]

EXECUTION = ExecutionModelConfig(
    queue_position=False,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
    market_key="sim_label",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    name="polymarket_telonex_quote_tick_joint_portfolio_runner",
    description="Joint-portfolio Telonex quote-tick backtest using varied historical replays",
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=30,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
    report=REPORT,
    empty_message=EMPTY_MESSAGE,
    partial_message=PARTIAL_MESSAGE,
    emit_html=False,
    chart_output_path="output",
    detail_plot_panels=DETAIL_PLOT_PANELS,
    return_summary_series=True,
    multi_replay_mode="joint_portfolio",
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
