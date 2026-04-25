# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-09.
# See the repository NOTICE file for provenance and licensing scope.

"""Joint-portfolio PMXT book backtest using fixed historical replays."""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

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
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket

SUMMARY_REPORT_PATH = "output/polymarket_book_joint_portfolio_runner_joint_portfolio.html"
SUMMARY_PLOT_PANELS = (
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
EMPTY_MESSAGE = "No PMXT joint-portfolio example windows met the book requirements."
PARTIAL_MESSAGE = "Completed {completed} of {total} joint-portfolio example replays."

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=Book,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_data",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    ),
)

# PMXT book collection started in late February 2026, so every replay
# uses 2026-03-01 -> 2026-04-11 (~6 weeks). Anchor here to time a single
# end-to-end run before scaling notebook trial counts.
_LONG_WINDOW_START = "2026-03-01T00:00:00Z"
_LONG_WINDOW_END = "2026-04-11T23:59:59Z"

REPLAYS = (
    BookReplay(
        market_slug="human-moon-landing-in-2026",
        token_index=0,
        start_time=_LONG_WINDOW_START,
        end_time=_LONG_WINDOW_END,
        metadata={"sim_label": "moon-landing-2026"},
    ),
    BookReplay(
        market_slug="new-coronavirus-pandemic-in-2026",
        token_index=0,
        start_time=_LONG_WINDOW_START,
        end_time=_LONG_WINDOW_END,
        metadata={"sim_label": "coronavirus-pandemic-2026"},
    ),
    BookReplay(
        market_slug="will-openais-market-cap-be-between-750b-and-1t-at-market-close-on-ipo-day",
        token_index=0,
        start_time=_LONG_WINDOW_START,
        end_time=_LONG_WINDOW_END,
        metadata={"sim_label": "openai-ipo-market-cap-750b-1t"},
    ),
    BookReplay(
        market_slug="okx-ipo-in-2026",
        token_index=0,
        start_time=_LONG_WINDOW_START,
        end_time=_LONG_WINDOW_END,
        metadata={"sim_label": "okx-ipo-2026"},
    ),
    BookReplay(
        market_slug="nothing-ever-happens-2026",
        token_index=0,
        start_time=_LONG_WINDOW_START,
        end_time=_LONG_WINDOW_END,
        metadata={"sim_label": "nothing-ever-happens-2026"},
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:BookDeepValueHoldStrategy",
        "config_path": "strategies:BookDeepValueHoldConfig",
        "config": {
            "trade_size": Decimal(5),
            "entry_price_max": 0.15,
            "single_entry": True,
        },
    }
]

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

REPORT = MarketReportConfig(
    count_key="book_events",
    count_label="Book Events",
    pnl_label="PnL (USDC)",
    market_key="sim_label",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    name="polymarket_book_joint_portfolio_runner",
    description="Joint-portfolio PMXT book backtest using varied historical replays",
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=30,
    min_book_events=500,
    min_price_range=0.005,
    execution=EXECUTION,
    report=REPORT,
    empty_message=EMPTY_MESSAGE,
    partial_message=PARTIAL_MESSAGE,
    return_summary_series=True,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
