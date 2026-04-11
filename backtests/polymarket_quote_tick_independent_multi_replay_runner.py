# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-29, 2026-03-31, 2026-04-03, 2026-04-04, and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""Independent PMXT quote-tick backtests using fixed historical replays."""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._experiments import build_replay_experiment
from prediction_market_extensions.backtesting._experiments import run_experiment
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_independent_multi_replay_runner"

DESCRIPTION = "Independent PMXT quote-tick backtests using varied historical replays"

EMIT_HTML = False
CHART_OUTPUT_PATH = "output"
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
SUMMARY_REPORT_PATH = f"output/{NAME}_independent_aggregate.html"
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)
EMPTY_MESSAGE = "No PMXT independent-replay example windows met the quote-tick requirements."
PARTIAL_MESSAGE = "Completed {completed} of {total} independent example replays."

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
    QuoteReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-23T00:00:00Z",
        end_time="2026-03-24T23:59:59Z",
        metadata={"sim_label": "openai-launch-mar-23-24"},
    ),
    QuoteReplay(
        market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
        token_index=0,
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "aberg-masters-full-window"},
    ),
    QuoteReplay(
        market_slug="will-the-tennessee-titans-draft-a-quarterback-in-the-first-round-of-the-2026-nfl-draft",
        token_index=0,
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "titans-draft-two-day-window"},
    ),
    QuoteReplay(
        market_slug="will-fc-heidenheim-be-relegated-from-the-bundesliga-after-the-202526-season-382",
        token_index=0,
        start_time="2026-04-07T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "heidenheim-late-session"},
    ),
    QuoteReplay(
        market_slug="will-the-south-african-reserve-bank-decrease-the-repo-rate-after-the-may-meeting",
        token_index=0,
        start_time="2026-04-06T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "sarb-rate-watch-window"},
    ),
    QuoteReplay(
        market_slug="will-nana-araba-wilmot-win-top-chef-season-23",
        token_index=0,
        start_time="2026-04-06T06:00:00Z",
        end_time="2026-04-07T18:00:00Z",
        metadata={"sim_label": "top-chef-finale-runup"},
    ),
    QuoteReplay(
        market_slug="will-drake-release-an-album-in-2026",
        token_index=0,
        start_time="2026-04-05T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "drake-weekend-window"},
    ),
    QuoteReplay(
        market_slug="will-ethan-agarwal-get-the-first-or-second-most-votes-in-the-2026-california-governor-primary-election",
        token_index=0,
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
        metadata={"sim_label": "agarwal-election-day"},
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickVWAPReversionStrategy",
        "config_path": "strategies:QuoteTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("5"),
            "vwap_window": 30,
            "entry_threshold": 0.0015,
            "exit_threshold": 0.0003,
            "min_tick_size": 0.0,
            "take_profit": 0.004,
            "stop_loss": 0.004,
        },
    }
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
    market_key="sim_label",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
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
    probability_window=30,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
    report=REPORT,
    empty_message=EMPTY_MESSAGE,
    partial_message=PARTIAL_MESSAGE,
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
    return_summary_series=True,
    multi_replay_mode="independent",
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
