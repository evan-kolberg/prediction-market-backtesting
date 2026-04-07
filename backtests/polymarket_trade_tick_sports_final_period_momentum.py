# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-04-03, 2026-04-04, and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Final-period momentum on a fixed Polymarket sports basket.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._experiments import build_backtest_for_experiment
from backtests._shared._experiments import build_replay_experiment
from backtests._shared._polymarket_trade_tick_multi_runner import (
    run_reported_multi_market_trade_backtest,
)
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import PolymarketTradeTickReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_sports_final_period_momentum"

DESCRIPTION = (
    "Late-breakout momentum on a fixed Polymarket sports basket pinned to market close"
)

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
SUMMARY_REPORT_PATH = f"output/{NAME}_multi_market.html"
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "equity",
    "periodic_pnl",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=TradeTick,
    vendor=Native,
    sources=(
        "gamma:https://gamma-api.polymarket.com",
        "trades:https://data-api.polymarket.com",
        "clob:https://clob.polymarket.com",
    ),
)

FIXED_LOOKBACK_DAYS = 7

REPLAYS = (
    PolymarketTradeTickReplay(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-26T23:53:59Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1774569239000000000,
        },
    ),
    PolymarketTradeTickReplay(
        market_slug="will-man-city-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:17Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797297000000000,
        },
    ),
    PolymarketTradeTickReplay(
        market_slug="will-chelsea-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:22:09Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773796929000000000,
        },
    ),
    PolymarketTradeTickReplay(
        market_slug="will-newcastle-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T22:56:01Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773874561000000000,
        },
    ),
    PolymarketTradeTickReplay(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:15Z",
        outcome="Yes",
        metadata={
            "market_close_time_ns": 1773797295000000000,
        },
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickFinalPeriodMomentumStrategy",
        "config_path": "strategies:TradeTickFinalPeriodMomentumConfig",
        "config": {
            "trade_size": Decimal("25"),
            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
            "final_period_minutes": 180,
            "entry_price": 0.8,
            "take_profit_price": 0.92,
            "stop_loss_price": 0.5,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USDC)",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=180,
    min_trades=25,
    min_price_range=0.01,
    report=REPORT,
    empty_message="No fixed Polymarket sports sims met the final-period requirements.",
    partial_message="Completed {completed} of {total} fixed sports sims.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
    return_summary_series=True,
)


@timing_harness
def run() -> None:
    run_reported_multi_market_trade_backtest(
        backtest=build_backtest_for_experiment(EXPERIMENT),
        report=REPORT,
        empty_message=EXPERIMENT.empty_message,
        partial_message=EXPERIMENT.partial_message,
    )


if __name__ == "__main__":
    run()
