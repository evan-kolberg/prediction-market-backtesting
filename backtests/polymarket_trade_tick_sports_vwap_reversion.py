# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-16, 2026-04-03, 2026-04-04, and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP reversion on a fixed Polymarket sports basket using native trade ticks.
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


NAME = "polymarket_trade_tick_sports_vwap_reversion"

DESCRIPTION = (
    "VWAP reversion on a fixed Polymarket sports basket pinned to market close"
)

EMIT_HTML = True
CHART_OUTPUT_PATH = "output"
SUMMARY_REPORT_PATH = f"output/{NAME}_multi_market.html"

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

# Pin each replay window to the market close so the fixed basket stays
# reproducible and under the public trades API offset ceiling.
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
        "strategy_path": "strategies:TradeTickVWAPReversionStrategy",
        "config_path": "strategies:TradeTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
            "vwap_window": 80,
            "entry_threshold": 0.02,
            "exit_threshold": 0.004,
            "min_tick_size": 10.0,
            "take_profit": 0.03,
            "stop_loss": 0.02,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USDC)",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
)

EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=80,
    min_trades=25,
    min_price_range=0.01,
    report=REPORT,
    empty_message="No fixed Polymarket sports sims met the trade-tick requirements.",
    partial_message="Completed {completed} of {total} fixed sports sims.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
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
