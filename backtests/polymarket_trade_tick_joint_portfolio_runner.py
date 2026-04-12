# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-09.
# See the repository NOTICE file for provenance and licensing scope.

"""Joint-portfolio VWAP-reversion backtest on a fixed Polymarket basket using native trade ticks."""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._experiments import build_replay_experiment
from prediction_market_extensions.backtesting._experiments import run_experiment
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import TradeReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import Native, Polymarket, TradeTick


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
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
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

SUMMARY_REPORT_PATH = "output/polymarket_trade_tick_joint_portfolio_runner_joint_portfolio.html"
EMPTY_MESSAGE = (
    "No fixed joint-portfolio Polymarket basket replays met the trade-tick requirements."
)
PARTIAL_MESSAGE = "Completed {completed} of {total} joint-portfolio basket replays."

REPLAYS = (
    TradeReplay(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-26T23:53:59Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1774569239000000000},
    ),
    TradeReplay(
        market_slug="will-man-city-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:17Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797297000000000},
    ),
    TradeReplay(
        market_slug="will-chelsea-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:22:09Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773796929000000000},
    ),
    TradeReplay(
        market_slug="will-newcastle-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T22:56:01Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773874561000000000},
    ),
    TradeReplay(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        lookback_days=FIXED_LOOKBACK_DAYS,
        end_time="2026-03-18T01:28:15Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797295000000000},
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
    }
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
    name="polymarket_trade_tick_joint_portfolio_runner",
    description=(
        "Joint-portfolio VWAP-reversion backtest on a fixed Polymarket basket pinned to market close"
    ),
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=80,
    min_trades=25,
    min_price_range=0.01,
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
