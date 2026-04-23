# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-09.
# See the repository NOTICE file for provenance and licensing scope.

"""Joint-portfolio breakout backtest on a fixed Kalshi basket using native trade ticks."""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._experiments import (
    build_replay_experiment,
    run_experiment,
)
from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
    StaticLatencyConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import TradeReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import Kalshi, Native, TradeTick

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
SUMMARY_REPORT_PATH = "output/kalshi_trade_tick_joint_portfolio_runner_joint_portfolio.html"
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)

EMPTY_MESSAGE = "No Kalshi basket sims met the trade-tick requirements."
PARTIAL_MESSAGE = "Completed {completed} of {total} joint-portfolio Kalshi replays."

DATA = MarketDataConfig(
    platform=Kalshi,
    data_type=TradeTick,
    vendor=Native,
    sources=("rest:https://api.elections.kalshi.com/trade-api/v2",),
)

REPLAYS = (
    TradeReplay(
        market_ticker="KXLAYOFFSYINFO-26-494000",
        start_time="2026-03-15T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "layoffs-infotech-window"},
    ),
    TradeReplay(
        market_ticker="KXCITRINI-28JUL01",
        start_time="2026-03-18T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "citrini-jul-window"},
    ),
    TradeReplay(
        market_ticker="KXPRESNOMR-28-MR",
        start_time="2026-03-24T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "presnomr-window"},
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickBreakoutStrategy",
        "config_path": "strategies:TradeTickBreakoutConfig",
        "config": {
            "trade_size": Decimal(1),
            "window": 60,
            "breakout_std": 1.35,
            "max_entry_price": 0.9,
            "take_profit": 0.025,
            "stop_loss": 0.02,
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
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USD)",
    market_key="sim_label",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    name="kalshi_trade_tick_joint_portfolio_runner",
    description="Joint-portfolio breakout backtest on a fixed Kalshi basket using trade ticks",
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=60,
    min_trades=200,
    min_price_range=0.03,
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
