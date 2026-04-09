# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-09.
# See the repository NOTICE file for provenance and licensing scope.

"""
Breakout strategy on a fixed Kalshi basket using native trade ticks.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._experiments import (
    build_backtest_for_experiment,
)
from prediction_market_extensions.backtesting._experiments import (
    build_replay_experiment,
)
from prediction_market_extensions.backtesting._kalshi_trade_tick_multi_runner import (
    run_reported_multi_market_trade_backtest,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketReportConfig,
)
from prediction_market_extensions.backtesting._prediction_market_runner import (
    MarketDataConfig,
)
from prediction_market_extensions.backtesting._replay_specs import KalshiTradeTickReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import (
    Kalshi,
    Native,
    TradeTick,
)


NAME = "kalshi_trade_tick_multi_sim_runner"

DESCRIPTION = "Breakout strategy on a fixed Kalshi basket using trade ticks"

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
EMPTY_MESSAGE = "No Kalshi basket sims met the trade-tick requirements."
PARTIAL_MESSAGE = "Completed {completed} of {total} fixed Kalshi sims."

DATA = MarketDataConfig(
    platform=Kalshi,
    data_type=TradeTick,
    vendor=Native,
    sources=("rest:https://api.elections.kalshi.com/trade-api/v2",),
)

REPLAYS = (
    KalshiTradeTickReplay(
        market_ticker="KXLAYOFFSYINFO-26-494000",
        start_time="2026-03-15T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "layoffs-infotech-window"},
    ),
    KalshiTradeTickReplay(
        market_ticker="KXCITRINI-28JUL01",
        start_time="2026-03-18T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "citrini-jul-window"},
    ),
    KalshiTradeTickReplay(
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
            "trade_size": Decimal("1"),
            "window": 60,
            "breakout_std": 1.35,
            "max_entry_price": 0.9,
            "take_profit": 0.025,
            "stop_loss": 0.02,
        },
    },
]

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
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=60,
    min_trades=200,
    min_price_range=0.03,
    report=REPORT,
    empty_message=EMPTY_MESSAGE,
    partial_message=PARTIAL_MESSAGE,
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
        empty_message=EMPTY_MESSAGE,
        partial_message=PARTIAL_MESSAGE,
    )


if __name__ == "__main__":
    run()
