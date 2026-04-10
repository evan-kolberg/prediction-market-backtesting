# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP-reversion strategy on one Polymarket market.
"""

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


NAME = "polymarket_trade_tick_vwap_reversion"

DESCRIPTION = "VWAP dislocation mean-reversion on a single Polymarket market"

EMIT_HTML = True
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

REPLAYS = (
    TradeReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-03-31T23:59:59Z",
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickVWAPReversionStrategy",
        "config_path": "strategies:TradeTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
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
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USDC)",
)


EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=30,
    min_trades=300,
    min_price_range=0.005,
    report=REPORT,
    empty_message="No Polymarket VWAP-reversion sims met the trade-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
