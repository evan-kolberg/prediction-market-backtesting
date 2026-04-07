# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
EMA-crossover momentum on one Polymarket market.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._experiments import build_replay_experiment
from backtests._shared._experiments import run_experiment
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import PolymarketTradeTickReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Native, Polymarket, TradeTick


NAME = "polymarket_trade_tick_ema_crossover"

DESCRIPTION = "EMA crossover momentum on a single Polymarket market"

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
    data_type=TradeTick,
    vendor=Native,
    sources=(
        "gamma:https://gamma-api.polymarket.com",
        "trades:https://data-api.polymarket.com",
        "clob:https://clob.polymarket.com",
    ),
)

REPLAYS = (
    PolymarketTradeTickReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        lookback_days=30,
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickEMACrossoverStrategy",
        "config_path": "strategies:TradeTickEMACrossoverConfig",
        "config": {
            "trade_size": Decimal("100"),
            "fast_period": 8,
            "slow_period": 24,
            "entry_buffer": 0.0003,
            "take_profit": 0.004,
            "stop_loss": 0.004,
        },
    },
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
    probability_window=24,
    min_trades=300,
    min_price_range=0.005,
    report=REPORT,
    empty_message="No Polymarket EMA crossover sims met the trade-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
