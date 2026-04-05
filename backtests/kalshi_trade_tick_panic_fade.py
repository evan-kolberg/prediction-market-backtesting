# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Panic-fade strategy on one Kalshi market.

Defaults to KXNEXTIRANLEADER-45JAN01-MKHA
and uses a 30-day trade-tick lookback.
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
from backtests._shared._replay_specs import KalshiTradeTickReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import Kalshi, Native, TradeTick


NAME = "kalshi_trade_tick_panic_fade"

DESCRIPTION = "Panic selloff fade strategy on a single Kalshi market using trade ticks"

EMIT_HTML = True
CHART_OUTPUT_PATH = "output"

DATA = MarketDataConfig(
    platform=Kalshi,
    data_type=TradeTick,
    vendor=Native,
    sources=("rest:https://api.elections.kalshi.com/trade-api/v2",),
)

REPLAYS = (
    KalshiTradeTickReplay(
        market_ticker="KXNEXTIRANLEADER-45JAN01-MKHA",
        lookback_days=30,
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickPanicFadeStrategy",
        "config_path": "strategies:TradeTickPanicFadeConfig",
        "config": {
            "trade_size": Decimal("1"),
            "drop_window": 50,
            "min_drop": 0.04,
            "panic_price": 0.35,
            "rebound_exit": 0.48,
            "max_holding_periods": 300,
            "take_profit": 0.035,
            "stop_loss": 0.02,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="trades",
    count_label="Trades",
    pnl_label="PnL (USD)",
)

EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=50,
    min_trades=1000,
    min_price_range=0.03,
    report=REPORT,
    empty_message="No Kalshi panic-fade sims met the trade-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
