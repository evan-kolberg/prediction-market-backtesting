# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Trade-tick mean-reversion (spread capture) on one Kalshi market.

Defaults to KXNEXTIRANLEADER-45JAN01-MKHA
and replays the 30 days ending at 2026-03-08T21:44:24Z.
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


# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "kalshi_trade_tick_spread_capture"

DESCRIPTION = (
    "Mean-reversion spread capture on a single Kalshi market using trade ticks"
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
        end_time="2026-03-08T21:44:24Z",
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickMeanReversionStrategy",
        "config_path": "strategies:TradeTickMeanReversionConfig",
        "config": {
            "trade_size": Decimal("1"),
            "vwap_window": 20,
            "entry_threshold": 0.01,
            "take_profit": 0.01,
            "stop_loss": 0.03,
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
    probability_window=20,
    min_trades=200,
    min_price_range=0.03,
    report=REPORT,
    empty_message="No Kalshi spread-capture sims met the trade-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
