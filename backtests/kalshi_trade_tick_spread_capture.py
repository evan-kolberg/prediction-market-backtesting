# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Trade-tick mean-reversion (spread capture) on one Kalshi market.

Defaults to KXNEXTIRANLEADER-45JAN01-MKHA
and uses a 30-day trade-tick lookback.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import run_reported_backtest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import NATIVE_VENDOR


# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "kalshi_trade_tick_spread_capture"
DESCRIPTION = (
    "Mean-reversion spread capture on a single Kalshi market using trade ticks"
)
PLATFORM = "kalshi"
DATA_TYPE = "trade_tick"
VENDOR = NATIVE_VENDOR.name

# ── Configure here ────────────────────────────────────────────────────────────
MARKET_TICKER = "KXNEXTIRANLEADER-45JAN01-MKHA"
LOOKBACK_DAYS = 30
MIN_TRADES = 1000
MIN_PRICE_RANGE = 0.03

VWAP_WINDOW = 20  # rolling average window
ENTRY_THRESHOLD = 0.01  # enter when close is 1¢ below rolling average (0-1 scale)
TAKE_PROFIT = 0.01  # exit when price recovers 1¢ above fill price
STOP_LOSS = 0.03  # stop out 3¢ below fill price
TRADE_SIZE = Decimal(1)
INITIAL_CASH = 100.0
DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=NATIVE_VENDOR,
    sources=(),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickMeanReversionStrategy",
        "config_path": "strategies:TradeTickMeanReversionConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "vwap_window": VWAP_WINDOW,
            "entry_threshold": ENTRY_THRESHOLD,
            "take_profit": TAKE_PROFIT,
            "stop_loss": STOP_LOSS,
        },
    },
]
SIMS = (MarketSimConfig(market_ticker=MARKET_TICKER, lookback_days=LOOKBACK_DAYS),)
REPORT = MarketReportConfig(
    count_key="trades", count_label="Trades", pnl_label="PnL (USD)"
)
BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=INITIAL_CASH,
    probability_window=VWAP_WINDOW,
    min_trades=MIN_TRADES,
    min_price_range=MIN_PRICE_RANGE,
)
# ─────────────────────────────────────────────────────────────────────────────


@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No Kalshi spread-capture sims met the trade-tick requirements.",
    )


if __name__ == "__main__":
    run()
