# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Breakout strategy on one Kalshi market.

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


NAME = "kalshi_trade_tick_breakout"
DESCRIPTION = "Volatility breakout strategy on a single Kalshi market using trade ticks"
PLATFORM = "kalshi"
DATA_TYPE = "trade_tick"
VENDOR = NATIVE_VENDOR.name

MARKET_TICKER = "KXNEXTIRANLEADER-45JAN01-MKHA"
LOOKBACK_DAYS = 30
MIN_TRADES = 1000
MIN_PRICE_RANGE = 0.03

WINDOW = 60
BREAKOUT_STD = 1.35
MAX_ENTRY_PRICE = 0.9
TAKE_PROFIT = 0.025
STOP_LOSS = 0.02

TRADE_SIZE = Decimal("1")
INITIAL_CASH = 100.0
DATA = MarketDataConfig(
    platform=PLATFORM,
    data_type=DATA_TYPE,
    vendor=NATIVE_VENDOR,
    sources=(),
)
STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:TradeTickBreakoutStrategy",
        "config_path": "strategies:TradeTickBreakoutConfig",
        "config": {
            "trade_size": TRADE_SIZE,
            "window": WINDOW,
            "breakout_std": BREAKOUT_STD,
            "max_entry_price": MAX_ENTRY_PRICE,
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
    probability_window=WINDOW,
    min_trades=MIN_TRADES,
    min_price_range=MIN_PRICE_RANGE,
)


@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No Kalshi breakout sims met the trade-tick requirements.",
    )


if __name__ == "__main__":
    run()
