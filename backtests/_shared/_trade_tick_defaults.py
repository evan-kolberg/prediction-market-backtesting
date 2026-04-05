# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Defaults shared by public trade-tick backtest runners.
"""

from backtests._shared._polymarket_quote_tick_defaults import DEFAULT_INITIAL_CASH
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_POLYMARKET_MARKET_SLUG,
)


DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS = 30
DEFAULT_FIXED_TRADE_TICK_SPORTS_LOOKBACK_DAYS = 7
DEFAULT_KALSHI_MARKET_TICKER = "KXNEXTIRANLEADER-45JAN01-MKHA"
DEFAULT_KALSHI_NATIVE_DATA_SOURCES = ("https://api.elections.kalshi.com/trade-api/v2",)
DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES = (
    "gamma=https://gamma-api.polymarket.com",
    "trades=https://data-api.polymarket.com",
    "clob=https://clob.polymarket.com",
)

__all__ = [
    "DEFAULT_FIXED_TRADE_TICK_SPORTS_LOOKBACK_DAYS",
    "DEFAULT_INITIAL_CASH",
    "DEFAULT_KALSHI_MARKET_TICKER",
    "DEFAULT_KALSHI_NATIVE_DATA_SOURCES",
    "DEFAULT_POLYMARKET_MARKET_SLUG",
    "DEFAULT_POLYMARKET_NATIVE_DATA_SOURCES",
    "DEFAULT_SINGLE_MARKET_TRADE_TICK_LOOKBACK_DAYS",
]
