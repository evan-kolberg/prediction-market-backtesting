# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Shared defaults for prediction-market backtest scripts.

Backtest entrypoints should import these constants instead of hardcoding
market IDs in each file.
"""

DEFAULT_KALSHI_MARKET_TICKER = "KXNEXTIRANLEADER-45JAN01-MKHA"
DEFAULT_POLYMARKET_MARKET_SLUG = (
    "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
)
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_INITIAL_CASH = 100.0
