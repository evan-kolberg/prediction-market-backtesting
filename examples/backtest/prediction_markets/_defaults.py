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
