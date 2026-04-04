# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-27 and 2026-04-01.
# See the repository NOTICE file for provenance and licensing scope.

"""
Defaults for Polymarket quote-tick backtests.
"""

DEFAULT_POLYMARKET_MARKET_SLUG = (
    "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
)
DEFAULT_INITIAL_CASH = 100.0
DEFAULT_PMXT_RELAY_SAMPLE_START_TIME = "2026-02-21T16:00:00Z"
DEFAULT_PMXT_RELAY_SAMPLE_END_TIME = "2026-02-23T10:00:00Z"
DEFAULT_PMXT_CLOSE_WINDOW_START_TIME = "2026-03-24T03:00:00Z"
DEFAULT_PMXT_CLOSE_WINDOW_END_TIME = "2026-03-24T08:00:00Z"
DEFAULT_PMXT_MARKET_ACTIVATION_START_NS = 1774326957277659000
DEFAULT_PMXT_MARKET_CLOSE_TIME_NS = 1774337757277659000
