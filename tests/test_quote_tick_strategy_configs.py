# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import pytest

from strategies import QuoteTickBreakoutConfig
from strategies import QuoteTickDeepValueHoldConfig
from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickFinalPeriodMomentumConfig
from strategies import QuoteTickLateFavoriteLimitHoldConfig
from strategies import QuoteTickMeanReversionConfig
from strategies import QuoteTickPanicFadeConfig
from strategies import QuoteTickRSIReversionConfig
from strategies import QuoteTickThresholdMomentumConfig
from strategies import QuoteTickVWAPReversionConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


@pytest.mark.parametrize(
    "config_cls",
    [
        QuoteTickBreakoutConfig,
        QuoteTickDeepValueHoldConfig,
        QuoteTickEMACrossoverConfig,
        QuoteTickFinalPeriodMomentumConfig,
        QuoteTickLateFavoriteLimitHoldConfig,
        QuoteTickMeanReversionConfig,
        QuoteTickPanicFadeConfig,
        QuoteTickRSIReversionConfig,
        QuoteTickThresholdMomentumConfig,
        QuoteTickVWAPReversionConfig,
    ],
)
def test_quote_tick_prediction_market_configs_construct(config_cls):
    config = config_cls(instrument_id=INSTRUMENT_ID)
    assert config.instrument_id == INSTRUMENT_ID
