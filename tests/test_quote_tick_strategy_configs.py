# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import (
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
    TradeTickLateFavoriteLimitHoldConfig,
)

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


@pytest.mark.parametrize(
    "config_cls",
    [QuoteTickLateFavoriteLimitHoldConfig, TradeTickLateFavoriteLimitHoldConfig],
)
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"entry_price": -0.01}, "entry_price"),
        ({"entry_price": 1.01}, "entry_price"),
        ({"activation_start_time_ns": -1}, "activation_start_time_ns"),
        ({"market_close_time_ns": -1}, "market_close_time_ns"),
        (
            {"activation_start_time_ns": 20, "market_close_time_ns": 10},
            "activation_start_time_ns",
        ),
    ],
)
def test_late_favorite_configs_validate_ranges(config_cls, kwargs, message):
    with pytest.raises(ValueError, match=message):
        config_cls(instrument_id=INSTRUMENT_ID, **kwargs)
