# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-25.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import (
    BookBreakoutConfig,
    BookDeepValueHoldConfig,
    BookEMACrossoverConfig,
    BookFinalPeriodMomentumConfig,
    BookLateFavoriteLimitHoldConfig,
    BookMeanReversionConfig,
    BookMicropriceImbalanceConfig,
    BookPanicFadeConfig,
    BookRSIReversionConfig,
    BookThresholdMomentumConfig,
    BookVWAPReversionConfig,
)

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


@pytest.mark.parametrize(
    "config_cls",
    [
        BookBreakoutConfig,
        BookDeepValueHoldConfig,
        BookEMACrossoverConfig,
        BookFinalPeriodMomentumConfig,
        BookLateFavoriteLimitHoldConfig,
        BookMeanReversionConfig,
        BookMicropriceImbalanceConfig,
        BookPanicFadeConfig,
        BookRSIReversionConfig,
        BookThresholdMomentumConfig,
        BookVWAPReversionConfig,
    ],
)
def test_book_prediction_market_configs_construct(config_cls):
    config = config_cls(instrument_id=INSTRUMENT_ID)
    assert config.instrument_id == INSTRUMENT_ID


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
def test_book_late_favorite_config_validates_ranges(kwargs, message):
    with pytest.raises(ValueError, match=message):
        BookLateFavoriteLimitHoldConfig(instrument_id=INSTRUMENT_ID, **kwargs)
