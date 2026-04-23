from __future__ import annotations

from decimal import Decimal

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import (
    BarBreakoutConfig,
    QuoteTickDeepValueHoldConfig,
    QuoteTickEMACrossoverConfig,
    QuoteTickFinalPeriodMomentumConfig,
    QuoteTickPanicFadeConfig,
    QuoteTickRSIReversionConfig,
    QuoteTickThresholdMomentumConfig,
)

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
BAR_TYPE = "unused-bar-type"


@pytest.mark.parametrize(
    ("config_cls", "kwargs", "message"),
    [
        (QuoteTickDeepValueHoldConfig, {"trade_size": Decimal("0")}, "trade_size"),
        (QuoteTickDeepValueHoldConfig, {"entry_price_max": 1.1}, "entry_price_max"),
        (QuoteTickEMACrossoverConfig, {"fast_period": 10, "slow_period": 10}, "fast_period"),
        (QuoteTickRSIReversionConfig, {"entry_rsi": -1.0}, "entry_rsi"),
        (QuoteTickRSIReversionConfig, {"entry_rsi": 60.0, "exit_rsi": 55.0}, "entry_rsi"),
        (QuoteTickPanicFadeConfig, {"drop_window": 0}, "drop_window"),
        (QuoteTickPanicFadeConfig, {"panic_price": 1.2}, "panic_price"),
        (
            QuoteTickThresholdMomentumConfig,
            {"market_close_time_ns": -1},
            "market_close_time_ns",
        ),
        (
            QuoteTickFinalPeriodMomentumConfig,
            {"final_period_minutes": 0},
            "final_period_minutes",
        ),
    ],
)
def test_strategy_configs_reject_invalid_ranges(config_cls, kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        config_cls(instrument_id=INSTRUMENT_ID, **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"breakout_buffer": -0.1}, "breakout_buffer"),
        ({"mean_reversion_buffer": -0.1}, "mean_reversion_buffer"),
        ({"min_holding_periods": -1}, "min_holding_periods"),
        ({"reentry_cooldown": -1}, "reentry_cooldown"),
        ({"max_entry_price": 1.1}, "max_entry_price"),
    ],
)
def test_breakout_config_rejects_new_invalid_ranges(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        BarBreakoutConfig(instrument_id=INSTRUMENT_ID, bar_type=BAR_TYPE, **kwargs)
