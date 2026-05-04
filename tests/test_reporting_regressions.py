from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from prediction_market_extensions.adapters.prediction_market import research


def test_serialize_fill_events_preserves_no_side_from_instrument_id() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "side": "BUY",
                "avg_px": 0.2,
                "filled_qty": 5,
                "ts_last": "2026-04-01T00:00:00Z",
                "instrument_id": "PM-TEST-NO",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test", fills_report=fills_report)

    assert events[0]["side"] == "no"
    assert events[0]["timestamp_ns"] == 1_775_001_600_000_000_000


def test_deserialize_fill_events_uses_serialized_side_when_present() -> None:
    models_module = SimpleNamespace(
        Side=SimpleNamespace(YES="yes-side", NO="no-side"),
        OrderAction=SimpleNamespace(BUY="buy-action", SELL="sell-action"),
        Fill=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    fills = research._deserialize_fill_events(
        market_id="pm-test-yes",
        fill_events=[
            {
                "order_id": "1",
                "action": "buy",
                "side": "no",
                "price": 0.2,
                "quantity": 5,
                "timestamp": "2026-04-01T00:00:00Z",
                "commission": 0.0,
            }
        ],
        models_module=models_module,
    )

    assert fills[0].side == "no-side"


def test_deserialize_fill_events_prefers_timestamp_ns() -> None:
    models_module = SimpleNamespace(
        Side=SimpleNamespace(YES="yes-side", NO="no-side"),
        OrderAction=SimpleNamespace(BUY="buy-action", SELL="sell-action"),
        Fill=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    fills = research._deserialize_fill_events(
        market_id="pm-test-yes",
        fill_events=[
            {
                "order_id": "1",
                "action": "buy",
                "side": "yes",
                "price": 0.2,
                "quantity": 5,
                "timestamp": "not-a-real-timestamp",
                "timestamp_ns": 1_775_001_600_123_456_000,
                "commission": 0.0,
            }
        ],
        models_module=models_module,
    )

    assert (
        fills[0].timestamp
        == pd.Timestamp(1_775_001_600_123_456_000, unit="ns", tz="UTC")
        .tz_localize(None)
        .to_pydatetime()
    )
