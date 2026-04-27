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


def test_serialize_fill_events_uses_per_fill_quantity_and_price() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "side": "BUY",
                "last_px": 0.40,
                "avg_px": 0.40,
                "last_qty": 40,
                "filled_qty": 40,
                "commission": 0.01,
                "commissions": 0.01,
                "ts_last": "2026-04-01T00:00:00Z",
                "instrument_id": "PM-TEST-YES",
            },
            {
                "client_order_id": "1",
                "side": "BUY",
                "last_px": 0.60,
                "avg_px": 0.52,
                "last_qty": 60,
                "filled_qty": 100,
                "commission": 0.02,
                "commissions": 0.03,
                "ts_last": "2026-04-01T00:01:00Z",
                "instrument_id": "PM-TEST-YES",
            },
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test-yes", fills_report=fills_report)

    assert [event["quantity"] for event in events] == [40.0, 60.0]
    assert [event["price"] for event in events] == [0.40, 0.60]
    assert [event["commission"] for event in events] == [0.01, 0.02]
    assert sum(event["price"] * event["quantity"] for event in events) == 52.0


def test_serialize_fill_events_prefers_order_side_for_trade_action() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "order_side": "BUY",
                "side": "YES",
                "last_px": 0.45,
                "last_qty": 5,
                "ts_last": "2026-04-01T00:00:00Z",
                "instrument_id": "PM-TEST-YES",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test-yes", fills_report=fills_report)

    assert events[0]["action"] == "buy"
    assert events[0]["side"] == "yes"


def test_serialize_fill_events_uses_yes_no_side_as_token_side() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "order_side": "BUY",
                "side": "NO",
                "last_px": 0.10,
                "last_qty": 5,
                "ts_last": "2026-04-01T00:00:00Z",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test", fills_report=fills_report)

    assert events[0]["action"] == "buy"
    assert events[0]["side"] == "no"


def test_serialize_fill_events_does_not_use_token_side_as_trade_action() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "side": "NO",
                "last_px": 0.10,
                "last_qty": 5,
                "ts_last": "2026-04-01T00:00:00Z",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test", fills_report=fills_report)

    assert events[0]["action"] == "buy"
    assert events[0]["side"] == "no"


def test_serialize_fill_events_prefers_explicit_order_side_over_token_side_action() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": "1",
                "action": "NO",
                "order_side": "SELL",
                "side": "NO",
                "last_px": 0.10,
                "last_qty": 5,
                "ts_last": "2026-04-01T00:00:00Z",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test", fills_report=fills_report)

    assert events[0]["action"] == "sell"
    assert events[0]["side"] == "no"


def test_serialize_fill_events_handles_pandas_na_and_raw_side_action() -> None:
    fills_report = pd.DataFrame(
        [
            {
                "client_order_id": pd.NA,
                "venue_order_id": "fallback-order-id",
                "action": "NO",
                "order_side": pd.NA,
                "side": "SELL",
                "instrument_side": pd.NA,
                "last_px": pd.NA,
                "avg_px": 0.33,
                "last_qty": pd.NA,
                "filled_qty": 7,
                "commission": pd.NA,
                "commissions": 0.01,
                "ts_last": pd.NA,
                "ts_event": "2026-04-01T00:00:01Z",
                "instrument_id": pd.NA,
                "symbol": "PM-TEST-NO",
            }
        ]
    )

    events = research._serialize_fill_events(market_id="pm-test", fills_report=fills_report)

    assert events == [
        {
            "order_id": "fallback-order-id",
            "market_id": "pm-test",
            "action": "sell",
            "side": "no",
            "price": 0.33,
            "quantity": 7.0,
            "timestamp": "2026-04-01T00:00:01+00:00",
            "commission": 0.01,
        }
    ]


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


def test_deserialize_fill_events_does_not_treat_token_side_action_as_sell() -> None:
    models_module = SimpleNamespace(
        Side=SimpleNamespace(YES="yes-side", NO="no-side"),
        OrderAction=SimpleNamespace(BUY="buy-action", SELL="sell-action"),
        Fill=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    fills = research._deserialize_fill_events(
        market_id="pm-test-no",
        fill_events=[
            {
                "order_id": "1",
                "action": "no",
                "side": "no",
                "price": 0.2,
                "quantity": 5,
                "timestamp": "2026-04-01T00:00:00Z",
                "commission": 0.0,
            }
        ],
        models_module=models_module,
    )

    assert fills[0].action == "buy-action"
    assert research._fill_event_position_delta({"action": "no", "side": "no", "quantity": 5}) == 5.0
    assert (
        research._fill_event_position_delta(
            {"action": "no", "order_side": "SELL", "side": "no", "quantity": 5}
        )
        == -5.0
    )


def test_deserialize_fill_events_handles_pandas_na_and_raw_side_action() -> None:
    models_module = SimpleNamespace(
        Side=SimpleNamespace(YES="yes-side", NO="no-side"),
        OrderAction=SimpleNamespace(BUY="buy-action", SELL="sell-action"),
        Fill=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    fills = research._deserialize_fill_events(
        market_id="pm-test-no",
        fill_events=[
            {
                "order_id": pd.NA,
                "action": pd.NA,
                "side": "no",
                "price": pd.NA,
                "quantity": 5,
                "timestamp": "2026-04-01T00:00:00Z",
                "commission": pd.NA,
            },
            {
                "order_id": "malformed-action",
                "action": "???",
                "side": "yes",
                "price": 0.2,
                "quantity": 5,
                "timestamp": "2026-04-01T00:00:01Z",
                "commission": 0.0,
            },
            {
                "order_id": "raw-side-sell",
                "action": "NO",
                "side": "SELL",
                "price": 0.2,
                "quantity": 4,
                "timestamp": "2026-04-01T00:00:02Z",
                "commission": 0.0,
            },
        ],
        models_module=models_module,
    )

    assert [fill.action for fill in fills] == ["buy-action", "sell-action"]
    assert [fill.order_id for fill in fills] == ["fill-1", "raw-side-sell"]
    assert [fill.side for fill in fills] == ["no-side", "no-side"]
    assert [fill.price for fill in fills] == [0.0, 0.2]
    assert [fill.quantity for fill in fills] == [5.0, 4.0]


def test_truncate_brier_series_handles_empty_range_index_series() -> None:
    empty = pd.Series(dtype=float)

    user, market, outcome = research._truncate_brier_series_at_cutoff(
        {"settlement_observable_time": "2026-04-13T00:00:00Z"},
        empty,
        empty,
        empty,
    )

    assert user.empty
    assert market.empty
    assert outcome.empty
