from __future__ import annotations

from prediction_market_extensions.adapters.kalshi.providers import market_dict_to_instrument
from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    infer_realized_outcome_from_metadata,
)
from prediction_market_extensions.adapters.prediction_market.info_sanitization import (
    extract_resolution_metadata,
    sanitize_info_for_simulation,
)


_RESOLVED_KEYS = (
    "result",
    "settlement_value",
    "expiration_value",
    "closed",
    "closedTime",
    "uma_resolution_status",
    "umaResolutionStatus",
    "is_50_50_outcome",
)


def _kalshi_market(*, result: str = "yes") -> dict[str, object]:
    return {
        "ticker": "KX-TEST",
        "event_ticker": "EVENT-TEST",
        "title": "Sanitization test market",
        "open_time": "2026-01-01T00:00:00+00:00",
        "close_time": "2026-12-31T00:00:00+00:00",
        "result": result,
        "closed": True,
        "closedTime": "2026-12-30T00:00:00+00:00",
        "settlement_value": 1.0,
    }


def test_kalshi_instrument_info_strips_resolution_fields() -> None:
    market = _kalshi_market()
    instrument = market_dict_to_instrument(market)

    sanitized_info = dict(instrument.info)
    for key in _RESOLVED_KEYS:
        assert key not in sanitized_info, f"instrument.info still leaks resolution field {key!r}"


def test_resolution_metadata_round_trip_kalshi() -> None:
    market = _kalshi_market(result="no")
    metadata = extract_resolution_metadata(market)

    assert metadata["result"] == "no"
    assert infer_realized_outcome_from_metadata(metadata, "yes") == 0.0
    assert infer_realized_outcome_from_metadata(metadata, "no") == 1.0


def test_polymarket_token_winner_is_redacted_in_simulation_info() -> None:
    market_info = {
        "condition_id": "0x" + "1" * 64,
        "question": "Will it happen?",
        "minimum_tick_size": "0.01",
        "minimum_order_size": "1",
        "end_date_iso": "2026-12-31T00:00:00Z",
        "maker_base_fee": "0",
        "taker_base_fee": "0",
        "tokens": [
            {"outcome": "Yes", "winner": True, "token_id": "1"},
            {"outcome": "No", "winner": False, "token_id": "2"},
        ],
        "is_50_50_outcome": False,
    }

    sanitized = sanitize_info_for_simulation(market_info)
    for token in sanitized["tokens"]:
        assert "winner" not in token, "token.winner is a look-ahead field"
    assert "is_50_50_outcome" not in sanitized

    metadata = extract_resolution_metadata(market_info)
    assert any(token.get("winner") is True for token in metadata.get("tokens", []))
    assert infer_realized_outcome_from_metadata(metadata, "Yes") == 1.0
    assert infer_realized_outcome_from_metadata(metadata, "No") == 0.0


def test_nested_resolution_fields_are_redacted_in_simulation_info() -> None:
    market_info = {
        "condition_id": "0x" + "1" * 64,
        "events": [
            {
                "title": "Nested event",
                "result": "yes",
                "settlement_value": 1.0,
                "tokens": [
                    {"outcome": "Yes", "winner": True, "token_id": "1"},
                    {"outcome": "No", "winner": False, "token_id": "2"},
                ],
            }
        ],
        "nested": {
            "uma_resolution_status": "resolved",
            "payload": {"expiration_value": 1.0, "safe": "kept"},
        },
    }

    sanitized = sanitize_info_for_simulation(market_info)

    event = sanitized["events"][0]
    assert "result" not in event
    assert "settlement_value" not in event
    assert all("winner" not in token for token in event["tokens"])
    assert "uma_resolution_status" not in sanitized["nested"]
    assert "expiration_value" not in sanitized["nested"]["payload"]
    assert sanitized["nested"]["payload"]["safe"] == "kept"


def test_sanitize_does_not_mutate_caller_payload() -> None:
    market = _kalshi_market()
    snapshot = dict(market)
    extract_resolution_metadata(market)
    sanitize_info_for_simulation(market)
    assert market == snapshot
