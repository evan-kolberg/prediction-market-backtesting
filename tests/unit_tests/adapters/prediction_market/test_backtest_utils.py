from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from nautilus_trader.adapters.prediction_market.backtest_utils import build_brier_inputs
from nautilus_trader.adapters.prediction_market.backtest_utils import infer_realized_outcome


def test_infer_realized_outcome_uses_token_winner_flag() -> None:
    instrument = SimpleNamespace(
        outcome="Yes",
        info={
            "closed": True,
            "tokens": [
                {"outcome": "Yes", "winner": True},
                {"outcome": "No", "winner": False},
            ],
        },
    )

    assert infer_realized_outcome(instrument) == 1.0


def test_infer_realized_outcome_handles_5050_markets() -> None:
    instrument = SimpleNamespace(
        outcome="Yes",
        info={
            "is_50_50_outcome": True,
            "tokens": [
                {"outcome": "Yes", "winner": False},
                {"outcome": "No", "winner": False},
            ],
        },
    )

    assert infer_realized_outcome(instrument) == 0.5


@pytest.mark.parametrize(
    ("outcome", "info", "expected"),
    [
        ("Yes", {"result": "yes"}, 1.0),
        ("Yes", {"result": "no"}, 0.0),
        ("No", {"result": "yes"}, 0.0),
        ("No", {"result": "no"}, 1.0),
        ("Yes", {"settlement_value": 1}, 1.0),
        ("Yes", {"settlement_value": 0}, 0.0),
        ("Yes", {"expiration_value": 100}, 1.0),
        ("Yes", {"expiration_value": 0}, 0.0),
    ],
)
def test_infer_realized_outcome_handles_kalshi_resolution_fields(
    outcome: str,
    info: dict[str, object],
    expected: float,
) -> None:
    instrument = SimpleNamespace(outcome=outcome, info=info)

    assert infer_realized_outcome(instrument) == expected


def test_build_brier_inputs_returns_empty_outcomes_without_resolution() -> None:
    points = [
        (pd.Timestamp("2025-01-01T00:00:00Z"), 0.2),
        (pd.Timestamp("2025-01-02T00:00:00Z"), 0.4),
        (pd.Timestamp("2025-01-03T00:00:00Z"), 0.6),
    ]

    user_probabilities, market_probabilities, outcomes = build_brier_inputs(
        points=points,
        window=2,
        realized_outcome=None,
    )

    assert len(user_probabilities) == 2
    assert len(market_probabilities) == 2
    assert outcomes.empty


def test_build_brier_inputs_uses_realized_outcome_when_available() -> None:
    points = [
        (pd.Timestamp("2025-01-01T00:00:00Z"), 0.2),
        (pd.Timestamp("2025-01-02T00:00:00Z"), 0.4),
        (pd.Timestamp("2025-01-03T00:00:00Z"), 0.6),
    ]

    _user_probabilities, _market_probabilities, outcomes = build_brier_inputs(
        points=points,
        window=2,
        realized_outcome=1.0,
    )

    assert list(outcomes) == [1.0, 1.0]
