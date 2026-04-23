from __future__ import annotations

from prediction_market_extensions.backtesting._result_policies import apply_binary_settlement_pnl


def test_settlement_pnl_is_not_applied_when_resolution_occurs_after_replay() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -1.25,
            "realized_outcome": 1.0,
            "fill_events": [{"action": "buy", "price": 0.90, "quantity": 25.0, "commission": 0.0}],
            "simulated_through": "2026-04-01T00:00:00+00:00",
            "settlement_observable_time": "2026-04-10T00:00:00+00:00",
        }
    )

    assert result["pnl"] == -1.25
    assert result["settlement_pnl_applied"] is False
    assert "warnings" in result
    assert "mark-to-market PnL" in result["warnings"][0]
