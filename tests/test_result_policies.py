from __future__ import annotations

import pandas as pd
import pytest

from prediction_market_extensions.backtesting._result_policies import (
    apply_binary_settlement_pnl,
    apply_joint_portfolio_settlement_pnl,
)


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


def test_settlement_pnl_is_not_applied_when_simulated_through_is_missing() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 1.25,
            "realized_outcome": 1.0,
            "fill_events": [{"action": "buy", "price": 0.90, "quantity": 25.0, "commission": 0.0}],
            "settlement_observable_time": "2026-04-10T00:00:00+00:00",
        }
    )

    assert result["pnl"] == 1.25
    assert result["settlement_pnl_applied"] is False
    assert "simulated_through is missing" in result["warnings"][0]


def test_settlement_pnl_updates_summary_series_at_resolution_time() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.02375,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                }
            ],
            "simulated_through": "2026-04-01T00:05:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
            ],
        }
    )

    assert result["pnl"] == pytest.approx(0.22625)
    assert result["equity_series"][-1][0] == "2026-04-01T00:05:00+00:00"
    assert result["equity_series"][-1][1] == pytest.approx(1000.22625)
    assert result["cash_series"][-1][0] == "2026-04-01T00:05:00+00:00"
    assert result["cash_series"][-1][1] == pytest.approx(1000.22625)
    assert result["pnl_series"][-1][0] == "2026-04-01T00:05:00+00:00"
    assert result["pnl_series"][-1][1] == pytest.approx(0.22625)
    assert result["settlement_equity_adjustment"] == pytest.approx(0.25)
    assert result["settlement_cash_adjustment"] == pytest.approx(5.0)


def test_settlement_series_prefers_market_close_when_expiration_precedes_replay() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.02375,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                }
            ],
            "simulated_through": "2026-04-26T18:05:00+00:00",
            "settlement_observable_time": "2026-04-26T00:00:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-26T18:05:00+00:00").value,
            "equity_series": [
                ("2026-04-26T18:04:30+00:00", 1000.0),
                ("2026-04-26T18:04:59.996000+00:00", 999.97625),
            ],
            "cash_series": [
                ("2026-04-26T18:04:30+00:00", 1000.0),
                ("2026-04-26T18:04:59.996000+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-26T18:04:30+00:00", 0.0),
                ("2026-04-26T18:04:59.996000+00:00", -0.02375),
            ],
        }
    )

    assert result["settlement_series_time"] == "2026-04-26T18:05:00+00:00"
    assert result["equity_series"][-2][1] == pytest.approx(999.97625)
    assert result["equity_series"][-1] == ("2026-04-26T18:05:00+00:00", pytest.approx(1000.22625))
    assert result["cash_series"][-1] == ("2026-04-26T18:05:00+00:00", pytest.approx(1000.22625))
    assert result["pnl_series"][-1] == ("2026-04-26T18:05:00+00:00", pytest.approx(0.22625))
    assert result["settlement_equity_adjustment"] == pytest.approx(0.25)
    assert result["settlement_cash_adjustment"] == pytest.approx(5.0)


def test_joint_portfolio_series_receive_settlement_adjustments() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.02375,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                }
            ],
            "simulated_through": "2026-04-01T00:05:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
            ],
            "joint_portfolio_equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
            ],
            "joint_portfolio_cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
            ],
            "joint_portfolio_pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
            ],
        }
    )

    results = apply_joint_portfolio_settlement_pnl([result])

    assert results[0]["joint_portfolio_equity_series"][-1][0] == ("2026-04-01T00:05:00+00:00")
    assert results[0]["joint_portfolio_equity_series"][-1][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_cash_series"][-1][0] == ("2026-04-01T00:05:00+00:00")
    assert results[0]["joint_portfolio_cash_series"][-1][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_pnl_series"][-1][0] == "2026-04-01T00:05:00+00:00"
    assert results[0]["joint_portfolio_pnl_series"][-1][1] == pytest.approx(0.22625)


def test_joint_portfolio_equity_carries_cash_payout_after_settlement() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.02375,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                }
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
                ("2026-04-01T00:06:00+00:00", -4.77375),
            ],
            "joint_portfolio_equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "joint_portfolio_cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "joint_portfolio_pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
                ("2026-04-01T00:06:00+00:00", -4.77375),
            ],
        }
    )

    results = apply_joint_portfolio_settlement_pnl([result])

    assert results[0]["joint_portfolio_equity_series"][-2][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_equity_series"][-1][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_cash_series"][-1][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_pnl_series"][-2][1] == pytest.approx(0.22625)
    assert results[0]["joint_portfolio_pnl_series"][-1][1] == pytest.approx(0.22625)


def test_joint_portfolio_settlement_does_not_double_count_stale_position_value() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -4.77375,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                    "timestamp": "2026-04-01T00:04:50+00:00",
                }
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
            "price_series": [
                ("2026-04-01T00:04:30+00:00", 0.95),
                ("2026-04-01T00:05:00+00:00", 0.95),
            ],
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -4.77375),
                ("2026-04-01T00:06:00+00:00", -4.77375),
            ],
            "joint_portfolio_equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 999.97625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "joint_portfolio_cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
                ("2026-04-01T00:06:00+00:00", 995.22625),
            ],
            "joint_portfolio_pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -0.02375),
                ("2026-04-01T00:06:00+00:00", -4.77375),
            ],
        }
    )

    assert result["settlement_equity_adjustment"] == pytest.approx(0.25)
    assert result["settlement_cash_adjustment"] == pytest.approx(5.0)

    results = apply_joint_portfolio_settlement_pnl([result])

    assert results[0]["joint_portfolio_equity_series"][-2][1] == pytest.approx(1000.22625)
    assert results[0]["joint_portfolio_equity_series"][-1][1] == pytest.approx(1000.22625)
    assert max(value for _, value in results[0]["joint_portfolio_equity_series"]) == pytest.approx(
        1000.22625
    )
