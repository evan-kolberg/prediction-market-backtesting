from __future__ import annotations

import pandas as pd
import pytest

from prediction_market_extensions.backtesting._result_policies import (
    BinarySettlementPnlPolicy,
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


def test_settlement_pnl_is_not_applied_without_observable_timestamp() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "price": 0.95,
                    "quantity": 10.0,
                    "commission": 0.0,
                }
            ],
            "simulated_through": "2026-04-01T00:05:00+00:00",
        }
    )

    assert result["pnl"] == 0.0
    assert result["settlement_pnl_applied"] is False
    assert "no settlement observable timestamp" in result["warnings"][0]


def test_settlement_pnl_is_not_applied_with_market_close_but_no_observable_timestamp() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "price": 0.95,
                    "quantity": 2.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:00+00:00",
                }
            ],
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
            "simulated_through": "2026-04-01T00:06:00+00:00",
        }
    )

    assert result["pnl"] == pytest.approx(0.0)
    assert result["settlement_pnl_applied"] is False
    assert "no settlement observable timestamp" in result["warnings"][0]


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


@pytest.mark.parametrize(
    ("commission", "expected_pnl"),
    [
        (0.02375, 0.22625),
        (-0.01, 0.26),
    ],
)
def test_settlement_series_adjusts_from_current_mark_to_market_anchor(
    commission: float, expected_pnl: float
) -> None:
    current_mtm_pnl = -0.95 * 5.0 - commission + 0.95 * 5.0
    result = apply_binary_settlement_pnl(
        {
            "pnl": current_mtm_pnl,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": commission,
                    "timestamp": "2026-04-01T00:05:00+00:00",
                }
            ],
            "simulated_through": "2026-04-01T00:05:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "price_series": [("2026-04-01T00:05:00+00:00", 0.95)],
            "equity_series": [("2026-04-01T00:05:00+00:00", 1000.0 + current_mtm_pnl)],
            "cash_series": [("2026-04-01T00:05:00+00:00", 1000.0 - 0.95 * 5.0 - commission)],
            "pnl_series": [("2026-04-01T00:05:00+00:00", current_mtm_pnl)],
        }
    )

    assert result["pnl"] == pytest.approx(expected_pnl)
    assert result["equity_series"][-1][1] == pytest.approx(1000.0 + expected_pnl)
    assert result["cash_series"][-1][1] == pytest.approx(1000.0 + expected_pnl)


def test_settlement_pnl_prunes_post_settlement_fill_payload() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.95,
            "fills": 2,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 1.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:59+00:00",
                },
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.05,
                    "quantity": 1.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:05:01+00:00",
                },
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
        }
    )

    assert result["settlement_pnl_applied"] is True
    assert result["post_settlement_fill_events_ignored"] == 1
    assert result["fills"] == 1
    assert result["fills_before_post_settlement_pruning"] == 2
    assert len(result["fill_events"]) == 1
    assert result["fill_events"][0]["price"] == pytest.approx(0.95)
    assert "downstream reports" in result["warnings"][0]


def test_settlement_pnl_prunes_custom_fill_payload_and_anchors_adjustment() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -4.77375,
            "fills": 2,
            "realized_outcome": 1.0,
            "execution_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.02375,
                    "timestamp": "2026-04-01T00:04:50+00:00",
                },
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.80,
                    "quantity": 5.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:06:00+00:00",
                },
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
            "price_series": [("2026-04-01T00:05:00+00:00", 0.95)],
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.22625),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", -4.77375),
            ],
        },
        fill_events_key="execution_events",
    )

    assert result["settlement_pnl_applied"] is True
    assert result["post_settlement_fill_events_ignored"] == 1
    assert result["fills"] == 1
    assert result["fills_before_post_settlement_pruning"] == 2
    assert len(result["execution_events"]) == 1
    assert result["settlement_equity_adjustment"] == pytest.approx(0.25)
    assert result["settlement_cash_adjustment"] == pytest.approx(5.0)


def test_settlement_pnl_ignores_non_mapping_fill_events() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [
                "malformed-fill-row",
                {"action": "buy", "price": 0.90, "quantity": 1.0, "commission": 0.0},
                object(),
            ],
            "simulated_through": "2026-04-01T00:05:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
        }
    )

    assert result["settlement_pnl_applied"] is True
    assert result["pnl"] == pytest.approx(0.10)
    assert result["final_signed_position"] == pytest.approx(1.0)


def test_binary_settlement_policy_respects_custom_pnl_and_exit_keys() -> None:
    result: dict[str, object] = {
        "strategy_pnl": -0.123,
        "outcome": 1.0,
        "executions": [
            {
                "action": "buy",
                "price": 0.95,
                "quantity": 5.0,
                "commission": 0.0,
            }
        ],
        "simulated_through": "2026-04-01T00:05:00+00:00",
        "settlement_observable_time": "2026-04-01T00:05:00+00:00",
    }
    policy = BinarySettlementPnlPolicy(
        pnl_key="strategy_pnl",
        market_exit_pnl_key="strategy_exit_pnl",
        fill_events_key="executions",
        realized_outcome_key="outcome",
    )

    policy.apply([result])

    assert result["strategy_pnl"] == pytest.approx(0.25)
    assert result["strategy_exit_pnl"] == pytest.approx(-0.123)
    assert "pnl" not in result
    assert "market_exit_pnl" not in result


def test_settlement_policy_flags_negative_token_inventory() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.60,
            "realized_outcome": 0.0,
            "fill_events": [
                {
                    "action": "sell",
                    "side": "yes",
                    "price": 0.60,
                    "quantity": 10.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:00+00:00",
                }
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
        }
    )

    assert result["backtest_realism_invalid"] is True
    assert result["terminated_early"] is True
    assert result["stop_reason"] == "invalid_short_position"
    assert result["min_signed_position"] == pytest.approx(-10.0)
    assert "Token inventory went negative" in result["warnings"][0]


def test_settlement_policy_runs_integrity_checks_when_observable_time_is_missing() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 6.0,
            "realized_outcome": 0.0,
            "fill_events": [
                {
                    "action": "sell",
                    "side": "yes",
                    "price": 0.60,
                    "quantity": 10.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:00+00:00",
                }
            ],
        }
    )

    assert result["settlement_pnl_applied"] is False
    assert result["backtest_realism_invalid"] is True
    assert result["stop_reason"] == "invalid_short_position"
    assert any("no settlement observable timestamp" in warning for warning in result["warnings"])
    assert any("Token inventory went negative" in warning for warning in result["warnings"])


def test_settlement_policy_flags_negative_cash_series() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -1.0,
            "realized_outcome": None,
            "fill_events": [],
            "cash_series": [
                ("2026-04-01T00:00:00+00:00", 5.0),
                ("2026-04-01T00:01:00+00:00", -1.0),
            ],
        }
    )

    assert result["backtest_realism_invalid"] is True
    assert result["terminated_early"] is True
    assert result["stop_reason"] == "account_error"
    assert result["min_cash"] == pytest.approx(-1.0)
    assert "Cash balance went negative" in result["warnings"][0]


def test_settlement_pnl_is_not_applied_when_requested_window_ends_before_resolution() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [{"action": "buy", "price": 0.95, "quantity": 5.0, "commission": 0.0}],
            "planned_end": "2026-04-01T00:04:00+00:00",
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
        }
    )

    assert result["pnl"] == 0.0
    assert result["settlement_pnl_applied"] is False
    assert "requested replay window" in result["warnings"][0]


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


def test_settlement_waits_for_market_close_after_observable_metadata() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -0.10,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.0,
                }
            ],
            "simulated_through": "2026-04-01T00:03:00+00:00",
            "settlement_observable_time": "2026-04-01T00:01:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
        }
    )

    assert result["pnl"] == pytest.approx(-0.10)
    assert result["settlement_pnl_applied"] is False
    assert "resolution was not observable" in result["warnings"][0]


def test_settlement_series_waits_for_later_observable_time_after_market_close() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:59+00:00",
                }
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:06:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
            "equity_series": [
                ("2026-04-01T00:04:59+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 1000.0),
                ("2026-04-01T00:06:00+00:00", 1000.0),
            ],
            "cash_series": [
                ("2026-04-01T00:04:59+00:00", 995.25),
                ("2026-04-01T00:05:00+00:00", 995.25),
                ("2026-04-01T00:06:00+00:00", 995.25),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:59+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", 0.0),
                ("2026-04-01T00:06:00+00:00", 0.0),
            ],
        }
    )

    assert result["settlement_series_time"] == "2026-04-01T00:06:00+00:00"
    assert result["equity_series"][1] == ("2026-04-01T00:05:00+00:00", pytest.approx(1000.0))
    assert result["equity_series"][-1] == ("2026-04-01T00:06:00+00:00", pytest.approx(1000.25))


def test_settlement_gating_accepts_pandas_timestamp_inputs() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": 0.0,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.0,
                }
            ],
            "simulated_through": pd.Timestamp("2026-04-01T00:05:00+00:00"),
            "settlement_observable_time": pd.Timestamp("2026-04-01T00:05:00+00:00"),
        }
    )

    assert result["settlement_pnl_applied"] is True
    assert result["pnl"] == pytest.approx(0.25)


def test_settlement_pnl_ignores_fills_after_settlement_cutoff() -> None:
    result = apply_binary_settlement_pnl(
        {
            "pnl": -3.75,
            "realized_outcome": 1.0,
            "fill_events": [
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.95,
                    "quantity": 5.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:04:59+00:00",
                },
                {
                    "action": "buy",
                    "side": "yes",
                    "price": 0.80,
                    "quantity": 5.0,
                    "commission": 0.0,
                    "timestamp": "2026-04-01T00:06:00+00:00",
                },
            ],
            "simulated_through": "2026-04-01T00:06:00+00:00",
            "settlement_observable_time": "2026-04-01T00:05:00+00:00",
            "market_close_time_ns": pd.Timestamp("2026-04-01T00:05:00+00:00").value,
            "price_series": [
                ("2026-04-01T00:04:30+00:00", 0.95),
                ("2026-04-01T00:05:00+00:00", 0.95),
                ("2026-04-01T00:06:00+00:00", 0.0),
            ],
            "equity_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 1000.0),
                ("2026-04-01T00:06:00+00:00", 996.0),
            ],
            "cash_series": [
                ("2026-04-01T00:04:30+00:00", 1000.0),
                ("2026-04-01T00:05:00+00:00", 995.25),
                ("2026-04-01T00:06:00+00:00", 991.25),
            ],
            "pnl_series": [
                ("2026-04-01T00:04:30+00:00", 0.0),
                ("2026-04-01T00:05:00+00:00", 0.0),
                ("2026-04-01T00:06:00+00:00", -4.0),
            ],
        }
    )

    assert result["pnl"] == pytest.approx(0.25)
    assert result["post_settlement_fill_events_ignored"] == 1
    assert "Ignored 1 fill event" in result["warnings"][0]
    assert result["equity_series"][-2][1] == pytest.approx(1000.25)
    assert result["equity_series"][-1][1] == pytest.approx(1000.25)
    assert result["cash_series"][-2][1] == pytest.approx(1000.25)
    assert result["cash_series"][-1][1] == pytest.approx(1000.25)
    assert result["pnl_series"][-2][1] == pytest.approx(0.25)
    assert result["pnl_series"][-1][1] == pytest.approx(0.25)


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
