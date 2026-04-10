from __future__ import annotations

import pytest

from prediction_market_extensions.adapters.prediction_market.research import save_aggregate_backtest_report
from prediction_market_extensions.adapters.prediction_market.research import save_joint_portfolio_backtest_report


def test_save_aggregate_backtest_report_accepts_mixed_iso_timestamp_precision(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "aggregate_mixed_timestamps.html"
    results = [
        {
            "slug": "market-mixed",
            "trades": 10,
            "fills": 1,
            "pnl": 1.0,
            "price_series": [("2026-03-14T17:57:40+00:00", 0.40), ("2026-03-14T17:57:40.123456+00:00", 0.42)],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:57:40.123456+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "outcome_series": [("2026-03-14T17:57:40+00:00", 1.0), ("2026-03-14T17:57:40.123456+00:00", 1.0)],
            "fill_events": [
                {
                    "order_id": "fill-mixed",
                    "market_id": "market-mixed",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 10.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                }
            ],
            "pnl_series": [("2026-03-14T17:57:40+00:00", 0.0), ("2026-03-14T17:57:40.123456+00:00", 1.0)],
            "equity_series": [("2026-03-14T17:57:40+00:00", 100.0), ("2026-03-14T17:57:40.123456+00:00", 101.0)],
            "cash_series": [("2026-03-14T17:57:40+00:00", 96.0), ("2026-03-14T17:57:40.123456+00:00", 96.0)],
        }
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=output_path,
        title="mixed timestamp precision chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "mixed timestamp precision chart" in html
    assert "market-mixed" in html


def test_save_joint_portfolio_backtest_report_accepts_mixed_iso_timestamp_precision(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "joint_mixed_timestamps.html"
    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 1,
            "pnl": 1.0,
            "price_series": [("2026-03-14T17:57:40+00:00", 0.40), ("2026-03-14T17:57:40.123456+00:00", 0.42)],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:57:40.123456+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "outcome_series": [("2026-03-14T17:57:40+00:00", 1.0), ("2026-03-14T17:57:40.123456+00:00", 1.0)],
            "fill_events": [
                {
                    "order_id": "fill-a",
                    "market_id": "market-a",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 10.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                }
            ],
            "joint_portfolio_pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:57:40.123456+00:00", 1.5),
            ],
            "joint_portfolio_equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:57:40.123456+00:00", 101.5),
            ],
            "joint_portfolio_cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:57:40.123456+00:00", 96.0),
            ],
        },
        {
            "slug": "market-b",
            "trades": 8,
            "fills": 1,
            "pnl": 0.5,
            "price_series": [("2026-03-14T17:57:40+00:00", 0.55), ("2026-03-14T17:57:40.123456+00:00", 0.57)],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.54),
                ("2026-03-14T17:57:40.123456+00:00", 0.56),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.55),
                ("2026-03-14T17:57:40.123456+00:00", 0.57),
            ],
            "outcome_series": [("2026-03-14T17:57:40+00:00", 0.0), ("2026-03-14T17:57:40.123456+00:00", 0.0)],
            "fill_events": [
                {
                    "order_id": "fill-b",
                    "market_id": "market-b",
                    "action": "sell",
                    "side": "yes",
                    "price": 0.57,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:40.123456+00:00",
                    "commission": 0.0,
                }
            ],
        },
    ]

    report_path = save_joint_portfolio_backtest_report(
        results=results,
        output_path=output_path,
        title="joint mixed timestamp precision chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "joint mixed timestamp precision chart" in html
    assert "market-a" in html
    assert "market-b" in html
