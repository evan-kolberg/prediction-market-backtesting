from __future__ import annotations

import pytest

from nautilus_trader.adapters.prediction_market.research import save_aggregate_backtest_report


def test_save_aggregate_backtest_report_writes_legacy_bokeh_html(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "aggregate.html"
    results = [
        {
            "slug": "market-a",
            "trades": 120,
            "fills": 8,
            "pnl": 12.5,
            "price_series": [
                ("2025-01-01T00:00:00+00:00", 0.40),
                ("2025-01-01T00:30:00+00:00", 0.48),
                ("2025-01-01T01:00:00+00:00", 0.53),
            ],
            "user_probability_series": [
                ("2025-01-01T00:30:00+00:00", 0.44),
                ("2025-01-01T01:00:00+00:00", 0.50),
            ],
            "market_probability_series": [
                ("2025-01-01T00:30:00+00:00", 0.48),
                ("2025-01-01T01:00:00+00:00", 0.53),
            ],
            "outcome_series": [
                ("2025-01-01T00:30:00+00:00", 1.0),
                ("2025-01-01T01:00:00+00:00", 1.0),
            ],
            "fill_events": [
                {
                    "order_id": "fill-a-buy",
                    "market_id": "market-a",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 10.0,
                    "timestamp": "2025-01-01T00:00:00+00:00",
                    "commission": 0.0,
                },
                {
                    "order_id": "fill-a-sell",
                    "market_id": "market-a",
                    "action": "sell",
                    "side": "yes",
                    "price": 0.53,
                    "quantity": 10.0,
                    "timestamp": "2025-01-01T01:00:00+00:00",
                    "commission": 0.0,
                },
            ],
            "pnl_series": [
                ("2025-01-01T00:00:00+00:00", 0.0),
                ("2025-01-01T00:30:00+00:00", 4.0),
                ("2025-01-01T01:00:00+00:00", 12.5),
            ],
            "equity_series": [
                ("2025-01-01T00:00:00+00:00", 100.0),
                ("2025-01-01T00:30:00+00:00", 104.0),
                ("2025-01-01T01:00:00+00:00", 112.5),
            ],
            "cash_series": [
                ("2025-01-01T00:00:00+00:00", 96.0),
                ("2025-01-01T00:30:00+00:00", 96.0),
                ("2025-01-01T01:00:00+00:00", 112.5),
            ],
        },
        {
            "slug": "market-b",
            "trades": 95,
            "fills": 4,
            "pnl": -3.25,
            "price_series": [
                ("2025-01-01T00:00:00+00:00", 0.62),
                ("2025-01-01T00:20:00+00:00", 0.58),
                ("2025-01-01T01:00:00+00:00", 0.51),
            ],
            "user_probability_series": [
                ("2025-01-01T00:20:00+00:00", 0.60),
                ("2025-01-01T01:00:00+00:00", 0.56),
            ],
            "market_probability_series": [
                ("2025-01-01T00:20:00+00:00", 0.58),
                ("2025-01-01T01:00:00+00:00", 0.51),
            ],
            "outcome_series": [
                ("2025-01-01T00:20:00+00:00", 0.0),
                ("2025-01-01T01:00:00+00:00", 0.0),
            ],
            "fill_events": [
                {
                    "order_id": "fill-b-buy",
                    "market_id": "market-b",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.62,
                    "quantity": 8.0,
                    "timestamp": "2025-01-01T00:00:00+00:00",
                    "commission": 0.0,
                },
                {
                    "order_id": "fill-b-sell",
                    "market_id": "market-b",
                    "action": "sell",
                    "side": "yes",
                    "price": 0.51,
                    "quantity": 8.0,
                    "timestamp": "2025-01-01T01:00:00+00:00",
                    "commission": 0.0,
                },
            ],
            "pnl_series": [
                ("2025-01-01T00:00:00+00:00", 0.0),
                ("2025-01-01T00:20:00+00:00", -1.0),
                ("2025-01-01T01:00:00+00:00", -3.25),
            ],
            "equity_series": [
                ("2025-01-01T00:00:00+00:00", 100.0),
                ("2025-01-01T00:20:00+00:00", 99.0),
                ("2025-01-01T01:00:00+00:00", 96.75),
            ],
            "cash_series": [
                ("2025-01-01T00:00:00+00:00", 95.04),
                ("2025-01-01T00:20:00+00:00", 95.04),
                ("2025-01-01T01:00:00+00:00", 96.75),
            ],
        },
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=output_path,
        title="custom multi-market chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        max_points_per_market=20,
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "custom multi-market chart" in html
    assert "Total Equity" in html
    assert "Equity" in html
    assert "Profit / Loss" in html
    assert "P&amp;L (periodic)" in html
    assert "Allocation" in html
    assert "YES Price" in html
    assert "Drawdown" in html
    assert "Rolling Sharpe" in html
    assert "Cash / Equity" in html
    assert "Cumulative Brier Advantage" in html
    assert "market-a" in html
    assert "market-b" in html
    assert "Fills (" in html
    assert "Monthly Returns" in html
    assert "plotly" not in html.lower()
