from __future__ import annotations

import pytest

from prediction_market_extensions.adapters.prediction_market import research
from prediction_market_extensions.adapters.prediction_market.research import print_backtest_summary


def test_print_backtest_summary_includes_rich_statistics(capsys) -> None:
    print_backtest_summary(
        results=[
            {
                "slug": "alpha",
                "book_events": 10,
                "fills": 2,
                "pnl": 1.25,
                "fill_events": [
                    {"price": 0.40, "quantity": 5.0},
                    {"price": 0.50, "quantity": 3.0},
                ],
                "equity_series": [
                    ("2026-01-01T00:00:00+00:00", 100.0),
                    ("2026-01-02T00:00:00+00:00", 102.0),
                    ("2026-01-03T00:00:00+00:00", 101.0),
                ],
                "joint_portfolio_equity_series": [
                    ("2026-01-01T00:00:00+00:00", 100.0),
                    ("2026-01-02T00:00:00+00:00", 102.0),
                    ("2026-01-03T00:00:00+00:00", 101.0),
                ],
                "portfolio_stats": {
                    "iterations": 12,
                    "total_events": 34,
                    "total_orders": 2,
                    "total_positions": 1,
                    "elapsed_time": 0.123,
                    "stats_returns": {
                        "Sharpe Ratio (252 days)": 1.5,
                        "Profit Factor": 2.0,
                    },
                    "stats_pnls": {
                        "pUSD": {
                            "PnL (total)": 1.0,
                            "Win Rate": 0.5,
                            "Expectancy": 0.25,
                        }
                    },
                },
                "requested_coverage_ratio": 0.75,
            },
            {
                "slug": "beta",
                "book_events": 7,
                "fills": 0,
                "pnl": -0.25,
                "fill_events": [],
                "requested_coverage_ratio": 1.0,
            },
        ],
        market_key="slug",
        count_key="book_events",
        count_label="Book Events",
        pnl_label="PnL (pUSD)",
    )

    output = capsys.readouterr().out

    assert "Qty" in output
    assert "AvgPx" in output
    assert "Notional" in output
    assert "Return" in output
    assert "MaxDD" in output
    assert "Sharpe" in output
    assert "Sortino" in output
    assert "PF" in output
    assert "Coverage" in output
    assert "8.00" in output
    assert "0.4375" in output
    assert "3.50" in output
    assert "+1.00%" in output
    assert "+75.00%" in output
    assert "TOTAL" in output
    assert "Portfolio run stats" in output
    assert "Events: 34" in output
    assert "Portfolio return stats" in output
    assert "Sharpe Ratio (252 days): 1.5" in output
    assert "Portfolio PnL stats (pUSD)" in output


def test_print_backtest_summary_aligns_count_header_with_values(capsys) -> None:
    print_backtest_summary(
        results=[
            {
                "slug": "a",
                "book_events": 123456,
                "fills": 77,
                "pnl": 0.0,
                "fill_events": [],
            }
        ],
        market_key="slug",
        count_key="book_events",
        count_label="Book Events",
        pnl_label="PnL (pUSD)",
    )

    output = capsys.readouterr().out
    header = next(line for line in output.splitlines() if line.startswith("Market"))
    row = next(line for line in output.splitlines() if line.startswith("a"))

    count_header_end = header.index("Book Events") + len("Book Events")
    count_value_end = row.index("123456") + len("123456")
    fills_header_end = header.index("Fills") + len("Fills")
    fills_value_end = row.index("77") + len("77")

    assert count_value_end == count_header_end
    assert fills_value_end == fills_header_end


def test_total_summary_uses_portfolio_stats_when_plot_series_disagrees(capsys) -> None:
    print_backtest_summary(
        results=[
            {
                "slug": "alpha",
                "book_events": 10,
                "fills": 2,
                "pnl": -0.5981,
                "fill_events": [],
                "joint_portfolio_equity_series": [
                    ("2026-01-01T00:00:00+00:00", 1000.0),
                    ("2026-01-01T00:05:00+00:00", 1002.1),
                ],
                "portfolio_stats": {
                    "stats_returns": {
                        "Sortino Ratio (252 days)": -15.8745,
                        "Profit Factor": 0.3621,
                    },
                    "stats_pnls": {
                        "pUSD": {
                            "PnL (total)": -0.5981,
                            "PnL% (total)": -0.05981,
                        }
                    },
                },
            }
        ],
        market_key="slug",
        count_key="book_events",
        count_label="Book Events",
        pnl_label="PnL (pUSD)",
    )

    output = capsys.readouterr().out
    total_row = next(line for line in output.splitlines() if line.startswith("TOTAL"))

    assert "-0.06%" in total_row
    assert "+0.21%" not in total_row
    assert "-15.87" in total_row
    assert "0.36" in total_row


def test_market_summary_reconciles_return_stats_to_result_pnl() -> None:
    row = research._summary_stats_for_result(
        {
            "pnl": -5.0,
            "fill_events": [
                {"price": 0.40, "quantity": 5.0},
                {"price": 0.50, "quantity": 3.0},
            ],
            "equity_series": [
                ("2026-01-01T00:00:00+00:00", 100.0),
                ("2026-01-02T00:00:00+00:00", 102.0),
                ("2026-01-03T00:00:00+00:00", 103.0),
            ],
            "requested_coverage_ratio": 0.5,
        }
    )

    assert row["fill_qty"] == pytest.approx(8.0)
    assert row["fill_notional"] == pytest.approx(3.5)
    assert row["avg_fill_price"] == pytest.approx(0.4375)
    assert row["return_pct"] == pytest.approx(-5.0)
    assert row["max_drawdown_pct"] == pytest.approx(-6.862745098)
    assert row["profit_factor"] == pytest.approx(0.291428571)
    assert row["coverage_pct"] == pytest.approx(50.0)


def test_total_summary_reconciles_return_stats_to_total_pnl_without_portfolio_stats() -> None:
    results = [
        {
            "slug": "alpha",
            "book_events": 10,
            "fills": 1,
            "pnl": -5.0,
            "fill_events": [{"price": 0.40, "quantity": 5.0}],
            "joint_portfolio_equity_series": [
                ("2026-01-01T00:00:00+00:00", 100.0),
                ("2026-01-02T00:00:00+00:00", 103.0),
            ],
            "requested_coverage_ratio": 0.25,
        },
        {
            "slug": "beta",
            "book_events": 20,
            "fills": 1,
            "pnl": 0.0,
            "fill_events": [{"price": 0.60, "quantity": 5.0}],
            "requested_coverage_ratio": 0.75,
        },
    ]
    rows = [research._summary_stats_for_result(result) for result in results]
    total = research._summary_stats_total(rows=rows, results=results)

    assert total["fill_qty"] == pytest.approx(10.0)
    assert total["fill_notional"] == pytest.approx(5.0)
    assert total["avg_fill_price"] == pytest.approx(0.5)
    assert total["return_pct"] == pytest.approx(-5.0)
    assert total["max_drawdown_pct"] == pytest.approx(-5.0)
    assert total["coverage_pct"] == pytest.approx(50.0)


def test_total_summary_ignores_portfolio_stats_when_pnl_basis_disagrees() -> None:
    results = [
        {
            "slug": "alpha",
            "book_events": 10,
            "fills": 1,
            "pnl": -20.0,
            "fill_events": [{"price": 0.40, "quantity": 5.0}],
            "joint_portfolio_equity_series": [
                ("2026-01-01T00:00:00+00:00", 1000.0),
                ("2026-01-02T00:00:00+00:00", 1005.0),
            ],
            "portfolio_stats": {
                "stats_returns": {
                    "Sharpe Ratio (252 days)": -12.58,
                    "Sortino Ratio (252 days)": -10.73,
                    "Profit Factor": 0.1395,
                },
                "stats_pnls": {
                    "pUSD": {
                        "PnL (total)": -23.59,
                        "PnL% (total)": -2.359,
                    }
                },
            },
        }
    ]
    rows = [research._summary_stats_for_result(result) for result in results]
    total = research._summary_stats_total(rows=rows, results=results)

    assert total["return_pct"] == pytest.approx(-2.0)
    assert total["max_drawdown_pct"] == pytest.approx(-2.0)
    assert total["sharpe"] != pytest.approx(-12.58)
    assert total["sortino"] != pytest.approx(-10.73)
    assert total["profit_factor"] != pytest.approx(0.1395)
