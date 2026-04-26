from __future__ import annotations

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
                        "USDC": {
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
        pnl_label="PnL (USDC)",
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
    assert "Portfolio PnL stats (USDC)" in output


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
        pnl_label="PnL (USDC)",
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
