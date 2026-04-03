"""Shared console output helpers for single-market trade-tick backtests."""

from __future__ import annotations

from typing import Any


def build_single_market_trade_summary_row(
    *,
    market_label: str,
    count: int,
    fills: int,
    pnl: float,
    prices: list[float],
) -> dict[str, Any]:
    """Build a normalized summary row for a single-market trade-tick backtest."""
    if not prices:
        raise ValueError("prices must not be empty")

    return {
        "market": market_label,
        "count": count,
        "fills": fills,
        "pnl": pnl,
        "min_price": min(prices),
        "max_price": max(prices),
        "last_price": prices[-1],
    }


def print_single_market_trade_summary(
    *,
    rows: list[dict[str, Any]],
    count_label: str,
    pnl_label: str,
    empty_message: str = "No markets had sufficient data.",
) -> None:
    """Print a richer single-market trade-tick summary table."""
    if not rows:
        print(empty_message)
        return

    col_w = max(len(str(row["market"])) for row in rows) + 2
    header = (
        f"{'Market':<{col_w}} {count_label:>7} {'Fills':>6} "
        f"{'Min Px':>8} {'Max Px':>8} {'Last Px':>8} {pnl_label:>12}"
    )
    sep = "-" * len(header)

    print(f"\n{sep}\n{header}\n{sep}")
    for row in rows:
        print(
            f"{row['market']:<{col_w}} {row['count']:>7} {row['fills']:>6} "
            f"{row['min_price']:>8.3f} {row['max_price']:>8.3f} {row['last_price']:>8.3f} "
            f"{row['pnl']:>+12.4f}"
        )

    total_pnl = sum(float(row["pnl"]) for row in rows)
    total_fills = sum(int(row["fills"]) for row in rows)
    print(sep)
    print(
        f"{'TOTAL':<{col_w}} {'':>7} {total_fills:>6} {'':>8} {'':>8} {'':>8} {total_pnl:>+12.4f}"
    )
    print(sep)
