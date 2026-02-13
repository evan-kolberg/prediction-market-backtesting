"""Shared fixtures for backtesting tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

# -- Backtesting fixtures --


def _make_bt_kalshi_trades() -> pd.DataFrame:
    """Kalshi trades for backtesting: 3 markets, chronological, varied prices."""
    rows = []
    base = pd.Timestamp("2024-01-15 10:00:00")
    trade_data = [
        ("BT-MKT-A", 25, "yes", 5),
        ("BT-MKT-A", 30, "no", 3),
        ("BT-MKT-B", 80, "yes", 2),
        ("BT-MKT-A", 15, "yes", 10),
        ("BT-MKT-C", 50, "yes", 4),
        ("BT-MKT-B", 85, "no", 6),
        ("BT-MKT-A", 20, "no", 8),
        ("BT-MKT-C", 45, "yes", 3),
        ("BT-MKT-B", 75, "yes", 5),
        ("BT-MKT-C", 55, "no", 7),
    ]
    for i, (ticker, yes_price, taker_side, count) in enumerate(trade_data):
        rows.append(
            {
                "trade_id": f"bt-{i}",
                "ticker": ticker,
                "count": count,
                "yes_price": yes_price,
                "no_price": 100 - yes_price,
                "taker_side": taker_side,
                "created_time": base + pd.Timedelta(hours=i),
                "_fetched_at": base,
            }
        )
    return pd.DataFrame(rows)


def _make_bt_kalshi_markets() -> pd.DataFrame:
    """Kalshi markets for backtesting: 3 markets with open/close times and results."""
    base = pd.Timestamp("2024-01-15 09:00:00")
    return pd.DataFrame(
        [
            {
                "ticker": "BT-MKT-A",
                "event_ticker": "BT-EVENT",
                "market_type": "binary",
                "title": "Test Market A",
                "yes_sub_title": "Yes",
                "no_sub_title": "No",
                "status": "finalized",
                "yes_bid": None,
                "yes_ask": None,
                "no_bid": None,
                "no_ask": None,
                "last_price": None,
                "volume": 500,
                "volume_24h": 100,
                "open_interest": 0,
                "result": "yes",
                "created_time": base,
                "open_time": base,
                "close_time": base + pd.Timedelta(days=1),
            },
            {
                "ticker": "BT-MKT-B",
                "event_ticker": "BT-EVENT",
                "market_type": "binary",
                "title": "Test Market B",
                "yes_sub_title": "Yes",
                "no_sub_title": "No",
                "status": "finalized",
                "yes_bid": None,
                "yes_ask": None,
                "no_bid": None,
                "no_ask": None,
                "last_price": None,
                "volume": 300,
                "volume_24h": 50,
                "open_interest": 0,
                "result": "no",
                "created_time": base,
                "open_time": base,
                "close_time": base + pd.Timedelta(days=1),
            },
            {
                "ticker": "BT-MKT-C",
                "event_ticker": "BT-EVENT",
                "market_type": "binary",
                "title": "Test Market C",
                "yes_sub_title": "Yes",
                "no_sub_title": "No",
                "status": "finalized",
                "yes_bid": None,
                "yes_ask": None,
                "no_bid": None,
                "no_ask": None,
                "last_price": None,
                "volume": 200,
                "volume_24h": 30,
                "open_interest": 0,
                "result": "yes",
                "created_time": base,
                "open_time": base,
                "close_time": base + pd.Timedelta(days=1),
            },
        ]
    )


@pytest.fixture(scope="session")
def bt_kalshi_trades_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    d = tmp_path_factory.mktemp("bt_kalshi_trades")
    _make_bt_kalshi_trades().to_parquet(d / "trades.parquet")
    return d


@pytest.fixture(scope="session")
def bt_kalshi_markets_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    d = tmp_path_factory.mktemp("bt_kalshi_markets")
    _make_bt_kalshi_markets().to_parquet(d / "markets.parquet")
    return d
