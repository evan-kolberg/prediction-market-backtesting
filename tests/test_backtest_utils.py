# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-03-15.
# See the repository NOTICE file for provenance and licensing scope.

import warnings
from datetime import datetime

from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    compute_binary_settlement_pnl,
    extract_price_points,
    to_naive_utc,
)


def test_compute_binary_settlement_pnl_marks_open_position_to_resolution():
    fill_events = [{"action": "buy", "price": 0.90, "quantity": 25, "commission": 0.0}]

    pnl = compute_binary_settlement_pnl(fill_events, 1.0)

    assert pnl == 2.5


def test_compute_binary_settlement_pnl_includes_realized_sales_and_commission():
    fill_events = [
        {"action": "buy", "price": 0.40, "quantity": 10, "commission": 0.10},
        {"action": "sell", "price": 0.55, "quantity": 4, "commission": 0.05},
    ]

    pnl = compute_binary_settlement_pnl(fill_events, 1.0)

    assert pnl == 4.05


class _QuoteStub:
    ts_event = 123
    bid_price = 0.41
    ask_price = 0.43


def test_extract_price_points_supports_mid_price():
    points = extract_price_points([_QuoteStub()], price_attr="mid_price")

    assert points == [(123, 0.42)]


def test_to_naive_utc_truncates_nanoseconds_without_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        value = to_naive_utc("2026-02-22T12:55:24.290235905Z")

    assert value == datetime(2026, 2, 22, 12, 55, 24, 290235)
