# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-03-15.
# See the repository NOTICE file for provenance and licensing scope.

import warnings
from datetime import datetime

from prediction_market_extensions.adapters.prediction_market import backtest_utils
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


def test_compute_binary_settlement_pnl_handles_open_short_positions() -> None:
    fill_events = [{"action": "sell", "price": 0.70, "quantity": 10, "commission": 0.0}]

    pnl = compute_binary_settlement_pnl(fill_events, 1.0)

    assert pnl == -3.0


def test_compute_binary_settlement_pnl_returns_none_without_fills() -> None:
    assert compute_binary_settlement_pnl([], 1.0) is None


def test_compute_binary_settlement_pnl_marks_no_contracts_to_inverse_outcome() -> None:
    fill_events = [
        {"action": "buy", "side": "no", "price": 0.30, "quantity": 10, "commission": 0.0}
    ]

    pnl = compute_binary_settlement_pnl(fill_events, 0.0)

    assert pnl == 7.0


class _QuoteStub:
    ts_event = 123
    bid_price = 0.41
    ask_price = 0.43


def test_extract_price_points_supports_mid_price():
    points = extract_price_points([_QuoteStub()], price_attr="mid_price")

    assert points == [(123, 0.42)]


def test_extract_price_points_applies_l2_book_deltas(monkeypatch) -> None:
    class FakeDeltas:
        def __init__(self, ts_event: int, updates: tuple[tuple[str, float], ...]) -> None:
            self.instrument_id = "POLYMARKET.TEST"
            self.ts_event = ts_event
            self.updates = updates

    class FakeOrderBook:
        def __init__(self, instrument_id, book_type):  # type: ignore[no-untyped-def]
            del instrument_id, book_type
            self._bid: float | None = None
            self._ask: float | None = None

        def apply_deltas(self, deltas: FakeDeltas) -> None:
            for side, price in deltas.updates:
                if side == "bid":
                    self._bid = price
                else:
                    self._ask = price

        def best_bid_price(self) -> float | None:
            return self._bid

        def best_ask_price(self) -> float | None:
            return self._ask

    monkeypatch.setattr(backtest_utils, "OrderBookDeltas", FakeDeltas)
    monkeypatch.setattr(backtest_utils, "OrderBook", FakeOrderBook)

    points = extract_price_points(
        [
            FakeDeltas(1, (("bid", 0.44), ("ask", 0.46))),
            FakeDeltas(2, (("ask", 0.50),)),
        ],
        price_attr="price",
    )

    assert points == [(1, 0.45), (2, 0.47)]


def test_to_naive_utc_truncates_nanoseconds_without_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        value = to_naive_utc("2026-02-22T12:55:24.290235905Z")

    assert value == datetime(2026, 2, 22, 12, 55, 24, 290235)


def test_extract_probability_frame_warns_before_clipping_bad_values() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = extract_price_points([], price_attr="price")  # keep import path exercised
        from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
            build_brier_inputs,
        )

        build_brier_inputs(
            points=[("2026-01-01T00:00:00Z", 1.2), ("2026-01-01T00:01:00Z", 0.8)],
            window=1,
        )

    assert any("outside [0.0, 1.0]" in str(w.message) for w in caught)
