"""Unit tests for the Broker order matching engine."""

from __future__ import annotations

from datetime import datetime

import pytest

from src.backtesting.broker import Broker
from src.backtesting.models import (
    OrderAction,
    OrderStatus,
    Platform,
    Side,
    TradeEvent,
)


def _trade(yes_price: float, market_id: str = "MKT-A") -> TradeEvent:
    """Helper to create a trade event at a given yes price."""
    return TradeEvent(
        timestamp=datetime(2024, 1, 15, 12, 0),
        market_id=market_id,
        platform=Platform.KALSHI,
        yes_price=yes_price,
        no_price=1.0 - yes_price,
        quantity=10.0,
        taker_side=Side.YES,
    )


class TestBuyYesFills:
    def test_fills_when_price_below_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.40), 10000.0)
        assert len(fills) == 1
        assert fills[0].price == 0.40
        assert fills[0].action == OrderAction.BUY
        assert fills[0].side == Side.YES

    def test_fills_when_price_equals_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.50), 10000.0)
        assert len(fills) == 1

    def test_no_fill_when_price_above_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.60), 10000.0)
        assert len(fills) == 0


class TestSellYesFills:
    def test_fills_when_price_above_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "sell", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.60), 10000.0)
        assert len(fills) == 1
        assert fills[0].price == 0.60

    def test_no_fill_when_price_below_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "sell", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.40), 10000.0)
        assert len(fills) == 0


class TestBuyNoFills:
    def test_fills_when_no_price_below_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "no", 0.40, 10.0)
        fills = broker.check_fills(_trade(0.70), 10000.0)
        assert len(fills) == 1
        assert fills[0].price == pytest.approx(0.30)  # no_price = 1.0 - 0.70
        assert fills[0].side == Side.NO

    def test_no_fill_when_no_price_above_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "no", 0.20, 10.0)
        fills = broker.check_fills(_trade(0.70), 10000.0)
        assert len(fills) == 0


class TestSellNoFills:
    def test_fills_when_no_price_above_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "sell", "no", 0.40, 10.0)
        fills = broker.check_fills(_trade(0.50), 10000.0)
        assert len(fills) == 1
        assert fills[0].price == 0.50  # no_price = 0.50

    def test_no_fill_when_no_price_below_limit(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "sell", "no", 0.60, 10.0)
        fills = broker.check_fills(_trade(0.50), 10000.0)
        assert len(fills) == 0


class TestCashConstraint:
    def test_buy_rejected_when_insufficient_cash(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 100.0)
        fills = broker.check_fills(_trade(0.40), 1.0)
        assert len(fills) == 0

    def test_buy_succeeds_with_exact_cash(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.50), 5.0)
        assert len(fills) == 1


class TestOrderManagement:
    def test_cancel_order(self) -> None:
        broker = Broker()
        order = broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        assert broker.cancel_order(order.order_id)
        assert order.status == OrderStatus.CANCELLED
        assert len(broker.pending_orders) == 0

    def test_cancel_nonexistent_order(self) -> None:
        broker = Broker()
        assert not broker.cancel_order("nonexistent")

    def test_cancel_all(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        broker.place_order("MKT-B", "buy", "yes", 0.30, 5.0)
        count = broker.cancel_all()
        assert count == 2
        assert len(broker.pending_orders) == 0

    def test_cancel_all_by_market(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        broker.place_order("MKT-B", "buy", "yes", 0.30, 5.0)
        count = broker.cancel_all("MKT-A")
        assert count == 1
        assert len(broker.pending_orders) == 1
        assert broker.pending_orders[0].market_id == "MKT-B"


class TestMarketIsolation:
    def test_order_only_matches_same_market(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.40, market_id="MKT-B"), 10000.0)
        assert len(fills) == 0


class TestMultipleFills:
    def test_multiple_orders_fill_on_one_trade(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 5.0)
        broker.place_order("MKT-A", "buy", "yes", 0.60, 3.0)
        fills = broker.check_fills(_trade(0.40), 10000.0)
        assert len(fills) == 2


class TestCommission:
    def test_commission_applied(self) -> None:
        broker = Broker(commission_rate=0.01)
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.50), 10000.0)
        assert len(fills) == 1
        assert fills[0].commission == 0.50 * 10.0 * 0.01

    def test_commission_blocks_fill_when_cash_insufficient(self) -> None:
        broker = Broker(commission_rate=0.10)
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        fills = broker.check_fills(_trade(0.50), 5.0)
        assert len(fills) == 0


class TestFilledOrderState:
    def test_filled_order_removed_from_pending(self) -> None:
        broker = Broker()
        broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        broker.check_fills(_trade(0.40), 10000.0)
        assert len(broker.pending_orders) == 0

    def test_filled_order_status_updated(self) -> None:
        broker = Broker()
        order = broker.place_order("MKT-A", "buy", "yes", 0.50, 10.0)
        broker.check_fills(_trade(0.40), 10000.0)
        assert order.status == OrderStatus.FILLED
        assert order.fill_price == 0.40
        assert order.filled_quantity == 10.0
