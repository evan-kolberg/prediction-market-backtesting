"""Order management and fill matching for prediction market backtesting.

The broker maintains pending limit orders and checks them against incoming
historical trades. Fill rules account for the yes/no duality of binary
contracts.
"""

from __future__ import annotations

from datetime import datetime

from src.backtesting.models import (
    Fill,
    Order,
    OrderAction,
    OrderStatus,
    Side,
    TradeEvent,
)


class Broker:
    """Manages pending limit orders and matches them against incoming trades.

    Fill logic:
        BUY YES  @ limit P: fills when trade.yes_price <= P
        SELL YES @ limit P: fills when trade.yes_price >= P
        BUY NO   @ limit P: fills when trade.no_price  <= P
        SELL NO  @ limit P: fills when trade.no_price  >= P

    Orders fill at the trade price (not the limit price) to avoid
    inflating returns beyond what was historically achievable.
    """

    def __init__(self, commission_rate: float = 0.0):
        self._pending: dict[str, Order] = {}
        self._commission_rate = commission_rate

    @property
    def pending_orders(self) -> list[Order]:
        """All currently pending orders."""
        return list(self._pending.values())

    def place_order(
        self,
        market_id: str,
        action: str,
        side: str,
        price: float,
        quantity: float,
        timestamp: datetime | None = None,
    ) -> Order:
        """Place a new limit order."""
        order = Order(
            market_id=market_id,
            action=OrderAction(action),
            side=Side(side),
            price=price,
            quantity=quantity,
            status=OrderStatus.PENDING,
            created_at=timestamp,
        )
        self._pending[order.order_id] = order
        return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True if found and cancelled."""
        order = self._pending.pop(order_id, None)
        if order is not None:
            order.status = OrderStatus.CANCELLED
            return True
        return False

    def cancel_all(self, market_id: str | None = None) -> int:
        """Cancel all pending orders, optionally filtered by market."""
        to_cancel = [oid for oid, order in self._pending.items() if market_id is None or order.market_id == market_id]
        for oid in to_cancel:
            self._pending[oid].status = OrderStatus.CANCELLED
            del self._pending[oid]
        return len(to_cancel)

    def check_fills(self, trade: TradeEvent, available_cash: float) -> list[Fill]:
        """Check all pending orders against an incoming trade.

        Args:
            trade: The incoming historical trade event.
            available_cash: Cash available for buy orders.

        Returns:
            List of Fill objects for orders that matched.
        """
        fills: list[Fill] = []
        to_remove: list[str] = []
        cash = available_cash

        for order_id, order in self._pending.items():
            if order.market_id != trade.market_id:
                continue

            fill_price = self._match(order, trade)
            if fill_price is None:
                continue

            cost = fill_price * order.quantity
            commission = cost * self._commission_rate

            if order.action == OrderAction.BUY and cost + commission > cash:
                continue

            if order.action == OrderAction.BUY:
                cash -= cost + commission

            fill = Fill(
                order_id=order_id,
                market_id=order.market_id,
                action=order.action,
                side=order.side,
                price=fill_price,
                quantity=order.quantity,
                timestamp=trade.timestamp,
                commission=commission,
            )
            fills.append(fill)
            to_remove.append(order_id)

            order.status = OrderStatus.FILLED
            order.filled_at = trade.timestamp
            order.fill_price = fill_price
            order.filled_quantity = order.quantity

        for oid in to_remove:
            del self._pending[oid]

        return fills

    def _match(self, order: Order, trade: TradeEvent) -> float | None:
        """Check if an order matches the trade. Returns fill price or None."""
        if order.action == OrderAction.BUY and order.side == Side.YES:
            if trade.yes_price <= order.price:
                return trade.yes_price
        elif order.action == OrderAction.SELL and order.side == Side.YES:
            if trade.yes_price >= order.price:
                return trade.yes_price
        elif order.action == OrderAction.BUY and order.side == Side.NO:
            if trade.no_price <= order.price:
                return trade.no_price
        elif order.action == OrderAction.SELL and order.side == Side.NO:
            if trade.no_price >= order.price:
                return trade.no_price
        return None
