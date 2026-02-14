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

    Realistic friction:
        - **Commission**: flat rate on notional (cost * rate).
        - **Slippage**: fills at a slightly worse price to model
          market impact (configurable in price-points, default 0.5¢).
        - **Liquidity ceiling**: each fill is capped at the historical
          trade quantity, modeling thin order books.
    """

    def __init__(
        self,
        commission_rate: float = 0.0,
        slippage: float = 0.005,
        liquidity_cap: bool = True,
    ):
        self._pending: dict[str, Order] = {}
        self._commission_rate = commission_rate
        self._slippage = slippage
        self._liquidity_cap = liquidity_cap

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
        # Track remaining liquidity from this trade (shared across orders)
        remaining_liquidity = trade.quantity if self._liquidity_cap else float("inf")

        for order_id, order in self._pending.items():
            if order.market_id != trade.market_id:
                continue

            fill_price = self._match(order, trade)
            if fill_price is None:
                continue

            # --- Apply slippage (adverse to trader) ---
            fill_price = self._apply_slippage(fill_price, order.action)

            # --- Liquidity cap: can only fill up to what was traded ---
            fill_qty = min(order.quantity, remaining_liquidity) if self._liquidity_cap else order.quantity
            if fill_qty <= 0:
                continue

            cost = fill_price * fill_qty
            commission = cost * self._commission_rate

            if order.action == OrderAction.BUY and cost + commission > cash:
                # Try partial fill with available cash
                if self._liquidity_cap:
                    max_qty = cash / (fill_price * (1 + self._commission_rate))
                    fill_qty = min(fill_qty, max_qty)
                    if fill_qty < 1.0:
                        continue
                    fill_qty = int(fill_qty)
                    cost = fill_price * fill_qty
                    commission = cost * self._commission_rate
                else:
                    continue

            if order.action == OrderAction.BUY:
                cash -= cost + commission

            remaining_liquidity -= fill_qty

            fill = Fill(
                order_id=order_id,
                market_id=order.market_id,
                action=order.action,
                side=order.side,
                price=fill_price,
                quantity=fill_qty,
                timestamp=trade.timestamp,
                commission=commission,
            )
            fills.append(fill)
            to_remove.append(order_id)

            order.status = OrderStatus.FILLED
            order.filled_at = trade.timestamp
            order.fill_price = fill_price
            order.filled_quantity = fill_qty

        for oid in to_remove:
            del self._pending[oid]

        return fills

    def _apply_slippage(self, price: float, action: OrderAction) -> float:
        """Nudge fill price adversely by the slippage amount.

        Buys fill slightly higher; sells fill slightly lower.
        Clamped to [0.01, 0.99] to stay within valid price bounds.
        """
        if self._slippage == 0.0:
            return price
        if action == OrderAction.BUY:
            return min(price + self._slippage, 0.99)
        return max(price - self._slippage, 0.01)

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
