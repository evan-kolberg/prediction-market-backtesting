"""Paper broker — pure-Python order matching and portfolio tracking for front testing.

Mirrors the Rust broker/portfolio logic so that strategies behave identically
in backtesting and front testing.  Performance is not critical here because
trades arrive in real-time (one every few seconds at most).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from src.backtesting.models import (
    Fill,
    Order,
    OrderAction,
    OrderStatus,
    PortfolioSnapshot,
    Side,
    TradeEvent,
)

# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------


@dataclass
class _Position:
    market_id: str
    quantity: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0


# ---------------------------------------------------------------------------
# Paper Broker
# ---------------------------------------------------------------------------


class PaperBroker:
    """Simulated broker + portfolio, matching the Rust engine semantics."""

    def __init__(
        self,
        initial_cash: float,
        commission_rate: float = 0.01,
        slippage: float = 0.005,
        liquidity_cap: bool = True,
        ema_decay: float = 0.1,
    ):
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.liquidity_cap = liquidity_cap
        self.ema_decay = ema_decay

        self._pending: dict[str, list[Order]] = defaultdict(list)
        self._positions: dict[str, _Position] = {}
        self._last_prices: dict[str, float] = {}
        self._ema_trade_sizes: dict[str, float] = {}
        self._resolved: set[str] = set()
        self._next_id = 1

    # -- Order API ----------------------------------------------------------

    def place_order(
        self,
        market_id: str,
        action: str,
        side: str,
        price: float,
        quantity: float,
        timestamp: datetime | None = None,
    ) -> Order:
        order = Order(
            order_id=str(self._next_id),
            market_id=market_id,
            action=OrderAction(action),
            side=Side(side),
            price=price,
            quantity=quantity,
            status=OrderStatus.PENDING,
            created_at=timestamp or datetime.now(),
        )
        self._next_id += 1
        self._pending[market_id].append(order)
        return order

    def cancel_order(self, order_id: str) -> bool:
        for orders in self._pending.values():
            for i, o in enumerate(orders):
                if o.order_id == order_id:
                    orders.pop(i)
                    return True
        return False

    def cancel_all(self, market_id: str | None = None) -> int:
        if market_id is not None:
            removed = self._pending.pop(market_id, [])
            return len(removed)
        count = sum(len(v) for v in self._pending.values())
        self._pending.clear()
        return count

    def get_pending_orders(self) -> list[Order]:
        return [o for orders in self._pending.values() for o in orders]

    # -- Fill matching ------------------------------------------------------

    def check_fills(self, trade: TradeEvent) -> list[Fill]:
        """Check pending orders for this trade's market.  Returns fills."""
        self._last_prices[trade.market_id] = trade.yes_price

        # Update EMA of trade size for market impact model
        mid = trade.market_id
        if mid in self._ema_trade_sizes:
            self._ema_trade_sizes[mid] = (
                self._ema_trade_sizes[mid] * (1 - self.ema_decay)
                + trade.quantity * self.ema_decay
            )
        else:
            self._ema_trade_sizes[mid] = trade.quantity
        avg_trade_size = self._ema_trade_sizes[mid]

        orders = self._pending.get(trade.market_id)
        if not orders:
            return []

        fills: list[Fill] = []
        cash = self.cash
        remaining_liq = trade.quantity if self.liquidity_cap else float("inf")
        to_remove: list[int] = []

        for idx, order in enumerate(orders):
            fill_price = self._match_order(order, trade)
            if fill_price is None:
                continue

            fill_price = self._apply_slippage(fill_price, order.action, order.quantity, avg_trade_size)

            fill_qty = min(order.quantity, remaining_liq) if self.liquidity_cap else order.quantity
            if fill_qty <= 0:
                continue

            cost = fill_price * fill_qty
            commission = cost * self.commission_rate

            if order.action == OrderAction.BUY and cost + commission > cash:
                if self.liquidity_cap:
                    max_qty = cash / (fill_price * (1 + self.commission_rate))
                    fill_qty = min(fill_qty, max_qty)
                    if fill_qty < 1.0:
                        continue
                    fill_qty = float(int(fill_qty))
                    cost = fill_price * fill_qty
                    commission = cost * self.commission_rate
                else:
                    continue

            if order.action == OrderAction.BUY:
                cash -= cost + commission

            remaining_liq -= fill_qty

            fill = Fill(
                order_id=order.order_id,
                market_id=order.market_id,
                action=order.action,
                side=order.side,
                price=fill_price,
                quantity=fill_qty,
                timestamp=trade.timestamp,
                commission=commission,
            )
            fills.append(fill)
            self._apply_fill(fill)

            order.status = OrderStatus.FILLED
            order.filled_at = trade.timestamp
            order.fill_price = fill_price
            order.filled_quantity = fill_qty
            to_remove.append(idx)

        for idx in reversed(to_remove):
            orders.pop(idx)

        return fills

    # -- Portfolio ----------------------------------------------------------

    def resolve_market(self, market_id: str, result: Side) -> float:
        if market_id in self._resolved:
            return 0.0
        pos = self._positions.get(market_id)
        if pos is None or pos.quantity == 0.0:
            self._resolved.add(market_id)
            return 0.0

        settlement = 1.0 if result == Side.YES else 0.0
        if pos.quantity > 0:
            payout = pos.quantity * settlement
        else:
            payout = abs(pos.quantity) * (1.0 - settlement)
        self.cash += payout

        if pos.quantity > 0:
            cost_basis = pos.quantity * pos.avg_entry_price
        else:
            cost_basis = abs(pos.quantity) * (1.0 - pos.avg_entry_price)
        resolution_pnl = payout - cost_basis
        pos.realized_pnl += resolution_pnl
        pos.quantity = 0.0
        pos.avg_entry_price = 0.0
        self._resolved.add(market_id)
        return resolution_pnl

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        unrealized = 0.0
        num_positions = 0
        for mid, pos in self._positions.items():
            if pos.quantity == 0.0 or mid in self._resolved:
                continue
            num_positions += 1
            last_price = self._last_prices.get(mid, pos.avg_entry_price)
            if pos.quantity > 0:
                unrealized += pos.quantity * (last_price - pos.avg_entry_price)
            else:
                unrealized += abs(pos.quantity) * (pos.avg_entry_price - last_price)
        return PortfolioSnapshot(
            timestamp=datetime.now(),
            cash=self.cash,
            total_equity=self.cash + unrealized,
            unrealized_pnl=unrealized,
            num_positions=num_positions,
        )

    # -- Internal -----------------------------------------------------------

    @staticmethod
    def _match_order(order: Order, trade: TradeEvent) -> float | None:
        # Taker-side-aware: a resting limit order only fills when the taker is on
        # the opposite side (mirrors the Rust broker logic).
        if order.action == OrderAction.BUY and order.side == Side.YES:
            if trade.taker_side == Side.NO and trade.yes_price <= order.price:
                return trade.yes_price
        elif order.action == OrderAction.SELL and order.side == Side.YES:
            if trade.taker_side == Side.YES and trade.yes_price >= order.price:
                return trade.yes_price
        elif order.action == OrderAction.BUY and order.side == Side.NO:
            if trade.taker_side == Side.YES and trade.no_price <= order.price:
                return trade.no_price
        elif order.action == OrderAction.SELL and order.side == Side.NO:
            if trade.taker_side == Side.NO and trade.no_price >= order.price:
                return trade.no_price
        return None

    def _apply_slippage(
        self, price: float, action: OrderAction, order_qty: float, avg_trade_size: float
    ) -> float:
        if self.slippage == 0:
            return price
        # Price-proportional spread: widens significantly at extreme prices.
        variance = max(price * (1 - price), 0.01)
        spread_factor = max(0.25 / variance, 1.0)
        # Square-root size impact.
        size_ratio = order_qty / max(avg_trade_size, 0.01)
        size_factor = max(size_ratio**0.5, 1.0)
        impact = self.slippage * spread_factor * size_factor
        if action == OrderAction.BUY:
            return min(price + impact, 0.99)
        return max(price - impact, 0.01)

    def _apply_fill(self, fill: Fill) -> None:
        pos = self._positions.get(fill.market_id)
        if pos is None:
            pos = _Position(market_id=fill.market_id)
            self._positions[fill.market_id] = pos

        if fill.action == OrderAction.BUY and fill.side == Side.YES:
            self._add_to_position(pos, fill.quantity, fill.price)
            self.cash -= fill.price * fill.quantity
        elif fill.action == OrderAction.SELL and fill.side == Side.YES:
            self._reduce_position(pos, fill.quantity, fill.price)
            self.cash += fill.price * fill.quantity
        elif fill.action == OrderAction.BUY and fill.side == Side.NO:
            yes_equiv = 1.0 - fill.price
            self._add_to_position(pos, -fill.quantity, yes_equiv)
            self.cash -= fill.price * fill.quantity
        elif fill.action == OrderAction.SELL and fill.side == Side.NO:
            yes_equiv = 1.0 - fill.price
            self._reduce_position(pos, -fill.quantity, yes_equiv)
            self.cash += fill.price * fill.quantity

        self.cash -= fill.commission

    @staticmethod
    def _add_to_position(pos: _Position, delta: float, price: float) -> None:
        if pos.quantity == 0.0:
            pos.quantity = delta
            pos.avg_entry_price = price
            return
        same_direction = (pos.quantity > 0) == (delta > 0)
        if same_direction:
            total_cost = abs(pos.quantity) * pos.avg_entry_price + abs(delta) * price
            pos.quantity += delta
            if pos.quantity != 0:
                pos.avg_entry_price = total_cost / abs(pos.quantity)
        else:
            PaperBroker._close_partial(pos, delta, price)

    @staticmethod
    def _reduce_position(pos: _Position, delta: float, price: float) -> None:
        PaperBroker._close_partial(pos, -delta, price)

    @staticmethod
    def _close_partial(pos: _Position, delta: float, price: float) -> None:
        closing_qty = min(abs(delta), abs(pos.quantity))
        if closing_qty == 0.0:
            pos.quantity += delta
            pos.avg_entry_price = price
            return
        if pos.quantity > 0:
            pnl = closing_qty * (price - pos.avg_entry_price)
        else:
            pnl = closing_qty * (pos.avg_entry_price - price)
        pos.realized_pnl += pnl
        remaining = abs(delta) - closing_qty
        pos.quantity += delta
        if abs(pos.quantity) < 1e-10:
            pos.quantity = 0.0
            pos.avg_entry_price = 0.0
        elif remaining > 0:
            pos.avg_entry_price = price
