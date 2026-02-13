"""Portfolio tracking for prediction market backtesting.

Manages cash, positions, and equity snapshots. All positions are stored
as yes-contract quantities where positive = long YES and negative = long NO.
"""

from __future__ import annotations

from datetime import datetime

from src.backtesting.models import (
    Fill,
    OrderAction,
    PortfolioSnapshot,
    Position,
    Side,
)


class Portfolio:
    """Tracks cash balance, positions, and equity over time.

    Position accounting uses yes-contract quantity with signed direction:
        +quantity = long YES contracts (paid yes_price per contract)
        -quantity = long NO contracts (paid no_price per contract)

    Market resolution settlement:
        YES outcome: each contract pays quantity * 1.0
        NO outcome:  each contract pays quantity * 0.0
    """

    def __init__(self, initial_cash: float = 10_000.0):
        self.cash: float = initial_cash
        self.initial_cash: float = initial_cash
        self.positions: dict[str, Position] = {}
        self._last_prices: dict[str, float] = {}
        self._snapshots: list[PortfolioSnapshot] = []
        self._resolved_markets: set[str] = set()

    def apply_fill(self, fill: Fill) -> None:
        """Update positions and cash based on a fill."""
        if fill.market_id not in self.positions:
            self.positions[fill.market_id] = Position(market_id=fill.market_id)

        pos = self.positions[fill.market_id]

        if fill.action == OrderAction.BUY and fill.side == Side.YES:
            self._add_to_position(pos, fill.quantity, fill.price)
            self.cash -= fill.price * fill.quantity
        elif fill.action == OrderAction.SELL and fill.side == Side.YES:
            self._reduce_position(pos, fill.quantity, fill.price)
            self.cash += fill.price * fill.quantity
        elif fill.action == OrderAction.BUY and fill.side == Side.NO:
            # Buying NO = shorting YES. Cost is no_price * qty.
            yes_equiv_price = 1.0 - fill.price
            self._add_to_position(pos, -fill.quantity, yes_equiv_price)
            self.cash -= fill.price * fill.quantity
        elif fill.action == OrderAction.SELL and fill.side == Side.NO:
            # Selling NO = buying YES back. Proceeds = no_price * qty.
            yes_equiv_price = 1.0 - fill.price
            self._reduce_position(pos, -fill.quantity, yes_equiv_price)
            self.cash += fill.price * fill.quantity

        self.cash -= fill.commission

    def _add_to_position(self, pos: Position, delta: float, price: float) -> None:
        """Add to a position in the same direction, updating average price."""
        if pos.quantity == 0.0:
            pos.quantity = delta
            pos.avg_entry_price = price
            return

        same_direction = (pos.quantity > 0) == (delta > 0)
        if same_direction:
            total_cost = abs(pos.quantity) * pos.avg_entry_price + abs(delta) * price
            pos.quantity += delta
            if pos.quantity != 0.0:
                pos.avg_entry_price = total_cost / abs(pos.quantity)
        else:
            self._close_partial(pos, delta, price)

    def _reduce_position(self, pos: Position, delta: float, price: float) -> None:
        """Reduce a position (opposite direction of existing)."""
        self._close_partial(pos, -delta, price)

    def _close_partial(self, pos: Position, delta: float, price: float) -> None:
        """Close part of a position, realizing P&L."""
        closing_qty = min(abs(delta), abs(pos.quantity))
        if closing_qty == 0.0:
            # Opening new position in opposite direction
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
        elif remaining > 0.0:
            # Flipped direction — new position at new price
            pos.avg_entry_price = price

    def resolve_market(self, market_id: str, result: Side) -> float:
        """Resolve a market and convert the position to cash.

        Returns the realized P&L from resolution.
        """
        if market_id in self._resolved_markets:
            return 0.0
        if market_id not in self.positions:
            return 0.0

        pos = self.positions[market_id]
        if pos.quantity == 0.0:
            self._resolved_markets.add(market_id)
            return 0.0

        settlement = 1.0 if result == Side.YES else 0.0
        payout = pos.quantity * settlement
        self.cash += payout

        cost_basis = pos.quantity * pos.avg_entry_price
        resolution_pnl = payout - cost_basis
        pos.realized_pnl += resolution_pnl

        pos.quantity = 0.0
        pos.avg_entry_price = 0.0
        self._resolved_markets.add(market_id)

        return resolution_pnl

    def update_price(self, market_id: str, yes_price: float) -> None:
        """Record latest yes price for mark-to-market valuation."""
        self._last_prices[market_id] = yes_price

    def snapshot(self, timestamp: datetime) -> PortfolioSnapshot:
        """Compute and store a portfolio snapshot."""
        unrealized = 0.0
        num_positions = 0
        for mid, pos in self.positions.items():
            if pos.quantity == 0.0 or mid in self._resolved_markets:
                continue
            num_positions += 1
            last_price = self._last_prices.get(mid, pos.avg_entry_price)
            if pos.quantity > 0:
                unrealized += pos.quantity * (last_price - pos.avg_entry_price)
            else:
                unrealized += abs(pos.quantity) * (pos.avg_entry_price - last_price)

        snap = PortfolioSnapshot(
            timestamp=timestamp,
            cash=self.cash,
            total_equity=self.cash + unrealized,
            unrealized_pnl=unrealized,
            num_positions=num_positions,
        )
        self._snapshots.append(snap)
        return snap

    def get_snapshot(self, timestamp: datetime) -> PortfolioSnapshot:
        """Compute a portfolio snapshot without storing it."""
        unrealized = 0.0
        num_positions = 0
        for mid, pos in self.positions.items():
            if pos.quantity == 0.0 or mid in self._resolved_markets:
                continue
            num_positions += 1
            last_price = self._last_prices.get(mid, pos.avg_entry_price)
            if pos.quantity > 0:
                unrealized += pos.quantity * (last_price - pos.avg_entry_price)
            else:
                unrealized += abs(pos.quantity) * (pos.avg_entry_price - last_price)

        return PortfolioSnapshot(
            timestamp=timestamp,
            cash=self.cash,
            total_equity=self.cash + unrealized,
            unrealized_pnl=unrealized,
            num_positions=num_positions,
        )

    @property
    def equity_curve(self) -> list[PortfolioSnapshot]:
        """All recorded snapshots."""
        return self._snapshots
