"""Buy low strategy — buys YES contracts below a price threshold, holds to resolution."""

from __future__ import annotations

from src.backtesting.models import TradeEvent
from src.backtesting.strategy import Strategy


class BuyLowStrategy(Strategy):
    """Buys YES contracts when price drops below a threshold and holds to resolution.

    Demonstrates the backtesting API with a simple mean-reversion concept:
    contracts priced very low may be undervalued relative to their true
    probability of resolving YES.

    Uses a percentage of available cash for each position (``alloc_pct``)
    so that the strategy deploys meaningful capital across many markets.
    """

    def __init__(
        self,
        threshold: float = 0.20,
        quantity: float = 10.0,
        alloc_pct: float = 0.10,
        max_positions: int = 50,
    ):
        super().__init__(
            name="buy_low",
            description=f"Buy YES when price < {threshold:.0%}, hold to resolution",
        )
        self.threshold = threshold
        self.base_quantity = quantity
        self.alloc_pct = alloc_pct
        self.max_positions = max_positions
        self._ordered: set[str] = set()

    def on_trade(self, trade: TradeEvent) -> None:
        """Place a buy order if price is below threshold and we haven't ordered in this market."""
        if trade.market_id in self._ordered:
            return

        if trade.yes_price >= self.threshold:
            return

        snap = self.portfolio
        if len(self._ordered) >= self.max_positions:
            return
        if snap.cash < 1.0:
            return

        # Size position as a percentage of available cash
        max_spend = snap.cash * self.alloc_pct
        qty = max(1.0, round(min(self.base_quantity, max_spend / trade.yes_price)))
        cost = qty * self.threshold
        if cost > snap.cash:
            qty = max(1.0, round(snap.cash / self.threshold))

        self.buy_yes(
            market_id=trade.market_id,
            price=self.threshold,
            quantity=qty,
        )
        self._ordered.add(trade.market_id)
