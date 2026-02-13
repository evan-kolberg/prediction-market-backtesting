"""Buy low strategy — buys YES contracts below a price threshold, holds to resolution."""

from __future__ import annotations

from src.backtesting.models import TradeEvent
from src.backtesting.strategy import Strategy


class BuyLowStrategy(Strategy):
    """Buys YES contracts when price drops below a threshold and holds to resolution.

    Demonstrates the backtesting API with a simple mean-reversion concept:
    contracts priced very low may be undervalued relative to their true
    probability of resolving YES.
    """

    def __init__(self, threshold: float = 0.20, quantity: float = 10.0):
        super().__init__(
            name="buy_low",
            description=f"Buy YES when price < {threshold:.0%}, hold to resolution",
        )
        self.threshold = threshold
        self.quantity = quantity
        self._ordered: set[str] = set()

    def on_trade(self, trade: TradeEvent) -> None:
        """Place a buy order if price is below threshold and we haven't ordered in this market."""
        if trade.market_id in self._ordered:
            return

        if trade.yes_price < self.threshold:
            self.buy_yes(
                market_id=trade.market_id,
                price=self.threshold,
                quantity=self.quantity,
            )
            self._ordered.add(trade.market_id)
