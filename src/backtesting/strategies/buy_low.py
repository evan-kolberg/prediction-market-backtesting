"""Buy Low strategy: Buys YES at 20 cents, holds until resolution."""

from src.backtesting.models import MarketInfo, Side, TradeEvent
from src.backtesting.strategy import Strategy


class BuyLowStrategy(Strategy):
    def __init__(self, threshold: float = 0.20, quantity: float = 100.0, initial_cash: float = 10_000.0):
        super().__init__(
            name="buy_low",
            description=f"Buy YES when price < {threshold:.0%}, hold to resolution.",
            initial_cash=initial_cash,
        )
        self.threshold = threshold
        self.quantity = quantity
        self._ordered: set[str] = set()

    def on_trade(self, trade: TradeEvent) -> None:
        """Place a buy order if price is below threshold and we haven't already bought in this market."""
        if trade.market_id in self._ordered:
            return
        if trade.yes_price < self.threshold:
            self.buy_yes(
                market_id=trade.market_id,
                price=trade.yes_price,
                quantity=self.quantity,
            )
            self._ordered.add(trade.market_id)

    def on_market_resolve(self, market: MarketInfo, result: Side) -> None:
        self._ordered.discard(market.market_id)
