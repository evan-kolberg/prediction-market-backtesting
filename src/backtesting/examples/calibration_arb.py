"""Calibration arbitrage strategy — exploits mispricings at extreme prices."""

from __future__ import annotations

from src.backtesting.models import TradeEvent
from src.backtesting.strategy import Strategy


class CalibrationArbStrategy(Strategy):
    """Exploits calibration mispricings at extreme price levels.

    Research shows markets at very low prices (< 5%) resolve YES more
    often than implied, and very high prices (> 95%) resolve YES less
    often than implied. This strategy buys YES on cheap markets and
    buys NO on expensive ones.
    """

    def __init__(
        self,
        low_threshold: float = 0.05,
        high_threshold: float = 0.95,
        quantity: float = 10.0,
    ):
        super().__init__(
            name="calibration_arb",
            description="Bet against calibration mispricings at extreme prices",
        )
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self.quantity = quantity
        self._entered: set[str] = set()

    def on_trade(self, trade: TradeEvent) -> None:
        """Enter longshot bias and favorite bias positions."""
        if trade.market_id in self._entered:
            return

        if trade.yes_price <= self.low_threshold:
            self.buy_yes(
                market_id=trade.market_id,
                price=self.low_threshold,
                quantity=self.quantity,
            )
            self._entered.add(trade.market_id)
        elif trade.yes_price >= self.high_threshold:
            self.buy_no(
                market_id=trade.market_id,
                price=1.0 - self.high_threshold,
                quantity=self.quantity,
            )
            self._entered.add(trade.market_id)
