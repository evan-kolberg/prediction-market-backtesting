"""Gambling addiction strategy — uses real degenerate gambling tactics."""

from __future__ import annotations

import random

from src.backtesting.models import MarketInfo, Side, TradeEvent
from src.backtesting.strategy import Strategy


class GamblingAddictionStrategy(Strategy):
    """Employs classic gambling tactics on prediction markets.

    Tactics used:
    - **Martingale**: doubles bet size after each losing market resolution,
      resets after a win.  The classic "can't lose forever" fallacy.
    - **Mean-reversion bias**: buys YES on cheap contracts ("it's due!")
      and NO on expensive ones ("no way that holds").
    - **Hot hand**: increases bet frequency after a winning streak.
    - **Max exposure cap**: never commits more than a fixed % of equity
      to open positions — even degenerates have a credit limit.
    """

    def __init__(
        self,
        base_bet_pct: float = 0.03,
        max_exposure: float = 0.50,
        max_positions: int = 100,
        martingale_cap: int = 4,
        seed: int = 42,
    ):
        super().__init__(
            name="gambling_addiction",
            description="Martingale + mean-reversion + hot hand, 50% max exposure",
        )
        self.base_bet_pct = base_bet_pct
        self.max_exposure = max_exposure
        self.max_positions = max_positions
        self.martingale_cap = martingale_cap
        self._rng = random.Random(seed)

        # Martingale state
        self._consecutive_losses: int = 0
        self._martingale_mult: float = 1.0

        # Hot hand state
        self._recent_wins: int = 0
        self._hot_hand_boost: float = 1.0

        # Position tracking
        self._active: set[str] = set()
        self._entry_prices: dict[str, float] = {}
        self._entry_sides: dict[str, str] = {}

    def _current_exposure(self) -> float:
        """Fraction of equity currently tied up in positions."""
        snap = self.portfolio
        if snap.total_equity <= 0:
            return 1.0
        position_value = snap.total_equity - snap.cash
        return max(0.0, position_value / snap.total_equity)

    def _bet_size(self, price: float, cash: float, equity: float) -> float:
        """Compute contract quantity using martingale + hot hand multipliers."""
        pct = self.base_bet_pct * self._martingale_mult * self._hot_hand_boost
        spend = equity * pct
        # Never spend more than available cash
        spend = min(spend, cash * 0.9)
        if price <= 0 or spend < price:
            return 0.0
        return max(1.0, round(spend / price))

    def on_trade(self, trade: TradeEvent) -> None:
        """Mean-reversion gambler: bet against extremes, skip the middle."""
        # Skip markets we already have a position in
        if trade.market_id in self._active:
            return

        yes_p = trade.yes_price
        no_p = trade.no_price

        # Mean-reversion filter: only bet on prices in the "cheap" tails
        # YES < 30% → "it's undervalued, buy YES"
        # YES > 70% → "that's too high, buy NO"
        # 30-70% → boring, skip
        go_yes: bool | None = None
        if yes_p < 0.30:
            go_yes = True
        elif yes_p > 0.70:
            go_yes = False
        else:
            # Small random chance to YOLO the middle anyway
            if self._rng.random() < 0.03:
                go_yes = self._rng.random() > 0.5
            else:
                return

        # Check exposure cap
        if self._current_exposure() >= self.max_exposure:
            return
        if len(self._active) >= self.max_positions:
            return

        snap = self.portfolio
        if snap.cash < 2.0:
            return

        price = yes_p if go_yes else no_p
        qty = self._bet_size(price, snap.cash, snap.total_equity)
        if qty < 1.0:
            return

        if go_yes:
            self.buy_yes(market_id=trade.market_id, price=yes_p, quantity=qty)
        else:
            self.buy_no(market_id=trade.market_id, price=no_p, quantity=qty)

        self._active.add(trade.market_id)
        self._entry_prices[trade.market_id] = price
        self._entry_sides[trade.market_id] = "yes" if go_yes else "no"

    def on_market_resolve(self, market: MarketInfo, result: Side) -> None:
        """Martingale: double after loss, reset after win."""
        mid = market.market_id
        if mid not in self._active:
            return

        entry_side = self._entry_sides.get(mid, "yes")
        won = (entry_side == "yes" and result == Side.YES) or (
            entry_side == "no" and result == Side.NO
        )

        if won:
            self._consecutive_losses = 0
            self._martingale_mult = 1.0
            self._recent_wins += 1
            # Hot hand: streak of 3+ wins → increase aggression
            if self._recent_wins >= 3:
                self._hot_hand_boost = min(2.0, 1.0 + self._recent_wins * 0.15)
        else:
            self._consecutive_losses += 1
            self._recent_wins = 0
            self._hot_hand_boost = 1.0
            # Martingale: double up, but cap it
            self._martingale_mult = min(
                2.0 ** min(self._consecutive_losses, self.martingale_cap),
                8.0,
            )

        self._active.discard(mid)
        self._entry_prices.pop(mid, None)
        self._entry_sides.pop(mid, None)
