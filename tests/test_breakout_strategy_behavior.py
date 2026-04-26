from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import BookBreakoutConfig, BookBreakoutStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _BreakoutHarness(BookBreakoutStrategy):
    def __init__(self, config: BookBreakoutConfig) -> None:
        super().__init__(config)
        self.entries = 0
        self.exits = 0
        self._position = False

    def _in_position(self) -> bool:
        return self._position

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entries += 1
        self._pending = True

    def _submit_exit(self) -> None:
        self.exits += 1
        self._pending = True

    def fill_entry(self, price: float, qty: float = 1.0) -> None:
        self._position = True
        self.on_order_filled(SimpleNamespace(order_side=OrderSide.BUY, last_px=price, last_qty=qty))

    def fill_exit(self, price: float, qty: float = 1.0) -> None:
        self._position = False
        self.on_order_filled(
            SimpleNamespace(order_side=OrderSide.SELL, last_px=price, last_qty=qty)
        )


def test_quote_breakout_requires_move_beyond_noise_before_entering() -> None:
    strategy = _BreakoutHarness(
        BookBreakoutConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(1),
            window=4,
            breakout_std=1.0,
            breakout_buffer=0.0005,
            mean_reversion_buffer=0.0005,
            min_holding_periods=2,
            reentry_cooldown=3,
        )
    )

    for price in (0.0130, 0.0130, 0.0130, 0.0130):
        strategy._on_price(price)

    strategy._on_price(0.0134)
    assert strategy.entries == 0

    strategy._on_price(0.0140)
    assert strategy.entries == 1


def test_quote_breakout_uses_hold_period_and_reentry_cooldown() -> None:
    strategy = _BreakoutHarness(
        BookBreakoutConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(1),
            window=4,
            breakout_std=1.0,
            breakout_buffer=0.0005,
            mean_reversion_buffer=0.0005,
            min_holding_periods=2,
            reentry_cooldown=3,
        )
    )

    for price in (0.0130, 0.0130, 0.0130, 0.0130, 0.0134, 0.0140):
        strategy._on_price(price)
    assert strategy.entries == 1
    strategy.fill_entry(0.0140)

    strategy._on_price(0.0137)
    assert strategy.exits == 0

    strategy._on_price(0.0126)
    assert strategy.exits == 1
    strategy.fill_exit(0.0126)

    for price in (0.0130, 0.0130, 0.0130):
        strategy._on_price(price)
    assert strategy.entries == 1

    strategy._on_price(0.0150)
    assert strategy.entries == 2
