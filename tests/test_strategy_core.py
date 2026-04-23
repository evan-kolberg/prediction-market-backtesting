from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import QuoteTickVWAPReversionConfig
from strategies.core import LongOnlyPredictionMarketStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _FakeQuantity:
    def __init__(self, value: Decimal) -> None:
        self._value = value

    def as_double(self) -> float:
        return float(self._value)


class _FakeInstrument:
    def __init__(self, *, min_quantity: Decimal | None) -> None:
        self.quote_currency = "USDC.e"
        self.taker_fee = Decimal(0)
        self.lot_size = None
        self.min_quantity = None if min_quantity is None else _FakeQuantity(min_quantity)

    def make_qty(self, value: float, round_down: bool = True) -> _FakeQuantity:
        quantity = Decimal(str(value)).quantize(
            Decimal("0.000001"), rounding=ROUND_DOWN if round_down else ROUND_DOWN
        )
        return _FakeQuantity(quantity)


class _EntryQuantityHarness(LongOnlyPredictionMarketStrategy):
    def __init__(
        self, *, trade_size: Decimal, free_balance: Decimal, min_quantity: Decimal | None
    ) -> None:
        super().__init__(
            QuoteTickVWAPReversionConfig(instrument_id=INSTRUMENT_ID, trade_size=trade_size)
        )
        self._free_balance = free_balance
        self._instrument = _FakeInstrument(min_quantity=min_quantity)

    def _subscribe(self) -> None:
        return None

    def _free_quote_balance(self) -> Decimal | None:
        return self._free_balance


def test_entry_quantity_skips_clipped_size_below_min_quantity() -> None:
    strategy = _EntryQuantityHarness(
        trade_size=Decimal(25),
        free_balance=Decimal("0.35"),
        min_quantity=Decimal(
            5,
        ),
    )

    quantity = strategy._entry_quantity(reference_price=0.074, visible_size=100.0)

    assert quantity is None


def test_entry_quantity_keeps_clipped_size_when_no_min_quantity_exists() -> None:
    strategy = _EntryQuantityHarness(
        trade_size=Decimal(25), free_balance=Decimal("0.35"), min_quantity=None
    )

    quantity = strategy._entry_quantity(reference_price=0.074, visible_size=100.0)

    assert quantity is not None
    assert quantity.as_double() < 5.0


def test_entry_quantity_leaves_cash_headroom_before_min_quantity_boundary() -> None:
    strategy = _EntryQuantityHarness(
        trade_size=Decimal(5),
        free_balance=Decimal(1),
        min_quantity=Decimal(
            5,
        ),
    )

    quantity = strategy._entry_quantity(reference_price=0.2, visible_size=100.0)

    assert quantity is None


def test_partial_exit_preserves_remaining_entry_cost_basis() -> None:
    strategy = _EntryQuantityHarness(
        trade_size=Decimal(100), free_balance=Decimal(100), min_quantity=None
    )

    strategy.on_order_filled(SimpleNamespace(order_side=OrderSide.BUY, last_px=0.50, last_qty=100))
    strategy.on_order_filled(SimpleNamespace(order_side=OrderSide.SELL, last_px=0.60, last_qty=40))

    assert strategy._entry_qty_sum == 60.0
    assert strategy._entry_cost_sum == 30.0
    assert strategy._entry_price == 0.50
