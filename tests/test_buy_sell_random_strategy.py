from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import BookBuySellRandomConfig, BookBuySellRandomStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _ImmediateRandomHarness(BookBuySellRandomStrategy):
    def __init__(self, config: BookBuySellRandomConfig) -> None:
        super().__init__(config)
        self.entries = 0
        self.exits = 0
        self.entry_contexts: list[tuple[float | None, float | None]] = []
        self._position = False

    def _random_offset_ns(self) -> int:
        return 0

    def _in_position(self) -> bool:
        return self._position

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entries += 1
        self.entry_contexts.append((reference_price, visible_size))
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


def test_buy_sell_random_alternates_buy_then_sell_on_random_schedule() -> None:
    strategy = _ImmediateRandomHarness(
        BookBuySellRandomConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(1),
            interval_seconds=3.0,
            random_seed=1,
        )
    )

    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        bid_size=10.0,
        ask_size=11.0,
        current_ts_ns=1_000_000_000,
    )
    assert strategy.entries == 1
    assert strategy.exits == 0
    assert strategy.entry_contexts == [(0.42, 11.0)]

    strategy.fill_entry(0.42)
    strategy._on_book_signal(
        bid=0.43,
        ask=0.45,
        bid_size=9.0,
        ask_size=10.0,
        current_ts_ns=2_000_000_000,
    )

    assert strategy.entries == 1
    assert strategy.exits == 1


def test_buy_sell_random_waits_for_next_interval_after_unfilled_attempt() -> None:
    strategy = _ImmediateRandomHarness(
        BookBuySellRandomConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(1),
            interval_seconds=3.0,
            random_seed=1,
        )
    )

    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        bid_size=10.0,
        ask_size=11.0,
        current_ts_ns=1_000_000_000,
    )
    assert strategy.entries == 1
    strategy.on_order_expired(SimpleNamespace())

    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        bid_size=10.0,
        ask_size=11.0,
        current_ts_ns=2_000_000_000,
    )
    assert strategy.entries == 1

    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        bid_size=10.0,
        ask_size=11.0,
        current_ts_ns=4_000_000_000,
    )
    assert strategy.entries == 2


def test_buy_sell_random_respects_entry_price_filter() -> None:
    strategy = _ImmediateRandomHarness(
        BookBuySellRandomConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(1),
            interval_seconds=3.0,
            random_seed=1,
            max_entry_price=0.50,
        )
    )

    strategy._on_book_signal(
        bid=0.60,
        ask=0.62,
        bid_size=10.0,
        ask_size=11.0,
        current_ts_ns=1_000_000_000,
    )

    assert strategy.entries == 0
