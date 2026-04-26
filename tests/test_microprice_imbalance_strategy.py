from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import BookMicropriceImbalanceConfig, BookMicropriceImbalanceStrategy
from strategies.microprice_imbalance import _as_float

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _MicropriceHarness(BookMicropriceImbalanceStrategy):
    def __init__(self, config: BookMicropriceImbalanceConfig) -> None:
        super().__init__(config)
        self.entries: list[tuple[float | None, float | None]] = []
        self.exits = 0
        self._position = False

    def _in_position(self) -> bool:
        return self._position

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entries.append((reference_price, visible_size))
        self._pending = True

    def _submit_exit(self) -> None:
        self.exits += 1
        self._pending = True

    def fill_entry(self, price: float, qty: float = 1.0, *, ts_event: int | None = None) -> None:
        self._position = True
        self.on_order_filled(
            SimpleNamespace(
                order_side=OrderSide.BUY,
                last_px=price,
                last_qty=qty,
                ts_event=ts_event,
            )
        )

    def fill_exit(
        self, price: float, qty: float = 1.0, *, flat: bool = True, ts_event: int | None = None
    ) -> None:
        self._position = not flat
        self.on_order_filled(
            SimpleNamespace(
                order_side=OrderSide.SELL,
                last_px=price,
                last_qty=qty,
                ts_event=ts_event,
            )
        )


def _strategy(**kwargs) -> _MicropriceHarness:
    config = {
        "instrument_id": INSTRUMENT_ID,
        "trade_size": Decimal(5),
        "entry_imbalance": 0.55,
        "exit_imbalance": 0.50,
        "min_microprice_edge": 0.001,
        "max_spread": 0.05,
        "min_holding_updates": 2,
        "reentry_cooldown_updates": 2,
        "take_profit": 0.0,
        "stop_loss": 0.0,
    }
    config.update(kwargs)
    return _MicropriceHarness(BookMicropriceImbalanceConfig(**config))


def test_microprice_imbalance_enters_on_tight_positive_book_pressure() -> None:
    strategy = _strategy()

    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.412,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
    )

    assert strategy.entries == [(0.412, 25.0)]


def test_as_float_accepts_nautilus_level_methods() -> None:
    assert _as_float(lambda: SimpleNamespace(as_double=lambda: 12.5)) == 12.5


def test_microprice_imbalance_rejects_wide_expected_slippage() -> None:
    strategy = _strategy(max_expected_slippage=0.01)

    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.425,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
    )

    assert strategy.entries == []


def test_microprice_imbalance_rejects_expected_entry_above_price_cap() -> None:
    strategy = _strategy(max_entry_price=0.42, max_expected_slippage=0.02)

    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.425,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
    )

    assert strategy.entries == []


def test_microprice_imbalance_rejects_locked_or_crossed_books() -> None:
    strategy = _strategy()

    strategy._on_book_signal(
        bid=0.41,
        ask=0.41,
        spread=0.0,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.41,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
    )

    assert strategy.entries == []


def test_microprice_imbalance_exits_after_pressure_fades_and_cooldown_blocks_reentry() -> None:
    strategy = _strategy()

    strategy.fill_entry(0.41)
    for _ in range(2):
        strategy._on_book_signal(
            bid=0.40,
            ask=0.42,
            spread=0.02,
            imbalance=0.49,
            microprice_edge=-0.002,
            expected_entry_price=0.42,
            entry_visible_size=25.0,
            exit_visible_size=10.0,
        )

    assert strategy.exits == 1
    strategy.fill_exit(0.40)

    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.41,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
    )

    assert len(strategy.entries) == 0


def test_microprice_imbalance_uses_wall_clock_min_hold_and_cooldown() -> None:
    strategy = _strategy(
        min_holding_updates=0,
        reentry_cooldown_updates=0,
        min_holding_seconds=60.0,
        reentry_cooldown_seconds=120.0,
    )

    strategy.fill_entry(0.41, ts_event=1_000_000_000)
    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        spread=0.02,
        imbalance=0.49,
        microprice_edge=-0.002,
        expected_entry_price=0.42,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
        current_ts_ns=30_000_000_000,
    )
    assert strategy.exits == 0

    strategy._on_book_signal(
        bid=0.40,
        ask=0.42,
        spread=0.02,
        imbalance=0.49,
        microprice_edge=-0.002,
        expected_entry_price=0.42,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
        current_ts_ns=62_000_000_000,
    )
    assert strategy.exits == 1

    strategy.fill_exit(0.40, ts_event=63_000_000_000)
    strategy._pending = False
    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.41,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
        current_ts_ns=120_000_000_000,
    )
    assert len(strategy.entries) == 0

    strategy._on_book_signal(
        bid=0.39,
        ask=0.41,
        spread=0.02,
        imbalance=0.62,
        microprice_edge=0.003,
        expected_entry_price=0.41,
        entry_visible_size=25.0,
        exit_visible_size=10.0,
        current_ts_ns=184_000_000_000,
    )
    assert len(strategy.entries) == 1


def test_microprice_imbalance_keeps_partial_exit_state_active() -> None:
    strategy = _strategy(reentry_cooldown_updates=3)

    strategy.fill_entry(0.41, qty=5.0)
    strategy._holding_updates = 7
    strategy.fill_exit(0.40, qty=2.0, flat=False)

    assert strategy._holding_updates == 7
    assert strategy._reentry_cooldown_remaining == 0


def test_on_order_book_uses_bbo_sizes_for_microprice_edge() -> None:
    strategy = _strategy(depth_levels=2)
    captured: dict[str, float | None] = {}

    class _Level:
        def __init__(self, size: float) -> None:
            self._size = size

        def size(self):  # type: ignore[no-untyped-def]
            return SimpleNamespace(as_double=lambda: self._size)

    class _Book:
        def best_bid_price(self) -> float:
            return 0.39

        def best_ask_price(self) -> float:
            return 0.41

        def best_bid_size(self) -> float:
            return 10.0

        def best_ask_size(self) -> float:
            return 10.0

        def spread(self) -> float:
            return 0.02

        def midpoint(self) -> float:
            return 0.40

        def bids(self) -> list[_Level]:
            return [_Level(10.0), _Level(90.0)]

        def asks(self) -> list[_Level]:
            return [_Level(10.0), _Level(0.0)]

    strategy._on_book_signal = lambda **kwargs: captured.update(kwargs)  # type: ignore[method-assign]

    strategy.on_order_book(_Book())  # type: ignore[arg-type]

    assert captured["imbalance"] == pytest.approx(100 / 110)
    assert captured["microprice_edge"] == pytest.approx(0.0)
