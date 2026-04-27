from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import (
    BookLateFavoriteLimitHoldConfig,
    BookLateFavoriteLimitHoldStrategy,
    BookLateFavoriteTakerHoldConfig,
    BookLateFavoriteTakerHoldStrategy,
)

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


def test_late_favorite_limit_order_acceptance_unblocks_strategy_state() -> None:
    strategy = BookLateFavoriteLimitHoldStrategy(
        BookLateFavoriteLimitHoldConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(5),
            entry_price=0.9,
        )
    )
    strategy._pending = True

    strategy.on_order_accepted(SimpleNamespace())

    assert strategy._pending is False
    assert strategy._entered_once is True


class _LateFavoriteTakerHarness(BookLateFavoriteTakerHoldStrategy):
    def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(
            BookLateFavoriteTakerHoldConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal(5),
                **kwargs,
            )
        )
        self.entries: list[tuple[float | None, float | None]] = []
        self.canceled: list[InstrumentId] = []
        self.exit_submitted = False
        self.in_position = False

    def _in_position(self) -> bool:
        return self.in_position

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entries.append((reference_price, visible_size))

    def _submit_exit(self) -> None:
        self.exit_submitted = True

    def cancel_all_orders(self, instrument_id) -> None:  # type: ignore[no-untyped-def]
        self.canceled.append(instrument_id)


def test_late_favorite_taker_enters_only_inside_configured_late_window() -> None:
    strategy = _LateFavoriteTakerHarness(activation_start_time_ns=100, market_close_time_ns=200)

    strategy._on_book_signal(
        bid=0.91,
        ask=0.93,
        midpoint=0.92,
        spread=0.02,
        ask_size=10.0,
        ts_event_ns=99,
    )
    strategy._on_book_signal(
        bid=0.91,
        ask=0.93,
        midpoint=0.92,
        spread=0.02,
        ask_size=10.0,
        ts_event_ns=150,
    )

    assert strategy.entries == [(0.93, 10.0)]


def test_late_favorite_taker_requires_executable_book_quality() -> None:
    strategy = _LateFavoriteTakerHarness()

    strategy._on_book_signal(
        bid=0.87,
        ask=0.93,
        midpoint=0.90,
        spread=0.06,
        ask_size=10.0,
        ts_event_ns=1,
    )
    strategy._on_book_signal(
        bid=0.91,
        ask=0.995,
        midpoint=0.9525,
        spread=0.085,
        ask_size=10.0,
        ts_event_ns=2,
    )
    strategy._on_book_signal(
        bid=0.91,
        ask=0.93,
        midpoint=0.92,
        spread=0.02,
        ask_size=4.0,
        ts_event_ns=3,
    )

    assert strategy.entries == []


def test_late_favorite_taker_can_enter_cheap_no_when_enabled() -> None:
    strategy = _LateFavoriteTakerHarness(enable_cheap_no_entry=True)

    strategy._on_book_signal(
        bid=0.01,
        ask=0.05,
        midpoint=0.03,
        spread=0.04,
        ask_size=10.0,
        ts_event_ns=1,
    )

    assert strategy.entries == [(0.05, 10.0)]


def test_late_favorite_taker_ignores_cheap_no_when_disabled() -> None:
    strategy = _LateFavoriteTakerHarness()

    strategy._on_book_signal(
        bid=0.01,
        ask=0.05,
        midpoint=0.03,
        spread=0.04,
        ask_size=10.0,
        ts_event_ns=1,
    )

    assert strategy.entries == []


def test_late_favorite_taker_rejects_wide_cheap_no_spread() -> None:
    strategy = _LateFavoriteTakerHarness(
        enable_cheap_no_entry=True,
        max_cheap_no_spread=0.04,
    )

    strategy._on_book_signal(
        bid=0.00,
        ask=0.05,
        midpoint=0.025,
        spread=0.05,
        ask_size=10.0,
        ts_event_ns=1,
    )

    assert strategy.entries == []


def test_late_favorite_taker_stop_holds_position_for_settlement() -> None:
    strategy = _LateFavoriteTakerHarness()

    strategy.on_stop()

    assert strategy.canceled == [INSTRUMENT_ID]
    assert strategy.exit_submitted is False
