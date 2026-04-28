# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Added in this repository on 2026-04-28.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import random
from decimal import Decimal

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_finite_nonnegative_float,
    require_nonnegative_int,
    require_positive_decimal,
    require_probability,
)
from strategies.core import LongOnlyPredictionMarketStrategy

_NANOSECONDS_PER_SECOND = 1_000_000_000
_DEFAULT_THREE_DAY_INTERVAL_SECONDS = 3.0 * 24.0 * 60.0 * 60.0


class BookBuySellRandomConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(5)
    interval_seconds: float = _DEFAULT_THREE_DAY_INTERVAL_SECONDS
    random_seed: int = 7
    max_entry_price: float = 1.0
    max_spread: float = 1.0
    min_visible_size: float = 0.0

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_finite_nonnegative_float("interval_seconds", self.interval_seconds)
        if self.interval_seconds <= 0.0:
            raise ValueError(f"interval_seconds must be > 0, got {self.interval_seconds}")
        require_nonnegative_int("random_seed", self.random_seed)
        require_probability("max_entry_price", self.max_entry_price)
        require_probability("max_spread", self.max_spread)
        require_finite_nonnegative_float("min_visible_size", self.min_visible_size)


def _as_float(value: object | None) -> float | None:
    if value is None:
        return None
    if callable(value):
        value = value()
    as_double = getattr(value, "as_double", None)
    if callable(as_double):
        return float(as_double())
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: object | None) -> int | None:
    if value is None:
        return None
    if callable(value):
        value = value()
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


class BookBuySellRandomStrategy(LongOnlyPredictionMarketStrategy):
    """
    Alternate random buy and sell attempts on a fixed three-day cadence.

    Each flat period schedules one random buy timestamp inside the next
    interval. After a buy fills, each long period schedules one random sell
    timestamp inside the next interval. The RNG is seeded for reproducible
    backtests.
    """

    def __init__(self, config: BookBuySellRandomConfig) -> None:
        super().__init__(config)
        self._rng = random.Random(int(config.random_seed))
        self._interval_ns = max(
            1,
            int(float(config.interval_seconds) * _NANOSECONDS_PER_SECOND),
        )
        self._buy_window_start_ns: int | None = None
        self._next_buy_ns: int | None = None
        self._sell_window_start_ns: int | None = None
        self._next_sell_ns: int | None = None

    def _subscribe(self) -> None:
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def _random_offset_ns(self) -> int:
        return self._rng.randrange(self._interval_ns + 1)

    def _schedule_buy_from(self, window_start_ns: int) -> None:
        self._buy_window_start_ns = int(window_start_ns)
        self._next_buy_ns = self._buy_window_start_ns + self._random_offset_ns()

    def _schedule_sell_from(self, window_start_ns: int) -> None:
        self._sell_window_start_ns = int(window_start_ns)
        self._next_sell_ns = self._sell_window_start_ns + self._random_offset_ns()

    def _advance_buy_window_after_attempt(self, current_ts_ns: int) -> None:
        if self._buy_window_start_ns is None:
            next_start = current_ts_ns + self._interval_ns
        else:
            next_start = self._buy_window_start_ns + self._interval_ns
            while next_start <= current_ts_ns:
                next_start += self._interval_ns
        self._schedule_buy_from(next_start)

    def _advance_sell_window_after_attempt(self, current_ts_ns: int) -> None:
        if self._sell_window_start_ns is None:
            next_start = current_ts_ns + self._interval_ns
        else:
            next_start = self._sell_window_start_ns + self._interval_ns
            while next_start <= current_ts_ns:
                next_start += self._interval_ns
        self._schedule_sell_from(next_start)

    def on_order_book(self, order_book: OrderBook) -> None:
        bid = _as_float(order_book.best_bid_price())
        ask = _as_float(order_book.best_ask_price())
        bid_size = _as_float(order_book.best_bid_size())
        ask_size = _as_float(order_book.best_ask_size())
        current_ts_ns = _as_int(getattr(order_book, "ts_event", None)) or _as_int(
            getattr(order_book, "ts_last", None)
        )
        if (
            bid is None
            or ask is None
            or bid_size is None
            or ask_size is None
            or current_ts_ns is None
        ):
            return
        self._on_book_signal(
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            current_ts_ns=current_ts_ns,
        )

    def _on_book_signal(
        self,
        *,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        current_ts_ns: int,
    ) -> None:
        if ask <= bid:
            return
        spread = ask - bid
        if spread > float(self.config.max_spread):
            return
        if ask_size < float(self.config.min_visible_size) or bid_size <= 0.0:
            return

        self._remember_market_context(
            entry_reference_price=ask,
            entry_visible_size=ask_size,
            exit_visible_size=bid_size,
        )
        if self._pending:
            return

        if not self._in_position():
            self._next_sell_ns = None
            self._sell_window_start_ns = None
            if self._next_buy_ns is None:
                self._schedule_buy_from(current_ts_ns)
            if current_ts_ns < int(self._next_buy_ns):
                return
            if ask > float(self.config.max_entry_price):
                self._advance_buy_window_after_attempt(current_ts_ns)
                return
            self._advance_buy_window_after_attempt(current_ts_ns)
            self._submit_entry(reference_price=ask, visible_size=ask_size)
            return

        self._next_buy_ns = None
        self._buy_window_start_ns = None
        if self._next_sell_ns is None:
            self._schedule_sell_from(current_ts_ns)
        if current_ts_ns < int(self._next_sell_ns):
            return
        self._advance_sell_window_after_attempt(current_ts_ns)
        self._submit_exit()

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        self._next_buy_ns = None
        self._buy_window_start_ns = None
        self._next_sell_ns = None
        self._sell_window_start_ns = None

    def on_reset(self) -> None:
        super().on_reset()
        self._rng = random.Random(int(self.config.random_seed))
        self._buy_window_start_ns = None
        self._next_buy_ns = None
        self._sell_window_start_ns = None
        self._next_sell_ns = None
