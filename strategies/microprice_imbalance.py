# -------------------------------------------------------------------------------------------------
# Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
# https://nautechsystems.io
#
# Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing
# permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
# Added in this repository on 2026-04-26.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_finite_nonnegative_float,
    require_less,
    require_nonnegative_int,
    require_positive_decimal,
    require_positive_int,
    require_probability,
)
from strategies.core import LongOnlyPredictionMarketStrategy


class _MicropriceImbalanceConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    depth_levels: int
    entry_imbalance: float
    exit_imbalance: float
    min_microprice_edge: float
    max_spread: float
    max_entry_price: float
    max_expected_slippage: float
    min_holding_updates: int
    reentry_cooldown_updates: int
    min_holding_seconds: float
    reentry_cooldown_seconds: float
    take_profit: float
    stop_loss: float


_NANOSECONDS_PER_SECOND = 1_000_000_000


class BookMicropriceImbalanceConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(5)
    depth_levels: int = 3
    entry_imbalance: float = 0.57
    exit_imbalance: float = 0.50
    min_microprice_edge: float = 0.001
    max_spread: float = 0.04
    max_entry_price: float = 0.95
    max_expected_slippage: float = 0.015
    min_holding_updates: int = 25
    reentry_cooldown_updates: int = 50
    min_holding_seconds: float = 0.0
    reentry_cooldown_seconds: float = 0.0
    take_profit: float = 0.01
    stop_loss: float = 0.015

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("depth_levels", self.depth_levels)
        require_probability("entry_imbalance", self.entry_imbalance)
        require_probability("exit_imbalance", self.exit_imbalance)
        require_less(
            "exit_imbalance",
            self.exit_imbalance,
            "entry_imbalance",
            self.entry_imbalance,
        )
        require_finite_nonnegative_float("min_microprice_edge", self.min_microprice_edge)
        require_probability("max_spread", self.max_spread)
        require_probability("max_entry_price", self.max_entry_price)
        require_finite_nonnegative_float("max_expected_slippage", self.max_expected_slippage)
        require_nonnegative_int("min_holding_updates", self.min_holding_updates)
        require_nonnegative_int("reentry_cooldown_updates", self.reentry_cooldown_updates)
        require_finite_nonnegative_float("min_holding_seconds", self.min_holding_seconds)
        require_finite_nonnegative_float("reentry_cooldown_seconds", self.reentry_cooldown_seconds)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


def _as_float(value: object | None) -> float | None:
    if value is None:
        return None
    if callable(value):
        value = value()
    as_double = getattr(value, "as_double", None)
    if callable(as_double):
        return float(as_double())
    return float(value)


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


class BookMicropriceImbalanceStrategy(LongOnlyPredictionMarketStrategy):
    """
    Long-only L2 microstructure strategy for prediction-market order books.

    The entry signal requires a tight enough spread, bid-side depth imbalance,
    and a microprice edge above the midpoint. It exits on weakening book
    pressure, or on configured take-profit/stop-loss thresholds.
    """

    def __init__(self, config: _MicropriceImbalanceConfig) -> None:
        super().__init__(config)
        self._holding_updates: int = 0
        self._reentry_cooldown_remaining: int = 0
        self._last_book_ts_ns: int | None = None
        self._entry_ts_ns: int | None = None
        self._flat_ts_ns: int | None = None

    def _subscribe(self) -> None:
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def _expected_entry_price(self, order_book: OrderBook) -> float | None:
        if self._instrument is None:
            return None
        try:
            quantity = self._instrument.make_qty(float(self.config.trade_size), round_down=True)
        except ValueError:
            return None
        avg_px = float(order_book.get_avg_px_for_quantity(quantity, OrderSide.BUY))
        return avg_px if avg_px > 0 else None

    def _depth_sum(self, levels: list[object]) -> float:
        depth = 0.0
        for level in levels[: int(self.config.depth_levels)]:
            size = _as_float(getattr(level, "size", None))
            if size is not None and size > 0.0:
                depth += size
        return depth

    def on_order_book(self, order_book: OrderBook) -> None:
        bid = _as_float(order_book.best_bid_price())
        ask = _as_float(order_book.best_ask_price())
        spread = _as_float(order_book.spread())
        midpoint = _as_float(order_book.midpoint())
        if bid is None or ask is None or spread is None or midpoint is None:
            return
        if ask <= bid or spread <= 0.0:
            return

        bid_depth = self._depth_sum(order_book.bids())
        ask_depth = self._depth_sum(order_book.asks())
        total_depth = bid_depth + ask_depth
        if bid_depth <= 0.0 or ask_depth <= 0.0 or total_depth <= 0.0:
            return

        imbalance = bid_depth / total_depth
        expected_entry_price = self._expected_entry_price(order_book)
        ask_size = _as_float(order_book.best_ask_size())
        bid_size = _as_float(order_book.best_bid_size())
        if ask_size is None or bid_size is None or ask_size <= 0.0 or bid_size <= 0.0:
            return

        bbo_depth = bid_size + ask_size
        microprice = ((ask * bid_size) + (bid * ask_size)) / bbo_depth
        microprice_edge = microprice - midpoint
        self._last_book_ts_ns = _as_int(getattr(order_book, "ts_last", None))

        self._on_book_signal(
            bid=bid,
            ask=ask,
            spread=spread,
            imbalance=imbalance,
            microprice_edge=microprice_edge,
            expected_entry_price=expected_entry_price,
            entry_visible_size=ask_size,
            exit_visible_size=bid_size,
            current_ts_ns=self._last_book_ts_ns,
        )

    def _on_book_signal(
        self,
        *,
        bid: float,
        ask: float,
        spread: float,
        imbalance: float,
        microprice_edge: float,
        expected_entry_price: float | None,
        entry_visible_size: float | None,
        exit_visible_size: float | None,
        current_ts_ns: int | None = None,
    ) -> None:
        if ask <= bid or spread <= 0.0:
            return
        entry_reference_price = expected_entry_price if expected_entry_price is not None else ask
        self._remember_market_context(
            entry_reference_price=entry_reference_price,
            entry_visible_size=entry_visible_size,
            exit_visible_size=exit_visible_size,
        )
        if self._pending:
            return

        if not self._in_position():
            if self._reentry_cooldown_remaining > 0:
                self._reentry_cooldown_remaining -= 1
                return
            if not self._reentry_cooldown_elapsed(current_ts_ns):
                return
            if entry_reference_price > float(self.config.max_entry_price):
                return
            if spread > float(self.config.max_spread):
                return
            if imbalance < float(self.config.entry_imbalance):
                return
            if microprice_edge < float(self.config.min_microprice_edge):
                return
            if expected_entry_price is not None and expected_entry_price - ask > float(
                self.config.max_expected_slippage
            ):
                return

            self._submit_entry(
                reference_price=entry_reference_price, visible_size=entry_visible_size
            )
            return

        self._holding_updates += 1
        if self._risk_exit(
            price=bid,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if self._holding_updates < int(self.config.min_holding_updates):
            return
        if not self._min_holding_elapsed(current_ts_ns):
            return

        if imbalance <= float(self.config.exit_imbalance) or microprice_edge <= -float(
            self.config.min_microprice_edge
        ):
            self._submit_exit()

    def _seconds_elapsed(
        self, *, start_ts_ns: int | None, current_ts_ns: int | None, seconds: float
    ) -> bool:
        if seconds <= 0.0:
            return True
        if start_ts_ns is None or current_ts_ns is None:
            return False
        elapsed_ns = current_ts_ns - start_ts_ns
        required_ns = int(seconds * _NANOSECONDS_PER_SECOND)
        return elapsed_ns >= required_ns

    def _min_holding_elapsed(self, current_ts_ns: int | None) -> bool:
        return self._seconds_elapsed(
            start_ts_ns=self._entry_ts_ns,
            current_ts_ns=current_ts_ns,
            seconds=float(self.config.min_holding_seconds),
        )

    def _reentry_cooldown_elapsed(self, current_ts_ns: int | None) -> bool:
        if float(self.config.reentry_cooldown_seconds) <= 0.0 or self._flat_ts_ns is None:
            return True
        return self._seconds_elapsed(
            start_ts_ns=self._flat_ts_ns,
            current_ts_ns=current_ts_ns,
            seconds=float(self.config.reentry_cooldown_seconds),
        )

    def _fill_ts_ns(self, event: object) -> int | None:
        return _as_int(getattr(event, "ts_event", None)) or self._last_book_ts_ns

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._holding_updates = 0
            self._reentry_cooldown_remaining = 0
            if self._entry_ts_ns is None:
                self._entry_ts_ns = self._fill_ts_ns(event)
            self._flat_ts_ns = None
        else:
            flat_after_fill = self._entry_qty_sum <= 0 or not self._in_position()
            if flat_after_fill:
                self._holding_updates = 0
                self._reentry_cooldown_remaining = int(self.config.reentry_cooldown_updates)
                self._entry_ts_ns = None
                self._flat_ts_ns = self._fill_ts_ns(event)

    def on_reset(self) -> None:
        super().on_reset()
        self._holding_updates = 0
        self._reentry_cooldown_remaining = 0
        self._last_book_ts_ns = None
        self._entry_ts_ns = None
        self._flat_ts_ns = None
