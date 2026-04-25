# -------------------------------------------------------------------------------------------------
# Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
# https://nautechsystems.io
#
# Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing
# permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
# Derived from NautilusTrader prediction-market example code.
# Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_finite_nonnegative_float,
    require_less,
    require_positive_decimal,
    require_positive_int,
    require_rsi,
)
from strategies.core import LongOnlyPredictionMarketStrategy


class _RSIReversionConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    period: int
    entry_rsi: float
    exit_rsi: float
    take_profit: float
    stop_loss: float


class BarRSIReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    period: int = 14
    entry_rsi: float = 30.0
    exit_rsi: float = 55.0
    take_profit: float = 0.03
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("period", self.period)
        require_rsi("entry_rsi", self.entry_rsi)
        require_rsi("exit_rsi", self.exit_rsi)
        require_less("entry_rsi", self.entry_rsi, "exit_rsi", self.exit_rsi)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


class BookRSIReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    period: int = 40
    entry_rsi: float = 25.0
    exit_rsi: float = 52.0
    take_profit: float = 0.02
    stop_loss: float = 0.015

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("period", self.period)
        require_rsi("entry_rsi", self.entry_rsi)
        require_rsi("exit_rsi", self.exit_rsi)
        require_less("entry_rsi", self.entry_rsi, "exit_rsi", self.exit_rsi)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


class _RSIReversionBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only RSI pullback strategy for mean reversion in prediction-market prices.
    """

    def __init__(self, config: _RSIReversionConfig) -> None:
        super().__init__(config)
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._last_price: float | None = None
        self._seed_gains: list[float] = []
        self._seed_losses: list[float] = []

    def _update_rsi(self, price: float) -> float | None:
        if self._last_price is None:
            self._last_price = price
            return None

        change = price - self._last_price
        self._last_price = price
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        period = int(self.config.period)

        if self._avg_gain is None or self._avg_loss is None:
            self._seed_gains.append(gain)
            self._seed_losses.append(loss)
            if len(self._seed_gains) < period:
                return None
            self._avg_gain = sum(self._seed_gains) / float(period)
            self._avg_loss = sum(self._seed_losses) / float(period)
        else:
            smoothing = float(period)
            self._avg_gain = ((self._avg_gain * (smoothing - 1.0)) + gain) / smoothing
            self._avg_loss = ((self._avg_loss * (smoothing - 1.0)) + loss) / smoothing

        if self._avg_loss == 0.0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _on_price(
        self,
        price: float,
        *,
        entry_price: float | None = None,
        visible_size: float | None = None,
        exit_visible_size: float | None = None,
    ) -> None:
        reference_price = price if entry_price is None else entry_price
        self._remember_market_context(
            entry_reference_price=reference_price,
            entry_visible_size=visible_size,
            exit_visible_size=exit_visible_size,
        )
        if self._pending:
            self._update_rsi(price)
            return

        rsi = self._update_rsi(price)
        if rsi is None:
            return

        if not self._in_position():
            if rsi <= float(self.config.entry_rsi):
                self._submit_entry(
                    reference_price=reference_price,
                    visible_size=visible_size,
                )
            return

        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            return

        if rsi >= float(self.config.exit_rsi):
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._avg_gain = None
        self._avg_loss = None
        self._last_price = None
        self._seed_gains.clear()
        self._seed_losses.clear()


class BarRSIReversionStrategy(_RSIReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class BookRSIReversionStrategy(_RSIReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def on_order_book(self, order_book) -> None:  # type: ignore[no-untyped-def]
        bid = order_book.best_bid_price()
        ask = order_book.best_ask_price()
        if bid is None or ask is None:
            return
        mid = (float(bid) + float(ask)) / 2.0
        ask_size = order_book.best_ask_size()
        bid_size = order_book.best_bid_size()
        self._remember_market_context(
            entry_reference_price=float(ask),
            entry_visible_size=float(ask_size) if ask_size is not None else None,
            exit_visible_size=float(bid_size) if bid_size is not None else None,
        )
        self._on_price(
            mid,
            entry_price=float(ask),
            visible_size=float(ask_size) if ask_size is not None else None,
            exit_visible_size=float(bid_size) if bid_size is not None else None,
        )
