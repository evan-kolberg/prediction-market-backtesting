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

from nautilus_trader.model.enums import BookType, OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import require_positive_decimal, require_probability
from strategies.core import LongOnlyPredictionMarketStrategy


class BookDeepValueHoldConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    entry_price_max: float = 0.25
    single_entry: bool = True

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_probability("entry_price_max", self.entry_price_max)


class _DeepValueHoldBase(LongOnlyPredictionMarketStrategy):
    """
    Buy when price is below a threshold and hold until strategy stop.
    """

    def __init__(self, config: BookDeepValueHoldConfig) -> None:
        super().__init__(config)
        self._entered_once: bool = False

    def _on_price(
        self,
        price: float,
        *,
        entry_price: float | None = None,
        visible_size: float | None = None,
        exit_visible_size: float | None = None,
    ) -> None:
        self._remember_market_context(
            entry_reference_price=price if entry_price is None else entry_price,
            entry_visible_size=visible_size,
            exit_visible_size=exit_visible_size,
        )
        if self._pending:
            return

        if self._in_position():
            return

        if self.config.single_entry and self._entered_once:
            return

        if price <= float(self.config.entry_price_max):
            self._submit_entry(
                reference_price=price if entry_price is None else entry_price,
                visible_size=visible_size,
            )

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._entered_once = True

    def on_reset(self) -> None:
        super().on_reset()
        self._entered_once = False


class BookDeepValueHoldStrategy(_DeepValueHoldBase):
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
