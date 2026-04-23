# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Derived from NautilusTrader prediction-market example code.
#  Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-16.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from collections import deque
from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies.core import LongOnlyPredictionMarketStrategy


class _PanicFadeConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    drop_window: int
    min_drop: float
    panic_price: float
    rebound_exit: float
    max_holding_periods: int
    take_profit: float
    stop_loss: float


class BarPanicFadeConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    drop_window: int = 12
    min_drop: float = 0.08
    panic_price: float = 0.30
    rebound_exit: float = 0.45
    max_holding_periods: int = 36
    take_profit: float = 0.06
    stop_loss: float = 0.03


class TradeTickPanicFadeConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    drop_window: int = 80
    min_drop: float = 0.06
    panic_price: float = 0.30
    rebound_exit: float = 0.42
    max_holding_periods: int = 500
    take_profit: float = 0.04
    stop_loss: float = 0.03


class QuoteTickPanicFadeConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    drop_window: int = 80
    min_drop: float = 0.06
    panic_price: float = 0.30
    rebound_exit: float = 0.42
    max_holding_periods: int = 500
    take_profit: float = 0.04
    stop_loss: float = 0.03


class _PanicFadeBase(LongOnlyPredictionMarketStrategy):
    """
    Buy panic selloffs below a threshold and exit on rebound, timeout, or risk.
    """

    def __init__(self, config: _PanicFadeConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=int(self.config.drop_window))
        self._holding_periods: int = 0

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
            self._prices.append(price)
            return

        if not self._in_position():
            if len(self._prices) < int(self.config.drop_window):
                self._prices.append(price)
                return
            peak = max(self._prices)
            drop = peak - price
            if price <= float(self.config.panic_price) and drop >= float(self.config.min_drop):
                self._submit_entry(
                    reference_price=reference_price,
                    visible_size=visible_size,
                )
            self._prices.append(price)
            return

        self._holding_periods += 1
        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            self._prices.append(price)
            return

        if price >= float(self.config.rebound_exit) or self._holding_periods >= int(
            self.config.max_holding_periods
        ):
            self._submit_exit()
        self._prices.append(price)

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._holding_periods = 0

    def on_reset(self) -> None:
        super().on_reset()
        self._prices.clear()
        self._holding_periods = 0


class BarPanicFadeStrategy(_PanicFadeBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class TradeTickPanicFadeStrategy(_PanicFadeBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price, entry_price=price, visible_size=float(tick.size))


class QuoteTickPanicFadeStrategy(_PanicFadeBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._remember_market_context(
            entry_reference_price=float(tick.ask_price),
            entry_visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
        self._on_price(
            (float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
