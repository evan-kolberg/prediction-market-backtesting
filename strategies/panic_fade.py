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

from __future__ import annotations

from collections import deque
from decimal import Decimal
from typing import Protocol

from strategies.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


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

    def _on_price(self, price: float) -> None:
        self._prices.append(price)
        if self._pending:
            return

        if not self._in_position():
            if len(self._prices) < int(self.config.drop_window):
                return
            peak = max(self._prices)
            drop = peak - price
            if price <= float(self.config.panic_price) and drop >= float(
                self.config.min_drop
            ):
                self._submit_entry()
            return

        self._holding_periods += 1
        if self._risk_exit(
            price=price,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if price >= float(self.config.rebound_exit) or self._holding_periods >= int(
            self.config.max_holding_periods
        ):
            self._submit_exit()

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
        self._on_price(float(bar.close))


class TradeTickPanicFadeStrategy(_PanicFadeBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        self._on_price(float(tick.price))


class QuoteTickPanicFadeStrategy(_PanicFadeBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._on_price((float(tick.bid_price) + float(tick.ask_price)) / 2.0)
