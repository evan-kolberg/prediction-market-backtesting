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
from math import sqrt
from typing import Protocol

from strategies.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


class _BreakoutConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    window: int
    breakout_std: float
    max_entry_price: float
    take_profit: float
    stop_loss: float


class BarBreakoutConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    window: int = 30
    breakout_std: float = 1.25
    max_entry_price: float = 0.92
    take_profit: float = 0.02
    stop_loss: float = 0.02


class TradeTickBreakoutConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    window: int = 120
    breakout_std: float = 1.5
    max_entry_price: float = 0.92
    take_profit: float = 0.015
    stop_loss: float = 0.02


class _BreakoutBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only breakout strategy with bounded entries for binary-outcome markets.
    """

    def __init__(self, config: _BreakoutConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=int(self.config.window))

    def _on_price(self, price: float) -> None:
        self._prices.append(price)
        if len(self._prices) < int(self.config.window) or self._pending:
            return

        window = list(self._prices)
        mean = sum(window) / len(window)
        variance = sum((value - mean) ** 2 for value in window) / len(window)
        std = sqrt(variance)
        breakout_level = mean + float(self.config.breakout_std) * std

        if not self._in_position():
            if price >= breakout_level and price <= float(self.config.max_entry_price):
                self._submit_entry()
            return

        if self._risk_exit(
            price=price,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if price <= mean:
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._prices.clear()


class BarBreakoutStrategy(_BreakoutBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        self._on_price(float(bar.close))


class TradeTickBreakoutStrategy(_BreakoutBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        self._on_price(float(tick.price))
