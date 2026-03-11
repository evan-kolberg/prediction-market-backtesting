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

from decimal import Decimal
from typing import Protocol

from strategies.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


class _EMACrossoverConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    fast_period: int
    slow_period: int
    entry_buffer: float
    take_profit: float
    stop_loss: float


class BarEMACrossoverConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    fast_period: int = 8
    slow_period: int = 21
    entry_buffer: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0


class TradeTickEMACrossoverConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    fast_period: int = 20
    slow_period: int = 60
    entry_buffer: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0


class _EMACrossoverBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only trend strategy for prediction-market price momentum.
    """

    def __init__(self, config: _EMACrossoverConfig) -> None:
        super().__init__(config)
        self._fast_ema: float | None = None
        self._slow_ema: float | None = None
        self._warmup: int = 0
        self._warmup_needed = max(
            int(self.config.fast_period), int(self.config.slow_period)
        )
        self._alpha_fast = 2.0 / (float(self.config.fast_period) + 1.0)
        self._alpha_slow = 2.0 / (float(self.config.slow_period) + 1.0)

    def _on_price(self, price: float) -> None:
        if self._fast_ema is None or self._slow_ema is None:
            self._fast_ema = price
            self._slow_ema = price
            self._warmup = 1
            return

        self._fast_ema = (
            self._alpha_fast * price + (1.0 - self._alpha_fast) * self._fast_ema
        )
        self._slow_ema = (
            self._alpha_slow * price + (1.0 - self._alpha_slow) * self._slow_ema
        )
        self._warmup += 1

        if self._warmup < self._warmup_needed or self._pending:
            return

        assert self._fast_ema is not None
        assert self._slow_ema is not None

        if not self._in_position():
            if self._fast_ema >= self._slow_ema + self.config.entry_buffer:
                self._submit_entry()
            return

        if self._risk_exit(
            price=price,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if self._fast_ema <= self._slow_ema - self.config.entry_buffer:
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._fast_ema = None
        self._slow_ema = None
        self._warmup = 0


class BarEMACrossoverStrategy(_EMACrossoverBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        self._on_price(float(bar.close))


class TradeTickEMACrossoverStrategy(_EMACrossoverBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        self._on_price(float(tick.price))
