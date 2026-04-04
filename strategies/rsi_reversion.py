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

from strategies.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


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


class TradeTickRSIReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    period: int = 40
    entry_rsi: float = 25.0
    exit_rsi: float = 52.0
    take_profit: float = 0.02
    stop_loss: float = 0.015


class QuoteTickRSIReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    period: int = 40
    entry_rsi: float = 25.0
    exit_rsi: float = 52.0
    take_profit: float = 0.02
    stop_loss: float = 0.015


class _RSIReversionBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only RSI pullback strategy for mean reversion in prediction-market prices.
    """

    def __init__(self, config: _RSIReversionConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=int(self.config.period) + 1)

    def _compute_rsi(self) -> float | None:
        if len(self._prices) < int(self.config.period) + 1:
            return None

        gains = 0.0
        losses = 0.0
        last = None
        for value in self._prices:
            if last is None:
                last = value
                continue
            change = value - last
            if change > 0.0:
                gains += change
            else:
                losses -= change
            last = value

        period = float(self.config.period)
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0.0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _on_price(
        self,
        price: float,
        *,
        entry_price: float | None = None,
        visible_size: float | None = None,
    ) -> None:
        self._prices.append(price)
        if self._pending:
            return

        rsi = self._compute_rsi()
        if rsi is None:
            return

        if not self._in_position():
            if rsi <= float(self.config.entry_rsi):
                self._submit_entry(
                    reference_price=price if entry_price is None else entry_price,
                    visible_size=visible_size,
                )
            return

        if self._risk_exit(
            price=price,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if rsi >= float(self.config.exit_rsi):
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._prices.clear()


class BarRSIReversionStrategy(_RSIReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class TradeTickRSIReversionStrategy(_RSIReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price, entry_price=price)


class QuoteTickRSIReversionStrategy(_RSIReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._on_price(
            (float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
        )
