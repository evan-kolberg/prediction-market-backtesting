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
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies.core import LongOnlyPredictionMarketStrategy


class _MeanReversionConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    entry_threshold: float
    take_profit: float
    stop_loss: float


class BarMeanReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    window: int = 20
    entry_threshold: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0


class TradeTickMeanReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    vwap_window: int = 20
    entry_threshold: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0


class QuoteTickMeanReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    window: int = 20
    entry_threshold: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0


class _MeanReversionBase(LongOnlyPredictionMarketStrategy):
    """
    Single-instrument mean-reversion base with one open position max.
    """

    _window_field = "window"

    def __init__(self, config: _MeanReversionConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=self._window())

    def _window(self) -> int:
        return int(getattr(self.config, self._window_field))

    def _on_price(
        self, price: float, *, entry_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self._prices.append(price)
        if len(self._prices) < self._window() or self._pending:
            return

        rolling_avg = sum(self._prices) / len(self._prices)
        if not self._in_position():
            if price <= rolling_avg - self.config.entry_threshold:
                self._submit_entry(
                    reference_price=price if entry_price is None else entry_price,
                    visible_size=visible_size,
                )
            return

        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            return

    def on_reset(self) -> None:
        super().on_reset()
        self._prices.clear()


class BarMeanReversionStrategy(_MeanReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class TradeTickMeanReversionStrategy(_MeanReversionBase):
    _window_field = "vwap_window"

    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price, entry_price=price)


class QuoteTickMeanReversionStrategy(_MeanReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._on_price(
            (float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
        )
