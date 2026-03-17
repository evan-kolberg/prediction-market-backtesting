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
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from collections import deque
from decimal import Decimal

from nautilus_trader.examples.strategies.prediction_market.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


class TradeTickVWAPReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    vwap_window: int = 80
    entry_threshold: float = 0.008
    exit_threshold: float = 0.002
    min_tick_size: float = 0.0
    take_profit: float = 0.015
    stop_loss: float = 0.02


class TradeTickVWAPReversionStrategy(LongOnlyPredictionMarketStrategy):
    """
    Trade-tick VWAP reversion strategy suited to intraday prediction-market noise.
    """

    def __init__(self, config: TradeTickVWAPReversionConfig) -> None:
        super().__init__(config)
        self._window: deque[tuple[float, float]] = deque(maxlen=int(self.config.vwap_window))
        self._weighted_sum: float = 0.0
        self._size_sum: float = 0.0

    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def _append_tick(self, *, price: float, size: float) -> None:
        if len(self._window) == self._window.maxlen:
            old_price, old_size = self._window.popleft()
            self._weighted_sum -= old_price * old_size
            self._size_sum -= old_size

        self._window.append((price, size))
        self._weighted_sum += price * size
        self._size_sum += size

    def on_trade_tick(self, tick: TradeTick) -> None:
        if self._pending:
            return

        price = float(tick.price)
        size = float(tick.size)
        if size < float(self.config.min_tick_size):
            return

        self._append_tick(price=price, size=size)
        if len(self._window) < int(self.config.vwap_window) or self._size_sum <= 0.0:
            return

        vwap = self._weighted_sum / self._size_sum
        if not self._in_position():
            if price <= vwap - float(self.config.entry_threshold):
                self._submit_entry()
            return

        if self._risk_exit(
            price=price,
            take_profit=self.config.take_profit,
            stop_loss=self.config.stop_loss,
        ):
            return

        if price >= vwap - float(self.config.exit_threshold):
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._window.clear()
        self._weighted_sum = 0.0
        self._size_sum = 0.0
