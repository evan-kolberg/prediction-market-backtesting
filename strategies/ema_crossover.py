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
#  Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-15.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_finite_nonnegative_float,
    require_less,
    require_positive_decimal,
    require_positive_int,
)
from strategies.core import LongOnlyPredictionMarketStrategy


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

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("fast_period", self.fast_period)
        require_positive_int("slow_period", self.slow_period)
        require_less("fast_period", self.fast_period, "slow_period", self.slow_period)
        require_finite_nonnegative_float("entry_buffer", self.entry_buffer)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


class TradeTickEMACrossoverConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    fast_period: int = 20
    slow_period: int = 60
    entry_buffer: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("fast_period", self.fast_period)
        require_positive_int("slow_period", self.slow_period)
        require_less("fast_period", self.fast_period, "slow_period", self.slow_period)
        require_finite_nonnegative_float("entry_buffer", self.entry_buffer)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


class QuoteTickEMACrossoverConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    fast_period: int = 20
    slow_period: int = 60
    entry_buffer: float = 0.0
    take_profit: float = 0.0
    stop_loss: float = 0.0

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_positive_int("fast_period", self.fast_period)
        require_positive_int("slow_period", self.slow_period)
        require_less("fast_period", self.fast_period, "slow_period", self.slow_period)
        require_finite_nonnegative_float("entry_buffer", self.entry_buffer)
        require_finite_nonnegative_float("take_profit", self.take_profit)
        require_finite_nonnegative_float("stop_loss", self.stop_loss)


class _EMACrossoverBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only trend strategy for prediction-market price momentum.
    """

    def __init__(self, config: _EMACrossoverConfig) -> None:
        super().__init__(config)
        self._fast_ema: float | None = None
        self._slow_ema: float | None = None
        self._seed_prices: list[float] = []
        self._warmup: int = 0
        self._warmup_needed = max(int(self.config.fast_period), int(self.config.slow_period))
        self._alpha_fast = 2.0 / (float(self.config.fast_period) + 1.0)
        self._alpha_slow = 2.0 / (float(self.config.slow_period) + 1.0)

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

        self._seed_prices.append(price)
        initialized_fast = False
        initialized_slow = False
        if self._fast_ema is None and len(self._seed_prices) >= int(self.config.fast_period):
            window = self._seed_prices[-int(self.config.fast_period) :]
            self._fast_ema = sum(window) / len(window)
            initialized_fast = True
        if self._slow_ema is None and len(self._seed_prices) >= int(self.config.slow_period):
            window = self._seed_prices[-int(self.config.slow_period) :]
            self._slow_ema = sum(window) / len(window)
            initialized_slow = True

        if self._fast_ema is None or self._slow_ema is None:
            self._warmup = len(self._seed_prices)
            return
        if initialized_fast or initialized_slow:
            self._warmup = len(self._seed_prices)
            if self._warmup <= self._warmup_needed:
                return

        self._fast_ema = self._alpha_fast * price + (1.0 - self._alpha_fast) * self._fast_ema
        self._slow_ema = self._alpha_slow * price + (1.0 - self._alpha_slow) * self._slow_ema
        self._warmup = len(self._seed_prices)

        if self._warmup < self._warmup_needed or self._pending:
            return

        assert self._fast_ema is not None
        assert self._slow_ema is not None

        if not self._in_position():
            if self._fast_ema >= self._slow_ema + self.config.entry_buffer:
                self._submit_entry(reference_price=reference_price, visible_size=visible_size)
            return

        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            return

        if self._fast_ema <= self._slow_ema - self.config.entry_buffer:
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._fast_ema = None
        self._slow_ema = None
        self._seed_prices.clear()
        self._warmup = 0


class BarEMACrossoverStrategy(_EMACrossoverBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class TradeTickEMACrossoverStrategy(_EMACrossoverBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price, entry_price=price, visible_size=None)


class QuoteTickEMACrossoverStrategy(_EMACrossoverBase):
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
