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
from math import sqrt
from typing import Protocol

from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies.core import LongOnlyPredictionMarketStrategy


class _BreakoutConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    window: int
    breakout_std: float
    breakout_buffer: float
    mean_reversion_buffer: float
    min_holding_periods: int
    reentry_cooldown: int
    max_entry_price: float
    take_profit: float
    stop_loss: float


class BarBreakoutConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    window: int = 30
    breakout_std: float = 1.25
    breakout_buffer: float = 0.0
    mean_reversion_buffer: float = 0.0
    min_holding_periods: int = 0
    reentry_cooldown: int = 0
    max_entry_price: float = 0.92
    take_profit: float = 0.02
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError(f"window must be > 0, got {self.window}")
        if self.trade_size <= 0:
            raise ValueError(f"trade_size must be > 0, got {self.trade_size}")
        if self.breakout_std <= 0:
            raise ValueError(f"breakout_std must be > 0, got {self.breakout_std}")
        if self.take_profit < 0:
            raise ValueError(f"take_profit must be >= 0, got {self.take_profit}")
        if self.stop_loss < 0:
            raise ValueError(f"stop_loss must be >= 0, got {self.stop_loss}")


class TradeTickBreakoutConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    window: int = 120
    breakout_std: float = 1.5
    breakout_buffer: float = 0.0
    mean_reversion_buffer: float = 0.0
    min_holding_periods: int = 0
    reentry_cooldown: int = 0
    max_entry_price: float = 0.92
    take_profit: float = 0.015
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError(f"window must be > 0, got {self.window}")
        if self.trade_size <= 0:
            raise ValueError(f"trade_size must be > 0, got {self.trade_size}")
        if self.breakout_std <= 0:
            raise ValueError(f"breakout_std must be > 0, got {self.breakout_std}")
        if self.take_profit < 0:
            raise ValueError(f"take_profit must be >= 0, got {self.take_profit}")
        if self.stop_loss < 0:
            raise ValueError(f"stop_loss must be >= 0, got {self.stop_loss}")


class QuoteTickBreakoutConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    window: int = 120
    breakout_std: float = 1.5
    breakout_buffer: float = 0.001
    mean_reversion_buffer: float = 0.0005
    min_holding_periods: int = 20
    reentry_cooldown: int = 80
    max_entry_price: float = 0.92
    take_profit: float = 0.015
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError(f"window must be > 0, got {self.window}")
        if self.trade_size <= 0:
            raise ValueError(f"trade_size must be > 0, got {self.trade_size}")
        if self.breakout_std <= 0:
            raise ValueError(f"breakout_std must be > 0, got {self.breakout_std}")
        if self.take_profit < 0:
            raise ValueError(f"take_profit must be >= 0, got {self.take_profit}")
        if self.stop_loss < 0:
            raise ValueError(f"stop_loss must be >= 0, got {self.stop_loss}")


class _BreakoutBase(LongOnlyPredictionMarketStrategy):
    """
    Long-only breakout strategy with bounded entries for binary-outcome markets.
    """

    def __init__(self, config: _BreakoutConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=int(self.config.window))
        self._holding_periods: int = 0
        self._last_price: float | None = None
        self._reentry_cooldown_remaining: int = 0

    def _append_price(self, price: float) -> None:
        self._prices.append(price)
        self._last_price = price

    def _breakout_buffer(self) -> float:
        return float(self.config.breakout_buffer)

    def _mean_reversion_buffer(self) -> float:
        return float(self.config.mean_reversion_buffer)

    def _min_holding_periods(self) -> int:
        return int(self.config.min_holding_periods)

    def _reentry_cooldown(self) -> int:
        return int(self.config.reentry_cooldown)

    def _requires_fresh_breakout_cross(self) -> bool:
        return (
            self._breakout_buffer() > 0.0
            or self._mean_reversion_buffer() > 0.0
            or self._min_holding_periods() > 0
            or self._reentry_cooldown() > 0
        )

    def _on_price(
        self,
        price: float,
        *,
        entry_price: float | None = None,
        visible_size: float | None = None,
        exit_visible_size: float | None = None,
    ) -> None:
        previous_price = self._last_price
        prior_window = list(self._prices)
        reference_price = price if entry_price is None else entry_price
        self._remember_market_context(
            entry_reference_price=reference_price,
            entry_visible_size=visible_size,
            exit_visible_size=exit_visible_size,
        )

        if len(prior_window) < int(self.config.window) or self._pending:
            self._append_price(price)
            return

        mean = sum(prior_window) / len(prior_window)
        variance = sum((value - mean) ** 2 for value in prior_window) / max(
            1, len(prior_window) - 1
        )
        std = sqrt(variance)
        breakout_level = mean + float(self.config.breakout_std) * std + self._breakout_buffer()
        exit_level = mean - self._mean_reversion_buffer()

        if not self._in_position():
            if self._reentry_cooldown_remaining > 0:
                self._reentry_cooldown_remaining -= 1
                self._append_price(price)
                return

            crossed_breakout = previous_price is not None and previous_price < breakout_level
            if (
                price >= breakout_level
                and price <= float(self.config.max_entry_price)
                and (crossed_breakout or not self._requires_fresh_breakout_cross())
            ):
                self._submit_entry(reference_price=reference_price, visible_size=visible_size)
            self._append_price(price)
            return

        self._holding_periods += 1
        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            self._append_price(price)
            return

        if self._holding_periods >= self._min_holding_periods() and price <= exit_level:
            self._submit_exit()
        self._append_price(price)

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._holding_periods = 0
            self._reentry_cooldown_remaining = 0
        else:
            self._holding_periods = 0
            self._reentry_cooldown_remaining = self._reentry_cooldown()

    def on_reset(self) -> None:
        super().on_reset()
        self._prices.clear()
        self._holding_periods = 0
        self._last_price = None
        self._reentry_cooldown_remaining = 0


class BarBreakoutStrategy(_BreakoutBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(close, entry_price=close)


class TradeTickBreakoutStrategy(_BreakoutBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price, entry_price=price, visible_size=float(tick.size))


class QuoteTickBreakoutStrategy(_BreakoutBase):
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
