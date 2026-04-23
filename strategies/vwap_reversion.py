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

from nautilus_trader.model.data import QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies.core import LongOnlyPredictionMarketStrategy


class TradeTickVWAPReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    vwap_window: int = 80
    entry_threshold: float = 0.008
    exit_threshold: float = 0.002
    min_tick_size: float = 0.0
    take_profit: float = 0.015
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        if self.vwap_window <= 0:
            raise ValueError(f"vwap_window must be > 0, got {self.vwap_window}")
        if self.trade_size <= 0:
            raise ValueError(f"trade_size must be > 0, got {self.trade_size}")
        if self.entry_threshold < 0:
            raise ValueError(f"entry_threshold must be >= 0, got {self.entry_threshold}")
        if self.exit_threshold < 0:
            raise ValueError(f"exit_threshold must be >= 0, got {self.exit_threshold}")
        if self.min_tick_size < 0:
            raise ValueError(f"min_tick_size must be >= 0, got {self.min_tick_size}")
        if self.take_profit < 0:
            raise ValueError(f"take_profit must be >= 0, got {self.take_profit}")
        if self.stop_loss < 0:
            raise ValueError(f"stop_loss must be >= 0, got {self.stop_loss}")


class QuoteTickVWAPReversionConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(1)
    vwap_window: int = 80
    entry_threshold: float = 0.008
    exit_threshold: float = 0.002
    min_tick_size: float = 0.0
    take_profit: float = 0.015
    stop_loss: float = 0.02

    def __post_init__(self) -> None:
        if self.vwap_window <= 0:
            raise ValueError(f"vwap_window must be > 0, got {self.vwap_window}")
        if self.trade_size <= 0:
            raise ValueError(f"trade_size must be > 0, got {self.trade_size}")
        if self.entry_threshold < 0:
            raise ValueError(f"entry_threshold must be >= 0, got {self.entry_threshold}")
        if self.exit_threshold < 0:
            raise ValueError(f"exit_threshold must be >= 0, got {self.exit_threshold}")
        if self.min_tick_size < 0:
            raise ValueError(f"min_tick_size must be >= 0, got {self.min_tick_size}")
        if self.take_profit < 0:
            raise ValueError(f"take_profit must be >= 0, got {self.take_profit}")
        if self.stop_loss < 0:
            raise ValueError(f"stop_loss must be >= 0, got {self.stop_loss}")


class _VWAPReversionBase(LongOnlyPredictionMarketStrategy):
    """
    Trade-tick VWAP reversion strategy suited to intraday prediction-market noise.
    """

    def __init__(self, config: TradeTickVWAPReversionConfig | QuoteTickVWAPReversionConfig) -> None:
        super().__init__(config)
        self._window: deque[tuple[float, float]] = deque(maxlen=int(self.config.vwap_window))
        self._weighted_sum: float = 0.0
        self._size_sum: float = 0.0

    def _append_point(self, *, price: float, size: float) -> None:
        if len(self._window) == self._window.maxlen:
            old_price, old_size = self._window.popleft()
            self._weighted_sum -= old_price * old_size
            self._size_sum -= old_size

        self._window.append((price, size))
        self._weighted_sum += price * size
        self._size_sum += size

        # Periodically recompute from scratch to prevent float drift.
        if len(self._window) >= self._window.maxlen and len(self._window) % 256 == 0:
            self._recompute_sums()

    def _recompute_sums(self) -> None:
        self._weighted_sum = sum(p * s for p, s in self._window)
        self._size_sum = sum(s for _, s in self._window)

    def _on_price_size(
        self,
        *,
        price: float,
        size: float,
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
            return

        if size < float(self.config.min_tick_size):
            return

        if len(self._window) < int(self.config.vwap_window) or self._size_sum <= 0.0:
            self._append_point(price=price, size=size)
            return

        vwap = self._weighted_sum / self._size_sum
        if not self._in_position():
            if price <= vwap - float(self.config.entry_threshold):
                self._submit_entry(
                    reference_price=reference_price,
                    visible_size=visible_size,
                )
            self._append_point(price=price, size=size)
            return

        if self._risk_exit(
            price=price, take_profit=self.config.take_profit, stop_loss=self.config.stop_loss
        ):
            self._append_point(price=price, size=size)
            return

        if price >= vwap - float(self.config.exit_threshold):
            self._submit_exit()
        self._append_point(price=price, size=size)

    def on_reset(self) -> None:
        super().on_reset()
        self._window.clear()
        self._weighted_sum = 0.0
        self._size_sum = 0.0


class TradeTickVWAPReversionStrategy(_VWAPReversionBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price_size(
            price=price,
            size=float(tick.size),
            entry_price=price,
            visible_size=float(tick.size),
        )


class QuoteTickVWAPReversionStrategy(_VWAPReversionBase):
    """
    Quote-tick variant using midpoint price and average top-of-book size as a proxy.
    """

    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._remember_market_context(
            entry_reference_price=float(tick.ask_price),
            entry_visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
        self._on_price_size(
            price=(float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            size=(float(tick.bid_size) + float(tick.ask_size)) / 2.0,
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
