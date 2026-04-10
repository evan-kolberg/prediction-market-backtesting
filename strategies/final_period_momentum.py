# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from strategies.core import LongOnlyPredictionMarketStrategy
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


class _FinalPeriodMomentumConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    market_close_time_ns: int
    final_period_minutes: int
    entry_price: float
    take_profit_price: float
    stop_loss_price: float


class BarFinalPeriodMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    market_close_time_ns: int = 0
    final_period_minutes: int = 30
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50


class TradeTickFinalPeriodMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(100)
    market_close_time_ns: int = 0
    final_period_minutes: int = 30
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50


class QuoteTickFinalPeriodMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(100)
    market_close_time_ns: int = 0
    final_period_minutes: int = 30
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50


class _FinalPeriodMomentumBase(LongOnlyPredictionMarketStrategy):
    """
    Buy late-game strength above a hard threshold and exit on target or stop.
    """

    def __init__(self, config: _FinalPeriodMomentumConfig) -> None:
        super().__init__(config)
        self._last_price: float | None = None
        self._has_entered: bool = False

    def _final_period_start_ns(self) -> int:
        close_time_ns = int(self.config.market_close_time_ns)
        if close_time_ns <= 0:
            return 0

        final_period_ns = int(self.config.final_period_minutes) * 60 * 1_000_000_000
        return max(0, close_time_ns - final_period_ns)

    def _is_in_final_period(self, ts_event_ns: int) -> bool:
        close_time_ns = int(self.config.market_close_time_ns)
        if close_time_ns <= 0:
            return False
        return self._final_period_start_ns() <= ts_event_ns <= close_time_ns

    def _crossed_above_entry(self, previous_price: float | None, price: float) -> bool:
        if previous_price is None:
            return False
        return previous_price < float(self.config.entry_price) <= price

    def _on_price(
        self, *, price: float, ts_event_ns: int, entry_price: float | None = None, visible_size: float | None = None
    ) -> None:
        previous_price = self._last_price
        self._last_price = price
        reference_price = price if entry_price is None else entry_price

        if self._pending:
            return

        if not self._in_position():
            if self._has_entered:
                return
            if not self._is_in_final_period(ts_event_ns):
                return
            if self._crossed_above_entry(previous_price, price):
                self._submit_entry(reference_price=reference_price, visible_size=visible_size)
            return

        if int(self.config.market_close_time_ns) > 0 and ts_event_ns >= int(self.config.market_close_time_ns):
            self._submit_exit()
            return

        if price >= float(self.config.take_profit_price) or price <= float(self.config.stop_loss_price):
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._last_price = None
        self._has_entered = False

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._has_entered = True


class BarFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(price=close, ts_event_ns=int(bar.ts_event), entry_price=close)


class TradeTickFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(price=price, ts_event_ns=int(tick.ts_event), entry_price=price)


class QuoteTickFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._on_price(
            price=(float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            ts_event_ns=int(tick.ts_event),
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
        )
