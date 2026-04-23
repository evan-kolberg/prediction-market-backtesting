# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_nonnegative_int,
    require_positive_decimal,
    require_probability,
)
from strategies.core import LongOnlyPredictionMarketStrategy


class _ThresholdMomentumConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal
    activation_start_time_ns: int
    market_close_time_ns: int
    entry_price: float
    take_profit_price: float
    stop_loss_price: float


class BarThresholdMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal(1)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_nonnegative_int("activation_start_time_ns", self.activation_start_time_ns)
        require_nonnegative_int("market_close_time_ns", self.market_close_time_ns)
        require_probability("entry_price", self.entry_price)
        require_probability("take_profit_price", self.take_profit_price)
        require_probability("stop_loss_price", self.stop_loss_price)


class TradeTickThresholdMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(100)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_nonnegative_int("activation_start_time_ns", self.activation_start_time_ns)
        require_nonnegative_int("market_close_time_ns", self.market_close_time_ns)
        require_probability("entry_price", self.entry_price)
        require_probability("take_profit_price", self.take_profit_price)
        require_probability("stop_loss_price", self.stop_loss_price)


class QuoteTickThresholdMomentumConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(100)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    entry_price: float = 0.80
    take_profit_price: float = 0.92
    stop_loss_price: float = 0.50

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_nonnegative_int("activation_start_time_ns", self.activation_start_time_ns)
        require_nonnegative_int("market_close_time_ns", self.market_close_time_ns)
        require_probability("entry_price", self.entry_price)
        require_probability("take_profit_price", self.take_profit_price)
        require_probability("stop_loss_price", self.stop_loss_price)


class _ThresholdMomentumBase(LongOnlyPredictionMarketStrategy):
    """
    Buy once on a threshold breakout, then exit on target, stop, or market close.
    """

    def __init__(self, config: _ThresholdMomentumConfig) -> None:
        super().__init__(config)
        self._last_price: float | None = None
        self._has_entered: bool = False

    def _crossed_above_entry(self, previous_price: float | None, price: float) -> bool:
        if previous_price is None:
            return False
        return previous_price < float(self.config.entry_price) <= price

    def _entry_window_is_open(self, ts_event_ns: int) -> bool:
        activation_start_ns = int(self.config.activation_start_time_ns)
        if activation_start_ns > 0 and ts_event_ns < activation_start_ns:
            return False

        close_time_ns = int(self.config.market_close_time_ns)
        if close_time_ns > 0 and ts_event_ns > close_time_ns:
            return False

        return True

    def _on_price(
        self,
        *,
        price: float,
        ts_event_ns: int,
        entry_price: float | None = None,
        visible_size: float | None = None,
        exit_visible_size: float | None = None,
    ) -> None:
        previous_price = self._last_price
        self._last_price = price
        reference_price = price if entry_price is None else entry_price
        self._remember_market_context(
            entry_reference_price=reference_price,
            entry_visible_size=visible_size,
            exit_visible_size=exit_visible_size,
        )

        if self._pending:
            return

        if not self._in_position():
            if self._has_entered:
                return
            if not self._entry_window_is_open(ts_event_ns):
                return
            if self._crossed_above_entry(previous_price, price):
                self._submit_entry(reference_price=reference_price, visible_size=visible_size)
            return

        if int(self.config.market_close_time_ns) > 0 and ts_event_ns >= int(
            self.config.market_close_time_ns
        ):
            self._submit_exit()
            return

        if price >= float(self.config.take_profit_price) or price <= float(
            self.config.stop_loss_price
        ):
            self._submit_exit()

    def on_reset(self) -> None:
        super().on_reset()
        self._last_price = None
        self._has_entered = False

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._has_entered = True


class BarThresholdMomentumStrategy(_ThresholdMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._on_price(
            price=close,
            ts_event_ns=int(bar.ts_event),
            entry_price=close,
        )


class TradeTickThresholdMomentumStrategy(_ThresholdMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._on_price(
            price=price,
            ts_event_ns=int(tick.ts_event),
            entry_price=price,
            visible_size=None,
        )


class QuoteTickThresholdMomentumStrategy(_ThresholdMomentumBase):
    def _subscribe(self) -> None:
        self.subscribe_quote_ticks(self.config.instrument_id)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self._remember_market_context(
            entry_reference_price=float(tick.ask_price),
            entry_visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
        self._on_price(
            price=(float(tick.bid_price) + float(tick.ask_price)) / 2.0,
            ts_event_ns=int(tick.ts_event),
            entry_price=float(tick.ask_price),
            visible_size=float(tick.ask_size),
            exit_visible_size=float(tick.bid_size),
        )
