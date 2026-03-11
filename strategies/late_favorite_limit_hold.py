from __future__ import annotations

from decimal import Decimal

from strategies.core import (
    LongOnlyPredictionMarketStrategy,
)
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig


class TradeTickLateFavoriteLimitHoldConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(25)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    entry_price: float = 0.90


class TradeTickLateFavoriteLimitHoldStrategy(LongOnlyPredictionMarketStrategy):
    """
    Submit one limit buy once a late-game favorite reaches the entry threshold.

    This strategy is intended for resolved-market backtests which mark any
    remaining position to settlement after the backtest completes.
    """

    def __init__(self, config: TradeTickLateFavoriteLimitHoldConfig) -> None:
        super().__init__(config)
        self._entered_once = False

    def _subscribe(self) -> None:
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        if self._pending or self._in_position() or self._entered_once:
            return

        ts_event_ns = int(tick.ts_event)
        if int(self.config.activation_start_time_ns) > 0 and ts_event_ns < int(
            self.config.activation_start_time_ns,
        ):
            return
        if int(self.config.market_close_time_ns) > 0 and ts_event_ns > int(
            self.config.market_close_time_ns,
        ):
            return

        price = float(tick.price)
        if price < float(self.config.entry_price):
            return

        assert self._instrument is not None
        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self._instrument.make_qty(float(self.config.trade_size)),
            price=self._instrument.make_price(price),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self._pending = True

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._entered_once = True

    def on_stop(self) -> None:
        # Leave filled positions open so the runner can mark them to settlement.
        self.cancel_all_orders(self.config.instrument_id)

    def on_reset(self) -> None:
        super().on_reset()
        self._entered_once = False
