from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig

from prediction_market_extensions.adapters.prediction_market.order_tags import (
    format_order_intent_tag,
)


@dataclass(frozen=True)
class _ScheduledProfileOrder:
    side: OrderSide
    size: float
    price: float
    timestamp_ns: int
    scheduled_time_ns: int
    transaction_hash: str


class BookProfileReplayConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    selection_key: str
    trades_by_key: Mapping[str, Sequence[Mapping[str, Any]]]
    lead_time_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not str(self.selection_key).strip():
            raise ValueError("selection_key is required")
        if not isinstance(self.trades_by_key, Mapping) or not self.trades_by_key:
            raise ValueError("trades_by_key must contain at least one scheduled trade group")
        if float(self.lead_time_seconds) < 0.0:
            raise ValueError("lead_time_seconds must be >= 0")


def _payload_float(payload: Mapping[str, Any], field: str) -> float:
    value = float(payload[field])
    if value <= 0.0:
        raise ValueError(f"scheduled profile trade {field} must be positive, got {value}")
    return value


def _payload_timestamp_ns(payload: Mapping[str, Any]) -> int:
    if "timestamp_ns" in payload:
        timestamp_ns = int(payload["timestamp_ns"])
    else:
        timestamp_ns = int(float(payload["timestamp"]) * 1_000_000_000)
    if timestamp_ns <= 0:
        raise ValueError(f"scheduled profile trade timestamp must be positive, got {timestamp_ns}")
    return timestamp_ns


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _scheduled_orders_from_config(
    config: BookProfileReplayConfig,
) -> tuple[_ScheduledProfileOrder, ...]:
    raw_trades = config.trades_by_key.get(str(config.selection_key))
    if raw_trades is None:
        raise ValueError(f"selection_key {config.selection_key!r} not found in trades_by_key")

    lead_time_ns = int(float(config.lead_time_seconds) * 1_000_000_000)
    scheduled: list[_ScheduledProfileOrder] = []
    for payload in raw_trades:
        raw_side = str(payload.get("side") or "").strip().upper()
        if raw_side == "BUY":
            side = OrderSide.BUY
        elif raw_side == "SELL":
            side = OrderSide.SELL
        else:
            raise ValueError(f"scheduled profile trade side must be BUY or SELL, got {raw_side!r}")
        timestamp_ns = _payload_timestamp_ns(payload)
        price = _payload_float(payload, "price")
        if price > 1.0:
            raise ValueError(f"scheduled profile trade price must be <= 1, got {price}")
        scheduled.append(
            _ScheduledProfileOrder(
                side=side,
                size=_payload_float(payload, "size"),
                price=price,
                timestamp_ns=timestamp_ns,
                scheduled_time_ns=max(0, timestamp_ns - lead_time_ns),
                transaction_hash=str(payload.get("transaction_hash") or ""),
            )
        )

    scheduled.sort(
        key=lambda trade: (
            trade.scheduled_time_ns,
            trade.timestamp_ns,
            trade.transaction_hash,
        )
    )
    return tuple(scheduled)


class BookProfileReplayStrategy(Strategy):
    """
    Submit IOC limit orders just before a known public profile trade occurs.

    This is an audit/validation strategy, not alpha. It lets us compare whether
    the book replay and fill model can reproduce an already observed wallet's
    execution when we submit the same side, price, and size slightly earlier.
    """

    def __init__(self, config: BookProfileReplayConfig) -> None:
        super().__init__(config)
        self._instrument = None
        self._order_book: OrderBook | None = None
        self._scheduled_orders = _scheduled_orders_from_config(config)
        self._next_order_index = 0

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self.config.instrument_id)
        if self._instrument is None:
            self.log.error(f"Instrument {self.config.instrument_id} not found - stopping.")
            self.stop()
            return
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def on_order_book_deltas(self, deltas) -> None:  # type: ignore[no-untyped-def]
        instrument_id = getattr(deltas, "instrument_id", self.config.instrument_id)
        if self._order_book is None:
            self._order_book = OrderBook(instrument_id, book_type=BookType.L2_MBP)
        self._order_book.apply_deltas(deltas)
        self.on_order_book(self._order_book)

    def on_order_book(self, order_book) -> None:  # type: ignore[no-untyped-def]
        self._submit_due_orders(ts_event_ns=int(order_book.ts_event))

    def _submit_due_orders(self, *, ts_event_ns: int) -> None:
        while self._next_order_index < len(self._scheduled_orders):
            scheduled_order = self._scheduled_orders[self._next_order_index]
            if scheduled_order.scheduled_time_ns > ts_event_ns:
                return
            self._next_order_index += 1
            self._submit_scheduled_order(scheduled_order)

    def _sell_quantity_cap(self) -> float:
        try:
            net_position = self.portfolio.net_position(self.config.instrument_id)
        except (AttributeError, KeyError, TypeError):
            return 0.0

        position_size = _decimal_or_none(net_position)
        if position_size is None and hasattr(net_position, "signed_decimal_qty"):
            try:
                position_size = _decimal_or_none(net_position.signed_decimal_qty())
            except TypeError:
                position_size = _decimal_or_none(getattr(net_position, "signed_decimal_qty", None))
        if position_size is None and hasattr(net_position, "signed_qty"):
            position_size = _decimal_or_none(getattr(net_position, "signed_qty", None))
        if position_size is None:
            return 0.0
        return float(max(position_size, Decimal("0")))

    def _submit_scheduled_order(self, scheduled_order: _ScheduledProfileOrder) -> None:
        assert self._instrument is not None
        requested_size = scheduled_order.size
        if scheduled_order.side == OrderSide.SELL:
            requested_size = min(requested_size, self._sell_quantity_cap())
            if requested_size <= 0.0:
                self.log.warning(
                    "Skipping scheduled profile SELL for "
                    f"{self.config.instrument_id}: no simulated inventory is available."
                )
                return

        try:
            quantity = self._instrument.make_qty(requested_size, round_down=True)
            price = self._instrument.make_price(scheduled_order.price)
        except ValueError as exc:
            self.log.warning(
                "Skipping scheduled profile order for "
                f"{self.config.instrument_id}: instrument rejected size/price ({exc})."
            )
            return
        if quantity.as_double() <= 0.0:
            return

        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=scheduled_order.side,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.IOC,
            reduce_only=scheduled_order.side == OrderSide.SELL,
            tags=[format_order_intent_tag("profile_replay")],
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)

    def on_reset(self) -> None:
        self._instrument = None
        self._order_book = None
        self._scheduled_orders = _scheduled_orders_from_config(self.config)
        self._next_order_index = 0


__all__ = [
    "BookProfileReplayConfig",
    "BookProfileReplayStrategy",
]
