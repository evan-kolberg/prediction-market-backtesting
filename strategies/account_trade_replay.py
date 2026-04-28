from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import msgspec
from nautilus_trader.model.enums import BookType, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig


_NANOS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True)
class _ScheduledTrade:
    sequence: int
    ts_ns: int
    side: OrderSide
    size: Decimal
    price: Decimal
    transaction_hash: str


class AccountReplayTrade(msgspec.Struct, frozen=True):
    ts: int
    side: str
    size: str
    price: str
    tx: str = ""


class BookAccountTradeReplayConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trades: tuple[AccountReplayTrade, ...] = ()
    trigger_on_trade_ticks: bool = True
    reduce_only_sells: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.trigger_on_trade_ticks, bool):
            raise TypeError("trigger_on_trade_ticks must be a bool")
        if not isinstance(self.reduce_only_sells, bool):
            raise TypeError("reduce_only_sells must be a bool")


class BookAccountTradeReplayStrategy(Strategy):
    """
    Replay a hard-coded account ledger as scheduled limit IOC orders.

    This strategy intentionally does not manufacture fills. It submits orders at
    the public trade timestamps and lets the existing L2 book/trade execution
    path decide whether those orders can fill.
    """

    def __init__(self, config: BookAccountTradeReplayConfig) -> None:
        super().__init__(config)
        self._instrument = None
        self._next_index = 0
        self._scheduled_trades = self._normalize_trades(config.trades)

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
        if self.config.trigger_on_trade_ticks:
            self.subscribe_trade_ticks(self.config.instrument_id)

    def on_order_book_deltas(self, deltas: Any) -> None:
        self._process_due(ts_ns=int(deltas.ts_event))

    def on_trade_tick(self, trade: Any) -> None:
        self._process_due(ts_ns=int(trade.ts_event))

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)

    def on_reset(self) -> None:
        self._instrument = None
        self._next_index = 0

    def _process_due(self, *, ts_ns: int) -> None:
        while self._next_index < len(self._scheduled_trades):
            scheduled = self._scheduled_trades[self._next_index]
            if scheduled.ts_ns > ts_ns:
                return
            self._next_index += 1
            self._submit_scheduled_trade(scheduled)

    def _submit_scheduled_trade(self, scheduled: _ScheduledTrade) -> None:
        assert self._instrument is not None
        try:
            quantity = self._instrument.make_qty(float(scheduled.size), round_down=True)
            price = self._instrument.make_price(float(scheduled.price))
        except ValueError as exc:
            self.log.warning(
                f"Skipping ledger trade {scheduled.sequence} for {self.config.instrument_id}: "
                f"instrument rejected size={scheduled.size} price={scheduled.price} ({exc})."
            )
            return

        if quantity.as_double() <= 0.0:
            self.log.warning(
                f"Skipping ledger trade {scheduled.sequence} for {self.config.instrument_id}: "
                f"rounded quantity is zero."
            )
            return

        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=scheduled.side,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.IOC,
            reduce_only=bool(self.config.reduce_only_sells and scheduled.side == OrderSide.SELL),
            tags=[
                "account_trade_replay",
                f"ledger_sequence={scheduled.sequence}",
                f"ledger_tx={scheduled.transaction_hash[-12:]}",
            ],
        )
        self.submit_order(order)

    @staticmethod
    def _normalize_trades(trades: tuple[AccountReplayTrade, ...]) -> tuple[_ScheduledTrade, ...]:
        normalized: list[_ScheduledTrade] = []
        for sequence, raw_trade in enumerate(trades, start=1):
            if isinstance(raw_trade, Mapping):
                ts_value = raw_trade.get("ts")
                size_value = raw_trade.get("size")
                price_value = raw_trade.get("price")
                side_value = raw_trade.get("side")
                tx_value = raw_trade.get("tx") or ""
            else:
                ts_value = raw_trade.ts
                size_value = raw_trade.size
                price_value = raw_trade.price
                side_value = raw_trade.side
                tx_value = raw_trade.tx

            ts_seconds = _coerce_int(ts_value, name=f"trade {sequence} ts")
            size = _coerce_decimal(size_value, name=f"trade {sequence} size")
            price = _coerce_decimal(price_value, name=f"trade {sequence} price")
            if size <= 0:
                raise ValueError(f"trade {sequence} size must be > 0, got {size}")
            if not Decimal("0") <= price <= Decimal("1"):
                raise ValueError(f"trade {sequence} price must be in [0, 1], got {price}")

            side_name = str(side_value or "").strip().upper()
            if side_name == "BUY":
                side = OrderSide.BUY
            elif side_name == "SELL":
                side = OrderSide.SELL
            else:
                raise ValueError(f"trade {sequence} side must be BUY or SELL, got {side_name!r}")

            normalized.append(
                _ScheduledTrade(
                    sequence=sequence,
                    ts_ns=ts_seconds * _NANOS_PER_SECOND,
                    side=side,
                    size=size,
                    price=price,
                    transaction_hash=str(tx_value),
                )
            )

        return tuple(sorted(normalized, key=lambda trade: (trade.ts_ns, trade.sequence)))


def _coerce_int(value: object, *, name: str) -> int:
    try:
        result = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if result < 0:
        raise ValueError(f"{name} must be non-negative, got {result}")
    return result


def _coerce_decimal(value: object, *, name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like, got {value!r}") from exc
