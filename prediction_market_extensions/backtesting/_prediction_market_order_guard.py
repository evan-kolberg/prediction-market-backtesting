from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide, OrderType

_EPSILON = Decimal("1e-9")
type _ReservationKey = tuple[int, str]


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    for attr in ("as_decimal", "as_double", "as_f64_c"):
        method = getattr(value, attr, None)
        if callable(method):
            try:
                return Decimal(str(method()))
            except (InvalidOperation, TypeError, ValueError):
                return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _call_or_value(value: object) -> object:
    if callable(value):
        try:
            return value()
        except TypeError:
            return value
    return value


def _order_side(order: object) -> OrderSide | None:
    side = getattr(order, "side", None)
    if side in (OrderSide.BUY, OrderSide.SELL):
        return side
    side = getattr(order, "order_side", None)
    if side in (OrderSide.BUY, OrderSide.SELL):
        return side
    is_buy = getattr(order, "is_buy", None)
    if bool(_call_or_value(is_buy)):
        return OrderSide.BUY
    is_sell = getattr(order, "is_sell", None)
    if bool(_call_or_value(is_sell)):
        return OrderSide.SELL
    return None


def _order_quantity(order: object) -> Decimal | None:
    return _decimal_or_none(getattr(order, "quantity", None))


def _order_leaves_quantity(order: object) -> Decimal | None:
    leaves_quantity = _decimal_or_none(getattr(order, "leaves_qty", None))
    if leaves_quantity is not None:
        return leaves_quantity
    return _decimal_or_none(getattr(order, "leaves_quantity", None))


def _order_price(order: object) -> Decimal | None:
    has_price = getattr(order, "has_price", None)
    if has_price is not None and not bool(_call_or_value(has_price)):
        return None
    return _decimal_or_none(getattr(order, "price", None))


def _order_type(order: object) -> OrderType | None:
    order_type = getattr(order, "order_type", None)
    return order_type if isinstance(order_type, OrderType) else None


def _client_order_id(order: object) -> str:
    return str(getattr(order, "client_order_id", "unknown-order"))


def _event_order_is_closed(strategy: object, event: object) -> bool:
    client_order_id = getattr(event, "client_order_id", None)
    if client_order_id is None:
        return True
    try:
        order = strategy.cache.order(client_order_id)
    except (AttributeError, KeyError, TypeError):
        return True
    if order is None:
        return True
    is_closed = getattr(order, "is_closed", True)
    return bool(_call_or_value(is_closed))


def _prediction_market_instrument(instrument: object | None) -> bool:
    if instrument is None:
        return False
    return instrument.__class__.__name__ == "BinaryOption" or hasattr(instrument, "outcome")


def _best_book_price(strategy: object, order: object, side: OrderSide) -> Decimal | None:
    instrument_id = getattr(order, "instrument_id", None)
    if instrument_id is None:
        return None
    try:
        book = strategy.cache.order_book(instrument_id)
    except (AttributeError, KeyError, TypeError):
        book = None
    if book is not None:
        method_name = "best_ask_price" if side == OrderSide.BUY else "best_bid_price"
        method = getattr(book, method_name, None)
        if callable(method):
            price = _decimal_or_none(method())
            if price is not None:
                return price
    try:
        quote = strategy.cache.quote_tick(instrument_id)
    except (AttributeError, KeyError, TypeError):
        quote = None
    if quote is None:
        return None
    attr = "ask_price" if side == OrderSide.BUY else "bid_price"
    return _decimal_or_none(getattr(quote, attr, None))


def _buy_reference_price(strategy: object, order: object) -> Decimal:
    price = _order_price(order)
    if price is not None and price > 0:
        return price
    price = _best_book_price(strategy, order, OrderSide.BUY)
    if price is not None and price > 0:
        return price
    return Decimal("1")


def _fee_rate(instrument: object | None, *, taker: bool) -> Decimal:
    if instrument is None:
        return Decimal("0")
    attr = "taker_fee" if taker else "maker_fee"
    rate = _decimal_or_none(getattr(instrument, attr, None))
    if rate is None:
        return Decimal("0")
    return max(rate, Decimal("0"))


def _buy_cash_cost(
    *,
    strategy: object,
    order: object,
    instrument: object | None,
    quantity: Decimal,
) -> Decimal:
    price = min(max(_buy_reference_price(strategy, order), Decimal("0")), Decimal("1"))
    order_type = _order_type(order)
    taker = order_type in (OrderType.MARKET, OrderType.MARKET_TO_LIMIT)
    fee = _fee_rate(instrument, taker=taker) * price * (Decimal("1") - price)
    return quantity * (price + fee)


def _free_quote_balance(strategy: object, instrument: object) -> Decimal | None:
    try:
        account = strategy.portfolio.account(venue=instrument.id.venue)
    except (AttributeError, KeyError, TypeError):
        return None
    if account is None:
        return None
    free_balance = account.balance_free(instrument.quote_currency)
    return _decimal_or_none(free_balance)


def _long_position_qty(strategy: object, instrument_id: object) -> Decimal:
    try:
        net_position = strategy.portfolio.net_position(instrument_id)
    except (AttributeError, KeyError, TypeError):
        return Decimal("0")
    position = _decimal_or_none(net_position)
    if position is None and hasattr(net_position, "signed_decimal_qty"):
        position = _decimal_or_none(_call_or_value(getattr(net_position, "signed_decimal_qty")))
    if position is None and hasattr(net_position, "signed_qty"):
        position = _decimal_or_none(getattr(net_position, "signed_qty"))
    if position is None:
        return Decimal("0")
    return max(position, Decimal("0"))


@dataclass
class _Reservation:
    instrument_id: object
    strategy_key: int
    quote_currency: object | None = None
    buy_cash: Decimal = Decimal("0")
    buy_cash_per_unit: Decimal = Decimal("0")
    sell_qty: Decimal = Decimal("0")


class PredictionMarketOrderGuard:
    """Pre-trade guard for long-only binary prediction-market backtests."""

    def __init__(self) -> None:
        self.warnings: list[str] = []
        self._reservations: dict[_ReservationKey, _Reservation] = {}

    def install(self, strategy: object) -> None:
        original_submit_order = strategy.submit_order

        def guarded_submit_order(order: object, *args: object, **kwargs: object) -> object:
            reservation_key = self._reservation_key(strategy, order)
            allowed, reason, reservation = self._check_order(strategy, order)
            if not allowed:
                self._deny_order(strategy, order, reason)
                return None
            if reservation is not None and reservation_key in self._reservations:
                self._deny_order(
                    strategy,
                    order,
                    f"duplicate open client_order_id {_client_order_id(order)}",
                )
                return None
            if reservation is not None:
                self._reservations[reservation_key] = reservation
            try:
                return original_submit_order(order, *args, **kwargs)
            except Exception:
                self._reservations.pop(reservation_key, None)
                raise

        strategy.submit_order = guarded_submit_order
        self._wrap_order_event(strategy, "on_order_filled", release_on_closed=False)
        for handler_name in (
            "on_order_rejected",
            "on_order_denied",
            "on_order_canceled",
            "on_order_expired",
        ):
            self._wrap_order_event(strategy, handler_name, release_on_closed=True)
        self._wrap_reset_event(strategy)

    def _strategy_key(self, strategy: object) -> int:
        return id(strategy)

    def _reservation_key(
        self, strategy: object, order_or_client_order_id: object
    ) -> _ReservationKey:
        if hasattr(order_or_client_order_id, "client_order_id"):
            client_order_id = _client_order_id(order_or_client_order_id)
        else:
            client_order_id = str(order_or_client_order_id)
        return self._strategy_key(strategy), client_order_id

    def _clear_strategy_reservations(self, strategy: object) -> None:
        strategy_key = self._strategy_key(strategy)
        stale_order_ids = [
            reservation_key
            for reservation_key, reservation in self._reservations.items()
            if reservation.strategy_key == strategy_key
        ]
        for reservation_key in stale_order_ids:
            self._reservations.pop(reservation_key, None)

    def _wrap_reset_event(self, strategy: object) -> None:
        original_handler = getattr(strategy, "on_reset", None)
        if not callable(original_handler):
            return

        def wrapped(*args: object, **kwargs: object) -> object:
            try:
                return original_handler(*args, **kwargs)
            finally:
                self._clear_strategy_reservations(strategy)

        setattr(strategy, "on_reset", wrapped)

    def _wrap_order_event(
        self,
        strategy: object,
        handler_name: str,
        *,
        release_on_closed: bool,
    ) -> None:
        original_handler = getattr(strategy, handler_name, None)
        if not callable(original_handler):
            return

        def wrapped(event: object, *args: object, **kwargs: object) -> object:
            if not bool(getattr(event, "_prediction_market_guard_denial", False)):
                client_order_id = getattr(event, "client_order_id", "")
                reservation_key = self._reservation_key(strategy, client_order_id)
                if release_on_closed or _event_order_is_closed(strategy, event):
                    self._reservations.pop(reservation_key, None)
                else:
                    self._refresh_reservation(
                        strategy, reservation_key, client_order_id=client_order_id, event=event
                    )
            return original_handler(event, *args, **kwargs)

        setattr(strategy, handler_name, wrapped)

    def _check_order(
        self, strategy: object, order: object
    ) -> tuple[bool, str, _Reservation | None]:
        instrument_id = getattr(order, "instrument_id", None)
        if instrument_id is None:
            return True, "", None
        try:
            instrument = strategy.cache.instrument(instrument_id)
        except (AttributeError, KeyError, TypeError):
            instrument = None
        if not _prediction_market_instrument(instrument):
            return True, "", None

        side = _order_side(order)
        quantity = _order_quantity(order)
        if side is None or quantity is None or quantity <= 0:
            return True, "", None

        if side == OrderSide.SELL:
            pending_sell_qty = sum(
                reservation.sell_qty
                for reservation in self._reservations.values()
                if reservation.instrument_id == instrument_id
            )
            available_qty = _long_position_qty(strategy, instrument_id) - pending_sell_qty
            if quantity > available_qty + _EPSILON:
                return (
                    False,
                    f"SELL quantity {quantity} exceeds available long token inventory "
                    f"{max(available_qty, Decimal('0'))}",
                    None,
                )
            return (
                True,
                "",
                _Reservation(
                    instrument_id=instrument_id,
                    strategy_key=self._strategy_key(strategy),
                    sell_qty=quantity,
                ),
            )

        quote_currency = getattr(instrument, "quote_currency", None)
        pending_buy_cash = sum(
            reservation.buy_cash
            for reservation in self._reservations.values()
            if reservation.quote_currency == quote_currency
        )
        free_cash = _free_quote_balance(strategy, instrument)
        if free_cash is None:
            return True, "", None
        cash_cost = _buy_cash_cost(
            strategy=strategy,
            order=order,
            instrument=instrument,
            quantity=quantity,
        )
        available_cash = free_cash - pending_buy_cash
        if cash_cost > available_cash + _EPSILON:
            return (
                False,
                f"BUY cash cost {cash_cost} exceeds available cash {max(available_cash, Decimal('0'))}",
                None,
            )
        return (
            True,
            "",
            _Reservation(
                instrument_id=instrument_id,
                strategy_key=self._strategy_key(strategy),
                quote_currency=quote_currency,
                buy_cash=cash_cost,
                buy_cash_per_unit=cash_cost / quantity,
            ),
        )

    def _deny_order(self, strategy: object, order: object, reason: str) -> None:
        message = (
            f"Prediction-market order guard denied {_client_order_id(order)} for "
            f"{getattr(order, 'instrument_id', 'unknown instrument')}: {reason}."
        )
        if message not in self.warnings:
            self.warnings.append(message)
        event = SimpleNamespace(
            client_order_id=getattr(order, "client_order_id", ""),
            instrument_id=getattr(order, "instrument_id", None),
            order_side=_order_side(order),
            order_type=_order_type(order),
            quantity=getattr(order, "quantity", None),
            price=getattr(order, "price", None),
            reason=reason,
            ts_event=getattr(order, "ts_init", 0),
            _prediction_market_guard_denial=True,
        )
        handler = getattr(strategy, "on_order_denied", None)
        if callable(handler):
            handler(event)

    def _refresh_reservation(
        self,
        strategy: object,
        reservation_key: _ReservationKey,
        *,
        client_order_id: object,
        event: object | None = None,
    ) -> None:
        existing = self._reservations.get(reservation_key)
        if existing is None:
            return
        try:
            order = strategy.cache.order(client_order_id)
        except (AttributeError, KeyError, TypeError):
            try:
                order = strategy.cache.order(str(client_order_id))
            except (AttributeError, KeyError, TypeError):
                order = None
        if order is None:
            self._reduce_reservation_from_fill(existing, event=event, side=None)
            return

        leaves_qty = _order_leaves_quantity(order)
        if leaves_qty is None:
            self._reduce_reservation_from_fill(existing, event=event, side=_order_side(order))
            return
        if leaves_qty <= 0:
            self._reservations.pop(reservation_key, None)
            return

        side = _order_side(order)
        instrument_id = getattr(order, "instrument_id", None)
        try:
            instrument = strategy.cache.instrument(instrument_id)
        except (AttributeError, KeyError, TypeError):
            instrument = None
        if side == OrderSide.BUY:
            existing.buy_cash = _buy_cash_cost(
                strategy=strategy,
                order=order,
                instrument=instrument,
                quantity=leaves_qty,
            )
            return
        if side == OrderSide.SELL:
            existing.sell_qty = leaves_qty

    def _reduce_reservation_from_fill(
        self, existing: _Reservation, *, event: object | None, side: OrderSide | None
    ) -> None:
        if event is None:
            return
        fill_qty = _decimal_or_none(
            getattr(
                event,
                "last_qty",
                getattr(event, "quantity", getattr(event, "filled_qty", None)),
            )
        )
        if fill_qty is None or fill_qty <= 0:
            return
        event_side = _order_side(event)
        side = event_side if event_side is not None else side
        if side == OrderSide.BUY or (side is None and existing.buy_cash > 0):
            existing.buy_cash = max(
                Decimal("0"), existing.buy_cash - (fill_qty * existing.buy_cash_per_unit)
            )
            return
        if side == OrderSide.SELL or (side is None and existing.sell_qty > 0):
            existing.sell_qty = max(Decimal("0"), existing.sell_qty - fill_qty)
