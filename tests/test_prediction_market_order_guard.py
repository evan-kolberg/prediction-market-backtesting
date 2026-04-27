from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide, OrderType

from prediction_market_extensions.backtesting._prediction_market_order_guard import (
    PredictionMarketOrderGuard,
)


class _Money:
    def __init__(self, value: float) -> None:
        self._value = value

    def as_double(self) -> float:
        return self._value


class _Account:
    def __init__(self, free: float) -> None:
        self._free = free

    def balance_free(self, currency: object) -> _Money:
        del currency
        return _Money(self._free)


class _Portfolio:
    def __init__(self, *, free: float, positions: dict[object, Decimal] | None = None) -> None:
        self._account = _Account(free)
        self._positions = positions or {}

    def account(self, *, venue: object) -> _Account:
        del venue
        return self._account

    def net_position(self, instrument_id: object) -> Decimal:
        return self._positions.get(instrument_id, Decimal("0"))


class _Cache:
    def __init__(self, instruments: dict[object, object], prices: dict[object, float]) -> None:
        self._instruments = instruments
        self._prices = prices
        self._orders: dict[object, object] = {}

    def instrument(self, instrument_id: object) -> object:
        return self._instruments[instrument_id]

    def order_book(self, instrument_id: object) -> object:
        price = self._prices[instrument_id]
        return SimpleNamespace(best_ask_price=lambda: price, best_bid_price=lambda: price)

    def order(self, client_order_id: object) -> object | None:
        return self._orders.get(client_order_id)


class _Strategy:
    def __init__(
        self,
        *,
        instruments: dict[object, object],
        prices: dict[object, float],
        free: float,
        positions: dict[object, Decimal] | None = None,
    ) -> None:
        self.cache = _Cache(instruments, prices)
        self.portfolio = _Portfolio(free=free, positions=positions)
        self.submitted: list[object] = []
        self.denied: list[object] = []
        self.filled: list[object] = []
        self.reset_count = 0

    def submit_order(self, order: object) -> None:
        self.submitted.append(order)
        self.cache._orders[getattr(order, "client_order_id")] = order

    def on_order_denied(self, event: object) -> None:
        self.denied.append(event)

    def on_order_filled(self, event: object) -> None:
        self.filled.append(event)

    def on_reset(self) -> None:
        self.reset_count += 1


class _ReentrantFillStrategy(_Strategy):
    def __init__(self, *, reentrant_order: object, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.reentrant_order = reentrant_order

    def on_order_filled(self, event: object) -> None:
        super().on_order_filled(event)
        order = self.reentrant_order
        self.reentrant_order = None
        if order is not None:
            self.submit_order(order)


def _instrument(instrument_id: object, *, currency: str = "USDC") -> object:
    return SimpleNamespace(
        id=SimpleNamespace(venue="POLYMARKET"),
        instrument_id=instrument_id,
        quote_currency=currency,
        taker_fee=Decimal("0"),
        maker_fee=Decimal("0"),
        outcome="Yes",
    )


def _order(
    client_order_id: str,
    *,
    instrument_id: object,
    side: OrderSide,
    quantity: float,
    price: float | None = None,
    order_type: OrderType = OrderType.MARKET,
) -> object:
    return SimpleNamespace(
        client_order_id=client_order_id,
        instrument_id=instrument_id,
        side=side,
        order_type=order_type,
        quantity=Decimal(str(quantity)),
        price=None if price is None else Decimal(str(price)),
        has_price=lambda: price is not None,
        ts_init=0,
    )


def test_order_guard_denies_naked_prediction_market_sell() -> None:
    instrument_id = "market-a"
    strategy = _Strategy(
        instruments={instrument_id: _instrument(instrument_id)},
        prices={instrument_id: 0.50},
        free=100.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    strategy.submit_order(
        _order(
            "sell-1",
            instrument_id=instrument_id,
            side=OrderSide.SELL,
            quantity=5.0,
            price=0.50,
            order_type=OrderType.LIMIT,
        )
    )

    assert strategy.submitted == []
    assert len(strategy.denied) == 1
    assert "SELL quantity" in strategy.denied[0].reason
    assert guard.warnings


def test_order_guard_reserves_buy_cash_across_instruments() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    strategy.submit_order(
        _order(
            "buy-a",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["buy-a"]
    assert len(strategy.denied) == 1
    assert "BUY cash cost" in strategy.denied[0].reason


def test_order_guard_reserves_buy_cash_across_strategies() -> None:
    market_a = "market-a"
    market_b = "market-b"
    instruments = {market_a: _instrument(market_a), market_b: _instrument(market_b)}
    guard = PredictionMarketOrderGuard()
    first = _Strategy(instruments=instruments, prices={market_a: 0.60, market_b: 0.60}, free=5.0)
    second = _Strategy(instruments=instruments, prices={market_a: 0.60, market_b: 0.60}, free=5.0)
    guard.install(first)
    guard.install(second)

    first.submit_order(
        _order(
            "buy-a",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    second.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in first.submitted] == ["buy-a"]
    assert second.submitted == []
    assert len(second.denied) == 1
    assert "BUY cash cost" in second.denied[0].reason


def test_order_guard_releases_buy_cash_reservation_when_order_closes() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    first_order = _order(
        "buy-a",
        instrument_id=market_a,
        side=OrderSide.BUY,
        quantity=5.0,
        price=0.60,
        order_type=OrderType.LIMIT,
    )
    first_order.is_closed = lambda: True
    strategy.submit_order(first_order)
    strategy.on_order_filled(SimpleNamespace(client_order_id="buy-a"))
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["buy-a", "buy-b"]
    assert strategy.denied == []


def test_order_guard_releases_closed_reservation_before_fill_handler_reenters() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _ReentrantFillStrategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
        reentrant_order=_order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        ),
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    first_order = _order(
        "buy-a",
        instrument_id=market_a,
        side=OrderSide.BUY,
        quantity=5.0,
        price=0.60,
        order_type=OrderType.LIMIT,
    )
    first_order.is_closed = lambda: True
    strategy.submit_order(first_order)
    strategy.on_order_filled(SimpleNamespace(client_order_id="buy-a"))

    assert [order.client_order_id for order in strategy.submitted] == ["buy-a", "buy-b"]
    assert strategy.denied == []


def test_order_guard_reduces_buy_cash_reservation_after_partial_fill() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.50, market_b: 0.50},
        free=10.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    first_order = _order(
        "buy-a",
        instrument_id=market_a,
        side=OrderSide.BUY,
        quantity=10.0,
        price=0.50,
        order_type=OrderType.LIMIT,
    )
    first_order.is_closed = lambda: False
    strategy.submit_order(first_order)
    strategy.portfolio._account._free = 7.5
    strategy.on_order_filled(
        SimpleNamespace(
            client_order_id="buy-a",
            instrument_id=market_a,
            order_side=OrderSide.BUY,
            last_qty=Decimal("5"),
            last_px=Decimal("0.50"),
        )
    )
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=8.0,
            price=0.50,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["buy-a", "buy-b"]
    assert strategy.denied == []


def test_order_guard_reduces_sell_inventory_reservation_after_partial_fill() -> None:
    market_a = "market-a"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a)},
        prices={market_a: 0.50},
        free=10.0,
        positions={market_a: Decimal("10")},
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    first_order = _order(
        "sell-a",
        instrument_id=market_a,
        side=OrderSide.SELL,
        quantity=8.0,
        price=0.50,
        order_type=OrderType.LIMIT,
    )
    first_order.is_closed = lambda: False
    strategy.submit_order(first_order)
    strategy.portfolio._positions[market_a] = Decimal("6")
    strategy.on_order_filled(
        SimpleNamespace(
            client_order_id="sell-a",
            instrument_id=market_a,
            order_side=OrderSide.SELL,
            last_qty=Decimal("4"),
            last_px=Decimal("0.50"),
        )
    )
    strategy.submit_order(
        _order(
            "sell-b",
            instrument_id=market_a,
            side=OrderSide.SELL,
            quantity=2.0,
            price=0.50,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["sell-a", "sell-b"]
    assert strategy.denied == []


def test_order_guard_updates_buy_cash_reservation_after_partial_fill() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    first_order = _order(
        "buy-a",
        instrument_id=market_a,
        side=OrderSide.BUY,
        quantity=5.0,
        price=0.60,
        order_type=OrderType.LIMIT,
    )
    first_order.leaves_qty = Decimal("2")
    first_order.is_closed = lambda: False
    strategy.submit_order(first_order)
    strategy.portfolio._account._free = 3.2
    strategy.on_order_filled(SimpleNamespace(client_order_id="buy-a"))
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=3.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["buy-a", "buy-b"]
    assert strategy.denied == []


def test_order_guard_clears_strategy_reservations_on_reset() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    strategy.submit_order(
        _order(
            "buy-a",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    strategy.on_reset()
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert strategy.reset_count == 1
    assert [order.client_order_id for order in strategy.submitted] == ["buy-a", "buy-b"]
    assert strategy.denied == []


def test_order_guard_duplicate_ids_across_strategies_do_not_overwrite_reservations() -> None:
    market_a = "market-a"
    market_b = "market-b"
    market_c = "market-c"
    instruments = {
        market_a: _instrument(market_a),
        market_b: _instrument(market_b),
        market_c: _instrument(market_c),
    }
    prices = {market_a: 0.60, market_b: 0.60, market_c: 0.60}
    guard = PredictionMarketOrderGuard()
    first = _Strategy(instruments=instruments, prices=prices, free=7.0)
    second = _Strategy(instruments=instruments, prices=prices, free=7.0)
    third = _Strategy(instruments=instruments, prices=prices, free=7.0)
    guard.install(first)
    guard.install(second)
    guard.install(third)

    first.submit_order(
        _order(
            "same-client-id",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    second.submit_order(
        _order(
            "same-client-id",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    third.submit_order(
        _order(
            "unique-third-id",
            instrument_id=market_c,
            side=OrderSide.BUY,
            quantity=2.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in first.submitted] == ["same-client-id"]
    assert [order.client_order_id for order in second.submitted] == ["same-client-id"]
    assert third.submitted == []
    assert len(third.denied) == 1
    assert "BUY cash cost" in third.denied[0].reason


def test_order_guard_duplicate_id_denial_does_not_release_original_reservation() -> None:
    market_a = "market-a"
    market_b = "market-b"
    strategy = _Strategy(
        instruments={market_a: _instrument(market_a), market_b: _instrument(market_b)},
        prices={market_a: 0.60, market_b: 0.60},
        free=5.0,
    )
    guard = PredictionMarketOrderGuard()
    guard.install(strategy)

    strategy.submit_order(
        _order(
            "same-client-id",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=5.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    strategy.submit_order(
        _order(
            "same-client-id",
            instrument_id=market_a,
            side=OrderSide.BUY,
            quantity=1.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )
    strategy.submit_order(
        _order(
            "buy-b",
            instrument_id=market_b,
            side=OrderSide.BUY,
            quantity=4.0,
            price=0.60,
            order_type=OrderType.LIMIT,
        )
    )

    assert [order.client_order_id for order in strategy.submitted] == ["same-client-id"]
    assert len(strategy.denied) == 2
    assert "duplicate open client_order_id" in strategy.denied[0].reason
    assert "BUY cash cost" in strategy.denied[1].reason
