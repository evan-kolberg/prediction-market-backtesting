from __future__ import annotations

import decimal

from nautilus_trader.adapters.prediction_market.fill_model import PredictionMarketTakerFillModel
from nautilus_trader.adapters.prediction_market.fill_model import (
    effective_prediction_market_slippage_tick,
)
from nautilus_trader.core.rust.model import OrderSide
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.stubs.execution import TestExecStubs


def _make_instrument(
    *,
    venue: str,
    symbol: str,
    currency: str,
    price_increment: str,
    price_precision: int,
) -> BinaryOption:
    return BinaryOption(
        instrument_id=InstrumentId(Symbol(symbol), Venue(venue)),
        raw_symbol=Symbol(symbol),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str(currency),
        activation_ns=0,
        expiration_ns=0,
        price_precision=price_precision,
        size_precision=2,
        price_increment=Price.from_str(price_increment),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=decimal.Decimal(0),
        taker_fee=decimal.Decimal("0.01"),
        outcome="Yes",
        description=f"{venue} test market",
        ts_event=0,
        ts_init=0,
    )


def test_effective_prediction_market_slippage_tick_uses_polymarket_tick_size() -> None:
    instrument = _make_instrument(
        venue="POLYMARKET",
        symbol="POLYTEST-YES",
        currency="USDC",
        price_increment="0.001",
        price_precision=3,
    )

    assert effective_prediction_market_slippage_tick(instrument) == 0.001


def test_effective_prediction_market_slippage_tick_uses_kalshi_order_tick() -> None:
    instrument = _make_instrument(
        venue="KALSHI",
        symbol="KXTEST-YES",
        currency="USD",
        price_increment="0.0001",
        price_precision=4,
    )

    assert effective_prediction_market_slippage_tick(instrument) == 0.01


def test_prediction_market_fill_model_slips_polymarket_market_orders_by_one_market_tick() -> None:
    instrument = _make_instrument(
        venue="POLYMARKET",
        symbol="POLYTEST-YES",
        currency="USDC",
        price_increment="0.001",
        price_precision=3,
    )
    fill_model = PredictionMarketTakerFillModel()
    order = TestExecStubs.market_order(instrument=instrument, order_side=OrderSide.BUY)

    result = fill_model.get_orderbook_for_fill_simulation(
        instrument,
        order,
        instrument.make_price(0.420),
        instrument.make_price(0.430),
    )

    assert result is not None
    bids = list(result.bids())
    asks = list(result.asks())
    assert bids[0].price == instrument.make_price(0.419)
    assert asks[0].price == instrument.make_price(0.431)


def test_prediction_market_fill_model_slips_kalshi_market_orders_by_one_cent() -> None:
    instrument = _make_instrument(
        venue="KALSHI",
        symbol="KXTEST-YES",
        currency="USD",
        price_increment="0.0001",
        price_precision=4,
    )
    fill_model = PredictionMarketTakerFillModel()
    order = TestExecStubs.market_order(instrument=instrument, order_side=OrderSide.BUY)

    result = fill_model.get_orderbook_for_fill_simulation(
        instrument,
        order,
        instrument.make_price(0.4200),
        instrument.make_price(0.4300),
    )

    assert result is not None
    bids = list(result.bids())
    asks = list(result.asks())
    assert bids[0].price == instrument.make_price(0.4100)
    assert asks[0].price == instrument.make_price(0.4400)


def test_prediction_market_fill_model_leaves_limit_orders_to_default_matching() -> None:
    instrument = _make_instrument(
        venue="POLYMARKET",
        symbol="POLYTEST-YES",
        currency="USDC",
        price_increment="0.001",
        price_precision=3,
    )
    fill_model = PredictionMarketTakerFillModel()
    order = TestExecStubs.limit_order(
        instrument=instrument,
        order_side=OrderSide.BUY,
        price=instrument.make_price(0.420),
    )

    result = fill_model.get_orderbook_for_fill_simulation(
        instrument,
        order,
        instrument.make_price(0.420),
        instrument.make_price(0.430),
    )

    assert result is None
