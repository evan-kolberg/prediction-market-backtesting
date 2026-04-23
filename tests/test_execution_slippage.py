from __future__ import annotations

import types
from decimal import Decimal

import pytest
from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.enums import OrderSide as OrderSideEnum
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price

from prediction_market_extensions.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
    effective_prediction_market_slippage_tick,
)
from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig


class _StubInstrument:
    def __init__(self, venue: str, price_increment: float) -> None:
        self.id = InstrumentId(Symbol("TEST-YES"), Venue(venue))
        self.price_increment = Decimal(str(price_increment))
        self.size_precision = 0

    def make_price(self, value: float) -> Price:
        return Price.from_str(f"{value:.4f}")


def _make_order(side: OrderSideEnum, order_type=OrderType.MARKET):
    return types.SimpleNamespace(side=side, order_type=order_type)


def test_default_slippage_ticks_is_one() -> None:
    model = PredictionMarketTakerFillModel()
    assert model._slippage_ticks == 1
    assert model._entry_slippage_pct == 0.0
    assert model._exit_slippage_pct == 0.0


def test_negative_slippage_ticks_raises() -> None:
    with pytest.raises(ValueError, match="slippage_ticks must be >= 0"):
        PredictionMarketTakerFillModel(slippage_ticks=-1)


def test_negative_entry_pct_raises() -> None:
    with pytest.raises(ValueError, match="entry_slippage_pct must be >= 0"):
        PredictionMarketTakerFillModel(entry_slippage_pct=-0.01)


def test_negative_exit_pct_raises() -> None:
    with pytest.raises(ValueError, match="exit_slippage_pct must be >= 0"):
        PredictionMarketTakerFillModel(exit_slippage_pct=-0.01)


def test_zero_slippage_ticks_valid() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=0)
    assert model._slippage_ticks == 0


def test_kalshi_tick_is_one_cent() -> None:
    instrument = _StubInstrument("KALSHI", 0.01)
    assert effective_prediction_market_slippage_tick(instrument) == 0.01


def test_polymarket_tick_uses_price_increment() -> None:
    instrument = _StubInstrument("POLYMARKET", 0.001)
    assert effective_prediction_market_slippage_tick(instrument) == 0.001


def test_tick_slippage_shifts_buy_adverse() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=1)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.BUY), best_bid=0.50, best_ask=0.51
    )
    assert book is not None
    assert float(book.best_ask_price()) == pytest.approx(0.52)


def test_tick_slippage_shifts_sell_adverse() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=1)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.SELL), best_bid=0.50, best_ask=0.51
    )
    assert book is not None
    assert float(book.best_bid_price()) == pytest.approx(0.49)


def test_pct_slippage_entry_buy() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=0, entry_slippage_pct=0.02)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.BUY), best_bid=0.50, best_ask=0.50
    )
    assert book is not None
    assert float(book.best_ask_price()) == pytest.approx(0.51)


def test_pct_slippage_exit_sell() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=0, exit_slippage_pct=0.03)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.SELL), best_bid=0.50, best_ask=0.50
    )
    assert book is not None
    assert float(book.best_bid_price()) == pytest.approx(0.485)


def test_tick_and_pct_stack() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=1, entry_slippage_pct=0.02)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.BUY), best_bid=0.50, best_ask=0.50
    )
    assert book is not None
    assert float(book.best_ask_price()) == pytest.approx(0.52)


def test_sell_uses_exit_pct_not_entry_pct() -> None:
    model = PredictionMarketTakerFillModel(
        slippage_ticks=0, entry_slippage_pct=0.10, exit_slippage_pct=0.03
    )
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.SELL), best_bid=0.50, best_ask=0.50
    )
    assert book is not None
    assert float(book.best_bid_price()) == pytest.approx(0.485)


def test_slippage_clamped_at_one_for_buy() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=0, entry_slippage_pct=1.0)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.BUY), best_bid=0.99, best_ask=0.99
    )
    assert book is not None
    assert float(book.best_ask_price()) == 1.0


def test_slippage_clamped_at_zero_for_sell() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=0, exit_slippage_pct=1.0)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.SELL), best_bid=0.01, best_ask=0.01
    )
    assert book is not None
    assert float(book.best_bid_price()) == 0.0


def test_limit_order_returns_none() -> None:
    model = PredictionMarketTakerFillModel(slippage_ticks=1)
    instrument = _StubInstrument("KALSHI", 0.01)
    book = model.get_orderbook_for_fill_simulation(
        instrument, _make_order(OrderSideEnum.BUY, OrderType.LIMIT), best_bid=0.50, best_ask=0.51
    )
    assert book is None


def test_execution_config_defaults() -> None:
    config = ExecutionModelConfig()
    assert config.slippage_ticks == 1
    assert config.entry_slippage_pct == 0.0
    assert config.exit_slippage_pct == 0.0


def test_execution_config_validation_negative_ticks() -> None:
    with pytest.raises(ValueError, match="slippage_ticks must be >= 0"):
        ExecutionModelConfig(slippage_ticks=-1)


def test_execution_config_validation_negative_entry_pct() -> None:
    with pytest.raises(ValueError, match="entry_slippage_pct must be >= 0"):
        ExecutionModelConfig(entry_slippage_pct=-0.01)


def test_execution_config_validation_negative_exit_pct() -> None:
    with pytest.raises(ValueError, match="exit_slippage_pct must be >= 0"):
        ExecutionModelConfig(exit_slippage_pct=-0.01)


def test_build_fill_model_kwargs() -> None:
    config = ExecutionModelConfig(
        slippage_ticks=2,
        entry_slippage_pct=0.03,
        exit_slippage_pct=0.05,
    )
    kwargs = config.build_fill_model_kwargs()
    assert kwargs == {
        "slippage_ticks": 2,
        "entry_slippage_pct": 0.03,
        "exit_slippage_pct": 0.05,
    }


def test_fill_model_from_config_kwargs() -> None:
    config = ExecutionModelConfig(
        slippage_ticks=2,
        entry_slippage_pct=0.03,
        exit_slippage_pct=0.05,
    )
    model = PredictionMarketTakerFillModel(**config.build_fill_model_kwargs())
    assert model._slippage_ticks == 2
    assert model._entry_slippage_pct == 0.03
    assert model._exit_slippage_pct == 0.05
