from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies.deep_value import TradeTickDeepValueHoldConfig, TradeTickDeepValueHoldStrategy
from strategies.ema_crossover import TradeTickEMACrossoverConfig, TradeTickEMACrossoverStrategy
from strategies.final_period_momentum import (
    TradeTickFinalPeriodMomentumConfig,
    TradeTickFinalPeriodMomentumStrategy,
)
from strategies.rsi_reversion import TradeTickRSIReversionConfig, TradeTickRSIReversionStrategy
from strategies.threshold_momentum import (
    TradeTickThresholdMomentumConfig,
    TradeTickThresholdMomentumStrategy,
)

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _NoPositionMixin:
    def _in_position(self) -> bool:
        return False

    def _risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool:
        del price, take_profit, stop_loss
        return False

    def _submit_exit(self) -> None:
        raise AssertionError("unexpected exit")


class _DeepValueHarness(_NoPositionMixin, TradeTickDeepValueHoldStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickDeepValueHoldConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal("1"),
                entry_price_max=0.25,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))


class _EMAHarness(_NoPositionMixin, TradeTickEMACrossoverStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickEMACrossoverConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal("1"),
                fast_period=2,
                slow_period=3,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))


class _RSIHarness(_NoPositionMixin, TradeTickRSIReversionStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickRSIReversionConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal("1"),
                period=2,
                entry_rsi=25.0,
                exit_rsi=60.0,
                take_profit=0.0,
                stop_loss=0.0,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))


class _ThresholdHarness(_NoPositionMixin, TradeTickThresholdMomentumStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickThresholdMomentumConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal("1"),
                activation_start_time_ns=10,
                market_close_time_ns=100,
                entry_price=0.8,
                take_profit_price=0.95,
                stop_loss_price=0.5,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))


class _FinalPeriodHarness(_NoPositionMixin, TradeTickFinalPeriodMomentumStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickFinalPeriodMomentumConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal("1"),
                market_close_time_ns=100,
                final_period_minutes=1,
                entry_price=0.8,
                take_profit_price=0.95,
                stop_loss_price=0.5,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))


def test_trade_tick_deep_value_enters_below_threshold() -> None:
    strategy = _DeepValueHarness()
    strategy.on_trade_tick(SimpleNamespace(price=0.20, size=5.0))
    assert strategy.entry_calls == [(0.20, None)]


def test_trade_tick_ema_crossover_enters_after_bullish_cross() -> None:
    strategy = _EMAHarness()
    for price in (0.10, 0.10, 0.10, 0.80):
        strategy.on_trade_tick(SimpleNamespace(price=price, size=1.0))
    assert strategy.entry_calls == [(0.80, None)]


def test_trade_tick_rsi_reversion_enters_on_oversold_reading() -> None:
    strategy = _RSIHarness()
    for price in (0.90, 0.80, 0.70):
        strategy.on_trade_tick(SimpleNamespace(price=price, size=1.0))
    assert strategy.entry_calls == [(0.70, None)]


def test_trade_tick_threshold_momentum_enters_on_breakout_cross() -> None:
    strategy = _ThresholdHarness()
    strategy.on_trade_tick(SimpleNamespace(price=0.79, size=1.0, ts_event=20))
    strategy.on_trade_tick(SimpleNamespace(price=0.81, size=1.0, ts_event=21))
    assert strategy.entry_calls == [(0.81, None)]


def test_trade_tick_final_period_momentum_enters_in_final_period() -> None:
    strategy = _FinalPeriodHarness()
    strategy.on_trade_tick(SimpleNamespace(price=0.79, size=1.0, ts_event=50))
    strategy.on_trade_tick(SimpleNamespace(price=0.81, size=1.0, ts_event=60))
    assert strategy.entry_calls == [(0.81, None)]
