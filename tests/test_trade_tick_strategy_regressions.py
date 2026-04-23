from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies.mean_reversion import (
    TradeTickMeanReversionConfig,
    TradeTickMeanReversionStrategy,
)
from strategies.panic_fade import (
    TradeTickPanicFadeConfig,
    TradeTickPanicFadeStrategy,
)
from strategies.vwap_reversion import (
    TradeTickVWAPReversionConfig,
    TradeTickVWAPReversionStrategy,
)

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


class _TradeTickMeanReversionHarness(TradeTickMeanReversionStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickMeanReversionConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal(1),
                vwap_window=3,
                entry_threshold=0.1,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))

    def _submit_exit(self) -> None:
        raise AssertionError("unexpected exit")

    def _in_position(self) -> bool:
        return False

    def _risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool:
        del price, take_profit, stop_loss
        return False


class _TradeTickVWAPReversionHarness(TradeTickVWAPReversionStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickVWAPReversionConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal(1),
                vwap_window=3,
                entry_threshold=0.1,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))

    def _submit_exit(self) -> None:
        raise AssertionError("unexpected exit")

    def _in_position(self) -> bool:
        return False

    def _risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool:
        del price, take_profit, stop_loss
        return False


class _TradeTickPanicFadeHarness(TradeTickPanicFadeStrategy):
    def __init__(self) -> None:
        super().__init__(
            TradeTickPanicFadeConfig(
                instrument_id=INSTRUMENT_ID,
                trade_size=Decimal(1),
                drop_window=3,
                min_drop=0.08,
                panic_price=0.92,
                rebound_exit=0.99,
                max_holding_periods=10,
                take_profit=0.0,
                stop_loss=0.0,
            )
        )
        self.entry_calls: list[tuple[float | None, float | None]] = []

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        self.entry_calls.append((reference_price, visible_size))

    def _submit_exit(self) -> None:
        raise AssertionError("unexpected exit")

    def _in_position(self) -> bool:
        return False

    def _risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool:
        del price, take_profit, stop_loss
        return False


def test_trade_tick_mean_reversion_uses_prior_window_and_trade_size() -> None:
    strategy = _TradeTickMeanReversionHarness()
    strategy._prices.extend([1.0, 1.0, 1.0])

    strategy.on_trade_tick(SimpleNamespace(price=0.86, size=2.0))

    assert strategy.entry_calls == [(0.86, 2.0)]


def test_trade_tick_vwap_reversion_uses_prior_window_and_trade_size() -> None:
    strategy = _TradeTickVWAPReversionHarness()
    for _ in range(3):
        strategy._append_point(price=1.0, size=1.0)

    strategy.on_trade_tick(SimpleNamespace(price=0.86, size=2.0))

    assert strategy.entry_calls == [(0.86, 2.0)]


def test_trade_tick_panic_fade_uses_prior_window_and_trade_size() -> None:
    strategy = _TradeTickPanicFadeHarness()
    strategy._prices.extend([1.0, 0.94, 0.94])

    strategy.on_trade_tick(SimpleNamespace(price=0.91, size=2.0))

    assert strategy.entry_calls == [(0.91, 2.0)]
