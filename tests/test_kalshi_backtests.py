from __future__ import annotations

import asyncio
import importlib

import pytest

from strategies import TradeTickBreakoutConfig
from strategies import TradeTickBreakoutStrategy
from strategies import TradeTickEMACrossoverConfig
from strategies import TradeTickEMACrossoverStrategy
from strategies import TradeTickMeanReversionConfig
from strategies import TradeTickMeanReversionStrategy
from strategies import TradeTickPanicFadeConfig
from strategies import TradeTickPanicFadeStrategy
from strategies import TradeTickRSIReversionConfig
from strategies import TradeTickRSIReversionStrategy
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("KALSHI-TEST"), Venue("KALSHI"))


@pytest.mark.parametrize(
    ("module_name", "strategy_cls", "config_cls"),
    [
        (
            "backtests.kalshi_trade_tick.kalshi_breakout",
            TradeTickBreakoutStrategy,
            TradeTickBreakoutConfig,
        ),
        (
            "backtests.kalshi_trade_tick.kalshi_ema_crossover",
            TradeTickEMACrossoverStrategy,
            TradeTickEMACrossoverConfig,
        ),
        (
            "backtests.kalshi_trade_tick.kalshi_panic_fade",
            TradeTickPanicFadeStrategy,
            TradeTickPanicFadeConfig,
        ),
        (
            "backtests.kalshi_trade_tick.kalshi_rsi_reversion",
            TradeTickRSIReversionStrategy,
            TradeTickRSIReversionConfig,
        ),
        (
            "backtests.kalshi_trade_tick.kalshi_spread_capture",
            TradeTickMeanReversionStrategy,
            TradeTickMeanReversionConfig,
        ),
    ],
)
def test_kalshi_backtests_build_expected_trade_tick_strategy(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    strategy_cls: type,
    config_cls: type,
):
    module = importlib.import_module(module_name)
    captured: dict[str, object] = {}

    async def _fake_run_single_market_trade_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return None

    monkeypatch.setattr(
        module,
        "run_single_market_trade_backtest",
        _fake_run_single_market_trade_backtest,
    )

    asyncio.run(module.run())

    strategy = captured["strategy_factory"](INSTRUMENT_ID)
    assert isinstance(strategy, strategy_cls)
    assert isinstance(strategy.config, config_cls)
    assert captured["name"] == module.NAME
    assert captured["market_ticker"] == module.MARKET_TICKER
    assert captured["lookback_days"] == module.LOOKBACK_DAYS
