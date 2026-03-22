from __future__ import annotations

import asyncio
import importlib

import pytest

from strategies import QuoteTickBreakoutConfig
from strategies import QuoteTickBreakoutStrategy
from strategies import QuoteTickDeepValueHoldConfig
from strategies import QuoteTickDeepValueHoldStrategy
from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickEMACrossoverStrategy
from strategies import QuoteTickFinalPeriodMomentumConfig
from strategies import QuoteTickFinalPeriodMomentumStrategy
from strategies import QuoteTickLateFavoriteLimitHoldConfig
from strategies import QuoteTickLateFavoriteLimitHoldStrategy
from strategies import QuoteTickMeanReversionConfig
from strategies import QuoteTickMeanReversionStrategy
from strategies import QuoteTickPanicFadeConfig
from strategies import QuoteTickPanicFadeStrategy
from strategies import QuoteTickRSIReversionConfig
from strategies import QuoteTickRSIReversionStrategy
from strategies import QuoteTickThresholdMomentumConfig
from strategies import QuoteTickThresholdMomentumStrategy
from strategies import QuoteTickVWAPReversionConfig
from strategies import QuoteTickVWAPReversionStrategy
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


@pytest.mark.parametrize(
    ("module_name", "strategy_cls", "config_cls"),
    [
        (
            "backtests.polymarket_pmxt_relay_breakout",
            QuoteTickBreakoutStrategy,
            QuoteTickBreakoutConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_deep_value_hold",
            QuoteTickDeepValueHoldStrategy,
            QuoteTickDeepValueHoldConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_ema_crossover",
            QuoteTickEMACrossoverStrategy,
            QuoteTickEMACrossoverConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_final_period_momentum",
            QuoteTickFinalPeriodMomentumStrategy,
            QuoteTickFinalPeriodMomentumConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_late_favorite_limit_hold",
            QuoteTickLateFavoriteLimitHoldStrategy,
            QuoteTickLateFavoriteLimitHoldConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_panic_fade",
            QuoteTickPanicFadeStrategy,
            QuoteTickPanicFadeConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_rsi_reversion",
            QuoteTickRSIReversionStrategy,
            QuoteTickRSIReversionConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_spread_capture",
            QuoteTickMeanReversionStrategy,
            QuoteTickMeanReversionConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_threshold_momentum",
            QuoteTickThresholdMomentumStrategy,
            QuoteTickThresholdMomentumConfig,
        ),
        (
            "backtests.polymarket_pmxt_relay_vwap_reversion",
            QuoteTickVWAPReversionStrategy,
            QuoteTickVWAPReversionConfig,
        ),
    ],
)
def test_pmxt_backtests_build_expected_quote_tick_strategy(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    strategy_cls: type,
    config_cls: type,
):
    module = importlib.import_module(module_name)
    captured: dict[str, object] = {}

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return None

    monkeypatch.setattr(
        module,
        "run_single_market_pmxt_backtest",
        _fake_run_single_market_pmxt_backtest,
    )

    asyncio.run(module.run())

    strategy = captured["strategy_factory"](INSTRUMENT_ID)
    assert isinstance(strategy, strategy_cls)
    assert isinstance(strategy.config, config_cls)
    assert captured["name"] == module.NAME
    assert captured["market_slug"] == module.MARKET_SLUG
    assert captured["token_index"] == module.TOKEN_INDEX
    assert captured["lookback_hours"] == module.LOOKBACK_HOURS
