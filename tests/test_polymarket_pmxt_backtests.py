from __future__ import annotations

import importlib

import pytest

from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_RELAY_SAMPLE_END_TIME,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_RELAY_SAMPLE_START_TIME,
)
from backtests._shared._strategy_configs import build_strategies_from_configs
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
EXPECTED_MARKET_SLUG = (
    "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"
)
EXPECTED_START_TIME = DEFAULT_PMXT_RELAY_SAMPLE_START_TIME
EXPECTED_END_TIME = DEFAULT_PMXT_RELAY_SAMPLE_END_TIME


@pytest.mark.parametrize(
    ("module_name", "strategy_cls", "config_cls"),
    [
        (
            "backtests.polymarket_quote_tick_pmxt_breakout",
            QuoteTickBreakoutStrategy,
            QuoteTickBreakoutConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_deep_value_hold",
            QuoteTickDeepValueHoldStrategy,
            QuoteTickDeepValueHoldConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_ema_crossover",
            QuoteTickEMACrossoverStrategy,
            QuoteTickEMACrossoverConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_final_period_momentum",
            QuoteTickFinalPeriodMomentumStrategy,
            QuoteTickFinalPeriodMomentumConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_late_favorite_limit_hold",
            QuoteTickLateFavoriteLimitHoldStrategy,
            QuoteTickLateFavoriteLimitHoldConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_panic_fade",
            QuoteTickPanicFadeStrategy,
            QuoteTickPanicFadeConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_rsi_reversion",
            QuoteTickRSIReversionStrategy,
            QuoteTickRSIReversionConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_spread_capture",
            QuoteTickMeanReversionStrategy,
            QuoteTickMeanReversionConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_threshold_momentum",
            QuoteTickThresholdMomentumStrategy,
            QuoteTickThresholdMomentumConfig,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_vwap_reversion",
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

    def _fake_run_reported_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return []

    monkeypatch.setattr(module, "run_reported_backtest", _fake_run_reported_backtest)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS,
        instrument_id=INSTRUMENT_ID,
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, strategy_cls)
    assert isinstance(strategy.config, config_cls)
    assert module.BACKTEST.name == module.NAME
    assert module.BACKTEST.data == module.DATA
    assert module.BACKTEST.sims == module.SIMS
    assert module.BACKTEST.initial_cash == module.INITIAL_CASH
    assert module.BACKTEST.min_quotes == module.MIN_QUOTES
    assert module.BACKTEST.min_price_range == module.MIN_PRICE_RANGE
    assert module.BACKTEST.probability_window == module.PROBABILITY_WINDOW
    assert len(module.SIMS) == 1
    sim = module.SIMS[0]
    assert sim.market_slug == EXPECTED_MARKET_SLUG
    assert sim.token_index == 0
    assert sim.start_time == EXPECTED_START_TIME
    assert sim.end_time == EXPECTED_END_TIME
    assert captured["backtest"] is module.BACKTEST
    assert captured["report"] == module.REPORT


def test_pmxt_sports_backtest_uses_fixed_samples(
    monkeypatch: pytest.MonkeyPatch,
):
    module = importlib.import_module(
        "backtests.polymarket_quote_tick_pmxt_sports_vwap_reversion"
    )
    finalized_calls: list[dict[str, object]] = []
    fake_results = [
        {
            "slug": spec.market_slug,
            "quotes": 1000,
            "fills": 2,
            "pnl": 1.25,
            "outcome": spec.outcome,
        }
        for spec in module.SIMS
    ]
    monkeypatch.setattr(module.BACKTEST, "run", lambda: fake_results)
    monkeypatch.setattr(
        module,
        "finalize_market_results",
        lambda **kwargs: finalized_calls.append(kwargs),
    )

    module.run()

    assert module.BACKTEST.name == module.NAME
    assert module.BACKTEST.data == module.DATA
    assert module.BACKTEST.sims == module.SIMS
    assert module.BACKTEST.initial_cash == module.INITIAL_CASH
    assert module.BACKTEST.min_quotes == module.MIN_QUOTES
    assert module.BACKTEST.min_price_range == module.MIN_PRICE_RANGE
    assert module.BACKTEST.probability_window == module.VWAP_WINDOW
    assert len(module.SIMS) == 5
    for sim in module.SIMS:
        assert sim.market_slug
        assert sim.start_time == EXPECTED_START_TIME
        assert sim.end_time == EXPECTED_END_TIME
        assert sim.outcome == "Yes"

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS,
        instrument_id=INSTRUMENT_ID,
    )
    assert len(strategies) == 1
    strategy = strategies[0]
    assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
    assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)

    assert len(finalized_calls) == 1
    assert finalized_calls[0]["name"] == module.NAME
    assert finalized_calls[0]["report"] == module.REPORT
    assert finalized_calls[0]["results"] == fake_results


def test_pmxt_runner_window_env_overrides(monkeypatch: pytest.MonkeyPatch):
    runner = importlib.import_module(
        "backtests._shared._polymarket_quote_tick_pmxt_runner"
    )
    monkeypatch.setenv("START_TIME", "2026-02-21T16:00:00Z")
    monkeypatch.setenv("END_TIME", "2026-02-23T06:00:00Z")
    monkeypatch.setenv("LOOKBACK_HOURS", "38")

    start_time, end_time, lookback_hours = runner._apply_window_env_overrides(  # noqa: SLF001
        start_time=EXPECTED_START_TIME,
        end_time=EXPECTED_END_TIME,
        lookback_hours=None,
    )

    assert start_time == "2026-02-21T16:00:00Z"
    assert end_time == "2026-02-23T06:00:00Z"
    assert lookback_hours == 38.0
