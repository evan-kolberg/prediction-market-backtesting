from __future__ import annotations

import asyncio
import importlib

import pytest

from backtests.polymarket_quote_tick._defaults import DEFAULT_PMXT_RELAY_SAMPLE_END_TIME
from backtests.polymarket_quote_tick._defaults import (
    DEFAULT_PMXT_RELAY_SAMPLE_START_TIME,
)
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
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_breakout",
            QuoteTickBreakoutStrategy,
            QuoteTickBreakoutConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_deep_value_hold",
            QuoteTickDeepValueHoldStrategy,
            QuoteTickDeepValueHoldConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_ema_crossover",
            QuoteTickEMACrossoverStrategy,
            QuoteTickEMACrossoverConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_final_period_momentum",
            QuoteTickFinalPeriodMomentumStrategy,
            QuoteTickFinalPeriodMomentumConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_late_favorite_limit_hold",
            QuoteTickLateFavoriteLimitHoldStrategy,
            QuoteTickLateFavoriteLimitHoldConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_panic_fade",
            QuoteTickPanicFadeStrategy,
            QuoteTickPanicFadeConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_rsi_reversion",
            QuoteTickRSIReversionStrategy,
            QuoteTickRSIReversionConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_spread_capture",
            QuoteTickMeanReversionStrategy,
            QuoteTickMeanReversionConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_threshold_momentum",
            QuoteTickThresholdMomentumStrategy,
            QuoteTickThresholdMomentumConfig,
        ),
        (
            "backtests.polymarket_quote_tick.polymarket_pmxt_relay_vwap_reversion",
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
    assert captured["market_slug"] == EXPECTED_MARKET_SLUG
    assert captured["token_index"] == 0
    assert captured["start_time"] == EXPECTED_START_TIME
    assert captured["end_time"] == EXPECTED_END_TIME
    assert captured["min_quotes"] == 500
    assert captured["min_price_range"] == 0.005
    assert captured["initial_cash"] == 100.0
    assert "lookback_hours" not in captured


def test_pmxt_sports_backtest_uses_fixed_samples(
    monkeypatch: pytest.MonkeyPatch,
):
    module = importlib.import_module(
        "backtests.polymarket_quote_tick.polymarket_pmxt_relay_sports_vwap_reversion"
    )
    captured_calls: list[dict[str, object]] = []

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured_calls.append(kwargs)
        return {
            "slug": kwargs["market_slug"],
            "quotes": 1000,
            "fills": 2,
            "pnl": 1.25,
            "chart_path": None,
        }

    monkeypatch.setattr(
        module,
        "run_single_market_pmxt_backtest",
        _fake_run_single_market_pmxt_backtest,
    )
    combined_report_calls: list[dict[str, object]] = []
    summary_report_calls: list[dict[str, object]] = []

    monkeypatch.setattr(module, "print_backtest_summary", lambda **kwargs: None)
    monkeypatch.setattr(
        module,
        "save_aggregate_backtest_report",
        lambda **kwargs: summary_report_calls.append(kwargs),
    )
    monkeypatch.setattr(
        module,
        "save_combined_backtest_report",
        lambda **kwargs: combined_report_calls.append(kwargs),
    )

    asyncio.run(module.run())

    assert len(captured_calls) == len(module.SPORT_MARKET_SAMPLES)
    for captured, sample in zip(
        captured_calls,
        module.SPORT_MARKET_SAMPLES,
        strict=True,
    ):
        assert captured["name"] == module.NAME
        assert captured["market_slug"] == sample["market_slug"]
        assert captured["token_index"] == sample["token_index"]
        assert captured["start_time"] == sample["start_time"]
        assert captured["end_time"] == sample["end_time"]
        assert captured["min_quotes"] == module.MIN_QUOTES
        assert captured["min_price_range"] == module.MIN_PRICE_RANGE
        assert captured["initial_cash"] == module.INITIAL_CASH
        assert captured["probability_window"] == module.VWAP_WINDOW
        assert "lookback_hours" not in captured

        strategy = captured["strategy_factory"](INSTRUMENT_ID)
        assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
        assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)

    assert len(combined_report_calls) == 1
    assert combined_report_calls[0]["output_path"] == module.COMBINED_REPORT_PATH
    assert len(combined_report_calls[0]["results"]) == len(module.SPORT_MARKET_SAMPLES)

    assert len(summary_report_calls) == 1
    assert summary_report_calls[0]["output_path"] == module.SUMMARY_REPORT_PATH
    assert len(summary_report_calls[0]["results"]) == len(module.SPORT_MARKET_SAMPLES)


def test_pmxt_runner_window_env_overrides(monkeypatch: pytest.MonkeyPatch):
    runner = importlib.import_module(
        "backtests.polymarket_quote_tick._polymarket_single_market_pmxt_runner"
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
