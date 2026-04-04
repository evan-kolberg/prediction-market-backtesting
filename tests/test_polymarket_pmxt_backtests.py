from __future__ import annotations

import importlib
import pandas as pd
import pytest

from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_CLOSE_WINDOW_END_TIME,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_CLOSE_WINDOW_START_TIME,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_MARKET_ACTIVATION_START_NS,
)
from backtests._shared._polymarket_quote_tick_defaults import (
    DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
)
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
EXPECTED_CLOSE_WINDOW_START_TIME = DEFAULT_PMXT_CLOSE_WINDOW_START_TIME
EXPECTED_CLOSE_WINDOW_END_TIME = DEFAULT_PMXT_CLOSE_WINDOW_END_TIME
EXPECTED_PMXT_SOURCES = (
    "/Volumes/LaCie/pmxt_raws",
    "r2.pmxt.dev",
    "209-209-10-83.sslip.io",
)
TIME_BASED_SINGLE_MARKET_MODULES = {
    "backtests.polymarket_quote_tick_pmxt_final_period_momentum",
    "backtests.polymarket_quote_tick_pmxt_late_favorite_limit_hold",
    "backtests.polymarket_quote_tick_pmxt_threshold_momentum",
}


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
    assert module.BACKTEST.initial_cash == 100.0
    assert module.BACKTEST.min_quotes == 500
    assert module.BACKTEST.min_price_range == 0.005
    assert module.BACKTEST.probability_window > 0
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert len(module.SIMS) == 1
    sim = module.SIMS[0]
    assert sim.market_slug == EXPECTED_MARKET_SLUG
    assert sim.token_index == 0
    if module_name in TIME_BASED_SINGLE_MARKET_MODULES:
        assert sim.start_time == EXPECTED_CLOSE_WINDOW_START_TIME
        assert sim.end_time == EXPECTED_CLOSE_WINDOW_END_TIME
    else:
        assert sim.start_time == EXPECTED_START_TIME
        assert sim.end_time == EXPECTED_END_TIME
    assert captured["backtest"] is module.BACKTEST
    assert captured["report"] == module.REPORT


def test_pmxt_late_favorite_runner_marks_settlement_pnl(
    monkeypatch: pytest.MonkeyPatch,
):
    module = importlib.import_module(
        "backtests.polymarket_quote_tick_pmxt_late_favorite_limit_hold"
    )
    finalized_calls: list[dict[str, object]] = []
    fake_results = [
        {
            "slug": module.SIMS[0].market_slug,
            "quotes": 1000,
            "fills": 1,
            "pnl": -0.75,
            "realized_outcome": 1.0,
            "fill_events": [{"action": "buy", "price": 0.90, "quantity": 25.0}],
        }
    ]
    monkeypatch.setattr(module.BACKTEST, "run", lambda: fake_results)
    monkeypatch.setattr(module, "compute_binary_settlement_pnl", lambda *_args: 2.5)
    monkeypatch.setattr(
        module,
        "finalize_market_results",
        lambda **kwargs: finalized_calls.append(kwargs),
    )

    module.run()

    assert module.REPORT.pnl_label == "Settlement PnL (USDC)"
    assert module.SIMS[0].start_time == EXPECTED_CLOSE_WINDOW_START_TIME
    assert module.SIMS[0].end_time == EXPECTED_CLOSE_WINDOW_END_TIME
    assert fake_results[0]["market_exit_pnl"] == -0.75
    assert fake_results[0]["pnl"] == 2.5
    assert len(finalized_calls) == 1
    assert finalized_calls[0]["name"] == module.NAME
    assert finalized_calls[0]["report"] == module.REPORT
    assert finalized_calls[0]["results"] == fake_results

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS,
        instrument_id=INSTRUMENT_ID,
    )
    assert len(strategies) == 1
    strategy = strategies[0]
    assert isinstance(strategy, QuoteTickLateFavoriteLimitHoldStrategy)
    assert isinstance(strategy.config, QuoteTickLateFavoriteLimitHoldConfig)


@pytest.mark.parametrize(
    ("module_name", "activation_start_ns", "market_close_time_ns"),
    [
        (
            "backtests.polymarket_quote_tick_pmxt_late_favorite_limit_hold",
            DEFAULT_PMXT_MARKET_ACTIVATION_START_NS,
            DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_threshold_momentum",
            DEFAULT_PMXT_MARKET_ACTIVATION_START_NS,
            DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
        ),
        (
            "backtests.polymarket_quote_tick_pmxt_final_period_momentum",
            DEFAULT_PMXT_MARKET_CLOSE_TIME_NS - 180 * 60 * 1_000_000_000,
            DEFAULT_PMXT_MARKET_CLOSE_TIME_NS,
        ),
    ],
)
def test_time_based_pmxt_single_market_samples_overlap_strategy_window(
    module_name: str,
    activation_start_ns: int,
    market_close_time_ns: int,
):
    module = importlib.import_module(module_name)
    sim = module.SIMS[0]

    assert sim.start_time == EXPECTED_CLOSE_WINDOW_START_TIME
    assert sim.end_time == EXPECTED_CLOSE_WINDOW_END_TIME
    assert sim.start_time < sim.end_time

    start_ns = pytest.importorskip("pandas").Timestamp(sim.start_time).value
    end_ns = pytest.importorskip("pandas").Timestamp(sim.end_time).value

    assert start_ns <= activation_start_ns
    assert market_close_time_ns <= end_ns


def test_pmxt_sports_backtest_discovers_live_samples(
    monkeypatch: pytest.MonkeyPatch,
):
    module = importlib.import_module(
        "backtests.polymarket_quote_tick_pmxt_sports_vwap_reversion"
    )
    now = pd.Timestamp("2026-04-04T20:00:00Z")
    html = """
    <a href="/event/cs2-prv-lgc-2026-04-04">cs2</a>
    <a href="/event/fl1-lil-rcl-2026-04-04">soccer</a>
    """

    market_payloads = {
        "cs2-prv-lgc-2026-04-04": {
            "slug": "cs2-prv-lgc-2026-04-04",
            "sportsMarketType": "moneyline",
            "gameStartTime": "2026-04-04 14:10:00+00",
            "bestBid": 0.45,
            "bestAsk": 0.46,
            "volume24hrClob": 900000.0,
            "liquidityClob": 150000.0,
        },
        "fl1-lil-rcl-2026-04-04-draw": {
            "slug": "fl1-lil-rcl-2026-04-04-draw",
            "sportsMarketType": "moneyline",
            "gameStartTime": "2026-04-04 19:05:00+00",
            "bestBid": 0.24,
            "bestAsk": 0.25,
            "volume24hrClob": 128000.0,
            "liquidityClob": 42000.0,
        },
        "fl1-lil-rcl-2026-04-04-rcl": {
            "slug": "fl1-lil-rcl-2026-04-04-rcl",
            "sportsMarketType": "moneyline",
            "gameStartTime": "2026-04-04 19:05:00+00",
            "bestBid": 0.10,
            "bestAsk": 0.11,
            "volume24hrClob": 131000.0,
            "liquidityClob": 37700.0,
        },
    }
    event_payloads = {
        "fl1-lil-rcl-2026-04-04": {
            "markets": [
                {"slug": "fl1-lil-rcl-2026-04-04-draw"},
                {"slug": "fl1-lil-rcl-2026-04-04-rcl"},
            ]
        }
    }

    monkeypatch.setattr(module, "_fetch_text", lambda _url: html)
    monkeypatch.setattr(module, "_fetch_market", lambda slug: market_payloads.get(slug))
    monkeypatch.setattr(module, "_fetch_event", lambda slug: event_payloads.get(slug))

    sims = module.discover_recent_market_sims(now=now, limit=3)

    finalized_calls: list[dict[str, object]] = []
    runner_calls: list[tuple[str, str, str]] = []

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        runner_calls.append(
            (
                kwargs["market_slug"],
                kwargs["start_time"],
                kwargs["end_time"],
            )
        )
        return {
            "slug": kwargs["market_slug"],
            "quotes": 1000,
            "fills": 2,
            "pnl": 1.25,
        }

    monkeypatch.setattr(module, "discover_recent_market_sims", lambda: sims)
    monkeypatch.setattr(
        module,
        "run_single_market_pmxt_backtest",
        _fake_run_single_market_pmxt_backtest,
    )
    monkeypatch.setattr(
        module,
        "finalize_market_results",
        lambda **kwargs: finalized_calls.append(kwargs),
    )

    module.run()

    assert module.BACKTEST.name == module.NAME
    assert module.BACKTEST.data == module.DATA
    assert module.BACKTEST.sims == module.SIMS
    assert module.BACKTEST.initial_cash == 100.0
    assert module.BACKTEST.min_quotes == 500
    assert module.BACKTEST.min_price_range == 0.005
    assert module.BACKTEST.probability_window == 30
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.SIMS == ()
    assert [sim.market_slug for sim in sims] == [
        "cs2-prv-lgc-2026-04-04",
        "fl1-lil-rcl-2026-04-04-draw",
        "fl1-lil-rcl-2026-04-04-rcl",
    ]
    assert sims[0].start_time == "2026-04-04T18:00:00Z"
    assert sims[0].end_time == "2026-04-04T20:00:00Z"
    assert sims[1].start_time == "2026-04-04T18:00:00Z"
    assert sims[1].end_time == "2026-04-04T20:00:00Z"

    for sim in sims:
        assert sim.market_slug
        assert sim.token_index == 0
        assert isinstance(sim.start_time, str) and sim.start_time
        assert isinstance(sim.end_time, str) and sim.end_time
        assert sim.outcome is None

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS,
        instrument_id=INSTRUMENT_ID,
    )
    assert len(strategies) == 1
    strategy = strategies[0]
    assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
    assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)

    assert runner_calls == [
        (sim.market_slug, sim.start_time, sim.end_time) for sim in sims
    ]
    assert len(finalized_calls) == 1
    assert finalized_calls[0]["name"] == module.NAME
    assert finalized_calls[0]["report"] == module.REPORT
    assert finalized_calls[0]["results"] == [
        {"slug": sim.market_slug, "quotes": 1000, "fills": 2, "pnl": 1.25}
        for sim in sims[: module._TARGET_SIM_COUNT]
    ]


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
