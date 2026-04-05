from __future__ import annotations

import asyncio

from backtests._shared import _polymarket_quote_tick_pmxt_multi_runner as multi_runner
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_runner import MarketDataConfig


def test_multi_sim_pmxt_runner_forwards_nautilus_log_level(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "slug": kwargs["market_slug"],
            "quotes": 2,
            "fills": 0,
            "pnl": 0.0,
        }

    monkeypatch.setattr(
        multi_runner,
        "run_single_market_pmxt_backtest",
        _fake_run_single_market_pmxt_backtest,
    )

    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
        ),
        sims=(
            MarketSimConfig(
                market_slug="demo-market",
                start_time="2026-02-21T16:00:00Z",
                end_time="2026-02-21T17:00:00Z",
            ),
        ),
        strategy_configs=(),
        initial_cash=100.0,
        probability_window=5,
        nautilus_log_level="INFO",
    )

    results = asyncio.run(
        multi_runner.run_multi_sim_pmxt_backtest_async(backtest=backtest)
    )

    assert len(results) == 1
    assert captured["nautilus_log_level"] == "INFO"


def test_multi_sim_pmxt_runner_defaults_to_warning_log_level(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "slug": kwargs["market_slug"],
            "quotes": 2,
            "fills": 0,
            "pnl": 0.0,
        }

    monkeypatch.setattr(
        multi_runner,
        "run_single_market_pmxt_backtest",
        _fake_run_single_market_pmxt_backtest,
    )

    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
        ),
        sims=(
            MarketSimConfig(
                market_slug="demo-market",
                start_time="2026-02-21T16:00:00Z",
                end_time="2026-02-21T17:00:00Z",
            ),
        ),
        strategy_configs=(),
        initial_cash=100.0,
        probability_window=5,
    )

    results = asyncio.run(
        multi_runner.run_multi_sim_pmxt_backtest_async(backtest=backtest)
    )

    assert len(results) == 1
    assert captured["nautilus_log_level"] == "WARNING"
