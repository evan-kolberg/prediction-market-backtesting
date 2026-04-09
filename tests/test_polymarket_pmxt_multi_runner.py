from __future__ import annotations

import asyncio
from types import SimpleNamespace

from prediction_market_extensions.backtesting import (
    _polymarket_quote_tick_pmxt_multi_runner as multi_runner,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketSimConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_runner import (
    MarketDataConfig,
)


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
        strategy_factory=lambda instrument_id: SimpleNamespace(
            instrument_id=instrument_id
        ),
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
        strategy_factory=lambda instrument_id: SimpleNamespace(
            instrument_id=instrument_id
        ),
        initial_cash=100.0,
        probability_window=5,
    )

    results = asyncio.run(
        multi_runner.run_multi_sim_pmxt_backtest_async(backtest=backtest)
    )

    assert len(results) == 1
    assert captured["nautilus_log_level"] == "INFO"


def test_multi_sim_pmxt_runner_emits_unique_chart_paths_for_repeated_markets(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def _fake_run_single_market_pmxt_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.append(kwargs)
        return {
            "slug": kwargs["market_slug"],
            "quotes": 2,
            "fills": 0,
            "pnl": 0.0,
            "chart_path": kwargs["chart_output_path"],
            "price_series": [],
            "pnl_series": [],
            "equity_series": [],
            "cash_series": [],
            "fill_events": [],
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
                metadata={"sim_label": "sample-a"},
            ),
            MarketSimConfig(
                market_slug="demo-market",
                start_time="2026-02-21T17:00:00Z",
                end_time="2026-02-21T18:00:00Z",
                metadata={"sim_label": "sample-b"},
            ),
        ),
        strategy_factory=lambda instrument_id: SimpleNamespace(
            instrument_id=instrument_id
        ),
        initial_cash=100.0,
        probability_window=5,
        emit_html=True,
        return_summary_series=True,
    )

    results = asyncio.run(
        multi_runner.run_multi_sim_pmxt_backtest_async(backtest=backtest)
    )

    assert len(results) == 2
    assert captured[0]["emit_html"] is True
    assert captured[0]["return_summary_series"] is True
    assert captured[1]["emit_html"] is True
    assert captured[1]["return_summary_series"] is True
    assert captured[0]["chart_output_path"] != captured[1]["chart_output_path"]
    assert "sample-a" in str(captured[0]["chart_output_path"])
    assert "sample-b" in str(captured[1]["chart_output_path"])
