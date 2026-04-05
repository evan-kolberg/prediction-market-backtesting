from __future__ import annotations

import asyncio
from types import SimpleNamespace

from backtests._shared import _polymarket_quote_tick_pmxt_runner as pmxt_runner
from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig


def test_pmxt_runner_uses_l2_execution_settings(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        captured["backtest"] = self
        return [
            {
                "slug": "demo-market",
                "quotes": 2,
                "fills": 0,
                "pnl": 0.0,
                "outcome": "YES",
                "realized_outcome": 1.0,
                "token_index": 0,
            }
        ]

    monkeypatch.setattr(
        pmxt_runner.PredictionMarketBacktest, "run_async", _fake_run_async
    )

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            lookback_hours=1.0,
            end_time="1970-01-01T01:00:00Z",
            min_quotes=2,
            min_price_range=0.0,
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result is not None
    backtest = captured["backtest"]
    assert backtest.data.platform == "polymarket"
    assert backtest.data.data_type == "quote_tick"
    assert backtest.data.vendor == "pmxt"
    assert backtest.emit_html is False
    assert backtest.execution.queue_position is False
    assert backtest.execution.build_latency_model() is None


def test_pmxt_runner_forwards_queue_position_and_latency(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        captured["backtest"] = self
        return [
            {
                "slug": "demo-market",
                "quotes": 2,
                "fills": 0,
                "pnl": 0.0,
                "outcome": "YES",
                "realized_outcome": 1.0,
                "token_index": 0,
            }
        ]

    monkeypatch.setattr(
        pmxt_runner.PredictionMarketBacktest, "run_async", _fake_run_async
    )

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            lookback_hours=1.0,
            end_time="1970-01-01T01:00:00Z",
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
            execution=ExecutionModelConfig(
                queue_position=True,
                latency_model=StaticLatencyConfig(
                    base_latency_ms=25.0,
                    insert_latency_ms=10.0,
                    update_latency_ms=5.0,
                    cancel_latency_ms=2.0,
                ),
            ),
        )
    )

    assert result is not None
    backtest = captured["backtest"]
    assert backtest.execution.queue_position is True
    latency_model = backtest.execution.build_latency_model()
    assert latency_model is not None
    assert latency_model.base_latency_nanos == 25_000_000
    assert latency_model.insert_latency_nanos == 35_000_000
    assert latency_model.update_latency_nanos == 30_000_000
    assert latency_model.cancel_latency_nanos == 27_000_000


def test_pmxt_runner_respects_explicit_start_and_end_times(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        captured["backtest"] = self
        return [
            {
                "slug": "demo-market",
                "quotes": 2,
                "fills": 0,
                "pnl": 0.0,
                "outcome": "YES",
                "realized_outcome": 1.0,
                "token_index": 0,
            }
        ]

    monkeypatch.setattr(
        pmxt_runner.PredictionMarketBacktest, "run_async", _fake_run_async
    )

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            start_time="2026-03-22T09:00:00Z",
            end_time="2026-03-22T13:00:00Z",
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result is not None
    sim = captured["backtest"].sims[0]
    assert sim.start_time == "2026-03-22T09:00:00Z"
    assert sim.end_time == "2026-03-22T13:00:00Z"
    assert sim.lookback_hours is None


def test_pmxt_runner_forwards_nautilus_log_level(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        captured["backtest"] = self
        return [
            {
                "slug": "demo-market",
                "quotes": 2,
                "fills": 0,
                "pnl": 0.0,
                "outcome": "YES",
                "realized_outcome": 1.0,
                "token_index": 0,
            }
        ]

    monkeypatch.setattr(
        pmxt_runner.PredictionMarketBacktest, "run_async", _fake_run_async
    )

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            lookback_hours=1.0,
            end_time="1970-01-01T01:00:00Z",
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            nautilus_log_level="INFO",
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result is not None
    assert captured["backtest"].nautilus_log_level == "INFO"
