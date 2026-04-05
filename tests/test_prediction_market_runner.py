from __future__ import annotations

import asyncio
from types import SimpleNamespace

from backtests._shared import _prediction_market_runner as runner
from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared.data_sources import Native
from backtests._shared.data_sources import PMXT
from backtests._shared.data_sources import PMXT_VENDOR
from backtests._shared.data_sources import Polymarket
from backtests._shared.data_sources import QuoteTick
from backtests._shared.data_sources import TradeTick


def test_market_data_config_normalizes_values() -> None:
    data = runner.MarketDataConfig(
        platform=Polymarket,
        data_type=QuoteTick,
        vendor=PMXT,
        sources=(" gamma-api.polymarket.com ", "", " /tmp/data "),
    )

    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "pmxt"
    assert data.sources == ("gamma-api.polymarket.com", "/tmp/data")


def test_generic_runner_dispatches_polymarket_trade_tick(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_runner(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(runner, "load_single_market_runner", lambda spec: _fake_runner)

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(
                platform=Polymarket,
                data_type=TradeTick,
                vendor=Native,
                sources=("gamma-api.polymarket.com",),
            ),
            market_slug="demo-market",
            lookback_days=2,
            probability_window=5,
            initial_cash=100.0,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result == {"ok": True}
    assert captured["market_slug"] == "demo-market"
    assert captured["lookback_days"] == 2
    assert captured["data_sources"] == ("gamma-api.polymarket.com",)


def test_generic_runner_dispatches_kalshi_trade_tick(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_runner(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(runner, "load_single_market_runner", lambda spec: _fake_runner)

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(
                platform="kalshi",
                data_type=TradeTick,
                vendor=Native,
                sources=("api.elections.kalshi.com/trade-api/v2",),
            ),
            market_ticker="KALSHI-TEST",
            lookback_days=2,
            probability_window=5,
            initial_cash=100.0,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result == {"ok": True}
    assert captured["market_ticker"] == "KALSHI-TEST"
    assert captured["lookback_days"] == 2
    assert captured["data_sources"] == ("api.elections.kalshi.com/trade-api/v2",)


def test_generic_runner_dispatches_pmxt_quote_tick(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_runner(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(runner, "load_single_market_runner", lambda spec: _fake_runner)

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(
                platform=Polymarket,
                data_type=QuoteTick,
                vendor=PMXT,
                sources=(
                    "local:/Volumes/LaCie/pmxt_raws",
                    "archive:mirror.example.com",
                    "relay:relay.example.com",
                ),
            ),
            market_slug="demo-market",
            token_index=1,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            min_quotes=5,
            probability_window=12,
            initial_cash=100.0,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        )
    )

    assert result == {"ok": True}
    assert captured["market_slug"] == "demo-market"
    assert captured["token_index"] == 1
    assert captured["start_time"] == "2026-03-21T10:00:00Z"
    assert captured["end_time"] == "2026-03-21T12:00:00Z"
    assert captured["data_sources"] == (
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:mirror.example.com",
        "relay:relay.example.com",
    )


def test_generic_runner_forwards_strategy_configs(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_runner(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(runner, "load_single_market_runner", lambda spec: _fake_runner)

    strategy_configs = [
        {
            "strategy_path": "strategies:QuoteTickBreakoutStrategy",
            "config_path": "strategies:QuoteTickBreakoutConfig",
            "config": {"window": 20},
        }
    ]

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(
                platform=Polymarket,
                data_type=QuoteTick,
                vendor=PMXT_VENDOR,
            ),
            market_slug="demo-market",
            probability_window=20,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            strategy_configs=strategy_configs,
        )
    )

    assert result == {"ok": True}
    assert captured["strategy_configs"] == strategy_configs
    assert captured["strategy_factory"] is None


def test_generic_runner_forwards_execution(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_runner(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(runner, "load_single_market_runner", lambda spec: _fake_runner)

    execution = ExecutionModelConfig(
        queue_position=True,
        latency_model=StaticLatencyConfig(base_latency_ms=12.5),
    )

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(
                platform=Polymarket,
                data_type=QuoteTick,
                vendor=PMXT,
            ),
            market_slug="demo-market",
            probability_window=20,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            execution=execution,
        )
    )

    assert result == {"ok": True}
    assert captured["execution"] == execution
