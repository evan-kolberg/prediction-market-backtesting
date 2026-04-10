from __future__ import annotations

import asyncio
from types import SimpleNamespace

from prediction_market_extensions.backtesting import _prediction_market_runner as runner
from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting.data_sources import Native
from prediction_market_extensions.backtesting.data_sources import PMXT
from prediction_market_extensions.backtesting.data_sources import PMXT_VENDOR
from prediction_market_extensions.backtesting.data_sources import Polymarket
from prediction_market_extensions.backtesting.data_sources import QuoteTick
from prediction_market_extensions.backtesting.data_sources import TradeTick
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay


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
    replay = object()

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(
        runner,
        "build_single_market_replay",
        lambda *, support, field_values: (
            captured.update(field_values=field_values, support=support) or replay
        ),
    )
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

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
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        )
    )

    assert result == {"ok": True}
    field_values = captured["field_values"]
    assert field_values["market_slug"] == "demo-market"
    assert field_values["lookback_days"] == 2
    assert captured["experiment"].replays == (replay,)
    assert captured["experiment"].data.sources == ("gamma-api.polymarket.com",)


def test_generic_runner_dispatches_kalshi_trade_tick(monkeypatch) -> None:
    captured: dict[str, object] = {}
    replay = object()

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(
        runner,
        "build_single_market_replay",
        lambda *, support, field_values: (
            captured.update(field_values=field_values, support=support) or replay
        ),
    )
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

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
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        )
    )

    assert result == {"ok": True}
    field_values = captured["field_values"]
    assert field_values["market_ticker"] == "KALSHI-TEST"
    assert field_values["lookback_days"] == 2
    assert captured["experiment"].replays == (replay,)
    assert captured["experiment"].data.sources == ("api.elections.kalshi.com/trade-api/v2",)


def test_generic_runner_dispatches_pmxt_quote_tick(monkeypatch) -> None:
    captured: dict[str, object] = {}
    replay = object()

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(
        runner,
        "build_single_market_replay",
        lambda *, support, field_values: (
            captured.update(field_values=field_values, support=support) or replay
        ),
    )
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

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
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        )
    )

    assert result == {"ok": True}
    field_values = captured["field_values"]
    assert field_values["market_slug"] == "demo-market"
    assert field_values["token_index"] == 1
    assert field_values["start_time"] == "2026-03-21T10:00:00Z"
    assert field_values["end_time"] == "2026-03-21T12:00:00Z"
    assert captured["experiment"].data.sources == (
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:mirror.example.com",
        "relay:relay.example.com",
    )


def test_generic_runner_forwards_strategy_configs(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(runner, "build_single_market_replay", lambda **_kwargs: object())
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

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
                platform=Polymarket, data_type=QuoteTick, vendor=PMXT_VENDOR
            ),
            market_slug="demo-market",
            probability_window=20,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            strategy_configs=strategy_configs,
        )
    )

    assert result == {"ok": True}
    assert captured["experiment"].strategy_configs == tuple(strategy_configs)
    assert captured["experiment"].strategy_factory is None


def test_generic_runner_forwards_execution(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(runner, "build_single_market_replay", lambda **_kwargs: object())
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

    execution = ExecutionModelConfig(
        queue_position=True, latency_model=StaticLatencyConfig(base_latency_ms=12.5)
    )

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(platform=Polymarket, data_type=QuoteTick, vendor=PMXT),
            market_slug="demo-market",
            probability_window=20,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            execution=execution,
        )
    )

    assert result == {"ok": True}
    assert captured["experiment"].execution == execution


def test_generic_runner_forwards_chart_output_path(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_replay_experiment_async(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return [{"ok": True}]

    monkeypatch.setattr(runner, "build_single_market_replay", lambda **_kwargs: object())
    monkeypatch.setattr(runner, "run_replay_experiment_async", _fake_run_replay_experiment_async)

    result = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=runner.MarketDataConfig(platform=Polymarket, data_type=QuoteTick, vendor=PMXT),
            market_slug="demo-market",
            probability_window=20,
            start_time="2026-03-21T10:00:00Z",
            end_time="2026-03-21T12:00:00Z",
            chart_output_path="output/demo_chart.html",
        )
    )

    assert result == {"ok": True}
    assert captured["experiment"].chart_output_path == "output/demo_chart.html"


def test_prediction_market_backtest_resolves_chart_output_paths() -> None:
    backtest = PredictionMarketBacktest(
        name="demo",
        data=runner.MarketDataConfig(platform=Polymarket, data_type=QuoteTick, vendor=PMXT),
        replays=(QuoteReplay(market_slug="demo-market"),),
        strategy_configs=[
            {
                "strategy_path": "strategies:DemoStrategy",
                "config_path": "strategies:DemoConfig",
                "config": {},
            }
        ],
        initial_cash=100.0,
        probability_window=5,
        emit_html=False,
        chart_output_path="output/custom.html",
    )
    multi_sim_backtest = PredictionMarketBacktest(
        name="demo",
        data=runner.MarketDataConfig(platform=Polymarket, data_type=QuoteTick, vendor=PMXT),
        replays=(QuoteReplay(market_slug="demo-market"), QuoteReplay(market_slug="demo-market-2")),
        strategy_configs=[
            {
                "strategy_path": "strategies:DemoStrategy",
                "config_path": "strategies:DemoConfig",
                "config": {},
            }
        ],
        initial_cash=100.0,
        probability_window=5,
        emit_html=False,
        chart_output_path="output/custom.html",
    )

    assert backtest._resolve_chart_output_path(market_id="demo-market").name == "custom.html"
    assert (
        multi_sim_backtest._resolve_chart_output_path(market_id="demo-market").name
        == "custom_demo-market.html"
    )
    multi_sim_backtest.chart_output_path = "output/{name}_{market_id}.html"
    assert (
        multi_sim_backtest._resolve_chart_output_path(market_id="demo-market").name
        == "demo_demo-market.html"
    )
