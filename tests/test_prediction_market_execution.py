from __future__ import annotations

import importlib
from types import SimpleNamespace

from prediction_market_extensions.backtesting._backtest_runtime import build_backtest_run_state
from prediction_market_extensions.backtesting._backtest_runtime import print_backtest_result_warnings
from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketSimConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import PredictionMarketBacktest
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig


class _EngineStub:
    def __init__(self, *, config) -> None:  # type: ignore[no-untyped-def]
        self.config = config
        self.venues: list[dict[str, object]] = []

    def add_venue(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self.venues.append(kwargs)


def test_prediction_market_backtest_build_engine_forwards_execution(monkeypatch):
    monkeypatch.setattr(backtest_module, "BacktestEngine", _EngineStub)

    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(platform="polymarket", data_type="quote_tick", vendor="pmxt"),
        sims=(MarketSimConfig(market_slug="demo-market"),),
        strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        initial_cash=100.0,
        probability_window=16,
        execution=ExecutionModelConfig(
            queue_position=True,
            latency_model=StaticLatencyConfig(
                base_latency_ms=25.0, insert_latency_ms=10.0, update_latency_ms=5.0, cancel_latency_ms=2.0
            ),
        ),
    )

    engine = backtest._build_engine()  # noqa: SLF001

    assert len(engine.venues) == 1
    venue_kwargs = engine.venues[0]
    assert venue_kwargs["queue_position"] is True
    assert venue_kwargs["liquidity_consumption"] is True

    latency_model = venue_kwargs["latency_model"]
    assert latency_model is not None
    assert latency_model.base_latency_nanos == 25_000_000
    assert latency_model.insert_latency_nanos == 35_000_000
    assert latency_model.update_latency_nanos == 30_000_000
    assert latency_model.cancel_latency_nanos == 27_000_000
    assert engine.config.risk_engine.bypass is False


def test_build_backtest_run_state_marks_forced_stop_with_partial_coverage():
    data = [SimpleNamespace(ts_init=0), SimpleNamespace(ts_init=10), SimpleNamespace(ts_init=20)]

    state = build_backtest_run_state(data=data, backtest_end_ns=10, forced_stop=True)

    assert state["terminated_early"] is True
    assert state["stop_reason"] == "account_error"
    assert state["planned_start"] == "1970-01-01T00:00:00+00:00"
    assert state["planned_end"] == "1970-01-01T00:00:00.000000020+00:00"
    assert state["loaded_start"] == "1970-01-01T00:00:00+00:00"
    assert state["loaded_end"] == "1970-01-01T00:00:00.000000020+00:00"
    assert state["simulated_through"] == "1970-01-01T00:00:00.000000010+00:00"
    assert state["coverage_ratio"] == 0.5
    assert state["requested_coverage_ratio"] == 0.5


def test_build_backtest_run_state_uses_requested_window_for_coverage():
    data = [SimpleNamespace(ts_init=10), SimpleNamespace(ts_init=20)]

    state = build_backtest_run_state(
        data=data, backtest_end_ns=20, forced_stop=False, requested_start_ns=0, requested_end_ns=30
    )

    assert state["terminated_early"] is False
    assert state["stop_reason"] is None
    assert state["planned_start"] == "1970-01-01T00:00:00+00:00"
    assert state["planned_end"] == "1970-01-01T00:00:00.000000030+00:00"
    assert state["loaded_start"] == "1970-01-01T00:00:00.000000010+00:00"
    assert state["loaded_end"] == "1970-01-01T00:00:00.000000020+00:00"
    assert state["simulated_through"] == "1970-01-01T00:00:00.000000020+00:00"
    assert state["coverage_ratio"] == 1.0
    assert state["requested_coverage_ratio"] == 2 / 3


def test_quote_tick_pmxt_runner_pins_passive_execution_heuristics():
    module = importlib.import_module("backtests.polymarket_quote_tick_ema_crossover")

    assert module.EXPERIMENT.execution.queue_position is True

    latency_model = module.EXPERIMENT.execution.build_latency_model()
    assert latency_model is not None
    assert latency_model.base_latency_nanos == 75_000_000
    assert latency_model.insert_latency_nanos == 85_000_000
    assert latency_model.update_latency_nanos == 80_000_000
    assert latency_model.cancel_latency_nanos == 80_000_000


def test_result_warnings_distinguish_loaded_and_requested_coverage(capsys):
    print_backtest_result_warnings(
        results=[
            {
                "terminated_early": True,
                "stop_reason": "incomplete_window",
                "slug": "demo-market",
                "simulated_through": "2026-03-24T06:00:00+00:00",
                "coverage_ratio": 0.5,
                "requested_coverage_ratio": 0.25,
            }
        ],
        market_key="slug",
    )

    output = capsys.readouterr().out
    assert "50.0% of the loaded-data window" in output
    assert "25.0% of the requested window" in output
