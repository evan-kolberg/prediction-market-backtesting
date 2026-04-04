from __future__ import annotations

from backtests._shared import _prediction_market_backtest as backtest_module
from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_runner import MarketDataConfig


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
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
        ),
        sims=(MarketSimConfig(market_slug="demo-market"),),
        strategy_configs=(),
        initial_cash=100.0,
        probability_window=16,
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
