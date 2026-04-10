from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting._experiments import build_backtest_for_experiment
from prediction_market_extensions.backtesting._experiments import build_replay_experiment
from prediction_market_extensions.backtesting._market_data_support import MarketDataSupport
from prediction_market_extensions.backtesting._market_data_support import build_single_market_replay
from prediction_market_extensions.backtesting._market_data_support import register_market_data_support
from prediction_market_extensions.backtesting._market_data_support import unregister_market_data_support
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.adapters.prediction_market import HistoricalReplayAdapter
from prediction_market_extensions.adapters.prediction_market import ReplayAdapterKey
from prediction_market_extensions.adapters.prediction_market import ReplayEngineProfile
from prediction_market_extensions.adapters.prediction_market import ReplayLoadRequest
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue


@dataclass(frozen=True)
class FakeReplay:
    market_slug: str


class FakeAdapter(HistoricalReplayAdapter):
    @property
    def key(self) -> ReplayAdapterKey:
        return ReplayAdapterKey("demo", "fake", "trade_tick")

    @property
    def replay_spec_type(self) -> type[FakeReplay]:
        return FakeReplay

    def build_single_market_replay(self, *, field_values: dict[str, Any]) -> FakeReplay:
        market_slug = field_values.get("market_slug")
        if market_slug is None:
            raise ValueError("market_slug is required for the fake adapter.")
        return FakeReplay(market_slug=str(market_slug))

    def configure_sources(self, *, sources: tuple[str, ...] | list[str]):
        return nullcontext(SimpleNamespace(summary=f"fake sources={tuple(sources)}"))

    @property
    def engine_profile(self) -> ReplayEngineProfile:
        return ReplayEngineProfile(
            venue=Venue("FAKE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            fee_model_factory=lambda: object(),
        )

    async def load_replay(self, replay: FakeReplay, *, request: ReplayLoadRequest):
        raise AssertionError("load_replay is not needed for this architecture test.")


class _EngineStub:
    def __init__(self, *, config) -> None:  # type: ignore[no-untyped-def]
        self.config = config
        self.venues: list[dict[str, object]] = []

    def add_venue(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self.venues.append(kwargs)


def test_new_adapter_registers_without_core_executor_changes(monkeypatch) -> None:
    support = MarketDataSupport(key=("demo", "trade_tick", "fake"), adapter=FakeAdapter())
    register_market_data_support(support)
    monkeypatch.setattr(backtest_module, "BacktestEngine", _EngineStub)

    try:
        replay = build_single_market_replay(support=support, field_values={"market_slug": "demo-market"})
        experiment = build_replay_experiment(
            name="demo-fake-runner",
            description="Fake adapter acceptance test",
            data=MarketDataConfig(platform="demo", data_type="trade_tick", vendor="fake", sources=("fake:memory",)),
            replays=(replay,),
            strategy_configs=[
                {"strategy_path": "strategies:DemoStrategy", "config_path": "strategies:DemoConfig", "config": {}}
            ],
            initial_cash=100.0,
            probability_window=5,
            min_trades=1,
            emit_html=False,
        )
        backtest = build_backtest_for_experiment(experiment)

        assert backtest.replays == (FakeReplay(market_slug="demo-market"),)
        engine = backtest._build_engine()  # noqa: SLF001
        assert len(engine.venues) == 1
        assert engine.venues[0]["venue"] == Venue("FAKE")
        assert engine.venues[0]["account_type"] == AccountType.CASH
    finally:
        unregister_market_data_support(("demo", "trade_tick", "fake"))
