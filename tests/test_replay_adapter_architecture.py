from __future__ import annotations

import asyncio
from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue

from prediction_market_extensions.adapters.prediction_market import (
    HistoricalReplayAdapter,
    ReplayAdapterKey,
    ReplayEngineProfile,
    ReplayLoadRequest,
)
from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting._experiments import (
    build_backtest_for_experiment,
    build_replay_experiment,
)
from prediction_market_extensions.backtesting._market_data_support import (
    MarketDataSupport,
    build_single_market_replay,
    register_market_data_support,
    unregister_market_data_support,
)
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting.data_sources import replay_adapters


@dataclass(frozen=True)
class FakeReplay:
    market_slug: str


class FakeAdapter(HistoricalReplayAdapter):
    @property
    def key(self) -> ReplayAdapterKey:
        return ReplayAdapterKey("demo", "fake", "book")

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
    support = MarketDataSupport(key=("demo", "book", "fake"), adapter=FakeAdapter())
    register_market_data_support(support)
    monkeypatch.setattr(backtest_module, "BacktestEngine", _EngineStub)

    try:
        replay = build_single_market_replay(
            support=support, field_values={"market_slug": "demo-market"}
        )
        experiment = build_replay_experiment(
            name="demo-fake-runner",
            description="Fake adapter acceptance test",
            data=MarketDataConfig(
                platform="demo", data_type="book", vendor="fake", sources=("fake:memory",)
            ),
            replays=(replay,),
            strategy_configs=[
                {
                    "strategy_path": "strategies:DemoStrategy",
                    "config_path": "strategies:DemoConfig",
                    "config": {},
                }
            ],
            initial_cash=100.0,
            probability_window=5,
            min_book_events=1,
        )
        backtest = build_backtest_for_experiment(experiment)

        assert backtest.replays == (FakeReplay(market_slug="demo-market"),)
        engine = backtest._build_engine()
        assert len(engine.venues) == 1
        assert engine.venues[0]["venue"] == Venue("FAKE")
        assert engine.venues[0]["account_type"] == AccountType.CASH
    finally:
        unregister_market_data_support(("demo", "book", "fake"))


def test_preflight_midpoints_apply_l2_book_state(monkeypatch) -> None:
    class FakeDeltas:
        def __init__(self, updates: tuple[tuple[str, float], ...]) -> None:
            self.updates = updates

    class FakeOrderBook:
        def __init__(self, instrument_id, book_type):  # type: ignore[no-untyped-def]
            del instrument_id, book_type
            self._bid: float | None = None
            self._ask: float | None = None

        def apply_deltas(self, deltas: FakeDeltas) -> None:
            for side, price in deltas.updates:
                if side == "bid":
                    self._bid = price
                else:
                    self._ask = price

        def best_bid_price(self) -> float | None:
            return self._bid

        def best_ask_price(self) -> float | None:
            return self._ask

    monkeypatch.setattr(replay_adapters, "OrderBook", FakeOrderBook)

    count, midpoints = replay_adapters._book_event_count_and_midpoints(
        instrument=SimpleNamespace(id="POLYMARKET.TEST"),
        records=(
            FakeDeltas((("bid", 0.49), ("ask", 0.51))),
            FakeDeltas((("ask", 0.55),)),
        ),
        deltas_type=FakeDeltas,
    )

    assert count == 2
    assert midpoints == (0.5, 0.52)
    assert replay_adapters._price_range(midpoints) == pytest.approx(0.02)


def test_trade_tick_loader_reports_api_and_cache_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys
) -> None:
    class FakeTradeLoader:
        condition_id = "0xcondition"
        token_id = "token"
        instrument = SimpleNamespace()

        def __init__(self) -> None:
            self.calls = 0

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            del start, end
            self.calls += 1
            return []

    loader = FakeTradeLoader()
    monkeypatch.setattr(replay_adapters, "_cache_home", lambda: tmp_path)

    trades = asyncio.run(
        replay_adapters._load_trade_ticks(
            loader,
            start=pd.Timestamp("2026-01-19T00:00:00Z"),
            end=pd.Timestamp("2026-01-19T23:59:59Z"),
            market_label="demo-market",
        )
    )
    output = capsys.readouterr().out

    assert trades == ()
    assert loader.calls == 1
    assert "Loading Polymarket trade ticks for execution demo-market" in output
    assert "polymarket api" in output

    cached_trades = asyncio.run(
        replay_adapters._load_trade_ticks(
            loader,
            start=pd.Timestamp("2026-01-19T00:00:00Z"),
            end=pd.Timestamp("2026-01-19T23:59:59Z"),
            market_label="demo-market",
        )
    )
    cached_output = capsys.readouterr().out

    assert cached_trades == ()
    assert loader.calls == 1
    assert "cache 2026-01-19.parquet" in cached_output
