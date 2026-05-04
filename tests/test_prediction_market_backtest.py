from __future__ import annotations

import asyncio
import os
from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from prediction_market_extensions.adapters.prediction_market import (
    LoadedReplay,
    ReplayAdapterKey,
    ReplayCoverageStats,
    ReplayEngineProfile,
    ReplayLoadRequest,
    ReplayWindow,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
    _loader_progress_env_for_workers,
    _resolve_replay_load_workers,
)
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue


@dataclass(frozen=True)
class _FakeReplay:
    market_slug: str


class _ConcurrentReplayAdapter:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    @property
    def key(self) -> ReplayAdapterKey:
        return ReplayAdapterKey("demo", "fake", "book")

    @property
    def replay_spec_type(self) -> type[_FakeReplay]:
        return _FakeReplay

    def configure_sources(self, *, sources):
        del sources
        return nullcontext(SimpleNamespace(summary="fake source"))

    @property
    def engine_profile(self) -> ReplayEngineProfile:
        return ReplayEngineProfile(
            venue=Venue("FAKE"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            fee_model_factory=lambda: object(),
        )

    async def load_replay(
        self, replay: _FakeReplay, *, request: ReplayLoadRequest
    ) -> LoadedReplay | None:
        del request
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
        finally:
            self.active -= 1

        if replay.market_slug == "skip":
            return None
        return LoadedReplay(
            replay=replay,
            instrument=SimpleNamespace(id=f"FAKE.{replay.market_slug}"),
            records=(),
            outcome="Yes",
            realized_outcome=None,
            metadata={},
            requested_window=ReplayWindow(),
            loaded_window=None,
            coverage_stats=ReplayCoverageStats(
                count=1,
                count_key="book_events",
                market_key="slug",
                market_id=replay.market_slug,
            ),
        )


class _BatchReplayAdapter(_ConcurrentReplayAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.batch_workers: int | None = None
        self.batch_replays: tuple[_FakeReplay, ...] = ()

    async def load_replays(self, replays, *, request: ReplayLoadRequest, workers: int):
        self.batch_workers = workers
        self.batch_replays = tuple(replays)
        loaded = []
        for replay in replays:
            loaded_sim = await self.load_replay(replay, request=request)
            if loaded_sim is not None:
                loaded.append(loaded_sim)
        return loaded


def _build_backtest(**kwargs) -> PredictionMarketBacktest:
    return PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(platform="polymarket", data_type="book", vendor="pmxt"),
        replays=(
            BookReplay(
                market_slug="demo-market",
                start_time="2026-02-21T16:00:00Z",
                end_time="2026-02-21T17:00:00Z",
            ),
        ),
        initial_cash=100.0,
        probability_window=5,
        **kwargs,
    )


def test_strategy_summary_label_uses_config_count() -> None:
    backtest = _build_backtest(
        strategy_configs=(
            {
                "strategy_path": "strategies:BookVWAPReversionStrategy",
                "config_path": "strategies:BookVWAPReversionConfig",
                "config": {"vwap_window": 30},
            },
        )
    )

    assert backtest._strategy_summary_label() == "1 strategy config(s)"


def test_strategy_summary_label_reports_factory() -> None:
    backtest = _build_backtest(strategy_factory=lambda instrument_id: instrument_id)

    assert backtest._strategy_summary_label() == "a strategy factory"


def test_run_async_rejects_duplicate_instruments(monkeypatch) -> None:
    backtest = _build_backtest(strategy_factory=lambda instrument_id: instrument_id)
    duplicate = SimpleNamespace(
        instrument=SimpleNamespace(id="POLYMARKET.DUPLICATE"),
        requested_window=SimpleNamespace(start_ns=None, end_ns=None),
    )

    async def _fake_load_sims_async():
        return [duplicate, duplicate]

    monkeypatch.setattr(backtest, "_load_sims_async", _fake_load_sims_async)

    with pytest.raises(ValueError, match="Duplicate instruments"):
        asyncio.run(backtest.run_async())


def test_load_sims_async_parallelizes_replays_and_preserves_order(monkeypatch) -> None:
    adapter = _ConcurrentReplayAdapter()
    monkeypatch.setattr(
        "prediction_market_extensions.backtesting._prediction_market_backtest.resolve_replay_adapter",
        lambda **kwargs: adapter,
    )
    monkeypatch.setenv("BACKTEST_REPLAY_LOAD_WORKERS", "2")
    backtest = PredictionMarketBacktest(
        name="parallel-demo",
        data=MarketDataConfig(platform="demo", data_type="book", vendor="fake"),
        replays=(
            _FakeReplay("first"),
            _FakeReplay("skip"),
            _FakeReplay("second"),
            _FakeReplay("third"),
        ),
        strategy_factory=lambda instrument_id: instrument_id,
        initial_cash=100.0,
        probability_window=5,
    )

    loaded = asyncio.run(backtest._load_sims_async())

    assert adapter.max_active == 2
    assert [sim.market_id for sim in loaded] == ["first", "second", "third"]


def test_load_sims_async_can_force_sequential_loading(monkeypatch) -> None:
    adapter = _ConcurrentReplayAdapter()
    monkeypatch.setattr(
        "prediction_market_extensions.backtesting._prediction_market_backtest.resolve_replay_adapter",
        lambda **kwargs: adapter,
    )
    monkeypatch.setenv("BACKTEST_REPLAY_LOAD_WORKERS", "1")
    backtest = PredictionMarketBacktest(
        name="parallel-demo",
        data=MarketDataConfig(platform="demo", data_type="book", vendor="fake"),
        replays=(
            _FakeReplay("first"),
            _FakeReplay("second"),
            _FakeReplay("third"),
        ),
        strategy_factory=lambda instrument_id: instrument_id,
        initial_cash=100.0,
        probability_window=5,
    )

    loaded = asyncio.run(backtest._load_sims_async())

    assert adapter.max_active == 1
    assert [sim.market_id for sim in loaded] == ["first", "second", "third"]


def test_load_sims_async_uses_adapter_batch_loader(monkeypatch) -> None:
    adapter = _BatchReplayAdapter()
    monkeypatch.setattr(
        "prediction_market_extensions.backtesting._prediction_market_backtest.resolve_replay_adapter",
        lambda **kwargs: adapter,
    )
    monkeypatch.setenv("BACKTEST_REPLAY_LOAD_WORKERS", "3")
    replays = (_FakeReplay("first"), _FakeReplay("second"))
    backtest = PredictionMarketBacktest(
        name="batch-demo",
        data=MarketDataConfig(platform="demo", data_type="book", vendor="fake"),
        replays=replays,
        strategy_factory=lambda instrument_id: instrument_id,
        initial_cash=100.0,
        probability_window=5,
    )

    loaded = asyncio.run(backtest._load_sims_async())

    assert adapter.batch_workers == 2
    assert adapter.batch_replays == replays
    assert [sim.market_id for sim in loaded] == ["first", "second"]


def test_replay_load_worker_cap_allows_100_market_source_fanout(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_REPLAY_LOAD_WORKERS", "128")

    assert _resolve_replay_load_workers(100) == 100
    assert _resolve_replay_load_workers(256) == 128


def test_parallel_loading_temporarily_suppresses_loader_progress(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)

    with _loader_progress_env_for_workers(2):
        assert os.environ["BACKTEST_LOADER_PROGRESS"] == "0"

    assert "BACKTEST_LOADER_PROGRESS" not in os.environ


def test_explicit_loader_progress_setting_is_preserved(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_LOADER_PROGRESS", "1")

    with _loader_progress_env_for_workers(2):
        assert os.environ["BACKTEST_LOADER_PROGRESS"] == "1"

    assert os.environ["BACKTEST_LOADER_PROGRESS"] == "1"
