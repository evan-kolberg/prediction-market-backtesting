from __future__ import annotations

import asyncio
import warnings
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
from prediction_market_extensions._runtime_log import capture_loader_events
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
from prediction_market_extensions.backtesting._replay_specs import BookReplay
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


def test_trade_days_for_window_uses_shared_native_window_planner() -> None:
    assert replay_adapters._trade_days_for_window(
        pd.Timestamp("2026-04-21T09:15:00Z"),
        pd.Timestamp("2026-04-23T00:00:00Z"),
    ) == (
        pd.Timestamp("2026-04-21T00:00:00Z"),
        pd.Timestamp("2026-04-22T00:00:00Z"),
        pd.Timestamp("2026-04-23T00:00:00Z"),
    )


def test_merge_records_uses_native_merge_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    books = (
        SimpleNamespace(name="book-late", ts_event=10, ts_init=30),
        SimpleNamespace(name="book-early", ts_event=5, ts_init=5),
    )
    trades = (SimpleNamespace(name="trade", ts_event=10, ts_init=1),)
    calls: list[dict[str, object]] = []

    def fake_merge_plan(**kwargs: object) -> list[tuple[int, int]]:
        calls.append(kwargs)
        return [(0, 1), (0, 0), (1, 0)]

    monkeypatch.setattr(replay_adapters, "replay_merge_plan", fake_merge_plan)

    merged = replay_adapters._merge_records(  # type: ignore[arg-type]
        book_records=books,
        trade_records=trades,
    )

    assert calls == [
        {
            "book_ts_events": [10, 5],
            "book_ts_inits": [30, 5],
            "trade_ts_events": [10],
            "trade_ts_inits": [1],
        }
    ]
    assert [record.name for record in merged] == ["book-early", "book-late", "trade"]


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


def test_preflight_skips_midpoints_when_price_range_filter_disabled(monkeypatch) -> None:
    class FakeDeltas:
        pass

    class ExplodingOrderBook:
        def __init__(self, instrument_id, book_type):  # type: ignore[no-untyped-def]
            del instrument_id, book_type
            raise AssertionError("midpoints should not be computed")

    monkeypatch.setattr(replay_adapters, "OrderBook", ExplodingOrderBook)

    count, midpoints = replay_adapters._book_event_count_and_prices_for_request(
        instrument=SimpleNamespace(id="POLYMARKET.TEST"),
        records=(FakeDeltas(), object(), FakeDeltas()),
        deltas_type=FakeDeltas,
        request=ReplayLoadRequest(min_price_range=0.0),
    )

    assert count == 2
    assert midpoints == ()


@pytest.mark.parametrize(
    ("adapter", "loader_symbol"),
    [
        (replay_adapters.PolymarketPMXTBookReplayAdapter(), "PolymarketPMXTDataLoader"),
        (
            replay_adapters.PolymarketTelonexBookReplayAdapter(),
            "PolymarketTelonexBookDataLoader",
        ),
    ],
)
def test_book_batch_loader_stages_metadata_then_books_then_trades(
    monkeypatch: pytest.MonkeyPatch,
    adapter,
    loader_symbol: str,
) -> None:
    events: list[str] = []

    class FakeLoader:
        def __init__(self, slug: str) -> None:
            self.slug = slug
            self.instrument = SimpleNamespace(id=f"POLYMARKET.{slug}", outcome="Yes")

        @classmethod
        async def from_market_slug(cls, market_slug: str, *, token_index: int = 0):
            events.append(f"metadata:{market_slug}:{token_index}")
            return cls(market_slug)

        def load_order_book_deltas(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            del args, kwargs
            events.append(f"book:{self.slug}")
            return (SimpleNamespace(kind="book", slug=self.slug, ts_event=1, ts_init=1),)

    original_resolver = replay_adapters._resolve_backtest_compat_symbol

    def fake_resolver(name: str, default):  # type: ignore[no-untyped-def]
        if name == loader_symbol:
            return FakeLoader
        return original_resolver(name, default)

    async def fake_load_trade_ticks(loader, *, start, end, market_label):  # type: ignore[no-untyped-def]
        del start, end, market_label
        events.append(f"trades:{loader.slug}")
        return (SimpleNamespace(kind="trade", slug=loader.slug, ts_event=2, ts_init=2),)

    def fake_merge_records(*, book_records, trade_records):  # type: ignore[no-untyped-def]
        events.append(f"merge:{book_records[0].slug}")
        return (*book_records, *trade_records)

    def fake_build_loaded_replay(
        self,
        *,
        prepared,
        records,
        book_event_count=None,
        request,
        vendor,
        source_label,
    ):  # type: ignore[no-untyped-def]
        del self, records, book_event_count, request, vendor, source_label
        slug = prepared.resolved.replay.market_slug
        events.append(f"build:{slug}")
        return SimpleNamespace(market_id=slug)

    monkeypatch.setattr(replay_adapters, "_resolve_backtest_compat_symbol", fake_resolver)
    monkeypatch.setattr(replay_adapters, "_load_trade_ticks", fake_load_trade_ticks)
    monkeypatch.setattr(replay_adapters, "_merge_records", fake_merge_records)
    monkeypatch.setattr(
        replay_adapters._BaseReplayAdapter,
        "_build_loaded_book_replay_or_none",
        fake_build_loaded_replay,
    )

    loaded = asyncio.run(
        adapter.load_replays(
            (
                BookReplay(
                    market_slug="first",
                    token_index=0,
                    start_time="2026-04-21T00:00:00Z",
                    end_time="2026-04-21T01:00:00Z",
                ),
                BookReplay(
                    market_slug="second",
                    token_index=1,
                    start_time="2026-04-21T00:00:00Z",
                    end_time="2026-04-21T01:00:00Z",
                ),
            ),
            request=ReplayLoadRequest(),
            workers=2,
        )
    )

    last_metadata = max(
        index for index, event in enumerate(events) if event.startswith("metadata:")
    )
    first_book = min(index for index, event in enumerate(events) if event.startswith("book:"))
    last_book = max(index for index, event in enumerate(events) if event.startswith("book:"))
    first_trade = min(index for index, event in enumerate(events) if event.startswith("trades:"))

    assert last_metadata < first_book
    assert last_book < first_trade
    assert [item.market_id for item in loaded] == ["first", "second"]


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
    output = capsys.readouterr().err

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
    cached_output = capsys.readouterr().err

    assert cached_trades == ()
    assert loader.calls == 1
    assert "polymarket cache 2026-01-19.parquet" in cached_output


def test_trade_tick_loader_fails_when_polymarket_offset_ceiling_is_final_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    class FakeCeilingLoader:
        condition_id = "0xcondition"
        token_id = "token"
        instrument = SimpleNamespace()

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            del start, end
            warnings.warn(
                "Polymarket public trades API hit its historical offset ceiling. "
                "Returning the trades fetched before the ceiling.",
                RuntimeWarning,
                stacklevel=2,
            )
            return []

    loader = FakeCeilingLoader()
    monkeypatch.setattr(replay_adapters, "_cache_home", lambda: tmp_path)
    cache_path = (
        tmp_path
        / "nautilus_trader"
        / "polymarket_trades"
        / loader.condition_id
        / loader.token_id
        / "2026-01-19.parquet"
    )

    with pytest.raises(RuntimeError, match="historical offset ceiling"):
        asyncio.run(
            replay_adapters._load_trade_ticks(
                loader,
                start=pd.Timestamp("2026-01-19T00:00:00Z"),
                end=pd.Timestamp("2026-01-19T23:59:59Z"),
                market_label="demo-market",
            )
        )

    assert not cache_path.exists()


def test_trade_tick_loader_falls_through_when_telonex_onchain_fills_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys
) -> None:
    class FakeTelonexLoader:
        condition_id = "0xcondition"
        token_id = "token"
        instrument = SimpleNamespace()

        def __init__(self) -> None:
            self.telonex_calls = 0
            self.polymarket_calls = 0
            self._telonex_last_trade_source = "telonex-local::/tmp/telonex"

        def load_telonex_onchain_fill_ticks(self, start, end):  # type: ignore[no-untyped-def]
            del start, end
            self.telonex_calls += 1
            return ()

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            del start, end
            self.polymarket_calls += 1
            return []

    loader = FakeTelonexLoader()
    monkeypatch.setattr(replay_adapters, "_cache_home", lambda: tmp_path)
    cache_path = (
        tmp_path
        / "nautilus_trader"
        / "polymarket_trades"
        / loader.condition_id
        / loader.token_id
        / "2026-01-19.parquet"
    )
    cache_path.parent.mkdir(parents=True)
    pd.DataFrame(
        columns=["price", "size", "aggressor_side", "trade_id", "ts_event", "ts_init"]
    ).to_parquet(cache_path, index=False)

    trades = asyncio.run(
        replay_adapters._load_trade_ticks(
            loader,
            start=pd.Timestamp("2026-01-19T00:00:00Z"),
            end=pd.Timestamp("2026-01-19T23:59:59Z"),
            market_label="demo-market",
        )
    )
    output = capsys.readouterr().err

    assert trades == ()
    assert loader.telonex_calls == 1
    assert loader.polymarket_calls == 0
    assert "polymarket cache 2026-01-19.parquet" in output
    assert "telonex local" not in output
    assert "telonex-local::/tmp/telonex" not in output


def test_trade_progress_labels_telonex_materialized_trade_cache(tmp_path, capsys) -> None:
    cache_path = tmp_path / "onchain_fills" / "2026-01-19.1-2.parquet"

    replay_adapters._print_trade_progress_line(
        day=pd.Timestamp("2026-01-19T00:00:00Z"),
        elapsed_secs=0.123,
        rows=4,
        source=f"telonex-trade-cache::{cache_path}",
    )
    output = capsys.readouterr().err

    assert "telonex onchain_fills cache 2026-01-19.1-2.parquet" in output


def test_trade_cache_write_failure_emits_error(tmp_path) -> None:
    bad_cache_root = tmp_path / "not-a-directory"
    bad_cache_root.write_text("occupied", encoding="utf-8")

    with capture_loader_events() as capture:
        replay_adapters._write_trade_cache(
            path=bad_cache_root / "trade.parquet",
            trades=(),
            market_label="demo-market",
            day=pd.Timestamp("2026-04-21T00:00:00Z"),
        )

    event = next(event for event in capture.events if event.stage == "cache_write")
    assert event.level == "ERROR"
    assert event.status == "error"
    assert event.vendor == "polymarket"
    assert event.source_kind == "cache"
    assert event.market_slug == "demo-market"
    assert event.origin == "replay_adapters._write_trade_cache"


def test_trade_progress_labels_telonex_trade_channels(tmp_path, capsys) -> None:
    replay_adapters._print_trade_progress_line(
        day=pd.Timestamp("2026-01-19T00:00:00Z"),
        elapsed_secs=0.123,
        rows=4,
        source=f"telonex-local::{tmp_path}",
    )
    replay_adapters._print_trade_progress_line(
        day=pd.Timestamp("2026-01-19T00:00:00Z"),
        elapsed_secs=0.123,
        rows=4,
        source=f"telonex-local-trades::{tmp_path}",
    )
    replay_adapters._print_trade_progress_line(
        day=pd.Timestamp("2026-01-20T00:00:00Z"),
        elapsed_secs=0.123,
        rows=5,
        source=(
            "telonex-api::https://api.telonex.io/v1/downloads/polymarket/onchain_fills/2026-01-20"
        ),
    )
    replay_adapters._print_trade_progress_line(
        day=pd.Timestamp("2026-01-20T00:00:00Z"),
        elapsed_secs=0.123,
        rows=5,
        source="telonex-api::https://api.telonex.io/v1/downloads/polymarket/trades/2026-01-20",
    )
    output = capsys.readouterr().err

    assert "telonex local onchain_fills" in output
    assert "telonex local trades" in output
    assert "telonex api onchain_fills" in output
    assert "telonex api trades" in output
