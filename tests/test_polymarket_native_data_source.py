from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import msgspec
import pytest
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument

from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from prediction_market_extensions._runtime_log import capture_loader_events
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    POLYMARKET_CLOB_BASE_URL_ENV,
    POLYMARKET_GAMMA_BASE_URL_ENV,
    POLYMARKET_TRADE_API_BASE_URL_ENV,
    RunnerPolymarketDataLoader,
    configured_polymarket_native_data_source,
)


class _FakeHttpClient:
    def __init__(self, payload: object, *, status: int = 200) -> None:
        self.payload = payload
        self.status = status
        self.requests: list[tuple[str, dict[str, object] | None]] = []

    async def get(self, *, url: str, params: dict[str, object] | None = None):  # type: ignore[no-untyped-def]
        self.requests.append((url, params))
        return SimpleNamespace(status=self.status, body=msgspec.json.encode(self.payload))


class _RoutingHttpClient:
    def __init__(self, routes: dict[tuple[str, tuple[tuple[str, object], ...] | None], object]):
        self.routes = routes
        self.requests: list[tuple[str, dict[str, object] | None]] = []

    async def get(self, *, url: str, params: dict[str, object] | None = None):  # type: ignore[no-untyped-def]
        self.requests.append((url, params))
        params_key = tuple(sorted(params.items())) if params else None
        payload = self.routes[(url, params_key)]
        return SimpleNamespace(status=200, body=msgspec.json.encode(payload))


def _metadata_market_payload(
    *, slug: str = "demo-market", condition_id: str = "0x" + "2" * 64
) -> dict[str, object]:
    return {
        "slug": slug,
        "conditionId": condition_id,
        "question": "Synthetic cached market?",
        "description": "Synthetic metadata cache fixture",
        "closed": True,
        "closedTime": "2026-04-01T00:00:00Z",
        "umaResolutionStatus": "resolved",
        "result": "Yes",
    }


def _metadata_details_payload(*, condition_id: str = "0x" + "2" * 64) -> dict[str, object]:
    return {
        "condition_id": condition_id,
        "question": "Synthetic cached market?",
        "minimum_tick_size": "0.01",
        "minimum_order_size": "1",
        "end_date_iso": "2026-12-31T00:00:00Z",
        "maker_base_fee": "0",
        "taker_base_fee": "0",
        "result": "Yes",
        "tokens": [
            {"token_id": "asset9876", "outcome": "Yes", "winner": True},
            {"token_id": "asset5432", "outcome": "No", "winner": False},
        ],
    }


def _make_polymarket_loader() -> PolymarketDataLoader:
    instrument = parse_polymarket_instrument(
        market_info={
            "condition_id": "0x" + "1" * 64,
            "question": "Synthetic Polymarket market",
            "minimum_tick_size": "0.01",
            "minimum_order_size": "1",
            "end_date_iso": "2026-12-31T00:00:00Z",
            "maker_base_fee": "0",
            "taker_base_fee": "0",
        },
        token_id="asset9876",
        outcome="Yes",
        ts_init=0,
    )
    loader = object.__new__(PolymarketDataLoader)
    loader._instrument = instrument
    loader._token_id = "asset9876"
    loader._condition_id = "0x" + "1" * 64
    return loader


def test_configured_polymarket_native_data_source_maps_explicit_endpoints() -> None:
    with configured_polymarket_native_data_source(
        sources=[
            "gamma-api.polymarket.com",
            "data-api.polymarket.com/trades",
            "clob.polymarket.com",
        ]
    ) as selection:
        assert "gamma:https://gamma-api.polymarket.com" in selection.summary
        assert "trades:https://data-api.polymarket.com" in selection.summary
        assert "clob:https://clob.polymarket.com" in selection.summary
        assert (
            RunnerPolymarketDataLoader._configured_gamma_base_url()
            == "https://gamma-api.polymarket.com"
        )
        assert (
            RunnerPolymarketDataLoader._configured_trade_api_base_url()
            == "https://data-api.polymarket.com"
        )
        assert (
            RunnerPolymarketDataLoader._configured_clob_base_url() == "https://clob.polymarket.com"
        )

    assert os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_CLOB_BASE_URL_ENV) is None


def test_polymarket_clob_market_fetch_logs_source(capsys) -> None:
    client = _FakeHttpClient({"condition_id": "0xcondition"})

    details = asyncio.run(PolymarketDataLoader._fetch_market_details("0xcondition", client))
    output = capsys.readouterr().err

    assert details == {"condition_id": "0xcondition"}
    assert client.requests == [("https://clob.polymarket.com/markets/0xcondition", None)]
    assert (
        "[INFO] loaders._fetch_market_details: Polymarket metadata discover start "
        "api https://clob.polymarket.com/markets"
    ) in output
    assert (
        "[INFO] loaders._fetch_market_details: Polymarket metadata discover complete "
        "api https://clob.polymarket.com/markets"
    ) in output


def test_polymarket_gamma_event_fetch_logs_source(capsys) -> None:
    client = _FakeHttpClient([{"slug": "demo-event", "markets": []}])

    event = asyncio.run(PolymarketDataLoader._fetch_event_by_slug("demo-event", client))
    output = capsys.readouterr().err

    assert event == {"slug": "demo-event", "markets": []}
    assert client.requests == [("https://gamma-api.polymarket.com/events", {"slug": "demo-event"})]
    assert (
        "[INFO] loaders._fetch_event_by_slug: Polymarket metadata discover start "
        "api https://gamma-api.polymarket.com/events"
    ) in output
    assert (
        "[INFO] loaders._fetch_event_by_slug: Polymarket metadata discover complete "
        "(1 rows) api https://gamma-api.polymarket.com/events"
    ) in output


def test_polymarket_gamma_list_fetches_log_pages(capsys) -> None:
    events_client = _FakeHttpClient([{"slug": "event-a"}])
    events_loader = object.__new__(PolymarketDataLoader)
    events_loader._http_client = events_client

    events = asyncio.run(events_loader.fetch_events(limit=1, offset=2))
    events_output = capsys.readouterr().err

    assert events == [{"slug": "event-a"}]
    assert events_client.requests == [
        (
            "https://gamma-api.polymarket.com/events",
            {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": "1",
                "offset": "2",
            },
        )
    ]
    assert (
        "[INFO] loaders.fetch_events: Polymarket metadata discover start "
        "api https://gamma-api.polymarket.com/events"
    ) in events_output
    assert (
        "[INFO] loaders.fetch_events: Polymarket metadata discover complete "
        "(1 rows) api https://gamma-api.polymarket.com/events"
    ) in events_output

    markets_client = _FakeHttpClient([{"slug": "market-a"}])
    markets_loader = object.__new__(PolymarketDataLoader)
    markets_loader._http_client = markets_client

    markets = asyncio.run(markets_loader.fetch_markets(limit=3, offset=4))
    markets_output = capsys.readouterr().err

    assert markets == [{"slug": "market-a"}]
    assert markets_client.requests == [
        (
            "https://gamma-api.polymarket.com/markets",
            {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": "3",
                "offset": "4",
            },
        )
    ]
    assert (
        "[INFO] loaders.fetch_markets: Polymarket metadata discover start "
        "api https://gamma-api.polymarket.com/markets"
    ) in markets_output
    assert (
        "[INFO] loaders.fetch_markets: Polymarket metadata discover complete "
        "(1 rows) api https://gamma-api.polymarket.com/markets"
    ) in markets_output


def test_from_market_slug_uses_run_metadata_cache_and_sanitizes_resolution(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv(PolymarketDataLoader._METADATA_CACHE_DIR_ENV, str(tmp_path))
    PolymarketDataLoader.clear_metadata_cache()
    condition_id = "0x" + "2" * 64
    client = _RoutingHttpClient(
        {
            (
                "https://gamma-api.polymarket.com/markets/slug/demo-market",
                None,
            ): _metadata_market_payload(condition_id=condition_id),
            (
                f"https://clob.polymarket.com/markets/{condition_id}",
                None,
            ): _metadata_details_payload(condition_id=condition_id),
            (
                "https://clob.polymarket.com/fee-rate",
                (("token_id", "asset9876"),),
            ): {"fee_rate_bps": "30"},
        }
    )

    async def _load_twice():
        first = await PolymarketDataLoader.from_market_slug("demo-market", http_client=client)
        second = await PolymarketDataLoader.from_market_slug("demo-market", http_client=client)
        return first, second

    first, second = asyncio.run(_load_twice())

    assert client.requests == [
        ("https://gamma-api.polymarket.com/markets/slug/demo-market", None),
        (f"https://clob.polymarket.com/markets/{condition_id}", None),
        ("https://clob.polymarket.com/fee-rate", {"token_id": "asset9876"}),
    ]
    assert first.instrument.id == second.instrument.id
    assert "result" not in second.instrument.info
    assert "winner" not in second.instrument.info["tokens"][0]
    assert second.resolution_metadata["result"] == "Yes"
    assert second.resolution_metadata["tokens"][0]["winner"] is True
    PolymarketDataLoader.clear_metadata_cache()


def test_from_market_slug_reuses_disk_metadata_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv(PolymarketDataLoader._METADATA_CACHE_DIR_ENV, str(tmp_path))
    PolymarketDataLoader.clear_metadata_cache()
    condition_id = "0x" + "4" * 64
    client = _RoutingHttpClient(
        {
            (
                "https://gamma-api.polymarket.com/markets/slug/demo-market",
                None,
            ): _metadata_market_payload(condition_id=condition_id),
            (
                f"https://clob.polymarket.com/markets/{condition_id}",
                None,
            ): _metadata_details_payload(condition_id=condition_id),
            (
                "https://clob.polymarket.com/fee-rate",
                (("token_id", "asset9876"),),
            ): {"fee_rate_bps": "30"},
        }
    )

    async def _load_once():
        return await PolymarketDataLoader.from_market_slug("demo-market", http_client=client)

    with capture_loader_events() as capture:
        first = asyncio.run(_load_once())
    assert len(client.requests) == 3
    cache_write_events = [
        event
        for event in capture.events
        if event.stage == "cache_write" and event.status == "complete"
    ]
    assert [event.attrs["kind"] for event in cache_write_events] == [
        "gamma-market",
        "clob-market",
        "fee-rate",
    ]
    assert all(event.origin == "loaders._write_metadata_disk_cache" for event in cache_write_events)
    assert all(event.source_kind == "cache" for event in cache_write_events)
    assert all(event.cache_path for event in cache_write_events)

    PolymarketDataLoader.clear_metadata_cache()
    client.requests.clear()
    with capture_loader_events() as second_capture:
        second = asyncio.run(_load_once())

    assert client.requests == []
    cache_read_events = [
        event
        for event in second_capture.events
        if event.stage == "cache_read" and event.status == "cache_hit"
    ]
    assert [event.attrs["kind"] for event in cache_read_events] == [
        "gamma-market",
        "clob-market",
        "fee-rate",
    ]
    assert all(event.origin == "loaders._read_metadata_disk_cache" for event in cache_read_events)
    assert first.instrument.id == second.instrument.id
    assert "result" not in second.instrument.info
    PolymarketDataLoader.clear_metadata_cache()


def test_metadata_disk_write_failure_emits_error(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    bad_cache_root = tmp_path / "not-a-directory"
    bad_cache_root.write_text("occupied", encoding="utf-8")
    monkeypatch.setenv(PolymarketDataLoader._METADATA_CACHE_DIR_ENV, str(bad_cache_root))

    with capture_loader_events() as capture:
        PolymarketDataLoader._write_metadata_disk_cache(
            "gamma-market",
            "https://gamma-api.polymarket.com",
            "demo-market",
            {"closed": True},
        )

    error_events = [
        event
        for event in capture.events
        if event.stage == "cache_write" and event.status == "error"
    ]
    assert len(error_events) == 1
    assert error_events[0].level == "ERROR"
    assert error_events[0].attrs["kind"] == "gamma-market"
    assert error_events[0].market_slug == "demo-market"


def test_metadata_cache_separates_native_source_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv(PolymarketDataLoader._METADATA_CACHE_DIR_ENV, str(tmp_path))
    RunnerPolymarketDataLoader.clear_metadata_cache()
    condition_id = "0x" + "3" * 64
    client = _RoutingHttpClient(
        {
            (
                "https://gamma-one.example/markets/slug/demo-market",
                None,
            ): _metadata_market_payload(condition_id=condition_id),
            (
                f"https://clob-one.example/markets/{condition_id}",
                None,
            ): _metadata_details_payload(condition_id=condition_id),
            (
                "https://clob-one.example/fee-rate",
                (("token_id", "asset9876"),),
            ): {"fee_rate_bps": "30"},
            (
                "https://gamma-two.example/markets/slug/demo-market",
                None,
            ): _metadata_market_payload(condition_id=condition_id),
            (
                f"https://clob-two.example/markets/{condition_id}",
                None,
            ): _metadata_details_payload(condition_id=condition_id),
            (
                "https://clob-two.example/fee-rate",
                (("token_id", "asset9876"),),
            ): {"fee_rate_bps": "40"},
        }
    )

    async def _load_with_distinct_configs():
        with configured_polymarket_native_data_source(
            sources=[
                "gamma:https://gamma-one.example",
                "clob:https://clob-one.example",
                "trades:https://trades-one.example",
            ]
        ):
            await RunnerPolymarketDataLoader.from_market_slug("demo-market", http_client=client)
        with configured_polymarket_native_data_source(
            sources=[
                "gamma:https://gamma-two.example",
                "clob:https://clob-two.example",
                "trades:https://trades-two.example",
            ]
        ):
            await RunnerPolymarketDataLoader.from_market_slug("demo-market", http_client=client)

    asyncio.run(_load_with_distinct_configs())

    assert client.requests == [
        ("https://gamma-one.example/markets/slug/demo-market", None),
        (f"https://clob-one.example/markets/{condition_id}", None),
        ("https://clob-one.example/fee-rate", {"token_id": "asset9876"}),
        ("https://gamma-two.example/markets/slug/demo-market", None),
        (f"https://clob-two.example/markets/{condition_id}", None),
        ("https://clob-two.example/fee-rate", {"token_id": "asset9876"}),
    ]
    RunnerPolymarketDataLoader.clear_metadata_cache()


def test_polymarket_public_trade_fetch_logs_pages(capsys) -> None:
    client = _FakeHttpClient([])
    loader = object.__new__(PolymarketDataLoader)
    loader._http_client = client

    trades = asyncio.run(loader.fetch_trades("0xcondition"))
    output = capsys.readouterr().err

    assert trades == []
    assert client.requests == [
        (
            "https://data-api.polymarket.com/trades",
            {"market": "0xcondition", "limit": 1000, "offset": 0},
        )
    ]
    assert "[INFO] loaders.fetch_trades: Fetching Polymarket public trades page" in output
    assert "[INFO] loaders.fetch_trades: Loaded Polymarket public trades page" in output
    assert "rows=0" in output


def test_polymarket_public_trades_parse_with_native_batch_warnings() -> None:
    loader = _make_polymarket_loader()

    with pytest.warns(RuntimeWarning) as caught:
        trades = loader.parse_trades(
            [
                {
                    "timestamp": 1_771_767_624,
                    "transactionHash": "0xaaaa",
                    "asset": "other-token",
                    "side": "BUY",
                    "price": "0.50",
                    "size": "1",
                },
                {
                    "timestamp": 1_771_767_624,
                    "transactionHash": "0xbbbb",
                    "asset": "asset9876",
                    "side": "mint",
                    "price": "0.42",
                    "size": "2",
                },
                {
                    "timestamp": 1_771_767_624,
                    "transactionHash": "0xcccc",
                    "asset": "asset9876",
                    "side": "SELL",
                    "price": "1.0",
                    "size": "3",
                },
                {
                    "timestamp": 1_771_767_624,
                    "transactionHash": "0xdddd",
                    "asset": "asset9876",
                    "side": "BUY",
                    "price": "0.41",
                    "size": "4",
                },
            ]
        )

    assert [str(warning.message) for warning in caught] == [
        "Polymarket trade 1 had unexpected side 'mint'; "
        "recording NO_AGGRESSOR for audit visibility.",
        "Skipping Polymarket trade with out-of-range or untradable price 1.0 at record 2.",
    ]
    assert [float(trade.price) for trade in trades] == [0.42, 0.41]
    assert [float(trade.size) for trade in trades] == [2.0, 4.0]
    assert [
        getattr(trade.aggressor_side, "name", str(trade.aggressor_side)) for trade in trades
    ] == [
        "NO_AGGRESSOR",
        "BUYER",
    ]
    assert [int(trade.ts_event) for trade in trades] == [
        1_771_767_624_000_000_000,
        1_771_767_624_000_000_002,
    ]
    assert [str(trade.trade_id) for trade in trades] == [
        "0xbbbb-9876-000000",
        "0xdddd-9876-000000",
    ]


def test_configured_polymarket_native_data_source_isolates_concurrent_loader_config() -> None:
    async def _capture(prefix: str) -> tuple[str, str, str]:
        with configured_polymarket_native_data_source(
            sources=[
                f"gamma:{prefix}.gamma-api.polymarket.com",
                f"trades:{prefix}.data-api.polymarket.com/trades",
                f"clob:{prefix}.clob.polymarket.com",
            ]
        ):
            await asyncio.sleep(0)
            return (
                RunnerPolymarketDataLoader._configured_gamma_base_url(),
                RunnerPolymarketDataLoader._configured_trade_api_base_url(),
                RunnerPolymarketDataLoader._configured_clob_base_url(),
            )

    async def _run() -> tuple[tuple[str, str, str], tuple[str, str, str]]:
        return await asyncio.gather(_capture("a"), _capture("b"))

    first, second = asyncio.run(_run())

    assert first == (
        "https://a.gamma-api.polymarket.com",
        "https://a.data-api.polymarket.com",
        "https://a.clob.polymarket.com",
    )
    assert second == (
        "https://b.gamma-api.polymarket.com",
        "https://b.data-api.polymarket.com",
        "https://b.clob.polymarket.com",
    )
    assert os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV) is None


def test_configured_polymarket_native_data_source_keeps_legacy_equals_prefixes() -> None:
    with configured_polymarket_native_data_source(
        sources=[
            "gamma=gamma-api.polymarket.com",
            "trades=data-api.polymarket.com/trades",
            "clob=clob.polymarket.com",
        ]
    ):
        assert (
            RunnerPolymarketDataLoader._configured_gamma_base_url()
            == "https://gamma-api.polymarket.com"
        )
        assert (
            RunnerPolymarketDataLoader._configured_trade_api_base_url()
            == "https://data-api.polymarket.com"
        )
        assert (
            RunnerPolymarketDataLoader._configured_clob_base_url() == "https://clob.polymarket.com"
        )
