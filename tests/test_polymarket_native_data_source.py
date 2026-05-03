from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import msgspec
import pytest
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument

from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
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
    assert "[INFO] loaders._fetch_market_details: Fetching Polymarket CLOB market details" in output
    assert "[INFO] loaders._fetch_market_details: Loaded Polymarket CLOB market details" in output


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
