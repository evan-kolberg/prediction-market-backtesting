from __future__ import annotations

import asyncio
import os

from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    POLYMARKET_CLOB_BASE_URL_ENV,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    POLYMARKET_GAMMA_BASE_URL_ENV,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    POLYMARKET_TRADE_API_BASE_URL_ENV,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    RunnerPolymarketDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    configured_polymarket_native_data_source,
)


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
            RunnerPolymarketDataLoader._configured_clob_base_url()
            == "https://clob.polymarket.com"
        )

    assert os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_CLOB_BASE_URL_ENV) is None


def test_configured_polymarket_native_data_source_isolates_concurrent_loader_config() -> (
    None
):
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


def test_configured_polymarket_native_data_source_keeps_legacy_equals_prefixes() -> (
    None
):
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
            RunnerPolymarketDataLoader._configured_clob_base_url()
            == "https://clob.polymarket.com"
        )
