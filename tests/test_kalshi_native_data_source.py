from __future__ import annotations

import asyncio
import os

from prediction_market_extensions.backtesting.data_sources.kalshi_native import KALSHI_REST_BASE_URL_ENV
from prediction_market_extensions.backtesting.data_sources.kalshi_native import RunnerKalshiDataLoader
from prediction_market_extensions.backtesting.data_sources.kalshi_native import configured_kalshi_native_data_source


def test_configured_kalshi_native_data_source_maps_explicit_endpoint() -> None:
    with configured_kalshi_native_data_source(sources=["rest:api.elections.kalshi.com/trade-api/v2"]) as selection:
        assert "rest:https://api.elections.kalshi.com/trade-api/v2" in selection.summary
        assert RunnerKalshiDataLoader._configured_rest_base_url() == "https://api.elections.kalshi.com/trade-api/v2"

    assert os.getenv(KALSHI_REST_BASE_URL_ENV) is None


def test_configured_kalshi_native_data_source_isolates_concurrent_loader_config() -> None:
    async def _capture(url: str) -> str:
        with configured_kalshi_native_data_source(sources=[url]):
            await asyncio.sleep(0)
            return RunnerKalshiDataLoader._configured_rest_base_url()

    async def _run() -> tuple[str, str]:
        return await asyncio.gather(
            _capture("api-a.kalshi.test/trade-api/v2"), _capture("api-b.kalshi.test/trade-api/v2")
        )

    first, second = asyncio.run(_run())

    assert first == "https://api-a.kalshi.test/trade-api/v2"
    assert second == "https://api-b.kalshi.test/trade-api/v2"
    assert os.getenv(KALSHI_REST_BASE_URL_ENV) is None


def test_configured_kalshi_native_data_source_keeps_legacy_bare_url_support() -> None:
    with configured_kalshi_native_data_source(sources=["api.elections.kalshi.com/trade-api/v2"]):
        assert RunnerKalshiDataLoader._configured_rest_base_url() == "https://api.elections.kalshi.com/trade-api/v2"
