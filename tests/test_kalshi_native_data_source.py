from __future__ import annotations

import os

from backtests._shared.data_sources.kalshi_native import KALSHI_REST_BASE_URL_ENV
from backtests._shared.data_sources.kalshi_native import (
    configured_kalshi_native_data_source,
)


def test_configured_kalshi_native_data_source_maps_explicit_endpoint() -> None:
    with configured_kalshi_native_data_source(
        sources=["api.elections.kalshi.com/trade-api/v2"]
    ) as selection:
        assert "rest=https://api.elections.kalshi.com/trade-api/v2" in selection.summary
        assert (
            os.environ[KALSHI_REST_BASE_URL_ENV]
            == "https://api.elections.kalshi.com/trade-api/v2"
        )

    assert os.getenv(KALSHI_REST_BASE_URL_ENV) is None
