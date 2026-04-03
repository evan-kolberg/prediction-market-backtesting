from __future__ import annotations

import os

from backtests._shared.data_sources.polymarket_native import (
    POLYMARKET_CLOB_BASE_URL_ENV,
)
from backtests._shared.data_sources.polymarket_native import (
    POLYMARKET_GAMMA_BASE_URL_ENV,
)
from backtests._shared.data_sources.polymarket_native import (
    POLYMARKET_TRADE_API_BASE_URL_ENV,
)
from backtests._shared.data_sources.polymarket_native import (
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
        assert "gamma=https://gamma-api.polymarket.com" in selection.summary
        assert "trades=https://data-api.polymarket.com" in selection.summary
        assert "clob=https://clob.polymarket.com" in selection.summary
        assert (
            os.environ[POLYMARKET_GAMMA_BASE_URL_ENV]
            == "https://gamma-api.polymarket.com"
        )
        assert (
            os.environ[POLYMARKET_TRADE_API_BASE_URL_ENV]
            == "https://data-api.polymarket.com"
        )
        assert os.environ[POLYMARKET_CLOB_BASE_URL_ENV] == "https://clob.polymarket.com"

    assert os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV) is None
    assert os.getenv(POLYMARKET_CLOB_BASE_URL_ENV) is None
