"""Smoke test for the PMXT-backed Polymarket L2 loader."""

import asyncio
import os

import pandas as pd
import pytest


EXPECTED_MARKET_SLUG = "will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026"


@pytest.mark.skipif(
    os.getenv("RUN_PMXT_INTEGRATION") != "1",
    reason="Set RUN_PMXT_INTEGRATION=1 to exercise the live PMXT archive",
)
def test_pmxt_loader_returns_quotes_and_book_deltas():
    from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader
    from nautilus_trader.model.data import OrderBookDeltas
    from nautilus_trader.model.data import QuoteTick

    async def _load():
        loader = await PolymarketPMXTDataLoader.from_market_slug(EXPECTED_MARKET_SLUG)
        end = pd.Timestamp.now(tz="UTC").floor("h") - pd.Timedelta(hours=3)
        start = end - pd.Timedelta(hours=2)
        return loader.load_order_book_and_quotes(start, end)

    data = asyncio.run(_load())

    assert any(isinstance(record, QuoteTick) for record in data)
    assert any(isinstance(record, OrderBookDeltas) for record in data)
