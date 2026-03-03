"""End-to-end tests for the Polymarket EMA-cross strategy.

These tests hit the real Polymarket REST API and run a full backtest, so they
require network access and take a few seconds to complete.
"""

import asyncio

import pytest

import strategies.polymarket_ema_cross as strat


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Redirect tearsheet output to a temp dir."""
    monkeypatch.chdir(tmp_path)  # tearsheet → tmp_path/output/


def test_loader_returns_trades():
    """Polymarket loader fetches at least one trade tick for the configured market."""
    from nautilus_trader.adapters.polymarket import PolymarketDataLoader

    async def _load():
        loader = await PolymarketDataLoader.from_market_slug(strat.MARKET_SLUG)
        trades = await loader.load_trades()
        return trades

    trades = asyncio.run(_load())
    assert len(trades) > 0, f"No trades returned for market '{strat.MARKET_SLUG}'"


def test_full_run_produces_tearsheet(tmp_path):
    """Full pipeline (fetch trades → backtest → tearsheet) runs without error."""
    asyncio.run(strat.run())

    tearsheet = tmp_path / "output" / "polymarket_ema_cross_tearsheet.html"
    assert tearsheet.exists(), "Tearsheet not created"
    assert tearsheet.stat().st_size > 0, "Tearsheet file is empty"
