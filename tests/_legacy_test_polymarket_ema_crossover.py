"""End-to-end tests for the Polymarket EMA-crossover backtest."""

import asyncio

import pytest

import backtests.polymarket_ema_crossover as strat


@pytest.fixture(autouse=True)
def _isolate_output(tmp_path, monkeypatch):
    """Redirect generated chart output to a temp directory."""
    monkeypatch.chdir(tmp_path)


def test_loader_returns_trades():
    """Polymarket loader fetches at least one trade tick for the configured market."""
    from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader

    async def _load():
        loader = await PolymarketDataLoader.from_market_slug(strat.MARKET_SLUG)
        return await loader.load_trades()

    trades = asyncio.run(_load())
    assert len(trades) > 0, f"No trades returned for market '{strat.MARKET_SLUG}'"


def test_full_run_produces_legacy_chart(tmp_path):
    """Full pipeline runs without error and writes a legacy HTML chart."""
    asyncio.run(strat.run())

    chart = tmp_path / "output" / f"{strat.NAME}_{strat.MARKET_SLUG}_legacy.html"
    assert chart.exists(), "Legacy chart not created"
    assert chart.stat().st_size > 0, "Legacy chart file is empty"
