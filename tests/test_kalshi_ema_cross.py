"""End-to-end tests for the Kalshi EMA-cross strategy.

These tests hit the real Kalshi REST API and run a full backtest, so they
require network access and take a few seconds to complete.
"""

import asyncio

import pytest

import strategies.kalshi_ema_cross as strat


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Redirect all file output to a temp dir and keep the date range
    wide enough to exercise the >5 000-candle chunking code path."""
    monkeypatch.setattr(strat, "CATALOG_PATH", str(tmp_path / "catalog"))
    monkeypatch.chdir(tmp_path)  # tearsheet → tmp_path/output/


def test_fetch_writes_catalog(tmp_path):
    """Bars are fetched in chunks and written to the Parquet catalog."""
    asyncio.run(strat._fetch_and_catalog())

    catalog_dir = tmp_path / "catalog"
    assert catalog_dir.exists(), "Catalog directory not created"
    parquet_files = list(catalog_dir.rglob("*.parquet"))
    assert parquet_files, "No Parquet files written to catalog"


def test_full_run_produces_tearsheet(tmp_path):
    """Full pipeline (fetch → catalog → backtest → tearsheet) runs without error."""
    asyncio.run(strat.run())

    tearsheet = tmp_path / "output" / "kalshi_ema_cross_tearsheet.html"
    assert tearsheet.exists(), "Tearsheet not created"
    assert tearsheet.stat().st_size > 0, "Tearsheet file is empty"
