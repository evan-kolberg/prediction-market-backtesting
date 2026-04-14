"""End-to-end test for the retained Kalshi single-runner example."""

from pathlib import Path

import pytest

import backtests.kalshi_trade_tick_breakout as strat

REPO_ROOT = Path(__file__).resolve().parents[1]


def _chart_path() -> Path:
    return (
        REPO_ROOT
        / "output"
        / f"kalshi_trade_tick_breakout_{strat.REPLAYS[0].market_ticker}_legacy.html"
    )


@pytest.fixture(autouse=True)
def _clean_chart_output():
    """Keep the repo-root chart artifact deterministic across test runs."""
    chart = _chart_path()
    chart.unlink(missing_ok=True)
    yield
    chart.unlink(missing_ok=True)


def test_full_run_produces_legacy_chart():
    """Full pipeline runs without error and writes a legacy HTML chart."""
    strat.run()

    chart = _chart_path()
    assert chart.exists(), "Legacy chart not created"
    assert chart.stat().st_size > 0, "Legacy chart file is empty"
