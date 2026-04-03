"""End-to-end test for the Kalshi EMA-crossover backtest."""

import pytest

import backtests.kalshi_trade_tick_ema_crossover as strat


@pytest.fixture(autouse=True)
def _isolate_output(tmp_path, monkeypatch):
    """Redirect generated chart output to a temp directory."""
    monkeypatch.chdir(tmp_path)


def test_full_run_produces_legacy_chart(tmp_path):
    """Full pipeline runs without error and writes a legacy HTML chart."""
    strat.run()

    chart = tmp_path / "output" / f"{strat.NAME}_{strat.MARKET_TICKER}_legacy.html"
    assert chart.exists(), "Legacy chart not created"
    assert chart.stat().st_size > 0, "Legacy chart file is empty"
