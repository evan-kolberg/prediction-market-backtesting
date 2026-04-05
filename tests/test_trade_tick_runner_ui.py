from __future__ import annotations

import asyncio
from types import SimpleNamespace

from backtests._shared import _kalshi_trade_tick_runner as kalshi_runner
from backtests._shared import _polymarket_trade_tick_runner as polymarket_runner


def test_kalshi_trade_tick_runner_uses_unified_summary(monkeypatch, capsys):
    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        return [
            {
                "ticker": "KALSHI-TEST",
                "trades": 3,
                "fills": 2,
                "pnl": -1.25,
                "outcome": "Yes",
                "realized_outcome": 1.0,
                "token_index": 0,
                "fill_events": [],
                "entry_min": 0.11,
                "last": 0.19,
                "max": 0.23,
            }
        ]

    monkeypatch.setattr(
        kalshi_runner.PredictionMarketBacktest, "run_async", _fake_run_async
    )

    asyncio.run(
        kalshi_runner.run_single_market_trade_backtest(
            name="kalshi_test",
            market_ticker="KALSHI-TEST",
            lookback_days=1,
            probability_window=5,
            initial_cash=100.0,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
            data_sources=("https://api.elections.kalshi.com",),
        )
    )

    out = capsys.readouterr().out
    assert "Market" in out
    assert "Trades" in out
    assert "Fills" in out
    assert "PnL (USD)" in out
    assert "KALSHI-TEST" in out


def test_polymarket_trade_tick_runner_uses_unified_summary(monkeypatch, capsys):
    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        return [
            {
                "slug": "demo-market",
                "trades": 3,
                "fills": 2,
                "pnl": -0.5477,
                "outcome": "Yes",
                "realized_outcome": 1.0,
                "token_index": 0,
                "market_label": "demo-market:Yes",
                "fill_events": [],
                "entry_min": 0.001,
                "last": 0.002,
                "max": 0.049,
            }
        ]

    monkeypatch.setattr(
        polymarket_runner.PredictionMarketBacktest,
        "run_async",
        _fake_run_async,
    )

    asyncio.run(
        polymarket_runner.run_single_market_trade_backtest(
            name="polymarket_test",
            market_slug="demo-market",
            token_index=0,
            lookback_days=1,
            probability_window=5,
            initial_cash=100.0,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
            data_sources=(
                "gamma-api.polymarket.com",
                "data-api.polymarket.com/trades",
                "clob.polymarket.com",
            ),
        )
    )

    out = capsys.readouterr().out
    assert "Market" in out
    assert "Trades" in out
    assert "Fills" in out
    assert "PnL (USDC)" in out
    assert "demo-market" in out
