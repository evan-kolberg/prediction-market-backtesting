from __future__ import annotations

import asyncio
from types import SimpleNamespace

from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting import _prediction_market_runner as runner
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig


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

    monkeypatch.setattr(backtest_module.PredictionMarketBacktest, "run_async", _fake_run_async)

    asyncio.run(
        runner.run_single_market_backtest(
            name="kalshi_test",
            data=MarketDataConfig(
                platform="kalshi",
                data_type="trade_tick",
                vendor="native",
                sources=("https://api.elections.kalshi.com",),
            ),
            market_ticker="KALSHI-TEST",
            lookback_days=1,
            probability_window=5,
            initial_cash=100.0,
            emit_html=False,
            report=MarketReportConfig(
                count_key="trades", count_label="Trades", pnl_label="PnL (USD)", market_key="ticker"
            ),
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        )
    )

    out = capsys.readouterr().out
    assert "KALSHI-TEST" in out
    assert "Fills" in out or "fills" in out
    assert "TOTAL" in out


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

    monkeypatch.setattr(backtest_module.PredictionMarketBacktest, "run_async", _fake_run_async)

    asyncio.run(
        runner.run_single_market_backtest(
            name="polymarket_test",
            data=MarketDataConfig(
                platform="polymarket",
                data_type="trade_tick",
                vendor="native",
                sources=(
                    "gamma-api.polymarket.com",
                    "data-api.polymarket.com/trades",
                    "clob.polymarket.com",
                ),
            ),
            market_slug="demo-market",
            token_index=0,
            lookback_days=1,
            probability_window=5,
            initial_cash=100.0,
            emit_html=False,
            report=MarketReportConfig(
                count_key="trades", count_label="Trades", pnl_label="PnL (USDC)", market_key="slug"
            ),
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        )
    )

    out = capsys.readouterr().out
    assert "demo-market" in out
    assert "Fills" in out or "fills" in out
    assert "TOTAL" in out
