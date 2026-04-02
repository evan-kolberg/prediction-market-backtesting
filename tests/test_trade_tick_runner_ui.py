from __future__ import annotations

import asyncio
from types import SimpleNamespace

from backtests.kalshi_trade_tick import (
    _kalshi_single_market_trade_runner as kalshi_runner,
)
from backtests.polymarket_trade_tick import (
    _polymarket_single_market_runner as polymarket_runner,
)


class _TradeStub:
    def __init__(self, price: float) -> None:
        self.price = price


def test_kalshi_trade_tick_runner_uses_unified_summary(monkeypatch, capsys):
    class _LoaderStub:
        instrument = SimpleNamespace(id="KALSHI.TEST")

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            return [_TradeStub(0.11), _TradeStub(0.23), _TradeStub(0.19)]

    async def _from_market_ticker(_cls, market_ticker):  # type: ignore[no-untyped-def]
        assert market_ticker == "KALSHI-TEST"
        return _LoaderStub()

    monkeypatch.setattr(
        kalshi_runner.KalshiDataLoader,
        "from_market_ticker",
        classmethod(_from_market_ticker),
    )
    monkeypatch.setattr(
        kalshi_runner,
        "run_market_backtest",
        lambda **kwargs: {
            "ticker": kwargs["market_id"],
            "trades": len(kwargs["data"]),
            "fills": 2,
            "pnl": -1.25,
        },
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
        )
    )

    out = capsys.readouterr().out
    assert "running backtest for KALSHI-TEST..." in out
    assert "Min Px" in out
    assert "Last Px" in out
    assert "PnL (USD)" in out
    assert "KALSHI-TEST" in out


def test_polymarket_trade_tick_runner_uses_unified_summary(monkeypatch, capsys):
    class _LoaderStub:
        instrument = SimpleNamespace(
            id="POLYMARKET.TEST",
            outcome="Yes",
        )

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            return [_TradeStub(0.001), _TradeStub(0.049), _TradeStub(0.002)]

    async def _from_market_slug(_cls, market_slug, token_index=0):  # type: ignore[no-untyped-def]
        assert market_slug == "demo-market"
        assert token_index == 0
        return _LoaderStub()

    monkeypatch.setattr(
        polymarket_runner.PolymarketDataLoader,
        "from_market_slug",
        classmethod(_from_market_slug),
    )
    monkeypatch.setattr(
        polymarket_runner,
        "run_market_backtest",
        lambda **kwargs: {
            "slug": kwargs["market_id"],
            "trades": len(kwargs["data"]),
            "fills": 2,
            "pnl": -0.5477,
        },
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
        )
    )

    out = capsys.readouterr().out
    assert "running backtest for demo-market:Yes..." in out
    assert "Min Px" in out
    assert "Last Px" in out
    assert "PnL (USDC)" in out
    assert "demo-market:Yes" in out
