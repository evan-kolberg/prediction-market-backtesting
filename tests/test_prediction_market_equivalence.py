from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pandas as pd

from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting import _prediction_market_runner as runner
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketSimConfig,
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig


class _EngineStub:
    def __init__(self, *, config) -> None:  # type: ignore[no-untyped-def]
        self.config = config
        self.trader = SimpleNamespace(
            generate_order_fills_report=lambda: pd.DataFrame(),
            generate_positions_report=lambda: pd.DataFrame(),
        )

    def add_venue(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    def add_instrument(self, instrument) -> None:  # type: ignore[no-untyped-def]
        return None

    def add_data(self, records) -> None:  # type: ignore[no-untyped-def]
        return None

    def add_strategy(self, strategy) -> None:  # type: ignore[no-untyped-def]
        return None

    def run(self) -> None:
        return None

    def get_result(self):  # type: ignore[no-untyped-def]
        return SimpleNamespace(backtest_end=2)

    def reset(self) -> None:
        return None

    def dispose(self) -> None:
        return None


class _FakeQuoteTick:
    def __init__(self, *, bid_price: float, ask_price: float, ts_init: int) -> None:
        self.bid_price = bid_price
        self.ask_price = ask_price
        self.ts_init = ts_init
        self.ts_event = ts_init


def _patch_backtest_runtime(monkeypatch) -> None:
    monkeypatch.setattr(backtest_module, "BacktestEngine", _EngineStub)
    monkeypatch.setattr(
        backtest_module.PredictionMarketBacktest,
        "_build_market_artifacts",
        lambda self, *, engine, loaded_sims, fills_report: {},
    )
    monkeypatch.setattr(
        backtest_module.PredictionMarketBacktest,
        "_build_joint_portfolio_artifacts",
        lambda self, *, engine, loaded_sims: {},
    )
    monkeypatch.setattr(backtest_module, "is_backtest_force_stop", lambda: False)


def test_kalshi_trade_tick_wrapper_matches_direct_backtest(monkeypatch) -> None:
    _patch_backtest_runtime(monkeypatch)

    async def _from_market_ticker(ticker: str):  # type: ignore[no-untyped-def]
        return SimpleNamespace(
            instrument=SimpleNamespace(id="KALSHI-TEST", outcome="Yes"),
            load_trades=lambda start, end: _load_trades(),
        )

    async def _load_trades():  # type: ignore[no-untyped-def]
        return [
            SimpleNamespace(price=0.4, ts_init=1, ts_event=1),
            SimpleNamespace(price=0.6, ts_init=2, ts_event=2),
        ]

    monkeypatch.setattr(
        backtest_module, "KalshiDataLoader", SimpleNamespace(from_market_ticker=_from_market_ticker)
    )

    direct = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="kalshi",
            data_type="trade_tick",
            vendor="native",
            sources=("api.elections.kalshi.com/trade-api/v2",),
        ),
        sims=(
            MarketSimConfig(
                market_ticker="KALSHI-TEST", lookback_days=1, end_time="2026-04-05T00:00:00Z"
            ),
        ),
        strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        initial_cash=100.0,
        probability_window=5,
        emit_html=False,
    ).run()[0]

    wrapper = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=MarketDataConfig(
                platform="kalshi",
                data_type="trade_tick",
                vendor="native",
                sources=("api.elections.kalshi.com/trade-api/v2",),
            ),
            market_ticker="KALSHI-TEST",
            lookback_days=1,
            probability_window=5,
            end_time="2026-04-05T00:00:00Z",
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
            emit_summary=False,
            emit_html=False,
        )
    )

    assert wrapper == direct


def test_polymarket_trade_tick_wrapper_matches_direct_backtest(monkeypatch) -> None:
    _patch_backtest_runtime(monkeypatch)

    async def _from_market_slug(slug: str, *, token_index: int = 0):  # type: ignore[no-untyped-def]
        return SimpleNamespace(
            instrument=SimpleNamespace(id=f"{slug}:{token_index}", outcome="Yes"),
            load_trades=lambda start, end: _load_trades(),
        )

    async def _load_trades():  # type: ignore[no-untyped-def]
        return [
            SimpleNamespace(price=0.45, ts_init=1, ts_event=1),
            SimpleNamespace(price=0.55, ts_init=2, ts_event=2),
        ]

    monkeypatch.setattr(
        backtest_module, "PolymarketDataLoader", SimpleNamespace(from_market_slug=_from_market_slug)
    )

    direct = PredictionMarketBacktest(
        name="demo",
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
        sims=(MarketSimConfig(market_slug="demo-market", lookback_days=1),),
        strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        initial_cash=100.0,
        probability_window=5,
        emit_html=False,
    ).run()[0]

    wrapper = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
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
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
            emit_summary=False,
            emit_html=False,
        )
    )

    assert wrapper is not None
    assert wrapper["slug"] == direct["slug"]
    assert wrapper["trades"] == direct["trades"]
    assert wrapper["fills"] == direct["fills"]
    assert wrapper["pnl"] == direct["pnl"]
    assert wrapper["outcome"] == direct["outcome"]
    assert wrapper["realized_outcome"] == direct["realized_outcome"]
    assert wrapper["token_index"] == direct["token_index"]


def test_pmxt_quote_tick_wrapper_matches_direct_backtest(monkeypatch) -> None:
    _patch_backtest_runtime(monkeypatch)
    monkeypatch.setattr(backtest_module, "QuoteTick", _FakeQuoteTick)

    async def _from_market_slug(slug: str, *, token_index: int = 0):  # type: ignore[no-untyped-def]
        return SimpleNamespace(
            instrument=SimpleNamespace(id=f"{slug}:{token_index}", outcome="Yes"),
            load_order_book_and_quotes=lambda start, end: [
                _FakeQuoteTick(bid_price=0.49, ask_price=0.51, ts_init=1),
                _FakeQuoteTick(bid_price=0.50, ask_price=0.52, ts_init=2),
            ],
        )

    monkeypatch.setattr(
        backtest_module,
        "PolymarketPMXTDataLoader",
        SimpleNamespace(from_market_slug=_from_market_slug),
    )

    direct = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
            sources=("local:/tmp/pmxt-a", "archive:archive.vendor.test"),
        ),
        sims=(
            MarketSimConfig(
                market_slug="demo-market",
                token_index=0,
                start_time="2026-02-21T16:00:00Z",
                end_time="2026-02-21T17:00:00Z",
            ),
        ),
        strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
        initial_cash=100.0,
        probability_window=5,
        emit_html=False,
    ).run()[0]

    wrapper = asyncio.run(
        runner.run_single_market_backtest(
            name="demo",
            data=MarketDataConfig(
                platform="polymarket",
                data_type="quote_tick",
                vendor="pmxt",
                sources=(
                    "local:/tmp/pmxt-a",
                    "archive:archive.vendor.test",
                ),
            ),
            market_slug="demo-market",
            token_index=0,
            probability_window=5,
            strategy_factory=lambda instrument_id: SimpleNamespace(instrument_id=instrument_id),
            start_time="2026-02-21T16:00:00Z",
            end_time="2026-02-21T17:00:00Z",
            emit_summary=False,
            emit_html=False,
        )
    )

    assert wrapper == direct
