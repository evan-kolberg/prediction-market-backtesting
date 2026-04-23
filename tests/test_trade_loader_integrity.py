from __future__ import annotations

import asyncio
from contextlib import nullcontext
from types import SimpleNamespace

import msgspec
import pandas as pd
import pytest

from prediction_market_extensions.adapters.kalshi.loaders import KalshiDataLoader
from prediction_market_extensions.adapters.kalshi.providers import market_dict_to_instrument
from prediction_market_extensions.adapters.polymarket.gamma_markets import (
    infer_gamma_token_winners,
)
from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from prediction_market_extensions.adapters.prediction_market import (
    LoadedReplay,
    ReplayCoverageStats,
    ReplayWindow,
)
from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    POLYMARKET_TRADE_API_BASE_URL_ENV,
    RunnerPolymarketDataLoader,
)


def _polymarket_instrument():
    return parse_polymarket_instrument(
        market_info={
            "condition_id": "0x" + "1" * 64,
            "question": "Synthetic Polymarket market",
            "minimum_tick_size": "0.01",
            "minimum_order_size": "1",
            "end_date_iso": "2026-12-31T00:00:00Z",
            "maker_base_fee": "0",
            "taker_base_fee": "0",
        },
        token_id="2" * 64,
        outcome="Yes",
        ts_init=0,
    )


def _kalshi_instrument():
    return market_dict_to_instrument(
        {
            "ticker": "KXTEST-26",
            "event_ticker": "KXTEST",
            "title": "Synthetic Kalshi market",
            "open_time": "2026-01-01T00:00:00+00:00",
            "close_time": "2026-12-31T00:00:00+00:00",
        }
    )


def _polymarket_trade(
    *,
    timestamp: int,
    transaction_hash: str,
    asset: str = "2" * 64,
    price: str = "0.50",
    size: str = "1",
    side: str = "BUY",
) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "transactionHash": transaction_hash,
        "asset": asset,
        "price": price,
        "size": size,
        "side": side,
    }


def test_polymarket_trade_load_sorts_same_second_trades_deterministically(monkeypatch) -> None:
    loader = PolymarketDataLoader.__new__(PolymarketDataLoader)
    loader._instrument = _polymarket_instrument()
    loader._token_id = "2" * 64
    loader._condition_id = "0x" + "1" * 64

    async def fake_fetch_trades(*, condition_id, start_ts, end_ts):  # type: ignore[no-untyped-def]
        del condition_id, start_ts, end_ts
        return [
            _polymarket_trade(timestamp=100, transaction_hash="0x" + "b" * 64),
            _polymarket_trade(timestamp=100, transaction_hash="0x" + "a" * 64),
        ]

    monkeypatch.setattr(loader, "fetch_trades", fake_fetch_trades)

    trades = asyncio.run(loader.load_trades())

    assert [trade.ts_event for trade in trades] == [100_000_000_000, 100_000_000_001]
    assert "a" * 24 in str(trades[0].trade_id)
    assert "b" * 24 in str(trades[1].trade_id)


def test_polymarket_parse_trades_disambiguates_same_token_multifills() -> None:
    loader = PolymarketDataLoader.__new__(PolymarketDataLoader)
    loader._instrument = _polymarket_instrument()
    loader._token_id = "2" * 64
    loader._condition_id = "0x" + "1" * 64
    shared_hash = "0x" + "c" * 64

    trades = loader.parse_trades(
        [
            _polymarket_trade(timestamp=100, transaction_hash=shared_hash, price="0.50"),
            _polymarket_trade(timestamp=100, transaction_hash=shared_hash, price="0.51"),
        ]
    )

    assert len({str(trade.trade_id) for trade in trades}) == 2
    assert [trade.ts_event for trade in trades] == [100_000_000_000, 100_000_000_001]


def test_polymarket_parse_trades_validates_price_inside_loop() -> None:
    loader = PolymarketDataLoader.__new__(PolymarketDataLoader)
    loader._instrument = _polymarket_instrument()
    loader._token_id = "2" * 64
    loader._condition_id = "0x" + "1" * 64

    with pytest.raises(ValueError, match="price must be in"):
        loader.parse_trades(
            [_polymarket_trade(timestamp=100, transaction_hash="0x" + "d" * 64, price="1.20")]
        )


def test_gamma_winner_inference_ignores_active_99_cent_markets() -> None:
    winners, is_50_50 = infer_gamma_token_winners(
        {
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.995", "0.005"]',
            "closed": False,
            "umaResolutionStatus": "",
        }
    )

    assert winners == {}
    assert is_50_50 is False


def test_gamma_winner_inference_allows_closed_resolved_prices() -> None:
    winners, is_50_50 = infer_gamma_token_winners(
        {
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.995", "0.005"]',
            "closed": True,
            "umaResolutionStatus": "resolved",
        }
    )

    assert winners == {"yes": True, "no": False}
    assert is_50_50 is False


class _Response:
    def __init__(self, status: int, payload: object) -> None:
        self.status = status
        self.body = payload if isinstance(payload, bytes) else msgspec.json.encode(payload)


class _OffsetCeilingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, *, url, params):  # type: ignore[no-untyped-def]
        del url, params
        self.calls += 1
        if self.calls == 1:
            return _Response(200, [{"timestamp": 10, "asset": "2" * 64}])
        return _Response(400, b"max historical activity offset exceeded")


@pytest.mark.parametrize("loader_cls", [PolymarketDataLoader, RunnerPolymarketDataLoader])
def test_polymarket_fetch_trades_returns_partial_data_on_offset_ceiling(
    loader_cls, monkeypatch: pytest.MonkeyPatch
) -> None:
    loader = loader_cls.__new__(loader_cls)
    loader._http_client = _OffsetCeilingClient()
    monkeypatch.setenv(POLYMARKET_TRADE_API_BASE_URL_ENV, "https://data-api.polymarket.com")

    with pytest.warns(RuntimeWarning, match="offset ceiling"):
        trades = asyncio.run(loader.fetch_trades(condition_id="0x" + "1" * 64, limit=1, start_ts=0))

    assert trades == [{"timestamp": 10, "asset": "2" * 64}]


def test_polymarket_fetch_trades_does_not_stop_on_one_old_trade_in_mixed_page() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, *, url, params):  # type: ignore[no-untyped-def]
            del url, params
            self.calls += 1
            if self.calls == 1:
                return _Response(200, [{"timestamp": 20}, {"timestamp": 5}])
            return _Response(200, [{"timestamp": 21}])

    loader = PolymarketDataLoader.__new__(PolymarketDataLoader)
    loader._http_client = Client()

    trades = asyncio.run(loader.fetch_trades(condition_id="0x" + "1" * 64, limit=2, start_ts=10))

    assert [trade["timestamp"] for trade in trades] == [20, 21]


def _kalshi_trade(
    *,
    ts: int = 100,
    created_time: str | None = None,
    trade_id: str | None = None,
    price: str = "0.50",
) -> dict[str, object]:
    trade: dict[str, object] = {
        "ts": ts,
        "yes_price_dollars": price,
        "count": 1,
        "taker_side": "yes",
    }
    if created_time is not None:
        trade["created_time"] = created_time
    if trade_id is not None:
        trade["trade_id"] = trade_id
    return trade


def test_kalshi_parse_trades_adds_intra_second_tiebreaker() -> None:
    loader = KalshiDataLoader.__new__(KalshiDataLoader)
    loader._instrument = _kalshi_instrument()

    trades = loader.parse_trades([_kalshi_trade(), _kalshi_trade(price="0.51")])

    assert [trade.ts_event for trade in trades] == [100_000_000_000, 100_000_000_001]
    assert len({str(trade.trade_id) for trade in trades}) == 2


def test_kalshi_load_trades_filters_with_subsecond_precision(monkeypatch) -> None:
    loader = KalshiDataLoader.__new__(KalshiDataLoader)
    loader._instrument = _kalshi_instrument()

    async def fake_fetch_trades(*, min_ts, max_ts):  # type: ignore[no-untyped-def]
        del min_ts, max_ts
        return [
            _kalshi_trade(created_time="2026-01-01T00:00:00.100000Z", trade_id="early"),
            _kalshi_trade(created_time="2026-01-01T00:00:00.600000Z", trade_id="late"),
        ]

    monkeypatch.setattr(loader, "fetch_trades", fake_fetch_trades)

    trades = asyncio.run(loader.load_trades(start=pd.Timestamp("2026-01-01T00:00:00.500000Z")))

    assert [str(trade.trade_id) for trade in trades] == ["late"]


def test_backtest_warns_on_large_loaded_data_gap(monkeypatch) -> None:
    replay = object()
    loaded = LoadedReplay(
        replay=replay,
        instrument=SimpleNamespace(id="instrument"),
        records=(
            SimpleNamespace(ts_event=0),
            SimpleNamespace(ts_event=5 * 60 * 60 * 1_000_000_000),
        ),
        outcome="Yes",
        realized_outcome=None,
        metadata={},
        requested_window=ReplayWindow(),
        loaded_window=None,
        coverage_stats=ReplayCoverageStats(
            count=2,
            count_key="trades",
            market_key="slug",
            market_id="gap-market",
        ),
    )

    class Adapter:
        replay_spec_type = object

        def configure_sources(self, sources):  # type: ignore[no-untyped-def]
            del sources
            return nullcontext(SimpleNamespace(summary="test-source"))

        async def load_replay(self, replay_arg, *, request):  # type: ignore[no-untyped-def]
            del replay_arg, request
            return loaded

    monkeypatch.setattr(backtest_module, "resolve_replay_adapter", lambda **kwargs: Adapter())

    backtest = PredictionMarketBacktest(
        name="gap-test",
        data=MarketDataConfig(platform="polymarket", data_type="trade_tick", vendor="native"),
        replays=[replay],
        strategy_configs=[],
        strategy_factory=lambda instrument_id: None,
        initial_cash=100.0,
        probability_window=10,
    )

    with pytest.warns(RuntimeWarning, match="5.00 hour data gap"):
        loaded_sims = asyncio.run(backtest._load_sims_async())

    assert loaded_sims == [loaded]
