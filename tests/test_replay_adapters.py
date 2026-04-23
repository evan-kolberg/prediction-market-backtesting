from __future__ import annotations

import asyncio
from types import SimpleNamespace

from prediction_market_extensions.adapters.prediction_market import ReplayLoadRequest
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting.data_sources import replay_adapters


class _FakeQuoteTick:
    def __init__(self, bid_price: float, ask_price: float, ts_event: int) -> None:
        self.bid_price = bid_price
        self.ask_price = ask_price
        self.ts_event = ts_event


class _FakeTelonexLoader:
    def __init__(self) -> None:
        self.instrument = SimpleNamespace(
            outcome="YES",
            info={"tokens": [{"winner": True}]},
            id="fake-instrument-id",
        )
        self.last_kwargs: dict[str, object] | None = None

    async def from_market_slug(self, market_slug: str, token_index: int = 0):  # pragma: no cover
        raise AssertionError("instance method should not be called")

    def load_order_book_and_quotes(self, start, end, **kwargs):  # type: ignore[no-untyped-def]
        self.last_kwargs = {"start": start, "end": end, **kwargs}
        return (_FakeQuoteTick(0.24, 0.26, 1),)


def test_telonex_replay_adapter_uses_loader_outcome_when_replay_outcome_missing(
    monkeypatch,
) -> None:
    loader = _FakeTelonexLoader()

    async def _fake_from_market_slug(market_slug: str, token_index: int = 0):
        assert market_slug == "demo-market"
        assert token_index == 0
        return loader

    def _fake_resolver(name: str, default):  # type: ignore[no-untyped-def]
        if name == "PolymarketTelonexQuoteDataLoader":
            return SimpleNamespace(from_market_slug=_fake_from_market_slug)
        if name == "QuoteTick":
            return _FakeQuoteTick
        return default

    monkeypatch.setattr(replay_adapters, "_resolve_backtest_compat_symbol", _fake_resolver)

    replay = QuoteReplay(
        market_slug="demo-market",
        token_index=0,
        start_time="2026-03-01T00:00:00Z",
        end_time="2026-03-01T00:05:00Z",
    )

    loaded = asyncio.run(
        replay_adapters.PolymarketTelonexQuoteReplayAdapter().load_replay(
            replay,
            request=ReplayLoadRequest(min_record_count=0, min_price_range=0.0),
        )
    )

    assert loaded is not None
    assert loader.last_kwargs is not None
    assert loader.last_kwargs["outcome"] == "YES"
    assert loaded.outcome == "YES"
