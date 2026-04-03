import asyncio
from types import SimpleNamespace

from backtests._shared import _polymarket_quote_tick_pmxt_runner as pmxt_runner


class _QuoteStub:
    def __init__(self, bid_price: float, ask_price: float) -> None:
        self.bid_price = bid_price
        self.ask_price = ask_price


def test_pmxt_runner_uses_l2_execution_settings(monkeypatch):
    captured: dict[str, object] = {}
    window: dict[str, object] = {}

    class _LoaderStub:
        instrument = SimpleNamespace(
            id="POLYMARKET.TEST",
            outcome="YES",
            info={},
        )

        def load_order_book_and_quotes(self, start, end):  # type: ignore[no-untyped-def]
            window["start"] = start
            window["end"] = end
            return [
                _QuoteStub(0.40, 0.42),
                object(),
                _QuoteStub(0.41, 0.43),
            ]

    async def _from_market_slug(_cls, market_slug, token_index=0):  # type: ignore[no-untyped-def]
        return _LoaderStub()

    def _fake_run_market_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "slug": kwargs["market_id"],
            "quotes": kwargs["data_count"],
            "fills": 0,
            "pnl": 0.0,
        }

    monkeypatch.setattr(pmxt_runner, "QuoteTick", _QuoteStub)
    monkeypatch.setattr(
        pmxt_runner.PolymarketPMXTDataLoader,
        "from_market_slug",
        classmethod(_from_market_slug),
    )
    monkeypatch.setattr(pmxt_runner, "run_market_backtest", _fake_run_market_backtest)

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            lookback_hours=1.0,
            end_time="1970-01-01T01:00:00Z",
            min_quotes=2,
            min_price_range=0.0,
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        ),
    )

    assert result is not None
    assert captured["apply_default_fill_model"] is False
    assert captured["book_type"] == pmxt_runner.BookType.L2_MBP
    assert captured["liquidity_consumption"] is True
    assert captured["price_attr"] == "mid_price"
    assert captured["count_key"] == "quotes"
    assert captured["data_count"] == 2
    assert str(window["start"]) == "1970-01-01 00:00:00+00:00"
    assert str(window["end"]) == "1970-01-01 01:00:00+00:00"


def test_pmxt_runner_respects_explicit_start_and_end_times(monkeypatch):
    window: dict[str, object] = {}

    class _LoaderStub:
        instrument = SimpleNamespace(
            id="POLYMARKET.TEST",
            outcome="YES",
            info={},
        )

        def load_order_book_and_quotes(self, start, end):  # type: ignore[no-untyped-def]
            window["start"] = start
            window["end"] = end
            return [_QuoteStub(0.40, 0.42), _QuoteStub(0.41, 0.43)]

    async def _from_market_slug(_cls, market_slug, token_index=0):  # type: ignore[no-untyped-def]
        return _LoaderStub()

    monkeypatch.setattr(pmxt_runner, "QuoteTick", _QuoteStub)
    monkeypatch.setattr(
        pmxt_runner.PolymarketPMXTDataLoader,
        "from_market_slug",
        classmethod(_from_market_slug),
    )
    monkeypatch.setattr(
        pmxt_runner,
        "run_market_backtest",
        lambda **kwargs: {
            "slug": kwargs["market_id"],
            "quotes": kwargs["data_count"],
            "fills": 0,
            "pnl": 0.0,
        },
    )

    result = asyncio.run(
        pmxt_runner.run_single_market_pmxt_backtest(
            name="pmxt_test",
            market_slug="demo-market",
            start_time="2026-03-22T09:00:00Z",
            end_time="2026-03-22T13:00:00Z",
            probability_window=5,
            initial_cash=100.0,
            emit_summary=False,
            emit_html=False,
            strategy_factory=lambda instrument_id: SimpleNamespace(
                instrument_id=instrument_id
            ),
        ),
    )

    assert result is not None
    assert str(window["start"]) == "2026-03-22 09:00:00+00:00"
    assert str(window["end"]) == "2026-03-22 13:00:00+00:00"
