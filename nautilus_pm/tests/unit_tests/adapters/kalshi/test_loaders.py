# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#

import decimal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import msgspec
import pandas as pd
import pytest

from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def make_instrument(ticker: str = "KXBTC-25MAR15-B100000") -> BinaryOption:
    """Return a minimal BinaryOption for testing."""
    return BinaryOption(
        instrument_id=InstrumentId(Symbol(ticker), Venue("KALSHI")),
        raw_symbol=Symbol(ticker),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str("USD"),
        activation_ns=0,
        expiration_ns=0,
        price_precision=4,
        size_precision=2,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=decimal.Decimal(0),
        taker_fee=decimal.Decimal(0),
        outcome="Yes",
        description="Test market",
        ts_event=0,
        ts_init=0,
    )


def test_init_stores_instrument():
    instrument = make_instrument()
    http_client = MagicMock()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=http_client)
    assert loader.instrument is instrument


def test_init_creates_default_http_client():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC")
    assert loader._http_client is not None


def make_market_dict(ticker: str = "KXBTC-25MAR15-B100000") -> dict:
    return {
        "ticker": ticker,
        "event_ticker": "KXBTC-25MAR15",
        "title": "BTC above 100k on March 15?",
        "open_time": "2025-01-01T00:00:00Z",
        "close_time": "2025-03-15T00:00:00Z",
        "latest_expiration_time": "2025-03-15T00:00:00Z",
    }


def make_event_dict(series_ticker: str = "KXBTC") -> dict:
    return {"event": {"series_ticker": series_ticker, "event_ticker": "KXBTC-25MAR15"}}


def make_mock_response(body: dict | list, status: int = 200):
    mock = MagicMock()
    mock.status = status
    mock.body = msgspec.json.encode(body)
    return mock


def make_candle_dict(end_ts: int = 1700000060) -> dict:
    return {
        "end_period_ts": end_ts,
        "yes_bid": {"open": "0.41", "high": "0.44", "low": "0.40", "close": "0.42"},
        "yes_ask": {"open": "0.43", "high": "0.46", "low": "0.42", "close": "0.44"},
        "price": {
            "open": "0.42",
            "high": "0.45",
            "low": "0.41",
            "close": "0.43",
            "mean": "0.42",
        },
        "volume": "100.00",
        "open_interest": "500.00",
    }


def make_trade_dict(
    ts: int = 1700000000,
    yes_price: str = "0.4200",
    count: str = "10.00",
    taker_side: str = "yes",
) -> dict:
    no_price = f"{1 - float(yes_price):.4f}"
    return {
        "ts": ts,
        "yes_price": yes_price,
        "no_price": no_price,
        "count": count,
        "taker_side": taker_side,
    }


def make_trade_dict_v2(
    created_time: str = "2026-03-05T19:39:35.459340Z",
    yes_price_dollars: str = "0.4200",
    count_fp: str = "10.00",
    taker_side: str = "yes",
    trade_id: str = "7aecfcc5-3df3-6ad6-df55-4cc92e61f6dd",
) -> dict:
    return {
        "created_time": created_time,
        "yes_price": int(float(yes_price_dollars) * 100),
        "yes_price_dollars": yes_price_dollars,
        "no_price": 100 - int(float(yes_price_dollars) * 100),
        "no_price_dollars": f"{1 - float(yes_price_dollars):.4f}",
        "count": int(float(count_fp)),
        "count_fp": count_fp,
        "price": float(yes_price_dollars),
        "taker_side": taker_side,
        "ticker": "KXBTC-25MAR15-B100000",
        "trade_id": trade_id,
    }


def test_normalize_price_handles_decimal_and_cent_boundaries():
    assert KalshiDataLoader._normalize_price(42) == pytest.approx(0.42)
    assert KalshiDataLoader._normalize_price("42") == pytest.approx(0.42)
    assert KalshiDataLoader._normalize_price(1) == pytest.approx(0.01)
    assert KalshiDataLoader._normalize_price("1") == pytest.approx(0.01)
    assert KalshiDataLoader._normalize_price(1.0) == pytest.approx(1.0)
    assert KalshiDataLoader._normalize_price("1.0") == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_from_market_ticker_returns_loader():
    ticker = "KXBTC-25MAR15-B100000"
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        side_effect=[
            make_mock_response({"market": make_market_dict(ticker)}),
            make_mock_response(make_event_dict("KXBTC")),
        ]
    )

    loader = await KalshiDataLoader.from_market_ticker(ticker, http_client=mock_client)

    assert isinstance(loader, KalshiDataLoader)
    assert loader.instrument.id.symbol.value == ticker
    assert loader._series_ticker == "KXBTC"
    assert mock_client.get.call_count == 2


@pytest.mark.asyncio
async def test_from_market_ticker_raises_on_404():
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_mock_response({}, status=404))

    with pytest.raises(ValueError, match="not found"):
        await KalshiDataLoader.from_market_ticker("NONEXISTENT", http_client=mock_client)


@pytest.mark.asyncio
async def test_from_market_ticker_raises_on_server_error():
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_mock_response({"error": "internal"}, status=500))

    with pytest.raises(RuntimeError, match="HTTP request failed"):
        await KalshiDataLoader.from_market_ticker("KXBTC-25MAR15-B100000", http_client=mock_client)


@pytest.mark.asyncio
async def test_fetch_trades_single_page():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"trades": [make_trade_dict()], "cursor": ""})
    )
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    trades = await loader.fetch_trades()

    assert len(trades) == 1
    assert trades[0]["ts"] == 1700000000
    mock_client.get.assert_called_once()
    assert mock_client.get.call_args.kwargs["url"].endswith("/markets/trades")
    assert mock_client.get.call_args.kwargs["params"]["ticker"] == instrument.id.symbol.value


@pytest.mark.asyncio
async def test_fetch_trades_paginates():
    instrument = make_instrument()
    mock_client = MagicMock()
    page1 = make_mock_response({"trades": [make_trade_dict(ts=1)], "cursor": "abc"})
    page2 = make_mock_response({"trades": [make_trade_dict(ts=2)], "cursor": ""})
    mock_client.get = AsyncMock(side_effect=[page1, page2])
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    trades = await loader.fetch_trades()

    assert len(trades) == 2
    assert mock_client.get.call_count == 2


def test_parse_trades_returns_trade_ticks():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    raw = [
        make_trade_dict(ts=1700000000, yes_price="0.4200", count="10.00", taker_side="yes"),
        make_trade_dict(ts=1700000001, yes_price="0.5000", count="5.00", taker_side="no"),
    ]
    ticks = loader.parse_trades(raw)

    assert len(ticks) == 2
    assert isinstance(ticks[0], TradeTick)
    assert ticks[0].aggressor_side == AggressorSide.BUYER
    assert ticks[1].aggressor_side == AggressorSide.SELLER
    # Timestamp: 1700000000 seconds → nanoseconds
    assert ticks[0].ts_event == 1700000000 * 1_000_000_000


def test_parse_trades_prefers_current_kalshi_payload_fields():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    tick = loader.parse_trades([make_trade_dict_v2()])[0]

    assert str(tick.trade_id) == "7aecfcc5-3df3-6ad6-df55-4cc92e61f6dd"
    assert str(tick.price) == "0.4200"
    assert str(tick.size) == "10.00"
    assert tick.ts_event == pd.Timestamp("2026-03-05T19:39:35.459340Z").value


def test_parse_trades_generates_unique_ids_without_trade_id():
    ticker = "KXNEXTIRANLEADER-45JAN01-MKHA-LONGSUFFIX"
    instrument = make_instrument(ticker=ticker)
    loader = KalshiDataLoader(
        instrument=instrument, series_ticker="KXNEXTIRANLEADER", http_client=MagicMock()
    )

    raw = [
        make_trade_dict(ts=1700000000, yes_price="0.4200", count="10.00"),
        make_trade_dict(ts=1700000000, yes_price="0.4300", count="10.00"),
        make_trade_dict(ts=1700000000, yes_price="0.4200", count="10.00"),
    ]
    for trade in raw:
        trade.pop("trade_id", None)

    ticks = loader.parse_trades(raw)

    assert len({str(tick.trade_id) for tick in ticks}) == 3
    assert all(len(str(tick.trade_id)) <= 36 for tick in ticks)


def test_parse_trades_unknown_side_gives_no_aggressor():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    raw = [make_trade_dict(taker_side="unknown")]
    ticks = loader.parse_trades(raw)

    assert ticks[0].aggressor_side == AggressorSide.NO_AGGRESSOR


@pytest.mark.asyncio
async def test_load_trades_filters_by_time_range():
    instrument = make_instrument()
    mock_client = MagicMock()
    raw = [
        make_trade_dict(ts=1000),  # before start - excluded
        make_trade_dict(ts=2000),  # in range - included
        make_trade_dict(ts=3000),  # after end - excluded
    ]
    mock_client.get = AsyncMock(return_value=make_mock_response({"trades": raw, "cursor": ""}))
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    start = pd.Timestamp(1500, unit="s", tz="UTC")  # ts=1000 is excluded
    end = pd.Timestamp(2000, unit="s", tz="UTC")  # ts=2000 is included

    ticks = await loader.load_trades(start=start, end=end)

    assert len(ticks) == 1
    ts_seconds = [t.ts_event // 1_000_000_000 for t in ticks]
    assert all(1500 <= ts <= 2000 for ts in ts_seconds)


@pytest.mark.asyncio
async def test_load_trades_sorted_chronologically():
    instrument = make_instrument()
    mock_client = MagicMock()
    raw = [
        make_trade_dict_v2(created_time="2026-03-05T19:39:37Z", trade_id="trade-3"),
        make_trade_dict_v2(created_time="2026-03-05T19:39:35Z", trade_id="trade-1"),
        make_trade_dict_v2(created_time="2026-03-05T19:39:36Z", trade_id="trade-2"),
    ]
    mock_client.get = AsyncMock(return_value=make_mock_response({"trades": raw, "cursor": ""}))
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    ticks = await loader.load_trades()

    ts_values = [t.ts_event for t in ticks]
    assert ts_values == sorted(ts_values)


@pytest.mark.asyncio
async def test_fetch_candlesticks_returns_raw_list():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"candlesticks": [make_candle_dict()]})
    )
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    candles = await loader.fetch_candlesticks(start_ts=1699999000, end_ts=1700000100)

    assert len(candles) == 1
    assert candles[0]["end_period_ts"] == 1700000060
    call_kwargs = mock_client.get.call_args
    # Verify live candlesticks endpoint with series_ticker in path
    assert "/series/KXBTC/markets/KXBTC-25MAR15-B100000/candlesticks" in call_kwargs.kwargs["url"]
    # Verify period_interval param sent as "1" (Minutes1 default)
    assert call_kwargs.kwargs["params"]["period_interval"] == "1"


@pytest.mark.asyncio
async def test_fetch_candlesticks_hours_interval():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_mock_response({"candlesticks": []}))
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    await loader.fetch_candlesticks(start_ts=0, end_ts=1, interval="Hours1")

    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["params"]["period_interval"] == "60"


@pytest.mark.asyncio
async def test_fetch_candlesticks_invalid_interval_raises():
    instrument = make_instrument()
    mock_client = MagicMock()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    with pytest.raises(ValueError, match="Invalid interval"):
        await loader.fetch_candlesticks(interval="Ticks1")


def test_parse_candlesticks_skips_empty_candles():
    """Candles with None OHLC (no trades in that period) must be skipped."""
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    empty_candle = {
        "end_period_ts": 1700000060,
        "price": {"open": None, "high": None, "low": None, "close": None, "mean": None},
        "volume": "0.00",
        "open_interest": "500.00",
    }
    raw = [empty_candle, make_candle_dict(end_ts=1700000120)]
    bars = loader.parse_candlesticks(raw, interval="Minutes1")

    assert len(bars) == 1
    assert bars[0].ts_event == 1700000120 * 1_000_000_000


def test_parse_candlesticks_returns_bars():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    raw = [make_candle_dict(end_ts=1700000060)]
    bars = loader.parse_candlesticks(raw, interval="Minutes1")

    assert len(bars) == 1
    assert isinstance(bars[0], Bar)
    assert bars[0].ts_event == 1700000060 * 1_000_000_000
    # price.open = "0.42" → price with 4 decimals
    assert str(bars[0].open) == "0.4200"
    assert str(bars[0].close) == "0.4300"
    assert str(bars[0].volume) == "100.00"


def test_parse_candlesticks_invalid_interval_raises():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=MagicMock())

    with pytest.raises(ValueError, match="Invalid interval"):
        loader.parse_candlesticks([], interval="Ticks1")


@pytest.mark.asyncio
async def test_load_bars_returns_sorted_bars():
    instrument = make_instrument()
    mock_client = MagicMock()
    candles = [
        make_candle_dict(end_ts=3000),
        make_candle_dict(end_ts=1000),
        make_candle_dict(end_ts=2000),
    ]
    mock_client.get = AsyncMock(return_value=make_mock_response({"candlesticks": candles}))
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    bars = await loader.load_bars()

    ts_values = [b.ts_event for b in bars]
    assert ts_values == sorted(ts_values)
    assert len(bars) == 3


@pytest.mark.asyncio
async def test_load_bars_passes_time_range_and_interval():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_mock_response({"candlesticks": []}))
    loader = KalshiDataLoader(instrument=instrument, series_ticker="KXBTC", http_client=mock_client)

    start = pd.Timestamp("2024-01-01", tz="UTC")
    end = pd.Timestamp("2024-01-31", tz="UTC")
    await loader.load_bars(start=start, end=end, interval="Hours1")

    call_kwargs = mock_client.get.call_args
    params = call_kwargs.kwargs["params"]
    assert params["start_ts"] == str(int(start.timestamp()))
    assert params["end_ts"] == str(int(end.timestamp()))
    assert params["period_interval"] == "60"
