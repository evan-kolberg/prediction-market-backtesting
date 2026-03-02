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
"""
Provides a data loader for historical Kalshi prediction market data.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pandas as pd

from nautilus_trader.adapters.kalshi.providers import KALSHI_REST_BASE
from nautilus_trader.adapters.kalshi.providers import _market_dict_to_instrument
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.instruments import BinaryOption


KALSHI_HTTP_RATE_LIMIT_RPS = 20  # Basic tier


class KalshiDataLoader:
    """
    Provides a data loader for historical Kalshi prediction market data.

    This loader fetches data from the Kalshi REST API:
    - ``GET /markets/{ticker}`` — instrument discovery
    - ``GET /historical/markets/{ticker}/trades`` — historical trades (cursor-paginated)
    - ``GET /series/{series_ticker}/markets/{ticker}/candlesticks`` — OHLCV bars

    If no ``http_client`` is provided, the loader creates one with a default
    rate limit of 20 requests per second (Kalshi Basic tier).

    Parameters
    ----------
    instrument : BinaryOption
        The binary option instrument to load data for.
    series_ticker : str
        The Kalshi series ticker for the instrument, e.g. ``"KXBTC"``.
        Required for the candlesticks endpoint path.
    http_client : nautilus_pyo3.HttpClient, optional
        HTTP client to use for requests. If not provided, a new client is created.
    """

    _INTERVAL_MAP: dict[str, int] = {
        "Minutes1": 1,
        "Hours1": 60,
        "Days1": 1440,
    }

    _INTERVAL_TO_AGGREGATION: dict[str, BarAggregation] = {
        "Minutes1": BarAggregation.MINUTE,
        "Hours1": BarAggregation.HOUR,
        "Days1": BarAggregation.DAY,
    }

    def __init__(
        self,
        instrument: BinaryOption,
        series_ticker: str,
        http_client: nautilus_pyo3.HttpClient | None = None,
    ) -> None:
        self._instrument = instrument
        self._series_ticker = series_ticker
        self._http_client = http_client or self._create_http_client()

    @staticmethod
    def _create_http_client() -> nautilus_pyo3.HttpClient:
        return nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_second(KALSHI_HTTP_RATE_LIMIT_RPS),
        )

    @property
    def instrument(self) -> BinaryOption:
        """Return the instrument for this loader."""
        return self._instrument

    @classmethod
    async def from_market_ticker(
        cls,
        ticker: str,
        http_client: nautilus_pyo3.HttpClient | None = None,
    ) -> KalshiDataLoader:
        """
        Create a loader by fetching market data for the given ticker.

        Parameters
        ----------
        ticker : str
            The Kalshi market ticker, e.g. ``"KXBTC-25MAR15-B100000"``.
        http_client : nautilus_pyo3.HttpClient, optional
            HTTP client to use. If not provided, a new client is created.

        Returns
        -------
        KalshiDataLoader

        Raises
        ------
        ValueError
            If the market ticker is not found.
        RuntimeError
            If the HTTP request fails.
        """
        client = http_client or cls._create_http_client()
        response = await client.get(url=f"{KALSHI_REST_BASE}/markets/{ticker}")

        if response.status == 404:
            raise ValueError(f"Market ticker '{ticker}' not found")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )

        data = msgspec.json.decode(response.body)
        market = data["market"]
        instrument = _market_dict_to_instrument(market)

        event_ticker = market["event_ticker"]
        event_response = await client.get(url=f"{KALSHI_REST_BASE}/events/{event_ticker}")
        if event_response.status != 200:
            raise RuntimeError(
                f"Failed to fetch event '{event_ticker}': "
                f"HTTP {event_response.status}: {event_response.body.decode('utf-8')}",
            )
        event_data = msgspec.json.decode(event_response.body)
        series_ticker = event_data["event"]["series_ticker"]

        return cls(instrument=instrument, series_ticker=series_ticker, http_client=client)

    async def fetch_trades(
        self,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Fetch historical trades from the Kalshi API.

        Automatically paginates using cursor-based pagination until all
        trades are retrieved.

        Parameters
        ----------
        min_ts : int, optional
            Minimum Unix timestamp in seconds (inclusive).
        max_ts : int, optional
            Maximum Unix timestamp in seconds (inclusive).
        limit : int, default 1000
            Number of trades per page (Kalshi maximum is 1000).

        Returns
        -------
        list[dict[str, Any]]
            Raw trade dicts as returned by the Kalshi API.
        """
        ticker = self._instrument.id.symbol.value
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"limit": str(limit)}
            if min_ts is not None:
                params["min_ts"] = str(min_ts)
            if max_ts is not None:
                params["max_ts"] = str(max_ts)
            if cursor:
                params["cursor"] = cursor

            response = await self._http_client.get(
                url=f"{KALSHI_REST_BASE}/historical/markets/{ticker}/trades",
                params=params,
            )

            if response.status != 200:
                raise RuntimeError(
                    f"HTTP request failed with status {response.status}: "
                    f"{response.body.decode('utf-8')}",
                )

            data = msgspec.json.decode(response.body)
            page_trades = data.get("trades", [])
            all_trades.extend(page_trades)

            cursor = data.get("cursor") or None
            if not cursor or not page_trades:
                break

        return all_trades

    async def fetch_candlesticks(
        self,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str = "Minutes1",
    ) -> list[dict[str, Any]]:
        """
        Fetch historical OHLCV candlesticks from the Kalshi API.

        Parameters
        ----------
        start_ts : int, optional
            Start Unix timestamp in seconds.
        end_ts : int, optional
            End Unix timestamp in seconds.
        interval : str, default "Minutes1"
            Candlestick interval. One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[dict[str, Any]]
            Raw candlestick dicts as returned by the Kalshi API.

        Raises
        ------
        ValueError
            If ``interval`` is not a recognized value.
        RuntimeError
            If the HTTP request fails.
        """
        if interval not in self._INTERVAL_MAP:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of: {list(self._INTERVAL_MAP.keys())}",
            )

        ticker = self._instrument.id.symbol.value
        params: dict[str, Any] = {
            "period_interval": str(self._INTERVAL_MAP[interval]),
        }
        if start_ts is not None:
            params["start_ts"] = str(start_ts)
        if end_ts is not None:
            params["end_ts"] = str(end_ts)

        response = await self._http_client.get(
            url=f"{KALSHI_REST_BASE}/series/{self._series_ticker}/markets/{ticker}/candlesticks",
            params=params,
        )

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )

        data = msgspec.json.decode(response.body)
        return data.get("candlesticks", [])

    def parse_trades(
        self,
        trades_data: list[dict[str, Any]],
    ) -> list[TradeTick]:
        """
        Parse raw Kalshi trade dicts into TradeTick objects.

        Parameters
        ----------
        trades_data : list[dict[str, Any]]
            Raw trade dicts from the Kalshi historical trades API.

        Returns
        -------
        list[TradeTick]
        """
        ticker = self._instrument.id.symbol.value
        instrument_id = self._instrument.id
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        trades: list[TradeTick] = []

        for trade in trades_data:
            ts_event = secs_to_nanos(trade["ts"])
            taker_side = trade.get("taker_side", "")
            if taker_side == "yes":
                aggressor_side = AggressorSide.BUYER
            elif taker_side == "no":
                aggressor_side = AggressorSide.SELLER
            else:
                aggressor_side = AggressorSide.NO_AGGRESSOR

            raw_id = f"{ticker}_{trade['ts']}_{trade['yes_price']}_{trade['count']}"
            trade_id = TradeId(raw_id[:36])

            trades.append(
                TradeTick(
                    instrument_id=instrument_id,
                    price=make_price(trade["yes_price"]),
                    size=make_qty(trade["count"]),
                    aggressor_side=aggressor_side,
                    trade_id=trade_id,
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        return trades

    def parse_candlesticks(
        self,
        candlesticks_data: list[dict[str, Any]],
        interval: str = "Minutes1",
    ) -> list[Bar]:
        """
        Parse raw Kalshi candlestick dicts into Bar objects.

        Parameters
        ----------
        candlesticks_data : list[dict[str, Any]]
            Raw candlestick dicts from the Kalshi API.
        interval : str, default "Minutes1"
            The candlestick interval. One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[Bar]

        Raises
        ------
        ValueError
            If ``interval`` is not a recognized value.
        """
        if interval not in self._INTERVAL_TO_AGGREGATION:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of: "
                f"{list(self._INTERVAL_TO_AGGREGATION.keys())}",
            )

        aggregation = self._INTERVAL_TO_AGGREGATION[interval]
        bar_spec = BarSpecification(step=1, aggregation=aggregation, price_type=PriceType.LAST)
        bar_type = BarType(
            instrument_id=self._instrument.id,
            bar_spec=bar_spec,
            aggregation_source=AggregationSource.EXTERNAL,
        )
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        bars: list[Bar] = []

        for candle in candlesticks_data:
            price = candle["price"]
            # Skip candles with no trades (OHLC values are None for empty periods)
            if price["open"] is None:
                continue
            ts_event = secs_to_nanos(candle["end_period_ts"])
            bars.append(
                Bar(
                    bar_type=bar_type,
                    open=make_price(price["open"]),
                    high=make_price(price["high"]),
                    low=make_price(price["low"]),
                    close=make_price(price["close"]),
                    volume=make_qty(candle["volume"]),
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        return bars

    async def load_bars(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        interval: str = "Minutes1",
    ) -> list[Bar]:
        """
        Load, parse, and sort bars (OHLCV candlesticks).

        Parameters
        ----------
        start : pd.Timestamp, optional
            Inclusive start time (timezone-aware).
        end : pd.Timestamp, optional
            Inclusive end time (timezone-aware).
        interval : str, default "Minutes1"
            Candlestick interval. One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[Bar]
            Bars sorted chronologically.
        """
        start_ts = int(start.timestamp()) if start is not None else None
        end_ts = int(end.timestamp()) if end is not None else None

        raw_candles = await self.fetch_candlesticks(
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
        )
        raw_candles.sort(key=lambda c: c["end_period_ts"])

        return self.parse_candlesticks(raw_candles, interval=interval)

    async def load_trades(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> list[TradeTick]:
        """
        Load, parse, and sort trade ticks.

        Fetches all historical trades for this instrument, optionally filtering
        by time range, then sorts chronologically.

        Parameters
        ----------
        start : pd.Timestamp, optional
            Inclusive lower bound (timezone-aware). If ``None``, no lower bound.
        end : pd.Timestamp, optional
            Inclusive upper bound (timezone-aware). If ``None``, no upper bound.

        Returns
        -------
        list[TradeTick]
            Trade ticks sorted chronologically.
        """
        min_ts = int(start.timestamp()) if start is not None else None
        max_ts = int(end.timestamp()) if end is not None else None

        raw_trades = await self.fetch_trades(min_ts=min_ts, max_ts=max_ts)

        # Client-side filter (API may return boundary-inclusive extras)
        if min_ts is not None:
            raw_trades = [t for t in raw_trades if t["ts"] >= min_ts]
        if max_ts is not None:
            raw_trades = [t for t in raw_trades if t["ts"] <= max_ts]

        raw_trades.sort(key=lambda t: t["ts"])

        return self.parse_trades(raw_trades)
