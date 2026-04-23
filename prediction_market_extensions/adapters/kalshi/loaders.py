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
"""
Provides a data loader for historical Kalshi prediction market data.
"""

from __future__ import annotations

import hashlib
from typing import Any, ClassVar
import warnings

import msgspec
import pandas as pd
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.model.data import Bar, BarSpecification, BarType, TradeTick
from nautilus_trader.model.enums import AggregationSource, AggressorSide, BarAggregation, PriceType
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.instruments import BinaryOption

from prediction_market_extensions.adapters.kalshi.providers import (
    KALSHI_REST_BASE,
    market_dict_to_instrument,
)

KALSHI_HTTP_RATE_LIMIT_RPS = 20  # Basic tier


class KalshiDataLoader:
    """
    Provides a data loader for historical Kalshi prediction market data.

    This loader fetches data from the Kalshi REST API:
    - ``GET /markets/{ticker}`` — instrument discovery
    - ``GET /markets/trades?ticker={ticker}`` — trades (cursor-paginated)
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

    _INTERVAL_MAP: ClassVar[dict[str, int]] = {"Minutes1": 1, "Hours1": 60, "Days1": 1440}

    _INTERVAL_TO_AGGREGATION: ClassVar[dict[str, BarAggregation]] = {
        "Minutes1": BarAggregation.MINUTE,
        "Hours1": BarAggregation.HOUR,
        "Days1": BarAggregation.DAY,
    }

    _TRADE_ENDPOINT = f"{KALSHI_REST_BASE}/markets/trades"
    _TRADE_PAGE_LIMIT = 1_000

    @staticmethod
    def _normalize_price(raw: float | str) -> float:
        """
        Normalize a Kalshi price to the 0-1 dollar range.

        The Kalshi API historically returned cent-scale integers (for example
        ``42`` for ``0.42``) and now also publishes dollar-scale decimals (for
        example ``"0.4200"`` or ``1.0``). This helper preserves decimal prices
        already in the ``0`` to ``1`` range while still converting cent-scale
        integers into dollar probabilities.
        """
        text = str(raw).strip()
        p = float(raw)

        has_decimal_marker = isinstance(raw, float) or "." in text or "e" in text.lower()
        if 0.0 <= p < 1.0 or (p == 1.0 and has_decimal_marker):
            normalized = p
        else:
            normalized = p / 100.0

        if not 0.0 <= normalized <= 1.0:
            raise ValueError(f"Kalshi price {raw!r} normalized outside [0, 1]: {normalized}")

        return normalized

    @staticmethod
    def _trade_timestamp_ns(trade: dict[str, Any]) -> int:
        created_time = trade.get("created_time")
        if created_time:
            return pd.Timestamp(created_time).value
        return secs_to_nanos(int(trade["ts"]))

    @classmethod
    def _trade_timestamp_seconds(cls, trade: dict[str, Any]) -> int:
        return cls._trade_timestamp_ns(trade) // 1_000_000_000

    @classmethod
    def _trade_sort_key(cls, trade: dict[str, Any]) -> tuple[int, str, str, str, str, str]:
        return (
            cls._trade_timestamp_ns(trade),
            str(trade.get("trade_id", "")),
            str(trade.get("created_time", trade.get("ts", ""))),
            str(trade.get("yes_price_dollars", trade.get("yes_price", trade.get("price", "")))),
            str(trade.get("count_fp", trade.get("count", ""))),
            str(trade.get("taker_side", "")),
        )

    @classmethod
    def _extract_yes_price(cls, trade: dict[str, Any]) -> float:
        if trade.get("yes_price_dollars") is not None:
            return float(trade["yes_price_dollars"])
        if trade.get("yes_price") is not None:
            return cls._normalize_price(trade["yes_price"])
        if trade.get("price") is not None:
            return cls._normalize_price(trade["price"])
        raise ValueError(f"Kalshi trade payload missing a yes-price field: {trade}")

    @staticmethod
    def _extract_quantity(
        payload: dict[str, Any], *, fp_key: str, raw_key: str
    ) -> str | int | float:
        if payload.get(fp_key) is not None:
            return payload[fp_key]
        return payload[raw_key]

    @staticmethod
    def _extract_candle_price(price_payload: dict[str, Any], field: str) -> float | None:
        value = price_payload.get(f"{field}_dollars")
        if value is not None:
            return float(value)

        raw_value = price_payload.get(field)
        if raw_value is None:
            return None

        return KalshiDataLoader._normalize_price(raw_value)

    @staticmethod
    def _fallback_trade_id(ticker: str, trade: dict[str, Any], occurrence: int) -> TradeId:
        raw_id = (
            f"{ticker}|{trade.get('created_time', trade.get('ts'))}|"
            f"{trade.get('yes_price_dollars', trade.get('yes_price', trade.get('price')))}|"
            f"{trade.get('count_fp', trade.get('count'))}|{trade.get('taker_side', '')}|{occurrence}"
        )
        return TradeId(hashlib.shake_256(raw_id.encode("utf-8")).hexdigest(18))

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
            default_quota=nautilus_pyo3.Quota.rate_per_second(KALSHI_HTTP_RATE_LIMIT_RPS)
        )

    @property
    def instrument(self) -> BinaryOption:
        """Return the instrument for this loader."""
        return self._instrument

    @classmethod
    async def from_market_ticker(
        cls, ticker: str, http_client: nautilus_pyo3.HttpClient | None = None
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
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        data = msgspec.json.decode(response.body)
        market = data["market"]
        instrument = market_dict_to_instrument(market)

        event_ticker = market["event_ticker"]
        event_response = await client.get(url=f"{KALSHI_REST_BASE}/events/{event_ticker}")
        if event_response.status != 200:
            raise RuntimeError(
                f"Failed to fetch event '{event_ticker}': "
                f"HTTP {event_response.status}: {event_response.body.decode('utf-8')}"
            )
        event_data = msgspec.json.decode(event_response.body)
        series_ticker = event_data["event"]["series_ticker"]

        return cls(
            instrument=instrument,
            series_ticker=series_ticker,
            http_client=client,
        )

    async def fetch_trades(
        self, min_ts: int | None = None, max_ts: int | None = None, limit: int = 1000
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
            Number of trades per page (capped to Kalshi's public maximum of 1000).

        Returns
        -------
        list[dict[str, Any]]
            Raw trade dicts as returned by the Kalshi API.
        """
        ticker = self._instrument.id.symbol.value
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None
        page_limit = min(limit, self._TRADE_PAGE_LIMIT)

        while True:
            params: dict[str, Any] = {"ticker": ticker, "limit": str(page_limit)}
            if min_ts is not None:
                params["min_ts"] = str(min_ts)
            if max_ts is not None:
                params["max_ts"] = str(max_ts)
            if cursor:
                params["cursor"] = cursor

            response = await self._http_client.get(url=self._TRADE_ENDPOINT, params=params)

            if response.status != 200:
                raise RuntimeError(
                    f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
                )

            data = msgspec.json.decode(response.body)
            page_trades = data.get("trades", [])
            all_trades.extend(page_trades)

            cursor = data.get("cursor") or None
            if not cursor or not page_trades:
                break

        return all_trades

    async def fetch_candlesticks(
        self, start_ts: int | None = None, end_ts: int | None = None, interval: str = "Minutes1"
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
                f"Invalid interval '{interval}'. Must be one of: {list(self._INTERVAL_MAP.keys())}"
            )

        ticker = self._instrument.id.symbol.value
        params: dict[str, Any] = {"period_interval": str(self._INTERVAL_MAP[interval])}
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
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        data = msgspec.json.decode(response.body)
        return data.get("candlesticks", [])

    def parse_trades(self, trades_data: list[dict[str, Any]]) -> list[TradeTick]:
        """
        Parse raw Kalshi trade dicts into TradeTick objects.

        Parameters
        ----------
        trades_data : list[dict[str, Any]]
            Raw trade dicts from the Kalshi trades API.

        Returns
        -------
        list[TradeTick]
        """
        ticker = self._instrument.id.symbol.value
        instrument_id = self._instrument.id
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        trades: list[TradeTick] = []
        trade_counts: dict[tuple[object, object, object, object], int] = {}

        timestamp_counts: dict[int, int] = {}

        for trade in trades_data:
            base_ts_event = self._trade_timestamp_ns(trade)
            occurrence_at_timestamp = timestamp_counts.get(base_ts_event, 0)
            timestamp_counts[base_ts_event] = occurrence_at_timestamp + 1
            remaining_ns_in_second = 999_999_999 - (base_ts_event % 1_000_000_000)
            ts_event = base_ts_event + min(occurrence_at_timestamp, remaining_ns_in_second)
            taker_side = trade.get("taker_side", "")
            if taker_side == "yes":
                aggressor_side = AggressorSide.BUYER
            elif taker_side == "no":
                aggressor_side = AggressorSide.SELLER
            else:
                if taker_side not in {"", None}:
                    warnings.warn(
                        f"Kalshi trade had unexpected taker_side {taker_side!r}; "
                        "recording NO_AGGRESSOR for audit visibility.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                aggressor_side = AggressorSide.NO_AGGRESSOR

            key = (
                trade.get("created_time", trade.get("ts")),
                trade.get("yes_price_dollars", trade.get("yes_price", trade.get("price"))),
                trade.get("count_fp", trade.get("count")),
                taker_side,
            )
            occurrence = trade_counts.get(key, 0)
            trade_counts[key] = occurrence + 1

            raw_trade_id = trade.get("trade_id")
            if raw_trade_id:
                trade_id = TradeId(str(raw_trade_id))
            else:
                trade_id = self._fallback_trade_id(ticker, trade, occurrence)

            try:
                price = self._extract_yes_price(trade)
                size = self._extract_quantity(trade, fp_key="count_fp", raw_key="count")
            except (KeyError, ValueError) as exc:
                warnings.warn(
                    f"Skipping malformed Kalshi trade payload: {exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue

            trades.append(
                TradeTick(
                    instrument_id=instrument_id,
                    price=make_price(price),
                    size=make_qty(size),
                    aggressor_side=aggressor_side,
                    trade_id=trade_id,
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        return trades

    def parse_candlesticks(
        self, candlesticks_data: list[dict[str, Any]], interval: str = "Minutes1"
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
                f"Invalid interval '{interval}'. Must be one of: {list(self._INTERVAL_TO_AGGREGATION.keys())}"
            )

        aggregation = self._INTERVAL_TO_AGGREGATION[interval]
        bar_spec = BarSpecification(
            step=1,
            aggregation=aggregation,
            price_type=PriceType.LAST,
        )

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
            open_price = self._extract_candle_price(price, "open")
            high_price = self._extract_candle_price(price, "high")
            low_price = self._extract_candle_price(price, "low")
            close_price = self._extract_candle_price(price, "close")
            if open_price is None or high_price is None or low_price is None or close_price is None:
                continue
            ts_event = secs_to_nanos(candle["end_period_ts"])
            bars.append(
                Bar(
                    bar_type=bar_type,
                    open=make_price(open_price),
                    high=make_price(high_price),
                    low=make_price(low_price),
                    close=make_price(close_price),
                    volume=make_qty(
                        self._extract_quantity(candle, fp_key="volume_fp", raw_key="volume")
                    ),
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
            start_ts=start_ts, end_ts=end_ts, interval=interval
        )
        raw_candles.sort(key=lambda c: c["end_period_ts"])

        return self.parse_candlesticks(raw_candles, interval=interval)

    async def load_trades(
        self, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None
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
        min_ts_ns = pd.Timestamp(start).value if start is not None else None
        max_ts_ns = pd.Timestamp(end).value if end is not None else None

        raw_trades = await self.fetch_trades(min_ts=min_ts, max_ts=max_ts)

        # Client-side filter (API may return boundary-inclusive extras)
        if min_ts_ns is not None:
            raw_trades = [t for t in raw_trades if self._trade_timestamp_ns(t) >= min_ts_ns]
        if max_ts_ns is not None:
            raw_trades = [t for t in raw_trades if self._trade_timestamp_ns(t) <= max_ts_ns]

        raw_trades.sort(key=self._trade_sort_key)

        return self.parse_trades(raw_trades)
