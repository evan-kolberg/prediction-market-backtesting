# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#
"""
Provides data loaders for historical Polymarket data from various APIs.
"""

from __future__ import annotations

import warnings
from decimal import Decimal, InvalidOperation
from typing import Any

import msgspec
import pandas as pd
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_HTTP_RATE_LIMIT
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.instruments import BinaryOption

from prediction_market_extensions._native import (
    polymarket_are_tradable_probability_prices,
    polymarket_normalize_trade_sides,
    polymarket_trade_event_timestamp_ns_batch,
    polymarket_trade_ids,
    polymarket_trade_sort_key,
    polymarket_trade_sort_keys,
)
from prediction_market_extensions._runtime_log import emit_loader_event
from prediction_market_extensions.adapters.polymarket.gamma_markets import infer_gamma_token_winners
from prediction_market_extensions.adapters.prediction_market.info_sanitization import (
    extract_resolution_metadata,
    sanitize_info_for_simulation,
)


def _trade_sort_key(trade: dict[str, Any]) -> tuple[int, str, str, str, str, str]:
    return polymarket_trade_sort_key(trade)


def _sort_trades_in_place(trades: list[dict[str, Any]]) -> None:
    sort_keys = polymarket_trade_sort_keys(trades)
    trades[:] = [
        trade
        for _sort_key, trade in sorted(
            zip(sort_keys, trades, strict=True), key=lambda item: item[0]
        )
    ]


class PolymarketDataLoader:
    """
    Provides a data loader for historical Polymarket market data.

    This loader fetches data from:
    - Polymarket Gamma API (market and event information)
    - Polymarket CLOB API (market details)
    - Polymarket Data API (historical trades)

    If no `http_client` is provided, the loader creates one with a default rate limit
    of 100 requests per minute, matching Polymarket's public endpoint limit.

    Parameters
    ----------
    instrument : BinaryOption
        The binary option instrument to load data for.
    token_id : str, optional
        The Polymarket token ID for this instrument.
    condition_id : str, optional
        The Polymarket condition ID for this instrument's market.
    http_client : nautilus_pyo3.HttpClient, optional
        The HTTP client to use for requests. If not provided, a new client will be created.

    """

    _TRADES_PAGE_LIMIT = 1_000
    _FEE_RATE_URL = "https://clob.polymarket.com/fee-rate"

    def __init__(
        self,
        instrument: BinaryOption,
        token_id: str | None = None,
        condition_id: str | None = None,
        http_client: nautilus_pyo3.HttpClient | None = None,
        resolution_metadata: dict[str, Any] | None = None,
    ) -> None:
        self._instrument = instrument
        self._token_id = token_id
        self._condition_id = condition_id
        self._http_client = http_client or self._create_http_client()
        self._resolution_metadata: dict[str, Any] = dict(resolution_metadata or {})

    @property
    def resolution_metadata(self) -> dict[str, Any]:
        """Resolution-bearing fields stripped from `instrument.info`.

        Strategies must not see resolution data during simulation, so it lives
        on the loader instead. Replay adapters and analytics read this to
        populate Brier scoring and settlement PnL.
        """
        return dict(self._resolution_metadata)

    @staticmethod
    def _create_http_client() -> nautilus_pyo3.HttpClient:
        return nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_minute(POLYMARKET_HTTP_RATE_LIMIT)
        )

    @staticmethod
    async def _fetch_market_by_slug(
        slug: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        PyCondition.valid_string(slug, "slug")

        emit_loader_event(
            f"Fetching Polymarket Gamma market slug={slug}",
            stage="discover",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/markets/slug",
            market_slug=slug,
        )
        response = await http_client.get(
            url=f"https://gamma-api.polymarket.com/markets/slug/{slug}"
        )

        if response.status == 404:
            raise ValueError(f"Market with slug '{slug}' not found")

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        data = msgspec.json.decode(response.body)

        if isinstance(data, list):
            if not data:
                raise ValueError(f"Market with slug '{slug}' not found")
            market = data[0]
        else:
            market = data

        if not isinstance(market, dict):
            raise RuntimeError(
                f"Unexpected response type for slug '{slug}': {type(market).__name__}"
            )

        emit_loader_event(
            f"Loaded Polymarket Gamma market slug={slug}",
            stage="discover",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/markets/slug",
            market_slug=slug,
        )
        return market

    @staticmethod
    async def _fetch_market_details(
        condition_id: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        PyCondition.valid_string(condition_id, "condition_id")

        emit_loader_event(
            f"Fetching Polymarket CLOB market details condition_id={condition_id}",
            stage="discover",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            source_kind="remote",
            source="https://clob.polymarket.com/markets",
            condition_id=condition_id,
        )
        response = await http_client.get(url=f"https://clob.polymarket.com/markets/{condition_id}")

        if response.status != 200:
            emit_loader_event(
                "Polymarket CLOB market details request failed "
                f"condition_id={condition_id} status={response.status}",
                level="WARNING",
                stage="discover",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                source_kind="remote",
                source="https://clob.polymarket.com/markets",
                condition_id=condition_id,
            )
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        market_details = msgspec.json.decode(response.body)
        emit_loader_event(
            f"Loaded Polymarket CLOB market details condition_id={condition_id}",
            stage="discover",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            source_kind="remote",
            source="https://clob.polymarket.com/markets",
            condition_id=condition_id,
        )
        return market_details

    @staticmethod
    def _coerce_fee_rate_bps(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None

        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    @classmethod
    async def _fetch_market_fee_rate_bps(
        cls, token_id: str, http_client: nautilus_pyo3.HttpClient
    ) -> Decimal | None:
        PyCondition.valid_string(token_id, "token_id")

        emit_loader_event(
            f"Fetching Polymarket CLOB fee rate token_id={token_id}",
            stage="fetch",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            source_kind="remote",
            source=cls._FEE_RATE_URL,
            token_id=token_id,
        )
        response = await http_client.get(url=cls._FEE_RATE_URL, params={"token_id": token_id})
        if response.status != 200:
            emit_loader_event(
                f"Polymarket CLOB fee-rate request failed token_id={token_id} "
                f"status={response.status}",
                level="WARNING",
                stage="fetch",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                source_kind="remote",
                source=cls._FEE_RATE_URL,
                token_id=token_id,
            )
            return None

        payload = msgspec.json.decode(response.body)
        if not isinstance(payload, dict):
            return None

        fee_rate_bps = cls._coerce_fee_rate_bps(payload.get("fee_rate_bps"))
        if fee_rate_bps is not None:
            return fee_rate_bps

        return cls._coerce_fee_rate_bps(payload.get("base_fee"))

    @classmethod
    async def _enrich_market_details_with_fee_rate(
        cls, market_details: dict[str, Any], token_id: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        existing_maker_fee = cls._coerce_fee_rate_bps(market_details.get("maker_base_fee"))
        existing_taker_fee = cls._coerce_fee_rate_bps(market_details.get("taker_base_fee"))
        if (existing_maker_fee is not None and existing_maker_fee > 0) or (
            existing_taker_fee is not None and existing_taker_fee > 0
        ):
            return market_details

        fee_rate_bps = await cls._fetch_market_fee_rate_bps(token_id, http_client)
        if fee_rate_bps is None:
            return market_details

        enriched = dict(market_details)
        enriched["maker_base_fee"] = "0"
        enriched["taker_base_fee"] = str(fee_rate_bps)
        return enriched

    @staticmethod
    async def _fetch_event_by_slug(
        slug: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        PyCondition.valid_string(slug, "slug")

        response = await http_client.get(
            url="https://gamma-api.polymarket.com/events", params={"slug": slug}
        )

        if response.status == 404:
            raise ValueError(f"Event with slug '{slug}' not found")

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        events = msgspec.json.decode(response.body)

        if not events:
            raise ValueError(f"Event with slug '{slug}' not found")

        return events[0]

    @classmethod
    async def from_market_slug(
        cls, slug: str, token_index: int = 0, http_client: nautilus_pyo3.HttpClient | None = None
    ) -> PolymarketDataLoader:
        """
        Create a loader by fetching market data from Polymarket APIs.

        Parameters
        ----------
        slug : str
            The market slug to search for.
        token_index : int, default 0
            The index of the token to use (0 for first outcome, 1 for second).
        http_client : nautilus_pyo3.HttpClient, optional
            The HTTP client to use for requests. If not provided, a new client will be created.

        Returns
        -------
        PolymarketDataLoader

        Raises
        ------
        ValueError
            If market with slug is not found or has no tokens.
        RuntimeError
            If HTTP requests fail.

        """
        client = http_client or cls._create_http_client()
        market = await cls._fetch_market_by_slug(slug, client)
        condition_id = market["conditionId"]
        market_details = await cls._fetch_market_details(condition_id, client)
        tokens = market_details.get("tokens", [])
        winner_lookup, is_50_50_outcome = infer_gamma_token_winners(market)

        if not tokens:
            raise ValueError(f"No tokens found for market: {condition_id}")

        if token_index >= len(tokens):
            raise ValueError(
                f"Token index {token_index} out of range (market has {len(tokens)} tokens)"
            )

        token = tokens[token_index]
        token_id = token["token_id"]
        outcome = token["outcome"]

        for market_token in tokens:
            token_outcome = str(market_token.get("outcome") or "").strip().casefold()
            if token_outcome in winner_lookup:
                market_token["winner"] = winner_lookup[token_outcome]

        market_details["tokens"] = tokens
        market_details["market_slug"] = market.get("slug") or market_details.get("market_slug")
        market_details["question"] = market.get("question") or market_details.get("question")
        market_details["description"] = market.get("description") or market_details.get(
            "description"
        )
        market_details["closed"] = market.get("closed", market_details.get("closed"))
        market_details["closedTime"] = market.get("closedTime") or market_details.get("closedTime")
        market_details["uma_resolution_status"] = market.get(
            "umaResolutionStatus"
        ) or market_details.get("uma_resolution_status")
        if is_50_50_outcome:
            market_details["is_50_50_outcome"] = True
        market_details = await cls._enrich_market_details_with_fee_rate(
            market_details, token_id, client
        )

        resolution_metadata = extract_resolution_metadata(market_details)
        instrument = parse_polymarket_instrument(
            market_info=sanitize_info_for_simulation(market_details),
            token_id=token_id,
            outcome=outcome,
        )

        return cls(
            instrument=instrument,
            token_id=token_id,
            condition_id=condition_id,
            http_client=client,
            resolution_metadata=resolution_metadata,
        )

    @classmethod
    async def from_event_slug(
        cls, slug: str, token_index: int = 0, http_client: nautilus_pyo3.HttpClient | None = None
    ) -> list[PolymarketDataLoader]:
        """
        Create loaders for all markets in an event.

        This is useful for events that contain multiple related markets,
        such as temperature bucket markets where each bucket is a separate market.

        Parameters
        ----------
        slug : str
            The event slug to fetch.
        token_index : int, default 0
            The index of the token to use (0 for first outcome, 1 for second).
        http_client : nautilus_pyo3.HttpClient, optional
            The HTTP client to use for requests. If not provided, a new client will be created.

        Returns
        -------
        list[PolymarketDataLoader]
            List of loaders, one for each market in the event.

        Raises
        ------
        ValueError
            If event with slug is not found, has no markets, or token_index is out of range.

        """
        client = http_client or cls._create_http_client()
        event = await cls._fetch_event_by_slug(slug, client)
        markets = event.get("markets", [])

        if not markets:
            raise ValueError(f"No markets found in event '{slug}'")

        loaders: list[PolymarketDataLoader] = []

        for market in markets:
            condition_id = market.get("conditionId")
            if not condition_id:
                continue

            market_details = await cls._fetch_market_details(condition_id, client)

            tokens = market_details.get("tokens", [])
            if not tokens:
                continue

            if token_index >= len(tokens):
                raise ValueError(
                    f"Token index {token_index} out of range (market {condition_id} has {len(tokens)} tokens)"
                )

            token = tokens[token_index]
            token_id = token["token_id"]
            outcome = token["outcome"]
            market_details = await cls._enrich_market_details_with_fee_rate(
                market_details, token_id, client
            )

            resolution_metadata = extract_resolution_metadata(market_details)
            instrument = parse_polymarket_instrument(
                market_info=sanitize_info_for_simulation(market_details),
                token_id=token_id,
                outcome=outcome,
            )

            loaders.append(
                cls(
                    instrument=instrument,
                    token_id=token_id,
                    condition_id=condition_id,
                    http_client=client,
                    resolution_metadata=resolution_metadata,
                )
            )

        return loaders

    @staticmethod
    async def query_market_by_slug(
        slug: str, http_client: nautilus_pyo3.HttpClient | None = None
    ) -> dict[str, Any]:
        """
        Query market data by slug without requiring a loader instance.

        Parameters
        ----------
        slug : str
            The market slug to fetch.
        http_client : nautilus_pyo3.HttpClient, optional
            The HTTP client to use for the request.

        Returns
        -------
        dict[str, Any]
            Market data dictionary.

        Raises
        ------
        ValueError
            If market with the given slug is not found.
        RuntimeError
            If HTTP request fails.

        """
        client = http_client or PolymarketDataLoader._create_http_client()
        return await PolymarketDataLoader._fetch_market_by_slug(slug, client)

    @staticmethod
    async def query_market_details(
        condition_id: str, http_client: nautilus_pyo3.HttpClient | None = None
    ) -> dict[str, Any]:
        """
        Query detailed market information without requiring a loader instance.

        Parameters
        ----------
        condition_id : str
            The market condition ID.
        http_client : nautilus_pyo3.HttpClient, optional
            The HTTP client to use for the request.

        Returns
        -------
        dict[str, Any]
            Detailed market information.

        Raises
        ------
        RuntimeError
            If HTTP request fails.

        """
        client = http_client or PolymarketDataLoader._create_http_client()
        return await PolymarketDataLoader._fetch_market_details(condition_id, client)

    @staticmethod
    async def query_event_by_slug(
        slug: str, http_client: nautilus_pyo3.HttpClient | None = None
    ) -> dict[str, Any]:
        """
        Query event data by slug without requiring a loader instance.

        Parameters
        ----------
        slug : str
            The event slug to fetch.
        http_client : nautilus_pyo3.HttpClient, optional
            The HTTP client to use for the request.

        Returns
        -------
        dict[str, Any]
            Event data dictionary containing 'markets' array and event metadata.

        Raises
        ------
        ValueError
            If event with the given slug is not found.
        RuntimeError
            If HTTP request fails.

        """
        client = http_client or PolymarketDataLoader._create_http_client()
        return await PolymarketDataLoader._fetch_event_by_slug(slug, client)

    @property
    def instrument(self) -> BinaryOption:
        """
        Return the instrument for this loader.
        """
        return self._instrument

    @property
    def token_id(self) -> str | None:
        """
        Return the token ID for this loader.
        """
        return self._token_id

    @property
    def condition_id(self) -> str | None:
        """
        Return the condition ID for this loader.
        """
        return self._condition_id

    async def load_trades(
        self, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None
    ) -> list[TradeTick]:
        """
        Load trade ticks from the Polymarket Data API.

        This is a convenience method that fetches and parses historical trades
        using the loader's stored condition_id and token_id.

        Parameters
        ----------
        start : pd.Timestamp, optional
            Start time filter. If ``None``, no lower bound.
        end : pd.Timestamp, optional
            End time filter. If ``None``, no upper bound.

        Returns
        -------
        list[TradeTick]
            Parsed trade ticks sorted chronologically, ready for backtesting.

        Raises
        ------
        ValueError
            If condition_id or token_id was not provided during initialization.

        """
        if self._condition_id is None:
            raise ValueError(
                "condition_id is required for this method. "
                "Use from_market_slug() to create a loader with condition_id, "
                "or pass condition_id to __init__()"
            )
        if self._token_id is None:
            raise ValueError(
                "token_id is required for this method. "
                "Use from_market_slug() to create a loader with token_id, "
                "or pass token_id to __init__()"
            )

        start_ts = int(start.timestamp()) if start is not None else None
        end_ts = int(end.timestamp()) if end is not None else None

        raw_trades = await self.fetch_trades(
            condition_id=self._condition_id, start_ts=start_ts, end_ts=end_ts
        )

        # Filter by token_id (API returns trades for all tokens in the condition)
        token_trades = [t for t in raw_trades if t["asset"] == self._token_id]

        if start_ts is not None:
            token_trades = [t for t in token_trades if t["timestamp"] >= start_ts]
        if end_ts is not None:
            token_trades = [t for t in token_trades if t["timestamp"] <= end_ts]

        _sort_trades_in_place(token_trades)

        return self.parse_trades(token_trades)

    async def fetch_event_by_slug(self, slug: str) -> dict[str, Any]:
        """
        Fetch an event by slug from the Polymarket Gamma API.

        Events contain multiple markets (e.g., temperature bucket markets
        are grouped under a single event like "highest-temperature-in-nyc-on-january-26").

        Parameters
        ----------
        slug : str
            The event slug to fetch.

        Returns
        -------
        dict[str, Any]
            Event data dictionary containing 'markets' array and event metadata.

        Raises
        ------
        ValueError
            If event with the given slug is not found.
        RuntimeError
            If HTTP requests fail.

        """
        return await self._fetch_event_by_slug(slug, self._http_client)

    async def fetch_events(
        self,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Fetch events from Polymarket Gamma API.

        Parameters
        ----------
        active : bool, default True
            Filter for active events.
        closed : bool, default False
            Include closed events.
        archived : bool, default False
            Include archived events.
        limit : int, default 100
            Maximum number of events to return.
        offset : int, default 0
            Offset for pagination.

        Returns
        -------
        list[dict[str, Any]]
            List of event data dictionaries.

        """
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "archived": str(archived).lower(),
            "limit": str(limit),
            "offset": str(offset),
        }
        response = await self._http_client.get(
            url="https://gamma-api.polymarket.com/events", params=params
        )

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        return msgspec.json.decode(response.body)

    async def get_event_markets(self, slug: str) -> list[dict[str, Any]]:
        """
        Get all markets within an event by slug.

        This is a convenience method that fetches an event and extracts its markets.

        Parameters
        ----------
        slug : str
            The event slug to fetch markets from.

        Returns
        -------
        list[dict[str, Any]]
            List of market dictionaries within the event.

        Raises
        ------
        ValueError
            If event with the given slug is not found.

        """
        event = await self.fetch_event_by_slug(slug)
        return event.get("markets", [])

    async def fetch_markets(
        self,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Fetch markets from Polymarket Gamma API.

        Parameters
        ----------
        active : bool, default True
            Filter for active markets.
        closed : bool, default False
            Include closed markets.
        archived : bool, default False
            Include archived markets.
        limit : int, default 100
            Maximum number of markets to return.
        offset : int, default 0
            Offset for pagination.

        Returns
        -------
        list[dict]
            List of market data dictionaries.

        """
        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "archived": str(archived).lower(),
            "limit": str(limit),
            "offset": str(offset),
        }
        response = await self._http_client.get(
            url="https://gamma-api.polymarket.com/markets", params=params
        )

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        return msgspec.json.decode(response.body)

    async def fetch_market_by_slug(self, slug: str) -> dict[str, Any]:
        """
        Fetch a single market by slug using the Polymarket Gamma API slug endpoint.

        Parameters
        ----------
        slug : str
            The market slug to fetch.

        Returns
        -------
        dict[str, Any]
            Market data dictionary.

        Raises
        ------
        ValueError
            If market with the given slug is not found.
        RuntimeError
            If HTTP requests fail.

        """
        return await self._fetch_market_by_slug(slug, self._http_client)

    async def find_market_by_slug(self, slug: str) -> dict[str, Any]:
        """
        Find a specific market by slug.

        Parameters
        ----------
        slug : str
            The market slug to search for.

        Returns
        -------
        dict[str, Any]
            Market data dictionary.

        Raises
        ------
        ValueError
            If market with the given slug is not found.

        """
        return await self.fetch_market_by_slug(slug)

    async def fetch_market_details(self, condition_id: str) -> dict[str, Any]:
        """
        Fetch detailed market information from Polymarket CLOB API.

        Parameters
        ----------
        condition_id : str
            The market condition ID.

        Returns
        -------
        dict[str, Any]
            Detailed market information.

        """
        return await self._fetch_market_details(condition_id, self._http_client)

    async def fetch_trades(
        self,
        condition_id: str,
        limit: int = _TRADES_PAGE_LIMIT,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch trades from the Polymarket Data API.

        Parameters
        ----------
        condition_id : str
            The market condition ID.
        limit : int, default 1,000
            Number of trades per request. The public API currently caps this at 1,000.
        start_ts : int, optional
            Lower timestamp bound in seconds. Used for client-side filtering and
            to stop pagination once older pages fall outside the requested window.
        end_ts : int, optional
            Upper timestamp bound in seconds. Used for client-side filtering.

        Returns
        -------
        list[dict[str, Any]]
            List of trade dictionaries (newest first).

        Notes
        -----
        This method automatically handles pagination using offset-based requests.
        It keeps paging until the API returns fewer than the requested page size
        or an empty page. The public endpoint does not expose reliable time-bound
        pagination parameters, so bounded loads stop once the fetched pages become
        older than ``start_ts``.

        """
        PyCondition.valid_string(condition_id, "condition_id")

        all_trades: list[dict[str, Any]] = []
        offset = 0
        page_limit = min(limit, self._TRADES_PAGE_LIMIT)

        while True:
            params: dict[str, Any] = {"market": condition_id, "limit": page_limit, "offset": offset}

            emit_loader_event(
                "Fetching Polymarket public trades page "
                f"condition_id={condition_id} offset={offset} limit={page_limit}",
                stage="fetch",
                vendor="polymarket",
                status="start",
                platform="polymarket",
                data_type="book",
                source_kind="remote",
                source="https://data-api.polymarket.com/trades",
                condition_id=condition_id,
                attrs={"offset": offset, "limit": page_limit},
            )
            response = await self._http_client.get(
                url="https://data-api.polymarket.com/trades", params=params
            )

            if response.status != 200:
                body_text = response.body.decode("utf-8")
                if "max historical activity offset" in body_text:
                    emit_loader_event(
                        "Polymarket public trades API hit historical offset ceiling "
                        f"condition_id={condition_id} offset={offset}",
                        level="WARNING",
                        stage="fetch",
                        vendor="polymarket",
                        status="skip",
                        platform="polymarket",
                        data_type="book",
                        source_kind="remote",
                        source="https://data-api.polymarket.com/trades",
                        condition_id=condition_id,
                        attrs={"offset": offset},
                    )
                    warnings.warn(
                        "Polymarket public trades API hit its historical offset ceiling. "
                        "Returning the trades fetched before the ceiling; high-activity "
                        "markets may be incomplete. Use another historical data source "
                        f"for full coverage. API response: {body_text}",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    break
                raise RuntimeError(
                    f"HTTP request failed with status {response.status}: {body_text}"
                )

            data = msgspec.json.decode(response.body)
            emit_loader_event(
                "Loaded Polymarket public trades page "
                f"condition_id={condition_id} offset={offset} rows={len(data)}",
                stage="fetch",
                vendor="polymarket",
                status="complete",
                platform="polymarket",
                data_type="book",
                source_kind="remote",
                source="https://data-api.polymarket.com/trades",
                condition_id=condition_id,
                rows=len(data),
                attrs={"offset": offset, "limit": page_limit},
            )

            if not data:
                break

            all_trades.extend(
                trade
                for trade in data
                if (end_ts is None or trade["timestamp"] <= end_ts)
                and (start_ts is None or trade["timestamp"] >= start_ts)
            )

            # Do not early-terminate on page timestamps: the public API does
            # not guarantee a stable sort order across pages.

            offset += len(data)

            if len(data) < page_limit:
                break

        return all_trades

    def parse_trades(self, trades_data: list[dict]) -> list[TradeTick]:
        """
        Parse trade data into TradeTicks.

        Parameters
        ----------
        trades_data : list[dict]
            Raw trade data from the Polymarket Data API.

        Returns
        -------
        list[TradeTick]
            List of TradeTicks for backtesting.

        """
        if self._token_id is None:
            raise ValueError(
                "token_id is required to parse trades. "
                "Use from_market_slug() to create a loader with token_id, "
                "or pass token_id to __init__()"
            )

        candidate_trades: list[tuple[dict, int, int, int, str, str, int]] = []
        instrument_id = self._instrument.id
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        token_id = self._token_id

        timestamp_counts: dict[int, int] = {}
        tx_asset_counts: dict[tuple[str, str], int] = {}

        for i, trade_data in enumerate(trades_data):
            # Skip trades for other tokens in the same condition
            if trade_data.get("asset") != token_id:
                continue

            base_ts_event = secs_to_nanos(trade_data["timestamp"])
            occurrence_in_second = timestamp_counts.get(base_ts_event, 0)
            timestamp_counts[base_ts_event] = occurrence_in_second + 1
            # Multi-token Polymarket transactions produce multiple fills that
            # share the same transactionHash.  Using only the last 36 chars can
            # collide, causing NautilusTrader to silently drop the second trade.
            # Disambiguate by appending the token suffix and a same-transaction
            # sequence number for same-token multi-fill transactions.
            _transaction_hash = str(trade_data["transactionHash"])
            _asset = str(trade_data.get("asset", ""))
            _tx_asset_key = (_transaction_hash, _asset)
            _tx_asset_sequence = tx_asset_counts.get(_tx_asset_key, 0)
            tx_asset_counts[_tx_asset_key] = _tx_asset_sequence + 1

            candidate_trades.append(
                (
                    trade_data,
                    i,
                    base_ts_event,
                    occurrence_in_second,
                    _transaction_hash,
                    _asset,
                    _tx_asset_sequence,
                )
            )

        side_values = polymarket_normalize_trade_sides(
            [str(trade_data.get("side", "")) for trade_data, *_rest in candidate_trades]
        )
        price_is_tradable = polymarket_are_tradable_probability_prices(
            [str(trade_data["price"]) for trade_data, *_rest in candidate_trades]
        )
        event_timestamps = polymarket_trade_event_timestamp_ns_batch(
            [
                (base_ts_event, occurrence_in_second)
                for _trade_data, _index, base_ts_event, occurrence_in_second, *_rest in candidate_trades
            ]
        )

        parsed_trades: list[tuple[dict, AggressorSide, int, str, str, int]] = []
        for (
            trade_data,
            original_index,
            _base_ts_event,
            _occurrence_in_second,
            _transaction_hash,
            _asset,
            _tx_asset_sequence,
        ), side_value, is_tradable, ts_event in zip(
            candidate_trades, side_values, price_is_tradable, event_timestamps, strict=True
        ):
            if side_value == "BUY":
                aggressor_side = AggressorSide.BUYER
            elif side_value == "SELL":
                aggressor_side = AggressorSide.SELLER
            else:
                warnings.warn(
                    f"Polymarket trade {original_index} had unexpected side "
                    f"{trade_data.get('side')!r}; recording NO_AGGRESSOR for audit visibility.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                aggressor_side = AggressorSide.NO_AGGRESSOR

            if not is_tradable:
                _raw_price = float(trade_data["price"])
                warnings.warn(
                    "Skipping Polymarket trade with out-of-range or untradable price "
                    f"{_raw_price!r} at record {original_index}.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            parsed_trades.append(
                (
                    trade_data,
                    aggressor_side,
                    ts_event,
                    _transaction_hash,
                    _asset,
                    _tx_asset_sequence,
                )
            )

        trade_ids = polymarket_trade_ids(
            [
                (transaction_hash, asset, sequence)
                for _trade_data, _aggressor_side, _ts_event, transaction_hash, asset, sequence in parsed_trades
            ]
        )

        trades: list[TradeTick] = []
        for (
            trade_data,
            aggressor_side,
            ts_event,
            _transaction_hash,
            _asset,
            _tx_asset_sequence,
        ), trade_id in zip(parsed_trades, trade_ids, strict=True):
            trade = TradeTick(
                instrument_id=instrument_id,
                price=make_price(trade_data["price"]),
                size=make_qty(trade_data["size"]),
                aggressor_side=aggressor_side,
                trade_id=TradeId(trade_id),
                ts_event=ts_event,
                ts_init=ts_event,
            )
            trades.append(trade)

        return trades
