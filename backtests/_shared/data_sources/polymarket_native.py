from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Sequence

import msgspec

from nautilus_trader.adapters.polymarket.loaders import PolymarketDataLoader

from backtests._shared.data_sources._common import env_value
from backtests._shared.data_sources._common import is_disabled
from backtests._shared.data_sources._common import looks_like_local_path
from backtests._shared.data_sources._common import normalize_urlish
from backtests._shared.data_sources._common import trim_url_suffix


POLYMARKET_GAMMA_BASE_URL_ENV = "POLYMARKET_GAMMA_BASE_URL"
POLYMARKET_CLOB_BASE_URL_ENV = "POLYMARKET_CLOB_BASE_URL"
POLYMARKET_TRADE_API_BASE_URL_ENV = "POLYMARKET_TRADE_API_BASE_URL"


@dataclass(frozen=True)
class PolymarketNativeDataSourceSelection:
    summary: str


class RunnerPolymarketDataLoader(PolymarketDataLoader):
    @classmethod
    def _configured_gamma_base_url(cls) -> str | None:
        value = env_value(os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV))
        if is_disabled(value):
            return None
        if value is None:
            return None
        return trim_url_suffix(value, ("/markets", "/events"))

    @classmethod
    def _configured_clob_base_url(cls) -> str | None:
        value = env_value(os.getenv(POLYMARKET_CLOB_BASE_URL_ENV))
        if is_disabled(value):
            return None
        if value is None:
            return None
        return trim_url_suffix(value, ("/markets", "/fee-rate"))

    @classmethod
    def _configured_trade_api_base_url(cls) -> str | None:
        value = env_value(os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV))
        if is_disabled(value):
            return None
        if value is None:
            return None
        return trim_url_suffix(value, ("/trades",))

    @classmethod
    async def _fetch_market_by_slug(
        cls,
        slug: str,
        http_client,
    ) -> dict[str, Any]:
        gamma_base_url = cls._configured_gamma_base_url()
        if gamma_base_url is None:
            return await super()._fetch_market_by_slug(slug, http_client)

        response = await http_client.get(url=f"{gamma_base_url}/markets/slug/{slug}")
        if response.status == 404:
            raise ValueError(f"Market with slug '{slug}' not found")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}",
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
                f"Unexpected response type for slug '{slug}': {type(market).__name__}",
            )
        return market

    @classmethod
    async def _fetch_market_details(
        cls,
        condition_id: str,
        http_client,
    ) -> dict[str, Any]:
        clob_base_url = cls._configured_clob_base_url()
        if clob_base_url is None:
            return await super()._fetch_market_details(condition_id, http_client)

        response = await http_client.get(url=f"{clob_base_url}/markets/{condition_id}")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}",
            )
        return msgspec.json.decode(response.body)

    @classmethod
    async def _fetch_market_fee_rate_bps(
        cls,
        token_id: str,
        http_client,
    ):
        clob_base_url = cls._configured_clob_base_url()
        if clob_base_url is None:
            return await super()._fetch_market_fee_rate_bps(token_id, http_client)

        response = await http_client.get(
            url=f"{clob_base_url}/fee-rate",
            params={"token_id": token_id},
        )
        if response.status != 200:
            return None

        payload = msgspec.json.decode(response.body)
        if not isinstance(payload, dict):
            return None

        fee_rate_bps = cls._coerce_fee_rate_bps(payload.get("fee_rate_bps"))
        if fee_rate_bps is not None:
            return fee_rate_bps
        return cls._coerce_fee_rate_bps(payload.get("base_fee"))

    @classmethod
    async def _fetch_event_by_slug(
        cls,
        slug: str,
        http_client,
    ) -> dict[str, Any]:
        gamma_base_url = cls._configured_gamma_base_url()
        if gamma_base_url is None:
            return await super()._fetch_event_by_slug(slug, http_client)

        response = await http_client.get(
            url=f"{gamma_base_url}/events",
            params={"slug": slug},
        )
        if response.status == 404:
            raise ValueError(f"Event with slug '{slug}' not found")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}",
            )

        events = msgspec.json.decode(response.body)
        if not events:
            raise ValueError(f"Event with slug '{slug}' not found")
        return events[0]

    async def fetch_events(
        self,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        gamma_base_url = self._configured_gamma_base_url()
        if gamma_base_url is None:
            return await super().fetch_events(
                active=active,
                closed=closed,
                archived=archived,
                limit=limit,
                offset=offset,
            )

        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "archived": str(archived).lower(),
            "limit": str(limit),
            "offset": str(offset),
        }
        response = await self._http_client.get(
            url=f"{gamma_base_url}/events",
            params=params,
        )
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}",
            )
        return msgspec.json.decode(response.body)

    async def fetch_markets(
        self,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        gamma_base_url = self._configured_gamma_base_url()
        if gamma_base_url is None:
            return await super().fetch_markets(
                active=active,
                closed=closed,
                archived=archived,
                limit=limit,
                offset=offset,
            )

        params = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "archived": str(archived).lower(),
            "limit": str(limit),
            "offset": str(offset),
        }
        response = await self._http_client.get(
            url=f"{gamma_base_url}/markets",
            params=params,
        )
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}",
            )
        return msgspec.json.decode(response.body)

    async def fetch_trades(
        self,
        condition_id: str,
        limit: int = PolymarketDataLoader._TRADES_PAGE_LIMIT,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        trade_api_base_url = self._configured_trade_api_base_url()
        if trade_api_base_url is None:
            return await super().fetch_trades(
                condition_id=condition_id,
                limit=limit,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        all_trades: list[dict[str, Any]] = []
        offset = 0
        page_limit = min(limit, self._TRADES_PAGE_LIMIT)

        while True:
            response = await self._http_client.get(
                url=f"{trade_api_base_url}/trades",
                params={
                    "market": condition_id,
                    "limit": page_limit,
                    "offset": offset,
                },
            )
            if response.status != 200:
                body_text = response.body.decode("utf-8")
                if "max historical activity offset" in body_text:
                    raise RuntimeError(
                        "Polymarket public trades API hit its historical offset ceiling. "
                        "Use a lower-activity market or another historical data source. "
                        f"API response: {body_text}",
                    )
                raise RuntimeError(
                    f"HTTP request failed with status {response.status}: {body_text}",
                )

            data = msgspec.json.decode(response.body)
            if not data:
                break

            all_trades.extend(
                trade
                for trade in data
                if (end_ts is None or trade["timestamp"] <= end_ts)
                and (start_ts is None or trade["timestamp"] >= start_ts)
            )
            if (
                start_ts is not None
                and min(trade["timestamp"] for trade in data) < start_ts
            ):
                break

            offset += len(data)
            if len(data) < page_limit:
                break

        return all_trades


def _summary_from_overrides(
    *,
    gamma_base_url: str | None,
    clob_base_url: str | None,
    trade_api_base_url: str | None,
) -> str:
    parts: list[str] = []
    if gamma_base_url is not None:
        parts.append(f"gamma={gamma_base_url}")
    if trade_api_base_url is not None:
        parts.append(f"trades={trade_api_base_url}")
    if clob_base_url is not None:
        parts.append(f"clob={clob_base_url}")
    if not parts:
        return "Polymarket source: native public endpoints"
    return "Polymarket source: native (" + ", ".join(parts) + ")"


def _resolve_explicit_sources(
    sources: Sequence[str],
) -> tuple[PolymarketNativeDataSourceSelection, dict[str, str | None]]:
    gamma_base_url: str | None = None
    clob_base_url: str | None = None
    trade_api_base_url: str | None = None

    for raw_source in sources:
        if looks_like_local_path(raw_source):
            raise ValueError(
                "Native Polymarket trade-tick sources do not support local path inputs yet. "
                f"Received {raw_source!r}.",
            )

        normalized = normalize_urlish(raw_source)
        lowered = normalized.casefold()
        if (
            "gamma" in lowered
            or lowered.endswith("/events")
            or lowered.endswith("/markets")
        ):
            gamma_base_url = trim_url_suffix(normalized, ("/markets", "/events"))
            continue
        if "data-api" in lowered or lowered.endswith("/trades"):
            trade_api_base_url = trim_url_suffix(normalized, ("/trades",))
            continue
        if "clob" in lowered or lowered.endswith("/fee-rate") or "/markets/" in lowered:
            clob_base_url = trim_url_suffix(normalized, ("/markets", "/fee-rate"))
            continue
        gamma_base_url = normalized

    return (
        PolymarketNativeDataSourceSelection(
            summary=_summary_from_overrides(
                gamma_base_url=gamma_base_url,
                clob_base_url=clob_base_url,
                trade_api_base_url=trade_api_base_url,
            ),
        ),
        {
            POLYMARKET_GAMMA_BASE_URL_ENV: gamma_base_url,
            POLYMARKET_CLOB_BASE_URL_ENV: clob_base_url,
            POLYMARKET_TRADE_API_BASE_URL_ENV: trade_api_base_url,
        },
    )


def resolve_polymarket_native_data_source_selection(
    sources: Sequence[str] | None = None,
) -> tuple[PolymarketNativeDataSourceSelection, dict[str, str | None]]:
    if sources:
        return _resolve_explicit_sources(sources)

    gamma_base_url = RunnerPolymarketDataLoader._configured_gamma_base_url()
    clob_base_url = RunnerPolymarketDataLoader._configured_clob_base_url()
    trade_api_base_url = RunnerPolymarketDataLoader._configured_trade_api_base_url()
    return (
        PolymarketNativeDataSourceSelection(
            summary=_summary_from_overrides(
                gamma_base_url=gamma_base_url,
                clob_base_url=clob_base_url,
                trade_api_base_url=trade_api_base_url,
            ),
        ),
        {},
    )


@contextmanager
def configured_polymarket_native_data_source(
    *,
    sources: Sequence[str] | None = None,
) -> Iterator[PolymarketNativeDataSourceSelection]:
    selection, updates = resolve_polymarket_native_data_source_selection(
        sources=sources
    )
    originals = {name: os.environ.get(name) for name in updates}

    try:
        for name, value in updates.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        yield selection
    finally:
        for name, value in originals.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
