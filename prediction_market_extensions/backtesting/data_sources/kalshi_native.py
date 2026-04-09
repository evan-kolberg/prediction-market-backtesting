from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Sequence

import msgspec

from prediction_market_extensions.adapters.kalshi.loaders import KalshiDataLoader
from prediction_market_extensions.adapters.kalshi.providers import (
    market_dict_to_instrument,
)

from prediction_market_extensions.backtesting.data_sources._common import env_value
from prediction_market_extensions.backtesting.data_sources._common import is_disabled
from prediction_market_extensions.backtesting.data_sources._common import (
    looks_like_local_path,
)
from prediction_market_extensions.backtesting.data_sources._common import (
    normalize_urlish,
)
from prediction_market_extensions.backtesting.data_sources._common import (
    trim_url_suffix,
)


KALSHI_REST_BASE_URL_ENV = "KALSHI_REST_BASE_URL"
_KALSHI_REST_SUFFIXES = ("/markets/trades", "/markets", "/events", "/series")


@dataclass(frozen=True)
class KalshiNativeDataSourceSelection:
    summary: str


@dataclass(frozen=True)
class KalshiNativeLoaderConfig:
    rest_base_url: str | None


_CURRENT_KALSHI_NATIVE_LOADER_CONFIG: ContextVar[KalshiNativeLoaderConfig | None] = (
    ContextVar("kalshi_native_loader_config", default=None)
)


def _current_loader_config() -> KalshiNativeLoaderConfig | None:
    return _CURRENT_KALSHI_NATIVE_LOADER_CONFIG.get()


class RunnerKalshiDataLoader(KalshiDataLoader):
    @classmethod
    def _configured_rest_base_url(cls) -> str:
        config = _current_loader_config()
        if config is not None:
            value = config.rest_base_url
        else:
            value = env_value(os.getenv(KALSHI_REST_BASE_URL_ENV))
        if value is None or is_disabled(value):
            raise ValueError(
                "Kalshi native data source requires an explicit REST base URL via "
                "DATA.sources or KALSHI_REST_BASE_URL."
            )
        return trim_url_suffix(value, _KALSHI_REST_SUFFIXES)

    @classmethod
    async def from_market_ticker(
        cls,
        ticker: str,
        http_client=None,
    ) -> RunnerKalshiDataLoader:
        client = http_client or cls._create_http_client()
        rest_base_url = cls._configured_rest_base_url()

        response = await client.get(url=f"{rest_base_url}/markets/{ticker}")
        if response.status == 404:
            raise ValueError(f"Market ticker '{ticker}' not found")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )

        data = msgspec.json.decode(response.body)
        market = data["market"]
        instrument = market_dict_to_instrument(market)

        event_ticker = market["event_ticker"]
        event_response = await client.get(url=f"{rest_base_url}/events/{event_ticker}")
        if event_response.status != 200:
            raise RuntimeError(
                f"Failed to fetch event '{event_ticker}': "
                f"HTTP {event_response.status}: {event_response.body.decode('utf-8')}",
            )

        event_data = msgspec.json.decode(event_response.body)
        series_ticker = event_data["event"]["series_ticker"]
        return cls(
            instrument=instrument, series_ticker=series_ticker, http_client=client
        )

    async def fetch_trades(
        self,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        ticker = self._instrument.id.symbol.value
        rest_base_url = self._configured_rest_base_url()
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None
        page_limit = min(limit, self._TRADE_PAGE_LIMIT)

        while True:
            params: dict[str, Any] = {
                "ticker": ticker,
                "limit": str(page_limit),
            }
            if min_ts is not None:
                params["min_ts"] = str(min_ts)
            if max_ts is not None:
                params["max_ts"] = str(max_ts)
            if cursor:
                params["cursor"] = cursor

            response = await self._http_client.get(
                url=f"{rest_base_url}/markets/trades",
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
        if interval not in self._INTERVAL_MAP:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of: {list(self._INTERVAL_MAP.keys())}",
            )

        ticker = self._instrument.id.symbol.value
        rest_base_url = self._configured_rest_base_url()
        params: dict[str, Any] = {
            "start_ts": str(start_ts) if start_ts is not None else None,
            "end_ts": str(end_ts) if end_ts is not None else None,
            "period_interval": self._INTERVAL_MAP[interval],
        }
        params = {key: value for key, value in params.items() if value is not None}

        response = await self._http_client.get(
            url=f"{rest_base_url}/series/{self._series_ticker}/markets/{ticker}/candlesticks",
            params=params,
        )
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )
        data = msgspec.json.decode(response.body)
        return data.get("candlesticks", [])


def _summary_from_rest_base_url(rest_base_url: str | None) -> str:
    if rest_base_url is None:
        raise ValueError(
            "Kalshi native data source requires an explicit REST base URL via "
            "DATA.sources or KALSHI_REST_BASE_URL."
        )
    return f"Kalshi source: native (rest:{rest_base_url})"


def _parse_named_source(raw_source: str) -> str | None:
    stripped = raw_source.strip()
    for separator in (":", "="):
        role, found, value = stripped.partition(separator)
        if not found:
            continue
        if role.strip().casefold() not in {"rest", "api"}:
            continue
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError(
                f"Kalshi source {raw_source!r} is missing a REST base URL value."
            )
        return normalized_value
    return None


def _resolve_explicit_sources(
    sources: Sequence[str],
) -> tuple[KalshiNativeDataSourceSelection, KalshiNativeLoaderConfig]:
    rest_base_url: str | None = None

    for raw_source in sources:
        candidate = _parse_named_source(raw_source) or raw_source
        if looks_like_local_path(candidate):
            raise ValueError(
                "Native Kalshi trade-tick sources do not support local path inputs yet. "
                f"Received {raw_source!r}.",
            )
        normalized = normalize_urlish(candidate)
        if rest_base_url is not None and normalized != rest_base_url:
            raise ValueError(
                "Kalshi explicit sources supports at most one REST base URL."
            )
        rest_base_url = trim_url_suffix(normalized, _KALSHI_REST_SUFFIXES)

    return (
        KalshiNativeDataSourceSelection(
            summary=(
                f"Kalshi source: native (rest:{rest_base_url})"
                if rest_base_url is not None
                else "Kalshi source: native public endpoint"
            ),
        ),
        KalshiNativeLoaderConfig(rest_base_url=rest_base_url),
    )


def resolve_kalshi_native_loader_config(
    sources: Sequence[str] | None = None,
) -> tuple[KalshiNativeDataSourceSelection, KalshiNativeLoaderConfig]:
    if sources:
        return _resolve_explicit_sources(sources)

    rest_base_url = env_value(os.getenv(KALSHI_REST_BASE_URL_ENV))
    if rest_base_url is not None and not is_disabled(rest_base_url):
        rest_base_url = trim_url_suffix(rest_base_url, _KALSHI_REST_SUFFIXES)
    else:
        rest_base_url = None

    return (
        KalshiNativeDataSourceSelection(
            summary=_summary_from_rest_base_url(rest_base_url),
        ),
        KalshiNativeLoaderConfig(rest_base_url=rest_base_url),
    )


def resolve_kalshi_native_data_source_selection(
    sources: Sequence[str] | None = None,
) -> tuple[KalshiNativeDataSourceSelection, dict[str, str | None]]:
    selection, config = resolve_kalshi_native_loader_config(sources=sources)
    if sources:
        return (
            selection,
            {
                KALSHI_REST_BASE_URL_ENV: config.rest_base_url,
            },
        )
    return selection, {}


@contextmanager
def configured_kalshi_native_data_source(
    *,
    sources: Sequence[str] | None = None,
) -> Iterator[KalshiNativeDataSourceSelection]:
    selection, config = resolve_kalshi_native_loader_config(sources=sources)
    token = _CURRENT_KALSHI_NATIVE_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_KALSHI_NATIVE_LOADER_CONFIG.reset(token)
