from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Sequence
from urllib.parse import urlparse

import msgspec

from prediction_market_extensions.adapters.polymarket.loaders import (
    PolymarketDataLoader,
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


POLYMARKET_GAMMA_BASE_URL_ENV = "POLYMARKET_GAMMA_BASE_URL"
POLYMARKET_CLOB_BASE_URL_ENV = "POLYMARKET_CLOB_BASE_URL"
POLYMARKET_TRADE_API_BASE_URL_ENV = "POLYMARKET_TRADE_API_BASE_URL"
_POLYMARKET_GAMMA_SUFFIXES = ("/markets", "/events", "/markets/slug")
_POLYMARKET_CLOB_SUFFIXES = ("/markets", "/fee-rate")
_POLYMARKET_TRADES_SUFFIXES = ("/trades",)
_POLYMARKET_SOURCE_ROLE_ALIASES = {
    "gamma": POLYMARKET_GAMMA_BASE_URL_ENV,
    "markets": POLYMARKET_GAMMA_BASE_URL_ENV,
    "clob": POLYMARKET_CLOB_BASE_URL_ENV,
    "orderbook": POLYMARKET_CLOB_BASE_URL_ENV,
    "trades": POLYMARKET_TRADE_API_BASE_URL_ENV,
    "trade_api": POLYMARKET_TRADE_API_BASE_URL_ENV,
}


@dataclass(frozen=True)
class PolymarketNativeDataSourceSelection:
    summary: str


@dataclass(frozen=True)
class PolymarketNativeLoaderConfig:
    gamma_base_url: str | None
    clob_base_url: str | None
    trade_api_base_url: str | None


_CURRENT_POLYMARKET_NATIVE_LOADER_CONFIG: ContextVar[
    PolymarketNativeLoaderConfig | None
] = ContextVar("polymarket_native_loader_config", default=None)


def _current_loader_config() -> PolymarketNativeLoaderConfig | None:
    return _CURRENT_POLYMARKET_NATIVE_LOADER_CONFIG.get()


class RunnerPolymarketDataLoader(PolymarketDataLoader):
    @classmethod
    def _configured_gamma_base_url(cls) -> str:
        config = _current_loader_config()
        if config is not None:
            value = config.gamma_base_url
        else:
            value = env_value(os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV))
        if is_disabled(value) or value is None:
            raise ValueError(
                "Polymarket native data source requires a gamma base URL via "
                "DATA.sources or POLYMARKET_GAMMA_BASE_URL."
            )
        return trim_url_suffix(value, _POLYMARKET_GAMMA_SUFFIXES)

    @classmethod
    def _configured_clob_base_url(cls) -> str:
        config = _current_loader_config()
        if config is not None:
            value = config.clob_base_url
        else:
            value = env_value(os.getenv(POLYMARKET_CLOB_BASE_URL_ENV))
        if is_disabled(value) or value is None:
            raise ValueError(
                "Polymarket native data source requires a clob base URL via "
                "DATA.sources or POLYMARKET_CLOB_BASE_URL."
            )
        return trim_url_suffix(value, _POLYMARKET_CLOB_SUFFIXES)

    @classmethod
    def _configured_trade_api_base_url(cls) -> str:
        config = _current_loader_config()
        if config is not None:
            value = config.trade_api_base_url
        else:
            value = env_value(os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV))
        if is_disabled(value) or value is None:
            raise ValueError(
                "Polymarket native data source requires a trades base URL via "
                "DATA.sources or POLYMARKET_TRADE_API_BASE_URL."
            )
        return trim_url_suffix(value, _POLYMARKET_TRADES_SUFFIXES)

    @classmethod
    async def _fetch_market_by_slug(
        cls,
        slug: str,
        http_client,
    ) -> dict[str, Any]:
        gamma_base_url = cls._configured_gamma_base_url()
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
        parts.append(f"gamma:{gamma_base_url}")
    if trade_api_base_url is not None:
        parts.append(f"trades:{trade_api_base_url}")
    if clob_base_url is not None:
        parts.append(f"clob:{clob_base_url}")
    if not parts:
        raise ValueError(
            "Polymarket native data source requires explicit gamma, trades, and clob "
            "base URLs via DATA.sources or environment variables."
        )
    return "Polymarket source: native (" + ", ".join(parts) + ")"


def _normalized_override(
    value: str | None,
    *,
    env_name: str,
    suffixes: tuple[str, ...],
) -> str | None:
    normalized = env_value(value)
    if normalized is None or is_disabled(normalized):
        return None
    return trim_url_suffix(normalized, suffixes)


def _parse_named_source(raw_source: str) -> tuple[str | None, str]:
    stripped = raw_source.strip()
    for separator in (":", "="):
        role, found, value = stripped.partition(separator)
        if not found:
            continue
        normalized_role = role.strip().casefold()
        env_name = _POLYMARKET_SOURCE_ROLE_ALIASES.get(normalized_role)
        if env_name is None:
            continue
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError(
                f"Polymarket source {raw_source!r} is missing a URL value."
            )
        return env_name, normalized_value
    return None, raw_source


def _infer_env_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    hostname = (parsed.netloc or parsed.path).casefold()
    normalized_path = parsed.path.rstrip("/")
    if "gamma-api." in hostname:
        return POLYMARKET_GAMMA_BASE_URL_ENV
    if "data-api." in hostname:
        return POLYMARKET_TRADE_API_BASE_URL_ENV
    if hostname.startswith("clob.") or ".clob." in hostname:
        return POLYMARKET_CLOB_BASE_URL_ENV
    if normalized_path.endswith("/trades"):
        return POLYMARKET_TRADE_API_BASE_URL_ENV
    if normalized_path.endswith("/fee-rate"):
        return POLYMARKET_CLOB_BASE_URL_ENV
    if normalized_path.endswith("/events"):
        return POLYMARKET_GAMMA_BASE_URL_ENV
    if normalized_path.endswith("/markets"):
        return POLYMARKET_GAMMA_BASE_URL_ENV
    raise ValueError(
        "Polymarket native source URLs must either include an explicit role prefix "
        "like gamma:..., trades:..., clob:... or end with /events, /markets, "
        "/trades, or /fee-rate."
    )


def _normalized_env_updates(
    *,
    gamma_base_url: str | None,
    clob_base_url: str | None,
    trade_api_base_url: str | None,
) -> dict[str, str | None]:
    return {
        POLYMARKET_GAMMA_BASE_URL_ENV: (
            trim_url_suffix(gamma_base_url, _POLYMARKET_GAMMA_SUFFIXES)
            if gamma_base_url is not None
            else None
        ),
        POLYMARKET_CLOB_BASE_URL_ENV: (
            trim_url_suffix(clob_base_url, _POLYMARKET_CLOB_SUFFIXES)
            if clob_base_url is not None
            else None
        ),
        POLYMARKET_TRADE_API_BASE_URL_ENV: (
            trim_url_suffix(trade_api_base_url, _POLYMARKET_TRADES_SUFFIXES)
            if trade_api_base_url is not None
            else None
        ),
    }


def _resolve_explicit_sources(
    sources: Sequence[str],
) -> tuple[PolymarketNativeDataSourceSelection, PolymarketNativeLoaderConfig]:
    updates = _normalized_env_updates(
        gamma_base_url=None,
        clob_base_url=None,
        trade_api_base_url=None,
    )

    for raw_source in sources:
        if looks_like_local_path(raw_source):
            raise ValueError(
                "Native Polymarket trade-tick sources do not support local path inputs yet. "
                f"Received {raw_source!r}.",
            )

        env_name, candidate = _parse_named_source(raw_source)
        normalized = normalize_urlish(candidate)
        resolved_env_name = env_name or _infer_env_name_from_url(normalized)
        existing = updates.get(resolved_env_name)
        if existing is not None and existing != normalized:
            raise ValueError(
                f"Polymarket native sources received multiple values for {resolved_env_name}."
            )
        updates[resolved_env_name] = normalized

    return (
        PolymarketNativeDataSourceSelection(
            summary=_summary_from_overrides(
                gamma_base_url=updates[POLYMARKET_GAMMA_BASE_URL_ENV],
                clob_base_url=updates[POLYMARKET_CLOB_BASE_URL_ENV],
                trade_api_base_url=updates[POLYMARKET_TRADE_API_BASE_URL_ENV],
            ),
        ),
        PolymarketNativeLoaderConfig(
            gamma_base_url=updates[POLYMARKET_GAMMA_BASE_URL_ENV],
            clob_base_url=updates[POLYMARKET_CLOB_BASE_URL_ENV],
            trade_api_base_url=updates[POLYMARKET_TRADE_API_BASE_URL_ENV],
        ),
    )


def resolve_polymarket_native_loader_config(
    sources: Sequence[str] | None = None,
) -> tuple[PolymarketNativeDataSourceSelection, PolymarketNativeLoaderConfig]:
    if sources:
        return _resolve_explicit_sources(sources)

    gamma_base_url = _normalized_override(
        os.getenv(POLYMARKET_GAMMA_BASE_URL_ENV),
        env_name=POLYMARKET_GAMMA_BASE_URL_ENV,
        suffixes=_POLYMARKET_GAMMA_SUFFIXES,
    )
    clob_base_url = _normalized_override(
        os.getenv(POLYMARKET_CLOB_BASE_URL_ENV),
        env_name=POLYMARKET_CLOB_BASE_URL_ENV,
        suffixes=_POLYMARKET_CLOB_SUFFIXES,
    )
    trade_api_base_url = _normalized_override(
        os.getenv(POLYMARKET_TRADE_API_BASE_URL_ENV),
        env_name=POLYMARKET_TRADE_API_BASE_URL_ENV,
        suffixes=_POLYMARKET_TRADES_SUFFIXES,
    )
    return (
        PolymarketNativeDataSourceSelection(
            summary=_summary_from_overrides(
                gamma_base_url=gamma_base_url,
                clob_base_url=clob_base_url,
                trade_api_base_url=trade_api_base_url,
            ),
        ),
        PolymarketNativeLoaderConfig(
            gamma_base_url=gamma_base_url,
            clob_base_url=clob_base_url,
            trade_api_base_url=trade_api_base_url,
        ),
    )


def resolve_polymarket_native_data_source_selection(
    sources: Sequence[str] | None = None,
) -> tuple[PolymarketNativeDataSourceSelection, dict[str, str | None]]:
    selection, config = resolve_polymarket_native_loader_config(sources=sources)
    if sources:
        return (
            selection,
            _normalized_env_updates(
                gamma_base_url=config.gamma_base_url,
                clob_base_url=config.clob_base_url,
                trade_api_base_url=config.trade_api_base_url,
            ),
        )
    return selection, {}


@contextmanager
def configured_polymarket_native_data_source(
    *,
    sources: Sequence[str] | None = None,
) -> Iterator[PolymarketNativeDataSourceSelection]:
    selection, config = resolve_polymarket_native_loader_config(sources=sources)
    token = _CURRENT_POLYMARKET_NATIVE_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_POLYMARKET_NATIVE_LOADER_CONFIG.reset(token)
