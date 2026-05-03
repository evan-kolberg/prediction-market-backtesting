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

import copy
import os
import time
import warnings
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, ClassVar

import msgspec
import numpy as np
import pandas as pd
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_HTTP_RATE_LIMIT
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.instruments import BinaryOption

from prediction_market_extensions._native import polymarket_public_trade_rows
from prediction_market_extensions._runtime_log import emit_loader_event
from prediction_market_extensions.adapters.polymarket.gamma_markets import infer_gamma_token_winners
from prediction_market_extensions.adapters.prediction_market.info_sanitization import (
    extract_resolution_metadata,
    sanitize_info_for_simulation,
)

_METADATA_CACHE_MISS = object()


def _rounded_float64_array(values: Any, precision: int) -> np.ndarray:
    return np.round(np.asarray(values, dtype=np.float64), decimals=precision)


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
    _METADATA_CACHE_DIR_ENV = "POLYMARKET_METADATA_CACHE_DIR"
    _METADATA_DISABLE_CACHE_ENV = "POLYMARKET_METADATA_DISABLE_CACHE"
    _METADATA_ACTIVE_TTL_SECS_ENV = "POLYMARKET_METADATA_ACTIVE_TTL_SECS"
    _METADATA_CLOSED_TTL_SECS_ENV = "POLYMARKET_METADATA_CLOSED_TTL_SECS"
    _METADATA_DEFAULT_ACTIVE_TTL_SECS = 5 * 60
    _METADATA_DEFAULT_CLOSED_TTL_SECS = 7 * 24 * 60 * 60
    _MARKET_SLUG_CACHE: ClassVar[dict[tuple[type, str, str], dict[str, Any]]] = {}
    _MARKET_DETAILS_CACHE: ClassVar[dict[tuple[type, str, str], dict[str, Any]]] = {}
    _EVENT_SLUG_CACHE: ClassVar[dict[tuple[type, str, str], dict[str, Any]]] = {}
    _FEE_RATE_CACHE: ClassVar[dict[tuple[type, str, str], Decimal | None]] = {}

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

    @classmethod
    def clear_metadata_cache(cls) -> None:
        cls._MARKET_SLUG_CACHE.clear()
        cls._MARKET_DETAILS_CACHE.clear()
        cls._EVENT_SLUG_CACHE.clear()
        cls._FEE_RATE_CACHE.clear()

    @classmethod
    def _gamma_metadata_cache_key(cls) -> str:
        return "https://gamma-api.polymarket.com"

    @classmethod
    def _clob_metadata_cache_key(cls) -> str:
        return "https://clob.polymarket.com"

    @staticmethod
    def _env_flag_enabled(value: str | None) -> bool:
        return bool(value and value.strip().casefold() in {"1", "true", "yes", "on"})

    @classmethod
    def _metadata_cache_dir(cls) -> Path | None:
        if cls._env_flag_enabled(os.getenv(cls._METADATA_DISABLE_CACHE_ENV)):
            return None
        configured = os.getenv(cls._METADATA_CACHE_DIR_ENV)
        if configured is None:
            xdg_cache_home = os.getenv("XDG_CACHE_HOME")
            base_dir = (
                Path(xdg_cache_home).expanduser() if xdg_cache_home else Path.home() / ".cache"
            )
            return base_dir / "nautilus_trader" / "polymarket_metadata" / "v1"
        value = configured.strip()
        if not value or value.casefold() in {"0", "false", "no", "off", "none", "disabled"}:
            return None
        if value.casefold() in {"1", "true", "yes", "on", "default"}:
            return cls._metadata_cache_dir_from_default()
        return Path(value).expanduser()

    @classmethod
    def _metadata_cache_dir_from_default(cls) -> Path:
        xdg_cache_home = os.getenv("XDG_CACHE_HOME")
        base_dir = Path(xdg_cache_home).expanduser() if xdg_cache_home else Path.home() / ".cache"
        return base_dir / "nautilus_trader" / "polymarket_metadata" / "v1"

    @classmethod
    def _metadata_cache_ttl_secs(cls, payload: dict[str, Any]) -> int:
        closed = bool(payload.get("closed") or payload.get("closedTime"))
        env_name = (
            cls._METADATA_CLOSED_TTL_SECS_ENV if closed else cls._METADATA_ACTIVE_TTL_SECS_ENV
        )
        default = (
            cls._METADATA_DEFAULT_CLOSED_TTL_SECS
            if closed
            else cls._METADATA_DEFAULT_ACTIVE_TTL_SECS
        )
        configured = os.getenv(env_name)
        if configured is None:
            return default
        try:
            return max(0, int(configured.strip()))
        except ValueError:
            return default

    @classmethod
    def _metadata_cache_path(cls, kind: str, base_key: str, identifier: str) -> Path | None:
        cache_dir = cls._metadata_cache_dir()
        if cache_dir is None:
            return None
        digest = sha256(f"{base_key}\n{identifier}".encode("utf-8")).hexdigest()
        return cache_dir / kind / f"{digest}.json"

    @staticmethod
    def _metadata_cache_event_fields(kind: str, identifier: str) -> dict[str, str]:
        if kind in {"gamma-market", "gamma-event"}:
            return {"market_slug": identifier}
        if kind == "clob-market":
            return {"condition_id": identifier}
        if kind == "fee-rate":
            return {"token_id": identifier}
        return {}

    @classmethod
    def _emit_metadata_cache_event(
        cls,
        message: str,
        *,
        kind: str,
        identifier: str,
        cache_path: Path,
        level: str = "INFO",
        stage: str,
        status: str,
        bytes_count: int | None = None,
        attrs: dict[str, Any] | None = None,
    ) -> None:
        event_attrs = {"kind": kind}
        if attrs:
            event_attrs.update(attrs)
        emit_loader_event(
            message,
            level=level,
            stage=stage,
            vendor="polymarket",
            status=status,
            platform="polymarket",
            data_type="metadata",
            source_kind="cache",
            source=f"metadata-cache::{cache_path}",
            cache_path=str(cache_path),
            bytes=bytes_count,
            attrs=event_attrs,
            stacklevel=3,
            **cls._metadata_cache_event_fields(kind, identifier),
        )

    @classmethod
    def _read_metadata_disk_cache(cls, kind: str, base_key: str, identifier: str) -> object:
        cache_path = cls._metadata_cache_path(kind, base_key, identifier)
        if cache_path is None or not cache_path.exists():
            return _METADATA_CACHE_MISS
        try:
            raw = cache_path.read_bytes()
            payload = msgspec.json.decode(raw)
            if not isinstance(payload, dict):
                cache_path.unlink(missing_ok=True)
                cls._emit_metadata_cache_event(
                    f"Invalid Polymarket metadata cache {kind} identifier={identifier}",
                    kind=kind,
                    identifier=identifier,
                    cache_path=cache_path,
                    level="WARNING",
                    stage="cache_read",
                    status="error",
                    bytes_count=len(raw),
                    attrs={"error": "cache envelope is not an object"},
                )
                return _METADATA_CACHE_MISS
            expires_ns = int(payload.get("expires_ns", 0))
            if expires_ns <= time.time_ns():
                cache_path.unlink(missing_ok=True)
                cls._emit_metadata_cache_event(
                    f"Expired Polymarket metadata cache {kind} identifier={identifier}",
                    kind=kind,
                    identifier=identifier,
                    cache_path=cache_path,
                    stage="cache_read",
                    status="expired",
                    bytes_count=len(raw),
                )
                return _METADATA_CACHE_MISS
            cls._emit_metadata_cache_event(
                f"Loaded Polymarket metadata cache {kind} identifier={identifier}",
                kind=kind,
                identifier=identifier,
                cache_path=cache_path,
                stage="cache_read",
                status="cache_hit",
                bytes_count=len(raw),
            )
            return copy.deepcopy(payload["payload"])
        except (OSError, KeyError, TypeError, ValueError, msgspec.DecodeError) as exc:
            try:
                cache_path.unlink(missing_ok=True)
            except OSError:
                pass
            cls._emit_metadata_cache_event(
                f"Failed to read Polymarket metadata cache {kind} identifier={identifier}",
                kind=kind,
                identifier=identifier,
                cache_path=cache_path,
                level="WARNING",
                stage="cache_read",
                status="error",
                attrs={"error": str(exc)},
            )
            return _METADATA_CACHE_MISS

    @classmethod
    def _write_metadata_disk_cache(
        cls, kind: str, base_key: str, identifier: str, payload: dict[str, Any]
    ) -> None:
        cache_path = cls._metadata_cache_path(kind, base_key, identifier)
        if cache_path is None:
            return
        ttl_secs = cls._metadata_cache_ttl_secs(payload)
        if ttl_secs <= 0:
            return
        now_ns = time.time_ns()
        envelope = {
            "schema": 1,
            "created_ns": now_ns,
            "expires_ns": now_ns + ttl_secs * 1_000_000_000,
            "payload": payload,
        }
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            bytes_written = tmp_path.write_bytes(msgspec.json.encode(envelope))
            os.replace(tmp_path, cache_path)
            cls._emit_metadata_cache_event(
                f"Wrote Polymarket metadata cache {kind} identifier={identifier}",
                kind=kind,
                identifier=identifier,
                cache_path=cache_path,
                stage="cache_write",
                status="complete",
                bytes_count=bytes_written,
                attrs={"kind": kind, "ttl_secs": ttl_secs},
            )
        except OSError as exc:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            cls._emit_metadata_cache_event(
                f"Failed to write Polymarket metadata cache {kind} identifier={identifier}",
                kind=kind,
                identifier=identifier,
                cache_path=cache_path,
                level="ERROR",
                stage="cache_write",
                status="error",
                attrs={"kind": kind, "error": str(exc)},
            )

    @classmethod
    async def _get_market_by_slug(
        cls, slug: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        key = (cls, cls._gamma_metadata_cache_key(), slug)
        cached = cls._MARKET_SLUG_CACHE.get(key)
        if cached is not None:
            emit_loader_event(
                f"Loaded Polymarket Gamma market slug={slug} from metadata cache",
                stage="discover",
                vendor="polymarket",
                status="cache_hit",
                platform="polymarket",
                source_kind="memory",
                source="metadata-cache",
                market_slug=slug,
            )
            return copy.deepcopy(cached)

        disk_payload = cls._read_metadata_disk_cache("gamma-market", key[1], slug)
        if disk_payload is not _METADATA_CACHE_MISS:
            market = copy.deepcopy(disk_payload)
            cls._MARKET_SLUG_CACHE[key] = copy.deepcopy(market)
            return market

        market = await cls._fetch_market_by_slug(slug, http_client)
        cls._MARKET_SLUG_CACHE[key] = copy.deepcopy(market)
        cls._write_metadata_disk_cache("gamma-market", key[1], slug, market)
        return market

    @classmethod
    async def _get_market_details(
        cls, condition_id: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        key = (cls, cls._clob_metadata_cache_key(), condition_id)
        cached = cls._MARKET_DETAILS_CACHE.get(key)
        if cached is not None:
            emit_loader_event(
                f"Loaded Polymarket CLOB market details condition_id={condition_id} "
                "from metadata cache",
                stage="discover",
                vendor="polymarket",
                status="cache_hit",
                platform="polymarket",
                source_kind="memory",
                source="metadata-cache",
                condition_id=condition_id,
            )
            return copy.deepcopy(cached)

        disk_payload = cls._read_metadata_disk_cache("clob-market", key[1], condition_id)
        if disk_payload is not _METADATA_CACHE_MISS:
            market_details = copy.deepcopy(disk_payload)
            cls._MARKET_DETAILS_CACHE[key] = copy.deepcopy(market_details)
            return market_details

        market_details = await cls._fetch_market_details(condition_id, http_client)
        cls._MARKET_DETAILS_CACHE[key] = copy.deepcopy(market_details)
        cls._write_metadata_disk_cache("clob-market", key[1], condition_id, market_details)
        return market_details

    @classmethod
    async def _get_event_by_slug(
        cls, slug: str, http_client: nautilus_pyo3.HttpClient
    ) -> dict[str, Any]:
        key = (cls, cls._gamma_metadata_cache_key(), slug)
        cached = cls._EVENT_SLUG_CACHE.get(key)
        if cached is not None:
            return copy.deepcopy(cached)

        disk_payload = cls._read_metadata_disk_cache("gamma-event", key[1], slug)
        if disk_payload is not _METADATA_CACHE_MISS:
            event = copy.deepcopy(disk_payload)
            cls._EVENT_SLUG_CACHE[key] = copy.deepcopy(event)
            return event

        event = await cls._fetch_event_by_slug(slug, http_client)
        cls._EVENT_SLUG_CACHE[key] = copy.deepcopy(event)
        cls._write_metadata_disk_cache("gamma-event", key[1], slug, event)
        return event

    @classmethod
    async def _get_market_fee_rate_bps(
        cls, token_id: str, http_client: nautilus_pyo3.HttpClient
    ) -> Decimal | None:
        key = (cls, cls._clob_metadata_cache_key(), token_id)
        if key in cls._FEE_RATE_CACHE:
            return cls._FEE_RATE_CACHE[key]

        disk_payload = cls._read_metadata_disk_cache("fee-rate", key[1], token_id)
        if disk_payload is not _METADATA_CACHE_MISS:
            fee_rate_bps = (
                cls._coerce_fee_rate_bps(disk_payload.get("fee_rate_bps"))
                if isinstance(disk_payload, dict)
                else None
            )
            cls._FEE_RATE_CACHE[key] = fee_rate_bps
            return fee_rate_bps

        fee_rate_bps = await cls._fetch_market_fee_rate_bps(token_id, http_client)
        cls._FEE_RATE_CACHE[key] = fee_rate_bps
        cls._write_metadata_disk_cache(
            "fee-rate",
            key[1],
            token_id,
            {"fee_rate_bps": str(fee_rate_bps) if fee_rate_bps is not None else None},
        )
        return fee_rate_bps

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
            emit_loader_event(
                f"Polymarket Gamma market request missed slug={slug} status={response.status}",
                level="WARNING",
                stage="discover",
                vendor="polymarket",
                status="skip",
                platform="polymarket",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/markets/slug",
                market_slug=slug,
            )
            raise ValueError(f"Market with slug '{slug}' not found")

        if response.status != 200:
            emit_loader_event(
                f"Polymarket Gamma market request failed slug={slug} status={response.status}",
                level="ERROR",
                stage="discover",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/markets/slug",
                market_slug=slug,
            )
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
        if fee_rate_bps is None:
            fee_rate_bps = cls._coerce_fee_rate_bps(payload.get("base_fee"))

        emit_loader_event(
            f"Loaded Polymarket CLOB fee rate token_id={token_id}",
            stage="fetch",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            source_kind="remote",
            source=cls._FEE_RATE_URL,
            token_id=token_id,
        )
        return fee_rate_bps

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

        fee_rate_bps = await cls._get_market_fee_rate_bps(token_id, http_client)
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

        emit_loader_event(
            f"Fetching Polymarket Gamma event slug={slug}",
            stage="discover",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/events",
            market_slug=slug,
        )
        response = await http_client.get(
            url="https://gamma-api.polymarket.com/events", params={"slug": slug}
        )

        if response.status == 404:
            emit_loader_event(
                f"Polymarket Gamma event request missed slug={slug} status={response.status}",
                level="WARNING",
                stage="discover",
                vendor="polymarket",
                status="skip",
                platform="polymarket",
                data_type="metadata",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/events",
                market_slug=slug,
            )
            raise ValueError(f"Event with slug '{slug}' not found")

        if response.status != 200:
            emit_loader_event(
                f"Polymarket Gamma event request failed slug={slug} status={response.status}",
                level="ERROR",
                stage="discover",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                data_type="metadata",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/events",
                market_slug=slug,
            )
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        events = msgspec.json.decode(response.body)

        if not events:
            emit_loader_event(
                f"Polymarket Gamma event response was empty slug={slug}",
                level="WARNING",
                stage="discover",
                vendor="polymarket",
                status="skip",
                platform="polymarket",
                data_type="metadata",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/events",
                market_slug=slug,
                rows=0,
            )
            raise ValueError(f"Event with slug '{slug}' not found")

        emit_loader_event(
            f"Loaded Polymarket Gamma event slug={slug}",
            stage="discover",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/events",
            market_slug=slug,
            rows=len(events) if isinstance(events, list) else None,
        )
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
        market = await cls._get_market_by_slug(slug, client)
        condition_id = market["conditionId"]
        market_details = await cls._get_market_details(condition_id, client)
        tokens = [dict(token) for token in market_details.get("tokens", [])]
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

        market_details = dict(market_details)
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
        event = await cls._get_event_by_slug(slug, client)
        markets = event.get("markets", [])

        if not markets:
            raise ValueError(f"No markets found in event '{slug}'")

        loaders: list[PolymarketDataLoader] = []

        for market in markets:
            condition_id = market.get("conditionId")
            if not condition_id:
                continue

            market_details = await cls._get_market_details(condition_id, client)

            tokens = [dict(token) for token in market_details.get("tokens", [])]
            if not tokens:
                continue

            if token_index >= len(tokens):
                raise ValueError(
                    f"Token index {token_index} out of range (market {condition_id} has {len(tokens)} tokens)"
                )

            token = tokens[token_index]
            token_id = token["token_id"]
            outcome = token["outcome"]
            market_details = dict(market_details)
            market_details["tokens"] = tokens
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

        window_trades = raw_trades
        if start_ts is not None:
            window_trades = [trade for trade in window_trades if trade["timestamp"] >= start_ts]
        if end_ts is not None:
            window_trades = [trade for trade in window_trades if trade["timestamp"] <= end_ts]

        return self._parse_public_trade_rows(window_trades, sort=True)

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
        emit_loader_event(
            "Fetching Polymarket Gamma events page "
            f"offset={offset} limit={limit} active={active} closed={closed} archived={archived}",
            stage="discover",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/events",
            attrs={
                "offset": offset,
                "limit": limit,
                "active": active,
                "closed": closed,
                "archived": archived,
            },
        )
        response = await self._http_client.get(
            url="https://gamma-api.polymarket.com/events", params=params
        )

        if response.status != 200:
            emit_loader_event(
                f"Polymarket Gamma events page request failed offset={offset} "
                f"limit={limit} status={response.status}",
                level="ERROR",
                stage="discover",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                data_type="metadata",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/events",
                attrs={"offset": offset, "limit": limit},
            )
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        events = msgspec.json.decode(response.body)
        emit_loader_event(
            f"Loaded Polymarket Gamma events page offset={offset} rows={len(events)}",
            stage="discover",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/events",
            rows=len(events),
            attrs={"offset": offset, "limit": limit},
        )
        return events

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
        emit_loader_event(
            "Fetching Polymarket Gamma markets page "
            f"offset={offset} limit={limit} active={active} closed={closed} archived={archived}",
            stage="discover",
            vendor="polymarket",
            status="start",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/markets",
            attrs={
                "offset": offset,
                "limit": limit,
                "active": active,
                "closed": closed,
                "archived": archived,
            },
        )
        response = await self._http_client.get(
            url="https://gamma-api.polymarket.com/markets", params=params
        )

        if response.status != 200:
            emit_loader_event(
                f"Polymarket Gamma markets page request failed offset={offset} "
                f"limit={limit} status={response.status}",
                level="ERROR",
                stage="discover",
                vendor="polymarket",
                status="error",
                platform="polymarket",
                data_type="metadata",
                source_kind="remote",
                source="https://gamma-api.polymarket.com/markets",
                attrs={"offset": offset, "limit": limit},
            )
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: {response.body.decode('utf-8')}"
            )

        markets = msgspec.json.decode(response.body)
        emit_loader_event(
            f"Loaded Polymarket Gamma markets page offset={offset} rows={len(markets)}",
            stage="discover",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            data_type="metadata",
            source_kind="remote",
            source="https://gamma-api.polymarket.com/markets",
            rows=len(markets),
            attrs={"offset": offset, "limit": limit},
        )
        return markets

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
        return self._parse_public_trade_rows(trades_data, sort=False)

    def _parse_public_trade_rows(
        self,
        trades_data: list[dict],
        *,
        sort: bool,
    ) -> list[TradeTick]:
        if self._token_id is None:
            raise ValueError(
                "token_id is required to parse trades. "
                "Use from_market_slug() to create a loader with token_id, "
                "or pass token_id to __init__()"
            )

        (
            prices,
            sizes,
            aggressor_sides,
            trade_ids,
            ts_events,
            ts_inits,
            unexpected_side_records,
            skipped_price_records,
        ) = polymarket_public_trade_rows(trades_data, token_id=self._token_id, sort=sort)
        for original_index, side in unexpected_side_records:
            warnings.warn(
                f"Polymarket trade {original_index} had unexpected side "
                f"{side!r}; recording NO_AGGRESSOR for audit visibility.",
                RuntimeWarning,
                stacklevel=2,
            )
        for original_index, raw_price in skipped_price_records:
            warnings.warn(
                "Skipping Polymarket trade with out-of-range or untradable price "
                f"{raw_price!r} at record {original_index}.",
                RuntimeWarning,
                stacklevel=2,
            )
        if not prices:
            return []

        instrument = self._instrument
        price_precision = int(instrument.price_precision)
        size_precision = int(instrument.size_precision)
        return list(
            TradeTick.from_raw_arrays_to_list(
                instrument.id,
                price_precision,
                size_precision,
                _rounded_float64_array(prices, price_precision),
                _rounded_float64_array(sizes, size_precision),
                np.asarray(aggressor_sides, dtype=np.uint8),
                trade_ids,
                np.asarray(ts_events, dtype=np.uint64),
                np.asarray(ts_inits, dtype=np.uint64),
            )
        )
