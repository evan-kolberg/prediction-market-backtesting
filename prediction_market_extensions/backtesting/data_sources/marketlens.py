from __future__ import annotations

import json
import os
import re
import threading
import time
import warnings
from collections.abc import Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC
from hashlib import sha256
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BookAction, OrderSide, RecordFlag

from prediction_market_extensions._native import fixed_raw_values, source_days_for_window_ns
from prediction_market_extensions._runtime_log import emit_loader_event
from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from prediction_market_extensions.backtesting.data_sources._common import (
    DISABLED_ENV_VALUES,
    normalize_urlish,
)

MARKETLENS_API_KEY_ENV = "MARKETLENS_API_KEY"
MARKETLENS_BASE_URL_ENV = "MARKETLENS_BASE_URL"
MARKETLENS_CACHE_ROOT_ENV = "MARKETLENS_CACHE_ROOT"
MARKETLENS_API_WORKERS_ENV = "MARKETLENS_API_WORKERS"
MARKETLENS_PREFETCH_WORKERS_ENV = "MARKETLENS_PREFETCH_WORKERS"

_MARKETLENS_DEFAULT_API_BASE_URL = "https://api.marketlens.trade/v1"
_MARKETLENS_HTTP_TIMEOUT_SECS = 120
_MARKETLENS_DEFAULT_API_WORKERS = 4
_MARKETLENS_DEFAULT_PREFETCH_WORKERS = 4
_MARKETLENS_HISTORY_PAGE_LIMIT = 100_000
_MARKETLENS_RATE_LIMIT_ATTEMPTS = 5
_MARKETLENS_RETRY_AFTER_FLOOR_SECS = 1.0
_MARKETLENS_USER_AGENT = "prediction-market-backtesting/1.0"
_MARKETLENS_API_PREFIX = "api:"
_MARKETLENS_SOURCE_API = "api"
_MARKETLENS_RAW_CACHE_SUBDIR = "history-days-v1"
_MARKETLENS_DELTAS_CACHE_SUBDIR = "book-deltas-v1"
_MARKETLENS_TRADE_TICKS_CACHE_SUBDIR = "trade-ticks-v1"
_MARKETLENS_NO_RETRY_ERROR_CODES = frozenset(
    {"DAILY_BUDGET_EXCEEDED", "ROW_LIMIT_EXCEEDED", "UNIT_LIMIT_EXCEEDED"}
)
_MARKETLENS_DAY_MS = 86_400_000
_MARKETLENS_RAW_CACHE_COLUMN_ORDER = (
    "kind",
    "t",
    "price",
    "size",
    "side",
    "trade_id",
    "is_reseed",
    "bids",
    "asks",
)
_MARKETLENS_DELTAS_CACHE_COLUMN_ORDER = (
    "event_index",
    "action",
    "side",
    "price",
    "size",
    "flags",
    "sequence",
    "ts_event",
    "ts_init",
)
_MARKETLENS_TRADE_TICKS_CACHE_COLUMN_ORDER = (
    "price",
    "size",
    "aggressor_side",
    "trade_id",
    "ts_event",
    "ts_init",
)

_MARKETLENS_API_SEMAPHORE_LOCK = threading.Lock()
_MARKETLENS_API_SEMAPHORE: tuple[int, threading.BoundedSemaphore] | None = None

_MARKETLENS_DAY_LOCKS_LOCK = threading.Lock()
_MARKETLENS_DAY_LOCKS: dict[tuple[str, str, str], threading.Lock] = {}


def _history_day_lock(base_url: str, market_id: str, date: str) -> threading.Lock:
    key = (base_url, market_id, date)
    with _MARKETLENS_DAY_LOCKS_LOCK:
        lock = _MARKETLENS_DAY_LOCKS.get(key)
        if lock is None:
            lock = _MARKETLENS_DAY_LOCKS[key] = threading.Lock()
        return lock


_FLIP_SIDE = {"BUY": "SELL", "SELL": "BUY"}


def _release_arrow_memory() -> None:
    try:
        pa.default_memory_pool().release_unused()
    except AttributeError:
        pass


def _unique_tmp_path(path: Path) -> Path:
    return path.with_name(
        f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}.{time.monotonic_ns()}"
    )


@dataclass(frozen=True)
class MarketlensSourceEntry:
    kind: str
    target: str | None = None
    api_key: str | None = None


@dataclass(frozen=True)
class MarketlensLoaderConfig:
    ordered_source_entries: tuple[MarketlensSourceEntry, ...]


@dataclass(frozen=True)
class MarketlensDataSourceSelection:
    mode: str
    summary: str


@dataclass
class _MarketlensDayResult:
    date: str
    records: list[OrderBookDeltas]
    source: str


_CURRENT_MARKETLENS_LOADER_CONFIG: ContextVar[MarketlensLoaderConfig | None] = ContextVar(
    "marketlens_loader_config", default=None
)


def _current_loader_config() -> MarketlensLoaderConfig | None:
    return _CURRENT_MARKETLENS_LOADER_CONFIG.get()


def _env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped.casefold() in DISABLED_ENV_VALUES:
        return None
    return stripped


def _resolve_api_workers() -> int:
    configured = _env_value(MARKETLENS_API_WORKERS_ENV)
    if configured is None:
        return _MARKETLENS_DEFAULT_API_WORKERS
    try:
        return max(1, int(configured))
    except ValueError:
        return _MARKETLENS_DEFAULT_API_WORKERS


def _resolve_prefetch_workers() -> int:
    configured = _env_value(MARKETLENS_PREFETCH_WORKERS_ENV)
    if configured is None:
        return _MARKETLENS_DEFAULT_PREFETCH_WORKERS
    try:
        return max(1, int(configured))
    except ValueError:
        return _MARKETLENS_DEFAULT_PREFETCH_WORKERS


def _marketlens_api_semaphore() -> threading.BoundedSemaphore:
    global _MARKETLENS_API_SEMAPHORE
    workers = _resolve_api_workers()
    with _MARKETLENS_API_SEMAPHORE_LOCK:
        if _MARKETLENS_API_SEMAPHORE is None or _MARKETLENS_API_SEMAPHORE[0] != workers:
            _MARKETLENS_API_SEMAPHORE = (workers, threading.BoundedSemaphore(workers))
        return _MARKETLENS_API_SEMAPHORE[1]


@contextmanager
def _marketlens_api_slot() -> Iterator[None]:
    semaphore = _marketlens_api_semaphore()
    semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


def _default_cache_root() -> Path:
    configured = os.getenv("XDG_CACHE_HOME")
    cache_home = Path(configured).expanduser() if configured else Path.home() / ".cache"
    return cache_home / "nautilus_trader" / "marketlens"


def _resolve_cache_root() -> Path | None:
    configured = os.getenv(MARKETLENS_CACHE_ROOT_ENV)
    if configured is None:
        return _default_cache_root()
    value = configured.strip()
    if value.casefold() in DISABLED_ENV_VALUES:
        return None
    return Path(value).expanduser()


def _normalize_api_base_url(value: str | None) -> str:
    if value is None or not value.strip():
        return _env_value(MARKETLENS_BASE_URL_ENV) or _MARKETLENS_DEFAULT_API_BASE_URL
    return normalize_urlish(value)


_UNEXPANDED_VAR_PATTERN = re.compile(r"\$\{[^}]+\}|\$[A-Za-z_][A-Za-z0-9_]*")


def _expand_source_vars(source: str) -> str:
    expanded = os.path.expandvars(source)
    return _UNEXPANDED_VAR_PATTERN.sub("", expanded)


def _classify_marketlens_sources(sources: Sequence[str]) -> tuple[MarketlensSourceEntry, ...]:
    entries: list[MarketlensSourceEntry] = []
    for raw_source in sources:
        expanded = _expand_source_vars(str(raw_source))
        stripped = expanded.strip()
        if not stripped:
            continue
        folded = stripped.casefold()
        if folded.startswith(_MARKETLENS_API_PREFIX):
            remainder = stripped[len(_MARKETLENS_API_PREFIX) :].strip()
            base_url: str | None = None
            api_key: str | None = None
            if remainder:
                if remainder.lower().startswith(("http://", "https://")):
                    base_url = remainder
                else:
                    api_key = remainder
            entries.append(
                MarketlensSourceEntry(
                    kind=_MARKETLENS_SOURCE_API,
                    target=_normalize_api_base_url(base_url),
                    api_key=api_key,
                )
            )
            continue
        raise ValueError(f"Unsupported Marketlens explicit source {stripped!r}. Use api:.")
    if not entries:
        raise ValueError("Marketlens requires at least one source. Use api:.")
    return tuple(entries)


def _default_marketlens_sources_from_env() -> tuple[MarketlensSourceEntry, ...]:
    env_key = _env_value(MARKETLENS_API_KEY_ENV)
    if env_key is not None:
        return (
            MarketlensSourceEntry(
                kind=_MARKETLENS_SOURCE_API,
                target=_normalize_api_base_url(None),
                api_key=env_key,
            ),
        )
    raise ValueError(
        "Marketlens requires DATA.sources with api: "
        f"or {MARKETLENS_API_KEY_ENV} set in the environment."
    )


def _source_summary_parts(entries: Sequence[MarketlensSourceEntry]) -> list[str]:
    parts: list[str] = ["cache"] if _resolve_cache_root() is not None else []
    for entry in entries:
        suffix = " (key set)" if entry.api_key else " (key missing)"
        parts.append(f"api {entry.target}{suffix}")
    return parts


def _source_summary(entries: Sequence[MarketlensSourceEntry]) -> str:
    parts = _source_summary_parts(entries)
    joined = " -> ".join(parts)
    return "\n".join(
        (
            f"Marketlens book source: explicit priority ({joined})",
            f"Marketlens trade source: same history stream ({joined})",
        )
    )


def resolve_marketlens_loader_config(
    *, sources: Sequence[str] | None = None
) -> tuple[MarketlensDataSourceSelection, MarketlensLoaderConfig]:
    if sources is None:
        current_config = _current_loader_config()
        if current_config is not None:
            return (
                MarketlensDataSourceSelection(
                    mode="auto",
                    summary=_source_summary(current_config.ordered_source_entries),
                ),
                current_config,
            )
    entries = (
        _classify_marketlens_sources(sources) if sources else _default_marketlens_sources_from_env()
    )
    return (
        MarketlensDataSourceSelection(mode="auto", summary=_source_summary(entries)),
        MarketlensLoaderConfig(ordered_source_entries=entries),
    )


def resolve_marketlens_data_source_selection(
    *, sources: Sequence[str] | None = None
) -> tuple[MarketlensDataSourceSelection, dict[str, str | None]]:
    selection, _config = resolve_marketlens_loader_config(sources=sources)
    return selection, {}


@contextmanager
def configured_marketlens_data_source(
    *, sources: Sequence[str] | None = None
) -> Iterator[MarketlensDataSourceSelection]:
    selection, config = resolve_marketlens_loader_config(sources=sources)
    token = _CURRENT_MARKETLENS_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_MARKETLENS_LOADER_CONFIG.reset(token)


class RunnerPolymarketMarketlensBookDataLoader(PolymarketDataLoader):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._marketlens_market_record: dict[str, object] | None = None
        self._marketlens_prefetch_workers = _resolve_prefetch_workers()
        self._marketlens_day_trade_ticks: dict[tuple[str, int, int], tuple[TradeTick, ...]] = {}

    @classmethod
    async def from_market_slug(
        cls, slug: str, token_index: int = 0, http_client=None
    ) -> "RunnerPolymarketMarketlensBookDataLoader":  # type: ignore[override]
        loader = await super().from_market_slug(
            slug=slug,
            token_index=token_index,
            http_client=http_client,
        )
        loader._marketlens_market_slug = slug
        loader._marketlens_token_index = token_index
        return loader

    @staticmethod
    def _config() -> MarketlensLoaderConfig:
        _selection, config = resolve_marketlens_loader_config()
        return config

    @staticmethod
    def _resolve_cache_root() -> Path | None:
        return _resolve_cache_root()

    def _resolve_prefetch_workers(self) -> int:
        return _resolve_prefetch_workers()

    @staticmethod
    def _marketlens_source_kind(source: str) -> str | None:
        if source.startswith(("marketlens-cache::", "marketlens-deltas-cache::")):
            return "cache"
        if source.startswith("marketlens-api::"):
            return "remote"
        return None

    @staticmethod
    def _marketlens_stage_for_source(source: str) -> str:
        return "cache_read" if "cache" in source else "fetch"

    def _day_progress(self, date: str, event: str, source: str, rows: int) -> None:
        status = "start"
        if event == "complete":
            if source == "none" and rows == 0:
                status = "skip"
            elif "cache" in source:
                status = "cache_hit"
            else:
                status = "complete"
        cache_path: str | None = None
        if "::" in source and "cache" in source:
            cache_path = source.partition("::")[2] or None
        emit_loader_event(
            f"Marketlens day {event} for {date}: {rows} rows from {source}",
            level="INFO",
            stage=self._marketlens_stage_for_source(source),
            status=status,
            vendor="marketlens",
            platform="polymarket",
            data_type="book",
            source_kind=self._marketlens_source_kind(source),
            source=None if source == "none" else source,
            cache_path=cache_path,
            market_slug=getattr(self, "_marketlens_market_slug", None),
            token_id=str(getattr(self, "_marketlens_token_index", "")),
            rows=rows,
            attrs={"date": date, "event": event},
        )

    @staticmethod
    def _api_error_payload(exc: HTTPError) -> dict[str, object]:
        try:
            payload = json.loads(exc.read())
        except (OSError, ValueError):
            return {}
        error = payload.get("error") if isinstance(payload, dict) else None
        return error if isinstance(error, dict) else {}

    def _get_json(
        self, *, entry: MarketlensSourceEntry, path: str, params: dict[str, object] | None = None
    ) -> dict[str, object]:
        api_key = entry.api_key or _env_value(MARKETLENS_API_KEY_ENV)
        if api_key is None:
            raise ValueError(f"{MARKETLENS_API_KEY_ENV} is required when using Marketlens api:.")
        url = f"{entry.target}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": _MARKETLENS_USER_AGENT,
            },
        )
        attempt = 0
        while True:
            with _marketlens_api_slot():
                try:
                    with urlopen(request, timeout=_MARKETLENS_HTTP_TIMEOUT_SECS) as response:
                        return json.loads(response.read())
                except HTTPError as exc:
                    if exc.code != 429:
                        raise
                    error = self._api_error_payload(exc)
                    code = str(error.get("code") or "")
                    if code in _MARKETLENS_NO_RETRY_ERROR_CODES:
                        raise ValueError(
                            f"Marketlens account limit reached ({code}): "
                            f"{error.get('message') or 'see marketlens.trade/console'}"
                        ) from exc
                    attempt += 1
                    if attempt >= _MARKETLENS_RATE_LIMIT_ATTEMPTS:
                        raise
                    try:
                        retry_after = float(exc.headers.get("Retry-After") or 0.0)
                    except ValueError:
                        retry_after = 0.0
                    delay = max(retry_after, _MARKETLENS_RETRY_AFTER_FLOOR_SECS)
            # Sleep outside the API slot so waiting on a rate-limit window
            # does not hold a concurrency slot other threads could use.
            time.sleep(delay)

    def _market_record(self) -> dict[str, object]:
        record = getattr(self, "_marketlens_market_record", None)
        if record is not None:
            return record
        if self._condition_id is None:
            raise ValueError(
                "condition_id is required to resolve a market against Marketlens. "
                "Use from_market_slug() to create the loader."
            )
        config = self._config()
        last_error: Exception | None = None
        for entry in config.ordered_source_entries:
            try:
                record = self._get_json(entry=entry, path=f"/markets/{self._condition_id}")
            except HTTPError as exc:
                if exc.code == 404:
                    raise ValueError(
                        f"Marketlens does not track condition_id {self._condition_id}."
                    ) from exc
                last_error = exc
                continue
            except URLError as exc:
                last_error = exc
                continue
            tier = record.get("collection_tier")
            if tier == "polled":
                raise ValueError(
                    f"Marketlens collected market {record.get('id')} at the polled tier "
                    "(periodic snapshots only, no L2 delta stream), so it cannot back an "
                    "L2 book replay."
                )
            self._marketlens_market_record = record
            return record
        if last_error is not None:
            raise last_error
        raise ValueError("Marketlens requires at least one source. Use api:.")

    def _marketlens_market_id(self) -> str:
        return str(self._market_record()["id"])

    @staticmethod
    def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
        start_utc = RunnerPolymarketMarketlensBookDataLoader._normalize_to_utc(start)
        end_utc = RunnerPolymarketMarketlensBookDataLoader._normalize_to_utc(end)
        return source_days_for_window_ns(
            int(start_utc.value), int(end_utc.value), semantics="inclusive"
        )

    @staticmethod
    def _normalize_to_utc(value: pd.Timestamp) -> pd.Timestamp:
        if value.tzinfo is None:
            return value.tz_localize(UTC)
        return value.tz_convert(UTC)

    def _day_window(
        self, date: str, *, start: pd.Timestamp, end: pd.Timestamp
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        day_start = pd.Timestamp(date, tz=UTC)
        day_end = day_start + pd.Timedelta(1, "D") - pd.Timedelta(1, "ns")
        start_utc = self._normalize_to_utc(start)
        end_utc = self._normalize_to_utc(end)
        clipped_start = start_utc if start_utc > day_start else day_start
        clipped_end = end_utc if end_utc < day_end else day_end
        if clipped_start > clipped_end:
            return None
        return clipped_start, clipped_end

    @staticmethod
    def _raw_cache_path(*, base_url: str, market_id: str, date: str) -> Path | None:
        cache_root = _resolve_cache_root()
        if cache_root is None:
            return None
        base_key = sha256(base_url.encode("utf-8")).hexdigest()[:16]
        return (
            cache_root
            / _MARKETLENS_RAW_CACHE_SUBDIR
            / base_key
            / "polymarket"
            / market_id
            / f"{date}.parquet"
        )

    @staticmethod
    def _raw_events_to_table(events: Sequence[dict[str, object]]) -> pa.Table:
        kinds: list[str] = []
        ts: list[int] = []
        prices: list[float | None] = []
        sizes: list[float | None] = []
        sides: list[str | None] = []
        trade_ids: list[str | None] = []
        is_reseeds: list[bool | None] = []
        bids: list[str | None] = []
        asks: list[str | None] = []
        for event in events:
            kind = str(event["type"])
            kinds.append(kind)
            ts.append(int(event["t"]))
            if kind == "snapshot":
                prices.append(None)
                sizes.append(None)
                sides.append(None)
                trade_ids.append(None)
                is_reseeds.append(bool(event.get("is_reseed")))
                bids.append(json.dumps(event.get("bids") or []))
                asks.append(json.dumps(event.get("asks") or []))
            else:
                prices.append(float(event["price"]))
                sizes.append(float(event["size"]))
                sides.append(str(event["side"]))
                trade_ids.append(str(event["id"]) if kind == "trade" else None)
                is_reseeds.append(None)
                bids.append(None)
                asks.append(None)
        return pa.table(
            {
                "kind": pa.array(kinds, pa.string()),
                "t": pa.array(ts, pa.int64()),
                "price": pa.array(prices, pa.float64()),
                "size": pa.array(sizes, pa.float64()),
                "side": pa.array(sides, pa.string()),
                "trade_id": pa.array(trade_ids, pa.string()),
                "is_reseed": pa.array(is_reseeds, pa.bool_()),
                "bids": pa.array(bids, pa.string()),
                "asks": pa.array(asks, pa.string()),
            }
        )

    @staticmethod
    def _raw_events_from_table(table: pa.Table) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        for row in table.to_pylist():
            kind = row["kind"]
            if kind == "snapshot":
                events.append(
                    {
                        "type": "snapshot",
                        "t": row["t"],
                        "is_reseed": bool(row["is_reseed"]),
                        "bids": json.loads(row["bids"] or "[]"),
                        "asks": json.loads(row["asks"] or "[]"),
                    }
                )
            elif kind == "delta":
                events.append(
                    {
                        "type": "delta",
                        "t": row["t"],
                        "price": row["price"],
                        "size": row["size"],
                        "side": row["side"],
                    }
                )
            else:
                events.append(
                    {
                        "type": "trade",
                        "t": row["t"],
                        "id": row["trade_id"],
                        "price": row["price"],
                        "size": row["size"],
                        "side": row["side"],
                    }
                )
        return events

    def _load_raw_cache_day(
        self, *, base_url: str, market_id: str, date: str
    ) -> list[dict[str, object]] | None:
        cache_path = self._raw_cache_path(base_url=base_url, market_id=market_id, date=date)
        if cache_path is None or not cache_path.exists():
            return None
        try:
            table = pq.read_table(cache_path, columns=list(_MARKETLENS_RAW_CACHE_COLUMN_ORDER))
            events = self._raw_events_from_table(table)
            del table
            _release_arrow_memory()
            return events
        except Exception as exc:  # noqa: BLE001 - stale/corrupt cache should self-heal
            try:
                cache_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Marketlens: ignored stale raw history cache {cache_path} ({exc})",
                stacklevel=2,
            )
            return None

    def _write_raw_cache_day(
        self,
        *,
        base_url: str,
        market_id: str,
        date: str,
        events: Sequence[dict[str, object]],
    ) -> None:
        cache_path = self._raw_cache_path(base_url=base_url, market_id=market_id, date=date)
        if cache_path is None:
            return
        tmp_path = _unique_tmp_path(cache_path)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(self._raw_events_to_table(events), tmp_path, compression="zstd")
            os.replace(tmp_path, cache_path)
        except Exception as exc:  # noqa: BLE001 - cache writes must not break replay
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Marketlens: failed to write raw history cache {cache_path} ({exc})",
                stacklevel=2,
            )

    def _fetch_history_window(
        self, *, entry: MarketlensSourceEntry, market_id: str, after_ms: int, before_ms: int
    ) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        cursor: str | None = None
        while True:
            params: dict[str, object] = {
                "after": after_ms,
                "before": before_ms,
                "include_trades": "true",
                "limit": _MARKETLENS_HISTORY_PAGE_LIMIT,
            }
            if cursor is not None:
                params["cursor"] = cursor
            payload = self._get_json(
                entry=entry, path=f"/markets/{market_id}/orderbook/history", params=params
            )
            data = payload.get("data")
            if isinstance(data, list):
                events.extend(data)
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            next_cursor = meta.get("cursor")
            if not meta.get("has_more") or next_cursor is None or next_cursor == cursor:
                break
            cursor = next_cursor
        return events

    def _fetch_history_day(
        self, *, entry: MarketlensSourceEntry, market_id: str, date: str
    ) -> list[dict[str, object]]:
        day_start_ms = int(pd.Timestamp(date, tz=UTC).value // 1_000_000)
        # after is exclusive, so day_start_ms - 1 keeps events at exactly
        # midnight in this day's file and lets the server anchor the page on
        # the last snapshot at or before the day boundary.
        events = self._fetch_history_window(
            entry=entry,
            market_id=market_id,
            after_ms=day_start_ms - 1,
            before_ms=day_start_ms + _MARKETLENS_DAY_MS,
        )
        first = events[0] if events else None
        if first is not None and first.get("type") == "snapshot":
            anchor_ms = int(first["t"])
            if anchor_ms < day_start_ms - 1:
                # The anchor snapshot can predate midnight by up to a snapshot
                # interval; refetch from it so the deltas between the anchor
                # and the day boundary keep the chain exact.
                events = (
                    self._fetch_history_window(
                        entry=entry,
                        market_id=market_id,
                        after_ms=anchor_ms,
                        before_ms=day_start_ms,
                    )
                    + events[1:]
                )
        return events

    def _load_history_day(self, *, date: str) -> tuple[list[dict[str, object]], str]:
        config = self._config()
        market_id = self._marketlens_market_id()
        last_error: Exception | None = None
        for entry in config.ordered_source_entries:
            assert entry.target is not None
            # Both token legs of a market share one raw day file and load
            # concurrently from separate loader instances; serialize per
            # (market, day) so the day is fetched and billed once.
            with _history_day_lock(entry.target, market_id, date):
                cached = self._load_raw_cache_day(
                    base_url=entry.target, market_id=market_id, date=date
                )
                if cached is not None:
                    return cached, f"marketlens-cache::{market_id}/{date}"
                try:
                    events = self._fetch_history_day(entry=entry, market_id=market_id, date=date)
                except HTTPError as exc:
                    error = self._api_error_payload(exc)
                    if str(error.get("code") or "") == "EXPORT_NOT_READY":
                        raise ValueError(
                            f"Marketlens has not finished building the history file for market "
                            f"{market_id} yet. Only resolved markets serve /orderbook/history."
                        ) from exc
                    last_error = exc
                    continue
                except URLError as exc:
                    last_error = exc
                    continue
                self._write_raw_cache_day(
                    base_url=entry.target, market_id=market_id, date=date, events=events
                )
            return events, f"marketlens-api::{entry.target}/markets/{market_id}/orderbook/history"
        if last_error is not None:
            raise last_error
        raise ValueError("Marketlens requires at least one source. Use api:.")

    def _instrument_precisions(self) -> tuple[int, int]:
        return int(self.instrument.price_precision), int(self.instrument.size_precision)

    def _parse_levels(
        self, raw_levels: object, *, invert: bool, price_precision: int, size_precision: int
    ) -> dict[float, float]:
        levels: dict[float, float] = {}
        if not isinstance(raw_levels, list):
            return levels
        for level in raw_levels:
            price = float(level["price"])
            if invert:
                price = 1.0 - price
            size = round(float(level["size"]), size_precision)
            if size > 0.0:
                levels[round(price, price_precision)] = size
        return levels

    def _convert_history_events(
        self,
        events: Sequence[dict[str, object]],
        *,
        start_ns: int,
        end_ns: int,
        token_index: int,
    ) -> tuple[list[OrderBookDeltas], list[tuple[float, float, str, str, int]]]:
        # The history stream is YES-centric, and Polymarket's CLOB keeps the
        # two binary token books as exact mirrors of one another, so the NO
        # leg (token_index=1) is served by inverting the stream.
        invert = token_index == 1
        price_precision, size_precision = self._instrument_precisions()
        instrument_id = self.instrument.id

        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        seeded = False
        emitted_any = False
        records: list[OrderBookDeltas] = []
        trades: list[tuple[float, float, str, str, int]] = []

        def _emit(rows: list[tuple[int, int, float, float]], ts_ns: int, *, seq_base: int) -> None:
            price_raws = fixed_raw_values([row[2] for row in rows], price_precision)
            size_raws = fixed_raw_values([row[3] for row in rows], size_precision)
            deltas = [
                OrderBookDelta.from_raw(
                    instrument_id,
                    action,
                    side,
                    price_raws[index],
                    price_precision,
                    size_raws[index],
                    size_precision,
                    0,
                    flags=int(RecordFlag.F_LAST) if index == len(rows) - 1 else 0,
                    sequence=seq_base + index if seq_base else 0,
                    ts_event=ts_ns,
                    ts_init=ts_ns,
                )
                for index, (action, side, _price, _size) in enumerate(rows)
            ]
            records.append(OrderBookDeltas(instrument_id, deltas))

        def _ladder_rows() -> list[tuple[int, int, float, float]]:
            rows: list[tuple[int, int, float, float]] = [
                (int(BookAction.CLEAR), int(OrderSide.NO_ORDER_SIDE), 0.0, 0.0)
            ]
            for price in sorted(bids):
                rows.append((int(BookAction.ADD), int(OrderSide.BUY), price, bids[price]))
            for price in sorted(asks, reverse=True):
                rows.append((int(BookAction.ADD), int(OrderSide.SELL), price, asks[price]))
            return rows

        def _emit_ladder(ts_ns: int) -> None:
            nonlocal emitted_any
            if not bids and not asks:
                return
            _emit(_ladder_rows(), ts_ns, seq_base=0)
            emitted_any = True

        pending_deltas: list[tuple[int, int, float, float]] = []
        pending_ts_ns: int | None = None

        def _flush_pending() -> None:
            nonlocal pending_deltas, pending_ts_ns, emitted_any
            if pending_deltas and pending_ts_ns is not None:
                _emit(pending_deltas, pending_ts_ns, seq_base=1)
                emitted_any = True
            pending_deltas = []
            pending_ts_ns = None

        # Served day files can carry short out-of-order bursts (measured up to
        # ~400ms of backwards timestamps on reseed-heavy days), and the engine
        # replays records in timestamp order, so restore exchange-clock order
        # here; the stable sort keeps the served order of same-millisecond
        # events.
        events = sorted(events, key=lambda event: int(event["t"]))

        for event in events:
            ts_ns = int(event["t"]) * 1_000_000
            if ts_ns > end_ns:
                break
            in_window = ts_ns >= start_ns
            kind = event["type"]

            if kind == "trade":
                if in_window:
                    price = float(event["price"])
                    side = str(event["side"])
                    if invert:
                        price = 1.0 - price
                        side = _FLIP_SIDE.get(side, side)
                    trades.append(
                        (
                            round(price, price_precision),
                            round(float(event["size"]), size_precision),
                            side,
                            str(event["id"]),
                            ts_ns,
                        )
                    )
                continue

            if in_window and seeded and not emitted_any:
                _emit_ladder(ts_ns)

            if kind == "snapshot":
                _flush_pending()
                raw_bids = event.get("asks") if invert else event.get("bids")
                raw_asks = event.get("bids") if invert else event.get("asks")
                new_bids = self._parse_levels(
                    raw_bids,
                    invert=invert,
                    price_precision=price_precision,
                    size_precision=size_precision,
                )
                new_asks = self._parse_levels(
                    raw_asks,
                    invert=invert,
                    price_precision=price_precision,
                    size_precision=size_precision,
                )
                if not in_window or not seeded:
                    bids, asks = new_bids, new_asks
                    seeded = True
                    if in_window:
                        _emit_ladder(ts_ns)
                    continue
                # Snapshot against a live book: emit only the corrections.
                # An intact snapshot+delta chain diffs to nothing; feed
                # reseeds and skipped pre-window deltas heal here. An empty
                # snapshot is a real empty book, not missing data (the
                # collector records empty books once a book has existed),
                # and diffs to deletions of every live level.
                rows: list[tuple[int, int, float, float]] = []
                for state, new_state, side in (
                    (bids, new_bids, int(OrderSide.BUY)),
                    (asks, new_asks, int(OrderSide.SELL)),
                ):
                    for price in state:
                        if price not in new_state:
                            rows.append((int(BookAction.DELETE), side, price, 0.0))
                    for price, size in new_state.items():
                        if state.get(price) != size:
                            rows.append((int(BookAction.UPDATE), side, price, size))
                bids, asks = new_bids, new_asks
                if rows:
                    _emit(rows, ts_ns, seq_base=1)
                    emitted_any = True
                continue

            # delta
            price = float(event["price"])
            side_name = str(event["side"])
            if invert:
                price = 1.0 - price
                side_name = _FLIP_SIDE.get(side_name, side_name)
            price = round(price, price_precision)
            size = round(float(event["size"]), size_precision)
            state = bids if side_name == "BUY" else asks
            side = int(OrderSide.BUY) if side_name == "BUY" else int(OrderSide.SELL)
            if size <= 0.0:
                if price not in state:
                    continue
                del state[price]
                row = (int(BookAction.DELETE), side, price, 0.0)
            else:
                state[price] = size
                row = (int(BookAction.UPDATE), side, price, size)
            if not in_window:
                continue
            if pending_ts_ns is not None and pending_ts_ns != ts_ns:
                _flush_pending()
            pending_ts_ns = ts_ns
            pending_deltas.append(row)

        _flush_pending()
        return records, trades

    def _trade_ticks_from_rows(
        self, rows: Sequence[tuple[float, float, str, str, int]]
    ) -> tuple[TradeTick, ...]:
        if not rows:
            return ()
        price_precision, size_precision = self._instrument_precisions()
        ordered = sorted(rows, key=lambda row: row[4])
        aggressor_map = {"BUY": 1, "SELL": 2}
        return tuple(
            TradeTick.from_raw_arrays_to_list(
                self.instrument.id,
                price_precision,
                size_precision,
                np.round(
                    np.asarray([row[0] for row in ordered], dtype=np.float64), price_precision
                ),
                np.round(np.asarray([row[1] for row in ordered], dtype=np.float64), size_precision),
                np.asarray([aggressor_map.get(row[2], 0) for row in ordered], dtype=np.uint8),
                [row[3] for row in ordered],
                np.asarray([row[4] for row in ordered], dtype=np.uint64),
                np.asarray([row[4] for row in ordered], dtype=np.uint64),
            )
        )

    def _materialized_cache_path(
        self,
        *,
        subdir: str,
        date: str,
        market_slug: str,
        token_index: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Path | None:
        cache_root = _resolve_cache_root()
        if cache_root is None:
            return None
        instrument_key = sha256(str(self.instrument.id).encode("utf-8")).hexdigest()[:16]
        start_ns = int(self._normalize_to_utc(start).value)
        end_ns = int(self._normalize_to_utc(end).value)
        return (
            cache_root
            / subdir
            / "polymarket"
            / market_slug
            / str(token_index)
            / f"instrument={instrument_key}"
            / f"{date}.{start_ns}-{end_ns}.parquet"
        )

    @staticmethod
    def _deltas_records_to_table(records: Sequence[OrderBookDeltas]) -> pa.Table:
        event_indexes: list[int] = []
        actions: list[int] = []
        sides: list[int] = []
        prices: list[float] = []
        sizes: list[float] = []
        flags: list[int] = []
        sequences: list[int] = []
        ts_events: list[int] = []
        ts_inits: list[int] = []
        for event_index, record in enumerate(records):
            for delta in record.deltas:
                event_indexes.append(event_index)
                actions.append(int(delta.action))
                sides.append(int(delta.order.side))
                prices.append(float(delta.order.price))
                sizes.append(float(delta.order.size))
                flags.append(int(delta.flags))
                sequences.append(int(delta.sequence))
                ts_events.append(int(delta.ts_event))
                ts_inits.append(int(delta.ts_init))
        return pa.table(
            {
                "event_index": pa.array(event_indexes, pa.int32()),
                "action": pa.array(actions, pa.uint8()),
                "side": pa.array(sides, pa.uint8()),
                "price": pa.array(prices, pa.float64()),
                "size": pa.array(sizes, pa.float64()),
                "flags": pa.array(flags, pa.uint8()),
                "sequence": pa.array(sequences, pa.int32()),
                "ts_event": pa.array(ts_events, pa.int64()),
                "ts_init": pa.array(ts_inits, pa.int64()),
            }
        )

    def _deltas_records_from_table(self, table: pa.Table) -> list[OrderBookDeltas]:
        def _column(name: str) -> np.ndarray:
            return table.column(name).combine_chunks().to_numpy(zero_copy_only=False)

        event_indexes = _column("event_index")
        actions = _column("action")
        sides = _column("side")
        flags = _column("flags")
        sequences = _column("sequence")
        ts_events = _column("ts_event")
        ts_inits = _column("ts_init")
        instrument_id = self.instrument.id
        price_precision, size_precision = self._instrument_precisions()
        price_raws = fixed_raw_values(_column("price"), price_precision)
        size_raws = fixed_raw_values(_column("size"), size_precision)

        records: list[OrderBookDeltas] = []
        current_event_index: int | None = None
        deltas: list[OrderBookDelta] = []
        for idx, raw_event_index in enumerate(event_indexes):
            event_index = int(raw_event_index)
            if current_event_index is None:
                current_event_index = event_index
            elif event_index != current_event_index:
                records.append(OrderBookDeltas(instrument_id, deltas))
                deltas = []
                current_event_index = event_index
            deltas.append(
                OrderBookDelta.from_raw(
                    instrument_id,
                    int(actions[idx]),
                    int(sides[idx]),
                    price_raws[idx],
                    price_precision,
                    size_raws[idx],
                    size_precision,
                    0,
                    flags=int(flags[idx]),
                    sequence=int(sequences[idx]),
                    ts_event=int(ts_events[idx]),
                    ts_init=int(ts_inits[idx]),
                )
            )
        if deltas:
            records.append(OrderBookDeltas(instrument_id, deltas))
        return records

    @staticmethod
    def _trade_ticks_to_cache_table(records: Sequence[TradeTick]) -> pa.Table:
        return pa.table(
            {
                "price": pa.array([float(record.price) for record in records], pa.float64()),
                "size": pa.array([float(record.size) for record in records], pa.float64()),
                "aggressor_side": pa.array(
                    [
                        getattr(record.aggressor_side, "name", str(record.aggressor_side))
                        for record in records
                    ],
                    pa.string(),
                ),
                "trade_id": pa.array([str(record.trade_id) for record in records], pa.string()),
                "ts_event": pa.array([int(record.ts_event) for record in records], pa.int64()),
                "ts_init": pa.array([int(record.ts_init) for record in records], pa.int64()),
            }
        )

    def _trade_ticks_from_cache_table(self, table: pa.Table) -> tuple[TradeTick, ...]:
        frame = table.to_pandas()
        if frame.empty:
            return ()
        sorted_frame = frame.sort_values(["ts_event", "ts_init"], kind="stable")
        price_precision, size_precision = self._instrument_precisions()
        aggressor_sides = (
            sorted_frame["aggressor_side"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map({"BUYER": 1, "SELLER": 2})
            .fillna(0)
            .to_numpy(dtype=np.uint8)
        )
        return tuple(
            TradeTick.from_raw_arrays_to_list(
                self.instrument.id,
                price_precision,
                size_precision,
                np.round(sorted_frame["price"].to_numpy(dtype=np.float64), price_precision),
                np.round(sorted_frame["size"].to_numpy(dtype=np.float64), size_precision),
                aggressor_sides,
                sorted_frame["trade_id"].astype(str).tolist(),
                sorted_frame["ts_event"].to_numpy(dtype=np.uint64),
                sorted_frame["ts_init"].to_numpy(dtype=np.uint64),
            )
        )

    def _read_materialized_table(
        self, cache_path: Path | None, *, columns: Sequence[str], label: str
    ) -> pa.Table | None:
        if cache_path is None or not cache_path.exists():
            return None
        try:
            table = pq.read_table(cache_path, columns=list(columns))
            if not set(columns).issubset(set(table.schema.names)):
                raise ValueError(f"missing required {label} cache columns")
            return table
        except Exception as exc:  # noqa: BLE001 - stale/corrupt cache should self-heal
            try:
                cache_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Marketlens: ignored stale materialized {label} cache {cache_path} ({exc})",
                stacklevel=2,
            )
            return None

    def _write_materialized_table(self, cache_path: Path | None, table: pa.Table) -> None:
        if cache_path is None:
            return
        tmp_path = _unique_tmp_path(cache_path)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, tmp_path, compression="zstd")
            os.replace(tmp_path, cache_path)
        except Exception as exc:  # noqa: BLE001 - cache writes must not break replay
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Marketlens: failed to write materialized cache {cache_path} ({exc})",
                stacklevel=2,
            )

    def _load_book_day(
        self,
        *,
        date: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        market_slug: str,
        token_index: int,
    ) -> _MarketlensDayResult:
        self._day_progress(date, "start", "none", 0)
        day_source = "none"
        emitted_day_complete = False
        try:
            day_window = self._day_window(date, start=start, end=end)
            if day_window is None:
                self._day_progress(date, "complete", day_source, 0)
                emitted_day_complete = True
                return _MarketlensDayResult(date=date, records=[], source=day_source)
            day_start, day_end = day_window

            deltas_cache_path = self._materialized_cache_path(
                subdir=_MARKETLENS_DELTAS_CACHE_SUBDIR,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                start=day_start,
                end=day_end,
            )
            cached_table = self._read_materialized_table(
                deltas_cache_path,
                columns=_MARKETLENS_DELTAS_CACHE_COLUMN_ORDER,
                label="deltas",
            )
            if cached_table is not None:
                records = self._deltas_records_from_table(cached_table)
                del cached_table
                _release_arrow_memory()
                day_source = f"marketlens-deltas-cache::{deltas_cache_path}"
                self._day_progress(date, "complete", day_source, len(records))
                emitted_day_complete = True
                return _MarketlensDayResult(date=date, records=records, source=day_source)

            events, day_source = self._load_history_day(date=date)
            records, trade_rows = self._convert_history_events(
                events,
                start_ns=int(day_start.value),
                end_ns=int(day_end.value),
                token_index=token_index,
            )
            self._write_materialized_table(
                deltas_cache_path, self._deltas_records_to_table(records)
            )
            trade_cache_path = self._materialized_cache_path(
                subdir=_MARKETLENS_TRADE_TICKS_CACHE_SUBDIR,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                start=day_start,
                end=day_end,
            )
            ticks = self._trade_ticks_from_rows(trade_rows)
            self._write_materialized_table(
                trade_cache_path, self._trade_ticks_to_cache_table(ticks)
            )
            # The adapter loads deltas then trades over the same window, so
            # keep the day's ticks in memory; the trades pass must not refetch
            # and re-bill the day when the cache is disabled or failed to
            # write.
            self._marketlens_day_trade_ticks[(date, int(day_start.value), int(day_end.value))] = (
                ticks
            )
            _release_arrow_memory()
            self._day_progress(date, "complete", day_source, len(records))
            emitted_day_complete = True
            return _MarketlensDayResult(date=date, records=records, source=day_source)
        finally:
            if not emitted_day_complete:
                self._day_progress(date, "complete", day_source, 0)

    def _iter_loaded_days(
        self,
        *,
        dates: list[str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        market_slug: str,
        token_index: int,
    ) -> Iterator[_MarketlensDayResult]:
        max_workers = min(
            getattr(self, "_marketlens_prefetch_workers", _resolve_prefetch_workers()),
            len(dates),
        )
        if max_workers <= 1:
            for date in dates:
                yield self._load_book_day(
                    date=date,
                    start=start,
                    end=end,
                    market_slug=market_slug,
                    token_index=token_index,
                )
            return

        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="marketlens-day"
        ) as pool:
            futures: dict[str, Future[_MarketlensDayResult]] = {}
            next_index = 0

            def _submit_next() -> None:
                nonlocal next_index
                if next_index >= len(dates):
                    return
                date = dates[next_index]
                next_index += 1
                futures[date] = pool.submit(
                    self._load_book_day,
                    date=date,
                    start=start,
                    end=end,
                    market_slug=market_slug,
                    token_index=token_index,
                )

            for _ in range(max_workers):
                _submit_next()

            for date in dates:
                result = futures.pop(date).result()
                _submit_next()
                yield result

    def load_order_book_deltas(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        market_slug: str,
        token_index: int,
        outcome: str | None = None,
    ) -> list[OrderBookDeltas]:
        records: list[OrderBookDeltas] = []
        for result in self._iter_loaded_days(
            dates=self._date_range(start, end),
            start=start,
            end=end,
            market_slug=market_slug,
            token_index=token_index,
        ):
            records.extend(result.records)
        return records

    def load_marketlens_trade_ticks(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        market_slug: str | None = None,
        token_index: int | None = None,
    ) -> tuple[TradeTick, ...]:
        resolved_market_slug = str(
            market_slug or getattr(self, "_marketlens_market_slug", None) or ""
        )
        resolved_token_index = (
            int(token_index)
            if token_index is not None
            else int(getattr(self, "_marketlens_token_index", 0))
        )
        all_trades: list[TradeTick] = []
        for date in self._date_range(start, end):
            day_window = self._day_window(date, start=start, end=end)
            if day_window is None:
                continue
            day_start, day_end = day_window
            memoized = self._marketlens_day_trade_ticks.get(
                (date, int(day_start.value), int(day_end.value))
            )
            if memoized is not None:
                all_trades.extend(memoized)
                continue
            trade_cache_path = self._materialized_cache_path(
                subdir=_MARKETLENS_TRADE_TICKS_CACHE_SUBDIR,
                date=date,
                market_slug=resolved_market_slug,
                token_index=resolved_token_index,
                start=day_start,
                end=day_end,
            )
            cached_table = self._read_materialized_table(
                trade_cache_path,
                columns=_MARKETLENS_TRADE_TICKS_CACHE_COLUMN_ORDER,
                label="trade tick",
            )
            if cached_table is not None:
                all_trades.extend(self._trade_ticks_from_cache_table(cached_table))
                del cached_table
                _release_arrow_memory()
                continue
            events, _source = self._load_history_day(date=date)
            _records, trade_rows = self._convert_history_events(
                events,
                start_ns=int(day_start.value),
                end_ns=int(day_end.value),
                token_index=resolved_token_index,
            )
            ticks = self._trade_ticks_from_rows(trade_rows)
            self._write_materialized_table(
                trade_cache_path, self._trade_ticks_to_cache_table(ticks)
            )
            self._marketlens_day_trade_ticks[(date, int(day_start.value), int(day_end.value))] = (
                ticks
            )
            all_trades.extend(ticks)
        all_trades.sort(key=lambda tick: (int(tick.ts_event), int(tick.ts_init)))
        return tuple(all_trades)


__all__ = [
    "MARKETLENS_API_KEY_ENV",
    "MARKETLENS_API_WORKERS_ENV",
    "MARKETLENS_BASE_URL_ENV",
    "MARKETLENS_CACHE_ROOT_ENV",
    "MARKETLENS_PREFETCH_WORKERS_ENV",
    "MarketlensDataSourceSelection",
    "MarketlensLoaderConfig",
    "RunnerPolymarketMarketlensBookDataLoader",
    "configured_marketlens_data_source",
    "resolve_marketlens_data_source_selection",
    "resolve_marketlens_loader_config",
]
