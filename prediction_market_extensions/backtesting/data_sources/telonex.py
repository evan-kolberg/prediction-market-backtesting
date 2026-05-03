from __future__ import annotations

import os
import re
import threading
import warnings
from collections.abc import Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from nautilus_trader.adapters.polymarket.schemas.book import (
    PolymarketBookLevel,
    PolymarketBookSnapshot,
)
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import RecordFlag
from nautilus_trader.model.identifiers import TradeId

from prediction_market_extensions._native import (
    telonex_api_cache_relative_path,
    telonex_api_url,
    telonex_deltas_cache_relative_path,
    telonex_flat_book_snapshot_diff_rows,
    telonex_local_consolidated_candidate_paths,
    telonex_local_daily_candidate_paths,
    telonex_source_days_for_window_ns,
    telonex_source_label_kind,
    telonex_stage_for_source,
    telonex_trade_ticks_cache_relative_path,
)
from prediction_market_extensions._runtime_log import emit_loader_event
from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from prediction_market_extensions.backtesting.data_sources._common import (
    DISABLED_ENV_VALUES,
    normalize_local_path,
    normalize_urlish,
)

TELONEX_API_KEY_ENV = "TELONEX_API_KEY"
TELONEX_API_BASE_URL_ENV = "TELONEX_API_BASE_URL"
TELONEX_LOCAL_DIR_ENV = "TELONEX_LOCAL_DIR"
TELONEX_CHANNEL_ENV = "TELONEX_CHANNEL"
TELONEX_CACHE_ROOT_ENV = "TELONEX_CACHE_ROOT"
TELONEX_PREFETCH_WORKERS_ENV = "TELONEX_PREFETCH_WORKERS"

_TELONEX_DEFAULT_API_BASE_URL = "https://api.telonex.io"
TELONEX_FULL_BOOK_CHANNEL = "book_snapshot_full"
TELONEX_ONCHAIN_FILLS_CHANNEL = "onchain_fills"
TELONEX_TRADES_CHANNEL = "trades"
_POLYMARKET_PUBLIC_TRADES_API_URL = "https://data-api.polymarket.com/trades"
_TELONEX_DEFAULT_CHANNEL = TELONEX_FULL_BOOK_CHANNEL
_TELONEX_EXCHANGE = "polymarket"
_TELONEX_HTTP_TIMEOUT_SECS = 60
_TELONEX_DEFAULT_PREFETCH_WORKERS = 128
_TELONEX_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
_TELONEX_USER_AGENT = "prediction-market-backtesting/1.0"
_TELONEX_LOCAL_PREFIX = "local:"
_TELONEX_API_PREFIX = "api:"
_TELONEX_SOURCE_LOCAL = "local"
_TELONEX_SOURCE_API = "api"
_TELONEX_BLOB_DB_FILENAME = "telonex.duckdb"
_TELONEX_DATA_SUBDIR = "data"
_TELONEX_CACHE_SUBDIR = "api-days"
_TELONEX_DELTAS_CACHE_SUBDIR = "book-deltas-v1"
_TELONEX_TRADE_TICKS_CACHE_SUBDIR = "trade-ticks-v1"
_TELONEX_DELTAS_CACHE_COLUMNS = frozenset(
    {
        "event_index",
        "action",
        "side",
        "price",
        "size",
        "flags",
        "sequence",
        "ts_event",
        "ts_init",
    }
)
_TELONEX_TRADE_TICKS_CACHE_COLUMNS = frozenset(
    {
        "price",
        "size",
        "aggressor_side",
        "trade_id",
        "ts_event",
        "ts_init",
    }
)
_TELONEX_TRADE_TICK_CHANNELS = (TELONEX_ONCHAIN_FILLS_CHANNEL, TELONEX_TRADES_CHANNEL)


@dataclass(frozen=True)
class TelonexSourceEntry:
    kind: str
    target: str | None = None
    api_key: str | None = None


@dataclass(frozen=True)
class TelonexLoaderConfig:
    channel: str
    ordered_source_entries: tuple[TelonexSourceEntry, ...]


@dataclass(frozen=True)
class TelonexDataSourceSelection:
    mode: str
    summary: str


@dataclass
class _TelonexDayResult:
    date: str
    records: list[OrderBookDeltas]
    source: str


_CURRENT_TELONEX_LOADER_CONFIG: ContextVar[TelonexLoaderConfig | None] = ContextVar(
    "telonex_loader_config", default=None
)


def _current_loader_config() -> TelonexLoaderConfig | None:
    return _CURRENT_TELONEX_LOADER_CONFIG.get()


def _env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped.casefold() in DISABLED_ENV_VALUES:
        return None
    return stripped


def _resolve_channel(channel: str | None = None) -> str:
    return (channel or _env_value(TELONEX_CHANNEL_ENV) or _TELONEX_DEFAULT_CHANNEL).casefold()


def _default_cache_root() -> Path:
    configured = os.getenv("XDG_CACHE_HOME")
    cache_home = Path(configured).expanduser() if configured else Path.home() / ".cache"
    return cache_home / "nautilus_trader" / "telonex"


def _resolve_api_cache_root() -> Path | None:
    configured = os.getenv(TELONEX_CACHE_ROOT_ENV)
    if configured is None:
        return _default_cache_root()
    value = configured.strip()
    if value.casefold() in DISABLED_ENV_VALUES:
        return None
    return Path(value).expanduser()


def _normalize_api_base_url(value: str | None) -> str:
    if value is None or not value.strip():
        return _env_value(TELONEX_API_BASE_URL_ENV) or _TELONEX_DEFAULT_API_BASE_URL
    return normalize_urlish(value)


_UNEXPANDED_VAR_PATTERN = re.compile(r"\$\{[^}]+\}|\$[A-Za-z_][A-Za-z0-9_]*")


def _expand_source_vars(source: str) -> str:
    """Expand ${VAR} / $VAR references against the current environment.

    Any references that remain unresolved after expansion are stripped so the
    classifier sees an empty remainder rather than a literal placeholder.
    """
    expanded = os.path.expandvars(source)
    return _UNEXPANDED_VAR_PATTERN.sub("", expanded)


def _classify_telonex_sources(sources: Sequence[str]) -> tuple[TelonexSourceEntry, ...]:
    entries: list[TelonexSourceEntry] = []
    for raw_source in sources:
        expanded = _expand_source_vars(str(raw_source))
        stripped = expanded.strip()
        if not stripped:
            continue
        folded = stripped.casefold()
        if folded.startswith(_TELONEX_LOCAL_PREFIX):
            remainder = stripped[len(_TELONEX_LOCAL_PREFIX) :].strip()
            if not remainder:
                raise ValueError(f"Telonex explicit source {raw_source!r} is missing a local path.")
            entries.append(
                TelonexSourceEntry(
                    kind=_TELONEX_SOURCE_LOCAL, target=normalize_local_path(remainder)
                )
            )
            continue
        if folded.startswith(_TELONEX_API_PREFIX):
            remainder = stripped[len(_TELONEX_API_PREFIX) :].strip()
            base_url: str | None = None
            api_key: str | None = None
            if remainder:
                if remainder.lower().startswith(("http://", "https://")):
                    base_url = remainder
                else:
                    api_key = remainder
            entries.append(
                TelonexSourceEntry(
                    kind=_TELONEX_SOURCE_API,
                    target=_normalize_api_base_url(base_url),
                    api_key=api_key,
                )
            )
            continue
        raise ValueError(
            f"Unsupported Telonex explicit source {stripped!r}. Use one of: local:, api:."
        )
    if not entries:
        raise ValueError("Telonex requires at least one source. Use local:/path or api:.")
    return tuple(entries)


def _default_telonex_sources_from_env() -> tuple[TelonexSourceEntry, ...]:
    local_dir = _env_value(TELONEX_LOCAL_DIR_ENV)
    if local_dir is not None:
        return (
            TelonexSourceEntry(kind=_TELONEX_SOURCE_LOCAL, target=normalize_local_path(local_dir)),
        )
    env_key = _env_value(TELONEX_API_KEY_ENV)
    if env_key is not None:
        return (
            TelonexSourceEntry(
                kind=_TELONEX_SOURCE_API,
                target=_normalize_api_base_url(None),
                api_key=env_key,
            ),
        )
    raise ValueError(
        "Telonex requires DATA.sources with local:/path or api:. "
        f"Set {TELONEX_API_KEY_ENV} only when intentionally using api:."
    )


def _source_summary_parts(entries: Sequence[TelonexSourceEntry]) -> list[str]:
    parts: list[str] = ["cache"] if _resolve_api_cache_root() is not None else []
    for entry in entries:
        if entry.kind == _TELONEX_SOURCE_LOCAL:
            parts.append(f"local {entry.target}")
        elif entry.kind == _TELONEX_SOURCE_API:
            suffix = " (key set)" if entry.api_key else " (key missing)"
            parts.append(f"api {entry.target}{suffix}")
    return parts


def _source_summary_line(label: str, parts: Sequence[str]) -> str:
    return f"Telonex {label} source: explicit priority (" + " -> ".join(parts) + ")"


def _trade_source_summary_parts(entries: Sequence[TelonexSourceEntry]) -> list[str]:
    parts: list[str] = []
    api_entries = [entry for entry in entries if entry.kind == _TELONEX_SOURCE_API]
    if api_entries and _resolve_api_cache_root() is not None:
        parts.append("cache")
    parts.extend(
        f"local {entry.target}" for entry in entries if entry.kind == _TELONEX_SOURCE_LOCAL
    )
    for entry in api_entries:
        suffix = " (key set)" if entry.api_key else " (key missing)"
        parts.append(f"api {entry.target}{suffix}")
    parts.extend(("polymarket cache", f"api {_POLYMARKET_PUBLIC_TRADES_API_URL}"))
    return parts


def _source_summary(entries: Sequence[TelonexSourceEntry]) -> str:
    book_parts = _source_summary_parts(entries)
    trade_parts = _trade_source_summary_parts(entries)
    return "\n".join(
        (
            _source_summary_line("book", book_parts),
            _source_summary_line("trade", trade_parts),
        )
    )


def resolve_telonex_loader_config(
    *, sources: Sequence[str] | None = None, channel: str | None = None
) -> tuple[TelonexDataSourceSelection, TelonexLoaderConfig]:
    if sources is None and channel is None:
        current_config = _current_loader_config()
        if current_config is not None:
            return (
                TelonexDataSourceSelection(
                    mode="auto",
                    summary=_source_summary(current_config.ordered_source_entries),
                ),
                current_config,
            )
    entries = _classify_telonex_sources(sources) if sources else _default_telonex_sources_from_env()
    return (
        TelonexDataSourceSelection(mode="auto", summary=_source_summary(entries)),
        TelonexLoaderConfig(channel=_resolve_channel(channel), ordered_source_entries=entries),
    )


def resolve_telonex_data_source_selection(
    *, sources: Sequence[str] | None = None
) -> tuple[TelonexDataSourceSelection, dict[str, str | None]]:
    selection, _config = resolve_telonex_loader_config(sources=sources)
    return selection, {}


@contextmanager
def configured_telonex_data_source(
    *, sources: Sequence[str] | None = None, channel: str | None = None
) -> Iterator[TelonexDataSourceSelection]:
    selection, config = resolve_telonex_loader_config(sources=sources, channel=channel)
    token = _CURRENT_TELONEX_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_TELONEX_LOADER_CONFIG.reset(token)


class RunnerPolymarketTelonexBookDataLoader(PolymarketDataLoader):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._ensure_blob_scan_caches()
        self._telonex_prefetch_workers = self._resolve_prefetch_workers()

    def _ensure_blob_scan_caches(self) -> None:
        if not hasattr(self, "_telonex_blob_scan_lock"):
            self._telonex_blob_scan_lock = threading.RLock()
        if not hasattr(self, "_telonex_readable_blob_parts"):
            self._telonex_readable_blob_parts: dict[Path, tuple[tuple[str, ...], bool]] = {}
        if not hasattr(self, "_telonex_unreadable_blob_parts_warned"):
            self._telonex_unreadable_blob_parts_warned: set[Path] = set()
        if not hasattr(self, "_telonex_incomplete_blob_partitions_warned"):
            self._telonex_incomplete_blob_partitions_warned: set[Path] = set()
        # Memoize blob-range frames keyed by (store_root, channel, market, token,
        # outcome, start_ym, end_ym). The pyarrow query already prunes by
        # year/month, so two callers with start/end inside the same month
        # window hit the same data. None is cached too so we don't retry
        # empty stores.
        if not hasattr(self, "_telonex_blob_range_frames"):
            self._telonex_blob_range_frames: dict[
                tuple[str, str, str, int, str | None, int, int], pd.DataFrame | None
            ] = {}
        # Cache the nanosecond timestamp array derived from each memoized
        # blob frame so _column_to_ns() is not called N times for an N-day
        # range on the same month-sized frame.
        if not hasattr(self, "_telonex_blob_ts_ns"):
            self._telonex_blob_ts_ns: dict[
                tuple[str, str, str, int, str | None, int, int], np.ndarray
            ] = {}

    @classmethod
    async def from_market_slug(
        cls, slug: str, token_index: int = 0, http_client=None
    ) -> "RunnerPolymarketTelonexBookDataLoader":  # type: ignore[override]
        loader = await super().from_market_slug(
            slug=slug,
            token_index=token_index,
            http_client=http_client,
        )
        loader._telonex_market_slug = slug
        loader._telonex_token_index = token_index
        loader._telonex_outcome = str(loader.instrument.outcome or "") or None
        return loader

    def _download_progress(
        self, url: str, downloaded_bytes: int, total_bytes: int | None, finished: bool
    ) -> None:
        callback = getattr(self, "_telonex_download_progress_callback", None)
        if callback is not None:
            callback(url, downloaded_bytes, total_bytes, finished)

    @staticmethod
    def _telonex_source_kind(source: str) -> str | None:
        return telonex_source_label_kind(source)

    @staticmethod
    def _telonex_stage_for_source(source: str) -> str:
        return telonex_stage_for_source(source)

    def _day_progress(self, date: str, event: str, source: str, rows: int) -> None:
        status = "start"
        if event == "complete":
            if source == "none" and rows == 0:
                status = "skip"
            elif "cache" in source:
                status = "cache_hit"
            else:
                status = "complete"
        emit_loader_event(
            f"Telonex day {event} for {date}: {rows} rows from {source}",
            level="INFO",
            stage=self._telonex_stage_for_source(source),
            status=status,
            vendor="telonex",
            platform="polymarket",
            data_type="book",
            source_kind=self._telonex_source_kind(source),
            source=None if source == "none" else source,
            market_slug=getattr(self, "_telonex_market_slug", None),
            token_id=str(getattr(self, "_telonex_token_index", "")),
            outcome=getattr(self, "_telonex_outcome", None),
            rows=rows,
            attrs={"date": date, "event": event},
        )
        callback = getattr(self, "_telonex_day_progress_callback", None)
        if callback is not None:
            callback(date, event, source, rows)

    @classmethod
    def _resolve_api_cache_root(cls) -> Path | None:
        return _resolve_api_cache_root()

    @classmethod
    def _resolve_prefetch_workers(cls) -> int:
        configured = _env_value(TELONEX_PREFETCH_WORKERS_ENV)
        if configured is None:
            return _TELONEX_DEFAULT_PREFETCH_WORKERS
        try:
            return max(1, int(configured))
        except ValueError:
            return _TELONEX_DEFAULT_PREFETCH_WORKERS

    def _config(self) -> TelonexLoaderConfig:
        config = _current_loader_config()
        if config is None:
            _selection, config = resolve_telonex_loader_config()
        return config

    @staticmethod
    def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
        start_utc = RunnerPolymarketTelonexBookDataLoader._normalize_to_utc(start)
        end_utc = RunnerPolymarketTelonexBookDataLoader._normalize_to_utc(end)
        return telonex_source_days_for_window_ns(int(start_utc.value), int(end_utc.value))

    @staticmethod
    def _outcome_segments(*, token_index: int, outcome: str | None) -> tuple[str, ...]:
        outcome_parts = [f"outcome_id={token_index}", str(token_index)]
        if outcome:
            outcome_parts.insert(0, outcome)
        return tuple(outcome_parts)

    @staticmethod
    def _local_blob_root(root: Path) -> Path | None:
        """Return the root if it looks like a Telonex Parquet store.

        A valid store has a manifest DuckDB file and a populated ``data/``
        directory. We detect both; an empty data dir (e.g. first run) still
        returns None so the caller falls back to API / daily-parquet paths.
        """
        manifest = root / _TELONEX_BLOB_DB_FILENAME
        data_dir = root / _TELONEX_DATA_SUBDIR
        if not manifest.exists() or not data_dir.exists():
            return None
        return root

    @staticmethod
    def _outcome_segment_candidates(*, token_index: int, outcome: str | None) -> tuple[str, ...]:
        segments = [f"outcome_id={token_index}", str(token_index)]
        if outcome:
            segments.insert(0, outcome)
        return tuple(segments)

    @staticmethod
    def _month_partition_dirs(
        *, channel_dir: Path, start: pd.Timestamp, end: pd.Timestamp
    ) -> tuple[Path, ...]:
        cursor = start.floor("D").replace(day=1)
        final = end.floor("D").replace(day=1)
        dirs: list[Path] = []
        while cursor <= final:
            dirs.append(channel_dir / f"year={cursor.year}" / f"month={cursor.month:02d}")
            cursor += pd.DateOffset(months=1)
        return tuple(dirs)

    def _readable_blob_part_paths(
        self, *, channel_dir: Path, start: pd.Timestamp, end: pd.Timestamp
    ) -> tuple[list[str], bool]:
        self._ensure_blob_scan_caches()
        paths: list[str] = []
        incomplete = False
        for partition_dir in self._month_partition_dirs(
            channel_dir=channel_dir,
            start=start,
            end=end,
        ):
            with self._telonex_blob_scan_lock:
                if partition_dir not in self._telonex_readable_blob_parts:
                    self._telonex_readable_blob_parts[partition_dir] = (
                        self._scan_readable_blob_part_paths(partition_dir)
                    )
                partition_paths, partition_incomplete = self._telonex_readable_blob_parts[
                    partition_dir
                ]
            if partition_incomplete:
                incomplete = True
                with self._telonex_blob_scan_lock:
                    if partition_dir in self._telonex_incomplete_blob_partitions_warned:
                        already_warned = True
                    else:
                        already_warned = False
                        self._telonex_incomplete_blob_partitions_warned.add(partition_dir)
                if not already_warned:
                    warnings.warn(
                        "Telonex: local blob partition "
                        f"{partition_dir} has unreadable part files; trying next "
                        "source to avoid partial local data.",
                        stacklevel=2,
                    )
            paths.extend(partition_paths)
        return paths, incomplete

    def _scan_readable_blob_part_paths(self, partition_dir: Path) -> tuple[tuple[str, ...], bool]:
        if not partition_dir.exists():
            return (), False

        paths: list[str] = []
        incomplete = False
        for path in sorted(partition_dir.glob("*.parquet")):
            # File-size check replaces pq.read_metadata() — reading every
            # parquet footer on a slow external disk was the dominant cost
            # before memoization kicked in.  A non-zero .parquet file on
            # disk is almost certainly readable; corrupted files will fail
            # at actual read time and are caught there.
            try:
                if path.stat().st_size <= 0:
                    raise OSError("empty file")
            except (OSError, ValueError):
                incomplete = True
                with self._telonex_blob_scan_lock:
                    if path in self._telonex_unreadable_blob_parts_warned:
                        already_warned = True
                    else:
                        already_warned = False
                        self._telonex_unreadable_blob_parts_warned.add(path)
                if not already_warned:
                    warnings.warn(
                        f"Telonex: parquet part is not readable yet: {path}; "
                        "trying next source instead of using partial local data.",
                        stacklevel=2,
                    )
                continue
            paths.append(str(path))
        return tuple(paths), incomplete

    def _manifest_blob_part_paths(
        self,
        *,
        store_root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> tuple[list[str], bool] | None:
        manifest = store_root / _TELONEX_BLOB_DB_FILENAME
        if not manifest.exists():
            return None

        query_start = self._normalize_to_utc(start).floor("D").date()
        query_end = self._normalize_to_utc(end).floor("D").date()
        segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)
        placeholders = ", ".join("?" for _ in segments)
        params: list[object] = [channel, market_slug, *segments, query_start, query_end]

        try:
            con = duckdb.connect(str(manifest), read_only=True)
            try:
                rows = con.execute(
                    "SELECT DISTINCT parquet_part FROM completed_days "
                    "WHERE channel = ? "
                    "AND market_slug = ? "
                    f"AND outcome_segment IN ({placeholders}) "
                    "AND day BETWEEN ? AND ? "
                    "AND rows > 0 "
                    "AND parquet_part IS NOT NULL "
                    "ORDER BY parquet_part",
                    params,
                ).fetchall()
            finally:
                con.close()
        except Exception:
            return None

        paths: list[str] = []
        incomplete = False
        for (raw_part,) in rows:
            if raw_part is None:
                continue
            part_path = Path(str(raw_part))
            if not part_path.is_absolute():
                part_path = store_root / part_path
            try:
                if part_path.stat().st_size <= 0:
                    raise OSError("empty file")
            except OSError:
                incomplete = True
                with self._telonex_blob_scan_lock:
                    if part_path in self._telonex_unreadable_blob_parts_warned:
                        already_warned = True
                    else:
                        already_warned = False
                        self._telonex_unreadable_blob_parts_warned.add(part_path)
                if not already_warned:
                    warnings.warn(
                        f"Telonex: manifest references unreadable parquet part {part_path}; "
                        "trying next source instead of using partial local data.",
                        stacklevel=2,
                    )
                continue
            paths.append(str(part_path))
        return paths, incomplete

    def _manifest_completed_row_count(
        self,
        *,
        store_root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        date: str,
    ) -> int | None:
        manifest = store_root / _TELONEX_BLOB_DB_FILENAME
        if not manifest.exists():
            return None

        segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)
        placeholders = ", ".join("?" for _ in segments)
        params: list[object] = [channel, market_slug, *segments, pd.Timestamp(date).date()]

        try:
            con = duckdb.connect(str(manifest), read_only=True)
            try:
                total_rows, completed_count = con.execute(
                    "SELECT COALESCE(SUM(rows), 0), COUNT(*) "
                    "FROM completed_days "
                    "WHERE channel = ? "
                    "AND market_slug = ? "
                    f"AND outcome_segment IN ({placeholders}) "
                    "AND day = ?",
                    params,
                ).fetchone()
            finally:
                con.close()
        except Exception:
            return None
        if int(completed_count or 0) <= 0:
            return None
        return int(total_rows or 0)

    def _manifest_empty_day_exists(
        self,
        *,
        store_root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        date: str,
    ) -> bool:
        manifest = store_root / _TELONEX_BLOB_DB_FILENAME
        if not manifest.exists():
            return False

        segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)
        placeholders = ", ".join("?" for _ in segments)
        params: list[object] = [channel, market_slug, *segments, pd.Timestamp(date).date()]

        try:
            con = duckdb.connect(str(manifest), read_only=True)
            try:
                (empty_count,) = con.execute(
                    "SELECT COUNT(*) "
                    "FROM empty_days "
                    "WHERE channel = ? "
                    "AND market_slug = ? "
                    f"AND outcome_segment IN ({placeholders}) "
                    "AND day = ?",
                    params,
                ).fetchone()
            finally:
                con.close()
        except Exception:
            return False
        return int(empty_count or 0) > 0

    def _load_blob_range(
        self,
        *,
        store_root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame | None:
        """Query the Hive-partitioned Parquet layout for a single (market,
        outcome) slice. Returns None when the channel has no data on disk.

        Memoized by (store_root, channel, market, token, outcome, start_day,
        end_day).

        Uses pyarrow.dataset with predicate pushdown instead of DuckDB, which
        eliminates SQL engine overhead, fetch_df() bulk materialization, and
        the frame.drop() copy — the dominant costs that made local-cache reads
        slower than the API.
        """
        self._ensure_blob_scan_caches()

        start_utc = self._normalize_to_utc(start)
        end_utc = self._normalize_to_utc(end)
        start_day = start_utc.floor("D").date()
        end_day = end_utc.floor("D").date()
        cache_key = (
            str(store_root),
            channel,
            market_slug,
            token_index,
            outcome,
            start_day,
            end_day,
        )
        self._ensure_blob_scan_caches()
        with self._telonex_blob_scan_lock:
            if cache_key in self._telonex_blob_range_frames:
                # Downstream consumers only `.to_numpy()` slices off the frame,
                # never mutate it, so returning the cached reference is safe.
                return self._telonex_blob_range_frames[cache_key]

        channel_dir = store_root / _TELONEX_DATA_SUBDIR / f"channel={channel}"
        if not channel_dir.exists():
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = None
            return None
        manifest_parts = self._manifest_blob_part_paths(
            store_root=store_root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            start=start_utc,
            end=end_utc,
        )
        if manifest_parts is None:
            # Legacy/no-manifest fallback: glob every part file under this channel,
            # regardless of year/month partition depth. Empty-but-existing channel
            # dirs yield no matches - guard with a cheap pre-check.
            part_paths, incomplete_parts = self._readable_blob_part_paths(
                channel_dir=channel_dir,
                start=start_utc,
                end=end_utc,
            )
        else:
            part_paths, incomplete_parts = manifest_parts
        if incomplete_parts:
            return None
        if not part_paths:
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = None
            return None

        segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)

        try:
            # Build pyarrow.dataset with Hive partitioning from the
            # year=/month= directory structure.  Predicate pushdown prunes
            # partition dirs and row groups at scan time — no SQL engine,
            # no schema-inference tax, no fetch_df() bulk allocation.
            part_dataset = ds.dataset(
                part_paths,
                format="parquet",
                partitioning="hive",
            )

            # Build filter expression:
            #   market_slug = ? AND outcome_segment IN (segments)
            #   AND year/month/timestamp between start and end.
            # Hive partition columns are inferred from directory names —
            # pure-numeric dirs (year=2026) become int, others become string.
            # Check the schema and compare with the matching type.
            schema = part_dataset.schema
            year_field = schema.field("year")
            month_field = schema.field("month")
            year_is_int = pa.types.is_integer(year_field.type)
            month_is_int = pa.types.is_integer(month_field.type)

            ym_pairs: list = []
            cursor = start_utc.replace(day=1)
            final_ym = end_utc.year * 100 + end_utc.month
            while True:
                cur_ym = cursor.year * 100 + cursor.month
                if cur_ym > final_ym:
                    break
                if year_is_int and month_is_int:
                    ym_pairs.append(
                        (ds.field("year") == cursor.year) & (ds.field("month") == cursor.month)
                    )
                else:
                    ym_pairs.append(
                        (ds.field("year") == str(cursor.year))
                        & (ds.field("month") == f"{cursor.month:02d}")
                    )
                if cursor.month == 12:
                    cursor = cursor.replace(year=cursor.year + 1, month=1)
                else:
                    cursor = cursor.replace(month=cursor.month + 1)

            if len(ym_pairs) == 1:
                ym_expr = ym_pairs[0]
            else:
                ym_expr = ym_pairs[0]
                for extra in ym_pairs[1:]:
                    ym_expr = ym_expr | extra

            filter_expr = (
                (ds.field("market_slug") == market_slug)
                & ds.field("outcome_segment").isin(segments)
                & ym_expr
            )
            schema_names = set(schema.names)
            if "timestamp_us" in schema_names:
                start_us = int(start_utc.value // 1_000)
                end_us = int(end_utc.value // 1_000)
                filter_expr = (
                    filter_expr
                    & (ds.field("timestamp_us") >= start_us)
                    & (ds.field("timestamp_us") <= end_us)
                )
            elif "timestamp_ms" in schema_names:
                start_ms = int(start_utc.value // 1_000_000)
                end_ms = int(end_utc.value // 1_000_000)
                filter_expr = (
                    filter_expr
                    & (ds.field("timestamp_ms") >= start_ms)
                    & (ds.field("timestamp_ms") <= end_ms)
                )

            # Project out Hive partition columns at scan time — no
            # post-hoc frame.drop() copy needed.
            data_columns = [
                f.name
                for f in part_dataset.schema
                if f.name not in ("market_slug", "outcome_segment", "year", "month")
            ]
            scanner = part_dataset.scanner(
                columns=data_columns,
                filter=filter_expr,
            )
            table = scanner.to_table()
            if table.num_rows == 0:
                with self._telonex_blob_scan_lock:
                    self._telonex_blob_range_frames[cache_key] = None
                return None
            frame = table.to_pandas()
        except (pa.ArrowInvalid, pa.ArrowIOError, OSError, ValueError) as exc:
            warnings.warn(
                f"Telonex: skipping blob store {store_root} for {market_slug}/"
                f"{token_index} ({channel}) — pyarrow failed: {exc}",
                stacklevel=2,
            )
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = None
            return None

        if frame.empty:
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = None
                self._telonex_blob_ts_ns.pop(cache_key, None)
            return None
        # Pre-compute the nanosecond timestamp array for every possible
        # timestamp column name so per-day callers of _column_to_ns() can
        # reuse it instead of converting the same month-sized column N times.
        ts_ns_map: dict[str, np.ndarray] = {}
        for col_candidates in (("timestamp_us", "timestamp_ms", "timestamp", "time"),):
            for col_name in col_candidates:
                if col_name in frame.columns:
                    ts_ns_map[col_name] = self._column_to_ns(
                        frame[col_name],
                        col_name,
                    )
                    break
        if ts_ns_map:
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = frame
                self._telonex_blob_ts_ns[cache_key] = ts_ns_map
        else:
            with self._telonex_blob_scan_lock:
                self._telonex_blob_range_frames[cache_key] = frame
        return frame

    def _cached_ts_ns_for_frame(self, frame: pd.DataFrame, column_name: str) -> np.ndarray | None:
        """Return a pre-computed ts_ns array if *frame* is a memoized blob
        frame (same object identity) and *column_name* was cached."""
        self._ensure_blob_scan_caches()
        blob_frames = getattr(self, "_telonex_blob_range_frames", None)
        if blob_frames is None:
            return None
        blob_ts = getattr(self, "_telonex_blob_ts_ns", None)
        with self._telonex_blob_scan_lock:
            for key, cached_frame in blob_frames.items():
                if cached_frame is frame:
                    if blob_ts is not None:
                        ts_map = blob_ts.get(key)
                        if ts_map is not None:
                            return ts_map.get(column_name)
                    return None
        return None

    @classmethod
    def _local_consolidated_candidates(
        cls,
        *,
        root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> tuple[Path, ...]:
        return telonex_local_consolidated_candidate_paths(
            root=root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )

    @classmethod
    def _local_daily_candidates(
        cls,
        *,
        root: Path,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> tuple[Path, ...]:
        return telonex_local_daily_candidate_paths(
            root=root,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )

    def _local_consolidated_path(
        self,
        *,
        root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> Path | None:
        for path in self._local_consolidated_candidates(
            root=root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        ):
            if path.exists():
                return path
        return None

    def _local_path_for_day(
        self,
        *,
        root: Path,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> Path | None:
        for path in self._local_daily_candidates(
            root=root,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        ):
            if path.exists():
                return path
        return None

    @staticmethod
    def _safe_read_parquet(path: Path) -> pd.DataFrame | None:
        try:
            return pd.read_parquet(path)
        except (OSError, ValueError, RuntimeError) as exc:
            warnings.warn(
                f"Telonex: skipping unreadable parquet {path} ({exc})",
                stacklevel=2,
            )
            return None

    def _load_local_range(
        self,
        *,
        root: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        path = self._local_consolidated_path(
            root=root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if path is None:
            return None
        return self._safe_read_parquet(path)

    def _load_local_day(
        self,
        *,
        root: Path,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        path = self._local_path_for_day(
            root=root,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if path is None:
            return None
        return self._safe_read_parquet(path)

    @staticmethod
    def _api_url(
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> str:
        return telonex_api_url(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )

    @classmethod
    def _api_cache_path(
        cls,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> Path | None:
        cache_root = cls._resolve_api_cache_root()
        if cache_root is None:
            return None
        normalized_base_url = base_url.rstrip("/")
        base_url_key = sha256(normalized_base_url.encode("utf-8")).hexdigest()[:16]
        return cache_root / telonex_api_cache_relative_path(
            base_url_key=base_url_key,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )

    def _load_api_cache_day(
        self,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        cache_path = self._api_cache_path(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if cache_path is None or not cache_path.exists():
            return None
        frame = self._safe_read_parquet(cache_path)
        if frame is not None:
            return frame
        try:
            cache_path.unlink()
        except OSError:
            pass
        return None

    def _write_api_cache_day(
        self,
        *,
        payload: bytes,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> None:
        cache_path = self._api_cache_path(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if cache_path is None:
            return
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_bytes(payload)
            os.replace(tmp_path, cache_path)
        except OSError as exc:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: failed to write API cache {cache_path} ({exc})",
                stacklevel=2,
            )

    @classmethod
    def _fast_api_cache_path(
        cls,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> Path | None:
        cache_path = cls._api_cache_path(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if cache_path is None:
            return None
        return cache_path.parent / f"{cache_path.stem}.fast.parquet"

    def _load_fast_cache_day(
        self,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        fast_path = self._fast_api_cache_path(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if fast_path is None or not fast_path.exists():
            return None
        frame = self._safe_read_parquet(fast_path)
        if frame is not None:
            return frame
        try:
            fast_path.unlink()
        except OSError:
            pass
        return None

    def _write_fast_cache_day(
        self,
        *,
        frame: pd.DataFrame,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> None:
        fast_path = self._fast_api_cache_path(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if fast_path is None:
            return
        if "bids" not in frame.columns or "asks" not in frame.columns:
            return
        bid_prices_list: list[list[str]] = []
        bid_sizes_list: list[list[str]] = []
        ask_prices_list: list[list[str]] = []
        ask_sizes_list: list[list[str]] = []
        for bids_val in frame["bids"]:
            bp: list[str] = []
            bs: list[str] = []
            if bids_val is not None:
                for item in bids_val:
                    if isinstance(item, dict):
                        bp.append(str(item.get("price", "")))
                        bs.append(str(item.get("size", "")))
                    else:
                        bp.append(str(getattr(item, "price", "")))
                        bs.append(str(getattr(item, "size", "")))
            bid_prices_list.append(bp)
            bid_sizes_list.append(bs)
        for asks_val in frame["asks"]:
            ap: list[str] = []
            as_: list[str] = []
            if asks_val is not None:
                for item in asks_val:
                    if isinstance(item, dict):
                        ap.append(str(item.get("price", "")))
                        as_.append(str(item.get("size", "")))
                    else:
                        ap.append(str(getattr(item, "price", "")))
                        as_.append(str(getattr(item, "size", "")))
            ask_prices_list.append(ap)
            ask_sizes_list.append(as_)
        fast_frame = frame.drop(columns=["bids", "asks"]).copy()
        fast_frame["bid_prices"] = bid_prices_list
        fast_frame["bid_sizes"] = bid_sizes_list
        fast_frame["ask_prices"] = ask_prices_list
        fast_frame["ask_sizes"] = ask_sizes_list
        tmp_path = fast_path.with_name(f"{fast_path.name}.tmp.{os.getpid()}")
        try:
            fast_path.parent.mkdir(parents=True, exist_ok=True)
            fast_frame.to_parquet(tmp_path, compression="zstd", index=False)
            os.replace(tmp_path, fast_path)
        except OSError as exc:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: failed to write fast cache {fast_path} ({exc})",
                stacklevel=2,
            )

    def _load_api_day_cached(
        self,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> tuple[pd.DataFrame | None, str]:
        """Try fast cache, then slow cache (migrating lazily).

        Returns ``(frame, source_label)`` where *source_label* is ``"none"``
        when no cached data was found.
        """
        fast_frame = self._load_fast_cache_day(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if fast_frame is not None:
            fast_path = self._fast_api_cache_path(
                base_url=base_url,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
            return fast_frame, (
                f"telonex-cache-fast::{fast_path}"
                if fast_path is not None
                else "telonex-cache-fast"
            )
        slow_frame = self._load_api_cache_day(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if slow_frame is not None:
            cache_path = self._api_cache_path(
                base_url=base_url,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
            try:
                self._write_fast_cache_day(
                    frame=slow_frame,
                    base_url=base_url,
                    channel=channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
            except Exception as exc:
                warnings.warn(
                    f"Telonex: failed to migrate slow cache to fast format ({exc})",
                    stacklevel=2,
                )
            return slow_frame, (
                f"telonex-cache::{cache_path}" if cache_path is not None else "telonex-cache"
            )
        return None, "none"

    @classmethod
    def _deltas_cache_path(
        cls,
        *,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        instrument_id: object,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Path | None:
        cache_root = cls._resolve_api_cache_root()
        if cache_root is None:
            return None
        instrument_key = sha256(str(instrument_id).encode("utf-8")).hexdigest()[:16]
        start_ns = int(cls._normalize_to_utc(start).value)
        end_ns = int(cls._normalize_to_utc(end).value)
        return cache_root / telonex_deltas_cache_relative_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_key=instrument_key,
            start_ns=start_ns,
            end_ns=end_ns,
        )

    def _load_deltas_cache_day(
        self,
        *,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> tuple[list[OrderBookDeltas] | None, str]:
        cache_path = self._deltas_cache_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_id=self.instrument.id,
            start=start,
            end=end,
        )
        if cache_path is None or not cache_path.exists():
            return None, "none"
        try:
            table = pq.read_table(cache_path)
            if not _TELONEX_DELTAS_CACHE_COLUMNS.issubset(set(table.schema.names)):
                raise ValueError("missing required deltas cache columns")
            data = table.to_pydict()
            records = self._deltas_records_from_columns(data)
        except Exception as exc:  # noqa: BLE001 - stale/corrupt cache should self-heal
            try:
                cache_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: ignored stale materialized deltas cache {cache_path} ({exc})",
                stacklevel=2,
            )
            return None, "none"
        return records, f"telonex-deltas-cache::{cache_path}"

    def _write_deltas_cache_day(
        self,
        *,
        records: Sequence[OrderBookDeltas],
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        cache_path = self._deltas_cache_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_id=self.instrument.id,
            start=start,
            end=end,
        )
        if cache_path is None:
            return
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                self._deltas_records_to_table(records),
                tmp_path,
                compression="zstd",
            )
            os.replace(tmp_path, cache_path)
        except Exception as exc:  # noqa: BLE001 - cache writes must not break replay
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: failed to write materialized deltas cache {cache_path} ({exc})",
                stacklevel=2,
            )

    @classmethod
    def _trade_ticks_cache_path(
        cls,
        *,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        instrument_id: object,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Path | None:
        cache_root = cls._resolve_api_cache_root()
        if cache_root is None:
            return None
        instrument_key = sha256(str(instrument_id).encode("utf-8")).hexdigest()[:16]
        start_ns = int(cls._normalize_to_utc(start).value)
        end_ns = int(cls._normalize_to_utc(end).value)
        return cache_root / telonex_trade_ticks_cache_relative_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_key=instrument_key,
            start_ns=start_ns,
            end_ns=end_ns,
        )

    def _load_trade_ticks_cache_day(
        self,
        *,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> tuple[tuple[TradeTick, ...] | None, str]:
        cache_path = self._trade_ticks_cache_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_id=self.instrument.id,
            start=start,
            end=end,
        )
        if cache_path is None or not cache_path.exists():
            return None, "none"
        try:
            table = pq.read_table(cache_path)
            if not _TELONEX_TRADE_TICKS_CACHE_COLUMNS.issubset(set(table.schema.names)):
                raise ValueError("missing required trade tick cache columns")
            frame = table.to_pandas()
            if frame.empty:
                # Empty Telonex onchain-fill caches are not authoritative for
                # execution matching. They can come from 404/no-file manifest
                # entries or older cache writes, and Polymarket may still have
                # public trade prints for the same day.
                try:
                    cache_path.unlink()
                except OSError:
                    pass
                return None, "none"
            records = self._trade_ticks_from_cache_frame(frame)
        except Exception as exc:  # noqa: BLE001 - stale/corrupt cache should self-heal
            try:
                cache_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: ignored stale materialized trade cache {cache_path} ({exc})",
                stacklevel=2,
            )
            return None, "none"
        return records, f"telonex-trade-cache::{cache_path}"

    def _write_trade_ticks_cache_day(
        self,
        *,
        records: Sequence[TradeTick],
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> None:
        if not records:
            return
        cache_path = self._trade_ticks_cache_path(
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            instrument_id=self.instrument.id,
            start=start,
            end=end,
        )
        if cache_path is None:
            return
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                self._trade_ticks_to_cache_table(records),
                tmp_path,
                compression="zstd",
            )
            os.replace(tmp_path, cache_path)
        except Exception as exc:  # noqa: BLE001 - cache writes must not break replay
            try:
                tmp_path.unlink()
            except OSError:
                pass
            warnings.warn(
                f"Telonex: failed to write materialized trade cache {cache_path} ({exc})",
                stacklevel=2,
            )

    @staticmethod
    def _trade_ticks_to_cache_table(records: Sequence[TradeTick]) -> pa.Table:
        prices: list[float] = []
        sizes: list[float] = []
        aggressor_sides: list[str] = []
        trade_ids: list[str] = []
        ts_events: list[int] = []
        ts_inits: list[int] = []
        for record in records:
            prices.append(float(record.price))
            sizes.append(float(record.size))
            aggressor_sides.append(
                getattr(record.aggressor_side, "name", str(record.aggressor_side))
            )
            trade_ids.append(str(record.trade_id))
            ts_events.append(int(record.ts_event))
            ts_inits.append(int(record.ts_init))
        return pa.table(
            {
                "price": pa.array(prices, pa.float64()),
                "size": pa.array(sizes, pa.float64()),
                "aggressor_side": pa.array(aggressor_sides, pa.string()),
                "trade_id": pa.array(trade_ids, pa.string()),
                "ts_event": pa.array(ts_events, pa.int64()),
                "ts_init": pa.array(ts_inits, pa.int64()),
            }
        )

    def _trade_ticks_from_cache_frame(self, frame: pd.DataFrame) -> tuple[TradeTick, ...]:
        if frame.empty:
            return ()
        instrument = self.instrument
        records: list[TradeTick] = []
        for row in frame.itertuples(index=False):
            aggressor_name = str(getattr(row, "aggressor_side"))
            aggressor_side = getattr(AggressorSide, aggressor_name, AggressorSide.NO_AGGRESSOR)
            records.append(
                TradeTick(
                    instrument_id=instrument.id,
                    price=instrument.make_price(getattr(row, "price")),
                    size=instrument.make_qty(getattr(row, "size")),
                    aggressor_side=aggressor_side,
                    trade_id=TradeId(str(getattr(row, "trade_id"))),
                    ts_event=int(getattr(row, "ts_event")),
                    ts_init=int(getattr(row, "ts_init")),
                )
            )
        records.sort(key=lambda trade: (int(trade.ts_event), int(trade.ts_init)))
        return tuple(records)

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

    def _deltas_records_from_columns(self, data: dict[str, list[object]]) -> list[OrderBookDeltas]:
        event_indexes = data["event_index"]
        actions = data["action"]
        sides = data["side"]
        prices = data["price"]
        sizes = data["size"]
        flags = data["flags"]
        sequences = data["sequence"]
        ts_events = data["ts_event"]
        ts_inits = data["ts_init"]

        records: list[OrderBookDeltas] = []
        current_event_index: int | None = None
        deltas: list[OrderBookDelta] = []
        instrument = self.instrument
        instrument_id = instrument.id
        for idx, raw_event_index in enumerate(event_indexes):
            event_index = int(raw_event_index)
            if current_event_index is None:
                current_event_index = event_index
            elif event_index != current_event_index:
                records.append(OrderBookDeltas(instrument_id, deltas))
                deltas = []
                current_event_index = event_index

            order = BookOrder(
                side=OrderSide(int(sides[idx])),
                price=instrument.make_price(float(prices[idx])),
                size=instrument.make_qty(float(sizes[idx])),
                order_id=0,
            )
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument_id,
                    action=BookAction(int(actions[idx])),
                    order=order,
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
    def _resolve_presigned_url(*, url: str, api_key: str) -> str:
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": _TELONEX_USER_AGENT,
            },
            method="GET",
        )

        class _NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
                return None

        opener = build_opener(_NoRedirect())
        try:
            response = opener.open(request, timeout=_TELONEX_HTTP_TIMEOUT_SECS)
            response.close()
        except HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308):
                location = exc.headers.get("Location")
                if not location:
                    raise
                return location
            raise
        raise HTTPError(url, 500, "Expected 302 redirect from Telonex", {}, None)

    def _load_api_day(
        self,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        api_key: str | None = None,
    ) -> pd.DataFrame | None:
        self._telonex_last_api_source = None
        cached_frame, cached_source = self._load_api_day_cached(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if cached_frame is not None:
            self._telonex_last_api_source = cached_source
            return cached_frame

        if api_key is None or not api_key.strip():
            api_key = _env_value(TELONEX_API_KEY_ENV)
        if api_key is None:
            raise ValueError(f"{TELONEX_API_KEY_ENV} is required when using Telonex api:.")

        url = self._api_url(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        self._telonex_last_api_source = f"telonex-api::{url}"
        try:
            presigned_url = self._resolve_presigned_url(url=url, api_key=api_key)
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise

        fetch_request = Request(presigned_url, headers={"User-Agent": _TELONEX_USER_AGENT})
        progress_url = f"telonex-api::{url}"
        try:
            with urlopen(fetch_request, timeout=_TELONEX_HTTP_TIMEOUT_SECS) as response:
                total_bytes_header = response.headers.get("Content-Length")
                total_bytes = int(total_bytes_header) if total_bytes_header else None
                downloaded = 0
                chunks: list[bytes] = []
                self._download_progress(progress_url, 0, total_bytes, False)
                while True:
                    chunk = response.read(_TELONEX_DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    self._download_progress(progress_url, downloaded, total_bytes, False)
                self._download_progress(progress_url, downloaded, total_bytes, True)
                payload = b"".join(chunks)
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        self._write_api_cache_day(
            payload=payload,
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        frame = pd.read_parquet(BytesIO(payload))
        try:
            self._write_fast_cache_day(
                frame=frame,
                base_url=base_url,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
        except Exception:
            pass
        return frame

    @staticmethod
    def _column_to_ns(column: pd.Series, column_name: str) -> np.ndarray:
        if column_name.endswith("_us"):
            return column.to_numpy(dtype="int64") * 1_000
        if column_name == "timestamp_ms":
            numeric = pd.to_numeric(column, errors="coerce")
            return (numeric.astype("float64") * 1_000_000).to_numpy(dtype="int64")
        if pd.api.types.is_numeric_dtype(column):
            return (column.astype("float64") * 1_000_000_000).to_numpy(dtype="int64")
        parsed = pd.to_datetime(column, utc=True, errors="coerce")
        return parsed.astype("int64").to_numpy()

    @staticmethod
    def _normalize_to_utc(value: pd.Timestamp) -> pd.Timestamp:
        if value.tzinfo is None:
            return value.tz_localize(UTC)
        return value.tz_convert(UTC)

    def _day_window(
        self, date: str, *, start: pd.Timestamp, end: pd.Timestamp
    ) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        day_start = pd.Timestamp(date, tz=UTC)
        day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        start_utc = self._normalize_to_utc(start)
        end_utc = self._normalize_to_utc(end)
        clipped_start = start_utc if start_utc > day_start else day_start
        clipped_end = end_utc if end_utc < day_end else day_end
        if clipped_start > clipped_end:
            return None
        return clipped_start, clipped_end

    @staticmethod
    def _first_present_column(frame: pd.DataFrame, names: Sequence[str], *, label: str) -> str:
        for name in names:
            if name in frame.columns:
                return name
        raise ValueError(f"Telonex {label} data is missing required columns: {', '.join(names)}")

    @staticmethod
    def _book_levels_from_value(value: object, *, side: str) -> tuple[PolymarketBookLevel, ...]:
        if value is None:
            return ()

        levels: list[tuple[float, str, str]] = []
        try:
            iterator = list(value)  # type: ignore[arg-type]
        except TypeError:
            return ()

        for raw_level in iterator:
            if raw_level is None:
                continue
            if isinstance(raw_level, dict):
                raw_price = raw_level.get("price")
                raw_size = raw_level.get("size")
            else:
                raw_price = getattr(raw_level, "price", None)
                raw_size = getattr(raw_level, "size", None)
            if raw_price is None or raw_size is None:
                continue
            price = float(raw_price)
            size = float(raw_size)
            if size <= 0:
                continue
            levels.append((price, str(raw_price), str(raw_size)))

        # Nautilus' Polymarket parser takes bids[-1] and asks[-1] as the touch.
        # Telonex stores bids best-first and asks best-first, so normalize the
        # ordering before parsing snapshots or applying order-book updates.
        reverse = side == "ask"
        levels.sort(key=lambda item: item[0], reverse=reverse)
        return tuple(PolymarketBookLevel(price=price, size=size) for _px, price, size in levels)

    @staticmethod
    def _book_levels_from_arrays(
        *,
        prices: object,
        sizes: object,
        side: str,
    ) -> tuple[PolymarketBookLevel, ...]:
        pairs: list[tuple[float, str, str]] = []
        for p, s in zip(prices, sizes):
            s_str = str(s)
            if float(s_str) <= 0:
                continue
            pairs.append((float(p), str(p), s_str))
        reverse = side == "ask"
        pairs.sort(key=lambda t: t[0], reverse=reverse)
        return tuple(PolymarketBookLevel(price=p_str, size=s_str) for _, p_str, s_str in pairs)

    @staticmethod
    def _book_side_map(levels: Sequence[PolymarketBookLevel]) -> dict[str, str]:
        return {str(level.price): str(level.size) for level in levels}

    def _snapshot_to_deltas(
        self,
        *,
        bids: Sequence[PolymarketBookLevel],
        asks: Sequence[PolymarketBookLevel],
        ts_event: int,
    ) -> OrderBookDeltas | None:
        snapshot = PolymarketBookSnapshot(
            market=str(getattr(self, "condition_id", "") or ""),
            asset_id=str(getattr(self, "token_id", "") or ""),
            bids=list(bids),
            asks=list(asks),
            timestamp=str(ts_event / 1_000_000),
        )
        return snapshot.parse_to_snapshot(instrument=self.instrument, ts_init=ts_event)

    def _diff_to_deltas(
        self,
        *,
        previous_bids: dict[str, str],
        previous_asks: dict[str, str],
        current_bids: dict[str, str],
        current_asks: dict[str, str],
        ts_event: int,
    ) -> OrderBookDeltas | None:
        changes: list[tuple[OrderSide, str, str]] = []

        for price in sorted(previous_bids.keys() | current_bids.keys(), key=float):
            size = current_bids.get(price)
            if size == previous_bids.get(price):
                continue
            changes.append((OrderSide.BUY, price, size or "0"))

        for price in sorted(previous_asks.keys() | current_asks.keys(), key=float, reverse=True):
            size = current_asks.get(price)
            if size == previous_asks.get(price):
                continue
            changes.append((OrderSide.SELL, price, size or "0"))

        if not changes:
            return None

        deltas: list[OrderBookDelta] = []
        instrument = self.instrument
        for idx, (side, price, size) in enumerate(changes):
            qty = instrument.make_qty(float(size))
            order = BookOrder(
                side=side,
                price=instrument.make_price(float(price)),
                size=qty,
                order_id=0,
            )
            deltas.append(
                OrderBookDelta(
                    instrument_id=instrument.id,
                    action=BookAction.UPDATE if qty > 0 else BookAction.DELETE,
                    order=order,
                    flags=RecordFlag.F_LAST if idx == len(changes) - 1 else 0,
                    sequence=idx + 1,
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )
        return OrderBookDeltas(instrument.id, deltas)

    def _book_events_from_frame(
        self,
        frame: pd.DataFrame,
        *,
        start: pd.Timestamp,
        end: pd.Timestamp,
        include_order_book: bool = True,
    ) -> list[OrderBookDeltas]:
        if frame.empty:
            return []

        timestamp_column = self._first_present_column(
            frame, ("timestamp_us", "timestamp_ms", "timestamp", "time"), label="book snapshot"
        )
        has_flat = "bid_prices" in frame.columns

        start_ns = int(self._normalize_to_utc(start).value)
        end_ns = int(self._normalize_to_utc(end).value)
        cached = self._cached_ts_ns_for_frame(frame, timestamp_column)
        ts_ns = (
            cached
            if cached is not None
            else self._column_to_ns(frame[timestamp_column], timestamp_column)
        )
        mask = ts_ns <= end_ns
        if not mask.any():
            return []

        if has_flat:
            bid_prices_values = frame["bid_prices"].to_numpy()[mask]
            bid_sizes_values = frame["bid_sizes"].to_numpy()[mask]
            ask_prices_values = frame["ask_prices"].to_numpy()[mask]
            ask_sizes_values = frame["ask_sizes"].to_numpy()[mask]
        else:
            bids_column = self._first_present_column(frame, ("bids",), label="book snapshot")
            asks_column = self._first_present_column(frame, ("asks",), label="book snapshot")
            bids_values = frame[bids_column].to_numpy()[mask]
            asks_values = frame[asks_column].to_numpy()[mask]

        ns_arr = ts_ns[mask]
        order = np.argsort(ns_arr, kind="stable")

        if has_flat and include_order_book:
            native_rows = telonex_flat_book_snapshot_diff_rows(
                timestamp_ns=ns_arr,
                bid_prices=bid_prices_values,
                bid_sizes=bid_sizes_values,
                ask_prices=ask_prices_values,
                ask_sizes=ask_sizes_values,
                start_ns=start_ns,
                end_ns=end_ns,
            )
            if native_rows is not None:
                (
                    first_snapshot_index,
                    event_indexes,
                    actions,
                    sides,
                    prices,
                    sizes,
                    flags,
                    sequences,
                    ts_events,
                    ts_inits,
                ) = native_rows

                events: list[OrderBookDeltas] = []
                if first_snapshot_index is not None:
                    bids = self._book_levels_from_arrays(
                        prices=bid_prices_values[first_snapshot_index],
                        sizes=bid_sizes_values[first_snapshot_index],
                        side="bid",
                    )
                    asks = self._book_levels_from_arrays(
                        prices=ask_prices_values[first_snapshot_index],
                        sizes=ask_sizes_values[first_snapshot_index],
                        side="ask",
                    )
                    ts_event = int(ns_arr[first_snapshot_index])
                    deltas = self._snapshot_to_deltas(bids=bids, asks=asks, ts_event=ts_event)
                    if deltas is not None:
                        events.append(deltas)

                if event_indexes:
                    events.extend(
                        self._deltas_records_from_columns(
                            {
                                "event_index": event_indexes,
                                "action": actions,
                                "side": sides,
                                "price": prices,
                                "size": sizes,
                                "flags": flags,
                                "sequence": sequences,
                                "ts_event": ts_events,
                                "ts_init": ts_inits,
                            }
                        )
                    )
                events.sort(key=lambda record: int(record.ts_event))
                return events

        events: list[OrderBookDeltas] = []
        previous_bids: dict[str, str] | None = None
        previous_asks: dict[str, str] | None = None
        emitted_snapshot = False

        for idx in order:
            ts_event = int(ns_arr[idx])
            if has_flat:
                bids = self._book_levels_from_arrays(
                    prices=bid_prices_values[idx], sizes=bid_sizes_values[idx], side="bid"
                )
                asks = self._book_levels_from_arrays(
                    prices=ask_prices_values[idx], sizes=ask_sizes_values[idx], side="ask"
                )
            else:
                bids = self._book_levels_from_value(bids_values[idx], side="bid")
                asks = self._book_levels_from_value(asks_values[idx], side="ask")
            current_bids = self._book_side_map(bids)
            current_asks = self._book_side_map(asks)

            if ts_event < start_ns:
                previous_bids = current_bids
                previous_asks = current_asks
                continue

            deltas: OrderBookDeltas | None
            if not emitted_snapshot:
                deltas = self._snapshot_to_deltas(bids=bids, asks=asks, ts_event=ts_event)
                emitted_snapshot = deltas is not None
            else:
                assert previous_bids is not None
                assert previous_asks is not None
                deltas = self._diff_to_deltas(
                    previous_bids=previous_bids,
                    previous_asks=previous_asks,
                    current_bids=current_bids,
                    current_asks=current_asks,
                    ts_event=ts_event,
                )

            if deltas is not None and include_order_book:
                events.append(deltas)
            previous_bids = current_bids
            previous_asks = current_asks

        events.sort(key=lambda record: int(record.ts_event))
        return events

    @staticmethod
    def _optional_column(frame: pd.DataFrame, names: Sequence[str]) -> str | None:
        for name in names:
            if name in frame.columns:
                return name
        return None

    @staticmethod
    def _aggressor_side_from_value(value: object) -> AggressorSide:
        normalized = str(value or "").strip().casefold().replace("-", "_")
        if normalized in {"buy", "buyer", "bid", "bidder", "taker_buy", "buying"}:
            return AggressorSide.BUYER
        if normalized in {"sell", "seller", "ask", "offer", "taker_sell", "selling"}:
            return AggressorSide.SELLER
        return AggressorSide.NO_AGGRESSOR

    def _onchain_fill_trade_ticks_from_frame(
        self,
        frame: pd.DataFrame,
        *,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[TradeTick]:
        if frame.empty:
            return []

        timestamp_column = self._first_present_column(
            frame,
            (
                "timestamp_us",
                "block_timestamp_us",
                "timestamp_ms",
                "timestamp",
                "block_timestamp",
                "time",
                "local_timestamp_us",
            ),
            label="onchain fills",
        )
        price_column = self._first_present_column(
            frame,
            ("price", "fill_price", "matched_price", "px", "price_usdc"),
            label="onchain fills",
        )
        size_column = self._first_present_column(
            frame,
            ("size", "quantity", "amount", "shares", "fill_size", "matched_size"),
            label="onchain fills",
        )
        side_column = self._optional_column(
            frame, ("side", "taker_side", "aggressor_side", "trader_side")
        )
        id_column = self._optional_column(
            frame,
            (
                "transaction_hash",
                "transactionHash",
                "tx_hash",
                "tx",
                "hash",
                "trade_id",
                "id",
            ),
        )

        start_ns = int(self._normalize_to_utc(start).value)
        end_ns = int(self._normalize_to_utc(end).value)
        ts_ns = self._column_to_ns(frame[timestamp_column], timestamp_column)
        mask = (ts_ns >= start_ns) & (ts_ns <= end_ns)
        if not mask.any():
            return []

        ns_values = ts_ns[mask]
        price_values = frame[price_column].to_numpy()[mask]
        size_values = frame[size_column].to_numpy()[mask]
        side_values = frame[side_column].to_numpy()[mask] if side_column is not None else None
        id_values = frame[id_column].to_numpy()[mask] if id_column is not None else None
        order = np.argsort(ns_values, kind="stable")

        make_price = self.instrument.make_price
        make_qty = self.instrument.make_qty
        instrument_id = self.instrument.id
        token_suffix = str(getattr(self, "token_id", "") or "")[-4:]
        timestamp_counts: dict[int, int] = {}
        trade_id_counts: dict[str, int] = {}
        trades: list[TradeTick] = []

        for sorted_index, idx in enumerate(order):
            raw_price = price_values[idx]
            raw_size = size_values[idx]
            if raw_price is None or raw_size is None:
                continue
            try:
                price_float = float(raw_price)
                size_float = float(raw_size)
            except (TypeError, ValueError):
                continue
            if not (0.0 < price_float < 1.0) or size_float <= 0.0:
                continue

            base_ts_event = int(ns_values[idx])
            occurrence_in_timestamp = timestamp_counts.get(base_ts_event, 0)
            timestamp_counts[base_ts_event] = occurrence_in_timestamp + 1
            ts_event = base_ts_event + min(occurrence_in_timestamp, 999)

            if side_values is None:
                aggressor_side = AggressorSide.NO_AGGRESSOR
            else:
                aggressor_side = self._aggressor_side_from_value(side_values[idx])

            if id_values is None:
                raw_id = f"telonex-{base_ts_event}-{sorted_index}"
            else:
                raw_id = str(id_values[idx])
            raw_id = raw_id if raw_id and raw_id.casefold() != "nan" else f"telonex-{base_ts_event}"
            sequence = trade_id_counts.get(raw_id, 0)
            trade_id_counts[raw_id] = sequence + 1
            id_suffix = raw_id[-24:]
            if token_suffix:
                trade_id = f"{id_suffix}-{token_suffix}-{sequence:06d}"
            else:
                trade_id = f"{id_suffix}-{sequence:06d}"

            trades.append(
                TradeTick(
                    instrument_id=instrument_id,
                    price=make_price(raw_price),
                    size=make_qty(raw_size),
                    aggressor_side=aggressor_side,
                    trade_id=TradeId(trade_id),
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        trades.sort(key=lambda trade: (int(trade.ts_event), int(trade.ts_init)))
        return trades

    def _empty_local_blob_day_frame(
        self,
        *,
        entry: TelonexSourceEntry,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        assert entry.target is not None
        blob_root = self._local_blob_root(Path(entry.target).expanduser())
        if blob_root is None:
            return None
        row_count = self._manifest_completed_row_count(
            store_root=blob_root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            date=date,
        )
        if row_count == 0 or (
            row_count is None
            and self._manifest_empty_day_exists(
                store_root=blob_root,
                channel=channel,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
                date=date,
            )
        ):
            return pd.DataFrame()
        return None

    def _parse_telonex_trade_frame(
        self,
        frame: pd.DataFrame,
        *,
        channel: str,
        source: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        market_slug: str,
        token_index: int,
    ) -> tuple[TradeTick, ...] | None:
        try:
            trades = self._onchain_fill_trade_ticks_from_frame(frame, start=start, end=end)
        except (TypeError, ValueError) as exc:
            warnings.warn(
                f"Telonex: source {source} returned unusable {channel} trade data "
                f"({market_slug}/{token_index}): {exc}; trying next source.",
                stacklevel=2,
            )
            return None
        if not trades:
            return None
        self._telonex_last_trade_source = source
        return tuple(trades)

    def load_telonex_onchain_fill_ticks(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        market_slug: str | None = None,
        token_index: int | None = None,
        outcome: str | None = None,
    ) -> tuple[TradeTick, ...] | None:
        """Load Telonex trade data for execution matching.

        Returns ``None`` when configured Telonex fill sources cannot provide
        non-empty execution ticks for the requested day/window, allowing the
        caller to fall back to Polymarket's public trades API. Empty Telonex
        local/API fill results are not treated as authoritative because another
        Telonex trade channel or the public trade feed can still contain
        execution prints for the same day.
        """
        self._telonex_last_trade_source = None
        resolved_market_slug = market_slug or getattr(self, "_telonex_market_slug", None)
        if not resolved_market_slug:
            return None
        resolved_token_index = (
            int(token_index)
            if token_index is not None
            else int(getattr(self, "_telonex_token_index", 0))
        )
        resolved_outcome = (
            outcome
            if outcome is not None
            else getattr(self, "_telonex_outcome", None)
            or str(self.instrument.outcome or "")
            or None
        )

        start_utc = self._normalize_to_utc(start)
        end_utc = self._normalize_to_utc(end)
        dates = self._date_range(start_utc, end_utc)
        if len(dates) != 1:
            return None
        date = dates[0]
        day_window = self._day_window(date, start=start_utc, end=end_utc)
        if day_window is None:
            return ()
        day_start, day_end = day_window

        config = self._config()

        for channel in _TELONEX_TRADE_TICK_CHANNELS:
            cached_trades, cached_source = self._load_trade_ticks_cache_day(
                channel=channel,
                date=date,
                market_slug=str(resolved_market_slug),
                token_index=resolved_token_index,
                outcome=resolved_outcome,
                start=day_start,
                end=day_end,
            )
            if cached_trades is not None:
                self._telonex_last_trade_source = cached_source
                return cached_trades

        api_entries = [
            entry for entry in config.ordered_source_entries if entry.kind == _TELONEX_SOURCE_API
        ]
        for channel in _TELONEX_TRADE_TICK_CHANNELS:
            for entry in api_entries:
                assert entry.target is not None
                frame, source = self._load_api_day_cached(
                    base_url=entry.target,
                    channel=channel,
                    date=date,
                    market_slug=str(resolved_market_slug),
                    token_index=resolved_token_index,
                    outcome=resolved_outcome,
                )
                if frame is None:
                    continue
                parsed = self._parse_telonex_trade_frame(
                    frame,
                    channel=channel,
                    source=source,
                    start=day_start,
                    end=day_end,
                    market_slug=str(resolved_market_slug),
                    token_index=resolved_token_index,
                )
                if parsed is not None:
                    self._write_trade_ticks_cache_day(
                        records=parsed,
                        channel=channel,
                        date=date,
                        market_slug=str(resolved_market_slug),
                        token_index=resolved_token_index,
                        outcome=resolved_outcome,
                        start=day_start,
                        end=day_end,
                    )
                    return parsed

        range_cache: dict[Path, pd.DataFrame | None] = {}
        for entry in config.ordered_source_entries:
            for channel in _TELONEX_TRADE_TICK_CHANNELS:
                if entry.kind == _TELONEX_SOURCE_LOCAL:
                    frame = self._try_load_day_from_local(
                        entry=entry,
                        channel=channel,
                        date=date,
                        market_slug=str(resolved_market_slug),
                        token_index=resolved_token_index,
                        outcome=resolved_outcome,
                        start=day_start,
                        end=day_end,
                        range_cache=range_cache,
                    )
                    if frame is None:
                        frame = self._empty_local_blob_day_frame(
                            entry=entry,
                            channel=channel,
                            date=date,
                            market_slug=str(resolved_market_slug),
                            token_index=resolved_token_index,
                            outcome=resolved_outcome,
                        )
                    source = (
                        f"telonex-local-trades::{entry.target}"
                        if channel == TELONEX_TRADES_CHANNEL
                        else f"telonex-local::{entry.target}"
                    )
                else:
                    frame, source = self._try_load_day_from_api_entry(
                        entry=entry,
                        channel=channel,
                        date=date,
                        market_slug=str(resolved_market_slug),
                        token_index=resolved_token_index,
                        outcome=resolved_outcome,
                    )
                if frame is None:
                    continue
                parsed = self._parse_telonex_trade_frame(
                    frame,
                    channel=channel,
                    source=source,
                    start=day_start,
                    end=day_end,
                    market_slug=str(resolved_market_slug),
                    token_index=resolved_token_index,
                )
                if parsed is not None:
                    self._write_trade_ticks_cache_day(
                        records=parsed,
                        channel=channel,
                        date=date,
                        market_slug=str(resolved_market_slug),
                        token_index=resolved_token_index,
                        outcome=resolved_outcome,
                        start=day_start,
                        end=day_end,
                    )
                    return parsed

        return None

    def _try_load_range_from_local(
        self,
        *,
        entry: TelonexSourceEntry,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> pd.DataFrame | None:
        assert entry.target is not None
        root = Path(entry.target).expanduser()
        blob_root = self._local_blob_root(root)
        if blob_root is not None:
            try:
                blob_frame = self._load_blob_range(
                    store_root=blob_root,
                    channel=channel,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    start=start,
                    end=end,
                )
            except Exception as exc:  # noqa: BLE001 — fall through to next source
                warnings.warn(
                    f"Telonex: local blob read failed at {blob_root} ({exc}); trying next source.",
                    stacklevel=2,
                )
                blob_frame = None
            if blob_frame is not None:
                return blob_frame
        try:
            return self._load_local_range(
                root=root,
                channel=channel,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
        except Exception as exc:  # noqa: BLE001 — fall through to next source
            warnings.warn(
                f"Telonex: local consolidated read failed at {root} ({exc}); trying next source.",
                stacklevel=2,
            )
            return None

    def _try_load_day_from_local(
        self,
        *,
        entry: TelonexSourceEntry,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        start: pd.Timestamp,
        end: pd.Timestamp,
        range_cache: dict[Path, pd.DataFrame | None],
    ) -> pd.DataFrame | None:
        assert entry.target is not None
        root = Path(entry.target).expanduser()
        blob_root = self._local_blob_root(root)
        if blob_root is not None:
            try:
                blob_frame = self._load_blob_range(
                    store_root=blob_root,
                    channel=channel,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    start=start,
                    end=end,
                )
            except Exception as exc:  # noqa: BLE001 — fall through to local layouts/API
                warnings.warn(
                    f"Telonex: local blob read failed at {blob_root} ({exc}); trying next source.",
                    stacklevel=2,
                )
                blob_frame = None
            if blob_frame is not None:
                return blob_frame

        try:
            daily_frame = self._load_local_day(
                root=root,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
        except Exception as exc:  # noqa: BLE001 — fall through to consolidated/API
            warnings.warn(
                f"Telonex: local daily read failed at {root} ({exc}); trying next source.",
                stacklevel=2,
            )
            daily_frame = None
        if daily_frame is not None:
            return daily_frame

        path = self._local_consolidated_path(
            root=root,
            channel=channel,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if path is None:
            return None
        if path not in range_cache:
            range_cache[path] = self._safe_read_parquet(path)
        return range_cache[path]

    def _try_load_day_from_entry(
        self,
        *,
        entry: TelonexSourceEntry,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        assert entry.target is not None
        try:
            if entry.kind == _TELONEX_SOURCE_LOCAL:
                return self._load_local_day(
                    root=Path(entry.target).expanduser(),
                    channel=channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
            if entry.kind == _TELONEX_SOURCE_API:
                return self._load_api_day(
                    base_url=entry.target,
                    channel=channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    api_key=entry.api_key,
                )
        except (HTTPError, URLError, OSError, ValueError, RuntimeError) as exc:
            warnings.warn(
                f"Telonex: source {entry.kind}:{entry.target} failed for {date} "
                f"({market_slug}/{token_index}): {exc}; trying next source.",
                stacklevel=2,
            )
            return None
        return None

    def _try_load_day_from_api_entry(
        self,
        *,
        entry: TelonexSourceEntry,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> tuple[pd.DataFrame | None, str]:
        assert entry.target is not None
        try:
            frame = self._load_api_day(
                base_url=entry.target,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
                api_key=entry.api_key,
            )
        except (HTTPError, URLError, OSError, ValueError, RuntimeError) as exc:
            warnings.warn(
                f"Telonex: source {entry.kind}:{entry.target} failed for {date} "
                f"({market_slug}/{token_index}): {exc}; trying next source.",
                stacklevel=2,
            )
            return None, "none"
        if frame is None:
            return None, "none"
        return (
            frame,
            self._telonex_api_source_label(
                base_url=entry.target,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            ),
        )

    def _telonex_api_source_label(
        self,
        *,
        base_url: str,
        channel: str,
        date: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> str:
        return "telonex-api::" + self._api_url(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )

    def _load_order_book_deltas_day(
        self,
        *,
        date: str,
        config: TelonexLoaderConfig,
        api_entries: Sequence[TelonexSourceEntry],
        start: pd.Timestamp,
        end: pd.Timestamp,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        include_order_book: bool,
        range_cache: dict[Path, pd.DataFrame | None],
    ) -> _TelonexDayResult:
        self._day_progress(date, "start", "none", 0)
        _ = api_entries  # API cache is consulted only when an API source is reached.
        day_source = "none"
        emitted_day_complete = False
        try:
            day_window = self._day_window(date, start=start, end=end)
            if day_window is None:
                self._day_progress(date, "complete", day_source, 0)
                emitted_day_complete = True
                return _TelonexDayResult(date=date, records=[], source=day_source)

            day_start, day_end = day_window
            cached_records, cached_source = self._load_deltas_cache_day(
                channel=config.channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
                start=day_start,
                end=day_end,
            )
            if cached_records is not None:
                self._day_progress(date, "complete", cached_source, len(cached_records))
                emitted_day_complete = True
                return _TelonexDayResult(date=date, records=cached_records, source=cached_source)

            frame: pd.DataFrame | None = None
            if frame is None:
                for entry in config.ordered_source_entries:
                    if entry.kind == _TELONEX_SOURCE_LOCAL:
                        frame = self._try_load_day_from_local(
                            entry=entry,
                            channel=config.channel,
                            date=date,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                            start=day_start,
                            end=day_end,
                            range_cache=range_cache,
                        )
                        day_source = (
                            f"telonex-local::{entry.target}" if frame is not None else day_source
                        )
                    else:
                        frame, source = self._try_load_day_from_api_entry(
                            entry=entry,
                            channel=config.channel,
                            date=date,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                        )
                        day_source = source if frame is not None else day_source
                    if frame is not None:
                        break

            if frame is None:
                self._day_progress(date, "complete", day_source, 0)
                emitted_day_complete = True
                return _TelonexDayResult(date=date, records=[], source=day_source)

            day_records = self._book_events_from_frame(
                frame,
                start=day_start,
                end=day_end,
                include_order_book=include_order_book,
            )
            if include_order_book:
                self._write_deltas_cache_day(
                    records=day_records,
                    channel=config.channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    start=day_start,
                    end=day_end,
                )
            self._day_progress(date, "complete", day_source, len(day_records))
            emitted_day_complete = True
            return _TelonexDayResult(date=date, records=day_records, source=day_source)
        finally:
            if not emitted_day_complete:
                self._day_progress(date, "complete", day_source, 0)

    def _iter_loaded_telonex_days(
        self,
        *,
        dates: list[str],
        config: TelonexLoaderConfig,
        api_entries: Sequence[TelonexSourceEntry],
        start: pd.Timestamp,
        end: pd.Timestamp,
        market_slug: str,
        token_index: int,
        outcome: str | None,
        include_order_book: bool,
    ) -> Iterator[_TelonexDayResult]:
        prefetch_workers = getattr(
            self, "_telonex_prefetch_workers", self._resolve_prefetch_workers()
        )
        max_workers = min(prefetch_workers, len(dates)) if api_entries else 1
        if max_workers <= 1:
            range_cache: dict[Path, pd.DataFrame | None] = {}
            for date in dates:
                yield self._load_order_book_deltas_day(
                    date=date,
                    config=config,
                    api_entries=api_entries,
                    start=start,
                    end=end,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    include_order_book=include_order_book,
                    range_cache=range_cache,
                )
            return

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="telonex-day") as pool:
            futures: dict[str, Future[_TelonexDayResult]] = {}
            next_index = 0

            def _submit_next() -> None:
                nonlocal next_index
                if next_index >= len(dates):
                    return
                date = dates[next_index]
                next_index += 1
                futures[date] = pool.submit(
                    self._load_order_book_deltas_day,
                    date=date,
                    config=config,
                    api_entries=api_entries,
                    start=start,
                    end=end,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    include_order_book=include_order_book,
                    range_cache={},
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
        outcome: str | None,
        include_order_book: bool = True,
    ) -> list[OrderBookDeltas]:
        config = self._config()
        records: list[OrderBookDeltas] = []
        api_entries = [
            entry for entry in config.ordered_source_entries if entry.kind == _TELONEX_SOURCE_API
        ]
        dates = self._date_range(start, end)
        for result in self._iter_loaded_telonex_days(
            dates=dates,
            config=config,
            api_entries=api_entries,
            start=start,
            end=end,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
            include_order_book=include_order_book,
        ):
            records.extend(result.records)
        records.sort(key=lambda record: int(record.ts_event))
        return records


__all__ = [
    "TELONEX_API_BASE_URL_ENV",
    "TELONEX_CACHE_ROOT_ENV",
    "TELONEX_API_KEY_ENV",
    "TELONEX_CHANNEL_ENV",
    "TELONEX_FULL_BOOK_CHANNEL",
    "TELONEX_LOCAL_DIR_ENV",
    "TELONEX_ONCHAIN_FILLS_CHANNEL",
    "TELONEX_PREFETCH_WORKERS_ENV",
    "TELONEX_TRADES_CHANNEL",
    "RunnerPolymarketTelonexBookDataLoader",
    "TelonexDataSourceSelection",
    "TelonexLoaderConfig",
    "configured_telonex_data_source",
    "resolve_telonex_data_source_selection",
    "resolve_telonex_loader_config",
]
