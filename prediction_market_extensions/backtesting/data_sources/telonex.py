from __future__ import annotations

import os
import re
import warnings
from hashlib import sha256
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import numpy as np
import pandas as pd
from nautilus_trader.model.data import QuoteTick

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

_TELONEX_DEFAULT_API_BASE_URL = "https://api.telonex.io"
_TELONEX_DEFAULT_CHANNEL = "quotes"
_TELONEX_EXCHANGE = "polymarket"
_TELONEX_HTTP_TIMEOUT_SECS = 60
_TELONEX_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
_TELONEX_USER_AGENT = "prediction-market-backtesting/1.0"
_TELONEX_LOCAL_PREFIX = "local:"
_TELONEX_API_PREFIX = "api:"
_TELONEX_SOURCE_LOCAL = "local"
_TELONEX_SOURCE_API = "api"
_TELONEX_BLOB_DB_FILENAME = "telonex.duckdb"
_TELONEX_DATA_SUBDIR = "data"
_TELONEX_CACHE_SUBDIR = "api-days"


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


def _resolve_channel() -> str:
    return (_env_value(TELONEX_CHANNEL_ENV) or _TELONEX_DEFAULT_CHANNEL).casefold()


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


def _source_summary(entries: Sequence[TelonexSourceEntry]) -> str:
    parts: list[str] = ["cache"] if _resolve_api_cache_root() is not None else []
    for entry in entries:
        if entry.kind == _TELONEX_SOURCE_LOCAL:
            parts.append(f"local {entry.target}")
        elif entry.kind == _TELONEX_SOURCE_API:
            suffix = " (key set)" if entry.api_key else " (key missing)"
            parts.append(f"api {entry.target}{suffix}")
    return "Telonex source: explicit priority (" + " -> ".join(parts) + ")"


def resolve_telonex_loader_config(
    *, sources: Sequence[str] | None = None
) -> tuple[TelonexDataSourceSelection, TelonexLoaderConfig]:
    if sources is None:
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
        TelonexLoaderConfig(channel=_resolve_channel(), ordered_source_entries=entries),
    )


def resolve_telonex_data_source_selection(
    *, sources: Sequence[str] | None = None
) -> tuple[TelonexDataSourceSelection, dict[str, str | None]]:
    selection, _config = resolve_telonex_loader_config(sources=sources)
    return selection, {}


@contextmanager
def configured_telonex_data_source(
    *, sources: Sequence[str] | None = None
) -> Iterator[TelonexDataSourceSelection]:
    selection, config = resolve_telonex_loader_config(sources=sources)
    token = _CURRENT_TELONEX_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_TELONEX_LOADER_CONFIG.reset(token)


class RunnerPolymarketTelonexQuoteDataLoader(PolymarketDataLoader):
    def _download_progress(
        self, url: str, downloaded_bytes: int, total_bytes: int | None, finished: bool
    ) -> None:
        callback = getattr(self, "_telonex_download_progress_callback", None)
        if callback is not None:
            callback(url, downloaded_bytes, total_bytes, finished)

    @classmethod
    def _resolve_api_cache_root(cls) -> Path | None:
        return _resolve_api_cache_root()

    def _config(self) -> TelonexLoaderConfig:
        config = _current_loader_config()
        if config is None:
            _selection, config = resolve_telonex_loader_config()
        return config

    @staticmethod
    def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
        first_day = start.tz_convert(UTC).floor("D")
        last_day = end.tz_convert(UTC).floor("D")
        days: list[str] = []
        cursor = first_day
        while cursor <= last_day:
            days.append(cursor.strftime("%Y-%m-%d"))
            cursor += pd.Timedelta(days=1)
        return days

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
        segments = [str(token_index)]
        if outcome:
            segments.insert(0, outcome)
        return tuple(segments)

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
        outcome) slice. Returns None when the channel has no data on disk."""
        try:
            import duckdb
        except ImportError:
            return None

        channel_dir = store_root / _TELONEX_DATA_SUBDIR / f"channel={channel}"
        if not channel_dir.exists():
            return None
        # Glob every part file under this channel, regardless of year/month
        # partition depth. Empty-but-existing channel dirs yield no matches,
        # in which case DuckDB will raise — guard with a cheap pre-check.
        part_glob = str(channel_dir / "**" / "*.parquet")
        if not any(channel_dir.rglob("*.parquet")):
            return None

        segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)
        placeholders = ", ".join(["?"] * len(segments))

        start_utc = self._normalize_to_utc(start)
        end_utc = self._normalize_to_utc(end)
        start_ym = start_utc.year * 100 + start_utc.month
        end_ym = end_utc.year * 100 + end_utc.month

        # DuckDB's hive_partitioning exposes year/month as VARCHAR by default —
        # cast to INTEGER before arithmetic so range pruning works.
        query = (
            "SELECT * FROM read_parquet(?, hive_partitioning=1, union_by_name=True) "
            f"WHERE market_slug = ? AND outcome_segment IN ({placeholders}) "
            "AND CAST(year AS INTEGER) * 100 + CAST(month AS INTEGER) "
            "BETWEEN ? AND ?"
        )
        params: list[object] = [part_glob, market_slug, *segments, start_ym, end_ym]

        con = duckdb.connect(":memory:")
        try:
            try:
                frame = con.execute(query, params).fetch_df()
            except duckdb.Error as exc:
                warnings.warn(
                    f"Telonex: skipping blob store {store_root} for {market_slug}/"
                    f"{token_index} ({channel}) — DuckDB failed: {exc}",
                    stacklevel=2,
                )
                return None
        finally:
            con.close()

        if frame is None or frame.empty:
            return None
        drop_cols = [
            c for c in ("market_slug", "outcome_segment", "year", "month") if c in frame.columns
        ]
        if drop_cols:
            frame = frame.drop(columns=drop_cols)
        return frame

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
        outcome_parts = cls._outcome_segments(token_index=token_index, outcome=outcome)
        candidates = [
            root / _TELONEX_EXCHANGE / market_slug / outcome_part / f"{channel}.parquet"
            for outcome_part in outcome_parts
        ]
        candidates.extend(
            root / _TELONEX_EXCHANGE / channel / market_slug / f"{outcome_part}.parquet"
            for outcome_part in outcome_parts
        )
        candidates.extend(
            root / channel / market_slug / f"{outcome_part}.parquet"
            for outcome_part in outcome_parts
        )
        return tuple(candidates)

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
        outcome_parts = cls._outcome_segments(token_index=token_index, outcome=outcome)
        candidates = [
            root / _TELONEX_EXCHANGE / market_slug / outcome_part / channel / f"{date}.parquet"
            for outcome_part in outcome_parts
        ]
        candidates.extend(
            root / _TELONEX_EXCHANGE / channel / market_slug / outcome_part / f"{date}.parquet"
            for outcome_part in outcome_parts
        )
        candidates.extend(
            root / channel / market_slug / outcome_part / f"{date}.parquet"
            for outcome_part in outcome_parts
        )
        candidates.extend(
            [
                root / _TELONEX_EXCHANGE / channel / f"{market_slug}_{token_index}_{date}.parquet",
                root / channel / f"{market_slug}_{token_index}_{date}.parquet",
                root / f"{market_slug}_{token_index}_{date}.parquet",
                root / f"{date}.parquet",
            ]
        )
        return tuple(candidates)

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
        params: dict[str, str] = {"slug": market_slug}
        if outcome:
            params["outcome"] = outcome
        else:
            params["outcome_id"] = str(token_index)
        return (
            f"{base_url.rstrip('/')}/v1/downloads/{_TELONEX_EXCHANGE}/{channel}/{date}"
            f"?{urlencode(params)}"
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
        outcome_segment = (
            f"outcome={quote(outcome, safe='')}" if outcome else f"outcome_id={token_index}"
        )
        return (
            cache_root
            / _TELONEX_CACHE_SUBDIR
            / base_url_key
            / _TELONEX_EXCHANGE
            / channel
            / quote(market_slug, safe="")
            / outcome_segment
            / f"{date}.parquet"
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
        cached = self._load_api_cache_day(
            base_url=base_url,
            channel=channel,
            date=date,
            market_slug=market_slug,
            token_index=token_index,
            outcome=outcome,
        )
        if cached is not None:
            cache_path = self._api_cache_path(
                base_url=base_url,
                channel=channel,
                date=date,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
            self._telonex_last_api_source = (
                f"telonex-cache::{cache_path}" if cache_path is not None else "telonex-cache"
            )
            return cached

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
        return pd.read_parquet(BytesIO(payload))

    @staticmethod
    def _column_to_ns(column: pd.Series, column_name: str) -> np.ndarray:
        if column_name == "timestamp_us":
            return column.to_numpy(dtype="int64") * 1_000
        if column_name == "timestamp_ms":
            return column.to_numpy(dtype="int64") * 1_000_000
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

    def _quote_ticks_from_frame(
        self, frame: pd.DataFrame, *, start: pd.Timestamp, end: pd.Timestamp
    ) -> list[QuoteTick]:
        if frame.empty:
            return []

        timestamp_column = self._first_present_column(
            frame, ("timestamp_us", "timestamp_ms", "timestamp", "time"), label="quote"
        )
        bid_price_column = self._first_present_column(
            frame, ("bid_price", "best_bid", "bid"), label="quote"
        )
        ask_price_column = self._first_present_column(
            frame, ("ask_price", "best_ask", "ask"), label="quote"
        )
        bid_size_column = self._first_present_column(
            frame, ("bid_size", "best_bid_size", "bid_qty", "bid_quantity"), label="quote"
        )
        ask_size_column = self._first_present_column(
            frame, ("ask_size", "best_ask_size", "ask_qty", "ask_quantity"), label="quote"
        )

        # Normalize the user's start/end to UTC ns — a naive Timestamp's `.value`
        # is wall-clock ns, not UTC, and would silently mis-filter the range.
        start_ns = int(self._normalize_to_utc(start).value)
        end_ns = int(self._normalize_to_utc(end).value)

        ts_ns = self._column_to_ns(frame[timestamp_column], timestamp_column)
        mask = (ts_ns >= start_ns) & (ts_ns <= end_ns)
        if not mask.any():
            return []

        bid_px = frame[bid_price_column].to_numpy()[mask]
        ask_px = frame[ask_price_column].to_numpy()[mask]
        bid_sz = frame[bid_size_column].to_numpy()[mask]
        ask_sz = frame[ask_size_column].to_numpy()[mask]
        ns_arr = ts_ns[mask]

        order = np.argsort(ns_arr, kind="stable")
        bid_px = bid_px[order]
        ask_px = ask_px[order]
        bid_sz = bid_sz[order]
        ask_sz = ask_sz[order]
        ns_arr = ns_arr[order]

        make_price = self.instrument.make_price
        make_qty = self.instrument.make_qty
        instrument_id = self.instrument.id
        return [
            QuoteTick(
                instrument_id=instrument_id,
                bid_price=make_price(bp),
                ask_price=make_price(ap),
                bid_size=make_qty(bs),
                ask_size=make_qty(sz),
                ts_event=int(ns),
                ts_init=int(ns),
            )
            for bp, ap, bs, sz, ns in zip(bid_px, ask_px, bid_sz, ask_sz, ns_arr, strict=True)
        ]

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

    def load_quotes(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> list[QuoteTick]:
        config = self._config()
        records: list[QuoteTick] = []
        api_entries = [
            entry for entry in config.ordered_source_entries if entry.kind == _TELONEX_SOURCE_API
        ]
        range_cache: dict[Path, pd.DataFrame | None] = {}
        for date in self._date_range(start, end):
            day_window = self._day_window(date, start=start, end=end)
            if day_window is None:
                continue
            day_start, day_end = day_window
            frame: pd.DataFrame | None = None
            for entry in api_entries:
                assert entry.target is not None
                frame = self._load_api_cache_day(
                    base_url=entry.target,
                    channel=config.channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
                if frame is not None:
                    break
            if frame is not None:
                records.extend(self._quote_ticks_from_frame(frame, start=day_start, end=day_end))
                continue

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
                else:
                    frame = self._try_load_day_from_entry(
                        entry=entry,
                        channel=config.channel,
                        date=date,
                        market_slug=market_slug,
                        token_index=token_index,
                        outcome=outcome,
                    )
                if frame is not None:
                    break
            if frame is None:
                continue
            records.extend(self._quote_ticks_from_frame(frame, start=day_start, end=day_end))
        records.sort(key=lambda quote: quote.ts_event)
        return records


__all__ = [
    "TELONEX_API_BASE_URL_ENV",
    "TELONEX_CACHE_ROOT_ENV",
    "TELONEX_API_KEY_ENV",
    "TELONEX_CHANNEL_ENV",
    "TELONEX_LOCAL_DIR_ENV",
    "RunnerPolymarketTelonexQuoteDataLoader",
    "TelonexDataSourceSelection",
    "TelonexLoaderConfig",
    "configured_telonex_data_source",
    "resolve_telonex_data_source_selection",
    "resolve_telonex_loader_config",
]
