from __future__ import annotations

import os
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

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
    parts: list[str] = []
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
    def _local_blob_db_path(root: Path) -> Path | None:
        candidate = root / _TELONEX_BLOB_DB_FILENAME
        return candidate if candidate.exists() else None

    @staticmethod
    def _outcome_segment_candidates(*, token_index: int, outcome: str | None) -> tuple[str, ...]:
        segments = [str(token_index)]
        if outcome:
            segments.insert(0, outcome)
        return tuple(segments)

    def _load_blob_range(
        self,
        *,
        db_path: Path,
        channel: str,
        market_slug: str,
        token_index: int,
        outcome: str | None,
    ) -> pd.DataFrame | None:
        try:
            import duckdb
        except ImportError:
            return None
        table = f"{channel}_data"
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()[0]
            if not exists:
                return None
            segments = self._outcome_segment_candidates(token_index=token_index, outcome=outcome)
            placeholders = ", ".join(["?"] * len(segments))
            query = (
                f'SELECT * FROM "{table}" '
                f"WHERE market_slug = ? AND outcome_segment IN ({placeholders})"
            )
            params = [market_slug, *segments]
            frame = con.execute(query, params).fetch_df()
        finally:
            con.close()
        if frame is None or frame.empty:
            return None
        drop_cols = [c for c in ("market_slug", "outcome_segment") if c in frame.columns]
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
        return pd.read_parquet(path)

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
        return pd.read_parquet(path)

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
        return pd.read_parquet(BytesIO(payload))

    @staticmethod
    def _timestamp_ns(row: pd.Series, timestamp_column: str) -> int:
        value = row[timestamp_column]
        if timestamp_column == "timestamp_us":
            return int(value) * 1_000
        if timestamp_column == "timestamp_ms":
            return int(value) * 1_000_000
        if isinstance(value, (int, float)):
            return int(float(value) * 1_000_000_000)
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(UTC)
        else:
            timestamp = timestamp.tz_convert(UTC)
        return int(timestamp.value)

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

        make_price = self.instrument.make_price
        make_qty = self.instrument.make_qty
        records: list[QuoteTick] = []
        start_ns = int(start.value)
        end_ns = int(end.value)
        for _index, row in frame.iterrows():
            ts_event = self._timestamp_ns(row, timestamp_column)
            if ts_event < start_ns or ts_event > end_ns:
                continue
            records.append(
                QuoteTick(
                    instrument_id=self.instrument.id,
                    bid_price=make_price(row[bid_price_column]),
                    ask_price=make_price(row[ask_price_column]),
                    bid_size=make_qty(row[bid_size_column]),
                    ask_size=make_qty(row[ask_size_column]),
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )
        records.sort(key=lambda quote: quote.ts_event)
        return records

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
        for entry in config.ordered_source_entries:
            if entry.kind != _TELONEX_SOURCE_LOCAL:
                break
            assert entry.target is not None
            root = Path(entry.target).expanduser()
            blob_db = self._local_blob_db_path(root)
            if blob_db is not None:
                blob_frame = self._load_blob_range(
                    db_path=blob_db,
                    channel=config.channel,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
                if blob_frame is not None:
                    return self._quote_ticks_from_frame(blob_frame, start=start, end=end)
            frame = self._load_local_range(
                root=root,
                channel=config.channel,
                market_slug=market_slug,
                token_index=token_index,
                outcome=outcome,
            )
            if frame is not None:
                return self._quote_ticks_from_frame(frame, start=start, end=end)

        for date in self._date_range(start, end):
            frame: pd.DataFrame | None = None
            for entry in config.ordered_source_entries:
                if entry.kind == _TELONEX_SOURCE_LOCAL:
                    assert entry.target is not None
                    frame = self._load_local_day(
                        root=Path(entry.target).expanduser(),
                        channel=config.channel,
                        date=date,
                        market_slug=market_slug,
                        token_index=token_index,
                        outcome=outcome,
                    )
                elif entry.kind == _TELONEX_SOURCE_API:
                    assert entry.target is not None
                    frame = self._load_api_day(
                        base_url=entry.target,
                        channel=config.channel,
                        date=date,
                        market_slug=market_slug,
                        token_index=token_index,
                        outcome=outcome,
                        api_key=entry.api_key,
                    )
                if frame is not None:
                    break
            if frame is None:
                continue
            records.extend(self._quote_ticks_from_frame(frame, start=start, end=end))
        records.sort(key=lambda quote: quote.ts_event)
        return records


__all__ = [
    "TELONEX_API_BASE_URL_ENV",
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
