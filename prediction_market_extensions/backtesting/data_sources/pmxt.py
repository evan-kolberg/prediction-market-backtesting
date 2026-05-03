from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.dataset as ds

from prediction_market_extensions._runtime_log import emit_loader_event
from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader
from prediction_market_extensions.backtesting.data_sources._common import (
    DISABLED_ENV_VALUES,
    env_value,
    normalize_local_path,
    normalize_urlish,
)

PMXT_DATA_SOURCE_ENV = "PMXT_DATA_SOURCE"
PMXT_LOCAL_RAWS_DIR_ENV = "PMXT_LOCAL_RAWS_DIR"
PMXT_RAW_ROOT_ENV = "PMXT_RAW_ROOT"
PMXT_DISABLE_REMOTE_ARCHIVE_ENV = "PMXT_DISABLE_REMOTE_ARCHIVE"
PMXT_REMOTE_BASE_URL_ENV = "PMXT_REMOTE_BASE_URL"
PMXT_CACHE_DIR_ENV = "PMXT_CACHE_DIR"
PMXT_DISABLE_CACHE_ENV = "PMXT_DISABLE_CACHE"
PMXT_SOURCE_PRIORITY_ENV = "PMXT_SOURCE_PRIORITY"
PMXT_PREFETCH_WORKERS_ENV = "PMXT_PREFETCH_WORKERS"
_PMXT_RUNNER_HTTP_USER_AGENT = "prediction-market-backtesting/1.0"
_PMXT_RUNNER_HTTP_TIMEOUT_SECS = 30
_PMXT_LOCAL_RAW_PREFETCH_WORKERS = "8"
_PMXT_ARCHIVE_SOURCE_PREFIXES = ("archive:",)
_PMXT_RAW_LOCAL_SOURCE_PREFIXES = ("local:",)

_PMXT_SOURCE_STAGE_RAW_LOCAL = "raw-local"
_PMXT_SOURCE_STAGE_RAW_REMOTE = "raw-remote"
_PMXT_VALID_SOURCE_STAGES = (
    _PMXT_SOURCE_STAGE_RAW_LOCAL,
    _PMXT_SOURCE_STAGE_RAW_REMOTE,
)

_MODE_ALIASES = {
    "": "auto",
    "auto": "auto",
    "default": "auto",
    "raw": "raw-remote",
    "raw-remote": "raw-remote",
    "remote-raw": "raw-remote",
    "raw-local": "raw-local",
    "local-raw": "raw-local",
    "local-raws": "raw-local",
}
_VALID_MODES = ("auto", "raw-remote", "raw-local")


@dataclass(frozen=True)
class PMXTLoaderConfig:
    mode: str
    raw_root: Path | None
    remote_base_urls: tuple[str, ...]
    disable_remote_archive: bool
    source_priority: tuple[str, ...]
    prefetch_workers: int | None = None
    ordered_source_entries: tuple[tuple[str, str], ...] = ()

    @property
    def remote_base_url(self) -> str | None:
        return self.remote_base_urls[0] if self.remote_base_urls else None


_CURRENT_PMXT_LOADER_CONFIG: ContextVar[PMXTLoaderConfig | None] = ContextVar(
    "pmxt_loader_config", default=None
)


def _current_loader_config() -> PMXTLoaderConfig | None:
    return _CURRENT_PMXT_LOADER_CONFIG.get()


class RunnerPolymarketPMXTDataLoader(PolymarketPMXTDataLoader):
    """
    Repo-layer PMXT loader extensions used by the backtest runners.

    This keeps BYOD/local-mirror behavior out of the vendored Nautilus subtree.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._pmxt_source_lock = threading.RLock()
        self._pmxt_remote_base_urls = self._resolve_remote_base_urls()
        self._pmxt_remote_base_url = (
            self._pmxt_remote_base_urls[0] if self._pmxt_remote_base_urls else None
        )
        self._pmxt_raw_root = self._resolve_raw_root()
        config = _current_loader_config()
        self._pmxt_disable_remote_archive = (
            config.disable_remote_archive
            if config is not None
            else self._env_flag_enabled(os.getenv(PMXT_DISABLE_REMOTE_ARCHIVE_ENV))
        )
        self._pmxt_source_priority = self._resolve_source_priority()
        self._pmxt_ordered_source_entries = (
            config.ordered_source_entries if config is not None else ()
        )

    @staticmethod
    def _row_count_from_batches(batches: Sequence[object]) -> int:
        return sum(int(getattr(batch, "num_rows", 0)) for batch in batches)

    @staticmethod
    def _hour_label(hour) -> str:  # type: ignore[no-untyped-def]
        try:
            return hour.tz_convert("UTC").isoformat()
        except Exception:
            return str(hour)

    def _emit_pmxt_source_event(
        self,
        *,
        message: str,
        stage: str,
        status: str,
        hour,
        source_kind: str | None = None,
        source: str | None = None,
        cache_path: Path | None = None,
        rows: int | None = None,
        bytes_count: int | None = None,
        level: str = "INFO",
    ) -> None:  # type: ignore[no-untyped-def]
        attrs: dict[str, object] = {"hour": self._hour_label(hour)}
        emit_loader_event(
            message,
            level=level,
            stage=stage,
            status=status,
            vendor="pmxt",
            platform="polymarket",
            data_type="book",
            source_kind=source_kind,
            source=source,
            cache_path=str(cache_path) if cache_path is not None else None,
            condition_id=getattr(self, "condition_id", None),
            token_id=getattr(self, "token_id", None),
            rows=rows,
            bytes=bytes_count,
            attrs=attrs,
        )

    @staticmethod
    def _source_kind_for_stage(stage: str) -> str:
        return "local" if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL else "remote"

    @staticmethod
    def _source_label_for_stage(stage: str, target: str | None) -> str | None:
        if target is None:
            return None
        if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
            return f"local:{target}"
        if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
            return f"archive:{target}"
        return target

    @classmethod
    def _resolve_raw_root(cls) -> Path | None:
        config = _current_loader_config()
        if config is not None:
            return config.raw_root

        configured = os.getenv(PMXT_RAW_ROOT_ENV)
        if configured is None:
            return None

        value = configured.strip()
        if value.casefold() in DISABLED_ENV_VALUES:
            return None

        return Path(value).expanduser()

    @classmethod
    def _resolve_remote_base_url(cls) -> str | None:
        urls = cls._resolve_remote_base_urls()
        return urls[0] if urls else None

    @classmethod
    def _resolve_remote_base_urls(cls) -> tuple[str, ...]:
        config = _current_loader_config()
        if config is not None:
            return config.remote_base_urls

        configured = env_value(os.getenv(PMXT_REMOTE_BASE_URL_ENV))
        if configured is None:
            return ()
        if configured.casefold() in DISABLED_ENV_VALUES:
            return ()
        urls: list[str] = []
        for part in configured.split(","):
            cleaned = part.strip()
            if not cleaned or cleaned.casefold() in DISABLED_ENV_VALUES:
                continue
            normalized = normalize_urlish(cleaned)
            if normalized and normalized not in urls:
                urls.append(normalized)
        return tuple(urls)

    def _archive_url_for_hour(self, hour):  # type: ignore[override]
        urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
        if not urls:
            single = getattr(self, "_pmxt_remote_base_url", None) or self._resolve_remote_base_url()
            if single is None:
                raise RuntimeError(
                    f"{PMXT_REMOTE_BASE_URL_ENV} is required for remote PMXT archive access."
                )
            urls = (single,)
        return f"{urls[0]}/{self._archive_filename_for_hour(hour)}"

    def _archive_urls_for_hour(self, hour):  # type: ignore[no-untyped-def]
        urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
        if not urls:
            single = getattr(self, "_pmxt_remote_base_url", None)
            urls = (single,) if single else ()
        filename = self._archive_filename_for_hour(hour)
        return tuple(f"{url}/{filename}" for url in urls)

    def _raw_path_for_hour(self, hour) -> Path | None:  # type: ignore[no-untyped-def]
        if self._pmxt_raw_root is None:
            return None

        ts = hour.tz_convert("UTC")
        return (
            self._pmxt_raw_root
            / str(ts.year)
            / f"{ts.month:02d}"
            / f"{ts.day:02d}"
            / self._archive_filename_for_hour(hour)
        )

    def _raw_paths_for_hour_at_root(self, raw_root: Path, hour) -> tuple[Path, ...]:  # type: ignore[no-untyped-def]
        return self._local_archive_candidate_paths_for_hour(raw_root, hour)

    def _load_local_raw_market_batches_from_root(
        self,
        raw_root: Path,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        for raw_path in self._raw_paths_for_hour_at_root(raw_root, hour):
            if not raw_path.exists():
                continue

            try:
                dataset = ds.dataset(str(raw_path), format="parquet")
            except (OSError, ValueError, pa.ArrowException):
                continue

            try:
                return self._scan_raw_market_batches(
                    dataset,
                    batch_size=batch_size,
                    source=str(raw_path),
                    total_bytes=self._progress_total_bytes(str(raw_path)),
                )
            except (OSError, ValueError, pa.ArrowException):
                continue

        return None

    def _load_local_raw_market_batches(self, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        if self._pmxt_raw_root is None:
            return None

        return self._load_local_raw_market_batches_from_root(
            self._pmxt_raw_root,
            hour,
            batch_size=batch_size,
        )

    def _load_local_archive_market_batches(self, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        if self._pmxt_raw_root is not None:
            return self._load_local_raw_market_batches(hour, batch_size=batch_size)

        return super()._load_local_archive_market_batches(hour, batch_size=batch_size)

    def _load_remote_market_batches(self, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        if self._pmxt_disable_remote_archive:
            return None

        urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
        if not urls and self._pmxt_remote_base_url is not None:
            urls = (self._pmxt_remote_base_url,)
        if not urls:
            return None

        original = self._pmxt_remote_base_url
        try:
            for url in urls:
                self._pmxt_remote_base_url = url
                batches = super()._load_remote_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    return batches
            return None
        finally:
            self._pmxt_remote_base_url = original

    def _archive_url_for_base_url(self, base_url: str, hour) -> str:  # type: ignore[no-untyped-def]
        return f"{base_url.rstrip('/')}/{self._archive_filename_for_hour(hour)}"

    def _load_remote_market_batches_from_base_url(
        self,
        base_url: str,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        return self._load_raw_market_batches_via_download(
            self._archive_url_for_base_url(base_url, hour),
            batch_size=batch_size,
        )

    @classmethod
    def _resolve_source_priority(cls) -> tuple[str, ...]:
        config = _current_loader_config()
        if config is not None:
            return config.source_priority

        configured = env_value(os.getenv(PMXT_SOURCE_PRIORITY_ENV))
        if configured is None:
            return _PMXT_VALID_SOURCE_STAGES

        priority: list[str] = []
        for part in configured.split(","):
            stage = part.strip().casefold()
            if not stage:
                continue
            if stage not in _PMXT_VALID_SOURCE_STAGES:
                valid_stages = ", ".join(_PMXT_VALID_SOURCE_STAGES)
                raise ValueError(
                    f"Unsupported {PMXT_SOURCE_PRIORITY_ENV} stage {stage!r}. Use one of: {valid_stages}."
                )
            if stage not in priority:
                priority.append(stage)
        return tuple(priority) or _PMXT_VALID_SOURCE_STAGES

    @classmethod
    def _resolve_prefetch_workers(cls) -> int:
        config = _current_loader_config()
        if config is not None and config.prefetch_workers is not None:
            return config.prefetch_workers
        return super()._resolve_prefetch_workers()

    @contextmanager
    def _scoped_source_entry(self, kind: str, target: str):  # type: ignore[no-untyped-def]
        """
        Temporarily bind the loader's active raw_root / remote_base_url
        to match the entry under evaluation. Restores the prior values afterwards.
        """
        lock = getattr(self, "_pmxt_source_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._pmxt_source_lock = lock
        with lock:
            prior_raw_root = self._pmxt_raw_root
            prior_remote_url = self._pmxt_remote_base_url
            prior_remote_urls = getattr(self, "_pmxt_remote_base_urls", ())
            try:
                if kind == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                    self._pmxt_raw_root = Path(target).expanduser()
                elif kind == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                    self._pmxt_remote_base_url = target
                    self._pmxt_remote_base_urls = (target,)
                yield
            finally:
                self._pmxt_raw_root = prior_raw_root
                self._pmxt_remote_base_url = prior_remote_url
                if hasattr(self, "_pmxt_remote_base_urls"):
                    self._pmxt_remote_base_urls = prior_remote_urls

    def _load_entry_batches(self, kind: str, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        if kind == _PMXT_SOURCE_STAGE_RAW_LOCAL:
            raw_root = getattr(self, "_pmxt_raw_root", None)
            if raw_root is None:
                return None
            return self._load_local_raw_market_batches_from_root(
                Path(raw_root).expanduser(),
                hour,
                batch_size=batch_size,
            )
        if kind == _PMXT_SOURCE_STAGE_RAW_REMOTE:
            remote_url = getattr(self, "_pmxt_remote_base_url", None)
            if remote_url is None:
                return None
            return self._load_remote_market_batches_from_base_url(
                str(remote_url),
                hour,
                batch_size=batch_size,
            )
        return None

    def _load_ordered_entry_batches(
        self,
        kind: str,
        target: str,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        if kind == _PMXT_SOURCE_STAGE_RAW_LOCAL:
            return self._load_local_raw_market_batches_from_root(
                Path(target).expanduser(),
                hour,
                batch_size=batch_size,
            )
        if kind == _PMXT_SOURCE_STAGE_RAW_REMOTE:
            return self._load_remote_market_batches_from_base_url(
                target,
                hour,
                batch_size=batch_size,
            )
        return None

    def _write_cache_if_enabled(self, hour, table) -> None:  # type: ignore[no-untyped-def]
        if self._pmxt_cache_dir is not None:
            cache_path = self._cache_path_for_hour(hour)
            with suppress(OSError, pa.ArrowException):
                self._write_market_cache(hour, table)
                self._emit_pmxt_source_event(
                    message=(
                        "Wrote PMXT filtered market cache "
                        f"for {self._hour_label(hour)} ({table.num_rows} rows)"
                    ),
                    stage="cache_write",
                    status="complete",
                    hour=hour,
                    source_kind="cache",
                    cache_path=cache_path,
                    rows=int(table.num_rows),
                )

    def _load_market_table(self, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        table = self._load_cached_market_table(hour)
        if table is not None:
            return table

        ordered_entries = getattr(self, "_pmxt_ordered_source_entries", ()) or ()
        if ordered_entries:
            for kind, target in ordered_entries:
                entry_batches = self._load_ordered_entry_batches(
                    kind,
                    target,
                    hour,
                    batch_size=batch_size,
                )
                if entry_batches is None:
                    continue
                if kind == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                    table = (
                        pa.Table.from_batches(entry_batches)
                        if entry_batches
                        else self._empty_market_table()
                    )
                    table = self._filter_table_to_token(table)
                else:
                    table = (
                        pa.Table.from_batches(entry_batches)
                        if entry_batches
                        else self._empty_market_table()
                    )
                self._write_cache_if_enabled(hour, table)
                return table
            return None

        for stage in self._pmxt_source_priority:
            if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                local_archive_batches = self._load_local_archive_market_batches(
                    hour, batch_size=batch_size
                )
                if local_archive_batches is not None:
                    table = (
                        pa.Table.from_batches(local_archive_batches)
                        if local_archive_batches
                        else self._empty_market_table()
                    )
                    self._write_cache_if_enabled(hour, table)
                    return table
                continue

            if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                remote_table = self._load_remote_market_table(hour, batch_size=batch_size)
                if remote_table is not None:
                    remote_table = self._filter_table_to_token(remote_table)
                    self._write_cache_if_enabled(hour, remote_table)
                    return remote_table
                continue

        return None

    def _load_market_batches(self, hour, *, batch_size: int):  # type: ignore[no-untyped-def]
        batches = self._load_cached_market_batches(hour)
        if batches is not None:
            cache_path = self._cache_path_for_hour(hour)
            rows = self._row_count_from_batches(batches)
            self._emit_pmxt_source_event(
                message=f"Loaded PMXT filtered cache for {self._hour_label(hour)} ({rows} rows)",
                stage="cache_read",
                status="cache_hit",
                hour=hour,
                source_kind="cache",
                cache_path=cache_path,
                rows=rows,
            )
            return batches
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is not None:
            self._emit_pmxt_source_event(
                message=f"PMXT filtered cache miss for {self._hour_label(hour)}",
                stage="cache_read",
                status="cache_miss",
                hour=hour,
                source_kind="cache",
                cache_path=cache_path,
            )

        ordered_entries = getattr(self, "_pmxt_ordered_source_entries", ()) or ()
        if ordered_entries:
            for kind, target in ordered_entries:
                source = self._source_label_for_stage(kind, target)
                self._emit_pmxt_source_event(
                    message=(
                        f"Trying PMXT {self._source_kind_for_stage(kind)} source "
                        f"for {self._hour_label(hour)}"
                    ),
                    stage="fetch",
                    status="start",
                    hour=hour,
                    source_kind=self._source_kind_for_stage(kind),
                    source=source,
                )
                entry_batches = self._load_ordered_entry_batches(
                    kind,
                    target,
                    hour,
                    batch_size=batch_size,
                )
                if entry_batches is not None:
                    rows = self._row_count_from_batches(entry_batches)
                    self._emit_pmxt_source_event(
                        message=(
                            f"Loaded PMXT {self._source_kind_for_stage(kind)} source "
                            f"for {self._hour_label(hour)} ({rows} rows)"
                        ),
                        stage="fetch",
                        status="complete",
                        hour=hour,
                        source_kind=self._source_kind_for_stage(kind),
                        source=source,
                        rows=rows,
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(entry_batches)
                            if entry_batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return entry_batches
                self._emit_pmxt_source_event(
                    message=(
                        f"PMXT {self._source_kind_for_stage(kind)} source had no usable data "
                        f"for {self._hour_label(hour)}"
                    ),
                    stage="fetch",
                    status="skip",
                    hour=hour,
                    source_kind=self._source_kind_for_stage(kind),
                    source=source,
                )
            return None

        for stage in self._pmxt_source_priority:
            if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                source = (
                    f"local:{self._pmxt_raw_root}"
                    if self._pmxt_raw_root is not None
                    else (
                        f"local:{self._pmxt_local_archive_dir}"
                        if getattr(self, "_pmxt_local_archive_dir", None) is not None
                        else None
                    )
                )
                self._emit_pmxt_source_event(
                    message=f"Trying PMXT local source for {self._hour_label(hour)}",
                    stage="fetch",
                    status="start",
                    hour=hour,
                    source_kind="local",
                    source=source,
                )
                batches = self._load_local_archive_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    rows = self._row_count_from_batches(batches)
                    self._emit_pmxt_source_event(
                        message=f"Loaded PMXT local source for {self._hour_label(hour)} ({rows} rows)",
                        stage="fetch",
                        status="complete",
                        hour=hour,
                        source_kind="local",
                        source=source,
                        rows=rows,
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return batches
                self._emit_pmxt_source_event(
                    message=f"PMXT local source had no usable data for {self._hour_label(hour)}",
                    stage="fetch",
                    status="skip",
                    hour=hour,
                    source_kind="local",
                    source=source,
                )
                continue

            if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                remote_urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
                source = ",".join(f"archive:{url}" for url in remote_urls) or (
                    f"archive:{self._pmxt_remote_base_url}"
                    if self._pmxt_remote_base_url is not None
                    else None
                )
                self._emit_pmxt_source_event(
                    message=f"Trying PMXT archive source for {self._hour_label(hour)}",
                    stage="fetch",
                    status="start",
                    hour=hour,
                    source_kind="remote",
                    source=source,
                )
                batches = self._load_remote_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    rows = self._row_count_from_batches(batches)
                    self._emit_pmxt_source_event(
                        message=(
                            f"Loaded PMXT archive source for {self._hour_label(hour)} ({rows} rows)"
                        ),
                        stage="fetch",
                        status="complete",
                        hour=hour,
                        source_kind="remote",
                        source=source,
                        rows=rows,
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return batches
                self._emit_pmxt_source_event(
                    message=f"PMXT archive source had no usable data for {self._hour_label(hour)}",
                    stage="fetch",
                    status="skip",
                    hour=hour,
                    source_kind="remote",
                    source=source,
                )
                continue

        return None

    def _download_to_file_with_progress(self, url: str, destination: Path) -> int | None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        request = Request(url, headers={"User-Agent": _PMXT_RUNNER_HTTP_USER_AGENT})
        with (
            urlopen(request, timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS) as response,
            destination.open("wb") as handle,
        ):
            total_bytes = self._content_length_from_response(response)
            downloaded_bytes = 0
            last_emit = 0.0
            supports_chunked_read = True
            self._emit_download_progress(
                url, downloaded_bytes=0, total_bytes=total_bytes, finished=False
            )
            while True:
                if supports_chunked_read:
                    try:
                        chunk = response.read(self._PMXT_DOWNLOAD_CHUNK_SIZE)
                    except TypeError:
                        supports_chunked_read = False
                        chunk = response.read()
                else:
                    break
                if not chunk:
                    break
                handle.write(chunk)
                downloaded_bytes += len(chunk)
                now = time.monotonic()
                if downloaded_bytes == total_bytes or (now - last_emit) >= 0.2:
                    self._emit_download_progress(
                        url,
                        downloaded_bytes=downloaded_bytes,
                        total_bytes=total_bytes,
                        finished=False,
                    )
                    last_emit = now
                if not supports_chunked_read:
                    break
            self._emit_download_progress(
                url, downloaded_bytes=downloaded_bytes, total_bytes=total_bytes, finished=True
            )

        if total_bytes is None:
            with suppress(OSError):
                total_bytes = destination.stat().st_size

        cache = getattr(self, "_pmxt_progress_size_cache", None)
        if cache is None:
            cache = {}
            self._pmxt_progress_size_cache = cache
        cache[url] = total_bytes
        return total_bytes

    def _download_payload_with_progress(self, url: str) -> bytes | None:
        request = Request(url, headers={"User-Agent": _PMXT_RUNNER_HTTP_USER_AGENT})
        with urlopen(request, timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS) as response:
            total_bytes = self._content_length_from_response(response)
            downloaded_bytes = 0
            last_emit = 0.0
            chunks: list[bytes] = []
            supports_chunked_read = True
            self._emit_download_progress(
                url, downloaded_bytes=0, total_bytes=total_bytes, finished=False
            )
            while True:
                if supports_chunked_read:
                    try:
                        chunk = response.read(self._PMXT_DOWNLOAD_CHUNK_SIZE)
                    except TypeError:
                        supports_chunked_read = False
                        chunk = response.read()
                else:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
                downloaded_bytes += len(chunk)
                now = time.monotonic()
                if downloaded_bytes == total_bytes or (now - last_emit) >= 0.2:
                    self._emit_download_progress(
                        url,
                        downloaded_bytes=downloaded_bytes,
                        total_bytes=total_bytes,
                        finished=False,
                    )
                    last_emit = now
                if not supports_chunked_read:
                    break
            self._emit_download_progress(
                url, downloaded_bytes=downloaded_bytes, total_bytes=total_bytes, finished=True
            )
            return b"".join(chunks)

    def _progress_total_bytes(self, source: str) -> int | None:  # type: ignore[override]
        if getattr(self, "_pmxt_scan_progress_callback", None) is None:
            return None

        cache = getattr(self, "_pmxt_progress_size_cache", None)
        if cache is None:
            cache = {}
            self._pmxt_progress_size_cache = cache
        if source in cache:
            return cache[source]

        total_bytes: int | None = None
        if "://" in source:
            request = Request(
                source, method="HEAD", headers={"User-Agent": _PMXT_RUNNER_HTTP_USER_AGENT}
            )
            try:
                with urlopen(request, timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS) as response:
                    total_bytes = self._content_length_from_response(response)
            except Exception:
                total_bytes = None
        else:
            try:
                total_bytes = Path(source).expanduser().stat().st_size
            except OSError:
                total_bytes = None

        cache[source] = total_bytes
        return total_bytes


@dataclass(frozen=True)
class PMXTDataSourceSelection:
    mode: str
    summary: str


def _normalize_mode(value: str | None) -> str:
    if value is None:
        return "auto"

    normalized = value.strip().casefold().replace("_", "-")
    try:
        return _MODE_ALIASES[normalized]
    except KeyError as exc:
        valid_modes = ", ".join(_VALID_MODES)
        raise ValueError(
            f"Unsupported {PMXT_DATA_SOURCE_ENV}={value!r}. Use one of: {valid_modes}."
        ) from exc


def _env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _env_enabled(name: str) -> bool:
    value = _env_value(name)
    if value is None:
        return False
    return value.casefold() not in DISABLED_ENV_VALUES


def _resolve_prefetch_workers_override(*, default_when_unset: int | None) -> int | None:
    configured = _env_value(PMXT_PREFETCH_WORKERS_ENV)
    if configured is None:
        return default_when_unset
    try:
        return max(1, int(configured))
    except ValueError:
        return default_when_unset


def _resolve_source_priority_override() -> tuple[str, ...]:
    configured = env_value(os.getenv(PMXT_SOURCE_PRIORITY_ENV))
    if configured is None:
        return _PMXT_VALID_SOURCE_STAGES

    priority: list[str] = []
    for part in configured.split(","):
        stage = part.strip().casefold()
        if not stage:
            continue
        if stage not in _PMXT_VALID_SOURCE_STAGES:
            valid_stages = ", ".join(_PMXT_VALID_SOURCE_STAGES)
            raise ValueError(
                f"Unsupported {PMXT_SOURCE_PRIORITY_ENV} stage {stage!r}. Use one of: {valid_stages}."
            )
        if stage not in priority:
            priority.append(stage)
    return tuple(priority) or _PMXT_VALID_SOURCE_STAGES


def _resolve_existing_remote_url() -> str | None:
    urls = _resolve_existing_remote_urls()
    return urls[0] if urls else None


def _resolve_existing_remote_urls() -> tuple[str, ...]:
    configured = os.getenv(PMXT_REMOTE_BASE_URL_ENV)
    if configured is None:
        return ()

    urls: list[str] = []
    for part in configured.split(","):
        cleaned = part.strip().rstrip("/")
        if not cleaned or cleaned.casefold() in DISABLED_ENV_VALUES:
            continue
        normalized = normalize_urlish(cleaned)
        if normalized and normalized not in urls:
            urls.append(normalized)
    return tuple(urls)


def _resolve_required_directory(env_name: str, *, label: str) -> Path:
    configured = os.getenv(env_name)
    if configured is None or configured.strip().casefold() in DISABLED_ENV_VALUES:
        raise ValueError(f"{env_name} is required when using {label}.")

    path = Path(configured).expanduser()
    if not path.exists():
        raise ValueError(f"{label} path does not exist: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} path is not a directory: {path}")
    return path


def _strip_prefixed_local_source(source: str, *, prefixes: Sequence[str]) -> str | None:
    for prefix in prefixes:
        if source.casefold().startswith(prefix):
            remainder = source[len(prefix) :].strip()
            if not remainder:
                raise ValueError(f"PMXT explicit source {source!r} is missing a local path.")
            return normalize_local_path(remainder)
    return None


def _strip_prefixed_remote_source(source: str, *, prefixes: Sequence[str]) -> str | None:
    for prefix in prefixes:
        if source.casefold().startswith(prefix):
            remainder = source[len(prefix) :].strip()
            if not remainder:
                raise ValueError(f"PMXT explicit source {source!r} is missing a remote URL.")
            return normalize_urlish(remainder)
    return None


def _classify_explicit_pmxt_sources(
    sources: Sequence[str],
) -> tuple[
    str | None,
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[tuple[str, str], ...],
]:
    """
    Classify explicit DATA.sources entries preserving user-provided order.

    Returns (first_raw_root, remote_urls_ordered_dedup, stage_priority_dedup,
             ordered_display_entries_dedup,
             ordered_entries_full_with_duplicates).

    ordered_entries_full_with_duplicates is the authoritative per-entry
    evaluation order — each entry is (kind, target) where kind is one of
    "raw-local" / "raw-remote". Duplicates are preserved so
    users can interleave the same target multiple times between other kinds.
    """
    raw_root_first: str | None = None
    remote_urls_dedup: list[str] = []
    priority_dedup: list[str] = []
    display_dedup: list[str] = []
    ordered_entries: list[tuple[str, str]] = []

    for source in sources:
        stripped = source.strip()
        if not stripped:
            continue
        if stripped.casefold() == "cache":
            raise ValueError(
                "Unsupported PMXT explicit source 'cache'. "
                "The cache layer is implicit. Use local:/path to pin a local raw "
                "mirror, or archive: to control remote fetch order."
            )
        normalized_archive = _strip_prefixed_remote_source(
            stripped, prefixes=_PMXT_ARCHIVE_SOURCE_PREFIXES
        )
        if normalized_archive is not None:
            ordered_entries.append((_PMXT_SOURCE_STAGE_RAW_REMOTE, normalized_archive))
            if normalized_archive not in remote_urls_dedup:
                remote_urls_dedup.append(normalized_archive)
            if _PMXT_SOURCE_STAGE_RAW_REMOTE not in priority_dedup:
                priority_dedup.append(_PMXT_SOURCE_STAGE_RAW_REMOTE)
            archive_display = f"archive {normalized_archive}"
            if archive_display not in display_dedup:
                display_dedup.append(archive_display)
            continue
        normalized_raw = _strip_prefixed_local_source(
            stripped, prefixes=_PMXT_RAW_LOCAL_SOURCE_PREFIXES
        )
        if normalized_raw is not None:
            ordered_entries.append((_PMXT_SOURCE_STAGE_RAW_LOCAL, normalized_raw))
            if raw_root_first is None:
                raw_root_first = normalized_raw
            if _PMXT_SOURCE_STAGE_RAW_LOCAL not in priority_dedup:
                priority_dedup.append(_PMXT_SOURCE_STAGE_RAW_LOCAL)
            raw_display = f"local {normalized_raw}"
            if raw_display not in display_dedup:
                display_dedup.append(raw_display)
            continue
        raise ValueError(
            f"Unsupported PMXT explicit source {stripped!r}. Use one of: local:, archive:."
        )

    return (
        raw_root_first,
        tuple(remote_urls_dedup),
        tuple(priority_dedup),
        tuple(display_dedup),
        tuple(ordered_entries),
    )


def _explicit_source_summary(
    *,
    ordered_sources: Sequence[str],
    ordered_entries: Sequence[tuple[str, str]] = (),
) -> str:
    if ordered_entries:
        labels = {
            _PMXT_SOURCE_STAGE_RAW_LOCAL: "local",
            _PMXT_SOURCE_STAGE_RAW_REMOTE: "archive",
        }
        parts = ["cache"] + [
            f"{labels.get(kind, kind)} {target}" for kind, target in ordered_entries
        ]
    else:
        parts = ["cache", *ordered_sources]
    return "PMXT source: explicit priority (" + " -> ".join(parts) + ")"


def resolve_pmxt_loader_config(
    *, sources: Sequence[str] | None = None
) -> tuple[PMXTDataSourceSelection, PMXTLoaderConfig]:
    if sources:
        (
            raw_root,
            remote_base_urls,
            source_priority,
            ordered_sources,
            ordered_source_entries,
        ) = _classify_explicit_pmxt_sources(sources)
        return (
            PMXTDataSourceSelection(
                mode="auto",
                summary=_explicit_source_summary(
                    ordered_sources=ordered_sources,
                    ordered_entries=ordered_source_entries,
                ),
            ),
            PMXTLoaderConfig(
                mode="auto",
                raw_root=Path(raw_root).expanduser() if raw_root is not None else None,
                remote_base_urls=remote_base_urls,
                disable_remote_archive=not remote_base_urls,
                source_priority=source_priority or _PMXT_VALID_SOURCE_STAGES,
                prefetch_workers=(
                    _resolve_prefetch_workers_override(
                        default_when_unset=int(_PMXT_LOCAL_RAW_PREFETCH_WORKERS)
                    )
                    if raw_root is not None
                    else None
                ),
                ordered_source_entries=ordered_source_entries,
            ),
        )

    configured_mode = os.getenv(PMXT_DATA_SOURCE_ENV)
    mode = _normalize_mode(configured_mode)
    source_priority = _resolve_source_priority_override()

    if configured_mode is None:
        raw_root = _env_value(PMXT_RAW_ROOT_ENV)
        remote_base_url = _env_value(PMXT_REMOTE_BASE_URL_ENV)
        raw_root_path = (
            Path(raw_root).expanduser()
            if raw_root is not None and raw_root.casefold() not in DISABLED_ENV_VALUES
            else None
        )
        resolved_remote_urls = _resolve_existing_remote_urls()
        disable_remote_archive = _env_enabled(PMXT_DISABLE_REMOTE_ARCHIVE_ENV)

        if raw_root_path is not None:
            return (
                PMXTDataSourceSelection(
                    mode="raw-local", summary=f"PMXT source: local raws ({raw_root_path})"
                ),
                PMXTLoaderConfig(
                    mode="raw-local",
                    raw_root=raw_root_path,
                    remote_base_urls=resolved_remote_urls,
                    disable_remote_archive=disable_remote_archive,
                    source_priority=source_priority,
                ),
            )

        if remote_base_url is not None and remote_base_url.casefold() in DISABLED_ENV_VALUES:
            return (
                PMXTDataSourceSelection(
                    mode="auto", summary="PMXT source: auto (cache -> local raws)"
                ),
                PMXTLoaderConfig(
                    mode="auto",
                    raw_root=None,
                    remote_base_urls=(),
                    disable_remote_archive=True,
                    source_priority=source_priority,
                ),
            )

        return (
            PMXTDataSourceSelection(
                mode="auto",
                summary="PMXT source: auto (cache -> local raws -> explicit remote raw)",
            ),
            PMXTLoaderConfig(
                mode="auto",
                raw_root=None,
                remote_base_urls=resolved_remote_urls,
                disable_remote_archive=disable_remote_archive,
                source_priority=source_priority,
            ),
        )

    if mode == "auto":
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary="PMXT source: auto (cache -> local raws -> explicit remote raw)",
            ),
            PMXTLoaderConfig(
                mode=mode,
                raw_root=None,
                remote_base_urls=_resolve_existing_remote_urls(),
                disable_remote_archive=False,
                source_priority=source_priority,
            ),
        )

    if mode == "raw-remote":
        return (
            PMXTDataSourceSelection(mode=mode, summary="PMXT source: raw remote archive"),
            PMXTLoaderConfig(
                mode=mode,
                raw_root=None,
                remote_base_urls=_resolve_existing_remote_urls(),
                disable_remote_archive=False,
                source_priority=source_priority,
            ),
        )

    if mode == "raw-local":
        raw_root = _resolve_required_directory(PMXT_LOCAL_RAWS_DIR_ENV, label="local PMXT raws")
        return (
            PMXTDataSourceSelection(mode=mode, summary=f"PMXT source: local raws ({raw_root})"),
            PMXTLoaderConfig(
                mode=mode,
                raw_root=raw_root,
                remote_base_urls=(),
                disable_remote_archive=True,
                source_priority=source_priority,
                prefetch_workers=_resolve_prefetch_workers_override(
                    default_when_unset=int(_PMXT_LOCAL_RAW_PREFETCH_WORKERS)
                ),
            ),
        )
    raise AssertionError(f"Unsupported PMXT mode normalization result: {mode}")


def _loader_config_to_env_updates(config: PMXTLoaderConfig) -> dict[str, str | None]:
    return {
        PMXT_RAW_ROOT_ENV: str(config.raw_root) if config.raw_root is not None else None,
        PMXT_REMOTE_BASE_URL_ENV: (
            ",".join(config.remote_base_urls) if config.remote_base_urls else "0"
        ),
        PMXT_DISABLE_REMOTE_ARCHIVE_ENV: ("1" if config.disable_remote_archive else None),
        PMXT_SOURCE_PRIORITY_ENV: ",".join(config.source_priority) or None,
        PMXT_PREFETCH_WORKERS_ENV: (
            str(config.prefetch_workers) if config.prefetch_workers is not None else None
        ),
    }


def resolve_pmxt_data_source_selection(
    *, sources: Sequence[str] | None = None
) -> tuple[PMXTDataSourceSelection, dict[str, str | None]]:
    selection, config = resolve_pmxt_loader_config(sources=sources)
    if sources or config.mode == "raw-local" or os.getenv(PMXT_DATA_SOURCE_ENV) is not None:
        return selection, _loader_config_to_env_updates(config)
    return selection, {}


@contextmanager
def configured_pmxt_data_source(
    *, sources: Sequence[str] | None = None
) -> Iterator[PMXTDataSourceSelection]:
    selection, config = resolve_pmxt_loader_config(sources=sources)
    token = _CURRENT_PMXT_LOADER_CONFIG.set(config)
    try:
        yield selection
    finally:
        _CURRENT_PMXT_LOADER_CONFIG.reset(token)
