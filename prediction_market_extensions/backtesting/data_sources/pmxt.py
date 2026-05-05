from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from prediction_market_extensions._runtime_log import (
    emit_loader_event,
    loader_progress_logs_enabled,
)
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
PMXT_CACHE_PREFETCH_WORKERS_ENV = "PMXT_CACHE_PREFETCH_WORKERS"
PMXT_ROW_GROUP_CHUNK_SIZE_ENV = "PMXT_ROW_GROUP_CHUNK_SIZE"
PMXT_ROW_GROUP_SCAN_WORKERS_ENV = "PMXT_ROW_GROUP_SCAN_WORKERS"
_PMXT_RUNNER_HTTP_USER_AGENT = "prediction-market-backtesting/1.0"
_PMXT_RUNNER_HTTP_TIMEOUT_SECS = 30
_PMXT_LOCAL_RAW_PREFETCH_WORKERS = "6"
_PMXT_DEFAULT_CACHE_PREFETCH_WORKERS = 32
_PMXT_DEFAULT_ROW_GROUP_CHUNK_SIZE = 4
_PMXT_DEFAULT_ROW_GROUP_SCAN_WORKERS = 2
_PMXT_ARCHIVE_SOURCE_PREFIXES = ("archive:",)
_PMXT_RAW_LOCAL_SOURCE_PREFIXES = ("local:",)

_PMXT_SOURCE_STAGE_RAW_LOCAL = "raw-local"
_PMXT_SOURCE_STAGE_RAW_REMOTE = "raw-remote"
_PMXT_VALID_SOURCE_STAGES = (
    _PMXT_SOURCE_STAGE_RAW_LOCAL,
    _PMXT_SOURCE_STAGE_RAW_REMOTE,
)


@dataclass
class _RawDownloadLockEntry:
    lock: threading.Lock
    users: int = 0


_PMXT_RAW_DOWNLOAD_LOCKS_LOCK = threading.Lock()
_PMXT_RAW_DOWNLOAD_LOCKS: dict[str, _RawDownloadLockEntry] = {}
_PMXT_ROW_GROUP_SCAN_LOCK = threading.Lock()
_PMXT_ROW_GROUP_SCAN_SEMAPHORE: threading.BoundedSemaphore | None = None
_PMXT_ROW_GROUP_SCAN_SEMAPHORE_WORKERS = 0

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


def _release_arrow_memory() -> None:
    try:
        pa.default_memory_pool().release_unused()
    except AttributeError:
        pass


def _resolve_positive_int_env(name: str, *, default: int) -> int:
    configured = env_value(os.getenv(name))
    if configured is None:
        return max(1, int(default))
    try:
        return max(1, int(configured))
    except ValueError:
        return max(1, int(default))


def _pmxt_row_group_scan_semaphore(workers: int) -> threading.BoundedSemaphore:
    global _PMXT_ROW_GROUP_SCAN_SEMAPHORE
    global _PMXT_ROW_GROUP_SCAN_SEMAPHORE_WORKERS

    resolved_workers = max(1, int(workers))
    with _PMXT_ROW_GROUP_SCAN_LOCK:
        if (
            _PMXT_ROW_GROUP_SCAN_SEMAPHORE is None
            or _PMXT_ROW_GROUP_SCAN_SEMAPHORE_WORKERS != resolved_workers
        ):
            _PMXT_ROW_GROUP_SCAN_SEMAPHORE = threading.BoundedSemaphore(resolved_workers)
            _PMXT_ROW_GROUP_SCAN_SEMAPHORE_WORKERS = resolved_workers
        return _PMXT_ROW_GROUP_SCAN_SEMAPHORE


@contextmanager
def _bounded_pmxt_row_group_scan(workers: int) -> Iterator[None]:
    semaphore = _pmxt_row_group_scan_semaphore(workers)
    semaphore.acquire()
    try:
        yield
    finally:
        semaphore.release()


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

    def _pmxt_source_attrs(
        self, hour, extra_attrs: dict[str, object] | None = None
    ) -> dict[str, object]:  # type: ignore[no-untyped-def]
        attrs: dict[str, object] = {"hour": self._hour_label(hour)}
        if extra_attrs:
            attrs.update(extra_attrs)
        return attrs

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

        return self._raw_path_for_hour_at_root(Path(self._pmxt_raw_root), hour)

    def _raw_path_for_hour_at_root(self, raw_root: Path, hour) -> Path:  # type: ignore[no-untyped-def]
        ts = hour.tz_convert("UTC")
        return (
            raw_root.expanduser()
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

            batches = self._load_raw_market_batches_from_local_file(
                raw_path,
                batch_size=batch_size,
                progress_source=str(raw_path),
                total_bytes=self._progress_total_bytes(str(raw_path)),
            )
            if batches is not None:
                return batches

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
        archive_url = self._archive_url_for_base_url(base_url, hour)
        persisted_batches = self._load_remote_market_batches_via_raw_root(
            archive_url,
            hour,
            batch_size=batch_size,
        )
        if persisted_batches is not None:
            return persisted_batches
        return self._load_raw_market_batches_via_download(archive_url, batch_size=batch_size)

    def _raw_persistence_root(self) -> Path | None:
        raw_root = getattr(self, "_pmxt_raw_root", None)
        if raw_root is not None:
            return Path(raw_root).expanduser()

        for kind, target in getattr(self, "_pmxt_ordered_source_entries", ()) or ():
            if kind == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                return Path(target).expanduser()
        return None

    @staticmethod
    def _raw_root_can_persist(raw_root: Path) -> bool:
        root = raw_root.expanduser()
        try:
            if root.exists():
                return root.is_dir() and os.access(root, os.W_OK | os.X_OK)

            parent = root.parent
            while parent != parent.parent and not parent.exists():
                parent = parent.parent
            return parent.exists() and os.access(parent, os.W_OK | os.X_OK)
        except OSError:
            return False

    def _emit_raw_persistence_skip(self, archive_url: str, raw_path: Path, hour) -> None:  # type: ignore[no-untyped-def]
        raw_root = self._raw_persistence_root()
        reason = (
            f"raw persistence root unavailable: {raw_root}"
            if raw_root is not None
            else "raw persistence root unavailable"
        )
        emit_loader_event(
            f"Skipping PMXT raw archive copy for {self._hour_label(hour)}",
            stage="raw_write",
            status="skip",
            vendor="pmxt",
            platform="polymarket",
            data_type="book",
            source_kind="local",
            source=f"archive:{archive_url}",
            cache_path=str(raw_path),
            condition_id=getattr(self, "condition_id", None),
            token_id=getattr(self, "token_id", None),
            attrs=self._pmxt_source_attrs(hour, {"reason": reason}),
        )

    @staticmethod
    @contextmanager
    def _raw_download_lock(raw_path: Path) -> Iterator[None]:
        key = str(raw_path.expanduser())
        with _PMXT_RAW_DOWNLOAD_LOCKS_LOCK:
            entry = _PMXT_RAW_DOWNLOAD_LOCKS.get(key)
            if entry is None:
                entry = _RawDownloadLockEntry(lock=threading.Lock())
                _PMXT_RAW_DOWNLOAD_LOCKS[key] = entry
            entry.users += 1
        try:
            with entry.lock:
                yield
        finally:
            with _PMXT_RAW_DOWNLOAD_LOCKS_LOCK:
                entry.users -= 1
                if entry.users == 0 and _PMXT_RAW_DOWNLOAD_LOCKS.get(key) is entry:
                    del _PMXT_RAW_DOWNLOAD_LOCKS[key]

    def _load_remote_market_batches_via_raw_root(
        self,
        archive_url: str,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        raw_root = self._raw_persistence_root()
        if raw_root is None:
            return None

        raw_path = self._raw_path_for_hour_at_root(raw_root, hour)
        if raw_path.exists():
            return self._load_raw_market_batches_from_local_file(
                raw_path,
                batch_size=batch_size,
                progress_source=str(raw_path),
                total_bytes=self._progress_total_bytes(str(raw_path)),
            )

        if not self._raw_root_can_persist(raw_root):
            self._emit_raw_persistence_skip(archive_url, raw_path, hour)
            return None

        with self._raw_download_lock(raw_path):
            if raw_path.exists():
                return self._load_raw_market_batches_from_local_file(
                    raw_path,
                    batch_size=batch_size,
                    progress_source=str(raw_path),
                    total_bytes=self._progress_total_bytes(str(raw_path)),
                )

            downloaded = self._download_remote_raw_to_local_root(
                archive_url,
                raw_path,
                hour,
            )
        if downloaded is None:
            return None

        return self._load_raw_market_batches_from_local_file(
            downloaded,
            batch_size=batch_size,
            progress_source=str(downloaded),
            total_bytes=self._progress_total_bytes(str(downloaded)) or downloaded.stat().st_size,
        )

    def _download_remote_raw_to_local_root(
        self,
        archive_url: str,
        raw_path: Path,
        hour,
    ) -> Path | None:  # type: ignore[no-untyped-def]
        raw_path = raw_path.expanduser()
        temp_path = raw_path.with_name(
            f".{raw_path.name}.{os.getpid()}.{threading.get_ident()}.{time.monotonic_ns()}.tmp"
        )
        emit_loader_event(
            f"Writing PMXT raw archive copy for {self._hour_label(hour)}",
            stage="raw_write",
            status="start",
            vendor="pmxt",
            platform="polymarket",
            data_type="book",
            source_kind="local",
            source=f"archive:{archive_url}",
            cache_path=str(raw_path),
            condition_id=getattr(self, "condition_id", None),
            token_id=getattr(self, "token_id", None),
            attrs=self._pmxt_source_attrs(hour),
        )
        try:
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            total_bytes = self._download_to_file_with_progress(archive_url, temp_path)
            if total_bytes is None and not temp_path.exists():
                return None
            if raw_path.exists():
                temp_path.unlink(missing_ok=True)
            else:
                os.replace(temp_path, raw_path)
            bytes_count = total_bytes
            if bytes_count is None:
                with suppress(OSError):
                    bytes_count = raw_path.stat().st_size
            cache = getattr(self, "_pmxt_progress_size_cache", None)
            if cache is None:
                cache = {}
                self._pmxt_progress_size_cache = cache
            cache[str(raw_path)] = bytes_count
            emit_loader_event(
                f"Wrote PMXT raw archive copy for {self._hour_label(hour)}",
                stage="raw_write",
                status="complete",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind="local",
                source=f"archive:{archive_url}",
                cache_path=str(raw_path),
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                bytes=bytes_count,
                attrs=self._pmxt_source_attrs(hour),
            )
            return raw_path
        except OSError as exc:
            temp_path.unlink(missing_ok=True)
            if "404" not in str(exc):
                emit_loader_event(
                    f"Failed to write PMXT raw archive copy for {self._hour_label(hour)}",
                    level="ERROR",
                    stage="raw_write",
                    status="error",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind="local",
                    source=f"archive:{archive_url}",
                    cache_path=str(raw_path),
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour, {"error": str(exc)}),
                )
            return None

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

    @classmethod
    def _resolve_cache_prefetch_workers(cls) -> int:
        return _resolve_positive_int_env(
            PMXT_CACHE_PREFETCH_WORKERS_ENV,
            default=_PMXT_DEFAULT_CACHE_PREFETCH_WORKERS,
        )

    @classmethod
    def _resolve_row_group_chunk_size(cls) -> int:
        return _resolve_positive_int_env(
            PMXT_ROW_GROUP_CHUNK_SIZE_ENV,
            default=_PMXT_DEFAULT_ROW_GROUP_CHUNK_SIZE,
        )

    @classmethod
    def _resolve_row_group_scan_workers(cls) -> int:
        return _resolve_positive_int_env(
            PMXT_ROW_GROUP_SCAN_WORKERS_ENV,
            default=_PMXT_DEFAULT_ROW_GROUP_SCAN_WORKERS,
        )

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

    def _raw_path_for_ordered_entry(self, kind: str, target: str, hour) -> Path | None:  # type: ignore[no-untyped-def]
        if kind == _PMXT_SOURCE_STAGE_RAW_LOCAL:
            for raw_path in self._raw_paths_for_hour_at_root(Path(target).expanduser(), hour):
                if raw_path.exists():
                    return raw_path
            return None

        if kind != _PMXT_SOURCE_STAGE_RAW_REMOTE:
            return None

        raw_root = self._raw_persistence_root()
        if raw_root is None:
            return None

        raw_path = self._raw_path_for_hour_at_root(raw_root, hour)
        if raw_path.exists():
            return raw_path

        archive_url = self._archive_url_for_base_url(target, hour)
        if not self._raw_root_can_persist(raw_root):
            self._emit_raw_persistence_skip(archive_url, raw_path, hour)
            return None

        with self._raw_download_lock(raw_path):
            if raw_path.exists():
                return raw_path
            return self._download_remote_raw_to_local_root(archive_url, raw_path, hour)

    def _raw_path_for_source_stage(self, stage: str, hour) -> Path | None:  # type: ignore[no-untyped-def]
        if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
            raw_root = getattr(self, "_pmxt_raw_root", None)
            if raw_root is None:
                raw_root = getattr(self, "_pmxt_local_archive_dir", None)
            if raw_root is None:
                return None
            for raw_path in self._raw_paths_for_hour_at_root(Path(raw_root).expanduser(), hour):
                if raw_path.exists():
                    return raw_path
            return None

        if stage != _PMXT_SOURCE_STAGE_RAW_REMOTE:
            return None

        remote_url = getattr(self, "_pmxt_remote_base_url", None)
        if remote_url is None:
            remote_urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
            remote_url = remote_urls[0] if remote_urls else None
        if remote_url is None:
            return None

        raw_root = self._raw_persistence_root()
        if raw_root is None:
            return None

        raw_path = self._raw_path_for_hour_at_root(raw_root, hour)
        if raw_path.exists():
            return raw_path

        archive_url = self._archive_url_for_base_url(str(remote_url), hour)
        if not self._raw_root_can_persist(raw_root):
            self._emit_raw_persistence_skip(archive_url, raw_path, hour)
            return None

        with self._raw_download_lock(raw_path):
            if raw_path.exists():
                return raw_path
            return self._download_remote_raw_to_local_root(archive_url, raw_path, hour)

    def _split_shared_fixed_table(
        self,
        table: pa.Table,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]]:
        if table.num_rows == 0:
            return {request_id: [] for request_id, _, _ in requests}

        if len(requests) > 4:
            try:
                return self._split_shared_fixed_table_one_pass(
                    table,
                    requests=requests,
                    batch_size=batch_size,
                )
            except (KeyError, TypeError, ValueError, pa.ArrowException):
                pass

        result: dict[int, list[pa.RecordBatch]] = {}
        for request_id, condition_id, token_id in requests:
            if "market" in table.schema.names:
                market_value = self._market_stats_value(
                    table.schema.field("market").type,
                    condition_id,
                )
                market_mask = pc.equal(table.column("market"), pa.scalar(market_value))
            else:
                market_mask = pc.equal(table.column("market_id"), condition_id)
            token_mask = pc.equal(table.column("asset_id"), token_id)
            mask = pc.and_(pc.fill_null(market_mask, False), pc.fill_null(token_mask, False))
            filtered = table.filter(mask).select(PolymarketPMXTDataLoader._PMXT_FIXED_COLUMNS)
            result[request_id] = list(filtered.to_batches(max_chunksize=batch_size))
        return result

    def _split_shared_fixed_table_one_pass(
        self,
        table: pa.Table,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]]:
        if "market" in table.schema.names:
            market_field = "market"
            market_type = table.schema.field("market").type
            request_by_key = {
                (self._market_stats_value(market_type, condition_id), token_id): request_id
                for request_id, condition_id, token_id in requests
            }
        else:
            market_field = "market_id"
            request_by_key = {
                (condition_id, token_id): request_id
                for request_id, condition_id, token_id in requests
            }

        row_indexes_by_request: dict[int, list[int]] = {
            request_id: [] for request_id, _, _ in requests
        }
        market_values = table.column(market_field).to_pylist()
        token_values = table.column("asset_id").to_pylist()
        for row_index, key in enumerate(zip(market_values, token_values, strict=True)):
            request_id = request_by_key.get(key)
            if request_id is not None:
                row_indexes_by_request[request_id].append(row_index)

        fixed_table = table.select(PolymarketPMXTDataLoader._PMXT_FIXED_COLUMNS)
        result: dict[int, list[pa.RecordBatch]] = {}
        for request_id, _, _ in requests:
            row_indexes = row_indexes_by_request[request_id]
            if not row_indexes:
                result[request_id] = []
                continue
            filtered = fixed_table.take(pa.array(row_indexes, type=pa.int64()))
            result[request_id] = list(filtered.to_batches(max_chunksize=batch_size))
        return result

    def _split_shared_payload_table(
        self,
        table: pa.Table,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]]:
        if table.num_rows == 0:
            return {request_id: [] for request_id, _, _ in requests}

        result: dict[int, list[pa.RecordBatch]] = {}
        for request_id, condition_id, token_id in requests:
            market_mask = pc.equal(table.column("market_id"), condition_id)
            token_mask = pc.match_substring_regex(
                table.column("data"), rf'"token_id"\s*:\s*"{re.escape(token_id)}"'
            )
            mask = pc.and_(pc.fill_null(market_mask, False), pc.fill_null(token_mask, False))
            filtered = table.filter(mask).select(PolymarketPMXTDataLoader._PMXT_COLUMNS)
            result[request_id] = list(filtered.to_batches(max_chunksize=batch_size))
        return result

    def _matching_shared_raw_fixed_market_row_group_requests(
        self,
        parquet_file: pq.ParquetFile,
        requests: Sequence[tuple[int, str, str]],
    ) -> list[tuple[int, tuple[tuple[int, str, str], ...]]] | None:
        schema = parquet_file.schema_arrow
        try:
            market_index = schema.names.index("market")
        except ValueError:
            return None
        token_index = schema.names.index("asset_id") if "asset_id" in schema.names else None

        market_type = schema.field("market").type
        request_market_values = tuple(
            (request, self._market_stats_value(market_type, request[1])) for request in requests
        )
        row_groups: list[tuple[int, tuple[tuple[int, str, str], ...]]] = []
        for index in range(parquet_file.num_row_groups):
            column = parquet_file.metadata.row_group(index).column(market_index)
            stats = column.statistics
            if stats is None or stats.min is None or stats.max is None:
                return None
            try:
                matching_requests = tuple(
                    request
                    for request, market_value in request_market_values
                    if stats.min <= market_value <= stats.max
                )
            except TypeError:
                return None
            if matching_requests and token_index is not None:
                token_stats = parquet_file.metadata.row_group(index).column(token_index).statistics
                if (
                    token_stats is not None
                    and token_stats.min is not None
                    and token_stats.max is not None
                ):
                    try:
                        matching_requests = tuple(
                            request
                            for request in matching_requests
                            if token_stats.min <= request[2] <= token_stats.max
                        )
                    except TypeError:
                        pass
            if matching_requests:
                row_groups.append((index, matching_requests))
        return row_groups

    def _load_shared_raw_fixed_market_batches_pyarrow(
        self,
        raw_path: Path,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]] | None:
        request_count = len(requests)
        parquet_file = pq.ParquetFile(raw_path)
        if not self._is_raw_fixed_schema(parquet_file.schema_arrow.names):
            return None

        row_group_requests = self._matching_shared_raw_fixed_market_row_group_requests(
            parquet_file,
            requests,
        )
        if row_group_requests is None:
            return None
        if not row_group_requests:
            return {request_id: [] for request_id, _, _ in requests}

        result: dict[int, list[pa.RecordBatch]] = {request_id: [] for request_id, _, _ in requests}
        raw_columns = [
            "event_type",
            "timestamp",
            "market",
            "asset_id",
            "bids",
            "asks",
            "price",
            "size",
            "side",
        ]
        market_type = parquet_file.schema_arrow.field("market").type
        event_type_value_set = pa.array(["book", "price_change"])

        chunk_size = self._resolve_row_group_chunk_size()
        scan_workers = self._resolve_row_group_scan_workers()
        for chunk_start in range(0, len(row_group_requests), chunk_size):
            chunk_items = row_group_requests[chunk_start : chunk_start + chunk_size]
            chunk = [row_group for row_group, _ in chunk_items]
            chunk_requests_by_id: dict[int, tuple[int, str, str]] = {}
            for _, chunk_requests in chunk_items:
                for request in chunk_requests:
                    chunk_requests_by_id.setdefault(request[0], request)
            chunk_requests = tuple(chunk_requests_by_id.values())
            chunk_condition_ids = sorted({condition_id for _, condition_id, _ in chunk_requests})
            chunk_token_ids = sorted({token_id for _, _, token_id in chunk_requests})
            chunk_market_values = [
                self._market_stats_value(market_type, condition_id)
                for condition_id in chunk_condition_ids
            ]
            chunk_market_value_set = pa.array(chunk_market_values, type=market_type)
            chunk_token_value_set = pa.array(chunk_token_ids)
            with _bounded_pmxt_row_group_scan(scan_workers):
                raw_table = filtered = table = split = timestamp_ns = None
                market_mask = event_type_mask = token_mask = mask = None
                try:
                    raw_table = parquet_file.read_row_groups(chunk, columns=raw_columns)
                    market_mask = pc.is_in(
                        raw_table.column("market"),
                        value_set=chunk_market_value_set,
                    )
                    event_type_mask = pc.is_in(
                        raw_table.column("event_type"),
                        value_set=event_type_value_set,
                    )
                    token_mask = pc.is_in(
                        raw_table.column("asset_id"),
                        value_set=chunk_token_value_set,
                    )
                    mask = pc.and_(
                        pc.and_(
                            pc.fill_null(market_mask, False),
                            pc.fill_null(event_type_mask, False),
                        ),
                        pc.fill_null(token_mask, False),
                    )
                    filtered = raw_table.filter(mask)
                    if filtered.num_rows == 0:
                        continue

                    timestamp_ns = pc.cast(
                        pc.cast(filtered.column("timestamp"), pa.timestamp("ns", tz="UTC")),
                        pa.int64(),
                    )
                    table = pa.Table.from_arrays(
                        [
                            filtered.column("market"),
                            filtered.column("event_type"),
                            timestamp_ns,
                            filtered.column("asset_id"),
                            filtered.column("bids"),
                            filtered.column("asks"),
                            pc.cast(filtered.column("price"), pa.string()),
                            pc.cast(filtered.column("size"), pa.string()),
                            filtered.column("side"),
                        ],
                        names=("market", *PolymarketPMXTDataLoader._PMXT_FIXED_COLUMNS),
                    )
                    split = self._split_shared_fixed_table(
                        table,
                        requests=chunk_requests,
                        batch_size=batch_size,
                    )
                    for request_id, batches in split.items():
                        if batches:
                            result[request_id].extend(batches)
                finally:
                    del raw_table, filtered, table, split, timestamp_ns
                    del market_mask, event_type_mask, token_mask, mask
                    _release_arrow_memory()
            if request_count > 8:
                _release_arrow_memory()

        return result

    def _load_shared_market_batches_from_raw_file(
        self,
        raw_path: Path,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]] | None:
        if not requests:
            return {}

        condition_ids = sorted({condition_id for _, condition_id, _ in requests})
        token_ids = sorted({token_id for _, _, token_id in requests})
        try:
            pyarrow_batches = self._load_shared_raw_fixed_market_batches_pyarrow(
                raw_path,
                requests=requests,
                batch_size=batch_size,
            )
            if pyarrow_batches is not None:
                return pyarrow_batches
        except (OSError, TypeError, ValueError, pa.ArrowException):
            pass

        connection = duckdb.connect(":memory:")
        try:
            schema_rows = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0", [str(raw_path)]
            ).fetchall()
            schema_names = {str(row[0]) for row in schema_rows}
            if self._is_raw_fixed_schema(schema_names):
                market_placeholders = ", ".join("?" for _ in condition_ids)
                token_placeholders = ", ".join("?" for _ in token_ids)
                query = (
                    "SELECT "
                    "decode(market) AS market_id, "
                    "event_type, "
                    "CAST(epoch_ns(timestamp) AS BIGINT) AS timestamp_ns, "
                    "asset_id, "
                    "bids, "
                    "asks, "
                    "CAST(price AS VARCHAR) AS price, "
                    "CAST(size AS VARCHAR) AS size, "
                    "side "
                    "FROM read_parquet(?) "
                    f"WHERE decode(market) IN ({market_placeholders}) "
                    "AND event_type IN ('book', 'price_change') "
                    f"AND asset_id IN ({token_placeholders})"
                )
                params: list[object] = [str(raw_path), *condition_ids, *token_ids]
                table = connection.execute(query, params).to_arrow_table()
                return self._split_shared_fixed_table(
                    table, requests=requests, batch_size=batch_size
                )

            if self._is_raw_payload_schema(schema_names):
                market_placeholders = ", ".join("?" for _ in condition_ids)
                query = (
                    "SELECT market_id, update_type, data FROM read_parquet(?) "
                    f"WHERE market_id IN ({market_placeholders}) "
                    "AND update_type IN ('book_snapshot', 'price_change')"
                )
                params = [str(raw_path), *condition_ids]
                table = connection.execute(query, params).to_arrow_table()
                return self._split_shared_payload_table(
                    table, requests=requests, batch_size=batch_size
                )

            return None
        finally:
            connection.close()

    def _load_shared_market_batches_from_remote_base_url(
        self,
        base_url: str,
        hour,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch]] | None:  # type: ignore[no-untyped-def]
        archive_url = self._archive_url_for_base_url(base_url, hour)
        raw_root = self._raw_persistence_root()
        if raw_root is not None:
            raw_path = self._raw_path_for_hour_at_root(raw_root, hour)
            if raw_path.exists():
                return self._load_shared_market_batches_from_raw_file(
                    raw_path,
                    requests=requests,
                    batch_size=batch_size,
                )

            if self._raw_root_can_persist(raw_root):
                with self._raw_download_lock(raw_path):
                    if raw_path.exists():
                        return self._load_shared_market_batches_from_raw_file(
                            raw_path,
                            requests=requests,
                            batch_size=batch_size,
                        )
                    downloaded = self._download_remote_raw_to_local_root(
                        archive_url, raw_path, hour
                    )
                if downloaded is not None:
                    return self._load_shared_market_batches_from_raw_file(
                        downloaded,
                        requests=requests,
                        batch_size=batch_size,
                    )
            else:
                self._emit_raw_persistence_skip(archive_url, raw_path, hour)

        try:
            with self._temporary_download_path(archive_url) as download_path:
                total_bytes = self._download_to_file_with_progress(archive_url, download_path)
                if total_bytes is None and not download_path.exists():
                    return None
                cache = getattr(self, "_pmxt_progress_size_cache", None)
                if cache is None:
                    cache = {}
                    self._pmxt_progress_size_cache = cache
                cache[archive_url] = total_bytes
                return self._load_shared_market_batches_from_raw_file(
                    download_path,
                    requests=requests,
                    batch_size=batch_size,
                )
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            return None
        except Exception:
            return None

    def load_shared_market_batches_for_hour(
        self,
        hour,
        *,
        requests: Sequence[tuple[int, str, str]],
        batch_size: int,
    ) -> dict[int, list[pa.RecordBatch] | None]:  # type: ignore[no-untyped-def]
        if not requests:
            return {}

        ordered_entries = getattr(self, "_pmxt_ordered_source_entries", ()) or ()
        source_entries = (
            ordered_entries
            if ordered_entries
            else tuple((stage, "") for stage in self._pmxt_source_priority)
        )
        missing = {request_id: None for request_id, _, _ in requests}

        for kind, target in source_entries:
            source_target = target or None
            source = self._source_label_for_stage(kind, source_target)
            source_kind = self._source_kind_for_stage(kind)
            emit_loader_event(
                (
                    f"Trying PMXT grouped {source_kind} source "
                    f"for {self._hour_label(hour)} ({len(requests)} request(s))"
                ),
                stage="fetch",
                status="start",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind=source_kind,
                source=source,
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour, {"request_count": len(requests)}),
            )
            if kind == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                remote_url = target or getattr(self, "_pmxt_remote_base_url", None)
                if remote_url is None:
                    remote_urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
                    remote_url = remote_urls[0] if remote_urls else None
                batches_by_request = (
                    self._load_shared_market_batches_from_remote_base_url(
                        str(remote_url),
                        hour,
                        requests=requests,
                        batch_size=batch_size,
                    )
                    if remote_url is not None
                    else None
                )
            else:
                raw_path = (
                    self._raw_path_for_ordered_entry(kind, target, hour)
                    if ordered_entries
                    else self._raw_path_for_source_stage(kind, hour)
                )
                if raw_path is None:
                    emit_loader_event(
                        (
                            f"PMXT grouped {source_kind} source "
                            f"had no usable data for {self._hour_label(hour)}"
                        ),
                        stage="fetch",
                        status="skip",
                        vendor="pmxt",
                        platform="polymarket",
                        data_type="book",
                        source_kind=source_kind,
                        source=source,
                        condition_id=getattr(self, "condition_id", None),
                        token_id=getattr(self, "token_id", None),
                        attrs=self._pmxt_source_attrs(hour, {"request_count": len(requests)}),
                    )
                    continue
                batches_by_request = self._load_shared_market_batches_from_raw_file(
                    raw_path,
                    requests=requests,
                    batch_size=batch_size,
                )
            if batches_by_request is None:
                emit_loader_event(
                    (
                        f"PMXT grouped {source_kind} source "
                        f"had no usable data for {self._hour_label(hour)}"
                    ),
                    stage="fetch",
                    status="skip",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind=source_kind,
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour, {"request_count": len(requests)}),
                )
                continue

            rows = sum(
                self._row_count_from_batches(batches) for batches in batches_by_request.values()
            )
            emit_loader_event(
                (
                    f"Loaded PMXT grouped {source_kind} source "
                    f"for {self._hour_label(hour)} ({rows} rows across {len(requests)} request(s))"
                ),
                stage="fetch",
                status="complete",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind=source_kind,
                source=source,
                rows=rows,
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour, {"request_count": len(requests)}),
            )
            return {**missing, **batches_by_request}

        return missing

    def _write_cache_if_enabled(self, hour, table) -> None:  # type: ignore[no-untyped-def]
        if self._pmxt_cache_dir is None:
            return
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is None:
            return
        try:
            self._write_market_cache(hour, table)
            emit_loader_event(
                (
                    "Wrote PMXT filtered market cache "
                    f"for {self._hour_label(hour)} ({table.num_rows} rows)"
                ),
                stage="cache_write",
                status="complete",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind="cache",
                cache_path=str(cache_path),
                rows=int(table.num_rows),
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour),
            )
        except (OSError, pa.ArrowException) as exc:
            emit_loader_event(
                f"Failed to write PMXT filtered market cache for {self._hour_label(hour)}",
                level="ERROR",
                stage="cache_write",
                status="error",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind="cache",
                cache_path=str(cache_path),
                rows=int(table.num_rows),
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour, {"error": str(exc)}),
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
            emit_loader_event(
                f"Loaded PMXT filtered cache for {self._hour_label(hour)} ({rows} rows)",
                stage="cache_read",
                status="cache_hit",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind="cache",
                cache_path=str(cache_path) if cache_path is not None else None,
                rows=rows,
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour),
            )
            return batches
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is not None:
            emit_loader_event(
                f"PMXT filtered cache miss for {self._hour_label(hour)}",
                stage="cache_read",
                status="cache_miss",
                vendor="pmxt",
                platform="polymarket",
                data_type="book",
                source_kind="cache",
                cache_path=str(cache_path),
                condition_id=getattr(self, "condition_id", None),
                token_id=getattr(self, "token_id", None),
                attrs=self._pmxt_source_attrs(hour),
            )

        ordered_entries = getattr(self, "_pmxt_ordered_source_entries", ()) or ()
        if ordered_entries:
            for kind, target in ordered_entries:
                source = self._source_label_for_stage(kind, target)
                emit_loader_event(
                    (
                        f"Trying PMXT {self._source_kind_for_stage(kind)} source "
                        f"for {self._hour_label(hour)}"
                    ),
                    stage="fetch",
                    status="start",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind=self._source_kind_for_stage(kind),
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
                )
                entry_batches = self._load_ordered_entry_batches(
                    kind,
                    target,
                    hour,
                    batch_size=batch_size,
                )
                if entry_batches is not None:
                    rows = self._row_count_from_batches(entry_batches)
                    emit_loader_event(
                        (
                            f"Loaded PMXT {self._source_kind_for_stage(kind)} source "
                            f"for {self._hour_label(hour)} ({rows} rows)"
                        ),
                        stage="fetch",
                        status="complete",
                        vendor="pmxt",
                        platform="polymarket",
                        data_type="book",
                        source_kind=self._source_kind_for_stage(kind),
                        source=source,
                        rows=rows,
                        condition_id=getattr(self, "condition_id", None),
                        token_id=getattr(self, "token_id", None),
                        attrs=self._pmxt_source_attrs(hour),
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(entry_batches)
                            if entry_batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return entry_batches
                emit_loader_event(
                    (
                        f"PMXT {self._source_kind_for_stage(kind)} source had no usable data "
                        f"for {self._hour_label(hour)}"
                    ),
                    stage="fetch",
                    status="skip",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind=self._source_kind_for_stage(kind),
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
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
                emit_loader_event(
                    f"Trying PMXT local source for {self._hour_label(hour)}",
                    stage="fetch",
                    status="start",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind="local",
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
                )
                batches = self._load_local_archive_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    rows = self._row_count_from_batches(batches)
                    emit_loader_event(
                        f"Loaded PMXT local source for {self._hour_label(hour)} ({rows} rows)",
                        stage="fetch",
                        status="complete",
                        vendor="pmxt",
                        platform="polymarket",
                        data_type="book",
                        source_kind="local",
                        source=source,
                        rows=rows,
                        condition_id=getattr(self, "condition_id", None),
                        token_id=getattr(self, "token_id", None),
                        attrs=self._pmxt_source_attrs(hour),
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return batches
                emit_loader_event(
                    f"PMXT local source had no usable data for {self._hour_label(hour)}",
                    stage="fetch",
                    status="skip",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind="local",
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
                )
                continue

            if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                remote_urls = getattr(self, "_pmxt_remote_base_urls", ()) or ()
                source = ",".join(f"archive:{url}" for url in remote_urls) or (
                    f"archive:{self._pmxt_remote_base_url}"
                    if self._pmxt_remote_base_url is not None
                    else None
                )
                emit_loader_event(
                    f"Trying PMXT archive source for {self._hour_label(hour)}",
                    stage="fetch",
                    status="start",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind="remote",
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
                )
                batches = self._load_remote_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    rows = self._row_count_from_batches(batches)
                    emit_loader_event(
                        (f"Loaded PMXT archive source for {self._hour_label(hour)} ({rows} rows)"),
                        stage="fetch",
                        status="complete",
                        vendor="pmxt",
                        platform="polymarket",
                        data_type="book",
                        source_kind="remote",
                        source=source,
                        rows=rows,
                        condition_id=getattr(self, "condition_id", None),
                        token_id=getattr(self, "token_id", None),
                        attrs=self._pmxt_source_attrs(hour),
                    )
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        self._write_cache_if_enabled(hour, table)
                    return batches
                emit_loader_event(
                    f"PMXT archive source had no usable data for {self._hour_label(hour)}",
                    stage="fetch",
                    status="skip",
                    vendor="pmxt",
                    platform="polymarket",
                    data_type="book",
                    source_kind="remote",
                    source=source,
                    condition_id=getattr(self, "condition_id", None),
                    token_id=getattr(self, "token_id", None),
                    attrs=self._pmxt_source_attrs(hour),
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
        if (
            getattr(self, "_pmxt_scan_progress_callback", None) is None
            and not loader_progress_logs_enabled()
        ):
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
