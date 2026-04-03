from __future__ import annotations

import os
import time
from contextlib import contextmanager
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence
from urllib.request import Request
from urllib.request import urlopen

import pyarrow.dataset as ds
import pyarrow as pa

from nautilus_trader.adapters.polymarket import PolymarketPMXTDataLoader

from backtests._shared.data_sources._common import DISABLED_ENV_VALUES
from backtests._shared.data_sources._common import env_value
from backtests._shared.data_sources._common import looks_like_local_path
from backtests._shared.data_sources._common import normalize_local_path
from backtests._shared.data_sources._common import normalize_urlish


PMXT_DATA_SOURCE_ENV = "PMXT_DATA_SOURCE"
PMXT_LOCAL_MIRROR_DIR_ENV = "PMXT_LOCAL_MIRROR_DIR"
PMXT_LOCAL_FILTERED_DIR_ENV = "PMXT_LOCAL_FILTERED_DIR"
PMXT_RAW_ROOT_ENV = "PMXT_RAW_ROOT"
PMXT_DISABLE_REMOTE_ARCHIVE_ENV = "PMXT_DISABLE_REMOTE_ARCHIVE"
PMXT_RELAY_BASE_URL_ENV = "PMXT_RELAY_BASE_URL"
PMXT_REMOTE_BASE_URL_ENV = "PMXT_REMOTE_BASE_URL"
PMXT_CACHE_DIR_ENV = "PMXT_CACHE_DIR"
PMXT_SOURCE_PRIORITY_ENV = "PMXT_SOURCE_PRIORITY"
PMXT_PREFETCH_WORKERS_ENV = "PMXT_PREFETCH_WORKERS"
_PMXT_RUNNER_HTTP_USER_AGENT = "prediction-market-backtesting/1.0"
_PMXT_RUNNER_HTTP_TIMEOUT_SECS = 30
_PMXT_LOCAL_RAW_PREFETCH_WORKERS = "4"

_PMXT_SOURCE_STAGE_RAW_LOCAL = "raw-local"
_PMXT_SOURCE_STAGE_RAW_REMOTE = "raw-remote"
_PMXT_SOURCE_STAGE_RELAY_RAW = "relay-raw"
_PMXT_VALID_SOURCE_STAGES = (
    _PMXT_SOURCE_STAGE_RAW_LOCAL,
    _PMXT_SOURCE_STAGE_RAW_REMOTE,
    _PMXT_SOURCE_STAGE_RELAY_RAW,
)

_MODE_ALIASES = {
    "": "auto",
    "auto": "auto",
    "default": "auto",
    "relay": "relay",
    "relay-first": "relay",
    "raw": "raw-remote",
    "raw-remote": "raw-remote",
    "remote-raw": "raw-remote",
    "raw-local": "raw-local",
    "local-raw": "raw-local",
    "mirror": "raw-local",
    "filtered-local": "filtered-local",
    "local-filtered": "filtered-local",
    "cache-local": "filtered-local",
}
_VALID_MODES = ("auto", "relay", "raw-remote", "raw-local", "filtered-local")


class RunnerPolymarketPMXTDataLoader(PolymarketPMXTDataLoader):
    """
    Repo-layer PMXT loader extensions used by the backtest runners.

    This keeps BYOD/local-mirror behavior out of the vendored Nautilus subtree.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._pmxt_remote_base_url = self._resolve_remote_base_url()
        self._pmxt_raw_root = self._resolve_raw_root()
        self._pmxt_disable_remote_archive = self._env_flag_enabled(
            os.getenv(PMXT_DISABLE_REMOTE_ARCHIVE_ENV)
        )
        self._pmxt_source_priority = self._resolve_source_priority()

    @classmethod
    def _resolve_raw_root(cls) -> Path | None:
        configured = os.getenv(PMXT_RAW_ROOT_ENV)
        if configured is None:
            return None

        value = configured.strip()
        if value.casefold() in DISABLED_ENV_VALUES:
            return None

        return Path(value).expanduser()

    @classmethod
    def _resolve_remote_base_url(cls) -> str | None:
        configured = env_value(os.getenv(PMXT_REMOTE_BASE_URL_ENV))
        if configured is None:
            return None
        if configured.casefold() in DISABLED_ENV_VALUES:
            return None
        return normalize_urlish(configured)

    @classmethod
    def _resolve_relay_base_url(cls) -> str | None:
        configured = env_value(os.getenv(PMXT_RELAY_BASE_URL_ENV))
        if configured is None:
            return None
        if configured.casefold() in DISABLED_ENV_VALUES:
            return None
        return normalize_urlish(configured)

    @classmethod
    def _archive_url_for_hour(cls, hour):
        remote_base_url = cls._resolve_remote_base_url()
        if remote_base_url is None:
            raise RuntimeError(
                f"{PMXT_REMOTE_BASE_URL_ENV} is required for remote PMXT archive access."
            )
        return f"{remote_base_url}/{cls._archive_filename_for_hour(hour)}"

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

    def _load_local_raw_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        raw_path = self._raw_path_for_hour(hour)
        if raw_path is None or not raw_path.exists():
            return None

        dataset = ds.dataset(str(raw_path), format="parquet")
        return self._scan_raw_market_batches(
            dataset,
            batch_size=batch_size,
            source=str(raw_path),
            total_bytes=self._progress_total_bytes(str(raw_path)),
        )

    def _load_local_archive_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        if self._pmxt_raw_root is not None:
            return self._load_local_raw_market_batches(hour, batch_size=batch_size)

        return super()._load_local_archive_market_batches(hour, batch_size=batch_size)

    def _load_remote_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        if self._pmxt_disable_remote_archive or self._pmxt_remote_base_url is None:
            return None

        return super()._load_remote_market_batches(hour, batch_size=batch_size)

    def _relay_url_for_hour(self, hour):  # type: ignore[no-untyped-def]
        del hour
        # The active mirror only serves raw hours. Filtered relay fetches belong to
        # the retired full-stack relay and should never be attempted by runner code.
        return None

    @classmethod
    def _resolve_source_priority(cls) -> tuple[str, ...]:
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
                    f"Unsupported {PMXT_SOURCE_PRIORITY_ENV} stage {stage!r}. "
                    f"Use one of: {valid_stages}."
                )
            if stage not in priority:
                priority.append(stage)
        return tuple(priority) or _PMXT_VALID_SOURCE_STAGES

    def _load_market_table(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        table = self._load_cached_market_table(hour)
        if table is not None:
            return table

        for stage in self._pmxt_source_priority:
            if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                local_archive_batches = self._load_local_archive_market_batches(
                    hour,
                    batch_size=batch_size,
                )
                if local_archive_batches is not None:
                    table = (
                        pa.Table.from_batches(local_archive_batches)
                        if local_archive_batches
                        else self._empty_market_table()
                    )
                    if self._pmxt_cache_dir is not None:
                        with suppress(OSError, pa.ArrowException):
                            self._write_market_cache(hour, table)
                    return table
                continue

            if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                remote_table = self._load_remote_market_table(
                    hour,
                    batch_size=batch_size,
                )
                if remote_table is not None:
                    remote_table = self._filter_table_to_token(remote_table)
                    if self._pmxt_cache_dir is not None:
                        with suppress(OSError, pa.ArrowException):
                            self._write_market_cache(hour, remote_table)
                    return remote_table
                continue

            relay_raw_batches = self._load_relay_raw_market_batches(
                hour,
                batch_size=batch_size,
            )
            if relay_raw_batches is not None:
                table = (
                    pa.Table.from_batches(relay_raw_batches)
                    if relay_raw_batches
                    else self._empty_market_table()
                )
                if self._pmxt_cache_dir is not None:
                    with suppress(OSError, pa.ArrowException):
                        self._write_market_cache(hour, table)
                return table

        return None

    def _load_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        batches = self._load_cached_market_batches(hour)
        if batches is not None:
            return batches

        for stage in self._pmxt_source_priority:
            if stage == _PMXT_SOURCE_STAGE_RAW_LOCAL:
                batches = self._load_local_archive_market_batches(
                    hour,
                    batch_size=batch_size,
                )
                if batches is not None:
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        with suppress(OSError, pa.ArrowException):
                            self._write_market_cache(hour, table)
                    return batches
                continue

            if stage == _PMXT_SOURCE_STAGE_RAW_REMOTE:
                batches = self._load_remote_market_batches(hour, batch_size=batch_size)
                if batches is not None:
                    if self._pmxt_cache_dir is not None:
                        table = (
                            pa.Table.from_batches(batches)
                            if batches
                            else self._empty_market_table()
                        )
                        with suppress(OSError, pa.ArrowException):
                            self._write_market_cache(hour, table)
                    return batches
                continue

            batches = self._load_relay_raw_market_batches(hour, batch_size=batch_size)
            if batches is not None:
                if self._pmxt_cache_dir is not None:
                    table = (
                        pa.Table.from_batches(batches)
                        if batches
                        else self._empty_market_table()
                    )
                    with suppress(OSError, pa.ArrowException):
                        self._write_market_cache(hour, table)
                return batches

        return None

    def _download_to_file_with_progress(
        self,
        url: str,
        destination: Path,
    ) -> int | None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        request = Request(url, headers={"User-Agent": _PMXT_RUNNER_HTTP_USER_AGENT})
        with (
            urlopen(
                request,
                timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS,
            ) as response,
            destination.open("wb") as handle,
        ):  # noqa: S310
            total_bytes = self._content_length_from_response(response)
            downloaded_bytes = 0
            last_emit = 0.0
            supports_chunked_read = True
            self._emit_download_progress(
                url,
                downloaded_bytes=0,
                total_bytes=total_bytes,
                finished=False,
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
                url,
                downloaded_bytes=downloaded_bytes,
                total_bytes=total_bytes,
                finished=True,
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
        with urlopen(
            request,
            timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS,
        ) as response:  # noqa: S310
            total_bytes = self._content_length_from_response(response)
            downloaded_bytes = 0
            last_emit = 0.0
            chunks: list[bytes] = []
            supports_chunked_read = True
            self._emit_download_progress(
                url,
                downloaded_bytes=0,
                total_bytes=total_bytes,
                finished=False,
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
                url,
                downloaded_bytes=downloaded_bytes,
                total_bytes=total_bytes,
                finished=True,
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
                source,
                method="HEAD",
                headers={"User-Agent": _PMXT_RUNNER_HTTP_USER_AGENT},
            )
            try:
                with urlopen(
                    request,
                    timeout=_PMXT_RUNNER_HTTP_TIMEOUT_SECS,
                ) as response:  # noqa: S310
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


def _resolve_existing_relay_url() -> str | None:
    configured = os.getenv(PMXT_RELAY_BASE_URL_ENV)
    if configured is None:
        return None

    value = configured.strip().rstrip("/")
    if value.casefold() in DISABLED_ENV_VALUES:
        return None
    return normalize_urlish(value) if value else None


def _resolve_existing_remote_url() -> str | None:
    configured = os.getenv(PMXT_REMOTE_BASE_URL_ENV)
    if configured is None:
        return None

    value = configured.strip().rstrip("/")
    if value.casefold() in DISABLED_ENV_VALUES:
        return None
    return normalize_urlish(value) if value else None


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


def _classify_explicit_pmxt_sources(
    sources: Sequence[str],
) -> tuple[str | None, str | None, str | None, tuple[str, ...], tuple[str, ...]]:
    raw_root: str | None = None
    remote_base_url: str | None = None
    relay_base_url: str | None = None
    priority: list[str] = []
    ordered_sources: list[str] = []

    for source in sources:
        stripped = source.strip()
        if not stripped:
            continue
        if stripped.casefold() == "cache":
            continue
        if looks_like_local_path(stripped):
            normalized_local = normalize_local_path(stripped)
            if raw_root is not None and normalized_local != raw_root:
                raise ValueError(
                    "PMXT explicit sources supports at most one local raw mirror path."
                )
            raw_root = normalized_local
            if _PMXT_SOURCE_STAGE_RAW_LOCAL not in priority:
                priority.append(_PMXT_SOURCE_STAGE_RAW_LOCAL)
            if normalized_local not in ordered_sources:
                ordered_sources.append(normalized_local)
            continue

        normalized_remote = normalize_urlish(stripped)
        if remote_base_url is None:
            remote_base_url = normalized_remote
            if _PMXT_SOURCE_STAGE_RAW_REMOTE not in priority:
                priority.append(_PMXT_SOURCE_STAGE_RAW_REMOTE)
            ordered_sources.append(normalized_remote)
            continue
        if relay_base_url is None:
            relay_base_url = normalized_remote
            if _PMXT_SOURCE_STAGE_RELAY_RAW not in priority:
                priority.append(_PMXT_SOURCE_STAGE_RELAY_RAW)
            ordered_sources.append(normalized_remote)
            continue
        raise ValueError(
            "PMXT explicit sources supports at most two remote sources: "
            "remote archive first, relay second."
        )

    return (
        raw_root,
        remote_base_url,
        relay_base_url,
        tuple(priority),
        tuple(ordered_sources),
    )


def _explicit_source_summary(
    *,
    ordered_sources: Sequence[str],
) -> str:
    parts: list[str] = ["cache", *ordered_sources]
    return "PMXT source: explicit priority (" + " -> ".join(parts) + ")"


def resolve_pmxt_data_source_selection(
    *,
    sources: Sequence[str] | None = None,
) -> tuple[
    PMXTDataSourceSelection,
    dict[str, str | None],
]:
    if sources:
        raw_root, remote_base_url, relay_base_url, source_priority, ordered_sources = (
            _classify_explicit_pmxt_sources(sources)
        )
        return (
            PMXTDataSourceSelection(
                mode="auto",
                summary=_explicit_source_summary(
                    ordered_sources=ordered_sources,
                ),
            ),
            {
                PMXT_RAW_ROOT_ENV: raw_root,
                PMXT_REMOTE_BASE_URL_ENV: remote_base_url or "0",
                PMXT_RELAY_BASE_URL_ENV: relay_base_url or "0",
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None if remote_base_url else "1",
                PMXT_SOURCE_PRIORITY_ENV: ",".join(source_priority) or None,
            },
        )

    configured_mode = os.getenv(PMXT_DATA_SOURCE_ENV)
    mode = _normalize_mode(configured_mode)

    if configured_mode is None:
        raw_root = _env_value(PMXT_RAW_ROOT_ENV)
        relay_base_url = _env_value(PMXT_RELAY_BASE_URL_ENV)
        remote_base_url = _env_value(PMXT_REMOTE_BASE_URL_ENV)
        cache_dir = _env_value(PMXT_CACHE_DIR_ENV)
        disable_remote_archive = _env_enabled(PMXT_DISABLE_REMOTE_ARCHIVE_ENV)

        if raw_root is not None and raw_root.casefold() not in DISABLED_ENV_VALUES:
            return (
                PMXTDataSourceSelection(
                    mode="raw-local",
                    summary=f"PMXT source: local raw mirror ({Path(raw_root).expanduser()})",
                ),
                {},
            )

        if disable_remote_archive and cache_dir is not None:
            return (
                PMXTDataSourceSelection(
                    mode="filtered-local",
                    summary=f"PMXT source: local filtered parquet ({Path(cache_dir).expanduser()})",
                ),
                {},
            )

        if (
            relay_base_url is not None
            and relay_base_url.casefold() in DISABLED_ENV_VALUES
        ):
            return (
                PMXTDataSourceSelection(
                    mode="raw-remote",
                    summary="PMXT source: raw remote archive (relay disabled)",
                ),
                {},
            )

        if (
            remote_base_url is not None
            and remote_base_url.casefold() in DISABLED_ENV_VALUES
        ):
            return (
                PMXTDataSourceSelection(
                    mode="relay",
                    summary="PMXT source: relay-first (remote raw disabled)",
                ),
                {},
            )

        return (
            PMXTDataSourceSelection(
                mode="auto",
                summary=(
                    "PMXT source: auto "
                    "(cache -> local raw -> explicit remote raw -> explicit relay)"
                ),
            ),
            {},
        )

    if mode == "auto":
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary=(
                    "PMXT source: auto "
                    "(cache -> local raw -> explicit remote raw -> explicit relay)"
                ),
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: _resolve_existing_relay_url(),
                PMXT_REMOTE_BASE_URL_ENV: _resolve_existing_remote_url(),
                PMXT_RAW_ROOT_ENV: None,
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None,
            },
        )

    if mode == "relay":
        relay_url = _resolve_existing_relay_url()
        if relay_url is None:
            raise ValueError(
                f"{PMXT_RELAY_BASE_URL_ENV} is required when using relay mode."
            )
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary=f"PMXT source: relay-first ({relay_url})",
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: relay_url,
                PMXT_REMOTE_BASE_URL_ENV: None,
                PMXT_RAW_ROOT_ENV: None,
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None,
            },
        )

    if mode == "raw-remote":
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary="PMXT source: raw remote archive (relay disabled)",
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: "0",
                PMXT_REMOTE_BASE_URL_ENV: None,
                PMXT_RAW_ROOT_ENV: None,
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None,
            },
        )

    if mode == "raw-local":
        raw_root = _resolve_required_directory(
            PMXT_LOCAL_MIRROR_DIR_ENV,
            label="local raw PMXT mirror",
        )
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary=f"PMXT source: local raw mirror ({raw_root})",
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: "0",
                PMXT_REMOTE_BASE_URL_ENV: "0",
                PMXT_RAW_ROOT_ENV: str(raw_root),
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: "1",
            },
        )

    filtered_root = _resolve_required_directory(
        PMXT_LOCAL_FILTERED_DIR_ENV,
        label="local filtered PMXT root",
    )
    return (
        PMXTDataSourceSelection(
            mode=mode,
            summary=f"PMXT source: local filtered parquet ({filtered_root})",
        ),
        {
            PMXT_RELAY_BASE_URL_ENV: "0",
            PMXT_REMOTE_BASE_URL_ENV: "0",
            PMXT_CACHE_DIR_ENV: str(filtered_root),
            PMXT_RAW_ROOT_ENV: None,
            PMXT_DISABLE_REMOTE_ARCHIVE_ENV: "1",
        },
    )


@contextmanager
def configured_pmxt_data_source(
    *,
    sources: Sequence[str] | None = None,
) -> Iterator[PMXTDataSourceSelection]:
    selection, updates = resolve_pmxt_data_source_selection(sources=sources)
    updates = dict(updates)
    if (
        updates.get(PMXT_RAW_ROOT_ENV) is not None
        and os.environ.get(PMXT_PREFETCH_WORKERS_ENV) is None
    ):
        updates[PMXT_PREFETCH_WORKERS_ENV] = _PMXT_LOCAL_RAW_PREFETCH_WORKERS
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
