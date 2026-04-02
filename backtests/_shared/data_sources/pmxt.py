from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow.dataset as ds

from nautilus_trader.adapters.polymarket import PolymarketPMXTDataLoader


PMXT_DATA_SOURCE_ENV = "PMXT_DATA_SOURCE"
PMXT_LOCAL_MIRROR_DIR_ENV = "PMXT_LOCAL_MIRROR_DIR"
PMXT_LOCAL_FILTERED_DIR_ENV = "PMXT_LOCAL_FILTERED_DIR"
PMXT_RAW_ROOT_ENV = "PMXT_RAW_ROOT"
PMXT_DISABLE_REMOTE_ARCHIVE_ENV = "PMXT_DISABLE_REMOTE_ARCHIVE"
PMXT_RELAY_BASE_URL_ENV = "PMXT_RELAY_BASE_URL"
PMXT_CACHE_DIR_ENV = "PMXT_CACHE_DIR"
PMXT_DEFAULT_RELAY_BASE_URL = "https://209-209-10-83.sslip.io"

_DISABLED_ENV_VALUES = {"", "0", "false", "no", "off", "none", "disabled"}
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
        self._pmxt_raw_root = self._resolve_raw_root()
        self._pmxt_disable_remote_archive = self._env_flag_enabled(
            os.getenv(PMXT_DISABLE_REMOTE_ARCHIVE_ENV)
        )

    @classmethod
    def _resolve_raw_root(cls) -> Path | None:
        configured = os.getenv(PMXT_RAW_ROOT_ENV)
        if configured is None:
            return None

        value = configured.strip()
        if value.casefold() in _DISABLED_ENV_VALUES:
            return None

        return Path(value).expanduser()

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
        scanner = dataset.scanner(
            columns=self._PMXT_REMOTE_COLUMNS,
            filter=self._market_filter(),
            batch_size=batch_size,
        )
        batches = []
        for batch in scanner.to_batches():
            filtered_batch = self._filter_batch_to_token(batch)
            if filtered_batch.num_rows:
                batches.append(filtered_batch)
        return batches

    def _load_local_archive_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        if self._pmxt_raw_root is not None:
            batches = self._load_local_raw_market_batches(hour, batch_size=batch_size)
            if batches is not None:
                return batches

        return super()._load_local_archive_market_batches(hour, batch_size=batch_size)

    def _load_remote_market_batches(
        self,
        hour,
        *,
        batch_size: int,
    ):  # type: ignore[no-untyped-def]
        if self._pmxt_disable_remote_archive:
            return None

        return super()._load_remote_market_batches(hour, batch_size=batch_size)


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
    return value.casefold() not in _DISABLED_ENV_VALUES


def _resolve_existing_relay_url() -> str:
    configured = os.getenv(PMXT_RELAY_BASE_URL_ENV)
    if configured is None:
        return PMXT_DEFAULT_RELAY_BASE_URL

    value = configured.strip().rstrip("/")
    if value.casefold() in _DISABLED_ENV_VALUES:
        return PMXT_DEFAULT_RELAY_BASE_URL
    return value or PMXT_DEFAULT_RELAY_BASE_URL


def _resolve_required_directory(env_name: str, *, label: str) -> Path:
    configured = os.getenv(env_name)
    if configured is None or configured.strip().casefold() in _DISABLED_ENV_VALUES:
        raise ValueError(f"{env_name} is required when using {label}.")

    path = Path(configured).expanduser()
    if not path.exists():
        raise ValueError(f"{label} path does not exist: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} path is not a directory: {path}")
    return path


def resolve_pmxt_data_source_selection() -> tuple[
    PMXTDataSourceSelection,
    dict[str, str | None],
]:
    configured_mode = os.getenv(PMXT_DATA_SOURCE_ENV)
    mode = _normalize_mode(configured_mode)

    if configured_mode is None:
        raw_root = _env_value(PMXT_RAW_ROOT_ENV)
        relay_base_url = _env_value(PMXT_RELAY_BASE_URL_ENV)
        cache_dir = _env_value(PMXT_CACHE_DIR_ENV)
        disable_remote_archive = _env_enabled(PMXT_DISABLE_REMOTE_ARCHIVE_ENV)

        if raw_root is not None and raw_root.casefold() not in _DISABLED_ENV_VALUES:
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
            and relay_base_url.casefold() in _DISABLED_ENV_VALUES
        ):
            return (
                PMXTDataSourceSelection(
                    mode="raw-remote",
                    summary="PMXT source: raw remote archive (relay disabled)",
                ),
                {},
            )

        return (
            PMXTDataSourceSelection(
                mode="auto",
                summary="PMXT source: auto (cache -> local raw -> relay filtered -> raw remote -> relay raw)",
            ),
            {},
        )

    if mode == "auto":
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary="PMXT source: auto (cache -> local raw -> relay filtered -> raw remote -> relay raw)",
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: _resolve_existing_relay_url(),
                PMXT_RAW_ROOT_ENV: None,
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None,
            },
        )

    if mode == "relay":
        relay_url = _resolve_existing_relay_url()
        return (
            PMXTDataSourceSelection(
                mode=mode,
                summary=f"PMXT source: relay-first ({relay_url})",
            ),
            {
                PMXT_RELAY_BASE_URL_ENV: relay_url,
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
                PMXT_RAW_ROOT_ENV: str(raw_root),
                PMXT_DISABLE_REMOTE_ARCHIVE_ENV: None,
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
            PMXT_CACHE_DIR_ENV: str(filtered_root),
            PMXT_RAW_ROOT_ENV: None,
            PMXT_DISABLE_REMOTE_ARCHIVE_ENV: "1",
        },
    )


@contextmanager
def configured_pmxt_data_source() -> Iterator[PMXTDataSourceSelection]:
    selection, updates = resolve_pmxt_data_source_selection()
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
