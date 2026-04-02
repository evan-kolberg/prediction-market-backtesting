# Added by Evan Kolberg to the NautilusTrader-derived subtree on 2026-03-15.
# Modified in this repository on 2026-03-19 and 2026-04-02.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable
from collections.abc import Iterator
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC
from io import BytesIO
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

import fsspec
import msgspec
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from nautilus_trader.adapters.polymarket.common.enums import PolymarketOrderSide
from nautilus_trader.adapters.polymarket.loaders import PolymarketDataLoader
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookLevel
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookSnapshot
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuote
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuotes
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BookType


class _PMXTBookSnapshotPayload(msgspec.Struct, frozen=True):
    update_type: str
    market_id: str
    token_id: str
    side: str
    best_bid: str | None
    best_ask: str | None
    timestamp: float
    bids: list[list[str]]
    asks: list[list[str]]


class _PMXTPriceChangePayload(msgspec.Struct, frozen=True):
    update_type: str
    market_id: str
    token_id: str
    side: str
    best_bid: str | None
    best_ask: str | None
    timestamp: float
    change_price: str
    change_size: str
    change_side: str


_PMXT_BOOK_SNAPSHOT_DECODER = msgspec.json.Decoder(type=_PMXTBookSnapshotPayload)
_PMXT_PRICE_CHANGE_DECODER = msgspec.json.Decoder(type=_PMXTPriceChangePayload)


class PolymarketPMXTDataLoader(PolymarketDataLoader):
    """
    Historical Polymarket L2 loader backed by the PMXT hourly archive.

    The PMXT archive stores one parquet file per UTC hour. Each row contains a
    market-scoped order-book event payload encoded as JSON. This loader filters
    to one market ID at parquet-scan time, then filters to the target token in
    Python and converts the payloads into Nautilus `OrderBookDeltas` and
    `QuoteTick` records.
    """

    _PMXT_BASE_URL = "https://r2.pmxt.dev"
    _PMXT_REMOTE_COLUMNS = [
        "market_id",
        "update_type",
        "data",
    ]
    _PMXT_COLUMNS = [
        "update_type",
        "data",
    ]
    _PMXT_CACHE_DIR_ENV = "PMXT_CACHE_DIR"
    _PMXT_DISABLE_CACHE_ENV = "PMXT_DISABLE_CACHE"
    _PMXT_LOCAL_ARCHIVE_DIR_ENV = "PMXT_LOCAL_ARCHIVE_DIR"
    _PMXT_RELAY_BASE_URL_ENV = "PMXT_RELAY_BASE_URL"
    _PMXT_DEFAULT_RELAY_BASE_URL = "https://209-209-10-83.sslip.io"
    _PMXT_PREFETCH_WORKERS_ENV = "PMXT_PREFETCH_WORKERS"
    _PMXT_HTTP_BLOCK_SIZE_MB_ENV = "PMXT_HTTP_BLOCK_SIZE_MB"
    _PMXT_HTTP_CACHE_TYPE_ENV = "PMXT_HTTP_CACHE_TYPE"
    _PMXT_DEFAULT_PREFETCH_WORKERS = 16
    _PMXT_DEFAULT_HTTP_BLOCK_SIZE = 32 * 1024 * 1024
    _PMXT_DEFAULT_HTTP_CACHE_TYPE = "readahead"
    _PMXT_DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._pmxt_cache_dir = self._resolve_cache_dir()
        self._pmxt_local_archive_dir = self._resolve_local_archive_dir()
        self._pmxt_relay_base_url = self._resolve_relay_base_url()
        self._pmxt_prefetch_workers = self._resolve_prefetch_workers()
        self._pmxt_http_block_size = self._resolve_http_block_size()
        self._pmxt_http_cache_type = self._resolve_http_cache_type()
        self._pmxt_download_progress_callback: Callable[
            [str, int, int | None, bool],
            None,
        ] | None = None
        self._reset_http_filesystem()

    @staticmethod
    def _normalize_timestamp(value: pd.Timestamp | str | None) -> pd.Timestamp | None:
        if value is None:
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(UTC)
        return ts.tz_convert(UTC)

    @staticmethod
    def _archive_hours(
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[pd.Timestamp]:
        cursor = start.floor("h") - pd.Timedelta(hours=1)
        final_hour = end.floor("h")
        hours: list[pd.Timestamp] = []
        while cursor <= final_hour:
            hours.append(cursor)
            cursor += pd.Timedelta(hours=1)
        return hours

    @classmethod
    def _archive_filename_for_hour(cls, hour: pd.Timestamp) -> str:
        ts = hour.tz_convert(UTC)
        return f"polymarket_orderbook_{ts.strftime('%Y-%m-%dT%H')}.parquet"

    @classmethod
    def _archive_url_for_hour(cls, hour: pd.Timestamp) -> str:
        return f"{cls._PMXT_BASE_URL}/{cls._archive_filename_for_hour(hour)}"

    @classmethod
    def _archive_relative_path_for_hour(cls, hour: pd.Timestamp) -> str:
        ts = hour.tz_convert(UTC)
        filename = cls._archive_filename_for_hour(ts)
        return (
            Path(ts.strftime("%Y")) / ts.strftime("%m") / ts.strftime("%d") / filename
        ).as_posix()

    @staticmethod
    def _env_flag_enabled(value: str | None) -> bool:
        if value is None:
            return False
        return value.strip().casefold() in {"1", "true", "yes", "on"}

    @classmethod
    def _default_cache_dir(cls) -> Path:
        xdg_cache_home = os.getenv("XDG_CACHE_HOME")
        base_dir = Path(xdg_cache_home).expanduser() if xdg_cache_home else Path.home() / ".cache"
        return base_dir / "nautilus_trader" / "pmxt"

    @classmethod
    def _resolve_cache_dir(cls) -> Path | None:
        if cls._env_flag_enabled(os.getenv(cls._PMXT_DISABLE_CACHE_ENV)):
            return None

        configured = os.getenv(cls._PMXT_CACHE_DIR_ENV)
        if configured is None:
            return cls._default_cache_dir()

        value = configured.strip()
        if not value or value.casefold() in {"0", "false", "no", "off", "none", "disabled"}:
            return None
        if value.casefold() in {"1", "true", "yes", "on", "default"}:
            return cls._default_cache_dir()
        return Path(value).expanduser()

    @classmethod
    def _resolve_relay_base_url(cls) -> str | None:
        configured = os.getenv(cls._PMXT_RELAY_BASE_URL_ENV)
        if configured is None:
            return cls._PMXT_DEFAULT_RELAY_BASE_URL

        value = configured.strip().rstrip("/")
        if not value or value.casefold() in {"0", "false", "no", "off", "none", "disabled"}:
            return None
        return value

    @classmethod
    def _resolve_local_archive_dir(cls) -> Path | None:
        configured = os.getenv(cls._PMXT_LOCAL_ARCHIVE_DIR_ENV)
        if configured is None:
            return None

        value = configured.strip()
        if not value or value.casefold() in {"0", "false", "no", "off", "none", "disabled"}:
            return None
        return Path(value).expanduser()

    @classmethod
    def _resolve_prefetch_workers(cls) -> int:
        configured = os.getenv(cls._PMXT_PREFETCH_WORKERS_ENV)
        if configured is None:
            return cls._PMXT_DEFAULT_PREFETCH_WORKERS

        value = configured.strip()
        if not value:
            return cls._PMXT_DEFAULT_PREFETCH_WORKERS

        try:
            return max(1, int(value))
        except ValueError:
            return cls._PMXT_DEFAULT_PREFETCH_WORKERS

    @classmethod
    def _resolve_http_block_size(cls) -> int:
        configured = os.getenv(cls._PMXT_HTTP_BLOCK_SIZE_MB_ENV)
        if configured is None:
            return cls._PMXT_DEFAULT_HTTP_BLOCK_SIZE

        value = configured.strip()
        if not value:
            return cls._PMXT_DEFAULT_HTTP_BLOCK_SIZE

        try:
            return max(1, int(value)) * 1024 * 1024
        except ValueError:
            return cls._PMXT_DEFAULT_HTTP_BLOCK_SIZE

    @classmethod
    def _resolve_http_cache_type(cls) -> str:
        configured = os.getenv(cls._PMXT_HTTP_CACHE_TYPE_ENV)
        if configured is None:
            return cls._PMXT_DEFAULT_HTTP_CACHE_TYPE

        value = configured.strip()
        return value or cls._PMXT_DEFAULT_HTTP_CACHE_TYPE

    def _reset_http_filesystem(self) -> None:
        self._pmxt_http_fs = fsspec.filesystem(
            "https",
            block_size=self._pmxt_http_block_size,
            cache_type=self._pmxt_http_cache_type,
        )
        self._pmxt_fs = pafs.PyFileSystem(
            pafs.FSSpecHandler(self._pmxt_http_fs),
        )

    @classmethod
    def _market_cache_path_for_hour(
        cls,
        cache_dir: Path,
        condition_id: str,
        token_id: str,
        hour: pd.Timestamp,
    ) -> Path:
        return cache_dir / condition_id / token_id / cls._archive_filename_for_hour(hour)

    def _cache_path_for_hour(self, hour: pd.Timestamp) -> Path | None:
        if self._pmxt_cache_dir is None or self.condition_id is None or self.token_id is None:
            return None

        return self._market_cache_path_for_hour(
            self._pmxt_cache_dir,
            self.condition_id,
            self.token_id,
            hour,
        )

    @classmethod
    def _local_archive_candidate_paths_for_hour(
        cls,
        archive_dir: Path,
        hour: pd.Timestamp,
    ) -> tuple[Path, ...]:
        ts = hour.tz_convert(UTC)
        filename = cls._archive_filename_for_hour(ts)
        return (
            archive_dir / filename,
            archive_dir / ts.strftime("%Y/%m/%d") / filename,
        )

    def _local_archive_paths_for_hour(self, hour: pd.Timestamp) -> tuple[Path, ...]:
        if self._pmxt_local_archive_dir is None:
            return ()
        return self._local_archive_candidate_paths_for_hour(
            self._pmxt_local_archive_dir,
            hour,
        )

    def _relay_url_for_hour(self, hour: pd.Timestamp) -> str | None:
        if self._pmxt_relay_base_url is None or self.condition_id is None or self.token_id is None:
            return None

        return (
            f"{self._pmxt_relay_base_url}/v1/filtered/"
            f"{quote(self.condition_id, safe='')}/"
            f"{quote(self.token_id, safe='')}/"
            f"{self._archive_filename_for_hour(hour)}"
        )

    def _relay_raw_url_for_hour(self, hour: pd.Timestamp) -> str | None:
        if self._pmxt_relay_base_url is None:
            return None

        return f"{self._pmxt_relay_base_url}/v1/raw/{self._archive_relative_path_for_hour(hour)}"

    def _market_filter(self):
        return (ds.field("market_id") == self.condition_id) & (
            (ds.field("update_type") == "book_snapshot")
            | (ds.field("update_type") == "price_change")
        )

    @classmethod
    def _empty_market_table(cls) -> pa.Table:
        return pa.table(
            {
                "update_type": pa.array([], type=pa.string()),
                "data": pa.array([], type=pa.string()),
            },
        )

    @classmethod
    def _to_market_batch(cls, batch: pa.RecordBatch) -> pa.RecordBatch:
        if batch.schema.names == cls._PMXT_COLUMNS:
            return batch
        return pa.RecordBatch.from_arrays(
            [
                batch.column("update_type"),
                batch.column("data"),
            ],
            names=cls._PMXT_COLUMNS,
        )

    def _filter_batch_to_token(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if self.token_id is None or batch.num_rows == 0:
            return self._to_market_batch(batch)

        token_mask = pc.match_substring_regex(
            batch.column("data"),
            rf'"token_id"\s*:\s*"{re.escape(self.token_id)}"',
        )
        token_mask = pc.fill_null(token_mask, False)
        return self._to_market_batch(batch.filter(token_mask))

    def _filter_raw_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if batch.num_rows == 0:
            return self._to_market_batch(batch)

        filtered_batch = batch
        if self.condition_id is not None:
            market_mask = pc.equal(filtered_batch.column("market_id"), self.condition_id)
            market_mask = pc.fill_null(market_mask, False)
            update_type_mask = pc.is_in(
                filtered_batch.column("update_type"),
                value_set=pa.array(["book_snapshot", "price_change"]),
            )
            update_type_mask = pc.fill_null(update_type_mask, False)
            filtered_batch = filtered_batch.filter(pc.and_(market_mask, update_type_mask))

        return self._filter_batch_to_token(filtered_batch)

    def _load_cached_market_table(self, hour: pd.Timestamp) -> pa.Table | None:
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is None or not cache_path.exists():
            return None

        try:
            dataset = ds.dataset(str(cache_path), format="parquet")
            return dataset.scanner(columns=self._PMXT_COLUMNS).to_table()
        except (OSError, ValueError, pa.ArrowException):
            cache_path.unlink(missing_ok=True)
            return None

    def _load_cached_market_batches(
        self,
        hour: pd.Timestamp,
    ) -> list[pa.RecordBatch] | None:
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is None or not cache_path.exists():
            return None

        try:
            dataset = ds.dataset(str(cache_path), format="parquet")
            scanner = dataset.scanner(columns=self._PMXT_COLUMNS)
            return list(scanner.to_batches())
        except (OSError, ValueError, pa.ArrowException):
            cache_path.unlink(missing_ok=True)
            return None

    def _write_market_cache(self, hour: pd.Timestamp, table: pa.Table) -> None:
        cache_path = self._cache_path_for_hour(hour)
        if cache_path is None:
            return

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
        try:
            pq.write_table(table, tmp_path)
            os.replace(tmp_path, cache_path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _scan_raw_market_batches(
        self,
        dataset: ds.Dataset,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch]:
        scanner = dataset.scanner(
            columns=self._PMXT_REMOTE_COLUMNS,
            filter=self._market_filter(),
            batch_size=batch_size,
        )
        batches: list[pa.RecordBatch] = []
        for batch in scanner.to_batches():
            filtered_batch = self._filter_batch_to_token(batch)
            if filtered_batch.num_rows:
                batches.append(filtered_batch)
        return batches

    def _load_remote_market_table(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> pa.Table | None:
        batches = self._load_remote_market_batches(hour, batch_size=batch_size)
        if batches is None:
            return None
        if not batches:
            return self._empty_market_table()
        return pa.Table.from_batches(batches)

    def _load_remote_market_batches(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        archive_url = self._archive_url_for_hour(hour)
        try:
            dataset = ds.dataset(
                archive_url,
                filesystem=self._pmxt_fs,
                format="parquet",
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            raise

        return self._scan_raw_market_batches(dataset, batch_size=batch_size)

    def _load_raw_market_batches_via_download(
        self,
        archive_url: str,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        try:
            payload = self._download_payload_with_progress(archive_url)
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            return None
        except Exception:
            return None

        if payload is None:
            return None

        try:
            parquet_file = pq.ParquetFile(BytesIO(payload))
            batches: list[pa.RecordBatch] = []
            for batch in parquet_file.iter_batches(
                batch_size=batch_size,
                columns=self._PMXT_REMOTE_COLUMNS,
            ):
                filtered_batch = self._filter_raw_batch(batch)
                if filtered_batch.num_rows:
                    batches.append(filtered_batch)
            return batches
        except (OSError, ValueError, pa.ArrowException):
            return None

    def _load_local_archive_market_batches(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        for archive_path in self._local_archive_paths_for_hour(hour):
            if not archive_path.exists():
                continue

            try:
                dataset = ds.dataset(str(archive_path), format="parquet")
            except (OSError, ValueError, pa.ArrowException):
                continue

            try:
                return self._scan_raw_market_batches(dataset, batch_size=batch_size)
            except (OSError, ValueError, pa.ArrowException):
                continue

        return None

    def _load_relay_raw_market_batches(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        relay_url = self._relay_raw_url_for_hour(hour)
        if relay_url is None:
            return None

        try:
            dataset = ds.dataset(
                relay_url,
                filesystem=self._pmxt_fs,
                format="parquet",
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            self._reset_http_filesystem()
            return self._load_raw_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )
        except Exception:
            self._reset_http_filesystem()
            return self._load_raw_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )

        try:
            return self._scan_raw_market_batches(dataset, batch_size=batch_size)
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            self._reset_http_filesystem()
            return self._load_raw_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )
        except Exception:
            self._reset_http_filesystem()
            return self._load_raw_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )

    def _load_relay_market_batches(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        relay_url = self._relay_url_for_hour(hour)
        if relay_url is None:
            return None

        try:
            dataset = ds.dataset(
                relay_url,
                filesystem=self._pmxt_fs,
                format="parquet",
            )
        except FileNotFoundError:
            return None
        except Exception:
            self._reset_http_filesystem()
            return self._load_relay_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )

        try:
            scanner = dataset.scanner(
                columns=self._PMXT_COLUMNS,
                batch_size=batch_size,
            )
            return list(scanner.to_batches())
        except Exception:
            self._reset_http_filesystem()
            return self._load_relay_market_batches_via_download(
                relay_url,
                batch_size=batch_size,
            )

    def _load_relay_market_batches_via_download(
        self,
        relay_url: str,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        try:
            payload = self._download_payload_with_progress(relay_url)
        except FileNotFoundError:
            return None
        except OSError as exc:
            if "404" in str(exc):
                return None
            return None
        except Exception:
            return None

        if payload is None:
            return None

        try:
            parquet_file = pq.ParquetFile(BytesIO(payload))
            return list(
                parquet_file.iter_batches(
                    batch_size=batch_size,
                    columns=self._PMXT_COLUMNS,
                )
            )
        except (OSError, ValueError, pa.ArrowException):
            return None

    def _filter_table_to_token(self, table: pa.Table) -> pa.Table:
        if self.token_id is None or table.num_rows == 0:
            return table

        token_mask = pc.match_substring_regex(
            table.column("data"),
            rf'"token_id"\s*:\s*"{re.escape(self.token_id)}"',
        )
        token_mask = pc.fill_null(token_mask, False)
        return table.filter(token_mask)

    def _load_market_table(
        self,
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> pa.Table | None:
        table = self._load_cached_market_table(hour)
        if table is not None:
            return table

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

        relay_batches = self._load_relay_market_batches(hour, batch_size=batch_size)
        if relay_batches is not None:
            table = (
                pa.Table.from_batches(relay_batches)
                if relay_batches
                else self._empty_market_table()
            )
            if self._pmxt_cache_dir is not None:
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, table)
            return table

        remote_table = self._load_remote_market_table(hour, batch_size=batch_size)
        if remote_table is not None:
            remote_table = self._filter_table_to_token(remote_table)
            if self._pmxt_cache_dir is not None:
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, remote_table)
            return remote_table

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
        hour: pd.Timestamp,
        *,
        batch_size: int,
    ) -> list[pa.RecordBatch] | None:
        batches = self._load_cached_market_batches(hour)
        if batches is not None:
            return batches

        batches = self._load_local_archive_market_batches(hour, batch_size=batch_size)
        if batches is not None:
            if self._pmxt_cache_dir is not None:
                table = pa.Table.from_batches(batches) if batches else self._empty_market_table()
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, table)
            return batches

        batches = self._load_relay_market_batches(hour, batch_size=batch_size)
        if batches is not None:
            if self._pmxt_cache_dir is not None:
                table = pa.Table.from_batches(batches) if batches else self._empty_market_table()
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, table)
            return batches

        batches = self._load_remote_market_batches(hour, batch_size=batch_size)
        if batches is not None:
            if self._pmxt_cache_dir is not None:
                table = pa.Table.from_batches(batches) if batches else self._empty_market_table()
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, table)
            return batches

        batches = self._load_relay_raw_market_batches(hour, batch_size=batch_size)
        if batches is not None:
            if self._pmxt_cache_dir is not None:
                table = pa.Table.from_batches(batches) if batches else self._empty_market_table()
                with suppress(OSError, pa.ArrowException):
                    self._write_market_cache(hour, table)
            return batches

        return None

    def _emit_download_progress(
        self,
        url: str,
        *,
        downloaded_bytes: int,
        total_bytes: int | None,
        finished: bool,
    ) -> None:
        callback = getattr(self, "_pmxt_download_progress_callback", None)
        if callback is None:
            return
        callback(url, downloaded_bytes, total_bytes, finished)

    @staticmethod
    def _content_length_from_response(response: object) -> int | None:
        headers = getattr(response, "headers", None)
        if headers is None:
            return None
        raw_value = headers.get("Content-Length")
        if raw_value is None:
            return None
        try:
            return max(0, int(raw_value))
        except (TypeError, ValueError):
            return None

    def _download_payload_with_progress(self, url: str) -> bytes | None:
        with urlopen(url) as response:  # noqa: S310
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

    def _iter_market_tables(
        self,
        hours: list[pd.Timestamp],
        *,
        batch_size: int,
    ) -> Iterator[tuple[pd.Timestamp, pa.Table | None]]:
        max_workers = min(self._pmxt_prefetch_workers, len(hours))
        if max_workers <= 1:
            for hour in hours:
                yield hour, self._load_market_table(hour, batch_size=batch_size)
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[pd.Timestamp, Future[pa.Table | None]] = {}
            next_index = 0

            def _submit_next() -> None:
                nonlocal next_index
                if next_index >= len(hours):
                    return
                hour = hours[next_index]
                next_index += 1
                futures[hour] = executor.submit(
                    self._load_market_table,
                    hour,
                    batch_size=batch_size,
                )

            for _ in range(max_workers):
                _submit_next()

            for hour in hours:
                table = futures.pop(hour).result()
                _submit_next()
                yield hour, table

    def _iter_market_batches(
        self,
        hours: list[pd.Timestamp],
        *,
        batch_size: int,
    ) -> Iterator[tuple[pd.Timestamp, list[pa.RecordBatch] | None]]:
        max_workers = min(self._pmxt_prefetch_workers, len(hours))
        if max_workers <= 1:
            for hour in hours:
                yield hour, self._load_market_batches(hour, batch_size=batch_size)
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[pd.Timestamp, Future[list[pa.RecordBatch] | None]] = {}
            next_index = 0

            def _submit_next() -> None:
                nonlocal next_index
                if next_index >= len(hours):
                    return
                hour = hours[next_index]
                next_index += 1
                futures[hour] = executor.submit(
                    self._load_market_batches,
                    hour,
                    batch_size=batch_size,
                )

            for _ in range(max_workers):
                _submit_next()

            for hour in hours:
                batches = futures.pop(hour).result()
                _submit_next()
                yield hour, batches

    @staticmethod
    def _timestamp_to_ms_string(timestamp_secs: float) -> str:
        return f"{timestamp_secs * 1000:.6f}"

    @staticmethod
    def _quote_from_book(
        *,
        instrument,
        local_book: OrderBook,
        ts_event_ns: int,
    ) -> QuoteTick | None:
        bid_price = local_book.best_bid_price()
        ask_price = local_book.best_ask_price()
        bid_size = local_book.best_bid_size()
        ask_size = local_book.best_ask_size()
        if bid_price is None or ask_price is None or bid_size is None or ask_size is None:
            return None

        return QuoteTick(
            instrument_id=instrument.id,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size,
            ask_size=ask_size,
            ts_event=ts_event_ns,
            # Process the quote immediately after the associated book update.
            ts_init=ts_event_ns + 1,
        )

    @staticmethod
    def _decode_book_snapshot(payload_text: str) -> _PMXTBookSnapshotPayload:
        return _PMXT_BOOK_SNAPSHOT_DECODER.decode(payload_text)

    @staticmethod
    def _decode_price_change(payload_text: str) -> _PMXTPriceChangePayload:
        return _PMXT_PRICE_CHANGE_DECODER.decode(payload_text)

    @staticmethod
    def _to_book_snapshot(payload: _PMXTBookSnapshotPayload) -> PolymarketBookSnapshot:
        return PolymarketBookSnapshot(
            market=payload.market_id,
            asset_id=payload.token_id,
            bids=[PolymarketBookLevel(price=price, size=size) for price, size in payload.bids],
            asks=[PolymarketBookLevel(price=price, size=size) for price, size in payload.asks],
            timestamp=PolymarketPMXTDataLoader._timestamp_to_ms_string(payload.timestamp),
        )

    @staticmethod
    def _to_price_change(payload: _PMXTPriceChangePayload) -> PolymarketQuotes:
        side = PolymarketOrderSide(payload.change_side)
        return PolymarketQuotes(
            market=payload.market_id,
            price_changes=[
                PolymarketQuote(
                    asset_id=payload.token_id,
                    price=payload.change_price,
                    side=side,
                    size=payload.change_size,
                    hash=(
                        f"pmxt:{payload.market_id}:{payload.token_id}:"
                        f"{payload.timestamp:.6f}:{payload.change_side}:{payload.change_price}"
                    ),
                    best_bid=payload.best_bid,
                    best_ask=payload.best_ask,
                ),
            ],
            timestamp=PolymarketPMXTDataLoader._timestamp_to_ms_string(payload.timestamp),
        )

    @staticmethod
    def _event_sort_key(record: OrderBookDeltas | QuoteTick) -> tuple[int, int, int]:
        ts_event = int(getattr(record, "ts_event", getattr(record, "ts_init", 0)))
        ts_init = int(getattr(record, "ts_init", ts_event))
        if isinstance(record, OrderBookDeltas):
            priority = 0
        elif isinstance(record, QuoteTick):
            priority = 1
        else:
            priority = 2
        return (ts_event, priority, ts_init)

    def _process_book_snapshot(
        self,
        payload_text: str,
        *,
        token_id: str,
        instrument,
        local_book: OrderBook,
        has_snapshot: bool,
        events: list[OrderBookDeltas | QuoteTick],
        start_ns: int,
        end_ns: int,
        include_order_book: bool,
        include_quotes: bool,
    ) -> tuple[OrderBook, bool]:
        payload = self._decode_book_snapshot(payload_text)
        if payload.token_id != token_id:
            return local_book, has_snapshot

        snapshot = self._to_book_snapshot(payload)
        deltas = snapshot.parse_to_snapshot(
            instrument=instrument,
            ts_init=int(payload.timestamp * 1_000_000_000),
        )
        if deltas is None:
            return local_book, has_snapshot

        event_ns = deltas.ts_event
        local_book = OrderBook(instrument.id, book_type=BookType.L2_MBP)
        local_book.apply_deltas(deltas)
        has_snapshot = True
        if event_ns < start_ns or event_ns > end_ns:
            return local_book, has_snapshot

        if include_order_book:
            events.append(deltas)
        if include_quotes:
            quote = snapshot.parse_to_quote(
                instrument=instrument,
                ts_init=deltas.ts_event + 1,
            )
            if quote is not None:
                events.append(quote)

        return local_book, has_snapshot

    def _process_price_change(
        self,
        payload_text: str,
        *,
        token_id: str,
        instrument,
        local_book: OrderBook,
        has_snapshot: bool,
        events: list[OrderBookDeltas | QuoteTick],
        start_ns: int,
        end_ns: int,
        include_order_book: bool,
        include_quotes: bool,
    ) -> OrderBook:
        if not has_snapshot:
            return local_book

        payload = self._decode_price_change(payload_text)
        if payload.token_id != token_id:
            return local_book

        quotes = self._to_price_change(payload)
        deltas = quotes.parse_to_deltas(
            instrument=instrument,
            ts_init=int(payload.timestamp * 1_000_000_000),
        )
        local_book.apply_deltas(deltas)

        event_ns = deltas.ts_event
        if event_ns < start_ns or event_ns > end_ns:
            return local_book

        if include_order_book:
            events.append(deltas)
        if include_quotes:
            quote = self._quote_from_book(
                instrument=instrument,
                local_book=local_book,
                ts_event_ns=deltas.ts_event,
            )
            if quote is not None:
                events.append(quote)

        return local_book

    def load_order_book_and_quotes(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        batch_size: int = 25_000,
        include_order_book: bool = True,
        include_quotes: bool = True,
    ) -> list[OrderBookDeltas | QuoteTick]:
        """
        Load one market's historical L2 updates from PMXT.

        Only the target token's rows are materialized in memory; each parquet file
        is filtered by market ID during scan and discarded once processed.
        """
        if self.condition_id is None:
            raise ValueError("condition_id is required for PMXT loading")
        if self.token_id is None:
            raise ValueError("token_id is required for PMXT loading")

        start_ts = self._normalize_timestamp(start)
        end_ts = self._normalize_timestamp(end)
        if start_ts is None or end_ts is None or end_ts <= start_ts:
            return []

        start_ns = start_ts.value
        end_ns = end_ts.value
        token_id = self.token_id
        instrument = self.instrument
        local_book = OrderBook(instrument.id, book_type=BookType.L2_MBP)
        has_snapshot = False
        events: list[OrderBookDeltas | QuoteTick] = []
        hours = self._archive_hours(start_ts, end_ts)

        for _hour, batches in self._iter_market_batches(hours, batch_size=batch_size):
            if not batches:
                continue

            for batch in batches:
                update_types = batch.column("update_type").to_pylist()
                payload_texts = batch.column("data").to_pylist()
                for update_type, payload_text in zip(update_types, payload_texts, strict=False):
                    if update_type == "book_snapshot":
                        local_book, has_snapshot = self._process_book_snapshot(
                            payload_text,
                            token_id=token_id,
                            instrument=instrument,
                            local_book=local_book,
                            has_snapshot=has_snapshot,
                            events=events,
                            start_ns=start_ns,
                            end_ns=end_ns,
                            include_order_book=include_order_book,
                            include_quotes=include_quotes,
                        )
                        continue

                    if update_type == "price_change":
                        local_book = self._process_price_change(
                            payload_text,
                            token_id=token_id,
                            instrument=instrument,
                            local_book=local_book,
                            has_snapshot=has_snapshot,
                            events=events,
                            start_ns=start_ns,
                            end_ns=end_ns,
                            include_order_book=include_order_book,
                            include_quotes=include_quotes,
                        )

        events.sort(key=self._event_sort_key)
        return events
