from __future__ import annotations

import asyncio
import gc
import io
import os
import random
import signal
import sys
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from itertools import islice
from pathlib import Path
from queue import Empty, Full, Queue
from socket import timeout as SocketTimeout
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError as UrllibHTTPError, URLError

import duckdb
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.auto import tqdm

TELONEX_API_KEY_ENV = "TELONEX_API_KEY"
TELONEX_PARSE_WORKERS_ENV = "TELONEX_PARSE_WORKERS"
TELONEX_WRITER_QUEUE_ITEMS_ENV = "TELONEX_WRITER_QUEUE_ITEMS"
TELONEX_PENDING_COMMIT_ITEMS_ENV = "TELONEX_PENDING_COMMIT_ITEMS"

_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_API_BASE_URL = "https://api.telonex.io"
_DEFAULT_CHANNEL = "book_snapshot_full"
_EXCHANGE = "polymarket"
_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
_MANIFEST_FILENAME = "telonex.duckdb"
_DATA_SUBDIR = "data"
_TARGET_PART_BYTES = 64 << 30  # uncompressed Arrow safety cap before rolling
_TARGET_PART_DISK_BYTES = 512 << 20  # compressed on-disk Parquet target before rolling
_TARGET_PART_PENDING_DAYS = 10_000  # manifest rows held by one open part before rolling
_PARQUET_COMPRESSION = "zstd"
_PARQUET_COMPRESSION_LEVEL = 3
# Keep pending_for_commit bounded so book_snapshot_full Arrow tables don't pin
# RAM for long. The parquet writer stays open across flushes and only rolls the
# part file when `_TARGET_PART_BYTES` is hit; the time threshold is intentionally
# high enough to avoid tiny row groups during high-throughput downloads.
_DEFAULT_COMMIT_BATCH_ROWS = 50_000
_DEFAULT_COMMIT_BATCH_SECS = 10.0
_MAX_PENDING_COMMIT_ITEMS = 128
_MEMORY_LOG_INTERVAL_SECS = 3600.0
_DEFAULT_MAX_RETRIES = 4
_RETRY_BACKOFF_BASE_SECS = 2.0
_TRANSIENT_HTTP_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
_DEFAULT_EMPTY_RECHECK_AFTER_DAYS = 7
_FORCE_EXIT_SIGNAL_COUNT = 5
_MARKETS_BATCH_ROWS = 100_000
_HTTP_KEEPALIVE_EXPIRY_SECS = 30.0
_DEFAULT_PARSE_WORKERS = min(8, max(1, os.cpu_count() or 2))
_ORDER_BOOK_LEVEL_TYPE = pa.struct([pa.field("price", pa.string()), pa.field("size", pa.string())])
_ORDER_BOOK_LEVELS_TYPE = pa.list_(pa.field("element", _ORDER_BOOK_LEVEL_TYPE))
_ORDER_BOOK_SIDE_COLUMNS = frozenset({"bids", "asks"})
_INT_TIMESTAMP_COLUMNS = frozenset({"timestamp_us", "block_timestamp_us"})
_FLOAT_TIMESTAMP_COLUMNS = frozenset({"local_timestamp_us"})

_CHANNEL_COLUMN_SUFFIX = {
    "trades": ("trades_from", "trades_to"),
    "quotes": ("quotes_from", "quotes_to"),
    "book_snapshot_5": ("book_snapshot_5_from", "book_snapshot_5_to"),
    "book_snapshot_25": ("book_snapshot_25_from", "book_snapshot_25_to"),
    "book_snapshot_full": ("book_snapshot_full_from", "book_snapshot_full_to"),
    "onchain_fills": ("onchain_fills_from", "onchain_fills_to"),
}
VALID_CHANNELS = tuple(_CHANNEL_COLUMN_SUFFIX.keys())


@dataclass(frozen=True)
class TelonexDownloadSummary:
    destination: str
    db_path: str  # manifest DuckDB path — kept named db_path for wire compat
    channels: list[str]
    base_url: str
    markets_considered: int
    requested_days: int | None
    downloaded_days: int
    skipped_existing_days: int
    missing_days: int
    failed_days: int
    cancelled_days: int
    bytes_downloaded: int
    start_date: str | None
    end_date: str | None
    db_size_bytes: int = 0
    interrupted: bool = False
    failed_samples: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _format_bytes(size: int | None) -> str:
    if size is None:
        return "? B"
    value = max(0, int(size))
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.2f} MiB"
    return f"{value / (1024 * 1024 * 1024):.2f} GiB"


def _get_rss_mb() -> float | None:
    """Get current process RSS in MiB, or None if unavailable.

    Prefers ``psutil`` (which returns true current RSS on every OS) and
    falls back to ``/proc/self/status`` on Linux or ``resource.getrusage``
    on macOS.  The macOS fallback reports *peak* RSS, not the current
    value, because ``ru_maxrss`` is the only per-process metric available
    without psutil — still useful as an upper-bound guard.
    """
    try:
        import psutil

        return psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    except Exception:
        pass
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024  # KiB -> MiB
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        if sys.platform == "darwin":
            return usage.ru_maxrss / (1024 * 1024)
        return usage.ru_maxrss / 1024  # KiB on Linux
    except (ImportError, OSError):
        return None


def _release_arrow_memory() -> None:
    """Release freed Arrow memory back to the OS allocator."""
    try:
        pa.default_memory_pool().release_unused()
    except AttributeError:
        pass  # older PyArrow without release_unused


def _get_arrow_allocated_mb() -> float | None:
    """Return bytes still owned by the Arrow memory pool in MiB."""
    try:
        return pa.default_memory_pool().bytes_allocated() / (1024 * 1024)
    except AttributeError:
        return None


def _parse_date_bound(value: str | None) -> date | None:
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(normalized, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
    return parsed.date()


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _api_url(
    *,
    base_url: str,
    channel: str,
    market_slug: str,
    outcome: str | None,
    outcome_id: int | None,
    day: date,
) -> str:
    params: dict[str, str] = {"slug": market_slug}
    if outcome is not None:
        params["outcome"] = outcome
    else:
        assert outcome_id is not None
        params["outcome_id"] = str(outcome_id)
    return (
        f"{base_url.rstrip('/')}/v1/downloads/{_EXCHANGE}/{channel}/{day:%Y-%m-%d}"
        f"?{urlencode(params)}"
    )


@dataclass
class _Job:
    market_slug: str
    outcome_segment: str
    outcome_id: int | None
    outcome: str | None
    channel: str
    day: date


@dataclass
class _CatalogJobIterable:
    frame: pd.DataFrame
    channel_col_idxs: list[tuple[str, int, int]]
    slug_idx: int
    outcomes: list[int]
    markets_considered: int
    total_jobs: int

    def __iter__(self) -> Iterator[_Job]:
        for row in self.frame.itertuples():
            slug = row[self.slug_idx]
            if not slug:
                continue
            slug_str = str(slug)
            for channel, from_idx, to_idx in self.channel_col_idxs:
                raw_from = row[from_idx]
                raw_to = row[to_idx]
                if raw_from is None or raw_to is None:
                    continue
                try:
                    if pd.isna(raw_from) or pd.isna(raw_to):
                        continue
                except (ValueError, TypeError):
                    pass
                start = (
                    raw_from.date()
                    if hasattr(raw_from, "date")
                    else _parse_date_bound(str(raw_from))
                )
                end = raw_to.date() if hasattr(raw_to, "date") else _parse_date_bound(str(raw_to))
                if start is None or end is None or start > end:
                    continue
                days = _date_range(start, end)
                for outcome_id in self.outcomes:
                    for day in days:
                        yield _Job(
                            market_slug=slug_str,
                            outcome_segment=str(outcome_id),
                            outcome_id=outcome_id,
                            outcome=None,
                            channel=channel,
                            day=day,
                        )


@dataclass
class _DownloadResult:
    job: _Job
    status: str  # "ok", "skipped", "missing", "failed", "cancelled"
    # `table` is populated after parsing `payload`. Keep this as Arrow instead
    # of pandas so the hot download path avoids DataFrame materialization and
    # pandas->Arrow conversion before writing consolidated parts.
    table: pa.Table | None
    payload: bytes | None
    bytes_downloaded: int
    error: str | None


@dataclass
class _FlushWriterQueue:
    reason: str
    close_parts: bool = False
    ack: threading.Event = field(default_factory=threading.Event)
    open_parts_before: int = 0
    open_parts_after: int = 0
    pending_part_days_before: int = 0
    pending_part_days_after: int = 0
    closed_parts: int = 0


class _CancelledError(Exception):
    pass


@dataclass
class _OpenPart:
    """A Parquet part-file that's open for appending row groups.

    Stays open across commit batches until it crosses the compressed on-disk
    part target or the uncompressed Arrow safety cap;
    only then do we close it and flush its manifest rows. Partial parts on
    crash are orphaned but never referenced from the manifest, so they're
    benign — the affected days re-download on the next run.
    """

    path: Path
    writer: pq.ParquetWriter
    schema: pa.Schema
    bytes_written: int
    disk_bytes: int
    pending: list[tuple[_DownloadResult, int]]  # (result, row_count) waiting for manifest commit


def _is_nullish_type(value_type: pa.DataType) -> bool:
    if pa.types.is_null(value_type):
        return True
    if pa.types.is_list(value_type) or pa.types.is_large_list(value_type):
        return _is_nullish_type(value_type.value_type)
    return False


def _normalize_telonex_table(table: pa.Table) -> pa.Table:
    fields: list[pa.Field] = []
    changed = False
    for schema_field in table.schema:
        target_type = schema_field.type
        if schema_field.name in _ORDER_BOOK_SIDE_COLUMNS and _is_nullish_type(schema_field.type):
            target_type = _ORDER_BOOK_LEVELS_TYPE
        elif schema_field.name in _INT_TIMESTAMP_COLUMNS and pa.types.is_floating(
            schema_field.type
        ):
            target_type = pa.int64()
        elif schema_field.name in _FLOAT_TIMESTAMP_COLUMNS and pa.types.is_integer(
            schema_field.type
        ):
            target_type = pa.float64()

        if target_type.equals(schema_field.type):
            fields.append(schema_field)
            continue
        fields.append(
            pa.field(
                schema_field.name,
                target_type,
                nullable=schema_field.nullable,
                metadata=schema_field.metadata,
            )
        )
        changed = True

    if not changed:
        return table

    schema = pa.schema(fields, metadata=table.schema.metadata)
    try:
        return table.cast(schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return table


def _merge_promotable_schema(base: pa.Schema, incoming: pa.Schema) -> pa.Schema | None:
    """Merge schemas when differences are additive or null-only.

    Parquet files cannot change schema after their writer is opened. This lets
    the store learn a stable channel schema for future parts while avoiding
    unsafe numeric/string coercions that could hide real data defects.
    """
    incoming_by_name = {field.name: field for field in incoming}
    merged_fields: list[pa.Field] = []
    for base_field in base:
        incoming_field = incoming_by_name.pop(base_field.name, None)
        if incoming_field is None:
            merged_fields.append(base_field)
            continue
        if base_field.type.equals(incoming_field.type):
            merged_fields.append(base_field)
        elif _is_nullish_type(base_field.type):
            merged_fields.append(incoming_field)
        elif _is_nullish_type(incoming_field.type):
            merged_fields.append(base_field)
        else:
            return None

    merged_fields.extend(incoming_by_name.values())
    return pa.schema(merged_fields, metadata=base.metadata or incoming.metadata)


def _align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table | None:
    source_names = set(table.schema.names)
    target_names = set(schema.names)
    if source_names - target_names:
        return None

    arrays = []
    for target_field in schema:
        source_index = table.schema.get_field_index(target_field.name)
        if source_index < 0:
            arrays.append(pa.nulls(table.num_rows, type=target_field.type))
            continue
        source_field = table.schema.field(source_index)
        if not source_field.type.equals(target_field.type) and not _is_nullish_type(
            source_field.type
        ):
            return None
        arrays.append(table.column(source_index))

    aligned = pa.Table.from_arrays(arrays, schema=schema)
    try:
        return aligned.cast(schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return None


class _TelonexParquetStore:
    """Hive-partitioned Parquet store with a small DuckDB manifest.

    Layout::

        <root>/
          telonex.duckdb                           -- manifest only (MB-scale)
          data/
            channel=<channel>/year=<y>/month=<mm>/part-NNNNNN.parquet

    Writer rolls a new part file when the open part crosses the compressed
    on-disk target or the uncompressed Arrow safety cap. It normalizes common
    Telonex schema drift before writing, so empty order-book sides and optional
    columns don't create one tiny file per batch. Readers query everything via
    `read_parquet('<root>/data/channel=X/**/*.parquet', hive_partitioning=1,
    union_by_name=True)` — DuckDB prunes on year/month for free.
    """

    def __init__(self, root: Path, *, manifest_name: str = _MANIFEST_FILENAME) -> None:
        self._root = root
        self._data_root = root / _DATA_SUBDIR
        self._data_root.mkdir(parents=True, exist_ok=True)
        self._manifest_path = root / manifest_name
        self._lock = threading.Lock()
        self._con = duckdb.connect(str(self._manifest_path))
        self._init_schema()
        self._writers: dict[tuple[str, int, int], _OpenPart] = {}
        self._channel_schemas: dict[str, pa.Schema] = {}
        self._closed = False
        # A previous run killed via SIGTERM/SIGKILL may have left half-written
        # Parquet files on disk — no footer, unreadable. Sweep them before any
        # new writes so the channel globs stay clean.
        self._remove_orphan_parts()

    @property
    def manifest_path(self) -> Path:
        return self._manifest_path

    @property
    def data_root(self) -> Path:
        return self._data_root

    @property
    def open_writer_count(self) -> int:
        """Number of open Parquet part writers (diagnostic)."""
        with self._lock:
            return len(self._writers)

    def _open_part_stats_locked(self) -> tuple[int, int]:
        return (
            len(self._writers),
            sum(len(part.pending) for part in self._writers.values()),
        )

    def open_part_stats(self) -> tuple[int, int]:
        """Return (open writer count, pending manifest days) for diagnostics."""
        with self._lock:
            return self._open_part_stats_locked()

    def close(self, *, progress_label: str | None = None) -> None:
        """Flush all open writers and close the manifest. Idempotent."""
        with self._lock:
            if self._closed:
                return
            keys = list(self._writers.keys())
            if progress_label is not None and keys:
                _open_parts, pending_part_days = self._open_part_stats_locked()
                print(
                    f"[telonex] {progress_label}: flushing {len(keys):,} open "
                    f"part writer(s), pending_manifest_days={pending_part_days:,}.",
                    file=sys.stderr,
                    flush=True,
                )
            for index, key in enumerate(keys, start=1):
                pending_days = len(self._writers[key].pending)
                if progress_label is not None:
                    channel, year, month = key
                    print(
                        f"[telonex] {progress_label}: flushing part {index:,}/"
                        f"{len(keys):,} channel={channel} year={year} "
                        f"month={month:02d} pending_days={pending_days:,}.",
                        file=sys.stderr,
                        flush=True,
                    )
                self._flush_open_part_locked(key)
            if progress_label is not None:
                print(
                    f"[telonex] {progress_label}: closing DuckDB manifest.",
                    file=sys.stderr,
                    flush=True,
                )
            self._remove_orphan_parts()
            self._con.close()
            self._closed = True
            if progress_label is not None:
                print(
                    f"[telonex] {progress_label}: closed.",
                    file=sys.stderr,
                    flush=True,
                )

    def _init_schema(self) -> None:
        with self._lock:
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS completed_days (
                    channel VARCHAR NOT NULL,
                    market_slug VARCHAR NOT NULL,
                    outcome_segment VARCHAR NOT NULL,
                    day DATE NOT NULL,
                    rows BIGINT NOT NULL,
                    bytes_downloaded BIGINT NOT NULL,
                    parquet_part VARCHAR,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (channel, market_slug, outcome_segment, day)
                )
                """
            )
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS empty_days (
                    channel VARCHAR NOT NULL,
                    market_slug VARCHAR NOT NULL,
                    outcome_segment VARCHAR NOT NULL,
                    day DATE NOT NULL,
                    status VARCHAR NOT NULL,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (channel, market_slug, outcome_segment, day)
                )
                """
            )

    def completed_keys(self, channel: str) -> set[tuple[str, str, date]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT market_slug, outcome_segment, day FROM completed_days WHERE channel = ?",
                [channel],
            ).fetchall()
        return {(row[0], row[1], row[2]) for row in rows}

    def empty_keys(
        self, channel: str, *, recheck_after_days: int | None = None
    ) -> set[tuple[str, str, date]]:
        where = "channel = ?"
        params: list[object] = [channel]
        if recheck_after_days is not None and recheck_after_days >= 0:
            cutoff = datetime.now(UTC) - timedelta(days=recheck_after_days)
            where += " AND checked_at > ?"
            params.append(cutoff)
        with self._lock:
            rows = self._con.execute(
                f"SELECT market_slug, outcome_segment, day FROM empty_days WHERE {where}",
                params,
            ).fetchall()
        return {(row[0], row[1], row[2]) for row in rows}

    def mark_empty(self, job: _Job, *, status: str) -> None:
        self.mark_empty_batch([(job, status)])

    def mark_empty_batch(self, entries: list[tuple[_Job, str]]) -> None:
        if not entries:
            return
        with self._lock:
            self._con.execute("BEGIN TRANSACTION")
            try:
                for job, status in entries:
                    self._con.execute(
                        "DELETE FROM completed_days "
                        "WHERE channel = ? AND market_slug = ? AND outcome_segment = ? AND day = ?",
                        [job.channel, job.market_slug, job.outcome_segment, job.day],
                    )
                    self._con.execute(
                        "INSERT OR REPLACE INTO empty_days "
                        "(channel, market_slug, outcome_segment, day, status) "
                        "VALUES (?, ?, ?, ?, ?)",
                        [job.channel, job.market_slug, job.outcome_segment, job.day, status],
                    )
                self._con.execute("COMMIT")
            except Exception:
                self._con.execute("ROLLBACK")
                raise

    def _partition_dir(self, channel: str, year: int, month: int) -> Path:
        # Hive-style keys so DuckDB's `hive_partitioning=1` recovers year/month
        # as queryable columns on read.
        return self._data_root / f"channel={channel}" / f"year={year}" / f"month={month:02d}"

    @staticmethod
    def _next_part_number(partition_dir: Path) -> int:
        if not partition_dir.exists():
            return 0
        nums: list[int] = []
        for path in partition_dir.glob("part-*.parquet"):
            try:
                nums.append(int(path.stem.rsplit("-", 1)[1]))
            except (ValueError, IndexError):
                continue
        return (max(nums) + 1) if nums else 0

    def _open_part(self, key: tuple[str, int, int], schema: pa.Schema) -> _OpenPart:
        channel, year, month = key
        partition_dir = self._partition_dir(channel, year, month)
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_num = self._next_part_number(partition_dir)
        part_path = partition_dir / f"part-{part_num:06d}.parquet"
        writer = pq.ParquetWriter(
            where=str(part_path),
            schema=schema,
            compression=_PARQUET_COMPRESSION,
            compression_level=_PARQUET_COMPRESSION_LEVEL,
        )
        return _OpenPart(
            path=part_path,
            writer=writer,
            schema=schema,
            bytes_written=0,
            disk_bytes=0,
            pending=[],
        )

    def _flush_open_part_locked(self, key: tuple[str, int, int]) -> None:
        """Close the open writer for a partition and commit its pending manifest rows.

        Caller MUST hold `self._lock`. On failure to commit manifest rows, the
        Parquet file on disk becomes an orphan (not referenced) — its days will
        be retried on the next run, producing a fresh part file.
        """
        part = self._writers.pop(key, None)
        if part is None:
            return
        try:
            part.writer.close()
        except Exception:
            # Close itself failed — try to unlink the half-written file so it
            # doesn't confuse readers or the next-part-number scan.
            try:
                part.path.unlink()
            except OSError:
                pass
            raise
        if not part.pending:
            # Empty writer (shouldn't normally happen). Delete the empty file.
            try:
                part.path.unlink()
            except OSError:
                pass
            return

        rel_part = str(part.path.relative_to(self._root))
        self._con.execute("BEGIN TRANSACTION")
        try:
            for result, row_count in part.pending:
                self._con.execute(
                    "DELETE FROM empty_days "
                    "WHERE channel = ? AND market_slug = ? AND outcome_segment = ? AND day = ?",
                    [
                        result.job.channel,
                        result.job.market_slug,
                        result.job.outcome_segment,
                        result.job.day,
                    ],
                )
                self._con.execute(
                    "INSERT OR REPLACE INTO completed_days "
                    "(channel, market_slug, outcome_segment, day, rows, "
                    "bytes_downloaded, parquet_part) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        result.job.channel,
                        result.job.market_slug,
                        result.job.outcome_segment,
                        result.job.day,
                        row_count,
                        result.bytes_downloaded,
                        rel_part,
                    ],
                )
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

    def _append_to_partition(
        self, key: tuple[str, int, int], entries: list[_DownloadResult]
    ) -> int:
        """Concat every entry's Arrow table for the partition and write it as a
        single row group. Arrow schema promotion unifies divergent column sets,
        so the partition keeps a single open writer and only rolls when the
        unified schema genuinely changes (new column appears) or the part
        crosses the byte target.
        Caller holds the lock.
        """
        enriched_entries: list[tuple[_DownloadResult, int, pa.Table]] = []
        for entry in entries:
            assert entry.table is not None
            table = entry.table
            row_count = table.num_rows
            enriched = table.append_column(
                "market_slug", pa.repeat(pa.scalar(entry.job.market_slug), row_count)
            ).append_column(
                "outcome_segment",
                pa.repeat(pa.scalar(entry.job.outcome_segment), row_count),
            )
            enriched_entries.append((entry, row_count, enriched))
            # Drop the dataclass reference so the table can be GC'd when
            # the local `enriched_entries` list is cleared below.
            entry.table = None

        try:
            table = pa.concat_tables(
                [table for _entry, _row_count, table in enriched_entries],
                promote_options="default",
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            total_rows = 0
            for entry, row_count, table in enriched_entries:
                total_rows += self._write_partition_table_locked(key, table, [(entry, row_count)])
            enriched_entries.clear()
            return total_rows

        pending = [(entry, row_count) for entry, row_count, _table in enriched_entries]
        enriched_entries.clear()
        return self._write_partition_table_locked(key, table, pending)

    def _write_partition_table_locked(
        self,
        key: tuple[str, int, int],
        table: pa.Table,
        pending: list[tuple[_DownloadResult, int]],
    ) -> int:
        """Write one Arrow table to a partition, rolling when needed.

        Caller holds the lock.
        """
        table = _normalize_telonex_table(table)
        channel = key[0]
        channel_schema = self._channel_schemas.get(channel)
        if channel_schema is None:
            channel_schema = table.schema
            self._channel_schemas[channel] = channel_schema
        else:
            merged_schema = _merge_promotable_schema(channel_schema, table.schema)
            if merged_schema is not None:
                channel_schema = merged_schema
                self._channel_schemas[channel] = channel_schema
                aligned = _align_table_to_schema(table, channel_schema)
                if aligned is not None:
                    table = aligned

        part = self._writers.get(key)
        if part is not None:
            aligned = _align_table_to_schema(table, part.schema)
            if aligned is not None:
                table = aligned
            else:
                # Parquet writers cannot change schema in-place. True additive
                # schema evolution rolls once, then future rows align to the
                # learned channel schema.
                self._flush_open_part_locked(key)
                part = None

        if part is None:
            open_schema = self._channel_schemas.get(channel)
            if open_schema is not None:
                aligned = _align_table_to_schema(table, open_schema)
                if aligned is not None:
                    table = aligned
            part = self._open_part(key, table.schema)
            self._writers[key] = part

        total_rows = table.num_rows
        part.writer.write_table(table)
        part.bytes_written += table.nbytes
        try:
            part.disk_bytes = part.path.stat().st_size
        except OSError:
            part.disk_bytes = 0
        part.pending.extend(pending)

        del table

        if (
            part.disk_bytes >= _TARGET_PART_DISK_BYTES
            or part.bytes_written >= _TARGET_PART_BYTES
            or len(part.pending) >= _TARGET_PART_PENDING_DAYS
        ):
            self._flush_open_part_locked(key)

        return total_rows

    def ingest_batch(self, results: list[_DownloadResult]) -> int:
        """Route a batch of downloads into open Parquet part writers and update
        manifest rows for empty-but-ok days. Non-empty days have their manifest
        rows committed as part of `_flush_open_part_locked` when the part closes.

        Raising here means nothing was promoted to the manifest for this batch —
        those days will be retried on the next run.
        """
        ok_by_partition: dict[tuple[str, int, int], list[_DownloadResult]] = {}
        empty_ok: list[_DownloadResult] = []
        missing: list[_DownloadResult] = []
        for result in results:
            if result.status == "missing":
                missing.append(result)
                continue
            if result.status != "ok":
                continue
            if result.table is None or result.table.num_rows == 0:
                empty_ok.append(result)
                continue
            key = (
                result.job.channel,
                result.job.day.year,
                result.job.day.month,
            )
            ok_by_partition.setdefault(key, []).append(result)

        total_rows = 0
        with self._lock:
            for key, entries in ok_by_partition.items():
                total_rows += self._append_to_partition(key, entries)

            # Empty-but-ok days are recorded inline — no Parquet file to reference.
            if empty_ok:
                self._con.execute("BEGIN TRANSACTION")
                try:
                    for entry in empty_ok:
                        self._con.execute(
                            "DELETE FROM empty_days "
                            "WHERE channel = ? AND market_slug = ? AND outcome_segment = ? "
                            "AND day = ?",
                            [
                                entry.job.channel,
                                entry.job.market_slug,
                                entry.job.outcome_segment,
                                entry.job.day,
                            ],
                        )
                        self._con.execute(
                            "INSERT OR REPLACE INTO completed_days "
                            "(channel, market_slug, outcome_segment, day, rows, "
                            "bytes_downloaded, parquet_part) "
                            "VALUES (?, ?, ?, ?, 0, ?, NULL)",
                            [
                                entry.job.channel,
                                entry.job.market_slug,
                                entry.job.outcome_segment,
                                entry.job.day,
                                entry.bytes_downloaded,
                            ],
                        )
                    self._con.execute("COMMIT")
                except Exception:
                    self._con.execute("ROLLBACK")
                    raise

            if missing:
                self._con.execute("BEGIN TRANSACTION")
                try:
                    for entry in missing:
                        self._con.execute(
                            "DELETE FROM completed_days "
                            "WHERE channel = ? AND market_slug = ? AND outcome_segment = ? "
                            "AND day = ?",
                            [
                                entry.job.channel,
                                entry.job.market_slug,
                                entry.job.outcome_segment,
                                entry.job.day,
                            ],
                        )
                        self._con.execute(
                            "INSERT OR REPLACE INTO empty_days "
                            "(channel, market_slug, outcome_segment, day, status) "
                            "VALUES (?, ?, ?, ?, ?)",
                            [
                                entry.job.channel,
                                entry.job.market_slug,
                                entry.job.outcome_segment,
                                entry.job.day,
                                entry.error or "404",
                            ],
                        )
                    self._con.execute("COMMIT")
                except Exception:
                    self._con.execute("ROLLBACK")
                    raise
        return total_rows

    def flush_all(self) -> int:
        """Close every open part writer, committing their pending manifest rows.
        Used at the end of a run so days aren't left in-memory."""
        with self._lock:
            closed = len(self._writers)
            for key in list(self._writers.keys()):
                self._flush_open_part_locked(key)
            return closed

    def size_bytes(self) -> int:
        total = 0
        try:
            total += self._manifest_path.stat().st_size
        except OSError:
            pass
        for path in self._data_root.rglob("*.parquet"):
            try:
                total += path.stat().st_size
            except OSError:
                continue
        return total

    def _remove_orphan_parts(self) -> int:
        """Delete Parquet parts not referenced by `completed_days.parquet_part`.

        Hard kills (SIGKILL, power loss) can leave half-written files with no
        Parquet footer; the days they contained aren't in the manifest either,
        so they'll be re-fetched on the next run. Sweeping the orphans prevents
        `read_parquet` globs from tripping over unreadable files.
        """
        referenced = {
            row[0]
            for row in self._con.execute(
                "SELECT DISTINCT parquet_part FROM completed_days WHERE parquet_part IS NOT NULL"
            ).fetchall()
        }
        removed = 0
        for path in self._data_root.rglob("*.parquet"):
            rel = str(path.relative_to(self._root))
            if rel in referenced:
                continue
            try:
                path.unlink()
                removed += 1
            except OSError:
                continue
        if removed:
            print(
                f"[telonex] Cleared {removed} orphan Parquet part(s) from a prior "
                "ungraceful shutdown. Their days will re-download.",
                file=sys.stderr,
            )
        return removed


def _fetch_markets_dataset(
    base_url: str, timeout_secs: int, *, show_progress: bool = False
) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/v1/datasets/polymarket/markets"
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=timeout_secs) as response:
        total_raw = response.headers.get("Content-Length")
        total = int(total_raw) if total_raw and total_raw.isdigit() else None
        chunks: list[bytes] = []
        with tqdm(
            total=total,
            desc="Fetching Telonex markets",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            dynamic_ncols=True,
            disable=not show_progress,
        ) as progress:
            while True:
                chunk = response.read(_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
                progress.update(len(chunk))

    payload = b"".join(chunks)
    parquet = pq.ParquetFile(io.BytesIO(payload))
    total_rows = parquet.metadata.num_rows if parquet.metadata is not None else None
    del total_rows
    batches = list(parquet.iter_batches(batch_size=_MARKETS_BATCH_ROWS))
    table = pa.Table.from_batches(batches, schema=parquet.schema_arrow)
    return table.to_pandas()


def _iter_days_for_market_tuple(
    row,
    *,
    from_idx: int,
    to_idx: int,
    window_start: date | None,
    window_end: date | None,
) -> list[date]:
    raw_from = row[from_idx]
    raw_to = row[to_idx]
    if raw_from is None or raw_to is None:
        return []
    # pd.isna catches NaT/NaN — a plain `in (None, "")` check misses those.
    try:
        if pd.isna(raw_from) or pd.isna(raw_to):
            return []
    except (ValueError, TypeError):
        pass
    if raw_from in (None, "") or raw_to in (None, ""):
        return []
    start = _parse_date_bound(raw_from)
    end = _parse_date_bound(raw_to)
    if start is None or end is None:
        return []
    if window_start is not None and start < window_start:
        start = window_start
    if window_end is not None and end > window_end:
        end = window_end
    if start > end:
        return []
    return _date_range(start, end)


def _iter_jobs_from_catalog(
    *,
    markets: pd.DataFrame,
    channels: list[str],
    outcomes: list[int],
    window_start: date | None,
    window_end: date | None,
    status_filter: str | None,
    slug_filter: set[str] | None,
    show_progress: bool,
) -> _CatalogJobIterable:
    """Plan catalog job metadata eagerly, then stream jobs on iteration.

    Uses itertuples() instead of iterrows() for ~10-100x faster row iteration.
    Drops unused columns upfront so the 5+ GiB catalog shrinks before
    the reusable iterable holds a reference to the slim frame.
    """
    # Collect only the columns we need: slug, status, and per-channel date bounds.
    needed_cols: list[str] = ["slug"]
    if status_filter is not None:
        needed_cols.append("status")
    for ch in channels:
        from_col, to_col = _CHANNEL_COLUMN_SUFFIX[ch]
        needed_cols.extend([from_col, to_col])
    # Keep only columns that actually exist in the frame.
    frame = markets[[c for c in needed_cols if c in markets.columns]].copy()
    if status_filter is not None:
        frame = frame[frame["status"] == status_filter]
    if slug_filter is not None:
        frame = frame[frame["slug"].isin(slug_filter)]
    if "slug" in frame.columns:
        frame = frame[frame["slug"].notna()]
        frame = frame[frame["slug"].astype(str) != ""]
    frame = frame.copy()

    # Vectorized date parsing: convert all date columns to normalized timestamps
    # upfront so the per-row loop does zero datetime parsing (the main bottleneck).
    window_start_ts = pd.Timestamp(window_start, tz=UTC) if window_start is not None else None
    window_end_ts = pd.Timestamp(window_end, tz=UTC) if window_end is not None else None
    del show_progress
    for ch in channels:
        from_col, to_col = _CHANNEL_COLUMN_SUFFIX[ch]
        for col in (from_col, to_col):
            if col in frame.columns:
                frame[col] = pd.to_datetime(
                    frame[col], utc=True, errors="coerce", format="mixed"
                ).dt.normalize()
        if from_col in frame.columns and to_col in frame.columns:
            if window_start_ts is not None:
                clip_start = frame[from_col].notna() & (frame[from_col] < window_start_ts)
                frame.loc[clip_start, from_col] = window_start_ts
            if window_end_ts is not None:
                clip_end = frame[to_col].notna() & (frame[to_col] > window_end_ts)
                frame.loc[clip_end, to_col] = window_end_ts

    # Pre-compute column indexes for itertuples (namedtuple attr positions).
    # itertuples()[0] is the Index; column values start at [1].
    col_index = {col: i + 1 for i, col in enumerate(frame.columns)}
    slug_idx = col_index.get("slug")
    channel_col_idxs: list[tuple[str, int, int]] = []
    for ch in channels:
        from_col, to_col = _CHANNEL_COLUMN_SUFFIX[ch]
        from_idx = col_index.get(from_col)
        to_idx = col_index.get(to_col)
        if from_idx is not None and to_idx is not None:
            channel_col_idxs.append((ch, from_idx, to_idx))
    if slug_idx is None:
        raise ValueError("Telonex markets catalog is missing required 'slug' column.")

    total_jobs = 0
    for ch, _from_idx, _to_idx in channel_col_idxs:
        from_col, to_col = _CHANNEL_COLUMN_SUFFIX[ch]
        valid = frame[from_col].notna() & frame[to_col].notna() & (frame[from_col] <= frame[to_col])
        if not valid.any():
            continue
        day_counts = (frame.loc[valid, to_col] - frame.loc[valid, from_col]).dt.days + 1
        total_jobs += int(day_counts.sum()) * len(outcomes)

    plan = _CatalogJobIterable(
        frame=frame,
        channel_col_idxs=channel_col_idxs,
        slug_idx=slug_idx,
        outcomes=outcomes,
        markets_considered=len(frame),
        total_jobs=total_jobs,
    )
    return plan


def _build_jobs_from_explicit(
    *,
    channels: list[str],
    market_slugs: list[str],
    outcome: str | None,
    outcome_id: int | None,
    start: date,
    end: date,
) -> list[_Job]:
    outcome_segment = outcome if outcome is not None else str(outcome_id)
    days = _date_range(start, end)
    jobs: list[_Job] = []
    for slug in market_slugs:
        for channel in channels:
            for day in days:
                jobs.append(
                    _Job(
                        market_slug=slug,
                        outcome_segment=str(outcome_segment),
                        outcome_id=outcome_id,
                        outcome=outcome,
                        channel=channel,
                        day=day,
                    )
                )
    return jobs


class _FakeHTTPError(Exception):
    """Raised for non-success HTTP status after the shared client follows the
    302. Carries a `code` field so upstream logic can match on 404."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _FakeHTTPError):
        return exc.code in _TRANSIENT_HTTP_CODES
    if isinstance(exc, UrllibHTTPError):
        return exc.code in _TRANSIENT_HTTP_CODES
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _TRANSIENT_HTTP_CODES
    if isinstance(
        exc,
        (
            httpx.TransportError,  # covers ConnectError, ReadError, WriteError, PoolTimeout, etc.
            URLError,
            SocketTimeout,
            TimeoutError,
            ConnectionError,
        ),
    ):
        return True
    return False


def _resolve_parse_worker_count(value: int | None) -> int:
    if value is not None:
        return max(1, value)
    raw = os.getenv(TELONEX_PARSE_WORKERS_ENV)
    if raw is not None and raw.strip():
        try:
            return max(1, int(raw))
        except ValueError:
            return _DEFAULT_PARSE_WORKERS
    return _DEFAULT_PARSE_WORKERS


def _resolve_positive_int(value: int | None, *, env_name: str, default: int) -> int:
    if value is not None:
        return max(1, value)
    raw = os.getenv(env_name)
    if raw is not None and raw.strip():
        try:
            return max(1, int(raw))
        except ValueError:
            return default
    return default


async def _download_day_bytes_with_retry_async(
    *,
    client: httpx.AsyncClient,
    timeout_secs: int,
    url: str,
    api_key: str,
    stop_event: asyncio.Event,
    progress_cb,
    max_retries: int,
    total_timeout_secs: float | None = None,
) -> bytes:
    """Fetch with retries. total_timeout_secs caps the entire attempt
    sequence (including backoff waits); None means no outer cap."""
    last_exc: BaseException | None = None
    deadline = time.monotonic() + total_timeout_secs if total_timeout_secs else None

    async def _attempt():
        nonlocal last_exc
        for attempt in range(max_retries):
            if stop_event.is_set():
                raise _CancelledError()
            if deadline is not None and time.monotonic() > deadline:
                raise asyncio.TimeoutError()
            try:
                return await _download_day_bytes_async(
                    client=client,
                    timeout_secs=timeout_secs,
                    url=url,
                    api_key=api_key,
                    stop_event=stop_event,
                    progress_cb=progress_cb,
                )
            except _CancelledError:
                raise
            except _FakeHTTPError as exc:
                if exc.code == 404:
                    raise
                last_exc = exc
                if not _is_transient(exc) or attempt == max_retries - 1:
                    raise
            except Exception as exc:
                last_exc = exc
                if not _is_transient(exc) or attempt == max_retries - 1:
                    raise
            backoff = min(
                _RETRY_BACKOFF_BASE_SECS * (2**attempt) + random.uniform(0, 0.5),
                30.0,
            )
            sleep_end = time.monotonic() + backoff
            if deadline is not None:
                sleep_end = min(sleep_end, deadline)
            while time.monotonic() < sleep_end:
                if stop_event.is_set():
                    raise _CancelledError()
                await asyncio.sleep(min(0.25, sleep_end - time.monotonic()))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("retry loop exited without success or exception")

    if total_timeout_secs is not None:
        try:
            return await asyncio.wait_for(_attempt(), timeout=total_timeout_secs)
        except asyncio.TimeoutError:
            raise _FakeHTTPError(408, f"total timeout ({total_timeout_secs:.0f}s)")
    return await _attempt()


async def _download_day_bytes_async(
    *,
    client: httpx.AsyncClient,
    timeout_secs: int,
    url: str,
    api_key: str,
    stop_event: asyncio.Event,
    progress_cb,
) -> bytes:
    """Fetch one day-file using the shared async client.

    The Telonex API endpoint responds with a 302 to an S3 presigned URL.
    `follow_redirects=True` collapses the redirect; httpx strips
    `Authorization` on cross-origin redirect so the token never leaks to S3.
    """
    del timeout_secs  # timeout is configured on the shared client.
    if stop_event.is_set():
        raise _CancelledError()
    headers = {"Authorization": f"Bearer {api_key}"}
    async with client.stream("GET", url, headers=headers) as response:
        if response.status_code == 404:
            raise _FakeHTTPError(404, "not found")
        if response.status_code >= 400:
            try:
                await response.aread()
            except Exception:
                pass
            raise _FakeHTTPError(response.status_code, f"HTTP {response.status_code}")
        total_header = response.headers.get("Content-Length")
        total_bytes = int(total_header) if total_header else None
        chunks: list[bytes] = []
        downloaded = 0
        progress_cb(0, total_bytes, False)
        async for chunk in response.aiter_bytes(chunk_size=_DOWNLOAD_CHUNK_SIZE):
            if stop_event.is_set():
                raise _CancelledError()
            if not chunk:
                continue
            chunks.append(chunk)
            downloaded += len(chunk)
            progress_cb(downloaded, total_bytes, False)
        progress_cb(downloaded, total_bytes, True)
    return b"".join(chunks)


@dataclass
class _ActiveDownload:
    job: _Job
    started_at: float
    downloaded_bytes: int
    total_bytes: int | None


class _ActiveRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: dict[int, _ActiveDownload] = {}
        self._counter = 0

    def start(self, job: _Job) -> int:
        with self._lock:
            self._counter += 1
            token = self._counter
            self._active[token] = _ActiveDownload(
                job=job,
                started_at=time.monotonic(),
                downloaded_bytes=0,
                total_bytes=None,
            )
            return token

    def update(self, token: int, downloaded: int, total: int | None) -> None:
        with self._lock:
            state = self._active.get(token)
            if state is None:
                return
            state.downloaded_bytes = downloaded
            if total is not None:
                state.total_bytes = total

    def finish(self, token: int) -> None:
        with self._lock:
            self._active.pop(token, None)

    def snapshot(self) -> list[_ActiveDownload]:
        with self._lock:
            return list(self._active.values())


def _postfix_text(
    *,
    downloaded_days: int,
    missing: int,
    failed: int,
    bytes_total: int,
    active: list[_ActiveDownload],
) -> str:
    now = time.monotonic()
    parts = [
        f"ok={downloaded_days}",
        f"miss={missing}",
        f"fail={failed}",
        _format_bytes(bytes_total),
    ]
    if active:
        shown = active[:2]
        detail = " | ".join(
            (
                f"{state.job.channel[:4]} {state.job.day:%m-%d} "
                f"{_format_bytes(state.downloaded_bytes)}"
                f"{('/' + _format_bytes(state.total_bytes)) if state.total_bytes else ''} "
                f"{now - state.started_at:4.1f}s"
            )
            for state in shown
        )
        overflow = f" +{len(active) - len(shown)} more" if len(active) > len(shown) else ""
        parts.append(f"active: {detail}{overflow}")
    return " ".join(parts)


def _prune_jobs_against_manifest(
    *,
    jobs: Iterable[_Job],
    store: _TelonexParquetStore,
    overwrite: bool,
    show_progress: bool,
    channels_hint: set[str] | None = None,
    recheck_empty_after_days: int | None = _DEFAULT_EMPTY_RECHECK_AFTER_DAYS,
) -> tuple[Iterator[_Job], list[int]]:
    """Filter out completed/empty days, yielding kept jobs lazily.

    Accepts an iterable (including a generator) so the upstream catalog
    jobs are never fully materialized. Returns a filtered job iterator
    plus a mutable skipped counter, which is final after the iterator is consumed.
    ``channels_hint`` pre-loads manifest keys without iterating jobs.
    """
    if overwrite:
        return iter(jobs), [0]

    completed_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    empty_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    channel_set = channels_hint or set()
    if show_progress and channel_set:
        print(
            f"[telonex] Loading resume manifest for {len(channel_set):,} channel(s)...",
            file=sys.stderr,
        )
    for channel in channel_set:
        completed_by_channel[channel] = store.completed_keys(channel)
        empty_by_channel[channel] = store.empty_keys(
            channel, recheck_after_days=recheck_empty_after_days
        )
    if show_progress and channel_set:
        completed = sum(len(keys) for keys in completed_by_channel.values())
        empty = sum(len(keys) for keys in empty_by_channel.values())
        print(
            f"[telonex] Resume manifest loaded: completed={completed:,} 404s={empty:,}.",
            file=sys.stderr,
        )

    skipped = [0]  # mutable so nested generator can update

    def _filtered() -> Iterator[_Job]:
        for job in jobs:
            key = (job.market_slug, job.outcome_segment, job.day)
            # Lazy-load channel keys on first encounter if not pre-loaded.
            if job.channel not in completed_by_channel:
                completed_by_channel[job.channel] = store.completed_keys(job.channel)
                empty_by_channel[job.channel] = store.empty_keys(
                    job.channel, recheck_after_days=recheck_empty_after_days
                )
            if key in completed_by_channel.get(job.channel, set()):
                skipped[0] += 1
                continue
            if key in empty_by_channel.get(job.channel, set()):
                skipped[0] += 1
                continue
            yield job

    return _filtered(), skipped  # read [0] after consuming


def _run_jobs(
    jobs: Iterable[_Job],
    *,
    store: _TelonexParquetStore,
    api_key: str,
    base_url: str,
    timeout_secs: int,
    workers: int,
    show_progress: bool,
    total_jobs: int | None = None,
    commit_batch_rows: int | None = None,
    commit_batch_secs: float | None = None,
    parse_workers: int | None = None,
    writer_queue_items: int | None = None,
    pending_commit_items: int | None = None,
) -> tuple[int, int, int, int, int, bool, list[str]]:
    # Resolve at call-time so monkeypatched module constants take effect in tests.
    if commit_batch_rows is None:
        commit_batch_rows = _DEFAULT_COMMIT_BATCH_ROWS
    if commit_batch_secs is None:
        commit_batch_secs = _DEFAULT_COMMIT_BATCH_SECS
    writer_queue_limit = _resolve_positive_int(
        writer_queue_items,
        env_name=TELONEX_WRITER_QUEUE_ITEMS_ENV,
        default=_MAX_PENDING_COMMIT_ITEMS,
    )
    pending_commit_limit = _resolve_positive_int(
        pending_commit_items,
        env_name=TELONEX_PENDING_COMMIT_ITEMS_ENV,
        default=_MAX_PENDING_COMMIT_ITEMS,
    )
    downloaded_days = 0
    missing_days = 0
    failed_days = 0
    cancelled_days = 0
    bytes_total = 0
    failed_samples: list[str] = []
    interrupted = False

    # `threading.Event` still drives the writer thread. A separate asyncio.Event
    # lets coroutines see stop requests via `await`.
    stop_event = threading.Event()
    active_registry = _ActiveRegistry()
    # Bounded so the writer applies backpressure on the async dispatcher: when
    # the writer falls behind, the put-side coroutine polls until a slot frees
    # up, which stops new jobs from being scheduled. Each queued result holds a
    # parsed Arrow table, so the queue must stay finite; the default is high
    # enough to avoid throttling normal high-throughput downloads.
    result_queue: Queue[_DownloadResult | _FlushWriterQueue] = Queue(maxsize=writer_queue_limit)

    # Dedicated bounded pool for CPU-bound parquet parsing. Using the default
    # asyncio executor (~40 threads) here creates heavy contention with the
    # writer thread and the event loop. Keeping this bounded also prevents
    # parsed Arrow tables from piling up faster than the writer can drain them.
    parse_worker_count = _resolve_parse_worker_count(parse_workers)
    parse_pool = ThreadPoolExecutor(
        max_workers=parse_worker_count, thread_name_prefix="telonex-parse"
    )
    # Semaphore limits in-flight parse tasks (including those queued in the
    # ThreadPoolExecutor's unbounded internal queue).  Without this, 128
    # concurrent downloads could all submit parse tasks at once, pinning
    # ~6 GiB of raw payloads in the executor queue for book_snapshot_full.
    parse_semaphore = asyncio.Semaphore(parse_worker_count * 4)

    progress = (
        tqdm(
            total=total_jobs,
            desc="Downloading Telonex days",
            unit="day",
            bar_format=(
                "{desc}: |{bar}| {percentage:.4f}% {n_fmt}/{total_fmt} day "
                "[{elapsed}<{remaining}] {postfix}"
            ),
            dynamic_ncols=True,
        )
        if show_progress and total_jobs != 0
        else None
    )

    state_lock = threading.Lock()
    last_postfix_ts = [0.0]
    writer_failed = threading.Event()
    async_stop: asyncio.Event | None = None
    interrupt_count = [0]

    def _refresh_postfix(force: bool = False) -> None:
        if progress is None:
            return
        now = time.monotonic()
        if not force and now - last_postfix_ts[0] < 0.2:
            return
        last_postfix_ts[0] = now
        snapshot = active_registry.snapshot()
        with state_lock:
            text = _postfix_text(
                downloaded_days=downloaded_days,
                missing=missing_days,
                failed=failed_days,
                bytes_total=bytes_total,
                active=snapshot,
            )
        progress.set_postfix_str(text, refresh=False)
        progress.refresh()

    heartbeat_stop = threading.Event()

    def _heartbeat() -> None:
        while not heartbeat_stop.wait(0.2):
            _refresh_postfix()

    heartbeat_thread = threading.Thread(target=_heartbeat, name="telonex-heartbeat", daemon=True)
    heartbeat_thread.start()

    # Test-monkeypatchable hook: tests patch module-level `_download_day_bytes`
    # to stub the network with a sync callable. When present, route through
    # the same retry/backoff logic used by the async network path.
    async def _call_stub_with_retry(stub, url: str, progress_cb) -> bytes:
        last_exc: BaseException | None = None
        for attempt in range(_DEFAULT_MAX_RETRIES):
            if async_stop.is_set():
                raise _CancelledError()
            try:
                result = stub(
                    timeout_secs=timeout_secs,
                    url=url,
                    api_key=api_key,
                    stop_event=stop_event,
                    progress_cb=progress_cb,
                )
                if asyncio.iscoroutine(result):
                    return await result
                return result
            except _CancelledError:
                raise
            except _FakeHTTPError as exc:
                if exc.code == 404:
                    raise
                last_exc = exc
                if not _is_transient(exc) or attempt == _DEFAULT_MAX_RETRIES - 1:
                    raise
            except Exception as exc:
                last_exc = exc
                if not _is_transient(exc) or attempt == _DEFAULT_MAX_RETRIES - 1:
                    raise
            backoff = _RETRY_BACKOFF_BASE_SECS * (2**attempt) + random.uniform(0, 0.5)
            deadline = time.monotonic() + backoff
            while time.monotonic() < deadline:
                if async_stop.is_set():
                    raise _CancelledError()
                await asyncio.sleep(min(0.25, deadline - time.monotonic()))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("retry loop exited without success or exception")

    async def _call_download(job: _Job, url: str, progress_cb, client: httpx.AsyncClient) -> bytes:
        stub = globals().get("_download_day_bytes")
        if stub is not None:
            return await _call_stub_with_retry(stub, url, progress_cb)
        return await _download_day_bytes_with_retry_async(
            client=client,
            timeout_secs=timeout_secs,
            url=url,
            api_key=api_key,
            stop_event=async_stop,
            progress_cb=progress_cb,
            max_retries=_DEFAULT_MAX_RETRIES,
            total_timeout_secs=float(timeout_secs * 3),
        )

    async def _do_one_async(job: _Job, client: httpx.AsyncClient) -> _DownloadResult:
        nonlocal missing_days, failed_days, cancelled_days, bytes_total, downloaded_days
        if async_stop.is_set():
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job,
                status="cancelled",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error=None,
            )

        token = active_registry.start(job)
        url = _api_url(
            base_url=base_url,
            channel=job.channel,
            market_slug=job.market_slug,
            outcome=job.outcome,
            outcome_id=job.outcome_id,
            day=job.day,
        )

        def _progress_cb(downloaded: int, total: int | None, finished: bool) -> None:
            active_registry.update(token, downloaded, total)

        payload: bytes | None = None
        try:
            payload = await _call_download(job, url, _progress_cb, client)
        except _CancelledError:
            active_registry.finish(token)
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job,
                status="cancelled",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error=None,
            )
        except asyncio.CancelledError:
            active_registry.finish(token)
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job,
                status="cancelled",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error=None,
            )
        except (_FakeHTTPError, UrllibHTTPError) as exc:
            active_registry.finish(token)
            if getattr(exc, "code", None) == 404:
                return _DownloadResult(
                    job=job,
                    status="missing",
                    table=None,
                    payload=None,
                    bytes_downloaded=0,
                    error="404",
                )
            code = getattr(exc, "code", "?")
            with state_lock:
                failed_days += 1
                if len(failed_samples) < 20:
                    failed_samples.append(f"{job.market_slug} {job.channel} {job.day} HTTP {code}")
            return _DownloadResult(
                job=job,
                status="failed",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error=f"HTTP {code}",
            )
        except Exception as exc:
            active_registry.finish(token)
            with state_lock:
                failed_days += 1
                if len(failed_samples) < 20:
                    failed_samples.append(
                        f"{job.market_slug} {job.channel} {job.day} {exc.__class__.__name__}"
                    )
            return _DownloadResult(
                job=job,
                status="failed",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error=str(exc),
            )

        active_registry.finish(token)
        if payload is None:
            with state_lock:
                failed_days += 1
            return _DownloadResult(
                job=job,
                status="failed",
                table=None,
                payload=None,
                bytes_downloaded=0,
                error="empty-body",
            )

        # Decode parquet on the dedicated small pool (see `parse_pool`). Keep
        # the result as Arrow so the writer can append columns and concatenate
        # without paying pandas materialization/conversion costs per day-file.
        # The semaphore bounds in-flight parse tasks so the executor's internal
        # queue doesn't pin unbounded raw-payload copies in RAM.
        loop = asyncio.get_running_loop()
        try:
            async with parse_semaphore:
                table = await loop.run_in_executor(parse_pool, pq.read_table, io.BytesIO(payload))
        except Exception as exc:
            with state_lock:
                failed_days += 1
                if len(failed_samples) < 20:
                    failed_samples.append(
                        f"{job.market_slug} {job.channel} {job.day} parquet-parse: {exc}"
                    )
            return _DownloadResult(
                job=job,
                status="failed",
                table=None,
                payload=None,
                bytes_downloaded=len(payload),
                error=str(exc),
            )

        bytes_dl = len(payload)
        del payload  # release raw bytes immediately; Result holds only Arrow table
        return _DownloadResult(
            job=job,
            status="ok",
            table=table,
            payload=None,
            bytes_downloaded=bytes_dl,
            error=None,
        )

    writer_done = threading.Event()
    pending_for_commit: list[_DownloadResult] = []
    last_commit_ts = time.monotonic()
    last_writer_drain_ts = time.monotonic()

    def _flush_pending(force: bool = False) -> None:
        nonlocal last_commit_ts, downloaded_days, missing_days, failed_days, bytes_total
        if not pending_for_commit:
            return
        total_pending_rows = sum(
            (
                entry.table.num_rows
                if entry.table is not None
                else 1
                if entry.status == "missing"
                else 0
            )
            for entry in pending_for_commit
        )
        if (
            not force
            and total_pending_rows < commit_batch_rows
            and time.monotonic() - last_commit_ts < commit_batch_secs
            and len(pending_for_commit) < pending_commit_limit
        ):
            return
        batch = pending_for_commit[:]
        pending_for_commit.clear()
        batch_days = len(batch)
        batch_ok = sum(1 for entry in batch if entry.status == "ok")
        batch_missing = sum(1 for entry in batch if entry.status == "missing")
        batch_bytes = sum(entry.bytes_downloaded for entry in batch if entry.status == "ok")
        try:
            store.ingest_batch(batch)
        except Exception as exc:  # noqa: BLE001
            # Do not count fetched bytes as durable results until the writer has
            # committed the manifest path. A writer failure leaves these days
            # retryable on the next run, so stop scheduling new work and report
            # the batch as failed instead of silently losing it behind a
            # successful summary.
            sample = batch[:3]
            sample_text = ", ".join(
                f"{r.job.channel}/{r.job.market_slug}/{r.job.day}" for r in sample
            )
            with state_lock:
                failed_days += batch_days
                if len(failed_samples) < 20:
                    failed_samples.append(
                        f"writer commit failed for {batch_days} day(s): "
                        f"{exc.__class__.__name__}: {exc}"
                    )
            for result in batch:
                result.table = None
            print(
                f"[telonex] writer: failed to commit batch of {batch_days} day(s) "
                f"({exc.__class__.__name__}: {exc}) — stopping; retry these days "
                f"on the next run. "
                f"Sample: {sample_text}",
                file=sys.stderr,
            )
            writer_failed.set()
            stop_event.set()
            if async_stop is not None:
                async_stop.set()
            last_commit_ts = time.monotonic()
            return

        with state_lock:
            downloaded_days += batch_ok
            missing_days += batch_missing
            bytes_total += batch_bytes
        last_commit_ts = time.monotonic()
        # Clear table refs from the batch so Arrow memory can be reclaimed
        # by the gc + Arrow pool release below.
        for result in batch:
            result.table = None
        del batch
        gc.collect()
        # Return freed Arrow memory to the OS allocator after each commit
        # cycle. Without this, the Arrow memory pool retains freed buffers
        # internally and RSS never drops even though the objects are gone.
        _release_arrow_memory()

    def _writer() -> None:
        nonlocal failed_days
        while not (writer_done.is_set() and result_queue.empty()) and not writer_failed.is_set():
            try:
                item = result_queue.get(timeout=0.25)
            except Empty:
                _flush_pending(force=True)
                continue
            try:
                if isinstance(item, _FlushWriterQueue):
                    try:
                        (
                            item.open_parts_before,
                            item.pending_part_days_before,
                        ) = store.open_part_stats()
                        _flush_pending(force=True)
                        if item.close_parts:
                            item.closed_parts = store.flush_all()
                        (
                            item.open_parts_after,
                            item.pending_part_days_after,
                        ) = store.open_part_stats()
                    except Exception as exc:  # noqa: BLE001
                        failed_count = max(
                            1,
                            item.pending_part_days_before,
                            item.pending_part_days_after,
                        )
                        with state_lock:
                            failed_days += failed_count
                            if len(failed_samples) < 20:
                                failed_samples.append(
                                    "writer forced drain failed for "
                                    f"{failed_count} pending day(s): "
                                    f"{exc.__class__.__name__}: {exc}"
                                )
                        print(
                            "[telonex] writer: failed to drain open part writers "
                            f"({exc.__class__.__name__}: {exc}) - stopping; "
                            "uncommitted days will retry on the next run.",
                            file=sys.stderr,
                        )
                        writer_failed.set()
                        stop_event.set()
                        if async_stop is not None:
                            async_stop.set()
                    finally:
                        item.ack.set()
                    continue
                result = item
                if result.status == "ok":
                    # The result already carries a parsed Arrow table (parsing ran
                    # in the dedicated parse pool on the async side, see
                    # `_do_one_async`). Writer just appends and periodically flushes.
                    pending_for_commit.append(result)
                    _flush_pending(force=False)
                elif result.status == "missing":
                    pending_for_commit.append(result)
                    _flush_pending(force=False)
                if progress is not None:
                    progress.update(1)
                    _refresh_postfix(force=True)
            finally:
                result_queue.task_done()
        _flush_pending(force=True)

    writer_thread = threading.Thread(target=_writer, name="telonex-writer", daemon=True)
    writer_thread.start()

    # --- async dispatch ---
    # `workers` is the concurrency ceiling: that many downloads are in flight
    # simultaneously. A coroutine waiting on a 302 is a few hundred bytes, not
    # an OS thread — so we can set this to thousands on a fast network and the
    # only real cost is the connection pool + open sockets.
    concurrency = max(1, workers)

    async def _dispatcher() -> None:
        nonlocal async_stop
        async_stop = asyncio.Event()

        # Wire signals → async_stop so Ctrl-C / SIGTERM let in-flight requests
        # drain cleanly. `asyncio.run` swallows the SIGINT KeyboardInterrupt
        # otherwise, leaving partial downloads in flight.
        loop = asyncio.get_running_loop()
        installed: list[int] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _async_stop_signal)
                installed.append(sig)
            except (NotImplementedError, RuntimeError):
                # add_signal_handler is main-thread only and unavailable on Windows
                pass

        def _async_stop_signal_local() -> None:
            pass  # placeholder so closure uses outer scope

        try:
            max_connections = max(1, concurrency * 2)
            max_keepalive_connections = max(1, concurrency)
            async with httpx.AsyncClient(
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive_connections,
                    keepalive_expiry=_HTTP_KEEPALIVE_EXPIRY_SECS,
                ),
                timeout=httpx.Timeout(
                    connect=min(30.0, float(timeout_secs)),
                    read=float(timeout_secs),
                    write=float(timeout_secs),
                    pool=float(max(timeout_secs * 2, 60)),
                ),
                headers={"User-Agent": _USER_AGENT},
            ) as client:
                job_iter = iter(jobs)
                in_flight: set[asyncio.Task] = set()
                # Prime: launch up to `concurrency` tasks at once. The semaphore
                # lives inside each task (via async_stop check) — we don't need
                # one because we directly cap the set size.
                for _ in range(concurrency):
                    try:
                        j = next(job_iter)
                    except StopIteration:
                        break
                    in_flight.add(asyncio.create_task(_do_one_async(j, client)))

                async def _handoff_item(item: _DownloadResult | _FlushWriterQueue) -> None:
                    # Cooperative put: poll the bounded queue so a slow writer
                    # throttles the dispatcher without starving the event loop or
                    # blocking a thread that couldn't be interrupted on SIGTERM.
                    while True:
                        if writer_failed.is_set():
                            return
                        try:
                            result_queue.put_nowait(item)
                            return
                        except Full:
                            await asyncio.sleep(0.05)

                async def _drain_writer_queue(reason: str) -> None:
                    if writer_failed.is_set():
                        return
                    flush = _FlushWriterQueue(
                        reason=reason,
                        close_parts=reason == "hourly",
                    )
                    await _handoff_item(flush)
                    while not flush.ack.is_set() and not writer_failed.is_set():
                        await asyncio.sleep(0.05)
                    gc.collect()
                    _release_arrow_memory()
                    if reason == "hourly":
                        rss = _get_rss_mb()
                        rss_text = f"{rss:.1f} MiB" if rss is not None else "unknown"
                        arrow_mb = _get_arrow_allocated_mb()
                        arrow_text = f"{arrow_mb:.1f} MiB" if arrow_mb is not None else "unknown"
                        print(
                            "[telonex] writer queue drained "
                            f"(reason=hourly, queued={result_queue.qsize()}, "
                            f"closed_parts={flush.closed_parts}, "
                            f"open_parts={flush.open_parts_before}->{flush.open_parts_after}, "
                            "pending_part_days="
                            f"{flush.pending_part_days_before}->{flush.pending_part_days_after}, "
                            f"rss={rss_text}, arrow_pool={arrow_text})",
                            file=sys.stderr,
                        )

                async def _handoff(result: _DownloadResult) -> None:
                    nonlocal last_writer_drain_ts
                    await _handoff_item(result)
                    now = time.monotonic()
                    if now - last_writer_drain_ts >= _MEMORY_LOG_INTERVAL_SECS:
                        await _drain_writer_queue("hourly")
                        last_writer_drain_ts = time.monotonic()

                while in_flight:
                    if async_stop.is_set() or writer_failed.is_set():
                        break
                    done, in_flight = await asyncio.wait(
                        in_flight, timeout=1.0, return_when=asyncio.FIRST_COMPLETED
                    )
                    for finished in done:
                        try:
                            result = finished.result()
                        except asyncio.CancelledError:
                            continue
                        except Exception as exc:
                            print(f"[telonex] worker raised {exc!r}", file=sys.stderr)
                            continue
                        await _handoff(result)
                    if not async_stop.is_set():
                        for _ in range(len(done)):
                            try:
                                j = next(job_iter)
                            except StopIteration:
                                break
                            in_flight.add(asyncio.create_task(_do_one_async(j, client)))

                # Drain remaining in-flight on stop.
                if in_flight:
                    for task in in_flight:
                        task.cancel()
                    try:
                        results = await asyncio.wait_for(
                            asyncio.gather(*in_flight, return_exceptions=True),
                            timeout=15.0,
                        )
                    except asyncio.TimeoutError:
                        stranded = sum(1 for t in in_flight if not t.done())
                        print(
                            f"[telonex] {stranded} task(s) still in flight after 15s drain "
                            "— forcing close",
                            file=sys.stderr,
                        )
                    else:
                        for r in results:
                            if isinstance(r, _DownloadResult):
                                await _handoff(r)
        finally:
            for sig in installed:
                try:
                    loop.remove_signal_handler(sig)
                except (NotImplementedError, RuntimeError):
                    pass
        # The shared async client closes itself via the async context manager.

    def _async_stop_signal() -> None:
        # Runs in the event loop thread. Flip both the threading event (writer,
        # retry loops still inspect it) and the asyncio event (coroutines).
        nonlocal interrupted
        interrupt_count[0] += 1
        if interrupt_count[0] >= _FORCE_EXIT_SIGNAL_COUNT:
            print(
                "\n[telonex] Force-exiting after 5 interrupt signals. Pending uncommitted "
                "downloads will retry on the next run.",
                file=sys.stderr,
                flush=True,
            )
            os._exit(130)
        if not stop_event.is_set():
            interrupted = True
            remaining = _FORCE_EXIT_SIGNAL_COUNT - interrupt_count[0]
            print(
                "\n[telonex] Signal received — draining in-flight downloads then "
                f"flushing pending rows. Send {remaining} more interrupt signal(s) "
                "to force-exit.",
                file=sys.stderr,
            )
        else:
            remaining = _FORCE_EXIT_SIGNAL_COUNT - interrupt_count[0]
            print(
                "\n[telonex] Still draining/flushing. "
                f"Send {remaining} more interrupt signal(s) to force-exit.",
                file=sys.stderr,
            )
        stop_event.set()
        if async_stop is not None:
            async_stop.set()

    try:
        try:
            asyncio.run(_dispatcher())
        except KeyboardInterrupt:
            interrupted = True
            stop_event.set()
    finally:
        writer_done.set()
        writer_thread.join(timeout=60.0)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=2.0)
        parse_pool.shutdown(wait=True, cancel_futures=True)
        if progress is not None:
            _refresh_postfix(force=True)
            progress.close()

    return (
        downloaded_days,
        missing_days,
        failed_days,
        cancelled_days,
        bytes_total,
        interrupted,
        failed_samples,
    )


def download_telonex_days(
    *,
    destination: Path,
    market_slugs: list[str] | None = None,
    outcome: str | None = None,
    outcome_id: int | None = None,
    channel: str | None = None,
    channels: list[str] | None = None,
    base_url: str = _DEFAULT_API_BASE_URL,
    start_date: str | None = None,
    end_date: str | None = None,
    all_markets: bool = False,
    status_filter: str | None = None,
    outcomes_for_all: list[int] | None = None,
    overwrite: bool = False,
    timeout_secs: int = 60,
    workers: int = 16,
    show_progress: bool = True,
    db_filename: str = _MANIFEST_FILENAME,
    recheck_empty_after_days: int | None = _DEFAULT_EMPTY_RECHECK_AFTER_DAYS,
    parse_workers: int | None = None,
    writer_queue_items: int | None = None,
    pending_commit_items: int | None = None,
    max_days: int | None = None,
) -> TelonexDownloadSummary:
    if channel is not None and channels is None:
        channels = [channel]
    if channels is None or not channels:
        channels = [_DEFAULT_CHANNEL]
    for ch in channels:
        if ch not in VALID_CHANNELS:
            raise ValueError(f"Unsupported channel {ch!r}. Valid: {', '.join(VALID_CHANNELS)}")

    api_key = os.getenv(TELONEX_API_KEY_ENV)
    if api_key is None or not api_key.strip():
        raise ValueError(
            f"{TELONEX_API_KEY_ENV} must be set in the environment to download Telonex files."
        )
    api_key = api_key.strip()

    # Per-request AsyncClient at high concurrency opens many sockets;
    # raise FD ceiling early so parquet writes don't hit EMFILE.
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = max(hard, 65536)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(target, hard), hard))
    except (ValueError, OSError, ImportError):
        pass

    normalized_destination = destination.expanduser().resolve()
    normalized_destination.mkdir(parents=True, exist_ok=True)
    store = _TelonexParquetStore(normalized_destination, manifest_name=db_filename)
    db_path = store.manifest_path
    writer_queue_limit = _resolve_positive_int(
        writer_queue_items,
        env_name=TELONEX_WRITER_QUEUE_ITEMS_ENV,
        default=_MAX_PENDING_COMMIT_ITEMS,
    )
    pending_commit_limit = _resolve_positive_int(
        pending_commit_items,
        env_name=TELONEX_PENDING_COMMIT_ITEMS_ENV,
        default=_MAX_PENDING_COMMIT_ITEMS,
    )

    window_start = _parse_date_bound(start_date)
    window_end = _parse_date_bound(end_date)
    if max_days is not None and max_days < 1:
        raise ValueError("--max-days must be >= 1 when provided.")

    # Route SIGTERM (from `timeout`, scheduler kills, etc.) through the same
    # KeyboardInterrupt path Ctrl-C already uses, so store.close() in the
    # `finally` gets to flush open Parquet writers instead of leaving orphans.
    # Nested/pre-existing handlers are preserved and restored.
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def _sigterm_as_interrupt(signum, frame):  # type: ignore[no-untyped-def]
        del signum, frame
        raise KeyboardInterrupt("SIGTERM")

    try:
        signal.signal(signal.SIGTERM, _sigterm_as_interrupt)
    except (ValueError, OSError):
        # Not in main thread — skip. The finally path still runs on Ctrl-C.
        previous_sigterm_handler = None

    markets_considered = 0
    try:
        if all_markets:
            if show_progress:
                print(f"Fetching markets dataset from {base_url.rstrip('/')}...", file=sys.stderr)
            markets = _fetch_markets_dataset(
                base_url, timeout_secs=max(30, timeout_secs), show_progress=show_progress
            )
            slug_filter = set(market_slugs) if market_slugs else None
            outcomes = outcomes_for_all or [0, 1]
            catalog_jobs = _iter_jobs_from_catalog(
                markets=markets,
                channels=list(channels),
                outcomes=outcomes,
                window_start=window_start,
                window_end=window_end,
                status_filter=status_filter,
                slug_filter=slug_filter,
                show_progress=show_progress,
            )
            jobs_iter = catalog_jobs
            markets_considered = catalog_jobs.markets_considered
            planned_jobs = catalog_jobs.total_jobs
            del markets  # free the full catalog; job iterable holds only the slim frame
        else:
            if not market_slugs:
                raise ValueError("Either --all-markets or --market-slug is required.")
            if outcome is None and outcome_id is None:
                raise ValueError("Provide --outcome or --outcome-id when not using --all-markets.")
            if outcome is not None and outcome_id is not None:
                raise ValueError("Provide only one of --outcome or --outcome-id.")
            if window_start is None or window_end is None:
                raise ValueError(
                    "--start-date and --end-date are required when not using --all-markets."
                )
            if window_start > window_end:
                raise ValueError(
                    f"Empty window: start_date {start_date!r} is after end_date {end_date!r}."
                )
            jobs_iter = _build_jobs_from_explicit(
                channels=list(channels),
                market_slugs=market_slugs,
                outcome=outcome,
                outcome_id=outcome_id,
                start=window_start,
                end=window_end,
            )
            markets_considered = len(set(market_slugs))
            planned_jobs = len(jobs_iter)

        # Prune against manifest.  _skipped_ref[0] is accurate only after
        # _run_jobs consumes the iterator chain, so we defer those reads.
        jobs_iter, _skipped_ref = _prune_jobs_against_manifest(
            jobs=jobs_iter,
            store=store,
            overwrite=overwrite,
            show_progress=show_progress,
            channels_hint=set(channels),
            recheck_empty_after_days=recheck_empty_after_days,
        )
        _skipped: int | None = None
        remaining_jobs = planned_jobs
        if max_days is not None:
            jobs_iter = islice(jobs_iter, max_days)
            if remaining_jobs is not None:
                remaining_jobs = min(remaining_jobs, max_days)
        if not all_markets:
            explicit_jobs = list(jobs_iter)
            jobs_iter = explicit_jobs
            _skipped = _skipped_ref[0]
            remaining_jobs = len(explicit_jobs)

        if show_progress:
            existing_store_size = store.size_bytes()
            completed_before = sum(len(store.completed_keys(ch)) for ch in channels)
            empty_before = sum(
                len(store.empty_keys(ch, recheck_after_days=recheck_empty_after_days))
                for ch in channels
            )
            if all_markets and planned_jobs is not None:
                # Exact all-market pruning is lazy so startup doesn't materialize
                # tens of millions of jobs. This estimate is accurate for the
                # normal full-catalog resume path and avoids printing the
                # unpruned total as "remaining".
                skipped_before = min(planned_jobs, completed_before + empty_before)
                remaining_jobs = max(0, planned_jobs - skipped_before)
                if max_days is not None:
                    remaining_jobs = min(remaining_jobs, max_days)
                remaining_text = f"estimated_remaining_after_resume={remaining_jobs:,}. "
            else:
                remaining_text = (
                    f"remaining={remaining_jobs:,}. " if remaining_jobs is not None else ""
                )
            print(
                f"[telonex] Resume summary: manifest={db_path} "
                f"data={store.data_root} total={_format_bytes(existing_store_size)}, "
                f"completed={completed_before:,} 404s={empty_before:,}, "
                f"planned={planned_jobs if planned_jobs is not None else 'streaming'}. "
                f"{remaining_text}"
                f"{f'max-days={max_days:,}. ' if max_days is not None else ''}"
                "Ctrl-C once to stop gracefully (manifest + parquets stay consistent); "
                "five interrupt signals force-exit.",
                file=sys.stderr,
            )
            print(
                f"[telonex] Channels={channels} workers={workers} "
                f"retries={_DEFAULT_MAX_RETRIES} timeout={timeout_secs}s "
                f"writer-queue={writer_queue_limit:,} "
                f"pending-commit={pending_commit_limit:,} "
                f"forced-drain={_MEMORY_LOG_INTERVAL_SECS:.0f}s "
                f"part-roll-at={_format_bytes(_TARGET_PART_DISK_BYTES)} on disk "
                f"or {_format_bytes(_TARGET_PART_BYTES)} Arrow "
                f"or {_TARGET_PART_PENDING_DAYS:,} pending days.",
                file=sys.stderr,
            )

        (
            downloaded,
            missing,
            failed,
            cancelled,
            bytes_total,
            interrupted,
            failed_samples,
        ) = _run_jobs(
            jobs_iter,
            store=store,
            api_key=api_key,
            base_url=base_url,
            timeout_secs=max(1, timeout_secs),
            workers=max(1, workers),
            show_progress=show_progress,
            total_jobs=remaining_jobs,
            parse_workers=parse_workers,
            writer_queue_items=writer_queue_limit,
            pending_commit_items=pending_commit_limit,
        )

        # Deferred reads: mutable list counters are final now that
        # _run_jobs has consumed the iterator chain.
        if _skipped is None:
            _skipped = _skipped_ref[0]
    finally:
        try:
            store.close(progress_label="final store close" if show_progress else None)
        finally:
            if previous_sigterm_handler is not None:
                try:
                    signal.signal(signal.SIGTERM, previous_sigterm_handler)
                except (ValueError, OSError):
                    pass

    start_out = f"{window_start:%Y-%m-%d}" if window_start else None
    end_out = f"{window_end:%Y-%m-%d}" if window_end else None

    requested_days = planned_jobs if max_days is None else remaining_jobs

    return TelonexDownloadSummary(
        destination=str(normalized_destination),
        db_path=str(db_path),
        channels=list(channels),
        base_url=base_url.rstrip("/"),
        markets_considered=markets_considered,
        requested_days=requested_days
        if requested_days is not None
        else downloaded + missing + failed + cancelled + _skipped,
        downloaded_days=downloaded,
        skipped_existing_days=_skipped,
        missing_days=missing,
        failed_days=failed,
        cancelled_days=cancelled,
        bytes_downloaded=bytes_total,
        start_date=start_out,
        end_date=end_out,
        db_size_bytes=store.size_bytes(),
        interrupted=interrupted,
        failed_samples=failed_samples,
    )


__all__ = [
    "TelonexDownloadSummary",
    "VALID_CHANNELS",
    "download_telonex_days",
]
