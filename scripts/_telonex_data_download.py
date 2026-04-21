from __future__ import annotations

import asyncio
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

_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_API_BASE_URL = "https://api.telonex.io"
_DEFAULT_CHANNEL = "quotes"
_EXCHANGE = "polymarket"
_DOWNLOAD_CHUNK_SIZE = 256 * 1024
_MANIFEST_FILENAME = "telonex.duckdb"
_DATA_SUBDIR = "data"
_TARGET_PART_BYTES = 1 << 30  # 1 GiB uncompressed Arrow before rolling
_PARQUET_COMPRESSION = "zstd"
_PARQUET_COMPRESSION_LEVEL = 3
# Keep pending_for_commit small so book_snapshot_full DataFrames don't pin RAM
# for long. With streaming row-group writes a smaller flush threshold has
# negligible overhead — the parquet writer stays open across flushes and only
# rolls the part file when `_TARGET_PART_BYTES` is hit.
_DEFAULT_COMMIT_BATCH_ROWS = 50_000
_DEFAULT_COMMIT_BATCH_SECS = 2.0
_DEFAULT_MAX_RETRIES = 4
_RETRY_BACKOFF_BASE_SECS = 2.0
_TRANSIENT_HTTP_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})

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
class _DownloadResult:
    job: _Job
    status: str  # "ok", "skipped", "missing", "failed", "cancelled"
    # `frame` is populated by the writer thread after parsing `payload` — the
    # network path only fills `payload`. This keeps GIL-heavy parquet parsing
    # off the asyncio default executor (which fights ~40 threads plus all the
    # in-flight download coroutines) and collapses the per-in-flight memory
    # footprint to a single bytes buffer instead of bytes + parsed DataFrame.
    frame: pd.DataFrame | None
    payload: bytes | None
    bytes_downloaded: int
    error: str | None


class _CancelledError(Exception):
    pass


@dataclass
class _OpenPart:
    """A Parquet part-file that's open for appending row groups.

    Stays open across commit batches until it crosses `_TARGET_PART_BYTES`;
    only then do we close it and flush its manifest rows. Partial parts on
    crash are orphaned but never referenced from the manifest, so they're
    benign — the affected days re-download on the next run.
    """

    path: Path
    writer: pq.ParquetWriter
    schema: pa.Schema
    bytes_written: int
    pending: list[tuple[_DownloadResult, int]]  # (result, row_count) waiting for manifest commit


class _TelonexParquetStore:
    """Hive-partitioned Parquet store with a small DuckDB manifest.

    Layout::

        <root>/
          telonex.duckdb                           -- manifest only (MB-scale)
          data/
            channel=<channel>/year=<y>/month=<mm>/part-NNNNNN.parquet

    Writer rolls a new part file when the open part crosses `_TARGET_PART_BYTES`
    or when the incoming batch's schema doesn't match the open writer's schema
    (new columns appearing mid-stream). Readers query everything via
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

    def close(self) -> None:
        """Flush all open writers and close the manifest. Idempotent."""
        with self._lock:
            for key in list(self._writers.keys()):
                self._flush_open_part_locked(key)
            self._con.close()

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

    def empty_keys(self, channel: str) -> set[tuple[str, str, date]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT market_slug, outcome_segment, day FROM empty_days WHERE channel = ?",
                [channel],
            ).fetchall()
        return {(row[0], row[1], row[2]) for row in rows}

    def mark_empty(self, job: _Job, *, status: str) -> None:
        with self._lock:
            self._con.execute(
                "INSERT OR REPLACE INTO empty_days "
                "(channel, market_slug, outcome_segment, day, status) "
                "VALUES (?, ?, ?, ?, ?)",
                [job.channel, job.market_slug, job.outcome_segment, job.day, status],
            )

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
        """Concat every entry's frame for the partition into one Arrow table
        and write it as a single row group. `pd.concat` unifies divergent
        column sets (filling NaN for missing cols), so the partition keeps a
        single open writer and only rolls when the unified schema genuinely
        changes (new column appears) or the part crosses the byte target.
        Caller holds the lock.

        Prior iterations tried a writer-per-schema-fingerprint to avoid
        rolls — but pandas→arrow type inference splits identical logical
        schemas (int64 vs Int64, string vs large_string) into distinct fp
        buckets, so we ended up with hundreds of tiny files and high
        per-write metadata overhead. Unifying via pandas first gives one big
        write call per flush.
        """
        enriched_frames: list[pd.DataFrame] = []
        per_entry_rows: list[int] = []
        for entry in entries:
            assert entry.frame is not None
            enriched = entry.frame
            enriched["market_slug"] = entry.job.market_slug
            enriched["outcome_segment"] = entry.job.outcome_segment
            enriched_frames.append(enriched)
            per_entry_rows.append(len(enriched))
            # Drop the dataclass reference so the DataFrame can be GC'd when
            # the local `enriched_frames` list is cleared below.
            entry.frame = None

        combined = pd.concat(enriched_frames, ignore_index=True, copy=False)
        enriched_frames.clear()
        table = pa.Table.from_pandas(combined, preserve_index=False)
        del combined

        part = self._writers.get(key)
        if part is not None and not part.schema.equals(table.schema):
            # Schema changed mid-partition (new column, type promotion, etc.) —
            # close the current part and start a new one. `union_by_name=True`
            # on read lets the two files coexist.
            self._flush_open_part_locked(key)
            part = None

        if part is None:
            part = self._open_part(key, table.schema)
            self._writers[key] = part

        total_rows = table.num_rows
        part.writer.write_table(table)
        part.bytes_written += table.nbytes
        for entry, row_count in zip(entries, per_entry_rows):
            part.pending.append((entry, row_count))

        del table

        if part.bytes_written >= _TARGET_PART_BYTES:
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
        for result in results:
            if result.status != "ok":
                continue
            if result.frame is None or result.frame.empty:
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
        return total_rows

    def flush_all(self) -> None:
        """Close every open part writer, committing their pending manifest rows.
        Used at the end of a run so days aren't left in-memory."""
        with self._lock:
            for key in list(self._writers.keys()):
                self._flush_open_part_locked(key)

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


def _fetch_markets_dataset(base_url: str, timeout_secs: int) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/v1/datasets/polymarket/markets"
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=timeout_secs) as response:
        payload = response.read()
    return pd.read_parquet(io.BytesIO(payload))


def _build_async_http_client(*, concurrency: int, timeout_secs: int) -> httpx.AsyncClient:
    """Shared pooled async HTTP client.

    Sized so every in-flight request can hold a keepalive slot to both the
    Telonex API host and the redirected-to S3 host simultaneously. `follow_
    redirects=True` collapses the "302 from api.telonex.io → GET s3" into one
    logical `client.get()` call; httpx strips `Authorization` on cross-origin
    redirect automatically.
    """
    # Two host buckets (api + s3) × some slack for races.
    pool_ceiling = max(concurrency * 2 + 32, 128)
    limits = httpx.Limits(
        max_connections=pool_ceiling,
        max_keepalive_connections=pool_ceiling,
        keepalive_expiry=120.0,
    )
    timeout = httpx.Timeout(
        connect=min(30.0, float(timeout_secs)),
        read=float(timeout_secs),
        write=float(timeout_secs),
        # Pool acquire waits can be longer — large concurrency bursts briefly
        # queue before a slot frees up.
        pool=float(max(timeout_secs * 2, 60)),
    )
    return httpx.AsyncClient(
        http2=False,  # S3 speaks HTTP/1.1; h2 on API saves little
        follow_redirects=True,
        limits=limits,
        timeout=timeout,
        headers={"User-Agent": _USER_AGENT},
    )


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
) -> tuple[Iterator[_Job], list[int]]:
    """Yield jobs lazily from the markets catalog.

    Returns (job_iterator, markets_considered_ref) so the caller can report
    the count without materializing the full job list (~66M entries => ~12 GiB).
    Read considered_ref[0] after the iterator is consumed.

    Uses itertuples() instead of iterrows() for ~10-100x faster row iteration.
    Drops unused columns upfront so the 5+ GiB catalog shrinks before
    the generator holds a reference to the slim frame.
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

    rows_iter = frame.itertuples()
    if show_progress:
        rows_iter = tqdm(
            rows_iter,
            total=len(frame),
            desc="Planning Telonex jobs",
            unit="market",
            leave=False,
        )
    considered = [0]  # mutable so nested generator can update
    _slug_idx = slug_idx  # capture for closure

    def _generate() -> Iterator[_Job]:
        for row in rows_iter:
            if _slug_idx is not None:
                slug = row[_slug_idx]
            else:
                continue
            if not slug:
                continue
            considered[0] += 1
            slug_str = str(slug)
            for channel, from_idx, to_idx in channel_col_idxs:
                days = _iter_days_for_market_tuple(
                    row,
                    from_idx=from_idx,
                    to_idx=to_idx,
                    window_start=window_start,
                    window_end=window_end,
                )
                if not days:
                    continue
                for outcome_id in outcomes:
                    for day in days:
                        yield _Job(
                            market_slug=slug_str,
                            outcome_segment=str(outcome_id),
                            outcome_id=outcome_id,
                            outcome=None,
                            channel=channel,
                            day=day,
                        )

    return _generate(), considered  # read [0] after consuming


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


async def _download_day_bytes_with_retry_async(
    *,
    client: httpx.AsyncClient,
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
    url: str,
    api_key: str,
    stop_event: asyncio.Event,
    progress_cb,
) -> bytes:
    """Fetch one day-file via the shared pooled async client.

    The Telonex API endpoint responds with a 302 to an S3 presigned URL.
    `follow_redirects=True` on the client collapses this to one logical GET;
    httpx strips `Authorization` on cross-origin redirect so the token never
    leaks to S3."""
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
) -> tuple[Iterator[_Job], int]:
    """Filter out completed/empty days, yielding kept jobs lazily.

    Accepts an iterable (including a generator) so the upstream catalog
    jobs are never fully materialized.  Returns a concrete list of jobs
    that still need downloading, plus the count of skipped ones.
    ``channels_hint`` pre-loads manifest keys without iterating jobs.
    """
    if overwrite:
        return iter(jobs), 0

    completed_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    empty_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    channel_set = channels_hint or set()
    prune_bar = (
        tqdm(total=len(channel_set), desc="Loading manifest", unit="ch", leave=False)
        if show_progress and channel_set
        else None
    )
    for channel in channel_set:
        completed_by_channel[channel] = store.completed_keys(channel)
        empty_by_channel[channel] = store.empty_keys(channel)
        if prune_bar is not None:
            prune_bar.update(1)
    if prune_bar is not None:
        prune_bar.close()

    skipped = [0]  # mutable so nested generator can update

    def _filtered() -> Iterator[_Job]:
        iterator = jobs
        if show_progress:
            iterator = tqdm(jobs, desc="Filtering resumable jobs", unit="day", leave=False)
        for job in iterator:
            key = (job.market_slug, job.outcome_segment, job.day)
            # Lazy-load channel keys on first encounter if not pre-loaded.
            if job.channel not in completed_by_channel:
                completed_by_channel[job.channel] = store.completed_keys(job.channel)
                empty_by_channel[job.channel] = store.empty_keys(job.channel)
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
    commit_batch_rows: int | None = None,
    commit_batch_secs: float | None = None,
) -> tuple[int, int, int, int, int, bool, list[str]]:
    # Resolve at call-time so monkeypatched module constants take effect in tests.
    if commit_batch_rows is None:
        commit_batch_rows = _DEFAULT_COMMIT_BATCH_ROWS
    if commit_batch_secs is None:
        commit_batch_secs = _DEFAULT_COMMIT_BATCH_SECS
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
    # up, which stops new jobs from being scheduled. Kept small relative to
    # `workers` because each queued result holds a parsed DataFrame — for
    # book_snapshot_full those can be 50+ MiB, so 32 × 50 MiB ≈ 1.5 GiB is our
    # worst-case live memory from the queue alone.
    result_queue: Queue[_DownloadResult] = Queue(maxsize=32)

    # Dedicated bounded pool for CPU-bound parquet parsing. Using the default
    # asyncio executor (~40 threads) here creates heavy GIL contention with
    # the writer thread and the event loop, so we cap parse parallelism at a
    # small fraction of cpu_count. Also keeps post-parse DataFrames from piling
    # up faster than the writer can drain them.
    parse_worker_count = min(4, max(1, (os.cpu_count() or 2)))
    parse_pool = ThreadPoolExecutor(
        max_workers=parse_worker_count, thread_name_prefix="telonex-parse"
    )

    progress = (
        tqdm(
            total=0,
            desc="Downloading Telonex days",
            unit="day",
            bar_format="{l_bar}{bar}| [{elapsed}<{remaining}] {postfix}",
        )
        if show_progress
        else None
    )
    commit_bar = (
        tqdm(
            total=0,
            desc="Committing rows",
            unit="row",
            bar_format="{l_bar}{bar}| [{elapsed}] {n_fmt} rows ({postfix})",
            leave=False,
        )
        if show_progress
        else None
    )

    state_lock = threading.Lock()
    last_postfix_ts = [0.0]

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
                    client=http_client,
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

    async def _call_download(job: _Job, url: str, progress_cb) -> bytes:
        stub = globals().get("_download_day_bytes")
        if stub is not None:
            return await _call_stub_with_retry(stub, url, progress_cb)
        return await _download_day_bytes_with_retry_async(
            client=http_client,
            url=url,
            api_key=api_key,
            stop_event=async_stop,
            progress_cb=progress_cb,
            max_retries=_DEFAULT_MAX_RETRIES,
            total_timeout_secs=float(timeout_secs * 3),
        )

    async def _do_one_async(job: _Job) -> _DownloadResult:
        nonlocal missing_days, failed_days, cancelled_days, bytes_total, downloaded_days
        if async_stop.is_set():
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job,
                status="cancelled",
                frame=None,
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
            payload = await _call_download(job, url, _progress_cb)
        except _CancelledError:
            active_registry.finish(token)
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job,
                status="cancelled",
                frame=None,
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
                frame=None,
                payload=None,
                bytes_downloaded=0,
                error=None,
            )
        except (_FakeHTTPError, UrllibHTTPError) as exc:
            active_registry.finish(token)
            if getattr(exc, "code", None) == 404:
                store.mark_empty(job, status="404")
                with state_lock:
                    missing_days += 1
                return _DownloadResult(
                    job=job,
                    status="missing",
                    frame=None,
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
                frame=None,
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
                frame=None,
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
                frame=None,
                payload=None,
                bytes_downloaded=0,
                error="empty-body",
            )

        # Parse parquet on the dedicated small pool (see `parse_pool`). This
        # caps parse concurrency so the event loop and writer don't compete
        # with dozens of parse threads for the GIL, while still allowing a few
        # parses to run in parallel — letting the writer saturate its
        # single-thread disk-write budget.
        loop = asyncio.get_running_loop()
        try:
            frame = await loop.run_in_executor(parse_pool, pd.read_parquet, io.BytesIO(payload))
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
                frame=None,
                payload=None,
                bytes_downloaded=len(payload),
                error=str(exc),
            )

        with state_lock:
            downloaded_days += 1
            bytes_total += len(payload)
        return _DownloadResult(
            job=job,
            status="ok",
            frame=frame,
            payload=None,
            bytes_downloaded=len(payload),
            error=None,
        )

    writer_done = threading.Event()
    pending_for_commit: list[_DownloadResult] = []
    last_commit_ts = time.monotonic()

    def _flush_pending(force: bool = False) -> None:
        nonlocal last_commit_ts
        if not pending_for_commit:
            return
        total_pending_rows = sum(
            (len(entry.frame) if entry.frame is not None else 0) for entry in pending_for_commit
        )
        if (
            not force
            and total_pending_rows < commit_batch_rows
            and time.monotonic() - last_commit_ts < commit_batch_secs
        ):
            return
        batch = pending_for_commit[:]
        pending_for_commit.clear()
        try:
            inserted = store.ingest_batch(batch)
        except Exception as exc:  # noqa: BLE001
            # ingest_batch is atomic — failure means nothing was committed and
            # the completed_days rows were rolled back, so these days will be
            # retried on the next run. Log, drop the batch, keep the writer alive.
            sample = batch[:3]
            sample_text = ", ".join(
                f"{r.job.channel}/{r.job.market_slug}/{r.job.day}" for r in sample
            )
            print(
                f"[telonex] writer: dropped batch of {len(batch)} day(s) after ingest failure "
                f"({exc.__class__.__name__}: {exc}) — will retry on next run. "
                f"Sample: {sample_text}",
                file=sys.stderr,
            )
            last_commit_ts = time.monotonic()
            return
        last_commit_ts = time.monotonic()
        if commit_bar is not None:
            commit_bar.update(inserted)
            commit_bar.set_postfix_str(f"db={_format_bytes(store.size_bytes())}", refresh=False)
            commit_bar.refresh()

    def _writer() -> None:
        while not (writer_done.is_set() and result_queue.empty()):
            try:
                result = result_queue.get(timeout=0.25)
            except Empty:
                _flush_pending(force=False)
                continue
            if result.status == "ok":
                # The result already carries a parsed DataFrame (parsing ran
                # in the dedicated parse pool on the async side, see
                # `_do_one_async`). Writer just appends and periodically flushes.
                pending_for_commit.append(result)
                _flush_pending(force=False)
            elif result.status == "missing":
                _flush_pending(force=False)
            if progress is not None:
                progress.update(1)
                _refresh_postfix(force=True)
        _flush_pending(force=True)

    writer_thread = threading.Thread(target=_writer, name="telonex-writer", daemon=True)
    writer_thread.start()

    # --- async dispatch ---
    # `workers` is the concurrency ceiling: that many downloads are in flight
    # simultaneously. A coroutine waiting on a 302 is a few hundred bytes, not
    # an OS thread — so we can set this to thousands on a fast network and the
    # only real cost is the connection pool + open sockets.
    concurrency = max(1, workers)
    http_client: httpx.AsyncClient | None = None
    async_stop: asyncio.Event | None = None

    async def _dispatcher() -> None:
        nonlocal http_client, async_stop
        async_stop = asyncio.Event()
        http_client = _build_async_http_client(concurrency=concurrency, timeout_secs=timeout_secs)

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
                in_flight.add(asyncio.create_task(_do_one_async(j)))

            async def _handoff(result: _DownloadResult) -> None:
                # Cooperative put: poll the bounded queue so a slow writer
                # throttles the dispatcher without starving the event loop or
                # blocking a thread that couldn't be interrupted on SIGTERM.
                while True:
                    try:
                        result_queue.put_nowait(result)
                        return
                    except Full:
                        if async_stop.is_set():
                            return
                        await asyncio.sleep(0.05)

            while in_flight:
                if async_stop.is_set():
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
                        in_flight.add(asyncio.create_task(_do_one_async(j)))

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
            if http_client is not None:
                try:
                    await asyncio.wait_for(http_client.aclose(), timeout=10.0)
                except asyncio.TimeoutError:
                    print(
                        "[telonex] httpx client close timed out — connections abandoned",
                        file=sys.stderr,
                    )

    def _async_stop_signal() -> None:
        # Runs in the event loop thread. Flip both the threading event (writer,
        # retry loops still inspect it) and the asyncio event (coroutines).
        nonlocal interrupted
        if not stop_event.is_set():
            interrupted = True
            print(
                "\n[telonex] Signal received — draining in-flight downloads then "
                "flushing pending rows. Send again to force-exit.",
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
        if commit_bar is not None:
            commit_bar.close()

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

    normalized_destination = destination.expanduser().resolve()
    normalized_destination.mkdir(parents=True, exist_ok=True)
    store = _TelonexParquetStore(normalized_destination, manifest_name=db_filename)
    db_path = store.manifest_path

    window_start = _parse_date_bound(start_date)
    window_end = _parse_date_bound(end_date)

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
            markets = _fetch_markets_dataset(base_url, timeout_secs=max(30, timeout_secs))
            if show_progress:
                print(f"Loaded {len(markets):,} markets", file=sys.stderr)
            slug_filter = set(market_slugs) if market_slugs else None
            outcomes = outcomes_for_all or [0, 1]
            jobs_iter, _markets_considered_ref = _iter_jobs_from_catalog(
                markets=markets,
                channels=list(channels),
                outcomes=outcomes,
                window_start=window_start,
                window_end=window_end,
                status_filter=status_filter,
                slug_filter=slug_filter,
                show_progress=show_progress,
            )
            planned_jobs = None  # unknown until consumed
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
        )

        if show_progress:
            existing_store_size = store.size_bytes()
            completed_before = sum(len(store.completed_keys(ch)) for ch in channels)
            empty_before = sum(len(store.empty_keys(ch)) for ch in channels)
            print(
                f"[telonex] Resume summary: manifest={db_path} "
                f"data={store.data_root} total={_format_bytes(existing_store_size)}, "
                f"completed={completed_before:,} 404s={empty_before:,}, "
                f"planned={planned_jobs if planned_jobs is not None else 'streaming'}. "
                f"Ctrl-C once to stop gracefully (manifest + parquets stay consistent).",
                file=sys.stderr,
            )
            print(
                f"[telonex] Channels={channels} workers={workers} "
                f"retries={_DEFAULT_MAX_RETRIES} timeout={timeout_secs}s "
                f"part-roll-at={_format_bytes(_TARGET_PART_BYTES)}.",
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
        )

        # Deferred reads: mutable list counters are final now that
        # _run_jobs has consumed the iterator chain.
        _skipped = _skipped_ref[0]
        if all_markets:
            markets_considered = _markets_considered_ref[0]
    finally:
        try:
            store.close()
        finally:
            if previous_sigterm_handler is not None:
                try:
                    signal.signal(signal.SIGTERM, previous_sigterm_handler)
                except (ValueError, OSError):
                    pass

    start_out = f"{window_start:%Y-%m-%d}" if window_start else None
    end_out = f"{window_end:%Y-%m-%d}" if window_end else None

    return TelonexDownloadSummary(
        destination=str(normalized_destination),
        db_path=str(db_path),
        channels=list(channels),
        base_url=base_url.rstrip("/"),
        markets_considered=markets_considered,
        requested_days=planned_jobs
        if planned_jobs is not None
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
