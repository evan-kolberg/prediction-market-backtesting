from __future__ import annotations

import io
import os
import random
import sys
import threading
import time
import concurrent.futures
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from socket import timeout as SocketTimeout
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import duckdb
import pandas as pd
from tqdm.auto import tqdm

from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_API_KEY_ENV,
)

_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_API_BASE_URL = "https://api.telonex.io"
_DEFAULT_CHANNEL = "quotes"
_EXCHANGE = "polymarket"
_DOWNLOAD_CHUNK_SIZE = 64 * 1024
_BLOB_DB_FILENAME = "telonex.duckdb"
_DEFAULT_COMMIT_BATCH_ROWS = 250_000
_DEFAULT_COMMIT_BATCH_SECS = 5.0
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
    db_path: str
    channels: list[str]
    base_url: str
    markets_considered: int
    requested_days: int
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
    frame: pd.DataFrame | None
    bytes_downloaded: int
    error: str | None


class _CancelledError(Exception):
    pass


class _TelonexBlobStore:
    """Single-file DuckDB store for Telonex Polymarket daily payloads.

    Per-channel tables are created on first insert from the incoming
    DataFrame's schema. Everything flows through one DuckDB connection behind a
    lock — parquets never land on the filesystem.
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._lock = threading.Lock()
        self._con = duckdb.connect(str(db_path))
        self._init_schema()
        self._channel_tables_ready: set[str] = set()

    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        with self._lock:
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

    def _ensure_channel_table(self, channel: str, sample_frame: pd.DataFrame) -> None:
        table = f"{channel}_data"
        if channel in self._channel_tables_ready:
            return
        exists = self._con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()[0]
        if not exists:
            schema_source = sample_frame.head(1).copy()
            if schema_source.empty:
                self._channel_tables_ready.add(channel)
                return
            schema_source["market_slug"] = "_schema_"
            schema_source["outcome_segment"] = "_schema_"
            self._con.register("_ingest_schema", schema_source)
            try:
                self._con.execute(
                    f'CREATE TABLE "{table}" AS SELECT * FROM _ingest_schema WHERE 1=0'
                )
            finally:
                self._con.unregister("_ingest_schema")
            self._con.execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_key_idx" '
                f'ON "{table}" (market_slug, outcome_segment)'
            )
        self._channel_tables_ready.add(channel)

    def ingest_batch(self, results: list[_DownloadResult]) -> int:
        """Insert a batch of successful downloads atomically."""
        by_channel: dict[str, list[_DownloadResult]] = {}
        for result in results:
            if result.status != "ok" or result.frame is None or result.frame.empty:
                by_channel.setdefault(result.job.channel, []).append(result)
                continue
            by_channel.setdefault(result.job.channel, []).append(result)

        total_rows = 0
        with self._lock:
            self._con.execute("BEGIN TRANSACTION")
            try:
                for channel, entries in by_channel.items():
                    non_empty = [
                        entry
                        for entry in entries
                        if entry.frame is not None and not entry.frame.empty
                    ]
                    if non_empty:
                        self._ensure_channel_table(channel, non_empty[0].frame)
                        enriched_frames: list[pd.DataFrame] = []
                        for entry in non_empty:
                            assert entry.frame is not None
                            enriched = entry.frame.copy()
                            enriched["market_slug"] = entry.job.market_slug
                            enriched["outcome_segment"] = entry.job.outcome_segment
                            enriched_frames.append(enriched)
                        combined = pd.concat(enriched_frames, ignore_index=True, copy=False)
                        table = f"{channel}_data"
                        self._con.register("_ingest_rows", combined)
                        try:
                            self._con.execute(
                                f'INSERT INTO "{table}" BY NAME SELECT * FROM _ingest_rows'
                            )
                        finally:
                            self._con.unregister("_ingest_rows")
                        total_rows += len(combined)

                    for entry in entries:
                        rows = 0
                        if entry.frame is not None and not entry.frame.empty:
                            rows = len(entry.frame)
                        self._con.execute(
                            "INSERT OR REPLACE INTO completed_days "
                            "(channel, market_slug, outcome_segment, day, rows, bytes_downloaded) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            [
                                channel,
                                entry.job.market_slug,
                                entry.job.outcome_segment,
                                entry.job.day,
                                rows,
                                entry.bytes_downloaded,
                            ],
                        )
                self._con.execute("COMMIT")
            except Exception:
                self._con.execute("ROLLBACK")
                raise
        return total_rows

    def size_bytes(self) -> int:
        try:
            return self._path.stat().st_size
        except OSError:
            return 0


def _fetch_markets_dataset(base_url: str, timeout_secs: int) -> pd.DataFrame:
    url = f"{base_url.rstrip('/')}/v1/datasets/polymarket/markets"
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=timeout_secs) as response:
        payload = response.read()
    return pd.read_parquet(io.BytesIO(payload))


def _iter_days_for_market(
    row: pd.Series,
    *,
    channel: str,
    window_start: date | None,
    window_end: date | None,
) -> list[date]:
    from_col, to_col = _CHANNEL_COLUMN_SUFFIX[channel]
    raw_from = row.get(from_col)
    raw_to = row.get(to_col)
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


def _build_jobs_from_catalog(
    *,
    markets: pd.DataFrame,
    channels: list[str],
    outcomes: list[int],
    window_start: date | None,
    window_end: date | None,
    status_filter: str | None,
    slug_filter: set[str] | None,
    show_progress: bool,
) -> tuple[list[_Job], int]:
    jobs: list[_Job] = []
    considered = 0
    frame = markets
    if status_filter is not None:
        frame = frame[frame["status"] == status_filter]
    if slug_filter is not None:
        frame = frame[frame["slug"].isin(slug_filter)]
    rows = frame.iterrows()
    if show_progress:
        rows = tqdm(
            rows,
            total=len(frame),
            desc="Planning Telonex jobs",
            unit="market",
            leave=False,
        )
    for _index, row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        considered += 1
        for channel in channels:
            days = _iter_days_for_market(
                row, channel=channel, window_start=window_start, window_end=window_end
            )
            if not days:
                continue
            for outcome_id in outcomes:
                for day in days:
                    jobs.append(
                        _Job(
                            market_slug=str(slug),
                            outcome_segment=str(outcome_id),
                            outcome_id=outcome_id,
                            outcome=None,
                            channel=channel,
                            day=day,
                        )
                    )
    return jobs, considered


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


def _resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": _USER_AGENT,
        },
        method="GET",
    )

    class _NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
            return None

    opener = build_opener(_NoRedirect())
    try:
        response = opener.open(request, timeout=timeout_secs)
        response.close()
    except HTTPError as exc:
        if exc.code in (301, 302, 303, 307, 308):
            location = exc.headers.get("Location")
            if not location:
                raise
            return location
        raise
    raise HTTPError(url, 500, "Expected 302 redirect from Telonex", {}, None)  # type: ignore[arg-type]


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in _TRANSIENT_HTTP_CODES
    if isinstance(exc, (URLError, SocketTimeout, TimeoutError, ConnectionError)):
        return True
    return False


def _download_day_bytes_with_retry(
    *,
    url: str,
    api_key: str,
    timeout_secs: int,
    stop_event: threading.Event,
    progress_cb,
    max_retries: int,
) -> bytes:
    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        if stop_event.is_set():
            raise _CancelledError()
        try:
            return _download_day_bytes(
                url=url,
                api_key=api_key,
                timeout_secs=timeout_secs,
                stop_event=stop_event,
                progress_cb=progress_cb,
            )
        except _CancelledError:
            raise
        except HTTPError as exc:
            if exc.code == 404:
                raise
            last_exc = exc
            if not _is_transient(exc) or attempt == max_retries - 1:
                raise
        except Exception as exc:
            last_exc = exc
            if not _is_transient(exc) or attempt == max_retries - 1:
                raise
        backoff = _RETRY_BACKOFF_BASE_SECS * (2**attempt) + random.uniform(0, 0.5)
        deadline = time.monotonic() + backoff
        while time.monotonic() < deadline:
            if stop_event.is_set():
                raise _CancelledError()
            time.sleep(min(0.25, deadline - time.monotonic()))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry loop exited without success or exception")


def _download_day_bytes(
    *,
    url: str,
    api_key: str,
    timeout_secs: int,
    stop_event: threading.Event,
    progress_cb,
) -> bytes:
    presigned = _resolve_presigned_url(url=url, api_key=api_key, timeout_secs=timeout_secs)
    request = Request(presigned, headers={"User-Agent": _USER_AGENT})
    with urlopen(request, timeout=timeout_secs) as response:
        total_header = response.headers.get("Content-Length")
        total_bytes = int(total_header) if total_header else None
        chunks: list[bytes] = []
        downloaded = 0
        progress_cb(0, total_bytes, False)
        while True:
            if stop_event.is_set():
                raise _CancelledError()
            chunk = response.read(_DOWNLOAD_CHUNK_SIZE)
            if not chunk:
                break
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
    skipped: int,
    missing: int,
    failed: int,
    bytes_total: int,
    active: list[_ActiveDownload],
) -> str:
    now = time.monotonic()
    parts = [
        f"ok={downloaded_days}",
        f"skip={skipped}",
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
    jobs: list[_Job],
    store: _TelonexBlobStore,
    overwrite: bool,
    show_progress: bool,
) -> tuple[list[_Job], int]:
    if overwrite:
        return jobs, 0

    completed_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    empty_by_channel: dict[str, set[tuple[str, str, date]]] = {}
    channels = {job.channel for job in jobs}
    prune_bar = (
        tqdm(total=len(channels), desc="Loading manifest", unit="ch", leave=False)
        if show_progress
        else None
    )
    for channel in channels:
        completed_by_channel[channel] = store.completed_keys(channel)
        empty_by_channel[channel] = store.empty_keys(channel)
        if prune_bar is not None:
            prune_bar.update(1)
    if prune_bar is not None:
        prune_bar.close()

    pruned: list[_Job] = []
    skipped = 0
    iterator = jobs
    if show_progress:
        iterator = tqdm(jobs, desc="Filtering resumable jobs", unit="day", leave=False)
    for job in iterator:
        key = (job.market_slug, job.outcome_segment, job.day)
        if key in completed_by_channel.get(job.channel, set()):
            skipped += 1
            continue
        if key in empty_by_channel.get(job.channel, set()):
            skipped += 1
            continue
        pruned.append(job)
    return pruned, skipped


def _run_jobs(
    jobs: list[_Job],
    *,
    store: _TelonexBlobStore,
    api_key: str,
    base_url: str,
    timeout_secs: int,
    workers: int,
    show_progress: bool,
    commit_batch_rows: int = _DEFAULT_COMMIT_BATCH_ROWS,
    commit_batch_secs: float = _DEFAULT_COMMIT_BATCH_SECS,
) -> tuple[int, int, int, int, int, bool, list[str]]:
    downloaded_days = 0
    missing_days = 0
    failed_days = 0
    cancelled_days = 0
    bytes_total = 0
    failed_samples: list[str] = []
    interrupted = False

    stop_event = threading.Event()
    active_registry = _ActiveRegistry()
    result_queue: Queue[_DownloadResult] = Queue()

    progress = (
        tqdm(
            total=len(jobs),
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
                skipped=0,
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

    def _do_one(job: _Job) -> _DownloadResult:
        nonlocal missing_days, failed_days, cancelled_days, bytes_total, downloaded_days
        if stop_event.is_set():
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job, status="cancelled", frame=None, bytes_downloaded=0, error=None
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
            payload = _download_day_bytes_with_retry(
                url=url,
                api_key=api_key,
                timeout_secs=timeout_secs,
                stop_event=stop_event,
                progress_cb=_progress_cb,
                max_retries=_DEFAULT_MAX_RETRIES,
            )
        except _CancelledError:
            active_registry.finish(token)
            with state_lock:
                cancelled_days += 1
            return _DownloadResult(
                job=job, status="cancelled", frame=None, bytes_downloaded=0, error=None
            )
        except HTTPError as exc:
            active_registry.finish(token)
            if exc.code == 404:
                store.mark_empty(job, status="404")
                with state_lock:
                    missing_days += 1
                return _DownloadResult(
                    job=job, status="missing", frame=None, bytes_downloaded=0, error="404"
                )
            with state_lock:
                failed_days += 1
                if len(failed_samples) < 20:
                    failed_samples.append(
                        f"{job.market_slug} {job.channel} {job.day} HTTP {exc.code}"
                    )
            return _DownloadResult(
                job=job, status="failed", frame=None, bytes_downloaded=0, error=f"HTTP {exc.code}"
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
                job=job, status="failed", frame=None, bytes_downloaded=0, error=str(exc)
            )

        active_registry.finish(token)
        if payload is None:
            with state_lock:
                failed_days += 1
            return _DownloadResult(
                job=job, status="failed", frame=None, bytes_downloaded=0, error="empty-body"
            )

        try:
            frame = pd.read_parquet(io.BytesIO(payload))
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
                bytes_downloaded=len(payload),
                error=str(exc),
            )

        with state_lock:
            downloaded_days += 1
            bytes_total += len(payload)
        return _DownloadResult(
            job=job, status="ok", frame=frame, bytes_downloaded=len(payload), error=None
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
        inserted = store.ingest_batch(batch)
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
            if result.status in ("ok", "missing"):
                if result.status == "ok":
                    pending_for_commit.append(result)
                _flush_pending(force=False)
            if progress is not None:
                progress.update(1)
                _refresh_postfix(force=True)
        _flush_pending(force=True)

    writer_thread = threading.Thread(target=_writer, name="telonex-writer", daemon=True)
    writer_thread.start()

    try:
        if workers <= 1:
            try:
                for job in jobs:
                    if stop_event.is_set():
                        break
                    result_queue.put(_do_one(job))
            except KeyboardInterrupt:
                interrupted = True
                stop_event.set()
                print(
                    "\n[telonex] Ctrl-C received — flushing pending rows to the DuckDB blob. "
                    "Press Ctrl-C again to force-exit (risk losing in-flight day).",
                    file=sys.stderr,
                )
        else:
            # Bounded-producer pattern: keep at most `workers * 3` futures in flight
            # so memory stays flat even when `jobs` has tens of millions of entries.
            in_flight_limit = max(workers * 3, workers + 2)
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="telonex-dl") as pool:
                job_iter = iter(jobs)
                in_flight: set = set()
                try:
                    for _ in range(in_flight_limit):
                        try:
                            job = next(job_iter)
                        except StopIteration:
                            break
                        in_flight.add(pool.submit(_do_one, job))
                    while in_flight:
                        if stop_event.is_set():
                            break
                        done, in_flight = concurrent.futures.wait(
                            in_flight, timeout=1.0, return_when=FIRST_COMPLETED
                        )
                        for finished in done:
                            try:
                                result_queue.put(finished.result())
                            except Exception as exc:  # noqa: BLE001
                                print(f"[telonex] worker raised {exc!r}", file=sys.stderr)
                        if not stop_event.is_set():
                            for _ in range(len(done)):
                                try:
                                    job = next(job_iter)
                                except StopIteration:
                                    break
                                in_flight.add(pool.submit(_do_one, job))
                except KeyboardInterrupt:
                    interrupted = True
                    stop_event.set()
                    print(
                        "\n[telonex] Ctrl-C received — finishing in-flight downloads then "
                        "flushing pending rows to the DuckDB blob. Press Ctrl-C again to "
                        "force-exit (risk losing in-flight day).",
                        file=sys.stderr,
                    )
                    for future in in_flight:
                        future.cancel()
    finally:
        writer_done.set()
        writer_thread.join(timeout=60.0)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=2.0)
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
    db_filename: str = _BLOB_DB_FILENAME,
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
    db_path = normalized_destination / db_filename
    store = _TelonexBlobStore(db_path)

    window_start = _parse_date_bound(start_date)
    window_end = _parse_date_bound(end_date)

    markets_considered = 0
    try:
        if all_markets:
            if show_progress:
                fetch_bar = tqdm(
                    total=1, desc="Fetching Telonex markets dataset", unit="ds", leave=False
                )
                print(f"Fetching markets dataset from {base_url.rstrip('/')}...", file=sys.stderr)
            markets = _fetch_markets_dataset(base_url, timeout_secs=max(30, timeout_secs))
            if show_progress:
                fetch_bar.update(1)
                fetch_bar.close()
                print(f"Loaded {len(markets):,} markets", file=sys.stderr)
            slug_filter = set(market_slugs) if market_slugs else None
            outcomes = outcomes_for_all or [0, 1]
            jobs, markets_considered = _build_jobs_from_catalog(
                markets=markets,
                channels=list(channels),
                outcomes=outcomes,
                window_start=window_start,
                window_end=window_end,
                status_filter=status_filter,
                slug_filter=slug_filter,
                show_progress=show_progress,
            )
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
            jobs = _build_jobs_from_explicit(
                channels=list(channels),
                market_slugs=market_slugs,
                outcome=outcome,
                outcome_id=outcome_id,
                start=window_start,
                end=window_end,
            )
            markets_considered = len(set(market_slugs))

        planned_jobs = len(jobs)
        jobs, skipped_existing = _prune_jobs_against_manifest(
            jobs=jobs, store=store, overwrite=overwrite, show_progress=show_progress
        )
        if show_progress and skipped_existing:
            print(
                f"Skipping {skipped_existing:,} day-files already recorded in the blob manifest",
                file=sys.stderr,
            )

        total_jobs = len(jobs)
        if show_progress:
            existing_blob_size = db_path.stat().st_size if db_path.exists() else 0
            completed_before = sum(len(store.completed_keys(ch)) for ch in channels)
            empty_before = sum(len(store.empty_keys(ch)) for ch in channels)
            print(
                f"[telonex] Resume summary: blob={db_path} "
                f"({_format_bytes(existing_blob_size)}), "
                f"completed={completed_before:,} 404s={empty_before:,}, "
                f"planned={planned_jobs:,} skipping={skipped_existing:,} "
                f"remaining={total_jobs:,}",
                file=sys.stderr,
            )
            print(
                f"[telonex] Channels={channels} workers={workers} "
                f"retries={_DEFAULT_MAX_RETRIES} timeout={timeout_secs}s. "
                f"Ctrl-C once to stop gracefully (manifest + blob are always consistent).",
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
            jobs,
            store=store,
            api_key=api_key,
            base_url=base_url,
            timeout_secs=max(1, timeout_secs),
            workers=max(1, workers),
            show_progress=show_progress,
        )
    finally:
        store.close()

    start_out = f"{window_start:%Y-%m-%d}" if window_start else None
    end_out = f"{window_end:%Y-%m-%d}" if window_end else None

    return TelonexDownloadSummary(
        destination=str(normalized_destination),
        db_path=str(db_path),
        channels=list(channels),
        base_url=base_url.rstrip("/"),
        markets_considered=markets_considered,
        requested_days=planned_jobs,
        downloaded_days=downloaded,
        skipped_existing_days=skipped_existing,
        missing_days=missing,
        failed_days=failed,
        cancelled_days=cancelled,
        bytes_downloaded=bytes_total,
        start_date=start_out,
        end_date=end_out,
        db_size_bytes=db_path.stat().st_size if db_path.exists() else 0,
        interrupted=interrupted,
        failed_samples=failed_samples,
    )


__all__ = [
    "TelonexDownloadSummary",
    "VALID_CHANNELS",
    "download_telonex_days",
]
