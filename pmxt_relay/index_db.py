from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
import json
from pathlib import Path
import threading
import time

from pmxt_relay.storage import parse_archive_hour

LOG = logging.getLogger(__name__)
_LOCKED_ERROR_SNIPPETS = (
    "database is locked",
    "database schema is locked",
    "database table is locked",
)


def _utc_now_datetime() -> datetime:
    return datetime.now(UTC)


def _utc_now() -> str:
    return _utc_now_datetime().isoformat()


@dataclass(frozen=True)
class FilteredHourArtifact:
    filename: str
    hour: str
    condition_id: str
    token_id: str
    local_path: str
    row_count: int
    byte_size: int


@dataclass(frozen=True)
class PrebuildProgress:
    filename: str
    created_at: str
    processed_rows: int
    total_rows: int


class RelayIndex:
    _REQUIRED_TABLES = frozenset({"archive_hours", "filtered_hours", "relay_events"})
    _REQUIRED_ARCHIVE_COLUMNS = frozenset(
        {"filtered_artifact_count", "prebuild_status", "prebuilt_at", "error_count"}
    )

    def __init__(
        self,
        db_path: Path,
        *,
        event_retention: int = 50000,
        lock_retry_delay_secs: float = 0.25,
    ) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn_lock = threading.RLock()
        self._event_retention = event_retention
        self._lock_retry_delay_secs = max(0.01, lock_retry_delay_secs)
        self._conn.execute("PRAGMA busy_timeout=60000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA wal_autocheckpoint=2000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._events_since_prune = 0

    def initialize(
        self,
        *,
        apply_maintenance: bool = True,
        reset_inflight: bool = False,
        reset_mirror_inflight: bool = True,
        reset_process_inflight: bool = True,
        reset_prebuild_inflight: bool = True,
    ) -> tuple[int, int, int]:
        schema_needs_bootstrap = self._schema_needs_bootstrap()
        if schema_needs_bootstrap or apply_maintenance:
            self._run_with_lock_retry(self._ensure_schema)
        if apply_maintenance:
            self._run_with_lock_retry(self._normalize_archive_hours)
            self.prune_events(best_effort=True)
        if reset_inflight and apply_maintenance:
            return self.reset_inflight_work(
                reset_mirror=reset_mirror_inflight,
                reset_process=reset_process_inflight,
                reset_prebuild=reset_prebuild_inflight,
            )
        return 0, 0, 0

    def _ensure_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS archive_hours (
                filename TEXT PRIMARY KEY,
                hour TEXT NOT NULL,
                source_url TEXT NOT NULL,
                archive_page INTEGER NOT NULL,
                discovered_at TEXT NOT NULL,
                local_path TEXT,
                etag TEXT,
                content_length INTEGER,
                last_modified TEXT,
                mirror_status TEXT NOT NULL DEFAULT 'pending',
                process_status TEXT NOT NULL DEFAULT 'pending',
                prebuild_status TEXT NOT NULL DEFAULT 'pending',
                filtered_artifact_count INTEGER NOT NULL DEFAULT 0,
                mirrored_at TEXT,
                processed_at TEXT,
                prebuilt_at TEXT,
                last_error TEXT,
                error_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS filtered_hours (
                filename TEXT NOT NULL,
                hour TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                local_path TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                byte_size INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (filename, condition_id, token_id)
            );

            CREATE INDEX IF NOT EXISTS idx_filtered_market_token_hour
            ON filtered_hours (condition_id, token_id, hour);

            CREATE INDEX IF NOT EXISTS idx_filtered_filename
            ON filtered_hours (filename);

            CREATE TABLE IF NOT EXISTS relay_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                event_type TEXT NOT NULL,
                filename TEXT,
                message TEXT NOT NULL,
                payload_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_relay_events_created_at
            ON relay_events (created_at DESC, id DESC);

            CREATE INDEX IF NOT EXISTS idx_relay_events_type_id
            ON relay_events (event_type, id DESC);
            """
        )
        self._ensure_archive_hours_column(
            "filtered_artifact_count",
            "INTEGER NOT NULL DEFAULT 0",
        )
        self._ensure_archive_hours_column(
            "prebuild_status",
            "TEXT NOT NULL DEFAULT 'pending'",
        )
        self._ensure_archive_hours_column(
            "prebuilt_at",
            "TEXT",
        )
        self._ensure_archive_hours_column(
            "error_count",
            "INTEGER NOT NULL DEFAULT 0",
        )

    def _normalize_archive_hours(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET prebuild_status = 'ready',
                    prebuilt_at = COALESCE(prebuilt_at, processed_at)
                WHERE filtered_artifact_count > 0
                  AND prebuild_status != 'ready'
                """
            )
            self._conn.execute(
                """
                UPDATE archive_hours
                SET prebuild_status = 'pending'
                WHERE (prebuild_status IS NULL OR prebuild_status = '')
                  AND filtered_artifact_count = 0
                """
            )

    def _schema_needs_bootstrap(self) -> bool:
        tables = {
            row[0]
            for row in self._fetchall(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not self._REQUIRED_TABLES.issubset(tables):
            return True
        archive_columns = {
            row[1] for row in self._fetchall("PRAGMA table_info(archive_hours)")
        }
        return not self._REQUIRED_ARCHIVE_COLUMNS.issubset(archive_columns)

    def _rollback_quietly(self) -> None:
        try:
            self._conn.rollback()
        except sqlite3.Error:
            pass

    def _run_with_lock_retry(
        self,
        operation,
        *,
        swallow_after_secs: float | None = None,
        default=None,
    ):
        delay = self._lock_retry_delay_secs
        started = time.monotonic()
        while True:
            try:
                with self._conn_lock:
                    return operation()
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if not any(snippet in message for snippet in _LOCKED_ERROR_SNIPPETS):
                    raise
                self._rollback_quietly()
                if (
                    swallow_after_secs is not None
                    and (time.monotonic() - started) >= swallow_after_secs
                ):
                    LOG.warning(
                        "Skipping best-effort relay index write after %.1fs of lock contention",
                        time.monotonic() - started,
                    )
                    return default
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    def _ensure_archive_hours_column(self, name: str, definition: str) -> None:
        columns = {row[1] for row in self._fetchall("PRAGMA table_info(archive_hours)")}
        if name in columns:
            return
        try:
            self._conn.execute(
                f"ALTER TABLE archive_hours ADD COLUMN {name} {definition}"
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise

    def _fetchall(
        self,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> list[sqlite3.Row]:
        return self._run_with_lock_retry(
            lambda: self._conn.execute(sql, params).fetchall()
        )

    def _fetchone(
        self,
        sql: str,
        params: tuple[object, ...] = (),
    ) -> sqlite3.Row | None:
        return self._run_with_lock_retry(
            lambda: self._conn.execute(sql, params).fetchone()
        )

    def _fetchscalar(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        default: object = None,
    ) -> object:
        row = self._fetchone(sql, params)
        if row is None:
            return default
        value = row[0]
        return default if value is None else value

    def prune_events(self, *, best_effort: bool = False) -> None:
        def operation() -> None:
            with self._conn:
                self._conn.execute(
                    """
                    DELETE FROM relay_events
                    WHERE id NOT IN (
                        SELECT id
                        FROM relay_events
                        ORDER BY id DESC
                        LIMIT ?
                    )
                    """,
                    (self._event_retention,),
                )

        result = self._run_with_lock_retry(
            operation,
            swallow_after_secs=10.0 if best_effort else None,
            default=False,
        )
        if result is not False:
            self._events_since_prune = 0

    def reset_inflight_work(
        self,
        *,
        reset_mirror: bool = True,
        reset_process: bool = True,
        reset_prebuild: bool = True,
    ) -> tuple[int, int, int]:
        def operation() -> tuple[int, int, int]:
            with self._conn:
                mirror_cursor = (
                    self._conn.execute(
                        """
                        UPDATE archive_hours
                        SET mirror_status = 'pending',
                            last_error = COALESCE(last_error, 'mirror interrupted by restart'),
                            error_count = error_count + 1
                        WHERE mirror_status = 'processing'
                        """
                    )
                    if reset_mirror
                    else None
                )
                process_cursor = (
                    self._conn.execute(
                        """
                        UPDATE archive_hours
                        SET process_status = 'pending',
                            last_error = COALESCE(last_error, 'processing interrupted by restart'),
                            error_count = error_count + 1
                        WHERE process_status = 'processing'
                        """
                    )
                    if reset_process
                    else None
                )
                prebuild_cursor = (
                    self._conn.execute(
                        """
                        UPDATE archive_hours
                        SET prebuild_status = 'pending',
                            last_error = COALESCE(last_error, 'prebuild interrupted by restart'),
                            error_count = error_count + 1
                        WHERE prebuild_status = 'processing'
                        """
                    )
                    if reset_prebuild
                    else None
                )
            return (
                0 if mirror_cursor is None else mirror_cursor.rowcount,
                0 if process_cursor is None else process_cursor.rowcount,
                0 if prebuild_cursor is None else prebuild_cursor.rowcount,
            )

        return self._run_with_lock_retry(operation)

    def upsert_discovered_hour(
        self, filename: str, source_url: str, archive_page: int
    ) -> bool:
        hour = parse_archive_hour(filename).isoformat()

        def operation() -> bool:
            with self._conn:
                cursor = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO archive_hours (
                        filename,
                        hour,
                        source_url,
                        archive_page,
                        discovered_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (filename, hour, source_url, archive_page, _utc_now()),
                )
                self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET archive_page = ?, source_url = ?
                    WHERE filename = ?
                    """,
                    (archive_page, source_url, filename),
                )
            return cursor.rowcount > 0

        return self._run_with_lock_retry(operation)

    def list_hours_needing_mirror(self) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status IN ('pending', 'error')
            ORDER BY error_count ASC, hour
            """
        )

    def mark_mirrored(
        self,
        filename: str,
        *,
        local_path: str,
        etag: str | None,
        content_length: int | None,
        last_modified: str | None,
    ) -> None:
        self._run_with_lock_retry(
            lambda: self._write_mark_mirrored(
                filename,
                local_path=local_path,
                etag=etag,
                content_length=content_length,
                last_modified=last_modified,
            )
        )

    def _write_mark_mirrored(
        self,
        filename: str,
        *,
        local_path: str,
        etag: str | None,
        content_length: int | None,
        last_modified: str | None,
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET
                    local_path = ?,
                    etag = ?,
                    content_length = ?,
                    last_modified = ?,
                    mirror_status = 'ready',
                    process_status = 'pending',
                    prebuild_status = 'pending',
                    filtered_artifact_count = 0,
                    mirrored_at = ?,
                    processed_at = NULL,
                    prebuilt_at = NULL,
                    last_error = NULL,
                    error_count = 0
                WHERE filename = ?
                """,
                (
                    local_path,
                    etag,
                    content_length,
                    last_modified,
                    _utc_now(),
                    filename,
                ),
            )

    def mark_mirroring(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'processing', last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )
        )

    def mark_mirror_error(self, filename: str, error: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'error',
                    last_error = ?,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, filename),
            )
        )

    def register_local_raw(
        self,
        filename: str,
        *,
        local_path: str,
        content_length: int | None,
        source_url: str,
        archive_page: int = 0,
    ) -> bool:
        hour = parse_archive_hour(filename).isoformat()

        def operation() -> bool:
            with self._conn:
                insert_cursor = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO archive_hours (
                        filename,
                        hour,
                        source_url,
                        archive_page,
                        discovered_at,
                        local_path,
                        content_length,
                        mirror_status,
                        mirrored_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?)
                    """,
                    (
                        filename,
                        hour,
                        source_url,
                        archive_page,
                        _utc_now(),
                        local_path,
                        content_length,
                        _utc_now(),
                    ),
                )
                update_cursor = self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET
                        source_url = ?,
                        local_path = ?,
                        content_length = COALESCE(content_length, ?),
                        mirror_status = 'ready',
                        mirrored_at = COALESCE(mirrored_at, ?)
                    WHERE filename = ?
                      AND (
                        local_path IS NULL
                        OR local_path != ?
                        OR mirror_status != 'ready'
                        OR (content_length IS NULL AND ? IS NOT NULL)
                      )
                    """,
                    (
                        source_url,
                        local_path,
                        content_length,
                        _utc_now(),
                        filename,
                        local_path,
                        content_length,
                    ),
                )
            return insert_cursor.rowcount > 0 or update_cursor.rowcount > 0

        return self._run_with_lock_retry(operation)

    def list_hours_needing_process(
        self,
        *,
        include_errors: bool = True,
    ) -> list[sqlite3.Row]:
        statuses = ("pending", "error") if include_errors else ("pending",)
        placeholders = ", ".join("?" for _ in statuses)
        return self._fetchall(
            f"""
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'ready' AND process_status IN ({placeholders})
            ORDER BY error_count ASC, hour
            """,
            tuple(statuses),
        )

    def list_processing_filenames(self) -> list[str]:
        rows = self._fetchall(
            """
            SELECT filename
            FROM archive_hours
            WHERE process_status = 'processing'
            ORDER BY hour
            """
        )
        return [
            str(row["filename"]) for row in rows if isinstance(row["filename"], str)
        ]

    def mark_processing(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET process_status = 'processing', last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )
        )

    def mark_process_error(self, filename: str, error: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET process_status = 'error',
                    last_error = ?,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, filename),
            )
        )

    def replace_filtered_hours(
        self,
        filename: str,
        artifacts: list[FilteredHourArtifact],
    ) -> None:
        def operation() -> None:
            with self._conn:
                self._conn.execute(
                    "DELETE FROM filtered_hours WHERE filename = ?",
                    (filename,),
                )
                self._conn.executemany(
                    """
                    INSERT INTO filtered_hours (
                        filename,
                        hour,
                        condition_id,
                        token_id,
                        local_path,
                        row_count,
                        byte_size,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            artifact.filename,
                            artifact.hour,
                            artifact.condition_id,
                            artifact.token_id,
                            artifact.local_path,
                            artifact.row_count,
                            artifact.byte_size,
                            _utc_now(),
                        )
                        for artifact in artifacts
                    ],
                )
                self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET process_status = 'ready',
                        prebuild_status = 'ready',
                        processed_at = ?,
                        prebuilt_at = ?,
                        filtered_artifact_count = ?,
                        last_error = NULL,
                        error_count = 0
                    WHERE filename = ?
                    """,
                    (_utc_now(), _utc_now(), len(artifacts), filename),
                )

        self._run_with_lock_retry(operation)

    def mark_sharded(
        self,
        filename: str,
    ) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET process_status = 'ready',
                    processed_at = ?,
                    last_error = NULL,
                    error_count = 0
                WHERE filename = ?
                """,
                (_utc_now(), filename),
            )
        )

    def mark_prebuilding(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET prebuild_status = 'processing',
                    last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )
        )

    def mark_prebuild_error(self, filename: str, error: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET prebuild_status = 'error',
                    last_error = ?,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, filename),
            )
        )

    _PREBUILT_BATCH_SIZE = 5000

    def mark_prebuilt(
        self,
        filename: str,
        *,
        filtered_artifact_count: int,
        artifacts: list[FilteredHourArtifact] | None = None,
    ) -> None:
        if artifacts:
            self._run_with_lock_retry(
                lambda: self._write_single_update(
                    "DELETE FROM filtered_hours WHERE filename = ?",
                    (filename,),
                )
            )
            for offset in range(0, len(artifacts), self._PREBUILT_BATCH_SIZE):
                batch = artifacts[offset : offset + self._PREBUILT_BATCH_SIZE]
                self._run_with_lock_retry(
                    lambda batch=batch: self._write_filtered_artifact_batch(batch)
                )
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET prebuild_status = 'ready',
                    prebuilt_at = ?,
                    filtered_artifact_count = ?,
                    last_error = NULL,
                    error_count = 0
                WHERE filename = ?
                """,
                (_utc_now(), filtered_artifact_count, filename),
            )
        )

    def _write_filtered_artifact_batch(
        self,
        batch: list[FilteredHourArtifact],
    ) -> None:
        with self._conn:
            self._conn.executemany(
                """
                INSERT INTO filtered_hours (
                    filename, hour, condition_id, token_id,
                    local_path, row_count, byte_size, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        a.filename,
                        a.hour,
                        a.condition_id,
                        a.token_id,
                        a.local_path,
                        a.row_count,
                        a.byte_size,
                        _utc_now(),
                    )
                    for a in batch
                ],
            )

    def list_hours_needing_filtered_prebuild(self) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'ready'
              AND process_status = 'ready'
              AND prebuild_status IN ('pending', 'error')
              AND local_path IS NOT NULL
            ORDER BY error_count ASC, hour DESC
            """
        )

    def list_completed_hours(self) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT filename, hour, filtered_artifact_count
            FROM archive_hours
            WHERE mirror_status = 'ready'
              AND process_status = 'ready'
              AND prebuild_status = 'ready'
            ORDER BY hour
            """
        )

    def list_filtered_for_filename(self, filename: str) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT *
            FROM filtered_hours
            WHERE filename = ?
            ORDER BY condition_id, token_id
            """,
            (filename,),
        )

    def get_filtered_hour(
        self,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> sqlite3.Row | None:
        return self._fetchone(
            """
            SELECT *
            FROM filtered_hours
            WHERE condition_id = ? AND token_id = ? AND filename = ?
            """,
            (condition_id, token_id, filename),
        )

    def list_filtered_hours(
        self,
        condition_id: str,
        token_id: str,
        *,
        start_hour: str | None = None,
        end_hour: str | None = None,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT *
            FROM filtered_hours
            WHERE condition_id = ? AND token_id = ?
        """
        params: list[str] = [condition_id, token_id]
        if start_hour is not None:
            query += " AND hour >= ?"
            params.append(start_hour)
        if end_hour is not None:
            query += " AND hour <= ?"
            params.append(end_hour)
        query += " ORDER BY hour"
        return self._fetchall(query, tuple(params))

    def _processed_rate_summary(self, *, window_hours: int) -> dict[str, int | float]:
        cutoff = (_utc_now_datetime() - timedelta(hours=window_hours)).isoformat()
        processed_hours_last_window = int(
            self._fetchscalar(
                """
                SELECT COUNT(*)
                FROM archive_hours
                WHERE prebuild_status = 'ready'
                  AND prebuilt_at IS NOT NULL
                  AND prebuilt_at >= ?
                """,
                (cutoff,),
                default=0,
            )
        )
        return {
            f"processed_hours_last_{window_hours}h": processed_hours_last_window,
            f"processed_hours_per_hour_{window_hours}h": round(
                processed_hours_last_window / max(1, window_hours),
                2,
            ),
        }

    def stats(self) -> dict[str, int | float | str | None]:
        row = self._fetchone(
            """
            SELECT
                COUNT(*) AS archive_hours,
                SUM(CASE WHEN mirror_status = 'ready' THEN 1 ELSE 0 END) AS mirrored_hours,
                SUM(CASE WHEN process_status = 'ready' THEN 1 ELSE 0 END) AS sharded_hours,
                SUM(CASE WHEN prebuild_status = 'ready' THEN 1 ELSE 0 END) AS processed_hours,
                SUM(CASE WHEN mirror_status = 'ready' AND process_status != 'ready' THEN 1 ELSE 0 END) AS ready_to_process_hours,
                SUM(CASE WHEN process_status = 'ready' AND prebuild_status != 'ready' THEN 1 ELSE 0 END) AS ready_to_prebuild_hours,
                SUM(CASE WHEN process_status = 'processing' OR prebuild_status = 'processing' THEN 1 ELSE 0 END) AS processing_hours,
                SUM(CASE WHEN process_status = 'processing' THEN 1 ELSE 0 END) AS sharding_hours,
                SUM(CASE WHEN prebuild_status = 'processing' THEN 1 ELSE 0 END) AS prebuilding_hours,
                SUM(CASE WHEN mirror_status = 'error' THEN 1 ELSE 0 END) AS mirror_errors,
                SUM(CASE WHEN process_status = 'error' THEN 1 ELSE 0 END) AS shard_errors,
                SUM(CASE WHEN prebuild_status = 'error' THEN 1 ELSE 0 END) AS prebuild_errors
            FROM archive_hours
            """
        )
        stats_row = dict(row) if row is not None else {}
        last_event_at = self._fetchscalar(
            "SELECT MAX(created_at) FROM relay_events",
            default=None,
        )
        last_error_at = self._fetchscalar(
            "SELECT MAX(created_at) FROM relay_events WHERE level = 'ERROR'",
            default=None,
        )
        payload = {
            "archive_hours": int(stats_row.get("archive_hours") or 0),
            "mirrored_hours": int(stats_row.get("mirrored_hours") or 0),
            "processed_hours": int(stats_row.get("processed_hours") or 0),
            "ready_to_process_hours": int(stats_row.get("ready_to_process_hours") or 0),
            "processing_hours": int(stats_row.get("processing_hours") or 0),
            "mirror_errors": int(stats_row.get("mirror_errors") or 0),
            "process_errors": int(stats_row.get("shard_errors") or 0),
            "last_event_at": last_event_at,
            "last_error_at": last_error_at,
        }
        payload.update(self._processed_rate_summary(window_hours=24))
        return payload

    def queue_summary(self) -> dict[str, int | str | None]:
        row = self._fetchone(
            """
            SELECT
                SUM(CASE WHEN mirror_status = 'pending' THEN 1 ELSE 0 END) AS mirror_pending,
                SUM(CASE WHEN mirror_status = 'processing' THEN 1 ELSE 0 END) AS mirror_processing,
                SUM(CASE WHEN mirror_status = 'error' THEN 1 ELSE 0 END) AS mirror_error,
                SUM(CASE WHEN mirror_status = 'ready' AND process_status != 'ready' THEN 1 ELSE 0 END) AS process_ready,
                SUM(CASE WHEN process_status = 'pending' THEN 1 ELSE 0 END) AS process_pending,
                SUM(CASE WHEN process_status = 'processing' THEN 1 ELSE 0 END) AS process_processing,
                SUM(CASE WHEN process_status = 'error' THEN 1 ELSE 0 END) AS process_error,
                SUM(CASE WHEN process_status = 'ready' AND prebuild_status != 'ready' THEN 1 ELSE 0 END) AS prebuild_ready,
                SUM(CASE WHEN prebuild_status = 'pending' THEN 1 ELSE 0 END) AS prebuild_pending,
                SUM(CASE WHEN prebuild_status = 'processing' THEN 1 ELSE 0 END) AS prebuild_processing,
                SUM(CASE WHEN prebuild_status = 'error' THEN 1 ELSE 0 END) AS prebuild_error,
                MAX(CASE WHEN mirror_status = 'ready' THEN hour END) AS latest_mirrored_hour,
                MAX(CASE WHEN prebuild_status = 'ready' THEN hour END) AS latest_processed_hour
            FROM archive_hours
            """
        )
        payload = dict(row) if row is not None else {}
        for key in (
            "prebuild_ready",
            "prebuild_pending",
            "prebuild_processing",
            "prebuild_error",
        ):
            payload.pop(key, None)
        return payload

    def current_processing_filename(self) -> str | None:
        row = self._fetchone(
            """
            SELECT filename
            FROM archive_hours
            WHERE process_status = 'processing' OR prebuild_status = 'processing'
            ORDER BY hour
            LIMIT 1
            """
        )
        if row is None:
            return None
        filename = row["filename"]
        return filename if isinstance(filename, str) else None

    def log_event(
        self,
        *,
        level: str,
        event_type: str,
        message: str,
        filename: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        payload_json = (
            json.dumps(payload, sort_keys=True) if payload is not None else None
        )
        inserted = self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                INSERT INTO relay_events (
                    created_at,
                    level,
                    event_type,
                    filename,
                    message,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (_utc_now(), level, event_type, filename, message, payload_json),
            ),
            swallow_after_secs=10.0,
            default=False,
        )
        if inserted is False:
            return
        self._events_since_prune += 1
        prune_threshold = 1 if self._event_retention <= 250 else 250
        if self._events_since_prune >= prune_threshold:
            self.prune_events(best_effort=True)

    def _write_single_update(
        self,
        sql: str,
        params: tuple[object, ...],
    ) -> bool:
        with self._conn:
            self._conn.execute(sql, params)
        return True

    def recent_events(self, *, limit: int = 100) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT *
            FROM relay_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

    def latest_prebuild_progress(self) -> PrebuildProgress | None:
        row = self._fetchone(
            """
            SELECT created_at, filename, payload_json
            FROM relay_events
            WHERE event_type IN ('filtered_prebuild_progress', 'process_progress')
            ORDER BY id DESC
            LIMIT 1
            """
        )
        if row is None or row["payload_json"] is None:
            return None

        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            return None

        filename = row["filename"]
        processed_rows = payload.get("processed_rows")
        total_rows = payload.get("total_rows")
        if not isinstance(filename, str):
            return None
        if type(processed_rows) is not int or type(total_rows) is not int:
            return None

        return PrebuildProgress(
            filename=filename,
            created_at=row["created_at"],
            processed_rows=processed_rows,
            total_rows=total_rows,
        )
