from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import UTC
from datetime import datetime
from pathlib import Path

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


class RelayIndex:
    _REQUIRED_TABLES = frozenset({"archive_hours", "relay_events"})
    _REQUIRED_ARCHIVE_COLUMNS = frozenset(
        {
            "filename",
            "hour",
            "source_url",
            "archive_page",
            "discovered_at",
            "local_path",
            "etag",
            "content_length",
            "last_modified",
            "mirror_status",
            "mirrored_at",
            "last_error",
            "error_count",
        }
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
    ) -> int:
        schema_needs_bootstrap = self._schema_needs_bootstrap()
        if schema_needs_bootstrap or apply_maintenance:
            self._run_with_lock_retry(self._ensure_schema)
        if apply_maintenance:
            self.prune_events(best_effort=True)
        if reset_inflight and apply_maintenance:
            return self.reset_inflight_work(reset_mirror=reset_mirror_inflight)
        return 0

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
                mirrored_at TEXT,
                last_error TEXT,
                error_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_archive_hours_mirror_status_hour
            ON archive_hours (mirror_status, hour DESC);

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

    def _write_single_update(self, sql: str, params: tuple[object, ...]) -> None:
        with self._conn:
            self._conn.execute(sql, params)

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

    def reset_inflight_work(self, *, reset_mirror: bool = True) -> int:
        if not reset_mirror:
            return 0

        def operation() -> int:
            with self._conn:
                cursor = self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET mirror_status = 'pending',
                        last_error = COALESCE(last_error, 'mirror interrupted by restart'),
                        error_count = error_count + 1
                    WHERE mirror_status = 'processing'
                    """
                )
            return cursor.rowcount

        return self._run_with_lock_retry(operation)

    def upsert_discovered_hour(
        self,
        filename: str,
        source_url: str,
        archive_page: int,
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
            ORDER BY error_count ASC, hour DESC
            """
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
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET
                    local_path = ?,
                    etag = ?,
                    content_length = ?,
                    last_modified = ?,
                    mirror_status = 'ready',
                    mirrored_at = ?,
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
        mirrored_at = _utc_now()

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
                        mirrored_at,
                        local_path,
                        content_length,
                        mirrored_at,
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
                        mirrored_at = COALESCE(mirrored_at, ?),
                        last_error = NULL
                    WHERE filename = ?
                      AND (
                        local_path IS NULL
                        OR local_path != ?
                        OR mirror_status != 'ready'
                        OR (content_length IS NULL AND ? IS NOT NULL)
                        OR source_url != ?
                      )
                    """,
                    (
                        source_url,
                        local_path,
                        content_length,
                        mirrored_at,
                        filename,
                        local_path,
                        content_length,
                        source_url,
                    ),
                )
            return (insert_cursor.rowcount + update_cursor.rowcount) > 0

        return self._run_with_lock_retry(operation)

    def stats(self) -> dict[str, int | str | None]:
        row = self._fetchone(
            """
            SELECT
                COUNT(*) AS archive_hours,
                SUM(CASE WHEN mirror_status = 'ready' THEN 1 ELSE 0 END) AS mirrored_hours,
                SUM(CASE WHEN mirror_status = 'error' THEN 1 ELSE 0 END) AS mirror_errors
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
        return {
            "archive_hours": int(stats_row.get("archive_hours") or 0),
            "mirrored_hours": int(stats_row.get("mirrored_hours") or 0),
            "mirror_errors": int(stats_row.get("mirror_errors") or 0),
            "last_event_at": last_event_at,
            "last_error_at": last_error_at,
        }

    def queue_summary(self) -> dict[str, int | str | None]:
        row = self._fetchone(
            """
            SELECT
                SUM(CASE WHEN mirror_status = 'pending' THEN 1 ELSE 0 END) AS mirror_pending,
                SUM(CASE WHEN mirror_status = 'processing' THEN 1 ELSE 0 END) AS mirror_processing,
                SUM(CASE WHEN mirror_status = 'error' THEN 1 ELSE 0 END) AS mirror_error,
                MAX(CASE WHEN mirror_status = 'ready' THEN hour END) AS latest_mirrored_hour,
                (
                    SELECT filename
                    FROM archive_hours latest_ready
                    WHERE latest_ready.mirror_status = 'ready'
                    ORDER BY latest_ready.hour DESC, latest_ready.mirrored_at DESC, latest_ready.filename DESC
                    LIMIT 1
                ) AS latest_mirrored_filename
            FROM archive_hours
            """
        )
        queue_row = dict(row) if row is not None else {}
        return {
            "mirror_pending": int(queue_row.get("mirror_pending") or 0),
            "mirror_processing": int(queue_row.get("mirror_processing") or 0),
            "mirror_error": int(queue_row.get("mirror_error") or 0),
            "latest_mirrored_hour": queue_row.get("latest_mirrored_hour"),
            "latest_mirrored_filename": queue_row.get("latest_mirrored_filename"),
        }

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

        def operation() -> None:
            with self._conn:
                self._conn.execute(
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
                )

        self._run_with_lock_retry(operation)
        self._events_since_prune += 1
        if self._events_since_prune >= 1000:
            self.prune_events(best_effort=True)

    def recent_events(self, *, limit: int = 100) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT
                id,
                created_at,
                level,
                event_type,
                filename,
                message,
                payload_json
            FROM relay_events
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (max(1, limit),),
        )
