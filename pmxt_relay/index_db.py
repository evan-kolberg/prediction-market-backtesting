from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from pmxt_relay.storage import parse_archive_hour

LOG = logging.getLogger(__name__)
_LOCKED_ERROR_SNIPPETS = (
    "database is locked",
    "database schema is locked",
    "database table is locked",
)
_DUPLICATE_COLUMN_ERROR_SNIPPET = "duplicate column name"
_LEGACY_ACTIVE_STATUS = "processing"
_MIN_NONEMPTY_RAW_BYTES = 1024 * 1024
REMIRROR_CONTENT_CHANGED_REASON = "upstream content changed"


def _utc_now_datetime() -> datetime:
    return datetime.now(UTC)


def _utc_now_at(now: datetime | None = None) -> str:
    reference = _utc_now_datetime() if now is None else now.astimezone(UTC)
    return reference.isoformat()


def _utc_now() -> str:
    return _utc_now_at()


def _parse_db_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
            "last_error_at",
            "next_retry_at",
            "error_count",
            "last_verified_at",
            "row_count",
        }
    )

    def __init__(
        self, db_path: Path, *, event_retention: int = 50000, lock_retry_delay_secs: float = 0.25
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
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        with self._conn_lock:
            if self._closed:
                return
            self._conn.close()
            self._closed = True

    def __enter__(self) -> RelayIndex:
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

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
                last_error_at TEXT,
                next_retry_at TEXT,
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
        archive_columns = {row[1] for row in self._conn.execute("PRAGMA table_info(archive_hours)")}
        if "last_error_at" not in archive_columns:
            self._ensure_archive_column("ALTER TABLE archive_hours ADD COLUMN last_error_at TEXT")
        if "next_retry_at" not in archive_columns:
            self._ensure_archive_column("ALTER TABLE archive_hours ADD COLUMN next_retry_at TEXT")
        if "last_verified_at" not in archive_columns:
            self._ensure_archive_column(
                "ALTER TABLE archive_hours ADD COLUMN last_verified_at TEXT"
            )
        if "row_count" not in archive_columns:
            self._ensure_archive_column("ALTER TABLE archive_hours ADD COLUMN row_count INTEGER")
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archive_hours_retry_status_hour
            ON archive_hours (mirror_status, next_retry_at, hour DESC)
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_archive_hours_verification
            ON archive_hours (mirror_status, last_verified_at ASC)
            """
        )
        self._conn.execute(
            """
            UPDATE archive_hours
            SET mirror_status = 'active'
            WHERE mirror_status = ?
            """,
            (_LEGACY_ACTIVE_STATUS,),
        )

    def _ensure_archive_column(self, sql: str) -> None:
        try:
            self._conn.execute(sql)
        except sqlite3.OperationalError as exc:
            if _DUPLICATE_COLUMN_ERROR_SNIPPET not in str(exc).lower():
                raise

    def _schema_needs_bootstrap(self) -> bool:
        tables = {
            row[0] for row in self._fetchall("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not self._REQUIRED_TABLES.issubset(tables):
            return True
        archive_columns = {row[1] for row in self._fetchall("PRAGMA table_info(archive_hours)")}
        return not self._REQUIRED_ARCHIVE_COLUMNS.issubset(archive_columns)

    def _rollback_quietly(self) -> None:
        with suppress(sqlite3.Error):
            self._conn.rollback()

    def _run_with_lock_retry(
        self, operation, *, swallow_after_secs: float | None = None, default=None
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

    def _fetchall(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        return self._run_with_lock_retry(lambda: self._conn.execute(sql, params).fetchall())

    def _fetchone(self, sql: str, params: tuple[object, ...] = ()) -> sqlite3.Row | None:
        return self._run_with_lock_retry(lambda: self._conn.execute(sql, params).fetchone())

    def _fetchscalar(
        self, sql: str, params: tuple[object, ...] = (), *, default: object = None
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
            operation, swallow_after_secs=10.0 if best_effort else None, default=False
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
                        last_error_at = COALESCE(last_error_at, ?),
                        next_retry_at = NULL,
                        error_count = error_count + 1
                    WHERE mirror_status IN ('active', ?)
                    """,
                    (_utc_now(), _LEGACY_ACTIVE_STATUS),
                )
            return cursor.rowcount

        return self._run_with_lock_retry(operation)

    def upsert_discovered_hour(self, filename: str, source_url: str, archive_page: int) -> bool:
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
                update_cursor = self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET archive_page = ?, source_url = ?
                    WHERE filename = ?
                      AND (
                        archive_page != ?
                        OR source_url != ?
                      )
                    """,
                    (archive_page, source_url, filename, archive_page, source_url),
                )
            return (cursor.rowcount + update_cursor.rowcount) > 0

        return self._run_with_lock_retry(operation)

    def list_hours_needing_mirror(self, *, now: datetime | None = None) -> list[sqlite3.Row]:
        retry_cutoff = _utc_now_at(now)
        return self._fetchall(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'pending'
               OR (
                    mirror_status IN ('error', 'quarantined')
                    AND (next_retry_at IS NULL OR next_retry_at <= ?)
               )
            ORDER BY
                CASE WHEN mirror_status = 'pending' THEN 0 ELSE 1 END,
                COALESCE(next_retry_at, hour) ASC,
                error_count ASC,
                hour DESC
            """,
            (retry_cutoff,),
        )

    def mark_mirroring(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'active',
                    last_error = NULL,
                    next_retry_at = NULL
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
                    last_error_at = ?,
                    next_retry_at = NULL,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, _utc_now(), filename),
            )
        )

    def mark_mirror_retry(self, filename: str, *, error: str, next_retry_at: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'error',
                    last_error = ?,
                    last_error_at = ?,
                    next_retry_at = ?,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, _utc_now(), next_retry_at, filename),
            )
        )

    def mark_mirror_quarantined(self, filename: str, *, error: str, next_retry_at: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'quarantined',
                    last_error = ?,
                    last_error_at = ?,
                    next_retry_at = ?,
                    error_count = error_count + 1
                WHERE filename = ?
                """,
                (error, _utc_now(), next_retry_at, filename),
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
                    last_error_at = NULL,
                    next_retry_at = NULL,
                    error_count = 0
                WHERE filename = ?
                """,
                (local_path, etag, content_length, last_modified, _utc_now(), filename),
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
                        mirrored_at,
                        error_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', ?, 0)
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
                        local_path = ?,
                        content_length = COALESCE(content_length, ?),
                        mirror_status = 'ready',
                        mirrored_at = COALESCE(mirrored_at, ?),
                        last_error = NULL,
                        last_error_at = NULL,
                        next_retry_at = NULL,
                        error_count = 0
                    WHERE filename = ?
                      AND (
                        local_path IS NULL
                        OR local_path != ?
                        OR mirror_status != 'ready'
                        OR (content_length IS NULL AND ? IS NOT NULL)
                      )
                      AND (
                        mirror_status != 'pending'
                        OR last_error IS NULL
                        OR last_error != ?
                      )
                    """,
                    (
                        local_path,
                        content_length,
                        mirrored_at,
                        filename,
                        local_path,
                        content_length,
                        REMIRROR_CONTENT_CHANGED_REASON,
                    ),
                )
            return (insert_cursor.rowcount + update_cursor.rowcount) > 0

        return self._run_with_lock_retry(operation)

    def list_hours_needing_verification(self, *, batch_size: int = 50) -> list[sqlite3.Row]:
        return self._fetchall(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'ready'
            ORDER BY last_verified_at ASC NULLS FIRST, mirrored_at ASC
            LIMIT ?
            """,
            (max(1, batch_size),),
        )

    def mark_verified(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET last_verified_at = ?
                WHERE filename = ?
                """,
                (_utc_now(), filename),
            )
        )

    def mark_needs_remirror(self, filename: str) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET mirror_status = 'pending',
                    last_verified_at = NULL,
                    error_count = 0,
                    last_error = ?,
                    last_error_at = ?,
                    next_retry_at = NULL,
                    row_count = NULL
                WHERE filename = ?
                """,
                (REMIRROR_CONTENT_CHANGED_REASON, _utc_now(), filename),
            )
        )

    def update_row_count(self, filename: str, row_count: int) -> None:
        self._run_with_lock_retry(
            lambda: self._write_single_update(
                """
                UPDATE archive_hours
                SET row_count = ?
                WHERE filename = ?
                """,
                (row_count, filename),
            )
        )

    def count_missing_hours(self, *, now: datetime | None = None) -> int:
        archive_hours = self._compute_elapsed_archive_hours(now=now)
        if archive_hours == 0:
            return 0
        now_reference = _utc_now_datetime() if now is None else now.astimezone(UTC)
        now_floor = now_reference.replace(minute=0, second=0, microsecond=0)
        discovered_hours = int(
            self._fetchscalar(
                """
                SELECT COUNT(DISTINCT hour)
                FROM archive_hours
                WHERE hour <= ?
                """,
                (now_floor.isoformat(),),
                default=0,
            )
        )
        return max(0, archive_hours - discovered_hours)

    def count_empty_hours(self) -> int:
        return int(
            self._fetchscalar(
                """
                SELECT COUNT(*)
                FROM archive_hours
                WHERE mirror_status = 'ready'
                  AND (
                    (row_count IS NOT NULL AND row_count = 0)
                    OR (content_length IS NOT NULL AND content_length < ?)
                  )
                """,
                (_MIN_NONEMPTY_RAW_BYTES,),
                default=0,
            )
        )

    def count_error_hours(self, *, now: datetime | None = None) -> int:
        now_reference = _utc_now_datetime() if now is None else now.astimezone(UTC)
        now_floor = now_reference.replace(minute=0, second=0, microsecond=0)
        row = self._fetchone(
            """
            SELECT
                COUNT(DISTINCT hour) AS discovered_hours,
                SUM(
                    CASE
                        WHEN mirror_status = 'ready' AND (row_count IS NULL OR row_count > 0)
                          AND (content_length IS NULL OR content_length >= ?)
                        THEN 1
                        ELSE 0
                    END
                ) AS mirrored_hours,
                SUM(
                    CASE
                        WHEN mirror_status = 'ready'
                          AND (
                            (row_count IS NOT NULL AND row_count = 0)
                            OR (content_length IS NOT NULL AND content_length < ?)
                          )
                        THEN 1
                        ELSE 0
                    END
                ) AS empty_hours
            FROM archive_hours
            WHERE hour <= ?
            """,
            (_MIN_NONEMPTY_RAW_BYTES, _MIN_NONEMPTY_RAW_BYTES, now_floor.isoformat()),
        )
        if row is None:
            return 0
        discovered_hours = int(row["discovered_hours"] or 0)
        mirrored_hours = int(row["mirrored_hours"] or 0)
        empty_hours = int(row["empty_hours"] or 0)
        return max(0, discovered_hours - mirrored_hours - empty_hours)

    def _compute_elapsed_archive_hours(self, *, now: datetime | None = None) -> int:
        min_hour_value = self._fetchscalar("SELECT MIN(hour) FROM archive_hours", default=None)
        min_hour = _parse_db_timestamp(min_hour_value if isinstance(min_hour_value, str) else None)
        if min_hour is None:
            return 0
        min_floor = min_hour.replace(minute=0, second=0, microsecond=0)
        now_reference = _utc_now_datetime() if now is None else now.astimezone(UTC)
        now_floor = now_reference.replace(minute=0, second=0, microsecond=0)
        delta_hours = int((now_floor - min_floor).total_seconds() // 3600) + 1
        return max(0, delta_hours)

    def stats(self, *, now: datetime | None = None) -> dict[str, int | str | None]:
        row = self._fetchone(
            """
            SELECT
                SUM(
                    CASE
                        WHEN mirror_status = 'ready' AND (row_count IS NULL OR row_count > 0)
                          AND (content_length IS NULL OR content_length >= ?)
                        THEN 1
                        ELSE 0
                    END
                ) AS mirrored_hours,
                SUM(CASE WHEN mirror_status IN ('error', 'quarantined') THEN 1 ELSE 0 END) AS mirror_errors,
                SUM(CASE WHEN mirror_status = 'quarantined' THEN 1 ELSE 0 END) AS mirror_quarantined
            FROM archive_hours
            """,
            (_MIN_NONEMPTY_RAW_BYTES,),
        )
        stats_row = dict(row) if row is not None else {}
        archive_hours = self._compute_elapsed_archive_hours(now=now)
        last_event_at = self._fetchscalar("SELECT MAX(created_at) FROM relay_events", default=None)
        last_error_at = self._fetchscalar(
            "SELECT MAX(created_at) FROM relay_events WHERE level = 'ERROR'", default=None
        )
        return {
            "archive_hours": archive_hours,
            "mirrored_hours": int(stats_row.get("mirrored_hours") or 0),
            "mirror_errors": int(stats_row.get("mirror_errors") or 0),
            "mirror_quarantined": int(stats_row.get("mirror_quarantined") or 0),
            "last_event_at": last_event_at,
            "last_error_at": last_error_at,
        }

    def queue_summary(self, *, now: datetime | None = None) -> dict[str, int | str | None]:
        retry_cutoff = _utc_now_at(now)
        row = self._fetchone(
            """
            SELECT
                SUM(CASE WHEN mirror_status = 'pending' THEN 1 ELSE 0 END) AS mirror_pending,
                SUM(CASE WHEN mirror_status IN ('active', ?) THEN 1 ELSE 0 END) AS mirror_active,
                SUM(CASE WHEN mirror_status IN ('error', 'quarantined') THEN 1 ELSE 0 END) AS mirror_error,
                SUM(CASE WHEN mirror_status IN ('error', 'quarantined') AND (next_retry_at IS NULL OR next_retry_at <= ?) THEN 1 ELSE 0 END) AS mirror_retry_due,
                SUM(CASE WHEN mirror_status IN ('error', 'quarantined') AND next_retry_at > ? THEN 1 ELSE 0 END) AS mirror_retry_waiting,
                SUM(CASE WHEN mirror_status = 'quarantined' THEN 1 ELSE 0 END) AS mirror_quarantined,
                MIN(CASE WHEN mirror_status IN ('error', 'quarantined') THEN next_retry_at END) AS next_retry_at,
                MAX(CASE WHEN mirror_status = 'ready' THEN hour END) AS latest_mirrored_hour,
                (
                    SELECT filename
                    FROM archive_hours latest_ready
                    WHERE latest_ready.mirror_status = 'ready'
                    ORDER BY latest_ready.hour DESC, latest_ready.mirrored_at DESC, latest_ready.filename DESC
                    LIMIT 1
                ) AS latest_mirrored_filename
            FROM archive_hours
            """,
            (_LEGACY_ACTIVE_STATUS, retry_cutoff, retry_cutoff),
        )
        queue_row = dict(row) if row is not None else {}
        return {
            "mirror_pending": int(queue_row.get("mirror_pending") or 0),
            "mirror_active": int(queue_row.get("mirror_active") or 0),
            "mirror_error": int(queue_row.get("mirror_error") or 0),
            "mirror_retry_due": int(queue_row.get("mirror_retry_due") or 0),
            "mirror_retry_waiting": int(queue_row.get("mirror_retry_waiting") or 0),
            "mirror_quarantined": int(queue_row.get("mirror_quarantined") or 0),
            "next_retry_at": queue_row.get("next_retry_at"),
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
        payload_json = json.dumps(payload, sort_keys=True) if payload is not None else None

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
