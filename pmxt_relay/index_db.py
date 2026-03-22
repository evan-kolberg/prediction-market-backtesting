from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
import json

from pmxt_relay.storage import parse_archive_hour


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class FilteredHourArtifact:
    filename: str
    hour: str
    condition_id: str
    token_id: str
    local_path: str
    row_count: int
    byte_size: int


class RelayIndex:
    def __init__(self, db_path: Path, *, event_retention: int = 50000) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._event_retention = event_retention
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA wal_autocheckpoint=2000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._events_since_prune = 0

    def initialize(
        self,
        *,
        reset_inflight: bool = False,
        reset_mirror_inflight: bool = True,
        reset_process_inflight: bool = True,
        reset_prebuild_inflight: bool = True,
    ) -> tuple[int, int, int]:
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
                last_error TEXT
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
        self._conn.commit()
        self.prune_events()
        if reset_inflight:
            return self.reset_inflight_work(
                reset_mirror=reset_mirror_inflight,
                reset_process=reset_process_inflight,
                reset_prebuild=reset_prebuild_inflight,
            )
        return 0, 0, 0

    def _ensure_archive_hours_column(self, name: str, definition: str) -> None:
        columns = {
            row[1] for row in self._conn.execute("PRAGMA table_info(archive_hours)")
        }
        if name in columns:
            return
        try:
            self._conn.execute(
                f"ALTER TABLE archive_hours ADD COLUMN {name} {definition}"
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise

    def prune_events(self) -> None:
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
        self._events_since_prune = 0

    def reset_inflight_work(
        self,
        *,
        reset_mirror: bool = True,
        reset_process: bool = True,
        reset_prebuild: bool = True,
    ) -> tuple[int, int, int]:
        with self._conn:
            mirror_cursor = (
                self._conn.execute(
                    """
                    UPDATE archive_hours
                    SET mirror_status = 'pending',
                        last_error = COALESCE(last_error, 'mirror interrupted by restart')
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
                        last_error = COALESCE(last_error, 'processing interrupted by restart')
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
                        last_error = COALESCE(last_error, 'prebuild interrupted by restart')
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

    def upsert_discovered_hour(
        self, filename: str, source_url: str, archive_page: int
    ) -> bool:
        hour = parse_archive_hour(filename).isoformat()
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

    def list_hours_needing_mirror(self) -> list[sqlite3.Row]:
        cursor = self._conn.execute(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status IN ('pending', 'error')
            ORDER BY hour
            """
        )
        return cursor.fetchall()

    def mark_mirrored(
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
                    last_error = NULL
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
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET mirror_status = 'processing', last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )

    def mark_mirror_error(self, filename: str, error: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET mirror_status = 'error', last_error = ?
                WHERE filename = ?
                """,
                (error, filename),
            )

    def list_hours_needing_process(
        self,
        *,
        include_errors: bool = True,
    ) -> list[sqlite3.Row]:
        statuses = ("pending", "error") if include_errors else ("pending",)
        placeholders = ", ".join("?" for _ in statuses)
        cursor = self._conn.execute(
            f"""
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'ready' AND process_status IN ({placeholders})
            ORDER BY hour
            """,
            statuses,
        )
        return cursor.fetchall()

    def mark_processing(self, filename: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET process_status = 'processing', last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )

    def mark_process_error(self, filename: str, error: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET process_status = 'error', last_error = ?
                WHERE filename = ?
                """,
                (error, filename),
            )

    def replace_filtered_hours(
        self,
        filename: str,
        artifacts: list[FilteredHourArtifact],
    ) -> None:
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
                    last_error = NULL
                WHERE filename = ?
                """,
                (_utc_now(), _utc_now(), len(artifacts), filename),
            )

    def mark_sharded(
        self,
        filename: str,
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET process_status = 'ready',
                    processed_at = ?,
                    last_error = NULL
                WHERE filename = ?
                """,
                (_utc_now(), filename),
            )

    def mark_prebuilding(self, filename: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET prebuild_status = 'processing',
                    last_error = NULL
                WHERE filename = ?
                """,
                (filename,),
            )

    def mark_prebuild_error(self, filename: str, error: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                UPDATE archive_hours
                SET prebuild_status = 'error',
                    last_error = ?
                WHERE filename = ?
                """,
                (error, filename),
            )

    def mark_prebuilt(
        self,
        filename: str,
        *,
        filtered_artifact_count: int,
        artifacts: list[FilteredHourArtifact] | None = None,
    ) -> None:
        with self._conn:
            if artifacts:
                self._conn.execute(
                    "DELETE FROM filtered_hours WHERE filename = ?",
                    (filename,),
                )
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
                        for a in artifacts
                    ],
                )
            self._conn.execute(
                """
                UPDATE archive_hours
                SET prebuild_status = 'ready',
                    prebuilt_at = ?,
                    filtered_artifact_count = ?,
                    last_error = NULL
                WHERE filename = ?
                """,
                (_utc_now(), filtered_artifact_count, filename),
            )

    def list_hours_needing_filtered_prebuild(self) -> list[sqlite3.Row]:
        cursor = self._conn.execute(
            """
            SELECT *
            FROM archive_hours
            WHERE mirror_status = 'ready'
              AND process_status = 'ready'
              AND prebuild_status IN ('pending', 'error')
              AND local_path IS NOT NULL
            ORDER BY hour DESC
            """
        )
        return cursor.fetchall()

    def list_filtered_for_filename(self, filename: str) -> list[sqlite3.Row]:
        cursor = self._conn.execute(
            """
            SELECT *
            FROM filtered_hours
            WHERE filename = ?
            ORDER BY condition_id, token_id
            """,
            (filename,),
        )
        return cursor.fetchall()

    def get_filtered_hour(
        self,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> sqlite3.Row | None:
        cursor = self._conn.execute(
            """
            SELECT *
            FROM filtered_hours
            WHERE condition_id = ? AND token_id = ? AND filename = ?
            """,
            (condition_id, token_id, filename),
        )
        return cursor.fetchone()

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
        cursor = self._conn.execute(query, params)
        return cursor.fetchall()

    def stats(self) -> dict[str, int | str | None]:
        row = self._conn.execute(
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
        ).fetchone()
        filtered_hours = self._conn.execute(
            """
            SELECT COALESCE(
                NULLIF(SUM(filtered_artifact_count), 0),
                (SELECT COUNT(*) FROM filtered_hours)
            )
            FROM archive_hours
            """
        ).fetchone()[0]
        last_event_at = self._conn.execute(
            "SELECT MAX(created_at) FROM relay_events"
        ).fetchone()[0]
        last_error_at = self._conn.execute(
            "SELECT MAX(created_at) FROM relay_events WHERE level = 'ERROR'"
        ).fetchone()[0]
        return {
            "archive_hours": row["archive_hours"],
            "mirrored_hours": row["mirrored_hours"],
            "sharded_hours": row["sharded_hours"],
            "processed_hours": row["processed_hours"],
            "ready_to_process_hours": row["ready_to_process_hours"],
            "ready_to_prebuild_hours": row["ready_to_prebuild_hours"],
            "processing_hours": row["processing_hours"],
            "sharding_hours": row["sharding_hours"],
            "prebuilding_hours": row["prebuilding_hours"],
            "mirror_errors": row["mirror_errors"],
            "process_errors": row["shard_errors"],
            "prebuild_errors": row["prebuild_errors"],
            "filtered_hours": filtered_hours,
            "last_event_at": last_event_at,
            "last_error_at": last_error_at,
        }

    def queue_summary(self) -> dict[str, int | str | None]:
        row = self._conn.execute(
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
                MAX(CASE WHEN process_status = 'ready' THEN hour END) AS latest_sharded_hour,
                MAX(CASE WHEN prebuild_status = 'ready' THEN hour END) AS latest_processed_hour
            FROM archive_hours
            """
        ).fetchone()
        return dict(row)

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
        self._events_since_prune += 1
        prune_threshold = 1 if self._event_retention <= 250 else 250
        if self._events_since_prune >= prune_threshold:
            self.prune_events()

    def recent_events(self, *, limit: int = 100) -> list[sqlite3.Row]:
        cursor = self._conn.execute(
            """
            SELECT *
            FROM relay_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cursor.fetchall()
