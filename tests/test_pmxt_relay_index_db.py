from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pmxt_relay.index_db import RelayIndex


def test_relay_index_events_and_queue_summary_are_mirror_only(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3", event_retention=2) as index:
        reset_count = index.initialize()
        assert reset_count == 0

        index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T12.parquet",
            "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
            1,
        )
        index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T13.parquet",
            "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T13.parquet",
            1,
        )
        index.mark_mirroring("polymarket_orderbook_2026-03-21T12.parquet")

        queue = index.queue_summary()
        assert queue["mirror_active"] == 1
        assert queue["mirror_pending"] == 1
        assert queue["mirror_retry_due"] == 0
        assert queue["mirror_retry_waiting"] == 0
        assert queue["mirror_quarantined"] == 0
        assert sorted(queue.keys()) == [
            "latest_mirrored_filename",
            "latest_mirrored_hour",
            "mirror_active",
            "mirror_error",
            "mirror_pending",
            "mirror_quarantined",
            "mirror_retry_due",
            "mirror_retry_waiting",
            "next_retry_at",
        ]

        index.log_event(
            level="INFO",
            event_type="first",
            message="first message",
        )

        index.log_event(
            level="INFO",
            event_type="second",
            message="second message",
        )

        index.log_event(
            level="ERROR",
            event_type="third",
            message="third message",
        )

        index.prune_events()

        events = index.recent_events(limit=10)
        stats = index.stats(now=datetime(2026, 3, 21, 13, 30, tzinfo=timezone.utc))

        assert [event["event_type"] for event in events] == ["third", "second"]
        assert len(events) == 2
        assert stats["archive_hours"] == 2
        assert stats["mirrored_hours"] == 0
        assert stats["mirror_errors"] == 0
        assert stats["mirror_quarantined"] == 0
        assert sorted(stats.keys()) == [
            "archive_hours",
            "last_error_at",
            "last_event_at",
            "mirror_errors",
            "mirror_quarantined",
            "mirrored_hours",
        ]
        assert stats["last_event_at"] is not None
        assert stats["last_error_at"] is not None


def test_upsert_discovered_hour_is_idempotent(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()

        first_insert = index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T12.parquet",
            "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
            1,
        )
        second_insert = index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T12.parquet",
            "https://mirror.example.com/polymarket_orderbook_2026-03-21T12.parquet",
            2,
        )

        assert first_insert is True
        assert second_insert is False
        row = index._conn.execute(
            "SELECT archive_page, source_url FROM archive_hours WHERE filename = ?",
            ("polymarket_orderbook_2026-03-21T12.parquet",),
        ).fetchone()
        assert row is not None
        assert row["archive_page"] == 2
        assert row["source_url"] == (
            "https://mirror.example.com/polymarket_orderbook_2026-03-21T12.parquet"
        )


def test_initialize_resets_stale_mirror_rows(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        index.upsert_discovered_hour(
            filename, "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet", 1
        )
        index.mark_mirroring(filename)

    with RelayIndex(tmp_path / "relay.sqlite3") as reopened:
        reset_count = reopened.initialize(reset_inflight=True)
        queue = reopened.queue_summary()

        assert reset_count == 1
        assert queue["mirror_pending"] == 1
        assert queue["mirror_active"] == 0


def test_register_local_raw_marks_hour_ready(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"

        changed = index.register_local_raw(
            filename,
            local_path="/srv/pmxt-relay/raw/2026/03/21/" + filename,
            content_length=123,
            source_url="https://r2.pmxt.dev/" + filename,
        )

        assert changed is True
        row = index._conn.execute(
            """
            SELECT local_path, content_length, mirror_status, mirrored_at
            FROM archive_hours
            WHERE filename = ?
            """,
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["mirror_status"] == "ready"
        assert row["content_length"] == 123
        assert row["mirrored_at"] is not None


def test_relay_index_context_manager_closes_connection(tmp_path: Path):
    db_path = tmp_path / "relay.sqlite3"
    with RelayIndex(db_path) as index:
        index.initialize()
        conn = index._conn

    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_queue_summary_reports_latest_mirrored_filename(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filenames = [
            "polymarket_orderbook_2026-03-21T12.parquet",
            "polymarket_orderbook_2026-03-21T13.parquet",
        ]
        for filename in filenames:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
            index.mark_mirrored(
                filename,
                local_path=f"/srv/pmxt-relay/raw/{filename}",
                etag=None,
                content_length=1,
                last_modified=None,
            )

        queue = index.queue_summary()

        assert queue["latest_mirrored_filename"] == filenames[-1]
        assert queue["latest_mirrored_hour"] == "2026-03-21T13:00:00+00:00"


def test_error_rows_back_off_until_next_retry(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        retry_at = "2026-03-21T13:00:00+00:00"
        index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        index.mark_mirror_retry(
            filename, error="transient upstream failure", next_retry_at=retry_at
        )

        due_now = index.list_hours_needing_mirror(
            now=datetime(2026, 3, 21, 12, 30, tzinfo=timezone.utc)
        )
        due_later = index.list_hours_needing_mirror(
            now=datetime(2026, 3, 21, 13, 30, tzinfo=timezone.utc)
        )
        queue_now = index.queue_summary(now=datetime(2026, 3, 21, 12, 30, tzinfo=timezone.utc))

        assert due_now == []
        assert [row["filename"] for row in due_later] == [filename]
        assert queue_now["mirror_error"] == 1
        assert queue_now["mirror_retry_due"] == 0
        assert queue_now["mirror_retry_waiting"] == 1
        assert queue_now["next_retry_at"] == retry_at


def test_quarantined_rows_count_as_errors_until_their_retry_window(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        retry_at = "2026-03-21T14:00:00+00:00"
        index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        index.mark_mirror_quarantined(
            filename, error="HTTP Error 404: Not Found", next_retry_at=retry_at
        )

        stats = index.stats()
        queue_now = index.queue_summary(now=datetime(2026, 3, 21, 13, 0, tzinfo=timezone.utc))
        due_later = index.list_hours_needing_mirror(
            now=datetime(2026, 3, 21, 14, 30, tzinfo=timezone.utc)
        )

        assert (
            index.list_hours_needing_mirror(now=datetime(2026, 3, 21, 13, 0, tzinfo=timezone.utc))
            == []
        )
        assert [row["filename"] for row in due_later] == [filename]
        assert stats["mirror_errors"] == 1
        assert stats["mirror_quarantined"] == 1
        assert queue_now["mirror_error"] == 1
        assert queue_now["mirror_quarantined"] == 1
        assert queue_now["mirror_retry_waiting"] == 1


def test_initialize_tolerates_duplicate_column_race_during_schema_upgrade(tmp_path: Path):
    db_path = tmp_path / "relay.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE archive_hours (
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

        CREATE TABLE relay_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            filename TEXT,
            message TEXT NOT NULL,
            payload_json TEXT
        );
        """
    )
    conn.close()

    with RelayIndex(db_path) as index:
        real_conn = index._conn

        class _DuplicateColumnRaceConn:
            def __init__(self, wrapped: sqlite3.Connection) -> None:
                self._wrapped = wrapped
                self._raised = False

            def close(self) -> None:
                self._wrapped.close()

            def executescript(self, sql: str):  # type: ignore[no-untyped-def]
                return self._wrapped.executescript(sql)

            def execute(self, sql: str, params=()):  # type: ignore[no-untyped-def]
                if (
                    not self._raised
                    and sql.strip() == "ALTER TABLE archive_hours ADD COLUMN last_error_at TEXT"
                ):
                    self._wrapped.execute(sql, params)
                    self._raised = True
                    raise sqlite3.OperationalError("duplicate column name: last_error_at")
                return self._wrapped.execute(sql, params)

        index._conn = _DuplicateColumnRaceConn(real_conn)  # type: ignore[assignment]
        index.initialize(apply_maintenance=False)

        columns = {
            row[1] for row in real_conn.execute("PRAGMA table_info(archive_hours)").fetchall()
        }
        assert "last_error_at" in columns
        assert "next_retry_at" in columns


def test_list_hours_needing_verification_returns_oldest_first(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filenames = [
            "polymarket_orderbook_2026-03-21T12.parquet",
            "polymarket_orderbook_2026-03-21T13.parquet",
            "polymarket_orderbook_2026-03-21T14.parquet",
        ]
        for filename in filenames:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
            index.mark_mirrored(
                filename,
                local_path=f"/srv/pmxt-relay/raw/{filename}",
                etag=None,
                content_length=1,
                last_modified=None,
            )
        with index._conn:
            index._conn.execute(
                "UPDATE archive_hours SET last_verified_at = ? WHERE filename = ?",
                ("2026-03-21T12:30:00+00:00", filenames[1]),
            )
            index._conn.execute(
                "UPDATE archive_hours SET last_verified_at = ? WHERE filename = ?",
                ("2026-03-21T13:30:00+00:00", filenames[2]),
            )

        rows = index.list_hours_needing_verification(batch_size=3)
        limited_rows = index.list_hours_needing_verification(batch_size=2)

        assert [row["filename"] for row in rows] == filenames
        assert [row["filename"] for row in limited_rows] == filenames[:2]


def test_mark_verified_sets_timestamp(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        index.mark_mirrored(
            filename,
            local_path=f"/srv/pmxt-relay/raw/{filename}",
            etag=None,
            content_length=1,
            last_modified=None,
        )

        index.mark_verified(filename)

        row = index._conn.execute(
            "SELECT last_verified_at FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["last_verified_at"] is not None


def test_mark_needs_remirror_resets_to_pending(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        index.mark_mirrored(
            filename,
            local_path=f"/srv/pmxt-relay/raw/{filename}",
            etag="abc",
            content_length=1,
            last_modified=None,
        )
        index.mark_verified(filename)

        index.mark_needs_remirror(filename)

        row = index._conn.execute(
            """
            SELECT mirror_status, last_verified_at, last_error
            FROM archive_hours
            WHERE filename = ?
            """,
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["mirror_status"] == "pending"
        assert row["last_verified_at"] is None
        assert row["last_error"] == "upstream content changed"


def test_count_missing_hours_only_counts_undownloadable_rows(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        ready = "polymarket_orderbook_2026-03-21T12.parquet"
        empty = "polymarket_orderbook_2026-03-21T13.parquet"
        quarantined = "polymarket_orderbook_2026-03-21T14.parquet"
        pending = "polymarket_orderbook_2026-03-21T15.parquet"
        for filename in [ready, empty, quarantined, pending]:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        for filename in [ready, empty]:
            index.mark_mirrored(
                filename,
                local_path=f"/srv/pmxt-relay/raw/{filename}",
                etag=None,
                content_length=1,
                last_modified=None,
            )
        index.update_row_count(empty, 0)
        index.mark_mirror_quarantined(
            quarantined, error="HTTP 404", next_retry_at="2026-03-21T16:00:00+00:00"
        )

        assert index.count_missing_hours() == 2


def test_stats_archive_hours_are_elapsed_wall_clock_hours(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        earliest = "polymarket_orderbook_2026-03-21T12.parquet"
        later = "polymarket_orderbook_2026-03-21T13.parquet"
        for filename in [earliest, later]:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
        index._conn.execute(
            "UPDATE archive_hours SET hour = ? WHERE filename = ?",
            ("2026-03-21T12:45:00Z", earliest),
        )
        index._conn.commit()

        stats = index.stats(now=datetime(2026, 3, 21, 15, 30, tzinfo=timezone.utc))

        assert stats["archive_hours"] == 4


def test_stats_mirrored_hours_exclude_known_empty_rows(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        empty = "polymarket_orderbook_2026-03-21T12.parquet"
        unknown = "polymarket_orderbook_2026-03-21T13.parquet"
        nonempty = "polymarket_orderbook_2026-03-21T14.parquet"
        for filename in [empty, unknown, nonempty]:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
            index.mark_mirrored(
                filename,
                local_path=f"/srv/pmxt-relay/raw/{filename}",
                etag=None,
                content_length=1,
                last_modified=None,
            )
        index.update_row_count(empty, 0)
        index.update_row_count(nonempty, 100)

        stats = index.stats(now=datetime(2026, 3, 21, 14, 30, tzinfo=timezone.utc))

        assert stats["mirrored_hours"] == 2


def test_count_empty_hours(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filenames = [
            "polymarket_orderbook_2026-03-21T12.parquet",
            "polymarket_orderbook_2026-03-21T13.parquet",
            "polymarket_orderbook_2026-03-21T14.parquet",
        ]
        for filename in filenames:
            index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
            index.mark_mirrored(
                filename,
                local_path=f"/srv/pmxt-relay/raw/{filename}",
                etag=None,
                content_length=1,
                last_modified=None,
            )
        index.update_row_count(filenames[0], 0)
        index.update_row_count(filenames[1], 100)

        assert index.count_empty_hours() == 1


def test_update_row_count(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)

        index.update_row_count(filename, 42)

        row = index._conn.execute(
            "SELECT row_count FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["row_count"] == 42


def test_schema_migration_adds_verification_columns(tmp_path: Path):
    with RelayIndex(tmp_path / "relay.sqlite3") as index:
        index.initialize()

        columns = {
            row[1] for row in index._conn.execute("PRAGMA table_info(archive_hours)").fetchall()
        }
        assert "last_verified_at" in columns
        assert "row_count" in columns
