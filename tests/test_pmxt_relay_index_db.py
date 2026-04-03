from __future__ import annotations

from pathlib import Path

from pmxt_relay.index_db import RelayIndex


def test_relay_index_events_and_queue_summary_are_mirror_only(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3", event_retention=2)
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
    assert queue["mirror_processing"] == 1
    assert queue["mirror_pending"] == 1
    assert "process_pending" not in queue

    index.log_event(level="INFO", event_type="first", message="first message")
    index.log_event(level="INFO", event_type="second", message="second message")
    index.log_event(level="ERROR", event_type="third", message="third message")
    index.prune_events()

    events = index.recent_events(limit=10)
    stats = index.stats()

    assert [event["event_type"] for event in events] == ["third", "second"]
    assert len(events) == 2
    assert stats["archive_hours"] == 2
    assert stats["mirrored_hours"] == 0
    assert stats["mirror_errors"] == 0
    assert "processed_hours" not in stats
    assert stats["last_event_at"] is not None
    assert stats["last_error_at"] is not None


def test_upsert_discovered_hour_is_idempotent(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
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
    row = index._conn.execute(  # noqa: SLF001
        "SELECT archive_page, source_url FROM archive_hours WHERE filename = ?",
        ("polymarket_orderbook_2026-03-21T12.parquet",),
    ).fetchone()
    assert row is not None
    assert row["archive_page"] == 2
    assert row["source_url"] == (
        "https://mirror.example.com/polymarket_orderbook_2026-03-21T12.parquet"
    )


def test_initialize_resets_stale_mirror_rows(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(
        filename,
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
        1,
    )
    index.mark_mirroring(filename)

    reopened = RelayIndex(tmp_path / "relay.sqlite3")
    reset_count = reopened.initialize(reset_inflight=True)
    queue = reopened.queue_summary()

    assert reset_count == 1
    assert queue["mirror_pending"] == 1
    assert queue["mirror_processing"] == 0


def test_register_local_raw_marks_hour_ready(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"

    changed = index.register_local_raw(
        filename,
        local_path="/srv/pmxt-relay/raw/2026/03/21/" + filename,
        content_length=123,
        source_url="https://r2.pmxt.dev/" + filename,
    )

    assert changed is True
    row = index._conn.execute(  # noqa: SLF001
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


def test_queue_summary_reports_latest_mirrored_filename(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
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
