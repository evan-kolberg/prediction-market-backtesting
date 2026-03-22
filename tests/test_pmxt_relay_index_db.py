from __future__ import annotations

from pathlib import Path

from pmxt_relay.index_db import RelayIndex


def test_relay_index_events_and_queue_summary(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3", event_retention=2)
    reset_counts = index.initialize()
    assert reset_counts == (0, 0, 0)

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
    index.mark_processing("polymarket_orderbook_2026-03-21T13.parquet")

    queue = index.queue_summary()
    assert queue["mirror_processing"] == 1
    assert queue["process_processing"] == 1
    assert queue["prebuild_processing"] == 0
    assert queue["process_ready"] == 0

    index.log_event(level="INFO", event_type="first", message="first message")
    index.log_event(level="INFO", event_type="second", message="second message")
    index.log_event(level="ERROR", event_type="third", message="third message")

    events = index.recent_events(limit=10)
    stats = index.stats()

    assert [event["event_type"] for event in events] == ["third", "second"]
    assert len(events) == 2
    assert stats["archive_hours"] == 2
    assert stats["ready_to_process_hours"] == 0
    assert stats["ready_to_prebuild_hours"] == 0
    assert stats["processing_hours"] == 1
    assert stats["mirror_errors"] == 0
    assert stats["process_errors"] == 0
    assert stats["prebuild_errors"] == 0
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
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
        2,
    )

    assert first_insert is True
    assert second_insert is False
    stats = index.stats()
    assert stats["archive_hours"] == 1


def test_initialize_resets_stale_inflight_rows(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
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
    index.mark_processing("polymarket_orderbook_2026-03-21T13.parquet")

    reopened = RelayIndex(tmp_path / "relay.sqlite3")
    reset_counts = reopened.initialize(reset_inflight=True)
    queue = reopened.queue_summary()

    assert reset_counts == (1, 1, 0)
    assert queue["mirror_pending"] == 2
    assert queue["process_pending"] == 2
    assert queue["mirror_processing"] == 0
    assert queue["process_processing"] == 0


def test_initialize_can_reset_prebuild_inflight_separately(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(
        filename,
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
        1,
    )
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)
    index.mark_prebuilding(filename)

    reopened = RelayIndex(tmp_path / "relay.sqlite3")
    reset_counts = reopened.initialize(
        reset_inflight=True,
        reset_mirror_inflight=False,
        reset_process_inflight=False,
        reset_prebuild_inflight=True,
    )

    assert reset_counts == (0, 0, 1)
    queue = reopened.queue_summary()
    assert queue["prebuild_pending"] == 1
    assert queue["prebuild_processing"] == 0


def test_list_hours_needing_process_excludes_already_processing(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
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
    index.mark_mirrored(
        "polymarket_orderbook_2026-03-21T12.parquet",
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_mirrored(
        "polymarket_orderbook_2026-03-21T13.parquet",
        local_path="/tmp/b",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_processing("polymarket_orderbook_2026-03-21T12.parquet")
    index.mark_process_error("polymarket_orderbook_2026-03-21T13.parquet", "boom")

    rows = index.list_hours_needing_process()

    assert [row["filename"] for row in rows] == [
        "polymarket_orderbook_2026-03-21T13.parquet"
    ]


def test_list_hours_needing_process_pending_only_skips_errors(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
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
    index.mark_mirrored(
        "polymarket_orderbook_2026-03-21T12.parquet",
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_mirrored(
        "polymarket_orderbook_2026-03-21T13.parquet",
        local_path="/tmp/b",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_process_error("polymarket_orderbook_2026-03-21T12.parquet", "boom")

    rows = index.list_hours_needing_process(include_errors=False)

    assert [row["filename"] for row in rows] == [
        "polymarket_orderbook_2026-03-21T13.parquet"
    ]


def test_mark_prebuilt_tracks_filtered_artifact_count(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(
        filename,
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet",
        1,
    )
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)

    rows = index.list_hours_needing_filtered_prebuild()
    assert [row["filename"] for row in rows] == [filename]

    index.mark_prebuilt(filename, filtered_artifact_count=42)

    stats = index.stats()

    assert stats["sharded_hours"] == 1
    assert stats["processed_hours"] == 1
    assert stats["ready_to_prebuild_hours"] == 0
    assert stats["filtered_hours"] == 42
    assert index.list_hours_needing_filtered_prebuild() == []
