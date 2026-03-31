from __future__ import annotations

from datetime import UTC
from datetime import datetime
from pathlib import Path

from pmxt_relay.index_db import FilteredHourArtifact, RelayIndex


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


def test_stats_include_processed_hours_per_hour_24h(tmp_path: Path, monkeypatch):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()

    recent_filename = "polymarket_orderbook_2026-03-21T12.parquet"
    stale_filename = "polymarket_orderbook_2026-03-20T12.parquet"
    for filename in (recent_filename, stale_filename):
        index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
        index.mark_mirrored(
            filename,
            local_path="/tmp/" + filename,
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.mark_sharded(filename)
        index.mark_prebuilt(filename, filtered_artifact_count=1)

    with index._conn:  # noqa: SLF001
        index._conn.execute(  # noqa: SLF001
            """
            UPDATE archive_hours
            SET prebuilt_at = ?
            WHERE filename = ?
            """,
            ("2026-03-22T11:30:00+00:00", recent_filename),
        )
        index._conn.execute(  # noqa: SLF001
            """
            UPDATE archive_hours
            SET prebuilt_at = ?
            WHERE filename = ?
            """,
            ("2026-03-20T10:00:00+00:00", stale_filename),
        )

    monkeypatch.setattr(
        "pmxt_relay.index_db._utc_now_datetime",
        lambda: datetime(2026, 3, 22, 12, 0, tzinfo=UTC),
    )

    stats = index.stats()

    assert stats["processed_hours_last_24h"] == 1
    assert stats["processed_hours_per_hour_24h"] == 0.04


def test_latest_prebuild_progress_returns_latest_progress_event(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()

    index.log_event(
        level="INFO",
        event_type="filtered_prebuild_progress",
        filename="polymarket_orderbook_2026-03-21T11.parquet",
        message="first prebuild progress",
        payload={"processed_rows": 128, "total_rows": 1024},
    )
    index.log_event(
        level="INFO",
        event_type="mirror_start",
        filename="polymarket_orderbook_2026-03-21T12.parquet",
        message="mirror is noisier than prebuild",
    )
    index.log_event(
        level="INFO",
        event_type="filtered_prebuild_progress",
        filename="polymarket_orderbook_2026-03-21T12.parquet",
        message="latest prebuild progress",
        payload={"processed_rows": 512, "total_rows": 2048},
    )

    progress = index.latest_prebuild_progress()

    assert progress is not None
    assert progress.filename == "polymarket_orderbook_2026-03-21T12.parquet"
    assert progress.processed_rows == 512
    assert progress.total_rows == 2048
    assert progress.created_at is not None


def test_error_count_deprioritizes_but_never_abandons(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)

    # Mirror errors: hour stays in queue no matter how many errors
    index.mark_mirror_error(filename, "timeout 1")
    assert len(index.list_hours_needing_mirror()) == 1
    index.mark_mirror_error(filename, "timeout 2")
    assert len(index.list_hours_needing_mirror()) == 1
    index.mark_mirror_error(filename, "timeout 3")
    assert len(index.list_hours_needing_mirror()) == 1  # still queued

    # Success resets error_count
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_process_error(filename, "corrupt 1")
    assert len(index.list_hours_needing_process()) == 1
    index.mark_process_error(filename, "corrupt 2")
    index.mark_process_error(filename, "corrupt 3")
    assert len(index.list_hours_needing_process()) == 1  # still queued

    # Prebuild errors — never abandoned
    index.mark_sharded(filename)  # resets error_count
    index.mark_prebuild_error(filename, "oom 1")
    assert len(index.list_hours_needing_filtered_prebuild()) == 1
    index.mark_prebuild_error(filename, "oom 2")
    index.mark_prebuild_error(filename, "oom 3")
    assert len(index.list_hours_needing_filtered_prebuild()) == 1  # still queued

    # Errored hours sort to the back (deprioritized, not abandoned)
    filename2 = "polymarket_orderbook_2026-03-21T13.parquet"
    index.upsert_discovered_hour(filename2, "https://r2.pmxt.dev/" + filename2, 1)
    index.mark_mirrored(
        filename2,
        local_path="/tmp/b",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename2)
    hours = index.list_hours_needing_filtered_prebuild()
    assert len(hours) == 2
    # Clean hour comes first (error_count=0), errored hour comes last
    assert hours[0]["filename"] == filename2
    assert hours[1]["filename"] == filename


def test_mark_prebuilt_registers_artifacts_in_filtered_hours(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)

    artifacts = [
        FilteredHourArtifact(
            filename=filename,
            hour="2026-03-21T12:00:00+00:00",
            condition_id="0x" + "ab" * 32,
            token_id="123",
            local_path="/srv/filtered/0xab/123/" + filename,
            row_count=100,
            byte_size=5000,
        ),
        FilteredHourArtifact(
            filename=filename,
            hour="2026-03-21T12:00:00+00:00",
            condition_id="0x" + "cd" * 32,
            token_id="456",
            local_path="/srv/filtered/0xcd/456/" + filename,
            row_count=50,
            byte_size=2500,
        ),
    ]
    index.mark_prebuilt(filename, filtered_artifact_count=2, artifacts=artifacts)

    # Verify filtered_hours table
    rows = index.list_filtered_for_filename(filename)
    assert len(rows) == 2
    assert rows[0]["condition_id"] == "0x" + "ab" * 32
    assert rows[0]["row_count"] == 100
    assert rows[1]["condition_id"] == "0x" + "cd" * 32

    # Verify the listing API works
    listed = index.list_filtered_hours("0x" + "ab" * 32, "123")
    assert len(listed) == 1
    assert listed[0]["filename"] == filename

    # Re-prebuilt replaces old artifacts
    new_artifacts = [artifacts[0]]
    index.mark_prebuilt(filename, filtered_artifact_count=1, artifacts=new_artifacts)
    rows = index.list_filtered_for_filename(filename)
    assert len(rows) == 1


def test_mark_prebuilt_without_artifacts_does_not_touch_filtered_hours(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)

    # mark_prebuilt without artifacts should NOT insert into filtered_hours
    index.mark_prebuilt(filename, filtered_artifact_count=42)
    rows = index.list_filtered_for_filename(filename)
    assert len(rows) == 0

    # But archive_hours should still show ready
    stats = index.stats()
    assert stats["processed_hours"] == 1


def test_event_pruning_respects_retention_limit(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3", event_retention=5)
    index.initialize()

    for i in range(10):
        index.log_event(level="INFO", event_type=f"evt_{i}", message=f"message {i}")

    events = index.recent_events(limit=100)
    assert len(events) == 5
    # Most recent events survive
    assert events[0]["event_type"] == "evt_9"
    assert events[-1]["event_type"] == "evt_5"


def test_mark_sharded_resets_error_count(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )

    # Accumulate process errors
    index.mark_process_error(filename, "err1")
    index.mark_process_error(filename, "err2")
    assert len(index.list_hours_needing_process()) == 1

    # mark_sharded resets error_count so the file is eligible for prebuild
    index.mark_sharded(filename)
    queue = index.queue_summary()
    assert queue["process_error"] == 0


def test_list_filtered_hours_with_hour_range(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()

    condition_id = "0x" + "ab" * 32
    token_id = "123"
    for hour_str in ["T10", "T12", "T14", "T16"]:
        filename = f"polymarket_orderbook_2026-03-21{hour_str}.parquet"
        index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
        index.mark_mirrored(
            filename,
            local_path="/tmp/a",
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.mark_sharded(filename)
        artifacts = [
            FilteredHourArtifact(
                filename=filename,
                hour=f"2026-03-21{hour_str}:00:00+00:00",
                condition_id=condition_id,
                token_id=token_id,
                local_path=f"/srv/filtered/{condition_id}/{token_id}/{filename}",
                row_count=100,
                byte_size=5000,
            ),
        ]
        index.mark_prebuilt(filename, filtered_artifact_count=1, artifacts=artifacts)

    # All hours
    all_hours = index.list_filtered_hours(condition_id, token_id)
    assert len(all_hours) == 4

    # Range filter
    ranged = index.list_filtered_hours(
        condition_id,
        token_id,
        start_hour="2026-03-21T12:00:00+00:00",
        end_hour="2026-03-21T14:00:00+00:00",
    )
    assert len(ranged) == 2

    # Non-existent market returns empty
    empty = index.list_filtered_hours("0xdeadbeef", "999")
    assert len(empty) == 0


def test_replace_filtered_hours_sets_all_statuses_ready(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)

    artifacts = [
        FilteredHourArtifact(
            filename=filename,
            hour="2026-03-21T12:00:00+00:00",
            condition_id="0x" + "ab" * 32,
            token_id="123",
            local_path="/srv/filtered/0xab/123/" + filename,
            row_count=100,
            byte_size=5000,
        ),
    ]
    index.replace_filtered_hours(filename, artifacts)

    queue = index.queue_summary()
    assert queue["process_error"] == 0
    assert queue["prebuild_error"] == 0
    rows = index.list_filtered_for_filename(filename)
    assert len(rows) == 1
    stats = index.stats()
    assert stats["processed_hours"] == 1


def test_get_filtered_hour_returns_single_row(tmp_path: Path):
    index = RelayIndex(tmp_path / "relay.sqlite3")
    index.initialize()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    condition_id = "0x" + "ab" * 32
    token_id = "123"
    index.upsert_discovered_hour(filename, "https://r2.pmxt.dev/" + filename, 1)
    index.mark_mirrored(
        filename,
        local_path="/tmp/a",
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)
    artifacts = [
        FilteredHourArtifact(
            filename=filename,
            hour="2026-03-21T12:00:00+00:00",
            condition_id=condition_id,
            token_id=token_id,
            local_path="/srv/filtered/test/" + filename,
            row_count=50,
            byte_size=2500,
        ),
    ]
    index.mark_prebuilt(filename, filtered_artifact_count=1, artifacts=artifacts)

    row = index.get_filtered_hour(condition_id, token_id, filename)
    assert row is not None
    assert row["row_count"] == 50
    assert row["byte_size"] == 2500

    # Non-existent returns None
    assert index.get_filtered_hour("0xdead", "999", filename) is None
