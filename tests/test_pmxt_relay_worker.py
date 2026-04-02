from __future__ import annotations

from dataclasses import replace
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq

from pmxt_relay.config import RelayConfig
from pmxt_relay.processor import ProcessedHourResult
from pmxt_relay.storage import raw_relative_path
from pmxt_relay.worker import RelayWorker


def _make_config(tmp_path: Path) -> RelayConfig:
    return RelayConfig(
        data_dir=tmp_path,
        bind_host="127.0.0.1",
        bind_port=8080,
        public_base_url=None,
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        raw_base_url="https://r2.pmxt.dev",
        poll_interval_secs=900,
        http_timeout_secs=30,
        archive_stale_pages=3,
        archive_max_pages=None,
        duckdb_threads=1,
        duckdb_memory_limit="1GB",
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        api_list_max_hours=2000,
    )


class _FakeResponse(BytesIO):
    def __init__(
        self,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(payload)
        self.headers = headers or {}

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        self.close()
        return False


def test_mirror_hour_falls_back_to_get_when_head_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _make_config(tmp_path)
    worker = RelayWorker(config, reset_inflight=False)
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    source_url = f"https://r2.pmxt.dev/{filename}"
    worker._index.upsert_discovered_hour(filename, source_url, 1)  # noqa: SLF001
    row = worker._index.list_hours_needing_mirror()[0]  # noqa: SLF001
    requested_methods: list[str] = []

    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
        assert timeout == config.http_timeout_secs
        requested_methods.append(request.get_method())
        if request.get_method() == "HEAD":
            raise HTTPError(
                request.full_url,
                403,
                "Forbidden",
                hdrs=None,
                fp=None,
            )
        return _FakeResponse(
            b"raw-payload",
            headers={
                "ETag": '"abc123"',
                "Last-Modified": "Sun, 21 Mar 2026 12:59:59 GMT",
                "Content-Length": "11",
            },
        )

    monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

    worker._mirror_hour(row)  # noqa: SLF001

    raw_path = config.raw_root / raw_relative_path(filename)
    assert raw_path.read_bytes() == b"raw-payload"
    assert requested_methods == ["HEAD", "GET"]

    stats = worker._index.stats()  # noqa: SLF001
    assert stats["archive_hours"] == 1
    assert stats["mirrored_hours"] == 1

    events = worker._index.recent_events(limit=10)  # noqa: SLF001
    assert any(row["event_type"] == "mirror_head_error" for row in events)
    assert any(row["event_type"] == "mirror_complete" for row in events)


def test_progress_reporting_requires_large_row_delta_or_completion(
    tmp_path: Path,
) -> None:
    worker = RelayWorker(_make_config(tmp_path), reset_inflight=False)

    assert worker._should_report_progress(  # noqa: SLF001
        processed_rows=0,
        total_rows=10,
        last_reported_rows=-1,
    )
    assert not worker._should_report_progress(  # noqa: SLF001
        processed_rows=123_456,
        total_rows=10_000_000,
        last_reported_rows=0,
    )
    assert worker._should_report_progress(  # noqa: SLF001
        processed_rows=5_000_000,
        total_rows=10_000_000,
        last_reported_rows=0,
    )
    assert worker._should_report_progress(  # noqa: SLF001
        processed_rows=10_000_000,
        total_rows=10_000_000,
        last_reported_rows=5_000_000,
    )


def test_clickhouse_backend_never_requests_processed_or_filtered_file_writes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = replace(_make_config(tmp_path), filtered_store_backend="clickhouse")
    config.ensure_directories()
    assert not config.processed_root.exists()
    assert not config.filtered_root.exists()
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path = config.raw_root / raw_relative_path(filename)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a"],
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes","seq":1}'],
            }
        ),
        raw_path,
    )

    class _FakeClickHouse:
        def __init__(self, config: RelayConfig) -> None:
            self.config = config
            self.reset_calls: list[str] = []
            self.completed_calls: list[tuple[str, str, int, int]] = []

        def ensure_schema(self) -> None:
            return None

        def backfill_completed_hour(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
        ) -> None:
            return None

        def hour_exists(self, filename: str) -> bool:
            return False

        def hour_data_exists(self, filename: str) -> bool:
            return True

        def hour_group_count(self, filename: str) -> int:
            return 0

        def reset_hour(self, filename: str) -> None:
            self.reset_calls.append(filename)

        def insert_batch(self, *, filename: str, hour: str, batch) -> None:  # type: ignore[no-untyped-def]
            return None

        def mark_hour_complete(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
            filtered_row_count: int,
        ) -> None:
            self.completed_calls.append(
                (filename, hour, filtered_group_count, filtered_row_count)
            )

    monkeypatch.setattr("pmxt_relay.worker.ClickHouseRelay", _FakeClickHouse)

    worker = RelayWorker(config, reset_inflight=False, skip_prebuild=False)
    worker._index.upsert_discovered_hour(  # noqa: SLF001
        filename,
        f"https://r2.pmxt.dev/{filename}",
        1,
    )
    worker._index.mark_mirrored(  # noqa: SLF001
        filename,
        local_path=str(raw_path),
        etag=None,
        content_length=None,
        last_modified=None,
    )

    captured_kwargs: dict[str, object] = {}

    def fake_process_hour(*args, **kwargs):  # type: ignore[no-untyped-def]
        captured_kwargs.update(kwargs)
        return ProcessedHourResult(
            artifacts=[],
            total_filtered_rows=1,
            filtered_group_count=1,
        )

    monkeypatch.setattr(worker._processor, "process_hour", fake_process_hour)  # noqa: SLF001

    assert worker._process_pending_hours(limit=1) == 1  # noqa: SLF001
    assert captured_kwargs["skip_filtered"] is True
    assert captured_kwargs["write_processed"] is False
    assert worker._clickhouse.reset_calls == []  # type: ignore[union-attr]  # noqa: SLF001
    assert worker._clickhouse.completed_calls == [  # type: ignore[union-attr]  # noqa: SLF001
        (filename, "2026-03-21T12:00:00+00:00", 1, 1)
    ]


def test_clickhouse_backend_resets_failed_hour_before_retry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = replace(_make_config(tmp_path), filtered_store_backend="clickhouse")
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path = config.raw_root / raw_relative_path(filename)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a"],
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes","seq":1}'],
            }
        ),
        raw_path,
    )

    class _FakeClickHouse:
        def __init__(self, config: RelayConfig) -> None:
            self.reset_calls: list[str] = []

        def ensure_schema(self) -> None:
            return None

        def backfill_completed_hour(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
        ) -> None:
            return None

        def hour_exists(self, filename: str) -> bool:
            return False

        def hour_data_exists(self, filename: str) -> bool:
            return True

        def hour_group_count(self, filename: str) -> int:
            return 0

        def reset_hour(self, filename: str) -> None:
            self.reset_calls.append(filename)

        def insert_batch(self, *, filename: str, hour: str, batch) -> None:  # type: ignore[no-untyped-def]
            return None

        def mark_hour_complete(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
            filtered_row_count: int,
        ) -> None:
            return None

    monkeypatch.setattr("pmxt_relay.worker.ClickHouseRelay", _FakeClickHouse)

    worker = RelayWorker(config, reset_inflight=False, skip_prebuild=False)
    worker._index.upsert_discovered_hour(  # noqa: SLF001
        filename,
        f"https://r2.pmxt.dev/{filename}",
        1,
    )
    worker._index.mark_mirrored(  # noqa: SLF001
        filename,
        local_path=str(raw_path),
        etag=None,
        content_length=None,
        last_modified=None,
    )
    worker._index.mark_process_error(filename, "timeout")  # noqa: SLF001

    monkeypatch.setattr(
        worker._processor,
        "process_hour",
        lambda *args, **kwargs: ProcessedHourResult(  # type: ignore[no-untyped-def]
            artifacts=[],
            total_filtered_rows=1,
            filtered_group_count=1,
        ),
    )

    assert worker._process_pending_hours(limit=1) == 1  # noqa: SLF001
    assert worker._clickhouse.reset_calls == [filename]  # type: ignore[union-attr]  # noqa: SLF001


def test_clickhouse_backend_resets_interrupted_hour_after_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = replace(_make_config(tmp_path), filtered_store_backend="clickhouse")
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path = config.raw_root / raw_relative_path(filename)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a"],
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes","seq":1}'],
            }
        ),
        raw_path,
    )

    class _FakeClickHouse:
        def __init__(self, config: RelayConfig) -> None:
            self.reset_calls: list[str] = []

        def ensure_schema(self) -> None:
            return None

        def backfill_completed_hour(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
        ) -> None:
            return None

        def hour_exists(self, filename: str) -> bool:
            return False

        def hour_data_exists(self, filename: str) -> bool:
            return False

        def hour_group_count(self, filename: str) -> int:
            return 0

        def reset_hour(self, filename: str) -> None:
            self.reset_calls.append(filename)

        def insert_batch(self, *, filename: str, hour: str, batch) -> None:  # type: ignore[no-untyped-def]
            return None

        def mark_hour_complete(
            self,
            *,
            filename: str,
            hour: str,
            filtered_group_count: int,
            filtered_row_count: int,
        ) -> None:
            return None

    monkeypatch.setattr("pmxt_relay.worker.ClickHouseRelay", _FakeClickHouse)

    worker = RelayWorker(config, reset_inflight=False, skip_prebuild=False)
    worker._index.upsert_discovered_hour(  # noqa: SLF001
        filename,
        f"https://r2.pmxt.dev/{filename}",
        1,
    )
    worker._index.mark_mirrored(  # noqa: SLF001
        filename,
        local_path=str(raw_path),
        etag=None,
        content_length=None,
        last_modified=None,
    )
    worker._index.mark_processing(filename)  # noqa: SLF001

    restarted_worker = RelayWorker(config, reset_inflight=True, skip_prebuild=False)
    monkeypatch.setattr(
        restarted_worker._processor,
        "process_hour",
        lambda *args, **kwargs: ProcessedHourResult(  # type: ignore[no-untyped-def]
            artifacts=[],
            total_filtered_rows=1,
            filtered_group_count=1,
        ),
    )

    assert restarted_worker._process_pending_hours(limit=1) == 1  # noqa: SLF001
    assert restarted_worker._clickhouse.reset_calls == [filename]  # type: ignore[union-attr]  # noqa: SLF001


def test_worker_adopts_existing_raw_files_into_mirrored_state(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path = config.raw_root / raw_relative_path(filename)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"raw-payload")

    worker = RelayWorker(config, reset_inflight=False, skip_prebuild=True)
    worker._index.upsert_discovered_hour(  # noqa: SLF001
        filename,
        f"https://r2.pmxt.dev/{filename}",
        1,
    )

    adopted = worker._adopt_local_raw_hours()  # noqa: SLF001

    row = worker._index._conn.execute(  # noqa: SLF001
        """
        SELECT mirror_status, local_path, content_length
        FROM archive_hours
        WHERE filename = ?
        """,
        (filename,),
    ).fetchone()

    assert adopted == 1
    assert row["mirror_status"] == "ready"
    assert row["local_path"] == str(raw_path)
    assert row["content_length"] == len(b"raw-payload")
