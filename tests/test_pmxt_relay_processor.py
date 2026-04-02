from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import pmxt_relay.processor as processor_module
from pmxt_relay.config import RelayConfig
from pmxt_relay.processor import RelayHourProcessor
from pmxt_relay.processor import materialize_filtered_hour
from pmxt_relay.storage import processed_relative_path


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


def test_hour_processor_writes_one_processed_shard_per_hour(tmp_path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": [
                    "condition-a",
                    "condition-a",
                    "condition-a",
                    "condition-b",
                ],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "trade",
                    "price_change",
                ],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-yes","seq":2}',
                    '{"token_id":"token-yes","seq":3}',
                    '{"token_id":"token-no","seq":4}',
                ],
            }
        ),
        raw_path,
    )

    processor = RelayHourProcessor(config)
    result = processor.process_hour(
        "polymarket_orderbook_2026-03-21T12.parquet",
        raw_path,
    )

    assert {
        (artifact.condition_id, artifact.token_id, artifact.row_count)
        for artifact in result.artifacts
    } == {
        ("condition-a", "token-yes", 2),
        ("condition-b", "token-no", 1),
    }

    processed_path = config.processed_root / processed_relative_path(
        "polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert pq.read_table(processed_path).to_pylist() == [
        {
            "market_id": "condition-a",
            "token_id": "token-yes",
            "update_type": "book_snapshot",
            "data": '{"token_id":"token-yes","seq":1}',
        },
        {
            "market_id": "condition-a",
            "token_id": "token-yes",
            "update_type": "price_change",
            "data": '{"token_id":"token-yes","seq":2}',
        },
        {
            "market_id": "condition-b",
            "token_id": "token-no",
            "update_type": "price_change",
            "data": '{"token_id":"token-no","seq":4}',
        },
    ]
    filtered_yes_path = (
        config.filtered_root
        / "condition-a"
        / "token-yes"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    filtered_no_path = (
        config.filtered_root
        / "condition-b"
        / "token-no"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert pq.read_table(filtered_yes_path).to_pylist() == [
        {
            "update_type": "book_snapshot",
            "data": '{"token_id":"token-yes","seq":1}',
        },
        {
            "update_type": "price_change",
            "data": '{"token_id":"token-yes","seq":2}',
        },
    ]
    assert pq.read_table(filtered_no_path).to_pylist() == [
        {
            "update_type": "price_change",
            "data": '{"token_id":"token-no","seq":4}',
        }
    ]


def test_hour_processor_can_stream_filtered_batches_without_writing_processed_shard(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    config.ensure_directories()
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T15.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": [
                    "condition-a",
                    "condition-a",
                    "condition-b",
                    "condition-b",
                ],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "price_change",
                    "trade",
                ],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-yes","seq":2}',
                    '{"token_id":"token-no","seq":3}',
                    '{"token_id":"token-no","seq":4}',
                ],
            }
        ),
        raw_path,
    )

    captured_batches: list[pa.RecordBatch] = []
    processor = RelayHourProcessor(config)
    result = processor.process_hour(
        "polymarket_orderbook_2026-03-21T15.parquet",
        raw_path,
        skip_filtered=True,
        write_processed=False,
        batch_sink=lambda hour, batch: captured_batches.append(batch),
    )

    assert result.artifacts == []
    assert result.total_filtered_rows == 3
    assert result.filtered_group_count == 2
    assert captured_batches
    assert sum(batch.num_rows for batch in captured_batches) == 3
    assert all("relay_row_index" in batch.schema.names for batch in captured_batches)
    processed_path = config.processed_root / processed_relative_path(
        "polymarket_orderbook_2026-03-21T15.parquet"
    )
    assert not processed_path.exists()


def test_materialized_filtered_hour_preserves_original_row_order(tmp_path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T13.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": [
                    "condition-a",
                    "condition-b",
                    "condition-a",
                    "condition-b",
                    "condition-a",
                ],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "price_change",
                    "price_change",
                    "price_change",
                ],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-no","seq":2}',
                    '{"token_id":"token-yes","seq":3}',
                    '{"token_id":"token-no","seq":4}',
                    '{"token_id":"token-yes","seq":5}',
                ],
            }
        ),
        raw_path,
        row_group_size=2,
    )

    processor = RelayHourProcessor(config)
    processor.process_hour(
        "polymarket_orderbook_2026-03-21T13.parquet",
        raw_path,
    )

    processed_path = config.processed_root / processed_relative_path(
        "polymarket_orderbook_2026-03-21T13.parquet"
    )
    filtered_path = (
        config.filtered_root
        / "condition-a"
        / "token-yes"
        / "polymarket_orderbook_2026-03-21T13.parquet"
    )
    materialize_filtered_hour(
        processed_path,
        filtered_path,
        condition_id="condition-a",
        token_id="token-yes",
    )

    assert pq.read_table(filtered_path).to_pylist() == [
        {"update_type": "book_snapshot", "data": '{"token_id":"token-yes","seq":1}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes","seq":3}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes","seq":5}'},
    ]


def test_hour_processor_reports_progress(tmp_path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a", "condition-b"],
                "update_type": ["book_snapshot", "price_change"],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-no","seq":2}',
                ],
            }
        ),
        raw_path,
    )

    progress: list[tuple[int, int]] = []
    processor = RelayHourProcessor(config)
    result = processor.process_hour(
        "polymarket_orderbook_2026-03-21T12.parquet",
        raw_path,
        progress_callback=lambda processed, total: progress.append((processed, total)),
    )

    assert len(result.artifacts) == 2
    assert progress[-1] == (2, 2)


def test_prebuild_from_processed_streams_once_and_preserves_order(
    tmp_path: Path,
    monkeypatch,
):
    config = _make_config(tmp_path)
    config.ensure_directories()
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T14.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": [
                    "condition-a",
                    "condition-b",
                    "condition-a",
                    "condition-a",
                    "condition-b",
                    "condition-a",
                ],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "price_change",
                    "price_change",
                    "book_snapshot",
                    "price_change",
                ],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-no","seq":2}',
                    '{"token_id":"token-yes","seq":3}',
                    '{"token_id":"token-yes","seq":4}',
                    '{"token_id":"token-no","seq":5}',
                    '{"token_id":"token-yes","seq":6}',
                ],
            }
        ),
        raw_path,
        row_group_size=2,
    )

    processor = RelayHourProcessor(config)
    processor.process_hour(
        "polymarket_orderbook_2026-03-21T14.parquet",
        raw_path,
        skip_filtered=True,
    )

    processed_path = config.processed_root / processed_relative_path(
        "polymarket_orderbook_2026-03-21T14.parquet"
    )
    monkeypatch.setattr(processor_module, "PARQUET_BATCH_SIZE", 2)

    progress: list[tuple[int, int]] = []
    artifacts = processor.prebuild_filtered_from_processed(
        "polymarket_orderbook_2026-03-21T14.parquet",
        processed_path,
        progress_callback=lambda processed, total: progress.append((processed, total)),
    )

    assert {
        (artifact.condition_id, artifact.token_id, artifact.row_count)
        for artifact in artifacts
    } == {
        ("condition-a", "token-yes", 4),
        ("condition-b", "token-no", 2),
    }
    assert progress == [(0, 6), (2, 6), (4, 6), (6, 6)]

    filtered_yes_path = (
        config.filtered_root
        / "condition-a"
        / "token-yes"
        / "polymarket_orderbook_2026-03-21T14.parquet"
    )
    filtered_no_path = (
        config.filtered_root
        / "condition-b"
        / "token-no"
        / "polymarket_orderbook_2026-03-21T14.parquet"
    )
    assert pq.read_table(filtered_yes_path).to_pylist() == [
        {"update_type": "book_snapshot", "data": '{"token_id":"token-yes","seq":1}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes","seq":3}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes","seq":4}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes","seq":6}'},
    ]
    assert pq.read_table(filtered_no_path).to_pylist() == [
        {"update_type": "price_change", "data": '{"token_id":"token-no","seq":2}'},
        {"update_type": "book_snapshot", "data": '{"token_id":"token-no","seq":5}'},
    ]


def test_prebuild_from_processed_matches_direct_filtered_outputs(
    tmp_path: Path,
    monkeypatch,
):
    direct_config = _make_config(tmp_path / "direct")
    prebuild_config = _make_config(tmp_path / "prebuild")
    direct_config.ensure_directories()
    prebuild_config.ensure_directories()

    filename = "polymarket_orderbook_2026-03-21T15.parquet"
    rows = pa.table(
        {
            "market_id": [
                "condition-a",
                "condition-b",
                "condition-a",
                "condition-c",
                "condition-b",
                "condition-a",
                "condition-c",
                "condition-a",
            ],
            "update_type": [
                "book_snapshot",
                "price_change",
                "price_change",
                "book_snapshot",
                "price_change",
                "price_change",
                "price_change",
                "book_snapshot",
            ],
            "data": [
                '{"token_id":"token-yes","seq":1}',
                '{"token_id":"token-no","seq":2}',
                '{"token_id":"token-yes","seq":3}',
                '{"token_id":"token-maybe","seq":4}',
                '{"token_id":"token-no","seq":5}',
                '{"token_id":"token-yes","seq":6}',
                '{"token_id":"token-maybe","seq":7}',
                '{"token_id":"token-yes","seq":8}',
            ],
        }
    )

    direct_raw_path = direct_config.raw_root / "2026" / "03" / "21" / filename
    prebuild_raw_path = prebuild_config.raw_root / "2026" / "03" / "21" / filename
    direct_raw_path.parent.mkdir(parents=True, exist_ok=True)
    prebuild_raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(rows, direct_raw_path, row_group_size=2)
    pq.write_table(rows, prebuild_raw_path, row_group_size=2)

    monkeypatch.setattr(processor_module, "PARQUET_BATCH_SIZE", 2)

    direct_processor = RelayHourProcessor(direct_config)
    direct_artifacts = direct_processor.process_hour(
        filename, direct_raw_path
    ).artifacts

    prebuild_processor = RelayHourProcessor(prebuild_config)
    prebuild_processor.process_hour(filename, prebuild_raw_path, skip_filtered=True)
    processed_path = prebuild_config.processed_root / processed_relative_path(filename)
    prebuilt_artifacts = prebuild_processor.prebuild_filtered_from_processed(
        filename,
        processed_path,
    )

    direct_keys = sorted(
        (artifact.condition_id, artifact.token_id, artifact.row_count)
        for artifact in direct_artifacts
    )
    prebuilt_keys = sorted(
        (artifact.condition_id, artifact.token_id, artifact.row_count)
        for artifact in prebuilt_artifacts
    )
    assert prebuilt_keys == direct_keys

    for condition_id, token_id, _row_count in direct_keys:
        direct_filtered_path = (
            direct_config.filtered_root / condition_id / token_id / filename
        )
        prebuilt_filtered_path = (
            prebuild_config.filtered_root / condition_id / token_id / filename
        )
        assert (
            pq.read_table(prebuilt_filtered_path).to_pylist()
            == pq.read_table(direct_filtered_path).to_pylist()
        )
