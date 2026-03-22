from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

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
        expose_raw=False,
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
