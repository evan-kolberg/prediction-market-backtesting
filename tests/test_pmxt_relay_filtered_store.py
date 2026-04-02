from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pmxt_relay.config import RelayConfig
from pmxt_relay.filtered_store import FilesystemFilteredHourStore
from pmxt_relay.index_db import FilteredHourArtifact
from pmxt_relay.index_db import RelayIndex


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


def test_filtered_store_lists_indexed_hours(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.ensure_directories()
    index = RelayIndex(config.db_path)
    index.initialize()

    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    index.upsert_discovered_hour(filename, f"https://r2.pmxt.dev/{filename}", 1)
    index.mark_mirrored(
        filename,
        local_path=str(config.raw_root / "2026" / "03" / "21" / filename),
        etag=None,
        content_length=None,
        last_modified=None,
    )
    index.mark_sharded(filename)
    index.mark_prebuilt(
        filename,
        filtered_artifact_count=1,
        artifacts=[
            FilteredHourArtifact(
                filename=filename,
                hour="2026-03-21T12:00:00+00:00",
                condition_id="0x" + ("ab" * 32),
                token_id="123",
                local_path=str(
                    config.filtered_root / ("0x" + ("ab" * 32)) / "123" / filename
                ),
                row_count=42,
                byte_size=1234,
            )
        ],
    )

    store = FilesystemFilteredHourStore(config, index)
    rows = store.list_hours("0x" + ("ab" * 32), "123")

    assert len(rows) == 1
    assert rows[0].filename == filename
    assert rows[0].row_count == 42
    assert rows[0].byte_size == 1234


def test_filtered_store_falls_back_to_filesystem_scan(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.ensure_directories()
    index = RelayIndex(config.db_path)
    index.initialize()
    condition_id = "0x" + ("cd" * 32)
    token_id = "456"
    filename = "polymarket_orderbook_2026-03-21T13.parquet"
    filtered_dir = config.filtered_root / condition_id / token_id
    filtered_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"update_type": ["book_snapshot"], "data": ['{"token_id":"456"}']}),
        filtered_dir / filename,
    )

    store = FilesystemFilteredHourStore(config, index)
    rows = store.list_hours(condition_id, token_id)

    assert len(rows) == 1
    assert rows[0].filename == filename
    assert rows[0].row_count is None
    assert rows[0].byte_size is not None


def test_filtered_store_resolves_paths_under_filtered_root(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.ensure_directories()
    index = RelayIndex(config.db_path)
    index.initialize()
    store = FilesystemFilteredHourStore(config, index)

    path = store.resolve_hour_path(
        "0x" + ("ef" * 32),
        "789",
        "polymarket_orderbook_2026-03-21T14.parquet",
    )

    assert path == (
        config.filtered_root
        / ("0x" + ("ef" * 32))
        / "789"
        / "polymarket_orderbook_2026-03-21T14.parquet"
    )
