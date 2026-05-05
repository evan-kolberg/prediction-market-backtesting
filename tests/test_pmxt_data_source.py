from __future__ import annotations

import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import Request

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import prediction_market_extensions.backtesting.data_sources.pmxt as pmxt_module
from prediction_market_extensions._runtime_log import capture_loader_events
from prediction_market_extensions.backtesting.data_sources.pmxt import (
    PMXT_CACHE_PREFETCH_WORKERS_ENV,
    PMXT_DATA_SOURCE_ENV,
    PMXT_LOCAL_RAWS_DIR_ENV,
    PMXT_PREFETCH_WORKERS_ENV,
    PMXT_RAW_ROOT_ENV,
    PMXT_REMOTE_BASE_URL_ENV,
    PMXT_ROW_GROUP_CHUNK_SIZE_ENV,
    PMXT_ROW_GROUP_SCAN_WORKERS_ENV,
    PMXT_SOURCE_PRIORITY_ENV,
    RunnerPolymarketPMXTDataLoader,
    configured_pmxt_data_source,
)


def _make_loader(
    *,
    cache_dir: Path | None = None,
    raw_root: Path | None = None,
    disable_remote_archive: bool = False,
) -> RunnerPolymarketPMXTDataLoader:
    loader = object.__new__(RunnerPolymarketPMXTDataLoader)
    loader._pmxt_cache_dir = cache_dir
    loader._pmxt_local_archive_dir = None
    loader._pmxt_remote_base_url = None
    loader._pmxt_source_priority = ("raw-local", "raw-remote")
    loader._condition_id = "condition-123"
    loader._token_id = "token-yes-123"
    loader._pmxt_prefetch_workers = 2
    loader._pmxt_download_progress_callback = None
    loader._pmxt_scan_progress_callback = None
    loader._pmxt_progress_size_cache = {}
    loader._pmxt_temp_download_root = (
        cache_dir if cache_dir is not None else Path.cwd()
    ) / ".pmxt-temp-downloads"
    loader._pmxt_raw_root = raw_root
    loader._pmxt_disable_remote_archive = disable_remote_archive
    loader._pmxt_last_load_gap_hours = ()
    return loader


def test_configured_pmxt_data_source_sets_raw_local_overrides(monkeypatch, tmp_path):
    mirror_root = tmp_path / "mirror"
    mirror_root.mkdir()
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "raw-local")
    monkeypatch.setenv(PMXT_LOCAL_RAWS_DIR_ENV, str(mirror_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "raw-local"
        assert str(mirror_root) in selection.summary
        assert RunnerPolymarketPMXTDataLoader._resolve_remote_base_url() is None
        assert RunnerPolymarketPMXTDataLoader._resolve_raw_root() == mirror_root
        assert RunnerPolymarketPMXTDataLoader._resolve_prefetch_workers() == 6

    assert os.getenv(PMXT_RAW_ROOT_ENV) is None


def test_configured_pmxt_data_source_preserves_manual_low_level_env(monkeypatch, tmp_path):
    mirror_root = tmp_path / "manual-mirror"
    mirror_root.mkdir()
    monkeypatch.delenv(PMXT_DATA_SOURCE_ENV, raising=False)
    monkeypatch.setenv(PMXT_RAW_ROOT_ENV, str(mirror_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "raw-local"
        assert RunnerPolymarketPMXTDataLoader._resolve_raw_root() == mirror_root


def test_configured_pmxt_data_source_requires_local_mirror(monkeypatch):
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "raw-local")
    monkeypatch.delenv(PMXT_LOCAL_RAWS_DIR_ENV, raising=False)

    with pytest.raises(ValueError, match=PMXT_LOCAL_RAWS_DIR_ENV), configured_pmxt_data_source():
        pass


def test_configured_pmxt_data_source_preserves_explicit_source_order(monkeypatch, tmp_path):
    mirror_root = tmp_path / "mirror"
    mirror_root.mkdir()
    monkeypatch.delenv(PMXT_DATA_SOURCE_ENV, raising=False)

    with configured_pmxt_data_source(
        sources=["archive:archive.vendor.test", f"local:{mirror_root}"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            "PMXT source: explicit priority "
            f"(cache -> archive https://archive.vendor.test -> local {mirror_root})"
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_raw_root() == mirror_root
        assert (
            RunnerPolymarketPMXTDataLoader._resolve_remote_base_url()
            == "https://archive.vendor.test"
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_source_priority() == (
            "raw-remote",
            "raw-local",
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_prefetch_workers() == 6

    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_REMOTE_BASE_URL_ENV) is None
    assert os.getenv(PMXT_SOURCE_PRIORITY_ENV) is None


def test_configured_pmxt_data_source_preserves_existing_prefetch_override(
    monkeypatch, tmp_path
) -> None:
    mirror_root = tmp_path / "mirror"
    mirror_root.mkdir()
    monkeypatch.setenv(PMXT_PREFETCH_WORKERS_ENV, "7")

    with configured_pmxt_data_source(sources=[f"local:{mirror_root}"]) as selection:
        assert selection.mode == "auto"
        assert RunnerPolymarketPMXTDataLoader._resolve_prefetch_workers() == 7


def test_runner_pmxt_cache_prefetch_workers_default_and_env(monkeypatch) -> None:
    monkeypatch.delenv(PMXT_CACHE_PREFETCH_WORKERS_ENV, raising=False)
    assert RunnerPolymarketPMXTDataLoader._resolve_cache_prefetch_workers() == 32

    monkeypatch.setenv(PMXT_CACHE_PREFETCH_WORKERS_ENV, "12")
    assert RunnerPolymarketPMXTDataLoader._resolve_cache_prefetch_workers() == 12

    monkeypatch.setenv(PMXT_CACHE_PREFETCH_WORKERS_ENV, "invalid")
    assert RunnerPolymarketPMXTDataLoader._resolve_cache_prefetch_workers() == 32


def test_runner_pmxt_row_group_scan_bounds_default_and_env(monkeypatch) -> None:
    monkeypatch.delenv(PMXT_ROW_GROUP_CHUNK_SIZE_ENV, raising=False)
    monkeypatch.delenv(PMXT_ROW_GROUP_SCAN_WORKERS_ENV, raising=False)
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_chunk_size() == 4
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_scan_workers() == 2

    monkeypatch.setenv(PMXT_ROW_GROUP_CHUNK_SIZE_ENV, "4")
    monkeypatch.setenv(PMXT_ROW_GROUP_SCAN_WORKERS_ENV, "3")
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_chunk_size() == 4
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_scan_workers() == 3

    monkeypatch.setenv(PMXT_ROW_GROUP_CHUNK_SIZE_ENV, "invalid")
    monkeypatch.setenv(PMXT_ROW_GROUP_SCAN_WORKERS_ENV, "invalid")
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_chunk_size() == 4
    assert RunnerPolymarketPMXTDataLoader._resolve_row_group_scan_workers() == 2


def test_configured_pmxt_data_source_rejects_cache_explicit_source() -> None:
    with (
        pytest.raises(ValueError, match="The cache layer is implicit"),
        configured_pmxt_data_source(sources=["cache"]),
    ):
        pass


@pytest.mark.parametrize(
    "source",
    [
        "r2v2.pmxt.dev",
        "/tmp/pmxt-raw",
        "local_raws:/tmp/pmxt-raw",
        "processed:/tmp/pmxt-processed",
        "raw:/tmp/pmxt-raw",
        "raw-remote:https://r2v2.pmxt.dev",
        "mirror:/tmp/pmxt-raw",
    ],
)
def test_configured_pmxt_data_source_rejects_legacy_or_unprefixed_explicit_sources(
    source: str,
) -> None:
    with (
        pytest.raises(ValueError, match="Use one of: local:, archive:"),
        configured_pmxt_data_source(sources=[source]),
    ):
        pass


def test_configured_pmxt_data_source_isolates_concurrent_loader_config(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.delenv(PMXT_REMOTE_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(PMXT_RAW_ROOT_ENV, raising=False)
    monkeypatch.delenv(PMXT_SOURCE_PRIORITY_ENV, raising=False)
    mirror_a = tmp_path / "mirror-a"
    mirror_b = tmp_path / "mirror-b"
    mirror_a.mkdir()
    mirror_b.mkdir()

    async def _capture(
        sources: list[str],
    ) -> tuple[Path | None, str | None, tuple[str, ...]]:
        with configured_pmxt_data_source(sources=sources):
            await asyncio.sleep(0)
            return (
                RunnerPolymarketPMXTDataLoader._resolve_raw_root(),
                RunnerPolymarketPMXTDataLoader._resolve_remote_base_url(),
                RunnerPolymarketPMXTDataLoader._resolve_source_priority(),
            )

    async def _run() -> tuple[
        tuple[Path | None, str | None, tuple[str, ...]],
        tuple[Path | None, str | None, tuple[str, ...]],
    ]:
        return await asyncio.gather(
            _capture([f"local:{mirror_a}", "archive:archive-a.vendor.test"]),
            _capture([f"local:{mirror_b}", "archive:archive-b.vendor.test"]),
        )

    first, second = asyncio.run(_run())

    assert first == (
        mirror_a,
        "https://archive-a.vendor.test",
        ("raw-local", "raw-remote"),
    ) or first == (
        mirror_a,
        "https://archive-a.vendor.test",
        ("raw-remote", "raw-local"),
    )
    assert second == (
        mirror_b,
        "https://archive-b.vendor.test",
        ("raw-local", "raw-remote"),
    ) or second == (
        mirror_b,
        "https://archive-b.vendor.test",
        ("raw-remote", "raw-local"),
    )
    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_REMOTE_BASE_URL_ENV) is None


def test_runner_loader_reads_market_rows_from_local_raw_mirror(tmp_path):
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-123", "condition-123", "condition-456"],
                "update_type": ["book_snapshot", "price_change", "price_change"],
                "data": [
                    '{"token_id":"token-yes-123","seq":1}',
                    '{"token_id":"token-no-999","seq":2}',
                    '{"token_id":"token-yes-123","seq":3}',
                ],
            }
        ),
        raw_path,
    )

    batches = loader._load_local_archive_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert pa.Table.from_batches(batches).to_pylist() == [
        {"update_type": "book_snapshot", "data": '{"token_id":"token-yes-123","seq":1}'}
    ]


def test_runner_loader_reads_fixed_schema_market_rows_from_local_raw_mirror(tmp_path):
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [
                        pd.Timestamp("2026-03-21T12:00:00.001Z"),
                        pd.Timestamp("2026-03-21T12:00:00.002Z"),
                        pd.Timestamp("2026-03-21T12:00:00.003Z"),
                        pd.Timestamp("2026-03-21T12:00:00.004Z"),
                    ],
                    type=pa.timestamp("ns", tz="UTC"),
                ),
                "market": [
                    b"condition-123",
                    b"condition-123",
                    b"condition-123",
                    b"condition-456",
                ],
                "event_type": ["book", "price_change", "price_change", "book"],
                "asset_id": [
                    "token-yes-123",
                    "token-yes-123",
                    "token-no-999",
                    "token-yes-123",
                ],
                "bids": ['[["0.48","11.0"]]', None, None, '[["0.47","1.0"]]'],
                "asks": ['[["0.52","9.0"]]', None, None, '[["0.53","1.0"]]'],
                "price": [None, "0.49", "0.51", None],
                "size": [None, "13.5", "8.0", None],
                "side": [None, "BUY", "SELL", None],
            }
        ),
        raw_path,
    )

    def _fail_duckdb(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("fixed-schema local raw mirror should use the PyArrow row-group path")

    loader._load_raw_market_batches_duckdb = _fail_duckdb  # type: ignore[method-assign]

    batches = loader._load_local_archive_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert pa.Table.from_batches(batches).to_pylist() == [
        {
            "event_type": "book",
            "timestamp_ns": 1_774_094_400_001_000_000,
            "asset_id": "token-yes-123",
            "bids": '[["0.48","11.0"]]',
            "asks": '[["0.52","9.0"]]',
            "price": None,
            "size": None,
            "side": None,
        },
        {
            "event_type": "price_change",
            "timestamp_ns": 1_774_094_400_002_000_000,
            "asset_id": "token-yes-123",
            "bids": None,
            "asks": None,
            "price": "0.49",
            "size": "13.5",
            "side": "BUY",
        },
    ]


def test_runner_loader_emits_cache_hit_event(tmp_path) -> None:
    loader = _make_loader(cache_dir=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    cache_path = loader._cache_path_for_hour(hour)
    assert cache_path is not None
    cache_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes-123","seq":1}'],
            }
        ),
        cache_path,
    )

    with capture_loader_events() as capture:
        batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is not None
    event = next(event for event in capture.events if event.stage == "cache_read")
    assert event.status == "cache_hit"
    assert event.vendor == "pmxt"
    assert event.origin == "pmxt._load_market_batches"
    assert event.source_kind == "cache"
    assert event.cache_path == str(cache_path)
    assert event.rows == 1


def test_runner_loader_emits_cache_write_error(tmp_path, monkeypatch) -> None:
    loader = _make_loader(cache_dir=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    table = pa.table(
        {
            "update_type": ["book_snapshot"],
            "data": ['{"token_id":"token-yes-123","seq":1}'],
        }
    )

    def fail_write(_hour, _table):  # type: ignore[no-untyped-def]
        raise OSError("disk full")

    monkeypatch.setattr(loader, "_write_market_cache", fail_write)

    with capture_loader_events() as capture:
        loader._write_cache_if_enabled(hour, table)

    event = next(event for event in capture.events if event.stage == "cache_write")
    assert event.level == "ERROR"
    assert event.status == "error"
    assert event.vendor == "pmxt"
    assert event.origin == "pmxt._write_cache_if_enabled"
    assert event.source_kind == "cache"
    assert event.attrs["error"] == "disk full"


def test_runner_loader_emits_ordered_source_skip_event(tmp_path) -> None:
    loader = _make_loader(cache_dir=None)
    missing_root = tmp_path / "missing"
    loader._pmxt_ordered_source_entries = (("raw-local", str(missing_root)),)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")

    with capture_loader_events() as capture:
        batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is None
    statuses = [
        (event.status, event.source_kind, event.source, event.origin) for event in capture.events
    ]
    assert (
        "start",
        "local",
        f"local:{missing_root}",
        "pmxt._load_market_batches",
    ) in statuses
    assert (
        "skip",
        "local",
        f"local:{missing_root}",
        "pmxt._load_market_batches",
    ) in statuses


def test_runner_loader_grouped_raw_hour_load_splits_requests(tmp_path) -> None:
    loader = _make_loader(cache_dir=None)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    condition_a = "0x" + ("a" * 64)
    condition_b = "0x" + ("b" * 64)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [
                        pd.Timestamp("2026-03-21T12:00:01Z"),
                        pd.Timestamp("2026-03-21T12:00:02Z"),
                        pd.Timestamp("2026-03-21T12:00:03Z"),
                    ],
                    type=pa.timestamp("ms", tz="UTC"),
                ),
                "market": pa.array(
                    [
                        condition_a.encode(),
                        condition_b.encode(),
                        ("0x" + ("c" * 64)).encode(),
                    ],
                    type=pa.binary(66),
                ),
                "event_type": ["book", "price_change", "book"],
                "asset_id": ["token-a", "token-b", "token-c"],
                "bids": ["[]", None, "[]"],
                "asks": ["[]", None, "[]"],
                "price": ["0.40", "0.60", "0.90"],
                "size": ["1.0", "2.0", "3.0"],
                "side": ["BUY", "SELL", "BUY"],
            }
        ),
        raw_path,
    )
    loader._pmxt_ordered_source_entries = (("raw-local", str(tmp_path)),)

    with capture_loader_events() as capture:
        batches_by_request = loader.load_shared_market_batches_for_hour(
            hour,
            requests=((0, condition_a, "token-a"), (1, condition_b, "token-b")),
            batch_size=1_000,
        )

    assert {
        key: loader._row_count_from_batches(value or [])
        for key, value in batches_by_request.items()
    } == {
        0: 1,
        1: 1,
    }
    assert batches_by_request[0][0].schema.names == loader._PMXT_FIXED_COLUMNS
    fetch_events = [event for event in capture.events if event.stage == "fetch"]
    assert [(event.status, event.origin) for event in fetch_events] == [
        ("start", "pmxt.load_shared_market_batches_for_hour"),
        ("complete", "pmxt.load_shared_market_batches_for_hour"),
    ]
    assert fetch_events[1].rows == 2


def test_runner_loader_grouped_raw_hour_scopes_requests_by_row_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader = _make_loader(cache_dir=None)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    condition_a = "0x" + ("a" * 64)
    condition_b = "0x" + ("b" * 64)
    condition_c = "0x" + ("c" * 64)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [
                        pd.Timestamp("2026-03-21T12:00:01Z"),
                        pd.Timestamp("2026-03-21T12:00:02Z"),
                        pd.Timestamp("2026-03-21T12:00:03Z"),
                    ],
                    type=pa.timestamp("ms", tz="UTC"),
                ),
                "market": pa.array(
                    [condition_a.encode(), condition_b.encode(), condition_c.encode()],
                    type=pa.binary(66),
                ),
                "event_type": ["book", "price_change", "book"],
                "asset_id": ["token-a", "token-b", "token-c"],
                "bids": ["[]", None, "[]"],
                "asks": ["[]", None, "[]"],
                "price": ["0.40", "0.60", "0.90"],
                "size": ["1.0", "2.0", "3.0"],
                "side": ["BUY", "SELL", "BUY"],
            }
        ),
        raw_path,
        row_group_size=1,
    )
    loader._pmxt_ordered_source_entries = (("raw-local", str(tmp_path)),)
    monkeypatch.setenv(PMXT_ROW_GROUP_CHUNK_SIZE_ENV, "1")

    original_split = loader._split_shared_fixed_table
    split_request_ids: list[tuple[int, ...]] = []

    def capture_split(table, *, requests, batch_size: int):  # type: ignore[no-untyped-def]
        split_request_ids.append(tuple(request_id for request_id, _, _ in requests))
        return original_split(table, requests=requests, batch_size=batch_size)

    monkeypatch.setattr(loader, "_split_shared_fixed_table", capture_split)

    batches_by_request = loader.load_shared_market_batches_for_hour(
        hour,
        requests=((0, condition_a, "token-a"), (1, condition_b, "token-b")),
        batch_size=1_000,
    )

    assert {
        key: loader._row_count_from_batches(value or [])
        for key, value in batches_by_request.items()
    } == {0: 1, 1: 1}
    assert split_request_ids == [(0,), (1,)]


def test_runner_loader_grouped_raw_hour_prunes_row_groups_by_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader = _make_loader(cache_dir=None)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    condition = "0x" + ("a" * 64)
    pq.write_table(
        pa.table(
            {
                "timestamp": pa.array(
                    [
                        pd.Timestamp("2026-03-21T12:00:01Z"),
                        pd.Timestamp("2026-03-21T12:00:02Z"),
                    ],
                    type=pa.timestamp("ms", tz="UTC"),
                ),
                "market": pa.array([condition.encode(), condition.encode()], type=pa.binary(66)),
                "event_type": ["book", "book"],
                "asset_id": ["token-a", "token-z"],
                "bids": ["[]", "[]"],
                "asks": ["[]", "[]"],
                "price": ["0.40", "0.90"],
                "size": ["1.0", "3.0"],
                "side": ["BUY", "BUY"],
            }
        ),
        raw_path,
        row_group_size=1,
    )
    loader._pmxt_ordered_source_entries = (("raw-local", str(tmp_path)),)
    monkeypatch.setenv(PMXT_ROW_GROUP_CHUNK_SIZE_ENV, "1")

    original_split = loader._split_shared_fixed_table
    split_request_ids: list[tuple[int, ...]] = []

    def capture_split(table, *, requests, batch_size: int):  # type: ignore[no-untyped-def]
        split_request_ids.append(tuple(request_id for request_id, _, _ in requests))
        return original_split(table, requests=requests, batch_size=batch_size)

    monkeypatch.setattr(loader, "_split_shared_fixed_table", capture_split)

    batches_by_request = loader.load_shared_market_batches_for_hour(
        hour,
        requests=((0, condition, "token-a"),),
        batch_size=1_000,
    )

    assert loader._row_count_from_batches(batches_by_request[0] or []) == 1
    assert split_request_ids == [(0,)]


def test_runner_loader_grouped_remote_uses_temp_when_raw_copy_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader = _make_loader(cache_dir=tmp_path / "cache", raw_root=tmp_path / "missing-raw-root")
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    loader._pmxt_ordered_source_entries = (("raw-remote", "https://archive.vendor.test"),)
    requests = ((0, "0x" + ("a" * 64), "token-a"),)
    downloaded: list[tuple[str, Path]] = []
    loaded: dict[str, object] = {}

    def fake_raw_copy(_archive_url: str, _raw_path: Path, _hour) -> Path | None:
        return None

    def fake_download(url: str, destination: Path) -> int:
        downloaded.append((url, destination))
        destination.write_bytes(b"raw")
        return 3

    def fake_load_shared(
        raw_path: Path,
        *,
        requests,
        batch_size: int,
    ):
        loaded["raw_path"] = raw_path
        loaded["exists_during_load"] = raw_path.exists()
        loaded["batch_size"] = batch_size
        return {request_id: [] for request_id, _, _ in requests}

    monkeypatch.setattr(loader, "_download_remote_raw_to_local_root", fake_raw_copy)
    monkeypatch.setattr(loader, "_download_to_file_with_progress", fake_download)
    monkeypatch.setattr(loader, "_load_shared_market_batches_from_raw_file", fake_load_shared)

    with capture_loader_events() as capture:
        batches_by_request = loader.load_shared_market_batches_for_hour(
            hour,
            requests=requests,
            batch_size=1_000,
        )

    assert batches_by_request == {0: []}
    assert len(downloaded) == 1
    assert downloaded[0][0] == (
        "https://archive.vendor.test/polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert downloaded[0][1].name == "polymarket_orderbook_2026-03-21T12.parquet"
    assert loaded["exists_during_load"] is True
    assert loaded["batch_size"] == 1_000
    assert Path(loaded["raw_path"]).is_relative_to(tmp_path / "cache" / ".pmxt-temp-downloads")
    fetch_events = [event for event in capture.events if event.stage == "fetch"]
    assert [(event.status, event.source_kind, event.origin) for event in fetch_events] == [
        ("start", "remote", "pmxt.load_shared_market_batches_for_hour"),
        ("complete", "remote", "pmxt.load_shared_market_batches_for_hour"),
    ]


def test_runner_loader_grouped_remote_skips_raw_copy_when_raw_root_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loader = _make_loader(cache_dir=tmp_path / "cache", raw_root=tmp_path / "missing-raw-root")
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    loader._pmxt_ordered_source_entries = (("raw-remote", "https://archive.vendor.test"),)
    requests = ((0, "0x" + ("a" * 64), "token-a"),)
    downloaded: list[tuple[str, Path]] = []
    loaded: dict[str, object] = {}

    def fail_raw_copy(_archive_url: str, _raw_path: Path, _hour) -> Path | None:
        raise AssertionError("unavailable raw root should use temporary archive download")

    def fake_download(url: str, destination: Path) -> int:
        downloaded.append((url, destination))
        destination.write_bytes(b"raw")
        return 3

    def fake_load_shared(
        raw_path: Path,
        *,
        requests,
        batch_size: int,
    ):
        loaded["raw_path"] = raw_path
        loaded["exists_during_load"] = raw_path.exists()
        loaded["batch_size"] = batch_size
        return {request_id: [] for request_id, _, _ in requests}

    monkeypatch.setattr(loader, "_raw_root_can_persist", lambda _raw_root: False)
    monkeypatch.setattr(loader, "_download_remote_raw_to_local_root", fail_raw_copy)
    monkeypatch.setattr(loader, "_download_to_file_with_progress", fake_download)
    monkeypatch.setattr(loader, "_load_shared_market_batches_from_raw_file", fake_load_shared)

    with capture_loader_events() as capture:
        batches_by_request = loader.load_shared_market_batches_for_hour(
            hour,
            requests=requests,
            batch_size=1_000,
        )

    assert batches_by_request == {0: []}
    assert len(downloaded) == 1
    assert loaded["exists_during_load"] is True
    assert loaded["batch_size"] == 1_000
    assert Path(loaded["raw_path"]).is_relative_to(tmp_path / "cache" / ".pmxt-temp-downloads")
    raw_write_events = [event for event in capture.events if event.stage == "raw_write"]
    assert [(event.status, event.source_kind) for event in raw_write_events] == [("skip", "local")]
    assert raw_write_events[0].attrs["reason"].startswith("raw persistence root unavailable")


def test_runner_loader_emits_source_events_from_direct_call_sites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    loader = _make_loader(cache_dir=None)
    loader._pmxt_source_priority = ("raw-local", "raw-remote")
    loader._pmxt_raw_root = Path("/tmp/pmxt-raws")
    loader._pmxt_remote_base_urls = ("https://archive.vendor.test",)
    loader._pmxt_remote_base_url = "https://archive.vendor.test"

    monkeypatch.setattr(loader, "_load_cached_market_batches", lambda _hour: None)
    monkeypatch.setattr(
        loader,
        "_load_local_archive_market_batches",
        lambda _hour, *, batch_size: None,
    )
    monkeypatch.setattr(
        loader,
        "_load_remote_market_batches",
        lambda _hour, *, batch_size: [],
    )

    with capture_loader_events() as capture:
        batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches == []
    fetch_events = [event for event in capture.events if event.stage == "fetch"]
    assert [(event.status, event.source_kind, event.origin) for event in fetch_events] == [
        ("start", "local", "pmxt._load_market_batches"),
        ("skip", "local", "pmxt._load_market_batches"),
        ("start", "remote", "pmxt._load_market_batches"),
        ("complete", "remote", "pmxt._load_market_batches"),
    ]


def test_runner_loader_emits_scan_progress_for_local_raw_mirror(monkeypatch, tmp_path) -> None:
    loader = _make_loader(raw_root=tmp_path)
    loader._pmxt_scan_progress_callback = object()
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"pmxt")
    captured: dict[str, object] = {}

    def fake_load_raw_file(
        parquet_path: Path, *, batch_size: int, progress_source: str, total_bytes: int | None
    ):
        captured["parquet_path"] = parquet_path
        captured["batch_size"] = batch_size
        captured["source"] = progress_source
        captured["total_bytes"] = total_bytes
        return ["batch"]

    monkeypatch.setattr(loader, "_load_raw_market_batches_from_local_file", fake_load_raw_file)

    batches = loader._load_local_archive_market_batches(hour, batch_size=1_000)

    assert batches == ["batch"]
    assert captured == {
        "parquet_path": raw_path,
        "batch_size": 1_000,
        "source": str(raw_path),
        "total_bytes": 4,
    }


def test_runner_loader_persists_remote_archive_download_to_raw_root(monkeypatch, tmp_path) -> None:
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    downloaded: list[tuple[str, Path]] = []
    loaded: dict[str, object] = {}

    def fake_download(url: str, destination: Path) -> int:
        downloaded.append((url, destination))
        destination.write_bytes(b"raw")
        return 3

    def fake_load_raw_file(
        parquet_path: Path, *, batch_size: int, progress_source: str, total_bytes: int | None
    ):
        loaded["parquet_path"] = parquet_path
        loaded["batch_size"] = batch_size
        loaded["source"] = progress_source
        loaded["total_bytes"] = total_bytes
        return ["batch"]

    monkeypatch.setattr(loader, "_download_to_file_with_progress", fake_download)
    monkeypatch.setattr(loader, "_load_raw_market_batches_from_local_file", fake_load_raw_file)

    with capture_loader_events() as capture:
        batches = loader._load_remote_market_batches_from_base_url(
            "https://archive.vendor.test",
            hour,
            batch_size=1_000,
        )

    assert batches == ["batch"]
    assert raw_path.read_bytes() == b"raw"
    assert len(downloaded) == 1
    assert downloaded[0][0] == (
        "https://archive.vendor.test/polymarket_orderbook_2026-03-21T12.parquet"
    )
    temp_path = downloaded[0][1]
    assert temp_path.parent == raw_path.parent
    assert temp_path.name.startswith(f".{raw_path.name}.{os.getpid()}.{threading.get_ident()}.")
    assert temp_path.name.endswith(".tmp")
    assert loaded == {
        "parquet_path": raw_path,
        "batch_size": 1_000,
        "source": str(raw_path),
        "total_bytes": 3,
    }
    raw_write_events = [event for event in capture.events if event.stage == "raw_write"]
    assert [(event.status, event.level, event.origin) for event in raw_write_events] == [
        ("start", "INFO", "pmxt._download_remote_raw_to_local_root"),
        ("complete", "INFO", "pmxt._download_remote_raw_to_local_root"),
    ]
    assert raw_write_events[1].bytes == 3
    assert raw_write_events[1].cache_path == str(raw_path)


def test_runner_loader_reuses_persisted_remote_archive_copy(monkeypatch, tmp_path) -> None:
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"raw")
    loaded_paths: list[Path] = []

    def fail_download(_url: str, _destination: Path) -> int:
        raise AssertionError("persisted raw copy should avoid remote download")

    def fake_load_raw_file(
        parquet_path: Path, *, batch_size: int, progress_source: str, total_bytes: int | None
    ):
        del batch_size, progress_source, total_bytes
        loaded_paths.append(parquet_path)
        return ["batch"]

    monkeypatch.setattr(loader, "_download_to_file_with_progress", fail_download)
    monkeypatch.setattr(loader, "_load_raw_market_batches_from_local_file", fake_load_raw_file)

    with capture_loader_events() as capture:
        batches = loader._load_remote_market_batches_from_base_url(
            "https://archive.vendor.test",
            hour,
            batch_size=1_000,
        )

    assert batches == ["batch"]
    assert loaded_paths == [raw_path]
    assert [event for event in capture.events if event.stage == "raw_write"] == []


def test_runner_loader_serializes_concurrent_remote_archive_persistence(
    monkeypatch,
    tmp_path,
) -> None:
    loader_a = _make_loader(raw_root=tmp_path)
    loader_b = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    download_count = 0
    active_downloads = 0
    max_active_downloads = 0
    lock = threading.Lock()

    def fake_download(self, url: str, destination: Path) -> int:  # type: ignore[no-untyped-def]
        del self, url
        nonlocal download_count, active_downloads, max_active_downloads
        with lock:
            download_count += 1
            active_downloads += 1
            max_active_downloads = max(max_active_downloads, active_downloads)
        time.sleep(0.05)
        destination.write_bytes(b"raw")
        with lock:
            active_downloads -= 1
        return 3

    def fake_load_raw_file(
        self, parquet_path: Path, *, batch_size: int, progress_source: str, total_bytes: int | None
    ):
        del self, batch_size, progress_source, total_bytes
        return [parquet_path.read_bytes()]

    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader,
        "_download_to_file_with_progress",
        fake_download,
    )
    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader,
        "_load_raw_market_batches_from_local_file",
        fake_load_raw_file,
    )

    def load(loader: RunnerPolymarketPMXTDataLoader):
        return loader._load_remote_market_batches_from_base_url(
            "https://archive.vendor.test",
            hour,
            batch_size=1_000,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(load, (loader_a, loader_b)))

    assert results == [[b"raw"], [b"raw"]]
    assert download_count == 1
    assert max_active_downloads == 1


def test_runner_loader_emits_raw_archive_write_error(monkeypatch, tmp_path) -> None:
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"

    def fail_download(_url: str, _destination: Path) -> int:
        raise OSError("disk full")

    monkeypatch.setattr(loader, "_download_to_file_with_progress", fail_download)

    with capture_loader_events() as capture:
        assert (
            loader._download_remote_raw_to_local_root(
                "https://archive.vendor.test/polymarket_orderbook_2026-03-21T12.parquet",
                raw_path,
                hour,
            )
            is None
        )

    event = next(event for event in capture.events if event.status == "error")
    assert event.level == "ERROR"
    assert event.stage == "raw_write"
    assert event.vendor == "pmxt"
    assert event.origin == "pmxt._download_remote_raw_to_local_root"
    assert event.cache_path == str(raw_path)
    assert event.attrs["error"] == "disk full"


def test_runner_loader_honors_per_entry_explicit_source_order(monkeypatch) -> None:
    loader = _make_loader()
    loader._pmxt_ordered_source_entries = (
        ("raw-remote", "https://first.archive.test"),
        ("raw-local", "/tmp/local-a"),
        ("raw-remote", "https://second.archive.test"),
    )
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(loader, "_load_cached_market_batches", lambda hour: None)

    def _record_local(self, raw_root, hour, *, batch_size):
        del self, hour, batch_size
        calls.append(("raw-local", str(raw_root)))
        return None

    def _record_remote(self, base_url, hour, *, batch_size):
        del self, hour, batch_size
        calls.append(("raw-remote", base_url))
        return None

    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader,
        "_load_remote_market_batches_from_base_url",
        _record_remote,
    )
    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader,
        "_load_local_raw_market_batches_from_root",
        _record_local,
    )

    assert (
        loader._load_market_batches(pd.Timestamp("2026-03-21T12:00:00Z"), batch_size=1_000) is None
    )
    assert calls == [
        ("raw-remote", "https://first.archive.test"),
        ("raw-local", "/tmp/local-a"),
        ("raw-remote", "https://second.archive.test"),
    ]


def test_runner_loader_prefetches_explicit_local_sources_without_serial_lock(
    monkeypatch,
) -> None:
    loader = _make_loader()
    loader._pmxt_ordered_source_entries = (("raw-local", "/tmp/local-a"),)
    loader._pmxt_prefetch_workers = 2
    hours = [pd.Timestamp("2026-03-21T11:00:00Z"), pd.Timestamp("2026-03-21T12:00:00Z")]
    active = 0
    max_active = 0
    lock = threading.Lock()

    monkeypatch.setattr(loader, "_load_cached_market_batches", lambda hour: None)

    def _record_local(raw_root, hour, *, batch_size):
        del raw_root, hour, batch_size
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return []

    monkeypatch.setattr(loader, "_load_local_raw_market_batches_from_root", _record_local)

    assert list(loader._iter_market_batches(hours, batch_size=1_000)) == [
        (hours[0], []),
        (hours[1], []),
    ]
    assert max_active == 2


def test_configured_pmxt_data_source_preserves_full_per_entry_order(tmp_path) -> None:
    mirror_a = tmp_path / "mirror-a"
    mirror_b = tmp_path / "mirror-b"
    mirror_a.mkdir()
    mirror_b.mkdir()

    sources = [
        "archive:archive-1.vendor.test",
        f"local:{mirror_a}",
        "archive:archive-2.vendor.test",
        f"local:{mirror_b}",
        "archive:archive-1.vendor.test",
    ]
    with configured_pmxt_data_source(sources=sources) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            "PMXT source: explicit priority ("
            "cache -> archive https://archive-1.vendor.test "
            f"-> local {mirror_a} "
            "-> archive https://archive-2.vendor.test "
            f"-> local {mirror_b} "
            "-> archive https://archive-1.vendor.test"
            ")"
        )

    from prediction_market_extensions.backtesting.data_sources.pmxt import (
        resolve_pmxt_loader_config,
    )

    _, cfg = resolve_pmxt_loader_config(sources=sources)
    assert cfg.ordered_source_entries == (
        ("raw-remote", "https://archive-1.vendor.test"),
        ("raw-local", str(mirror_a)),
        ("raw-remote", "https://archive-2.vendor.test"),
        ("raw-local", str(mirror_b)),
        ("raw-remote", "https://archive-1.vendor.test"),
    )
    assert cfg.remote_base_urls == (
        "https://archive-1.vendor.test",
        "https://archive-2.vendor.test",
    )
    assert cfg.raw_root == Path(str(mirror_a))


def test_runner_loader_uses_instance_archive_url_in_threaded_prefetch(monkeypatch) -> None:
    hours = [pd.Timestamp("2026-03-21T11:00:00Z"), pd.Timestamp("2026-03-21T12:00:00Z")]
    seen_urls: list[str] = []

    def fake_load_market_batches(self, hour, *, batch_size):  # type: ignore[no-untyped-def]
        assert batch_size == 1_000
        seen_urls.append(self._archive_url_for_hour(hour))
        return []

    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader, "_load_market_batches", fake_load_market_batches
    )

    with configured_pmxt_data_source(sources=["archive:archive.vendor.test"]):
        loader = object.__new__(RunnerPolymarketPMXTDataLoader)
        loader._pmxt_prefetch_workers = 2
        loader._pmxt_remote_base_url = RunnerPolymarketPMXTDataLoader._resolve_remote_base_url()

        assert list(loader._iter_market_batches(hours, batch_size=1_000)) == [
            (hours[0], []),
            (hours[1], []),
        ]

    assert seen_urls == [
        "https://archive.vendor.test/polymarket_orderbook_2026-03-21T11.parquet",
        "https://archive.vendor.test/polymarket_orderbook_2026-03-21T12.parquet",
    ]


def test_runner_loader_uses_user_agent_for_remote_downloads(monkeypatch, tmp_path) -> None:
    loader = _make_loader()
    payload = b"pmxt-test-payload"
    captured_request: Request | None = None
    captured_timeout: float | None = None

    class FakeResponse:
        def __init__(self, body: bytes) -> None:
            self._body = body
            self._offset = 0
            self.headers = {"Content-Length": str(len(body))}

        def read(self, size: int = -1) -> bytes:
            if self._offset >= len(self._body):
                return b""
            if size < 0:
                chunk = self._body[self._offset :]
                self._offset = len(self._body)
                return chunk
            chunk = self._body[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_urlopen(request, timeout=None):
        nonlocal captured_request
        nonlocal captured_timeout
        captured_request = request
        captured_timeout = timeout
        return FakeResponse(payload)

    monkeypatch.setattr(pmxt_module, "urlopen", fake_urlopen)

    destination = tmp_path / "download.parquet"
    total_bytes = loader._download_to_file_with_progress(
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T11.parquet", destination
    )

    assert total_bytes == len(payload)
    assert destination.read_bytes() == payload
    assert captured_request is not None
    assert dict(captured_request.header_items())["User-agent"] == (
        "prediction-market-backtesting/1.0"
    )
    assert captured_timeout == 30


def test_runner_loader_uses_timeout_for_remote_payload_and_head(monkeypatch) -> None:
    loader = _make_loader()
    payload = b"pmxt-test-payload"
    requests: list[tuple[Request, float | None]] = []

    class FakeResponse:
        def __init__(self, body: bytes) -> None:
            self._body = body
            self._offset = 0
            self.headers = {"Content-Length": str(len(body))}

        def read(self, size: int = -1) -> bytes:
            if self._offset >= len(self._body):
                return b""
            if size < 0:
                chunk = self._body[self._offset :]
                self._offset = len(self._body)
                return chunk
            chunk = self._body[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_urlopen(request, timeout=None):
        requests.append((request, timeout))
        return FakeResponse(payload)

    monkeypatch.setattr(pmxt_module, "urlopen", fake_urlopen)
    loader._pmxt_scan_progress_callback = object()

    assert (
        loader._download_payload_with_progress(
            "https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T12.parquet"
        )
        == payload
    )
    assert loader._progress_total_bytes(
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T12.parquet"
    ) == len(payload)

    assert len(requests) == 2
    assert requests[0][0].get_method() == "GET"
    assert requests[1][0].get_method() == "HEAD"
    assert requests[0][1] == 30
    assert requests[1][1] == 30
