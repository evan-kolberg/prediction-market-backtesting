from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtests._shared.data_sources.pmxt import PMXT_CACHE_DIR_ENV
from backtests._shared.data_sources.pmxt import PMXT_DATA_SOURCE_ENV
from backtests._shared.data_sources.pmxt import PMXT_DISABLE_REMOTE_ARCHIVE_ENV
from backtests._shared.data_sources.pmxt import PMXT_LOCAL_FILTERED_DIR_ENV
from backtests._shared.data_sources.pmxt import PMXT_LOCAL_MIRROR_DIR_ENV
from backtests._shared.data_sources.pmxt import PMXT_RAW_ROOT_ENV
from backtests._shared.data_sources.pmxt import PMXT_RELAY_BASE_URL_ENV
from backtests._shared.data_sources.pmxt import RunnerPolymarketPMXTDataLoader
from backtests._shared.data_sources.pmxt import configured_pmxt_data_source


def _make_loader(
    *,
    cache_dir: Path | None = None,
    raw_root: Path | None = None,
    disable_remote_archive: bool = False,
) -> RunnerPolymarketPMXTDataLoader:
    loader = object.__new__(RunnerPolymarketPMXTDataLoader)
    loader._pmxt_cache_dir = cache_dir
    loader._pmxt_local_archive_dir = None
    loader._pmxt_relay_base_url = None
    loader._condition_id = "condition-123"
    loader._token_id = "token-yes-123"
    loader._pmxt_prefetch_workers = 2
    loader._pmxt_http_block_size = 32 * 1024 * 1024
    loader._pmxt_http_cache_type = "readahead"
    loader._pmxt_raw_root = raw_root
    loader._pmxt_disable_remote_archive = disable_remote_archive
    loader._reset_http_filesystem()
    return loader


def test_configured_pmxt_data_source_sets_raw_local_overrides(monkeypatch, tmp_path):
    mirror_root = tmp_path / "mirror"
    mirror_root.mkdir()
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "raw-local")
    monkeypatch.setenv(PMXT_LOCAL_MIRROR_DIR_ENV, str(mirror_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "raw-local"
        assert str(mirror_root) in selection.summary
        assert os.environ[PMXT_RELAY_BASE_URL_ENV] == "0"
        assert os.environ[PMXT_RAW_ROOT_ENV] == str(mirror_root)
        assert PMXT_DISABLE_REMOTE_ARCHIVE_ENV not in os.environ

    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_RELAY_BASE_URL_ENV) is None


def test_configured_pmxt_data_source_sets_filtered_local_overrides(
    monkeypatch,
    tmp_path,
):
    filtered_root = tmp_path / "filtered"
    filtered_root.mkdir()
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "filtered-local")
    monkeypatch.setenv(PMXT_LOCAL_FILTERED_DIR_ENV, str(filtered_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "filtered-local"
        assert str(filtered_root) in selection.summary
        assert os.environ[PMXT_RELAY_BASE_URL_ENV] == "0"
        assert os.environ[PMXT_CACHE_DIR_ENV] == str(filtered_root)
        assert os.environ[PMXT_DISABLE_REMOTE_ARCHIVE_ENV] == "1"


def test_configured_pmxt_data_source_preserves_manual_low_level_env(
    monkeypatch,
    tmp_path,
):
    mirror_root = tmp_path / "manual-mirror"
    mirror_root.mkdir()
    monkeypatch.delenv(PMXT_DATA_SOURCE_ENV, raising=False)
    monkeypatch.setenv(PMXT_RAW_ROOT_ENV, str(mirror_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "raw-local"
        assert os.environ[PMXT_RAW_ROOT_ENV] == str(mirror_root)


def test_configured_pmxt_data_source_requires_local_mirror(monkeypatch):
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "raw-local")
    monkeypatch.delenv(PMXT_LOCAL_MIRROR_DIR_ENV, raising=False)

    with pytest.raises(ValueError, match=PMXT_LOCAL_MIRROR_DIR_ENV):
        with configured_pmxt_data_source():
            pass


def test_runner_loader_reads_market_rows_from_local_raw_mirror(tmp_path):
    loader = _make_loader(raw_root=tmp_path)
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = (
        tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": [
                    "condition-123",
                    "condition-123",
                    "condition-456",
                ],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "price_change",
                ],
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
        {
            "update_type": "book_snapshot",
            "data": '{"token_id":"token-yes-123","seq":1}',
        },
    ]
