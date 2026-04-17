from __future__ import annotations

import asyncio
import os
from pathlib import Path
from urllib.request import Request

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import prediction_market_extensions.backtesting.data_sources.pmxt as pmxt_module
from prediction_market_extensions.backtesting.data_sources.pmxt import (
    PMXT_DATA_SOURCE_ENV,
    PMXT_LOCAL_RAWS_DIR_ENV,
    PMXT_PREFETCH_WORKERS_ENV,
    PMXT_RAW_ROOT_ENV,
    PMXT_RELAY_BASE_URL_ENV,
    PMXT_REMOTE_BASE_URL_ENV,
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
    loader._pmxt_relay_base_url = None
    loader._pmxt_source_priority = ("raw-local", "raw-remote", "relay-raw")
    loader._condition_id = "condition-123"
    loader._token_id = "token-yes-123"
    loader._pmxt_prefetch_workers = 2
    loader._pmxt_http_block_size = 32 * 1024 * 1024
    loader._pmxt_http_cache_type = "readahead"
    loader._pmxt_download_progress_callback = None
    loader._pmxt_scan_progress_callback = None
    loader._pmxt_progress_size_cache = {}
    loader._pmxt_temp_download_root = (
        cache_dir if cache_dir is not None else Path.cwd()
    ) / ".pmxt-temp-downloads"
    loader._pmxt_raw_root = raw_root
    loader._pmxt_disable_remote_archive = disable_remote_archive
    loader._reset_http_filesystem()
    return loader


def test_configured_pmxt_data_source_sets_raw_local_overrides(monkeypatch, tmp_path):
    mirror_root = tmp_path / "mirror"
    mirror_root.mkdir()
    monkeypatch.setenv(PMXT_DATA_SOURCE_ENV, "raw-local")
    monkeypatch.setenv(PMXT_LOCAL_RAWS_DIR_ENV, str(mirror_root))

    with configured_pmxt_data_source() as selection:
        assert selection.mode == "raw-local"
        assert str(mirror_root) in selection.summary
        assert RunnerPolymarketPMXTDataLoader._resolve_relay_base_url() is None
        assert RunnerPolymarketPMXTDataLoader._resolve_remote_base_url() is None
        assert RunnerPolymarketPMXTDataLoader._resolve_raw_root() == mirror_root
        assert RunnerPolymarketPMXTDataLoader._resolve_prefetch_workers() == 4

    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_RELAY_BASE_URL_ENV) is None


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
        sources=["archive:archive.vendor.test", f"local:{mirror_root}", "relay:relay.vendor.test"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            "PMXT source: explicit priority "
            f"(cache -> archive https://archive.vendor.test -> local {mirror_root} "
            "-> relay https://relay.vendor.test)"
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_raw_root() == mirror_root
        assert (
            RunnerPolymarketPMXTDataLoader._resolve_remote_base_url()
            == "https://archive.vendor.test"
        )
        assert (
            RunnerPolymarketPMXTDataLoader._resolve_relay_base_url() == "https://relay.vendor.test"
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_source_priority() == (
            "raw-remote",
            "raw-local",
            "relay-raw",
        )
        assert RunnerPolymarketPMXTDataLoader._resolve_prefetch_workers() == 4

    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_REMOTE_BASE_URL_ENV) is None
    assert os.getenv(PMXT_RELAY_BASE_URL_ENV) is None
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
        "relay-raw:https://relay.vendor.test",
    ],
)
def test_configured_pmxt_data_source_rejects_legacy_or_unprefixed_explicit_sources(
    source: str,
) -> None:
    with (
        pytest.raises(ValueError, match="Use one of: local:, archive:, relay:"),
        configured_pmxt_data_source(sources=[source]),
    ):
        pass


def test_configured_pmxt_data_source_isolates_concurrent_loader_config(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.delenv(PMXT_REMOTE_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(PMXT_RELAY_BASE_URL_ENV, raising=False)
    monkeypatch.delenv(PMXT_RAW_ROOT_ENV, raising=False)
    monkeypatch.delenv(PMXT_SOURCE_PRIORITY_ENV, raising=False)
    mirror_a = tmp_path / "mirror-a"
    mirror_b = tmp_path / "mirror-b"
    mirror_a.mkdir()
    mirror_b.mkdir()

    async def _capture(
        sources: list[str],
    ) -> tuple[Path | None, str | None, str | None, tuple[str, ...]]:
        with configured_pmxt_data_source(sources=sources):
            await asyncio.sleep(0)
            return (
                RunnerPolymarketPMXTDataLoader._resolve_raw_root(),
                RunnerPolymarketPMXTDataLoader._resolve_remote_base_url(),
                RunnerPolymarketPMXTDataLoader._resolve_relay_base_url(),
                RunnerPolymarketPMXTDataLoader._resolve_source_priority(),
            )

    async def _run() -> tuple[
        tuple[Path | None, str | None, str | None, tuple[str, ...]],
        tuple[Path | None, str | None, str | None, tuple[str, ...]],
    ]:
        return await asyncio.gather(
            _capture(
                [f"local:{mirror_a}", "archive:archive-a.vendor.test", "relay:relay-a.vendor.test"]
            ),
            _capture(
                [f"local:{mirror_b}", "archive:archive-b.vendor.test", "relay:relay-b.vendor.test"]
            ),
        )

    first, second = asyncio.run(_run())

    assert first == (
        mirror_a,
        "https://archive-a.vendor.test",
        "https://relay-a.vendor.test",
        ("raw-local", "raw-remote", "relay-raw"),
    ) or first == (
        mirror_a,
        "https://archive-a.vendor.test",
        "https://relay-a.vendor.test",
        ("raw-remote", "raw-local", "relay-raw"),
    )
    assert second == (
        mirror_b,
        "https://archive-b.vendor.test",
        "https://relay-b.vendor.test",
        ("raw-local", "raw-remote", "relay-raw"),
    ) or second == (
        mirror_b,
        "https://archive-b.vendor.test",
        "https://relay-b.vendor.test",
        ("raw-remote", "raw-local", "relay-raw"),
    )
    assert os.getenv(PMXT_RAW_ROOT_ENV) is None
    assert os.getenv(PMXT_REMOTE_BASE_URL_ENV) is None
    assert os.getenv(PMXT_RELAY_BASE_URL_ENV) is None


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


def test_runner_loader_emits_scan_progress_for_local_raw_mirror(monkeypatch, tmp_path) -> None:
    loader = _make_loader(raw_root=tmp_path)
    loader._pmxt_scan_progress_callback = object()
    hour = pd.Timestamp("2026-03-21T12:00:00Z")
    raw_path = tmp_path / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"pmxt")
    captured: dict[str, object] = {}

    def fake_dataset(path: str, *, format: str):
        captured["dataset_path"] = path
        captured["dataset_format"] = format
        return object()

    def fake_scan(dataset, *, batch_size: int, source: str, total_bytes: int | None):
        captured["dataset"] = dataset
        captured["batch_size"] = batch_size
        captured["source"] = source
        captured["total_bytes"] = total_bytes
        return ["batch"]

    monkeypatch.setattr(pmxt_module.ds, "dataset", fake_dataset)
    monkeypatch.setattr(loader, "_scan_raw_market_batches", fake_scan)

    batches = loader._load_local_archive_market_batches(hour, batch_size=1_000)

    assert batches == ["batch"]
    assert captured == {
        "dataset_path": str(raw_path),
        "dataset_format": "parquet",
        "dataset": captured["dataset"],
        "batch_size": 1_000,
        "source": str(raw_path),
        "total_bytes": 4,
    }


def test_runner_loader_never_uses_filtered_relay_path() -> None:
    loader = _make_loader()
    hour = pd.Timestamp("2026-03-21T12:00:00Z")

    assert loader._relay_url_for_hour(hour) is None


def test_runner_loader_honors_explicit_source_priority(monkeypatch) -> None:
    loader = _make_loader()
    loader._pmxt_source_priority = ("raw-remote", "raw-local", "relay-raw")
    calls: list[str] = []

    monkeypatch.setattr(loader, "_load_cached_market_batches", lambda hour: None)
    monkeypatch.setattr(
        loader,
        "_load_remote_market_batches",
        lambda hour, *, batch_size: calls.append("raw-remote") or None,
    )
    monkeypatch.setattr(
        loader,
        "_load_local_archive_market_batches",
        lambda hour, *, batch_size: calls.append("raw-local") or [],
    )
    monkeypatch.setattr(
        loader,
        "_load_relay_raw_market_batches",
        lambda hour, *, batch_size: calls.append("relay-raw") or [],
    )

    assert loader._load_market_batches(pd.Timestamp("2026-03-21T12:00:00Z"), batch_size=1_000) == []
    assert calls == ["raw-remote", "raw-local"]


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
