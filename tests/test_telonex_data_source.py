from __future__ import annotations

import os
import subprocess
import importlib
import threading
import time
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.model.data import OrderBookDeltas

import prediction_market_extensions.backtesting.data_sources.telonex as telonex_module
from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_CACHE_ROOT_ENV,
    TELONEX_API_KEY_ENV,
    TELONEX_FULL_BOOK_CHANNEL,
    TELONEX_LOCAL_DIR_ENV,
    TELONEX_PREFETCH_WORKERS_ENV,
    RunnerPolymarketTelonexBookDataLoader,
    configured_telonex_data_source,
    resolve_telonex_loader_config,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_polymarket_loader() -> RunnerPolymarketTelonexBookDataLoader:
    instrument = parse_polymarket_instrument(
        market_info={
            "condition_id": "0x" + "1" * 64,
            "question": "Synthetic Telonex market",
            "minimum_tick_size": "0.01",
            "minimum_order_size": "1",
            "end_date_iso": "2026-12-31T00:00:00Z",
            "maker_base_fee": "0",
            "taker_base_fee": "0",
        },
        token_id="2" * 64,
        outcome="Yes",
        ts_init=0,
    )
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    loader._instrument = instrument
    loader._token_id = "2" * 64
    loader._condition_id = "0x" + "1" * 64
    return loader


class _FakeHTTPResponse:
    def __init__(self, payload: bytes) -> None:
        self._buffer = BytesIO(payload)
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)


def _book_parquet_payload(timestamp_us: int, *, bid: str = "0.42", ask: str = "0.44") -> bytes:
    buffer = BytesIO()
    pd.DataFrame(
        {
            "timestamp_us": [timestamp_us],
            "bids": [[{"price": bid, "size": "10"}]],
            "asks": [[{"price": ask, "size": "11"}]],
        }
    ).to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_configured_telonex_data_source_preserves_explicit_order(tmp_path) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            f"Telonex source: explicit priority (cache -> local {local_root} -> "
            "api https://api.example.test (key missing))"
        )

        _selection, config = resolve_telonex_loader_config()
        assert config.channel == TELONEX_FULL_BOOK_CHANNEL
        assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
            ("local", str(local_root)),
            ("api", "https://api.example.test"),
        ]


def test_configured_telonex_data_source_can_pin_full_book_channel(tmp_path) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}"], channel="book_snapshot_full"
    ):
        _selection, config = resolve_telonex_loader_config()

    assert config.channel == TELONEX_FULL_BOOK_CHANNEL


def test_configured_telonex_data_source_omits_disabled_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, "0")

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.summary == (
            f"Telonex source: explicit priority (local {local_root} -> "
            "api https://api.example.test (key missing))"
        )


def test_configured_telonex_data_source_expands_env_in_api_key(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "sk-live-42")
    with configured_telonex_data_source(sources=["api:${TELONEX_API_KEY}"]) as selection:
        assert "(key set)" in selection.summary
        _selection, config = resolve_telonex_loader_config()
        assert config.ordered_source_entries[0].api_key == "sk-live-42"


def test_configured_telonex_data_source_reports_missing_key_when_var_unset(monkeypatch) -> None:
    monkeypatch.delenv("TELONEX_API_KEY", raising=False)
    with configured_telonex_data_source(sources=["api:${TELONEX_API_KEY}"]) as selection:
        assert "(key missing)" in selection.summary
        _selection, config = resolve_telonex_loader_config()
        assert config.ordered_source_entries[0].api_key is None


def test_telonex_default_api_source_requires_key_only_from_env(monkeypatch) -> None:
    monkeypatch.delenv(TELONEX_LOCAL_DIR_ENV, raising=False)
    monkeypatch.delenv(TELONEX_API_KEY_ENV, raising=False)

    with pytest.raises(ValueError, match=TELONEX_API_KEY_ENV):
        resolve_telonex_loader_config()

    monkeypatch.setenv(TELONEX_API_KEY_ENV, "test-key")
    _selection, config = resolve_telonex_loader_config()

    assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
        ("api", "https://api.telonex.io")
    ]
    assert os.getenv(TELONEX_API_KEY_ENV) == "test-key"


def test_telonex_prefetch_workers_default_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TELONEX_PREFETCH_WORKERS_ENV, raising=False)
    assert RunnerPolymarketTelonexBookDataLoader._resolve_prefetch_workers() == 128

    monkeypatch.setenv(TELONEX_PREFETCH_WORKERS_ENV, "7")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_prefetch_workers() == 7

    monkeypatch.setenv(TELONEX_PREFETCH_WORKERS_ENV, "invalid")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_prefetch_workers() == 128


def test_telonex_api_url_uses_slug_and_outcome_id_without_key() -> None:
    url = RunnerPolymarketTelonexBookDataLoader._api_url(
        base_url="https://api.telonex.io/",
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-20",
        market_slug="will-the-us-strike-iran-next-433",
        token_index=1,
        outcome=None,
    )

    assert url == (
        "https://api.telonex.io/v1/downloads/polymarket/book_snapshot_full/2026-01-20"
        "?slug=will-the-us-strike-iran-next-433&outcome_id=1"
    )


def test_telonex_runner_api_downloads_cache_then_clear(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cache_root = tmp_path / "telonex-cache"
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(cache_root))
    payload = _book_parquet_payload(1_768_780_800_000_000)
    resolve_calls: list[tuple[str, str]] = []
    fetch_calls: list[str] = []

    def fake_resolve_presigned_url(*, url: str, api_key: str) -> str:
        resolve_calls.append((url, api_key))
        return "https://presigned.example.test/day.parquet"

    def fake_urlopen(request, timeout: int):  # type: ignore[no-untyped-def]
        fetch_calls.append(request.full_url)
        return _FakeHTTPResponse(payload)

    monkeypatch.setattr(
        RunnerPolymarketTelonexBookDataLoader,
        "_resolve_presigned_url",
        staticmethod(fake_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fake_urlopen)

    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    load_kwargs = {
        "base_url": "https://api.example.test",
        "channel": TELONEX_FULL_BOOK_CHANNEL,
        "date": "2026-01-19",
        "market_slug": "us-recession-by-end-of-2026",
        "token_index": 0,
        "outcome": None,
    }

    first = loader._load_api_day(**load_kwargs, api_key="test-key")

    assert first is not None
    assert len(first) == 1
    assert len(resolve_calls) == 1
    assert len(fetch_calls) == 1
    cache_path = loader._api_cache_path(**load_kwargs)
    assert cache_path is not None
    assert cache_path.exists()
    assert cache_path.is_relative_to(cache_root)
    assert loader._telonex_last_api_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/book_snapshot_full/"
        "2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0"
    )

    def fail_resolve_presigned_url(*, url: str, api_key: str) -> str:
        raise AssertionError("cache hit should not request a presigned URL")

    def fail_urlopen(request, timeout: int):  # type: ignore[no-untyped-def]
        raise AssertionError("cache hit should not download from Telonex")

    monkeypatch.setattr(
        RunnerPolymarketTelonexBookDataLoader,
        "_resolve_presigned_url",
        staticmethod(fail_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fail_urlopen)

    second = loader._load_api_day(**load_kwargs, api_key=None)

    assert second is not None
    assert list(second.columns) == [
        "timestamp_us",
        "bid_prices",
        "bid_sizes",
        "ask_prices",
        "ask_sizes",
    ]
    assert second.iloc[0]["bid_prices"] == ["0.42"]
    assert second.iloc[0]["ask_prices"] == ["0.44"]
    fast_cache_path = cache_path.with_name(f"{cache_path.stem}.fast.parquet")
    assert loader._telonex_last_api_source == f"telonex-cache-fast::{fast_cache_path}"

    result = subprocess.run(
        [
            "make",
            "clear-telonex-cache",
            f"TELONEX_CACHE_ROOT={cache_root}",
            f"TELONEX_DATA_DESTINATION={tmp_path / 'telonex-data'}",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert 'rm -rf "' in result.stdout
    assert str(cache_root) in result.stdout
    assert cache_root.exists()
    assert not cache_path.exists()

    monkeypatch.setattr(
        RunnerPolymarketTelonexBookDataLoader,
        "_resolve_presigned_url",
        staticmethod(fake_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fake_urlopen)

    third = loader._load_api_day(**load_kwargs, api_key="test-key")

    assert third is not None
    pd.testing.assert_frame_equal(first, third)
    assert len(resolve_calls) == 2
    assert len(fetch_calls) == 2
    assert cache_path.exists()
    assert loader._telonex_last_api_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/book_snapshot_full/"
        "2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0"
    )


def test_telonex_full_book_snapshots_replay_l2_deltas() -> None:
    loader = _make_polymarket_loader()
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000, 1_768_780_800_100_000],
            "bids": [
                [{"price": "0.34", "size": "10"}, {"price": "0.33", "size": "20"}],
                [{"price": "0.34", "size": "7"}, {"price": "0.32", "size": "5"}],
            ],
            "asks": [
                [{"price": "0.39", "size": "11"}, {"price": "0.40", "size": "22"}],
                [{"price": "0.38", "size": "12"}, {"price": "0.40", "size": "22"}],
            ],
        }
    )

    records = loader._book_events_from_frame(
        frame,
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-20T00:00:00Z"),
    )

    assert all(isinstance(record, OrderBookDeltas) for record in records)
    assert len(records) == 2


def test_telonex_crossed_snapshot_resets_until_next_valid_full_snapshot() -> None:
    loader = _make_polymarket_loader()
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000, 1_768_780_801_000_000],
            "bids": [
                [{"price": "0.60", "size": "10"}],
                [{"price": "0.34", "size": "10"}],
            ],
            "asks": [
                [{"price": "0.55", "size": "11"}],
                [{"price": "0.39", "size": "11"}],
            ],
        }
    )

    with pytest.warns(RuntimeWarning, match="crossed/invalid full-book"):
        records = loader._book_events_from_frame(
            frame,
            start=pd.Timestamp("2026-01-19T00:00:00Z"),
            end=pd.Timestamp("2026-01-20T00:00:00Z"),
        )

    assert len(records) == 1
    assert records[0].is_snapshot
    assert int(records[0].ts_event) == 1_768_780_801_000_000_000


def test_telonex_timestamp_ms_keeps_exact_end_boundary_record() -> None:
    loader = _make_polymarket_loader()
    timestamp_ms = 1_768_780_800_123
    frame = pd.DataFrame(
        {
            "timestamp_ms": [timestamp_ms],
            "bids": [[{"price": "0.34", "size": "10"}]],
            "asks": [[{"price": "0.39", "size": "11"}]],
        }
    )
    event_ns = timestamp_ms * 1_000_000

    records = loader._book_events_from_frame(
        frame,
        start=pd.Timestamp(event_ns - 1_000, unit="ns", tz="UTC"),
        end=pd.Timestamp(event_ns, unit="ns", tz="UTC"),
    )

    assert len(records) == 1
    assert int(records[0].ts_event) == event_ns


def test_telonex_materialized_deltas_cache_round_trips(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path))
    loader = _make_polymarket_loader()
    start = pd.Timestamp("2026-01-19T00:00:00Z")
    end = pd.Timestamp("2026-01-19T23:59:59Z")
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000, 1_768_780_800_100_000],
            "bids": [
                [{"price": "0.34", "size": "10"}],
                [{"price": "0.34", "size": "8"}],
            ],
            "asks": [
                [{"price": "0.39", "size": "11"}],
                [{"price": "0.38", "size": "12"}],
            ],
        }
    )
    records = loader._book_events_from_frame(frame, start=start, end=end)
    metadata = loader._deltas_cache_metadata_payload(
        source={"kind": "test"},
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
        start=start,
        end=end,
    )

    loader._write_deltas_cache_day(
        records=records,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
        start=start,
        end=end,
        metadata=metadata,
    )
    cached_records, source = loader._load_deltas_cache_day(
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
        start=start,
        end=end,
        expected_metadata=metadata,
    )

    assert source.startswith("telonex-deltas-cache::")
    assert cached_records is not None
    assert len(cached_records) == len(records)
    assert [len(record.deltas) for record in cached_records] == [
        len(record.deltas) for record in records
    ]
    assert [int(record.ts_event) for record in cached_records] == [
        int(record.ts_event) for record in records
    ]


def test_telonex_api_source_skips_materialized_deltas_cache_metadata() -> None:
    loader = _make_polymarket_loader()
    config = telonex_module.TelonexLoaderConfig(
        channel=TELONEX_FULL_BOOK_CHANNEL,
        ordered_source_entries=(
            telonex_module.TelonexSourceEntry(
                kind="api",
                target="https://api.example.test",
                api_key="test-key",
            ),
        ),
    )
    start = pd.Timestamp("2026-01-19T00:00:00Z")
    end = pd.Timestamp("2026-01-19T23:59:59Z")

    source = loader._deltas_cache_source_fingerprint(
        config=config,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
    )

    assert source is None
    assert (
        loader._deltas_cache_metadata_payload(
            source=source,
            channel=TELONEX_FULL_BOOK_CHANNEL,
            date="2026-01-19",
            market_slug="cache-test",
            token_index=0,
            outcome="Yes",
            start=start,
            end=end,
        )
        is None
    )


def test_telonex_fast_api_cache_invalidates_when_raw_cache_changes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "cache"))
    loader = _make_polymarket_loader()
    load_kwargs = {
        "base_url": "https://api.example.test",
        "channel": TELONEX_FULL_BOOK_CHANNEL,
        "date": "2026-01-19",
        "market_slug": "cache-correction-market",
        "token_index": 0,
        "outcome": "Yes",
    }
    payload_v1 = _book_parquet_payload(1_768_780_800_000_000, bid="0.42")
    payload_v2 = _book_parquet_payload(1_768_780_800_000_000, bid="0.77")
    frame_v1 = pd.read_parquet(BytesIO(payload_v1))

    loader._write_api_cache_day(payload=payload_v1, **load_kwargs)
    loader._write_fast_cache_day(frame=frame_v1, **load_kwargs)
    time.sleep(0.001)
    loader._write_api_cache_day(payload=payload_v2, **load_kwargs)

    frame, source = loader._load_api_day_cached(**load_kwargs)

    assert frame is not None
    assert source.startswith("telonex-cache::")
    assert frame.iloc[0]["bids"][0]["price"] == "0.77"


def test_telonex_blob_source_fingerprint_tracks_part_file_changes(tmp_path: Path) -> None:
    loader = _make_polymarket_loader()
    local_root = tmp_path / "telonex-blob"
    part_path = (
        local_root
        / "data"
        / "channel=book_snapshot_full"
        / "year=2026"
        / "month=01"
        / "part-000.parquet"
    )
    part_path.parent.mkdir(parents=True)
    (local_root / "telonex.duckdb").write_bytes(b"legacy/no-manifest")

    def write_part(best_bid: str) -> None:
        pq.write_table(
            pa.table(
                {
                    "market_slug": ["blob-market"],
                    "outcome_segment": ["Yes"],
                    "timestamp_us": [1_768_780_800_000_000],
                    "bids": [[{"price": best_bid, "size": "10"}]],
                    "asks": [[{"price": "0.44", "size": "11"}]],
                    "year": [2026],
                    "month": [1],
                }
            ),
            part_path,
        )

    write_part("0.42")
    entry = telonex_module.TelonexSourceEntry(kind="local", target=str(local_root))
    first = loader._telonex_source_fingerprint_for_entry(
        entry=entry,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="blob-market",
        token_index=0,
        outcome="Yes",
    )
    time.sleep(0.001)
    write_part("0.55")
    second = loader._telonex_source_fingerprint_for_entry(
        entry=entry,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="blob-market",
        token_index=0,
        outcome="Yes",
    )

    assert first is not None
    assert second is not None
    assert first["layout"] == "blob-legacy"
    assert "parts" in first
    assert first != second


def test_telonex_local_fingerprint_tracks_same_size_same_mtime_correction(
    tmp_path: Path,
) -> None:
    path = tmp_path / "day.parquet"
    path.write_bytes(b"bid=0.4200")
    first_stat = path.stat()
    first = RunnerPolymarketTelonexBookDataLoader._local_file_fingerprint(path)

    time.sleep(0.01)
    path.write_bytes(b"bid=0.7700")
    os.utime(path, ns=(first_stat.st_atime_ns, first_stat.st_mtime_ns))
    second_stat = path.stat()
    if second_stat.st_ctime_ns == first_stat.st_ctime_ns:
        pytest.skip("filesystem did not expose ctime change for same-size rewrite")
    second = RunnerPolymarketTelonexBookDataLoader._local_file_fingerprint(path)

    assert first is not None
    assert second is not None
    assert first["size"] == second["size"]
    assert first["mtime_ns"] == second["mtime_ns"]
    assert first["ctime_ns"] != second["ctime_ns"]
    assert first != second


def test_telonex_blob_empty_manifest_falls_back_to_legacy_parts(tmp_path: Path) -> None:
    loader = _make_polymarket_loader()
    local_root = tmp_path / "telonex-blob"
    part_path = (
        local_root
        / "data"
        / f"channel={TELONEX_FULL_BOOK_CHANNEL}"
        / "year=2026"
        / "month=01"
        / "part-000.parquet"
    )
    part_path.parent.mkdir(parents=True)
    con = telonex_module.duckdb.connect(str(local_root / "telonex.duckdb"))
    try:
        con.execute(
            "CREATE TABLE completed_days("
            "channel VARCHAR, market_slug VARCHAR, outcome_segment VARCHAR, "
            "day DATE, rows BIGINT, parquet_part VARCHAR)"
        )
    finally:
        con.close()
    pd.DataFrame(
        {
            "market_slug": ["stale-manifest-market"],
            "outcome_segment": ["Yes"],
            "timestamp_us": [1_768_780_800_000_000],
            "bids": [[{"price": "0.42", "size": "10"}]],
            "asks": [[{"price": "0.44", "size": "11"}]],
        }
    ).to_parquet(part_path, index=False)

    frame = loader._load_blob_range(
        store_root=local_root,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        market_slug="stale-manifest-market",
        token_index=0,
        outcome="Yes",
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-19T23:59:59Z"),
    )
    fingerprint = loader._local_blob_source_fingerprint_for_day(
        root=local_root,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="stale-manifest-market",
        token_index=0,
        outcome="Yes",
    )

    assert frame is not None
    assert len(frame) == 1
    assert frame.iloc[0]["bids"][0]["price"] == "0.42"
    assert fingerprint is not None
    assert fingerprint["layout"] == "blob-legacy"
    assert len(fingerprint["parts"]) == 1


def test_telonex_materialized_deltas_cache_ignores_unversioned_stale_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "cache"))
    loader = _make_polymarket_loader()
    market_slug = "timestamp-cache-test"
    date = "2026-01-19"
    timestamp_ms = 1_768_780_800_123
    correct_ns = timestamp_ms * 1_000_000
    stale_ns = int(float(timestamp_ms) * 1_000_000)
    start = pd.Timestamp(correct_ns - 1_000_000, unit="ns", tz="UTC")
    end = pd.Timestamp(correct_ns, unit="ns", tz="UTC")
    local_root = tmp_path / "local"
    local_path = (
        local_root
        / "polymarket"
        / market_slug
        / "0"
        / TELONEX_FULL_BOOK_CHANNEL
        / f"{date}.parquet"
    )
    local_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {
            "timestamp_ms": [timestamp_ms],
            "bids": [[{"price": "0.34", "size": "10"}]],
            "asks": [[{"price": "0.39", "size": "11"}]],
        }
    )
    frame.to_parquet(local_path, index=False)
    correct_records = loader._book_events_from_frame(frame, start=start, end=end)
    stale_frame = loader._deltas_records_to_table(correct_records).to_pandas()
    stale_frame["ts_event"] = stale_ns
    stale_frame["ts_init"] = stale_ns
    cache_path = loader._deltas_cache_path(
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date=date,
        market_slug=market_slug,
        token_index=0,
        outcome="Yes",
        instrument_id=loader.instrument.id,
        start=start,
        end=end,
    )
    assert cache_path is not None
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    stale_frame.to_parquet(cache_path, index=False)

    loader._config = lambda: telonex_module.TelonexLoaderConfig(  # type: ignore[method-assign]
        channel=TELONEX_FULL_BOOK_CHANNEL,
        ordered_source_entries=(
            telonex_module.TelonexSourceEntry(kind="local", target=str(local_root)),
        ),
    )

    records = loader.load_order_book_deltas(
        start,
        end,
        market_slug=market_slug,
        token_index=0,
        outcome="Yes",
    )

    assert [int(record.ts_event) for record in records] == [correct_ns]


def test_telonex_full_book_loader_uses_local_before_api_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    config = module.TelonexLoaderConfig(
        channel="book_snapshot_full",
        ordered_source_entries=(
            module.TelonexSourceEntry(kind="local", target="/tmp/local"),
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "bids": [[{"price": "0.34", "size": "10"}]],
            "asks": [[{"price": "0.39", "size": "11"}]],
        }
    )
    calls: list[str] = []
    progress_events: list[tuple[str, str, str, int]] = []

    monkeypatch.setattr(loader, "_config", lambda: config)
    loader._telonex_day_progress_callback = lambda date, event, source, rows: (
        progress_events.append((date, event, source, rows))
    )

    def fail_cache(**kwargs: object) -> tuple[pd.DataFrame, str]:
        calls.append("cache")
        raise AssertionError("api cache should not be checked before an earlier local source")

    def fake_local(**kwargs: object) -> pd.DataFrame:
        calls.append("local")
        return frame

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when local has the day")

    monkeypatch.setattr(loader, "_load_api_day_cached", fail_cache)
    monkeypatch.setattr(loader, "_load_deltas_cache_day", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_write_deltas_cache_day", lambda **kwargs: None)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fake_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(loader, "_try_load_day_from_api_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_book_events_from_frame",
        lambda _frame, *, start, end, include_order_book: [SimpleNamespace(ts_event=1, ts_init=1)],
    )

    records = loader.load_order_book_deltas(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="cache-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["local"]
    assert progress_events[0] == ("2026-01-19", "start", "none", 0)
    assert progress_events[1][0:2] == ("2026-01-19", "complete")
    assert "telonex-local" in progress_events[1][2]
    assert progress_events[1][3] == 1


def test_telonex_full_book_loader_prefetches_api_days(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    loader._telonex_prefetch_workers = 3
    config = module.TelonexLoaderConfig(
        channel="book_snapshot_full",
        ordered_source_entries=(
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    active = 0
    max_active = 0
    active_lock = threading.Lock()

    monkeypatch.setattr(loader, "_config", lambda: config)
    monkeypatch.setattr(loader, "_load_api_day_cached", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_load_deltas_cache_day", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_write_deltas_cache_day", lambda **kwargs: None)

    def fake_api_day(*, date: str, **kwargs: object) -> pd.DataFrame:
        del kwargs
        nonlocal active, max_active
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            day = int(date.rsplit("-", 1)[1])
            return pd.DataFrame(
                {
                    "timestamp_us": [1_768_780_800_000_000 + day],
                    "bids": [[{"price": "0.34", "size": "10"}]],
                    "asks": [[{"price": "0.39", "size": "11"}]],
                }
            )
        finally:
            with active_lock:
                active -= 1

    monkeypatch.setattr(loader, "_load_api_day", fake_api_day)
    monkeypatch.setattr(
        loader,
        "_book_events_from_frame",
        lambda frame, *, start, end, include_order_book: [
            SimpleNamespace(
                ts_event=int(frame["timestamp_us"].iloc[0]),
                ts_init=int(frame["timestamp_us"].iloc[0]),
            )
        ],
    )

    records = loader.load_order_book_deltas(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-21 23:59:59", tz="UTC"),
        market_slug="prefetch-test",
        token_index=0,
        outcome=None,
    )

    assert max_active > 1
    assert [int(record.ts_event) for record in records] == sorted(
        int(record.ts_event) for record in records
    )


def test_telonex_blob_timestamp_cache_reads_are_thread_safe() -> None:
    loader_cls = telonex_module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    loader._ensure_blob_scan_caches()
    frame = pd.DataFrame({"timestamp_us": [1, 2, 3]})
    ts_ns = np.array([1_000, 2_000, 3_000], dtype=np.int64)
    target_key = ("root", "book_snapshot_full", "market", 0, None, 20260101, 20260131)

    with loader._telonex_blob_scan_lock:
        for idx in range(500):
            loader._telonex_blob_range_frames[
                ("root", "book_snapshot_full", f"other-{idx}", 0, None, 20260101, 20260131)
            ] = None
        loader._telonex_blob_range_frames[target_key] = frame
        loader._telonex_blob_ts_ns[target_key] = {"timestamp_us": ts_ns}

    stop = threading.Event()

    def _writer() -> None:
        idx = 0
        while not stop.is_set():
            with loader._telonex_blob_scan_lock:
                loader._telonex_blob_range_frames[
                    ("root", "book_snapshot_full", f"writer-{idx}", 0, None, 20260101, 20260131)
                ] = None
            idx += 1

    thread = threading.Thread(target=_writer)
    thread.start()
    try:
        for _ in range(200):
            assert loader._cached_ts_ns_for_frame(frame, "timestamp_us") is ts_ns
    finally:
        stop.set()
        thread.join(timeout=2)


def test_telonex_full_book_loader_falls_back_to_api_when_blob_partition_is_incomplete(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    local_root = tmp_path / "telonex"
    partition_dir = local_root / "data" / "channel=book_snapshot_full" / "year=2026" / "month=01"
    partition_dir.mkdir(parents=True)
    (local_root / "telonex.duckdb").write_bytes(b"not-used")
    (partition_dir / "part-000001.parquet").write_bytes(b"incomplete")

    config = module.TelonexLoaderConfig(
        channel="book_snapshot_full",
        ordered_source_entries=(
            module.TelonexSourceEntry(kind="local", target=str(local_root)),
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    api_frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "bids": [[{"price": "0.34", "size": "10"}]],
            "asks": [[{"price": "0.39", "size": "11"}]],
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(loader, "_config", lambda: config)
    monkeypatch.setattr(loader, "_load_api_day_cached", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_load_deltas_cache_day", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_write_deltas_cache_day", lambda **kwargs: None)

    def fake_api_day(**kwargs: object) -> pd.DataFrame:
        calls.append("api")
        return api_frame

    monkeypatch.setattr(loader, "_load_api_day", fake_api_day)
    monkeypatch.setattr(
        loader,
        "_book_events_from_frame",
        lambda _frame, *, start, end, include_order_book: [SimpleNamespace(ts_event=1, ts_init=1)],
    )

    with pytest.warns(UserWarning, match="skipping blob store"):
        records = loader.load_order_book_deltas(
            pd.Timestamp("2026-01-19", tz="UTC"),
            pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
            market_slug="fallback-test",
            token_index=0,
            outcome=None,
        )

    assert len(records) == 1
    assert calls == ["api"]


def test_telonex_local_range_matches_consolidated_download_script_layout(tmp_path) -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    local_path = (
        tmp_path / "polymarket" / "us-recession-by-end-of-2026" / "0" / "book_snapshot_full.parquet"
    )
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_consolidated_path(
            root=tmp_path,
            channel=TELONEX_FULL_BOOK_CHANNEL,
            market_slug="us-recession-by-end-of-2026",
            token_index=0,
            outcome=None,
        )
        == local_path
    )


def test_telonex_local_blob_candidates_include_outcome_id_segment() -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)

    assert loader._outcome_segment_candidates(token_index=0, outcome="Yes") == (
        "Yes",
        "outcome_id=0",
        "0",
    )


def test_telonex_local_path_matches_daily_download_script_layout(tmp_path) -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    local_path = (
        tmp_path
        / "polymarket"
        / "us-recession-by-end-of-2026"
        / "0"
        / TELONEX_FULL_BOOK_CHANNEL
        / "2026-01-19.parquet"
    )
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_path_for_day(
            root=tmp_path,
            channel=TELONEX_FULL_BOOK_CHANNEL,
            date="2026-01-19",
            market_slug="us-recession-by-end-of-2026",
            token_index=0,
            outcome=None,
        )
        == local_path
    )


def test_telonex_rejects_unprefixed_sources() -> None:
    with pytest.raises(ValueError, match="Use one of: local:, api:"):
        resolve_telonex_loader_config(sources=["https://api.telonex.io"])
