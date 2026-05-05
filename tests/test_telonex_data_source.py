from __future__ import annotations

import os
import subprocess
import importlib
import threading
import time
from datetime import date
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.enums import AggressorSide

import prediction_market_extensions.backtesting.data_sources.telonex as telonex_module
from scripts import _telonex_data_download as telonex_download
from prediction_market_extensions._runtime_log import capture_loader_events
from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_API_WORKERS_ENV,
    TELONEX_CACHE_PREFETCH_WORKERS_ENV,
    TELONEX_CACHE_ROOT_ENV,
    TELONEX_API_KEY_ENV,
    TELONEX_FILE_WORKERS_ENV,
    TELONEX_FULL_BOOK_CHANNEL,
    TELONEX_LOCAL_DIR_ENV,
    TELONEX_LOCAL_PREFETCH_WORKERS_ENV,
    TELONEX_ONCHAIN_FILLS_CHANNEL,
    TELONEX_PREFETCH_WORKERS_ENV,
    TELONEX_TRADES_CHANNEL,
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


def test_telonex_api_slot_respects_global_worker_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TELONEX_API_WORKERS_ENV, "2")
    monkeypatch.setattr(telonex_module, "_TELONEX_API_SEMAPHORE", None)
    active = 0
    max_active = 0
    lock = threading.Lock()

    def _worker() -> None:
        nonlocal active, max_active
        with telonex_module._telonex_api_slot():
            with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)

    assert max_active == 2


def test_telonex_file_slot_respects_global_worker_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TELONEX_FILE_WORKERS_ENV, "3")
    monkeypatch.setattr(telonex_module, "_TELONEX_FILE_SEMAPHORE", None)
    active = 0
    max_active = 0
    lock = threading.Lock()

    def _worker() -> None:
        nonlocal active, max_active
        with telonex_module._telonex_file_slot():
            with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)

    assert max_active == 3


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


def _book_parquet_payload(timestamp_us: int) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(
        {
            "timestamp_us": [timestamp_us],
            "bids": [[{"price": "0.42", "size": "10"}]],
            "asks": [[{"price": "0.44", "size": "11"}]],
        }
    ).to_parquet(buffer, index=False)
    return buffer.getvalue()


def _write_onchain_fills_store(
    root: Path, *, rows: int = 1, channel: str = TELONEX_ONCHAIN_FILLS_CHANNEL
) -> None:
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000 + index for index in range(rows)],
            "price": ["0.42"] * rows,
            "size": ["7"] * rows,
            "side": ["BUY"] * rows,
            "transaction_hash": [f"0xlocaltx{index}" for index in range(rows)],
        }
    )
    store = telonex_download._TelonexParquetStore(root)
    try:
        store.ingest_batch(
            [
                telonex_download._DownloadResult(
                    job=telonex_download._Job(
                        market_slug="onchain-fill-market",
                        outcome_segment="0",
                        outcome_id=0,
                        outcome=None,
                        channel=channel,
                        day=date(2026, 1, 19),
                    ),
                    status="ok",
                    table=telonex_download.pa.Table.from_pandas(frame, preserve_index=False),
                    payload=None,
                    bytes_downloaded=100,
                    error=None,
                )
            ]
        )
    finally:
        store.close()


def _write_onchain_fills_empty_marker(root: Path) -> None:
    store = telonex_download._TelonexParquetStore(root)
    try:
        store.ingest_batch(
            [
                telonex_download._DownloadResult(
                    job=telonex_download._Job(
                        market_slug="onchain-fill-market",
                        outcome_segment="0",
                        outcome_id=0,
                        outcome=None,
                        channel=TELONEX_ONCHAIN_FILLS_CHANNEL,
                        day=date(2026, 1, 19),
                    ),
                    status="missing",
                    table=None,
                    payload=None,
                    bytes_downloaded=0,
                    error="404",
                )
            ]
        )
    finally:
        store.close()


def test_configured_telonex_data_source_preserves_explicit_order(tmp_path) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            f"Telonex book source: explicit priority (cache -> local {local_root} -> "
            "api https://api.example.test (key missing))\n"
            f"Telonex trade source: explicit priority (cache -> local {local_root} -> "
            "api https://api.example.test (key missing) -> polymarket cache -> "
            "api https://data-api.polymarket.com/trades)"
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
            f"Telonex book source: explicit priority (local {local_root} -> "
            "api https://api.example.test (key missing))\n"
            f"Telonex trade source: explicit priority (local {local_root} -> "
            "api https://api.example.test (key missing) -> polymarket cache -> "
            "api https://data-api.polymarket.com/trades)"
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


def test_telonex_cache_prefetch_workers_default_and_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TELONEX_CACHE_PREFETCH_WORKERS_ENV, raising=False)
    assert RunnerPolymarketTelonexBookDataLoader._resolve_cache_prefetch_workers() == 64

    monkeypatch.setenv(TELONEX_CACHE_PREFETCH_WORKERS_ENV, "11")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_cache_prefetch_workers() == 11

    monkeypatch.setenv(TELONEX_CACHE_PREFETCH_WORKERS_ENV, "invalid")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_cache_prefetch_workers() == 64


def test_telonex_api_workers_default_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TELONEX_API_WORKERS_ENV, raising=False)
    assert RunnerPolymarketTelonexBookDataLoader._resolve_api_worker_limit() == 32

    monkeypatch.setenv(TELONEX_API_WORKERS_ENV, "48")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_api_worker_limit() == 48

    monkeypatch.setenv(TELONEX_API_WORKERS_ENV, "invalid")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_api_worker_limit() == 32


def test_telonex_local_prefetch_workers_default_and_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TELONEX_LOCAL_PREFETCH_WORKERS_ENV, raising=False)
    assert RunnerPolymarketTelonexBookDataLoader._resolve_local_prefetch_workers() == 4

    monkeypatch.setenv(TELONEX_LOCAL_PREFETCH_WORKERS_ENV, "3")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_local_prefetch_workers() == 3

    monkeypatch.setenv(TELONEX_LOCAL_PREFETCH_WORKERS_ENV, "invalid")
    assert RunnerPolymarketTelonexBookDataLoader._resolve_local_prefetch_workers() == 4


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


def test_telonex_api_native_uses_temp_file_when_cache_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, "0")
    loader = _make_polymarket_loader()
    payload = BytesIO()
    pq.write_table(pa.table({"timestamp_us": [1_768_780_800_000_000]}), payload)
    data = payload.getvalue()

    class FakeResponse:
        headers = {"Content-Length": str(len(data))}

        def __init__(self) -> None:
            self.offset = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            del exc_type, exc, tb
            return False

        def read(self, size: int) -> bytes:
            chunk = data[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

    monkeypatch.setattr(
        loader,
        "_resolve_presigned_url",
        lambda *, url, api_key: "https://download.example.test/day.parquet",
    )
    monkeypatch.setattr(telonex_module, "urlopen", lambda *args, **kwargs: FakeResponse())

    native_paths: list[Path] = []

    def fake_native_rows(*, path, row_groups, start_ns, end_ns):  # type: ignore[no-untyped-def]
        del row_groups, start_ns, end_ns
        native_path = Path(path)
        assert native_path.exists()
        native_paths.append(native_path)
        return (None, [], [], [], [], [], [], [], [], [])

    monkeypatch.setattr(
        telonex_module,
        "telonex_parquet_book_snapshot_diff_rows",
        fake_native_rows,
    )

    result = loader._try_load_deltas_day_from_api_native(
        entry=telonex_module.TelonexSourceEntry(
            kind="api",
            target="https://api.example.test",
            api_key="test-key",
        ),
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-20",
        market_slug="api-temp-market",
        token_index=0,
        outcome=None,
        start=pd.Timestamp("2026-01-20T00:00:00Z"),
        end=pd.Timestamp("2026-01-20T23:59:59Z"),
    )

    assert result is not None
    records, delta_columns, source = result
    assert records == []
    assert delta_columns["event_index"] == []
    assert source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/"
        "book_snapshot_full/2026-01-20?slug=api-temp-market&outcome_id=0"
    )
    assert native_paths
    assert not native_paths[0].exists()


def test_telonex_onchain_fills_prefer_local_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "telonex-cache"))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_store(local_root)
    loader = _make_polymarket_loader()

    def fail_api_day(self, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        raise AssertionError("local onchain_fills should be read before Telonex API")

    monkeypatch.setattr(RunnerPolymarketTelonexBookDataLoader, "_load_api_day", fail_api_day)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert trades is not None
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.42)
    assert float(trades[0].size) == pytest.approx(7.0)
    assert trades[0].aggressor_side == AggressorSide.BUYER
    assert loader._telonex_last_trade_source == f"telonex-local::{local_root}"


def test_telonex_onchain_fills_materialize_local_store_to_trade_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "telonex-cache"))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_store(local_root)
    loader = _make_polymarket_loader()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        first = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert first is not None
    assert len(first) == 1
    assert loader._telonex_last_trade_source == f"telonex-local::{local_root}"

    def fail_local(**kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        raise AssertionError("materialized trade cache should be read before local store")

    def fail_api(self, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        raise AssertionError("materialized trade cache should be read before API")

    monkeypatch.setattr(loader, "_try_load_day_from_local", fail_local)
    monkeypatch.setattr(RunnerPolymarketTelonexBookDataLoader, "_load_api_day", fail_api)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        second = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert second is not None
    assert len(second) == 1
    assert float(second[0].price) == pytest.approx(0.42)
    assert str(loader._telonex_last_trade_source).startswith("telonex-trade-cache::")


def test_telonex_onchain_fills_use_cache_before_local_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cache_root = tmp_path / "telonex-cache"
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(cache_root))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_store(local_root)
    loader = _make_polymarket_loader()
    cache_path = loader._api_cache_path(
        base_url="https://api.example.test",
        channel=TELONEX_ONCHAIN_FILLS_CHANNEL,
        date="2026-01-19",
        market_slug="onchain-fill-market",
        token_index=0,
        outcome="Yes",
    )
    assert cache_path is not None
    cache_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "price": ["0.44"],
            "size": ["3"],
            "side": ["SELL"],
            "transaction_hash": ["0xcachedtx"],
        }
    ).to_parquet(cache_path, index=False)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert trades is not None
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.44)
    assert trades[0].aggressor_side == AggressorSide.SELLER
    assert loader._telonex_last_trade_source == f"telonex-cache::{cache_path}"


def test_telonex_onchain_fills_empty_local_manifest_falls_through_to_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "telonex-cache"))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_empty_marker(local_root)
    loader = _make_polymarket_loader()
    api_calls: list[str] = []

    def fake_api_day(self, **kwargs):  # type: ignore[no-untyped-def]
        del self
        assert kwargs["channel"] == TELONEX_ONCHAIN_FILLS_CHANNEL
        api_calls.append(kwargs["market_slug"])
        return pd.DataFrame(
            {
                "timestamp_us": [1_768_780_800_000_000],
                "price": ["0.43"],
                "size": ["5"],
                "side": ["SELL"],
                "transaction_hash": ["0xapitx"],
            }
        )

    monkeypatch.setattr(RunnerPolymarketTelonexBookDataLoader, "_load_api_day", fake_api_day)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert api_calls == ["onchain-fill-market"]
    assert trades is not None
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.43)
    assert trades[0].aggressor_side == AggressorSide.SELLER
    assert loader._telonex_last_trade_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/"
        "onchain_fills/2026-01-19?slug=onchain-fill-market&outcome=Yes"
    )


def test_telonex_onchain_fills_empty_local_manifest_falls_through_to_telonex_trades(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "telonex-cache"))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_empty_marker(local_root)
    _write_onchain_fills_store(local_root, channel=TELONEX_TRADES_CHANNEL)
    loader = _make_polymarket_loader()

    def fail_api_day(self, **kwargs):  # type: ignore[no-untyped-def]
        del self, kwargs
        raise AssertionError("local Telonex trades should be read before API fallback")

    monkeypatch.setattr(RunnerPolymarketTelonexBookDataLoader, "_load_api_day", fail_api_day)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert trades is not None
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.42)
    assert loader._telonex_last_trade_source == f"telonex-local-trades::{local_root}"


def test_telonex_onchain_fills_ignores_stale_empty_trade_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cache_root = tmp_path / "telonex-cache"
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(cache_root))
    local_root = tmp_path / "telonex-local"
    _write_onchain_fills_store(local_root)
    loader = _make_polymarket_loader()
    cache_path = loader._trade_ticks_cache_path(
        channel=TELONEX_ONCHAIN_FILLS_CHANNEL,
        date="2026-01-19",
        market_slug="onchain-fill-market",
        token_index=0,
        outcome="Yes",
        instrument_id=loader.instrument.id,
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-19T23:59:59Z"),
    )
    assert cache_path is not None
    cache_path.parent.mkdir(parents=True)
    loader._trade_ticks_to_cache_table(()).to_pandas().to_parquet(cache_path, index=False)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert trades is not None
    assert len(trades) == 1
    assert loader._telonex_last_trade_source == f"telonex-local::{local_root}"
    assert len(pd.read_parquet(cache_path)) == 1


def test_telonex_onchain_fills_try_api_before_polymarket_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path / "telonex-cache"))
    local_root = tmp_path / "missing-local"
    local_root.mkdir()
    loader = _make_polymarket_loader()
    api_calls: list[str] = []

    def fake_api_day(self, **kwargs):  # type: ignore[no-untyped-def]
        del self
        assert kwargs["channel"] == TELONEX_ONCHAIN_FILLS_CHANNEL
        api_calls.append(kwargs["market_slug"])
        return pd.DataFrame(
            {
                "timestamp_us": [1_768_780_800_000_000],
                "price": ["0.43"],
                "size": ["5"],
                "side": ["SELL"],
                "transaction_hash": ["0xapitx"],
            }
        )

    monkeypatch.setattr(RunnerPolymarketTelonexBookDataLoader, "_load_api_day", fake_api_day)

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ):
        trades = loader.load_telonex_onchain_fill_ticks(
            pd.Timestamp("2026-01-19T00:00:00Z"),
            pd.Timestamp("2026-01-19T23:59:59Z"),
            market_slug="onchain-fill-market",
            token_index=0,
            outcome=None,
        )

    assert api_calls == ["onchain-fill-market"]
    assert trades is not None
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.43)
    assert trades[0].aggressor_side == AggressorSide.SELLER
    assert loader._telonex_last_trade_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/"
        "onchain_fills/2026-01-19?slug=onchain-fill-market&outcome=Yes"
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


def test_telonex_delta_columns_preserve_instrument_rounding() -> None:
    loader = _make_polymarket_loader()
    records = loader._deltas_records_from_columns(
        {
            "event_index": [0],
            "action": [1],
            "side": [1],
            "price": [0.105],
            "size": [1009.1234564],
            "flags": [0],
            "sequence": [0],
            "ts_event": [100],
            "ts_init": [100],
        }
    )
    delta = records[0].deltas[0]

    assert delta.order.price.raw == loader.instrument.make_price(0.105).raw
    assert delta.order.size.raw == loader.instrument.make_qty(1009.1234564).raw


def test_telonex_delta_cache_table_avoids_python_dict_materialization() -> None:
    loader = _make_polymarket_loader()
    table = pa.table(
        {
            "event_index": pa.chunked_array([pa.array([0], pa.int32()), pa.array([1], pa.int32())]),
            "action": pa.array([1, 2], pa.uint8()),
            "side": pa.array([1, 2], pa.uint8()),
            "price": pa.array([0.105, 0.205], pa.float64()),
            "size": pa.array([1009.1234564, 7.0], pa.float64()),
            "flags": pa.array([128, 128], pa.uint8()),
            "sequence": pa.array([0, 0], pa.int32()),
            "ts_event": pa.array([100, 101], pa.int64()),
            "ts_init": pa.array([100, 101], pa.int64()),
        }
    )

    records = loader._deltas_records_from_table(table)

    assert len(records) == 2
    first_delta = records[0].deltas[0]
    second_delta = records[1].deltas[0]
    assert first_delta.order.price.raw == loader.instrument.make_price(0.105).raw
    assert first_delta.order.size.raw == loader.instrument.make_qty(1009.1234564).raw
    assert second_delta.order.price.raw == loader.instrument.make_price(0.205).raw


def test_telonex_flat_book_snapshots_use_native_diff_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_polymarket_loader()
    ts0 = 1_768_780_800_000_000_000
    ts1 = 1_768_780_800_100_000_000
    frame = pd.DataFrame(
        {
            "timestamp_us": [ts0 // 1_000, ts1 // 1_000],
            "bid_prices": [["0.34"], ["0.34"]],
            "bid_sizes": [["10"], ["7"]],
            "ask_prices": [["0.39"], ["0.39"]],
            "ask_sizes": [["11"], ["11"]],
        }
    )
    calls: list[dict[str, object]] = []

    def fake_native_diff_rows(
        **kwargs: object,
    ) -> tuple[
        int,
        list[int],
        list[int],
        list[int],
        list[float],
        list[float],
        list[int],
        list[int],
        list[int],
        list[int],
    ]:
        calls.append(kwargs)
        return (
            0,
            [0, 0, 0, 1],
            [4, 1, 1, 2],
            [0, 1, 2, 1],
            [0.0, 0.34, 0.39, 0.34],
            [0.0, 10.0, 11.0, 7.0],
            [0, 0, 128, 128],
            [0, 0, 0, 1],
            [ts0, ts0, ts0, ts1],
            [ts0, ts0, ts0, ts1],
        )

    monkeypatch.setattr(
        telonex_module,
        "telonex_flat_book_snapshot_diff_rows",
        fake_native_diff_rows,
    )

    records = loader._book_events_from_frame(
        frame,
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-20T00:00:00Z"),
    )

    assert calls
    assert [int(value) for value in calls[0]["timestamp_ns"]] == [ts0, ts1]
    assert len(records) == 2
    assert len(records[0].deltas) == 3
    assert int(records[1].deltas[0].action) == 2
    assert int(records[1].deltas[0].order.side) == 1
    assert float(records[1].deltas[0].order.price) == pytest.approx(0.34)
    assert float(records[1].deltas[0].order.size) == pytest.approx(7.0)
    assert int(records[1].deltas[0].flags) == 128


def test_telonex_nested_book_snapshots_use_native_diff_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_polymarket_loader()
    ts0 = 1_768_780_800_000_000_000
    ts1 = 1_768_780_800_100_000_000
    frame = pd.DataFrame(
        {
            "timestamp_us": [ts0 // 1_000, ts1 // 1_000],
            "bids": [
                [{"price": "0.34", "size": "10"}],
                [{"price": "0.34", "size": "7"}],
            ],
            "asks": [
                [{"price": "0.39", "size": "11"}],
                [{"price": "0.39", "size": "11"}],
            ],
        }
    )
    calls: list[dict[str, object]] = []

    def fake_native_diff_rows(
        **kwargs: object,
    ) -> tuple[
        int,
        list[int],
        list[int],
        list[int],
        list[float],
        list[float],
        list[int],
        list[int],
        list[int],
        list[int],
    ]:
        calls.append(kwargs)
        return (
            0,
            [0, 0, 0, 1],
            [4, 1, 1, 2],
            [0, 1, 2, 1],
            [0.0, 0.34, 0.39, 0.34],
            [0.0, 10.0, 11.0, 7.0],
            [0, 0, 128, 128],
            [0, 0, 0, 1],
            [ts0, ts0, ts0, ts1],
            [ts0, ts0, ts0, ts1],
        )

    monkeypatch.setattr(
        telonex_module,
        "telonex_nested_book_snapshot_diff_rows",
        fake_native_diff_rows,
    )

    records = loader._book_events_from_frame(
        frame,
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-20T00:00:00Z"),
    )

    assert calls
    assert [int(value) for value in calls[0]["timestamp_ns"]] == [ts0, ts1]
    assert list(calls[0]["bids"]) == list(frame["bids"])
    assert len(records) == 2
    assert len(records[0].deltas) == 3
    assert int(records[1].deltas[0].action) == 2
    assert int(records[1].deltas[0].order.side) == 1
    assert float(records[1].deltas[0].order.price) == pytest.approx(0.34)
    assert float(records[1].deltas[0].order.size) == pytest.approx(7.0)


def test_telonex_onchain_fills_use_native_trade_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_polymarket_loader()
    ts_event = 1_768_780_800_000_000_000
    frame = pd.DataFrame(
        {
            "timestamp_us": [ts_event // 1_000],
            "price": ["0.42"],
            "size": ["7"],
            "side": ["BUY"],
            "transaction_hash": ["0xnative"],
        }
    )
    calls: list[dict[str, object]] = []

    def fake_native_trade_rows(
        **kwargs: object,
    ) -> tuple[
        list[float],
        list[float],
        list[int],
        list[str],
        list[int],
        list[int],
    ]:
        calls.append(kwargs)
        return ([0.42], [7.0], [1], ["native-trade-id"], [ts_event], [ts_event])

    monkeypatch.setattr(telonex_module, "telonex_onchain_fill_trade_rows", fake_native_trade_rows)

    trades = loader._onchain_fill_trade_ticks_from_frame(
        frame,
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-20T00:00:00Z"),
    )

    assert calls
    assert [int(value) for value in calls[0]["timestamp_ns"]] == [ts_event]
    assert len(trades) == 1
    assert float(trades[0].price) == pytest.approx(0.42)
    assert float(trades[0].size) == pytest.approx(7.0)
    assert trades[0].aggressor_side == AggressorSide.BUYER
    assert str(trades[0].trade_id) == "native-trade-id"
    assert int(trades[0].ts_event) == ts_event


def test_telonex_native_trade_rows_preserve_instrument_rounding() -> None:
    loader = _make_polymarket_loader()
    trade = loader._trade_ticks_from_native_columns(
        ([0.105], [7.1234564], [1], ["rounding-check"], [100], [100])
    )[0]

    assert trade.price.raw == loader.instrument.make_price(0.105).raw
    assert trade.size.raw == loader.instrument.make_qty(7.1234564).raw


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

    loader._write_deltas_cache_day(
        records=records,
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
        start=start,
        end=end,
    )
    cached_records, source = loader._load_deltas_cache_day(
        channel=TELONEX_FULL_BOOK_CHANNEL,
        date="2026-01-19",
        market_slug="cache-test",
        token_index=0,
        outcome="Yes",
        start=start,
        end=end,
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


def test_telonex_materialized_deltas_cache_writes_use_unique_temp_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(tmp_path))
    monkeypatch.setenv(TELONEX_FILE_WORKERS_ENV, "2")
    monkeypatch.setattr(telonex_module, "_TELONEX_FILE_SEMAPHORE", None)
    loader = _make_polymarket_loader()
    start = pd.Timestamp("2026-01-19T00:00:00Z")
    end = pd.Timestamp("2026-01-19T23:59:59Z")
    records = loader._book_events_from_frame(
        pd.DataFrame(
            {
                "timestamp_us": [1_768_780_800_000_000],
                "bids": [[{"price": "0.34", "size": "10"}]],
                "asks": [[{"price": "0.39", "size": "11"}]],
            }
        ),
        start=start,
        end=end,
    )
    barrier = threading.Barrier(2)
    seen_tmp_paths: list[Path] = []
    seen_lock = threading.Lock()
    original_write_table = telonex_module.pq.write_table

    def write_table_with_barrier(table, where, *args, **kwargs):  # type: ignore[no-untyped-def]
        with seen_lock:
            seen_tmp_paths.append(Path(where))
        barrier.wait(timeout=5)
        return original_write_table(table, where, *args, **kwargs)

    monkeypatch.setattr(telonex_module.pq, "write_table", write_table_with_barrier)

    def write_cache() -> None:
        loader._write_deltas_cache_day(
            records=records,
            channel=TELONEX_FULL_BOOK_CHANNEL,
            date="2026-01-19",
            market_slug="cache-test",
            token_index=0,
            outcome="Yes",
            start=start,
            end=end,
        )

    with capture_loader_events() as capture:
        threads = [threading.Thread(target=write_cache) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert len(seen_tmp_paths) == 2
    assert len(set(seen_tmp_paths)) == 2
    assert not [event for event in capture.events if event.level == "ERROR"]


def test_telonex_day_progress_emits_loader_event() -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    loader._telonex_market_slug = "cache-test"
    loader._telonex_token_index = 0
    loader._telonex_outcome = "Yes"

    with capture_loader_events() as capture:
        loader._day_progress(
            "2026-01-19",
            "complete",
            "telonex-deltas-cache::/tmp/day.parquet",
            12,
        )

    assert len(capture.events) == 1
    event = capture.events[0]
    assert event.vendor == "telonex"
    assert event.stage == "cache_read"
    assert event.status == "cache_hit"
    assert event.source_kind == "cache"
    assert event.market_slug == "cache-test"
    assert event.token_id == "0"
    assert event.rows == 12


def test_telonex_api_cache_write_failure_emits_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    bad_cache_root = tmp_path / "not-a-directory"
    bad_cache_root.write_text("occupied", encoding="utf-8")
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(bad_cache_root))
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)

    with capture_loader_events() as capture:
        loader._write_api_cache_day(
            payload=b"payload",
            base_url="https://api.telonex.io",
            channel=TELONEX_FULL_BOOK_CHANNEL,
            date="2026-04-21",
            market_slug="cache-test",
            token_index=0,
            outcome="Yes",
        )

    event = next(event for event in capture.events if event.stage == "cache_write")
    assert event.level == "ERROR"
    assert event.status == "error"
    assert event.vendor == "telonex"
    assert event.origin == "telonex._write_api_cache_day"
    assert event.source_kind == "cache"
    assert event.market_slug == "cache-test"
    assert event.attrs["cache_kind"] == "api"


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
        "_book_events_and_delta_columns_from_frame",
        lambda _frame, *, start, end, include_order_book: (
            [SimpleNamespace(ts_event=1, ts_init=1)],
            None,
        ),
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
        "_book_events_and_delta_columns_from_frame",
        lambda frame, *, start, end, include_order_book: (
            [
                SimpleNamespace(
                    ts_event=int(frame["timestamp_us"].iloc[0]),
                    ts_init=int(frame["timestamp_us"].iloc[0]),
                )
            ],
            None,
        ),
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


def test_telonex_local_source_caps_day_prefetch_even_with_api_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    loader._telonex_prefetch_workers = 128
    monkeypatch.delenv(module.TELONEX_LOCAL_PREFETCH_WORKERS_ENV, raising=False)
    config = module.TelonexLoaderConfig(
        channel="book_snapshot_full",
        ordered_source_entries=(
            module.TelonexSourceEntry(kind="local", target=str(tmp_path / "telonex")),
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    active = 0
    max_active = 0
    active_lock = threading.Lock()

    monkeypatch.setattr(loader, "_config", lambda: config)
    monkeypatch.setattr(loader, "_load_deltas_cache_day", lambda **kwargs: (None, "none"))
    monkeypatch.setattr(loader, "_try_load_day_from_local", lambda **kwargs: None)
    monkeypatch.setattr(loader, "_write_deltas_cache_day", lambda **kwargs: None)
    monkeypatch.setattr(loader, "_day_progress", lambda *args, **kwargs: None)

    def fake_api_day(*, date: str, **kwargs: object) -> pd.DataFrame:
        del kwargs
        nonlocal active, max_active
        with active_lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.02)
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
        "_book_events_and_delta_columns_from_frame",
        lambda frame, *, start, end, include_order_book: (
            [
                SimpleNamespace(
                    ts_event=int(frame["timestamp_us"].iloc[0]),
                    ts_init=int(frame["timestamp_us"].iloc[0]),
                )
            ],
            None,
        ),
    )

    records = loader.load_order_book_deltas(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-21 23:59:59", tz="UTC"),
        market_slug="prefetch-test",
        token_index=0,
        outcome=None,
    )

    assert 1 < max_active <= loader_cls._resolve_local_prefetch_workers()
    assert len(records) == 3


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
        loader._telonex_blob_frame_id_by_key[target_key] = id(frame)
        loader._telonex_blob_ts_ns_by_frame_id[id(frame)] = {"timestamp_us": ts_ns}

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
        "_book_events_and_delta_columns_from_frame",
        lambda _frame, *, start, end, include_order_book: (
            [SimpleNamespace(ts_event=1, ts_init=1)],
            None,
        ),
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


def test_telonex_blob_reader_prunes_with_row_group_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    local_root = tmp_path / "telonex"
    partition_dir = local_root / "data" / "channel=book_snapshot_full" / "year=2026" / "month=01"
    partition_dir.mkdir(parents=True)
    part_path = partition_dir / "part-000000.parquet"

    def _table(market_slug: str, ts_us: int) -> pa.Table:
        return pa.table(
            {
                "timestamp_us": pa.array([ts_us], type=pa.int64()),
                "exchange": pa.array(["polymarket"]),
                "bids": pa.array([[{"price": "0.34", "size": "10"}]]),
                "asks": pa.array([[{"price": "0.39", "size": "11"}]]),
                "market_slug": pa.array([market_slug]),
                "outcome_segment": pa.array(["Yes"]),
            }
        )

    first = _table("target-market", 1_768_780_800_000_000)
    with pq.ParquetWriter(part_path, first.schema) as writer:
        writer.write_table(first)
        writer.write_table(_table("other-market", 1_768_780_800_000_000))
        writer.write_table(_table("target-market", 1_768_867_200_000_000))

    def fail_dataset(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("row-group indexed blobs should not use dataset fallback")

    monkeypatch.setattr(module.ds, "dataset", fail_dataset)

    frame = loader._load_blob_range(
        store_root=local_root,
        channel="book_snapshot_full",
        market_slug="target-market",
        token_index=0,
        outcome="Yes",
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-19T23:59:59Z"),
    )

    assert frame is not None
    assert frame["timestamp_us"].tolist() == [1_768_780_800_000_000]
    assert "bids" not in frame.columns
    assert frame["bid_prices"].tolist() == [["0.34"]]
    assert frame["ask_sizes"].tolist() == [["11"]]


def test_telonex_local_daily_file_is_used_before_blob_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    local_root = tmp_path / "telonex"
    daily_path = (
        local_root
        / "polymarket"
        / "book_snapshot_full"
        / "daily-test-market"
        / "Yes"
        / "2026-01-19.parquet"
    )
    daily_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "bids": [[{"price": "0.34", "size": "10"}]],
            "asks": [[{"price": "0.39", "size": "11"}]],
        }
    ).to_parquet(daily_path, index=False)
    (local_root / "data" / "channel=book_snapshot_full" / "year=2026" / "month=01").mkdir(
        parents=True
    )
    (local_root / "telonex.duckdb").write_bytes(b"not-used")

    def fail_blob(**kwargs: object) -> None:
        del kwargs
        raise AssertionError("daily replay file should be checked before blob store")

    monkeypatch.setattr(loader, "_load_blob_range", fail_blob)

    frame = loader._try_load_day_from_local(
        entry=telonex_module.TelonexSourceEntry(kind="local", target=str(local_root)),
        channel="book_snapshot_full",
        date="2026-01-19",
        market_slug="daily-test-market",
        token_index=0,
        outcome="Yes",
        start=pd.Timestamp("2026-01-19T00:00:00Z"),
        end=pd.Timestamp("2026-01-19T23:59:59Z"),
        range_cache={},
    )

    assert frame is not None
    assert frame["timestamp_us"].tolist() == [1_768_780_800_000_000]


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
