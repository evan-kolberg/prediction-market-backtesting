from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import pyarrow as pa

from nautilus_trader.adapters.polymarket import pmxt as pmxt_module
from nautilus_trader.adapters.polymarket.pmxt import PolymarketPMXTDataLoader


def _make_loader(cache_dir: Path | None) -> PolymarketPMXTDataLoader:
    loader = object.__new__(PolymarketPMXTDataLoader)
    loader._pmxt_cache_dir = cache_dir
    loader._pmxt_relay_base_url = None
    loader._condition_id = "condition-123"
    loader._token_id = "token-yes-123"
    loader._pmxt_prefetch_workers = 2
    loader._pmxt_http_block_size = 32 * 1024 * 1024
    loader._pmxt_http_cache_type = "readahead"
    loader._reset_http_filesystem()
    return loader


def test_resolve_cache_dir_defaults_to_xdg_cache_home(monkeypatch, tmp_path):
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_CACHE_DIR_ENV, raising=False)
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_DISABLE_CACHE_ENV, raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg-cache"))

    assert PolymarketPMXTDataLoader._resolve_cache_dir() == (
        tmp_path / "xdg-cache" / "nautilus_trader" / "pmxt"
    )


def test_resolve_prefetch_workers_parses_env(monkeypatch):
    monkeypatch.delenv(
        PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, raising=False
    )
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 16

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, "8")
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 8

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, "invalid")
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 16


def test_resolve_relay_base_url_parses_env(monkeypatch):
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_RELAY_BASE_URL_ENV, raising=False)
    assert (
        PolymarketPMXTDataLoader._resolve_relay_base_url()
        == "https://209-209-10-83.sslip.io"
    )

    monkeypatch.setenv(
        PolymarketPMXTDataLoader._PMXT_RELAY_BASE_URL_ENV,
        "http://relay.local:8080/",
    )
    assert (
        PolymarketPMXTDataLoader._resolve_relay_base_url() == "http://relay.local:8080"
    )

    monkeypatch.setenv(
        PolymarketPMXTDataLoader._PMXT_RELAY_BASE_URL_ENV,
        "0",
    )
    assert PolymarketPMXTDataLoader._resolve_relay_base_url() is None


def test_resolve_http_tuning_parses_env(monkeypatch):
    monkeypatch.delenv(
        PolymarketPMXTDataLoader._PMXT_HTTP_BLOCK_SIZE_MB_ENV, raising=False
    )
    monkeypatch.delenv(
        PolymarketPMXTDataLoader._PMXT_HTTP_CACHE_TYPE_ENV, raising=False
    )

    assert PolymarketPMXTDataLoader._resolve_http_block_size() == 32 * 1024 * 1024
    assert PolymarketPMXTDataLoader._resolve_http_cache_type() == "readahead"

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_HTTP_BLOCK_SIZE_MB_ENV, "64")
    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_HTTP_CACHE_TYPE_ENV, "bytes")

    assert PolymarketPMXTDataLoader._resolve_http_block_size() == 64 * 1024 * 1024
    assert PolymarketPMXTDataLoader._resolve_http_cache_type() == "bytes"

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_HTTP_BLOCK_SIZE_MB_ENV, "invalid")
    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_HTTP_CACHE_TYPE_ENV, "")

    assert PolymarketPMXTDataLoader._resolve_http_block_size() == 32 * 1024 * 1024
    assert PolymarketPMXTDataLoader._resolve_http_cache_type() == "readahead"


def test_load_market_table_writes_token_filtered_cache(tmp_path):
    loader = _make_loader(tmp_path)
    hour = pd.Timestamp("2026-03-16T12:00:00Z")
    remote_table = pa.table(
        {
            "update_type": ["book_snapshot", "price_change", "price_change"],
            "data": [
                '{"token_id":"token-yes-123","payload":"keep-1"}',
                '{"token_id":"token-no-456","payload":"drop"}',
                '{"token_id":"token-yes-123","payload":"keep-2"}',
            ],
        },
    )

    loader._load_remote_market_table = lambda _hour, *, batch_size: remote_table  # type: ignore[method-assign]

    loaded = loader._load_market_table(hour, batch_size=1_000)

    assert loaded is not None
    assert loaded.to_pylist() == [
        {
            "update_type": "book_snapshot",
            "data": '{"token_id":"token-yes-123","payload":"keep-1"}',
        },
        {
            "update_type": "price_change",
            "data": '{"token_id":"token-yes-123","payload":"keep-2"}',
        },
    ]
    assert loader._cache_path_for_hour(hour) == (
        tmp_path
        / "condition-123"
        / "token-yes-123"
        / "polymarket_orderbook_2026-03-16T12.parquet"
    )

    cached = loader._load_cached_market_table(hour)
    assert cached is not None
    assert cached.to_pylist() == loaded.to_pylist()


def test_load_market_table_prefers_cached_table(tmp_path):
    loader = _make_loader(tmp_path)
    hour = pd.Timestamp("2026-03-16T13:00:00Z")
    cached_table = pa.table(
        {
            "update_type": ["book_snapshot"],
            "data": ['{"token_id":"token-yes-123","payload":"cached"}'],
        },
    )
    loader._write_market_cache(hour, cached_table)

    def _fail_remote(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("remote load should not run when cache exists")

    loader._load_remote_market_table = _fail_remote  # type: ignore[method-assign]

    loaded = loader._load_market_table(hour, batch_size=1_000)

    assert loaded is not None
    assert loaded.to_pylist() == cached_table.to_pylist()


def test_load_market_batches_prefers_relay_before_remote(tmp_path):
    loader = _make_loader(tmp_path)
    loader._pmxt_relay_base_url = "http://relay.local:8080"
    hour = pd.Timestamp("2026-03-16T13:00:00Z")
    relay_batch = pa.record_batch(
        [
            pa.array(["book_snapshot"]),
            pa.array(['{"token_id":"token-yes-123","payload":"relay"}']),
        ],
        names=["update_type", "data"],
    )

    loader._load_relay_market_batches = (  # type: ignore[method-assign]
        lambda _hour, *, batch_size: [relay_batch]
    )

    def _fail_remote(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("remote load should not run when relay responds")

    loader._load_remote_market_batches = _fail_remote  # type: ignore[method-assign]

    batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert (
        batches[0].column("data")[0].as_py()
        == '{"token_id":"token-yes-123","payload":"relay"}'
    )


def test_load_market_batches_falls_back_to_remote_when_relay_errors(
    tmp_path, monkeypatch
):
    loader = _make_loader(tmp_path)
    loader._pmxt_relay_base_url = "http://relay.local:8080"
    hour = pd.Timestamp("2026-03-16T13:00:00Z")
    remote_batch = pa.record_batch(
        [
            pa.array(["book_snapshot"]),
            pa.array(['{"token_id":"token-yes-123","payload":"remote"}']),
        ],
        names=["update_type", "data"],
    )

    def _raise_relay(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("429 Too Many Requests")

    monkeypatch.setattr(pmxt_module.ds, "dataset", _raise_relay)
    loader._load_remote_market_batches = lambda _hour, *, batch_size: [remote_batch]  # type: ignore[method-assign]

    batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert (
        batches[0].column("data")[0].as_py()
        == '{"token_id":"token-yes-123","payload":"remote"}'
    )


def test_decode_book_snapshot_accepts_null_top_of_book_fields():
    payload = PolymarketPMXTDataLoader._decode_book_snapshot(
        '{"update_type":"book_snapshot","market_id":"condition-123","token_id":"token-yes-123",'
        '"side":"NO","best_bid":null,"best_ask":"0.02","timestamp":1771767624.001295,'
        '"bids":[],"asks":[["0.99","10"]]}'
    )

    assert payload.best_bid is None
    assert payload.best_ask == "0.02"


def test_decode_price_change_accepts_null_top_of_book_fields():
    payload = PolymarketPMXTDataLoader._decode_price_change(
        '{"update_type":"price_change","market_id":"condition-123","token_id":"token-yes-123",'
        '"side":"NO","best_bid":null,"best_ask":"0.02","timestamp":1771767624.001295,'
        '"change_price":"0.02","change_size":"10","change_side":"SELL"}'
    )

    assert payload.best_bid is None
    assert payload.best_ask == "0.02"


def test_iter_market_tables_preserves_hour_order(tmp_path):
    loader = _make_loader(tmp_path)
    hours = [
        pd.Timestamp("2026-03-16T12:00:00Z"),
        pd.Timestamp("2026-03-16T13:00:00Z"),
        pd.Timestamp("2026-03-16T14:00:00Z"),
    ]
    delays = {
        hours[0]: 0.05,
        hours[1]: 0.0,
        hours[2]: 0.01,
    }

    def _load(hour, *, batch_size):  # type: ignore[no-untyped-def]
        time.sleep(delays[hour])
        return pa.table(
            {
                "update_type": ["book_snapshot"],
                "data": [hour.isoformat()],
            },
        )

    loader._load_market_table = _load  # type: ignore[method-assign]

    yielded = list(loader._iter_market_tables(hours, batch_size=1_000))

    assert [hour for hour, _ in yielded] == hours
    assert [table.to_pylist()[0]["data"] for _, table in yielded] == [
        hour.isoformat() for hour in hours
    ]


def test_iter_market_batches_preserves_hour_order(tmp_path):
    loader = _make_loader(tmp_path)
    hours = [
        pd.Timestamp("2026-03-16T12:00:00Z"),
        pd.Timestamp("2026-03-16T13:00:00Z"),
        pd.Timestamp("2026-03-16T14:00:00Z"),
    ]
    delays = {
        hours[0]: 0.05,
        hours[1]: 0.0,
        hours[2]: 0.01,
    }

    def _load(hour, *, batch_size):  # type: ignore[no-untyped-def]
        time.sleep(delays[hour])
        return [
            pa.record_batch(
                [
                    pa.array(["book_snapshot"]),
                    pa.array([hour.isoformat()]),
                ],
                names=["update_type", "data"],
            ),
        ]

    loader._load_market_batches = _load  # type: ignore[method-assign]

    yielded = list(loader._iter_market_batches(hours, batch_size=1_000))

    assert [hour for hour, _ in yielded] == hours
    assert [batches[0].column("data")[0].as_py() for _, batches in yielded] == [
        hour.isoformat() for hour in hours
    ]
