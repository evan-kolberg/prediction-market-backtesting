from __future__ import annotations

import time
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from prediction_market_extensions.adapters.polymarket import pmxt as pmxt_module
from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader


def _make_loader(
    cache_dir: Path | None, *, local_archive_dir: Path | None = None
) -> PolymarketPMXTDataLoader:
    loader = object.__new__(PolymarketPMXTDataLoader)
    loader._pmxt_cache_dir = cache_dir
    loader._pmxt_local_archive_dir = local_archive_dir
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
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, raising=False)
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 16

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, "8")
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 8

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_PREFETCH_WORKERS_ENV, "invalid")
    assert PolymarketPMXTDataLoader._resolve_prefetch_workers() == 16


def test_resolve_local_archive_dir_parses_env(monkeypatch, tmp_path):
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_LOCAL_ARCHIVE_DIR_ENV, raising=False)
    assert PolymarketPMXTDataLoader._resolve_local_archive_dir() is None

    monkeypatch.setenv(
        PolymarketPMXTDataLoader._PMXT_LOCAL_ARCHIVE_DIR_ENV, str(tmp_path / "pmxt-archive")
    )
    assert PolymarketPMXTDataLoader._resolve_local_archive_dir() == (tmp_path / "pmxt-archive")

    monkeypatch.setenv(PolymarketPMXTDataLoader._PMXT_LOCAL_ARCHIVE_DIR_ENV, "0")
    assert PolymarketPMXTDataLoader._resolve_local_archive_dir() is None


def test_resolve_http_tuning_parses_env(monkeypatch):
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_HTTP_BLOCK_SIZE_MB_ENV, raising=False)
    monkeypatch.delenv(PolymarketPMXTDataLoader._PMXT_HTTP_CACHE_TYPE_ENV, raising=False)

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
        }
    )

    loader._load_remote_market_table = lambda _hour, *, batch_size: remote_table  # type: ignore[method-assign]

    loaded = loader._load_market_table(hour, batch_size=1_000)

    assert loaded is not None
    assert loaded.to_pylist() == [
        {"update_type": "book_snapshot", "data": '{"token_id":"token-yes-123","payload":"keep-1"}'},
        {"update_type": "price_change", "data": '{"token_id":"token-yes-123","payload":"keep-2"}'},
    ]
    assert loader._cache_path_for_hour(hour) == (
        tmp_path / "condition-123" / "token-yes-123" / "polymarket_orderbook_2026-03-16T12.parquet"
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
        }
    )
    loader._write_market_cache(hour, cached_table)

    def _fail_remote(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("remote load should not run when cache exists")

    loader._load_remote_market_table = _fail_remote  # type: ignore[method-assign]

    loaded = loader._load_market_table(hour, batch_size=1_000)

    assert loaded is not None
    assert loaded.to_pylist() == cached_table.to_pylist()


def test_scan_raw_market_batches_emits_scan_progress(tmp_path):
    loader = _make_loader(tmp_path / "cache")
    raw_path = tmp_path / "polymarket_orderbook_2026-03-16T13.parquet"
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-123", "condition-123"],
                "update_type": ["book_snapshot", "price_change"],
                "data": [
                    '{"token_id":"token-yes-123","payload":"keep"}',
                    '{"token_id":"token-no-456","payload":"drop"}',
                ],
            }
        ),
        raw_path,
    )

    events: list[tuple[int, int, int, int | None, bool]] = []
    loader._pmxt_scan_progress_callback = (
        lambda _source, scanned_batches, scanned_rows, matched_rows, total_bytes, finished: (
            events.append(  # type: ignore[assignment]
                (scanned_batches, scanned_rows, matched_rows, total_bytes, finished)
            )
        )
    )

    dataset = ds.dataset(str(raw_path), format="parquet")
    batches = loader._scan_raw_market_batches(
        dataset, batch_size=1_000, source=str(raw_path), total_bytes=raw_path.stat().st_size
    )

    assert batches
    assert events
    assert events[0] == (0, 0, 0, raw_path.stat().st_size, False)
    assert events[-1] == (1, 2, 1, raw_path.stat().st_size, True)


def test_cleanup_stale_temp_downloads_reaps_dead_process_roots(tmp_path, monkeypatch):
    loader = _make_loader(tmp_path)
    dead_root = loader._pmxt_temp_download_root / "pid-999999"
    dead_hour = dead_root / "hour-dead"
    dead_hour.mkdir(parents=True, exist_ok=True)
    (dead_hour / "payload.parquet").write_bytes(b"stale")

    monkeypatch.setattr(loader, "_pid_is_active", lambda pid: False)

    loader._cleanup_stale_temp_downloads()

    assert not dead_root.exists()


def test_load_remote_market_batches_downloads_to_temp_file_and_emits_progress(
    tmp_path, monkeypatch
):
    loader = _make_loader(tmp_path)
    hour = pd.Timestamp("2026-03-16T13:00:00Z")

    remote_buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-123", "condition-123", "other-condition"],
                "update_type": ["book_snapshot", "price_change", "book_snapshot"],
                "data": [
                    '{"token_id":"token-yes-123","payload":"snapshot"}',
                    '{"token_id":"token-yes-123","payload":"delta"}',
                    '{"token_id":"token-yes-123","payload":"drop-market"}',
                ],
            }
        ),
        remote_buffer,
    )

    class _Response:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload
            self._offset = 0

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            del exc_type, exc, tb
            return False

        @property
        def headers(self) -> dict[str, str]:
            return {"Content-Length": str(len(self._payload))}

        def read(self, size: int = -1) -> bytes:
            if size < 0:
                size = len(self._payload) - self._offset
            chunk = self._payload[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

    download_events: list[tuple[int, int | None, bool]] = []
    scan_events: list[tuple[int, int, int, int | None, bool]] = []
    loader._pmxt_download_progress_callback = (
        lambda _source, downloaded_bytes, total_bytes, finished: download_events.append(
            (downloaded_bytes, total_bytes, finished)
        )
    )
    loader._pmxt_scan_progress_callback = (
        lambda _source, scanned_batches, scanned_rows, matched_rows, total_bytes, finished: (
            scan_events.append((scanned_batches, scanned_rows, matched_rows, total_bytes, finished))
        )
    )

    monkeypatch.setattr(
        pmxt_module,
        "urlopen",
        lambda url: _Response(remote_buffer.getvalue()),  # type: ignore[arg-type]
    )

    batches = loader._load_remote_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert sum(batch.num_rows for batch in batches) == 2
    assert download_events
    assert download_events[0][0] == 0
    assert download_events[-1][0] == len(remote_buffer.getvalue())
    assert download_events[-1][2] is True
    assert scan_events
    assert scan_events[-1][:3] == (1, 2, 2)
    assert scan_events[-1][3] == len(remote_buffer.getvalue())
    assert scan_events[-1][4] is True
    assert loader._pmxt_temp_download_root.exists()
    assert not any(loader._pmxt_temp_download_root.iterdir())


def test_load_market_batches_prefers_local_archive_before_remote(tmp_path):
    raw_root = tmp_path / "raw-hours"
    loader = _make_loader(tmp_path / "cache", local_archive_dir=raw_root)
    hour = pd.Timestamp("2026-03-16T13:00:00Z")
    raw_path = raw_root / "polymarket_orderbook_2026-03-16T13.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-123", "condition-123", "other-condition"],
                "update_type": ["book_snapshot", "price_change", "book_snapshot"],
                "data": [
                    '{"token_id":"token-yes-123","payload":"local-book"}',
                    '{"token_id":"token-yes-123","payload":"local-price"}',
                    '{"token_id":"token-yes-123","payload":"drop-market"}',
                ],
            }
        ),
        raw_path,
    )

    def _fail_remote(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("remote load should not run when local archive exists")

    loader._load_remote_market_batches = _fail_remote  # type: ignore[method-assign]

    batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert [batch.to_pylist() for batch in batches] == [
        [
            {
                "update_type": "book_snapshot",
                "data": '{"token_id":"token-yes-123","payload":"local-book"}',
            },
            {
                "update_type": "price_change",
                "data": '{"token_id":"token-yes-123","payload":"local-price"}',
            },
        ]
    ]


def test_load_market_batches_reads_nested_local_archive_layout(tmp_path):
    raw_root = tmp_path / "raw-hours"
    loader = _make_loader(tmp_path / "cache", local_archive_dir=raw_root)
    hour = pd.Timestamp("2026-03-17T05:00:00Z")
    raw_path = raw_root / "2026/03/17/polymarket_orderbook_2026-03-17T05.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-123"],
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes-123","payload":"nested-local"}'],
            }
        ),
        raw_path,
    )

    batches = loader._load_market_batches(hour, batch_size=1_000)

    assert batches is not None
    assert batches[0].column("data")[0].as_py() == (
        '{"token_id":"token-yes-123","payload":"nested-local"}'
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
    delays = {hours[0]: 0.05, hours[1]: 0.0, hours[2]: 0.01}

    def _load(hour, *, batch_size):  # type: ignore[no-untyped-def]
        time.sleep(delays[hour])
        return pa.table({"update_type": ["book_snapshot"], "data": [hour.isoformat()]})

    loader._load_market_table = _load  # type: ignore[method-assign]

    yielded = list(loader._iter_market_tables(hours, batch_size=1_000))

    assert [hour for hour, _ in yielded] == hours
    assert [table.to_pylist()[0]["data"] for _, table in yielded] == [
        hour.isoformat() for hour in hours
    ]


def test_event_sort_key_orders_book_updates_before_quotes(monkeypatch):
    class _FakeOrderBookDeltas:
        def __init__(self, ts_event: int, ts_init: int) -> None:
            self.ts_event = ts_event
            self.ts_init = ts_init

    class _FakeQuoteTick:
        def __init__(self, ts_event: int, ts_init: int) -> None:
            self.ts_event = ts_event
            self.ts_init = ts_init

    monkeypatch.setattr(pmxt_module, "OrderBookDeltas", _FakeOrderBookDeltas)
    monkeypatch.setattr(pmxt_module, "QuoteTick", _FakeQuoteTick)

    quote = _FakeQuoteTick(ts_event=10, ts_init=11)
    delta = _FakeOrderBookDeltas(ts_event=10, ts_init=20)

    ordered = sorted([quote, delta], key=PolymarketPMXTDataLoader._event_sort_key)

    assert ordered == [delta, quote]


def test_load_order_book_and_quotes_keeps_snapshot_before_quote(monkeypatch, tmp_path):
    loader = _make_loader(tmp_path)
    loader._instrument = SimpleNamespace(id="POLYMARKET.TEST")
    hour = pd.Timestamp("2026-03-16T12:00:00Z")

    class _FakeOrderBook:
        def __init__(self, instrument_id, book_type):  # type: ignore[no-untyped-def]
            self.instrument_id = instrument_id
            self.book_type = book_type

    class _FakeOrderBookDeltas:
        def __init__(self, ts_event: int, ts_init: int) -> None:
            self.ts_event = ts_event
            self.ts_init = ts_init

    class _FakeQuoteTick:
        def __init__(self, ts_event: int, ts_init: int) -> None:
            self.ts_event = ts_event
            self.ts_init = ts_init

    monkeypatch.setattr(pmxt_module, "OrderBook", _FakeOrderBook)
    monkeypatch.setattr(pmxt_module, "OrderBookDeltas", _FakeOrderBookDeltas)
    monkeypatch.setattr(pmxt_module, "QuoteTick", _FakeQuoteTick)

    loader._archive_hours = lambda _start, _end: [hour]  # type: ignore[method-assign]
    loader._iter_market_batches = (  # type: ignore[method-assign]
        lambda hours, *, batch_size: iter(
            [
                (
                    hour,
                    [
                        pa.record_batch(
                            [
                                pa.array(["book_snapshot"]),
                                pa.array(['{"token_id":"token-yes-123"}']),
                            ],
                            names=["update_type", "data"],
                        )
                    ],
                )
            ]
        )
    )

    def _process_book_snapshot(  # type: ignore[no-untyped-def]
        payload_text,
        *,
        token_id,
        instrument,
        local_book,
        has_snapshot,
        events,
        start_ns,
        end_ns,
        include_order_book,
        include_quotes,
    ):
        del payload_text, token_id, instrument, has_snapshot, start_ns, end_ns
        if include_order_book:
            events.append(_FakeOrderBookDeltas(ts_event=10, ts_init=20))
        if include_quotes:
            events.append(_FakeQuoteTick(ts_event=10, ts_init=11))
        return local_book, True

    monkeypatch.setattr(loader, "_process_book_snapshot", _process_book_snapshot)

    data = loader.load_order_book_and_quotes(hour, hour + pd.Timedelta(hours=1))

    assert [type(record).__name__ for record in data] == ["_FakeOrderBookDeltas", "_FakeQuoteTick"]


def test_iter_market_batches_preserves_hour_order(tmp_path):
    loader = _make_loader(tmp_path)
    hours = [
        pd.Timestamp("2026-03-16T12:00:00Z"),
        pd.Timestamp("2026-03-16T13:00:00Z"),
        pd.Timestamp("2026-03-16T14:00:00Z"),
    ]
    delays = {hours[0]: 0.05, hours[1]: 0.0, hours[2]: 0.01}

    def _load(hour, *, batch_size):  # type: ignore[no-untyped-def]
        time.sleep(delays[hour])
        return [
            pa.record_batch(
                [pa.array(["book_snapshot"]), pa.array([hour.isoformat()])],
                names=["update_type", "data"],
            )
        ]

    loader._load_market_batches = _load  # type: ignore[method-assign]

    yielded = list(loader._iter_market_batches(hours, batch_size=1_000))

    assert [hour for hour, _ in yielded] == hours
    assert [batches[0].column("data")[0].as_py() for _, batches in yielded] == [
        hour.isoformat() for hour in hours
    ]
