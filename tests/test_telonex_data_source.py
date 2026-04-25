from __future__ import annotations

import os
import subprocess
import importlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.model.data import OrderBookDeltas, QuoteTick

import prediction_market_extensions.backtesting.data_sources.telonex as telonex_module
from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_CACHE_ROOT_ENV,
    TELONEX_API_KEY_ENV,
    TELONEX_LOCAL_DIR_ENV,
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


def _quote_parquet_payload(timestamp_us: int) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(
        {
            "timestamp_us": [timestamp_us],
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
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
        assert config.channel == "quotes"
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

    assert config.channel == "book_snapshot_full"


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


def test_telonex_api_url_uses_slug_and_outcome_id_without_key() -> None:
    url = RunnerPolymarketTelonexBookDataLoader._api_url(
        base_url="https://api.telonex.io/",
        channel="quotes",
        date="2026-01-20",
        market_slug="will-the-us-strike-iran-next-433",
        token_index=1,
        outcome=None,
    )

    assert url == (
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-20"
        "?slug=will-the-us-strike-iran-next-433&outcome_id=1"
    )


def test_telonex_runner_api_downloads_cache_then_clear(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cache_root = tmp_path / "telonex-cache"
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(cache_root))
    payload = _quote_parquet_payload(1_768_780_800_000_000)
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
        "channel": "quotes",
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
        "telonex-api::https://api.example.test/v1/downloads/polymarket/quotes/"
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
    pd.testing.assert_frame_equal(first, second)
    assert loader._telonex_last_api_source == f"telonex-cache::{cache_path}"

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
        "telonex-api::https://api.example.test/v1/downloads/polymarket/quotes/"
        "2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0"
    )


def test_telonex_load_quotes_uses_cache_before_local(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    config = module.TelonexLoaderConfig(
        channel="quotes",
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
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(loader, "_config", lambda: config)

    def fake_cache(**kwargs: object) -> pd.DataFrame:
        calls.append("cache")
        return frame

    def fail_local(**kwargs: object) -> None:
        calls.append("local")
        raise AssertionError("local should not be checked when cache has the day")

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when cache has the day")

    monkeypatch.setattr(loader, "_load_api_cache_day", fake_cache)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fail_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_quote_ticks_from_frame",
        lambda _frame, *, start, end: [SimpleNamespace(ts_event=1)],
    )

    records = loader.load_quotes(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="cache-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["cache"]


def test_telonex_load_quotes_uses_local_before_api_after_cache_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    config = module.TelonexLoaderConfig(
        channel="quotes",
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
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(loader, "_config", lambda: config)

    def fake_cache(**kwargs: object) -> None:
        calls.append("cache")
        return None

    def fake_local(**kwargs: object) -> pd.DataFrame:
        calls.append("local")
        return frame

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when local has the day")

    monkeypatch.setattr(loader, "_load_api_cache_day", fake_cache)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fake_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_quote_ticks_from_frame",
        lambda _frame, *, start, end: [SimpleNamespace(ts_event=1)],
    )

    records = loader.load_quotes(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="local-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["cache", "local"]


def test_telonex_full_book_snapshots_replay_l2_deltas_and_quotes() -> None:
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

    assert [type(record) for record in records] == [
        OrderBookDeltas,
        QuoteTick,
        OrderBookDeltas,
        QuoteTick,
    ]
    quotes = [record for record in records if isinstance(record, QuoteTick)]
    assert [(float(quote.bid_price), float(quote.ask_price)) for quote in quotes] == [
        (0.34, 0.39),
        (0.34, 0.38),
    ]


def test_telonex_full_book_loader_uses_cache_before_local(monkeypatch: pytest.MonkeyPatch) -> None:
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

    def fake_cache(**kwargs: object) -> pd.DataFrame:
        calls.append("cache")
        return frame

    def fail_local(**kwargs: object) -> None:
        calls.append("local")
        raise AssertionError("local should not be checked when cache has the day")

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when cache has the day")

    monkeypatch.setattr(loader, "_load_api_cache_day", fake_cache)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fail_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_book_events_from_frame",
        lambda _frame, *, start, end, include_order_book, include_quotes: [
            SimpleNamespace(ts_event=1, ts_init=1)
        ],
    )

    records = loader.load_order_book_and_quotes(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="cache-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["cache"]
    assert progress_events[0] == ("2026-01-19", "start", "none", 0)
    assert progress_events[1][0:2] == ("2026-01-19", "complete")
    assert progress_events[1][2].startswith("telonex-cache::")
    assert progress_events[1][3] == 1


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
    monkeypatch.setattr(loader, "_load_api_cache_day", lambda **kwargs: None)

    def fake_api_day(**kwargs: object) -> pd.DataFrame:
        calls.append("api")
        return api_frame

    monkeypatch.setattr(loader, "_load_api_day", fake_api_day)
    monkeypatch.setattr(
        loader,
        "_book_events_from_frame",
        lambda _frame, *, start, end, include_order_book, include_quotes: [
            SimpleNamespace(ts_event=1, ts_init=1)
        ],
    )

    with pytest.warns(UserWarning, match="pyarrow failed|trying next source"):
        records = loader.load_order_book_and_quotes(
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
    local_path = tmp_path / "polymarket" / "us-recession-by-end-of-2026" / "0" / "quotes.parquet"
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_consolidated_path(
            root=tmp_path,
            channel="quotes",
            market_slug="us-recession-by-end-of-2026",
            token_index=0,
            outcome=None,
        )
        == local_path
    )


def test_telonex_local_path_matches_daily_download_script_layout(tmp_path) -> None:
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    local_path = (
        tmp_path
        / "polymarket"
        / "us-recession-by-end-of-2026"
        / "0"
        / "quotes"
        / "2026-01-19.parquet"
    )
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_path_for_day(
            root=tmp_path,
            channel="quotes",
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


def test_telonex_blob_range_query_is_memoized_across_days(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Per-day callers must share one pyarrow.dataset scan for a single (slug, month).

    Before this regression test, _load_blob_range opened a fresh in-memory
    DuckDB connection and re-executed the same query for every backtest day.
    For a 30-day range that meant 30 redundant queries against the same
    Hive-partitioned parquet files — the dominant cost the user observed as
    "telonex cache slower than the API".  The pyarrow.dataset rewrite preserves
    the same memoization guarantee: one read per (store, channel, slug, token,
    outcome, start_ym, end_ym).
    """
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexBookDataLoader
    loader = loader_cls.__new__(loader_cls)
    loader._ensure_blob_scan_caches()
    store_root = tmp_path
    channel_dir = store_root / "data" / "channel=book_snapshot_full"
    month_dir = channel_dir / "year=2026" / "month=03"
    month_dir.mkdir(parents=True)
    (store_root / "telonex.duckdb").write_bytes(b"not-used")

    cached_frame = pd.DataFrame({"timestamp_us": [1], "bids": [[]], "asks": [[]]})
    scan_count = 0
    real_load_blob_range = loader._load_blob_range

    def counting_load_blob_range(**kwargs):
        nonlocal scan_count
        cache_key = (
            str(kwargs["store_root"]),
            kwargs["channel"],
            kwargs["market_slug"],
            kwargs["token_index"],
            kwargs["outcome"],
            kwargs["start"].year * 100 + kwargs["start"].month,
            kwargs["end"].year * 100 + kwargs["end"].month,
        )
        if cache_key in loader._telonex_blob_range_frames:
            scan_count += 0  # cache hit
        else:
            scan_count += 1  # actual read
        return real_load_blob_range(**kwargs)

    # Write a tiny valid parquet file so pyarrow.dataset can read it.
    import pyarrow as pa

    table = pa.table(
        {
            "market_slug": ["memo-test"],
            "outcome_segment": ["0"],
            "timestamp_us": [1],
            "bids": [["[]"]],
            "asks": [["[]"]],
        }
    )
    pa.parquet.write_table(table, month_dir / "part-000001.parquet")

    days = pd.date_range("2026-03-01", "2026-03-05", freq="D", tz="UTC")
    results = []
    for day in days:
        result = loader._load_blob_range(
            store_root=store_root,
            channel="book_snapshot_full",
            market_slug="memo-test",
            token_index=0,
            outcome=None,
            start=day,
            end=day + pd.Timedelta(hours=23, minutes=59),
        )
        results.append(result)

    # All 5 days should return the same cached frame object.
    non_none = [r for r in results if r is not None]
    assert len(non_none) == 5
    for frame in non_none[1:]:
        assert frame is non_none[0], "per-day callers must share the same frame"

    # The internal cache should have exactly one entry for this month.
    assert len(loader._telonex_blob_range_frames) == 1
