from __future__ import annotations

from datetime import date
from io import BytesIO
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from scripts import _telonex_data_download as telonex_download


def _parquet_payload(timestamp_us: int) -> bytes:
    frame = pd.DataFrame(
        {
            "timestamp_us": [timestamp_us],
            "bid_price": [0.44],
            "ask_price": [0.45],
            "bid_size": [10.0],
            "ask_size": [12.0],
        }
    )
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _parquet_payload_with_extra(
    timestamp_us: int, *, extra_columns: dict[str, object] | None = None
) -> bytes:
    data: dict[str, list[object]] = {
        "timestamp_us": [timestamp_us],
        "bid_price": [0.44],
        "ask_price": [0.45],
        "bid_size": [10.0],
        "ask_size": [12.0],
    }
    if extra_columns:
        for key, value in extra_columns.items():
            data[key] = [value]
    frame = pd.DataFrame(data)
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _parquet_payload_with_local_timestamp(value: int | float) -> bytes:
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "local_timestamp_us": [value],
            "bid_price": [0.44],
            "ask_price": [0.45],
        }
    )
    buffer = BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _book_snapshot_payload(
    timestamp_us: int,
    *,
    bids_type: str = "levels",
    asks_type: str = "levels",
) -> bytes:
    levels_type = telonex_download.pa.list_(
        telonex_download.pa.field(
            "element",
            telonex_download.pa.struct(
                [
                    telonex_download.pa.field("price", telonex_download.pa.string()),
                    telonex_download.pa.field("size", telonex_download.pa.string()),
                ]
            ),
        )
    )

    def side(kind: str):
        if kind == "null":
            return telonex_download.pa.array(
                [[]], type=telonex_download.pa.list_(telonex_download.pa.null())
            )
        return telonex_download.pa.array(
            [[{"price": "0.44", "size": "10"}]],
            type=levels_type,
        )

    table = telonex_download.pa.table(
        {
            "timestamp_us": telonex_download.pa.array(
                [timestamp_us], type=telonex_download.pa.int64()
            ),
            "local_timestamp_us": telonex_download.pa.array(
                [float(timestamp_us)], type=telonex_download.pa.float64()
            ),
            "exchange": ["polymarket"],
            "market_id": ["m1"],
            "slug": ["book-market"],
            "asset_id": ["asset-0"],
            "outcome": ["Yes"],
            "bids": side(bids_type),
            "asks": side(asks_type),
        }
    )
    buffer = BytesIO()
    telonex_download.pq.write_table(table, buffer)
    return buffer.getvalue()


def _install_payload_stub(
    monkeypatch: pytest.MonkeyPatch,
    payloads_by_day: dict[str, bytes],
    *,
    seen_urls: list[str] | None = None,
    seen_auth: list[str] | None = None,
    fail_first_n: dict[str, int] | None = None,
    raise_for_day: dict[str, Exception] | None = None,
) -> None:
    """Intercept the one network hop in the pipeline — `_download_day_bytes` —
    and serve fixtures by day. Everything above this layer (jobs, manifest,
    parquet writer) still exercises real code."""
    fail_first_n = fail_first_n or {}
    raise_for_day = raise_for_day or {}
    call_counts: dict[str, int] = {}

    def fake_download_day_bytes(*, timeout_secs, url, api_key, stop_event, progress_cb):
        del timeout_secs, stop_event
        if seen_urls is not None:
            seen_urls.append(url)
        if seen_auth is not None:
            seen_auth.append(api_key)
        # URL path looks like .../quotes/2026-01-19?slug=...
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        call_counts[day] = call_counts.get(day, 0) + 1

        if day in raise_for_day and call_counts[day] == 1:
            raise raise_for_day[day]

        if fail_first_n.get(day, 0) >= call_counts[day]:
            raise telonex_download._FakeHTTPError(503, "Service Unavailable")

        if day not in payloads_by_day:
            raise telonex_download._FakeHTTPError(404, "not found")

        payload = payloads_by_day[day]
        progress_cb(len(payload), len(payload), True)
        return payload

    # `_download_day_bytes` is not a real module attr (the network path is
    # async internally); pass raising=False to install it as a test hook.
    # `_run_jobs` looks it up via `globals().get(...)` at call time.
    monkeypatch.setattr(
        telonex_download, "_download_day_bytes", fake_download_day_bytes, raising=False
    )


def test_download_telonex_days_writes_duckdb_blob(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen_urls: list[str] = []
    seen_auth: list[str] = []
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads, seen_urls=seen_urls, seen_auth=seen_auth)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )

    assert summary.requested_days == 2
    assert summary.downloaded_days == 2
    assert summary.skipped_existing_days == 0
    assert summary.failed_days == 0
    assert summary.missing_days == 0
    assert sorted(seen_urls) == [
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0",
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-20?slug=us-recession-by-end-of-2026&outcome_id=0",
    ]
    assert sorted(seen_auth) == ["test-key", "test-key"]

    manifest_path = tmp_path / "telonex.duckdb"
    assert manifest_path.exists()
    assert summary.db_path == str(manifest_path)
    assert summary.db_size_bytes > 0

    data_root = tmp_path / "data"
    assert data_root.exists()
    parquet_files = sorted(data_root.rglob("*.parquet"))
    assert len(parquet_files) >= 1
    for path in parquet_files:
        rel = path.relative_to(data_root).parts
        assert rel[0].startswith("channel=")
        assert rel[1].startswith("year=")
        assert rel[2].startswith("month=")
        assert path.name.startswith("part-")

    con = duckdb.connect(str(manifest_path), read_only=True)
    try:
        manifest = con.execute(
            "SELECT channel, market_slug, outcome_segment, day, rows, parquet_part "
            "FROM completed_days ORDER BY day"
        ).fetchall()
    finally:
        con.close()

    assert len(manifest) == 2
    assert {row[0] for row in manifest} == {"quotes"}
    assert {row[1] for row in manifest} == {"us-recession-by-end-of-2026"}
    assert {row[2] for row in manifest} == {"0"}
    assert all(row[5] is not None for row in manifest)

    glob = str(data_root / "channel=quotes" / "**" / "*.parquet")
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            "SELECT market_slug, outcome_segment, timestamp_us FROM "
            "read_parquet(?, hive_partitioning=1, union_by_name=True) "
            "ORDER BY timestamp_us",
            [glob],
        ).fetchall()
    finally:
        con.close()
    assert [row[2] for row in rows] == [1_768_780_800_000_000, 1_768_867_200_000_000]


def test_download_telonex_days_requires_key_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("TELONEX_API_KEY", raising=False)

    with pytest.raises(ValueError, match="TELONEX_API_KEY"):
        telonex_download.download_telonex_days(
            destination=tmp_path,
            market_slugs=["us-recession-by-end-of-2026"],
            outcome_id=0,
            start_date="2026-01-19",
            end_date="2026-01-19",
            show_progress=False,
        )


def test_download_telonex_days_resumes_from_manifest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)

    first = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert first.downloaded_days == 1

    def boom(*_args, **_kwargs):
        raise AssertionError("should not retry a skipped day")

    monkeypatch.setattr(telonex_download, "_download_day_bytes", boom)

    second = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert second.downloaded_days == 0
    assert second.skipped_existing_days == 1


def test_download_telonex_days_records_404_so_reruns_skip_empty_days(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, {})  # every day 404s

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["no-such-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert summary.missing_days == 1
    assert summary.downloaded_days == 0

    def boom(*_args, **_kwargs):
        raise AssertionError("should not retry a known-empty day")

    monkeypatch.setattr(telonex_download, "_download_day_bytes", boom)

    rerun = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["no-such-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert rerun.skipped_existing_days == 1


def test_download_telonex_days_rechecks_stale_404_and_clears_empty_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, {})  # first run records a 404

    first = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["late-arriving-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert first.missing_days == 1

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"))
    try:
        con.execute("UPDATE empty_days SET checked_at = TIMESTAMP '2026-01-01 00:00:00'")
    finally:
        con.close()

    _install_payload_stub(
        monkeypatch,
        {"2026-01-19": _parquet_payload(1_768_780_800_000_000)},
    )

    second = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["late-arriving-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
        recheck_empty_after_days=7,
    )

    assert second.skipped_existing_days == 0
    assert second.downloaded_days == 1
    assert second.missing_days == 0

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        completed = con.execute("SELECT COUNT(*) FROM completed_days").fetchone()[0]
        empty = con.execute("SELECT COUNT(*) FROM empty_days").fetchone()[0]
    finally:
        con.close()

    assert completed == 1
    assert empty == 0


def test_download_telonex_days_all_markets_expands_every_channel(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured_jobs = []

    markets = pd.DataFrame(
        {
            "slug": ["market-one"],
            "status": ["resolved"],
            "quotes_from": ["2026-01-19"],
            "quotes_to": ["2026-01-19"],
            "trades_from": ["2026-01-19"],
            "trades_to": ["2026-01-19"],
            "book_snapshot_5_from": ["2026-01-19"],
            "book_snapshot_5_to": ["2026-01-19"],
            "book_snapshot_25_from": ["2026-01-19"],
            "book_snapshot_25_to": ["2026-01-19"],
            "book_snapshot_full_from": ["2026-01-19"],
            "book_snapshot_full_to": ["2026-01-19"],
            "onchain_fills_from": ["2026-01-19"],
            "onchain_fills_to": ["2026-01-19"],
        }
    )

    def fake_fetch_markets_dataset(
        base_url: str, timeout_secs: int, *, show_progress: bool = False
    ) -> pd.DataFrame:
        del base_url, timeout_secs, show_progress
        return markets

    def fake_run_jobs(jobs, **kwargs):
        del kwargs
        job_list = list(jobs)
        captured_jobs.extend(job_list)
        return (len(job_list), 0, 0, 0, 123, False, [])

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_fetch_markets_dataset", fake_fetch_markets_dataset)
    monkeypatch.setattr(telonex_download, "_run_jobs", fake_run_jobs)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        all_markets=True,
        channels=list(telonex_download.VALID_CHANNELS),
        show_progress=False,
    )

    assert summary.markets_considered == 1
    assert summary.requested_days == 12
    assert summary.downloaded_days == 12
    assert {job.channel for job in captured_jobs} == set(telonex_download.VALID_CHANNELS)
    assert {job.outcome_segment for job in captured_jobs} == {"0", "1"}


def test_all_markets_catalog_date_parsing_handles_mixed_valid_formats() -> None:
    markets = pd.DataFrame(
        {
            "slug": ["date-only", "iso-timestamp", "missing-bounds"],
            "quotes_from": ["2026-01-19", "2026-01-20T05:00:00Z", None],
            "quotes_to": ["2026-01-19", "2026-01-20T23:59:59Z", None],
        }
    )

    jobs_iter = telonex_download._iter_jobs_from_catalog(
        markets=markets,
        channels=["quotes"],
        outcomes=[0],
        window_start=None,
        window_end=None,
        status_filter=None,
        slug_filter=None,
        show_progress=False,
    )

    jobs = list(jobs_iter)

    assert jobs_iter.markets_considered == 3
    assert jobs_iter.total_jobs == 2
    assert [(job.market_slug, job.day.isoformat()) for job in jobs] == [
        ("date-only", "2026-01-19"),
        ("iso-timestamp", "2026-01-20"),
    ]


def test_all_markets_catalog_window_clipping_preserves_missing_bounds() -> None:
    markets = pd.DataFrame(
        {
            "slug": ["clipped", "missing-from", "missing-to"],
            "quotes_from": ["2026-01-18", None, "2026-01-18"],
            "quotes_to": ["2026-01-22", "2026-01-22", None],
        }
    )

    jobs_iter = telonex_download._iter_jobs_from_catalog(
        markets=markets,
        channels=["quotes"],
        outcomes=[0],
        window_start=telonex_download._parse_date_bound("2026-01-20"),
        window_end=telonex_download._parse_date_bound("2026-01-21"),
        status_filter=None,
        slug_filter=None,
        show_progress=False,
    )

    jobs = list(jobs_iter)

    assert jobs_iter.markets_considered == 3
    assert jobs_iter.total_jobs == 2
    assert [(job.market_slug, job.day.isoformat()) for job in jobs] == [
        ("clipped", "2026-01-20"),
        ("clipped", "2026-01-21"),
    ]


def test_download_telonex_days_schema_evolves_when_later_day_has_new_column(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Day 2's parquet has an `origin_asset_id` column that day 1's didn't —
    the writer must roll a new part rather than crashing."""
    payloads = {
        "2026-01-19": _parquet_payload_with_extra(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload_with_extra(
            1_768_867_200_000_000, extra_columns={"origin_asset_id": "abc123"}
        ),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["evolving-schema-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )
    assert summary.downloaded_days == 2
    assert summary.failed_days == 0

    glob = str(tmp_path / "data" / "channel=quotes" / "**" / "*.parquet")
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            "SELECT origin_asset_id FROM "
            "read_parquet(?, hive_partitioning=1, union_by_name=True) "
            "ORDER BY timestamp_us",
            [glob],
        ).fetchall()
    finally:
        con.close()
    assert rows == [(None,), ("abc123",)]


def test_download_telonex_days_normalizes_local_timestamps_to_float(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _parquet_payload_with_local_timestamp(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload_with_local_timestamp(1_768_867_200_000_000.75),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 10_000_000)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 999.0)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["mixed-arrow-type-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )

    assert summary.downloaded_days == 2
    assert summary.failed_days == 0

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        manifest = con.execute(
            "SELECT day, parquet_part FROM completed_days ORDER BY day"
        ).fetchall()
    finally:
        con.close()

    assert len(manifest) == 2
    assert manifest[0][1] == manifest[1][1]

    part = tmp_path / manifest[0][1]
    schema = telonex_download.pq.ParquetFile(part).schema_arrow
    assert schema.field("local_timestamp_us").type == telonex_download.pa.float64()


def test_download_telonex_days_keeps_empty_book_sides_in_one_part(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _book_snapshot_payload(1_768_780_800_000_000),
        "2026-01-20": _book_snapshot_payload(1_768_867_200_000_000, bids_type="null"),
        "2026-01-21": _book_snapshot_payload(1_768_953_600_000_000, asks_type="null"),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["book-market"],
        outcome_id=0,
        channel="book_snapshot_full",
        start_date="2026-01-19",
        end_date="2026-01-21",
        show_progress=False,
        workers=1,
    )

    assert summary.downloaded_days == 3
    assert summary.failed_days == 0
    parts = sorted((tmp_path / "data").rglob("*.parquet"))
    assert len(parts) == 1
    schema = telonex_download.pq.ParquetFile(parts[0]).schema_arrow
    assert schema.field("bids").type.equals(telonex_download._ORDER_BOOK_LEVELS_TYPE)
    assert schema.field("asks").type.equals(telonex_download._ORDER_BOOK_LEVELS_TYPE)


def test_download_telonex_days_reuses_promoted_optional_column_schema(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _parquet_payload_with_extra(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload_with_extra(
            1_768_867_200_000_000, extra_columns={"origin_asset_id": "abc123"}
        ),
        "2026-01-21": _parquet_payload_with_extra(1_768_953_600_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["optional-column-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-21",
        show_progress=False,
        workers=1,
    )

    assert summary.downloaded_days == 3
    assert summary.failed_days == 0

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        rows = con.execute("SELECT day, parquet_part FROM completed_days ORDER BY day").fetchall()
    finally:
        con.close()

    assert len({row[1] for row in rows}) == 2
    assert rows[1][1] == rows[2][1]


def test_download_telonex_days_retries_transient_5xx_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads, fail_first_n={"2026-01-19": 2})
    monkeypatch.setattr(telonex_download, "_RETRY_BACKOFF_BASE_SECS", 0.0)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["flaky-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert summary.downloaded_days == 1
    assert summary.failed_days == 0


def test_resolve_parse_worker_count_uses_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELONEX_PARSE_WORKERS", "12")
    assert telonex_download._resolve_parse_worker_count(None) == 12
    assert telonex_download._resolve_parse_worker_count(3) == 3


def test_run_jobs_reuses_one_async_http_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = _parquet_payload(1_768_780_800_000_000)
    clients = []
    requested_urls: list[str] = []

    class FakeResponse:
        status_code = 200
        headers = {"Content-Length": str(len(payload))}

        async def aread(self) -> bytes:
            return payload

        async def aiter_bytes(self, *, chunk_size: int):
            del chunk_size
            yield payload

    class FakeStream:
        async def __aenter__(self) -> FakeResponse:
            return FakeResponse()

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            del exc_type, exc, tb
            return False

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            del args, kwargs
            clients.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
            del exc_type, exc, tb
            return False

        def stream(self, method: str, url: str, *, headers: dict[str, str]) -> FakeStream:
            assert method == "GET"
            assert headers["Authorization"] == "Bearer test-key"
            requested_urls.append(url)
            return FakeStream()

    monkeypatch.setattr(telonex_download.httpx, "AsyncClient", FakeAsyncClient)
    store = telonex_download._TelonexParquetStore(tmp_path)
    try:
        jobs = [
            telonex_download._Job(
                market_slug="pooled-market",
                outcome_segment="0",
                outcome_id=0,
                outcome=None,
                channel="quotes",
                day=date(2026, 1, 19),
            ),
            telonex_download._Job(
                market_slug="pooled-market",
                outcome_segment="0",
                outcome_id=0,
                outcome=None,
                channel="quotes",
                day=date(2026, 1, 20),
            ),
        ]

        downloaded, missing, failed, cancelled, _bytes, interrupted, samples = (
            telonex_download._run_jobs(
                jobs,
                store=store,
                api_key="test-key",
                base_url="https://api.telonex.io",
                timeout_secs=60,
                workers=2,
                show_progress=False,
                total_jobs=len(jobs),
            )
        )
    finally:
        store.close()

    assert downloaded == 2
    assert missing == 0
    assert failed == 0
    assert cancelled == 0
    assert interrupted is False
    assert samples == []
    assert len(clients) == 1
    assert len(requested_urls) == 2


def test_download_telonex_days_reports_writer_commit_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    def fail_ingest(self, results):  # type: ignore[no-untyped-def]
        del self, results
        raise RuntimeError("simulated writer failure")

    monkeypatch.setattr(telonex_download._TelonexParquetStore, "ingest_batch", fail_ingest)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["writer-failure-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )

    assert summary.downloaded_days == 0
    assert summary.failed_days == 1
    assert summary.bytes_downloaded == 0
    assert "writer commit failed" in summary.failed_samples[0]

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        completed = con.execute("SELECT count(*) FROM completed_days").fetchone()[0]
    finally:
        con.close()
    assert completed == 0


def test_download_telonex_progress_format_includes_bar_percent_and_eta(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}
    progress_kwargs: list[dict[str, object]] = []

    class FakeTqdm:
        def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            del args
            progress_kwargs.append(kwargs)
            self.n = 0

        def update(self, value: int) -> None:
            self.n += value

        def set_postfix_str(self, value: str, refresh: bool = False) -> None:
            del value, refresh

        def refresh(self) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "tqdm", FakeTqdm)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["progress-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=True,
        workers=1,
    )

    assert summary.downloaded_days == 1
    assert progress_kwargs
    download_bar_formats = [
        str(kwargs.get("bar_format", ""))
        for kwargs in progress_kwargs
        if kwargs.get("desc") == "Downloading Telonex days"
    ]
    assert download_bar_formats
    assert all(fmt.startswith("{desc}: |") for fmt in download_bar_formats)
    assert all("{bar}" in fmt for fmt in download_bar_formats)
    assert all("{percentage:.4f}%" in fmt for fmt in download_bar_formats)
    assert all("{remaining}" in fmt for fmt in download_bar_formats)


def test_all_markets_progress_only_uses_fetch_and_download_bars(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    markets = pd.DataFrame(
        {
            "slug": ["market-one"],
            "quotes_from": ["2026-01-19"],
            "quotes_to": ["2026-01-19"],
        }
    )
    progress_descs: list[str] = []

    class FakeTqdm:
        def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            del args
            desc = kwargs.get("desc")
            if desc is not None:
                progress_descs.append(str(desc))
            self.n = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            del exc_type, exc, tb
            return False

        def update(self, value: int) -> None:
            self.n += value

        def set_postfix_str(self, value: str, refresh: bool = False) -> None:
            del value, refresh

        def refresh(self) -> None:
            pass

        def close(self) -> None:
            pass

    def fake_fetch_markets_dataset(
        base_url: str, timeout_secs: int, *, show_progress: bool = False
    ) -> pd.DataFrame:
        del base_url, timeout_secs
        if show_progress:
            telonex_download.tqdm(total=10, desc="Fetching Telonex markets").close()
        return markets

    def fake_run_jobs(jobs, **kwargs):
        assert kwargs["show_progress"] is True
        list(jobs)
        telonex_download.tqdm(total=1, desc="Downloading Telonex days").close()
        return (1, 0, 0, 0, 100, False, [])

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "tqdm", FakeTqdm)
    monkeypatch.setattr(telonex_download, "_fetch_markets_dataset", fake_fetch_markets_dataset)
    monkeypatch.setattr(telonex_download, "_run_jobs", fake_run_jobs)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        all_markets=True,
        channels=["quotes"],
        show_progress=True,
    )

    assert summary.downloaded_days == 1
    assert progress_descs == ["Fetching Telonex markets", "Downloading Telonex days"]


def test_all_markets_resume_progress_total_excludes_manifest_hits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}
    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)

    first = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["already-downloaded"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )
    assert first.downloaded_days == 1

    markets = pd.DataFrame(
        {
            "slug": ["already-downloaded"],
            "quotes_from": ["2026-01-19"],
            "quotes_to": ["2026-01-19"],
        }
    )
    captured_total_jobs: list[int | None] = []

    def fake_fetch_markets_dataset(
        base_url: str, timeout_secs: int, *, show_progress: bool = False
    ) -> pd.DataFrame:
        del base_url, timeout_secs, show_progress
        return markets

    def fake_run_jobs(jobs, **kwargs):
        captured_total_jobs.append(kwargs["total_jobs"])
        kept_jobs = list(jobs)
        assert len(kept_jobs) == 1
        assert kept_jobs[0].outcome_id == 1
        return (0, 0, 0, 0, 0, False, [])

    monkeypatch.setattr(telonex_download, "_fetch_markets_dataset", fake_fetch_markets_dataset)
    monkeypatch.setattr(telonex_download, "_run_jobs", fake_run_jobs)

    second = telonex_download.download_telonex_days(
        destination=tmp_path,
        all_markets=True,
        channels=["quotes"],
        show_progress=True,
        workers=1,
    )

    assert second.skipped_existing_days == 1
    assert captured_total_jobs == [1]


def test_downloaded_parquet_is_readable_by_telonex_loader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end: downloader's Parquet layout must match what the reader expects."""
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)

    telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )

    from prediction_market_extensions.backtesting.data_sources.telonex import (
        RunnerPolymarketTelonexBookDataLoader,
    )

    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    blob_root = loader._local_blob_root(tmp_path)
    assert blob_root is not None

    frame = loader._load_blob_range(
        store_root=blob_root,
        channel="quotes",
        market_slug="us-recession-by-end-of-2026",
        token_index=0,
        outcome=None,
        start=pd.Timestamp("2026-01-19", tz="UTC"),
        end=pd.Timestamp("2026-01-20 23:59:59", tz="UTC"),
    )
    assert frame is not None and len(frame) == 2
    assert "market_slug" not in frame.columns
    assert "outcome_segment" not in frame.columns
    assert "year" not in frame.columns
    assert "month" not in frame.columns
    assert set(frame["timestamp_us"]) == {1_768_780_800_000_000, 1_768_867_200_000_000}

    frame_dec = loader._load_blob_range(
        store_root=blob_root,
        channel="quotes",
        market_slug="us-recession-by-end-of-2026",
        token_index=0,
        outcome=None,
        start=pd.Timestamp("2025-12-01", tz="UTC"),
        end=pd.Timestamp("2025-12-31", tz="UTC"),
    )
    assert frame_dec is None


def test_telonex_blob_reader_uses_requested_channel_only(tmp_path: Path) -> None:
    store = telonex_download._TelonexParquetStore(tmp_path)
    try:
        store.ingest_batch(
            [
                telonex_download._DownloadResult(
                    job=telonex_download._Job(
                        market_slug="channel-test",
                        outcome_segment="0",
                        outcome_id=0,
                        outcome=None,
                        channel="quotes",
                        day=date(2026, 1, 19),
                    ),
                    status="ok",
                    table=telonex_download.pa.Table.from_pandas(
                        pd.DataFrame(
                            {
                                "timestamp_us": [1_768_780_800_000_000],
                                "bid_price": [0.44],
                                "ask_price": [0.45],
                                "bid_size": [10.0],
                                "ask_size": [12.0],
                            }
                        ),
                        preserve_index=False,
                    ),
                    payload=None,
                    bytes_downloaded=100,
                    error=None,
                ),
                telonex_download._DownloadResult(
                    job=telonex_download._Job(
                        market_slug="channel-test",
                        outcome_segment="0",
                        outcome_id=0,
                        outcome=None,
                        channel="trades",
                        day=date(2026, 1, 19),
                    ),
                    status="ok",
                    table=telonex_download.pa.Table.from_pandas(
                        pd.DataFrame(
                            {
                                "timestamp_us": [1_768_780_900_000_000],
                                "bid_price": [0.10],
                                "ask_price": [0.90],
                                "bid_size": [1.0],
                                "ask_size": [1.0],
                            }
                        ),
                        preserve_index=False,
                    ),
                    payload=None,
                    bytes_downloaded=100,
                    error=None,
                ),
            ]
        )
    finally:
        store.close()

    from prediction_market_extensions.backtesting.data_sources.telonex import (
        RunnerPolymarketTelonexBookDataLoader,
    )

    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    frame = loader._load_blob_range(
        store_root=tmp_path,
        channel="quotes",
        market_slug="channel-test",
        token_index=0,
        outcome=None,
        start=pd.Timestamp("2026-01-19", tz="UTC"),
        end=pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
    )

    assert frame is not None
    assert list(frame["timestamp_us"]) == [1_768_780_800_000_000]


def test_download_telonex_days_rolls_part_files_when_threshold_exceeded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_BYTES", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["roll-test"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )

    parts = sorted((tmp_path / "data").rglob("*.parquet"))
    assert len(parts) == 2

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        rows = con.execute("SELECT day, parquet_part FROM completed_days ORDER BY day").fetchall()
    finally:
        con.close()
    assert len({row[1] for row in rows}) == 2


def test_download_telonex_days_rolls_part_files_when_disk_threshold_exceeded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_BYTES", 1 << 40)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_DISK_BYTES", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["disk-roll-test"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )

    parts = sorted((tmp_path / "data").rglob("*.parquet"))
    assert len(parts) == 2


def test_download_telonex_days_rolls_part_files_when_pending_manifest_rows_exceeded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
        "2026-01-21": _parquet_payload(1_768_953_600_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_BYTES", 1 << 40)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_DISK_BYTES", 1 << 40)
    monkeypatch.setattr(telonex_download, "_TARGET_PART_PENDING_DAYS", 2)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_ROWS", 1)
    monkeypatch.setattr(telonex_download, "_DEFAULT_COMMIT_BATCH_SECS", 0.0)

    telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["pending-roll-test"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-21",
        show_progress=False,
        workers=1,
    )

    parts = sorted((tmp_path / "data").rglob("*.parquet"))
    assert len(parts) == 2

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        rows = con.execute("SELECT day, parquet_part FROM completed_days ORDER BY day").fetchall()
    finally:
        con.close()
    assert len(rows) == 3
    assert len({row[1] for row in rows}) == 2


def test_store_sweeps_orphan_parquet_on_startup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payloads = {"2026-01-19": _parquet_payload(1_768_780_800_000_000)}

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(monkeypatch, payloads)

    telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["orphan-test"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
        workers=1,
    )

    parts_after_first = sorted((tmp_path / "data").rglob("*.parquet"))
    assert len(parts_after_first) == 1
    real_part = parts_after_first[0]

    orphan = real_part.parent / "part-999999.parquet"
    orphan.write_bytes(b"not a valid parquet footer")
    assert orphan.exists()

    store = telonex_download._TelonexParquetStore(tmp_path)
    try:
        assert not orphan.exists()
        assert real_part.exists()
    finally:
        store.close()


def test_store_sweeps_orphan_parquet_on_close(tmp_path: Path) -> None:
    store = telonex_download._TelonexParquetStore(tmp_path)
    orphan_dir = tmp_path / "data" / "channel=book_snapshot_full" / "year=2026" / "month=01"
    orphan_dir.mkdir(parents=True, exist_ok=True)
    orphan = orphan_dir / "part-999999.parquet"
    telonex_download.pq.write_table(
        telonex_download.pa.table({"timestamp_us": [1_768_780_800_000_000]}),
        orphan,
    )
    assert orphan.exists()

    store.close()

    assert not orphan.exists()


def test_download_telonex_days_resumes_midrun_interruption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Day 1 commits, day 2 raises before commit. On rerun, day 1 skips and
    day 2 re-fetches successfully."""
    payloads = {
        "2026-01-19": _parquet_payload(1_768_780_800_000_000),
        "2026-01-20": _parquet_payload(1_768_867_200_000_000),
    }

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    _install_payload_stub(
        monkeypatch,
        payloads,
        raise_for_day={"2026-01-20": RuntimeError("simulated mid-run crash")},
    )
    monkeypatch.setattr(telonex_download, "_RETRY_BACKOFF_BASE_SECS", 0.0)

    summary_a = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["crash-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )
    assert summary_a.downloaded_days == 1
    assert summary_a.failed_days == 1

    # Crash resolved on rerun — reinstall a clean stub.
    seen_urls: list[str] = []
    _install_payload_stub(monkeypatch, payloads, seen_urls=seen_urls)

    summary_b = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["crash-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )
    assert summary_b.downloaded_days == 1
    assert summary_b.skipped_existing_days == 1
    assert summary_b.failed_days == 0
    # Only day 2 should have been refetched.
    assert all("2026-01-20" in url for url in seen_urls)

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        days = sorted(
            row[0] for row in con.execute("SELECT day FROM completed_days ORDER BY day").fetchall()
        )
    finally:
        con.close()
    assert [d.isoformat() for d in days] == ["2026-01-19", "2026-01-20"]
