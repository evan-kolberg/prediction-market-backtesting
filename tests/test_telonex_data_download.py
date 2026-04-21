from __future__ import annotations

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

    def fake_fetch_markets_dataset(base_url: str, timeout_secs: int) -> pd.DataFrame:
        del base_url, timeout_secs
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
            "slug": ["date-only", "iso-timestamp"],
            "quotes_from": ["2026-01-19", "2026-01-20T05:00:00Z"],
            "quotes_to": ["2026-01-19", "2026-01-20T23:59:59Z"],
        }
    )

    jobs_iter, considered = telonex_download._iter_jobs_from_catalog(
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

    assert considered[0] == 2
    assert [(job.market_slug, job.day.isoformat()) for job in jobs] == [
        ("date-only", "2026-01-19"),
        ("iso-timestamp", "2026-01-20"),
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
        RunnerPolymarketTelonexQuoteDataLoader,
    )

    loader = RunnerPolymarketTelonexQuoteDataLoader.__new__(RunnerPolymarketTelonexQuoteDataLoader)
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
