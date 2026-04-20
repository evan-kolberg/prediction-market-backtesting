from __future__ import annotations

from io import BytesIO
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from scripts import _telonex_data_download as telonex_download


class _Response:
    def __init__(self, payload: bytes, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self._offset = 0
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
        return False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


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


def test_download_telonex_days_writes_duckdb_blob(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    requested_urls: list[str] = []
    resolved_urls: list[str] = []
    auth_keys: list[str] = []
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
        "https://download.example/2026-01-20.parquet": _parquet_payload(1_768_867_200_000_000),
    }

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del timeout_secs
        requested_urls.append(url)
        auth_keys.append(api_key)
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        resolved = f"https://download.example/{day}.parquet"
        resolved_urls.append(resolved)
        return resolved

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)

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
    assert sorted(requested_urls) == [
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0",
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-20?slug=us-recession-by-end-of-2026&outcome_id=0",
    ]
    assert sorted(resolved_urls) == [
        "https://download.example/2026-01-19.parquet",
        "https://download.example/2026-01-20.parquet",
    ]
    assert sorted(auth_keys) == ["test-key", "test-key"]

    manifest_path = tmp_path / "telonex.duckdb"
    assert manifest_path.exists()
    assert summary.db_path == str(manifest_path)
    assert summary.db_size_bytes > 0

    # The store is Hive-partitioned Parquet: data/channel=X/year=.../month=.../part-*.parquet
    data_root = tmp_path / "data"
    assert data_root.exists()
    parquet_files = sorted(data_root.rglob("*.parquet"))
    assert len(parquet_files) >= 1, "expected at least one Parquet part file"
    # Every part file sits under the expected Hive path.
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

    # Resolve part paths via the manifest, then read the rows back via DuckDB's
    # hive-partitioned read_parquet API — the same code path readers take.
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
    assert {row[0] for row in rows} == {"us-recession-by-end-of-2026"}
    assert {row[1] for row in rows} == {"0"}


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
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
    }

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del url, api_key, timeout_secs
        return "https://download.example/2026-01-19.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)

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

    def unexpected_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        raise AssertionError(f"unexpected Telonex request for {request.full_url}")

    monkeypatch.setattr(telonex_download, "urlopen", unexpected_urlopen)

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
    from urllib.error import HTTPError

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        raise HTTPError(url, 404, "Not Found", {}, None)  # type: ignore[arg-type]

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)

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

    def boom(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("should not retry a known-empty day")

    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", boom)

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

    def fake_run_jobs(jobs, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        captured_jobs.extend(jobs)
        return (len(jobs), 0, 0, 0, 123, False, [])

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


def _parquet_payload_with_extra(
    timestamp_us: int, *, extra_columns: dict[str, object] | None = None
) -> bytes:
    data = {
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


def test_download_telonex_days_schema_evolves_when_later_day_has_new_column(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Day 2's parquet has an `origin_asset_id` column that day 1's didn't —
    the writer must ALTER TABLE rather than crashing with BinderException."""
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload_with_extra(
            1_768_780_800_000_000
        ),
        "https://download.example/2026-01-20.parquet": _parquet_payload_with_extra(
            1_768_867_200_000_000,
            extra_columns={"origin_asset_id": "abc123"},
        ),
    }

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        return f"https://download.example/{day}.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)
    # Force each day into its own commit batch so day 2 triggers the
    # schema-evolution path against an already-created table.
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

    # Two commit batches with different schemas must both land on disk.
    # `union_by_name=True` on read reconciles the per-file schemas, so the
    # reader sees `origin_asset_id` as NULL for day 1 and populated for day 2.
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
    from urllib.error import HTTPError

    call_count = {"n": 0}
    payload = _parquet_payload(1_768_780_800_000_000)

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise HTTPError(url, 503, "Service Unavailable", {}, None)  # type: ignore[arg-type]
        return "https://download.example/day.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)
    # Zero out the retry backoff so the test finishes quickly.
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
    # 2 transient failures + 1 success
    assert call_count["n"] == 3


def test_downloaded_parquet_is_readable_by_telonex_loader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """End-to-end: the downloader's Parquet layout must be what the reader
    expects. Without this, the downloader can silently produce files no loader
    can consume."""
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
        "https://download.example/2026-01-20.parquet": _parquet_payload(1_768_867_200_000_000),
    }

    def fake_resolve(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        return f"https://download.example/{day}.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)

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
    assert blob_root is not None, "downloader output should be detected as a blob store"

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
    # Bookkeeping columns stripped.
    assert "market_slug" not in frame.columns
    assert "outcome_segment" not in frame.columns
    assert "year" not in frame.columns
    assert "month" not in frame.columns
    assert set(frame["timestamp_us"]) == {1_768_780_800_000_000, 1_768_867_200_000_000}

    # Month-range pruning works: January-only query drops all rows.
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
    """With a tiny part-roll threshold, each batch closes one file and opens
    the next. The manifest must reference the correct file for each day."""
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
        "https://download.example/2026-01-20.parquet": _parquet_payload(1_768_867_200_000_000),
    }

    def fake_resolve(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        return f"https://download.example/{day}.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)
    # Force a part roll after each day by setting the threshold below one row's size.
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
    # Both days fell in the same (channel=quotes, year=2026, month=01) partition.
    # With threshold=1, each day triggered a roll ⇒ two distinct part files.
    assert len(parts) == 2
    assert all("part-" in p.name for p in parts)

    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        rows = con.execute("SELECT day, parquet_part FROM completed_days ORDER BY day").fetchall()
    finally:
        con.close()
    assert len({row[1] for row in rows}) == 2  # different parts for different days


def test_store_sweeps_orphan_parquet_on_startup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A SIGKILL between writer.close() and manifest update leaves a Parquet
    file on disk that isn't referenced. On the next startup, the store must
    delete the orphan so read_parquet globs stay clean."""
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
    }

    def fake_resolve(*, url: str, api_key: str, timeout_secs: int) -> str:
        del url, api_key, timeout_secs
        return "https://download.example/2026-01-19.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)

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

    # Plant a decoy orphan next to the real file — simulates a half-written
    # part left behind by SIGKILL.
    orphan = real_part.parent / "part-999999.parquet"
    orphan.write_bytes(b"not a valid parquet footer")
    assert orphan.exists()

    # Re-open the store — no download, just init — and the orphan should go.
    store = telonex_download._TelonexParquetStore(tmp_path)
    try:
        assert not orphan.exists(), "orphan should be swept on startup"
        assert real_part.exists(), "manifest-referenced file must survive"
    finally:
        store.close()


def test_download_telonex_days_resumes_midrun_interruption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Simulate the realistic crash case: day 1 commits to the blob, day 2
    raises before it can commit. On the next run, day 1 must skip and day 2
    must re-fetch and succeed."""
    payloads = {
        "https://download.example/2026-01-19.parquet": _parquet_payload(1_768_780_800_000_000),
        "https://download.example/2026-01-20.parquet": _parquet_payload(1_768_867_200_000_000),
    }
    seen_days: list[str] = []

    def fake_resolve_presigned_url(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        seen_days.append(day)
        if day == "2026-01-20":
            raise RuntimeError("simulated mid-run crash before day 2 commits")
        return f"https://download.example/{day}.parquet"

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        payload = payloads[request.full_url]
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", fake_resolve_presigned_url)
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)
    # Zero backoff so retries are fast.
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

    seen_days.clear()

    # Now the crash is fixed — day 2 resolves normally.
    def resolve_ok(*, url: str, api_key: str, timeout_secs: int) -> str:
        del api_key, timeout_secs
        day = url.rsplit("/", 1)[1].split("?", 1)[0]
        seen_days.append(day)
        return f"https://download.example/{day}.parquet"

    monkeypatch.setattr(telonex_download, "_resolve_presigned_url", resolve_ok)

    summary_b = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["crash-market"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
        workers=1,
    )
    # Day 1 already committed → skip. Day 2 → fresh fetch.
    assert summary_b.downloaded_days == 1
    assert summary_b.skipped_existing_days == 1
    assert summary_b.failed_days == 0
    assert seen_days == ["2026-01-20"]

    # Final blob has both days.
    con = duckdb.connect(str(tmp_path / "telonex.duckdb"), read_only=True)
    try:
        days = sorted(
            row[0] for row in con.execute("SELECT day FROM completed_days ORDER BY day").fetchall()
        )
    finally:
        con.close()
    assert [d.isoformat() for d in days] == ["2026-01-19", "2026-01-20"]
