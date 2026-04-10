from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import pmxt_relay.raw_mirror_verifier as verifier
from pmxt_relay.storage import raw_relative_path


def test_verify_local_raw_mirror_reports_missing_and_corrupt_files(tmp_path: Path, monkeypatch):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()

    good_name = "polymarket_orderbook_2026-03-21T12.parquet"
    corrupt_name = "polymarket_orderbook_2026-03-21T13.parquet"
    missing_name = "polymarket_orderbook_2026-03-21T14.parquet"
    filenames = [good_name, corrupt_name, missing_name]

    monkeypatch.setattr(verifier, "_discover_archive_filenames", lambda **_: filenames)

    good_path = raw_root / raw_relative_path(good_name)
    good_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"market_id": ["0xabc"], "update_type": ["book_snapshot"], "data": ['{"token_id":"1"}']}), good_path
    )

    corrupt_path = raw_root / raw_relative_path(corrupt_name)
    corrupt_path.parent.mkdir(parents=True, exist_ok=True)
    corrupt_path.write_text("not parquet")

    summary = verifier.verify_local_raw_mirror(
        vendor="pmxt",
        raw_root=raw_root,
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        raw_base_url="https://r2.pmxt.dev",
        check_upstream=False,
        check_parquet=True,
    )

    assert summary.expected_files == 3
    assert summary.upstream_available_files == 3
    assert summary.upstream_missing_files == 0
    assert summary.local_present_files == 2
    assert summary.local_missing_files == 1
    assert summary.local_corrupt_files == 1
    assert summary.local_missing_samples == [missing_name]
    assert summary.local_corrupt_samples == [corrupt_name]


def test_verify_local_raw_mirror_separates_in_progress_downloads(tmp_path: Path, monkeypatch):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()

    in_progress_name = "polymarket_orderbook_2026-03-21T15.parquet"
    monkeypatch.setattr(verifier, "_discover_archive_filenames", lambda **_: [in_progress_name])

    local_path = raw_root / raw_relative_path(in_progress_name)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(b"partial")
    local_path.with_suffix(local_path.suffix + ".aria2").write_text("control")

    summary = verifier.verify_local_raw_mirror(
        vendor="pmxt",
        raw_root=raw_root,
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        raw_base_url="https://r2.pmxt.dev",
        check_upstream=False,
        check_parquet=True,
    )

    assert summary.local_present_files == 1
    assert summary.local_in_progress_files == 1
    assert summary.local_corrupt_files == 0
    assert summary.local_in_progress_samples == [in_progress_name]
