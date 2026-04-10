from __future__ import annotations

import asyncio
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

import aiohttp
import pyarrow.parquet as pq

from pmxt_relay.archive import extract_archive_filenames
from pmxt_relay.archive import fetch_archive_page
from pmxt_relay.storage import raw_relative_path


def _discover_archive_filenames(
    *,
    archive_listing_url: str,
    timeout_secs: int,
    stale_pages: int = 1,
    max_pages: int | None = None,
) -> list[str]:
    filenames: list[str] = []
    seen: set[str] = set()
    page = 1
    stale = 0
    while True:
        if max_pages is not None and page > max_pages:
            break
        html = fetch_archive_page(archive_listing_url, page, timeout_secs)
        page_filenames = extract_archive_filenames(html)
        if not page_filenames:
            break
        page_new = 0
        for filename in page_filenames:
            if filename in seen:
                continue
            seen.add(filename)
            filenames.append(filename)
            page_new += 1
        if page_new == 0:
            stale += 1
            if stale >= stale_pages:
                break
        else:
            stale = 0
        page += 1
    return filenames


def _resolve_local_raw_path(raw_root: Path, filename: str) -> Path | None:
    archive_path = raw_root / raw_relative_path(filename)
    if archive_path.exists():
        return archive_path
    flat_path = raw_root / filename
    if flat_path.exists():
        return flat_path
    return None


@dataclass(frozen=True)
class RawMirrorVerificationSummary:
    vendor: str
    raw_root: str
    archive_listing_url: str
    raw_base_url: str
    expected_files: int
    upstream_available_files: int
    upstream_missing_files: int
    local_present_files: int
    local_missing_files: int
    local_in_progress_files: int
    local_corrupt_files: int
    local_zero_byte_files: int
    parquet_schema_failures: int
    required_columns: tuple[str, ...]
    upstream_missing_samples: list[str]
    local_missing_samples: list[str]
    local_corrupt_samples: list[str]
    local_in_progress_samples: list[str]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


async def _head_available_files(
    *, filenames: list[str], raw_base_url: str, concurrency: int, timeout_secs: int
) -> tuple[set[str], list[str]]:
    available: set[str] = set()
    missing: list[str] = []
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def head_one(session: aiohttp.ClientSession, filename: str) -> None:
        url = f"{raw_base_url.rstrip('/')}/{filename}"
        async with semaphore:
            try:
                async with session.head(
                    url, timeout=aiohttp.ClientTimeout(total=timeout_secs)
                ) as response:
                    if response.status == 200:
                        available.add(filename)
                    elif response.status == 404:
                        missing.append(filename)
                    else:
                        missing.append(filename)
            except Exception:
                missing.append(filename)

    connector = aiohttp.TCPConnector(limit=max(1, concurrency))
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*(head_one(session, filename) for filename in filenames))

    return available, sorted(missing)


def verify_local_raw_mirror(
    *,
    vendor: str,
    raw_root: Path,
    archive_listing_url: str,
    raw_base_url: str,
    timeout_secs: int = 60,
    stale_pages: int = 1,
    max_pages: int | None = None,
    check_upstream: bool = True,
    check_parquet: bool = True,
    upstream_head_concurrency: int = 32,
    sample_limit: int = 20,
) -> RawMirrorVerificationSummary:
    normalized_vendor = vendor.strip().casefold()
    if normalized_vendor != "pmxt":
        raise ValueError(
            f"Unsupported vendor '{vendor}'. The raw mirror verifier currently supports: pmxt"
        )

    normalized_raw_root = raw_root.expanduser().resolve()
    filenames = _discover_archive_filenames(
        archive_listing_url=archive_listing_url,
        timeout_secs=timeout_secs,
        stale_pages=stale_pages,
        max_pages=max_pages,
    )
    if check_upstream:
        available_upstream, upstream_missing = asyncio.run(
            _head_available_files(
                filenames=filenames,
                raw_base_url=raw_base_url,
                concurrency=upstream_head_concurrency,
                timeout_secs=timeout_secs,
            )
        )
    else:
        available_upstream = set(filenames)
        upstream_missing = []

    local_present_files = 0
    local_missing: list[str] = []
    local_in_progress: list[str] = []
    local_corrupt: list[str] = []
    local_zero_byte_files = 0
    parquet_schema_failures = 0
    required_columns = ("market_id", "update_type", "data")

    for filename in filenames:
        if check_upstream and filename not in available_upstream:
            continue
        local_path = _resolve_local_raw_path(normalized_raw_root, filename)
        if local_path is None:
            local_missing.append(filename)
            continue
        local_present_files += 1
        try:
            if local_path.stat().st_size <= 0:
                local_zero_byte_files += 1
                local_corrupt.append(filename)
                continue
        except FileNotFoundError:
            local_missing.append(filename)
            continue
        if local_path.with_suffix(local_path.suffix + ".aria2").exists():
            local_in_progress.append(filename)
            continue
        if not check_parquet:
            continue
        try:
            metadata = pq.read_metadata(local_path)
        except Exception:
            local_corrupt.append(filename)
            continue
        schema_names = tuple(metadata.schema.names)
        if any(column not in schema_names for column in required_columns):
            parquet_schema_failures += 1
            local_corrupt.append(filename)

    return RawMirrorVerificationSummary(
        vendor=normalized_vendor,
        raw_root=str(normalized_raw_root),
        archive_listing_url=archive_listing_url,
        raw_base_url=raw_base_url.rstrip("/"),
        expected_files=len(filenames),
        upstream_available_files=len(available_upstream),
        upstream_missing_files=len(upstream_missing),
        local_present_files=local_present_files,
        local_missing_files=len(local_missing),
        local_in_progress_files=len(local_in_progress),
        local_corrupt_files=len(local_corrupt),
        local_zero_byte_files=local_zero_byte_files,
        parquet_schema_failures=parquet_schema_failures,
        required_columns=required_columns,
        upstream_missing_samples=upstream_missing[:sample_limit],
        local_missing_samples=local_missing[:sample_limit],
        local_corrupt_samples=local_corrupt[:sample_limit],
        local_in_progress_samples=local_in_progress[:sample_limit],
    )
