from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import os
import time
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

from tqdm.auto import tqdm

from pmxt_relay.archive import extract_archive_filenames
from pmxt_relay.archive import fetch_archive_page
from pmxt_relay.storage import parse_archive_hour
from pmxt_relay.storage import raw_relative_path


_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_ARCHIVE_LISTING_URL = "https://archive.pmxt.dev/data/Polymarket"
_DEFAULT_ARCHIVE_BASE_URL = "https://r2.pmxt.dev"
_DEFAULT_RELAY_BASE_URL = "https://209-209-10-83.sslip.io"
_DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024
_STATUS_REFRESH_SECS = 0.2


@dataclass(frozen=True)
class RawDownloadSummary:
    destination: str
    requested_hours: int
    downloaded_hours: int
    skipped_existing_hours: int
    failed_hours: list[str]
    source_hits: dict[str, int]
    source_order: list[str]
    start_hour: str | None
    end_hour: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _parse_hour_bound(value: str | None) -> datetime | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(normalized, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
    return parsed.replace(minute=0, second=0, microsecond=0)


def discover_archive_hours(
    *,
    archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL,
    timeout_secs: int = 60,
    stale_pages: int = 1,
    max_pages: int | None = None,
) -> list[datetime]:
    if stale_pages < 1:
        raise ValueError("stale_pages must be >= 1")

    discovered: dict[str, datetime] = {}
    stale_count = 0
    page = 1

    while max_pages is None or page <= max_pages:
        html = fetch_archive_page(archive_listing_url, page, timeout_secs)
        page_filenames = extract_archive_filenames(html)
        new_count = 0
        for filename in page_filenames:
            if filename in discovered:
                continue
            discovered[filename] = parse_archive_hour(filename)
            new_count += 1
        if new_count == 0:
            stale_count += 1
            if stale_count >= stale_pages:
                break
        else:
            stale_count = 0
        page += 1

    return [discovered[name] for name in sorted(discovered, key=discovered.__getitem__)]


def _filter_filenames_to_window(
    filenames: list[str],
    *,
    start_hour: datetime | None,
    end_hour: datetime | None,
) -> list[str]:
    selected: list[str] = []
    for filename in filenames:
        hour = parse_archive_hour(filename)
        if start_hour is not None and hour < start_hour:
            continue
        if end_hour is not None and hour > end_hour:
            continue
        selected.append(filename)
    return selected


def _archive_url(base_url: str, filename: str) -> str:
    return f"{base_url.rstrip('/')}/{filename}"


def _relay_url(base_url: str, filename: str) -> str:
    relative = raw_relative_path(filename).as_posix()
    return f"{base_url.rstrip('/')}/v1/raw/{relative}"


def _set_status(
    progress_bar: tqdm | None,
    status: str,
    *,
    force: bool = False,
) -> None:
    if progress_bar is None:
        return
    now = time.monotonic()
    last_update = float(getattr(progress_bar, "_pmxt_last_status_ts", 0.0))
    last_status = str(getattr(progress_bar, "_pmxt_last_status", ""))
    if not force and status == last_status and now - last_update < _STATUS_REFRESH_SECS:
        return
    progress_bar.set_postfix_str(status)
    progress_bar.refresh()
    setattr(progress_bar, "_pmxt_last_status_ts", now)
    setattr(progress_bar, "_pmxt_last_status", status)


def _download_one(
    *,
    url: str,
    destination: Path,
    timeout_secs: int,
    progress_bar: tqdm | None,
    status_prefix: str,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.tmp.{os.getpid()}")
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with (
            urlopen(request, timeout=timeout_secs) as response,
            tmp_path.open("wb") as handle,
        ):
            total_bytes_header = response.headers.get("Content-Length")
            total_bytes = int(total_bytes_header) if total_bytes_header else None
            written = 0
            _set_status(progress_bar, f"{status_prefix} 0.0 MiB", force=True)
            while True:
                chunk = response.read(_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                handle.write(chunk)
                written += len(chunk)
                if total_bytes is None:
                    status = f"{status_prefix} {written / (1024 * 1024):.1f} MiB"
                else:
                    status = (
                        f"{status_prefix} {written / (1024 * 1024):.1f}/"
                        f"{total_bytes / (1024 * 1024):.1f} MiB"
                    )
                _set_status(progress_bar, status)
        os.replace(tmp_path, destination)
    finally:
        tmp_path.unlink(missing_ok=True)


def download_raw_hours(
    *,
    destination: Path,
    archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL,
    archive_base_url: str = _DEFAULT_ARCHIVE_BASE_URL,
    relay_base_url: str = _DEFAULT_RELAY_BASE_URL,
    source_order: list[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    overwrite: bool = False,
    timeout_secs: int = 60,
    show_progress: bool = True,
    discovery_stale_pages: int = 1,
    discovery_max_pages: int | None = None,
) -> RawDownloadSummary:
    normalized_destination = destination.expanduser().resolve()
    normalized_destination.mkdir(parents=True, exist_ok=True)

    selected_sources = source_order or ["archive", "relay"]
    source_sequence: list[str] = []
    for source in selected_sources:
        normalized = source.strip().casefold()
        if normalized not in {"archive", "relay"}:
            raise ValueError(
                f"Unsupported PMXT raw source {source!r}. Use archive or relay."
            )
        if normalized not in source_sequence:
            source_sequence.append(normalized)
    if not source_sequence:
        raise ValueError("At least one PMXT raw source must be enabled.")

    start_hour = _parse_hour_bound(start_time)
    end_hour = _parse_hour_bound(end_time)
    if start_hour is not None and end_hour is not None:
        filenames = []
        current = start_hour
        while current <= end_hour:
            filenames.append(
                f"polymarket_orderbook_{current.strftime('%Y-%m-%dT%H')}.parquet"
            )
            current += timedelta(hours=1)
    else:
        discovered_hours = discover_archive_hours(
            archive_listing_url=archive_listing_url,
            timeout_secs=timeout_secs,
            stale_pages=discovery_stale_pages,
            max_pages=discovery_max_pages,
        )
        filenames = [
            f"polymarket_orderbook_{hour.strftime('%Y-%m-%dT%H')}.parquet"
            for hour in discovered_hours
        ]
        filenames = _filter_filenames_to_window(
            filenames,
            start_hour=start_hour,
            end_hour=end_hour,
        )

    progress_bar = (
        tqdm(total=len(filenames), desc="Downloading PMXT raws", unit="hour")
        if show_progress
        else None
    )
    source_hits: Counter[str] = Counter()
    failed_hours: list[str] = []
    downloaded_hours = 0
    skipped_existing_hours = 0

    try:
        for filename in filenames:
            destination_path = normalized_destination / raw_relative_path(filename)
            hour_label = parse_archive_hour(filename).isoformat()
            if destination_path.exists() and not overwrite:
                skipped_existing_hours += 1
                _set_status(progress_bar, f"skip {hour_label}", force=True)
                if progress_bar is not None:
                    progress_bar.update(1)
                continue

            last_error: Exception | None = None
            for source in source_sequence:
                if source == "archive":
                    url = _archive_url(archive_base_url, filename)
                    source_label = f"archive:{archive_base_url.rstrip('/')}"
                else:
                    url = _relay_url(relay_base_url, filename)
                    source_label = f"relay:{relay_base_url.rstrip('/')}"
                try:
                    _download_one(
                        url=url,
                        destination=destination_path,
                        timeout_secs=timeout_secs,
                        progress_bar=progress_bar,
                        status_prefix=f"{source} {hour_label}",
                    )
                    source_hits[source_label] += 1
                    downloaded_hours += 1
                    last_error = None
                    break
                except HTTPError as exc:
                    last_error = exc
                    if exc.code != 404:
                        continue
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue

            if last_error is not None:
                failed_hours.append(hour_label)
                _set_status(progress_bar, f"failed {hour_label}", force=True)
            if progress_bar is not None:
                progress_bar.update(1)
    finally:
        if progress_bar is not None:
            progress_bar.close()

    return RawDownloadSummary(
        destination=str(normalized_destination),
        requested_hours=len(filenames),
        downloaded_hours=downloaded_hours,
        skipped_existing_hours=skipped_existing_hours,
        failed_hours=failed_hours,
        source_hits=dict(source_hits),
        source_order=source_sequence,
        start_hour=start_hour.isoformat() if start_hour is not None else None,
        end_hour=end_hour.isoformat() if end_hour is not None else None,
    )
