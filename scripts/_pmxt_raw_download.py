from __future__ import annotations

import os
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pyarrow.parquet as pq
from tqdm.auto import tqdm

from pmxt_relay.archive import extract_archive_filenames, fetch_archive_page
from pmxt_relay.storage import parse_archive_hour, raw_relative_path

_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_ARCHIVE_LISTING_URL = "https://archive.pmxt.dev/Polymarket/v2"
_DEFAULT_ARCHIVE_BASE_URL = "https://r2v2.pmxt.dev"
_DEFAULT_V1_ARCHIVE_LISTING_URL = "https://archive.pmxt.dev/Polymarket/v1"
_DEFAULT_V1_ARCHIVE_BASE_URL = "https://r2.pmxt.dev"
_DEFAULT_RELAY_BASE_URL = "https://209-209-10-83.sslip.io"
_DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024
_STATUS_REFRESH_SECS = 0.2
_MIN_NONEMPTY_RAW_BYTES = 1024 * 1024
_RAW_FILENAME_PREFIX = "polymarket_orderbook_"
_RAW_FILENAME_SUFFIX = ".parquet"


@dataclass(frozen=True)
class ArchiveSource:
    listing_url: str
    base_url: str


@dataclass(frozen=True)
class RawDownloadSummary:
    destination: str
    requested_hours: int
    archive_listed_hours: int
    downloaded_hours: int
    skipped_existing_hours: int
    refreshed_existing_hours: int
    archive_missing_hours: list[str]
    failed_hours: list[str]
    missing_local_hours: list[str]
    empty_local_hours: list[str]
    zero_row_local_hours: list[str]
    small_local_hours: list[str]
    source_hits: dict[str, int]
    source_order: list[str]
    archive_sources: list[str]
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


def discover_archive_filenames(
    *,
    archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL,
    timeout_secs: int = 60,
    stale_pages: int = 1,
    max_pages: int | None = None,
) -> list[str]:
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

    return sorted(discovered, key=discovered.__getitem__, reverse=True)


def discover_archive_hours(
    *,
    archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL,
    timeout_secs: int = 60,
    stale_pages: int = 1,
    max_pages: int | None = None,
) -> list[datetime]:
    return [
        parse_archive_hour(filename)
        for filename in discover_archive_filenames(
            archive_listing_url=archive_listing_url,
            timeout_secs=timeout_secs,
            stale_pages=stale_pages,
            max_pages=max_pages,
        )
    ]


def _filter_filenames_to_window(
    filenames: list[str], *, start_hour: datetime | None, end_hour: datetime | None
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


def _sort_filenames_newest_first(filenames: list[str]) -> list[str]:
    return sorted(filenames, key=parse_archive_hour, reverse=True)


def _filename_for_hour(hour: datetime) -> str:
    return f"polymarket_orderbook_{hour.strftime('%Y-%m-%dT%H')}.parquet"


def _hour_range_filenames(*, start_hour: datetime, end_hour: datetime) -> list[str]:
    filenames: list[str] = []
    current = start_hour
    while current <= end_hour:
        filenames.append(_filename_for_hour(current))
        current += timedelta(hours=1)
    return filenames


def _archive_url(base_url: str, filename: str) -> str:
    return f"{base_url.rstrip('/')}/{filename}"


def _relay_url(base_url: str, filename: str) -> str:
    relative = raw_relative_path(filename).as_posix()
    return f"{base_url.rstrip('/')}/v1/raw/{relative}"


def _source_url(
    *, source: str, filename: str, archive_base_url: str, relay_base_url: str
) -> tuple[str, str]:
    if source == "archive":
        return _archive_url(archive_base_url, filename), f"archive:{archive_base_url.rstrip('/')}"
    return _relay_url(relay_base_url, filename), f"relay:{relay_base_url.rstrip('/')}"


def _archive_sources_from_args(
    *,
    archive_sources: list[tuple[str, str]] | None,
    archive_listing_url: str,
    archive_base_url: str,
) -> list[ArchiveSource]:
    if archive_sources is None:
        return [ArchiveSource(archive_listing_url.rstrip("/"), archive_base_url.rstrip("/"))]
    normalized: list[ArchiveSource] = []
    seen: set[tuple[str, str]] = set()
    for listing_url, base_url in archive_sources:
        source = ArchiveSource(listing_url.rstrip("/"), base_url.rstrip("/"))
        key = (source.listing_url, source.base_url)
        if key in seen:
            continue
        normalized.append(source)
        seen.add(key)
    if not normalized:
        raise ValueError("At least one PMXT archive source must be configured.")
    return normalized


def _archive_candidate_urls(
    *,
    filename: str,
    archive_sources: list[ArchiveSource],
    discovered_archive_base_urls: dict[str, str],
) -> list[tuple[str, str]]:
    discovered_base_url = discovered_archive_base_urls.get(filename)
    if discovered_base_url is not None:
        return [
            (
                _archive_url(discovered_base_url, filename),
                f"archive:{discovered_base_url.rstrip('/')}",
            )
        ]
    return [
        (_archive_url(source.base_url, filename), f"archive:{source.base_url.rstrip('/')}")
        for source in archive_sources
    ]


def _candidate_urls(
    *,
    source: str,
    filename: str,
    archive_sources: list[ArchiveSource],
    discovered_archive_base_urls: dict[str, str],
    relay_base_url: str,
) -> list[tuple[str, str]]:
    if source == "archive":
        return _archive_candidate_urls(
            filename=filename,
            archive_sources=archive_sources,
            discovered_archive_base_urls=discovered_archive_base_urls,
        )
    return [(_relay_url(relay_base_url, filename), f"relay:{relay_base_url.rstrip('/')}")]


def _remote_content_length(*, url: str, timeout_secs: int) -> int | None:
    request = Request(url, method="HEAD", headers={"User-Agent": _USER_AGENT})
    try:
        with urlopen(request, timeout=timeout_secs) as response:
            value = response.headers.get("Content-Length")
    except Exception:
        return None
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _hour_label_for_filename(filename: str) -> str:
    if filename.startswith(_RAW_FILENAME_PREFIX) and filename.endswith(_RAW_FILENAME_SUFFIX):
        return filename.removeprefix(_RAW_FILENAME_PREFIX).removesuffix(_RAW_FILENAME_SUFFIX)
    return parse_archive_hour(filename).strftime("%Y-%m-%dT%H")


def _progress_bar_description(*, total_hours: int, completed_hours: int, active_hours: int) -> str:
    if total_hours <= 0:
        return "Downloading raw hours"

    completed = min(max(0, completed_hours), total_hours)
    active = min(max(0, active_hours), total_hours)
    if active > 0:
        return f"Downloading raw hours ({completed}/{total_hours} done, {active} active)"
    if completed >= total_hours:
        return f"Downloading raw hours ({total_hours}/{total_hours} done)"
    return f"Downloading raw hours ({completed}/{total_hours} done)"


def _format_mib(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.1f} MiB"


def _active_status_text(
    *,
    source: str,
    hour_label: str,
    written_bytes: int,
    total_bytes: int | None,
    elapsed_secs: float,
) -> str:
    if total_bytes is None:
        transfer = _format_mib(written_bytes)
    else:
        transfer = f"{_format_mib(written_bytes)}/{_format_mib(total_bytes)}"
    return f"active: {source} {hour_label} {transfer} {elapsed_secs:4.1f}s"


def _hour_result_text(*, hour_label: str, elapsed_secs: float, detail: str, source: str) -> str:
    return f"  {hour_label:>13s}  {elapsed_secs:6.3f}s  {detail:>10s}  {source}"


def _format_download_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        message = f"HTTP {exc.code}"
    else:
        message = str(exc) or exc.__class__.__name__
    return message.replace("\n", " ")[:180]


def _source_priority_summary(
    *, source_sequence: list[str], archive_sources: list[ArchiveSource], relay_base_url: str
) -> str:
    parts: list[str] = []
    for source in source_sequence:
        if source == "archive":
            archive_labels = ", ".join(source.base_url.rstrip("/") for source in archive_sources)
            parts.append(f"archive {archive_labels}")
        else:
            parts.append(f"relay {relay_base_url.rstrip('/')}")
    return "PMXT raw source: explicit priority (" + " -> ".join(parts) + ")"


def _window_label_from_filenames(filenames: list[str]) -> tuple[str | None, str | None]:
    if not filenames:
        return None, None
    ordered = sorted(filenames, key=parse_archive_hour)
    return _hour_label_for_filename(ordered[0]), _hour_label_for_filename(ordered[-1])


def _read_parquet_row_count(path: Path) -> int | None:
    try:
        return pq.read_metadata(path).num_rows
    except Exception:
        return None


def _local_raw_is_empty(path: Path) -> bool:
    try:
        path.stat()
    except OSError:
        return True
    return False


def _existing_refresh_reason(
    *,
    path: Path,
    source_urls: list[str],
    timeout_secs: int,
) -> str | None:
    try:
        local_size = path.stat().st_size
    except OSError:
        return "unreadable"

    if _local_raw_is_empty(path):
        return "empty"

    for url in source_urls:
        remote_size = _remote_content_length(url=url, timeout_secs=timeout_secs)
        if remote_size is not None and remote_size > local_size:
            return f"remote-larger:{_format_mib(remote_size)}"
    return None


def _validate_local_raw_hours(
    *, destination: Path, filenames: list[str]
) -> tuple[list[str], list[str], list[str], list[str]]:
    missing: list[str] = []
    for filename in filenames:
        hour_label = parse_archive_hour(filename).isoformat()
        destination_path = destination / raw_relative_path(filename)
        if not destination_path.exists():
            missing.append(hour_label)
    return missing, [], [], []


def _pid_is_active(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _stale_tmp_download_paths(destination: Path) -> list[Path]:
    if not destination.parent.exists():
        return []

    tmp_paths: list[Path] = []
    plain_tmp_path = destination.with_name(f"{destination.name}.tmp")
    if plain_tmp_path.exists():
        tmp_paths.append(plain_tmp_path)
    tmp_paths.extend(sorted(destination.parent.glob(f"{destination.name}.tmp.*")))
    return tmp_paths


def _is_stale_tmp_download_path(tmp_path: Path, *, destination_exists: bool) -> bool:
    if tmp_path.name.endswith(".tmp"):
        return destination_exists

    tmp_marker = ".tmp."
    if tmp_marker not in tmp_path.name:
        return False

    pid_text = tmp_path.name.rsplit(tmp_marker, maxsplit=1)[-1]
    try:
        pid = int(pid_text)
    except ValueError:
        return True
    return not _pid_is_active(pid)


def _cleanup_stale_tmp_downloads(destination: Path) -> int:
    destination_exists = destination.exists()
    removed = 0
    for tmp_path in _stale_tmp_download_paths(destination):
        if not tmp_path.is_file():
            continue
        if not _is_stale_tmp_download_path(tmp_path, destination_exists=destination_exists):
            continue
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            continue
        removed += 1
    return removed


def _set_status(
    progress_bar: tqdm | None,
    *,
    total_hours: int,
    completed_hours: int,
    active_hours: int,
    status: str,
    force: bool = False,
) -> None:
    if progress_bar is None:
        return
    description = _progress_bar_description(
        total_hours=total_hours, completed_hours=completed_hours, active_hours=active_hours
    )
    now = time.monotonic()
    last_update = float(getattr(progress_bar, "_pmxt_last_status_ts", 0.0))
    last_status = str(getattr(progress_bar, "_pmxt_last_status", ""))
    last_description = str(getattr(progress_bar, "_pmxt_last_description", ""))
    if (
        not force
        and status == last_status
        and description == last_description
        and now - last_update < _STATUS_REFRESH_SECS
    ):
        return
    progress_bar.set_description_str(description, refresh=False)
    progress_bar.set_postfix_str(status, refresh=False)
    progress_bar.refresh()
    progress_bar._pmxt_last_status_ts = now
    progress_bar._pmxt_last_status = status
    progress_bar._pmxt_last_description = description


def _write_progress_line(progress_bar: tqdm | None, line: str) -> None:
    if progress_bar is None:
        return
    progress_bar.write(line)


def _download_one(
    *,
    url: str,
    destination: Path,
    timeout_secs: int,
    progress_bar: tqdm | None,
    total_hours: int,
    completed_hours: int,
    source: str,
    hour_label: str,
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.tmp.{os.getpid()}")
    request = Request(url, headers={"User-Agent": _USER_AGENT})
    started_at = time.perf_counter()
    try:
        _set_status(
            progress_bar,
            total_hours=total_hours,
            completed_hours=completed_hours,
            active_hours=1,
            status=_active_status_text(
                source=source,
                hour_label=hour_label,
                written_bytes=0,
                total_bytes=None,
                elapsed_secs=0.0,
            ),
            force=True,
        )
        with urlopen(request, timeout=timeout_secs) as response, tmp_path.open("wb") as handle:
            total_bytes_header = response.headers.get("Content-Length")
            total_bytes = int(total_bytes_header) if total_bytes_header else None
            written = 0
            while True:
                chunk = response.read(_DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                handle.write(chunk)
                written += len(chunk)
                _set_status(
                    progress_bar,
                    total_hours=total_hours,
                    completed_hours=completed_hours,
                    active_hours=1,
                    status=_active_status_text(
                        source=source,
                        hour_label=hour_label,
                        written_bytes=written,
                        total_bytes=total_bytes,
                        elapsed_secs=time.perf_counter() - started_at,
                    ),
                )
        os.replace(tmp_path, destination)
        if written == 0 and total_bytes is not None:
            return total_bytes
        return written
    finally:
        tmp_path.unlink(missing_ok=True)


def download_raw_hours(
    *,
    destination: Path,
    archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL,
    archive_base_url: str = _DEFAULT_ARCHIVE_BASE_URL,
    archive_sources: list[tuple[str, str]] | None = None,
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
    resolved_archive_sources = _archive_sources_from_args(
        archive_sources=archive_sources,
        archive_listing_url=archive_listing_url,
        archive_base_url=archive_base_url,
    )

    selected_sources = source_order or ["archive", "relay"]
    source_sequence: list[str] = []
    for source in selected_sources:
        normalized = source.strip().casefold()
        if normalized not in {"archive", "relay"}:
            raise ValueError(f"Unsupported PMXT raw source {source!r}. Use archive or relay.")
        if normalized not in source_sequence:
            source_sequence.append(normalized)
    if not source_sequence:
        raise ValueError("At least one PMXT raw source must be enabled.")

    start_hour = _parse_hour_bound(start_time)
    end_hour = _parse_hour_bound(end_time)
    archive_missing_hours: list[str] = []
    archive_listed_hours = 0
    discovered_archive_base_urls: dict[str, str] = {}
    if start_hour is not None and end_hour is not None:
        filenames = _hour_range_filenames(start_hour=start_hour, end_hour=end_hour)
    else:
        discovered_filenames: list[str] = []
        discovered_seen: set[str] = set()
        for archive_source in resolved_archive_sources:
            source_filenames = discover_archive_filenames(
                archive_listing_url=archive_source.listing_url,
                timeout_secs=timeout_secs,
                stale_pages=discovery_stale_pages,
                max_pages=discovery_max_pages,
            )
            for filename in source_filenames:
                if filename in discovered_seen:
                    continue
                discovered_archive_base_urls[filename] = archive_source.base_url
                discovered_filenames.append(filename)
                discovered_seen.add(filename)
        discovered_filenames = _sort_filenames_newest_first(discovered_filenames)
        discovered_filenames = _filter_filenames_to_window(
            discovered_filenames, start_hour=start_hour, end_hour=end_hour
        )
        archive_listed_hours = len(discovered_filenames)
        if discovered_filenames:
            filenames = list(discovered_filenames)
            download_filenames = list(discovered_filenames)
        else:
            filenames = []
            download_filenames = []
    if start_hour is not None and end_hour is not None:
        download_filenames = list(filenames)
    filenames = _sort_filenames_newest_first(filenames)
    download_filenames = _sort_filenames_newest_first(download_filenames)
    if not filenames:
        if start_hour is not None and end_hour is not None and start_hour > end_hour:
            raise ValueError(
                f"PMXT raw download window is empty: start_time {start_time!r} is after "
                f"end_time {end_time!r}."
            )
        raise RuntimeError(
            "No PMXT raw archive hours were discovered or selected. "
            "Checked listings "
            f"{[source.listing_url for source in resolved_archive_sources]!r}. "
            "Pass --start-time/--end-time for an explicit window or check the archive listing URL."
        )

    if show_progress:
        print(
            _source_priority_summary(
                source_sequence=source_sequence,
                archive_sources=resolved_archive_sources,
                relay_base_url=relay_base_url,
            )
        )
        window_start_label, window_end_label = _window_label_from_filenames(filenames)
        window_parts = [f"requested_hours={len(filenames)}"]
        if window_start_label is not None:
            window_parts.append(f"window_start={window_start_label}")
        if window_end_label is not None:
            window_parts.append(f"window_end={window_end_label}")
        print(
            f"Downloading PMXT raw hours to {normalized_destination} ({', '.join(window_parts)})..."
        )

    progress_bar = (
        tqdm(
            total=len(download_filenames),
            desc=_progress_bar_description(
                total_hours=len(download_filenames), completed_hours=0, active_hours=0
            ),
            unit="hr",
            leave=False,
            bar_format=("{l_bar}{bar}| [{elapsed}<{remaining}]{postfix}"),
        )
        if show_progress
        else None
    )
    source_hits: Counter[str] = Counter()
    failed_hours: list[str] = []
    archive_object_missing_hours: list[str] = []
    downloaded_hours = 0
    skipped_existing_hours = 0
    refreshed_existing_hours = 0
    completed_hours = 0

    try:
        for filename in download_filenames:
            destination_path = normalized_destination / raw_relative_path(filename)
            _cleanup_stale_tmp_downloads(destination_path)
            hour_label = _hour_label_for_filename(filename)
            if destination_path.exists() and not overwrite:
                source_urls = [
                    url
                    for candidate_source in source_sequence
                    for url, _source_label in _candidate_urls(
                        source=candidate_source,
                        filename=filename,
                        archive_sources=resolved_archive_sources,
                        discovered_archive_base_urls=discovered_archive_base_urls,
                        relay_base_url=relay_base_url,
                    )
                ]
                refresh_reason = _existing_refresh_reason(
                    path=destination_path,
                    source_urls=source_urls,
                    timeout_secs=timeout_secs,
                )
                if refresh_reason is None:
                    skipped_existing_hours += 1
                    _write_progress_line(
                        progress_bar,
                        _hour_result_text(
                            hour_label=hour_label,
                            elapsed_secs=0.0,
                            detail="existing",
                            source="skip",
                        ),
                    )
                    if progress_bar is not None:
                        progress_bar.update(1)
                    completed_hours += 1
                    _set_status(
                        progress_bar,
                        total_hours=len(download_filenames),
                        completed_hours=completed_hours,
                        active_hours=0,
                        status="",
                        force=True,
                    )
                    continue
                refreshed_existing_hours += 1
                _write_progress_line(
                    progress_bar,
                    _hour_result_text(
                        hour_label=hour_label,
                        elapsed_secs=0.0,
                        detail="refresh",
                        source=refresh_reason,
                    ),
                )

            last_error: Exception | None = None
            hour_started_at = time.perf_counter()
            completed_source: str | None = None
            downloaded_size_bytes: int | None = None
            for source in source_sequence:
                source_candidates = _candidate_urls(
                    source=source,
                    filename=filename,
                    archive_sources=resolved_archive_sources,
                    discovered_archive_base_urls=discovered_archive_base_urls,
                    relay_base_url=relay_base_url,
                )
                for url, source_label in source_candidates:
                    try:
                        downloaded_size_bytes = _download_one(
                            url=url,
                            destination=destination_path,
                            timeout_secs=timeout_secs,
                            progress_bar=progress_bar,
                            total_hours=len(download_filenames),
                            completed_hours=completed_hours,
                            source=source,
                            hour_label=hour_label,
                        )
                        source_hits[source_label] += 1
                        downloaded_hours += 1
                        completed_source = source
                        last_error = None
                        break
                    except HTTPError as exc:
                        last_error = exc
                        if exc.code != 404:
                            continue
                    except Exception as exc:
                        last_error = exc
                        continue
                if last_error is None:
                    break

            elapsed_secs = time.perf_counter() - hour_started_at
            if last_error is not None:
                hour_iso = parse_archive_hour(filename).isoformat()
                missing_object = isinstance(last_error, HTTPError) and last_error.code == 404
                if missing_object:
                    archive_object_missing_hours.append(hour_iso)
                    detail = "missing"
                else:
                    failed_hours.append(hour_iso)
                    detail = "failed"
                _write_progress_line(
                    progress_bar,
                    _hour_result_text(
                        hour_label=hour_label,
                        elapsed_secs=elapsed_secs,
                        detail=detail,
                        source=(
                            f"{' -> '.join(source_sequence)}; "
                            f"last_error={_format_download_error(last_error)}"
                        ),
                    ),
                )
            elif downloaded_size_bytes is not None and completed_source is not None:
                _write_progress_line(
                    progress_bar,
                    _hour_result_text(
                        hour_label=hour_label,
                        elapsed_secs=elapsed_secs,
                        detail=_format_mib(downloaded_size_bytes),
                        source=completed_source,
                    ),
                )
            if progress_bar is not None:
                progress_bar.update(1)
            completed_hours += 1
            _set_status(
                progress_bar,
                total_hours=len(download_filenames),
                completed_hours=completed_hours,
                active_hours=0,
                status="",
                force=True,
            )
    finally:
        if progress_bar is not None:
            progress_bar.close()

    archive_object_missing_set = set(archive_object_missing_hours)
    validation_filenames = [
        filename
        for filename in download_filenames
        if parse_archive_hour(filename).isoformat() not in archive_object_missing_set
    ]
    (
        missing_local_hours,
        empty_local_hours,
        zero_row_local_hours,
        small_local_hours,
    ) = _validate_local_raw_hours(
        destination=normalized_destination, filenames=validation_filenames
    )

    return RawDownloadSummary(
        destination=str(normalized_destination),
        requested_hours=len(filenames),
        archive_listed_hours=archive_listed_hours,
        downloaded_hours=downloaded_hours,
        skipped_existing_hours=skipped_existing_hours,
        refreshed_existing_hours=refreshed_existing_hours,
        archive_missing_hours=archive_missing_hours + archive_object_missing_hours,
        failed_hours=failed_hours,
        missing_local_hours=missing_local_hours,
        empty_local_hours=empty_local_hours,
        zero_row_local_hours=zero_row_local_hours,
        small_local_hours=small_local_hours,
        source_hits=dict(source_hits),
        source_order=source_sequence,
        archive_sources=[source.base_url for source in resolved_archive_sources],
        start_hour=start_hour.isoformat() if start_hour is not None else None,
        end_hour=end_hour.isoformat() if end_hour is not None else None,
    )
