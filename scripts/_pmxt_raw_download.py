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
_RAW_FILENAME_PREFIX = "polymarket_orderbook_"
_RAW_FILENAME_SUFFIX = ".parquet"


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


def _hour_label_for_filename(filename: str) -> str:
    if filename.startswith(_RAW_FILENAME_PREFIX) and filename.endswith(
        _RAW_FILENAME_SUFFIX
    ):
        return filename.removeprefix(_RAW_FILENAME_PREFIX).removesuffix(
            _RAW_FILENAME_SUFFIX
        )
    return parse_archive_hour(filename).strftime("%Y-%m-%dT%H")


def _progress_bar_description(
    *,
    total_hours: int,
    completed_hours: int,
    active_hours: int,
) -> str:
    if total_hours <= 0:
        return "Downloading raw hours"

    completed = min(max(0, completed_hours), total_hours)
    active = min(max(0, active_hours), total_hours)
    if active > 0:
        return (
            f"Downloading raw hours ({completed}/{total_hours} done, {active} active)"
        )
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


def _hour_result_text(
    *,
    hour_label: str,
    elapsed_secs: float,
    detail: str,
    source: str,
) -> str:
    return f"  {hour_label:>13s}  {elapsed_secs:6.3f}s  {detail:>10s}  {source}"


def _source_priority_summary(
    *,
    source_sequence: list[str],
    archive_base_url: str,
    relay_base_url: str,
) -> str:
    parts: list[str] = []
    for source in source_sequence:
        if source == "archive":
            parts.append(f"archive {archive_base_url.rstrip('/')}")
        else:
            parts.append(f"relay {relay_base_url.rstrip('/')}")
    return "PMXT raw source: explicit priority (" + " -> ".join(parts) + ")"


def _window_label_from_filenames(
    filenames: list[str],
) -> tuple[str | None, str | None]:
    if not filenames:
        return None, None
    return _hour_label_for_filename(filenames[0]), _hour_label_for_filename(
        filenames[-1]
    )


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


def _is_stale_tmp_download_path(
    tmp_path: Path,
    *,
    destination_exists: bool,
) -> bool:
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
        if not _is_stale_tmp_download_path(
            tmp_path,
            destination_exists=destination_exists,
        ):
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
        total_hours=total_hours,
        completed_hours=completed_hours,
        active_hours=active_hours,
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
    setattr(progress_bar, "_pmxt_last_status_ts", now)
    setattr(progress_bar, "_pmxt_last_status", status)
    setattr(progress_bar, "_pmxt_last_description", description)


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
        with (
            urlopen(request, timeout=timeout_secs) as response,
            tmp_path.open("wb") as handle,
        ):
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

    if show_progress:
        print(
            _source_priority_summary(
                source_sequence=source_sequence,
                archive_base_url=archive_base_url,
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
            f"Downloading PMXT raw hours to {normalized_destination} "
            f"({', '.join(window_parts)})..."
        )

    progress_bar = (
        tqdm(
            total=len(filenames),
            desc=_progress_bar_description(
                total_hours=len(filenames),
                completed_hours=0,
                active_hours=0,
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
    downloaded_hours = 0
    skipped_existing_hours = 0
    completed_hours = 0

    try:
        for filename in filenames:
            destination_path = normalized_destination / raw_relative_path(filename)
            _cleanup_stale_tmp_downloads(destination_path)
            hour_label = _hour_label_for_filename(filename)
            if destination_path.exists() and not overwrite:
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
                    total_hours=len(filenames),
                    completed_hours=completed_hours,
                    active_hours=0,
                    status="",
                    force=True,
                )
                continue

            last_error: Exception | None = None
            hour_started_at = time.perf_counter()
            completed_source: str | None = None
            downloaded_size_bytes: int | None = None
            for source in source_sequence:
                if source == "archive":
                    url = _archive_url(archive_base_url, filename)
                    source_label = f"archive:{archive_base_url.rstrip('/')}"
                else:
                    url = _relay_url(relay_base_url, filename)
                    source_label = f"relay:{relay_base_url.rstrip('/')}"
                try:
                    downloaded_size_bytes = _download_one(
                        url=url,
                        destination=destination_path,
                        timeout_secs=timeout_secs,
                        progress_bar=progress_bar,
                        total_hours=len(filenames),
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
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue

            elapsed_secs = time.perf_counter() - hour_started_at
            if last_error is not None:
                failed_hours.append(parse_archive_hour(filename).isoformat())
                _write_progress_line(
                    progress_bar,
                    _hour_result_text(
                        hour_label=hour_label,
                        elapsed_secs=elapsed_secs,
                        detail="failed",
                        source=" -> ".join(source_sequence),
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
                total_hours=len(filenames),
                completed_hours=completed_hours,
                active_hours=0,
                status="",
                force=True,
            )
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
