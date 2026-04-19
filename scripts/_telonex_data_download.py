from __future__ import annotations

import os
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tqdm.auto import tqdm

from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_API_KEY_ENV,
)

_USER_AGENT = "prediction-market-backtesting/1.0"
_DEFAULT_API_BASE_URL = "https://api.telonex.io"
_DEFAULT_CHANNEL = "quotes"
_EXCHANGE = "polymarket"
_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
_STATUS_REFRESH_SECS = 0.2


@dataclass(frozen=True)
class TelonexDownloadSummary:
    destination: str
    channel: str
    base_url: str
    market_slugs: list[str]
    outcome: str | None
    outcome_id: int | None
    requested_days: int
    downloaded_days: int
    skipped_existing_days: int
    missing_days: list[str]
    failed_days: list[str]
    source_hits: dict[str, int]
    start_date: str | None
    end_date: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _parse_date_bound(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(normalized, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
    return parsed.date()


def _date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _outcome_segment(*, outcome: str | None, outcome_id: int | None) -> str:
    if outcome is not None:
        return outcome
    if outcome_id is None:
        raise ValueError("Either outcome or outcome_id must be provided.")
    return str(outcome_id)


def _destination_path(
    *,
    destination: Path,
    channel: str,
    market_slug: str,
    outcome_segment: str,
    day: date,
) -> Path:
    return (
        destination
        / _EXCHANGE
        / channel
        / market_slug
        / outcome_segment
        / f"{day:%Y-%m-%d}.parquet"
    )


def _api_url(
    *,
    base_url: str,
    channel: str,
    market_slug: str,
    outcome: str | None,
    outcome_id: int | None,
    day: date,
) -> str:
    params: dict[str, str] = {"slug": market_slug}
    if outcome is not None:
        params["outcome"] = outcome
    else:
        assert outcome_id is not None
        params["outcome_id"] = str(outcome_id)
    return (
        f"{base_url.rstrip('/')}/v1/downloads/{_EXCHANGE}/{channel}/{day:%Y-%m-%d}"
        f"?{urlencode(params)}"
    )


def _progress_bar_description(*, total: int, completed: int, active: int) -> str:
    if total <= 0:
        return "Downloading Telonex days"
    if active > 0:
        return f"Downloading Telonex days ({completed}/{total} done, {active} active)"
    if completed >= total:
        return f"Downloading Telonex days ({total}/{total} done)"
    return f"Downloading Telonex days ({completed}/{total} done)"


def _format_mib(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.1f} MiB"


def _day_label(day: date) -> str:
    return f"{day:%Y-%m-%d}"


def _active_status_text(
    *,
    market_slug: str,
    day: date,
    written_bytes: int,
    total_bytes: int | None,
    elapsed_secs: float,
) -> str:
    if total_bytes is None:
        transfer = _format_mib(written_bytes)
    else:
        transfer = f"{_format_mib(written_bytes)}/{_format_mib(total_bytes)}"
    return f"active: telonex {market_slug} {_day_label(day)} {transfer} {elapsed_secs:4.1f}s"


def _day_result_text(
    *, market_slug: str, day: date, elapsed_secs: float, detail: str, source: str
) -> str:
    return f"  {market_slug}  {_day_label(day)}  {elapsed_secs:6.3f}s  {detail:>10s}  {source}"


def _format_download_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        message = f"HTTP {exc.code}"
    else:
        message = str(exc) or exc.__class__.__name__
    return message.replace("\n", " ")[:180]


def _set_status(
    progress_bar: tqdm | None,
    *,
    total: int,
    completed: int,
    active: int,
    status: str,
    force: bool = False,
) -> None:
    if progress_bar is None:
        return
    description = _progress_bar_description(total=total, completed=completed, active=active)
    now = time.monotonic()
    last_update = float(getattr(progress_bar, "_telonex_last_status_ts", 0.0))
    last_status = str(getattr(progress_bar, "_telonex_last_status", ""))
    last_description = str(getattr(progress_bar, "_telonex_last_description", ""))
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
    progress_bar._telonex_last_status_ts = now
    progress_bar._telonex_last_status = status
    progress_bar._telonex_last_description = description


def _write_progress_line(progress_bar: tqdm | None, line: str) -> None:
    if progress_bar is None:
        return
    progress_bar.write(line)


def _download_one(
    *,
    url: str,
    destination: Path,
    api_key: str,
    timeout_secs: int,
    progress_bar: tqdm | None,
    total: int,
    completed: int,
    market_slug: str,
    day: date,
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_name(f"{destination.name}.tmp.{os.getpid()}")
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": _USER_AGENT,
        },
    )
    started_at = time.perf_counter()
    try:
        _set_status(
            progress_bar,
            total=total,
            completed=completed,
            active=1,
            status=_active_status_text(
                market_slug=market_slug,
                day=day,
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
                    total=total,
                    completed=completed,
                    active=1,
                    status=_active_status_text(
                        market_slug=market_slug,
                        day=day,
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


def download_telonex_days(
    *,
    destination: Path,
    market_slugs: list[str],
    outcome: str | None = None,
    outcome_id: int | None = None,
    channel: str = _DEFAULT_CHANNEL,
    base_url: str = _DEFAULT_API_BASE_URL,
    start_date: str | None = None,
    end_date: str | None = None,
    overwrite: bool = False,
    timeout_secs: int = 60,
    show_progress: bool = True,
) -> TelonexDownloadSummary:
    if not market_slugs:
        raise ValueError("At least one market_slug must be provided.")
    if outcome is None and outcome_id is None:
        raise ValueError("Provide --outcome or --outcome-id.")
    if outcome is not None and outcome_id is not None:
        raise ValueError("Provide only one of --outcome or --outcome-id.")

    api_key = os.getenv(TELONEX_API_KEY_ENV)
    if api_key is None or not api_key.strip():
        raise ValueError(
            f"{TELONEX_API_KEY_ENV} must be set in the environment to download Telonex files."
        )
    api_key = api_key.strip()

    normalized_destination = destination.expanduser().resolve()
    normalized_destination.mkdir(parents=True, exist_ok=True)

    start = _parse_date_bound(start_date)
    end = _parse_date_bound(end_date)
    if start is None:
        raise ValueError("--start-date is required.")
    if end is None:
        raise ValueError("--end-date is required.")
    if start > end:
        raise ValueError(
            f"Telonex download window is empty: start_date {start_date!r} is after "
            f"end_date {end_date!r}."
        )

    outcome_segment = _outcome_segment(outcome=outcome, outcome_id=outcome_id)
    days = _date_range(start, end)

    day_jobs: list[tuple[str, date]] = [(slug, day) for slug in market_slugs for day in days]
    total_days = len(day_jobs)

    if show_progress:
        print(
            f"Telonex source: api {base_url.rstrip('/')} (channel={channel}, "
            f"outcome={outcome_segment!r})"
        )
        print(
            f"Downloading Telonex daily Parquet files to {normalized_destination} "
            f"(markets={len(market_slugs)}, days={len(days)}, requested={total_days}, "
            f"window={_day_label(start)}..{_day_label(end)})..."
        )

    progress_bar = (
        tqdm(
            total=total_days,
            desc=_progress_bar_description(total=total_days, completed=0, active=0),
            unit="day",
            leave=False,
            bar_format=("{l_bar}{bar}| [{elapsed}<{remaining}]{postfix}"),
        )
        if show_progress
        else None
    )

    source_hits: Counter[str] = Counter()
    failed_days: list[str] = []
    missing_days: list[str] = []
    downloaded_days = 0
    skipped_existing_days = 0
    completed = 0
    base_label = f"api:{base_url.rstrip('/')}"

    try:
        for market_slug, day in day_jobs:
            destination_path = _destination_path(
                destination=normalized_destination,
                channel=channel,
                market_slug=market_slug,
                outcome_segment=outcome_segment,
                day=day,
            )
            day_started_at = time.perf_counter()
            if destination_path.exists() and not overwrite:
                skipped_existing_days += 1
                _write_progress_line(
                    progress_bar,
                    _day_result_text(
                        market_slug=market_slug,
                        day=day,
                        elapsed_secs=time.perf_counter() - day_started_at,
                        detail="existing",
                        source="skip",
                    ),
                )
                if progress_bar is not None:
                    progress_bar.update(1)
                completed += 1
                _set_status(
                    progress_bar,
                    total=total_days,
                    completed=completed,
                    active=0,
                    status="",
                    force=True,
                )
                continue

            url = _api_url(
                base_url=base_url,
                channel=channel,
                market_slug=market_slug,
                outcome=outcome,
                outcome_id=outcome_id,
                day=day,
            )
            try:
                downloaded_size_bytes = _download_one(
                    url=url,
                    destination=destination_path,
                    api_key=api_key,
                    timeout_secs=timeout_secs,
                    progress_bar=progress_bar,
                    total=total_days,
                    completed=completed,
                    market_slug=market_slug,
                    day=day,
                )
                source_hits[base_label] += 1
                downloaded_days += 1
                _write_progress_line(
                    progress_bar,
                    _day_result_text(
                        market_slug=market_slug,
                        day=day,
                        elapsed_secs=time.perf_counter() - day_started_at,
                        detail=_format_mib(downloaded_size_bytes),
                        source="api",
                    ),
                )
            except HTTPError as exc:
                if exc.code == 404:
                    missing_days.append(f"{market_slug} {_day_label(day)}")
                    detail = "missing"
                else:
                    failed_days.append(f"{market_slug} {_day_label(day)}")
                    detail = "failed"
                _write_progress_line(
                    progress_bar,
                    _day_result_text(
                        market_slug=market_slug,
                        day=day,
                        elapsed_secs=time.perf_counter() - day_started_at,
                        detail=detail,
                        source=f"api; last_error={_format_download_error(exc)}",
                    ),
                )
            except Exception as exc:
                failed_days.append(f"{market_slug} {_day_label(day)}")
                _write_progress_line(
                    progress_bar,
                    _day_result_text(
                        market_slug=market_slug,
                        day=day,
                        elapsed_secs=time.perf_counter() - day_started_at,
                        detail="failed",
                        source=f"api; last_error={_format_download_error(exc)}",
                    ),
                )

            if progress_bar is not None:
                progress_bar.update(1)
            completed += 1
            _set_status(
                progress_bar,
                total=total_days,
                completed=completed,
                active=0,
                status="",
                force=True,
            )
    finally:
        if progress_bar is not None:
            progress_bar.close()

    return TelonexDownloadSummary(
        destination=str(normalized_destination),
        channel=channel,
        base_url=base_url.rstrip("/"),
        market_slugs=list(market_slugs),
        outcome=outcome,
        outcome_id=outcome_id,
        requested_days=total_days,
        downloaded_days=downloaded_days,
        skipped_existing_days=skipped_existing_days,
        missing_days=missing_days,
        failed_days=failed_days,
        source_hits=dict(source_hits),
        start_date=_day_label(start),
        end_date=_day_label(end),
    )
