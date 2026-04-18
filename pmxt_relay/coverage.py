from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from pmxt_relay.storage import ARCHIVE_FILENAME_RE

PMXT_ARCHIVE_START_HOUR = datetime(2026, 2, 21, 16, tzinfo=UTC)


def floor_utc_hour(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def elapsed_archive_hours(
    *, start_hour: datetime = PMXT_ARCHIVE_START_HOUR, now: datetime | None = None
) -> int:
    current = floor_utc_hour(datetime.now(UTC) if now is None else now)
    start = floor_utc_hour(start_hour)
    if current < start:
        return 0
    return int((current - start).total_seconds() // 3600) + 1


def iter_archive_hours_desc(
    *, start_hour: datetime = PMXT_ARCHIVE_START_HOUR, now: datetime | None = None
):
    current = floor_utc_hour(datetime.now(UTC) if now is None else now)
    start = floor_utc_hour(start_hour)
    while current >= start:
        yield current
        current -= timedelta(hours=1)


def count_raw_dump_files(raw_root: Path) -> int:
    if not raw_root.exists():
        return 0
    count = 0
    for path in raw_root.rglob("polymarket_orderbook_*.parquet"):
        if path.is_file() and ARCHIVE_FILENAME_RE.fullmatch(path.name):
            count += 1
    return count
