from __future__ import annotations

import re
from datetime import UTC
from datetime import datetime
from pathlib import Path


ARCHIVE_FILENAME_RE = re.compile(r"^polymarket_orderbook_(\d{4}-\d{2}-\d{2}T\d{2})\.parquet$")


def parse_archive_hour(filename: str) -> datetime:
    match = ARCHIVE_FILENAME_RE.match(filename)
    if match is None:
        raise ValueError(f"Unsupported PMXT archive filename: {filename}")
    return datetime.strptime(match.group(1), "%Y-%m-%dT%H").replace(tzinfo=UTC)


def raw_relative_path(filename: str) -> Path:
    hour = parse_archive_hour(filename)
    return Path(str(hour.year)) / f"{hour.month:02d}" / f"{hour.day:02d}" / filename
