from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FilteredHourEntry:
    filename: str
    hour: str
    row_count: int | None
    byte_size: int | None
