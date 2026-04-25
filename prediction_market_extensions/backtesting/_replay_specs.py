from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

type TimestampLike = pd.Timestamp | str | object


@dataclass(frozen=True)
class BookReplay:
    market_slug: str
    token_index: int = 0
    lookback_hours: float | None = None
    start_time: TimestampLike | None = None
    end_time: TimestampLike | None = None
    outcome: str | None = None
    metadata: Mapping[str, Any] | None = None


type ReplaySpec = BookReplay


__all__ = [
    "BookReplay",
    "ReplaySpec",
    "TimestampLike",
]
