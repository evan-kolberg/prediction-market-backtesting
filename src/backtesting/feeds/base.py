"""Abstract interface for chronological trade data feeds."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime

from src.backtesting.models import MarketInfo, TradeEvent


class BaseFeed(ABC):
    """Abstract interface for a chronological trade data feed.

    Implementations load trades from platform-specific parquet files,
    normalize them into TradeEvent objects, and yield them in
    chronological order.
    """

    @abstractmethod
    def markets(self) -> dict[str, MarketInfo]:
        """Return all available market metadata, keyed by market_id."""
        ...

    @abstractmethod
    def trade_count(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> int:
        """Return total number of trades matching the given filters.

        Used by the engine to provide ETA in the progress bar.
        """
        ...

    @abstractmethod
    def trades(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        batch_size: int = 50_000,
    ) -> Iterator[TradeEvent]:
        """Yield normalized TradeEvent objects in chronological order.

        Args:
            market_ids: Filter to specific markets. None means all.
            start_time: Only include trades after this time.
            end_time: Only include trades before this time.
            batch_size: Rows to fetch per DuckDB page for memory efficiency.
        """
        ...
