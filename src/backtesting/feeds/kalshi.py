"""Kalshi data feed — loads trades and markets from parquet files."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import duckdb

from src.backtesting.feeds.base import BaseFeed
from src.backtesting.models import (
    MarketInfo,
    MarketStatus,
    Platform,
    Side,
    TradeEvent,
)


class KalshiFeed(BaseFeed):
    """Data feed that loads Kalshi trades and markets from parquet files.

    Normalizes Kalshi cent prices (1-99) to floats in [0.0, 1.0].
    Yields trades in chronological order with configurable filtering.
    """

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
    ):
        base_dir = Path(__file__).parent.parent.parent.parent
        self.trades_dir = Path(trades_dir or base_dir / "data" / "kalshi" / "trades")
        self.markets_dir = Path(markets_dir or base_dir / "data" / "kalshi" / "markets")
        self._markets: dict[str, MarketInfo] | None = None

    def markets(self) -> dict[str, MarketInfo]:
        """Load all Kalshi market metadata from parquet files."""
        if self._markets is not None:
            return self._markets

        con = duckdb.connect()
        rows = con.execute(
            f"""
            SELECT ticker, event_ticker, title, status, result,
                   open_time, close_time
            FROM '{self.markets_dir}/*.parquet'
            """
        ).fetchall()

        self._markets = {}
        for ticker, event_ticker, title, status, result, open_time, close_time in rows:
            if status == "finalized" and result == "yes":
                ms = MarketStatus.RESOLVED_YES
                side: Side | None = Side.YES
            elif status == "finalized" and result == "no":
                ms = MarketStatus.RESOLVED_NO
                side = Side.NO
            elif status == "closed":
                ms = MarketStatus.CLOSED
                side = None
            else:
                ms = MarketStatus.OPEN
                side = None

            self._markets[ticker] = MarketInfo(
                market_id=ticker,
                platform=Platform.KALSHI,
                title=title or "",
                open_time=open_time,
                close_time=close_time,
                result=side,
                status=ms,
                event_id=event_ticker,
            )

        return self._markets

    def _where_sql(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> str:
        """Build shared WHERE clause for trade queries."""
        parts: list[str] = []
        if market_ids:
            ids_str = ", ".join(f"'{m}'" for m in market_ids)
            parts.append(f"ticker IN ({ids_str})")
        if start_time:
            parts.append(f"created_time >= '{start_time.isoformat()}'")
        if end_time:
            parts.append(f"created_time <= '{end_time.isoformat()}'")
        return " AND ".join(parts) if parts else "1=1"

    def trade_count(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> int:
        """Return total number of Kalshi trades matching filters."""
        con = duckdb.connect()
        where = self._where_sql(market_ids, start_time, end_time)
        result = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{self.trades_dir}/*.parquet'
            WHERE {where}
            """
        ).fetchone()
        return result[0] if result else 0

    def trades(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        batch_size: int = 50_000,
    ) -> Iterator[TradeEvent]:
        """Yield normalized Kalshi trades in chronological order."""
        con = duckdb.connect()
        where_sql = self._where_sql(market_ids, start_time, end_time)

        result = con.execute(
            f"""
            SELECT trade_id, ticker, count, yes_price, no_price,
                   taker_side, created_time
            FROM '{self.trades_dir}/*.parquet'
            WHERE {where_sql}
            ORDER BY created_time, trade_id
            """
        )

        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break

            for trade_id, ticker, count, yes_price, no_price, taker_side, created_time in rows:
                yield TradeEvent(
                    timestamp=created_time,
                    market_id=ticker,
                    platform=Platform.KALSHI,
                    yes_price=yes_price / 100.0,
                    no_price=no_price / 100.0,
                    quantity=float(count),
                    taker_side=Side.YES if taker_side == "yes" else Side.NO,
                    raw_id=trade_id,
                )
