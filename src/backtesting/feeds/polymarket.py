"""Polymarket data feed — loads blockchain trades and markets from parquet files."""

from __future__ import annotations

import json
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


class PolymarketFeed(BaseFeed):
    """Data feed for Polymarket CTF Exchange trades.

    Handles the complexity of blockchain trade data: joining trades with
    block timestamps, mapping token IDs to markets, and computing prices
    from maker/taker amounts.
    """

    def __init__(
        self,
        trades_dir: Path | str | None = None,
        markets_dir: Path | str | None = None,
        blocks_dir: Path | str | None = None,
    ):
        base_dir = Path(__file__).parent.parent.parent.parent
        data_dir = base_dir / "data"
        if not data_dir.exists():
            data_dir = base_dir / "prediction-market-analysis" / "data"
        self.trades_dir = Path(trades_dir or data_dir / "polymarket" / "trades")
        self.markets_dir = Path(markets_dir or data_dir / "polymarket" / "markets")
        self.blocks_dir = Path(blocks_dir or data_dir / "polymarket" / "blocks")
        self._markets: dict[str, MarketInfo] | None = None
        self._token_to_market: dict[str, tuple[str, int]] | None = None

    def markets(self) -> dict[str, MarketInfo]:
        """Load Polymarket markets, resolving outcomes from final prices."""
        if self._markets is not None:
            return self._markets

        con = duckdb.connect()
        rows = con.execute(
            f"""
            SELECT id, condition_id, question, clob_token_ids,
                   outcome_prices, active, closed, end_date, created_at
            FROM '{self.markets_dir}/*.parquet'
            """
        ).fetchall()

        self._markets = {}
        for mid, _condition_id, question, clob_token_ids, outcome_prices, _active, closed, end_date, created_at in rows:
            result_side = self._resolve_outcome(outcome_prices, closed)

            if result_side is not None:
                status = MarketStatus.RESOLVED_YES if result_side == Side.YES else MarketStatus.RESOLVED_NO
            elif closed:
                status = MarketStatus.CLOSED
            else:
                status = MarketStatus.OPEN

            token_map = self._parse_token_map(clob_token_ids)

            self._markets[mid] = MarketInfo(
                market_id=mid,
                platform=Platform.POLYMARKET,
                title=question or "",
                open_time=created_at,
                close_time=end_date,
                result=result_side,
                status=status,
                token_id_map=token_map,
            )

        return self._markets

    def _filter_sql(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> tuple[str, str]:
        """Build shared WHERE fragments for trade queries."""
        time_parts: list[str] = []
        if start_time:
            time_parts.append(f"b.timestamp >= '{start_time.isoformat()}'")
        if end_time:
            time_parts.append(f"b.timestamp <= '{end_time.isoformat()}'")
        time_sql = " AND ".join(time_parts) if time_parts else "1=1"

        market_sql = ""
        if market_ids:
            ids_str = ", ".join(f"'{m}'" for m in market_ids)
            market_sql = f"AND tm.market_id IN ({ids_str})"

        return time_sql, market_sql

    def _setup_token_map_table(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create the in-memory token map table for query joins."""
        if self._token_to_market is None:
            self._build_token_map()
        if not self._token_to_market:
            return
        con.execute("CREATE TABLE _token_map (token_id VARCHAR, market_id VARCHAR, outcome_idx INTEGER)")
        records = [(tid, mid, oidx) for tid, (mid, oidx) in self._token_to_market.items()]
        con.executemany("INSERT INTO _token_map VALUES (?, ?, ?)", records)

    def trade_count(
        self,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> int:
        """Return total number of Polymarket trades matching filters."""
        if self._token_to_market is None:
            self._build_token_map()
        if not self._token_to_market:
            return 0

        con = duckdb.connect()
        self._setup_token_map_table(con)
        time_sql, market_sql = self._filter_sql(market_ids, start_time, end_time)

        result = con.execute(
            f"""
            SELECT COUNT(*)
            FROM '{self.trades_dir}/*.parquet' t
            JOIN '{self.blocks_dir}/*.parquet' b ON t.block_number = b.block_number
            JOIN _token_map tm ON (
                CASE
                    WHEN t.maker_asset_id::VARCHAR = '0'
                    THEN t.taker_asset_id::VARCHAR
                    ELSE t.maker_asset_id::VARCHAR
                END = tm.token_id
            )
            WHERE t.taker_amount > 0
              AND t.maker_amount > 0
              AND {time_sql}
              {market_sql}
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
        """Yield normalized Polymarket trades in chronological order.

        Joins blockchain trades with block timestamps and token-to-market
        mappings. Computes prices from maker/taker amounts.
        """
        if self._token_to_market is None:
            self._build_token_map()

        if not self._token_to_market:
            return

        con = duckdb.connect()
        self._setup_token_map_table(con)
        time_sql, market_sql = self._filter_sql(market_ids, start_time, end_time)

        query = f"""
            SELECT
                b.timestamp AS ts,
                tm.market_id,
                tm.outcome_idx,
                CASE
                    WHEN t.maker_asset_id::VARCHAR = '0'
                    THEN t.maker_amount::DOUBLE / NULLIF(t.taker_amount::DOUBLE, 0)
                    ELSE t.taker_amount::DOUBLE / NULLIF(t.maker_amount::DOUBLE, 0)
                END AS price,
                CASE
                    WHEN t.maker_asset_id::VARCHAR = '0'
                    THEN t.taker_amount::DOUBLE / 1e6
                    ELSE t.maker_amount::DOUBLE / 1e6
                END AS quantity,
                t.transaction_hash
            FROM '{self.trades_dir}/*.parquet' t
            JOIN '{self.blocks_dir}/*.parquet' b ON t.block_number = b.block_number
            JOIN _token_map tm ON (
                CASE
                    WHEN t.maker_asset_id::VARCHAR = '0'
                    THEN t.taker_asset_id::VARCHAR
                    ELSE t.maker_asset_id::VARCHAR
                END = tm.token_id
            )
            WHERE t.taker_amount > 0
              AND t.maker_amount > 0
              AND {time_sql}
              {market_sql}
            ORDER BY b.timestamp, t.block_number, t.log_index
        """

        result = con.execute(query)

        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break

            for ts, market_id, outcome_idx, price, quantity, tx_hash in rows:
                if price is None or price <= 0:
                    continue

                timestamp = self._parse_timestamp(ts)
                if timestamp is None:
                    continue

                if outcome_idx == 0:
                    yes_price = min(max(price, 0.0), 1.0)
                    taker_side = Side.YES
                else:
                    yes_price = min(max(1.0 - price, 0.0), 1.0)
                    taker_side = Side.NO

                yield TradeEvent(
                    timestamp=timestamp,
                    market_id=market_id,
                    platform=Platform.POLYMARKET,
                    yes_price=yes_price,
                    no_price=1.0 - yes_price,
                    quantity=quantity,
                    taker_side=taker_side,
                    raw_id=tx_hash,
                )

    def _build_token_map(self) -> None:
        """Build token_id -> (market_id, outcome_index) mapping from market metadata."""
        self._token_to_market = {}
        for market_id, info in self.markets().items():
            if info.token_id_map:
                for token_id, outcome_idx in info.token_id_map.items():
                    self._token_to_market[token_id] = (market_id, outcome_idx)

    @staticmethod
    def _resolve_outcome(outcome_prices: str | None, closed: bool) -> Side | None:
        """Determine market result from final outcome prices."""
        if not closed or not outcome_prices:
            return None
        try:
            prices = json.loads(outcome_prices)
            if len(prices) >= 2:
                p0, p1 = float(prices[0]), float(prices[1])
                if p0 > 0.99 and p1 < 0.01:
                    return Side.YES
                if p0 < 0.01 and p1 > 0.99:
                    return Side.NO
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
        return None

    @staticmethod
    def _parse_token_map(clob_token_ids: str | None) -> dict[str, int] | None:
        """Parse clob_token_ids JSON into a token_id -> outcome_index map."""
        if not clob_token_ids:
            return None
        try:
            tokens = json.loads(clob_token_ids)
            if isinstance(tokens, list) and len(tokens) == 2:
                return {str(tokens[0]): 0, str(tokens[1]): 1}
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
        return None

    @staticmethod
    def _parse_timestamp(ts: object) -> datetime | None:
        """Parse a timestamp from DuckDB which may be str or datetime."""
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None
