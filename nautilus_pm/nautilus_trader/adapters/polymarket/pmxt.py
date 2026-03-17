# Added by Evan Kolberg to the NautilusTrader-derived subtree on 2026-03-15.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from datetime import UTC

import fsspec
import msgspec
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from nautilus_trader.adapters.polymarket.common.enums import PolymarketOrderSide
from nautilus_trader.adapters.polymarket.loaders import PolymarketDataLoader
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookLevel
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketBookSnapshot
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuote
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketQuotes
from nautilus_trader.core.datetime import nanos_to_secs
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BookType


class _PMXTBookSnapshotPayload(msgspec.Struct, frozen=True):
    update_type: str
    market_id: str
    token_id: str
    side: str
    best_bid: str
    best_ask: str
    timestamp: float
    bids: list[list[str]]
    asks: list[list[str]]


class _PMXTPriceChangePayload(msgspec.Struct, frozen=True):
    update_type: str
    market_id: str
    token_id: str
    side: str
    best_bid: str
    best_ask: str
    timestamp: float
    change_price: str
    change_size: str
    change_side: str


class PolymarketPMXTDataLoader(PolymarketDataLoader):
    """
    Historical Polymarket L2 loader backed by the PMXT hourly archive.

    The PMXT archive stores one parquet file per UTC hour. Each row contains a
    market-scoped order-book event payload encoded as JSON. This loader filters
    to one market ID at parquet-scan time, then filters to the target token in
    Python and converts the payloads into Nautilus `OrderBookDeltas` and
    `QuoteTick` records.
    """

    _PMXT_BASE_URL = "https://r2.pmxt.dev"
    _PMXT_COLUMNS = [
        "timestamp_received",
        "market_id",
        "update_type",
        "data",
    ]

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._pmxt_fs = pafs.PyFileSystem(
            pafs.FSSpecHandler(fsspec.filesystem("https")),
        )

    @staticmethod
    def _normalize_timestamp(value: pd.Timestamp | str | None) -> pd.Timestamp | None:
        if value is None:
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(UTC)
        return ts.tz_convert(UTC)

    @staticmethod
    def _archive_hours(
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[pd.Timestamp]:
        cursor = start.floor("h") - pd.Timedelta(hours=1)
        final_hour = end.floor("h")
        hours: list[pd.Timestamp] = []
        while cursor <= final_hour:
            hours.append(cursor)
            cursor += pd.Timedelta(hours=1)
        return hours

    @classmethod
    def _archive_url_for_hour(cls, hour: pd.Timestamp) -> str:
        ts = hour.tz_convert(UTC)
        return (
            f"{cls._PMXT_BASE_URL}/polymarket_orderbook_"
            f"{ts.strftime('%Y-%m-%dT%H')}.parquet"
        )

    @staticmethod
    def _timestamp_to_ms_string(timestamp_secs: float) -> str:
        return f"{timestamp_secs * 1000:.6f}"

    @staticmethod
    def _quote_from_book(
        *,
        instrument,
        local_book: OrderBook,
        ts_event_ns: int,
    ) -> QuoteTick | None:
        bid_price = local_book.best_bid_price()
        ask_price = local_book.best_ask_price()
        bid_size = local_book.best_bid_size()
        ask_size = local_book.best_ask_size()
        if bid_price is None or ask_price is None or bid_size is None or ask_size is None:
            return None

        return QuoteTick(
            instrument_id=instrument.id,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size,
            ask_size=ask_size,
            ts_event=ts_event_ns,
            # Process the quote immediately after the associated book update.
            ts_init=ts_event_ns + 1,
        )

    @staticmethod
    def _decode_book_snapshot(payload_text: str) -> _PMXTBookSnapshotPayload:
        return msgspec.json.decode(
            payload_text.encode("utf-8"),
            type=_PMXTBookSnapshotPayload,
        )

    @staticmethod
    def _decode_price_change(payload_text: str) -> _PMXTPriceChangePayload:
        return msgspec.json.decode(
            payload_text.encode("utf-8"),
            type=_PMXTPriceChangePayload,
        )

    @staticmethod
    def _to_book_snapshot(payload: _PMXTBookSnapshotPayload) -> PolymarketBookSnapshot:
        return PolymarketBookSnapshot(
            market=payload.market_id,
            asset_id=payload.token_id,
            bids=[
                PolymarketBookLevel(price=price, size=size)
                for price, size in payload.bids
            ],
            asks=[
                PolymarketBookLevel(price=price, size=size)
                for price, size in payload.asks
            ],
            timestamp=PolymarketPMXTDataLoader._timestamp_to_ms_string(payload.timestamp),
        )

    @staticmethod
    def _to_price_change(payload: _PMXTPriceChangePayload) -> PolymarketQuotes:
        side = PolymarketOrderSide(payload.change_side)
        return PolymarketQuotes(
            market=payload.market_id,
            price_changes=[
                PolymarketQuote(
                    asset_id=payload.token_id,
                    price=payload.change_price,
                    side=side,
                    size=payload.change_size,
                    hash=(
                        f"pmxt:{payload.market_id}:{payload.token_id}:"
                        f"{payload.timestamp:.6f}:{payload.change_side}:{payload.change_price}"
                    ),
                    best_bid=payload.best_bid,
                    best_ask=payload.best_ask,
                ),
            ],
            timestamp=PolymarketPMXTDataLoader._timestamp_to_ms_string(payload.timestamp),
        )

    def load_order_book_and_quotes(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        batch_size: int = 25_000,
        include_order_book: bool = True,
        include_quotes: bool = True,
    ) -> list[OrderBookDeltas | QuoteTick]:
        """
        Load one market's historical L2 updates from PMXT.

        Only the target token's rows are materialized in memory; each parquet file
        is filtered by market ID during scan and discarded once processed.
        """
        if self.condition_id is None:
            raise ValueError("condition_id is required for PMXT loading")
        if self.token_id is None:
            raise ValueError("token_id is required for PMXT loading")

        start_ts = self._normalize_timestamp(start)
        end_ts = self._normalize_timestamp(end)
        if start_ts is None or end_ts is None or end_ts <= start_ts:
            return []

        token_id = self.token_id
        instrument = self.instrument
        local_book = OrderBook(instrument.id, book_type=BookType.L2_MBP)
        has_snapshot = False
        events: list[OrderBookDeltas | QuoteTick] = []

        for hour in self._archive_hours(start_ts, end_ts):
            archive_url = self._archive_url_for_hour(hour)
            try:
                dataset = ds.dataset(
                    archive_url,
                    filesystem=self._pmxt_fs,
                    format="parquet",
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                if "404" in str(exc):
                    continue
                raise

            scanner = dataset.scanner(
                columns=self._PMXT_COLUMNS,
                filter=ds.field("market_id") == self.condition_id,
                batch_size=batch_size,
            )

            for batch in scanner.to_batches():
                for row in batch.to_pylist():
                    payload_text = str(row["data"])
                    update_type = str(row["update_type"])

                    if update_type == "book_snapshot":
                        payload = self._decode_book_snapshot(payload_text)
                        if payload.token_id != token_id:
                            continue

                        snapshot = self._to_book_snapshot(payload)
                        deltas = snapshot.parse_to_snapshot(
                            instrument=instrument,
                            ts_init=int(payload.timestamp * 1_000_000_000),
                        )
                        if deltas is None:
                            continue

                        local_book = OrderBook(instrument.id, book_type=BookType.L2_MBP)
                        local_book.apply_deltas(deltas)
                        has_snapshot = True
                        event_ts = pd.Timestamp(
                            nanos_to_secs(deltas.ts_event),
                            unit="s",
                            tz=UTC,
                        )
                        if event_ts < start_ts or event_ts > end_ts:
                            continue

                        if include_order_book:
                            events.append(deltas)
                        if include_quotes:
                            quote = snapshot.parse_to_quote(
                                instrument=instrument,
                                ts_init=deltas.ts_event + 1,
                            )
                            if quote is not None:
                                events.append(quote)
                        continue

                    if update_type != "price_change" or not has_snapshot:
                        continue

                    payload = self._decode_price_change(payload_text)
                    if payload.token_id != token_id:
                        continue

                    quotes = self._to_price_change(payload)
                    deltas = quotes.parse_to_deltas(
                        instrument=instrument,
                        ts_init=int(payload.timestamp * 1_000_000_000),
                    )
                    local_book.apply_deltas(deltas)

                    event_ts = pd.Timestamp(payload.timestamp, unit="s", tz=UTC)
                    if event_ts < start_ts or event_ts > end_ts:
                        continue

                    if include_order_book:
                        events.append(deltas)
                    if include_quotes:
                        quote = self._quote_from_book(
                            instrument=instrument,
                            local_book=local_book,
                            ts_event_ns=deltas.ts_event,
                        )
                        if quote is not None:
                            events.append(quote)

        events.sort(key=lambda record: int(getattr(record, "ts_init", getattr(record, "ts_event", 0))))
        return events
