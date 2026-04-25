from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from typing import Any

import pandas as pd
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import OrderBookDeltas, QuoteTick
from nautilus_trader.model.enums import AccountType, BookType, OmsType

from prediction_market_extensions.adapters.polymarket.fee_model import PolymarketFeeModel
from prediction_market_extensions.adapters.prediction_market import (
    HistoricalReplayAdapter,
    LoadedReplay,
    ReplayAdapterKey,
    ReplayCoverageStats,
    ReplayEngineProfile,
    ReplayLoadRequest,
    ReplayWindow,
)
from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    infer_realized_outcome,
    infer_realized_outcome_from_metadata,
)
from prediction_market_extensions.backtesting._backtest_runtime import _record_timestamp_ns
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader as PolymarketPMXTDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.pmxt import configured_pmxt_data_source
from prediction_market_extensions.backtesting.data_sources.telonex import (
    RunnerPolymarketTelonexBookDataLoader as PolymarketTelonexBookDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_FULL_BOOK_CHANNEL,
    configured_telonex_data_source,
)


def _resolve_backtest_compat_symbol(name: str, default: Any) -> Any:
    try:
        module = import_module(
            "prediction_market_extensions.backtesting._prediction_market_backtest"
        )
    except Exception:
        return default
    return getattr(module, name, default)


def _loader_realized_outcome(loader: Any) -> float | None:
    metadata = getattr(loader, "resolution_metadata", None)
    if metadata:
        outcome_name = str(getattr(loader.instrument, "outcome", "") or "")
        return infer_realized_outcome_from_metadata(metadata, outcome_name)
    return infer_realized_outcome(loader.instrument)


def _normalize_timestamp(value: object | None, *, default_now: bool = False) -> pd.Timestamp:
    if value is None:
        if not default_now:
            raise ValueError("timestamp is required")
        value = datetime.now(UTC)

    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        if not default_now:
            raise ValueError("timestamp is required")
        timestamp = pd.Timestamp(datetime.now(UTC))
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(UTC)
    return timestamp.tz_convert(UTC)


def _loaded_window(records: tuple[object, ...]) -> ReplayWindow | None:
    start_ns: int | None = None
    end_ns: int | None = None
    for record in records:
        timestamp_ns = _record_timestamp_ns(record)
        if timestamp_ns is None:
            continue
        if start_ns is None or timestamp_ns < start_ns:
            start_ns = timestamp_ns
        if end_ns is None or timestamp_ns > end_ns:
            end_ns = timestamp_ns
    if start_ns is None and end_ns is None:
        return None
    return ReplayWindow(start_ns=start_ns, end_ns=end_ns)


def _requested_window(start: pd.Timestamp, end: pd.Timestamp) -> ReplayWindow:
    return ReplayWindow(start_ns=int(start.value), end_ns=int(end.value))


def _price_range(prices: tuple[float, ...]) -> float:
    if not prices:
        return 0.0
    return max(prices) - min(prices)


def _validate_replay_window(
    *,
    market_label: str,
    count_label: str,
    count: int,
    min_record_count: int,
    prices: tuple[float, ...],
    min_price_range: float,
) -> bool:
    if count < min_record_count:
        print(f"Skip {market_label}: {count} {count_label} < {min_record_count} required")
        return False
    if prices and _price_range(prices) < min_price_range:
        print(
            f"Skip {market_label}: price range {_price_range(prices):.3f} < {min_price_range:.3f}"
        )
        return False
    return True


L2_BOOK_ENGINE_PROFILE = ReplayEngineProfile(
    venue=POLYMARKET_VENUE,
    oms_type=OmsType.NETTING,
    account_type=AccountType.CASH,
    base_currency=USDC_POS,
    fee_model_factory=PolymarketFeeModel,
    fill_model_mode="passive_book",
    book_type=BookType.L2_MBP,
    liquidity_consumption=True,
)


@dataclass(frozen=True)
class _BaseReplayAdapter(HistoricalReplayAdapter):
    _key: ReplayAdapterKey
    _replay_spec_type: type[Any]
    _configure_sources_fn: Callable[..., AbstractContextManager[Any]]
    _engine_profile: ReplayEngineProfile
    _single_market_required_fields: tuple[str, ...]
    _single_market_forwarded_fields: tuple[str, ...]
    _single_market_replay_factory: Callable[[Mapping[str, Any]], Any]

    @property
    def key(self) -> ReplayAdapterKey:
        return self._key

    @property
    def replay_spec_type(self) -> type[Any]:
        return self._replay_spec_type

    def configure_sources(
        self, *, sources: tuple[str, ...] | list[str]
    ) -> AbstractContextManager[Any]:
        return self._configure_sources_fn(sources=sources)

    @property
    def engine_profile(self) -> ReplayEngineProfile:
        return self._engine_profile

    def build_single_market_replay(self, *, field_values: Mapping[str, Any]) -> Any:
        for field_name in self._single_market_required_fields:
            if field_values.get(field_name) is None:
                raise ValueError(f"{field_name} is required for this backtest selection.")

        replay_fields: dict[str, Any] = {}
        for field_name in self._single_market_forwarded_fields:
            value = field_values.get(field_name)
            if value is not None:
                replay_fields[field_name] = value
        return self._single_market_replay_factory(replay_fields)

    def _build_loaded_replay(
        self,
        *,
        replay: Any,
        instrument: Any,
        records: tuple[Any, ...],
        count: int,
        count_key: str,
        market_key: str,
        market_id: str,
        prices: tuple[float, ...],
        outcome: str,
        realized_outcome: float | None,
        metadata: dict[str, Any],
        requested_window: ReplayWindow,
    ) -> LoadedReplay:
        return LoadedReplay(
            replay=replay,
            instrument=instrument,
            records=records,
            outcome=outcome,
            realized_outcome=realized_outcome,
            metadata=metadata,
            requested_window=requested_window,
            loaded_window=_loaded_window(records),
            coverage_stats=ReplayCoverageStats(
                count=count,
                count_key=count_key,
                market_key=market_key,
                market_id=market_id,
                prices=prices,
            ),
            instrument_ids=(instrument.id,),
        )


class PolymarketPMXTBookReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("polymarket", "pmxt", "book"),
            _replay_spec_type=BookReplay,
            _configure_sources_fn=configured_pmxt_data_source,
            _engine_profile=L2_BOOK_ENGINE_PROFILE,
            _single_market_required_fields=("market_slug",),
            _single_market_forwarded_fields=(
                "market_slug",
                "token_index",
                "lookback_hours",
                "start_time",
                "end_time",
                "outcome",
                "metadata",
            ),
            _single_market_replay_factory=lambda fields: BookReplay(
                market_slug=str(fields["market_slug"]),
                token_index=int(fields.get("token_index", 0)),
                lookback_hours=fields.get("lookback_hours"),
                start_time=fields.get("start_time"),
                end_time=fields.get("end_time"),
                outcome=fields.get("outcome"),
                metadata=fields.get("metadata"),
            ),
        )

    async def load_replay(
        self, replay: BookReplay, *, request: ReplayLoadRequest
    ) -> LoadedReplay | None:
        end = _normalize_timestamp(
            replay.end_time if replay.end_time is not None else request.default_end_time,
            default_now=True,
        )
        if replay.start_time is not None:
            start = _normalize_timestamp(replay.start_time)
        else:
            lookback_hours = (
                replay.lookback_hours
                if replay.lookback_hours is not None
                else request.default_lookback_hours
            )
            if lookback_hours is None:
                raise ValueError(
                    "start_time/end_time or lookback_hours is required for PMXT book replays."
                )
            start = end - pd.Timedelta(hours=float(lookback_hours))

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )

        print(
            f"Loading PMXT Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})..."
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol(
                "PolymarketPMXTDataLoader", PolymarketPMXTDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            records = tuple(loader.load_order_book_and_quotes(start, end))
        except Exception as exc:
            print(f"Skip {replay.market_slug}: unable to load PMXT L2 book data ({exc})")
            return None

        if not records:
            print(f"Skip {replay.market_slug}: no PMXT L2 book data returned")
            return None

        prices: list[float] = []
        book_event_count = 0
        quote_tick_type = _resolve_backtest_compat_symbol("QuoteTick", QuoteTick)
        deltas_type = _resolve_backtest_compat_symbol("OrderBookDeltas", OrderBookDeltas)
        for record in records:
            if isinstance(record, deltas_type):
                book_event_count += 1
            if isinstance(record, quote_tick_type):
                prices.append((float(record.bid_price) + float(record.ask_price)) / 2.0)

        prices_tuple = tuple(prices)
        if not _validate_replay_window(
            market_label=replay.market_slug,
            count_label="book events",
            count=book_event_count,
            min_record_count=request.min_record_count,
            prices=prices_tuple,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=records,
            count=book_event_count,
            count_key="book_events",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices_tuple,
            outcome=str(loader.instrument.outcome or replay.outcome or ""),
            realized_outcome=_loader_realized_outcome(loader),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


class PolymarketTelonexBookReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("polymarket", "telonex", "book"),
            _replay_spec_type=BookReplay,
            _configure_sources_fn=lambda *, sources: configured_telonex_data_source(
                sources=sources,
                channel=TELONEX_FULL_BOOK_CHANNEL,
            ),
            _engine_profile=L2_BOOK_ENGINE_PROFILE,
            _single_market_required_fields=("market_slug",),
            _single_market_forwarded_fields=(
                "market_slug",
                "token_index",
                "lookback_hours",
                "start_time",
                "end_time",
                "outcome",
                "metadata",
            ),
            _single_market_replay_factory=lambda fields: BookReplay(
                market_slug=str(fields["market_slug"]),
                token_index=int(fields.get("token_index", 0)),
                lookback_hours=fields.get("lookback_hours"),
                start_time=fields.get("start_time"),
                end_time=fields.get("end_time"),
                outcome=fields.get("outcome"),
                metadata=fields.get("metadata"),
            ),
        )

    async def load_replay(
        self, replay: BookReplay, *, request: ReplayLoadRequest
    ) -> LoadedReplay | None:
        end = _normalize_timestamp(
            replay.end_time if replay.end_time is not None else request.default_end_time,
            default_now=True,
        )
        if replay.start_time is not None:
            start = _normalize_timestamp(replay.start_time)
        else:
            lookback_hours = (
                replay.lookback_hours
                if replay.lookback_hours is not None
                else request.default_lookback_hours
            )
            if lookback_hours is None:
                raise ValueError(
                    "start_time/end_time or lookback_hours is required for Telonex book replays."
                )
            start = end - pd.Timedelta(hours=float(lookback_hours))

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )

        print(
            f"Loading Telonex Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})..."
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol(
                "PolymarketTelonexBookDataLoader", PolymarketTelonexBookDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            selected_outcome = str(loader.instrument.outcome or replay.outcome or "")
            records = tuple(
                loader.load_order_book_and_quotes(
                    start,
                    end,
                    market_slug=replay.market_slug,
                    token_index=replay.token_index,
                    outcome=selected_outcome or None,
                )
            )
        except Exception as exc:
            print(f"Skip {replay.market_slug}: unable to load Telonex L2 book data ({exc})")
            return None

        if not records:
            print(f"Skip {replay.market_slug}: no Telonex L2 book data returned")
            return None

        quote_tick_type = _resolve_backtest_compat_symbol("QuoteTick", QuoteTick)
        deltas_type = _resolve_backtest_compat_symbol("OrderBookDeltas", OrderBookDeltas)
        prices: list[float] = []
        book_event_count = 0
        for record in records:
            if isinstance(record, deltas_type):
                book_event_count += 1
            if isinstance(record, quote_tick_type):
                prices.append((float(record.bid_price) + float(record.ask_price)) / 2.0)

        prices_tuple = tuple(prices)
        if not _validate_replay_window(
            market_label=replay.market_slug,
            count_label="book events",
            count=book_event_count,
            min_record_count=request.min_record_count,
            prices=prices_tuple,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=records,
            count=book_event_count,
            count_key="book_events",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices_tuple,
            outcome=selected_outcome,
            realized_outcome=_loader_realized_outcome(loader),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


BUILTIN_REPLAY_ADAPTERS: tuple[HistoricalReplayAdapter, ...] = (
    PolymarketPMXTBookReplayAdapter(),
    PolymarketTelonexBookReplayAdapter(),
)


__all__ = ["BUILTIN_REPLAY_ADAPTERS", "L2_BOOK_ENGINE_PROFILE"]
