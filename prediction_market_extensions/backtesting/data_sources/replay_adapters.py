from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from typing import Any

import pandas as pd
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.model.currencies import USD, USDC_POS
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType, BookType, OmsType
from nautilus_trader.model.identifiers import Venue

from prediction_market_extensions.adapters.kalshi.fee_model import KalshiProportionalFeeModel
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
)
from prediction_market_extensions.backtesting._backtest_runtime import _record_timestamp_ns
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay, TradeReplay
from prediction_market_extensions.backtesting.data_sources.kalshi_native import (
    RunnerKalshiDataLoader as KalshiDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.kalshi_native import (
    configured_kalshi_native_data_source,
)
from prediction_market_extensions.backtesting.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader as PolymarketPMXTDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.pmxt import configured_pmxt_data_source
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    RunnerPolymarketDataLoader as PolymarketDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    configured_polymarket_native_data_source,
)
from prediction_market_extensions.backtesting.data_sources.telonex import (
    RunnerPolymarketTelonexQuoteDataLoader as PolymarketTelonexQuoteDataLoader,
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


class KalshiTradeTickReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("kalshi", "native", "trade_tick"),
            _replay_spec_type=TradeReplay,
            _configure_sources_fn=configured_kalshi_native_data_source,
            _engine_profile=ReplayEngineProfile(
                venue=Venue("KALSHI"),
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USD,
                fee_model_factory=KalshiProportionalFeeModel,
            ),
            _single_market_required_fields=("market_ticker",),
            _single_market_forwarded_fields=(
                "market_ticker",
                "lookback_days",
                "start_time",
                "end_time",
                "outcome",
                "metadata",
            ),
            _single_market_replay_factory=lambda fields: TradeReplay(
                market_ticker=str(fields["market_ticker"]),
                lookback_days=fields.get("lookback_days"),
                start_time=fields.get("start_time"),
                end_time=fields.get("end_time"),
                outcome=fields.get("outcome"),
                metadata=fields.get("metadata"),
            ),
        )

    async def load_replay(
        self, replay: TradeReplay, *, request: ReplayLoadRequest
    ) -> LoadedReplay | None:
        end = _normalize_timestamp(
            replay.end_time if replay.end_time is not None else request.default_end_time,
            default_now=True,
        )
        if replay.start_time is not None:
            start = _normalize_timestamp(replay.start_time)
        else:
            lookback_days = (
                replay.lookback_days
                if replay.lookback_days is not None
                else request.default_lookback_days
            )
            if lookback_days is None:
                raise ValueError("lookback_days or start_time is required for Kalshi replays.")
            start = end - pd.Timedelta(days=float(lookback_days))

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )

        print(
            f"Loading Kalshi market {replay.market_ticker} "
            f"(window_start={start.isoformat()}, window_end={end.isoformat()})..."
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol("KalshiDataLoader", KalshiDataLoader)
            loader = await loader_cls.from_market_ticker(replay.market_ticker)
            trades = tuple(await loader.load_trades(start, end))
        except Exception as exc:
            print(f"Skip {replay.market_ticker}: unable to load trades ({exc})")
            return None

        if not trades:
            print(f"Skip {replay.market_ticker}: no trades returned")
            return None

        prices = tuple(float(trade.price) for trade in trades)
        if not _validate_replay_window(
            market_label=replay.market_ticker,
            count_label="trades",
            count=len(trades),
            min_record_count=request.min_record_count,
            prices=prices,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=trades,
            count=len(trades),
            count_key="trades",
            market_key="ticker",
            market_id=replay.market_ticker,
            prices=prices,
            outcome=str(replay.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


class PolymarketTradeTickReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("polymarket", "native", "trade_tick"),
            _replay_spec_type=TradeReplay,
            _configure_sources_fn=configured_polymarket_native_data_source,
            _engine_profile=ReplayEngineProfile(
                venue=POLYMARKET_VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USDC_POS,
                fee_model_factory=PolymarketFeeModel,
            ),
            _single_market_required_fields=("market_slug",),
            _single_market_forwarded_fields=(
                "market_slug",
                "token_index",
                "lookback_days",
                "start_time",
                "end_time",
                "outcome",
                "metadata",
            ),
            _single_market_replay_factory=lambda fields: TradeReplay(
                market_slug=str(fields["market_slug"]),
                token_index=int(fields.get("token_index", 0)),
                lookback_days=fields.get("lookback_days"),
                start_time=fields.get("start_time"),
                end_time=fields.get("end_time"),
                outcome=fields.get("outcome"),
                metadata=fields.get("metadata"),
            ),
        )

    async def load_replay(
        self, replay: TradeReplay, *, request: ReplayLoadRequest
    ) -> LoadedReplay | None:
        end = _normalize_timestamp(
            replay.end_time if replay.end_time is not None else request.default_end_time,
            default_now=True,
        )
        if replay.start_time is not None:
            start = _normalize_timestamp(replay.start_time)
        else:
            lookback_days = (
                replay.lookback_days
                if replay.lookback_days is not None
                else request.default_lookback_days
            )
            if lookback_days is None:
                raise ValueError(
                    "lookback_days or start_time is required for Polymarket trade-tick replays."
                )
            start = end - pd.Timedelta(days=float(lookback_days))

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )

        print(
            f"Loading Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})..."
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol(
                "PolymarketDataLoader", PolymarketDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            trades = tuple(await loader.load_trades(start, end))
        except Exception as exc:
            print(f"Skip {replay.market_slug}: unable to load trades ({exc})")
            return None

        if not trades:
            print(f"Skip {replay.market_slug}: no trades returned")
            return None

        prices = tuple(float(trade.price) for trade in trades)
        if not _validate_replay_window(
            market_label=replay.market_slug,
            count_label="trades",
            count=len(trades),
            min_record_count=request.min_record_count,
            prices=prices,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=trades,
            count=len(trades),
            count_key="trades",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices,
            outcome=str(loader.instrument.outcome or replay.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


class PolymarketPMXTQuoteReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("polymarket", "pmxt", "quote_tick"),
            _replay_spec_type=QuoteReplay,
            _configure_sources_fn=configured_pmxt_data_source,
            _engine_profile=ReplayEngineProfile(
                venue=POLYMARKET_VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USDC_POS,
                fee_model_factory=PolymarketFeeModel,
                fill_model_mode="passive_book",
                book_type=BookType.L2_MBP,
                liquidity_consumption=True,
            ),
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
            _single_market_replay_factory=lambda fields: QuoteReplay(
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
        self, replay: QuoteReplay, *, request: ReplayLoadRequest
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
                    "start_time/end_time or lookback_hours is required for PMXT quote replays."
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
            print(f"Skip {replay.market_slug}: unable to load PMXT quotes ({exc})")
            return None

        if not records:
            print(f"Skip {replay.market_slug}: no PMXT records returned")
            return None

        prices: list[float] = []
        quote_count = 0
        quote_tick_type = _resolve_backtest_compat_symbol("QuoteTick", QuoteTick)
        for record in records:
            if not isinstance(record, quote_tick_type):
                continue
            quote_count += 1
            prices.append((float(record.bid_price) + float(record.ask_price)) / 2.0)

        prices_tuple = tuple(prices)
        if not _validate_replay_window(
            market_label=replay.market_slug,
            count_label="quotes",
            count=quote_count,
            min_record_count=request.min_record_count,
            prices=prices_tuple,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=records,
            count=quote_count,
            count_key="quotes",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices_tuple,
            outcome=str(loader.instrument.outcome or replay.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


class PolymarketTelonexQuoteReplayAdapter(_BaseReplayAdapter):
    def __init__(self) -> None:
        super().__init__(
            _key=ReplayAdapterKey("polymarket", "telonex", "quote_tick"),
            _replay_spec_type=QuoteReplay,
            _configure_sources_fn=lambda *, sources: configured_telonex_data_source(
                sources=sources,
                channel=TELONEX_FULL_BOOK_CHANNEL,
            ),
            _engine_profile=ReplayEngineProfile(
                venue=POLYMARKET_VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USDC_POS,
                fee_model_factory=PolymarketFeeModel,
                fill_model_mode="passive_book",
                book_type=BookType.L2_MBP,
                liquidity_consumption=True,
            ),
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
            _single_market_replay_factory=lambda fields: QuoteReplay(
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
        self, replay: QuoteReplay, *, request: ReplayLoadRequest
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
                    "start_time/end_time or lookback_hours is required for Telonex quote replays."
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
                "PolymarketTelonexQuoteDataLoader", PolymarketTelonexQuoteDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            records = tuple(
                loader.load_order_book_and_quotes(
                    start,
                    end,
                    market_slug=replay.market_slug,
                    token_index=replay.token_index,
                    outcome=replay.outcome,
                )
            )
        except Exception as exc:
            print(f"Skip {replay.market_slug}: unable to load Telonex L2 book data ({exc})")
            return None

        if not records:
            print(f"Skip {replay.market_slug}: no Telonex L2 book data returned")
            return None

        quote_tick_type = _resolve_backtest_compat_symbol("QuoteTick", QuoteTick)
        prices: list[float] = []
        quote_count = 0
        for record in records:
            if not isinstance(record, quote_tick_type):
                continue
            quote_count += 1
            prices.append((float(record.bid_price) + float(record.ask_price)) / 2.0)

        prices_tuple = tuple(prices)
        if not _validate_replay_window(
            market_label=replay.market_slug,
            count_label="quotes",
            count=quote_count,
            min_record_count=request.min_record_count,
            prices=prices_tuple,
            min_price_range=request.min_price_range,
        ):
            return None

        return self._build_loaded_replay(
            replay=replay,
            instrument=loader.instrument,
            records=records,
            count=quote_count,
            count_key="quotes",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices_tuple,
            outcome=str(loader.instrument.outcome or replay.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(start, end),
        )


BUILTIN_REPLAY_ADAPTERS: tuple[HistoricalReplayAdapter, ...] = (
    KalshiTradeTickReplayAdapter(),
    PolymarketTradeTickReplayAdapter(),
    PolymarketPMXTQuoteReplayAdapter(),
    PolymarketTelonexQuoteReplayAdapter(),
)


__all__ = ["BUILTIN_REPLAY_ADAPTERS"]
