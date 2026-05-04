from __future__ import annotations

import asyncio
import os
import time
import warnings
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np
import pyarrow as pa
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import OrderBookDeltas, TradeTick
from nautilus_trader.model.enums import AccountType, BookType, OmsType

from prediction_market_extensions._native import replay_merge_plan
from prediction_market_extensions._native import source_days_for_window_ns
from prediction_market_extensions._runtime_log import emit_loader_event
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


def _best_book_midpoint(book: OrderBook) -> float | None:
    best_bid = book.best_bid_price()
    best_ask = book.best_ask_price()
    if best_bid is None or best_ask is None:
        return None
    return (float(best_bid) + float(best_ask)) / 2.0


def _book_event_count_and_midpoints(
    *, instrument: Any, records: tuple[object, ...], deltas_type: type[Any]
) -> tuple[int, tuple[float, ...]]:
    book = OrderBook(instrument.id, book_type=BookType.L2_MBP)
    book_event_count = 0
    prices: list[float] = []
    for record in records:
        if not isinstance(record, deltas_type):
            continue
        book_event_count += 1
        book.apply_deltas(record)
        midpoint = _best_book_midpoint(book)
        if midpoint is not None:
            prices.append(midpoint)
    return book_event_count, tuple(prices)


def _book_event_count(records: tuple[object, ...], *, deltas_type: type[Any]) -> int:
    return sum(1 for record in records if isinstance(record, deltas_type))


def _book_event_count_and_prices_for_request(
    *,
    instrument: Any,
    records: tuple[object, ...],
    deltas_type: type[Any],
    request: ReplayLoadRequest,
) -> tuple[int, tuple[float, ...]]:
    if request.min_price_range > 0.0:
        return _book_event_count_and_midpoints(
            instrument=instrument,
            records=records,
            deltas_type=deltas_type,
        )
    return _book_event_count(records, deltas_type=deltas_type), ()


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
        emit_loader_event(
            f"Skip {market_label}: {count} {count_label} < {min_record_count} required",
            level="WARNING",
            stage="validate",
            status="skip",
            rows=count,
        )
        return False
    if prices and _price_range(prices) < min_price_range:
        emit_loader_event(
            f"Skip {market_label}: price range {_price_range(prices):.3f} < {min_price_range:.3f}",
            level="WARNING",
            stage="validate",
            status="skip",
        )
        return False
    return True


def _cache_home() -> Path:
    configured = os.getenv("XDG_CACHE_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".cache"


def _trade_cache_path(*, loader: Any, date: pd.Timestamp) -> Path | None:
    condition_id = getattr(loader, "condition_id", None)
    token_id = getattr(loader, "token_id", None)
    if not condition_id or not token_id:
        return None
    return (
        _cache_home()
        / "nautilus_trader"
        / "polymarket_trades"
        / str(condition_id)
        / str(token_id)
        / f"{date.strftime('%Y-%m-%d')}.parquet"
    )


def _trade_record_sort_key(record: TradeTick) -> tuple[int, int]:
    return (int(record.ts_event), int(record.ts_init))


def _serialize_trade_ticks(trades: tuple[TradeTick, ...]) -> pd.DataFrame:
    rows = [
        {
            "price": float(trade.price),
            "size": float(trade.size),
            "aggressor_side": getattr(trade.aggressor_side, "name", str(trade.aggressor_side)),
            "trade_id": str(trade.trade_id),
            "ts_event": int(trade.ts_event),
            "ts_init": int(trade.ts_init),
        }
        for trade in trades
    ]
    return pd.DataFrame.from_records(rows)


def _trade_ticks_from_native_columns(
    *,
    loader: Any,
    data: tuple[list[float], list[float], list[int], list[str], list[int], list[int]],
) -> tuple[TradeTick, ...]:
    prices, sizes, aggressor_sides, trade_ids, ts_events, ts_inits = data
    instrument = loader.instrument
    return tuple(
        TradeTick.from_raw_arrays_to_list(
            instrument.id,
            int(instrument.price_precision),
            int(instrument.size_precision),
            _rounded_float64_array(prices, int(instrument.price_precision)),
            _rounded_float64_array(sizes, int(instrument.size_precision)),
            np.asarray(aggressor_sides, dtype=np.uint8),
            [str(value) for value in trade_ids],
            np.asarray(ts_events, dtype=np.uint64),
            np.asarray(ts_inits, dtype=np.uint64),
        )
    )


def _trade_ticks_from_cache_frame_native(
    *, loader: Any, frame: pd.DataFrame
) -> tuple[TradeTick, ...]:
    if frame.empty:
        return ()
    instrument = loader.instrument
    sorted_frame = frame.sort_values(["ts_event", "ts_init"], kind="stable")
    aggressor_sides = (
        sorted_frame["aggressor_side"]
        .astype(str)
        .str.strip()
        .str.upper()
        .map({"BUYER": 1, "SELLER": 2})
        .fillna(0)
        .to_numpy(dtype=np.uint8)
    )
    return tuple(
        TradeTick.from_raw_arrays_to_list(
            instrument.id,
            int(instrument.price_precision),
            int(instrument.size_precision),
            _rounded_float64_array(
                sorted_frame["price"].to_numpy(dtype=np.float64),
                int(instrument.price_precision),
            ),
            _rounded_float64_array(
                sorted_frame["size"].to_numpy(dtype=np.float64),
                int(instrument.size_precision),
            ),
            aggressor_sides,
            sorted_frame["trade_id"].astype(str).tolist(),
            sorted_frame["ts_event"].to_numpy(dtype=np.uint64),
            sorted_frame["ts_init"].to_numpy(dtype=np.uint64),
        )
    )


def _rounded_float64_array(values: Any, precision: int) -> np.ndarray:
    return np.round(np.asarray(values, dtype=np.float64), decimals=precision)


def _deserialize_trade_ticks(*, loader: Any, frame: pd.DataFrame) -> tuple[TradeTick, ...]:
    if frame.empty:
        return ()
    return _trade_ticks_from_cache_frame_native(loader=loader, frame=frame)


def _write_trade_cache(
    *,
    path: Path,
    trades: tuple[TradeTick, ...],
    market_label: str,
    day: pd.Timestamp,
) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    frame = _serialize_trade_ticks(trades)
    day_label = _trade_day_label(day)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(tmp_path, compression="zstd", index=False)
        os.replace(tmp_path, path)
        emit_loader_event(
            f"Wrote Polymarket trade cache for {market_label} {day_label}",
            stage="cache_write",
            vendor="polymarket",
            status="complete",
            platform="polymarket",
            data_type="book",
            source_kind="cache",
            source=f"polymarket-trade-cache::{path}",
            cache_path=str(path),
            market_slug=market_label,
            rows=len(trades),
            trade_ticks=len(trades),
            attrs={"day": day_label},
        )
    except Exception as exc:  # noqa: BLE001 - cache writes must not break replay
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        emit_loader_event(
            f"Failed to write Polymarket trade cache for {market_label} {day_label}",
            level="ERROR",
            stage="cache_write",
            vendor="polymarket",
            status="error",
            platform="polymarket",
            data_type="book",
            source_kind="cache",
            source=f"polymarket-trade-cache::{path}",
            cache_path=str(path),
            market_slug=market_label,
            rows=len(trades),
            trade_ticks=len(trades),
            attrs={"day": day_label, "error": str(exc)},
        )


def _trade_day_label(day: pd.Timestamp) -> str:
    return day.strftime("%Y-%m-%d")


def _print_trade_progress_header(
    *, market_label: str, start: pd.Timestamp, end: pd.Timestamp
) -> None:
    emit_loader_event(
        f"Loading Polymarket trade ticks for execution {market_label} "
        f"(window_start={start.isoformat()}, window_end={end.isoformat()})...",
        stage="fetch",
        vendor="polymarket",
        status="start",
        platform="polymarket",
        data_type="book",
        market_slug=market_label,
        window_start_ns=int(start.value),
        window_end_ns=int(end.value),
    )


def _trade_source_label(source: str) -> str:
    if source.startswith("telonex-trade-cache::"):
        cache_path = source.partition("::")[2]
        if cache_path:
            if "/trades/" in cache_path:
                return f"telonex trades cache {Path(cache_path).name}"
            return f"telonex onchain_fills cache {Path(cache_path).name}"
        return "telonex trade cache"
    if source.startswith("telonex-local-trades::"):
        return "telonex local trades"
    if source.startswith("telonex-local::"):
        return "telonex local onchain_fills"
    if source.startswith(("telonex-cache::", "telonex-cache-fast::")):
        if "/trades/" in source:
            return "telonex cache trades"
        return "telonex cache onchain_fills"
    if source.startswith("telonex-api::"):
        if "/trades/" in source:
            return "telonex api trades"
        return "telonex api onchain_fills"
    return source


def _print_trade_progress_line(
    *,
    day: pd.Timestamp,
    elapsed_secs: float,
    rows: int,
    source: str,
) -> None:
    emit_loader_event(
        f"trades {_trade_day_label(day):>10s}  {elapsed_secs:7.3f}s  "
        f"{rows:8d} rows  {_trade_source_label(source)}",
        stage="fetch",
        vendor="polymarket",
        status="complete",
        platform="polymarket",
        data_type="book",
        source=source,
        rows=rows,
        trade_ticks=rows,
        elapsed_ms=elapsed_secs * 1000.0,
        attrs={"day": _trade_day_label(day), "source_label": _trade_source_label(source)},
    )


def _polymarket_ceiling_warning(caught_warnings: list[warnings.WarningMessage]) -> str | None:
    for caught in caught_warnings:
        message = str(caught.message)
        if "Polymarket public trades API hit its historical offset ceiling" in message:
            return message
    return None


def _trade_days_for_window(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, ...]:
    start_utc = _normalize_timestamp(start)
    end_utc = _normalize_timestamp(end)
    return tuple(
        pd.Timestamp(day, tz=UTC)
        for day in source_days_for_window_ns(
            int(start_utc.value),
            int(end_utc.value),
            semantics="inclusive",
        )
    )


async def _load_trade_ticks(
    loader: Any, *, start: pd.Timestamp, end: pd.Timestamp, market_label: str
) -> tuple[TradeTick, ...]:
    start_utc = start.tz_convert(UTC)
    end_utc = end.tz_convert(UTC)
    all_trades: list[TradeTick] = []
    _print_trade_progress_header(market_label=market_label, start=start_utc, end=end_utc)
    for current_day in _trade_days_for_window(start_utc, end_utc):
        day_start = current_day
        day_end = min(current_day + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1), end_utc)
        cache_path = _trade_cache_path(loader=loader, date=current_day)
        day_trades: tuple[TradeTick, ...]
        started_at = time.perf_counter()
        telonex_loader = getattr(loader, "load_telonex_onchain_fill_ticks", None)
        telonex_trades = None
        if callable(telonex_loader):
            telonex_trades = await asyncio.to_thread(telonex_loader, day_start, day_end)
        if telonex_trades:
            day_trades = tuple(sorted(telonex_trades, key=_trade_record_sort_key))
            source = str(
                getattr(loader, "_telonex_last_trade_source", None) or "telonex onchain_fills"
            )
        elif cache_path is not None and cache_path.exists():
            frame = await asyncio.to_thread(pd.read_parquet, cache_path)
            day_trades = await asyncio.to_thread(
                _deserialize_trade_ticks, loader=loader, frame=frame
            )
            source = f"polymarket cache {cache_path.name}"
        else:
            emit_loader_event(
                "Fetching Polymarket public trades API "
                f"{market_label} day={_trade_day_label(current_day)} "
                f"condition_id={getattr(loader, 'condition_id', None)} "
                f"token_id={getattr(loader, 'token_id', None)}",
                stage="fetch",
                vendor="polymarket",
                status="start",
                platform="polymarket",
                data_type="book",
                source_kind="remote",
                source="https://data-api.polymarket.com/trades",
                market_slug=market_label,
                condition_id=getattr(loader, "condition_id", None),
                token_id=getattr(loader, "token_id", None),
                attrs={"day": _trade_day_label(current_day)},
            )
            with warnings.catch_warnings(record=True) as caught_warnings:
                warnings.simplefilter("always", RuntimeWarning)
                fetched = await loader.load_trades(day_start, day_end)
            ceiling_warning = _polymarket_ceiling_warning(caught_warnings)
            if ceiling_warning is not None:
                raise RuntimeError(
                    "Polymarket public trades API fallback failed for "
                    f"{market_label} on {_trade_day_label(current_day)}: {ceiling_warning}"
                )
            day_trades = tuple(sorted(fetched, key=_trade_record_sort_key))
            if cache_path is not None:
                await asyncio.to_thread(
                    _write_trade_cache,
                    path=cache_path,
                    trades=day_trades,
                    market_label=market_label,
                    day=current_day,
                )
            source = "polymarket api"
        _print_trade_progress_line(
            day=current_day,
            elapsed_secs=time.perf_counter() - started_at,
            rows=len(day_trades),
            source=source,
        )
        all_trades.extend(
            trade
            for trade in day_trades
            if int(start_utc.value) <= int(trade.ts_event) <= int(end_utc.value)
        )
    all_trades.sort(key=_trade_record_sort_key)
    return tuple(all_trades)


def _merge_records(
    *, book_records: tuple[OrderBookDeltas, ...], trade_records: tuple[TradeTick, ...]
) -> tuple[object, ...]:
    plan = replay_merge_plan(
        book_ts_events=[int(record.ts_event) for record in book_records],
        book_ts_inits=[int(record.ts_init) for record in book_records],
        trade_ts_events=[int(record.ts_event) for record in trade_records],
        trade_ts_inits=[int(record.ts_init) for record in trade_records],
    )
    return tuple(book_records[index] if kind == 0 else trade_records[index] for kind, index in plan)


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
class _ResolvedBookReplay:
    replay: BookReplay
    start: pd.Timestamp
    end: pd.Timestamp


@dataclass(frozen=True)
class _PreparedBookReplay:
    resolved: _ResolvedBookReplay
    loader: Any
    outcome: str


@dataclass(frozen=True)
class _LoadedBookReplay:
    prepared: _PreparedBookReplay
    book_records: tuple[OrderBookDeltas, ...]
    book_event_count: int


async def _gather_bounded(
    values: Sequence[Any],
    *,
    workers: int,
    func: Callable[[Any], Any],
) -> list[Any]:
    worker_count = min(max(1, int(workers)), max(1, len(values)))
    if worker_count <= 1:
        return [await func(value) for value in values]

    semaphore = asyncio.Semaphore(worker_count)

    async def _run(value: Any) -> Any:
        async with semaphore:
            return await func(value)

    return list(await asyncio.gather(*(_run(value) for value in values)))


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

    def _resolve_book_replay_window(
        self, replay: BookReplay, *, request: ReplayLoadRequest, source_label: str
    ) -> _ResolvedBookReplay:
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
                    f"start_time/end_time or lookback_hours is required for {source_label} book replays."
                )
            start = end - pd.Timedelta(hours=float(lookback_hours))

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )
        return _ResolvedBookReplay(replay=replay, start=start, end=end)

    @staticmethod
    def _emit_book_replay_start(*, resolved: _ResolvedBookReplay, vendor: str) -> None:
        replay = resolved.replay
        label = vendor.upper() if vendor == "pmxt" else vendor.title()
        emit_loader_event(
            f"Loading {label} Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={resolved.start.isoformat()}, "
            f"window_end={resolved.end.isoformat()})...",
            stage="fetch",
            vendor=vendor,
            status="start",
            platform="polymarket",
            data_type="book",
            market_slug=replay.market_slug,
            token_id=str(replay.token_index),
            window_start_ns=int(resolved.start.value),
            window_end_ns=int(resolved.end.value),
        )

    @staticmethod
    def _emit_book_replay_fetch_error(
        *, replay: BookReplay, vendor: str, source_label: str, error: Exception
    ) -> None:
        emit_loader_event(
            f"Skip {replay.market_slug}: unable to load {source_label} L2 book data ({error})",
            level="WARNING",
            stage="fetch",
            vendor=vendor,
            status="error",
            platform="polymarket",
            data_type="book",
            market_slug=replay.market_slug,
        )

    def _build_loaded_book_replay_or_none(
        self,
        *,
        prepared: _PreparedBookReplay,
        records: tuple[object, ...],
        book_event_count: int | None = None,
        request: ReplayLoadRequest,
        vendor: str,
        source_label: str,
    ) -> LoadedReplay | None:
        replay = prepared.resolved.replay
        if not records:
            emit_loader_event(
                f"Skip {replay.market_slug}: no {source_label} L2 book data returned",
                level="WARNING",
                stage="validate",
                vendor=vendor,
                status="skip",
                platform="polymarket",
                data_type="book",
                market_slug=replay.market_slug,
            )
            return None

        deltas_type = _resolve_backtest_compat_symbol("OrderBookDeltas", OrderBookDeltas)
        if request.min_price_range > 0.0 or book_event_count is None:
            book_event_count, prices_tuple = _book_event_count_and_prices_for_request(
                instrument=prepared.loader.instrument,
                records=records,
                deltas_type=deltas_type,
                request=request,
            )
        else:
            prices_tuple = ()
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
            instrument=prepared.loader.instrument,
            records=records,
            count=book_event_count,
            count_key="book_events",
            market_key="slug",
            market_id=replay.market_slug,
            prices=prices_tuple,
            outcome=prepared.outcome,
            realized_outcome=_loader_realized_outcome(prepared.loader),
            metadata=dict(replay.metadata or {}),
            requested_window=_requested_window(prepared.resolved.start, prepared.resolved.end),
        )

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

        emit_loader_event(
            f"Loading PMXT Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})...",
            stage="fetch",
            vendor="pmxt",
            status="start",
            platform="polymarket",
            data_type="book",
            market_slug=replay.market_slug,
            token_id=str(replay.token_index),
            window_start_ns=int(start.value),
            window_end_ns=int(end.value),
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol(
                "PolymarketPMXTDataLoader", PolymarketPMXTDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            book_records = tuple(await asyncio.to_thread(loader.load_order_book_deltas, start, end))
            trade_records = await _load_trade_ticks(
                loader, start=start, end=end, market_label=replay.market_slug
            )
            records = _merge_records(book_records=book_records, trade_records=trade_records)
        except Exception as exc:
            emit_loader_event(
                f"Skip {replay.market_slug}: unable to load PMXT L2 book data ({exc})",
                level="WARNING",
                stage="fetch",
                vendor="pmxt",
                status="error",
                platform="polymarket",
                data_type="book",
                market_slug=replay.market_slug,
            )
            return None

        if not records:
            emit_loader_event(
                f"Skip {replay.market_slug}: no PMXT L2 book data returned",
                level="WARNING",
                stage="validate",
                vendor="pmxt",
                status="skip",
                platform="polymarket",
                data_type="book",
                market_slug=replay.market_slug,
            )
            return None

        deltas_type = _resolve_backtest_compat_symbol("OrderBookDeltas", OrderBookDeltas)
        book_event_count, prices_tuple = _book_event_count_and_prices_for_request(
            instrument=loader.instrument,
            records=records,
            deltas_type=deltas_type,
            request=request,
        )
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

    async def load_replays(
        self,
        replays: Sequence[BookReplay],
        *,
        request: ReplayLoadRequest,
        workers: int,
    ) -> list[LoadedReplay]:
        resolved_replays = [
            self._resolve_book_replay_window(replay, request=request, source_label="PMXT")
            for replay in replays
        ]
        for resolved in resolved_replays:
            self._emit_book_replay_start(resolved=resolved, vendor="pmxt")

        loader_cls = _resolve_backtest_compat_symbol(
            "PolymarketPMXTDataLoader", PolymarketPMXTDataLoader
        )

        async def _prepare(resolved: _ResolvedBookReplay) -> _PreparedBookReplay | None:
            replay = resolved.replay
            try:
                loader = await loader_cls.from_market_slug(
                    replay.market_slug, token_index=replay.token_index
                )
                return _PreparedBookReplay(
                    resolved=resolved,
                    loader=loader,
                    outcome=str(loader.instrument.outcome or replay.outcome or ""),
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="pmxt",
                    source_label="PMXT",
                    error=exc,
                )
                return None

        prepared = [
            item
            for item in await _gather_bounded(resolved_replays, workers=workers, func=_prepare)
            if item is not None
        ]

        async def _load_book(prepared_replay: _PreparedBookReplay) -> _LoadedBookReplay | None:
            replay = prepared_replay.resolved.replay
            try:
                records = tuple(
                    await asyncio.to_thread(
                        prepared_replay.loader.load_order_book_deltas,
                        prepared_replay.resolved.start,
                        prepared_replay.resolved.end,
                    )
                )
                return _LoadedBookReplay(
                    prepared=prepared_replay,
                    book_records=records,
                    book_event_count=len(records),
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="pmxt",
                    source_label="PMXT",
                    error=exc,
                )
                return None

        async def _load_cached_hour(
            item: tuple[int, pd.Timestamp],
        ) -> tuple[int, pd.Timestamp, list[pa.RecordBatch] | None, bool]:
            index, hour = item
            prepared_replay = prepared[index]
            loader = prepared_replay.loader

            def _load() -> tuple[list[pa.RecordBatch] | None, bool]:
                batches = loader._load_cached_market_batches(hour)
                cache_path = loader._cache_path_for_hour(hour)
                if batches is not None:
                    rows = loader._row_count_from_batches(batches)
                    loader._emit_pmxt_source_event(
                        message=f"Loaded PMXT filtered cache for {loader._hour_label(hour)} ({rows} rows)",
                        stage="cache_read",
                        status="cache_hit",
                        hour=hour,
                        source_kind="cache",
                        cache_path=cache_path,
                        rows=rows,
                        origin_function="_load_cached_market_batches",
                    )
                    return batches, True
                if cache_path is not None:
                    loader._emit_pmxt_source_event(
                        message=f"PMXT filtered cache miss for {loader._hour_label(hour)}",
                        stage="cache_read",
                        status="cache_miss",
                        hour=hour,
                        source_kind="cache",
                        cache_path=cache_path,
                        origin_function="_load_cached_market_batches",
                    )
                return None, False

            batches, hit = await asyncio.to_thread(_load)
            return index, hour, batches, hit

        async def _load_grouped_pmxt_books() -> list[_LoadedBookReplay]:
            if not prepared:
                return []

            resolved_batch_size = int(
                getattr(
                    prepared[0].loader,
                    "_pmxt_scan_batch_size",
                    getattr(prepared[0].loader, "_PMXT_DEFAULT_SCAN_BATCH_SIZE", 100_000),
                )
            )
            hours_by_index: dict[int, tuple[pd.Timestamp, ...]] = {
                index: tuple(
                    prepared_replay.loader._archive_hours(
                        prepared_replay.resolved.start,
                        prepared_replay.resolved.end,
                    )
                )
                for index, prepared_replay in enumerate(prepared)
            }
            hour_batches_by_index: dict[int, dict[pd.Timestamp, list[pa.RecordBatch] | None]] = {
                index: {} for index in range(len(prepared))
            }
            cache_checks = [
                (index, hour) for index, hours in hours_by_index.items() for hour in hours
            ]
            cache_results = await _gather_bounded(
                cache_checks,
                workers=workers,
                func=_load_cached_hour,
            )
            missing_by_hour: dict[pd.Timestamp, list[int]] = {}
            for index, hour, batches, hit in cache_results:
                if hit:
                    hour_batches_by_index[index][hour] = batches
                else:
                    missing_by_hour.setdefault(hour, []).append(index)

            async def _load_shared_hour(
                item: tuple[pd.Timestamp, list[int]],
            ) -> tuple[pd.Timestamp, dict[int, list[pa.RecordBatch] | None]]:
                hour, indexes = item
                representative = prepared[indexes[0]].loader
                requests = tuple(
                    (
                        index,
                        str(prepared[index].loader.condition_id),
                        str(prepared[index].loader.token_id),
                    )
                    for index in indexes
                    if getattr(prepared[index].loader, "condition_id", None) is not None
                    and getattr(prepared[index].loader, "token_id", None) is not None
                )
                batches_by_request = await asyncio.to_thread(
                    representative.load_shared_market_batches_for_hour,
                    hour,
                    requests=requests,
                    batch_size=resolved_batch_size,
                )
                return hour, batches_by_request

            shared_results = await _gather_bounded(
                tuple(missing_by_hour.items()),
                workers=workers,
                func=_load_shared_hour,
            )
            cache_writes: list[tuple[int, pd.Timestamp, list[pa.RecordBatch]]] = []
            for hour, batches_by_request in shared_results:
                for index in missing_by_hour.get(hour, ()):
                    batches = batches_by_request.get(index)
                    hour_batches_by_index[index][hour] = batches
                    if batches is not None:
                        cache_writes.append((index, hour, batches))

            async def _write_grouped_cache(
                item: tuple[int, pd.Timestamp, list[pa.RecordBatch]],
            ) -> None:
                index, hour, batches = item
                loader = prepared[index].loader
                table = pa.Table.from_batches(batches) if batches else loader._empty_market_table()
                await asyncio.to_thread(loader._write_cache_if_enabled, hour, table)

            await _gather_bounded(cache_writes, workers=workers, func=_write_grouped_cache)

            async def _build_book_from_grouped_hours(index: int) -> _LoadedBookReplay | None:
                prepared_replay = prepared[index]
                replay = prepared_replay.resolved.replay
                hour_batches = tuple(
                    (hour, hour_batches_by_index[index].get(hour)) for hour in hours_by_index[index]
                )
                try:
                    records = tuple(
                        await asyncio.to_thread(
                            prepared_replay.loader.load_order_book_deltas_from_hour_batches,
                            prepared_replay.resolved.start,
                            prepared_replay.resolved.end,
                            hour_batches,
                        )
                    )
                    return _LoadedBookReplay(
                        prepared=prepared_replay,
                        book_records=records,
                        book_event_count=len(records),
                    )
                except Exception as exc:
                    self._emit_book_replay_fetch_error(
                        replay=replay,
                        vendor="pmxt",
                        source_label="PMXT",
                        error=exc,
                    )
                    return None

            return [
                item
                for item in await _gather_bounded(
                    tuple(range(len(prepared))),
                    workers=workers,
                    func=_build_book_from_grouped_hours,
                )
                if item is not None
            ]

        can_group_pmxt_books = all(
            hasattr(item.loader, "load_shared_market_batches_for_hour")
            and hasattr(item.loader, "load_order_book_deltas_from_hour_batches")
            for item in prepared
        )
        if can_group_pmxt_books:
            loaded_books = await _load_grouped_pmxt_books()
        else:
            loaded_books = [
                item
                for item in await _gather_bounded(prepared, workers=workers, func=_load_book)
                if item is not None
            ]
        prepared_loaded = [loaded_book.prepared for loaded_book in loaded_books]
        book_event_counts = [loaded_book.book_event_count for loaded_book in loaded_books]
        book_slots: list[tuple[OrderBookDeltas, ...] | None] = [
            loaded_book.book_records for loaded_book in loaded_books
        ]
        del loaded_books

        async def _load_trades_and_build(index: int) -> LoadedReplay | None:
            prepared_replay = prepared_loaded[index]
            replay = prepared_replay.resolved.replay
            book_records = book_slots[index]
            if book_records is None:
                return None
            try:
                trade_records = await _load_trade_ticks(
                    prepared_replay.loader,
                    start=prepared_replay.resolved.start,
                    end=prepared_replay.resolved.end,
                    market_label=replay.market_slug,
                )
                records = await asyncio.to_thread(
                    _merge_records,
                    book_records=book_records,
                    trade_records=trade_records,
                )
                book_slots[index] = None
                return await asyncio.to_thread(
                    self._build_loaded_book_replay_or_none,
                    prepared=prepared_replay,
                    records=records,
                    book_event_count=book_event_counts[index],
                    request=request,
                    vendor="pmxt",
                    source_label="PMXT",
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="pmxt",
                    source_label="PMXT",
                    error=exc,
                )
                return None

        loaded = await _gather_bounded(
            tuple(range(len(prepared_loaded))),
            workers=workers,
            func=_load_trades_and_build,
        )
        return [loaded_sim for loaded_sim in loaded if loaded_sim is not None]


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

        emit_loader_event(
            f"Loading Telonex Polymarket market {replay.market_slug} "
            f"(token_index={replay.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})...",
            stage="fetch",
            vendor="telonex",
            status="start",
            platform="polymarket",
            data_type="book",
            market_slug=replay.market_slug,
            token_id=str(replay.token_index),
            window_start_ns=int(start.value),
            window_end_ns=int(end.value),
        )
        try:
            loader_cls = _resolve_backtest_compat_symbol(
                "PolymarketTelonexBookDataLoader", PolymarketTelonexBookDataLoader
            )
            loader = await loader_cls.from_market_slug(
                replay.market_slug, token_index=replay.token_index
            )
            selected_outcome = str(loader.instrument.outcome or replay.outcome or "")
            book_records = tuple(
                await asyncio.to_thread(
                    loader.load_order_book_deltas,
                    start,
                    end,
                    market_slug=replay.market_slug,
                    token_index=replay.token_index,
                    outcome=selected_outcome or None,
                )
            )
            trade_records = await _load_trade_ticks(
                loader, start=start, end=end, market_label=replay.market_slug
            )
            records = _merge_records(book_records=book_records, trade_records=trade_records)
        except Exception as exc:
            emit_loader_event(
                f"Skip {replay.market_slug}: unable to load Telonex L2 book data ({exc})",
                level="WARNING",
                stage="fetch",
                vendor="telonex",
                status="error",
                platform="polymarket",
                data_type="book",
                market_slug=replay.market_slug,
            )
            return None

        if not records:
            emit_loader_event(
                f"Skip {replay.market_slug}: no Telonex L2 book data returned",
                level="WARNING",
                stage="validate",
                vendor="telonex",
                status="skip",
                platform="polymarket",
                data_type="book",
                market_slug=replay.market_slug,
            )
            return None

        deltas_type = _resolve_backtest_compat_symbol("OrderBookDeltas", OrderBookDeltas)
        book_event_count, prices_tuple = _book_event_count_and_prices_for_request(
            instrument=loader.instrument,
            records=records,
            deltas_type=deltas_type,
            request=request,
        )
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

    async def load_replays(
        self,
        replays: Sequence[BookReplay],
        *,
        request: ReplayLoadRequest,
        workers: int,
    ) -> list[LoadedReplay]:
        resolved_replays = [
            self._resolve_book_replay_window(replay, request=request, source_label="Telonex")
            for replay in replays
        ]
        for resolved in resolved_replays:
            self._emit_book_replay_start(resolved=resolved, vendor="telonex")

        loader_cls = _resolve_backtest_compat_symbol(
            "PolymarketTelonexBookDataLoader", PolymarketTelonexBookDataLoader
        )

        async def _prepare(resolved: _ResolvedBookReplay) -> _PreparedBookReplay | None:
            replay = resolved.replay
            try:
                loader = await loader_cls.from_market_slug(
                    replay.market_slug, token_index=replay.token_index
                )
                return _PreparedBookReplay(
                    resolved=resolved,
                    loader=loader,
                    outcome=str(loader.instrument.outcome or replay.outcome or ""),
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="telonex",
                    source_label="Telonex",
                    error=exc,
                )
                return None

        prepared = [
            item
            for item in await _gather_bounded(resolved_replays, workers=workers, func=_prepare)
            if item is not None
        ]

        async def _load_book(prepared_replay: _PreparedBookReplay) -> _LoadedBookReplay | None:
            replay = prepared_replay.resolved.replay
            try:
                records = tuple(
                    await asyncio.to_thread(
                        prepared_replay.loader.load_order_book_deltas,
                        prepared_replay.resolved.start,
                        prepared_replay.resolved.end,
                        market_slug=replay.market_slug,
                        token_index=replay.token_index,
                        outcome=prepared_replay.outcome or None,
                    )
                )
                return _LoadedBookReplay(
                    prepared=prepared_replay,
                    book_records=records,
                    book_event_count=len(records),
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="telonex",
                    source_label="Telonex",
                    error=exc,
                )
                return None

        loaded_books = [
            item
            for item in await _gather_bounded(prepared, workers=workers, func=_load_book)
            if item is not None
        ]
        prepared_loaded = [loaded_book.prepared for loaded_book in loaded_books]
        book_event_counts = [loaded_book.book_event_count for loaded_book in loaded_books]
        book_slots: list[tuple[OrderBookDeltas, ...] | None] = [
            loaded_book.book_records for loaded_book in loaded_books
        ]
        del loaded_books

        async def _load_trades_and_build(index: int) -> LoadedReplay | None:
            prepared_replay = prepared_loaded[index]
            replay = prepared_replay.resolved.replay
            book_records = book_slots[index]
            if book_records is None:
                return None
            try:
                trade_records = await _load_trade_ticks(
                    prepared_replay.loader,
                    start=prepared_replay.resolved.start,
                    end=prepared_replay.resolved.end,
                    market_label=replay.market_slug,
                )
                records = await asyncio.to_thread(
                    _merge_records,
                    book_records=book_records,
                    trade_records=trade_records,
                )
                book_slots[index] = None
                return await asyncio.to_thread(
                    self._build_loaded_book_replay_or_none,
                    prepared=prepared_replay,
                    records=records,
                    book_event_count=book_event_counts[index],
                    request=request,
                    vendor="telonex",
                    source_label="Telonex",
                )
            except Exception as exc:
                self._emit_book_replay_fetch_error(
                    replay=replay,
                    vendor="telonex",
                    source_label="Telonex",
                    error=exc,
                )
                return None

        loaded = await _gather_bounded(
            tuple(range(len(prepared_loaded))),
            workers=workers,
            func=_load_trades_and_build,
        )
        return [loaded_sim for loaded_sim in loaded if loaded_sim is not None]


BUILTIN_REPLAY_ADAPTERS: tuple[HistoricalReplayAdapter, ...] = (
    PolymarketPMXTBookReplayAdapter(),
    PolymarketTelonexBookReplayAdapter(),
)


__all__ = ["BUILTIN_REPLAY_ADAPTERS", "L2_BOOK_ENGINE_PROFILE"]
