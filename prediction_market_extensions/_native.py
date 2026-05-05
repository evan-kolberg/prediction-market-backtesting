from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Literal

NATIVE_ENV = "PREDICTION_MARKET_NATIVE"
NATIVE_REQUIRE_ENV = "PREDICTION_MARKET_NATIVE_REQUIRE"

_DISABLED_VALUES = frozenset({"0", "false", "no", "off"})
_ENABLED_VALUES = frozenset({"1", "true", "yes", "on"})
WindowSemantics = Literal["half_open", "inclusive"]

_EXTENSION: ModuleType | None | Literal[False] = None


def _env_enabled(name: str) -> bool | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip().casefold()
    if normalized in _ENABLED_VALUES:
        return True
    if normalized in _DISABLED_VALUES:
        return False
    return None


def _extension_module() -> ModuleType | None:
    global _EXTENSION
    require_native = _env_enabled(NATIVE_REQUIRE_ENV) is True
    if _env_enabled(NATIVE_ENV) is False:
        if require_native:
            raise RuntimeError(
                f"{NATIVE_REQUIRE_ENV}=1 but {NATIVE_ENV}=0 disables native loading."
            )
        return None
    if _EXTENSION is False:
        if require_native:
            raise RuntimeError(
                f"{NATIVE_REQUIRE_ENV}=1 but prediction_market_extensions._native_ext "
                "is not importable. Build it with `make native-develop`."
            )
        return None
    if _EXTENSION is not None:
        return _EXTENSION
    try:
        _EXTENSION = import_module("prediction_market_extensions._native_ext")
    except ImportError as exc:
        if require_native:
            raise RuntimeError(
                f"{NATIVE_REQUIRE_ENV}=1 but prediction_market_extensions._native_ext "
                "is not importable. Build it with `make native-develop`."
            ) from exc
        _EXTENSION = False
        return None
    return _EXTENSION


def _required_extension_module() -> ModuleType:
    module = _extension_module()
    if module is None:
        raise RuntimeError(
            "Rust native data loading is required on v4. "
            "Build the extension with `make native-develop`, or remove "
            f"{NATIVE_ENV}=0 from the environment."
        )
    return module


def native_available() -> bool:
    module = _extension_module()
    if module is None:
        return False
    return bool(module.native_available())


def _required_native_function(module: ModuleType, name: str):
    try:
        return getattr(module, name)
    except AttributeError as exc:
        raise RuntimeError(
            "prediction_market_extensions._native_ext is stale and is missing "
            f"{name}(). Rebuild it with `make native-develop`."
        ) from exc


def _validate_semantics(semantics: str) -> WindowSemantics:
    normalized = semantics.strip().replace("-", "_").casefold()
    if normalized in {"half_open", "inclusive"}:
        return normalized  # type: ignore[return-value]
    raise ValueError("window semantics must be 'half_open' or 'inclusive'")


def source_days_for_window_ns(
    start_ns: int, end_ns: int, *, semantics: str = "inclusive"
) -> list[str]:
    normalized_semantics = _validate_semantics(semantics)
    module = _required_extension_module()
    return list(module.source_days_for_window(start_ns, end_ns, normalized_semantics))


def telonex_source_days_for_window_ns(start_ns: int, end_ns: int) -> list[str]:
    module = _required_extension_module()
    return list(module.telonex_source_days_for_window(start_ns, end_ns))


def telonex_day_window_ns(date: str, start_ns: int, end_ns: int) -> tuple[int, int] | None:
    module = _required_extension_module()
    value = module.telonex_day_window_ns(date, start_ns, end_ns)
    if value is None:
        return None
    return (int(value[0]), int(value[1]))


def telonex_flat_book_snapshot_diff_rows(
    *,
    timestamp_ns: Sequence[int],
    bid_prices: Sequence[Sequence[str]],
    bid_sizes: Sequence[Sequence[str]],
    ask_prices: Sequence[Sequence[str]],
    ask_sizes: Sequence[Sequence[str]],
    start_ns: int,
    end_ns: int,
) -> tuple[
    int | None,
    list[int],
    list[int],
    list[int],
    list[float],
    list[float],
    list[int],
    list[int],
    list[int],
    list[int],
]:
    module = _required_extension_module()
    (
        first_snapshot_index,
        event_index,
        action,
        side,
        price,
        size,
        flags,
        sequence,
        ts_event,
        ts_init,
    ) = module.telonex_flat_book_snapshot_diff_rows(
        [int(value) for value in timestamp_ns],
        list(bid_prices),
        list(bid_sizes),
        list(ask_prices),
        list(ask_sizes),
        int(start_ns),
        int(end_ns),
    )
    return (
        None if first_snapshot_index is None else int(first_snapshot_index),
        [int(value) for value in event_index],
        [int(value) for value in action],
        [int(value) for value in side],
        [float(value) for value in price],
        [float(value) for value in size],
        [int(value) for value in flags],
        [int(value) for value in sequence],
        [int(value) for value in ts_event],
        [int(value) for value in ts_init],
    )


def telonex_nested_book_snapshot_diff_rows(
    *,
    timestamp_ns: Sequence[int],
    bids: Sequence[object],
    asks: Sequence[object],
    start_ns: int,
    end_ns: int,
) -> tuple[
    int | None,
    list[int],
    list[int],
    list[int],
    list[float],
    list[float],
    list[int],
    list[int],
    list[int],
    list[int],
]:
    module = _required_extension_module()
    nested_diff_rows = _required_native_function(module, "telonex_nested_book_snapshot_diff_rows")
    (
        first_snapshot_index,
        event_index,
        action,
        side,
        price,
        size,
        flags,
        sequence,
        ts_event,
        ts_init,
    ) = nested_diff_rows(
        [int(value) for value in timestamp_ns],
        list(bids),
        list(asks),
        int(start_ns),
        int(end_ns),
    )
    return (
        None if first_snapshot_index is None else int(first_snapshot_index),
        [int(value) for value in event_index],
        [int(value) for value in action],
        [int(value) for value in side],
        [float(value) for value in price],
        [float(value) for value in size],
        [int(value) for value in flags],
        [int(value) for value in sequence],
        [int(value) for value in ts_event],
        [int(value) for value in ts_init],
    )


def telonex_parquet_book_snapshot_diff_rows(
    *,
    path: str,
    row_groups: Sequence[int],
    start_ns: int,
    end_ns: int,
) -> tuple[
    int | None,
    list[int],
    list[int],
    list[int],
    list[float],
    list[float],
    list[int],
    list[int],
    list[int],
    list[int],
]:
    module = _required_extension_module()
    parquet_diff_rows = _required_native_function(module, "telonex_parquet_book_snapshot_diff_rows")
    (
        first_snapshot_index,
        event_index,
        action,
        side,
        price,
        size,
        flags,
        sequence,
        ts_event,
        ts_init,
    ) = parquet_diff_rows(
        path,
        [int(value) for value in row_groups],
        int(start_ns),
        int(end_ns),
    )
    return (
        None if first_snapshot_index is None else int(first_snapshot_index),
        [int(value) for value in event_index],
        [int(value) for value in action],
        [int(value) for value in side],
        [float(value) for value in price],
        [float(value) for value in size],
        [int(value) for value in flags],
        [int(value) for value in sequence],
        [int(value) for value in ts_event],
        [int(value) for value in ts_init],
    )


def telonex_onchain_fill_trade_rows(
    *,
    timestamp_ns: Sequence[int],
    prices: Sequence[object],
    sizes: Sequence[object],
    sides: Sequence[object] | None,
    ids: Sequence[object] | None,
    start_ns: int,
    end_ns: int,
    token_suffix: str,
) -> tuple[
    list[float],
    list[float],
    list[int],
    list[str],
    list[int],
    list[int],
]:
    module = _required_extension_module()
    (
        out_prices,
        out_sizes,
        aggressor_sides,
        trade_ids,
        ts_events,
        ts_inits,
    ) = module.telonex_onchain_fill_trade_rows(
        [int(value) for value in timestamp_ns],
        list(prices),
        list(sizes),
        None if sides is None else list(sides),
        None if ids is None else list(ids),
        int(start_ns),
        int(end_ns),
        str(token_suffix),
    )
    return (
        [float(value) for value in out_prices],
        [float(value) for value in out_sizes],
        [int(value) for value in aggressor_sides],
        [str(value) for value in trade_ids],
        [int(value) for value in ts_events],
        [int(value) for value in ts_inits],
    )


def decimal_seconds_to_ns(value: object) -> int:
    text = str(value)
    module = _required_extension_module()
    return int(module.decimal_seconds_to_ns(text))


def float_seconds_to_ms_string(value: float) -> str:
    module = _required_extension_module()
    return str(module.float_seconds_to_ms_string(float(value)))


def fixed_raw_values(values: Sequence[object], precision: int) -> list[int]:
    module = _required_extension_module()
    return [
        int(value)
        for value in _required_native_function(module, "fixed_raw_values")(
            [float(value) for value in values],
            int(precision),
        )
    ]


def pmxt_payload_sort_key(update_type: str, payload_text: str) -> tuple[int, int]:
    module = _required_extension_module()
    timestamp_ns, priority = module.pmxt_payload_sort_key(update_type, payload_text)
    return (int(timestamp_ns), int(priority))


def pmxt_sort_payload_columns(
    update_type_columns: Sequence[Sequence[str]],
    payload_text_columns: Sequence[Sequence[str]],
) -> list[tuple[int, int, str, str]]:
    module = _required_extension_module()
    sort_payload_columns = _required_native_function(module, "pmxt_sort_payload_columns")
    return [
        (int(timestamp_ns), int(priority), str(update_type), str(payload_text))
        for timestamp_ns, priority, update_type, payload_text in sort_payload_columns(
            update_type_columns,
            payload_text_columns,
        )
    ]


def pmxt_payload_delta_rows(
    *,
    update_type_columns: Sequence[Sequence[str]],
    payload_text_columns: Sequence[Sequence[str]],
    token_id: str,
    start_ns: int,
    end_ns: int,
    has_snapshot: bool,
    last_payload_key: tuple[int, int] | None,
) -> tuple[
    bool,
    tuple[int, int] | None,
    dict[str, list[object]],
]:
    module = _required_extension_module()
    payload_delta_rows = _required_native_function(module, "pmxt_payload_delta_rows")
    (
        next_has_snapshot,
        last_timestamp_ns,
        last_priority,
        event_index,
        action,
        side,
        price,
        size,
        flags,
        sequence,
        ts_event,
        ts_init,
    ) = payload_delta_rows(
        update_type_columns,
        payload_text_columns,
        str(token_id),
        int(start_ns),
        int(end_ns),
        bool(has_snapshot),
        None if last_payload_key is None else int(last_payload_key[0]),
        None if last_payload_key is None else int(last_payload_key[1]),
    )
    next_last_payload_key = (
        None
        if last_timestamp_ns is None or last_priority is None
        else (int(last_timestamp_ns), int(last_priority))
    )
    return (
        bool(next_has_snapshot),
        next_last_payload_key,
        {
            "event_index": [int(value) for value in event_index],
            "action": [int(value) for value in action],
            "side": [int(value) for value in side],
            "price": [float(value) for value in price],
            "size": [float(value) for value in size],
            "flags": [int(value) for value in flags],
            "sequence": [int(value) for value in sequence],
            "ts_event": [int(value) for value in ts_event],
            "ts_init": [int(value) for value in ts_init],
        },
    )


def pmxt_fixed_delta_rows(
    *,
    event_type_columns: Sequence[Sequence[str]],
    timestamp_ns_columns: Sequence[Sequence[int]],
    asset_id_columns: Sequence[Sequence[str]],
    bids_json_columns: Sequence[Sequence[object]],
    asks_json_columns: Sequence[Sequence[object]],
    price_columns: Sequence[Sequence[object]],
    size_columns: Sequence[Sequence[object]],
    side_columns: Sequence[Sequence[object]],
    token_id: str,
    start_ns: int,
    end_ns: int,
    has_snapshot: bool,
    last_payload_key: tuple[int, int] | None,
) -> tuple[
    bool,
    tuple[int, int] | None,
    dict[str, list[object]],
]:
    module = _required_extension_module()
    fixed_delta_rows = _required_native_function(module, "pmxt_fixed_delta_rows")
    (
        next_has_snapshot,
        last_timestamp_ns,
        last_priority,
        event_index,
        action,
        side,
        price,
        size,
        flags,
        sequence,
        ts_event,
        ts_init,
    ) = fixed_delta_rows(
        event_type_columns,
        timestamp_ns_columns,
        asset_id_columns,
        bids_json_columns,
        asks_json_columns,
        price_columns,
        size_columns,
        side_columns,
        str(token_id),
        int(start_ns),
        int(end_ns),
        bool(has_snapshot),
        None if last_payload_key is None else int(last_payload_key[0]),
        None if last_payload_key is None else int(last_payload_key[1]),
    )
    next_last_payload_key = (
        None
        if last_timestamp_ns is None or last_priority is None
        else (int(last_timestamp_ns), int(last_priority))
    )
    return (
        bool(next_has_snapshot),
        next_last_payload_key,
        {
            "event_index": [int(value) for value in event_index],
            "action": [int(value) for value in action],
            "side": [int(value) for value in side],
            "price": [float(value) for value in price],
            "size": [float(value) for value in size],
            "flags": [int(value) for value in flags],
            "sequence": [int(value) for value in sequence],
            "ts_event": [int(value) for value in ts_event],
            "ts_init": [int(value) for value in ts_init],
        },
    )


def polymarket_trade_sort_key(trade: Mapping[str, object]) -> tuple[int, str, str, str, str, str]:
    timestamp = int(trade["timestamp"])
    transaction_hash = str(trade.get("transactionHash", ""))
    asset = str(trade.get("asset", ""))
    side = str(trade.get("side", ""))
    price = str(trade.get("price", ""))
    size = str(trade.get("size", ""))
    module = _required_extension_module()
    return tuple(
        module.polymarket_trade_sort_key(timestamp, transaction_hash, asset, side, price, size)
    )  # type: ignore[return-value]


def polymarket_trade_sort_keys(
    trades: Sequence[Mapping[str, object]],
) -> list[tuple[int, str, str, str, str, str]]:
    rows = [
        (
            int(trade["timestamp"]),
            str(trade.get("transactionHash", "")),
            str(trade.get("asset", "")),
            str(trade.get("side", "")),
            str(trade.get("price", "")),
            str(trade.get("size", "")),
        )
        for trade in trades
    ]
    module = _required_extension_module()
    return [
        (
            int(timestamp),
            str(transaction_hash),
            str(asset),
            str(side),
            str(price),
            str(size),
        )
        for timestamp, transaction_hash, asset, side, price, size in module.polymarket_trade_sort_keys(
            rows
        )
    ]


def polymarket_trade_id(transaction_hash: str, asset: str, sequence: int) -> str:
    module = _required_extension_module()
    return str(module.polymarket_trade_id(transaction_hash, asset, sequence))


def polymarket_trade_ids(rows: Sequence[tuple[str, str, int]]) -> list[str]:
    module = _required_extension_module()
    return [str(value) for value in module.polymarket_trade_ids(list(rows))]


def polymarket_normalize_trade_side(side: str) -> str:
    module = _required_extension_module()
    return str(module.polymarket_normalize_trade_side(side))


def polymarket_normalize_trade_sides(sides: Sequence[str]) -> list[str]:
    module = _required_extension_module()
    return [str(value) for value in module.polymarket_normalize_trade_sides(list(sides))]


def polymarket_is_tradable_probability_price(price: str) -> bool:
    module = _required_extension_module()
    return bool(module.polymarket_is_tradable_probability_price(price))


def polymarket_are_tradable_probability_prices(prices: Sequence[str]) -> list[bool]:
    module = _required_extension_module()
    return [
        bool(value) for value in module.polymarket_are_tradable_probability_prices(list(prices))
    ]


def polymarket_trade_event_timestamp_ns(
    base_timestamp_ns: int,
    occurrence_in_second: int,
) -> int:
    module = _required_extension_module()
    return int(module.polymarket_trade_event_timestamp_ns(base_timestamp_ns, occurrence_in_second))


def polymarket_trade_event_timestamp_ns_batch(
    rows: Sequence[tuple[int, int]],
) -> list[int]:
    module = _required_extension_module()
    return [int(value) for value in module.polymarket_trade_event_timestamp_ns_batch(list(rows))]


def polymarket_public_trade_rows(
    trades: Sequence[Mapping[str, object]],
    *,
    token_id: str,
    sort: bool = False,
) -> tuple[
    list[float],
    list[float],
    list[int],
    list[str],
    list[int],
    list[int],
    list[tuple[int, str]],
    list[tuple[int, float]],
]:
    module = _required_extension_module()
    rows = [
        (
            index,
            int(trade["timestamp"]),
            str(trade.get("transactionHash", "")),
            str(trade.get("asset", "")),
            str(trade.get("side", "")),
            str(trade.get("price", "")),
            str(trade.get("size", "")),
        )
        for index, trade in enumerate(trades)
    ]
    result = _required_native_function(module, "polymarket_public_trade_rows")(
        rows,
        token_id,
        bool(sort),
    )
    (
        prices,
        sizes,
        aggressor_sides,
        trade_ids,
        ts_events,
        ts_inits,
        unexpected_side_records,
        skipped_price_records,
    ) = result
    return (
        [float(value) for value in prices],
        [float(value) for value in sizes],
        [int(value) for value in aggressor_sides],
        [str(value) for value in trade_ids],
        [int(value) for value in ts_events],
        [int(value) for value in ts_inits],
        [(int(index), str(side)) for index, side in unexpected_side_records],
        [(int(index), float(price)) for index, price in skipped_price_records],
    )


def replay_merge_plan(
    *,
    book_ts_events: Sequence[int],
    book_ts_inits: Sequence[int],
    trade_ts_events: Sequence[int],
    trade_ts_inits: Sequence[int],
) -> list[tuple[int, int]]:
    module = _required_extension_module()
    merge_plan = _required_native_function(module, "replay_merge_plan")
    return [
        (int(kind), int(index))
        for kind, index in merge_plan(
            [int(value) for value in book_ts_events],
            [int(value) for value in book_ts_inits],
            [int(value) for value in trade_ts_events],
            [int(value) for value in trade_ts_inits],
        )
    ]


def pmxt_archive_hours_for_window_ns(start_ns: int, end_ns: int) -> list[int]:
    module = _required_extension_module()
    return list(module.pmxt_archive_hours_for_window(start_ns, end_ns))


def telonex_source_label_kind(source: str) -> str | None:
    module = _required_extension_module()
    value = module.telonex_source_label_kind(source)
    return None if value is None else str(value)


def telonex_stage_for_source(source: str) -> str:
    module = _required_extension_module()
    return str(module.telonex_stage_for_source(source))


def telonex_api_url(
    *,
    base_url: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> str:
    module = _required_extension_module()
    return str(module.telonex_api_url(base_url, channel, date, market_slug, token_index, outcome))


def telonex_api_cache_relative_path(
    *,
    base_url_key: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> Path:
    module = _required_extension_module()
    return Path(
        str(
            module.telonex_api_cache_relative_path(
                base_url_key, channel, date, market_slug, token_index, outcome
            )
        )
    )


def telonex_deltas_cache_relative_path(
    *,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
    instrument_key: str,
    start_ns: int,
    end_ns: int,
) -> Path:
    module = _required_extension_module()
    return Path(
        str(
            module.telonex_deltas_cache_relative_path(
                channel,
                date,
                market_slug,
                token_index,
                outcome,
                instrument_key,
                start_ns,
                end_ns,
            )
        )
    )


def telonex_trade_ticks_cache_relative_path(
    *,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
    instrument_key: str,
    start_ns: int,
    end_ns: int,
) -> Path:
    module = _required_extension_module()
    return Path(
        str(
            module.telonex_trade_ticks_cache_relative_path(
                channel,
                date,
                market_slug,
                token_index,
                outcome,
                instrument_key,
                start_ns,
                end_ns,
            )
        )
    )


def telonex_local_consolidated_candidate_paths(
    *,
    root: Path,
    channel: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> tuple[Path, ...]:
    module = _required_extension_module()
    return tuple(
        Path(path)
        for path in module.telonex_local_consolidated_candidate_paths(
            str(root), channel, market_slug, token_index, outcome
        )
    )


def telonex_local_daily_candidate_paths(
    *,
    root: Path,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> tuple[Path, ...]:
    module = _required_extension_module()
    return tuple(
        Path(path)
        for path in module.telonex_local_daily_candidate_paths(
            str(root), channel, date, market_slug, token_index, outcome
        )
    )


__all__ = [
    "NATIVE_ENV",
    "NATIVE_REQUIRE_ENV",
    "WindowSemantics",
    "decimal_seconds_to_ns",
    "fixed_raw_values",
    "float_seconds_to_ms_string",
    "native_available",
    "pmxt_archive_hours_for_window_ns",
    "pmxt_fixed_delta_rows",
    "pmxt_payload_delta_rows",
    "pmxt_payload_sort_key",
    "pmxt_sort_payload_columns",
    "polymarket_are_tradable_probability_prices",
    "polymarket_is_tradable_probability_price",
    "polymarket_normalize_trade_side",
    "polymarket_normalize_trade_sides",
    "polymarket_public_trade_rows",
    "polymarket_trade_id",
    "polymarket_trade_ids",
    "polymarket_trade_event_timestamp_ns",
    "polymarket_trade_event_timestamp_ns_batch",
    "polymarket_trade_sort_key",
    "polymarket_trade_sort_keys",
    "replay_merge_plan",
    "source_days_for_window_ns",
    "telonex_api_url",
    "telonex_api_cache_relative_path",
    "telonex_day_window_ns",
    "telonex_deltas_cache_relative_path",
    "telonex_flat_book_snapshot_diff_rows",
    "telonex_local_consolidated_candidate_paths",
    "telonex_local_daily_candidate_paths",
    "telonex_nested_book_snapshot_diff_rows",
    "telonex_onchain_fill_trade_rows",
    "telonex_parquet_book_snapshot_diff_rows",
    "telonex_source_days_for_window_ns",
    "telonex_source_label_kind",
    "telonex_stage_for_source",
    "telonex_trade_ticks_cache_relative_path",
]
