from __future__ import annotations

import os
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Literal
from urllib.parse import urlencode

NATIVE_ENV = "PREDICTION_MARKET_NATIVE"
NATIVE_REQUIRE_ENV = "PREDICTION_MARKET_NATIVE_REQUIRE"

_DISABLED_VALUES = frozenset({"0", "false", "no", "off"})
_ENABLED_VALUES = frozenset({"1", "true", "yes", "on"})
_NANOS_PER_DAY = 86_400_000_000_000
_NANOS_PER_HOUR = 3_600_000_000_000
_SECONDS_PER_DAY = 86_400
_TELONEX_EXCHANGE = "polymarket"

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


def _format_utc_day(day_index: int) -> str:
    return datetime.fromtimestamp(day_index * _SECONDS_PER_DAY, UTC).strftime("%Y-%m-%d")


def _source_days_for_window_python(
    start_ns: int, end_ns: int, semantics: WindowSemantics
) -> list[str]:
    if semantics == "half_open":
        if end_ns <= start_ns:
            return []
        effective_end_ns = end_ns - 1
    else:
        if end_ns < start_ns:
            return []
        effective_end_ns = end_ns

    first_day = start_ns // _NANOS_PER_DAY
    last_day = effective_end_ns // _NANOS_PER_DAY
    return [_format_utc_day(day) for day in range(first_day, last_day + 1)]


def _pmxt_archive_hours_for_window_python(start_ns: int, end_ns: int) -> list[int]:
    if end_ns <= start_ns:
        return []

    first_hour = start_ns // _NANOS_PER_HOUR - 1
    last_hour = end_ns // _NANOS_PER_HOUR
    return [hour * _NANOS_PER_HOUR for hour in range(first_hour, last_hour + 1)]


def _decimal_seconds_to_ns_python(value: object) -> int:
    return int((Decimal(str(value)) * Decimal("1000000000")).to_integral_value())


def _float_seconds_to_ms_string_python(value: float) -> str:
    return f"{value * 1000:.6f}"


def _fixed_raw_values_python(values: Sequence[object], precision: int) -> list[int]:
    rounded_values = [round(float(value), precision) for value in values]
    return [int(round(value * 10_000_000_000_000_000)) for value in rounded_values]


def _pmxt_payload_sort_key_python(update_type: str, payload_text: str) -> tuple[int, int]:
    if update_type == "book_snapshot":
        priority = 0
    elif update_type == "price_change":
        priority = 1
    else:
        return (0, 2)

    payload = json.loads(payload_text)
    return (_decimal_seconds_to_ns_python(payload["timestamp"]), priority)


def _polymarket_trade_sort_key_python(
    timestamp: int,
    transaction_hash: str,
    asset: str,
    side: str,
    price: str,
    size: str,
) -> tuple[int, str, str, str, str, str]:
    return (timestamp, transaction_hash, asset, side, price, size)


def _polymarket_trade_id_python(transaction_hash: str, asset: str, sequence: int) -> str:
    return f"{transaction_hash[-24:]}-{asset[-4:]}-{sequence:06d}"


def _polymarket_normalize_trade_side_python(side: str) -> str:
    normalized = side.strip().upper()
    if normalized in {"BUY", "SELL"}:
        return normalized
    return "unknown"


def _polymarket_is_tradable_probability_price_python(price: str) -> bool:
    try:
        value = float(price)
    except ValueError:
        return False
    return 0.0 < value < 1.0


def _polymarket_trade_event_timestamp_ns_python(
    base_timestamp_ns: int,
    occurrence_in_second: int,
) -> int:
    return base_timestamp_ns + min(occurrence_in_second, 999_999_999)


def _polymarket_public_trade_rows_python(
    trades: Sequence[Mapping[str, object]],
    *,
    token_id: str,
    sort: bool,
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
    candidates = [row for row in rows if row[3] == token_id]
    if sort:
        candidates.sort(key=lambda row: row[1:])

    prices: list[float] = []
    sizes: list[float] = []
    aggressor_sides: list[int] = []
    trade_ids: list[str] = []
    ts_events: list[int] = []
    ts_inits: list[int] = []
    unexpected_side_records: list[tuple[int, str]] = []
    skipped_price_records: list[tuple[int, float]] = []
    timestamp_counts: dict[int, int] = {}
    tx_asset_counts: dict[tuple[str, str], int] = {}

    for candidate_index, row in enumerate(candidates):
        original_index, timestamp, transaction_hash, asset, side, price_text, size_text = row
        record_index = candidate_index if sort else original_index
        base_ts_event = timestamp * 1_000_000_000
        occurrence_in_second = timestamp_counts.get(base_ts_event, 0)
        timestamp_counts[base_ts_event] = occurrence_in_second + 1
        tx_asset_key = (transaction_hash, asset)
        tx_asset_sequence = tx_asset_counts.get(tx_asset_key, 0)
        tx_asset_counts[tx_asset_key] = tx_asset_sequence + 1
        side_value = _polymarket_normalize_trade_side_python(side)
        if side_value == "BUY":
            aggressor_side = 1
        elif side_value == "SELL":
            aggressor_side = 2
        else:
            unexpected_side_records.append((record_index, side))
            aggressor_side = 0
        price = float(price_text)
        if not 0.0 < price < 1.0:
            skipped_price_records.append((record_index, price))
            continue
        ts_event = _polymarket_trade_event_timestamp_ns_python(
            base_ts_event,
            occurrence_in_second,
        )
        prices.append(price)
        sizes.append(float(size_text))
        aggressor_sides.append(aggressor_side)
        trade_ids.append(_polymarket_trade_id_python(transaction_hash, asset, tx_asset_sequence))
        ts_events.append(ts_event)
        ts_inits.append(ts_event)
    return (
        prices,
        sizes,
        aggressor_sides,
        trade_ids,
        ts_events,
        ts_inits,
        unexpected_side_records,
        skipped_price_records,
    )


def _telonex_source_label_kind_python(source: str) -> str | None:
    if source == "none":
        return None
    if "cache" in source:
        return "cache"
    if source.startswith("telonex-local"):
        return "local"
    if source.startswith("telonex-api"):
        return "remote"
    return None


def _telonex_stage_for_source_python(source: str) -> str:
    return "cache_read" if "cache" in source else "fetch"


def _telonex_api_url_python(
    base_url: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> str:
    params = {"slug": market_slug}
    if outcome:
        params["outcome"] = outcome
    else:
        params["outcome_id"] = str(token_index)
    return (
        f"{base_url.rstrip('/')}/v1/downloads/{_TELONEX_EXCHANGE}/{channel}/{date}"
        f"?{urlencode(params)}"
    )


def _telonex_outcome_segments(token_index: int, outcome: str | None) -> list[str]:
    segments = [f"outcome_id={token_index}", str(token_index)]
    if outcome:
        segments.insert(0, outcome)
    return segments


def _telonex_local_consolidated_candidate_paths_python(
    root: str,
    channel: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> list[str]:
    root_path = Path(root)
    outcome_parts = _telonex_outcome_segments(token_index, outcome)
    candidates = [
        root_path / _TELONEX_EXCHANGE / market_slug / outcome_part / f"{channel}.parquet"
        for outcome_part in outcome_parts
    ]
    candidates.extend(
        root_path / _TELONEX_EXCHANGE / channel / market_slug / f"{outcome_part}.parquet"
        for outcome_part in outcome_parts
    )
    candidates.extend(
        root_path / channel / market_slug / f"{outcome_part}.parquet"
        for outcome_part in outcome_parts
    )
    return [str(path) for path in candidates]


def _telonex_local_daily_candidate_paths_python(
    root: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> list[str]:
    root_path = Path(root)
    outcome_parts = _telonex_outcome_segments(token_index, outcome)
    candidates = [
        root_path / _TELONEX_EXCHANGE / market_slug / outcome_part / channel / f"{date}.parquet"
        for outcome_part in outcome_parts
    ]
    candidates.extend(
        root_path / _TELONEX_EXCHANGE / channel / market_slug / outcome_part / f"{date}.parquet"
        for outcome_part in outcome_parts
    )
    candidates.extend(
        root_path / channel / market_slug / outcome_part / f"{date}.parquet"
        for outcome_part in outcome_parts
    )
    candidates.extend(
        [
            root_path / _TELONEX_EXCHANGE / channel / f"{market_slug}_{token_index}_{date}.parquet",
            root_path / channel / f"{market_slug}_{token_index}_{date}.parquet",
            root_path / f"{market_slug}_{token_index}_{date}.parquet",
            root_path / f"{date}.parquet",
        ]
    )
    return [str(path) for path in candidates]


def source_days_for_window_ns(
    start_ns: int, end_ns: int, *, semantics: str = "inclusive"
) -> list[str]:
    normalized_semantics = _validate_semantics(semantics)
    module = _extension_module()
    if module is not None:
        return list(module.source_days_for_window(start_ns, end_ns, normalized_semantics))
    return _source_days_for_window_python(start_ns, end_ns, normalized_semantics)


def telonex_source_days_for_window_ns(start_ns: int, end_ns: int) -> list[str]:
    module = _extension_module()
    if module is not None:
        return list(module.telonex_source_days_for_window(start_ns, end_ns))
    return _source_days_for_window_python(start_ns, end_ns, "inclusive")


def telonex_day_window_ns(date: str, start_ns: int, end_ns: int) -> tuple[int, int] | None:
    module = _extension_module()
    if module is not None:
        value = module.telonex_day_window_ns(date, start_ns, end_ns)
        if value is None:
            return None
        return (int(value[0]), int(value[1]))
    day_start = pd_day_start_ns(date)
    day_end = day_start + _NANOS_PER_DAY - 1
    clipped_start = max(start_ns, day_start)
    clipped_end = min(end_ns, day_end)
    if clipped_start > clipped_end:
        return None
    return (clipped_start, clipped_end)


def telonex_flat_book_snapshot_diff_rows(
    *,
    timestamp_ns: Sequence[int],
    bid_prices: Sequence[Sequence[str]],
    bid_sizes: Sequence[Sequence[str]],
    ask_prices: Sequence[Sequence[str]],
    ask_sizes: Sequence[Sequence[str]],
    start_ns: int,
    end_ns: int,
) -> (
    tuple[
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
    ]
    | None
):
    module = _extension_module()
    if module is None:
        return None
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
) -> (
    tuple[
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
    ]
    | None
):
    module = _extension_module()
    if module is None:
        return None
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
) -> (
    tuple[
        list[float],
        list[float],
        list[int],
        list[str],
        list[int],
        list[int],
    ]
    | None
):
    module = _extension_module()
    if module is None:
        return None
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


def pd_day_start_ns(date: str) -> int:
    return int(datetime.fromisoformat(date).replace(tzinfo=UTC).timestamp() * 1_000_000_000)


def decimal_seconds_to_ns(value: object) -> int:
    text = str(value)
    module = _extension_module()
    if module is not None:
        return int(module.decimal_seconds_to_ns(text))
    return _decimal_seconds_to_ns_python(text)


def float_seconds_to_ms_string(value: float) -> str:
    module = _extension_module()
    if module is not None:
        return str(module.float_seconds_to_ms_string(float(value)))
    return _float_seconds_to_ms_string_python(float(value))


def fixed_raw_values(values: Sequence[object], precision: int) -> list[int]:
    module = _extension_module()
    if module is not None:
        return [
            int(value)
            for value in _required_native_function(module, "fixed_raw_values")(
                [float(value) for value in values],
                int(precision),
            )
        ]
    return _fixed_raw_values_python(values, precision)


def pmxt_payload_sort_key(update_type: str, payload_text: str) -> tuple[int, int]:
    module = _extension_module()
    if module is not None:
        timestamp_ns, priority = module.pmxt_payload_sort_key(update_type, payload_text)
        return (int(timestamp_ns), int(priority))
    return _pmxt_payload_sort_key_python(update_type, payload_text)


def pmxt_sort_payload_columns(
    update_type_columns: Sequence[Sequence[str]],
    payload_text_columns: Sequence[Sequence[str]],
) -> list[tuple[int, int, str, str]]:
    module = _extension_module()
    if module is not None:
        sort_payload_columns = _required_native_function(module, "pmxt_sort_payload_columns")
        return [
            (int(timestamp_ns), int(priority), str(update_type), str(payload_text))
            for timestamp_ns, priority, update_type, payload_text in sort_payload_columns(
                update_type_columns,
                payload_text_columns,
            )
        ]

    if len(update_type_columns) != len(payload_text_columns):
        raise ValueError(
            "PMXT payload column count mismatch: "
            f"{len(update_type_columns)} update_type column(s), "
            f"{len(payload_text_columns)} payload column(s)"
        )

    items: list[tuple[str, str]] = []
    for column_index, (update_types, payload_texts) in enumerate(
        zip(update_type_columns, payload_text_columns, strict=True)
    ):
        if len(update_types) != len(payload_texts):
            raise ValueError(
                "PMXT payload row count mismatch in column "
                f"{column_index}: {len(update_types)} update_type row(s), "
                f"{len(payload_texts)} payload row(s)"
            )
        items.extend(
            (str(update_type), str(payload_text))
            for update_type, payload_text in zip(update_types, payload_texts, strict=True)
        )
    keyed_items = [
        (_pmxt_payload_sort_key_python(update_type, payload_text), update_type, payload_text)
        for update_type, payload_text in items
    ]
    return [
        (timestamp_ns, priority, update_type, payload_text)
        for (timestamp_ns, priority), update_type, payload_text in sorted(
            keyed_items,
            key=lambda item: item[0],
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
) -> (
    tuple[
        bool,
        tuple[int, int] | None,
        dict[str, list[object]],
    ]
    | None
):
    module = _extension_module()
    if module is None:
        return None
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


def polymarket_trade_sort_key(trade: Mapping[str, object]) -> tuple[int, str, str, str, str, str]:
    timestamp = int(trade["timestamp"])
    transaction_hash = str(trade.get("transactionHash", ""))
    asset = str(trade.get("asset", ""))
    side = str(trade.get("side", ""))
    price = str(trade.get("price", ""))
    size = str(trade.get("size", ""))
    module = _extension_module()
    if module is not None:
        return tuple(
            module.polymarket_trade_sort_key(timestamp, transaction_hash, asset, side, price, size)
        )  # type: ignore[return-value]
    return _polymarket_trade_sort_key_python(
        timestamp,
        transaction_hash,
        asset,
        side,
        price,
        size,
    )


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
    module = _extension_module()
    if module is not None:
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
    return [_polymarket_trade_sort_key_python(*row) for row in rows]


def polymarket_trade_id(transaction_hash: str, asset: str, sequence: int) -> str:
    module = _extension_module()
    if module is not None:
        return str(module.polymarket_trade_id(transaction_hash, asset, sequence))
    return _polymarket_trade_id_python(transaction_hash, asset, sequence)


def polymarket_trade_ids(rows: Sequence[tuple[str, str, int]]) -> list[str]:
    module = _extension_module()
    if module is not None:
        return [str(value) for value in module.polymarket_trade_ids(list(rows))]
    return [
        _polymarket_trade_id_python(transaction_hash, asset, sequence)
        for transaction_hash, asset, sequence in rows
    ]


def polymarket_normalize_trade_side(side: str) -> str:
    module = _extension_module()
    if module is not None:
        return str(module.polymarket_normalize_trade_side(side))
    return _polymarket_normalize_trade_side_python(side)


def polymarket_normalize_trade_sides(sides: Sequence[str]) -> list[str]:
    module = _extension_module()
    if module is not None:
        return [str(value) for value in module.polymarket_normalize_trade_sides(list(sides))]
    return [_polymarket_normalize_trade_side_python(side) for side in sides]


def polymarket_is_tradable_probability_price(price: str) -> bool:
    module = _extension_module()
    if module is not None:
        return bool(module.polymarket_is_tradable_probability_price(price))
    return _polymarket_is_tradable_probability_price_python(price)


def polymarket_are_tradable_probability_prices(prices: Sequence[str]) -> list[bool]:
    module = _extension_module()
    if module is not None:
        return [
            bool(value) for value in module.polymarket_are_tradable_probability_prices(list(prices))
        ]
    return [_polymarket_is_tradable_probability_price_python(price) for price in prices]


def polymarket_trade_event_timestamp_ns(
    base_timestamp_ns: int,
    occurrence_in_second: int,
) -> int:
    module = _extension_module()
    if module is not None:
        return int(
            module.polymarket_trade_event_timestamp_ns(base_timestamp_ns, occurrence_in_second)
        )
    return _polymarket_trade_event_timestamp_ns_python(base_timestamp_ns, occurrence_in_second)


def polymarket_trade_event_timestamp_ns_batch(
    rows: Sequence[tuple[int, int]],
) -> list[int]:
    module = _extension_module()
    if module is not None:
        return [
            int(value) for value in module.polymarket_trade_event_timestamp_ns_batch(list(rows))
        ]
    return [
        _polymarket_trade_event_timestamp_ns_python(base_timestamp_ns, occurrence_in_second)
        for base_timestamp_ns, occurrence_in_second in rows
    ]


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
    module = _extension_module()
    if module is not None:
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
    return _polymarket_public_trade_rows_python(trades, token_id=token_id, sort=sort)


def replay_merge_plan(
    *,
    book_ts_events: Sequence[int],
    book_ts_inits: Sequence[int],
    trade_ts_events: Sequence[int],
    trade_ts_inits: Sequence[int],
) -> list[tuple[int, int]] | None:
    module = _extension_module()
    if module is None:
        return None
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
    module = _extension_module()
    if module is not None:
        return list(module.pmxt_archive_hours_for_window(start_ns, end_ns))
    return _pmxt_archive_hours_for_window_python(start_ns, end_ns)


def telonex_source_label_kind(source: str) -> str | None:
    module = _extension_module()
    if module is not None:
        value = module.telonex_source_label_kind(source)
        return None if value is None else str(value)
    return _telonex_source_label_kind_python(source)


def telonex_stage_for_source(source: str) -> str:
    module = _extension_module()
    if module is not None:
        return str(module.telonex_stage_for_source(source))
    return _telonex_stage_for_source_python(source)


def telonex_api_url(
    *,
    base_url: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> str:
    module = _extension_module()
    if module is not None:
        return str(
            module.telonex_api_url(base_url, channel, date, market_slug, token_index, outcome)
        )
    return _telonex_api_url_python(base_url, channel, date, market_slug, token_index, outcome)


def _telonex_outcome_cache_segment(token_index: int, outcome: str | None) -> str:
    if outcome:
        return f"outcome={quote_path_component(outcome)}"
    return f"outcome_id={token_index}"


def quote_path_component(value: str) -> str:
    return urlencode({"": value})[1:].replace("+", "%20")


def telonex_api_cache_relative_path(
    *,
    base_url_key: str,
    channel: str,
    date: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> Path:
    module = _extension_module()
    if module is not None:
        return Path(
            str(
                module.telonex_api_cache_relative_path(
                    base_url_key, channel, date, market_slug, token_index, outcome
                )
            )
        )
    return (
        Path("api-days")
        / base_url_key
        / _TELONEX_EXCHANGE
        / channel
        / quote_path_component(market_slug)
        / _telonex_outcome_cache_segment(token_index, outcome)
        / f"{date}.parquet"
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
    module = _extension_module()
    if module is not None:
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
    return (
        Path("book-deltas-v1")
        / _TELONEX_EXCHANGE
        / channel
        / quote_path_component(market_slug)
        / _telonex_outcome_cache_segment(token_index, outcome)
        / f"instrument={instrument_key}"
        / f"{date}.{start_ns}-{end_ns}.parquet"
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
    module = _extension_module()
    if module is not None:
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
    return (
        Path("trade-ticks-v1")
        / _TELONEX_EXCHANGE
        / channel
        / quote_path_component(market_slug)
        / _telonex_outcome_cache_segment(token_index, outcome)
        / f"instrument={instrument_key}"
        / f"{date}.{start_ns}-{end_ns}.parquet"
    )


def telonex_local_consolidated_candidate_paths(
    *,
    root: Path,
    channel: str,
    market_slug: str,
    token_index: int,
    outcome: str | None,
) -> tuple[Path, ...]:
    module = _extension_module()
    if module is not None:
        return tuple(
            Path(path)
            for path in module.telonex_local_consolidated_candidate_paths(
                str(root), channel, market_slug, token_index, outcome
            )
        )
    return tuple(
        Path(path)
        for path in _telonex_local_consolidated_candidate_paths_python(
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
    module = _extension_module()
    if module is not None:
        return tuple(
            Path(path)
            for path in module.telonex_local_daily_candidate_paths(
                str(root), channel, date, market_slug, token_index, outcome
            )
        )
    return tuple(
        Path(path)
        for path in _telonex_local_daily_candidate_paths_python(
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
    "telonex_source_days_for_window_ns",
    "telonex_source_label_kind",
    "telonex_stage_for_source",
    "telonex_trade_ticks_cache_relative_path",
]
