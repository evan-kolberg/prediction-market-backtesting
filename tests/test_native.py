from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pytest

import prediction_market_extensions._native as native
from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader
from prediction_market_extensions.backtesting.data_sources.telonex import (
    RunnerPolymarketTelonexBookDataLoader,
)

APR_21_2026_NS = 1_776_729_600_000_000_000
APR_28_2026_NS = 1_777_334_400_000_000_000
APR_27_2026_END_NS = 1_777_334_399_999_999_999
NANOS_PER_HOUR = 3_600_000_000_000
NANOS_PER_MINUTE = 60_000_000_000


def test_source_days_for_half_open_week_excludes_boundary_day() -> None:
    assert native.source_days_for_window_ns(
        APR_21_2026_NS,
        APR_28_2026_NS,
        semantics="half_open",
    ) == [
        "2026-04-21",
        "2026-04-22",
        "2026-04-23",
        "2026-04-24",
        "2026-04-25",
        "2026-04-26",
        "2026-04-27",
    ]


def test_source_days_for_inclusive_boundary_includes_boundary_day() -> None:
    assert (
        native.source_days_for_window_ns(
            APR_21_2026_NS,
            APR_28_2026_NS,
            semantics="inclusive",
        )[-1]
        == "2026-04-28"
    )


def test_source_days_for_inclusive_end_of_day_matches_half_open_boundary() -> None:
    assert native.source_days_for_window_ns(
        APR_21_2026_NS,
        APR_27_2026_END_NS,
        semantics="inclusive",
    ) == native.source_days_for_window_ns(
        APR_21_2026_NS,
        APR_28_2026_NS,
        semantics="half_open",
    )


def test_telonex_date_range_uses_shared_native_window_planner() -> None:
    assert RunnerPolymarketTelonexBookDataLoader._date_range(
        pd.Timestamp("2026-04-21T00:00:00Z"),
        pd.Timestamp("2026-04-27T23:59:59.999999999Z"),
    ) == [
        "2026-04-21",
        "2026-04-22",
        "2026-04-23",
        "2026-04-24",
        "2026-04-25",
        "2026-04-26",
        "2026-04-27",
    ]


def test_telonex_source_helpers_use_native_planning_and_labels(tmp_path) -> None:
    assert native.telonex_source_label_kind("telonex-deltas-cache::/tmp/day.parquet") == "cache"
    assert native.telonex_source_label_kind("telonex-local::/tmp/day.parquet") == "local"
    assert native.telonex_source_label_kind("telonex-api::https://api.telonex.io") == "remote"
    assert native.telonex_stage_for_source("telonex-cache::/tmp/day.parquet") == "cache_read"
    assert native.telonex_stage_for_source("telonex-api::https://api.telonex.io") == "fetch"
    assert native.telonex_day_window_ns(
        "2026-04-21",
        APR_21_2026_NS + NANOS_PER_HOUR,
        APR_21_2026_NS + 2 * NANOS_PER_HOUR,
    ) == (APR_21_2026_NS + NANOS_PER_HOUR, APR_21_2026_NS + 2 * NANOS_PER_HOUR)
    assert native.telonex_api_url(
        base_url="https://api.telonex.io/",
        channel="book_snapshot_full",
        date="2026-04-21",
        market_slug="demo market",
        token_index=0,
        outcome="Yes",
    ) == (
        "https://api.telonex.io/v1/downloads/polymarket/book_snapshot_full/2026-04-21"
        "?slug=demo+market&outcome=Yes"
    )
    assert (
        native.telonex_local_consolidated_candidate_paths(
            root=tmp_path,
            channel="book_snapshot_full",
            market_slug="demo-market",
            token_index=0,
            outcome="Yes",
        )[0]
        == tmp_path / "polymarket" / "demo-market" / "Yes" / "book_snapshot_full.parquet"
    )
    assert native.telonex_api_cache_relative_path(
        base_url_key="abc123",
        channel="book_snapshot_full",
        date="2026-04-21",
        market_slug="demo market",
        token_index=0,
        outcome="Yes",
    ) == Path(
        "api-days/abc123/polymarket/book_snapshot_full/demo%20market/outcome=Yes/2026-04-21.parquet"
    )
    assert native.telonex_deltas_cache_relative_path(
        channel="book_snapshot_full",
        date="2026-04-21",
        market_slug="demo market",
        token_index=0,
        outcome="Yes",
        instrument_key="inst123",
        start_ns=APR_21_2026_NS,
        end_ns=APR_21_2026_NS + NANOS_PER_HOUR,
    ) == Path(
        "book-deltas-v1/polymarket/book_snapshot_full/demo%20market/outcome=Yes/"
        f"instrument=inst123/2026-04-21.{APR_21_2026_NS}-{APR_21_2026_NS + NANOS_PER_HOUR}.parquet"
    )


def test_telonex_flat_book_snapshot_diff_rows_uses_native_diff_engine() -> None:
    rows = native.telonex_flat_book_snapshot_diff_rows(
        timestamp_ns=[90, 100, 110, 120],
        bid_prices=[
            ["0.10"],
            ["0.10", "0.09"],
            ["0.10", "0.08"],
            ["0.08"],
        ],
        bid_sizes=[
            ["1"],
            ["1", "2"],
            ["3", "4"],
            ["4"],
        ],
        ask_prices=[
            ["0.80"],
            ["0.80", "0.90"],
            ["0.80", "0.95"],
            ["0.95"],
        ],
        ask_sizes=[
            ["1"],
            ["1", "2"],
            ["3", "4"],
            ["4"],
        ],
        start_ns=100,
        end_ns=120,
    )

    assert rows is not None
    (
        first_snapshot_index,
        event_indexes,
        actions,
        sides,
        prices,
        sizes,
        flags,
        sequences,
        ts,
        ts_init,
    ) = rows
    assert first_snapshot_index == 1
    assert event_indexes == [0, 0, 0, 0, 0, 0, 1, 1]
    assert actions == [2, 3, 2, 2, 3, 2, 3, 3]
    assert sides == [1, 1, 1, 2, 2, 2, 1, 2]
    assert prices == [0.08, 0.09, 0.10, 0.95, 0.90, 0.80, 0.10, 0.80]
    assert sizes == [4.0, 0.0, 3.0, 4.0, 0.0, 3.0, 0.0, 0.0]
    assert flags == [0, 0, 0, 0, 0, 128, 0, 128]
    assert sequences == [1, 2, 3, 4, 5, 6, 1, 2]
    assert ts == [110, 110, 110, 110, 110, 110, 120, 120]
    assert ts_init == ts


def test_telonex_onchain_fill_trade_rows_normalizes_execution_ticks() -> None:
    rows = native.telonex_onchain_fill_trade_rows(
        timestamp_ns=[99, 100, 100, 101, 102],
        prices=["0.20", "bad", "0.42", "0.43", "0.44"],
        sizes=["1", "8", "9", "10", "11"],
        sides=["buy", "sell", "ask", "taker-sell", "none"],
        ids=[
            "pre",
            "bad",
            "txabcdefghijklmnopqrstuvwxyz",
            "txabcdefghijklmnopqrstuvwxyz",
            "nan",
        ],
        start_ns=100,
        end_ns=102,
        token_suffix="3456",
    )

    assert rows is not None
    prices, sizes, aggressor_sides, trade_ids, ts_events, ts_inits = rows
    assert prices == [0.42, 0.43, 0.44]
    assert sizes == [9.0, 10.0, 11.0]
    assert aggressor_sides == [2, 2, 0]
    assert trade_ids == [
        "cdefghijklmnopqrstuvwxyz-3456-000000",
        "cdefghijklmnopqrstuvwxyz-3456-000001",
        "telonex-102-3456-000000",
    ]
    assert ts_events == [100, 101, 102]
    assert ts_inits == ts_events


def test_pmxt_archive_hour_planner_includes_prior_snapshot_hour_and_final_hour() -> None:
    start_ns = APR_21_2026_NS + 9 * NANOS_PER_HOUR + 15 * NANOS_PER_MINUTE
    end_ns = APR_21_2026_NS + 10 * NANOS_PER_HOUR + 10 * NANOS_PER_MINUTE

    assert native.pmxt_archive_hours_for_window_ns(start_ns, end_ns) == [
        APR_21_2026_NS + 8 * NANOS_PER_HOUR,
        APR_21_2026_NS + 9 * NANOS_PER_HOUR,
        APR_21_2026_NS + 10 * NANOS_PER_HOUR,
    ]


def test_pmxt_loader_archive_hours_uses_shared_native_window_planner() -> None:
    assert PolymarketPMXTDataLoader._archive_hours(
        pd.Timestamp("2026-04-21T09:15:00Z"),
        pd.Timestamp("2026-04-21T10:10:00Z"),
    ) == [
        pd.Timestamp("2026-04-21T08:00:00Z"),
        pd.Timestamp("2026-04-21T09:00:00Z"),
        pd.Timestamp("2026-04-21T10:00:00Z"),
    ]


def test_decimal_seconds_to_ns_matches_pmxt_timestamp_precision() -> None:
    assert native.decimal_seconds_to_ns(1771767624.001295) == 1_771_767_624_001_295_000
    assert PolymarketPMXTDataLoader._timestamp_to_ns(1771767624.001295) == (
        1_771_767_624_001_295_000
    )


def test_float_seconds_to_ms_string_matches_existing_pmxt_format() -> None:
    assert native.float_seconds_to_ms_string(1771767624.001295) == "1771767624001.295166"
    assert PolymarketPMXTDataLoader._timestamp_to_ms_string(1771767624.001295) == (
        "1771767624001.295166"
    )


def test_pmxt_payload_sort_key_uses_native_timestamp_extraction() -> None:
    payload_text = (
        '{"update_type":"book_snapshot","market_id":"condition-123",'
        '"token_id":"token-yes-123","timestamp":1771767624.001295,'
        '"bids":[],"asks":[]}'
    )
    loader = object.__new__(PolymarketPMXTDataLoader)

    assert native.pmxt_payload_sort_key("book_snapshot", payload_text) == (
        1_771_767_624_001_295_000,
        0,
    )
    assert loader._payload_sort_key("book_snapshot", payload_text) == (
        1_771_767_624_001_295_000,
        0,
    )
    assert native.pmxt_payload_sort_key("unknown", "{}") == (0, 2)
    assert native.pmxt_payload_sort_keys(
        [
            ("price_change", '{"update_type":"price_change","timestamp":1771767624.001296}'),
            ("book_snapshot", payload_text),
        ]
    ) == [
        (1_771_767_624_001_296_000, 1),
        (1_771_767_624_001_295_000, 0),
    ]
    assert native.pmxt_sort_payloads(
        [
            ("price_change", '{"update_type":"price_change","timestamp":1771767624.001296}'),
            ("book_snapshot", payload_text),
        ]
    ) == [
        (1_771_767_624_001_295_000, 0, "book_snapshot", payload_text),
        (
            1_771_767_624_001_296_000,
            1,
            "price_change",
            '{"update_type":"price_change","timestamp":1771767624.001296}',
        ),
    ]
    assert native.pmxt_extract_payload_fields(payload_text) == (
        "book_snapshot",
        "book_snapshot",
        1_771_767_624_001_295_000,
        "condition-123",
        "token-yes-123",
        None,
    )


def test_pmxt_sort_payload_columns_sorts_without_row_tuple_input() -> None:
    payload_text = (
        '{"update_type":"book_snapshot","market_id":"condition-123",'
        '"token_id":"token-yes-123","timestamp":1771767624.001295,'
        '"bids":[],"asks":[]}'
    )
    price_change = '{"update_type":"price_change","timestamp":1771767624.001296}'

    assert native.pmxt_sort_payload_columns(
        [["price_change"], ["book_snapshot"]],
        [[price_change], [payload_text]],
    ) == [
        (1_771_767_624_001_295_000, 0, "book_snapshot", payload_text),
        (1_771_767_624_001_296_000, 1, "price_change", price_change),
    ]


def test_polymarket_trade_helpers_use_native_sort_and_id_logic() -> None:
    trade = {
        "timestamp": "1771767624",
        "transactionHash": "0x1234567890abcdef1234567890abcdef",
        "asset": "asset9876",
        "side": "BUY",
        "price": "0.42",
        "size": "10",
    }

    assert native.polymarket_trade_sort_key(trade) == (
        1_771_767_624,
        "0x1234567890abcdef1234567890abcdef",
        "asset9876",
        "BUY",
        "0.42",
        "10",
    )
    assert (
        native.polymarket_trade_id(
            "0x1234567890abcdef1234567890abcdef",
            "asset9876",
            42,
        )
        == "90abcdef1234567890abcdef-9876-000042"
    )
    assert native.polymarket_trade_ids(
        [
            ("0x1234567890abcdef1234567890abcdef", "asset9876", 42),
            ("0x1234567890abcdef1234567890abcdef", "asset9876", 43),
        ]
    ) == [
        "90abcdef1234567890abcdef-9876-000042",
        "90abcdef1234567890abcdef-9876-000043",
    ]
    assert native.polymarket_normalize_trade_sides(["BUY", " sell ", "mint"]) == [
        "BUY",
        "SELL",
        "unknown",
    ]
    assert native.polymarket_are_tradable_probability_prices(["0.42", "0", "1", "nan"]) == [
        True,
        False,
        False,
        False,
    ]
    assert native.polymarket_trade_event_timestamp_ns_batch(
        [(1_771_767_624_000_000_000, 0), (1_771_767_624_000_000_000, 42)]
    ) == [1_771_767_624_000_000_000, 1_771_767_624_000_000_042]


def test_native_can_be_disabled_forces_python_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(native.NATIVE_ENV, "0")
    monkeypatch.setenv(native.NATIVE_REQUIRE_ENV, "0")

    assert native.native_available() is False
    assert (
        native.source_days_for_window_ns(
            APR_21_2026_NS,
            APR_28_2026_NS,
            semantics="half-open",
        )[-1]
        == "2026-04-27"
    )
    assert native.pmxt_archive_hours_for_window_ns(
        APR_21_2026_NS,
        APR_21_2026_NS + NANOS_PER_HOUR,
    ) == [
        APR_21_2026_NS - NANOS_PER_HOUR,
        APR_21_2026_NS,
        APR_21_2026_NS + NANOS_PER_HOUR,
    ]
    assert native.decimal_seconds_to_ns(1771767624.001295) == 1_771_767_624_001_295_000
    assert native.float_seconds_to_ms_string(1771767624.001295) == "1771767624001.295166"
    assert native.pmxt_payload_sort_key(
        "price_change",
        '{"update_type":"price_change","timestamp":1771767624.001296}',
    ) == (1_771_767_624_001_296_000, 1)
    assert native.pmxt_sort_payload_columns(
        [["price_change"], ["book_snapshot"]],
        [
            ['{"update_type":"price_change","timestamp":1771767624.001296}'],
            ['{"update_type":"book_snapshot","timestamp":1771767624.001295}'],
        ],
    ) == [
        (
            1_771_767_624_001_295_000,
            0,
            "book_snapshot",
            '{"update_type":"book_snapshot","timestamp":1771767624.001295}',
        ),
        (
            1_771_767_624_001_296_000,
            1,
            "price_change",
            '{"update_type":"price_change","timestamp":1771767624.001296}',
        ),
    ]
    assert (
        native.polymarket_trade_id(
            "0x1234567890abcdef1234567890abcdef",
            "asset9876",
            42,
        )
        == "90abcdef1234567890abcdef-9876-000042"
    )


def test_native_require_raises_when_extension_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(native)
    monkeypatch.setenv(module.NATIVE_REQUIRE_ENV, "1")
    monkeypatch.setattr(module, "import_module", lambda _name: (_ for _ in ()).throw(ImportError))

    try:
        with pytest.raises(RuntimeError, match="PREDICTION_MARKET_NATIVE_REQUIRE=1"):
            module.native_available()
    finally:
        importlib.reload(module)
