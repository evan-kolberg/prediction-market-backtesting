from __future__ import annotations

import argparse
import importlib
import importlib.machinery
import importlib.util
import os
import statistics
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument

import prediction_market_extensions._native as native
from prediction_market_extensions.backtesting.data_sources.telonex import (
    RunnerPolymarketTelonexBookDataLoader,
)


APR_21_2026_NS = 1_776_729_600_000_000_000
NANOS_PER_HOUR = 3_600_000_000_000


def _configure_native(enabled: bool):
    os.environ[native.NATIVE_ENV] = "1" if enabled else "0"
    os.environ[native.NATIVE_REQUIRE_ENV] = "1" if enabled else "0"
    return importlib.reload(native)


def _load_extension_from_path(path: Path):
    loader = importlib.machinery.ExtensionFileLoader(
        "prediction_market_extensions._native_ext",
        str(path),
    )
    spec = importlib.util.spec_from_loader(
        "prediction_market_extensions._native_ext",
        loader,
    )
    if spec is None:
        raise RuntimeError(f"Could not load native extension from {path}")
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def _configure_native_extension(enabled: bool, extension_path: Path | None):
    mod = _configure_native(enabled)
    if not enabled or extension_path is None:
        return mod

    extension_module = _load_extension_from_path(extension_path)

    def _import_module_override(name: str):
        if name == "prediction_market_extensions._native_ext":
            return extension_module
        return importlib.import_module(name)

    mod.import_module = _import_module_override
    mod._EXTENSION = None
    return mod


def _time_call(fn: Callable[[], object], repeats: int) -> list[float]:
    timings: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - started)
    return timings


def _payloads(items: int) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for index in range(items):
        ts = f"1771767624.{index % 1_000_000:06d}"
        if index % 5 == 0:
            rows.append(
                (
                    "book_snapshot",
                    (
                        '{"update_type":"book_snapshot","market_id":"condition-123",'
                        f'"token_id":"token-yes-123","timestamp":{ts},'
                        '"bids":[["0.48","11.0"]],"asks":[["0.52","9.0"]]}'
                    ),
                )
            )
        else:
            rows.append(
                (
                    "price_change",
                    (
                        '{"update_type":"price_change","market_id":"condition-123",'
                        f'"token_id":"token-yes-123","timestamp":{ts},'
                        '"change_side":"BUY","change_price":"0.51","change_size":"13.5"}'
                    ),
                )
            )
    return rows


def _telonex_inputs(items: int) -> list[tuple[str, str, str, int, str | None]]:
    rows: list[tuple[str, str, str, int, str | None]] = []
    for index in range(items):
        rows.append(
            (
                "book_snapshot_full",
                f"2026-04-{21 + (index % 7):02d}",
                f"example-market-{index % 101}",
                index % 2,
                "Yes" if index % 3 == 0 else None,
            )
        )
    return rows


def _make_telonex_loader() -> RunnerPolymarketTelonexBookDataLoader:
    instrument = parse_polymarket_instrument(
        market_info={
            "condition_id": "0x" + "1" * 64,
            "question": "Synthetic Telonex benchmark market",
            "minimum_tick_size": "0.01",
            "minimum_order_size": "1",
            "end_date_iso": "2026-12-31T00:00:00Z",
            "maker_base_fee": "0",
            "taker_base_fee": "0",
        },
        token_id="2" * 64,
        outcome="Yes",
        ts_init=0,
    )
    loader = RunnerPolymarketTelonexBookDataLoader.__new__(RunnerPolymarketTelonexBookDataLoader)
    loader._instrument = instrument
    loader._token_id = "2" * 64
    loader._condition_id = "0x" + "1" * 64
    return loader


def _telonex_flat_frame(items: int) -> pd.DataFrame:
    timestamp_start_us = APR_21_2026_NS // 1_000
    bid_prices: list[list[str]] = []
    bid_sizes: list[list[str]] = []
    ask_prices: list[list[str]] = []
    ask_sizes: list[list[str]] = []
    for index in range(items):
        bid_prices.append(["0.40", "0.39", "0.38"])
        ask_prices.append(["0.61", "0.62", "0.63"])
        bid_sizes.append([f"{10 + index % 17}", f"{7 + index % 11}", "5"])
        ask_sizes.append([f"{9 + index % 13}", f"{8 + index % 7}", "4"])
    return pd.DataFrame(
        {
            "timestamp_us": [timestamp_start_us + index * 1_000_000 for index in range(items)],
            "bid_prices": bid_prices,
            "bid_sizes": bid_sizes,
            "ask_prices": ask_prices,
            "ask_sizes": ask_sizes,
        }
    )


def _telonex_trade_frame(items: int) -> pd.DataFrame:
    timestamp_start_us = APR_21_2026_NS // 1_000
    return pd.DataFrame(
        {
            "timestamp_us": [timestamp_start_us + index * 1_000 for index in range(items)],
            "price": [0.40 + (index % 10) * 0.01 for index in range(items)],
            "size": [1.0 + (index % 17) for index in range(items)],
            "side": ["buy" if index % 2 == 0 else "sell" for index in range(items)],
            "transaction_hash": [f"0xsynthetic{index // 2:024x}" for index in range(items)],
        }
    )


def _bench_native_mode(
    *,
    enabled: bool,
    items: int,
    telonex_events: int,
    repeats: int,
    pmxt_rows: list[tuple[str, str]],
    telonex_rows: list[tuple[str, str, str, int, str | None]],
    telonex_frame: pd.DataFrame,
    telonex_trade_frame: pd.DataFrame,
    native_extension_path: Path | None,
) -> dict[str, float | bool]:
    mod = _configure_native_extension(enabled, native_extension_path)
    mod.native_available()
    root = Path("/Volumes/LaCie/telonex_data")
    base_url = "https://api.telonex.io/"
    start_ns = APR_21_2026_NS + 9 * NANOS_PER_HOUR + 15 * 60_000_000_000
    end_ns = APR_21_2026_NS + 10 * NANOS_PER_HOUR + 10 * 60_000_000_000

    def pmxt_payload_sort_key_loop() -> int:
        total = 0
        for update_type, payload in pmxt_rows:
            timestamp_ns, priority = mod.pmxt_payload_sort_key(update_type, payload)
            total += timestamp_ns + priority
        return total

    def pmxt_payload_sort_keys_batch() -> int:
        return sum(
            timestamp_ns + priority
            for timestamp_ns, priority in mod.pmxt_payload_sort_keys(pmxt_rows)
        )

    def pmxt_sort_payloads_batch() -> int:
        return sum(
            timestamp_ns + priority
            for timestamp_ns, priority, _update_type, _payload in mod.pmxt_sort_payloads(pmxt_rows)
        )

    def pmxt_extract_payload_fields_loop() -> int:
        total = 0
        for _update_type, payload in pmxt_rows:
            fields = mod.pmxt_extract_payload_fields(payload)
            total += fields[2]
        return total

    def telonex_path_loop() -> int:
        total = 0
        for channel, date, market_slug, token_index, outcome in telonex_rows:
            total += len(
                mod.telonex_local_daily_candidate_paths(
                    root=root,
                    channel=channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
            )
            total += len(
                mod.telonex_local_consolidated_candidate_paths(
                    root=root,
                    channel=channel,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
            )
            total += len(
                str(
                    mod.telonex_api_cache_relative_path(
                        base_url_key="api.telonex.io",
                        channel=channel,
                        date=date,
                        market_slug=market_slug,
                        token_index=token_index,
                        outcome=outcome,
                    )
                )
            )
            total += len(
                mod.telonex_api_url(
                    base_url=base_url,
                    channel=channel,
                    date=date,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                )
            )
        return total

    def window_planner_loop() -> int:
        total = 0
        for index in range(items):
            start = start_ns + index * NANOS_PER_HOUR
            end = end_ns + index * NANOS_PER_HOUR
            total += len(mod.pmxt_archive_hours_for_window_ns(start, end))
            total += len(mod.telonex_source_days_for_window_ns(start, end))
            day_window = mod.telonex_day_window_ns("2026-04-21", start_ns, end_ns)
            if day_window is not None:
                total += day_window[1] - day_window[0]
        return total

    def telonex_flat_book_events() -> int:
        loader = _make_telonex_loader()
        events = loader._book_events_from_frame(
            telonex_frame,
            start=pd.Timestamp(APR_21_2026_NS, unit="ns", tz="UTC"),
            end=pd.Timestamp(
                APR_21_2026_NS + (telonex_events + 1) * 1_000_000_000,
                unit="ns",
                tz="UTC",
            ),
            include_order_book=True,
        )
        return sum(len(record.deltas) for record in events)

    def telonex_onchain_fill_trade_ticks() -> int:
        loader = _make_telonex_loader()
        trades = loader._onchain_fill_trade_ticks_from_frame(
            telonex_trade_frame,
            start=pd.Timestamp(APR_21_2026_NS, unit="ns", tz="UTC"),
            end=pd.Timestamp(
                APR_21_2026_NS + (telonex_events + 1) * 1_000_000,
                unit="ns",
                tz="UTC",
            ),
        )
        return len(trades)

    cases = {
        "pmxt_payload_sort_key_loop": pmxt_payload_sort_key_loop,
        "pmxt_payload_sort_keys_batch": pmxt_payload_sort_keys_batch,
        "pmxt_sort_payloads_batch": pmxt_sort_payloads_batch,
        "pmxt_extract_payload_fields_loop": pmxt_extract_payload_fields_loop,
        "telonex_path_loop": telonex_path_loop,
        "window_planner_loop": window_planner_loop,
        "telonex_flat_book_events": telonex_flat_book_events,
        "telonex_onchain_fill_trade_ticks": telonex_onchain_fill_trade_ticks,
    }
    results: dict[str, float | bool] = {"native_available": bool(mod.native_available())}
    for name, fn in cases.items():
        fn()
        timings = _time_call(fn, repeats)
        results[name] = statistics.median(timings)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark native vs Python loader helper paths.")
    parser.add_argument("--items", type=int, default=20_000)
    parser.add_argument("--telonex-events", type=int, default=5_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--native-extension-path",
        type=Path,
        default=None,
        help="Optional extension artifact to use for native-mode timings.",
    )
    args = parser.parse_args()

    pmxt_rows = _payloads(args.items)
    telonex_rows = _telonex_inputs(args.items)
    telonex_frame = _telonex_flat_frame(args.telonex_events)
    telonex_trade_frame = _telonex_trade_frame(args.telonex_events)

    native_results = _bench_native_mode(
        enabled=True,
        items=args.items,
        telonex_events=args.telonex_events,
        repeats=args.repeats,
        pmxt_rows=pmxt_rows,
        telonex_rows=telonex_rows,
        telonex_frame=telonex_frame,
        telonex_trade_frame=telonex_trade_frame,
        native_extension_path=args.native_extension_path,
    )
    python_results = _bench_native_mode(
        enabled=False,
        items=args.items,
        telonex_events=args.telonex_events,
        repeats=args.repeats,
        pmxt_rows=pmxt_rows,
        telonex_rows=telonex_rows,
        telonex_frame=telonex_frame,
        telonex_trade_frame=telonex_trade_frame,
        native_extension_path=args.native_extension_path,
    )

    print(f"items={args.items} telonex_events={args.telonex_events} repeats={args.repeats}")
    if args.native_extension_path is not None:
        print(f"native_extension_path={args.native_extension_path}")
    print(f"native_available(native mode)={native_results['native_available']}")
    print(f"native_available(python fallback)={python_results['native_available']}")
    print("case,native_s,python_s,speedup")
    for key in native_results:
        if key == "native_available":
            continue
        native_s = float(native_results[key])
        python_s = float(python_results[key])
        speedup = python_s / native_s if native_s else float("inf")
        print(f"{key},{native_s:.6f},{python_s:.6f},{speedup:.2f}x")


if __name__ == "__main__":
    main()
