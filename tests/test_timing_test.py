from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

from prediction_market_extensions.backtesting._timing_test import (
    _active_transfer_progress,
    _loader_progress_enabled,
    _progress_bar_description,
    _progress_bar_position,
    _progress_bar_total,
    _text_progress_bar,
    _transfer_label,
    _transfer_progress_fraction,
)


def test_loader_progress_enabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)

    assert _loader_progress_enabled()


def test_loader_progress_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_LOADER_PROGRESS", "0")

    assert not _loader_progress_enabled()


def test_transfer_label_identifies_local_raw_paths() -> None:
    label = _transfer_label(
        "/Volumes/storage/pmxt_data/2026/02/22/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "local raw"


def test_transfer_label_identifies_cache_paths() -> None:
    label = _transfer_label(
        "cache::/Users/example/.cache/nautilus_trader/pmxt/cond/token/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "cache polymarket_orderbook_2026-02-22T11.parquet"


def test_transfer_label_identifies_r2_raw_urls() -> None:
    label = _transfer_label("https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T11.parquet")

    assert label == "r2 raw"


def test_transfer_label_identifies_telonex_sources() -> None:
    cache_label = _transfer_label(
        "telonex-cache::/Users/test/.cache/nautilus_trader/telonex/api-days/hash/"
        "polymarket/book_snapshot_full/slug/outcome_id=0/2026-03-01.parquet"
    )
    fast_cache_label = _transfer_label(
        "telonex-cache-fast::/Users/test/.cache/nautilus_trader/telonex/api-days/hash/"
        "polymarket/book_snapshot_full/slug/outcome_id=0/2026-03-01.fast.parquet"
    )
    deltas_cache_label = _transfer_label(
        "telonex-deltas-cache::/Users/test/.cache/nautilus_trader/telonex/book-deltas-v1/"
        "polymarket/book_snapshot_full/slug/outcome_id=0/instrument=abc/2026-03-01.parquet"
    )
    local_blob_label = _transfer_label("telonex-local::/Volumes/storage/telonex_data")
    local_label = _transfer_label(
        "telonex-local::/Volumes/storage/telonex_data/polymarket/book_snapshot_full/slug/0/2026-03-01.parquet"
    )
    api_label = _transfer_label(
        "telonex-api::https://api.telonex.io/v1/downloads/polymarket/book_snapshot_full/2026-03-01?slug=slug&outcome_id=0"
    )

    assert cache_label == "telonex cache 2026-03-01.parquet"
    assert fast_cache_label == "telonex cache 2026-03-01.fast.parquet"
    assert deltas_cache_label == "telonex deltas cache 2026-03-01.parquet"
    assert local_blob_label == "telonex local"
    assert local_label == "telonex local"
    assert api_label == "telonex api"


def test_progress_bar_description_reports_started_hours_before_completion() -> None:
    description = _progress_bar_description(total_hours=44, started_hours=4, completed_hours=0)

    assert description == "Fetching hours (4/44 started, 4 active)"


def test_progress_bar_description_reports_completion_and_active_work() -> None:
    description = _progress_bar_description(total_hours=44, started_hours=7, completed_hours=3)

    assert description == "Fetching hours (3/44 done, 4 active)"


def test_progress_bar_description_can_report_days() -> None:
    description = _progress_bar_description(
        total_hours=3, started_hours=1, completed_hours=0, item_label="days"
    )

    assert description == "Fetching days (1/3 started, 1 active)"


def test_progress_bar_description_uses_actual_active_transfer_count() -> None:
    description = _progress_bar_description(
        total_hours=44, started_hours=39, completed_hours=0, active_hours=8
    )

    assert description == "Fetching hours (39/44 started, 8 active)"


def test_progress_bar_total_matches_total_hours() -> None:
    assert _progress_bar_total(7) == 7


def test_text_progress_bar_renders_plain_log_bar() -> None:
    assert _text_progress_bar(2, 4, width=8) == "[####----]"
    assert _text_progress_bar(0, 0, width=4) == "[----]"


def test_progress_bar_position_includes_active_transfer_progress() -> None:
    assert _progress_bar_position(total_hours=7, completed_hours=0, active_hours_progress=0.0) == 0
    assert (
        _progress_bar_position(total_hours=7, completed_hours=3, active_hours_progress=1.5) == 4.5
    )


def test_transfer_progress_fraction_uses_download_bytes() -> None:
    assert (
        _transfer_progress_fraction(
            mode="download", downloaded_bytes=50, total_bytes=100, scanned_batches=0
        )
        == 0.45
    )


def test_transfer_progress_fraction_does_not_front_load_local_scan() -> None:
    assert (
        _transfer_progress_fraction(
            mode="scan",
            source="/Volumes/storage/pmxt_data/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
            downloaded_bytes=0,
            total_bytes=100,
            scanned_batches=0,
        )
        == 0.0
    )
    assert _transfer_progress_fraction(
        mode="scan",
        source="/Volumes/storage/pmxt_data/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
        downloaded_bytes=0,
        total_bytes=100,
        scanned_batches=2,
    ) == pytest.approx(2 / 3)


def test_active_transfer_progress_dedupes_by_hour() -> None:
    active_hours, active_progress = _active_transfer_progress(
        {
            "one": {
                "url": "https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T15.parquet",
                "hour_key": "2026-02-22T15:00:00+00:00",
                "mode": "download",
                "downloaded_bytes": 50,
                "total_bytes": 100,
                "scanned_batches": 0,
            },
            "two": {
                "url": "/Volumes/storage/pmxt_data/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
                "hour_key": "2026-02-22T15:00:00+00:00",
                "mode": "scan",
                "downloaded_bytes": 0,
                "total_bytes": 100,
                "scanned_batches": 2,
            },
        }
    )

    assert active_hours == 1
    assert active_progress == pytest.approx(2 / 3)


def test_install_timing_patches_runner_loader_override() -> None:
    from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader
    from prediction_market_extensions.backtesting import _timing_test as timing_module
    from prediction_market_extensions.backtesting.data_sources.pmxt import (
        RunnerPolymarketPMXTDataLoader,
    )

    timing_module = importlib.reload(timing_module)

    method_names = (
        "_load_cached_market_batches",
        "_load_local_archive_market_batches",
        "_load_remote_market_batches",
        "_load_market_batches",
        "_iter_market_batches",
    )
    base_originals = {name: getattr(PolymarketPMXTDataLoader, name) for name in method_names}
    runner_originals = {
        name: getattr(RunnerPolymarketPMXTDataLoader, name) for name in method_names
    }
    runner_shared_original = RunnerPolymarketPMXTDataLoader.load_shared_market_batches_for_hour
    runner_had_own = {
        name: name in RunnerPolymarketPMXTDataLoader.__dict__ for name in method_names
    }
    runner_had_shared = (
        "load_shared_market_batches_for_hour" in RunnerPolymarketPMXTDataLoader.__dict__
    )

    try:
        timing_module.install_timing()

        assert (
            RunnerPolymarketPMXTDataLoader._load_market_batches
            is not runner_originals["_load_market_batches"]
        )
        assert (
            RunnerPolymarketPMXTDataLoader._iter_market_batches
            is not runner_originals["_iter_market_batches"]
        )
        assert (
            PolymarketPMXTDataLoader._load_market_batches
            is not base_originals["_load_market_batches"]
        )
        assert (
            RunnerPolymarketPMXTDataLoader.load_shared_market_batches_for_hour
            is not runner_shared_original
        )
    finally:
        timing_module._installed = False
        for name, original in base_originals.items():
            setattr(PolymarketPMXTDataLoader, name, original)
        for name, original in runner_originals.items():
            if runner_had_own[name]:
                setattr(RunnerPolymarketPMXTDataLoader, name, original)
            elif name in RunnerPolymarketPMXTDataLoader.__dict__:
                delattr(RunnerPolymarketPMXTDataLoader, name)
        if runner_had_shared:
            RunnerPolymarketPMXTDataLoader.load_shared_market_batches_for_hour = (
                runner_shared_original
            )
        elif "load_shared_market_batches_for_hour" in RunnerPolymarketPMXTDataLoader.__dict__:
            delattr(RunnerPolymarketPMXTDataLoader, "load_shared_market_batches_for_hour")


def test_grouped_pmxt_timing_drives_tqdm_progress(monkeypatch) -> None:
    from prediction_market_extensions.backtesting import _timing_test as timing_module
    from prediction_market_extensions.backtesting.data_sources.pmxt import (
        RunnerPolymarketPMXTDataLoader,
    )

    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)
    timing_module = importlib.reload(timing_module)
    updates: list[object] = []

    class FakeTqdm:
        def __init__(self, *, total, desc, unit, leave, bar_format):  # type: ignore[no-untyped-def]
            del desc, unit, leave, bar_format
            self.total = total
            self.n = 0.0
            updates.append(("init", total))

        def update(self, value):  # type: ignore[no-untyped-def]
            self.n += value
            updates.append(("update", value))

        def set_description_str(self, value, *, refresh):  # type: ignore[no-untyped-def]
            del refresh
            updates.append(("desc", value))

        def set_postfix_str(self, value, *, refresh):  # type: ignore[no-untyped-def]
            del refresh
            updates.append(("postfix", value))

        def clear(self, *, nolock):  # type: ignore[no-untyped-def]
            del nolock
            updates.append(("clear", None))

        def close(self):  # type: ignore[no-untyped-def]
            updates.append(("close", None))

    original_shared = RunnerPolymarketPMXTDataLoader.load_shared_market_batches_for_hour
    had_shared = "load_shared_market_batches_for_hour" in RunnerPolymarketPMXTDataLoader.__dict__

    def fake_shared(self, hour, *, requests, batch_size):  # type: ignore[no-untyped-def]
        del hour, requests, batch_size
        self._pmxt_download_progress_callback("https://r2v2.pmxt.dev/hour.parquet", 0, 100, False)
        self._pmxt_download_progress_callback("https://r2v2.pmxt.dev/hour.parquet", 100, 100, True)
        return {}

    monkeypatch.setitem(sys.modules, "tqdm", SimpleNamespace(tqdm=FakeTqdm))
    monkeypatch.setattr(
        RunnerPolymarketPMXTDataLoader,
        "load_shared_market_batches_for_hour",
        fake_shared,
    )

    try:
        timing_module.install_timing()
        loader = object.__new__(RunnerPolymarketPMXTDataLoader)
        loader._pmxt_prefetch_workers = 2
        from prediction_market_extensions._runtime_log import capture_loader_events

        with capture_loader_events() as capture:
            loader.load_shared_market_batches_for_hour(
                "2026-04-22T15:00:00+00:00",
                requests=((0, "condition", "token"),),
                batch_size=1000,
            )
    finally:
        timing_module._installed = False
        if had_shared:
            RunnerPolymarketPMXTDataLoader.load_shared_market_batches_for_hour = original_shared
        elif "load_shared_market_batches_for_hour" in RunnerPolymarketPMXTDataLoader.__dict__:
            delattr(RunnerPolymarketPMXTDataLoader, "load_shared_market_batches_for_hour")

    assert ("init", 1) in updates
    assert any(item[0] == "postfix" and "100 B/100 B" in item[1] for item in updates)
    assert ("close", None) in updates
    assert any("PMXT book progress [" in event.message for event in capture.events)


def test_install_timing_patches_telonex_loader() -> None:
    from prediction_market_extensions.backtesting import _timing_test as timing_module
    from prediction_market_extensions.backtesting.data_sources.telonex import (
        RunnerPolymarketTelonexBookDataLoader,
    )

    timing_module = importlib.reload(timing_module)
    original_load_order_book_deltas = RunnerPolymarketTelonexBookDataLoader.load_order_book_deltas

    try:
        timing_module.install_timing()

        assert (
            RunnerPolymarketTelonexBookDataLoader.load_order_book_deltas
            is not original_load_order_book_deltas
        )
    finally:
        timing_module._installed = False
        RunnerPolymarketTelonexBookDataLoader.load_order_book_deltas = (
            original_load_order_book_deltas
        )
