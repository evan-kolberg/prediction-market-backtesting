from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

from prediction_market_extensions.backtesting._timing_test import (
    _active_transfer_progress,
    _format_completed_hour_line,
    _progress_bar_description,
    _progress_bar_position,
    _progress_bar_total,
    _transfer_label,
    _transfer_progress_fraction,
)


def test_transfer_label_identifies_local_raw_paths() -> None:
    label = _transfer_label(
        "/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "local raw 2026-02-22T11"


def test_transfer_label_identifies_cache_paths() -> None:
    label = _transfer_label(
        "cache::/Users/example/.cache/nautilus_trader/pmxt/cond/token/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "cache 2026-02-22T11"


def test_transfer_label_identifies_r2_raw_urls() -> None:
    label = _transfer_label("https://r2v2.pmxt.dev/polymarket_orderbook_2026-02-22T11.parquet")

    assert label == "r2 raw 2026-02-22T11"


def test_transfer_label_identifies_telonex_sources() -> None:
    cache_label = _transfer_label(
        "telonex-cache::/Users/test/.cache/nautilus_trader/telonex/api-days/hash/"
        "polymarket/quotes/slug/outcome_id=0/2026-03-01.parquet"
    )
    local_blob_label = _transfer_label("telonex-local::/Volumes/LaCie/telonex_data")
    local_label = _transfer_label(
        "telonex-local::/Volumes/LaCie/telonex_data/polymarket/quotes/slug/0/2026-03-01.parquet"
    )
    api_label = _transfer_label(
        "telonex-api::https://api.telonex.io/v1/downloads/polymarket/quotes/2026-03-01?slug=slug&outcome_id=0"
    )

    assert cache_label == "telonex cache 2026-03-01.parquet"
    assert local_blob_label == "telonex local telonex_data"
    assert local_label == "telonex local 2026-03-01.parquet"
    assert api_label == "telonex api 2026-03-01"


def test_format_completed_hour_line_keeps_long_elapsed_values_aligned() -> None:
    line = _format_completed_hour_line(
        datetime(2026, 2, 22, 1, tzinfo=timezone.utc),
        elapsed=22.608,
        rows=4156,
        source="local-raw::/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T01.parquet",
    )

    assert line == "  2026-02-22T01:00:00+00:00     22.608s      4156 rows  local raw 2026-02-22T01"


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
            source="/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
            downloaded_bytes=0,
            total_bytes=100,
            scanned_batches=0,
        )
        == 0.0
    )
    assert _transfer_progress_fraction(
        mode="scan",
        source="/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
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
                "url": "/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T15.parquet",
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
    runner_had_own = {
        name: name in RunnerPolymarketPMXTDataLoader.__dict__ for name in method_names
    }

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
    finally:
        timing_module._installed = False
        for name, original in base_originals.items():
            setattr(PolymarketPMXTDataLoader, name, original)
        for name, original in runner_originals.items():
            if runner_had_own[name]:
                setattr(RunnerPolymarketPMXTDataLoader, name, original)
            elif name in RunnerPolymarketPMXTDataLoader.__dict__:
                delattr(RunnerPolymarketPMXTDataLoader, name)


def test_install_timing_patches_telonex_loader() -> None:
    from prediction_market_extensions.backtesting import _timing_test as timing_module
    from prediction_market_extensions.backtesting.data_sources.telonex import (
        RunnerPolymarketTelonexQuoteDataLoader,
    )

    timing_module = importlib.reload(timing_module)
    original = RunnerPolymarketTelonexQuoteDataLoader.load_quotes

    try:
        timing_module.install_timing()

        assert RunnerPolymarketTelonexQuoteDataLoader.load_quotes is not original
    finally:
        timing_module._installed = False
        RunnerPolymarketTelonexQuoteDataLoader.load_quotes = original
