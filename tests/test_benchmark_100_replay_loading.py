from __future__ import annotations

import argparse
import asyncio
import os
import sys
from types import SimpleNamespace

from scripts import benchmark_100_replay_loading


def _args(*, show_events: bool = False, show_progress: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        replay_workers=64,
        materialize_workers=4,
        source="cache",
        vendor="pmxt",
        telonex_api_workers=None,
        telonex_prefetch_workers=None,
        telonex_cache_prefetch_workers=None,
        telonex_file_workers=None,
        telonex_local_prefetch_workers=None,
        pmxt_prefetch_workers=None,
        pmxt_cache_prefetch_workers=None,
        pmxt_row_group_chunk_size=None,
        pmxt_row_group_scan_workers=None,
        pmxt_grouped_market_chunk_size=None,
        limit=None,
        offset=0,
        sample_interval=0.5,
        memory_limit_gb=24.0,
        time_limit_secs=None,
        case_name=None,
        show_events=show_events,
        show_progress=show_progress,
    )


def test_benchmark_disables_progress_for_quiet_measurements(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)
    monkeypatch.delenv("BACKTEST_ENABLE_TIMING", raising=False)

    benchmark_100_replay_loading._apply_worker_env(_args())

    assert os.environ["BACKTEST_LOADER_PROGRESS"] == "0"
    assert os.environ["BACKTEST_ENABLE_TIMING"] == "0"


def test_benchmark_show_events_enables_progress_harness(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)
    monkeypatch.delenv("BACKTEST_ENABLE_TIMING", raising=False)

    benchmark_100_replay_loading._apply_worker_env(_args(show_events=True))

    assert os.environ["BACKTEST_LOADER_PROGRESS"] == "1"
    assert os.environ["BACKTEST_ENABLE_TIMING"] == "1"


def test_benchmark_show_progress_enables_progress_harness(monkeypatch) -> None:
    monkeypatch.delenv("BACKTEST_LOADER_PROGRESS", raising=False)
    monkeypatch.delenv("BACKTEST_ENABLE_TIMING", raising=False)

    benchmark_100_replay_loading._apply_worker_env(_args(show_progress=True))

    assert os.environ["BACKTEST_LOADER_PROGRESS"] == "1"
    assert os.environ["BACKTEST_ENABLE_TIMING"] == "1"


def test_benchmark_visible_mode_installs_timing_harness(monkeypatch) -> None:
    installed: list[bool] = []

    class FakeBacktest:
        async def _load_sims_async(self):  # type: ignore[no-untyped-def]
            return []

    class FakeSampler:
        max_rss_bytes = 0

        def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    monkeypatch.setitem(
        sys.modules,
        "prediction_market_extensions.backtesting._timing_harness",
        SimpleNamespace(install_timing_harness=lambda: installed.append(True)),
    )
    monkeypatch.setattr(
        benchmark_100_replay_loading,
        "_build_backtest",
        lambda **kwargs: FakeBacktest(),
    )
    monkeypatch.setattr(benchmark_100_replay_loading, "MemorySampler", FakeSampler)

    asyncio.run(benchmark_100_replay_loading._load_once(_args(show_progress=True)))

    assert installed == [True]
