from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import threading
import time
from collections.abc import Sequence
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import psutil
from dotenv import load_dotenv


def _ensure_repo_root() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)
    if str(repo_root) not in os.sys.path:
        os.sys.path.insert(0, str(repo_root))


def _set_env(name: str, value: int | str | None) -> None:
    if value is None:
        return
    os.environ[name] = str(value)


def _source_tuple(vendor: str, source: str) -> tuple[str, ...]:
    if vendor == "telonex":
        if source == "api":
            return ("api:${TELONEX_API_KEY}",)
        if source in {"local", "cache"}:
            return ("local:/Volumes/storage/telonex_data", "api:${TELONEX_API_KEY}")
    if vendor == "pmxt":
        if source in {"local", "cache"}:
            return (
                "local:/Volumes/storage/pmxt_data",
                "archive:r2v2.pmxt.dev",
                "archive:r2.pmxt.dev",
            )
    raise ValueError(f"Unsupported vendor/source pair: {vendor}/{source}")


def _replays_for_vendor(
    vendor: str,
    *,
    limit: int | None = None,
    offset: int = 0,
) -> tuple[Any, ...]:
    from prediction_market_extensions.backtesting._replay_specs import BookReplay

    if vendor == "telonex":
        from backtests.polymarket_telonex_book_100_replay_runner import (
            POPULAR_MARKET_SLUGS,
            WINDOW_END,
            WINDOW_END_NS,
            WINDOW_START,
            WINDOW_START_NS,
        )
    elif vendor == "pmxt":
        from backtests.polymarket_pmxt_book_100_replay_runner import (
            POPULAR_MARKET_SLUGS,
            WINDOW_END,
            WINDOW_END_NS,
            WINDOW_START,
            WINDOW_START_NS,
        )
    else:
        raise ValueError(f"Unsupported vendor: {vendor}")

    slugs = POPULAR_MARKET_SLUGS[offset:]
    if limit is not None:
        slugs = slugs[:limit]
    return tuple(
        BookReplay(
            market_slug=slug,
            token_index=0,
            start_time=WINDOW_START,
            end_time=WINDOW_END,
            metadata={
                "sim_label": slug,
                "replay_window_start_ns": WINDOW_START_NS,
                "replay_window_end_ns": WINDOW_END_NS,
            },
        )
        for slug in slugs
    )


def _build_backtest(
    *,
    vendor: str,
    source: str,
    source_limit: int | None = None,
    source_offset: int = 0,
) -> Any:
    from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        PredictionMarketBacktest,
    )
    from prediction_market_extensions.backtesting.data_sources import (
        Book,
        PMXT,
        Polymarket,
        Telonex,
    )

    vendor_type = Telonex if vendor == "telonex" else PMXT
    return PredictionMarketBacktest(
        name=f"{vendor}-{source}-100-replay-load-benchmark",
        data=MarketDataConfig(
            platform=Polymarket,
            data_type=Book,
            vendor=vendor_type,
            sources=_source_tuple(vendor, source),
        ),
        replays=_replays_for_vendor(vendor, limit=source_limit, offset=source_offset),
        strategy_configs=[
            {
                "strategy_path": "strategies:BookMicropriceImbalanceStrategy",
                "config_path": "strategies:BookMicropriceImbalanceConfig",
                "config": {},
            }
        ],
        initial_cash=1_000.0,
        probability_window=30,
        min_book_events=25,
        min_price_range=0.0,
    )


class MemorySampler:
    def __init__(
        self,
        *,
        interval_secs: float,
        limit_gb: float | None,
        time_limit_secs: float | None,
    ) -> None:
        self._interval_secs = float(interval_secs)
        self._limit_bytes = int(limit_gb * 1024**3) if limit_gb is not None else None
        self._time_limit_secs = time_limit_secs
        self.max_rss_bytes = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._process = psutil.Process(os.getpid())
        self._started_at = 0.0

    def start(self) -> None:
        self._started_at = time.perf_counter()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _sample_rss(self) -> int:
        try:
            processes = [self._process, *self._process.children(recursive=True)]
            return sum(process.memory_info().rss for process in processes if process.is_running())
        except psutil.Error:
            return 0

    def _run(self) -> None:
        while not self._stop.wait(self._interval_secs):
            rss = self._sample_rss()
            self.max_rss_bytes = max(self.max_rss_bytes, rss)
            if self._limit_bytes is not None and rss > self._limit_bytes:
                print(
                    json.dumps(
                        {
                            "status": "killed-memory-guard",
                            "rss_gb": round(rss / 1024**3, 3),
                            "limit_gb": round(self._limit_bytes / 1024**3, 3),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                os._exit(137)
            if (
                self._time_limit_secs is not None
                and time.perf_counter() - self._started_at > self._time_limit_secs
            ):
                print(
                    json.dumps(
                        {
                            "status": "killed-time-guard",
                            "elapsed_s": round(time.perf_counter() - self._started_at, 3),
                            "max_rss_gb": round(self.max_rss_bytes / 1024**3, 3),
                            "time_limit_s": round(self._time_limit_secs, 3),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                os._exit(124)


def _apply_worker_env(args: argparse.Namespace) -> None:
    _set_env("BACKTEST_REPLAY_LOAD_WORKERS", args.replay_workers)
    _set_env("BACKTEST_REPLAY_MATERIALIZE_WORKERS", args.materialize_workers)
    _set_env("BACKTEST_LOADER_PROGRESS", "0")
    _set_env("BACKTEST_ENABLE_TIMING", "0")
    if args.source in {"api", "local"}:
        if args.vendor == "telonex":
            _set_env("TELONEX_CACHE_ROOT", "0")
        if args.vendor == "pmxt":
            _set_env("PMXT_DISABLE_CACHE", "1")
    if args.vendor == "telonex":
        _set_env("TELONEX_API_WORKERS", args.telonex_api_workers)
        _set_env("TELONEX_PREFETCH_WORKERS", args.telonex_prefetch_workers)
        _set_env("TELONEX_CACHE_PREFETCH_WORKERS", args.telonex_cache_prefetch_workers)
        _set_env("TELONEX_FILE_WORKERS", args.telonex_file_workers)
        _set_env("TELONEX_LOCAL_PREFETCH_WORKERS", args.telonex_local_prefetch_workers)
    if args.vendor == "pmxt":
        _set_env("PMXT_PREFETCH_WORKERS", args.pmxt_prefetch_workers)
        _set_env("PMXT_CACHE_PREFETCH_WORKERS", args.pmxt_cache_prefetch_workers)
        _set_env("PMXT_ROW_GROUP_CHUNK_SIZE", args.pmxt_row_group_chunk_size)
        _set_env("PMXT_ROW_GROUP_SCAN_WORKERS", args.pmxt_row_group_scan_workers)
        _set_env("PMXT_GROUPED_MARKET_CHUNK_SIZE", args.pmxt_grouped_market_chunk_size)


async def _load_once(args: argparse.Namespace) -> dict[str, Any]:
    from prediction_market_extensions._runtime_log import loader_event_sinks

    backtest = _build_backtest(
        vendor=args.vendor,
        source=args.source,
        source_limit=args.limit,
        source_offset=args.offset,
    )
    sampler = MemorySampler(
        interval_secs=args.sample_interval,
        limit_gb=args.memory_limit_gb,
        time_limit_secs=args.time_limit_secs,
    )
    gc.collect()
    started_at = time.perf_counter()
    sampler.start()
    try:
        sink_context = nullcontext() if args.show_events else loader_event_sinks([])
        with sink_context:
            loaded = await backtest._load_sims_async()
    finally:
        sampler.stop()
    elapsed = time.perf_counter() - started_at
    book_events = sum(
        sim.coverage_stats.count
        for sim in loaded
        if sim.coverage_stats.count_key in {"book_events", "records"}
    )
    records = sum(len(sim.records) for sim in loaded)
    return {
        "case": args.case_name or f"{args.vendor}-{args.source}",
        "vendor": args.vendor,
        "source": args.source,
        "elapsed_s": round(elapsed, 3),
        "max_rss_gb": round(sampler.max_rss_bytes / 1024**3, 3),
        "loaded": len(loaded),
        "records": records,
        "book_events": book_events,
        "trade_ticks": max(0, records - book_events),
        "workers": {
            "BACKTEST_REPLAY_LOAD_WORKERS": os.getenv("BACKTEST_REPLAY_LOAD_WORKERS"),
            "BACKTEST_REPLAY_MATERIALIZE_WORKERS": os.getenv("BACKTEST_REPLAY_MATERIALIZE_WORKERS"),
            "TELONEX_API_WORKERS": os.getenv("TELONEX_API_WORKERS"),
            "TELONEX_PREFETCH_WORKERS": os.getenv("TELONEX_PREFETCH_WORKERS"),
            "TELONEX_CACHE_PREFETCH_WORKERS": os.getenv("TELONEX_CACHE_PREFETCH_WORKERS"),
            "TELONEX_FILE_WORKERS": os.getenv("TELONEX_FILE_WORKERS"),
            "TELONEX_LOCAL_PREFETCH_WORKERS": os.getenv("TELONEX_LOCAL_PREFETCH_WORKERS"),
            "PMXT_PREFETCH_WORKERS": os.getenv("PMXT_PREFETCH_WORKERS"),
            "PMXT_CACHE_PREFETCH_WORKERS": os.getenv("PMXT_CACHE_PREFETCH_WORKERS"),
            "PMXT_ROW_GROUP_CHUNK_SIZE": os.getenv("PMXT_ROW_GROUP_CHUNK_SIZE"),
            "PMXT_ROW_GROUP_SCAN_WORKERS": os.getenv("PMXT_ROW_GROUP_SCAN_WORKERS"),
            "PMXT_GROUPED_MARKET_CHUNK_SIZE": os.getenv("PMXT_GROUPED_MARKET_CHUNK_SIZE"),
            "PMXT_DISABLE_CACHE": os.getenv("PMXT_DISABLE_CACHE"),
            "TELONEX_CACHE_ROOT": os.getenv("TELONEX_CACHE_ROOT"),
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor", choices=("telonex", "pmxt"), required=True)
    parser.add_argument("--source", choices=("api", "local", "cache"), required=True)
    parser.add_argument("--case-name")
    parser.add_argument("--replay-workers", type=int, default=64)
    parser.add_argument("--materialize-workers", type=int, default=4)
    parser.add_argument("--telonex-api-workers", type=int)
    parser.add_argument("--telonex-prefetch-workers", type=int)
    parser.add_argument("--telonex-cache-prefetch-workers", type=int)
    parser.add_argument("--telonex-file-workers", type=int)
    parser.add_argument("--telonex-local-prefetch-workers", type=int)
    parser.add_argument("--pmxt-prefetch-workers", type=int)
    parser.add_argument("--pmxt-cache-prefetch-workers", type=int)
    parser.add_argument("--pmxt-row-group-chunk-size", type=int)
    parser.add_argument("--pmxt-row-group-scan-workers", type=int)
    parser.add_argument("--pmxt-grouped-market-chunk-size", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--show-events", action="store_true")
    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument("--memory-limit-gb", type=float, default=24.0)
    parser.add_argument("--time-limit-secs", type=float)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _ensure_repo_root()
    load_dotenv()
    args = _parser().parse_args(argv)
    _apply_worker_env(args)
    result = asyncio.run(_load_once(args))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
