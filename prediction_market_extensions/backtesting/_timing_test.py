"""Timing harness — measures per-hour fetch time, source, and overall progress.

Can be used standalone:
    uv run python prediction_market_extensions/backtesting/_timing_test.py <backtest_file>

Or imported and activated before running any backtest:
    from prediction_market_extensions.backtesting._timing_test import install_timing
    install_timing()

Or wrapped explicitly on a runner:
    from prediction_market_extensions.backtesting._timing_harness import timing_harness

    @timing_harness
    async def run() -> None:
        ...
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_installed = False
_COMPLETED_HOUR_TIMESTAMP_WIDTH = 25
_COMPLETED_HOUR_ELAPSED_WIDTH = 9
_COMPLETED_HOUR_ROWS_WIDTH = 8
_TELONEX_TIMING_WORKERS_ENV = "TELONEX_TIMING_WORKERS"


def _hour_label(source: str) -> str:
    parsed = urlparse(source)
    path = parsed.path or source
    filename = Path(path).name
    if filename.startswith("polymarket_orderbook_") and filename.endswith(".parquet"):
        return filename.removeprefix("polymarket_orderbook_").removesuffix(".parquet")
    return filename or path


def _format_bytes(size: int | None) -> str:
    if size is None:
        return "? B"
    value = max(0, int(size))
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MiB"
    return f"{value / (1024 * 1024 * 1024):.2f} GiB"


def _transfer_label(source: str) -> str:
    for prefix, label in (
        ("cache::", "cache"),
        ("local-raw::", "local raw"),
        ("remote-raw::", "r2 raw"),
        ("telonex-cache::", "telonex cache"),
        ("telonex-local::", "telonex local"),
        ("telonex-api::", "telonex api"),
    ):
        if source.startswith(prefix):
            return f"{label} {_hour_label(source.removeprefix(prefix))}"

    if source in {"none", "unknown", "local raw"}:
        return source

    parsed = urlparse(source)
    hour_label = _hour_label(source)
    if parsed.scheme == "file" or source.startswith("/"):
        return f"local raw {hour_label}"
    return f"r2 raw {hour_label}"


def _progress_bar_description(
    *,
    total_hours: int,
    started_hours: int,
    completed_hours: int,
    active_hours: int | None = None,
    item_label: str = "hours",
) -> str:
    if total_hours <= 0:
        return f"Fetching {item_label}"

    started = min(max(0, started_hours), total_hours)
    completed = min(max(0, completed_hours), total_hours)
    if active_hours is None:
        active = max(0, started - completed)
    else:
        active = min(max(0, active_hours), total_hours)

    if completed == 0 and active > 0:
        return f"Fetching {item_label} ({started}/{total_hours} started, {active} active)"
    if completed == 0 and started > 0:
        return f"Fetching {item_label} ({started}/{total_hours} started)"
    if active > 0:
        return f"Fetching {item_label} ({completed}/{total_hours} done, {active} active)"
    if completed >= total_hours:
        return f"Fetching {item_label} ({total_hours}/{total_hours} done)"
    return f"Fetching {item_label} ({completed}/{total_hours} done)"


def _hour_progress_key(hour) -> str:  # type: ignore[no-untyped-def]
    try:
        return hour.isoformat()
    except AttributeError:
        return str(hour)


def _progress_bar_total(total_hours: int) -> int:
    return max(0, total_hours)


def _progress_bar_position(
    *, total_hours: int, completed_hours: int, active_hours_progress: float = 0.0
) -> float:
    total = max(0, total_hours)
    completed = min(max(0, completed_hours), total)
    remaining = max(0.0, float(total - completed))
    active_progress = min(max(0.0, active_hours_progress), remaining)
    return completed + active_progress


def _format_completed_hour_line(
    hour,  # type: ignore[no-untyped-def]
    *,
    elapsed: float,
    rows: int,
    source: str,
    timestamp_width: int = _COMPLETED_HOUR_TIMESTAMP_WIDTH,
) -> str:
    return (
        f"  {_hour_progress_key(hour):>{timestamp_width}s}"
        f"  {elapsed:{_COMPLETED_HOUR_ELAPSED_WIDTH}.3f}s"
        f"  {rows:>{_COMPLETED_HOUR_ROWS_WIDTH}} rows  {_transfer_label(source)}"
    )


def _hour_label_from_hour(hour) -> str:  # type: ignore[no-untyped-def]
    try:
        return hour.strftime("%Y-%m-%dT%H")
    except AttributeError:
        return _hour_label(_hour_progress_key(hour))


def _is_local_scan_source(source: str | None) -> bool:
    if source is None:
        return False
    parsed = urlparse(source)
    if parsed.scheme == "file":
        return True
    return "://" not in source


def _transfer_progress_fraction(
    *,
    mode: str | None,
    source: str | None = None,
    downloaded_bytes: int,
    total_bytes: int | None,
    scanned_batches: int,
) -> float:
    if mode == "scan":
        batches = max(0, scanned_batches)
        if _is_local_scan_source(source):
            if batches == 0:
                return 0.0
            return min(0.99, 1.0 - (1.0 / (1.0 + batches)))
        if batches == 0:
            return 0.90
        tail_fraction = 1.0 - (1.0 / (1.0 + batches))
        return min(0.99, 0.90 + (0.09 * tail_fraction))

    total = total_bytes if total_bytes is not None else None
    downloaded = max(0, downloaded_bytes)
    if total is not None and total > 0:
        return min(0.90, (downloaded / total) * 0.90)
    if downloaded > 0:
        return 0.45
    return 0.0


def _active_transfer_progress(downloads: dict[str, dict[str, object]]) -> tuple[int, float]:
    progress_by_hour: dict[str, float] = {}
    for state in downloads.values():
        hour_key = str(state.get("hour_key") or state.get("url") or "")
        if not hour_key:
            continue
        progress_by_hour[hour_key] = max(
            progress_by_hour.get(hour_key, 0.0),
            _transfer_progress_fraction(
                mode=(str(state.get("mode")) if state.get("mode") is not None else None),
                source=str(state.get("url")) if state.get("url") is not None else None,
                downloaded_bytes=int(state.get("downloaded_bytes", 0)),
                total_bytes=(
                    int(state["total_bytes"]) if state.get("total_bytes") is not None else None
                ),
                scanned_batches=int(state.get("scanned_batches", 0)),
            ),
        )
    return len(progress_by_hour), sum(progress_by_hour.values())


def install_timing() -> None:
    """Monkey-patch the PMXT loader to show per-hour progress, timing, and source."""
    global _installed
    if _installed:
        return
    _installed = True

    from tqdm import tqdm

    from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader

    try:
        from prediction_market_extensions.backtesting.data_sources.pmxt import (
            RunnerPolymarketPMXTDataLoader,
        )
    except ImportError:
        RunnerPolymarketPMXTDataLoader = None
    try:
        from prediction_market_extensions.backtesting.data_sources.telonex import (
            RunnerPolymarketTelonexBookDataLoader,
        )
    except ImportError:
        RunnerPolymarketTelonexBookDataLoader = None

    source_local = threading.local()
    pbar_state: dict = {"bar": None}
    pbar_lock = threading.Lock()
    progress_state: dict[str, int] = {"total_hours": 0, "started_hours": 0, "completed_hours": 0}
    hour_keys_by_label: dict[str, str] = {}
    progress_keys: dict[str, set[str]] = {"started": set(), "completed": set()}
    transfer_state: dict[str, object] = {
        "downloads": {},
        "stop": threading.Event(),
        "spinner_index": 0,
        "parallel": False,
        "item_label": "hours",
    }

    def _ensure_transfer_state(
        *, url: str, total_bytes: int | None, mode: str | None = None, hour_key: str | None = None
    ) -> dict[str, object]:
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        state = downloads.get(url)
        resolved_hour_key = hour_key or hour_keys_by_label.get(_hour_label(url))
        if state is None:
            state = {
                "url": url,
                "started_at": time.monotonic(),
                "downloaded_bytes": 0,
                "total_bytes": total_bytes,
                "mode": mode,
                "scanned_batches": 0,
                "scanned_rows": 0,
                "matched_rows": 0,
                "hour_key": resolved_hour_key,
            }
            downloads[url] = state
        else:
            if total_bytes is not None:
                state["total_bytes"] = total_bytes
            if mode is not None:
                state["mode"] = mode
            if resolved_hour_key is not None:
                state["hour_key"] = resolved_hour_key
        return state

    def _close_transfer_state(url: str) -> None:
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        downloads.pop(url, None)

    def _transfer_status_text() -> str:
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        if not downloads:
            return ""
        spinner_frames = "|/-\\"
        now = time.monotonic()
        spinner_index = (int(transfer_state["spinner_index"]) + 1) % len(spinner_frames)
        transfer_state["spinner_index"] = spinner_index
        spinner = spinner_frames[spinner_index]
        labels: list[str] = []
        active_downloads = list(downloads.values())
        for state in active_downloads[:2]:
            elapsed = now - float(state["started_at"])
            mode = state.get("mode")
            downloaded_bytes = int(state["downloaded_bytes"])
            total_bytes = state["total_bytes"]
            if mode == "scan":
                size_text = _format_bytes(total_bytes) if total_bytes else "scan"
                scanned_batches = int(state["scanned_batches"])
                scanned_rows = int(state["scanned_rows"])
                matched_rows = int(state["matched_rows"])
                detail_parts: list[str] = []
                if scanned_batches:
                    detail_parts.append(f"{scanned_batches}b")
                if matched_rows:
                    detail_parts.append(f"{matched_rows:,}r")
                elif scanned_rows:
                    detail_parts.append(f"{scanned_rows:,}r")
                detail = " ".join(detail_parts)
                labels.append(
                    f"{_transfer_label(str(state['url']))} scan {size_text}"
                    f"{(' ' + detail) if detail else ''} {elapsed:4.1f}s"
                )
            elif total_bytes:
                labels.append(
                    f"{_transfer_label(str(state['url']))} "
                    f"{_format_bytes(downloaded_bytes)}/{_format_bytes(total_bytes)} "
                    f"{elapsed:4.1f}s"
                )
            else:
                labels.append(
                    f"{_transfer_label(str(state['url']))} "
                    f"{_format_bytes(downloaded_bytes)} {elapsed:4.1f}s"
                )
        if len(active_downloads) > len(labels):
            labels.append(f"+{len(active_downloads) - len(labels)} more")
        prefix = "prefetch:" if bool(transfer_state["parallel"]) else "active:"
        return f"{prefix} {spinner} " + " | ".join(labels)

    def _refresh_transfer_status() -> None:
        bar = pbar_state["bar"]
        if bar is None:
            return
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        active_hours, active_progress = _active_transfer_progress(downloads)
        target_position = _progress_bar_position(
            total_hours=int(progress_state["total_hours"]),
            completed_hours=int(progress_state["completed_hours"]),
            active_hours_progress=active_progress,
        )
        if target_position > float(bar.n):
            bar.update(target_position - bar.n)
        bar.set_description_str(
            _progress_bar_description(
                total_hours=int(progress_state["total_hours"]),
                started_hours=int(progress_state["started_hours"]),
                completed_hours=int(progress_state["completed_hours"]),
                active_hours=active_hours,
                item_label=str(transfer_state["item_label"]),
            ),
            refresh=False,
        )
        status_text = _transfer_status_text()
        bar.set_postfix_str(status_text, refresh=True)

    def _mark_hour_started(hour) -> None:  # type: ignore[no-untyped-def]
        key = _hour_progress_key(hour)
        hour_keys_by_label[_hour_label_from_hour(hour)] = key
        started = progress_keys["started"]
        if key in started:
            return
        started.add(key)
        progress_state["started_hours"] = len(started)

    def _mark_hour_completed(hour) -> None:  # type: ignore[no-untyped-def]
        key = _hour_progress_key(hour)
        hour_keys_by_label[_hour_label_from_hour(hour)] = key
        completed = progress_keys["completed"]
        if key in completed:
            return
        completed.add(key)
        progress_state["completed_hours"] = len(completed)

    def _download_progress(
        url: str, downloaded_bytes: int, total_bytes: int | None, finished: bool
    ) -> None:
        with pbar_lock:
            state = _ensure_transfer_state(
                url=url,
                total_bytes=total_bytes,
                mode="download",
            )

            state["downloaded_bytes"] = downloaded_bytes
            state["total_bytes"] = total_bytes
            _refresh_transfer_status()
            if finished:
                _close_transfer_state(url)
                _refresh_transfer_status()

    def _scan_progress(
        source: str,
        scanned_batches: int,
        scanned_rows: int,
        matched_rows: int,
        total_bytes: int | None,
        finished: bool,
    ) -> None:
        with pbar_lock:
            state = _ensure_transfer_state(
                url=source,
                total_bytes=total_bytes,
                mode="scan",
            )

            state["scanned_batches"] = scanned_batches
            state["scanned_rows"] = scanned_rows
            state["matched_rows"] = matched_rows
            state["total_bytes"] = total_bytes
            _refresh_transfer_status()
            if finished:
                _close_transfer_state(source)
                _refresh_transfer_status()

    def _transfer_heartbeat() -> None:
        stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
        while not stop_event.wait(0.2):
            with pbar_lock:
                downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                if downloads:
                    _refresh_transfer_status()

    def _start_transfer(hour, url: str | None) -> None:  # type: ignore[no-untyped-def]
        if url is None:
            return
        with pbar_lock:
            _mark_hour_started(hour)
            _ensure_transfer_state(
                url=url,
                total_bytes=None,
                hour_key=_hour_progress_key(hour),
            )

            _refresh_transfer_status()

    def _finish_transfer(url: str | None) -> None:
        if url is None:
            return
        with pbar_lock:
            _close_transfer_state(url)
            _refresh_transfer_status()

    def _install_full_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_load = loader_cls._load_market_batches
        orig_cached = loader_cls._load_cached_market_batches
        orig_local_archive = loader_cls._load_local_archive_market_batches
        orig_remote = loader_cls._load_remote_market_batches
        orig_iter = loader_cls._iter_market_batches

        def patched_cached(self, hour):
            result = orig_cached(self, hour)
            if result is not None:
                cache_path = self._cache_path_for_hour(hour)
                source_local.source = f"cache::{cache_path}"
            return result

        def patched_local_archive(self, hour, *, batch_size):
            result = orig_local_archive(self, hour, batch_size=batch_size)
            if result is not None:
                archive_paths = self._local_archive_paths_for_hour(hour)
                existing_path = next((path for path in archive_paths if path.exists()), None)
                source_local.source = (
                    f"local-raw::{existing_path}" if existing_path is not None else "local raw"
                )
            return result

        def patched_remote(self, hour, *, batch_size):
            remote_url = self._archive_url_for_hour(hour)
            _start_transfer(hour, remote_url)
            try:
                result = orig_remote(self, hour, batch_size=batch_size)
            finally:
                _finish_transfer(remote_url)
            if result is not None:
                source_local.source = f"remote-raw::{remote_url}"
            return result

        def timed_load(self, hour, *, batch_size):
            source_local.source = "none"
            t0 = time.perf_counter()
            with pbar_lock:
                _mark_hour_started(hour)
                _refresh_transfer_status()
            result = orig_load(self, hour, batch_size=batch_size)
            elapsed = time.perf_counter() - t0
            rows = sum(b.num_rows for b in result) if result else 0
            source = getattr(source_local, "source", "unknown")

            with pbar_lock:
                bar = pbar_state["bar"]
                if bar is not None:
                    bar.write(
                        _format_completed_hour_line(
                            hour,
                            elapsed=elapsed,
                            rows=rows,
                            source=source,
                        )
                    )
                    _mark_hour_completed(hour)
                    _refresh_transfer_status()
            return result

        def patched_iter(self, hours, *, batch_size):
            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                progress_state["total_hours"] = len(hours)
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "hours"
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat, name="pmxt-timing-heartbeat", daemon=True
                )
                pbar_state["bar"] = tqdm(
                    total=_progress_bar_total(len(hours)),
                    desc=_progress_bar_description(
                        total_hours=len(hours), started_hours=0, completed_hours=0
                    ),
                    unit="hr",
                    leave=False,
                    bar_format=("{l_bar}{bar}| [{elapsed}<{remaining}]{postfix}"),
                )
                previous_callback = getattr(self, "_pmxt_download_progress_callback", None)
                previous_scan_callback = getattr(self, "_pmxt_scan_progress_callback", None)
                self._pmxt_download_progress_callback = _download_progress
                self._pmxt_scan_progress_callback = _scan_progress
                transfer_state["parallel"] = (
                    min(getattr(self, "_pmxt_prefetch_workers", 1), len(hours)) > 1
                )
                heartbeat_thread.start()
            try:
                yield from orig_iter(self, hours, batch_size=batch_size)
            finally:
                with pbar_lock:
                    self._pmxt_download_progress_callback = previous_callback
                    self._pmxt_scan_progress_callback = previous_scan_callback
                    stop_event.set()
                    downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                    downloads.clear()
                    transfer_state["parallel"] = False
                    transfer_state["item_label"] = "hours"
                    progress_state["total_hours"] = 0
                    progress_state["started_hours"] = 0
                    progress_state["completed_hours"] = 0
                    hour_keys_by_label.clear()
                    progress_keys["started"].clear()
                    progress_keys["completed"].clear()
                    bar = pbar_state["bar"]
                    if bar is not None:
                        bar.clear(nolock=True)
                        bar.set_postfix_str("", refresh=False)
                        bar.close()
                        pbar_state["bar"] = None
                heartbeat_thread.join(timeout=1.0)

        loader_cls._load_cached_market_batches = patched_cached
        loader_cls._load_local_archive_market_batches = patched_local_archive
        loader_cls._load_remote_market_batches = patched_remote
        loader_cls._load_market_batches = timed_load
        loader_cls._iter_market_batches = patched_iter

    def _install_runner_local_archive_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_local_archive = loader_cls._load_local_archive_market_batches

        def patched_local_archive(self, hour, *, batch_size):
            result = orig_local_archive(self, hour, batch_size=batch_size)
            if result is not None:
                raw_path = self._raw_path_for_hour(hour)
                if raw_path is not None and raw_path.exists():
                    source_local.source = f"local-raw::{raw_path}"
                else:
                    archive_paths = self._local_archive_paths_for_hour(hour)
                    existing_path = next((path for path in archive_paths if path.exists()), None)
                    source_local.source = (
                        f"local-raw::{existing_path}" if existing_path is not None else "local raw"
                    )
            return result

        loader_cls._load_local_archive_market_batches = patched_local_archive

    def _install_telonex_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_load_order_book_and_quotes = loader_cls.load_order_book_and_quotes

        def _run_with_telonex_day_timing(self, dates, load_fn):  # type: ignore[no-untyped-def]
            day_started_at: dict[str, float] = {}

            def _day_progress(date: str, event: str, source: str, rows: int) -> None:
                with pbar_lock:
                    if event == "start":
                        day_started_at[date] = time.perf_counter()
                        _mark_hour_started(date)
                        _refresh_transfer_status()
                        return
                    if event != "complete":
                        return
                    elapsed = time.perf_counter() - day_started_at.get(date, time.perf_counter())
                    bar = pbar_state["bar"]
                    if bar is not None:
                        bar.write(
                            _format_completed_hour_line(
                                date,
                                elapsed=elapsed,
                                rows=rows,
                                source=source,
                                timestamp_width=10,
                            )
                        )
                        _mark_hour_completed(date)
                        _refresh_transfer_status()

            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                progress_state["total_hours"] = len(dates)
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "days"
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat, name="telonex-timing-heartbeat", daemon=True
                )
                pbar_state["bar"] = tqdm(
                    total=_progress_bar_total(len(dates)),
                    desc=_progress_bar_description(
                        total_hours=len(dates),
                        started_hours=0,
                        completed_hours=0,
                        item_label="days",
                    ),
                    unit="day",
                    leave=False,
                    bar_format=("{l_bar}{bar}| [{elapsed}<{remaining}]{postfix}"),
                )
                previous_download_callback = getattr(
                    self, "_telonex_download_progress_callback", None
                )
                previous_day_callback = getattr(self, "_telonex_day_progress_callback", None)
                self._telonex_download_progress_callback = _download_progress
                self._telonex_day_progress_callback = _day_progress
                transfer_state["parallel"] = False
                heartbeat_thread.start()

            try:
                return load_fn()
            finally:
                with pbar_lock:
                    self._telonex_download_progress_callback = previous_download_callback
                    self._telonex_day_progress_callback = previous_day_callback
                    stop_event.set()
                    downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                    downloads.clear()
                    transfer_state["parallel"] = False
                    transfer_state["item_label"] = "hours"
                    progress_state["total_hours"] = 0
                    progress_state["started_hours"] = 0
                    progress_state["completed_hours"] = 0
                    hour_keys_by_label.clear()
                    progress_keys["started"].clear()
                    progress_keys["completed"].clear()
                    bar = pbar_state["bar"]
                    if bar is not None:
                        bar.clear(nolock=True)
                        bar.set_postfix_str("", refresh=False)
                        bar.close()
                        pbar_state["bar"] = None
                heartbeat_thread.join(timeout=1.0)

        def timed_load_quotes(
            self,
            start,
            end,
            *,
            market_slug: str,
            token_index: int,
            outcome: str | None,
        ):
            config = self._config()
            dates = self._date_range(start, end)
            records = []

            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                progress_state["total_hours"] = len(dates)
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "days"
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat, name="telonex-timing-heartbeat", daemon=True
                )
                pbar_state["bar"] = tqdm(
                    total=_progress_bar_total(len(dates)),
                    desc=_progress_bar_description(
                        total_hours=len(dates),
                        started_hours=0,
                        completed_hours=0,
                        item_label="days",
                    ),
                    unit="day",
                    leave=False,
                    bar_format=("{l_bar}{bar}| [{elapsed}<{remaining}]{postfix}"),
                )
                previous_download_callback = getattr(
                    self, "_telonex_download_progress_callback", None
                )
                self._telonex_download_progress_callback = _download_progress
                transfer_state["parallel"] = False
                heartbeat_thread.start()

            try:
                has_api_cache = False
                for date in dates:
                    for entry in config.ordered_source_entries:
                        if entry.kind != "api":
                            continue
                        assert entry.target is not None
                        cache_path = self._api_cache_path(
                            base_url=entry.target,
                            channel=config.channel,
                            date=date,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                        )
                        if cache_path is not None and cache_path.exists():
                            has_api_cache = True
                            break
                    if has_api_cache:
                        break

                if dates and not has_api_cache:
                    for entry in config.ordered_source_entries:
                        if entry.kind != "local":
                            break
                        assert entry.target is not None
                        root = Path(entry.target).expanduser()
                        blob_root = self._local_blob_root(root)
                        if blob_root is not None:
                            blob_frame = self._load_blob_range(
                                store_root=blob_root,
                                channel=config.channel,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                                start=start,
                                end=end,
                            )
                            if blob_frame is not None:
                                source_label = f"telonex-local::{blob_root}"
                                total_bytes = None
                                started_at = time.perf_counter()
                                with pbar_lock:
                                    for date in dates:
                                        _mark_hour_started(date)
                                    _ensure_transfer_state(
                                        url=source_label,
                                        total_bytes=total_bytes,
                                        mode="scan",
                                        hour_key=_hour_progress_key(dates[0]),
                                    )
                                    _refresh_transfer_status()
                                row_count = len(blob_frame)
                                _scan_progress(source_label, 1, row_count, 0, total_bytes, False)
                                records = self._quote_ticks_from_frame(
                                    blob_frame, start=start, end=end
                                )
                                _scan_progress(
                                    source_label,
                                    1,
                                    row_count,
                                    len(records),
                                    total_bytes,
                                    True,
                                )
                                elapsed = time.perf_counter() - started_at
                                with pbar_lock:
                                    bar = pbar_state["bar"]
                                    if bar is not None:
                                        for date in dates:
                                            bar.write(
                                                _format_completed_hour_line(
                                                    date,
                                                    elapsed=elapsed if date == dates[-1] else 0.0,
                                                    rows=0,
                                                    source=source_label,
                                                    timestamp_width=10,
                                                )
                                            )
                                            _mark_hour_completed(date)
                                        _refresh_transfer_status()
                                records.sort(key=lambda quote: quote.ts_event)
                                return records
                        local_path = self._local_consolidated_path(
                            root=root,
                            channel=config.channel,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                        )
                        if local_path is None:
                            continue

                        source_label = f"telonex-local::{local_path}"
                        try:
                            total_bytes = local_path.stat().st_size
                        except OSError:
                            total_bytes = None
                        started_at = time.perf_counter()
                        with pbar_lock:
                            for date in dates:
                                _mark_hour_started(date)
                            _ensure_transfer_state(
                                url=source_label,
                                total_bytes=total_bytes,
                                mode="scan",
                                hour_key=_hour_progress_key(dates[0]),
                            )
                            _refresh_transfer_status()

                        frame = self._load_local_range(
                            root=root,
                            channel=config.channel,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                        )
                        if frame is None:
                            _finish_transfer(source_label)
                            continue

                        row_count = len(frame)
                        _scan_progress(source_label, 1, row_count, 0, total_bytes, False)
                        records = self._quote_ticks_from_frame(frame, start=start, end=end)
                        _scan_progress(
                            source_label,
                            1,
                            row_count,
                            len(records),
                            total_bytes,
                            True,
                        )
                        rows_by_date: dict[str, int] = {}
                        for quote in records:
                            date_key = (
                                datetime.fromtimestamp(quote.ts_event / 1_000_000_000, tz=UTC)
                                .date()
                                .isoformat()
                            )
                            rows_by_date[date_key] = rows_by_date.get(date_key, 0) + 1

                        elapsed = time.perf_counter() - started_at
                        with pbar_lock:
                            bar = pbar_state["bar"]
                            if bar is not None:
                                for date in dates:
                                    bar.write(
                                        _format_completed_hour_line(
                                            date,
                                            elapsed=elapsed if date == dates[-1] else 0.0,
                                            rows=rows_by_date.get(str(date), 0),
                                            source=source_label,
                                            timestamp_width=10,
                                        )
                                    )
                                    _mark_hour_completed(date)
                                _refresh_transfer_status()
                        records.sort(key=lambda quote: quote.ts_event)
                        return records

                def _fetch_one_day(date: str) -> tuple[str, list, str, float]:
                    local_records: list = []
                    day_source_label = "unknown"
                    day_total_bytes: int | None = None
                    frame = None
                    day_started_at = time.perf_counter()
                    day_window = self._day_window(date, start=start, end=end)
                    if day_window is None:
                        return (date, local_records, day_source_label, 0.0)
                    day_start, day_end = day_window

                    with pbar_lock:
                        _mark_hour_started(date)
                        _refresh_transfer_status()

                    for entry in config.ordered_source_entries:
                        if entry.kind != "api":
                            continue
                        assert entry.target is not None
                        cache_path = self._api_cache_path(
                            base_url=entry.target,
                            channel=config.channel,
                            date=date,
                            market_slug=market_slug,
                            token_index=token_index,
                            outcome=outcome,
                        )
                        if cache_path is None or not cache_path.exists():
                            continue
                        source_key = f"telonex-cache::{cache_path}"
                        day_source_label = source_key
                        try:
                            day_total_bytes = cache_path.stat().st_size
                        except OSError:
                            day_total_bytes = None
                        _start_transfer(date, source_key)
                        try:
                            frame = self._load_api_cache_day(
                                base_url=entry.target,
                                channel=config.channel,
                                date=date,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                            )
                        finally:
                            _finish_transfer(source_key)
                        if frame is not None:
                            break

                    for entry in config.ordered_source_entries:
                        if frame is not None:
                            break
                        source_key: str | None = None
                        if entry.kind == "local":
                            assert entry.target is not None
                            root = Path(entry.target).expanduser()
                            blob_root = self._local_blob_root(root)
                            if blob_root is not None:
                                source_key = f"telonex-local::{blob_root}"
                                day_source_label = source_key
                                _start_transfer(date, source_key)
                                try:
                                    frame = self._load_blob_range(
                                        store_root=blob_root,
                                        channel=config.channel,
                                        market_slug=market_slug,
                                        token_index=token_index,
                                        outcome=outcome,
                                        start=day_start,
                                        end=day_end,
                                    )
                                finally:
                                    _finish_transfer(source_key)
                                if frame is not None:
                                    break
                            local_path = self._local_path_for_day(
                                root=root,
                                channel=config.channel,
                                date=date,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                            )
                            if local_path is not None:
                                source_key = f"telonex-local::{local_path}"
                                day_source_label = source_key
                                try:
                                    day_total_bytes = local_path.stat().st_size
                                except OSError:
                                    day_total_bytes = None
                                _start_transfer(date, source_key)
                                try:
                                    frame = self._load_local_day(
                                        root=root,
                                        channel=config.channel,
                                        date=date,
                                        market_slug=market_slug,
                                        token_index=token_index,
                                        outcome=outcome,
                                    )
                                finally:
                                    _finish_transfer(source_key)
                                if frame is not None:
                                    break
                            local_path = self._local_consolidated_path(
                                root=root,
                                channel=config.channel,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                            )
                            if local_path is None:
                                continue
                            source_key = f"telonex-local::{local_path}"
                            day_source_label = source_key
                            try:
                                day_total_bytes = local_path.stat().st_size
                            except OSError:
                                day_total_bytes = None
                            _start_transfer(date, source_key)
                            try:
                                frame = self._load_local_range(
                                    root=root,
                                    channel=config.channel,
                                    market_slug=market_slug,
                                    token_index=token_index,
                                    outcome=outcome,
                                )
                            finally:
                                _finish_transfer(source_key)
                        elif entry.kind == "api":
                            assert entry.target is not None
                            api_url = self._api_url(
                                base_url=entry.target,
                                channel=config.channel,
                                date=date,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                            )
                            cache_path = self._api_cache_path(
                                base_url=entry.target,
                                channel=config.channel,
                                date=date,
                                market_slug=market_slug,
                                token_index=token_index,
                                outcome=outcome,
                            )
                            if cache_path is not None and cache_path.exists():
                                source_key = f"telonex-cache::{cache_path}"
                                try:
                                    day_total_bytes = cache_path.stat().st_size
                                except OSError:
                                    day_total_bytes = None
                            else:
                                source_key = f"telonex-api::{api_url}"
                            day_source_label = source_key
                            _start_transfer(date, source_key)
                            try:
                                frame = self._load_api_day(
                                    base_url=entry.target,
                                    channel=config.channel,
                                    date=date,
                                    market_slug=market_slug,
                                    token_index=token_index,
                                    outcome=outcome,
                                    api_key=getattr(entry, "api_key", None),
                                )
                            finally:
                                _finish_transfer(source_key)
                            actual_source = getattr(self, "_telonex_last_api_source", None)
                            if actual_source:
                                day_source_label = actual_source
                                if actual_source.startswith("telonex-cache::"):
                                    actual_cache_path = Path(
                                        actual_source.removeprefix("telonex-cache::")
                                    )
                                    try:
                                        day_total_bytes = actual_cache_path.stat().st_size
                                    except OSError:
                                        day_total_bytes = None
                        if frame is not None:
                            break

                    if frame is not None:
                        row_count = len(frame)
                        _scan_progress(day_source_label, 1, row_count, 0, day_total_bytes, False)
                        local_records = self._quote_ticks_from_frame(
                            frame, start=day_start, end=day_end
                        )
                        _scan_progress(
                            day_source_label,
                            1,
                            row_count,
                            len(local_records),
                            day_total_bytes,
                            True,
                        )

                    elapsed = time.perf_counter() - day_started_at
                    with pbar_lock:
                        bar = pbar_state["bar"]
                        if bar is not None:
                            bar.write(
                                _format_completed_hour_line(
                                    date,
                                    elapsed=elapsed,
                                    rows=len(local_records),
                                    source=day_source_label,
                                    timestamp_width=10,
                                )
                            )
                            _mark_hour_completed(date)
                            _refresh_transfer_status()
                    return (date, local_records, day_source_label, elapsed)

                workers = max(1, int(os.getenv(_TELONEX_TIMING_WORKERS_ENV, "8")))
                workers = min(workers, len(dates)) if dates else 1
                with pbar_lock:
                    transfer_state["parallel"] = workers > 1
                if workers <= 1:
                    for date in dates:
                        _, day_records, _, _ = _fetch_one_day(date)
                        records.extend(day_records)
                else:
                    with ThreadPoolExecutor(
                        max_workers=workers, thread_name_prefix="telonex-fetch"
                    ) as executor:
                        for _date, day_records, _source, _elapsed in executor.map(
                            _fetch_one_day, dates
                        ):
                            records.extend(day_records)
                records.sort(key=lambda quote: quote.ts_event)
                return records
            finally:
                with pbar_lock:
                    self._telonex_download_progress_callback = previous_download_callback
                    stop_event.set()
                    downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                    downloads.clear()
                    transfer_state["parallel"] = False
                    transfer_state["item_label"] = "hours"
                    progress_state["total_hours"] = 0
                    progress_state["started_hours"] = 0
                    progress_state["completed_hours"] = 0
                    hour_keys_by_label.clear()
                    progress_keys["started"].clear()
                    progress_keys["completed"].clear()
                    bar = pbar_state["bar"]
                    if bar is not None:
                        bar.clear(nolock=True)
                        bar.set_postfix_str("", refresh=False)
                        bar.close()
                        pbar_state["bar"] = None
                heartbeat_thread.join(timeout=1.0)

        def timed_load_order_book_and_quotes(
            self,
            start,
            end,
            *,
            market_slug: str,
            token_index: int,
            outcome: str | None,
            include_order_book: bool = True,
            include_quotes: bool = True,
        ):
            dates = self._date_range(start, end)
            return _run_with_telonex_day_timing(
                self,
                dates,
                lambda: orig_load_order_book_and_quotes(
                    self,
                    start,
                    end,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    include_order_book=include_order_book,
                    include_quotes=include_quotes,
                ),
            )

        loader_cls.load_quotes = timed_load_quotes
        loader_cls.load_order_book_and_quotes = timed_load_order_book_and_quotes

    if RunnerPolymarketPMXTDataLoader is not None:
        # Patch the repo-layer runner first because it overrides
        # _load_market_batches; patching only the base class leaves local
        # mirror scans outside the started/completed hour bookkeeping.
        _install_full_timing(RunnerPolymarketPMXTDataLoader)
        _install_runner_local_archive_timing(RunnerPolymarketPMXTDataLoader)
    _install_full_timing(PolymarketPMXTDataLoader)
    if RunnerPolymarketTelonexBookDataLoader is not None:
        _install_telonex_timing(RunnerPolymarketTelonexBookDataLoader)


def _load_backtest_module(path_str: str):
    path = Path(path_str).resolve()
    if not path.exists():
        print(f"Error: {path} does not exist", file=sys.stderr)
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("_backtest", path)
    mod = importlib.util.module_from_spec(spec)
    backtest_dir = str(path.parent)
    if backtest_dir not in sys.path:
        sys.path.insert(0, backtest_dir)
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: uv run python prediction_market_extensions/backtesting/_timing_test.py <backtest_file>",
            file=sys.stderr,
        )
        sys.exit(1)

    install_timing()

    bt = _load_backtest_module(sys.argv[1])
    if not hasattr(bt, "run"):
        print(f"Error: {sys.argv[1]} has no run() coroutine", file=sys.stderr)
        sys.exit(1)

    print(f"\nPMXT per-hour fetch timing: {Path(sys.argv[1]).name}\n")
    wall_start = time.perf_counter()
    asyncio.run(bt.run())
    wall_total = time.perf_counter() - wall_start
    print(f"\nTotal wall time: {wall_total:.2f}s")
