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
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_installed = False
_TELONEX_TIMING_WORKERS_ENV = "TELONEX_TIMING_WORKERS"
_LOADER_PROGRESS_ENV = "BACKTEST_LOADER_PROGRESS"
_LOADER_PROGRESS_LINES_ENV = "BACKTEST_LOADER_PROGRESS_LINES"
_LOADER_PROGRESS_LOG_INTERVAL_ENV = "BACKTEST_LOADER_PROGRESS_LOG_INTERVAL"
_DEFAULT_PROGRESS_LOG_INTERVAL_SECS = 2.0


def _env_flag_enabled(value: str | None, *, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().casefold() not in {"0", "false", "no", "off"}


def _loader_progress_enabled() -> bool:
    return _env_flag_enabled(os.getenv(_LOADER_PROGRESS_ENV))


def _loader_progress_lines_enabled() -> bool:
    return _env_flag_enabled(os.getenv(_LOADER_PROGRESS_LINES_ENV), default=True)


def _loader_progress_log_interval_secs() -> float:
    configured = os.getenv(_LOADER_PROGRESS_LOG_INTERVAL_ENV)
    if configured is None:
        return _DEFAULT_PROGRESS_LOG_INTERVAL_SECS
    try:
        return max(0.1, float(configured))
    except ValueError:
        return _DEFAULT_PROGRESS_LOG_INTERVAL_SECS


def _hour_label(source: str) -> str:
    parsed = urlparse(source)
    path = parsed.path or source
    filename = Path(path).name
    if filename.startswith("polymarket_orderbook_") and filename.endswith(".parquet"):
        return filename.removeprefix("polymarket_orderbook_").removesuffix(".parquet")
    return filename or path


def _filename_label(source: str) -> str:
    parsed = urlparse(source)
    path = parsed.path or source
    return Path(path).name or path


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
        ("telonex-deltas-cache::", "telonex deltas cache"),
        ("telonex-cache-fast::", "telonex cache"),
        ("telonex-cache::", "telonex cache"),
        ("telonex-local::", "telonex local"),
        ("telonex-api::", "telonex api"),
    ):
        if source.startswith(prefix):
            remainder = source.removeprefix(prefix)
            if prefix == "cache::":
                return f"{label} {_filename_label(remainder)}"
            if prefix in {"local-raw::", "remote-raw::", "telonex-local::", "telonex-api::"}:
                return label
            return f"{label} {_hour_label(remainder)}"

    if source in {"none", "unknown", "local raw"}:
        return source

    parsed = urlparse(source)
    if parsed.scheme == "file" or source.startswith("/"):
        return "local raw"
    return "r2 raw"


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


def _text_progress_bar(position: float, total: int, *, width: int = 24) -> str:
    if total <= 0:
        return "[" + ("-" * width) + "]"
    fraction = min(1.0, max(0.0, position / float(total)))
    filled = min(width, max(0, int(round(fraction * width))))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _progress_bar_position(
    *, total_hours: int, completed_hours: int, active_hours_progress: float = 0.0
) -> float:
    total = max(0, total_hours)
    completed = min(max(0, completed_hours), total)
    remaining = max(0.0, float(total - completed))
    active_progress = min(max(0.0, active_hours_progress), remaining)
    return completed + active_progress


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

    from prediction_market_extensions._runtime_log import emit_loader_event
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
        "vendor": "loader",
    }
    progress_line_state: dict[str, float] = {"last_emit": 0.0}
    grouped_loader_refcounts: dict[int, int] = {}
    grouped_loader_callbacks: dict[int, tuple[object, object]] = {}
    grouped_active_calls = 0
    grouped_heartbeat_thread: threading.Thread | None = None

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

    def _emit_plain_progress_line(*, force: bool = False) -> None:
        if not _loader_progress_lines_enabled():
            return
        now = time.monotonic()
        interval = _loader_progress_log_interval_secs()
        if not force and (now - progress_line_state["last_emit"]) < interval:
            return

        total = int(progress_state["total_hours"])
        started = int(progress_state["started_hours"])
        completed = int(progress_state["completed_hours"])
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        active_hours, active_progress = _active_transfer_progress(downloads)
        position = _progress_bar_position(
            total_hours=total,
            completed_hours=completed,
            active_hours_progress=active_progress,
        )
        percent = 0.0 if total <= 0 else min(100.0, max(0.0, position / total * 100.0))
        item_label = str(transfer_state["item_label"])
        vendor = str(transfer_state["vendor"])
        status_text = _transfer_status_text()
        message = (
            f"{vendor} book progress {_text_progress_bar(position, total)} "
            f"{position:.1f}/{total} {item_label} ({percent:.1f}%; "
            f"started={started}, done={completed}, active={active_hours})"
        )
        if status_text:
            message = f"{message} {status_text}"

        progress_line_state["last_emit"] = now
        emit_loader_event(
            message,
            level="INFO",
            stage="runtime",
            status="progress",
            vendor=vendor.casefold(),
            platform="polymarket",
            data_type="book",
            rows=completed,
            attrs={
                "progress_position": round(position, 3),
                "progress_total": total,
                "started": started,
                "active": active_hours,
                "item_label": item_label,
            },
            stacklevel=3,
        )

    def _refresh_transfer_status(*, emit_line: bool = True) -> None:
        if emit_line:
            _emit_plain_progress_line()

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

    def _set_dynamic_total_hours() -> None:
        total_hours = max(
            int(progress_state["total_hours"]),
            len(progress_keys["started"]),
            len(progress_keys["completed"]),
        )
        progress_state["total_hours"] = total_hours

    def _enter_grouped_pmxt_loader_callbacks(loader) -> None:  # type: ignore[no-untyped-def]
        key = id(loader)
        with pbar_lock:
            count = grouped_loader_refcounts.get(key, 0)
            if count == 0:
                grouped_loader_callbacks[key] = (
                    getattr(loader, "_pmxt_download_progress_callback", None),
                    getattr(loader, "_pmxt_scan_progress_callback", None),
                )
                loader._pmxt_download_progress_callback = _download_progress
                loader._pmxt_scan_progress_callback = _scan_progress
            grouped_loader_refcounts[key] = count + 1

    def _exit_grouped_pmxt_loader_callbacks(loader) -> None:  # type: ignore[no-untyped-def]
        key = id(loader)
        with pbar_lock:
            count = grouped_loader_refcounts.get(key, 0)
            if count <= 1:
                previous_download, previous_scan = grouped_loader_callbacks.pop(key, (None, None))
                loader._pmxt_download_progress_callback = previous_download
                loader._pmxt_scan_progress_callback = previous_scan
                grouped_loader_refcounts.pop(key, None)
                return
            grouped_loader_refcounts[key] = count - 1

    def _start_grouped_pmxt_progress(hour, *, parallel: bool) -> None:  # type: ignore[no-untyped-def]
        nonlocal grouped_active_calls
        nonlocal grouped_heartbeat_thread

        with pbar_lock:
            stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
            first_active_call = grouped_active_calls == 0
            if first_active_call:
                stop_event.clear()
                progress_state["total_hours"] = 0
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "hours"
                transfer_state["vendor"] = "PMXT"
                transfer_state["parallel"] = parallel
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()

            grouped_active_calls += 1
            _mark_hour_started(hour)
            _set_dynamic_total_hours()
            _refresh_transfer_status(emit_line=False)
            _emit_plain_progress_line()

            if grouped_heartbeat_thread is None or not grouped_heartbeat_thread.is_alive():
                grouped_heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat,
                    name="pmxt-grouped-timing-heartbeat",
                    daemon=True,
                )
                grouped_heartbeat_thread.start()

    def _finish_grouped_pmxt_progress(hour) -> None:  # type: ignore[no-untyped-def]
        nonlocal grouped_active_calls
        nonlocal grouped_heartbeat_thread

        thread_to_join: threading.Thread | None = None
        with pbar_lock:
            _mark_hour_completed(hour)
            _set_dynamic_total_hours()
            _refresh_transfer_status(emit_line=False)
            grouped_active_calls = max(0, grouped_active_calls - 1)
            _emit_plain_progress_line()
            if grouped_active_calls == 0:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.set()
                downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                downloads.clear()
                transfer_state["parallel"] = False
                transfer_state["item_label"] = "hours"
                transfer_state["vendor"] = "loader"
                progress_state["total_hours"] = 0
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                thread_to_join = grouped_heartbeat_thread
                grouped_heartbeat_thread = None
        if thread_to_join is not None:
            thread_to_join.join(timeout=1.0)

    def _install_full_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_load = loader_cls._load_market_batches
        orig_cached = loader_cls._load_cached_market_batches
        orig_local_archive = loader_cls._load_local_archive_market_batches
        orig_remote = loader_cls._load_remote_market_batches
        orig_iter = loader_cls._iter_market_batches
        orig_shared = getattr(loader_cls, "load_shared_market_batches_for_hour", None)

        def patched_cached(self, hour):
            return orig_cached(self, hour)

        def patched_local_archive(self, hour, *, batch_size):
            return orig_local_archive(self, hour, batch_size=batch_size)

        def patched_remote(self, hour, *, batch_size):
            remote_url = self._archive_url_for_hour(hour)
            _start_transfer(hour, remote_url)
            try:
                return orig_remote(self, hour, batch_size=batch_size)
            finally:
                _finish_transfer(remote_url)

        def timed_load(self, hour, *, batch_size):
            with pbar_lock:
                _mark_hour_started(hour)
                _refresh_transfer_status()
            result = orig_load(self, hour, batch_size=batch_size)
            with pbar_lock:
                _mark_hour_completed(hour)
                _refresh_transfer_status()
            return result

        def patched_iter(self, hours, *, batch_size):
            if not _loader_progress_enabled():
                yield from orig_iter(self, hours, batch_size=batch_size)
                return

            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                progress_state["total_hours"] = len(hours)
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "hours"
                transfer_state["vendor"] = "PMXT"
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat, name="pmxt-timing-heartbeat", daemon=True
                )
                previous_callback = getattr(self, "_pmxt_download_progress_callback", None)
                previous_scan_callback = getattr(self, "_pmxt_scan_progress_callback", None)
                self._pmxt_download_progress_callback = _download_progress
                self._pmxt_scan_progress_callback = _scan_progress
                transfer_state["parallel"] = (
                    min(getattr(self, "_pmxt_prefetch_workers", 1), len(hours)) > 1
                )
                _emit_plain_progress_line()
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
                    transfer_state["vendor"] = "loader"
                    progress_state["total_hours"] = 0
                    progress_state["started_hours"] = 0
                    progress_state["completed_hours"] = 0
                    hour_keys_by_label.clear()
                    progress_keys["started"].clear()
                    progress_keys["completed"].clear()
                heartbeat_thread.join(timeout=1.0)

        loader_cls._load_cached_market_batches = patched_cached
        loader_cls._load_local_archive_market_batches = patched_local_archive
        loader_cls._load_remote_market_batches = patched_remote
        loader_cls._load_market_batches = timed_load
        loader_cls._iter_market_batches = patched_iter

        if callable(orig_shared):

            def patched_shared(self, hour, *, requests, batch_size):  # type: ignore[no-untyped-def]
                if not _loader_progress_enabled():
                    return orig_shared(self, hour, requests=requests, batch_size=batch_size)

                parallel = min(getattr(self, "_pmxt_prefetch_workers", 1), 2) > 1
                _enter_grouped_pmxt_loader_callbacks(self)
                _start_grouped_pmxt_progress(hour, parallel=parallel)
                try:
                    return orig_shared(self, hour, requests=requests, batch_size=batch_size)
                finally:
                    _finish_grouped_pmxt_progress(hour)
                    _exit_grouped_pmxt_loader_callbacks(self)

            loader_cls.load_shared_market_batches_for_hour = patched_shared

    def _install_runner_local_archive_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_local_archive = loader_cls._load_local_archive_market_batches

        def patched_local_archive(self, hour, *, batch_size):
            return orig_local_archive(self, hour, batch_size=batch_size)

        loader_cls._load_local_archive_market_batches = patched_local_archive

    def _install_telonex_timing(loader_cls) -> None:  # type: ignore[no-untyped-def]
        orig_load_order_book_deltas = loader_cls.load_order_book_deltas

        def _run_with_telonex_day_timing(self, dates, load_fn):  # type: ignore[no-untyped-def]
            def _day_progress(date: str, event: str, source: str, rows: int) -> None:
                del source, rows
                with pbar_lock:
                    if event == "start":
                        _mark_hour_started(date)
                        _refresh_transfer_status(emit_line=False)
                        _emit_plain_progress_line()
                        return
                    if event != "complete":
                        return
                    _mark_hour_completed(date)
                    _refresh_transfer_status(emit_line=False)
                    _emit_plain_progress_line()

            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                progress_state["total_hours"] = len(dates)
                progress_state["started_hours"] = 0
                progress_state["completed_hours"] = 0
                transfer_state["item_label"] = "days"
                transfer_state["vendor"] = "Telonex"
                hour_keys_by_label.clear()
                progress_keys["started"].clear()
                progress_keys["completed"].clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat, name="telonex-timing-heartbeat", daemon=True
                )
                previous_download_callback = getattr(
                    self, "_telonex_download_progress_callback", None
                )
                previous_day_callback = getattr(self, "_telonex_day_progress_callback", None)
                self._telonex_download_progress_callback = _download_progress
                self._telonex_day_progress_callback = _day_progress
                transfer_state["parallel"] = (
                    min(getattr(self, "_telonex_prefetch_workers", 1), len(dates)) > 1
                )
                _emit_plain_progress_line()
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
                    transfer_state["vendor"] = "loader"
                    progress_state["total_hours"] = 0
                    progress_state["started_hours"] = 0
                    progress_state["completed_hours"] = 0
                    hour_keys_by_label.clear()
                    progress_keys["started"].clear()
                    progress_keys["completed"].clear()
                heartbeat_thread.join(timeout=1.0)

        def timed_load_order_book_deltas(
            self,
            start,
            end,
            *,
            market_slug: str,
            token_index: int,
            outcome: str | None,
            include_order_book: bool = True,
        ):
            if not _loader_progress_enabled():
                return orig_load_order_book_deltas(
                    self,
                    start,
                    end,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    include_order_book=include_order_book,
                )

            dates = self._date_range(start, end)
            return _run_with_telonex_day_timing(
                self,
                dates,
                lambda: orig_load_order_book_deltas(
                    self,
                    start,
                    end,
                    market_slug=market_slug,
                    token_index=token_index,
                    outcome=outcome,
                    include_order_book=include_order_book,
                ),
            )

        loader_cls.load_order_book_deltas = timed_load_order_book_deltas

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
