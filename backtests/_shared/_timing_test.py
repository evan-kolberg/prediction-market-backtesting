"""Timing harness — measures per-hour fetch time, source, and overall progress.

Can be used standalone:
    uv run python backtests/_shared/_timing_test.py <backtest_file>

Or imported and activated before running any backtest:
    from backtests._shared._timing_test import install_timing
    install_timing()
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_installed = False


def install_timing() -> None:
    """Monkey-patch the PMXT loader to show per-hour progress, timing, and source."""
    global _installed
    if _installed:
        return
    _installed = True

    from tqdm import tqdm
    from nautilus_trader.adapters.polymarket.pmxt import PolymarketPMXTDataLoader

    try:
        from backtests._shared.data_sources.pmxt import (
            RunnerPolymarketPMXTDataLoader,
        )
    except ImportError:
        RunnerPolymarketPMXTDataLoader = None

    source_local = threading.local()
    pbar_state: dict = {"bar": None}
    pbar_lock = threading.Lock()
    transfer_state: dict[str, object] = {
        "downloads": {},
        "stop": threading.Event(),
        "spinner_index": 0,
    }

    def _transfer_label(url: str) -> str:
        filename = Path(urlparse(url).path).name
        hour_label = filename.removeprefix("polymarket_orderbook_").removesuffix(
            ".parquet"
        )
        if "/v1/raw/" in url:
            return f"relay raw {hour_label}"
        return f"r2 raw {hour_label}"

    def _ensure_transfer_state(
        *,
        url: str,
        total_bytes: int | None,
    ) -> dict[str, object]:
        downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
        state = downloads.get(url)
        if state is None:
            state = {
                "url": url,
                "started_at": time.monotonic(),
                "downloaded_bytes": 0,
                "total_bytes": total_bytes,
            }
            downloads[url] = state
        elif total_bytes is not None:
            state["total_bytes"] = total_bytes
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
            downloaded_bytes = int(state["downloaded_bytes"])
            total_bytes = state["total_bytes"]
            if total_bytes:
                mib_total = total_bytes / (1024 * 1024)
                mib_downloaded = downloaded_bytes / (1024 * 1024)
                labels.append(
                    f"{_transfer_label(str(state['url']))} {mib_downloaded:,.1f}/{mib_total:,.1f} MiB {elapsed:4.1f}s"
                )
            else:
                mib_downloaded = downloaded_bytes / (1024 * 1024)
                labels.append(
                    f"{_transfer_label(str(state['url']))} {mib_downloaded:,.1f} MiB {elapsed:4.1f}s"
                )
        if len(active_downloads) > len(labels):
            labels.append(f"+{len(active_downloads) - len(labels)} more")
        return f"{spinner} " + " | ".join(labels)

    def _refresh_transfer_status() -> None:
        bar = pbar_state["bar"]
        if bar is None:
            return
        status_text = _transfer_status_text()
        bar.set_postfix_str(status_text, refresh=True)

    def _download_progress(
        url: str,
        downloaded_bytes: int,
        total_bytes: int | None,
        finished: bool,
    ) -> None:
        with pbar_lock:
            state = _ensure_transfer_state(url=url, total_bytes=total_bytes)
            state["downloaded_bytes"] = downloaded_bytes
            state["total_bytes"] = total_bytes
            _refresh_transfer_status()
            if finished:
                _close_transfer_state(url)
                _refresh_transfer_status()

    def _transfer_heartbeat() -> None:
        stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
        while not stop_event.wait(0.2):
            with pbar_lock:
                downloads: dict[str, dict[str, object]] = transfer_state["downloads"]  # type: ignore[assignment]
                if downloads:
                    _refresh_transfer_status()

    def _start_transfer(url: str | None) -> None:
        if url is None:
            return
        with pbar_lock:
            _ensure_transfer_state(url=url, total_bytes=None)
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
        orig_relay = loader_cls._load_relay_market_batches
        orig_relay_raw = loader_cls._load_relay_raw_market_batches
        orig_local_archive = loader_cls._load_local_archive_market_batches
        orig_remote = loader_cls._load_remote_market_batches
        orig_iter = loader_cls._iter_market_batches

        def patched_cached(self, hour):
            result = orig_cached(self, hour)
            if result is not None:
                cache_path = self._cache_path_for_hour(hour)
                source_local.source = str(cache_path)
            return result

        def patched_relay(self, hour, *, batch_size):
            result = orig_relay(self, hour, batch_size=batch_size)
            if result is not None:
                source_local.source = self._pmxt_relay_base_url or "relay"
            return result

        def patched_relay_raw(self, hour, *, batch_size):
            relay_raw_url = self._relay_raw_url_for_hour(hour)
            _start_transfer(relay_raw_url)
            try:
                result = orig_relay_raw(self, hour, batch_size=batch_size)
            finally:
                _finish_transfer(relay_raw_url)
            if result is not None:
                source_local.source = relay_raw_url or "relay-raw"
            return result

        def patched_local_archive(self, hour, *, batch_size):
            result = orig_local_archive(self, hour, batch_size=batch_size)
            if result is not None:
                archive_paths = self._local_archive_paths_for_hour(hour)
                existing_path = next(
                    (path for path in archive_paths if path.exists()),
                    None,
                )
                source_local.source = (
                    str(existing_path) if existing_path else "local-raw"
                )
            return result

        def patched_remote(self, hour, *, batch_size):
            remote_url = self._archive_url_for_hour(hour)
            _start_transfer(remote_url)
            try:
                result = orig_remote(self, hour, batch_size=batch_size)
            finally:
                _finish_transfer(remote_url)
            if result is not None:
                source_local.source = self._PMXT_BASE_URL
            return result

        def timed_load(self, hour, *, batch_size):
            source_local.source = "none"
            t0 = time.perf_counter()
            result = orig_load(self, hour, batch_size=batch_size)
            elapsed = time.perf_counter() - t0
            rows = sum(b.num_rows for b in result) if result else 0
            source = getattr(source_local, "source", "unknown")

            with pbar_lock:
                bar = pbar_state["bar"]
                if bar is not None:
                    bar.write(
                        f"  {hour.isoformat():>25s}  {elapsed:6.3f}s  {rows:>6} rows  {source}"
                    )
                    bar.update(1)
            return result

        def patched_iter(self, hours, *, batch_size):
            with pbar_lock:
                stop_event: threading.Event = transfer_state["stop"]  # type: ignore[assignment]
                stop_event.clear()
                heartbeat_thread = threading.Thread(
                    target=_transfer_heartbeat,
                    name="pmxt-timing-heartbeat",
                    daemon=True,
                )
                pbar_state["bar"] = tqdm(
                    total=len(hours),
                    desc="Fetching hours",
                    unit="hr",
                    bar_format=(
                        "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
                        "{postfix}"
                    ),
                )
                previous_callback = getattr(
                    self,
                    "_pmxt_download_progress_callback",
                    None,
                )
                self._pmxt_download_progress_callback = _download_progress
                heartbeat_thread.start()
            try:
                yield from orig_iter(self, hours, batch_size=batch_size)
            finally:
                with pbar_lock:
                    self._pmxt_download_progress_callback = previous_callback
                    stop_event.set()
                    downloads: dict[str, dict[str, object]] = transfer_state[
                        "downloads"
                    ]  # type: ignore[assignment]
                    downloads.clear()
                    bar = pbar_state["bar"]
                    if bar is not None:
                        bar.set_postfix_str("", refresh=False)
                        bar.close()
                        pbar_state["bar"] = None
                heartbeat_thread.join(timeout=1.0)

        loader_cls._load_cached_market_batches = patched_cached
        loader_cls._load_relay_market_batches = patched_relay
        loader_cls._load_relay_raw_market_batches = patched_relay_raw
        loader_cls._load_local_archive_market_batches = patched_local_archive
        loader_cls._load_remote_market_batches = patched_remote
        loader_cls._load_market_batches = timed_load
        loader_cls._iter_market_batches = patched_iter

    def _install_runner_local_archive_timing(
        loader_cls,
    ) -> None:  # type: ignore[no-untyped-def]
        orig_local_archive = loader_cls._load_local_archive_market_batches

        def patched_local_archive(self, hour, *, batch_size):
            result = orig_local_archive(self, hour, batch_size=batch_size)
            if result is not None:
                raw_path = self._raw_path_for_hour(hour)
                if raw_path is not None and raw_path.exists():
                    source_local.source = str(raw_path)
                else:
                    archive_paths = self._local_archive_paths_for_hour(hour)
                    existing_path = next(
                        (path for path in archive_paths if path.exists()),
                        None,
                    )
                    source_local.source = (
                        str(existing_path) if existing_path is not None else "local-raw"
                    )
            return result

        loader_cls._load_local_archive_market_batches = patched_local_archive

    _install_full_timing(PolymarketPMXTDataLoader)
    if RunnerPolymarketPMXTDataLoader is not None:
        _install_runner_local_archive_timing(RunnerPolymarketPMXTDataLoader)


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
            "Usage: uv run python backtests/_shared/_timing_test.py <backtest_file>",
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
