"""Timing harness — measures per-hour fetch time, source, and overall progress.

Can be used standalone:
    uv run python backtests/_timing_test.py <backtest_file>

Or imported and activated before running any backtest:
    from backtests._timing_test import install_timing
    install_timing()
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import threading
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
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

    source_local = threading.local()
    pbar_state: dict = {"bar": None}
    pbar_lock = threading.Lock()

    orig_load = PolymarketPMXTDataLoader._load_market_batches
    orig_cached = PolymarketPMXTDataLoader._load_cached_market_batches
    orig_relay = PolymarketPMXTDataLoader._load_relay_market_batches
    orig_remote = PolymarketPMXTDataLoader._load_remote_market_batches
    orig_iter = PolymarketPMXTDataLoader._iter_market_batches

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

    def patched_remote(self, hour, *, batch_size):
        result = orig_remote(self, hour, batch_size=batch_size)
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
            pbar_state["bar"] = tqdm(
                total=len(hours),
                desc="Fetching hours",
                unit="hr",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            )
        try:
            yield from orig_iter(self, hours, batch_size=batch_size)
        finally:
            with pbar_lock:
                bar = pbar_state["bar"]
                if bar is not None:
                    bar.close()
                    pbar_state["bar"] = None

    PolymarketPMXTDataLoader._load_cached_market_batches = patched_cached
    PolymarketPMXTDataLoader._load_relay_market_batches = patched_relay
    PolymarketPMXTDataLoader._load_remote_market_batches = patched_remote
    PolymarketPMXTDataLoader._load_market_batches = timed_load
    PolymarketPMXTDataLoader._iter_market_batches = patched_iter


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
            "Usage: uv run python backtests/_timing_test.py <backtest_file>",
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
