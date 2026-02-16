"""Drop-in replacement for Engine that uses the compiled Rust core.

Usage::

    from src.backtesting.rust_engine import Engine  # instead of engine.Engine

The API is identical — same constructor, same ``run() -> BacktestResult``.
Falls back to the pure-Python engine if the Rust module isn't installed.
"""

from __future__ import annotations

import time
from datetime import datetime

from src.backtesting.feeds.base import BaseFeed
from src.backtesting.logger import BacktestLogger
from src.backtesting.metrics import compute_metrics
from src.backtesting.models import BacktestResult, Platform
from src.backtesting.strategy import Strategy

try:
    from backtesting_engine import RustEngine as _RustEngine

    HAS_RUST = True
except ImportError:
    HAS_RUST = False

__all__ = ["Engine", "HAS_RUST"]


class Engine:
    """Backtesting engine powered by a compiled Rust core.

    Same constructor signature as :class:`src.backtesting.engine.Engine`.
    Strategy callbacks still execute in Python; the hot loop (broker,
    portfolio, lifecycle events) runs as compiled Rust via PyO3.
    """

    def __init__(
        self,
        feed: BaseFeed,
        strategy: Strategy,
        initial_cash: float = 10_000.0,
        commission_rate: float = 0.01,
        slippage: float = 0.005,
        liquidity_cap: bool = True,
        snapshot_interval: int = 1000,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        progress: bool = True,
        market_sample: float | None = None,
        seed: int = 42,
        verbose: bool = False,
    ):
        if not HAS_RUST:
            raise ImportError(
                "Rust engine not available. Install with:\n"
                "  cd crates/backtesting_engine && maturin develop --release"
            )

        self.feed = feed
        self.strategy = strategy
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.liquidity_cap = liquidity_cap
        self.snapshot_interval = snapshot_interval
        self.market_ids = market_ids
        self.start_time = start_time
        self.end_time = end_time
        self.progress = progress
        self.market_sample = market_sample
        self.seed = seed
        self.verbose = verbose

    def run(self) -> BacktestResult:
        """Execute the full simulation and return results."""
        wall_start = time.monotonic()
        logger = BacktestLogger(print_live=True)

        # -- Create Rust engine --
        rust = _RustEngine(
            initial_cash=self.initial_cash,
            commission_rate=self.commission_rate,
            slippage=self.slippage,
            liquidity_cap=self.liquidity_cap,
            snapshot_interval=self.snapshot_interval,
        )

        all_markets = self.feed.markets()

        # -- Market sampling (same logic as Python engine) --
        active_market_ids = self.market_ids
        _cached_trade_total: int | None = None
        if self.market_sample is not None and self.market_sample < 1.0:
            volumes = self.feed.market_volumes(
                market_ids=active_market_ids,
                start_time=self.start_time,
                end_time=self.end_time,
            )
            ranked = sorted(volumes, key=volumes.get, reverse=True)  # type: ignore[arg-type]
            k = max(1, int(len(ranked) * self.market_sample))
            active_market_ids = ranked[:k]
            all_markets = {mid: all_markets[mid] for mid in active_market_ids if mid in all_markets}
            _cached_trade_total = sum(volumes[mid] for mid in active_market_ids if mid in volumes)

        # -- Determine platform --
        platform = Platform.KALSHI
        if all_markets:
            first_market = next(iter(all_markets.values()))
            platform = first_market.platform

        # -- Wire strategy to Rust engine --
        def _place_order(market_id, action, side, price, quantity, timestamp=None):
            order = rust.place_order(market_id, action, side, price, quantity)
            if logger.print_live:
                logger.order_submitted(rust.current_time, order)
            return order

        def _cancel_order(order_id):
            return rust.cancel_order(order_id)

        def _cancel_all(market_id=None):
            return rust.cancel_all(market_id)

        self.strategy._place_order = _place_order
        self.strategy._cancel_order_fn = _cancel_order
        self.strategy._cancel_all_fn = _cancel_all
        self.strategy._get_portfolio = rust.get_portfolio_snapshot
        self.strategy._get_open_orders = rust.get_pending_orders
        self.strategy._get_markets = lambda: all_markets

        # -- Trade iterator with optional progress bar --
        trade_iter = self.feed.trades(
            market_ids=active_market_ids,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        progress_bar = None
        if self.progress:
            from src.backtesting.progress import PinnedProgress

            if _cached_trade_total is not None:
                total = _cached_trade_total
            else:
                total = self.feed.trade_count(
                    market_ids=active_market_ids,
                    start_time=self.start_time,
                    end_time=self.end_time,
                )
            progress_bar = PinnedProgress(
                trade_iter,
                total=total,
                desc=self.strategy.name,
                unit=" trades",
            )
            trade_iter = progress_bar
            logger.write_fn = progress_bar.write

        # -- Run the Rust engine --
        logger.start(None, self.strategy.name, platform.value, len(all_markets), self.initial_cash)

        try:
            result_data = rust.run(trade_iter, self.strategy, all_markets, logger.write_fn)
        finally:
            if progress_bar is not None:
                progress_bar._refresh_bar()
                progress_bar._teardown()

        # -- Compute metrics in Python --
        equity_curve = result_data["equity_curve"]
        fills = result_data["fills"]
        market_pnls = result_data.get("market_pnls", {})
        metrics = compute_metrics(equity_curve, fills, self.initial_cash, market_pnls)

        final_equity = result_data["final_equity"]
        elapsed = time.monotonic() - wall_start
        logger.end(result_data.get("end_time"), 0.0, final_equity, elapsed)

        return BacktestResult(
            equity_curve=equity_curve,
            fills=fills,
            metrics=metrics,
            strategy_name=self.strategy.name,
            platform=platform,
            start_time=result_data.get("start_time"),
            end_time=result_data.get("end_time"),
            initial_cash=self.initial_cash,
            final_equity=final_equity,
            num_markets_traded=result_data["num_markets_traded"],
            num_markets_resolved=result_data["num_markets_resolved"],
            event_log=result_data.get("event_log", []),
            market_prices=result_data.get("market_prices", {}),
            market_pnls=market_pnls,
        )
