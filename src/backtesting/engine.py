"""Simulation loop orchestrator for prediction market backtesting.

Coordinates the data feed, strategy, broker, and portfolio through
a chronological replay of historical trades.
"""

from __future__ import annotations

import random
import time
from collections.abc import Iterable
from datetime import datetime

from src.backtesting.broker import Broker
from src.backtesting.feeds.base import BaseFeed
from src.backtesting.logger import BacktestLogger
from src.backtesting.metrics import compute_metrics
from src.backtesting.models import (
    BacktestResult,
    Fill,
    MarketInfo,
    Platform,
    TradeEvent,
)
from src.backtesting.portfolio import Portfolio
from src.backtesting.strategy import Strategy


class Engine:
    """Central orchestrator for backtesting simulations.

    Replays historical trades in chronological order, firing market
    lifecycle events and checking order fills at each step. Deterministic
    given the same data and strategy parameters.
    """

    def __init__(
        self,
        feed: BaseFeed,
        strategy: Strategy,
        initial_cash: float = 10_000.0,
        commission_rate: float = 0.0,
        snapshot_interval: int = 1000,
        market_ids: list[str] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        progress: bool = True,
        market_sample: float | None = None,
        seed: int = 42,
        verbose: bool = False,
    ):
        self.feed = feed
        self.strategy = strategy
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
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
        broker = Broker(commission_rate=self.commission_rate)
        portfolio = Portfolio(initial_cash=self.initial_cash)
        all_fills: list[Fill] = []
        price_history: dict[str, list[tuple[datetime, float]]] = {}
        filled_market_ids: set[str] = set()
        all_markets = self.feed.markets()

        # Sample a subset of markets if requested
        active_market_ids = self.market_ids
        if self.market_sample is not None and self.market_sample < 1.0:
            pool = list(active_market_ids) if active_market_ids else list(all_markets.keys())
            k = max(1, int(len(pool) * self.market_sample))
            rng = random.Random(self.seed)
            active_market_ids = rng.sample(pool, k)
            all_markets = {mid: all_markets[mid] for mid in active_market_ids if mid in all_markets}

        # Determine platform
        platform = Platform.KALSHI
        if all_markets:
            first_market = next(iter(all_markets.values()))
            platform = first_market.platform

        # Mutable ref so logging wrappers can read the current trade timestamp
        now_ref: list[datetime | None] = [None]

        self._wire_strategy(self.strategy, broker, portfolio, all_markets, logger, now_ref)
        self.strategy.initialize()

        logger.start(None, self.strategy.name, platform.value, len(all_markets), self.initial_cash)

        open_events = self._lifecycle_events(all_markets, "open_time")
        close_events = self._lifecycle_events(all_markets, "close_time")
        opened: set[str] = set()
        closed: set[str] = set()
        resolved: set[str] = set()

        open_idx = 0
        close_idx = 0
        trade_count = 0
        fill_count = 0
        first_time: datetime | None = None
        last_time: datetime | None = None

        trade_iter: Iterable[TradeEvent] = self.feed.trades(
            market_ids=active_market_ids,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        progress_bar: PinnedProgress[TradeEvent] | None = None
        if self.progress:
            from src.backtesting.progress import PinnedProgress

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
            # Route logger output through the scroll region so
            # events stream above the pinned progress bar.
            logger.write_fn = progress_bar.write

        for trade in trade_iter:
            now = trade.timestamp
            now_ref[0] = now
            if first_time is None:
                first_time = now
            last_time = now

            # 1. Fire market lifecycle events up to current time
            open_idx = self._fire_opens(
                now,
                open_events,
                open_idx,
                all_markets,
                opened,
                self.strategy,
                logger,
            )
            close_idx = self._fire_closes(
                now,
                close_events,
                close_idx,
                all_markets,
                closed,
                resolved,
                broker,
                portfolio,
                self.strategy,
                logger,
            )

            # 2. Update mark-to-market price
            portfolio.update_price(trade.market_id, trade.yes_price)

            # 3. Check pending orders for fills
            fills = broker.check_fills(trade, portfolio.cash)
            for fill in fills:
                # Track position state before fill for open/close detection
                pos_before = portfolio.positions.get(fill.market_id)
                qty_before = pos_before.quantity if pos_before else 0.0

                portfolio.apply_fill(fill)
                all_fills.append(fill)
                fill_count += 1
                filled_market_ids.add(fill.market_id)

                pos_after = portfolio.positions[fill.market_id]
                qty_after = pos_after.quantity

                logger.order_filled(now, fill, portfolio.cash)

                # Detect position opened / closed
                if qty_before == 0.0 and qty_after != 0.0:
                    logger.position_opened(
                        now,
                        fill.market_id,
                        qty_after,
                        pos_after.avg_entry_price,
                        portfolio.cash,
                    )
                elif qty_before != 0.0 and qty_after == 0.0:
                    logger.position_closed(now, fill.market_id, pos_after.realized_pnl)

                self.strategy.on_fill(fill)

                # Periodic portfolio update in log
                if fill_count % 10 == 0:
                    snap = portfolio.get_snapshot(now)
                    logger.portfolio_update(now, snap.cash, snap.total_equity, snap.num_positions)

            # 4. Strategy reacts to the trade
            self.strategy.on_trade(trade)

            # 5. Periodic portfolio snapshot
            trade_count += 1
            if trade_count % self.snapshot_interval == 0:
                portfolio.snapshot(now)
                for mid in filled_market_ids:
                    if mid in portfolio._last_prices:
                        price_history.setdefault(mid, []).append((now, portfolio._last_prices[mid]))

        # Resolve remaining markets that have known outcomes
        ts = last_time or datetime.min
        for market_id, info in all_markets.items():
            if info.result is not None and market_id not in resolved:
                # Log close if it wasn't fired during the loop
                if market_id not in closed:
                    closed.add(market_id)
                    logger.market_close(ts, info)

                # Log cancellations
                cancelled = [o for o in broker.pending_orders if o.market_id == market_id]
                broker.cancel_all(market_id)
                for order in cancelled:
                    logger.order_cancelled(ts, order)

                pnl = portfolio.resolve_market(market_id, info.result)
                self.strategy.on_market_resolve(info, info.result)
                resolved.add(market_id)

                logger.market_resolve(ts, info, info.result, pnl)
                if market_id in portfolio.positions and portfolio.positions[market_id].quantity == 0.0:
                    logger.position_closed(ts, market_id, portfolio.positions[market_id].realized_pnl)

        if last_time:
            portfolio.snapshot(last_time)
            for mid in filled_market_ids:
                if mid in portfolio._last_prices:
                    price_history.setdefault(mid, []).append((last_time, portfolio._last_prices[mid]))

        self.strategy.finalize()

        equity_curve = portfolio.equity_curve
        markets_traded = {f.market_id for f in all_fills}

        # Collect realized P&L per market (includes resolution payouts)
        market_pnls = {mid: pos.realized_pnl for mid, pos in portfolio.positions.items() if mid in markets_traded}

        metrics = compute_metrics(equity_curve, all_fills, self.initial_cash, market_pnls)

        # Only count resolved markets that were actually traded
        resolved_and_traded = resolved & markets_traded

        final_equity = equity_curve[-1].total_equity if equity_curve else self.initial_cash
        elapsed = time.monotonic() - wall_start
        logger.end(last_time, portfolio.cash, final_equity, elapsed)

        return BacktestResult(
            equity_curve=equity_curve,
            fills=all_fills,
            metrics=metrics,
            strategy_name=self.strategy.name,
            platform=platform,
            start_time=first_time,
            end_time=last_time,
            initial_cash=self.initial_cash,
            final_equity=final_equity,
            num_markets_traded=len(markets_traded),
            num_markets_resolved=len(resolved_and_traded),
            event_log=logger.lines,
            market_prices=price_history,
            market_pnls=market_pnls,
        )

    def _wire_strategy(
        self,
        strategy: Strategy,
        broker: Broker,
        portfolio: Portfolio,
        markets: dict[str, MarketInfo],
        logger: BacktestLogger,
        now_ref: list[datetime | None],
    ) -> None:
        """Inject broker/portfolio hooks into the strategy with logging wrappers."""

        def _logging_place_order(market_id, action, side, price, quantity, timestamp=None):
            order = broker.place_order(market_id, action, side, price, quantity, timestamp)
            ts = now_ref[0] or datetime.min
            logger.order_submitted(ts, order)
            return order

        def _logging_cancel_order(order_id):
            # Capture order before cancellation for logging
            order = broker._pending.get(order_id)
            result = broker.cancel_order(order_id)
            if result and order is not None:
                ts = now_ref[0] or datetime.min
                logger.order_cancelled(ts, order)
            return result

        def _logging_cancel_all(market_id=None):
            # Capture orders before cancellation for logging
            to_cancel = [o for o in broker.pending_orders if market_id is None or o.market_id == market_id]
            count = broker.cancel_all(market_id)
            ts = now_ref[0] or datetime.min
            for order in to_cancel:
                logger.order_cancelled(ts, order)
            return count

        strategy._place_order = _logging_place_order
        strategy._cancel_order_fn = _logging_cancel_order
        strategy._cancel_all_fn = _logging_cancel_all
        strategy._get_portfolio = lambda: portfolio.get_snapshot(datetime.min)
        strategy._get_open_orders = lambda: broker.pending_orders
        strategy._get_markets = lambda: markets

    @staticmethod
    def _lifecycle_events(
        markets: dict[str, MarketInfo],
        time_field: str,
    ) -> list[tuple[datetime, str]]:
        """Build sorted (timestamp, market_id) list for lifecycle events."""
        events = []
        for mid, info in markets.items():
            t = getattr(info, time_field, None)
            if t is not None:
                events.append((t, mid))
        events.sort(key=lambda x: x[0])
        return events

    @staticmethod
    def _fire_opens(
        now: datetime,
        events: list[tuple[datetime, str]],
        idx: int,
        markets: dict[str, MarketInfo],
        opened: set[str],
        strategy: Strategy,
        logger: BacktestLogger,
    ) -> int:
        """Fire on_market_open for all markets whose open_time <= now."""
        while idx < len(events) and events[idx][0] <= now:
            _, mid = events[idx]
            idx += 1
            if mid not in opened and mid in markets:
                opened.add(mid)
                logger.market_open(now, markets[mid])
                strategy.on_market_open(markets[mid])
        return idx

    @staticmethod
    def _fire_closes(
        now: datetime,
        events: list[tuple[datetime, str]],
        idx: int,
        markets: dict[str, MarketInfo],
        closed: set[str],
        resolved: set[str],
        broker: Broker,
        portfolio: Portfolio,
        strategy: Strategy,
        logger: BacktestLogger,
    ) -> int:
        """Fire on_market_close and on_market_resolve for markets whose close_time <= now."""
        while idx < len(events) and events[idx][0] <= now:
            _, mid = events[idx]
            idx += 1
            if mid not in closed and mid in markets:
                closed.add(mid)
                logger.market_close(now, markets[mid])
                strategy.on_market_close(markets[mid])
                info = markets[mid]
                if info.result is not None and mid not in resolved:
                    # Log cancellations
                    cancelled = [o for o in broker.pending_orders if o.market_id == mid]
                    broker.cancel_all(mid)
                    for order in cancelled:
                        logger.order_cancelled(now, order)

                    pnl = portfolio.resolve_market(mid, info.result)
                    strategy.on_market_resolve(info, info.result)
                    resolved.add(mid)

                    logger.market_resolve(now, info, info.result, pnl)
                    if mid in portfolio.positions and portfolio.positions[mid].quantity == 0.0:
                        logger.position_closed(now, mid, portfolio.positions[mid].realized_pnl)
        return idx
